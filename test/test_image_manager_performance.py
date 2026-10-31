from __future__ import annotations

import asyncio
import io
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI
from PIL import Image

from services.storage.image_index_storage import ImageIndexStorage
from services.image_storage_service import ImageStorageService
import services.image_service as manager
import services.image_storage_service as storage_module


@pytest.fixture
def store(tmp_path):
    index = ImageIndexStorage(tmp_path / 'image_index.json')
    yield index
    index.close()


def seed(store, count=35):
    items = {f'2026/10/{28 + n % 2}/{n:04d}.png': {
        'date': f'2026-10-{28 + n % 2}', 'created_at': f'2026-10-{28 + n % 2} 10:{n:02d}:00',
        'size': 1024, 'local': True, 'name': f'{n:04d}.png'} for n in range(count)}
    store.replace(items)
    return items


def test_index_pagination_dates_paths_and_stable_order(store):
    items = seed(store)
    expected = sorted(items, key=lambda k: (items[k]['created_at'], k), reverse=True)
    result = store.list_page(page=2, page_size=12)
    assert result['total'] == 35
    assert [x['path'] for x in result['items']] == expected[12:24]
    filtered = store.list_page(start_date='2026-10-29', end_date='2026-10-29')
    assert filtered['total'] == 17
    assert all(x['date'] == '2026-10-29' for x in filtered['items'])
    chosen = expected[:4]
    assert store.list_page(matching_paths=chosen, paths_only=True)['paths'] == chosen
    assert store.list_page(matching_paths=[])['total'] == 0
    assert store.list_page(page=999, page_size=12)['page'] == 3
    assert len(store.list_page(page=999, page_size=12)['items']) == 11
    store.delete(expected[0])
    assert store.list_page()['total'] == 34


def test_index_expression_query_plan_uses_created_index(store):
    seed(store)
    with store.connection() as connection:
        plan = str(connection.execute("EXPLAIN QUERY PLAN SELECT path,data FROM images ORDER BY json_extract(data, '$.created_at') DESC,path DESC LIMIT 12").fetchall())
    assert 'images_created' in plan
    assert 'TEMP B-TREE' not in plan


def test_indexed_page_never_scans_directory_or_decodes_originals(tmp_path, monkeypatch):
    service = ImageStorageService(tmp_path / 'image_index.json')
    try:
        seed(service._index_store)
        monkeypatch.setattr(service, 'list_items', Mock(side_effect=AssertionError('full directory scan')))
        monkeypatch.setattr(service, '_load_index', Mock(side_effect=AssertionError('full metadata load')))
        monkeypatch.setattr(manager, 'image_storage_service', service)
        monkeypatch.setattr(service, '_public_url', lambda rel, base: base + '/images/' + rel)
        monkeypatch.setattr(manager, 'load_tags', lambda: {'2026/10/29/0001.png': ['a', 'b']})
        result = manager.list_images_page('https://current.test', tags=['a', 'b'])
        assert result['total'] == 1
        assert len(result['items']) == 1
        assert result['items'][0]['thumbnail_url'].startswith('https://current.test/image-thumbnails/')
        assert manager.list_images_page('', tags=['missing'])['items'] == []
        assert manager.list_images_page('', paths_only=True)['total'] == 35
    finally:
        service._index_store.close()


def test_empty_legacy_index_discovers_files_once(tmp_path, monkeypatch):
    root = tmp_path / 'images'
    root.mkdir()
    Image.new('RGB', (10, 20)).save(root / 'legacy.png')
    cfg = SimpleNamespace(images_dir=root, base_url='', get_image_storage_settings=lambda: {})
    monkeypatch.setattr(storage_module, 'config', cfg)
    service = ImageStorageService(tmp_path / 'image_index.json')
    try:
        assert service.list_page()['total'] == 1
        monkeypatch.setattr(service, 'list_items', Mock(side_effect=AssertionError('repeated scan')))
        assert service.list_page()['total'] == 1
    finally:
        service._index_store.close()


def test_thumbnail_singleflight_atomic_cache_and_headers(tmp_path, monkeypatch):
    images, thumbs = tmp_path / 'images', tmp_path / 'thumbs'
    images.mkdir()
    source = images / 'sample.png'
    Image.new('RGB', (640, 480), 'red').save(source)
    monkeypatch.setattr(manager, 'config', SimpleNamespace(images_dir=images, image_thumbnails_dir=thumbs))
    monkeypatch.setattr(manager, 'image_storage_service', SimpleNamespace(has_local=lambda _: True))
    original = Image.open
    calls = []
    def tracked(*args, **kwargs):
        calls.append(1)
        time.sleep(0.02)
        return original(*args, **kwargs)
    monkeypatch.setattr(manager.Image, 'open', tracked)
    with ThreadPoolExecutor(8) as pool:
        targets = list(pool.map(lambda _: manager.ensure_thumbnail('sample.png'), range(8)))
    assert len(calls) == 1
    assert len(set(targets)) == 1
    with original(targets[0]) as image:
        assert image.size == (320, 240)
        image.load()
    assert not list(thumbs.rglob('*.tmp'))
    assert not manager._THUMBNAIL_LOCKS._entries
    response = manager.get_thumbnail_response('sample.png')
    assert response.headers['cache-control'] == 'public, max-age=300'
    assert len(calls) == 1


def test_image_routes_offload_thumbnail_and_list_work(monkeypatch):
    import api.system as routes
    event_loop_thread = threading.get_ident()
    worker_threads = []
    def page(*args, **kwargs):
        worker_threads.append(threading.get_ident())
        return {'items': [], 'total': 0, 'page': 1, 'page_size': 12}
    def thumbnail(path):
        worker_threads.append(threading.get_ident())
        from starlette.responses import Response
        return Response(b'png', media_type='image/png')
    monkeypatch.setattr(routes, 'require_admin', lambda _: None)
    monkeypatch.setattr(routes, 'list_images_page', page)
    monkeypatch.setattr(routes, 'get_thumbnail_response', thumbnail)
    app = FastAPI()
    app.include_router(routes.create_router("test"))
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            assert (await client.get('/api/images?page=1&page_size=12')).json()['total'] == 0
            assert (await client.get('/image-thumbnails/sample.png')).status_code == 200
            assert (await client.get('/api/images?page=0')).status_code == 422
    asyncio.run(exercise())
    assert len(worker_threads) == 2
    assert all(t != event_loop_thread for t in worker_threads)
