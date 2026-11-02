import asyncio
import hashlib
from functools import partial
import json
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
from types import SimpleNamespace

import pytest
from curl_cffi import CurlOpt
from curl_cffi.aio import ffi
from curl_cffi.requests.exceptions import RequestException

from services import image_ca
from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageSession


@pytest.fixture(autouse=True)
def reset_directory_cache():
    image_ca._default_ca_directory.cache_clear()
    yield
    image_ca._default_ca_directory.cache_clear()


def prepare_manifest(monkeypatch, tmp_path):
    bundle = tmp_path / 'bundle.pem'
    bundle.write_bytes(b'fixture-bundle')
    directory = tmp_path / 'ca'
    directory.mkdir()
    (directory / 'manifest.json').write_text(json.dumps({
        'version': 1, 'certificates': 1,
        'sha256': hashlib.sha256(bundle.read_bytes()).hexdigest(),
    }), encoding='utf8')
    monkeypatch.setattr(image_ca, 'sys', SimpleNamespace(platform='linux'))
    monkeypatch.setattr(image_ca, 'DEFAULT_CACERT', str(bundle))
    monkeypatch.setattr(image_ca, 'CA_DIRECTORY', directory)
    return bundle, directory


def test_only_default_verified_requests_use_identical_trust_directory(monkeypatch, tmp_path):
    _, directory = prepare_manifest(monkeypatch, tmp_path)
    original = {CurlOpt.TIMEOUT_MS: 15000}
    result = image_ca.image_ca_options(original, True)
    assert result[CurlOpt.CAPATH] == str(directory)
    assert result[CurlOpt.CAINFO] == ffi.NULL
    assert original == {CurlOpt.TIMEOUT_MS: 15000}
    for verify in (False, None, '/custom/roots.pem'):
        assert image_ca.image_ca_options(original, verify) is original
    for option in (CurlOpt.CAINFO, CurlOpt.CAPATH, CurlOpt.CAINFO_BLOB, CurlOpt.SSL_CTX_FUNCTION):
        custom = {option: 'custom'}
        assert image_ca.image_ca_options(custom, True) is custom


@pytest.mark.parametrize('problem', ['missing', 'changed_bundle', 'invalid_json', 'wrong_version'])
def test_missing_or_stale_directory_preserves_original_curl_defaults(monkeypatch, tmp_path, problem):
    bundle, directory = prepare_manifest(monkeypatch, tmp_path)
    manifest = directory / 'manifest.json'
    if problem == 'missing':
        manifest.unlink()
    elif problem == 'changed_bundle':
        bundle.write_bytes(b'new-trust-set')
    elif problem == 'invalid_json':
        manifest.write_text('{')
    else:
        manifest.write_text('{"version": 2, "certificates": 1}')
    original = {}
    assert image_ca.image_ca_options(original, True) is original


def test_ca_builder_rejects_empty_bundle_and_existing_directory(tmp_path):
    bundle = tmp_path / 'empty.pem'
    bundle.write_bytes(b'no certificates')
    with pytest.raises(ValueError, match='no certificates'):
        image_ca.build_ca_directory(bundle, tmp_path / 'ca')
    assert not (tmp_path / 'ca').exists()
    bundle.write_bytes(b'-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----')
    target = tmp_path / 'exists'
    target.mkdir()
    with pytest.raises(FileExistsError):
        image_ca.build_ca_directory(bundle, target)


def test_ca_builder_rejects_incomplete_hash_index(monkeypatch, tmp_path):
    bundle = tmp_path / 'bundle.pem'
    bundle.write_bytes(b'-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----')
    monkeypatch.setattr(image_ca.subprocess, 'run', lambda *args, **kwargs: None)
    with pytest.raises(ValueError, match='complete bundle'):
        image_ca.build_ca_directory(bundle, tmp_path / 'ca')


@pytest.mark.skipif(sys.platform != 'linux' or not shutil.which('openssl'), reason='Linux TLS integration')
def test_native_tls_keeps_chain_hostname_and_explicit_ca_verification(monkeypatch, tmp_path):
    def certificate(name):
        cert, key = tmp_path / f'{name}.pem', tmp_path / f'{name}.key'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
            '-days', '1', '-keyout', str(key), '-out', str(cert), '-subj', '/CN=localhost',
            '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return cert, key
    trusted, trusted_key = certificate('trusted')
    unknown, unknown_key = certificate('unknown')
    bundle = tmp_path / 'full.pem'
    bundle.write_bytes(Path(image_ca.DEFAULT_CACERT).read_bytes() + trusted.read_bytes())
    directory = tmp_path / 'trust'
    count = image_ca.build_ca_directory(bundle, directory)
    assert count > 1 and list(directory.glob('*.0'))
    monkeypatch.setattr(image_ca, 'DEFAULT_CACERT', str(bundle))
    monkeypatch.setattr(image_ca, 'CA_DIRECTORY', directory)

    async def scenario():
        handlers = set()
        async def serve(reader, writer):
            handlers.add(asyncio.current_task())
            try:
                await reader.readuntil(b'\r\n\r\n')
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok')
                await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                try: await writer.wait_closed()
                except ConnectionError: pass
                handlers.discard(asyncio.current_task())
        servers = []
        for cert, key in ((trusted, trusted_key), (unknown, unknown_key)):
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            servers.append(await asyncio.start_server(serve, '127.0.0.1', 0, ssl=context))
        good, bad = [server.sockets[0].getsockname()[1] for server in servers]
        def request(port, **kwargs):
            options = kwargs.pop('curl_options', {})
            session = AsyncImageSession(trust_env=False, verify=True, curl_options=options)
            try:
                response = session.get(f'https://127.0.0.1:{port}/', timeout=3, **kwargs)
                assert response.content == b'ok'
                # 请求级优化不污染后续传输或调用方选项。
                assert session.session.curl_options == options
            finally: session.close()
        def wrong_hostname():
            session = AsyncImageSession(trust_env=False, verify=True,
                curl_options={CurlOpt.RESOLVE: [f'wrong.test:{good}:127.0.0.1']})
            try: session.get(f'https://wrong.test:{good}/', timeout=3)
            finally: session.close()
        try:
            await run_cooperative(request, good)
            with pytest.raises(RequestException) as error:
                await run_cooperative(request, bad)
            assert error.value.code == 60
            with pytest.raises(RequestException) as error:
                await run_cooperative(wrong_hostname)
            assert error.value.code == 60
            await run_cooperative(partial(request, good, verify=str(bundle)))
            with pytest.raises(RequestException):
                await run_cooperative(partial(request, good, verify=str(unknown)))
            await run_cooperative(partial(request, bad, verify=False))
        finally:
            for server in servers:
                server.close()
                await server.wait_closed()
            await asyncio.gather(*tuple(handlers), return_exceptions=True)
    asyncio.run(scenario())
