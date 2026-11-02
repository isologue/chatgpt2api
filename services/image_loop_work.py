"""图片循环工作时间线；热路径仅更新循环私有计数，每 0.5 秒发布一次。"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from threading import Lock, get_native_id
from weakref import WeakSet

_CATEGORIES = frozenset({'curl_socket', 'curl_completion', 'business_slice',
    'curl_socket_event', 'curl_timer_event', 'curl_watchdog_event',
    'curl_bootstrap', 'curl_download', 'curl_stream', 'curl_metadata'})
_EVENTS = frozenset({'socket_events', 'timer_events', 'watchdog_events', 'coalesced_events',
    'batches', 'yields', 'max_pending', 'max_wait_ms', 'timer_cancels',
    'pressure_batches', 'wait_count', 'wait_total_ms', 'max_socket_wait_ms',
    'max_timer_wait_ms', 'max_watchdog_wait_ms'})
_recorders = WeakSet()
_registry_lock = Lock()


class LoopWorkRecorder:
    WINDOW_SECS = 300
    PUBLISH_SECS = 0.5

    def __init__(self):
        self.native_thread_id = get_native_id()
        self._offset = time.time() - time.perf_counter()
        self._started = time.perf_counter()
        self._work = {}
        self._events = {}
        self._lag_ms = 0.0
        self._history = deque(maxlen=600)
        self._slow = deque(maxlen=8)
        self._download_bytes = 0
        self._download_total = 0
        self._download_state = {'active': 0, 'waiting': 0, 'limit': 0}
        self._lock = Lock()
        with _registry_lock:
            _recorders.add(self)

    def record(self, category, started, elapsed, cpu):
        # 只从所属事件循环调用；不保存 task、账号、URL、正文或请求上下文。
        if category not in _CATEGORIES:
            return
        item = self._work.get(category)
        if item is None:
            item = self._work[category] = {'count': 0, 'total_ms': 0.0,
                'cpu_ms': 0.0, 'max_ms': 0.0, 'max_at': 0.0}
        ms = max(0.0, elapsed * 1000)
        item['count'] += 1
        item['total_ms'] += ms
        item['cpu_ms'] += max(0.0, cpu * 1000)
        if ms >= item['max_ms']:
            item['max_ms'] = ms
            item['max_at'] = self._offset + started

    def event(self, name, value=1):
        if name not in _EVENTS:
            return
        previous = self._events.get(name, 0)
        self._events[name] = max(previous, value) if name.startswith('max_') else previous + value

    def slow_callback(self, stage, source, started, elapsed, cpu, *, phase=None):
        # 固定枚举和数值，每个周期最多八项，不保存请求或连接信息。
        self._slow.append({'at': self._offset + started, 'stage': stage,
            **({'phase': phase} if phase is not None else {}),
            'source': source, 'wall_ms': round(elapsed * 1000, 3),
            'cpu_ms': round(max(0.0, cpu) * 1000, 3)})

    def download_bytes(self, amount):
        self._download_bytes += amount
        self._download_total += amount

    def download_state(self, active, waiting, limit):
        self._download_state = {'active': active, 'waiting': waiting, 'limit': limit}

    def flush(self, lag=0.0, *, force=False):
        now = time.perf_counter()
        self._lag_ms = max(self._lag_ms, lag * 1000)
        if not force and now - self._started < self.PUBLISH_SECS:
            return
        sample = {'at': self._offset + now, 'started_at': self._offset + self._started,
            'span_ms': (now - self._started) * 1000, 'lag_ms': self._lag_ms,
            'work': self._work, 'curl_dispatch': self._events,
            'slow_callbacks': list(self._slow),
            'download': {**self._download_state, 'bytes': self._download_bytes,
                'total_bytes': self._download_total,
                'bytes_per_sec': self._download_bytes / max(.000001, now - self._started)}}
        self._slow.clear()
        self._download_bytes = 0
        self._work, self._events, self._lag_ms, self._started = {}, {}, 0.0, now
        with self._lock:
            self._history.append((now, sample))

    def samples(self, now):
        with self._lock:
            return tuple(sample for mono, sample in self._history if mono >= now - self.WINDOW_SECS)


def get_loop_work():
    loop = asyncio.get_running_loop()
    recorder = getattr(loop, '_image_loop_work', None)
    if recorder is None:
        recorder = loop._image_loop_work = LoopWorkRecorder()
    return recorder


def snapshot():
    now = time.perf_counter()
    with _registry_lock:
        recorders = tuple(_recorders)
    samples = [sample for recorder in recorders for sample in recorder.samples(now)]
    buckets = {}
    for sample in samples:
        # 以完成时刻分桶。跨桶的长工作仍保留实际开始时刻和单次最大值。
        key = int(sample['at'] // 5) * 5
        bucket = buckets.setdefault(key, {'at': key, 'lag_ms': 0.0, 'work': {}, 'curl_dispatch': {},
            'slow_callbacks': [], 'download': {'bytes': 0, 'span_ms': 0.0, 'peak_active': 0, 'peak_waiting': 0}})
        bucket['lag_ms'] = max(bucket['lag_ms'], sample['lag_ms'])
        bucket['slow_callbacks'] = sorted(bucket['slow_callbacks'] + sample['slow_callbacks'],
            key=lambda item: item['wall_ms'], reverse=True)[:8]
        download = bucket['download']
        download['bytes'] += sample['download']['bytes']
        download['span_ms'] += sample['span_ms']
        download['peak_active'] = max(download['peak_active'], sample['download']['active'])
        download['peak_waiting'] = max(download['peak_waiting'], sample['download']['waiting'])
        for name, value in sample['curl_dispatch'].items():
            old = bucket['curl_dispatch'].get(name, 0)
            bucket['curl_dispatch'][name] = max(old, value) if name.startswith('max_') else old + value
        for name, value in sample['work'].items():
            item = bucket['work'].setdefault(name, {'count': 0, 'total_ms': 0.0,
                'cpu_ms': 0.0, 'max_ms': 0.0, 'max_at': 0.0})
            for field in ('count', 'total_ms', 'cpu_ms'):
                item[field] += value[field]
            if value['max_ms'] >= item['max_ms']:
                item['max_ms'], item['max_at'] = value['max_ms'], value['max_at']
    timeline = [buckets[key] for key in sorted(buckets)]
    for bucket in timeline:
        bucket['lag_ms'] = round(bucket['lag_ms'], 3)
        download = bucket['download']
        download['bytes_per_sec'] = round(download['bytes'] * 1000 / max(.001, download['span_ms']), 3)
        for key in bucket['curl_dispatch']:
            if key.endswith('_ms'):
                bucket['curl_dispatch'][key] = round(bucket['curl_dispatch'][key], 3)
        for item in bucket['work'].values():
            for field in ('total_ms', 'cpu_ms', 'max_ms'):
                item[field] = round(item[field], 3)
    # 只汇总每个循环的最新样本，暂停发布的循环不冒充实时速率。
    latest = [rows[-1] for recorder in recorders if (rows := recorder.samples(now))
        and now - (rows[-1]['at'] - recorder._offset) <= 2]
    current = {name: sum(row['download'][name] for row in latest)
        for name in ('active', 'waiting', 'limit', 'bytes_per_sec', 'total_bytes')}
    current.update({'sampled_at': max((row['at'] for row in latest), default=None),
        'rate_scope': 'application_body_read', 'stale': not latest,
        'native_thread_ids': [recorder.native_thread_id for recorder in recorders]})
    return {'window_secs': 300, 'bucket_secs': 5, 'timeline': timeline, 'download': current,
        'categories': sorted(_CATEGORIES)}
