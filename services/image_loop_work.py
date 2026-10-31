"""图片循环工作时间线；热路径仅更新循环私有计数，每 0.5 秒发布一次。"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from threading import Lock
from weakref import WeakSet

_CATEGORIES = frozenset({'curl_socket', 'curl_completion', 'business_slice'})
_recorders = WeakSet()
_registry_lock = Lock()


class LoopWorkRecorder:
    WINDOW_SECS = 300
    PUBLISH_SECS = 0.5

    def __init__(self):
        self._offset = time.time() - time.perf_counter()
        self._started = time.perf_counter()
        self._work = {}
        self._lag_ms = 0.0
        self._history = deque(maxlen=600)
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

    def flush(self, lag=0.0, *, force=False):
        now = time.perf_counter()
        self._lag_ms = max(self._lag_ms, lag * 1000)
        if not force and now - self._started < self.PUBLISH_SECS:
            return
        sample = {'at': self._offset + now, 'started_at': self._offset + self._started,
            'span_ms': (now - self._started) * 1000, 'lag_ms': self._lag_ms,
            'work': self._work}
        self._work, self._lag_ms, self._started = {}, 0.0, now
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
        bucket = buckets.setdefault(key, {'at': key, 'lag_ms': 0.0, 'work': {}})
        bucket['lag_ms'] = max(bucket['lag_ms'], sample['lag_ms'])
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
        for item in bucket['work'].values():
            for field in ('total_ms', 'cpu_ms', 'max_ms'):
                item[field] = round(item[field], 3)
    return {'window_secs': 300, 'bucket_secs': 5, 'timeline': timeline,
        'categories': sorted(_CATEGORIES)}
