"""Measured HTTP-only client for authorized loopback load tests; no implicit TLS context."""
from http.client import HTTPConnection
import time


class MeasuredResponse:
    def __init__(self, connection, response, timings, started, max_bytes):
        self.connection = connection
        self.response = response
        self.timings = timings
        self.started = started
        self.max_bytes = max_bytes
        self.bytes_read = 0

    def __getattr__(self, name):
        return getattr(self.response, name)

    def __enter__(self):
        return self

    def read(self, amount=None):
        started = time.perf_counter()
        try:
            remaining = self.max_bytes - self.bytes_read
            data = self.response.read(min(amount, remaining + 1) if amount is not None else remaining + 1)
            self.bytes_read += len(data)
            if self.bytes_read > self.max_bytes:
                raise ValueError("load-test response exceeds configured limit")
            return data
        finally:
            self.timings['body_read_ms'] = self.timings.get('body_read_ms', 0) + (time.perf_counter()-started)*1000
            self.timings['client_total_ms'] = (time.perf_counter()-self.started)*1000

    def close(self):
        try:
            self.response.close()
        finally:
            self.connection.close()
            self.timings['client_total_ms'] = (time.perf_counter()-self.started)*1000

    def __exit__(self, *exc):
        self.close()


def measured_http(host, port, path, key, body=None, timeout=15, timings=None, max_bytes=16*1024*1024):
    """Use only with HTTP loopback ingress; returns HTTP errors as measured responses."""
    if host not in {'127.0.0.1', '::1', 'localhost'}:
        raise ValueError('This diagnostic client is restricted to loopback HTTP')
    if not path.startswith('/') or path.startswith('//'):
        raise ValueError('Expected an absolute local HTTP path')
    timings = {} if timings is None else timings
    started = time.perf_counter()
    connection = HTTPConnection(host, port, timeout=timeout)
    headers = {'Content-Type': 'application/json', 'Connection': 'close'}
    if key:
        headers['Authorization'] = 'Bearer ' + key
    mark = time.perf_counter()
    timings['prepare_ms'] = (mark-started)*1000
    stage = 'connect'
    try:
        connection.connect()
        now = time.perf_counter()
        timings['connect_ms'] = (now-mark)*1000
        mark, stage = now, 'send'
        connection.request('POST' if body is not None else 'GET', path, body, headers)
        now = time.perf_counter()
        timings['send_ms'] = (now-mark)*1000
        mark, stage = now, 'response_headers'
        response = connection.getresponse()
        now = time.perf_counter()
        timings['response_headers_ms'] = (now-mark)*1000
        timings['client_total_ms'] = (now-started)*1000
        return MeasuredResponse(connection, response, timings, started, max_bytes)
    except BaseException:
        timings[stage+'_failed_ms'] = (time.perf_counter()-mark)*1000
        timings['client_total_ms'] = (time.perf_counter()-started)*1000
        connection.close()
        raise
