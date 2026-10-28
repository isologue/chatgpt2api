import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from utils.log import BufferedStreamHandler


class SlowSink(logging.Handler):
    def __init__(self):
        super().__init__()
        self.entered = Event()
        self.release = Event()
        self.messages = []

    def emit(self, record):
        self.entered.set()
        assert self.release.wait(5)
        self.messages.append(record.getMessage())


def record(message):
    return logging.LogRecord("test", logging.INFO, __file__, 1, message, (), None)


def test_slow_output_does_not_block_producers():
    sink = SlowSink()
    handler = BufferedStreamHandler(sink, capacity=128)
    try:
        assert handler.handle(record("first"))
        assert sink.entered.wait(2)
        with ThreadPoolExecutor(8) as pool:
            pending = [pool.submit(handler.handle, record(str(n))) for n in range(100)]
            assert all(future.result(timeout=1) for future in pending)
        assert handler.stats()["pending"] == 101
        assert not handler.flush(timeout=0.01)
        sink.release.set()
        assert handler.flush()
        assert len(sink.messages) == 101
        assert handler.stats()["pending"] == 0
    finally:
        sink.release.set()
        handler.close()


def test_full_runtime_log_queue_is_bounded_and_recovers():
    sink = SlowSink()
    handler = BufferedStreamHandler(sink, capacity=1)
    try:
        assert handler.handle(record("first"))
        assert sink.entered.wait(2)
        assert handler.handle(record("second"))
        assert not handler.handle(record("overflow"))
        assert handler.stats()["queued"] == 1
        assert handler.stats()["dropped"] == 1
        sink.release.set()
        assert handler.flush()
        assert handler.handle(record("after"))
        handler.close()
        assert sink.messages == ["first", "second", "after"]
        assert not handler.handle(record("closed"))
    finally:
        sink.release.set()
        handler.close()


def test_failed_runtime_sink_does_not_kill_writer():
    class FailingSink(logging.Handler):
        def emit(self, record):
            raise OSError("output unavailable")

    handler = BufferedStreamHandler(FailingSink(), capacity=8)
    try:
        assert handler.handle(record("one"))
        assert handler.flush()
        assert handler.handle(record("two"))
        assert handler.flush()
        assert handler.stats()["failed"] == 2
        assert handler.stats()["completed"] == 2
    finally:
        handler.close()
