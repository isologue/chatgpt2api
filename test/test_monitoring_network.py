from services import monitoring


def test_network_rate_excludes_loopback_and_reports_resets(monkeypatch):
    monkeypatch.setattr(monitoring, '_network_state', None)
    clock = iter([10., 12., 14.])
    monkeypatch.setattr(monitoring.time, 'monotonic', lambda: next(clock))
    base = {'received_bytes': 100, 'sent_bytes': 200, 'receive_errors': 0,
            'send_errors': 0, 'receive_drops': 0, 'send_drops': 0}
    samples = iter([base, {**base, 'received_bytes': 300, 'sent_bytes': 600}, base])
    monkeypatch.setattr(monitoring, '_network_totals', lambda: next(samples))
    assert monitoring._network()['received_bytes_per_sec'] is None
    current = monitoring._network()
    assert current['received_bytes_per_sec'] == 100
    assert current['sent_bytes_per_sec'] == 200
    assert current['scope'] == 'process_network_namespace'
    assert monitoring._network()['received_bytes_per_sec'] == 0


def test_unavailable_network_does_not_break_monitoring(monkeypatch):
    def unavailable():
        raise OSError('not Linux')
    monkeypatch.setattr(monitoring, '_network_totals', unavailable)
    assert monitoring._network() == {'available': False}


def test_interface_parser_excludes_loopback(monkeypatch):
    text = 'headers\nheaders\nlo: 999 1 1 1 0 0 0 0 999 1 1 1 0 0 0 0\neth0: 100 1 2 3 0 0 0 0 200 1 4 5 0 0 0 0\n'
    monkeypatch.setattr(monitoring.Path, 'read_text', lambda *a, **k: text)
    assert monitoring._network_totals() == {'received_bytes': 100, 'sent_bytes': 200,
        'receive_errors': 2, 'receive_drops': 3, 'send_errors': 4, 'send_drops': 5}
