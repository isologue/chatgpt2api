"""本地 TLS 对照：同一 curl 调度器、相同 CA 集合，比较 CA 文件与哈希目录。"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import ssl
import statistics
import subprocess
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import benchmark_image_curl as benchmark
from services import image_ca
from services.image_async_transport import AsyncImageSession


def tls_fixture(folder):
    cert, key = folder / 'cert.pem', folder / 'key.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt',
        'ec_paramgen_curve:P-256', '-nodes', '-days', '1', '-keyout', str(key),
        '-out', str(cert), '-subj', '/CN=localhost', '-addext',
        'subjectAltName=DNS:localhost,IP:127.0.0.1'],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    bundle = folder / 'full.pem'
    bundle.write_bytes(Path(image_ca.DEFAULT_CACERT).read_bytes() + cert.read_bytes())
    directory = folder / 'trust'
    count = image_ca.build_ca_directory(bundle, directory)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    return context, bundle, directory, count


async def main(options):
    if sys.platform != 'linux':
        raise SystemExit('Run this comparison inside the Linux image')
    results = []
    with tempfile.TemporaryDirectory(prefix='image-ca-benchmark-') as temporary:
        context, bundle, directory, count = tls_fixture(Path(temporary))
        with patch.object(image_ca, 'DEFAULT_CACERT', str(bundle)), patch.object(image_ca, 'CA_DIRECTORY', directory):
            image_ca._default_ca_directory.cache_clear()
            for repeat in range(options.repeats):
                modes = ('file', 'directory') if repeat % 2 == 0 else ('directory', 'file')
                for mode in modes:
                    class VerifiedSession(AsyncImageSession):
                        def __init__(self, **kwargs):
                            kwargs['verify'] = str(bundle) if mode == 'file' else True
                            super().__init__(**kwargs)
                    with patch.object(benchmark, 'AsyncImageSession', VerifiedSession):
                        result = await benchmark.trial('tuned', options.requests, context)
                    result.update(trust=mode, repeat=repeat + 1)
                    results.append(result)
                    print(json.dumps(result), flush=True)
            image_ca._default_ca_directory.cache_clear()
    medians = {}
    for mode in ('file', 'directory'):
        rows = [row for row in results if row['trust'] == mode]
        medians[mode] = {key: round(statistics.median(values), 3) for key, values in {
            'p95_ms': [row['latency']['p95_ms'] for row in rows],
            'loop_peak_ms': [row['loop_lag']['max_ms'] for row in rows],
            'max_queue_wait_ms': [row['dispatch'].get('max_wait_ms', 0) for row in rows],
            'wall_ms': [row['wall_ms'] for row in rows],
            'cpu_ms': [row['cpu_ms'] for row in rows],
        }.items()}
    data = {'scope': 'Linux local TLS; verified chain and hostname, identical CA roots; '
            '4 TLS connections per request, 512KiB body; excludes upstream and production network',
        'requests': options.requests, 'trust_certificates': count, 'results': results, 'median': medians}
    if options.output:
        Path(options.output).write_text(json.dumps(data, indent=2), encoding='utf8')
    print(json.dumps(medians, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requests', type=int, default=500)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
