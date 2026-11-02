"""本地验证 TLS 启动及回调调度；使用合成正文和临时证书，不访问上游。"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import benchmark_image_curl as benchmark
from scripts.benchmark_image_ca import tls_fixture
from services import image_ca
from services.image_async_transport import AsyncImageSession
from services.image_metrics import phase


async def main(options):
    baseline = benchmark.load_baseline(None, options.baseline_file)
    results = []
    with tempfile.TemporaryDirectory(prefix='image-startup-') as temporary:
        context, bundle, directory, certificates = tls_fixture(Path(temporary))
        with patch.object(image_ca, 'DEFAULT_CACERT', str(bundle)), patch.object(image_ca, 'CA_DIRECTORY', directory):
            image_ca._default_ca_directory.cache_clear()
            try:
                for repeat in range(options.repeats):
                    for mode in (('legacy', 'tuned') if repeat % 2 == 0 else ('tuned', 'legacy')):
                        first_responses = []

                        class VerifiedSession(AsyncImageSession):
                            def __init__(self, **kwargs):
                                kwargs.update(verify=True, impersonate='chrome')
                                super().__init__(**kwargs)

                            async def _request(self, method, url, **kwargs):
                                bootstrap = url.endswith('/')
                                started = time.perf_counter()
                                stage = ('upstream_bootstrap' if bootstrap else
                                    'image_download_body' if url.endswith('/image') else 'requirements_token')
                                with phase(stage):
                                    response = await super()._request(method, url, **kwargs)
                                if bootstrap:
                                    first_responses.append(time.perf_counter() - started)
                                return response

                        with patch.object(benchmark, 'AsyncImageSession', VerifiedSession):
                            result = await benchmark.trial(mode, options.requests, context, baseline)
                        result.update(repeat=repeat + 1, first_response=benchmark.stats(first_responses))
                        results.append(result)
                        print(json.dumps(result), flush=True)
            finally:
                image_ca._default_ca_directory.cache_clear()
    medians = {}
    for mode in ('legacy', 'tuned'):
        rows = [row for row in results if row['mode'] == mode]
        medians[mode] = {key: round(statistics.median(values), 3) for key, values in {
            'p95_ms': [row['latency']['p95_ms'] for row in rows],
            'first_response_p95_ms': [row['first_response']['p95_ms'] for row in rows],
            'callback_queue_peak_ms': [row['dispatch']['max_wait_ms'] for row in rows],
            'loop_peak_ms': [row['loop_lag']['max_ms'] for row in rows],
            'loop_p95_ms': [row['loop_lag']['p95_ms'] for row in rows],
            'wall_ms': [row['wall_ms'] for row in rows],
            'cpu_ms': [row['cpu_ms'] for row in rows],
        }.items()}
    output = {'scope': 'Local verified TLS with Chrome impersonation; four connections per account; '
        'HTML, two metadata queries, synthetic 512KiB body, private cookies and headers. '
        'Server and client share the benchmark loop; excludes upstream, network bandwidth and production load.',
        'baseline_file': options.baseline_file, 'requests': options.requests,
        'certificates': certificates, 'results': results, 'median': medians}
    if options.output:
        Path(options.output).write_text(json.dumps(output, indent=2), encoding='utf8')
    print(json.dumps(medians, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-file', required=True)
    parser.add_argument('--requests', type=int, default=500)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
