"""Local TLS/HTTP comparison of per-session vs coordinated curl watchdogs; no upstream."""
from __future__ import annotations

import argparse
import asyncio
import json
import math
from pathlib import Path
import ssl
import statistics
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from curl_cffi import AsyncCurl
from curl_cffi.aio import CURL_SOCKET_TIMEOUT
from curl_cffi.requests import AsyncSession
from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageSession
from services.image_curl import ImageAsyncCurl, ImageCurlSession
from services.image_metrics import ImageMetrics, current_metrics


def tls_context(folder, cert_path=None, key_path=None):
    if cert_path and key_path:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_path, key_path)
        return context
    # 仅本机临时证书；不导入系统信任库，退出后自动删除。
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    now = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]), False)
            .sign(key, hashes.SHA256()))
    cert_path, key_path = Path(folder)/'cert.pem', Path(folder)/'key.pem'
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                         serialization.NoEncryption()))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    return context


def stats(values):
    values = sorted(values)
    return {k: round(values[max(0, math.ceil(len(values)*q)-1)]*1000, 3)
            for k, q in [('p50_ms', .5), ('p95_ms', .95), ('max_ms', 1)]} if values else {}


async def trial(mode, count, context):
    driver_class = AsyncCurl if mode == 'legacy' else ImageAsyncCurl
    session_class = AsyncSession if mode == 'legacy' else ImageCurlSession
    callbacks = {'all': 0, 'timeout': 0, 'wall_s': 0., 'max_s': 0.}
    original = driver_class.socket_action
    def action(self, sockfd, bitmask):
        started = time.perf_counter()
        try:
            return original(self, sockfd, bitmask)
        finally:
            elapsed = time.perf_counter() - started
            callbacks['all'] += 1
            callbacks['timeout'] += sockfd == CURL_SOCKET_TIMEOUT
            callbacks['wall_s'] += elapsed
            callbacks['max_s'] = max(callbacks['max_s'], elapsed)
    image = b'x'*(512*1024)
    html = b'<html data-build="sandbox">'+b'local TLS benchmark '*13000+b'</html>'
    connections, active, peak = set(), 0, 0
    async def handle(reader, writer):
        nonlocal active, peak
        task = asyncio.current_task()
        connections.add(task); active += 1; peak = max(peak, active)
        try:
            raw = await reader.readuntil(b'\r\n\r\n')
            lines = raw.decode().split('\r\n')
            headers = dict(line.split(': ', 1) for line in lines[1:] if ': ' in line)
            account = headers.get('Authorization', '')
            path = lines[0].split(' ')[1]
            if path == '/':
                body = html
                cookie = b'Set-Cookie: account='+account.encode()+b'; Path=/\r\n'
            elif path == '/image':
                body, cookie = image, b''
            else:
                body = json.dumps({'account': account, 'cookie': headers.get('Cookie'),
                                   'agent': headers.get('User-Agent')}).encode()
                cookie = b''
            await asyncio.sleep(.15)
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+
                         b'\r\nConnection: close\r\n'+cookie+b'\r\n')
            for offset in range(0, len(body), 64*1024):
                writer.write(body[offset:offset+64*1024])
                await writer.drain()
                await asyncio.sleep(.002)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except (ConnectionError, ssl.SSLError): pass
            connections.discard(task); active -= 1
    server = await asyncio.start_server(handle, '127.0.0.1', 0, backlog=1024, ssl=context)
    url = ('https' if context else 'http')+'://127.0.0.1:'+str(server.sockets[0].getsockname()[1])
    lag, latency, drivers = [], [], set()
    stopped = asyncio.Event()
    async def monitor():
        while not stopped.is_set():
            target = time.perf_counter()+.01
            await asyncio.sleep(.01)
            lag.append(max(0., time.perf_counter()-target))
    def work(index):
        account = 'sandbox-'+str(index)
        session = AsyncImageSession(trust_env=False, verify=False,
                                    headers={'Authorization': account, 'User-Agent': account})
        try:
            assert session.get(url+'/', timeout=30).content == html
            drivers.add(session.session.acurl)
            for _ in range(2):
                data = session.get(url+'/metadata', timeout=30).json()
                assert data == {'account': account, 'agent': account, 'cookie': 'account='+account}
            response = session.get(url+'/image', stream=True, timeout=30)
            try: assert b''.join(response.iter_content(chunk_size=64*1024)) == image
            finally: response.close()
        finally: session.close()
    async def call(index):
        metrics = ImageMetrics(); metrics.diagnostics.configure_budget(60)
        token = current_metrics.set(metrics); started = time.perf_counter()
        try: await run_cooperative(work, index)
        finally: current_metrics.reset(token)
        latency.append(time.perf_counter()-started)
    sampler = asyncio.create_task(monitor()); start, cpu = time.perf_counter(), time.process_time()
    try:
        with patch('services.image_async_transport.AsyncSession', session_class), patch.object(driver_class, 'socket_action', action):
            await asyncio.gather(*(call(i) for i in range(count)))
    finally:
        stopped.set(); await sampler
        server.close(); await server.wait_closed()
        if connections: await asyncio.gather(*tuple(connections), return_exceptions=True)
    assert len(drivers) == count and all(d._curlm is None for d in drivers)
    assert not getattr(asyncio.get_running_loop(), '_image_curl_watchdog', None)
    return {'mode': mode, 'requests': count, 'tls': bool(context),
            'watchdog_grace_secs': 1.0 if mode == 'tuned' else None, 'latency': stats(latency),
            'loop_lag': stats(lag), 'wall_ms': round((time.perf_counter()-start)*1000, 3),
            'cpu_ms': round((time.process_time()-cpu)*1000, 3), 'connections_peak': peak,
            'callbacks': callbacks, 'account_isolation_checks': count*2}


async def main(options):
    results = []
    with tempfile.TemporaryDirectory(prefix='image-curl-local-') as folder:
        context = None if options.http else tls_context(folder, options.cert, options.key)
        for repeat in range(options.repeats):
            for mode in (('legacy', 'tuned') if repeat % 2 == 0 else ('tuned', 'legacy')):
                result = await trial(mode, options.requests, context)
                result['repeat'] = repeat+1; results.append(result)
                print(json.dumps(result), flush=True)
    summaries = {}
    for mode in ('legacy', 'tuned'):
        items = [r for r in results if r['mode'] == mode]
        summaries[mode] = {k: round(statistics.median(values), 3) for k, values in {
            'p95_ms': [r['latency']['p95_ms'] for r in items],
            'loop_peak_ms': [r['loop_lag']['max_ms'] for r in items],
            'wall_ms': [r['wall_ms'] for r in items], 'cpu_ms': [r['cpu_ms'] for r in items],
            'timeout_callbacks': [r['callbacks']['timeout'] for r in items],
            'all_callbacks': [r['callbacks']['all'] for r in items]}.items()}
    output = {'scope': 'local TLS/HTTP, HTML, metadata, cookie/header isolation, 512KiB body; excludes upstream and disk',
              'results': results, 'median': summaries}
    if options.output: Path(options.output).write_text(json.dumps(output, indent=2), encoding='utf8')
    print(json.dumps(summaries, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requests', type=int, default=200)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--http', action='store_true')
    parser.add_argument('--cert')
    parser.add_argument('--key')
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
