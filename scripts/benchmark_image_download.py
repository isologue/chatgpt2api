"""独立进程 TLS 正文压测；无外网、不调用生图、不下载或解码真实图片。"""
from __future__ import annotations
import argparse, asyncio, json, math, os, ssl, statistics, subprocess, sys, tempfile, time
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

def stats(values):
    a=sorted(values)
    return {k:round(a[max(0,math.ceil(q*len(a))-1)],3) if a else None for k,q in [('p50',.5),('p95',.95),('max',1)]}

async def serve(args):
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(args.cert,args.key)
    block=b'x'*args.chunk;tasks=set()
    if args.h2:
        context.set_alpn_protocols(['h2'])
        from h2.connection import H2Connection
        from h2.config import H2Configuration
        from h2.events import RequestReceived
    async def h2_body(reader,writer):
        conn=H2Connection(config=H2Configuration(client_side=False))
        conn.initiate_connection();writer.write(conn.data_to_send());await writer.drain()
        stream=None;offset=0
        while stream is None or offset<args.size:
            if stream is None or conn.local_flow_control_window(stream)<=0:
                raw=await reader.read(65536)
                if not raw:return
                for event in conn.receive_data(raw):
                    if isinstance(event,RequestReceived):
                        stream=event.stream_id
                        conn.send_headers(stream,[(':status','200'),('content-length',str(args.size))])
            if stream is not None:
                amount=min(args.chunk,args.size-offset,conn.max_outbound_frame_size,conn.local_flow_control_window(stream))
                if amount:
                    conn.send_data(stream,block[:amount],end_stream=offset+amount==args.size);offset+=amount
            writer.write(conn.data_to_send());await writer.drain()
            pause=max(args.delay, amount*len(tasks)/args.rate) if args.rate else args.delay
            if pause:await asyncio.sleep(pause)
        # 等客户端消费 END_STREAM 后主动关闭，避免有控制帧入站时
        # 测试服务端立刻关 socket，制造无关的连接重置。
        try:await asyncio.wait_for(reader.read(), 2)
        except (TimeoutError, ConnectionError):pass
    async def handle(reader,writer):
        tasks.add(asyncio.current_task())
        try:
            if args.h2:
                await h2_body(reader,writer)
                return
            await reader.readuntil(b'\r\n\r\n')
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(args.size).encode()+b'\r\nConnection: close\r\n\r\n')
            for offset in range(0,args.size,args.chunk):
                writer.write(block[:min(args.chunk,args.size-offset)])
                await writer.drain()
                pause=max(args.delay, min(args.chunk,args.size-offset)*len(tasks)/args.rate) if args.rate else args.delay
                if pause:await asyncio.sleep(pause)
        except (ConnectionError,ssl.SSLError):pass
        finally:
            writer.close()
            try:await writer.wait_closed()
            except (ConnectionError,ssl.SSLError):pass
            tasks.discard(asyncio.current_task())
    server=await asyncio.start_server(handle,'127.0.0.1',0,backlog=2048,ssl=context)
    print(server.sockets[0].getsockname()[1],flush=True)
    await server.serve_forever()

async def trial(args,mode,url,bundle,directory):
    from curl_cffi.requests import AsyncSession as StockSession
    from services.image_curl import ImageCurlSession
    from services.image_async_transport import AsyncImageSession
    if mode == 'before':
        import importlib.util
        spec=importlib.util.spec_from_file_location('download_transport_baseline',args.baseline_transport)
        baseline=importlib.util.module_from_spec(spec);spec.loader.exec_module(baseline)
        AsyncImageSession=baseline.AsyncImageSession
    from services.image_async import run_cooperative
    from services import image_ca
    from services.image_metrics import ImageMetrics,current_metrics
    from services.image_loop_work import get_loop_work
    loop=asyncio.get_running_loop()
    from services.image_download_pool import DownloadProcessPool
    pool = DownloadProcessPool(4) if mode == 'process' else None
    if pool is not None:
        loop._image_download_pool = pool
        await pool.warm()
    previous=getattr(loop,'_image_curl_callbacks',None)
    if previous:
        assert not previous.pending and previous.handle is None
        del loop._image_curl_callbacks
    work=get_loop_work();work.flush(force=True)
    latencies=[];errors={};error_examples=[];read_bytes=0;lag=[];stop=False
    async def watch():
        while not stop:
            target=time.perf_counter()+.02;await asyncio.sleep(.02);lag.append(max(0,time.perf_counter()-target)*1000)
    def consume():
        from curl_cffi import CurlOpt
        session=AsyncImageSession(verify=str(bundle),trust_env=False,impersonate=args.impersonate,
            curl_options={CurlOpt.BUFFERSIZE: args.buffer} if args.buffer else {})
        response=None
        try:
            from services.image_metrics import phase
            with phase('image_download_http'):
                response=session.get(url,stream=True,timeout=args.timeout)
            size=sum(len(c) for c in response.iter_content(chunk_size=256*1024))
            assert size==args.size,(size,args.size)
            return size
        finally:
            if response:response.close()
            session.close()
    async def request():
        nonlocal read_bytes
        metrics=ImageMetrics();metrics.diagnostics.configure_budget(args.total_timeout)
        token=current_metrics.set(metrics);started=time.perf_counter()
        try:
            amount=await run_cooperative(consume)
            read_bytes+=amount
        except Exception as exc:
            errors[type(exc).__name__+':'+str(getattr(exc,'code',''))]=errors.get(type(exc).__name__+':'+str(getattr(exc,'code','')),0)+1
            if len(error_examples)<3:error_examples.append(str(exc))
        finally:
            current_metrics.reset(token);latencies.append((time.perf_counter()-started)*1000)
    def worker_cpu():
        if pool is None or sys.platform != 'linux': return 0.
        total = 0.
        for worker in pool.workers:
            if worker.process is not None:
                fields = Path(f'/proc/{worker.process.pid}/stat').read_text(encoding='ascii').rsplit(')', 1)[1].split()
                total += (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')
        return total
    child_cpu = worker_cpu()
    sampler=asyncio.create_task(watch());started=time.perf_counter();cpu=time.process_time()
    with patch.object(image_ca,'DEFAULT_CACERT',str(bundle)),patch.object(image_ca,'CA_DIRECTORY',directory),patch('services.image_async_transport.AsyncSession',StockSession if mode=='stock' else ImageCurlSession):
        image_ca._default_ca_directory.cache_clear()
        await asyncio.gather(*(request() for _ in range(args.requests)))
        image_ca._default_ca_directory.cache_clear()
    stop=True;await sampler
    work.flush(force=True)
    samples=work.samples(time.perf_counter());sample=samples[-1]
    result={'mode':mode,'requests':args.requests,'success':args.requests-sum(errors.values()),'errors':errors,'error_examples':error_examples,'bytes':read_bytes,'latency_ms':stats(latencies),'loop_lag_ms':stats(lag),'wall_ms':round((time.perf_counter()-started)*1000,3),'cpu_ms':round((time.process_time()-cpu)*1000,3),'dispatch':sample.get('curl_dispatch',{}),'work':sample.get('work',{})}
    result['download_worker_cpu_ms'] = round((worker_cpu() - child_cpu) * 1000, 3)
    result['total_client_cpu_ms'] = round(result['cpu_ms'] + result['download_worker_cpu_ms'], 3)
    if pool is not None:
        await pool.close()
        del loop._image_download_pool
    print(json.dumps(result),flush=True);return result

async def main(args):
    from scripts.benchmark_image_ca import tls_fixture
    from services.config import config
    config.data['image_download_concurrency']=args.download_concurrency
    results=[]
    with tempfile.TemporaryDirectory(prefix='download-local-') as folder:
        folder=Path(folder);_,bundle,directory,_=tls_fixture(folder)
        process=subprocess.Popen([sys.executable,__file__,'--serve','--cert',str(folder/'cert.pem'),'--key',str(folder/'key.pem'),'--size',str(args.size),'--chunk',str(args.chunk),'--delay',str(args.delay),'--rate',str(args.rate)]+(['--h2'] if args.h2 else []),stdout=subprocess.PIPE,text=True)
        try:
            port=int(process.stdout.readline());url=f'https://127.0.0.1:{port}/body'
            for i in range(args.repeats):
                for mode in (args.modes if i%2==0 else list(reversed(args.modes))):
                    r=await trial(args,mode,url,bundle,directory);r['repeat']=i+1;results.append(r)
        finally:
            process.terminate();process.wait(timeout=10)
    result={'scope':'independent local TLS server process, synthetic binary body, verified TLS; no upstream or real images','settings':vars(args),'results':results}
    if args.output:Path(args.output).write_text(json.dumps(result,indent=2),encoding='utf8')

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--serve',action='store_true');p.add_argument('--cert');p.add_argument('--key');p.add_argument('--size',type=int,default=3*1024*1024);p.add_argument('--chunk',type=int,default=4096);p.add_argument('--delay',type=float,default=.002);p.add_argument('--requests',type=int,default=500);p.add_argument('--timeout',type=float,default=15);p.add_argument('--total-timeout',type=float,default=120);p.add_argument('--repeats',type=int,default=2);p.add_argument('--modes',nargs='+',default=['stock','queued']);p.add_argument('--rate',type=int,default=0);p.add_argument('--download-concurrency',type=int,default=64);p.add_argument('--baseline-transport');p.add_argument('--h2',action='store_true');p.add_argument('--buffer',type=int,default=0);p.add_argument('--impersonate');p.add_argument('--output');args=p.parse_args()
    asyncio.run(serve(args) if args.serve else main(args))
