"""Compare standard asyncio and uvloop against one independent, synthetic H2 server."""
from __future__ import annotations
import argparse, asyncio, json, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.benchmark_image_ca import tls_fixture
from scripts import benchmark_image_download as download
from scripts import benchmark_image_metadata as metadata
from services.image_async_transport import AsyncImageSession
from services.config import config


def main(args):
    import uvloop
    rows=[]
    config.data['image_download_concurrency']=args.download_concurrency
    with tempfile.TemporaryDirectory(prefix='image-event-loop-') as folder:
        folder=Path(folder)
        _,bundle,directory,_=tls_fixture(folder)
        command=[sys.executable,'scripts/benchmark_image_download.py','--serve','--h2','--cert',str(folder/'cert.pem'),'--key',str(folder/'key.pem'),'--size',str(args.size),'--chunk',str(args.chunk),'--delay',str(args.delay)]
        server=subprocess.Popen(command,stdout=subprocess.PIPE,text=True)
        try:
            url='https://127.0.0.1:'+server.stdout.readline().strip()+'/body'
            for repeat in range(args.repeats):
                for mode in (['asyncio','uvloop'] if repeat%2==0 else ['uvloop','asyncio']):
                    factory=asyncio.new_event_loop if mode=='asyncio' else uvloop.new_event_loop
                    with asyncio.Runner(loop_factory=factory) as runner:
                        if args.kind=='metadata':
                            runner.run(metadata.trial(AsyncImageSession,20,url,b'x'*args.size,bundle,directory))
                            row=runner.run(metadata.trial(AsyncImageSession,args.requests,url,b'x'*args.size,bundle,directory))
                        else:
                            row=runner.run(download.trial(args,'queued',url,bundle,directory))
                    row.update(loop=mode,repeat=repeat+1,kind=args.kind)
                    rows.append(row)
                    print(json.dumps({k:v for k,v in row.items() if k not in {'work','dispatch'}}),flush=True)
        finally:
            server.terminate();server.wait(timeout=10)
    Path(args.output).write_text(json.dumps({'scope':'offline, independent local TLS/H2 server, synthetic bytes; no production accounts or images','settings':vars(args),'results':rows},indent=2),encoding='utf8')
    if any(row['errors'] for row in rows):raise RuntimeError('Comparison contains failed requests')

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--kind',choices=['metadata','download'],default='metadata')
    p.add_argument('--size',type=int,default=524288)
    p.add_argument('--requests',type=int,default=500)
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--chunk',type=int,default=4096)
    p.add_argument('--delay',type=float,default=0)
    p.add_argument('--download-concurrency',type=int,default=64)
    p.add_argument('--output',required=True)
    a=p.parse_args()
    for k,v in dict(timeout=30,total_timeout=120,buffer=0,impersonate='chrome110').items():setattr(a,k,v)
    main(a)
