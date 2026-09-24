"""Send bounded, explicitly synthetic traffic to the isolated monitoring environment."""
import argparse
import fcntl
from concurrent.futures import ThreadPoolExecutor
import json
import time
import uuid
from pathlib import Path
from urllib.request import Request, build_opener, ProxyHandler

HTTP=build_opener(ProxyHandler({}))
BASE='http://127.0.0.1:38080'
def call(path,payload,token=None,key=None):
    headers={'Content-Type':'application/json'}
    if token:headers['Authorization']='Bearer '+token
    if key:headers['Idempotency-Key']=key
    with HTTP.open(Request(BASE+path,json.dumps(payload).encode(),headers),timeout=15) as response:
        return response.status,json.load(response)

def run():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rate',type=int,default=20)
    p.add_argument('--seconds',type=int,default=30)
    p.add_argument('--errors',action='store_true',help='One explicit provider 500, then REVIEW_REQUIRED after lease expiry')
    args=p.parse_args()
    if not 1<=args.rate<=100 or not 1<=args.seconds<=120:p.error('rate 1..100 and seconds 1..120 required')
    _,auth=call('/api/v1/auth/token',{'username':'local-user','password':'local-password'})
    token=auth['data']['accessToken']
    run='monitor-demo-'+uuid.uuid4().hex
    started=time.monotonic()
    def send(i):
        # Every tenth request repeats its predecessor to show internal duplicate suppression.
        key=run+'-'+str(i-1 if i%10==9 else i)
        status,data=call('/api/v1/deliveries',{'deliveryType':'EMAIL','payload':{'message':'monitoring demo'}},token,key)
        return {'status':status,'deliveryId':data['data']['deliveryId']}
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures=[]
        for i in range(args.rate*args.seconds):
            delay=started+i/args.rate-time.monotonic()
            if delay>0:time.sleep(delay)
            futures.append(pool.submit(send,i))
        results=[f.result() for f in futures]
    print(json.dumps({'run':run,'requests':len(results),'uniqueIds':len({x['deliveryId'] for x in results}),
                      'all202':all(x['status']==202 for x in results),'exampleDeliveryId':results[0]['deliveryId']},ensure_ascii=False))
    if args.errors:
        status,data=call('/api/v1/deliveries',{'deliveryType':'EMAIL','payload':{'message':'explicit failure demo','forceFail':True}},token,run+'-forced-failure')
        print(json.dumps({'intentionalProviderFailure':True,'status':status,'deliveryId':data['data']['deliveryId']},ensure_ascii=False))

def main():
    directory=Path(__file__).resolve().parents[2]/'.monitoring'
    directory.mkdir(exist_ok=True)
    with (directory/'benchmark.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        run()

if __name__=='__main__':main()
