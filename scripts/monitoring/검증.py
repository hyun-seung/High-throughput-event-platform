"""Validate provisioned dashboards, real datasource queries, scrape health and audit logs."""
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.request import build_opener, ProxyHandler
from urllib.parse import urlencode

ROOT=Path(__file__).resolve().parents[2]
HTTP=build_opener(ProxyHandler({}))
GRAFANA='http://127.0.0.1:13000'
PROM='http://127.0.0.1:19099'

def get(url):
    with HTTP.open(url,timeout=30) as response:return json.load(response)

def main():
    errors=[]; queries=[]
    targets=get(PROM+'/api/v1/targets')['data']['activeTargets']
    for t in targets:
        if t['health']!='up':errors.append({'target':t['labels']['job'],'error':t['lastError']})
    for path in sorted((ROOT/'monitoring/grafana/dashboards').glob('*.json')):
        local=json.loads(path.read_text())
        remote=get(GRAFANA+'/api/dashboards/uid/'+local['uid'])['dashboard']
        expected={p['id']:(p['title'],p.get('targets'),p.get('transformations')) for p in local['panels']}
        provisioned={p['id']:(p['title'],p.get('targets'),p.get('transformations')) for p in remote['panels']}
        if provisioned!=expected:errors.append({'dashboard':local['uid'],'error':'provisioning mismatch'})
        for panel in local['panels']:
            for target in panel.get('targets',[]):
                query=target['expr'].replace('$__rate_interval','2m').replace('$__range','15m').replace('$tenant','1').replace('$delivery','')
                loki=panel['datasource']['uid']=='platform-loki'
                base=GRAFANA+'/api/datasources/proxy/uid/platform-loki/loki/api/v1/' if loki else PROM+'/api/v1/'
                endpoint='query_range' if panel['type']=='logs' else 'query'
                try:
                    result=get(base+endpoint+'?'+urlencode({'query':query,'limit':20} if loki else {'query':query}))
                    if result.get('status')!='success':raise RuntimeError(str(result))
                    queries.append({'dashboard':local['uid'],'panel':panel['title'],'series':len(result['data']['result']),'warnings':result.get('warnings',[])})
                except Exception as e:errors.append({'panel':panel['title'],'query':query,'error':str(e)})
    logs=get(GRAFANA+'/api/datasources/proxy/uid/platform-loki/loki/api/v1/query_range?'+urlencode({'query':'{service=~"api|ingress|dispatch"}','limit':1000}))
    values=[entry for stream in logs['data']['result'] for _,entry in stream['values']]
    if not values:errors.append({'logs':'No audit logs; run demo after startup'})
    for line in values:
        record=json.loads(line)
        if record.get('logger_name')!='delivery.audit':errors.append({'logs':'Unexpected non-audit logger'})
        if any(key in record for key in ('payload','password','accessToken','authorization','Idempotency-Key')):errors.append({'logs':'Unexpected sensitive field'})
    for name,query in {
        'persisted_acceptances':'sum(delivery_outcomes_total{application="dispatch-worker",outcome="dispatch_accepted"}) > 0',
        'fresh_kafka_probe':'platform_kafka_probe_up == 1',
        'drained_backlog':'sum(platform_kafka_committed_lag) == 0',
    }.items():
        observed=get(PROM+'/api/v1/query?'+urlencode({'query':query}))
        if not observed['data']['result']:errors.append({'businessCheck':name,'error':'Expected observation absent'})
    for service in ('api','ingress','dispatch'):
        observed=get(GRAFANA+'/api/datasources/proxy/uid/platform-loki/loki/api/v1/query_range?'+urlencode({'query':'{service="'+service+'"}', 'limit':1}))
        if not observed['data']['result']:errors.append({'logs':service+' audit events absent'})
    result={'checkedAt':datetime.now(timezone.utc).isoformat(),'targets':[{'job':x['labels']['job'],'health':x['health']} for x in targets],
            'queries':queries,'auditLogsInspected':len(values),'errors':errors,'pass':not errors}
    directory=ROOT/'.monitoring';directory.mkdir(exist_ok=True)
    (directory/'verification.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'pass':not errors,'queries':len(queries),'auditLogs':len(values),'errors':errors},ensure_ascii=False,indent=2))
    if errors:raise SystemExit(1)

if __name__=='__main__':main()
