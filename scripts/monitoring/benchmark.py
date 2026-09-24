#!/usr/bin/env python3
"""Reconcile bounded k6 traffic against the running, isolated Grafana demo stack.
Never restarts services, resets offsets, changes policies or deletes existing data.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'poc'))
from run import Runner, ROOT, RESULTS, TOOLS, request, write_json
from evidence import KafkaProbe


# Runs inside the already available Python collector. Management ports stay private.
# Auth token and IDs travel on stdin, never in command arguments or evidence files.
READ_INSIDE = '''
import json, sys
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from collector import get
data=json.load(sys.stdin)
def read(url,headers=None):return get(url,headers).decode()
if data['mode']=='metrics':
    urls={'api':'http://api:19080','ingress':'http://ingress:19081',
          'dispatch':'http://dispatch:19082','simulator':'http://simulator:19090'}
    def scrape(app):
        headers={'Authorization':'Bearer '+data['token']} if app=='api' else None
        return app,read(urls[app]+'/actuator/prometheus',headers)
    with ThreadPoolExecutor(max_workers=4) as pool:result=dict(pool.map(scrape,urls))
elif data['mode']=='counts':
    from evidence import attempt_id
    def count(delivery):
        attempt=attempt_id(delivery)
        try:value=json.loads(read('http://simulator:19090/actuator/simulator/'+attempt))
        except HTTPError as error:
            if error.code!=404:raise
            value={'calls':0,'effects':0}
        return attempt,value
    with ThreadPoolExecutor(max_workers=8) as pool:result=dict(pool.map(count,data['deliveries']))
else:result=json.loads(read('http://simulator:19090/actuator/simulator'))
print(json.dumps(result))
'''


class MonitoringRunner(Runner):
    def __init__(self, args):
        super().__init__(args)
        self.api_url = 'http://127.0.0.1:38080'
        self.dynamo_url = 'http://127.0.0.1:38000'
        self.compose += ['-f', str(ROOT / 'monitoring/compose.yml')]
        self.compose[self.compose.index('platform-poc')] = 'platform-monitoring'
        self.compose_env.update(KAFKA_HOST_PORT='39092', REDIS_HOST_PORT='36379',
                                POSTGRES_HOST_PORT='35432', DYNAMODB_HOST_PORT='38000')

    def inside(self, mode, **fields):
        return json.loads(self.docker('exec', '-T', 'collector', 'python', '-c', READ_INSIDE,
                          input=json.dumps({'mode': mode, **fields}), timeout=240))

    def assert_apps(self):
        targets = json.loads(request('http://127.0.0.1:19099/api/v1/targets'))['data']['activeTargets']
        expected = {'api', 'ingress', 'dispatch', 'simulator', 'kafka-observer',
                    'redis', 'postgres', 'prometheus', 'loki', 'alloy'}
        if {x['labels']['job'] for x in targets if x['health'] == 'up'} != expected:
            raise RuntimeError('Monitoring targets are not all UP; preserve the stack and investigate')

    def scrape_metrics(self):
        return self.inside('metrics', token=self.token)

    def provider_counts(self, deliveries):
        return self.inside('counts', deliveries=deliveries)

    def initialize(self):
        if platform.system() == 'Darwin' and shutil.which('caffeinate'):
            self.awake = subprocess.Popen(['caffeinate', '-i', '-w', str(os.getpid())])
        if shutil.disk_usage(ROOT).free < 2 * 1024**3:
            raise RuntimeError('Less than 2 GiB free disk')
        version = subprocess.check_output([str(TOOLS / 'k6'), 'version'], text=True).strip()
        if not version.startswith('k6 v1.8.1 '):
            raise RuntimeError('Install the pinned PoC tools first')
        self.assert_apps()
        self.token = json.loads(request(self.api_url + '/api/v1/auth/token',
                    payload={'username': 'local-user', 'password': 'local-password'}))['data']['accessToken']
        self.tenant = int(self.docker('exec', '-T', 'postgres', 'psql', '-U', 'delivery', '-d', 'delivery', '-Atc',
                          "SELECT id FROM users WHERE username='local-user'"))
        if self.tenant != 1:
            raise RuntimeError('Unexpected demo tenant; refusing to change policy')
        self.probe = KafkaProbe('localhost:39092')
        if self.probe.snapshot()['lag']:
            raise RuntimeError('Existing backlog must drain before measurement')
        simulator = self.inside('summary')
        if simulator['deduplicate']:
            raise RuntimeError('Provider deduplication must be disabled')
        # Record actual running image/JAR identity, not whatever target/*.jar now contains.
        names = self.docker('ps', '-q').splitlines()
        inspected = json.loads(subprocess.check_output(['docker', 'inspect', *names], text=True))
        allowed = {'INGRESS_CONCURRENCY', 'DISPATCH_CONCURRENCY', 'INGRESS_KAFKA_LINGER_MS',
                   'SIMULATOR_RESPONSE_DELAY_MILLIS', 'SIMULATOR_DEDUPLICATE'}
        containers = []
        for item in inspected:
            row = {'name': item['Name'], 'image': item['Image'], 'startedAt': item['State']['StartedAt'],
                   'settings': dict(v.split('=', 1) for v in item['Config']['Env'] if v.split('=', 1)[0] in allowed)}
            for mount in item['Mounts']:
                if mount['Destination'] == '/app/app.jar':
                    service = item['Config']['Labels']['com.docker.compose.service']
                    row['jarSha256'] = self.docker('exec', '-T', service, 'sha256sum', '/app/app.jar').split()[0]
            containers.append(row)
        for service, setting in [('ingress', 'INGRESS_CONCURRENCY'), ('dispatch', 'DISPATCH_CONCURRENCY')]:
            row = next(x for x in containers if x['name'] == '/platform-monitoring-' + service + '-1')
            if row['settings'].get(setting) != '3':
                raise RuntimeError('Benchmark expects worker concurrency 3')
        ingress = next(x for x in containers if x['name'] == '/platform-monitoring-ingress-1')
        if ingress['settings'].get('INGRESS_KAFKA_LINGER_MS') != '5':
            raise RuntimeError('Benchmark expects ingress linger 5ms')
        sources = [Path(__file__), *sorted((ROOT / 'scripts/poc').glob('*.py')), ROOT / 'scripts/poc/load.js']
        write_json(self.directory / 'environment.json', {
            'runId': self.run_id, 'suite': self.args.suite, 'composeProject': 'platform-monitoring',
            'gitCommit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'workingTreeDirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
            'platform': platform.platform(), 'logicalCpus': os.cpu_count(), 'k6': version,
            'monitoringEnabled': True, 'containers': containers, 'simulatorBefore': simulator,
            'harnessSha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
            'limitations': ['Shared existing local stack; no simultaneous demo traffic',
                            'Additional measurement scrapes and Docker stats included in host load',
                            'No unmonitored control run; not a monitoring overhead estimate']})
        print(f'RUN {self.run_id}: existing monitoring stack ready; data and services will be preserved', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', choices=['smoke', 'baseline'], default='smoke')
    parser.add_argument('--drain-seconds', type=int, default=300)
    args = parser.parse_args()
    if not 2 <= args.drain_seconds <= 300: parser.error('drain-seconds must be 2..300')
    RESULTS.mkdir(exist_ok=True)
    (ROOT / '.monitoring').mkdir(exist_ok=True)
    def interrupted(signum, frame): raise KeyboardInterrupt(f'Signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    with (ROOT / '.monitoring/benchmark.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        runner = MonitoringRunner(args)
        try:
            runner.initialize()
            runner.phase('smoke-unique', 5, 2)
            runner.phase('smoke-duplicate', 5, 2, 'duplicate')
            if args.suite == 'baseline':
                runner.phase('warmup', 10, 60)
                runner.phase('duplicate-load', 100, 20, 'duplicate')
                for rate in (50, 100): runner.phase(f'monitored-{rate}', rate, 180)
            print(f'COMPLETE: {runner.directory}', flush=True)
        except BaseException as error:
            write_json(runner.directory / 'failure.json', {'type': type(error).__name__, 'message': str(error)})
            raise
        finally:
            # Runner only stops processes it created; this attached runner owns no apps/infra/policy.
            runner.close()


if __name__ == '__main__': main()
