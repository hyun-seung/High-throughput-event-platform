#!/usr/bin/env python3
"""Isolated v2 API → provider → receipt → SQL → customer HTTP → cleanup proof.
Creates a unique Compose project; stops only owned processes/containers and retains volumes/evidence.
"""
import argparse
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

import boto3
from confluent_kafka import Consumer, TopicPartition
from receipt_flow import ROOT, Run, stop, wait_until

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
MODULES = {'provider': 'external-api-simulator', 'receipt': 'receipt-api',
           'ingress': 'delivery-ingress-worker', 'api': 'event-api',
           'dispatch': 'dispatch-worker', 'result': 'delivery-result-worker'}


def http(url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json', **(headers or {})})
    with HTTP.open(req, timeout=10) as response:
        raw = response.read()
        return response.status, json.loads(raw) if raw else None


def verify_delivery(row, outcome, route, expected_calls, provider, origin, steps, completed_ttl, callbacks):
    """Require independent SQL, provider, DDB, Redis and customer evidence before PASS."""
    result = row['result_json']
    assert row['notification_status'] == 'DELIVERED' and row['cleanup_status'] == 'DONE', row
    assert (result['outcome'], result['routeOrder']) == (outcome, route), result
    assert [p['calls'] for p in provider] == expected_calls, provider
    assert not origin and not steps, (origin, steps)
    assert completed_ttl > 0, completed_ttl
    received = [r for b in callbacks if b['status'] == 204
                for r in b['body']['results'] if r['deliveryId'] == result['deliveryId']]
    assert received and all(r == result for r in received), received


class FullFlow:
    def __init__(self, args):
        self.args = args
        self.run_id = 'full-flow-' + uuid.uuid4().hex[:12]
        self.directory = ROOT / '.poc-results' / self.run_id
        self.directory.mkdir(parents=True)
        self.apps, self.logs, self.cases = {}, [], []
        self.callback_records, self.callback_errors = [], []
        self.lock = threading.Lock()
        self.server = None
        self.probe = None
        self.infra_started = False
        self.ports = {}
        sockets = []
        try:
            for name in ['kafka', 'redis', 'postgres', 'dynamo', 'tcp'] + list(MODULES) + [n + '_metrics' for n in MODULES]:
                s = socket.socket(); s.bind(('127.0.0.1', 0)); sockets.append(s)
                self.ports[name] = s.getsockname()[1]
        finally:
            for s in sockets: s.close()
        self.compose = ['docker', 'compose', '-p', self.run_id, '-f', str(ROOT / 'compose.yml')]
        self.compose_env = {**os.environ, **{name: str(self.ports[key]) for name, key in
            [('KAFKA_HOST_PORT', 'kafka'), ('REDIS_HOST_PORT', 'redis'),
             ('POSTGRES_HOST_PORT', 'postgres'), ('DYNAMODB_HOST_PORT', 'dynamo')]}}

    def write(self, name, value):
        (self.directory / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')

    def docker(self, *args, input=None):
        return subprocess.check_output(self.compose + list(args), cwd=ROOT, env=self.compose_env,
                                       text=True, input=input, timeout=240)

    def sql(self, query):
        return self.docker('exec', '-T', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1',
                           '-U', 'delivery', '-d', 'delivery', '-At', input=query).strip()

    def redis(self, *args):
        return self.docker('exec', '-T', 'redis', 'redis-cli', '--raw', *args).strip()

    def alive(self):
        for name, process in self.apps.items():
            if process.poll() is not None: raise RuntimeError(name + ' exited; inspect ' + str(self.directory))
        with self.lock:
            if self.callback_errors: raise AssertionError(self.callback_errors)

    def start_container(self, name, log):
        # Local Desktop reproducer: first start hangs in Created, a second request succeeds.
        # Retry only a still-Created owned container; never restart a running/failed service.
        attempts = []
        for attempt in (1, 2):
            started = time.monotonic()
            try:
                subprocess.run(['docker', 'start', name], stdout=log, stderr=subprocess.STDOUT,
                               check=True, timeout=10)
                attempts.append({'attempt': attempt, 'outcome': 'started', 'seconds': time.monotonic() - started})
                self.write(name + '-start.json', attempts)
                return
            except subprocess.TimeoutExpired:
                state = json.loads(subprocess.check_output(['docker', 'inspect', name, '--format', '{{json .State}}'],
                                                           text=True, timeout=10))
                attempts.append({'attempt': attempt, 'outcome': 'timeout', 'state': state,
                                 'seconds': time.monotonic() - started})
                self.write(name + '-start.json', attempts)
                if state['Running'] or (state['Status'] == 'exited' and state['ExitCode'] == 0): return
                if state['Status'] != 'created' or attempt == 2: raise
                print('Docker start timeout in Created; retrying owned container:', name, flush=True)

    def start_infrastructure(self):
        # Preserve Compose's definitions and explicitly honor the init dependency.
        self.infra_started = True
        with (self.directory / 'infra.log').open('w') as log:
            subprocess.run(self.compose + ['create'], cwd=ROOT, env=self.compose_env,
                stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
            init = self.run_id + '-dynamodb-data-init-1'
            self.start_container(init, log)
            code = subprocess.check_output(['docker', 'wait', init], text=True, timeout=30).strip()
            if code != '0': raise RuntimeError('DynamoDB volume initialization failed: ' + code)
            names = [self.run_id + '-' + service + '-1' for service in ('redis', 'postgres', 'kafka', 'dynamodb-local')]
            for name in names:
                self.start_container(name, log)
            def healthy():
                states = json.loads(subprocess.check_output(['docker', 'inspect', *names], text=True, timeout=10))
                for item in states:
                    state = item['State']
                    if not state['Running']:
                        raise RuntimeError(item['Name'] + ' stopped during infrastructure startup')
                return all(i['State'].get('Health', {}).get('Status') == 'healthy' for i in states)
            wait_until(healthy, 180)
        self.write('infra-health.json', {name: 'healthy' for name in names})

    def initialize(self):
        java_home = os.environ.get('JAVA_HOME')
        if not java_home: raise RuntimeError('Set JAVA_HOME to JDK 21')
        self.java = str(Path(java_home) / 'bin/java')
        jars = {n: ROOT / m / 'target' / (m + '-1.0-SNAPSHOT.jar') for n, m in MODULES.items()}
        hashes = {n: hashlib.sha256(p.read_bytes()).hexdigest() for n, p in jars.items()}
        self.write('environment.json', {'runId': self.run_id, 'ports': self.ports,
            'gitCommit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'workingTreeDirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
            'jarSha256': hashes, 'harnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'primaryTtlSeconds': 8, 'secondaryTtlSeconds': 12, 'recoveryPollMillis': 3000,
            'infraStartup': 'compose create -> docker start init -> wait init exit 0 -> start services -> healthcheck',
            'productionRecoveryPollMillis': 600000, 'providerDeduplicate': False,
            'scope': 'small-count functional proof, not load/HA/production timing proof'})
        self.start_infrastructure()
        self.sql((ROOT / 'scripts/local-init.sql').read_text())
        tenant = int(self.sql("SELECT id FROM users WHERE username='local-user';"))
        self.redis('HSET', f'request-control:{{user:{tenant}}}:policy', 'blocked', 'false',
                   'tpsEnabled', 'true', 'requestsPerSecond', '2000', 'burstCapacity', '2000',
                   'quotaEnabled', 'true', 'monthlyLimit', '1000000')
        token = secrets.token_urlsafe(32)
        owner = self

        class Receiver(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                try:
                    if self.headers.get('Authorization') != 'Bearer ' + token:
                        raise AssertionError('Invalid customer authorization')
                    body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    if not (body['tenantId'] == tenant and 1 <= len(body['results']) <= 100
                            and self.headers.get('Idempotency-Key') == body['batchId']):
                        raise AssertionError('Invalid customer batch')
                    with owner.lock:
                        status = 503 if not owner.callback_records else 204
                        owner.callback_records.append({'body': body, 'status': status})
                    self.send_response(status); self.send_header('Content-Length', '0'); self.end_headers()
                except Exception as failure:
                    with owner.lock: owner.callback_errors.append(str(failure))
                    self.send_error(400)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR', 'LANG') if k in os.environ}
        env.update({'SPRING_PROFILES_ACTIVE': 'dev', 'KAFKA_BOOTSTRAP_SERVERS': f'localhost:{self.ports["kafka"]}',
            'REDIS_HOST': '127.0.0.1', 'REDIS_PORT': str(self.ports['redis']),
            'DB_URL': f'jdbc:postgresql://localhost:{self.ports["postgres"]}/delivery',
            'DB_USERNAME': 'delivery', 'DB_PASSWORD': 'delivery',
            'DYNAMODB_ENDPOINT': f'http://localhost:{self.ports["dynamo"]}',
            'EXTERNAL_API_BASE_URL': f'http://localhost:{self.ports["provider"]}',
            'EXTERNAL_TCP_HOST': '127.0.0.1', 'EXTERNAL_TCP_PORT': str(self.ports['tcp']),
            'SIMULATOR_TCP_PORT': str(self.ports['tcp']), 'SIMULATOR_DEDUPLICATE': 'false',
            'SIMULATOR_RECEIPTS_ENABLED': 'true', 'RECEIPT_PRIMARY_SECRET': secrets.token_urlsafe(32),
            'RECEIPT_SECONDARY_SECRET': secrets.token_urlsafe(32),
            'SIMULATOR_RECEIPTS_PRIMARY_URL': f'http://localhost:{self.ports["receipt"]}/api/v1/receipts/mock-provider',
            'SIMULATOR_RECEIPTS_SECONDARY_URL': f'http://localhost:{self.ports["receipt"]}/api/v1/receipts/tcp-provider',
            'INGRESS_CONCURRENCY': '1', 'DISPATCH_CONCURRENCY': '1', 'RESULT_CONCURRENCY': '1',
            'DISPATCH_LIFECYCLE_ENABLED': 'true', 'DISPATCH_LIFECYCLE_RECOVERY_POLL_MS': '3000',
            'DISPATCH_PRIMARY_TTL': '8s', 'SECONDARY_TTL': '12s',
            'CUSTOMER_NOTIFICATIONS_ENABLED': 'true', 'DELIVERY_CLEANUP_ENABLED': 'true',
            'JWT_SECRET': 'bG9jYWwtZGV2ZWxvcG1lbnQtb25seS1zZWNyZXQtYXQtbGVhc3QtMzItYnl0ZXMtbG9uZw=='})
        for name in MODULES:
            opts = [f'--server.port={self.ports[name]}', '--server.address=127.0.0.1',
                    f'--management.server.port={self.ports[name + "_metrics"]}']
            if name == 'result':
                opts += [f'--notification.customers.{tenant}.url=http://127.0.0.1:{self.server.server_port}/results',
                         f'--notification.customers.{tenant}.token={token}']
            log = (self.directory / (name + '.log')).open('w'); self.logs.append(log)
            self.apps[name] = subprocess.Popen([self.java, '-Xms64m', '-Xmx256m', '-jar', str(jars[name])] + opts,
                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            def ready():
                self.alive()
                try:
                    headers = {}
                    if name == 'api':
                        self.token = http(f'http://localhost:{self.ports["api"]}/api/v1/auth/token',
                            {'username': 'local-user', 'password': 'local-password'})[1]['data']['accessToken']
                        headers['Authorization'] = 'Bearer ' + self.token
                    return http(f'http://127.0.0.1:{self.ports[name + "_metrics"]}/actuator/health',
                                headers=headers)[1]['status'] == 'UP'
                except (OSError, ValueError): return False
            wait_until(ready, 90)
        self.token = http(f'http://localhost:{self.ports["api"]}/api/v1/auth/token',
                         {'username': 'local-user', 'password': 'local-password'})[1]['data']['accessToken']
        self.db = boto3.client('dynamodb', endpoint_url=env['DYNAMODB_ENDPOINT'], region_name='ap-northeast-2',
                               aws_access_key_id='dummy', aws_secret_access_key='dummy')
        self.probe = Consumer({'bootstrap.servers': env['KAFKA_BOOTSTRAP_SERVERS'], 'broker.address.family': 'v4',
                              'group.id': 'delivery-ingress-worker', 'enable.auto.commit': False})
        wait_until(lambda: all('partitions assigned:' in (self.directory / (n + '.log')).read_text()
                               for n in ('ingress', 'dispatch', 'result')), 45)
        print('READY', self.directory, flush=True)

    def submit(self, key, payload, fallback):
        self.alive()
        status, body = http(f'http://localhost:{self.ports["api"]}/api/v1/deliveries',
            {'deliveryType': 'SMS', 'payload': payload, 'fallbackAllowed': fallback},
            {'Authorization': 'Bearer ' + self.token, 'Idempotency-Key': key})
        assert status == 202, body
        return body['data']['deliveryId']

    def history(self):
        return json.loads(self.sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT h.result_json, "
            "n.status AS notification_status, n.attempt_count, c.status AS cleanup_status "
            "FROM delivery_results.delivery_history h JOIN delivery_results.customer_notification_outbox n "
            "USING(result_event_id) JOIN delivery_results.delivery_cleanup_outbox c USING(result_event_id)) t;"))

    def counts(self, execution, route):
        key = ('tcp:' if route == 2 else '') + Run.attempt(execution, route)
        try: return http(f'http://localhost:{self.ports["provider_metrics"]}/actuator/simulator/{key}')[1]
        except urllib.error.HTTPError as error:
            if error.code != 404: raise
            return {'calls': 0, 'effects': 0}

    def completed(self, request_key):
        self.alive()
        rows = [r for r in self.history() if r['result_json']['requestKey'] == request_key]
        assert len(rows) <= 1, rows
        if rows and rows[0]['notification_status'] == 'DELIVERED' and rows[0]['cleanup_status'] == 'DONE':
            return rows[0]
        return None

    def case(self, name, payload, fallback, outcome, route, calls, drop_schedule=False):
        key = self.run_id + '-' + str(len(self.cases))
        request_key = self.submit(key, payload, fallback)
        if drop_schedule:
            def accepted():
                self.alive()
                origin = self.db.get_item(TableName='ORIGIN', Key={'pk': {'S': 'DELIVERY#' + request_key},
                    'sk': {'S': 'META'}}, ConsistentRead=True).get('Item', {})
                if not origin: return None
                execution = origin['delivery_id']['S']
                step = self.db.get_item(TableName='STEP', Key={'pk': {'S': 'DELIVERY#' + execution},
                    'sk': {'S': 'ATTEMPT#' + Run.attempt(execution)}}, ConsistentRead=True).get('Item', {})
                return execution if step.get('status', {}).get('S') == 'ACCEPTED' else None
            execution = wait_until(accepted)
            assert int(self.redis('ZREM', 'delivery:due', execution)) == 1
        row = wait_until(lambda: self.completed(request_key), 60)
        result = row['result_json']; execution = result['deliveryId']
        provider = [self.counts(execution, r) for r in (1, 2)]
        origin = self.db.get_item(TableName='ORIGIN', Key={'pk': {'S': 'DELIVERY#' + request_key},
            'sk': {'S': 'META'}}, ConsistentRead=True).get('Item')
        steps = self.db.query(TableName='STEP', KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={':pk': {'S': 'DELIVERY#' + execution}}, ConsistentRead=True)['Items']
        completed_ttl = int(self.redis('TTL', 'delivery:completed:' + request_key))
        with self.lock:
            verify_delivery(row, outcome, route, calls, provider, origin, steps, completed_ttl, self.callback_records)
        self.cases.append({'name': name, 'clientMsgId': key, 'requestKey': request_key,
                           'history': row, 'provider': provider, 'redisScheduleRemoved': drop_schedule,
                           'originAbsent': True, 'stepsAbsent': True, 'completedRedisPresent': True})
        self.write('cases.json', self.cases)
        print('PASS', name, flush=True)
        return key, request_key

    def drained(self):
        topic = 'delivery.requested.v1'
        partitions = self.probe.list_topics(topic, timeout=5).topics[topic].partitions
        for partition in partitions:
            tp = TopicPartition(topic, partition)
            end = self.probe.get_watermark_offsets(tp, timeout=5)[1]
            if end and self.probe.committed([tp], timeout=5)[0].offset != end: return False
        return True

    def suite(self):
        key, request_key = self.case('1차 성공·고객 503 후 재전송·정리', {}, False, 'DELIVERED', 1, [1, 0])
        with self.lock:
            assert self.callback_records[0]['status'] == 503
            assert self.callback_records[0]['body'] == self.callback_records[1]['body']
        assert self.submit(key, {}, False) == request_key
        wait_until(self.drained)
        assert len(self.history()) == 1
        assert self.counts(self.cases[0]['history']['result_json']['deliveryId'], 1)['calls'] == 1
        self.cases[0]['completedDuplicateBlocked'] = True
        self.case('실패 웹훅 뒤 TCP 2차 성공', {'simulatorReceiptCodes': ['FALLBACK']}, True, 'DELIVERED', 2, [1, 1])
        self.case('재시도 웹훅 뒤 1차 성공', {'simulatorReceiptCodes': ['RETRY_1S', 'DELIVERED']}, False, 'DELIVERED', 1, [2, 0])
        self.case('Redis 일정 유실 뒤 DDB 복구·1차 만료', {'simulatorReceiptCodes': ['NONE']}, False, 'EXPIRED', 1, [1, 0], True)
        self.case('1차 만료 뒤 TCP 2차 성공', {'simulatorReceiptCodes': ['NONE']}, True, 'DELIVERED', 2, [1, 1])
        self.case('1차·2차 미수신 만료', {'simulatorReceiptCodes': ['NONE'], 'simulatorTcpReceiptCodes': ['NONE']}, True, 'EXPIRED', 2, [1, 1])
        last = self.cases[-1]['history']['result_json']
        elapsed = (datetime.fromisoformat(last['deadline'].replace('Z', '+00:00')) -
                   datetime.fromisoformat(last['occurredAt'].replace('Z', '+00:00'))).total_seconds()
        assert 19.999 <= elapsed <= 20, last
        rows = self.history()
        assert len(rows) == len(self.cases) == 6
        assert {r['result_json']['deliveryId'] for r in rows} == {c['history']['result_json']['deliveryId'] for c in self.cases}
        for case in self.cases:
            execution = case['history']['result_json']['deliveryId']
            assert [self.counts(execution, r)['calls'] for r in (1, 2)] == [p['calls'] for p in case['provider']]
        self.write('cases.json', self.cases)
        self.write('history.json', rows)
        for name in MODULES:
            req = urllib.request.Request(f'http://localhost:{self.ports[name + "_metrics"]}/actuator/prometheus',
                headers={'Authorization': 'Bearer ' + self.token} if name == 'api' else {})
            with HTTP.open(req, timeout=5) as response: (self.directory / (name + '.prom')).write_bytes(response.read())
        self.write('summary.json', {'passed': 6, 'historyRows': len(rows), 'completedDuplicateBlocked': True,
            'customerRetrySameBatch': True, 'redisScheduleLossRecovered': True,
            'resultCounts': {outcome: sum(r['result_json']['outcome'] == outcome for r in rows) for outcome in ('DELIVERED', 'EXPIRED')},
            'completedAt': datetime.now(timezone.utc).isoformat(), 'scope': 'functional small-count, not TPS benchmark'})

    def close(self):
        for process in reversed(list(self.apps.values())): stop(process)
        if self.probe: self.probe.close()
        if self.server:
            self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=3)
        for log in self.logs: log.close()
        with self.lock: self.write('customer-receipts.json', self.callback_records)
        if self.infra_started:
            with (self.directory / 'infra-stop.log').open('w') as log:
                subprocess.run(self.compose + ['stop', '--timeout', '20'], cwd=ROOT, env=self.compose_env,
                    stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
        self.write('cleanup.json', {'ownedProcessesStopped': True, 'composeProject': self.run_id,
                                   'containersStopped': self.infra_started, 'volumesRetained': True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    run = FullFlow(args)
    print('EVIDENCE', run.directory, flush=True)
    try:
        run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally: run.close()


if __name__ == '__main__': main()
