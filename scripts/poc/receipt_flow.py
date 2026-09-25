#!/usr/bin/env python3
"""Local Dispatch → HTTP/TCP provider → Receipt API → Kafka → DynamoDB verification.
No Docker restart, business topic consumption, table scan, or offset reset.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
import urllib.request
from urllib.parse import urlparse
import uuid

import boto3
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from evidence import java_id

ROOT = Path(__file__).resolve().parents[2]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
TABLE = 'delivery_state'


def request(url):
    with HTTP.open(url, timeout=3) as response:
        return response.read().decode()


def wait_until(check, seconds=25):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        result = check()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError('Timed out: ' + str(check))


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


class Run:
    def __init__(self, args):
        self.args = args
        self.run_id = 'receipt-flow-' + uuid.uuid4().hex
        self.directory = ROOT / '.poc-results' / self.run_id
        self.directory.mkdir(parents=True)
        self.topic = self.run_id + '.dispatch'
        self.receipts = self.run_id + '.receipts'
        self.group = self.run_id + '.worker'
        self.receipt_group = self.run_id + '.result'
        self.apps, self.files, self.deliveries, self.cases, self.probes = {}, [], [], [], []
        self.admin = AdminClient({'bootstrap.servers': args.kafka, 'broker.address.family': 'v4'})
        self.db = boto3.client('dynamodb', endpoint_url=args.dynamo, region_name='ap-northeast-2',
                               aws_access_key_id='dummy', aws_secret_access_key='dummy')
        self.producer = Producer({'bootstrap.servers': args.kafka, 'broker.address.family': 'v4', 'acks': 'all', 'enable.idempotence': True})
        self.ports = {}
        self.created_topics = []

    def initialize(self):
        self.db.describe_table(TableName=TABLE)  # existing local infrastructure only
        # Reserve distinct ephemeral ports during allocation; child bind failures abort the run.
        sockets = []
        try:
            for name in ('provider', 'tcp', 'receipt', 'dispatch', 'provider_metrics', 'receipt_metrics', 'dispatch_metrics'):
                s = socket.socket(); s.bind(('127.0.0.1', 0)); sockets.append(s)
                self.ports[name] = s.getsockname()[1]
        finally:
            for s in sockets: s.close()
        for topic in (self.topic, self.receipts):
            self.admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1,
                config={'min.insync.replicas': '1', 'retention.ms': '3600000'})])[topic].result(15)
            self.created_topics.append(topic)
        primary_secret, secondary_secret = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        env = {key: os.environ[key] for key in ('PATH', 'HOME', 'TMPDIR', 'LANG') if key in os.environ}
        env.update({'KAFKA_BOOTSTRAP_SERVERS': self.args.kafka, 'DYNAMODB_ENDPOINT': self.args.dynamo,
                    'RECEIPT_PRIMARY_SECRET': primary_secret, 'RECEIPT_SECONDARY_SECRET': secondary_secret,
                    'EXTERNAL_API_BASE_URL': f'http://127.0.0.1:{self.ports["provider"]}',
                    'EXTERNAL_TCP_HOST': '127.0.0.1', 'EXTERNAL_TCP_PORT': str(self.ports['tcp']),
                    'SIMULATOR_TCP_PORT': str(self.ports['tcp']), 'SIMULATOR_RECEIPTS_ENABLED': 'true',
                    'SIMULATOR_RECEIPTS_PRIMARY_URL': f'http://127.0.0.1:{self.ports["receipt"]}/api/v1/receipts/mock-provider',
                    'SIMULATOR_RECEIPTS_SECONDARY_URL': f'http://127.0.0.1:{self.ports["receipt"]}/api/v1/receipts/tcp-provider',
                    'SIMULATOR_METRICS_PORT': str(self.ports['provider_metrics']),
                    'RECEIPT_METRICS_PORT': str(self.ports['receipt_metrics']),
                    'DISPATCH_METRICS_PORT': str(self.ports['dispatch_metrics']),
                    'DISPATCH_CONCURRENCY': '1', 'DISPATCH_PRIMARY_TTL': '20s', 'SECONDARY_TTL': '30s',
                    'DISPATCH_REDELIVERY_BACKOFF_MS': '100', 'SIMULATOR_DEDUPLICATE': 'false'})
        java = str(Path(os.environ['JAVA_HOME']) / 'bin/java')
        modules = {'receipt': 'receipt-api', 'provider': 'external-api-simulator', 'dispatch': 'dispatch-worker'}
        hashes = {}
        self.child_env = env
        for name, module in modules.items():
            jar = ROOT / module / 'target' / (module + '-1.0-SNAPSHOT.jar')
            hashes[module] = hashlib.sha256(jar.read_bytes()).hexdigest()
            log = (self.directory / (name + '.log')).open('w'); self.files.append(log)
            opts = [f'--server.port={self.ports[name]}', '--server.address=127.0.0.1']
            if name == 'receipt': opts += [f'--receipt.topic={self.receipts}', '--receipt.partitions=1']
            if name == 'dispatch': opts += [f'--dispatch.requests.topic={self.topic}', f'--dispatch.requests.group={self.group}',
                                           f'--dispatch.receipts.topic={self.receipts}', f'--dispatch.receipts.group={self.receipt_group}']
            opts += self.extra_options(name)
            self.apps[name] = subprocess.Popen([java, '-Xms64m', '-Xmx256m', '-jar', str(jar)] + opts,
                                              cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        self.write('environment.json', {'runId': self.run_id, 'ports': self.ports, 'kafka': self.args.kafka,
            'dynamo': self.args.dynamo, 'topics': self.created_topics, 'groups': [self.group, self.receipt_group],
            'primaryTtlSeconds': 20, 'secondaryTtlSeconds': 30, 'providerDeduplicate': False,
            'baseCommit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'workingTreeDirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
            'harnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'jarSha256': hashes})
        def ready():
            self.alive()
            try:
                for app in modules:
                    if json.loads(request(f'http://127.0.0.1:{self.ports[app + "_metrics"]}/actuator/health'))['status'] != 'UP':
                        return False
                return 'partitions assigned:' in (self.directory / 'dispatch.log').read_text()
            except (OSError, ValueError): return False
        wait_until(ready, 90)
        for topic, group in ((self.topic, self.group), (self.receipts, self.receipt_group)):
            probe = Consumer({'bootstrap.servers': self.args.kafka, 'broker.address.family': 'v4', 'group.id': group, 'enable.auto.commit': False})
            self.probes.append((topic, probe))  # committed() reads only, never subscribe/join

    def extra_options(self, name):
        return []

    def alive(self):
        for name, process in self.apps.items():
            if process.poll() is not None: raise AssertionError(f'{name} exited; inspect {self.directory}')

    def write(self, name, value):
        (self.directory / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')

    def send(self, payload=None, fallback=False, age=0):
        delivery = str(uuid.uuid4()); self.deliveries.append(delivery)
        now = (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat().replace('+00:00', 'Z')
        event = {'schemaVersion': 1, 'eventId': java_id(f'event:{delivery}:DISPATCH_REQUESTED:v1'),
                 'eventType': 'DISPATCH_REQUESTED', 'deliveryId': delivery, 'tenantId': 999,
                 'deliveryType': 'SMS', 'payload': payload or {}, 'occurredAt': now,
                 'correlationId': delivery, 'causationId': java_id(f'event:{delivery}:DELIVERY_REQUESTED:v1'),
                 'fallbackAllowed': fallback}
        self.db.put_item(TableName=TABLE, Item={'pk': {'S': 'DELIVERY#' + delivery}, 'sk': {'S': 'META'},
            'delivery_id': {'S': delivery}, 'tenant_id': {'N': '999'}, 'delivery_type': {'S': 'SMS'}, 'payload': {'S': json.dumps(event['payload'])},
            'occurred_at': {'S': now}, 'fallback_allowed': {'BOOL': fallback}}, ConditionExpression='attribute_not_exists(pk)')
        self.publish(event)
        return delivery, event

    def publish(self, event):
        errors = []
        self.producer.produce(self.topic, key=event['deliveryId'], value=json.dumps(event),
                              on_delivery=lambda error, _: errors.append(error) if error else None)
        assert self.producer.flush(10) == 0 and not errors, errors

    @staticmethod
    def attempt(delivery, route=1):
        provider = 'mock-provider' if route == 1 else 'tcp-provider'
        return java_id(f'attempt:{delivery}:{provider}:{route}:1')

    def read(self, pk, sk):
        return self.db.get_item(TableName=TABLE, Key={'pk': {'S': pk}, 'sk': {'S': sk}}, ConsistentRead=True).get('Item', {})

    def state(self, delivery, route=1):
        self.alive()
        return self.read('DELIVERY#' + delivery, 'ATTEMPT#' + self.attempt(delivery, route))

    def await_state(self, delivery, status, route=1, seconds=25):
        return wait_until(lambda: (item if item.get('status', {}).get('S') == status else None)
                          if (item := self.state(delivery, route)) else None, seconds)

    def counts(self, delivery, route=1):
        key = ('tcp:' if route == 2 else '') + self.attempt(delivery, route)
        return json.loads(request(f'http://127.0.0.1:{self.ports["provider_metrics"]}/actuator/simulator/{key}'))

    def drained(self):
        def check():
            self.alive()
            offsets = {}
            for topic, probe in self.probes:
                end = probe.get_watermark_offsets(TopicPartition(topic, 0), timeout=3)[1]
                offset = probe.committed([TopicPartition(topic, 0)], timeout=3)[0].offset
                if end and offset != end: return False
                offsets[topic] = {'committed': offset, 'end': end}
            return offsets
        return wait_until(check)

    def record(self, name, delivery, expected_calls=1, secondary_calls=0, **extra):
        offsets = self.drained()
        primary = self.counts(delivery)
        assert primary['calls'] == expected_calls, (name, primary)
        secondary = self.state(delivery, 2)
        counts2 = self.counts(delivery, 2) if secondary else None
        if secondary_calls:
            assert counts2['calls'] == secondary_calls, (name, counts2)
        else: assert not secondary, (name, secondary)
        self.cases.append({'name': name, 'deliveryId': delivery, 'primary': self.state(delivery),
                           'secondary': secondary, 'primaryProvider': primary, 'secondaryProvider': counts2,
                           'offsets': offsets, **extra})
        self.write('results.json', self.cases)
        print('PASS', name, flush=True)

    def suite(self):
        delivery, event = self.send({'simulatorReceiptCopies': 3})
        initial = self.await_state(delivery, 'DELIVERED')
        time.sleep(.6); self.publish(event); self.drained()
        assert self.state(delivery) == initial
        self.record('중복 웹훅과 Dispatch 재전달의 추가 발송 없음', delivery)

        delivery, _ = self.send({'simulatorReceiptCodes': ['FALLBACK'], 'simulatorReceiptCopies': 3}, True)
        self.await_state(delivery, 'DELIVERED', 2); time.sleep(.6)
        self.record('실패 웹훅의 허용된 TCP 대체 발송', delivery, secondary_calls=1)

        delivery, _ = self.send({'simulatorReceiptCodes': ['FALLBACK']})
        self.await_state(delivery, 'DECISION_PENDING')
        self.record('대체 발송 비허용 요청', delivery)

        delivery, _ = self.send({'simulatorReceiptCodes': ['RETRY_1S', 'DELIVERED'],
                                'simulatorReceiptCopies': 3, 'simulatorReceiptCopyGapMillis': 700})
        state = self.await_state(delivery, 'DELIVERED'); time.sleep(1.6)
        assert state['retry_count']['N'] == '1'
        self.record('1차 재시도와 이전 회차 중복의 뒤늦은 도착', delivery, expected_calls=2)

        delivery, _ = self.send({'simulatorResultCode': 'FALLBACK', 'simulatorTcpReceiptCodes': ['RETRY_1S', 'DELIVERED']}, True)
        state = self.await_state(delivery, 'DELIVERED', 2)
        assert state['retry_count']['N'] == '1'
        self.record('HTTP 실패 후 TCP 발송 및 2차 웹훅 재시도', delivery, secondary_calls=2)

        delivery, _ = self.send({'simulatorReceiptCodes': ['RETRY_1S']}, True)
        state = self.await_state(delivery, 'DECISION_PENDING')
        assert state['retry_count']['N'] == '3' and state['failure_reason']['S'] == 'RETRY_EXHAUSTED_RETRY_1S'
        self.record('1차 재시도 3회 소진 후 추가 호출 없음', delivery, expected_calls=4)

        delivery, _ = self.send({'simulatorResultCode': 'FALLBACK', 'simulatorTcpMode': 'close-after-effect',
                                'simulatorTcpReceiptDelayMillis': 0}, True)
        self.await_state(delivery, 'DELIVERED', 2)
        self.record('TCP 접수 응답 유실 후 웹훅 성공으로 수렴', delivery, secondary_calls=1)

        delivery, _ = self.send({'simulatorReceiptDelayMillis': 1500})
        self.await_state(delivery, 'ACCEPTED'); self.await_state(delivery, 'DELIVERED')
        self.record('지연 웹훅의 접수 상태에서 성공 전이', delivery)

        for route in (1, 2):
            payload = {'simulatorResponseDelayMillis': 4000, 'simulatorReceiptDelayMillis': 0} if route == 1 else {
                'simulatorResultCode': 'FALLBACK', 'simulatorTcpResponseDelayMillis': 4000, 'simulatorTcpReceiptDelayMillis': 0}
            start = time.monotonic(); delivery, _ = self.send(payload, route == 2)
            state = self.await_state(delivery, 'DELIVERED', route, seconds=3.5)
            elapsed = time.monotonic() - start
            assert elapsed < 4
            time.sleep(4.2)
            assert self.state(delivery, route) == state
            self.record(f'{route}차 접수 응답보다 먼저 온 성공 웹훅', delivery,
                        secondary_calls=1 if route == 2 else 0, deliveredBeforeResponseSeconds=round(elapsed, 3))

        delivery, _ = self.send({'simulatorReceiptCodes': ['NONE']})
        state = self.await_state(delivery, 'ACCEPTED'); time.sleep(1)
        assert self.state(delivery) == state
        self.record('웹훅 미발송은 ACCEPTED 유지', delivery)

        delivery, _ = self.send({'simulatorReceiptDelayMillis': 3000}, age=18)
        state = self.await_state(delivery, 'ACCEPTED'); time.sleep(3.5)
        self.drained(); assert self.state(delivery) == state
        receipt = java_id(f'simulator-receipt:false:{self.attempt(delivery)}:1')
        assert not self.read('RECEIPT#' + java_id('receipt:mock-provider:' + receipt), 'META')
        self.record('만료 후 웹훅의 상태·marker 미변경', delivery)
        for app in ('provider', 'receipt', 'dispatch'):
            (self.directory / (app + '.prom')).write_text(request(f'http://127.0.0.1:{self.ports[app + "_metrics"]}/actuator/prometheus'))
        provider_metrics = (self.directory / 'provider.prom').read_text()
        acknowledged = sum(float(line.rsplit(' ', 1)[1]) for line in provider_metrics.splitlines()
                           if line.startswith('simulator_receipt_events_total{') and 'outcome="acknowledged"' in line)
        receipt_end = self.drained()[self.receipts]['end']
        assert acknowledged == receipt_end, (acknowledged, receipt_end)
        self.write('summary.json', {'passed': len(self.cases), 'acknowledgedWebhooks': acknowledged,
                                   'receiptTopicRecords': receipt_end, 'cases': [c['name'] for c in self.cases],
                                   'scope': 'Dispatch-to-Attempt; excludes customer API, periodic expiry, customer notification'})

    def cleanup(self):
        for process in reversed(list(self.apps.values())): stop(process)
        for _, probe in self.probes: probe.close()
        for log in self.files: log.close()
        # Enumerate only this run's exact IDs. Preserve raw results before deleting test records.
        errors = []
        for delivery in self.deliveries:
            keys = [('DELIVERY#' + delivery, 'META')]
            for route in (1, 2):
                attempt = self.attempt(delivery, route)
                keys.append(('DELIVERY#' + delivery, 'ATTEMPT#' + attempt))
                for invocation in range(1, 5):
                    receipt = java_id(f'simulator-receipt:{str(route == 2).lower()}:{attempt}:{invocation}')
                    provider = 'mock-provider' if route == 1 else 'tcp-provider'
                    keys.append(('RECEIPT#' + java_id(f'receipt:{provider}:{receipt}'), 'META'))
            for pk, sk in keys:
                try: self.db.delete_item(TableName=TABLE, Key={'pk': {'S': pk}, 'sk': {'S': sk}})
                except Exception as failure: errors.append(type(failure).__name__)
        for operation, values in ((self.admin.delete_topics, self.created_topics),
                                  (self.admin.delete_consumer_groups, [self.group, self.receipt_group])):
            try:
                for future in operation(values).values(): future.result(15)
            except Exception as failure: errors.append(str(failure))
        self.write('cleanup.json', {'errors': errors})
        if errors: raise RuntimeError('Cleanup incomplete; inspect cleanup.json')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka', default='localhost:39092')
    parser.add_argument('--dynamo', default='http://localhost:18000')
    args = parser.parse_args()
    if not all(host.split(':')[0] in ('localhost', '127.0.0.1') for host in args.kafka.split(',')):
        parser.error('Local Kafka only')
    endpoint = urlparse(args.dynamo)
    if endpoint.scheme != 'http' or endpoint.hostname not in ('localhost', '127.0.0.1'):
        parser.error('Local DynamoDB only')
    run = Run(args)
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    finally: run.cleanup()


if __name__ == '__main__': main()
