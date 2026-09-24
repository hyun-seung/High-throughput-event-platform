#!/usr/bin/env python3
"""Local periodic expiry/final outbox proof. Requires an empty lifecycle index and exclusive test writers."""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from urllib.parse import urlparse
from urllib.error import HTTPError

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import ConfigResource, ResourceType, NewTopic
from receipt_flow import Run, ROOT, TABLE, request, wait_until, stop


class LifecycleRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.finalized = self.run_id + '.finalized'
        self.skip_input = False
        self.final_results = []
        self.final_records = []
        self.final_probe = None

    def extra_options(self, name):
        if name != 'dispatch': return []
        return ['--dispatch.lifecycle.enabled=true', '--dispatch.lifecycle.poll-ms=100',
                '--dispatch.lifecycle.concurrency=2', '--dispatch.lifecycle.partitions=1',
                '--dispatch.primary-ttl=2s', '--dispatch.secondary.ttl=3s',
                '--dispatch.lifecycle.topic=' + self.finalized]

    def initialize(self):
        # Refuse to reconcile pre-existing indexed work in shared local infrastructure.
        for shard in range(16):
            page = self.db.query(TableName=TABLE, IndexName='lifecycle_due_v1',
                KeyConditionExpression='lifecycle_bucket = :bucket',
                ExpressionAttributeValues={':bucket': {'S': 'lifecycle-v1-' + str(shard)}}, Limit=1)
            if page['Items']: raise RuntimeError('Lifecycle index is not empty; use isolated infrastructure')
        self.admin.create_topics([NewTopic(self.finalized, num_partitions=1, replication_factor=1,
            config={'min.insync.replicas': '1', 'retention.ms': '3600000'})])[self.finalized].result(15)
        self.created_topics.append(self.finalized)
        super().initialize()
        self.final_probe = Consumer({'bootstrap.servers': self.args.kafka, 'broker.address.family': 'v4',
            'group.id': self.run_id + '.final-probe', 'enable.auto.commit': False})
        self.final_probe.assign([TopicPartition(self.finalized, 0, 0)])
        environment = json.loads((self.directory / 'environment.json').read_text())
        environment.update({'primaryTtlSeconds': 2, 'secondaryTtlSeconds': 3, 'lifecyclePollMillis': 100,
                            'lifecycleConcurrency': 2, 'exclusiveEmptyIndexRequired': True})
        import hashlib
        environment['lifecycleHarnessSha256'] = hashlib.sha256(__import__('pathlib').Path(__file__).read_bytes()).hexdigest()
        self.write('environment.json', environment)

    def publish(self, event):
        if not self.skip_input: super().publish(event)

    def final(self, delivery, state='PUBLISHED'):
        def ready():
            self.alive()
            item = self.read('DELIVERY#' + delivery, 'FINAL')
            return item if item.get('publish_state', {}).get('S') == state else None
        return wait_until(ready)

    def check_final(self, name, delivery, outcome, route, calls, secondary_calls=0):
        item = self.final(delivery)
        result = json.loads(item['result_event']['S'])
        assert result['outcome'] == outcome and result['routeOrder'] == route, result
        assert 'lifecycle_bucket' not in item
        def counts(route, expected):
            try: actual = self.counts(delivery, route)
            except HTTPError as failure:
                assert failure.code == 404 and expected == 0, (route, expected, failure.code)
                return None
            assert actual['calls'] == expected, actual
            return actual
        primary = counts(1, calls)
        other = counts(2, secondary_calls)
        self.final_results.append({'name': name, 'result': result, 'primaryProvider': primary,
                                   'secondaryProvider': other, 'outbox': item})
        self.write('final-results.json', self.final_results)
        print('PASS', name, flush=True)
        return result

    def topic_max_bytes(self, size):
        resource = ConfigResource(ResourceType.TOPIC, self.finalized, set_config={'max.message.bytes': str(size)})
        self.admin.alter_configs([resource])[resource].result(15)

    def suite(self):
        delivery, event = self.send()
        before = self.check_final('정상 웹훅 성공의 최종 Kafka 인계', delivery, 'DELIVERED', 1, 1)
        self.publish(event); self.drained()
        assert json.loads(self.final(delivery)['result_event']['S']) == before
        assert self.counts(delivery)['calls'] == 1

        delivery, _ = self.send({'simulatorReceiptCodes': ['NONE']})
        self.check_final('1차 미수신 만료와 대체 비허용', delivery, 'EXPIRED', 1, 1)

        delivery, _ = self.send({'simulatorReceiptCodes': ['NONE']}, True)
        self.check_final('1차 만료 후 허용된 TCP 대체 성공', delivery, 'DELIVERED', 2, 1, 1)

        delivery, _ = self.send({'simulatorReceiptCodes': ['NONE'], 'simulatorTcpReceiptCodes': ['NONE']}, True)
        result = self.check_final('1차·2차 연속 미수신의 전체 만료', delivery, 'EXPIRED', 2, 1, 1)
        from datetime import datetime
        elapsed = (datetime.fromisoformat(result['deadline']) - datetime.fromisoformat(result['occurredAt'])).total_seconds()
        assert 4.999 <= elapsed <= 5, result

        # Pause only this test's worker beyond both deadlines, then resume the original process.
        delivery, _ = self.send({'simulatorReceiptCodes': ['NONE'], 'simulatorTcpReceiptCodes': ['NONE']}, True)
        self.await_state(delivery, 'ACCEPTED')
        os.kill(self.apps['dispatch'].pid, signal.SIGSTOP)
        try: time.sleep(5.5)
        finally: os.kill(self.apps['dispatch'].pid, signal.SIGCONT)
        result = self.check_final('Worker 중단 후 원래 전체 기한으로 만료', delivery, 'EXPIRED', 2, 1)
        assert not self.state(delivery, 2).get('provider_processed_at')

        # Legacy META-only work is recovered from an explicit ID manifest, with no Dispatch input.
        self.skip_input = True
        delivery, _ = self.send({'simulatorReceiptCodes': ['NONE']}, True, age=10)
        self.skip_input = False
        subprocess.run([sys.executable, str(ROOT / 'scripts/dynamodb/lifecycle_index.py'), '--endpoint', self.args.dynamo,
                        '--apply', '--delivery-id', delivery], check=True, stdout=subprocess.DEVNULL)
        self.check_final('Attempt가 없는 원본의 명시적 보강 후 만료 복구', delivery, 'EXPIRED', 2, 0)

        # Broker rejects this topic's oversized record; final outbox must survive a JVM restart.
        self.topic_max_bytes(100)
        delivery, _ = self.send()
        pending = self.final(delivery, 'PENDING')
        wait_until(lambda: f'Lifecycle work retained. deliveryId={delivery},' in (self.directory / 'dispatch.log').read_text())
        self.write('pending-before-restart.json', pending)
        command = self.apps['dispatch'].args
        stop(self.apps['dispatch'])
        self.topic_max_bytes(1048588)
        log = (self.directory / 'dispatch-restarted.log').open('w'); self.files.append(log)
        self.apps['dispatch'] = subprocess.Popen(command, cwd=ROOT, env=self.child_env, stdout=log, stderr=subprocess.STDOUT)
        result = self.check_final('Kafka 발행 거절·JVM 재시작 후 동일 최종 결과 복구', delivery, 'DELIVERED', 1, 1)
        assert result == json.loads(pending['result_event']['S'])

        expected = {r['result']['deliveryId']: r['result'] for r in self.final_results}
        def received():
            message = self.final_probe.poll(.2)
            if message and not message.error(): self.final_records.append(json.loads(message.value()))
            found = {r['deliveryId']: r for r in self.final_records}
            return found if set(found) == set(expected) else None
        found = wait_until(received)
        end = self.final_probe.get_watermark_offsets(TopicPartition(self.finalized, 0), timeout=3)[1]
        until = time.monotonic() + 5
        while self.final_probe.position([TopicPartition(self.finalized, 0)])[0].offset < end and time.monotonic() < until:
            message = self.final_probe.poll(.1)
            if message and not message.error(): self.final_records.append(json.loads(message.value()))
        assert len(self.final_records) == end
        assert found == expected
        assert all(record == expected[record['deliveryId']] for record in self.final_records)
        self.write('final-kafka-records.json', self.final_records)
        for app in ('provider', 'receipt', 'dispatch'):
            (self.directory / (app + '.prom')).write_text(request(f'http://127.0.0.1:{self.ports[app + "_metrics"]}/actuator/prometheus'))
        self.write('summary.json', {'passed': len(self.final_results), 'uniqueFinalResults': len(found),
                                   'physicalKafkaRecordsObserved': len(self.final_records),
                                   'cases': [r['name'] for r in self.final_results]})

    def cleanup(self):
        if self.final_probe: self.final_probe.close()
        try: super().cleanup()
        finally:
            for delivery in self.deliveries:
                self.db.delete_item(TableName=TABLE, Key={'pk': {'S': 'DELIVERY#' + delivery}, 'sk': {'S': 'FINAL'}})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka', default='localhost:39092')
    parser.add_argument('--dynamo', default='http://localhost:18000')
    args = parser.parse_args()
    if not all(host.split(':')[0] in ('localhost', '127.0.0.1') for host in args.kafka.split(',')):
        parser.error('Local Kafka only')
    endpoint = urlparse(args.dynamo)
    if endpoint.scheme != 'http' or endpoint.hostname not in ('localhost', '127.0.0.1'): parser.error('Local DynamoDB only')
    run = LifecycleRun(args)
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    finally: run.cleanup()


if __name__ == '__main__': main()
