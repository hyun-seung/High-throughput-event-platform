#!/usr/bin/env python3
"""SIGKILL/restart proof at three durable handoffs in an owned full-flow environment."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import threading
import time

from confluent_kafka import Consumer, TopicPartition
from full_flow import FullFlow, ROOT, http, verify_delivery
from receipt_flow import Run, wait_until


def verify_ack_loss(before, after, callbacks):
    assert before['status'] == 'IN_FLIGHT' and before['attempt_count'] == 1, before
    assert after['status'] == 'DELIVERED' and after['attempt_count'] == 2, after
    assert before['batch_id'] == after['batch_id'] and before['request_body'] == after['request_body']
    assert len(callbacks) == 2 and [c['status'] for c in callbacks] == [0, 204], callbacks
    assert callbacks[0]['body'] == callbacks[1]['body'] == json.loads(before['request_body'])


def verify_uncertain_step(before, after, provider):
    for field in ('status', 'version', 'retry_count', 'deadline_at', 'lease_until'):
        assert before[field] == after[field], (field, before, after)
    assert after['status']['S'] == 'PROCESSING' and after['retry_count']['N'] == '0', after
    assert provider['calls'] == provider['effects'] == 1, provider


class ProcessRecovery(FullFlow):
    def __init__(self, args):
        super().__init__(args)
        self.stopped_commands = {}
        self.restarts = {}; self.transitions = []; self.readers = {}
        self.block_next_callback = False
        self.customer_received = threading.Event(); self.release_response = threading.Event()

    def application_overrides(self):
        return {'DISPATCH_PRIMARY_TTL': '90s', 'SECONDARY_TTL': '30s', 'DISPATCH_LEASE_DURATION': '10s',
                'EXTERNAL_API_READ_TIMEOUT': '30s', 'CUSTOMER_NOTIFICATION_HTTP_TIMEOUT': '10s',
                'CUSTOMER_NOTIFICATION_LEASE': '20s'}

    def fail_first_notification(self): return False

    def before_customer_response(self, record):
        with self.lock:
            block = self.block_next_callback
            if block:
                self.block_next_callback = False
                record['status'] = 0; record['responseWithheld'] = True
        if not block: return True
        self.customer_received.set()
        if not self.release_response.wait(30):
            raise AssertionError('Crash injection did not release held customer response')
        return False

    def initialize(self):
        super().initialize()
        env = json.loads((self.directory / 'environment.json').read_text())
        env.update({'scope': 'real SIGKILL/restart, small-count functional proof; not TPS/Kubernetes/AZ proof',
                    'primaryTtlSeconds': 90, 'secondaryTtlSeconds': 30, 'dispatchLeaseSeconds': 10,
                    'customerHttpTimeoutSeconds': 10, 'customerLeaseSeconds': 20,
                    'recoveryHarnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        self.write('environment.json', env)

    def kill_owned(self, name):
        process = self.apps[name]
        assert process.poll() is None, name
        self.stopped_commands[name] = process.args
        process.kill(); code = process.wait(timeout=5)
        assert code == -signal.SIGKILL, (name, code)
        del self.apps[name]  # Only this deliberately killed child is exempt from alive().
        self.transitions.append({'application': name, 'action': 'SIGKILL', 'pid': process.pid,
                                 'exitCode': code, 'at': datetime.now(timezone.utc).isoformat()})
        self.write('process-transitions.json', self.transitions)

    def restart_owned(self, name):
        assert name not in self.apps and name in self.stopped_commands
        number = self.restarts.get(name, 0) + 1; self.restarts[name] = number
        path = self.directory / f'{name}-restart-{number}.log'
        log = path.open('w'); self.logs.append(log)
        process = subprocess.Popen(self.stopped_commands.pop(name), cwd=ROOT, env=self.application_env,
                                   stdout=log, stderr=subprocess.STDOUT)
        self.apps[name] = process
        self.transitions.append({'application': name, 'action': 'restart', 'pid': process.pid,
                                 'at': datetime.now(timezone.utc).isoformat()})
        self.write('process-transitions.json', self.transitions)
        def ready():
            self.alive()
            try:
                return http(f'http://127.0.0.1:{self.ports[name + "_metrics"]}/actuator/health')[1]['status'] == 'UP' \
                    and 'partitions assigned:' in path.read_text()
            except (OSError, ValueError): return False
        wait_until(ready, 65)

    def offsets(self, topic, group):
        key = (topic, group)
        if key not in self.readers:
            self.readers[key] = Consumer({'bootstrap.servers': f'localhost:{self.ports["kafka"]}',
                'broker.address.family': 'v4', 'group.id': group, 'enable.auto.commit': False})
        reader = self.readers[key]
        parts = [TopicPartition(topic, p) for p in reader.list_topics(topic, timeout=5).topics[topic].partitions]
        commits = {p.partition: p.offset for p in reader.committed(parts, timeout=5)}
        rows = []
        for part in parts:
            low, high = reader.get_watermark_offsets(part, timeout=5)
            committed = commits[part.partition]; effective = low if committed < 0 else committed
            assert low <= effective <= high
            rows.append({'partition': part.partition, 'committed': committed, 'high': high, 'lag': high - effective})
        return rows

    def origin(self, request_key):
        return self.db.get_item(TableName='ORIGIN', Key={'pk': {'S': 'DELIVERY#' + request_key},
            'sk': {'S': 'META'}}, ConsistentRead=True).get('Item', {})

    def step(self, execution):
        return self.db.get_item(TableName='STEP', Key={'pk': {'S': 'DELIVERY#' + execution},
            'sk': {'S': 'ATTEMPT#' + Run.attempt(execution)}}, ConsistentRead=True).get('Item', {})

    def batch(self, result_event):
        # All event IDs come from validated system results, never interpolated user SQL.
        import uuid
        event = str(uuid.UUID(result_event))
        return json.loads(self.sql("SELECT row_to_json(t) FROM (SELECT b.batch_id, b.status, b.attempt_count, "
            "b.request_body, b.lease_until FROM delivery_results.customer_notification_batch b "
            "JOIN delivery_results.customer_notification_outbox n USING(batch_id) "
            f"WHERE n.result_event_id = '{event}') t;"))

    def finish_case(self, name, request_key, outcome, extra):
        row = wait_until(lambda: self.completed(request_key), 130)
        result = row['result_json']; execution = result['deliveryId']
        provider = [self.counts(execution, r) for r in (1, 2)]
        origin = self.origin(request_key)
        steps = self.db.query(TableName='STEP', KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={':pk': {'S': 'DELIVERY#' + execution}}, ConsistentRead=True)['Items']
        ttl = int(self.redis('TTL', 'delivery:completed:' + request_key))
        with self.lock: verify_delivery(row, outcome, 1, [1, 0], provider, origin, steps, ttl, self.callback_records)
        assert provider[0]['effects'] == 1 and provider[1]['effects'] == 0
        self.cases.append({'name': name, 'requestKey': request_key, 'history': row, 'provider': provider,
                           'originAbsent': True, 'stepsAbsent': True, 'completedRedisPresent': True, **extra})
        self.write('cases.json', self.cases)
        print('PASS', name, flush=True)
        return row

    def result_before_sql(self):
        self.kill_owned('result')
        request_key = self.submit(self.run_id + '-result-before-sql', {}, False)
        def persisted():
            self.alive(); origin = self.origin(request_key)
            if not origin: return None
            step = self.step(origin['delivery_id']['S'])
            return step if 'result_event' in step else None
        step = wait_until(persisted)
        durable = json.loads(step['result_event']['S'])
        offsets = wait_until(lambda: (v if sum(p['lag'] for p in v) > 0 else None)
            if (v := self.offsets('delivery.finalized.v1', 'delivery-result-worker-v1')) else None)
        assert not self.history()
        self.write('before-sql-kill-boundary.json', {'step': step, 'offsets': offsets, 'historyRows': 0})
        self.restart_owned('result')
        row = self.finish_case('결과 worker 종료 중 Kafka·STEP 보존 후 SQL 인계 복구', request_key, 'DELIVERED', {'offsetsWhileStopped': offsets})
        assert row['result_json'] == durable

    def customer_ack_loss(self):
        with self.lock: self.block_next_callback = True
        request_key = self.submit(self.run_id + '-customer-ack-loss', {}, False)
        wait_until(lambda: self.alive() or self.customer_received.is_set(), 30)
        with self.lock: held = next(r for r in self.callback_records if r.get('responseWithheld'))
        assert held['body']['results'][0]['requestKey'] == request_key
        event = held['body']['results'][0]['eventId']
        before = self.batch(event)
        assert before['status'] == 'IN_FLIGHT' and before['attempt_count'] == 1
        self.kill_owned('result')
        stopped = self.batch(event)
        assert stopped == before, (before, stopped)
        self.write('customer-ack-loss-boundary.json', {'batch': stopped, 'customerBodyReceivedWithoutAck': held})
        self.release_response.set()
        self.restart_owned('result')
        self.finish_case('고객 본문 수신 후 응답 미확인 종료·동일 묶음 재전송', request_key, 'DELIVERED', {'batchBeforeKill': before})
        after = self.batch(event)
        with self.lock: callbacks = [r for r in self.callback_records if r['body']['batchId'] == before['batch_id']]
        verify_ack_loss(before, after, callbacks)
        self.cases[-1]['batchAfterRestart'] = after
        self.write('cases.json', self.cases)

    def dispatch_unknown(self):
        request_key = self.submit(self.run_id + '-dispatch-unknown',
            {'simulatorResponseDelayMillis': 60000, 'simulatorReceiptCodes': ['NONE']}, False)
        def effected():
            self.alive(); origin = self.origin(request_key)
            if not origin: return None
            execution = origin['delivery_id']['S']
            return execution if self.counts(execution, 1)['effects'] == 1 else None
        execution = wait_until(effected, 15)
        before = self.step(execution)
        assert before['status']['S'] == 'PROCESSING' and 'result_event' not in before
        self.kill_owned('dispatch')
        stopped = self.step(execution); assert before == stopped
        offsets = self.offsets('delivery.dispatch-requested.v1', 'delivery-dispatch-worker')
        assert sum(p['lag'] for p in offsets) > 0
        self.write('dispatch-unknown-boundary.json', {'step': stopped, 'provider': self.counts(execution, 1), 'offsets': offsets})
        self.restart_owned('dispatch')
        wait_until(lambda: self.alive() or not any(p['lag'] for p in self.offsets('delivery.dispatch-requested.v1', 'delivery-dispatch-worker')), 60)
        after = self.step(execution)
        verify_uncertain_step(before, after, self.counts(execution, 1))
        assert time.time() * 1000 < int(after['deadline_at']['N']), 'Recovery did not observe the pre-deadline unknown state'
        row = self.finish_case('업체 효과 후 발송 worker 종료·자동 재발송 없이 원래 기한 만료', request_key, 'EXPIRED',
            {'stepBeforeKill': before, 'stepAfterReplay': after, 'uncommittedDispatchOffsets': offsets})
        assert int(datetime.fromisoformat(row['result_json']['deadline'].replace('Z', '+00:00')).timestamp() * 1000) == int(before['deadline_at']['N'])
        assert row['result_json']['resultAt'] == row['result_json']['deadline']

    def suite(self):
        self.result_before_sql(); self.customer_ack_loss(); self.dispatch_unknown()
        rows = self.history()
        assert len(rows) == len(self.cases) == 3
        assert {r['result_json']['deliveryId'] for r in rows} == {c['history']['result_json']['deliveryId'] for c in self.cases}
        for case in self.cases:
            assert self.counts(case['history']['result_json']['deliveryId'], 1)['calls'] == 1
        offsets = {}
        for topic, group in [('delivery.requested.v1', 'delivery-ingress-worker'),
                             ('delivery.dispatch-requested.v1', 'delivery-dispatch-worker'),
                             ('delivery.receipt-received.v1', 'delivery-receipt-result-worker'),
                             ('delivery.finalized.v1', 'delivery-result-worker-v1')]:
            offsets[topic] = wait_until(lambda: (rows if not any(p['lag'] for p in rows) else None)
                if (rows := self.offsets(topic, group)) else None, 30)
        self.write('history.json', rows)
        self.write('summary.json', {'passed': 3, 'historyRows': 3, 'actualSigkills': 3, 'restarts': self.restarts,
            'providerCalls': 3, 'providerEffects': 3, 'customerBatchReplayIdentical': True,
            'unknownPreservedWithoutResend': True, 'originalDeadlinePreserved': True, 'finalOffsets': offsets,
            'completedAt': datetime.now(timezone.utc).isoformat(), 'scope': 'small-count process crash recovery; not load/AZ proof'})

    def close(self):
        self.release_response.set()
        for reader in self.readers.values(): reader.close()
        super().close()


def main():
    run = ProcessRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)}); raise
    finally: run.close()


if __name__ == '__main__': main()
