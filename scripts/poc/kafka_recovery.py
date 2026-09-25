#!/usr/bin/env python3
"""Owned Kafka outages: reject unconfirmed HTTP acceptance and recover durable final results."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.error

from full_flow import FullFlow, HTTP, http
from process_recovery import ProcessRecovery
from postgres_recovery import GROUPS
from receipt_flow import Run, java_id, wait_until
from run import metric


def verify_owned_kafka(item, project):
    labels = item['Config']['Labels']
    assert labels.get('com.docker.compose.project') == project
    assert labels.get('com.docker.compose.service') == 'kafka'


def kafka_volumes(item):
    # Docker inspect does not promise mount ordering. Compare destination -> volume identity.
    return {m['Destination']: m['Name'] for m in item['Mounts'] if m['Type'] == 'volume'}


def verify_unconfirmed(response, expected_code):
    assert response['status'] == 503, response
    body = response['body']
    assert body.get('code', body.get('data', {}).get('code')) == expected_code, response


def verify_retained_expiry(evidence):
    assert evidence['kafka']['status'] == 'exited' and not evidence['kafka']['running']
    assert evidence['publishFailuresAfter'] > evidence['publishFailuresBefore']
    assert evidence['historyRows'] == evidence['historyRowsBefore']
    assert evidence['customerRequests'] == evidence['customerRequestsBefore']
    assert evidence['completedRedisTtl'] == -2
    result = json.loads(evidence['step']['result_event']['S'])
    assert evidence['origin']['delivery_id']['S'] == result['deliveryId']
    assert result['requestKey'] == evidence['requestKey']
    assert result['outcome'] == 'EXPIRED' and result['routeOrder'] == 1
    assert result['resultAt'] == result['deadline']
    millis = round(datetime.fromisoformat(result['deadline'].replace('Z', '+00:00')).timestamp() * 1000)
    assert millis == int(evidence['step']['deadline_at']['N'])
    assert evidence['provider'] == [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}]


class KafkaRecovery(ProcessRecovery):
    def application_overrides(self):
        return {'DISPATCH_PRIMARY_TTL': '60s', 'SECONDARY_TTL': '80s',
                'KAFKA_PUBLISH_MAX_BLOCK_MS': '2000', 'KAFKA_PUBLISH_REQUEST_TIMEOUT_MS': '1000',
                'KAFKA_PUBLISH_DELIVERY_TIMEOUT_MS': '3000'}

    def initialize(self):
        FullFlow.initialize(self)
        self.tenant = int(self.sql("SELECT id FROM users WHERE username='local-user';"))
        env = json.loads((self.directory / 'environment.json').read_text())
        env.update({'primaryTtlSeconds': 60, 'secondaryTtlSeconds': 80,
                    'apiProducerTimeoutsMs': {'maxBlock': 2000, 'request': 1000, 'delivery': 3000},
                    'scope': 'single Kafka broker stop and same-volume restart; not replication/AZ/ack-loss/load proof',
                    'kafkaRecoveryHarnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        self.write('environment.json', env)

    def kafka(self):
        item = json.loads(subprocess.check_output(['docker', 'inspect', self.run_id + '-kafka-1'], text=True, timeout=10))[0]
        verify_owned_kafka(item, self.run_id)
        return {'id': item['Id'], 'status': item['State']['Status'], 'running': item['State']['Running'],
                'health': item['State'].get('Health', {}).get('Status'),
                'volumes': kafka_volumes(item),
                'at': datetime.now(timezone.utc).isoformat()}

    def pids(self):
        self.alive(); return {name: process.pid for name, process in self.apps.items()}

    def drain(self):
        result = {}
        for topic, group in GROUPS.items():
            def drained():
                self.alive(); rows = self.offsets(topic, group)
                return rows if all(p['lag'] == 0 for p in rows) else None
            result[topic] = wait_until(drained, 45)
        return result

    def stop_kafka(self, label):
        before = self.kafka(); assert before['running'] and before['health'] == 'healthy'
        assert before['volumes']['/var/lib/kafka/data'] == self.run_id + '_kafka-data'
        self.write(label + '-before.json', {'kafka': before, 'offsets': self.drain(), 'applicationPids': self.pids()})
        subprocess.run(['docker', 'stop', '--timeout', '20', before['id']], check=True, timeout=40, stdout=subprocess.DEVNULL)
        state = self.kafka(); assert state['status'] == 'exited' and not state['running']
        self.write(label + '-stopped.json', state)
        return before

    def restore_kafka(self, before, label):
        state = self.kafka(); assert state['id'] == before['id'] and state['status'] == 'exited'
        subprocess.run(['docker', 'start', state['id']], check=True, timeout=20, stdout=subprocess.DEVNULL)
        wait_until(lambda: self.kafka()['health'] == 'healthy', 60)
        restored = self.kafka()
        self.write(label + '-restored.json', restored)
        assert restored['id'] == before['id'] and restored['volumes'] == before['volumes']

    def accepted(self, request_key):
        self.alive(); origin = self.origin(request_key)
        if not origin: return None
        step = self.step(origin['delivery_id']['S'])
        return (origin, step) if step.get('status', {}).get('S') == 'ACCEPTED' else None

    def receipt(self, body):
        # Test credential remains in memory; no headers or environment in evidence.
        return http(f'http://localhost:{self.ports["receipt"]}/api/v1/receipts/mock-provider', body,
                    {'Authorization': 'Bearer ' + self.application_env['RECEIPT_PRIMARY_SECRET']})

    def unconfirmed(self, operation, expected):
        try: operation()
        except urllib.error.HTTPError as error:
            response = {'status': error.code, 'body': json.loads(error.read())}
            verify_unconfirmed(response, expected)
            return response
        raise AssertionError('Broker unavailable but operation did not return HTTP 503')

    def failures(self):
        with HTTP.open(f'http://localhost:{self.ports["dispatch_metrics"]}/actuator/prometheus', timeout=5) as response:
            text = response.read().decode()
        (self.directory / 'dispatch-latest.prom').write_text(text)
        return metric(text, 'delivery_lifecycle_events_total', outcome='work_failed')

    def suite(self):
        pids = self.pids()
        pending_key = self.submit(self.run_id + '-receipt', {'simulatorReceiptCodes': ['NONE']}, False)
        origin, step = wait_until(lambda: self.accepted(pending_key))
        execution = origin['delivery_id']['S']
        receipt = {'receiptId': self.run_id + '-manual', 'deliveryId': execution, 'attemptId': Run.attempt(execution),
                   'outcome': 'DELIVERED', 'code': 'DELIVERED', 'occurredAt': datetime.now(timezone.utc).isoformat(), 'invocation': 1}
        client_key = self.run_id + '-unconfirmed'
        rejected_key = java_id(f'delivery:{self.tenant}:{client_key}')
        before = self.stop_kafka('acceptance')
        api_error = self.unconfirmed(lambda: self.submit(client_key, {}, False), 9003)
        receipt_error = self.unconfirmed(lambda: self.receipt(receipt), 'RECEIPT_UNCONFIRMED')
        assert not self.history() and not self.origin(rejected_key)
        with self.lock: assert not self.callback_records
        assert self.step(execution)['status']['S'] == 'ACCEPTED'
        assert self.counts(execution, 1) == {'calls': 1, 'effects': 1}
        self.write('unconfirmed.json', {'api': api_error, 'receipt': receipt_error, 'receiptBody': receipt,
                    'pendingOrigin': origin, 'pendingStep': step, 'rejectedRequestKey': rejected_key,
                    'rejectedOriginAbsent': True, 'historyRows': 0, 'customerRequests': 0})
        self.restore_kafka(before, 'acceptance')
        # Retry the exact receiptId/body after recovery; a 503 never means it is safe to change identity.
        def retry_receipt():
            self.alive()
            try: return self.receipt(receipt)[0] == 202
            except urllib.error.HTTPError as error:
                if error.code == 503: return False
                raise
        wait_until(retry_receipt, 15)
        self.finish_case('웹훅 503 뒤 동일 본문 재전송·성공 복구', pending_key, 'DELIVERED', {'receipt503': receipt_error})
        retried_key = self.submit(client_key, {}, False); assert retried_key == rejected_key
        self.finish_case('신규 접수 503 뒤 동일 ClientMsgId 재접수', retried_key, 'DELIVERED', {'api503': api_error})
        self.drain()
        expiry_key = self.submit(self.run_id + '-expiry', {'simulatorReceiptCodes': ['NONE']}, False)
        expiry_origin, initial_step = wait_until(lambda: self.accepted(expiry_key))
        expiry_execution = expiry_origin['delivery_id']['S']
        failures_before = self.failures()
        history_before = len(self.history())
        with self.lock: customer_before = len(self.callback_records)
        before = self.stop_kafka('expiry')
        def pending_result():
            self.alive(); value = self.step(expiry_execution)
            return value if 'result_event' in value else None
        final_step = wait_until(pending_result, 90)
        wait_until(lambda: self.alive() or self.failures() > failures_before, 25)
        with self.lock: customer_after = len(self.callback_records)
        evidence = {'requestKey': expiry_key, 'kafka': self.kafka(), 'origin': self.origin(expiry_key),
            'step': final_step, 'initialStep': initial_step, 'publishFailuresBefore': failures_before,
            'publishFailuresAfter': self.failures(), 'historyRowsBefore': history_before, 'historyRows': len(self.history()),
            'customerRequestsBefore': customer_before, 'customerRequests': customer_after,
            'provider': [self.counts(expiry_execution, r) for r in (1, 2)],
            'completedRedisTtl': int(self.redis('TTL', 'delivery:completed:' + expiry_key))}
        self.write('retained-expiry.json', evidence)
        verify_retained_expiry(evidence)
        assert final_step['deadline_at'] == initial_step['deadline_at']
        assert pids == self.pids()
        self.restore_kafka(before, 'expiry')
        row = self.finish_case('Kafka 중단 중 만료 결과 저장·복구 후 고객 전달', expiry_key, 'EXPIRED', {})
        assert row['result_json'] == json.loads(final_step['result_event']['S'])
        final_offsets = self.drain()
        for label in ('acceptance', 'expiry'):
            old = json.loads((self.directory / (label + '-before.json')).read_text())['offsets']
            for topic, partitions in old.items():
                after = {p['partition']: p for p in final_offsets[topic]}
                for partition in partitions:
                    restored = after[partition['partition']]
                    assert restored['high'] >= partition['high']
                    assert max(0, restored['committed']) >= max(0, partition['committed'])
        rows = self.history()
        assert len(rows) == 3 and {r['result_json']['requestKey'] for r in rows} == {pending_key, retried_key, expiry_key}
        for row in rows: assert self.counts(row['result_json']['deliveryId'], 1) == {'calls': 1, 'effects': 1}
        assert pids == self.pids()
        self.write('history.json', rows)
        self.write('summary.json', {'passed': 3, 'historyRows': 3, 'resultCounts': {'DELIVERED': 2, 'EXPIRED': 1},
            'apiUnconfirmedStatus': 503, 'receiptUnconfirmedStatus': 503, 'sameReceiptBodyRetried': True,
            'providerCalls': 3, 'providerEffects': 3, 'applicationPidsUnchanged': True,
            'sameKafkaContainerAndVolumes': True, 'finalOffsets': final_offsets,
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'single local broker stop/restore, functional only; no multi-AZ or throughput claim'})


def main():
    run = KafkaRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)}); raise
    finally: run.close()


if __name__ == '__main__': main()
