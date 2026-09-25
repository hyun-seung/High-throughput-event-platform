#!/usr/bin/env python3
"""Owned DynamoDB Local outage before claim and across an accepted attempt's deadline."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import time

import boto3
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError, ReadTimeoutError
from full_flow import FullFlow, HTTP
from process_recovery import ProcessRecovery
from postgres_recovery import GROUPS
from receipt_flow import wait_until
from run import metric


def verify_owned_dynamo(item, project):
    labels = item['Config']['Labels']
    assert labels.get('com.docker.compose.project') == project
    assert labels.get('com.docker.compose.service') == 'dynamodb-local'


def verify_blocked(before, during, calls, failure):
    assert during['dynamo']['status'] == 'exited' and not during['dynamo']['running']
    assert during['applicationPids'] == before['applicationPids']
    assert during['metrics'][failure] > before['metrics'][failure]
    assert during['provider'] == [{'calls': calls, 'effects': calls}, {'calls': 0, 'effects': 0}]
    assert during['historyRows'] == during['customerResults'] == 0
    assert during['completedRedisTtl'] == -2
    if calls == 0:
        old = {p['partition']: max(0, p['committed']) for p in before['dispatchOffsets']}
        new = {p['partition']: max(0, p['committed']) for p in during['dispatchOffsets']}
        assert old == new and sum(p['lag'] for p in during['dispatchOffsets']) > 0


class DynamoRecovery(ProcessRecovery):
    def __init__(self, args):
        super().__init__(args)
        self.paused_dispatch = False
        self.signals = []

    def application_overrides(self):
        return {'DISPATCH_PRIMARY_TTL': '90s', 'SECONDARY_TTL': '40s'}

    def initialize(self):
        FullFlow.initialize(self)
        self.fast_db = boto3.client('dynamodb', endpoint_url=self.application_env['DYNAMODB_ENDPOINT'],
            region_name='ap-northeast-2', aws_access_key_id='dummy', aws_secret_access_key='dummy',
            config=Config(connect_timeout=1, read_timeout=1, retries={'max_attempts': 0}))
        env = json.loads((self.directory / 'environment.json').read_text())
        env.update({'primaryTtlSeconds': 90, 'secondaryTtlSeconds': 40,
            'scope': 'local DynamoDB endpoint unavailable with intact volume; not managed-service/AZ/PITR proof',
            'dynamoRecoveryHarnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'sharedRecoveryHarnessSha256': hashlib.sha256(Path(__file__).with_name('process_recovery.py').read_bytes()).hexdigest()})
        self.write('environment.json', env)

    def dynamo(self):
        item = json.loads(subprocess.check_output(['docker', 'inspect', self.run_id + '-dynamodb-local-1'],
                                                  text=True, timeout=10))[0]
        verify_owned_dynamo(item, self.run_id)
        return {'id': item['Id'], 'status': item['State']['Status'], 'running': item['State']['Running'],
                'health': item['State'].get('Health', {}).get('Status'),
                'volumes': [m['Name'] for m in item['Mounts'] if m['Type'] == 'volume'],
                'observedAt': datetime.now(timezone.utc).isoformat()}

    def pids(self):
        self.alive()
        return {name: process.pid for name, process in self.apps.items()}

    def metrics(self, filename=None):
        with HTTP.open(f'http://127.0.0.1:{self.ports["dispatch_metrics"]}/actuator/prometheus', timeout=5) as response:
            text = response.read().decode()
        if filename: (self.directory / filename).write_text(text)
        return {'claimFailed': metric(text, 'delivery_stage_duration_seconds_count', stage='dispatch_claim', result='failure'),
                'dynamoFailed': metric(text, 'delivery_dynamodb_duration_seconds_count', result='failure'),
                'lifecycleFailed': metric(text, 'delivery_lifecycle_events_total', outcome='work_failed')}

    def dispatch_signal(self, suspend):
        process = self.apps['dispatch']
        assert process.poll() is None and self.paused_dispatch != suspend
        process.send_signal(signal.SIGSTOP if suspend else signal.SIGCONT)
        self.paused_dispatch = suspend
        self.signals.append({'pid': process.pid, 'signal': 'SIGSTOP' if suspend else 'SIGCONT',
                             'at': datetime.now(timezone.utc).isoformat()})
        self.write('dispatch-signals.json', self.signals)
        if suspend:
            wait_until(lambda: 'T' in subprocess.check_output(['ps', '-o', 'stat=', '-p', str(process.pid)], text=True), 5)

    def stop_dynamo(self, name):
        before = self.dynamo()
        assert before['running'] and before['health'] == 'healthy'
        subprocess.run(['docker', 'stop', '--timeout', '20', before['id']],
                       check=True, timeout=40, stdout=subprocess.DEVNULL)
        stopped = self.dynamo()
        assert stopped['status'] == 'exited' and not stopped['running']
        try: self.fast_db.list_tables()
        except (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError) as error:
            self.write(name + '-outage-start.json', {'before': before, 'stopped': stopped, 'probeFailure': type(error).__name__})
        else: raise AssertionError('DynamoDB probe unexpectedly succeeded during outage')
        return before

    def restore_dynamo(self, name, before):
        current = self.dynamo()
        assert current['id'] == before['id'] and current['status'] == 'exited'
        subprocess.run(['docker', 'start', current['id']], check=True, timeout=20, stdout=subprocess.DEVNULL)
        def ready():
            self.alive()
            if self.dynamo()['health'] != 'healthy': return False
            try: return {'ORIGIN', 'STEP'} <= set(self.fast_db.list_tables()['TableNames'])
            except (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError): return False
        wait_until(ready, 60)
        restored = self.dynamo()
        assert restored['id'] == before['id'] and restored['volumes'] == before['volumes']
        self.write(name + '-restored.json', restored)

    def snapshot(self, request_key, execution, name):
        with self.lock:
            received = sum(r['requestKey'] == request_key for b in self.callback_records for r in b['body']['results'])
        return {'dynamo': self.dynamo(), 'applicationPids': self.pids(), 'metrics': self.metrics(name + '.prom'),
                'dispatchOffsets': self.offsets('delivery.dispatch-requested.v1', GROUPS['delivery.dispatch-requested.v1']),
                'provider': [self.counts(execution, route) for route in (1, 2)],
                'historyRows': sum(r['result_json']['requestKey'] == request_key for r in self.history()),
                'customerResults': received, 'completedRedisTtl': int(self.redis('TTL', 'delivery:completed:' + request_key))}

    def before_claim(self):
        before = {'applicationPids': self.pids(), 'metrics': self.metrics()}
        self.dispatch_signal(True)
        key = self.submit(self.run_id + '-before-claim', {}, False)
        origin = wait_until(lambda: self.origin(key))
        execution = origin['delivery_id']['S']
        assert not self.step(execution)
        before['dispatchOffsets'] = wait_until(lambda: self.pending_dispatch())
        before.update({'requestKey': key, 'origin': origin, 'stepAbsent': True})
        self.write('claim-before.json', before)
        storage = self.stop_dynamo('claim')
        self.dispatch_signal(False)
        wait_until(lambda: self.alive() or self.metrics()['claimFailed'] >= before['metrics']['claimFailed'] + 2, 40)
        during = self.snapshot(key, execution, 'claim-during')
        verify_blocked(before, during, 0, 'claimFailed')
        self.write('claim-during.json', during)
        self.restore_dynamo('claim', storage)
        result = self.finish_case('선점 전 DDB 장애: 호출 보류·입력 보존 후 1회 발송', key, 'DELIVERED', {})
        assert result['result_json']['occurredAt'] == origin['occurred_at']['S']

    def pending_dispatch(self):
        self.alive()
        rows = self.offsets('delivery.dispatch-requested.v1', GROUPS['delivery.dispatch-requested.v1'])
        return rows if any(p['lag'] > 0 for p in rows) else None

    def expiry(self):
        key = self.submit(self.run_id + '-expiry', {'simulatorReceiptCodes': ['NONE']}, False)
        def accepted():
            self.alive(); origin = self.origin(key)
            if not origin: return None
            step = self.step(origin['delivery_id']['S'])
            return (origin, step) if step.get('status', {}).get('S') == 'ACCEPTED' else None
        origin, step = wait_until(accepted)
        execution = origin['delivery_id']['S']; deadline = int(step['deadline_at']['N'])
        before = {'requestKey': key, 'origin': origin, 'step': step,
                  'applicationPids': self.pids(), 'metrics': self.metrics()}
        self.write('expiry-before.json', before)
        storage = self.stop_dynamo('expiry')
        wait_until(lambda: self.alive() or (time.time() * 1000 > deadline and
                   self.metrics()['lifecycleFailed'] > before['metrics']['lifecycleFailed']), 110)
        during = self.snapshot(key, execution, 'expiry-during')
        verify_blocked(before, during, 1, 'lifecycleFailed')
        assert datetime.fromisoformat(during['dynamo']['observedAt']).timestamp() * 1000 > deadline
        self.write('expiry-during.json', during)
        self.restore_dynamo('expiry', storage)
        result = self.finish_case('접수 후 DDB 장애: 기한 경과 후 재발송 없이 만료', key, 'EXPIRED', {})['result_json']
        assert round(datetime.fromisoformat(result['deadline'].replace('Z', '+00:00')).timestamp() * 1000) == deadline
        assert result['resultAt'] == result['deadline'] and result['occurredAt'] == origin['occurred_at']['S']

    def suite(self):
        pids = self.pids()
        self.before_claim(); self.expiry()
        rows = self.history()
        assert len(rows) == 2 and {r['result_json']['requestKey'] for r in rows} == {c['requestKey'] for c in self.cases}
        final = {}
        for topic, group in GROUPS.items():
            def drained():
                self.alive(); offsets = self.offsets(topic, group)
                return offsets if all(p['lag'] == 0 for p in offsets) else None
            final[topic] = wait_until(drained, 45)
        assert pids == self.pids()
        for row in rows: assert self.counts(row['result_json']['deliveryId'], 1) == {'calls': 1, 'effects': 1}
        self.write('history.json', rows)
        self.write('summary.json', {'passed': 2, 'historyRows': 2, 'providerCalls': 2, 'providerEffects': 2,
            'applicationPidsUnchanged': True, 'claimBlockedWithoutSend': True, 'originalDeadlinePreserved': True,
            'resultCounts': {'DELIVERED': 1, 'EXPIRED': 1}, 'finalOffsets': final,
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'DynamoDB Local stop/start before claim and across deadline; not managed-service/PITR/HA/load proof'})

    def close(self):
        if self.paused_dispatch: self.dispatch_signal(False)
        if hasattr(self, 'fast_db'): self.fast_db.close()
        super().close()


def main():
    run = DynamoRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)}); raise
    finally: run.close()


if __name__ == '__main__': main()
