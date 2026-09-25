#!/usr/bin/env python3
"""Owned PostgreSQL outage: preserve finalized results, then recover without restarting apps."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

from full_flow import FullFlow, HTTP
from process_recovery import ProcessRecovery
from receipt_flow import wait_until
from run import metric


GROUPS = {'delivery.requested.v1': 'delivery-ingress-worker',
          'delivery.dispatch-requested.v1': 'delivery-dispatch-worker',
          'delivery.receipt-received.v1': 'delivery-receipt-result-worker',
          'delivery.finalized.v1': 'delivery-result-worker-v1'}


def verify_owned_postgres(item, project):
    labels = item['Config']['Labels']
    assert labels.get('com.docker.compose.project') == project
    assert labels.get('com.docker.compose.service') == 'postgres'


def verify_outage(before, during):
    assert during['postgres']['status'] == 'exited' and not during['postgres']['running']
    assert during['sqlProbeRejected']
    assert during['applicationPids'] == before['applicationPids']
    assert during['storeFailures'] >= before['storeFailures'] + 2
    assert during['customerRequests'] == 0
    old = {p['partition']: max(0, p['committed']) for p in before['offsets']}
    new = {p['partition']: max(0, p['committed']) for p in during['offsets']}
    assert old == new, (old, new)
    assert sum(p['lag'] for p in during['offsets']) >= 2
    assert len(during['requests']) == 2
    assert {r['expectedOutcome'] for r in during['requests']} == {'DELIVERED', 'EXPIRED'}
    for row in during['requests']:
        result = json.loads(row['step']['result_event']['S'])
        assert row['origin']['delivery_id']['S'] == result['deliveryId']
        assert result['requestKey'] == row['requestKey']
        assert result['outcome'] == row['expectedOutcome'] and result['routeOrder'] == 1
        if result['outcome'] == 'EXPIRED':
            assert result['resultAt'] == result['deadline']
            assert round(datetime.fromisoformat(result['deadline'].replace('Z', '+00:00')).timestamp() * 1000) == int(row['step']['deadline_at']['N'])
        assert row['provider'] == [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}]
        assert row['completedRedisTtl'] == -2


class PostgresRecovery(ProcessRecovery):
    # Reuse read-only offset/STEP probes and the per-request final reconciliation.
    # This suite never calls kill_owned/restart_owned: every JVM must retain its PID.
    def application_overrides(self):
        return {'DISPATCH_PRIMARY_TTL': '30s', 'SECONDARY_TTL': '40s'}

    def initialize(self):
        FullFlow.initialize(self)
        env = json.loads((self.directory / 'environment.json').read_text())
        env.update({'primaryTtlSeconds': 30, 'secondaryTtlSeconds': 40,
                    'scope': 'single local PostgreSQL service outage; not failover/WAL loss/load proof',
                    'postgresRecoveryHarnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    'sharedRecoveryHarnessSha256': hashlib.sha256(Path(__file__).with_name('process_recovery.py').read_bytes()).hexdigest()})
        self.write('environment.json', env)

    def postgres(self):
        item = json.loads(subprocess.check_output(['docker', 'inspect', self.run_id + '-postgres-1'],
                                                  text=True, timeout=10))[0]
        verify_owned_postgres(item, self.run_id)
        return {'id': item['Id'], 'status': item['State']['Status'], 'running': item['State']['Running'],
                'health': item['State'].get('Health', {}).get('Status'),
                'volumes': [m['Name'] for m in item['Mounts'] if m['Type'] == 'volume'],
                'observedAt': datetime.now(timezone.utc).isoformat()}

    def pids(self):
        self.alive()
        return {name: process.pid for name, process in self.apps.items()}

    def store_failures(self, file=None):
        with HTTP.open(f'http://127.0.0.1:{self.ports["result_metrics"]}/actuator/prometheus', timeout=5) as response:
            text = response.read().decode()
        if file: (self.directory / file).write_text(text)
        return metric(text, 'delivery_result_records_total', outcome='failed')

    def pending_result(self, request_key):
        self.alive()
        origin = self.origin(request_key)
        if not origin: return None
        step = self.step(origin['delivery_id']['S'])
        return step if 'result_event' in step else None

    def suite(self):
        assert not self.history()
        topic = 'delivery.finalized.v1'; group = GROUPS[topic]
        before = {'postgres': self.postgres(), 'applicationPids': self.pids(),
                  'offsets': self.offsets(topic, group), 'storeFailures': self.store_failures(), 'historyRows': 0}
        assert before['postgres']['running'] and before['postgres']['health'] == 'healthy'
        self.write('before-outage.json', before)
        # The ID is validated against both project and service labels before mutation.
        subprocess.run(['docker', 'stop', '--time', '20', before['postgres']['id']],
                       check=True, timeout=40, stdout=subprocess.DEVNULL)
        state = self.postgres()
        assert state['status'] == 'exited' and not state['running']
        probe = subprocess.run(self.compose + ['exec', '-T', 'postgres', 'psql', '-U', 'delivery',
                               '-d', 'delivery', '-Atc', 'SELECT 1'], cwd=self.directory,
                               env=self.compose_env, text=True, capture_output=True, timeout=10)
        assert probe.returncode != 0, 'SQL probe unexpectedly succeeded while PostgreSQL was stopped'
        self.write('outage-start.json', {'postgres': state, 'sqlProbeExitCode': probe.returncode,
                                       'sqlProbeError': probe.stderr.strip()})
        requests = []
        for suffix, payload, outcome in [('success', {}, 'DELIVERED'),
                                        ('expiry', {'simulatorReceiptCodes': ['NONE']}, 'EXPIRED')]:
            key = self.run_id + '-' + suffix
            request_key = self.submit(key, payload, False)
            requests.append({'clientMsgId': key, 'requestKey': request_key, 'apiStatus': 202, 'expectedOutcome': outcome})
        self.write('accepted-inputs.json', requests)
        for row in requests:
            step = wait_until(lambda: self.pending_result(row['requestKey']), 60)
            result = json.loads(step['result_event']['S'])
            assert result['outcome'] == row['expectedOutcome']
        wait_until(lambda: self.alive() or self.store_failures() >= before['storeFailures'] + 2, 30)
        for row in requests:
            row['origin'] = self.origin(row['requestKey'])
            execution = row['origin']['delivery_id']['S']
            row['step'] = self.step(execution)
            row['provider'] = [self.counts(execution, route) for route in (1, 2)]
            row['completedRedisTtl'] = int(self.redis('TTL', 'delivery:completed:' + row['requestKey']))
        with self.lock: received = len(self.callback_records)
        during = {'postgres': self.postgres(), 'sqlProbeRejected': probe.returncode != 0,
                  'applicationPids': self.pids(), 'offsets': self.offsets(topic, group),
                  'storeFailures': self.store_failures('result-during-outage.prom'),
                  'customerRequests': received, 'requests': requests}
        self.write('during-outage.json', during)
        verify_outage(before, during)
        print('PASS PostgreSQL 중단 중 접수·발송·만료 진행 및 Kafka·ORIGIN·STEP 보존', flush=True)
        state = self.postgres()
        assert state['id'] == before['postgres']['id'] and state['status'] == 'exited'
        subprocess.run(['docker', 'start', state['id']], check=True, timeout=20, stdout=subprocess.DEVNULL)
        def sql_ready():
            self.alive()
            if self.postgres()['health'] != 'healthy': return False
            return self.sql('SELECT 1;') == '1'
        wait_until(sql_ready, 60)
        restored = self.postgres()
        assert restored['id'] == before['postgres']['id'] and restored['volumes'] == before['postgres']['volumes']
        self.write('postgres-restored.json', restored)
        for row in requests:
            result = self.finish_case('SQL 복구 후 ' + row['expectedOutcome'] + ' 이력·고객 통지·정리',
                                      row['requestKey'], row['expectedOutcome'], {})
            assert result['result_json'] == json.loads(row['step']['result_event']['S'])
        history = self.history()
        assert len(history) == 2 and {r['result_json']['requestKey'] for r in history} == {r['requestKey'] for r in requests}
        offsets = {}
        for topic, group in GROUPS.items():
            def drained():
                self.alive(); rows = self.offsets(topic, group)
                return rows if all(p['lag'] == 0 for p in rows) else None
            offsets[topic] = wait_until(drained, 45)
        for row in history:
            assert self.counts(row['result_json']['deliveryId'], 1) == {'calls': 1, 'effects': 1}
            assert row['attempt_count'] == 1
        with self.lock:
            received = [r for batch in self.callback_records for r in batch['body']['results']]
            assert len(received) == 2 and all(b['status'] == 204 for b in self.callback_records)
        assert before['applicationPids'] == self.pids()
        self.write('history.json', history)
        self.store_failures('result-after-recovery.prom')
        self.write('summary.json', {'passed': 2, 'historyRows': 2, 'resultCounts': {'DELIVERED': 1, 'EXPIRED': 1},
            'applicationPidsUnchanged': True, 'providerCalls': 2, 'providerEffects': 2,
            'customerResults': 2, 'finalOffsets': offsets, 'immutableResultsPreserved': True,
            'sqlBeforeCleanupVerified': True, 'offsetPreservedDuringOutage': True,
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'local PostgreSQL service stop/start with intact volume; not commit-ACK loss/failover/AZ/load proof'})


def main():
    run = PostgresRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)}); raise
    finally: run.close()


if __name__ == '__main__': main()
