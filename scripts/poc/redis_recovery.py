#!/usr/bin/env python3
"""Owned Redis outage: admission bypass, active idempotency, expiry and policy restoration."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.error
import urllib.request

from full_flow import FullFlow, HTTP, http
from process_recovery import ProcessRecovery
from postgres_recovery import GROUPS
from receipt_flow import wait_until
from run import metric


def verify_owned_redis(item, project):
    labels = item['Config']['Labels']
    assert labels.get('com.docker.compose.project') == project
    assert labels.get('com.docker.compose.service') == 'redis'


def verify_without_cache(row, expected, provider, origin, steps, callbacks):
    result = row['result_json']
    assert result['outcome'] == expected and result['routeOrder'] == 1
    assert row['notification_status'] == 'DELIVERED' and row['cleanup_status'] == 'DONE'
    assert provider == [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}]
    assert not origin and not steps
    received = [r for b in callbacks if b['status'] == 204 for r in b['body']['results']
                if r['deliveryId'] == result['deliveryId']]
    assert received and all(r == result for r in received)


class RedisRecovery(ProcessRecovery):
    def application_overrides(self):
        return {'DISPATCH_PRIMARY_TTL': '30s', 'SECONDARY_TTL': '40s'}

    def initialize(self):
        FullFlow.initialize(self)
        self.tenant = int(self.sql("SELECT id FROM users WHERE username='local-user';"))
        self.policy = f'request-control:{{user:{self.tenant}}}:policy'
        self.bucket = f'request-control:{{user:{self.tenant}}}:bucket'
        # Use the API host's local calendar, matching Java YearMonth.now().
        self.quota = f'request-control:{{user:{self.tenant}}}:quota:' + datetime.now().strftime('%Y%m')
        self.rejections = []
        env = json.loads((self.directory / 'environment.json').read_text())
        env.update({'primaryTtlSeconds': 30, 'secondaryTtlSeconds': 40,
                    'scope': 'single local Redis service outage and same AOF volume restore; not replica/HA/load proof',
                    'redisRecoveryHarnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    'sharedRecoveryHarnessSha256': hashlib.sha256(Path(__file__).with_name('process_recovery.py').read_bytes()).hexdigest()})
        self.write('environment.json', env)

    def redis_state(self):
        item = json.loads(subprocess.check_output(['docker', 'inspect', self.run_id + '-redis-1'], text=True, timeout=10))[0]
        verify_owned_redis(item, self.run_id)
        return {'id': item['Id'], 'status': item['State']['Status'], 'running': item['State']['Running'],
                'health': item['State'].get('Health', {}).get('Status'),
                'volumes': [m['Name'] for m in item['Mounts'] if m['Type'] == 'volume'],
                'at': datetime.now(timezone.utc).isoformat()}

    def pids(self):
        self.alive(); return {name: p.pid for name, p in self.apps.items()}

    def metrics(self, app, name):
        req = urllib.request.Request(f'http://127.0.0.1:{self.ports[app + "_metrics"]}/actuator/prometheus',
            headers={'Authorization': 'Bearer ' + self.token} if app == 'api' else {})
        with HTTP.open(req, timeout=5) as response: text = response.read().decode()
        (self.directory / name).write_text(text)
        return text

    def reject(self, label, code):
        try:
            http(f'http://localhost:{self.ports["api"]}/api/v1/deliveries',
                 {'deliveryType': 'SMS', 'payload': {}, 'fallbackAllowed': False},
                 {'Authorization': 'Bearer ' + self.token, 'Idempotency-Key': self.run_id + '-' + label})
        except urllib.error.HTTPError as error:
            body = json.loads(error.read()); assert error.code == 429 and body['data']['code'] == code, body
            self.rejections.append({'label': label, 'status': error.code, 'body': body})
            self.write('rejections.json', self.rejections)
        else: raise AssertionError('Limit should have rejected ' + label)

    def blocked_policies(self, prefix):
        self.redis('HSET', self.policy, 'tpsEnabled', 'false', 'quotaEnabled', 'true', 'monthlyLimit', '1')
        self.redis('SET', self.quota, '1')
        self.reject(prefix + '-quota', 3002)
        self.redis('HSET', self.policy, 'tpsEnabled', 'true', 'requestsPerSecond', '0.001',
                   'burstCapacity', '1', 'quotaEnabled', 'false')
        seconds, micros = self.redis('TIME').splitlines()
        self.redis('HSET', self.bucket, 'tokens', '0', 'lastRefillTime', str(int(seconds) * 1000 + int(micros) // 1000))
        self.reject(prefix + '-tps', 3001)
        self.redis('HSET', self.policy, 'quotaEnabled', 'true')

    def during_case(self, name, key, expected):
        row = wait_until(lambda: self.completed(key), 90)
        execution = row['result_json']['deliveryId']
        provider = [self.counts(execution, route) for route in (1, 2)]
        origin = self.origin(key)
        steps = self.db.query(TableName='STEP', KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={':pk': {'S': 'DELIVERY#' + execution}}, ConsistentRead=True)['Items']
        with self.lock: verify_without_cache(row, expected, provider, origin, steps, self.callback_records)
        assert self.redis_state()['status'] == 'exited'
        self.cases.append({'name': name, 'requestKey': key, 'history': row, 'provider': provider,
                           'originAbsent': True, 'stepsAbsent': True, 'redisUnavailableAtCompletion': True})
        self.write('cases.json', self.cases)
        print('PASS', name, flush=True)
        return row

    def drain(self):
        final = {}
        for topic, group in GROUPS.items():
            def drained():
                self.alive(); rows = self.offsets(topic, group)
                return rows if all(p['lag'] == 0 for p in rows) else None
            final[topic] = wait_until(drained, 45)
        return final

    def suite(self):
        pids = self.pids()
        self.blocked_policies('before')
        assert not self.history()
        baseline = metric(self.metrics('api', 'api-before.prom'), 'delivery_admission_duration_seconds_count',
                          outcome='redis_unavailable_bypass')
        original = self.redis_state(); assert original['running'] and original['health'] == 'healthy'
        self.write('before-outage.json', {'redis': original, 'applicationPids': pids, 'bypassCount': baseline})
        subprocess.run(['docker', 'stop', '--timeout', '20', original['id']], check=True, timeout=40, stdout=subprocess.DEVNULL)
        assert self.redis_state()['status'] == 'exited'
        self.write('redis-stopped.json', self.redis_state())
        normal = self.submit(self.run_id + '-normal', {}, False)
        client_key = self.run_id + '-expiry'; payload = {'simulatorReceiptCodes': ['NONE']}
        expiry = self.submit(client_key, payload, False)
        def accepted():
            self.alive(); origin = self.origin(expiry)
            if not origin: return None
            step = self.step(origin['delivery_id']['S'])
            return (origin, step) if step.get('status', {}).get('S') == 'ACCEPTED' else None
        origin, step = wait_until(accepted)
        assert self.submit(client_key, payload, False) == expiry
        wait_until(lambda: self.alive() or not any(p['lag'] for p in self.offsets('delivery.requested.v1', GROUPS['delivery.requested.v1'])))
        assert self.origin(expiry)['delivery_id'] == origin['delivery_id']
        assert self.counts(origin['delivery_id']['S'], 1) == {'calls': 1, 'effects': 1}
        self.write('active-duplicate.json', {'requestKey': expiry, 'origin': origin, 'step': step,
                                           'provider': self.counts(origin['delivery_id']['S'], 1)})
        self.during_case('Redis 중단 중 정상 발송·고객 통지·정리', normal, 'DELIVERED')
        result = self.during_case('Redis 일정 없이 DDB 복구로 만료·진행 중 중복 차단', expiry, 'EXPIRED')['result_json']
        assert result['resultAt'] == result['deadline']
        assert round(datetime.fromisoformat(result['deadline'].replace('Z', '+00:00')).timestamp() * 1000) == int(step['deadline_at']['N'])
        bypass = metric(self.metrics('api', 'api-during.prom'), 'delivery_admission_duration_seconds_count', outcome='redis_unavailable_bypass')
        assert bypass - baseline == 3
        cache_errors = {app: metric(self.metrics(app, app + '-during.prom'), 'delivery_cache_errors_total')
                        for app in ('ingress', 'dispatch', 'result')}
        assert all(value > 0 for value in cache_errors.values())
        self.write('during-outage.json', {'redis': self.redis_state(), 'bypassedRequests': bypass - baseline,
                    'cacheErrors': cache_errors, 'historyRows': len(self.history()), 'finalOffsets': self.drain()})
        state = self.redis_state(); assert state['id'] == original['id'] and state['status'] == 'exited'
        subprocess.run(['docker', 'start', state['id']], check=True, timeout=20, stdout=subprocess.DEVNULL)
        wait_until(lambda: self.redis_state()['health'] == 'healthy' and self.redis('PING') == 'PONG', 60)
        restored = self.redis_state(); assert restored['id'] == original['id'] and restored['volumes'] == original['volumes']
        def api_ready():
            self.alive()
            try:
                return http(f'http://127.0.0.1:{self.ports["api_metrics"]}/actuator/health',
                    headers={'Authorization': 'Bearer ' + self.token})[1]['status'] == 'UP'
            except (OSError, ValueError): return False
        wait_until(api_ready, 30)
        # Require the old exhausted policy to apply before changing any restored key.
        self.reject('restored-policy-quota', 3002)
        missing = {key: int(self.redis('TTL', 'delivery:completed:' + key)) for key in (normal, expiry)}
        assert all(ttl == -2 for ttl in missing.values())
        assert self.redis('GET', self.quota) == '1'  # Bypassed usage is not retroactively reconstructed.
        self.write('restored.json', {'redis': restored, 'missingCompletionTtls': missing, 'quotaUsage': 1})
        self.blocked_policies('after')
        self.redis('HSET', self.policy, 'requestsPerSecond', '2000', 'burstCapacity', '2000', 'monthlyLimit', '1000000')
        self.redis('DEL', self.bucket)
        recovered_key = self.run_id + '-recovered'
        recovered = self.submit(recovered_key, {}, False)
        self.finish_case('Redis 복구 후 정책 적용·완료 TTL 생성', recovered, 'DELIVERED', {})
        assert self.submit(recovered_key, {}, False) == recovered
        final = self.drain()
        rows = self.history()
        assert len(rows) == 3 and {r['result_json']['requestKey'] for r in rows} == {normal, expiry, recovered}
        for row in rows: assert self.counts(row['result_json']['deliveryId'], 1) == {'calls': 1, 'effects': 1}
        assert pids == self.pids()
        self.write('history.json', rows)
        self.write('summary.json', {'passed': 3, 'historyRows': 3, 'resultCounts': {'DELIVERED': 2, 'EXPIRED': 1},
            'bypassedRequests': 3, 'activeDuplicateBlocked': True, 'completedDuplicateBlockedAfterRestore': True,
            'missingCompletionTtlsAfterRestore': missing, 'quotaNotBackfilled': True,
            'providerCalls': 3, 'providerEffects': 3, 'applicationPidsUnchanged': True, 'finalOffsets': final,
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'local Redis unavailable/same-volume recovery; no HA/empty-cache restore/TPS capacity claim'})


def main():
    run = RedisRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)}); raise
    finally: run.close()


if __name__ == '__main__': main()
