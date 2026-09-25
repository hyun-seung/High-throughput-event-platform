#!/usr/bin/env python3
"""Short open-loop load measurement through customer receipt and DynamoDB cleanup."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

from confluent_kafka import Consumer, TopicPartition
from evidence import complete_input, percentile, read_manifest
from full_flow import FullFlow, ROOT, HTTP, MODULES, http
from receipt_flow import Run, stop

GROUPS = {'delivery.requested.v1': 'delivery-ingress-worker',
          'delivery.dispatch-requested.v1': 'delivery-dispatch-worker',
          'delivery.receipt-received.v1': 'delivery-receipt-result-worker',
          'delivery.finalized.v1': 'delivery-result-worker-v1'}


def epoch(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def distribution(values):
    return {name: percentile(values, p) for name, p in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}


def reconcile_phase(starts, responses, history, callbacks):
    expected = {r['deliveryId'] for r in responses.values()}
    if len(expected) != len(starts) or any(r['status'] != 202 or not r['deliveryId'] for r in responses.values()):
        raise AssertionError('Every unique input needs a confirmed API requestKey')
    rows = [r for r in history if r['result_json']['requestKey'] in expected]
    if len(rows) != len(expected) or {r['result_json']['requestKey'] for r in rows} != expected:
        raise AssertionError('Input/history set mismatch or duplicate execution')
    earliest = {}
    for batch in callbacks:
        if batch['status'] != 204: raise AssertionError('Unexpected customer response in normal load test')
        for result in batch['body']['results']:
            if result['requestKey'] not in expected: continue
            key = result['deliveryId']
            if key in earliest and earliest[key][0] != result: raise AssertionError('Conflicting customer result')
            if key not in earliest: earliest[key] = (result, epoch(batch['receivedAt']))
    times = {r['deliveryId']: starts[i]['started'] / 1000 for i, r in responses.items()}
    latencies = {stage: [] for stage in ['providerResult', 'finalized', 'sqlStored', 'customerReceived', 'cleanup']}
    for row in rows:
        result = row['result_json']; execution = result['deliveryId']
        if result['outcome'] != 'DELIVERED' or result['routeOrder'] != 1: raise AssertionError('Unexpected delivery result')
        if row['notification_status'] != 'DELIVERED' or row['cleanup_status'] != 'DONE': raise AssertionError('Incomplete SQL workflow')
        if execution not in earliest or earliest[execution][0] != result: raise AssertionError('Missing/conflicting customer receipt')
        start = times[result['requestKey']]
        for stage, end in [('providerResult', epoch(result['resultAt'])), ('finalized', epoch(result['finalizedAt'])),
                           ('sqlStored', epoch(row['stored_at'])), ('customerReceived', earliest[execution][1]),
                           ('cleanup', epoch(row['cleanup_completed_at']))]:
            if end < start: raise AssertionError('Negative wall-clock latency')
            latencies[stage].append((end - start) * 1000)
    return rows, {stage: distribution(values) for stage, values in latencies.items()}


class LoadRun(FullFlow):
    def __init__(self, args):
        super().__init__(args)
        self.readers = {}
        self.load = None
        self.phases = []

    def application_overrides(self):
        return {'INGRESS_CONCURRENCY': '3', 'DISPATCH_CONCURRENCY': '3', 'RESULT_CONCURRENCY': '3',
                'DISPATCH_PRIMARY_TTL': '3h', 'SECONDARY_TTL': '4h',
                'DISPATCH_LIFECYCLE_RECOVERY_POLL_MS': '600000',
                'DISPATCH_LIFECYCLE_POLL_MS': str(self.args.lifecycle_poll_ms)}

    def fail_first_notification(self): return False

    def initialize(self):
        self.k6 = ROOT / '.poc-tools/k6'
        version = subprocess.check_output([str(self.k6), 'version'], text=True).strip()
        if not version.startswith('k6 v1.8.1 '): raise RuntimeError('Pinned k6 v1.8.1 required')
        super().initialize()
        env = json.loads((self.directory / 'environment.json').read_text())
        env.update({'primaryTtlSeconds': 10800, 'secondaryTtlSeconds': 14400, 'recoveryPollMillis': 600000,
                    'lifecyclePollMillis': self.args.lifecycle_poll_ms, 'workerConcurrency': 3, 'k6': version,
                    'rates': self.args.rates, 'secondsPerPhase': self.args.seconds,
                    'scope': 'short normal primary-success full-flow load; not sustained capacity/HA proof',
                    'loadHarnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    'loadScriptSha256': hashlib.sha256((ROOT / 'scripts/poc/load.js').read_bytes()).hexdigest()})
        self.write('environment.json', env)
        for topic, group in GROUPS.items():
            self.readers[topic] = Consumer({'bootstrap.servers': f'localhost:{self.ports["kafka"]}',
                'broker.address.family': 'v4', 'group.id': group, 'enable.auto.commit': False})
        self.partitions = {topic: list(reader.list_topics(topic, timeout=5).topics[topic].partitions)
                           for topic, reader in self.readers.items()}

    def snapshot(self, phase, index):
        self.alive()
        started = time.monotonic()
        counts = json.loads(self.sql("SELECT json_build_object('history',(SELECT count(*) FROM delivery_results.delivery_history),"
            "'notified',(SELECT count(*) FROM delivery_results.customer_notification_outbox WHERE status='DELIVERED'),"
            "'cleaned',(SELECT count(*) FROM delivery_results.delivery_cleanup_outbox WHERE status='DONE'));"))
        lags = {}
        for topic, reader in self.readers.items():
            parts = [TopicPartition(topic, p) for p in self.partitions[topic]]
            commits = {p.partition: p.offset for p in reader.committed(parts, timeout=5)}
            lag = 0
            for part in parts:
                low, high = reader.get_watermark_offsets(part, timeout=5)
                commit = commits[part.partition]
                effective = low if commit < 0 else commit
                if not low <= effective <= high: raise AssertionError('Kafka offset outside retained range')
                lag += high - effective
            lags[topic] = lag
        def scrape(name):
            req = urllib.request.Request(f'http://localhost:{self.ports[name + "_metrics"]}/actuator/prometheus',
                headers={'Authorization': 'Bearer ' + self.token} if name == 'api' else {})
            with HTTP.open(req, timeout=5) as response:
                with gzip.open(phase / f'{index:04}-{name}.prom.gz', 'wb') as output: output.write(response.read())
        with ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(scrape, MODULES))
        provider = http(f'http://localhost:{self.ports["provider_metrics"]}/actuator/simulator')[1]
        sample = {'time': time.time(), **counts, 'kafkaLag': lags, 'providerCalls': provider['calls'],
                  'redisDue': int(self.redis('ZCARD', 'delivery:due'))}
        sample['collectionSeconds'] = time.monotonic() - started
        if sample['collectionSeconds'] > 15: raise AssertionError('Measurement collector stalled')
        with (phase / 'samples.jsonl').open('a') as output: output.write(json.dumps(sample) + '\n')
        return sample

    def phase(self, rate):
        label = f'{rate}tps'
        path = self.directory / label; path.mkdir()
        before = self.snapshot(path, 0)
        assert not any(before['kafkaLag'].values()) and before['history'] == before['cleaned'] == before['notified']
        env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR', 'LANG') if k in os.environ}
        env.update({'POC_RATE': str(rate), 'POC_SECONDS': str(self.args.seconds), 'POC_RUN_ID': self.run_id + '-' + label,
                    'POC_MODE': 'unique', 'POC_TOKEN': self.token, 'POC_API': f'http://localhost:{self.ports["api"]}',
                    'POC_SUMMARY': str(path / 'k6-summary.json'), 'K6_NO_USAGE_REPORT': 'true'})
        print('LOAD', rate, 'TPS x', self.args.seconds, 'seconds', flush=True)
        start = time.monotonic(); wall_start = time.time()
        samples = [before]
        with (path / 'k6.log').open('w') as log:
            self.load = subprocess.Popen([str(self.k6), 'run', '--log-format', 'raw', '--console-output', str(path / 'requests.jsonl'),
                str(ROOT / 'scripts/poc/load.js')], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            while self.load.poll() is None:
                if time.monotonic() - start > self.args.seconds + 60: raise TimeoutError('k6 did not finish')
                time.sleep(2)
                samples.append(self.snapshot(path, len(samples)))
            assert self.load.returncode == 0, 'k6 checks/thresholds failed'
        starts, responses = read_manifest(path / 'requests.jsonl')
        summary = json.loads((path / 'k6-summary.json').read_text())
        metrics = summary['metrics']
        # k6 handleSummary uses values nested under metrics.
        value = lambda key, stat: metrics.get(key, {}).get('values', {}).get(stat, 0)
        assert complete_input(rate * self.args.seconds, len(starts), len(responses),
                              int(value('iterations', 'count')), int(value('dropped_iterations', 'count')))
        deadline = time.monotonic() + self.args.drain_timeout
        target = before['history'] + len(starts)
        while samples[-1]['cleaned'] != target or samples[-1]['notified'] != target or any(samples[-1]['kafkaLag'].values()):
            if time.monotonic() > deadline: raise TimeoutError('Full-flow drain exceeded budget; retain backlog evidence')
            time.sleep(2)
            samples.append(self.snapshot(path, len(samples)))
            if len(samples) % 10 == 0:
                print('DRAIN', label, 'history/notified/cleaned', *(samples[-1][k] - before[k] for k in ('history','notified','cleaned')), '/', len(starts), flush=True)
        elapsed = time.monotonic() - start
        if abs((time.time() - wall_start) - elapsed) > 1: raise AssertionError('Wall/monotonic clock discontinuity')
        history = self.history()
        with self.lock: rows, latencies = reconcile_phase(starts, responses, history, self.callback_records)
        self.write(label + '/history.json', rows)
        def verify(row):
            result = row['result_json']; execution = result['deliveryId']
            if self.counts(execution, 1)['calls'] != 1: raise AssertionError('Duplicate/missing provider call')
            if self.counts(execution, 2)['calls'] != 0: raise AssertionError('Unexpected secondary call')
            if self.db.get_item(TableName='ORIGIN', Key={'pk': {'S': 'DELIVERY#' + result['requestKey']},
                'sk': {'S': 'META'}}, ConsistentRead=True).get('Item'): raise AssertionError('ORIGIN remains')
            if self.db.query(TableName='STEP', KeyConditionExpression='pk = :pk', ExpressionAttributeValues={':pk': {'S': 'DELIVERY#' + execution}},
                ConsistentRead=True)['Items']: raise AssertionError('STEP remains')
        with ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(verify, rows))
        report = {'rate': rate, 'inputSeconds': self.args.seconds, 'requests': len(starts),
                  'apiLatencyMillis': metrics['http_req_duration']['values'],
                  'latencyFromClientStartMillis': latencies, 'inputAndDrainSeconds': elapsed,
                  'completionTpsIncludingDrain': len(starts) / elapsed,
                  'maxKafkaLag': {topic: max(s['kafkaLag'][topic] for s in samples) for topic in GROUPS},
                  'maxRedisDue': max(s['redisDue'] for s in samples), 'allRequestsReconciled': True,
                  'providerCallsExactlyOnce': True, 'originAndStepsAbsent': True,
                  'sampleCount': len(samples), 'baselineCounts': before, 'finalCounts': samples[-1]}
        self.phases.append(report); self.write('load-summary.json', self.phases)
        print('PASS', label, 'requests', len(starts), 'completion TPS', round(report['completionTpsIncludingDrain'], 2), flush=True)

    def suite(self):
        for rate in self.args.rates: self.phase(rate)

    def close(self):
        stop(self.load)
        for reader in self.readers.values(): reader.close()
        super().close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rates', type=int, nargs='+', default=[10, 100])
    parser.add_argument('--seconds', type=int, default=5)
    parser.add_argument('--drain-timeout', type=int, default=300)
    parser.add_argument('--lifecycle-poll-ms', type=int, default=1000)
    args = parser.parse_args()
    if len(set(args.rates)) != len(args.rates) or not all(1 <= r <= 100 for r in args.rates) or not 1 <= args.seconds <= 30:
        parser.error('Distinct rates 1..100 TPS and seconds 1..30 required')
    if not 1 <= args.drain_timeout <= 600 or not 10 <= args.lifecycle_poll_ms <= 1000: parser.error('Invalid time limits')
    run = LoadRun(args)
    print('EVIDENCE', run.directory, flush=True)
    try: run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)}); raise
    finally: run.close()


if __name__ == '__main__': main()
