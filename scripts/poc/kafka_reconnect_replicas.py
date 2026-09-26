#!/usr/bin/env python3
"""Measure Java Kafka reconnect attempts across two ingress and dispatch JVMs during an owned outage."""
import argparse
from datetime import datetime, timezone
import json
import re
import socket
import subprocess
import time

from full_flow import ROOT, http
from kafka_recovery import KafkaRecovery
from receipt_flow import wait_until


ATTEMPT = re.compile(r'^([^ ]+) .*org\.apache\.kafka\.clients\.NetworkClient.*Connection to node .*could not be established', re.M)


def failed_connections(text):
    return ATTEMPT.findall(text)


class KafkaReconnectReplicas(KafkaRecovery):
    def __init__(self, args):
        super().__init__(args)
        self.outage_seconds = args.outage_seconds

    def fail_first_notification(self):
        return False

    def duplicate(self, name):
        clone = name + '_2'
        assert clone not in self.apps and name in self.apps
        ports = {}
        for label in ('server', 'metrics'):
            with socket.socket() as address:
                address.bind(('127.0.0.1', 0))
                ports[label] = address.getsockname()[1]
        command = list(self.apps[name].args)
        command = [f'--server.port={ports["server"]}' if arg.startswith('--server.port=') else
                   f'--management.server.port={ports["metrics"]}' if arg.startswith('--management.server.port=') else arg
                   for arg in command]
        path = self.directory / (clone + '.log')
        log = path.open('w'); self.logs.append(log)
        self.apps[clone] = subprocess.Popen(command, cwd=ROOT, env=self.application_env,
                                            stdout=log, stderr=subprocess.STDOUT)

        def ready():
            self.alive()
            try:
                return http(f'http://127.0.0.1:{ports["metrics"]}/actuator/health')[1]['status'] == 'UP' \
                    and 'partitions assigned:' in path.read_text()
            except OSError:
                return False
        wait_until(ready, 90)
        return {'name': clone, 'pid': self.apps[clone].pid, 'ports': ports}

    def snapshot(self):
        return {name: (self.directory / (name + '.log')).stat().st_size for name in self.apps}

    def attempts_since(self, positions):
        results = {}
        for name, offset in positions.items():
            with (self.directory / (name + '.log')).open('rb') as log:
                log.seek(offset)
                timestamps = failed_connections(log.read().decode(errors='replace'))
            results[name] = {'failedConnections': len(timestamps), 'first': timestamps[0] if timestamps else None,
                             'last': timestamps[-1] if timestamps else None}
        return results

    def suite(self):
        replicas = [self.duplicate(name) for name in ('ingress', 'dispatch')]
        self.write('replicas.json', replicas)
        baseline = self.submit(self.run_id + '-before', {}, False)
        self.finish_case('복제 전후 기본 인계', baseline, 'DELIVERED', {})
        before_pids = self.pids()
        broker = self.stop_kafka('replica-outage')
        positions = self.snapshot()
        started = time.monotonic()
        deadline = started + self.outage_seconds
        while time.monotonic() < deadline:
            self.alive()
            time.sleep(max(0, min(1, deadline - time.monotonic())))
        measured_outage_seconds = time.monotonic() - started
        attempts = self.attempts_since(positions)
        self.write('reconnect-attempts.json', {'durationSeconds': measured_outage_seconds,
                                               'perProcess': attempts, 'source': 'Java NetworkClient log lines'})
        for name in ('ingress', 'ingress_2', 'dispatch', 'dispatch_2'):
            assert attempts[name]['failedConnections'] > 0, (name, attempts)
        assert before_pids == self.pids()
        self.restore_kafka(broker, 'replica-outage')
        wait_until(lambda: all('partitions assigned:' in (self.directory / (n + '.log')).read_text()
                               for n in ('ingress', 'ingress_2', 'dispatch', 'dispatch_2')), 45)
        after = [self.submit(self.run_id + '-after-' + str(i), {}, False) for i in range(2)]
        for request_key in after:
            self.finish_case('복제 인스턴스 Kafka 복구 후 인계', request_key, 'DELIVERED', {})
        self.drain()
        assert before_pids == self.pids()
        self.write('summary.json', {'passed': 3, 'measuredOutageSeconds': measured_outage_seconds,
            'javaProcessCount': len(self.apps), 'consumerReplicaCounts': {'ingress': 2, 'dispatch': 2},
            'failedConnectionLogLines': {name: item['failedConnections'] for name, item in attempts.items()},
            'applicationPidsUnchanged': True, 'historyRows': len(self.history()),
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'two local JVMs per consumer group, one broker; log-observed failed connections, not network-level attempts or Kubernetes Pods'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--outage-seconds', type=int, default=60)
    args = parser.parse_args()
    if args.outage_seconds < 30: parser.error('--outage-seconds must be at least 30')
    run = KafkaReconnectReplicas(args)
    print('EVIDENCE', run.directory, flush=True)
    try:
        run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally:
        run.close()


if __name__ == '__main__': main()
