#!/usr/bin/env python3
"""Check public main-port probes and a real SIGTERM/restart of the dispatch worker."""
import argparse
from datetime import datetime, timezone
import json
import time
import urllib.error

from 전체_흐름 import http
from 카프카_복구 import KafkaRecovery
from 수신결과_흐름 import wait_until


class GracefulRecovery(KafkaRecovery):
    def fail_first_notification(self):
        return False

    def probe_response(self, name, path):
        try:
            status, body = http(f'http://127.0.0.1:{self.ports[name]}/{path}')
            return {'status': status, 'body': body}
        except urllib.error.HTTPError as error:
            return {'status': error.code, 'body': json.loads(error.read())}

    def suite(self):
        probes = {}
        for name in ('api', 'receipt', 'ingress', 'dispatch', 'result'):
            probes[name] = {path: self.probe_response(name, path) for path in ('livez', 'readyz')}
            assert all(p['status'] == 200 and p['body']['status'] == 'UP' for p in probes[name].values()), probes[name]
        self.write('probes-before.json', probes)
        before = self.submit(self.run_id + '-before', {}, False)
        self.finish_case('SIGTERM 전 결과 인계', before, 'DELIVERED', {})
        process = self.apps['dispatch']
        old_pid = process.pid
        started = time.monotonic()
        process.terminate(35)
        exit_code = process.wait(timeout=35)
        shutdown_seconds = time.monotonic() - started
        del self.apps['dispatch']
        shutdown_log = (self.directory / 'dispatch.log').read_text()
        graceful = 'Commencing graceful shutdown' in shutdown_log and 'Graceful shutdown complete' in shutdown_log
        self.write('shutdown.json', {'signal': 'SIGTERM', 'pid': old_pid, 'exitCode': exit_code,
                                     'seconds': shutdown_seconds, 'gracefulLogSeen': graceful})
        assert exit_code in (0, 143) and graceful, (exit_code, graceful)
        log_path = self.directory / 'dispatch-restarted.log'
        log = log_path.open('w'); self.logs.append(log)
        replacement = process.restart(log_path)
        self.apps['dispatch'] = replacement
        assert replacement.pid != old_pid

        def ready():
            self.alive()
            try:
                return self.probe_response('dispatch', 'readyz')['status'] == 200 \
                    and 'partitions assigned:' in log_path.read_text()
            except OSError:
                return False
        wait_until(ready, 90)
        after = self.submit(self.run_id + '-after', {}, False)
        self.finish_case('SIGTERM 재시작 후 결과 인계', after, 'DELIVERED', {})
        self.drain()
        rows = self.history()
        assert len(rows) == 2 and {r['result_json']['requestKey'] for r in rows} == {before, after}
        self.write('summary.json', {'passed': 2, 'probeApplications': len(probes),
            'unauthenticatedLiveAndReadyOnMainPorts': True, 'dispatchSigtermExitCode': exit_code,
            'dispatchShutdownSeconds': shutdown_seconds, 'newDispatchPid': replacement.pid,
            'historyRows': len(rows), 'businessTopicsDrained': True,
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'local JVM graceful stop while idle and restart; not in-flight termination or Kubernetes eviction'})


def main():
    run = GracefulRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try:
        run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally:
        run.close()


if __name__ == '__main__': main()
