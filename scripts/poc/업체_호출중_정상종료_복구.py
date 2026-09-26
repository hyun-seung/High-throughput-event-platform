#!/usr/bin/env python3
"""SIGTERM during an external provider call; verify durable unknown state and original deadline."""
import argparse
from datetime import datetime, timezone
import time

from 프로세스_복구 import ProcessRecovery, verify_uncertain_step
from 수신결과_흐름 import wait_until


class GracefulInflightRecovery(ProcessRecovery):
    def kill_owned(self, name):
        assert name in self.apps
        process = self.apps[name]
        assert process.poll() is None
        self.stopped_commands[name] = process.args
        started = time.monotonic()
        process.terminate()
        code = process.wait(timeout=75)
        duration = time.monotonic() - started
        del self.apps[name]
        log = (self.directory / (name + '.log')).read_text()
        graceful = 'Commencing graceful shutdown' in log and 'Graceful shutdown complete' in log
        self.transitions.append({'application': name, 'action': 'SIGTERM', 'pid': process.pid,
                                 'exitCode': code, 'shutdownSeconds': duration, 'gracefulLogSeen': graceful,
                                 'at': datetime.now(timezone.utc).isoformat()})
        self.write('process-transitions.json', self.transitions)
        assert code in (0, 143) and graceful, (code, graceful)

    def suite(self):
        request_key = self.submit(self.run_id + '-dispatch-inflight',
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
        stopped = self.step(execution)
        assert before == stopped
        offsets = self.offsets('delivery.dispatch-requested.v1', 'delivery-dispatch-worker')
        self.write('inflight-sigterm-boundary.json', {'step': stopped,
            'provider': self.counts(execution, 1), 'dispatchOffsets': offsets})
        self.restart_owned('dispatch')
        after = self.step(execution)
        verify_uncertain_step(before, after, self.counts(execution, 1))
        assert time.time() * 1000 < int(after['deadline_at']['N'])
        result = self.finish_case('업체 효과 후 SIGTERM·원래 기한 만료', request_key, 'EXPIRED',
            {'stepBeforeTermination': before, 'stepAfterRestart': after, 'dispatchOffsetsAtStop': offsets})
        assert result['result_json']['resultAt'] == result['result_json']['deadline']
        assert int(datetime.fromisoformat(result['result_json']['deadline'].replace('Z', '+00:00')).timestamp() * 1000) \
            == int(before['deadline_at']['N'])
        assert self.counts(execution, 1) == {'calls': 1, 'effects': 1}
        self.write('summary.json', {'passed': 1, 'sigtermExitCode': self.transitions[0]['exitCode'],
            'shutdownSeconds': self.transitions[0]['shutdownSeconds'], 'gracefulLogSeen': True,
            'restartPid': self.transitions[1]['pid'], 'providerCalls': 1,
            'providerEffects': 1, 'dispatchLagAtStop': sum(p['lag'] for p in offsets),
            'result': result['result_json']['outcome'],
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'single local dispatch JVM, SIGTERM after provider effect during pending HTTP response'})


def main():
    run = GracefulInflightRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try:
        run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally:
        run.close()


if __name__ == '__main__': main()
