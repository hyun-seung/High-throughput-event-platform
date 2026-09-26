#!/usr/bin/env python3
"""SIGTERM after a customer receives the result body but before acknowledging it."""
import argparse
from datetime import datetime, timezone

from 업체_호출중_정상종료_복구 import GracefulInflightRecovery
from 프로세스_복구 import verify_ack_loss
from 수신결과_흐름 import wait_until


class GracefulCustomerRecovery(GracefulInflightRecovery):
    def suite(self):
        with self.lock:
            self.block_next_callback = True
        request_key = self.submit(self.run_id + '-customer-sigterm', {}, False)
        wait_until(lambda: self.alive() or self.customer_received.is_set(), 30)
        with self.lock:
            held = next(r for r in self.callback_records if r.get('responseWithheld'))
        assert held['body']['results'][0]['requestKey'] == request_key
        before = self.batch(held['body']['results'][0]['eventId'])
        assert before['status'] == 'IN_FLIGHT' and before['attempt_count'] == 1
        self.kill_owned('result')
        stopped = self.batch(held['body']['results'][0]['eventId'])
        assert stopped['batch_id'] == before['batch_id']
        assert stopped['request_body'] == before['request_body']
        assert stopped['attempt_count'] == 1 and stopped['status'] in ('IN_FLIGHT', 'PENDING'), stopped
        self.write('customer-sigterm-boundary.json', {'before': before, 'stopped': stopped,
            'customerBodyReceivedWithoutAck': held})
        self.release_response.set()
        self.restart_owned('result')
        self.finish_case('고객 응답 미확인 중 SIGTERM·동일 묶음 재전송', request_key, 'DELIVERED',
            {'batchBeforeTermination': before, 'batchAtStop': stopped})
        after = self.batch(held['body']['results'][0]['eventId'])
        with self.lock:
            callbacks = [r for r in self.callback_records if r['body']['batchId'] == before['batch_id']]
        verify_ack_loss(before, after, callbacks)
        self.write('summary.json', {'passed': 1, 'sigtermExitCode': self.transitions[0]['exitCode'],
            'shutdownSeconds': self.transitions[0]['shutdownSeconds'], 'gracefulLogSeen': True,
            'restartPid': self.transitions[1]['pid'], 'statusAtStop': stopped['status'],
            'batchReplayIdentical': True, 'customerReceipts': len(callbacks),
            'finalAttemptCount': after['attempt_count'], 'result': 'DELIVERED',
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'single local result JVM, SIGTERM after customer receives body before HTTP acknowledgement'})


def main():
    run = GracefulCustomerRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try:
        run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally:
        run.close()


if __name__ == '__main__': main()
