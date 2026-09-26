#!/usr/bin/env python3
"""SIGTERM after a secondary TCP provider effect but before its reply is received."""
import argparse
from datetime import datetime, timezone
import time

from 전체_흐름 import verify_delivery
from 업체_호출중_정상종료_복구 import GracefulInflightRecovery
from 프로세스_복구 import verify_uncertain_step
from 수신결과_흐름 import Run, wait_until


class GracefulTcpRecovery(GracefulInflightRecovery):
    def application_overrides(self):
        return {**super().application_overrides(), 'SECONDARY_TTL': '90s',
                'EXTERNAL_TCP_EXCHANGE_TIMEOUT': '30s'}

    def secondary_step(self, execution):
        return self.db.get_item(TableName='STEP', Key={'pk': {'S': 'DELIVERY#' + execution},
            'sk': {'S': 'ATTEMPT#' + Run.attempt(execution, 2)}}, ConsistentRead=True).get('Item', {})

    def suite(self):
        request_key = self.submit(self.run_id + '-tcp-inflight',
            {'simulatorResultCode': 'FALLBACK', 'simulatorTcpResponseDelayMillis': 60000,
             'simulatorTcpReceiptCodes': ['NONE']}, True)
        def effected():
            self.alive(); origin = self.origin(request_key)
            if not origin: return None
            execution = origin['delivery_id']['S']
            return execution if self.counts(execution, 2)['effects'] == 1 else None
        execution = wait_until(effected, 20)
        before = self.secondary_step(execution)
        assert before['status']['S'] == 'PROCESSING' and 'result_event' not in before
        self.kill_owned('dispatch')
        stopped = self.secondary_step(execution)
        assert before == stopped
        self.write('tcp-sigterm-boundary.json', {'step': stopped,
            'primaryProvider': self.counts(execution, 1), 'secondaryProvider': self.counts(execution, 2)})
        self.restart_owned('dispatch')
        after = self.secondary_step(execution)
        verify_uncertain_step(before, after, self.counts(execution, 2))
        assert time.time() * 1000 < int(after['deadline_at']['N'])
        row = wait_until(lambda: self.completed(request_key), 130)
        result = row['result_json']
        provider = [self.counts(execution, route) for route in (1, 2)]
        origin = self.origin(request_key)
        steps = self.db.query(TableName='STEP', KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={':pk': {'S': 'DELIVERY#' + execution}}, ConsistentRead=True)['Items']
        ttl = int(self.redis('TTL', 'delivery:completed:' + request_key))
        with self.lock:
            verify_delivery(row, 'EXPIRED', 2, [1, 1], provider, origin, steps, ttl, self.callback_records)
        assert provider == [{'calls': 1, 'effects': 0}, {'calls': 1, 'effects': 1}], provider
        assert result['resultAt'] == result['deadline']
        assert int(datetime.fromisoformat(result['deadline'].replace('Z', '+00:00')).timestamp() * 1000) \
            == int(before['deadline_at']['N'])
        self.write('summary.json', {'passed': 1, 'sigtermExitCode': self.transitions[0]['exitCode'],
            'shutdownSeconds': self.transitions[0]['shutdownSeconds'], 'gracefulLogSeen': True,
            'restartPid': self.transitions[1]['pid'], 'primaryProvider': provider[0], 'secondaryProvider': provider[1],
            'result': result['outcome'], 'resultRouteOrder': result['routeOrder'], 'historyRows': 1,
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'scope': 'single local dispatch JVM, SIGTERM after secondary TCP effect during pending response'})


def main():
    run = GracefulTcpRecovery(argparse.ArgumentParser(description=__doc__).parse_args())
    print('EVIDENCE', run.directory, flush=True)
    try:
        run.initialize(); run.suite()
    except Exception as failure:
        run.write('failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally:
        run.close()


if __name__ == '__main__': main()
