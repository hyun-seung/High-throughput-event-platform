import copy
import json
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import Mock

from process_recovery import ProcessRecovery, verify_ack_loss, verify_uncertain_step


class CrashEvidenceTest(unittest.TestCase):
    def test_ack_loss_requires_same_durable_batch_and_confirmed_retry(self):
        body = {'batchId': 'batch-1', 'results': [{'eventId': 'event-1'}]}
        before = {'status': 'IN_FLIGHT', 'attempt_count': 1, 'batch_id': 'batch-1', 'request_body': json.dumps(body)}
        after = {**before, 'status': 'DELIVERED', 'attempt_count': 2}
        callbacks = [{'status': 0, 'body': body}, {'status': 204, 'body': body}]
        verify_ack_loss(before, after, callbacks)
        for mutate in ('batch', 'body', 'attempt', 'pending', 'first_ack', 'missing_retry'):
            a, c = copy.deepcopy(after), copy.deepcopy(callbacks)
            if mutate == 'batch': a['batch_id'] = 'other-batch'
            if mutate == 'body': c[-1]['body']['results'][0]['eventId'] = 'other-event'
            if mutate == 'attempt': a['attempt_count'] = 3
            if mutate == 'pending': a['status'] = 'PENDING'
            if mutate == 'first_ack': c[0]['status'] = 204
            if mutate == 'missing_retry': c.pop()
            with self.subTest(mutate=mutate), self.assertRaises(AssertionError): verify_ack_loss(before, a, c)

    def test_unknown_recovery_cannot_advance_attempt_or_deadline_or_resend(self):
        before = {'status': {'S': 'PROCESSING'}, 'version': {'N': '1'}, 'retry_count': {'N': '0'},
                  'deadline_at': {'N': '90000'}, 'lease_until': {'N': '10000'}}
        counts = {'calls': 1, 'effects': 1}
        verify_uncertain_step(before, copy.deepcopy(before), counts)
        for field in before:
            after = copy.deepcopy(before); after[field] = {'S': 'changed'}
            with self.subTest(field=field), self.assertRaises(AssertionError): verify_uncertain_step(before, after, counts)
        for bad in ({'calls': 2, 'effects': 1}, {'calls': 1, 'effects': 0}):
            with self.assertRaises(AssertionError): verify_uncertain_step(before, before, bad)

    def test_sigkill_targets_only_owned_live_child_and_records_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            run = object.__new__(ProcessRecovery); run.directory = Path(directory)
            child = Mock(args=['owned-java', '-jar', 'test.jar']); child.poll.return_value = None
            child.wait.return_value = -signal.SIGKILL; child.pid = 123
            run.apps = {'result': child}; run.stopped_commands = {}; run.transitions = []
            with self.assertRaises(KeyError): run.kill_owned('other')
            child.kill.assert_not_called()
            run.kill_owned('result'); child.kill.assert_called_once_with()
            self.assertNotIn('result', run.apps)
            self.assertEqual(-signal.SIGKILL, run.transitions[0]['exitCode'])
            self.assertNotIn('args', json.loads((run.directory / 'process-transitions.json').read_text())[0])

    def test_normal_exit_cannot_be_reported_as_sigkill_proof(self):
        run = object.__new__(ProcessRecovery)
        child = Mock(args=['owned-java']); child.poll.return_value = None; child.wait.return_value = 0
        run.apps = {'result': child}; run.stopped_commands = {}; run.transitions = []
        with self.assertRaises(AssertionError): run.kill_owned('result')
        self.assertEqual([], run.transitions)


if __name__ == '__main__': unittest.main()
