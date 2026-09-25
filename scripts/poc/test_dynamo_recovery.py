import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import signal

from dynamo_recovery import DynamoRecovery, verify_blocked, verify_owned_dynamo


class DynamoOutageEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.before = {'applicationPids': {'dispatch': 123}, 'metrics': {'claimFailed': 0, 'lifecycleFailed': 0},
                       'dispatchOffsets': [{'partition': 0, 'committed': -1001, 'lag': 1}]}
        self.during = {'dynamo': {'status': 'exited', 'running': False}, 'applicationPids': {'dispatch': 123},
                       'metrics': {'claimFailed': 2, 'lifecycleFailed': 1},
                       'dispatchOffsets': [{'partition': 0, 'committed': -1001, 'lag': 1}],
                       'provider': [{'calls': 0, 'effects': 0}, {'calls': 0, 'effects': 0}],
                       'historyRows': 0, 'customerResults': 0, 'completedRedisTtl': -2}

    def test_pending_claim_and_expiry_require_independent_evidence(self):
        verify_blocked(self.before, self.during, 0, 'claimFailed')
        self.during['provider'][0] = {'calls': 1, 'effects': 1}
        verify_blocked(self.before, self.during, 1, 'lifecycleFailed')

    def test_false_outage_offset_advance_or_external_send_cannot_pass(self):
        for field, value in [('dynamo', {'status': 'running', 'running': True}), ('applicationPids', {'dispatch': 124}),
                             ('metrics', {'claimFailed': 0}), ('historyRows', 1), ('customerResults', 1),
                             ('completedRedisTtl', 24), ('dispatchOffsets', [{'partition': 0, 'committed': 1, 'lag': 1}]),
                             ('provider', [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}])]:
            bad = copy.deepcopy(self.during); bad[field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError): verify_blocked(self.before, bad, 0, 'claimFailed')

    def test_only_owned_dynamo_can_be_stopped(self):
        labels = {'com.docker.compose.project': 'owned', 'com.docker.compose.service': 'dynamodb-local'}
        verify_owned_dynamo({'Config': {'Labels': labels}}, 'owned')
        for field, value in [('com.docker.compose.project', 'shared'), ('com.docker.compose.service', 'postgres')]:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                verify_owned_dynamo({'Config': {'Labels': {**labels, field: value}}}, 'owned')

    def test_suspend_and_resume_only_tracked_child_without_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            run = object.__new__(DynamoRecovery); run.directory = Path(directory)
            child = Mock(); child.poll.return_value = None; child.pid = 123
            run.apps = {'dispatch': child}; run.paused_dispatch = False; run.signals = []
            with patch('dynamo_recovery.subprocess.check_output', return_value='T'):
                run.dispatch_signal(True)
                with self.assertRaises(AssertionError): run.dispatch_signal(True)
                run.dispatch_signal(False)
            self.assertEqual([call.args[0] for call in child.send_signal.call_args_list], [signal.SIGSTOP, signal.SIGCONT])
            self.assertFalse(run.paused_dispatch)
            self.assertEqual([entry['pid'] for entry in run.signals], [123, 123])


if __name__ == '__main__': unittest.main()
