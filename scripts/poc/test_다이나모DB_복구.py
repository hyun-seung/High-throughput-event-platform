import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import signal

from 다이나모DB_복구 import DynamoRecovery, verify_owned_dynamo


class DynamoOutageEvidenceTest(unittest.TestCase):
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
            with patch('다이나모DB_복구.subprocess.check_output', return_value='T'):
                run.dispatch_signal(True)
                with self.assertRaises(AssertionError): run.dispatch_signal(True)
                run.dispatch_signal(False)
            self.assertEqual([call.args[0] for call in child.send_signal.call_args_list], [signal.SIGSTOP, signal.SIGCONT])
            self.assertFalse(run.paused_dispatch)
            self.assertEqual([entry['pid'] for entry in run.signals], [123, 123])


if __name__ == '__main__': unittest.main()
