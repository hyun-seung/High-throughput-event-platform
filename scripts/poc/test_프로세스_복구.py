import json
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import Mock

from 프로세스_복구 import ProcessRecovery


class CrashEvidenceTest(unittest.TestCase):
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
