"""Prevent the full-flow harness from accepting incomplete or conflicting evidence."""
import json
from pathlib import Path
import tempfile
import subprocess
import threading
import unittest
from unittest.mock import patch
from 전체_흐름 import FullFlow
from 전체_흐름_부하 import LoadRun


class InfrastructureStartupTest(unittest.TestCase):
    def test_load_runner_closes_after_infrastructure_startup_failure(self):
        runner = object.__new__(LoadRun)
        runner.load = None
        runner.readers = {}
        with patch.object(FullFlow, 'close') as close:
            runner.close()
        close.assert_called_once_with()

    def runner(self, directory):
        runner = object.__new__(FullFlow)
        runner.directory = Path(directory)
        runner.run_id = 'full-flow-test'
        runner.compose = ['docker', 'compose', '-p', runner.run_id]
        runner.compose_env = {}
        return runner

    def test_init_must_finish_before_services_and_all_healthchecks_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            order = []
            def run(command, **kwargs): order.append(command)
            def output(command, **kwargs):
                order.append(command)
                if command[1] == 'wait': return '0\n'
                return json.dumps([{'State': {'Running': True, 'Health': {'Status': 'healthy'}}} for _ in range(4)])
            with patch('전체_흐름.subprocess.run', side_effect=run), patch('전체_흐름.subprocess.check_output', side_effect=output):
                runner.start_infrastructure()
            self.assertEqual(order[0], runner.compose + ['create'])
            self.assertEqual(order[1:3], [['docker', 'start', 'full-flow-test-dynamodb-data-init-1'],
                                       ['docker', 'wait', 'full-flow-test-dynamodb-data-init-1']])
            self.assertEqual([c[2] for c in order[3:7]], ['full-flow-test-' + s + '-1' for s in
                             ('redis', 'postgres', 'kafka', 'dynamodb-local')])
            self.assertEqual(order[-1][1], 'inspect')
            self.assertTrue((Path(directory) / 'infra-health.json').exists())

    def test_failed_init_does_not_start_services(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            with patch('전체_흐름.subprocess.run') as run, patch('전체_흐름.subprocess.check_output', return_value='1\n'):
                with self.assertRaisesRegex(RuntimeError, 'initialization failed'): runner.start_infrastructure()
            self.assertEqual(run.call_count, 2)
            self.assertFalse((Path(directory) / 'infra-health.json').exists())

    def test_exited_service_is_not_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            states = json.dumps([{'Name': 'kafka', 'State': {'Running': False}}])
            with patch('전체_흐름.subprocess.run'), patch('전체_흐름.subprocess.check_output', side_effect=['0\n', states]):
                with self.assertRaisesRegex(RuntimeError, 'stopped during'): runner.start_infrastructure()
            self.assertFalse((Path(directory) / 'infra-health.json').exists())

    def test_timed_out_created_container_is_retried_once(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            state = json.dumps({'Running': False, 'Status': 'created'})
            timeout = subprocess.TimeoutExpired(['docker', 'start'], 10)
            with patch('전체_흐름.subprocess.run', side_effect=[timeout, None]) as run, \
                    patch('전체_흐름.subprocess.check_output', return_value=state):
                runner.start_container('owned-container', None)
            self.assertEqual(run.call_count, 2)
            attempts = json.loads((Path(directory) / 'owned-container-start.json').read_text())
            self.assertEqual([a['outcome'] for a in attempts], ['timeout', 'started'])

    def test_timeout_with_running_container_does_not_start_it_again(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            with patch('전체_흐름.subprocess.run', side_effect=subprocess.TimeoutExpired([], 10)) as run, \
                    patch('전체_흐름.subprocess.check_output', return_value='{"Running":true}'):
                runner.start_container('owned-container', None)
            self.assertEqual(run.call_count, 1)

    def test_second_start_timeout_stops_retrying(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            with patch('전체_흐름.subprocess.run', side_effect=subprocess.TimeoutExpired([], 10)) as run, \
                    patch('전체_흐름.subprocess.check_output', return_value='{"Running":false,"Status":"created"}'):
                with self.assertRaises(subprocess.TimeoutExpired): runner.start_container('owned-container', None)
            self.assertEqual(run.call_count, 2)

    def test_cleanup_removes_only_owned_project_network_and_keeps_volumes(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            runner.apps = {}; runner.probe = None; runner.server = None; runner.logs = []
            runner.lock = threading.Lock(); runner.callback_records = []; runner.infra_started = True
            with patch('전체_흐름.subprocess.run') as run:
                runner.close()
            command = run.call_args.args[0]
            self.assertEqual(command, runner.compose + ['down', '--timeout', '20'])
            self.assertNotIn('--volumes', command)
            cleanup = json.loads((Path(directory) / 'cleanup.json').read_text())
            self.assertTrue(cleanup['containersRemoved'] and cleanup['networkRemoved'] and cleanup['volumesRetained'])


if __name__ == '__main__': unittest.main()
