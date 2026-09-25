"""Prevent the full-flow harness from accepting incomplete or conflicting evidence."""
import copy
import json
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch
from full_flow import FullFlow, verify_delivery


class FullFlowEvidenceTest(unittest.TestCase):
    def setUp(self):
        result = {'deliveryId': 'execution-1', 'requestKey': 'request-1', 'outcome': 'DELIVERED', 'routeOrder': 1}
        self.args = [
            {'result_json': result, 'notification_status': 'DELIVERED', 'cleanup_status': 'DONE'},
            'DELIVERED', 1, [1, 0], [{'calls': 1}, {'calls': 0}], None, [], 86000,
            [{'status': 503, 'body': {'results': [copy.deepcopy(result)]}},
             {'status': 204, 'body': {'results': [copy.deepcopy(result)]}}]]

    def test_complete_evidence_and_identical_callback_retry_pass(self):
        verify_delivery(*self.args)
        self.args[-1].append(copy.deepcopy(self.args[-1][-1]))
        verify_delivery(*self.args)

    def test_sql_done_cannot_replace_customer_ack(self):
        self.args[-1] = self.args[-1][:1]
        with self.assertRaises(AssertionError): verify_delivery(*self.args)

    def test_customer_result_must_match_sql(self):
        self.args[-1][-1]['body']['results'][0]['outcome'] = 'EXPIRED'
        with self.assertRaises(AssertionError): verify_delivery(*self.args)

    def test_remaining_step_or_origin_fails_cleanup_proof(self):
        for position, remaining in ((5, {'pk': 'origin'}), (6, [{'pk': 'step'}])):
            with self.subTest(position=position):
                args = copy.deepcopy(self.args); args[position] = remaining
                with self.assertRaises(AssertionError): verify_delivery(*args)

    def test_unexpected_provider_call_fails(self):
        self.args[4][1]['calls'] = 1
        with self.assertRaises(AssertionError): verify_delivery(*self.args)

    def test_missing_completion_or_pending_sql_fails(self):
        args = copy.deepcopy(self.args); args[7] = -2
        with self.assertRaises(AssertionError): verify_delivery(*args)
        for field in ('notification_status', 'cleanup_status'):
            with self.subTest(field=field):
                args = copy.deepcopy(self.args); args[0][field] = 'PENDING'
                with self.assertRaises(AssertionError): verify_delivery(*args)


class InfrastructureStartupTest(unittest.TestCase):
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
            with patch('full_flow.subprocess.run', side_effect=run), patch('full_flow.subprocess.check_output', side_effect=output):
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
            with patch('full_flow.subprocess.run') as run, patch('full_flow.subprocess.check_output', return_value='1\n'):
                with self.assertRaisesRegex(RuntimeError, 'initialization failed'): runner.start_infrastructure()
            self.assertEqual(run.call_count, 2)
            self.assertFalse((Path(directory) / 'infra-health.json').exists())

    def test_exited_service_is_not_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            states = json.dumps([{'Name': 'kafka', 'State': {'Running': False}}])
            with patch('full_flow.subprocess.run'), patch('full_flow.subprocess.check_output', side_effect=['0\n', states]):
                with self.assertRaisesRegex(RuntimeError, 'stopped during'): runner.start_infrastructure()
            self.assertFalse((Path(directory) / 'infra-health.json').exists())

    def test_timed_out_created_container_is_retried_once(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            state = json.dumps({'Running': False, 'Status': 'created'})
            timeout = subprocess.TimeoutExpired(['docker', 'start'], 10)
            with patch('full_flow.subprocess.run', side_effect=[timeout, None]) as run, \
                    patch('full_flow.subprocess.check_output', return_value=state):
                runner.start_container('owned-container', None)
            self.assertEqual(run.call_count, 2)
            attempts = json.loads((Path(directory) / 'owned-container-start.json').read_text())
            self.assertEqual([a['outcome'] for a in attempts], ['timeout', 'started'])

    def test_timeout_with_running_container_does_not_start_it_again(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            with patch('full_flow.subprocess.run', side_effect=subprocess.TimeoutExpired([], 10)) as run, \
                    patch('full_flow.subprocess.check_output', return_value='{"Running":true}'):
                runner.start_container('owned-container', None)
            self.assertEqual(run.call_count, 1)

    def test_second_start_timeout_stops_retrying(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = self.runner(directory)
            with patch('full_flow.subprocess.run', side_effect=subprocess.TimeoutExpired([], 10)) as run, \
                    patch('full_flow.subprocess.check_output', return_value='{"Running":false,"Status":"created"}'):
                with self.assertRaises(subprocess.TimeoutExpired): runner.start_container('owned-container', None)
            self.assertEqual(run.call_count, 2)


if __name__ == '__main__': unittest.main()
