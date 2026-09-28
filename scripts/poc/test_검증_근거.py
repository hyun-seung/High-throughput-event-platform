import json
import io
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from 검증_근거 import REQUESTED, DISPATCH, DLT, KafkaProbe, SupervisedProcess, attempt_id, complete_input, delivery_id, read_manifest, reconcile


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.delivery = delivery_id(1, 'test-key')
        self.attempt = attempt_id(self.delivery)
        self.starts = {0: {'key': 'test-key'}}
        self.results = {0: {'status': 202, 'deliveryId': self.delivery}}
        self.records = [{'topic': topic, 'deliveryId': self.delivery} for topic in (REQUESTED, DISPATCH)]
        self.items = {('DELIVERY#' + self.delivery, 'META'): {'occurred_at': {'S': '2026-09-24T00:00:00Z'}},
                      ('DELIVERY#' + self.delivery, 'ATTEMPT#' + self.attempt):
                          {'status': {'S': 'ACCEPTED'}, 'updated_at': {'S': '2026-09-24T00:00:00.100Z'}}}
        self.provider = {self.attempt: {'calls': 1, 'effects': 1}}

    def evaluate(self):
        return reconcile(self.starts, self.results, 1, self.records, self.items, self.provider)

    def test_duplicate_http_inputs_may_have_multiple_kafka_records_but_one_effect(self):
        self.starts[1] = self.starts[0]
        self.results[1] = self.results[0]
        self.records *= 2
        rows, summary = self.evaluate()
        self.assertTrue(summary['consistent'])
        self.assertEqual(1, summary['uniqueRequests'])
        self.assertEqual(2, summary['submitted'])
        self.assertAlmostEqual(100, rows[0]['persistedTimestampLatencyMs'])

    def test_request_key_maps_to_a_distinct_execution_in_v2(self):
        execution = 'c1e9b6d1-47fc-4507-8406-0dcd7e780506'
        attempt = attempt_id(execution)
        self.items[('DELIVERY#' + self.delivery, 'META')]['delivery_id'] = {'S': execution}
        self.items[('DELIVERY#' + execution, 'ATTEMPT#' + attempt)] = self.items.pop(('DELIVERY#' + self.delivery, 'ATTEMPT#' + self.attempt))
        self.provider = {attempt: {'calls': 1, 'effects': 1}}
        self.records[1] = {'topic': DISPATCH, 'deliveryId': execution, 'requestKey': self.delivery}
        rows, summary = self.evaluate()
        self.assertTrue(summary['consistent'])
        self.assertEqual(execution, rows[0]['executionId'])

    def test_multiple_generations_cannot_hide_behind_one_request_key(self):
        self.records.append({'topic': DISPATCH, 'deliveryId': 'another-execution', 'requestKey': self.delivery})
        rows, summary = self.evaluate()
        self.assertFalse(summary['consistent'])
        self.assertIn('multiple_execution_generations', rows[0]['problems'])

    def test_provider_deduplication_cannot_hide_internal_duplicate_call(self):
        self.provider[self.attempt] = {'calls': 2, 'effects': 1}
        rows, summary = self.evaluate()
        self.assertFalse(summary['consistent'])
        self.assertIn('provider_call_or_effect_mismatch', rows[0]['problems'])

    def test_503_and_missing_response_are_still_reconciled_with_persisted_acceptance(self):
        self.starts[1] = self.starts[0]
        self.results[0]['status'] = 503
        rows, summary = self.evaluate()
        self.assertEqual('ACCEPTED', rows[0]['state'])
        self.assertEqual(1, summary['unanswered'])
        self.assertFalse(summary['consistent'])

    def test_202_without_evidence_is_unexplained_and_fails(self):
        self.records.clear()
        self.items.clear()
        self.provider[self.attempt] = {'calls': 0, 'effects': 0}
        rows, summary = self.evaluate()
        self.assertEqual('UNEXPLAINED', rows[0]['state'])
        self.assertFalse(summary['consistent'])

    def test_dlt_and_accepted_coexistence_does_not_count_as_normal_success(self):
        self.records.append({'topic': DLT, 'deliveryId': self.delivery})
        rows, summary = self.evaluate()
        self.assertEqual('ACCEPTED', rows[0]['state'])
        self.assertIn('dlt_present', rows[0]['problems'])
        self.assertFalse(summary['consistent'])

    def test_unexpected_kafka_input_detects_cross_run_contamination(self):
        self.records.append({'topic': REQUESTED, 'deliveryId': 'not-this-run'})
        _, summary = self.evaluate()
        self.assertEqual(['not-this-run'], summary['unexpectedKafkaIds'])
        self.assertFalse(summary['consistent'])

    def test_manifest_preserves_unfinished_attempt_and_rejects_duplicate_log_events(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'requests.jsonl'
            event = json.dumps({'kind': 'start', 'iteration': 0, 'key': 'key'}) + '\n'
            path.write_text(event)
            starts, results = read_manifest(path)
            self.assertEqual(1, len(starts))
            self.assertFalse(results)
            path.write_text(event * 2)
            with self.assertRaises(ValueError): read_manifest(path)

    def test_arrival_boundary_allows_one_extra_but_no_lost_or_unanswered_input(self):
        self.assertTrue(complete_input(10, 10, 10, 10, 0))
        self.assertTrue(complete_input(10, 11, 11, 11, 0))
        self.assertFalse(complete_input(10, 9, 9, 9, 0))
        self.assertFalse(complete_input(10, 11, 10, 10, 0))
        self.assertFalse(complete_input(10, 10, 10, 10, 1))

    def test_kafka_probe_keeps_one_java_process_for_snapshot_and_close(self):
        process = MagicMock()
        process.stdin = io.StringIO()
        process.stdout = io.StringIO('{"ready":true}\n{"lag":2,"partitions":[]}\n{"closed":true}\n')
        process.poll.return_value = None
        with patch('검증_근거.subprocess.Popen', return_value=process) as start:
            probe = KafkaProbe('localhost:29092')
            self.assertEqual(2, probe.snapshot(oldest=False)['lag'])
            sent = process.stdin.getvalue().splitlines()
            self.assertEqual([{'op': 'snapshot', 'oldest': False}], [json.loads(row) for row in sent])
            probe.close()
            start.assert_called_once()

    def test_killed_supervisor_cleans_only_its_owned_child_group(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / 'child.log'
            with log_path.open('w') as log:
                child = SupervisedProcess(['sleep', '30'], log_path, directory, os.environ.copy(), log)
                pid = child.pid
                try:
                    self.assertEqual(child.supervisor.pid, os.getpgid(pid))
                    child.supervisor.kill()
                    child.supervisor.wait(timeout=5)
                    child.close()
                    for _ in range(50):
                        try: os.kill(pid, 0)
                        except ProcessLookupError: break
                        time.sleep(.05)
                    else: self.fail('Supervised child remained after owner cleanup')
                finally:
                    if child.supervisor.poll() is None: child.close()


if __name__ == '__main__':
    unittest.main()
