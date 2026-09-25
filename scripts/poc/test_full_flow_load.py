import copy
import unittest
from full_flow_load import reconcile_phase


class FullFlowLoadEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.starts = {0: {'started': 1000}}
        self.responses = {0: {'deliveryId': 'request-1', 'status': 202}}
        result = {'deliveryId': 'execution-1', 'requestKey': 'request-1', 'outcome': 'DELIVERED',
                  'routeOrder': 1, 'resultAt': '1970-01-01T00:00:02Z', 'finalizedAt': '1970-01-01T00:00:03Z'}
        self.rows = [{'result_json': result, 'notification_status': 'DELIVERED', 'cleanup_status': 'DONE',
                      'stored_at': '1970-01-01T00:00:04Z', 'cleanup_completed_at': '1970-01-01T00:00:06Z'}]
        self.callbacks = [{'status': 204, 'receivedAt': '1970-01-01T00:00:05Z',
                           'body': {'results': [copy.deepcopy(result)]}}]

    def verify(self):
        return reconcile_phase(self.starts, self.responses, self.rows, self.callbacks)

    def test_distinct_stage_latencies_are_measured_from_client_start(self):
        _, stats = self.verify()
        self.assertEqual([stats[k]['p95'] for k in ('providerResult', 'finalized', 'sqlStored', 'customerReceived', 'cleanup')],
                         [1000, 2000, 3000, 4000, 5000])

    def test_equal_counts_with_wrong_request_keys_fail(self):
        self.rows[0]['result_json']['requestKey'] = 'other-request'
        with self.assertRaises(AssertionError): self.verify()

    def test_duplicate_history_generation_fails(self):
        self.rows.append(copy.deepcopy(self.rows[0]))
        with self.assertRaises(AssertionError): self.verify()

    def test_sql_without_customer_receipt_fails(self):
        self.callbacks = []
        with self.assertRaises(AssertionError): self.verify()

    def test_customer_payload_conflict_fails(self):
        self.callbacks[0]['body']['results'][0]['outcome'] = 'EXPIRED'
        with self.assertRaises(AssertionError): self.verify()

    def test_pending_cleanup_or_unconfirmed_input_fails(self):
        self.rows[0]['cleanup_status'] = 'PENDING'
        with self.assertRaises(AssertionError): self.verify()
        self.rows[0]['cleanup_status'] = 'DONE'
        self.responses[0]['status'] = 503
        with self.assertRaises(AssertionError): self.verify()

    def test_negative_wall_clock_latency_fails(self):
        self.starts[0]['started'] = 7000
        with self.assertRaises(AssertionError): self.verify()


if __name__ == '__main__': unittest.main()
