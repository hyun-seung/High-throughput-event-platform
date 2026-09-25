import copy
import json
import unittest

from postgres_recovery import verify_outage, verify_owned_postgres


class PostgresOutageEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.before = {'applicationPids': {'result': 101}, 'storeFailures': 0,
                       'offsets': [{'partition': 0, 'committed': -1001, 'high': 0, 'lag': 0}]}
        rows = []
        for outcome in ('DELIVERED', 'EXPIRED'):
            result = {'deliveryId': outcome, 'requestKey': outcome, 'outcome': outcome, 'routeOrder': 1,
                      'resultAt': '2026-09-25T00:00:00Z', 'deadline': '2026-09-25T00:00:00Z'}
            rows.append({'expectedOutcome': outcome, 'requestKey': outcome,
                         'origin': {'delivery_id': {'S': outcome}},
                         'step': {'result_event': {'S': json.dumps(result)}, 'deadline_at': {'N': '1790294400000'}},
                         'provider': [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}], 'completedRedisTtl': -2})
        self.during = {'postgres': {'status': 'exited', 'running': False}, 'sqlProbeRejected': True,
                       'applicationPids': {'result': 101}, 'storeFailures': 2, 'customerRequests': 0,
                       'offsets': [{'partition': 0, 'committed': -1001, 'high': 2, 'lag': 2}], 'requests': rows}

    def test_complete_evidence_passes(self):
        verify_outage(self.before, self.during)

    def test_healthy_storage_or_missing_failed_work_is_not_outage_proof(self):
        for field, value in [('postgres', {'status': 'running', 'running': True}), ('sqlProbeRejected', False),
                             ('storeFailures', 0), ('customerRequests', 1), ('applicationPids', {'result': 102}),
                             ('offsets', [{'partition': 0, 'committed': 1, 'high': 3, 'lag': 2}])]:
            bad = copy.deepcopy(self.during); bad[field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError): verify_outage(self.before, bad)

    def test_missing_durable_state_duplicate_send_or_early_cleanup_fails(self):
        for field, value in [('origin', {'delivery_id': {'S': 'wrong'}}),
                             ('provider', [{'calls': 2, 'effects': 2}, {'calls': 0, 'effects': 0}]),
                             ('completedRedisTtl', 86000), ('requestKey', 'wrong'), ('expectedOutcome', 'EXPIRED')]:
            bad = copy.deepcopy(self.during); bad['requests'][0][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError): verify_outage(self.before, bad)
        bad = copy.deepcopy(self.during); bad['requests'][1]['step']['deadline_at']['N'] = '1790294400001'
        with self.assertRaises(AssertionError): verify_outage(self.before, bad)

    def test_only_owned_postgres_can_be_stopped(self):
        labels = {'com.docker.compose.project': 'owned', 'com.docker.compose.service': 'postgres'}
        verify_owned_postgres({'Config': {'Labels': labels}}, 'owned')
        for field, value in [('com.docker.compose.project', 'shared'), ('com.docker.compose.service', 'redis')]:
            bad = {**labels, field: value}
            with self.subTest(field=field), self.assertRaises(AssertionError):
                verify_owned_postgres({'Config': {'Labels': bad}}, 'owned')


if __name__ == '__main__': unittest.main()
