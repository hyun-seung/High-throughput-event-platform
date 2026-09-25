import copy
import json
import unittest

from kafka_recovery import verify_owned_kafka, verify_unconfirmed, verify_retained_expiry, kafka_volumes


class KafkaRecoveryEvidenceTest(unittest.TestCase):
    def test_volume_identity_is_independent_of_docker_mount_order(self):
        mounts = [{'Type': 'volume', 'Destination': '/var/lib/kafka/data', 'Name': 'data'},
                  {'Type': 'volume', 'Destination': '/etc/kafka/secrets', 'Name': 'secrets'}]
        self.assertEqual(kafka_volumes({'Mounts': mounts}), kafka_volumes({'Mounts': list(reversed(mounts))}))
        self.assertEqual('data', kafka_volumes({'Mounts': mounts})['/var/lib/kafka/data'])

    def test_only_owned_kafka_can_be_mutated(self):
        labels = {'com.docker.compose.project': 'owned', 'com.docker.compose.service': 'kafka'}
        verify_owned_kafka({'Config': {'Labels': labels}}, 'owned')
        for field, value in [('com.docker.compose.project', 'shared'), ('com.docker.compose.service', 'redis')]:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                verify_owned_kafka({'Config': {'Labels': {**labels, field: value}}}, 'owned')

    def test_202_or_unrelated_503_is_not_unconfirmed_contract(self):
        verify_unconfirmed({'status': 503, 'body': {'data': {'code': 9003}}}, 9003)
        verify_unconfirmed({'status': 503, 'body': {'code': 'RECEIPT_UNCONFIRMED'}}, 'RECEIPT_UNCONFIRMED')
        for status, code in [(202, 9003), (500, 9003), (503, 123)]:
            with self.subTest(status=status, code=code), self.assertRaises(AssertionError):
                verify_unconfirmed({'status': status, 'body': {'code': code}}, 9003)

    def evidence(self):
        result = {'deliveryId': 'execution', 'requestKey': 'request', 'outcome': 'EXPIRED', 'routeOrder': 1,
                  'deadline': '2026-09-25T00:00:00Z', 'resultAt': '2026-09-25T00:00:00Z'}
        return {'kafka': {'status': 'exited', 'running': False}, 'requestKey': 'request',
                'publishFailuresBefore': 0, 'publishFailuresAfter': 1, 'historyRowsBefore': 2, 'historyRows': 2,
                'customerRequestsBefore': 2, 'customerRequests': 2, 'completedRedisTtl': -2,
                'origin': {'delivery_id': {'S': 'execution'}},
                'step': {'result_event': {'S': json.dumps(result)}, 'deadline_at': {'N': '1790294400000'}},
                'provider': [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}]}

    def test_retained_result_requires_unavailable_broker_and_no_early_cleanup(self):
        evidence = self.evidence(); verify_retained_expiry(evidence)
        for field, value in [('kafka', {'status': 'running', 'running': True}), ('publishFailuresAfter', 0),
                             ('historyRows', 3), ('customerRequests', 3), ('completedRedisTtl', 86400),
                             ('provider', [{'calls': 2, 'effects': 2}, {'calls': 0, 'effects': 0}])]:
            bad = copy.deepcopy(evidence); bad[field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError): verify_retained_expiry(bad)

    def test_wrong_identity_or_extended_deadline_fails(self):
        for field, value in [('deliveryId', 'other'), ('requestKey', 'other'), ('outcome', 'DELIVERED'),
                             ('resultAt', '2026-09-25T00:00:01Z')]:
            bad = self.evidence(); result = json.loads(bad['step']['result_event']['S']); result[field] = value
            bad['step']['result_event']['S'] = json.dumps(result)
            with self.subTest(field=field), self.assertRaises(AssertionError): verify_retained_expiry(bad)
        bad = self.evidence(); bad['step']['deadline_at']['N'] = '1790294400001'
        with self.assertRaises(AssertionError): verify_retained_expiry(bad)


if __name__ == '__main__': unittest.main()
