import unittest

from 카프카_복구 import verify_owned_kafka, verify_unconfirmed, kafka_volumes
from 카프카_복제_재연결 import failed_connections


class KafkaRecoveryEvidenceTest(unittest.TestCase):
    def test_only_java_network_client_failed_connections_are_counted(self):
        log = ('2026-09-26T13:38:24.523+09:00  WARN app org.apache.kafka.clients.NetworkClient : '
               '[Consumer clientId=x] Connection to node -1 (localhost:9092) could not be established.\n'
               '2026-09-26T13:38:25.028+09:00  WARN app org.apache.kafka.clients.NetworkClient : '
               '[Consumer clientId=x] Bootstrap broker localhost:9092 disconnected\n'
               '%3|1758888888|FAIL|observer#consumer-1: Connect to localhost:9092 failed\n')
        self.assertEqual(['2026-09-26T13:38:24.523+09:00'], failed_connections(log))

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

if __name__ == '__main__': unittest.main()
