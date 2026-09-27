import unittest

from 포스트그레스_복구 import verify_owned_postgres


class PostgresOutageEvidenceTest(unittest.TestCase):
    def test_only_owned_postgres_can_be_stopped(self):
        labels = {'com.docker.compose.project': 'owned', 'com.docker.compose.service': 'postgres'}
        verify_owned_postgres({'Config': {'Labels': labels}}, 'owned')
        for field, value in [('com.docker.compose.project', 'shared'), ('com.docker.compose.service', 'redis')]:
            bad = {**labels, field: value}
            with self.subTest(field=field), self.assertRaises(AssertionError):
                verify_owned_postgres({'Config': {'Labels': bad}}, 'owned')


if __name__ == '__main__': unittest.main()
