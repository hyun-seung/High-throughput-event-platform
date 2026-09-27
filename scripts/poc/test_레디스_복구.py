import io
import json
import unittest
import urllib.error
from unittest.mock import Mock, patch

from 레디스_복구 import RedisRecovery, verify_owned_redis


class RedisOutageEvidenceTest(unittest.TestCase):
    def test_only_owned_redis_can_be_stopped(self):
        labels = {'com.docker.compose.project': 'owned', 'com.docker.compose.service': 'redis'}
        verify_owned_redis({'Config': {'Labels': labels}}, 'owned')
        for field, value in [('com.docker.compose.project', 'shared'), ('com.docker.compose.service', 'postgres')]:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                verify_owned_redis({'Config': {'Labels': {**labels, field: value}}}, 'owned')

    def test_rejection_requires_exact_limit_code_and_http_429(self):
        run = object.__new__(RedisRecovery); run.ports = {'api': 1}; run.token = 'test'; run.run_id = 'test'
        run.rejections = []; run.write = Mock()
        for status, code, passes in [(429, 3001, True), (429, 3002, False), (503, 3001, False)]:
            failure = urllib.error.HTTPError('http://localhost', status, 'test', {},
                                            io.BytesIO(json.dumps({'data': {'code': code}}).encode()))
            with patch('레디스_복구.http', side_effect=failure):
                if passes: run.reject('tps', 3001)
                else:
                    with self.assertRaises(AssertionError): run.reject('tps', 3001)
        with patch('레디스_복구.http', return_value=(202, {})):
            with self.assertRaises(AssertionError): run.reject('tps', 3001)


if __name__ == '__main__': unittest.main()
