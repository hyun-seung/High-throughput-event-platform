import copy
import io
import json
import unittest
import urllib.error
from unittest.mock import Mock, patch

from redis_recovery import RedisRecovery, verify_owned_redis, verify_without_cache


class RedisOutageEvidenceTest(unittest.TestCase):
    def setUp(self):
        result = {'deliveryId': 'execution', 'outcome': 'EXPIRED', 'routeOrder': 1}
        self.args = [{'result_json': result, 'notification_status': 'DELIVERED', 'cleanup_status': 'DONE'},
                     'EXPIRED', [{'calls': 1, 'effects': 1}, {'calls': 0, 'effects': 0}], {}, [],
                     [{'status': 204, 'body': {'results': [copy.deepcopy(result)]}}]]

    def test_durable_completion_can_pass_without_redis_marker(self):
        verify_without_cache(*self.args)

    def test_missing_http_ack_unfinished_cleanup_or_resend_fails(self):
        for index, value in [(2, [{'calls': 2, 'effects': 1}, {'calls': 0, 'effects': 0}]),
                             (3, {'pk': 'origin'}), (4, [{'pk': 'step'}]), (5, [])]:
            bad = copy.deepcopy(self.args); bad[index] = value
            with self.subTest(index=index), self.assertRaises(AssertionError): verify_without_cache(*bad)
        bad = copy.deepcopy(self.args); bad[0]['cleanup_status'] = 'PENDING'
        with self.assertRaises(AssertionError): verify_without_cache(*bad)
        bad = copy.deepcopy(self.args); bad[5][0]['body']['results'][0]['outcome'] = 'DELIVERED'
        with self.assertRaises(AssertionError): verify_without_cache(*bad)

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
            with patch('redis_recovery.http', side_effect=failure):
                if passes: run.reject('tps', 3001)
                else:
                    with self.assertRaises(AssertionError): run.reject('tps', 3001)
        with patch('redis_recovery.http', return_value=(202, {})):
            with self.assertRaises(AssertionError): run.reject('tps', 3001)


if __name__ == '__main__': unittest.main()
