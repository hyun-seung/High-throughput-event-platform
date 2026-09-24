import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts/poc'))
import collector

class CollectorTest(unittest.TestCase):
    def setUp(self):
        self.snapshot={'partitions':[{'topic':'delivery.requested.v1','partition':0,'end':12,'start':0,'lag':2,'oldestUncommittedAgeSeconds':3}]}
    def test_failed_or_stale_probe_does_not_publish_old_lag_as_current(self):
        for healthy,now in [(False,101),(True,146)]:
            text=collector.exposition(self.snapshot,100,healthy,now)
            self.assertIn('platform_kafka_probe_up 0',text)
            self.assertNotIn('platform_kafka_committed_lag{',text)
    def test_healthy_probe_preserves_group_and_partition(self):
        text=collector.exposition(self.snapshot,100,True,110)
        self.assertIn('group="delivery-ingress-worker"} 2',text)
        self.assertIn('platform_kafka_probe_up 1',text)
    def test_api_401_refreshes_token_without_anonymous_fallback(self):
        collector.token='expired'
        with patch.object(collector,'get',side_effect=[HTTPError('local',401,'expired',{},None),json.dumps({'data':{'accessToken':'fresh'}}).encode(),b'metric 1']) as get:
            self.assertEqual(b'metric 1',collector.api_metrics())
            self.assertEqual({'Authorization':'Bearer expired'},get.call_args_list[0].args[1])
            self.assertEqual({'Authorization':'Bearer fresh'},get.call_args_list[2].args[1])
    def test_non_auth_error_is_not_hidden(self):
        collector.token='valid'
        with patch.object(collector,'get',side_effect=HTTPError('local',503,'unavailable',{},None)):
            with self.assertRaises(HTTPError):collector.api_metrics()

if __name__=='__main__':unittest.main()
