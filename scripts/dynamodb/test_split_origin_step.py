import os
import unittest
import uuid
from unittest.mock import patch

from split_origin_step import canonical, destination, ensure_tables, migrate


@unittest.skipUnless(os.getenv('DYNAMODB_TEST_ENDPOINT'), 'Local DynamoDB endpoint required')
class SplitTableIntegrationTest(unittest.TestCase):
    def setUp(self):
        import boto3
        from urllib.parse import urlparse
        endpoint = os.environ['DYNAMODB_TEST_ENDPOINT']
        if urlparse(endpoint).hostname not in ('localhost', '127.0.0.1'):
            raise ValueError('Local only')
        self.db = boto3.client('dynamodb', endpoint_url=endpoint, region_name='ap-northeast-2',
                              aws_access_key_id='dummy', aws_secret_access_key='dummy')
        self.source = 'test_split_' + uuid.uuid4().hex
        self.db.create_table(TableName=self.source, BillingMode='PAY_PER_REQUEST',
            KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'}])
        self.db.get_waiter('table_exists').wait(TableName=self.source)
        ensure_tables(self.db)
        request, execution, receipt = (str(uuid.uuid4()) for _ in range(3))
        self.items = [
            {'pk': {'S': 'DELIVERY#' + request}, 'sk': {'S': 'META'}, 'delivery_id': {'S': execution},
             'payload': {'S': '{"test":true}'}, 'schema_version': {'N': '2'}},
            {'pk': {'S': 'DELIVERY#' + execution}, 'sk': {'S': 'ATTEMPT#' + uuid.uuid4().hex},
             'request_key': {'S': request}, 'version': {'N': '1'}, 'receipt_marker_ids': {'SS': ['b', 'a']}},
            {'pk': {'S': 'DELIVERY#' + execution}, 'sk': {'S': 'FINAL'}, 'publish_state': {'S': 'PENDING'}},
            {'pk': {'S': 'RECEIPT#' + receipt}, 'sk': {'S': 'META'}, 'delivery_id': {'S': execution}},
        ]
        for item in self.items: self.db.put_item(TableName=self.source, Item=item)

    def tearDown(self):
        for item in self.items:
            self.db.delete_item(TableName=destination(item), Key={k: item[k] for k in ('pk', 'sk')})
        self.db.delete_table(TableName=self.source)
        self.db.close()

    def test_dry_run_copy_and_repeat_preserve_source_and_all_values(self):
        import subprocess, sys, json
        from pathlib import Path
        output = subprocess.check_output([sys.executable, str(Path(__file__).with_name('split_origin_step.py')),
                '--endpoint', os.environ['DYNAMODB_TEST_ENDPOINT'], '--source-table', self.source], text=True)
        plan = json.loads(output)
        self.assertFalse(plan['applied'])
        self.assertEqual({'ORIGIN': 1, 'STEP': 3}, plan['destinations'])
        self.assertEqual(4, plan['missingBefore'])
        self.assertEqual(4, migrate(self.db, self.source, True)['missingBefore'])
        self.assertEqual(0, migrate(self.db, self.source, True)['missingBefore'])
        self.assertEqual(4, self.db.scan(TableName=self.source, ConsistentRead=True)['Count'])
        for item in self.items:
            saved = self.db.get_item(TableName=destination(item), Key={k: item[k] for k in ('pk', 'sk')}, ConsistentRead=True)['Item']
            self.assertEqual(canonical(item), canonical(saved))

    def test_conflict_never_overwrites_existing_target_or_copies_other_rows(self):
        original = self.items[0]
        changed = {**original, 'payload': {'S': 'newer-origin'}}
        self.db.put_item(TableName='ORIGIN', Item=changed)
        with self.assertRaises(ValueError): migrate(self.db, self.source, True)
        self.assertEqual(changed, self.db.get_item(TableName='ORIGIN', Key={k: original[k] for k in ('pk', 'sk')}, ConsistentRead=True)['Item'])
        step = self.items[1]
        self.assertNotIn('Item', self.db.get_item(TableName='STEP', Key={k: step[k] for k in ('pk', 'sk')}, ConsistentRead=True))

    def test_partial_copy_after_lost_ack_can_resume_without_rewriting(self):
        actual_put = self.db.put_item
        def lost_ack(**kwargs):
            actual_put(**kwargs)
            raise RuntimeError('copy response lost')
        with patch.object(self.db, 'put_item', side_effect=lost_ack):
            with self.assertRaises(RuntimeError): migrate(self.db, self.source, True)
        self.assertEqual(3, migrate(self.db, self.source, True)['missingBefore'])
        self.assertEqual(0, migrate(self.db, self.source)['missingBefore'])


if __name__ == '__main__': unittest.main()
