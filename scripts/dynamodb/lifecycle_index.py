#!/usr/bin/env python3
"""Explicit, local-only lifecycle index migration. No scans; legacy IDs supplied by the operator."""
import argparse
import json
import time
from urllib.parse import urlparse
import boto3

INDEX = 'lifecycle_due_v1'
TABLE = 'delivery_state'


def bucket(delivery):
    # Java String.hashCode over UTF-16 code units; delivery IDs are canonical ASCII UUIDs.
    value = 0
    if not delivery.isascii(): raise ValueError('Expected ASCII delivery ID')
    for char in delivery:
        value = (31 * value + ord(char)) & 0xffffffff
    if value >= 0x80000000: value -= 0x100000000
    return 'lifecycle-v1-' + str(value % 16)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default='http://localhost:18000')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--delivery-id', action='append', default=[])
    args = parser.parse_args()
    endpoint = urlparse(args.endpoint)
    if endpoint.scheme != 'http' or endpoint.hostname not in ('localhost', '127.0.0.1'):
        parser.error('Local DynamoDB only')
    db = boto3.client('dynamodb', endpoint_url=args.endpoint, region_name='ap-northeast-2',
                       aws_access_key_id='dummy', aws_secret_access_key='dummy')
    indexes = db.describe_table(TableName=TABLE)['Table'].get('GlobalSecondaryIndexes', [])
    exists = any(i['IndexName'] == INDEX for i in indexes)
    print(json.dumps({'indexExists': exists, 'apply': args.apply, 'backfillIds': args.delivery_id}))
    if not args.apply: return
    if not exists:
        db.update_table(TableName=TABLE, AttributeDefinitions=[
            {'AttributeName': 'lifecycle_bucket', 'AttributeType': 'S'}, {'AttributeName': 'lifecycle_due', 'AttributeType': 'N'}],
            GlobalSecondaryIndexUpdates=[{'Create': {'IndexName': INDEX, 'KeySchema': [
                {'AttributeName': 'lifecycle_bucket', 'KeyType': 'HASH'}, {'AttributeName': 'lifecycle_due', 'KeyType': 'RANGE'}],
                'Projection': {'ProjectionType': 'KEYS_ONLY'}}}])
    until = time.monotonic() + 60
    while time.monotonic() < until:
        indexes = db.describe_table(TableName=TABLE)['Table'].get('GlobalSecondaryIndexes', [])
        if any(i['IndexName'] == INDEX and i['IndexStatus'] == 'ACTIVE' for i in indexes): break
        time.sleep(.5)
    else: raise RuntimeError('Index not ACTIVE; retain existing state and retry later')
    import uuid
    for delivery in args.delivery_id:
        if str(uuid.UUID(delivery)) != delivery: raise ValueError('Expected canonical UUID')
        items, cursor = [], {}
        while True:
            page = db.query(TableName=TABLE, KeyConditionExpression='pk = :pk',
                ExpressionAttributeValues={':pk': {'S': 'DELIVERY#' + delivery}}, ConsistentRead=True,
                **({'ExclusiveStartKey': cursor} if cursor else {}))
            items += page['Items']; cursor = page.get('LastEvaluatedKey')
            if not cursor: break
        has_attempt = any(i['sk']['S'].startswith('ATTEMPT#') for i in items)
        for item in items:
            if 'lifecycle_bucket' in item or item.get('lifecycle_closed', {}).get('BOOL'): continue
            sk = item['sk']['S']; condition = 'attribute_exists(pk) AND attribute_not_exists(lifecycle_bucket)'
            names, values = {}, {':bucket': {'S': bucket(delivery)}}
            if sk == 'META' and not has_attempt: due = 0
            elif sk.startswith('ATTEMPT#'):
                state = item['status']['S']
                if state not in ('DELIVERED', 'DECISION_PENDING', 'RETRY_SCHEDULED', 'PROCESSING', 'ACCEPTED', 'REVIEW_REQUIRED'):
                    raise ValueError('Unrecognized Attempt state; manual review required')
                due = 0 if state in ('DELIVERED', 'DECISION_PENDING') else int(item.get('next_attempt_at', item.get('deadline_at', item.get('primary_deadline')))['N'])
                condition += ' AND #version = :version AND #state = :state AND attribute_not_exists(lifecycle_closed)'
                names = {'#version': 'version', '#state': 'status'}
                values.update({':version': item['version'], ':state': item['status']})
            elif sk == 'FINAL' and item.get('publish_state', {}).get('S') == 'PENDING':
                due = 0
                condition += ' AND publish_state = :pending AND result_event = :event'
                values.update({':pending': {'S': 'PENDING'}, ':event': item['result_event']})
            else: continue
            values[':due'] = {'N': str(due)}
            db.update_item(TableName=TABLE, Key={k: item[k] for k in ('pk', 'sk')},
                UpdateExpression='SET lifecycle_bucket = :bucket, lifecycle_due = :due',
                ConditionExpression=condition, ExpressionAttributeValues=values,
                **({'ExpressionAttributeNames': names} if names else {}))
        print('backfilled', delivery)


if __name__ == '__main__': main()
