#!/usr/bin/env python3
"""Local offline copy from delivery_state into ORIGIN/STEP. Default: read-only verification/plan.

Stop every source/destination writer before --apply. The source is never deleted.
Partial copies can be resumed; an existing destination must match exactly.
"""
import argparse
from collections import Counter
import json
from urllib.parse import urlparse


def destination(item):
    pk, sk = item['pk']['S'], item['sk']['S']
    if pk.startswith('DELIVERY#'):
        if sk == 'META': return 'ORIGIN'
        if sk == 'FINAL' or sk.startswith('ATTEMPT#'): return 'STEP'
    if pk.startswith('RECEIPT#') and sk == 'META': return 'STEP'
    raise ValueError('Unrecognized source record; manual mapping required')


def ensure_tables(db):
    for table in ('ORIGIN', 'STEP'):
        try:
            description = db.describe_table(TableName=table)['Table']
        except db.exceptions.ResourceNotFoundException:
            db.create_table(TableName=table, BillingMode='PAY_PER_REQUEST',
                KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}],
                AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'},
                    {'AttributeName': 'lifecycle_bucket', 'AttributeType': 'S'}, {'AttributeName': 'lifecycle_due', 'AttributeType': 'N'}],
                GlobalSecondaryIndexes=[{'IndexName': 'lifecycle_due_v1', 'KeySchema': [
                    {'AttributeName': 'lifecycle_bucket', 'KeyType': 'HASH'}, {'AttributeName': 'lifecycle_due', 'KeyType': 'RANGE'}],
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}}])
            db.get_waiter('table_exists').wait(TableName=table)
            description = db.describe_table(TableName=table)['Table']
        keys = {(k['AttributeName'], k['KeyType']) for k in description['KeySchema']}
        indexes = description.get('GlobalSecondaryIndexes', [])
        if keys != {('pk', 'HASH'), ('sk', 'RANGE')} or not any(
                i['IndexName'] == 'lifecycle_due_v1' and i['IndexStatus'] == 'ACTIVE'
                and i['Projection']['ProjectionType'] == 'KEYS_ONLY'
                and {(k['AttributeName'], k['KeyType']) for k in i['KeySchema']}
                    == {('lifecycle_bucket', 'HASH'), ('lifecycle_due', 'RANGE')} for i in indexes):
            raise ValueError('Destination table/index schema mismatch')


def canonical(value):
    # DynamoDB sets are unordered. Compare values without modifying stored content.
    if isinstance(value, dict):
        return {k: sorted(v) if k in ('SS', 'NS', 'BS') else canonical(v) for k, v in value.items()}
    if isinstance(value, list): return [canonical(v) for v in value]
    return value


def migrate(db, source, apply=False):
    if source in ('ORIGIN', 'STEP'): raise ValueError('Source cannot be a destination table')
    records = []
    for page in db.get_paginator('scan').paginate(TableName=source, ConsistentRead=True):
        for item in page['Items']:
            records.append((destination(item), item))
    if apply: ensure_tables(db)
    counts = Counter(table for table, _ in records)
    pending = []
    for table, item in records:
        key = {k: item[k] for k in ('pk', 'sk')}
        try: existing = db.get_item(TableName=table, Key=key, ConsistentRead=True).get('Item')
        except db.exceptions.ResourceNotFoundException: existing = None
        if existing is None: pending.append((table, item))
        elif canonical(existing) != canonical(item): raise ValueError('Destination conflict; no existing record was overwritten')
    if apply:
        for table, item in pending:
            db.put_item(TableName=table, Item=item, ConditionExpression='attribute_not_exists(pk) AND attribute_not_exists(sk)')
        # Every source record, including previously copied records, must still match before cutover.
        for table, item in records:
            saved = db.get_item(TableName=table, Key={k: item[k] for k in ('pk', 'sk')}, ConsistentRead=True).get('Item')
            if canonical(saved) != canonical(item): raise ValueError('Post-copy verification failed; keep writers stopped')
    return {'source': source, 'sourceItems': len(records), 'destinations': dict(counts),
            'missingBefore': len(pending), 'applied': apply, 'sourceDeleted': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default='http://localhost:18000')
    parser.add_argument('--source-table', default='delivery_state')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--writers-stopped', action='store_true', help='Confirm ALL source/destination writers are stopped')
    args = parser.parse_args()
    endpoint = urlparse(args.endpoint)
    if endpoint.scheme != 'http' or endpoint.hostname not in ('localhost', '127.0.0.1'):
        parser.error('This migration utility is local-only')
    if args.apply and not args.writers_stopped: parser.error('--apply requires --writers-stopped')
    import boto3
    from botocore.config import Config
    db = boto3.client('dynamodb', endpoint_url=args.endpoint, region_name='ap-northeast-2',
            aws_access_key_id='dummy', aws_secret_access_key='dummy',
            config=Config(connect_timeout=3, read_timeout=10, retries={'max_attempts': 2}))
    try:
        print(json.dumps(migrate(db, args.source_table, args.apply), ensure_ascii=False))
    finally:
        db.close()


if __name__ == '__main__': main()
