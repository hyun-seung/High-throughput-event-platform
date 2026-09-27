"""Read-only Kafka/DB probes and request-level reconciliation for the local PoC."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

REQUESTED = 'delivery.requested.v1'
DISPATCH = 'delivery.dispatch-requested.v1'
DLT = 'delivery.requested.dlt.v1'
TOPICS = (REQUESTED, DISPATCH, DLT)
GROUPS = {REQUESTED: 'delivery-ingress-worker', DISPATCH: 'delivery-dispatch-worker'}


def java_id(text):
    return str(uuid.UUID(bytes=hashlib.md5(text.encode()).digest(), version=3))


def delivery_id(tenant, key):
    return java_id(f'delivery:{tenant}:{key}')


def attempt_id(delivery):
    return java_id(f'attempt:{delivery}:mock-provider:1:1')


def complete_input(planned, started, answered, iterations, dropped):
    result = subprocess.run(_java_command('input-complete', planned, started, answered, iterations, dropped),
                            text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def read_manifest(path):
    result = subprocess.run(_java_command('manifest', path), text=True, capture_output=True)
    if result.returncode: raise ValueError(result.stderr.strip())
    evidence = json.loads(result.stdout)
    return ({int(i): row for i, row in evidence['starts'].items()},
            {int(i): row for i, row in evidence['results'].items()})


def _java_command(name, *args):
    root = Path(__file__).resolve().parents[2]
    java = str(Path(os.environ['JAVA_HOME']) / 'bin/java') if os.environ.get('JAVA_HOME') else 'java'
    return [java, '-jar', str(root / 'verification-tools/target/verification-tools-1.0-SNAPSHOT.jar'), name,
            *(str(arg) for arg in args)]


def reconcile(starts, results, tenant, records, items, provider):
    payload = {'starts': starts, 'results': results, 'tenant': tenant, 'records': records,
               'items': [{'pk': pk, 'sk': sk, 'item': item} for (pk, sk), item in items.items()],
               'provider': provider}
    result = subprocess.run(_java_command('poc-reconcile'), input=json.dumps(payload),
                            text=True, capture_output=True, check=True)
    evidence = json.loads(result.stdout)
    return evidence['rows'], evidence['summary']


class KafkaProbe:
    def __init__(self, bootstrap):
        from confluent_kafka import Consumer, TopicPartition
        self.tp = TopicPartition
        base = {'bootstrap.servers': bootstrap, 'enable.auto.commit': False, 'enable.auto.offset.store': False,
                'allow.auto.create.topics': False, 'socket.timeout.ms': 5000, 'session.timeout.ms': 10000,
                'broker.address.family': 'v4'}
        self.groups = {topic: Consumer({**base, 'group.id': group}) for topic, group in GROUPS.items()}
        self.reader = Consumer({**base, 'group.id': 'poc-read-only-' + uuid.uuid4().hex})
        metadata = self.reader.list_topics(timeout=10)
        self.partitions = {}
        for topic in TOPICS:
            if topic not in metadata.topics or metadata.topics[topic].error:
                raise RuntimeError(f'Missing topic {topic}')
            self.partitions[topic] = sorted(metadata.topics[topic].partitions)

    def snapshot(self, oldest=True):
        now = time.time()
        rows = []
        for topic in TOPICS:
            partitions = [self.tp(topic, part) for part in self.partitions[topic]]
            commits = {p.partition: p.offset for p in self.groups[topic].committed(partitions, timeout=5)} if topic in self.groups else {}
            for part in partitions:
                start, end = self.reader.get_watermark_offsets(part, timeout=5, cached=False)
                committed = commits.get(part.partition)
                # An uninitialized group starts at earliest in this project's configuration.
                effective = start if committed is not None and committed < 0 else committed
                if effective is not None and not start <= effective <= end:
                    raise RuntimeError('Committed offset outside retained range; do not report zero lag')
                lag = end - effective if effective is not None else None
                age = None
                if oldest and lag:
                    self.reader.assign([self.tp(topic, part.partition, effective)])
                    message = self.reader.poll(3)
                    if message is None or message.error() or message.offset() != effective:
                        raise RuntimeError('Cannot inspect oldest uncommitted record')
                    timestamp = message.timestamp()[1]
                    if timestamp >= 0: age = max(0, time.time() - timestamp / 1000)
                rows.append({'topic': topic, 'partition': part.partition, 'start': start, 'end': end,
                             'committed': committed, 'lag': lag, 'oldestUncommittedAgeSeconds': age})
        return {'time': now, 'partitions': rows, 'lag': sum(row['lag'] or 0 for row in rows),
                'oldestUncommittedAgeSeconds': max((r['oldestUncommittedAgeSeconds'] or 0 for r in rows), default=0)}

    def records(self, before, after):
        initial = {(r['topic'], r['partition']): r['end'] for r in before['partitions']}
        rows = []
        for bounds in after['partitions']:
            topic, part, end = bounds['topic'], bounds['partition'], bounds['end']
            start = initial[(topic, part)]
            if start < bounds['start']: raise RuntimeError('Kafka evidence expired during the run')
            if start == end: continue
            self.reader.assign([self.tp(topic, part, start)])
            deadline = time.monotonic() + 60
            position = start
            while position < end:
                if time.monotonic() > deadline: raise TimeoutError('Kafka evidence collection timed out')
                message = self.reader.poll(1)
                if message is None: continue
                if message.error(): raise RuntimeError(str(message.error()))
                if message.offset() >= end: break
                if message.offset() != position: raise RuntimeError('Kafka offset gap in test evidence')
                position = message.offset() + 1
                try: event = json.loads(message.value())
                except (ValueError, TypeError): event = {}
                rows.append({'topic': topic, 'partition': part, 'offset': message.offset(),
                             'deliveryId': event.get('deliveryId'), 'requestKey': event.get('requestKey'), 'occurredAt': event.get('occurredAt')})
        return rows

    def close(self):
        for consumer in [self.reader, *self.groups.values()]: consumer.close()


def read_items(endpoint, deliveries):
    import boto3
    from botocore.config import Config
    client = boto3.client('dynamodb', endpoint_url=endpoint, region_name='ap-northeast-2',
                         aws_access_key_id='local', aws_secret_access_key='local',
                         config=Config(connect_timeout=3, read_timeout=5, retries={'max_attempts': 2}))
    items = {}

    def fetch(table, keys):
        for index in range(0, len(keys), 100):
            pending = {table: {'Keys': keys[index:index+100], 'ConsistentRead': True,
                              'ProjectionExpression': 'pk,sk,#s,occurred_at,updated_at,attempt_id,delivery_id',
                              'ExpressionAttributeNames': {'#s': 'status'}}}
            for retry in range(6):
                result = client.batch_get_item(RequestItems=pending)
                for item in result['Responses'].get(table, []):
                    items[(item['pk']['S'], item['sk']['S'])] = item
                pending = result.get('UnprocessedKeys', {})
                if not pending: break
                time.sleep(min(2, 0.1 * 2 ** retry))
            else: raise RuntimeError('Unprocessed DB keys remain; reconciliation incomplete')
    try:
        fetch('ORIGIN', [{'pk': {'S': 'DELIVERY#' + delivery}, 'sk': {'S': 'META'}} for delivery in deliveries])
        executions = {items.get(('DELIVERY#' + delivery, 'META'), {}).get('delivery_id', {}).get('S', delivery)
                      for delivery in deliveries}
        fetch('STEP', [{'pk': {'S': 'DELIVERY#' + execution}, 'sk': {'S': 'ATTEMPT#' + attempt_id(execution)}}
                       for execution in sorted(executions)])
    finally:
        client.close()
    return items
