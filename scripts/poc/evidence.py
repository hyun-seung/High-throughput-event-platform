"""Read-only Kafka/DB probes and request-level reconciliation for the local PoC."""
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
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


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low = int(index)
    return ordered[low] + (ordered[min(low + 1, len(ordered) - 1)] - ordered[low]) * (index - low)


def complete_input(planned, started, answered, iterations, dropped):
    # An arrival exactly at the duration boundary can add one iteration in k6.
    # Reconcile it too; never discard an extra request or forgive a missing response.
    return planned <= started <= planned + 1 and started == answered == iterations and dropped == 0


def read_manifest(path):
    starts, results = {}, {}
    for line in path.read_text().splitlines():
        # k6 --log-format raw --console-output writes each JSON message on its own line.
        row = json.loads(line)
        target = starts if row['kind'] == 'start' else results if row['kind'] == 'result' else None
        if target is None or row['iteration'] in target:
            raise ValueError('Unknown or repeated manifest event')
        target[row['iteration']] = row
    if not starts or not set(results).issubset(starts):
        raise ValueError('Missing starts or orphaned responses in manifest')
    for iteration, result in results.items():
        if starts[iteration]['key'] != result['key']:
            raise ValueError('Manifest key mismatch')
    return starts, results


def reconcile(starts, results, tenant, records, items, provider):
    by_key = defaultdict(list)
    for iteration, row in starts.items():
        by_key[row['key']].append(results.get(iteration, {'status': 0, 'deliveryId': None}))
    kafka = {topic: Counter() for topic in TOPICS}
    for row in records:
        if row['deliveryId'] is None:
            raise ValueError('Unparseable Kafka record cannot be silently omitted')
        kafka[row['topic']][row.get('requestKey') or row['deliveryId']] += 1
    dispatch_executions = defaultdict(set)
    for row in records:
        if row['topic'] == DISPATCH:
            dispatch_executions[row.get('requestKey') or row['deliveryId']].add(row['deliveryId'])
    rows, latencies = [], []
    for key, responses in by_key.items():
        delivery = delivery_id(tenant, key)
        pk = 'DELIVERY#' + delivery
        meta = items.get((pk, 'META'))
        execution_id = (meta or {}).get('delivery_id', {}).get('S', delivery)
        attempt = attempt_id(execution_id)
        execution = items.get(('DELIVERY#' + execution_id, 'ATTEMPT#' + attempt))
        state = execution.get('status', {}).get('S') if execution else None
        if state is None:
            state = 'META_ONLY' if meta else 'DLT' if kafka[DLT][delivery] else 'KAFKA_ONLY' if kafka[REQUESTED][delivery] else 'UNEXPLAINED'
        errors = []
        if any(r['status'] != 202 for r in responses): errors.append('http_unconfirmed_or_rejected')
        if any(r['status'] == 202 and r['deliveryId'] != delivery for r in responses): errors.append('response_id_mismatch')
        if state != 'ACCEPTED' or not meta: errors.append('not_persisted_accepted')
        if not kafka[REQUESTED][delivery] or not kafka[DISPATCH][delivery]: errors.append('missing_kafka_evidence')
        if kafka[DLT][delivery]: errors.append('dlt_present')
        if len(dispatch_executions[delivery]) > 1: errors.append('multiple_execution_generations')
        counts = provider.get(attempt, {'calls': 0, 'effects': 0})
        if counts != {'calls': 1, 'effects': 1}: errors.append('provider_call_or_effect_mismatch')
        latency = None
        if meta and execution and state == 'ACCEPTED':
            begin = datetime.fromisoformat(meta['occurred_at']['S'].replace('Z', '+00:00'))
            end = datetime.fromisoformat(execution['updated_at']['S'].replace('Z', '+00:00'))
            latency = (end - begin).total_seconds() * 1000
            if latency < 0: errors.append('invalid_wall_clock')
            else: latencies.append(latency)
        rows.append({'key': key, 'deliveryId': delivery, 'executionId': execution_id, 'attemptId': attempt, 'state': state,
                     'httpStatuses': [r['status'] for r in responses], 'provider': counts,
                     'kafka': {topic: kafka[topic][delivery] for topic in TOPICS},
                     'persistedTimestampLatencyMs': latency, 'problems': errors})
    expected = {row['deliveryId'] for row in rows}
    unexpected = sorted(set().union(*(set(values) for values in kafka.values())) - expected)
    return rows, {'uniqueRequests': len(rows), 'submitted': len(starts), 'responses': len(results),
                  'httpStatuses': dict(Counter(str(r['status']) for r in results.values())),
                  'unanswered': len(starts) - len(results),
                  'states': dict(Counter(r['state'] for r in rows)),
                  'problemRequests': sum(bool(r['problems']) for r in rows),
                  'unexpectedKafkaIds': unexpected,
                  'persistedTimestampLatencyMs': {f'p{int(p*100)}': percentile(latencies, p) for p in (.50, .95, .99)},
                  'consistent': all(not r['problems'] for r in rows) and not unexpected and len(results) == len(starts)}


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
