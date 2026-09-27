"""Read-only Kafka/DB probes and request-level reconciliation for the local PoC."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

REQUESTED = 'delivery.requested.v1'
DISPATCH = 'delivery.dispatch-requested.v1'
DLT = 'delivery.requested.dlt.v1'
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


def verify_evidence(name, evidence):
    result = subprocess.run(_java_command(name), input=json.dumps(evidence), text=True, capture_output=True)
    if result.returncode or not json.loads(result.stdout).get('consistent'):
        raise AssertionError(result.stderr.strip() or name + ' evidence mismatch')


def stop_owned_process(pid, action):
    subprocess.run(_java_command('stop-owned-process', pid, action), check=True,
                   stdout=subprocess.DEVNULL)


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
        self.process = subprocess.Popen(_java_command('poc-kafka', bootstrap), stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, text=True, bufsize=1)
        ready = self.process.stdout.readline()
        if not ready or not json.loads(ready).get('ready'):
            self.close()
            raise RuntimeError('Java Kafka probe did not become ready')

    def _request(self, payload):
        if self.process.poll() is not None: raise RuntimeError('Java Kafka probe stopped')
        self.process.stdin.write(json.dumps(payload) + '\n')
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line: raise RuntimeError('Java Kafka probe stopped without a response')
        response = json.loads(line)
        if 'error' in response: raise RuntimeError(response['error'])
        return response

    def snapshot(self, oldest=True):
        return self._request({'op': 'snapshot', 'oldest': oldest})

    def records(self, before, after):
        return self._request({'op': 'records', 'before': before, 'after': after})

    def close(self):
        if self.process.poll() is None:
            try:
                self._request({'op': 'close'})
                self.process.wait(timeout=5)
            except (OSError, RuntimeError, subprocess.TimeoutExpired):
                self.process.terminate()
                try: self.process.wait(timeout=5)
                except subprocess.TimeoutExpired: self.process.kill(); self.process.wait(timeout=5)
        self.process.stdin.close()
        self.process.stdout.close()


def read_items(endpoint, deliveries):
    result = subprocess.run(_java_command('poc-items', endpoint), input=json.dumps(deliveries),
                            text=True, capture_output=True, check=True)
    return {(item['pk']['S'], item['sk']['S']): item for item in json.loads(result.stdout)}
