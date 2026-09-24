#!/usr/bin/env python3
"""Isolated local API-to-provider PoC. Retains evidence and volumes; never resets offsets."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

from evidence import KafkaProbe, attempt_id, complete_input, delivery_id, read_items, read_manifest, reconcile

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / '.poc-tools'
RESULTS = ROOT / '.poc-results'
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
PORTS = {'KAFKA_HOST_PORT': '29092', 'REDIS_HOST_PORT': '26379', 'POSTGRES_HOST_PORT': '25432',
         'DYNAMODB_HOST_PORT': '28000', 'DELIVERY_API_PORT': '28080', 'MOCK_PROVIDER_PORT': '28090',
         'INGRESS_HTTP_PORT': '28081', 'DISPATCH_HTTP_PORT': '28082',
         'DELIVERY_API_METRICS_PORT': '29080', 'INGRESS_METRICS_PORT': '29081',
         'DISPATCH_METRICS_PORT': '29082', 'SIMULATOR_METRICS_PORT': '29090'}
MANAGEMENT = {'api': 29080, 'ingress': 29081, 'dispatch': 29082, 'simulator': 29090}
MODULES = {'api': 'event-api', 'ingress': 'delivery-ingress-worker', 'dispatch': 'dispatch-worker', 'simulator': 'external-api-simulator'}
POLICY = {'blocked': 'false', 'tpsEnabled': 'true', 'requestsPerSecond': '2000', 'burstCapacity': '2000',
          'quotaEnabled': 'true', 'monthlyLimit': '1000000'}


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def request(url, token=None, payload=None):
    headers = {'Authorization': 'Bearer ' + token} if token else {}
    if payload is not None: headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=json.dumps(payload).encode() if payload is not None else None, headers=headers)
    with HTTP.open(req, timeout=10) as response: return response.read().decode()


def metric(text, name, **tags):
    value = 0.0
    for line in text.splitlines():
        if not (line.startswith(name + '{') or line.startswith(name + ' ')): continue
        labels = dict(re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"\\]*)"', line))
        if all(labels.get(key) == expected for key, expected in tags.items()):
            value += float(line.rsplit(' ', 1)[1])
    return value


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try: process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


class Runner:
    def __init__(self, args):
        self.args = args
        self.run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
        self.directory = RESULTS / self.run_id
        self.directory.mkdir(parents=True)
        self.apps, self.files = {}, []
        self.probe = None
        self.load = None
        self.token = None
        self.policy_before = None
        self.compose_env = {**os.environ, **PORTS}
        self.compose = ['docker', 'compose', '-p', 'platform-poc', '-f', str(ROOT / 'compose.yml')]
        self.infra_started = False
        self.summaries = []
        self.awake = None
        self.api_url = 'http://localhost:28080'
        self.dynamo_url = 'http://localhost:28000'

    def docker(self, *args, input=None, timeout=180):
        return subprocess.check_output(self.compose + list(args), env=self.compose_env, cwd=ROOT,
                                       input=input, text=True, timeout=timeout)

    def initialize(self):
        if platform.system() == 'Darwin' and shutil.which('caffeinate'):
            # Only while this runner lives; no persistent system power setting is changed.
            self.awake = subprocess.Popen(['caffeinate', '-i', '-w', str(os.getpid())])
        if shutil.disk_usage(ROOT).free < 2 * 1024**3: raise RuntimeError('Less than 2 GiB free disk')
        version = subprocess.check_output([str(TOOLS / 'k6'), 'version'], text=True).strip()
        if not version.startswith('k6 v1.8.1 '): raise RuntimeError('Run setup.py to install pinned k6 v1.8.1')
        java_home = os.environ.get('JAVA_HOME', '')
        self.java = str(Path(java_home) / 'bin/java') if java_home else 'java'
        java_version = subprocess.check_output([self.java, '-XshowSettings:properties', '-version'], stderr=subprocess.STDOUT, text=True)
        if not re.search(r'java.specification.version = 21\b', java_version): raise RuntimeError('JAVA_HOME must point to JDK 21')
        jars = {app: ROOT / module / 'target' / f'{module}-1.0-SNAPSHOT.jar' for app, module in MODULES.items()}
        if not all(path.exists() for path in jars.values()): raise RuntimeError('Build all application JARs before running')
        for port in list(MANAGEMENT.values()) + [28080, 28090, 28081, 28082]:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', port))  # Do not replace or kill an existing app.
        dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)
        manifest = {'runId': self.run_id, 'gitCommit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                    'workingTreeDirty': bool(dirty), 'platform': platform.platform(), 'logicalCpus': os.cpu_count(),
                    'java': java_version.strip(), 'k6': version,
                    'pythonPackages': subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'], text=True).splitlines(),
                    'ports': PORTS, 'composeProject': 'platform-poc', 'heapPerApp': '-Xms128m -Xmx512m',
                    'simulatorDelayMillis': self.args.delay_ms, 'simulatorDeduplicate': False,
                    'workerConcurrency': self.args.worker_concurrency,
                    'ingressLingerMillis': self.args.ingress_linger_ms,
                    'workerAckMode': 'RECORD',
                    'suite': self.args.suite, 'jarsSha256': {app: hashlib.sha256(path.read_bytes()).hexdigest() for app, path in jars.items()},
                    'harnessSha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in Path(__file__).parent.glob('*') if path.is_file()}}
        if platform.system() == 'Darwin':
            manifest['hostMemoryBytes'] = int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'], text=True))
        write_json(self.directory / 'environment.json', manifest)
        print(f'RUN {self.run_id}: starting isolated infrastructure', flush=True)
        self.infra_started = True
        with (self.directory / 'infra-start.log').open('w') as log:
            subprocess.run(self.compose + ['up', '-d', '--wait', '--wait-timeout', '180'], env=self.compose_env,
                           cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=240)
        (self.directory / 'images.txt').write_text(self.docker('images'))
        self.docker('exec', '-T', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'delivery', '-d', 'delivery',
                    input=(ROOT / 'scripts/local-init.sql').read_text())
        self.tenant = int(self.docker('exec', '-T', 'postgres', 'psql', '-U', 'delivery', '-d', 'delivery', '-Atc',
                                    "SELECT id FROM users WHERE username='local-user'"))
        self.policy_key = f'request-control:{{user:{self.tenant}}}:policy'
        existing = self.docker('exec', '-T', 'redis', 'redis-cli', '--raw', 'HGETALL', self.policy_key).splitlines()
        self.policy_before = dict(zip(existing[::2], existing[1::2]))
        write_json(self.directory / 'policy.json', {'before': self.policy_before, 'during': POLICY, 'tenantId': self.tenant})
        self.docker('exec', '-T', 'redis', 'redis-cli', 'HSET', self.policy_key,
                    *[item for pair in POLICY.items() for item in pair])
        # Explicit environment excludes accidental application/config/K6 overrides from the caller.
        base = {key: os.environ[key] for key in ('PATH', 'HOME', 'TMPDIR', 'LANG') if key in os.environ}
        base.update(PORTS)
        base.update({'SPRING_PROFILES_ACTIVE': 'dev', 'KAFKA_BOOTSTRAP_SERVERS': 'localhost:29092',
                     'REDIS_HOST': 'localhost', 'REDIS_PORT': '26379', 'REDIS_DATABASE': '0',
                     'DB_URL': 'jdbc:postgresql://localhost:25432/delivery', 'DB_USERNAME': 'delivery', 'DB_PASSWORD': 'delivery',
                     'DYNAMODB_ENDPOINT': 'http://localhost:28000', 'EXTERNAL_API_BASE_URL': 'http://localhost:28090',
                     'SIMULATOR_TCP_ENABLED': 'false', 'SIMULATOR_DEDUPLICATE': 'false', 'SIMULATOR_RESPONSE_DELAY_MILLIS': str(self.args.delay_ms),
                     'SIMULATOR_MAX_TRACKED_KEYS': '100000',
                     'INGRESS_CONCURRENCY': str(self.args.worker_concurrency),
                     'DISPATCH_CONCURRENCY': str(self.args.worker_concurrency),
                     'INGRESS_KAFKA_LINGER_MS': str(self.args.ingress_linger_ms),
                     'JWT_SECRET': 'bG9jYWwtZGV2ZWxvcG1lbnQtb25seS1zZWNyZXQtYXQtbGVhc3QtMzItYnl0ZXMtbG9uZw=='})
        for app in ('simulator', 'ingress', 'dispatch', 'api'):
            log = (self.directory / f'{app}.log').open('w')
            self.files.append(log)
            port = {'api': 28080, 'simulator': 28090, 'ingress': 28081, 'dispatch': 28082}[app]
            self.apps[app] = subprocess.Popen([self.java, '-Xms128m', '-Xmx512m', '-jar', str(jars[app]),
                                               f'--server.port={port}'], env=base, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            self.assert_apps()
            try:
                self.token = json.loads(request('http://localhost:28080/api/v1/auth/token',
                    payload={'username': 'local-user', 'password': 'local-password'}))['data']['accessToken']
                for app in MANAGEMENT:
                    request(f'http://127.0.0.1:{MANAGEMENT[app]}/actuator/prometheus', self.token if app == 'api' else None)
                self.probe = KafkaProbe('localhost:29092')
                # A web health endpoint alone does not prove Kafka assignment.
                if all((self.directory / f'{app}.log').read_text().count('partitions assigned:')
                       >= self.args.worker_concurrency for app in ('ingress', 'dispatch')):
                    break
                self.probe.close()
                self.probe = None
            except (OSError, ValueError, RuntimeError): pass
            time.sleep(1)
        else: raise RuntimeError('Applications or consumer assignments not ready; see app logs')
        if self.probe.snapshot()['lag'] != 0:
            raise RuntimeError('Pre-existing pending work in the PoC cluster; preserve and resolve before a new run')
        summary = json.loads(request('http://127.0.0.1:29090/actuator/simulator'))
        if summary['deduplicate'] or summary['calls'] != 0: raise RuntimeError('Simulator not empty or deduplication enabled')
        print('READY: authenticated API, consumers assigned, zero backlog, provider deduplication off', flush=True)

    def assert_apps(self):
        for app, process in self.apps.items():
            if process.poll() is not None: raise RuntimeError(f'{app} exited unexpectedly')

    def scrape_metrics(self):
        def scrape(app):
            return request(f'http://127.0.0.1:{MANAGEMENT[app]}/actuator/prometheus', self.token if app == 'api' else None)
        with ThreadPoolExecutor(max_workers=4) as pool:
            return dict(zip(MANAGEMENT, pool.map(scrape, MANAGEMENT)))

    def provider_counts(self, deliveries):
        def counts(delivery):
            attempt = attempt_id(delivery)
            try: value = json.loads(request('http://127.0.0.1:29090/actuator/simulator/' + attempt))
            except urllib.error.HTTPError as error:
                if error.code != 404: raise
                value = {'calls': 0, 'effects': 0}
            return attempt, value
        with ThreadPoolExecutor(max_workers=8) as pool:
            return dict(pool.map(counts, deliveries))

    def snapshot(self, directory, index):
        sample_started = time.time()
        self.assert_apps()
        if shutil.disk_usage(ROOT).free < 1024**3:
            raise RuntimeError('Less than 1 GiB free disk; retain evidence and stop load')
        texts = self.scrape_metrics()
        metrics_time = time.time()
        for app, text in texts.items():
            with gzip.open(directory / f'{index:04}-{app}.prom.gz', 'wt') as output: output.write(text)
        kafka = self.probe.snapshot()
        kafka['metricsTime'] = metrics_time
        kafka['persistedAcceptanceCounter'] = metric(texts['dispatch'], 'delivery_outcomes_total', outcome='dispatch_accepted')
        pids = [os.getpid(), *[process.pid for process in self.apps.values()]]
        if self.load and self.load.poll() is None: pids.append(self.load.pid)
        resources = {'time': time.time(), 'roles': {app: process.pid for app, process in self.apps.items()},
                     'collectorPid': os.getpid(), 'k6Pid': self.load.pid if self.load else None,
                     'ps': subprocess.check_output(['ps', '-p', ','.join(map(str, pids)), '-o', 'pid=,pcpu=,rss=,vsz='],
                                                   text=True, timeout=5)}
        if index % 2 == 0:
            containers = self.docker('ps', '-q').splitlines()
            resources['dockerStats'] = [json.loads(line) for line in subprocess.check_output(
                ['docker', 'stats', '--no-stream', '--format', '{{json .}}', *containers], text=True, timeout=15).splitlines()]
        write_json(directory / f'{index:04}-resources.json', resources)
        with (directory / 'samples.jsonl').open('a') as output: output.write(json.dumps(kafka) + '\n')
        if time.time() - sample_started > 30:
            raise RuntimeError('Sampling paused for over 30 seconds; environment discontinuity invalidates the run')
        return kafka, texts

    def phase(self, label, rate, seconds, mode='unique'):
        directory = self.directory / label
        directory.mkdir()
        print(f'PHASE {label}: {rate} requests/s for {seconds}s, mode={mode}', flush=True)
        before, initial_metrics = self.snapshot(directory, 0)
        if before['lag']: raise RuntimeError('Previous phase has backlog')
        env = {key: os.environ[key] for key in ('PATH', 'HOME', 'TMPDIR', 'LANG') if key in os.environ}
        env.update({'POC_RATE': str(rate), 'POC_SECONDS': str(seconds), 'POC_RUN_ID': self.run_id + '-' + label,
                    'POC_MODE': mode, 'POC_TOKEN': self.token, 'POC_API': self.api_url,
                    'POC_SUMMARY': str(directory / 'k6-summary.json'), 'K6_NO_USAGE_REPORT': 'true'})
        start = time.monotonic()
        start_wall = time.time()
        log = (directory / 'k6.log').open('w')
        self.files.append(log)
        self.load = subprocess.Popen([str(TOOLS / 'k6'), 'run', '--quiet', '--no-usage-report', '--log-format', 'raw',
                    '--console-output', str(directory / 'requests.jsonl'), str(ROOT / 'scripts/poc/load.js')],
                    cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        samples, index, aborted = [], 1, None
        next_sample = time.monotonic()
        try:
            while self.load.poll() is None:
                if abs((time.time() - start_wall) - (time.monotonic() - start)) > 5:
                    raise RuntimeError('Wall clock/monotonic clock discontinuity; measurement invalid')
                if time.monotonic() >= next_sample:
                    snapshot, texts = self.snapshot(directory, index)
                    index += 1
                    samples.append(snapshot)
                    for app, original in initial_metrics.items():
                        if metric(original, 'process_start_time_seconds') != metric(texts[app], 'process_start_time_seconds'):
                            raise RuntimeError(f'{app} restarted during measurement')
                    for outcome in ('dispatch_review', 'ingress_dlt', 'ingress_dlt_failed'):
                        app = 'dispatch' if outcome.startswith('dispatch') else 'ingress'
                        if metric(texts[app], 'delivery_outcomes_total', outcome=outcome) > metric(initial_metrics[app], 'delivery_outcomes_total', outcome=outcome):
                            raise RuntimeError('Review or DLT observed; stop increasing load')
                    if len(samples) >= 6 and snapshot['lag'] > max(100, rate * 5):
                        if all(a['lag'] < b['lag'] for a, b in zip(samples[-6:], samples[-5:])):
                            raise RuntimeError('Sustained increasing backlog for six samples')
                    next_sample = time.monotonic() + 5
                if time.monotonic() - start > seconds + 60: raise RuntimeError('Load generator exceeded time budget')
                time.sleep(0.1)
        except Exception as error:
            aborted = str(error)
            stop(self.load)
        return_code = self.load.wait()
        load_finished = time.monotonic()
        deadline = load_finished + self.args.drain_seconds
        stable = 0
        while time.monotonic() < deadline:
            after, final_metrics = self.snapshot(directory, index)
            index += 1
            samples.append(after)
            stable = stable + 1 if after['lag'] == 0 else 0
            if stable >= 2: break
            time.sleep(1)
        drained = stable >= 2
        completed = time.monotonic()
        records = self.probe.records(before, after)
        with (directory / 'kafka-records.jsonl').open('w') as output:
            for row in records: output.write(json.dumps(row) + '\n')
        starts, results = read_manifest(directory / 'requests.jsonl')
        deliveries = sorted({delivery_id(self.tenant, row['key']) for row in starts.values()})
        items = read_items(self.dynamo_url, deliveries)
        write_json(directory / 'db-items.json', list(items.values()))
        provider = self.provider_counts(deliveries)
        rows, summary = reconcile(starts, results, self.tenant, records, items, provider)
        with (directory / 'reconciliation.jsonl').open('w') as output:
            for row in rows: output.write(json.dumps(row) + '\n')
        k6 = json.loads((directory / 'k6-summary.json').read_text())
        dropped = k6['metrics'].get('dropped_iterations', {}).get('values', {}).get('count', 0)
        iterations = k6['metrics'].get('iterations', {}).get('values', {}).get('count', 0)
        delta = metric(final_metrics['dispatch'], 'delivery_outcomes_total', outcome='dispatch_accepted') - metric(initial_metrics['dispatch'], 'delivery_outcomes_total', outcome='dispatch_accepted')
        summary.update({'label': label, 'rate': rate, 'seconds': seconds, 'mode': mode,
                        'plannedRequests': rate * seconds,
                        'k6ExitCode': return_code, 'droppedIterations': dropped, 'completedIterations': iterations,
                        'aborted': aborted, 'drained': drained, 'maxLag': max(r['lag'] for r in samples),
                        'oldestUncommittedAgeSeconds': max(r['oldestUncommittedAgeSeconds'] for r in samples),
                        'drainObservationSeconds': completed - load_finished,
                        'completionWindowSeconds': completed - start,
                        'metricsWindowSeconds': after['metricsTime'] - before['metricsTime'],
                        'persistedAcceptancesPerWindowSecond': delta / (after['metricsTime'] - before['metricsTime']),
                        'httpDurationMs': k6['metrics'].get('http_req_duration', {}).get('values', {}),
                        'bodyBytes': len(json.dumps({'deliveryType': 'EMAIL', 'payload': {'message': 'x' * 960}}, separators=(',', ':')).encode()),
                        'pass': summary['consistent'] and drained and return_code == 0 and not dropped and not aborted
                                and complete_input(rate * seconds, len(starts), len(results), iterations, dropped)})
        write_json(directory / 'result.json', summary)
        self.summaries.append(summary)
        write_json(self.directory / 'results.json', self.summaries)
        print(f"RESULT {label}: pass={summary['pass']} submitted={summary['submitted']} unique={summary['uniqueRequests']} "
              f"states={summary['states']} problems={summary['problemRequests']} maxLag={summary['maxLag']} "
              f"httpP95={summary['httpDurationMs'].get('p(95)')}ms", flush=True)
        if not summary['pass']: raise RuntimeError(f'{label} did not meet acceptance criteria; later phases stopped')

    def close(self):
        stop(self.load)
        if self.probe: self.probe.close()
        for process in reversed(list(self.apps.values())): stop(process)
        for stream in self.files: stream.close()
        if self.policy_before is not None:
            try:
                for field in POLICY:
                    if field in self.policy_before:
                        self.docker('exec', '-T', 'redis', 'redis-cli', 'HSET', self.policy_key, field, self.policy_before[field])
                    else:
                        self.docker('exec', '-T', 'redis', 'redis-cli', 'HDEL', self.policy_key, field)
                write_json(self.directory / 'policy-restored.json', {'restored': True, 'usageCountersPreserved': True})
            except Exception as error:
                write_json(self.directory / 'policy-restored.json', {'restored': False, 'error': str(error)})
                print('Policy restoration failed; inspect policy-restored.json', file=sys.stderr)
        if self.infra_started:
            try: self.docker('stop', timeout=90)
            except Exception as error: print(f'PoC infrastructure stop failed: {error}', file=sys.stderr)
        stop(self.awake)


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', choices=['smoke', 'baseline', 'comparison'], default='smoke')
    parser.add_argument('--worker-concurrency', type=int, choices=[1, 2, 3], default=1)
    parser.add_argument('--ingress-linger-ms', type=int, choices=[0, 5], default=5)
    parser.add_argument('--delay-ms', type=int, default=0)
    parser.add_argument('--drain-seconds', type=int, default=300)
    args = parser.parse_args()
    if not 0 <= args.delay_ms <= 60000 or not 2 <= args.drain_seconds <= 300: parser.error('Invalid delay/drain limit')
    RESULTS.mkdir(exist_ok=True)
    with (RESULTS / 'runner.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        runner = Runner(args)
        try:
            runner.initialize()
            runner.phase('smoke-unique', 5, 2)
            runner.phase('smoke-duplicate', 5, 2, 'duplicate')
            if args.suite == 'baseline':
                runner.phase('warmup', 10, 60)
                for rate in (10, 50, 100): runner.phase(f'baseline-{rate}', rate, 180)
            elif args.suite == 'comparison':
                runner.phase('warmup', 10, 60)
                runner.phase('duplicate-load', 100, 20, 'duplicate')
                for rate in (50, 100): runner.phase(f'comparison-{rate}', rate, 180)
            print(f'COMPLETE: {runner.directory}', flush=True)
        except BaseException as error:
            write_json(runner.directory / 'failure.json', {'type': type(error).__name__, 'message': str(error)})
            print(f'FAILED: {error}; evidence={runner.directory}', file=sys.stderr, flush=True)
            raise
        finally:
            runner.close()


if __name__ == '__main__':
    main()
