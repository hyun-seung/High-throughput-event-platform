"""Read-only Kafka observations and a JWT-authenticated API metrics relay.
No DynamoDB scan, no consumer subscription/commit, no request body logging.
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, build_opener, ProxyHandler
from urllib.error import HTTPError
from evidence import KafkaProbe, GROUPS

HTTP = build_opener(ProxyHandler({}))
LOCK = threading.Lock()
snapshot = None
last_success = 0
healthy = False
api_lock = threading.Lock()
token = None


def get(url, headers=None, payload=None):
    req = Request(url, headers=headers or {}, data=json.dumps(payload).encode() if payload is not None else None)
    with HTTP.open(req, timeout=5) as response:
        return response.read()


def api_metrics():
    global token
    with api_lock:
        for attempt in range(2):
            if token is None:
                auth = get('http://api:8080/api/v1/auth/token', {'Content-Type': 'application/json'},
                           {'username': 'local-user', 'password': 'local-password'})
                token = json.loads(auth)['data']['accessToken']
            try:
                return get('http://api:19080/actuator/prometheus', {'Authorization': 'Bearer ' + token})
            except HTTPError as error:
                if error.code != 401 or attempt: raise
                token = None
        raise RuntimeError('Authentication unavailable')


def kafka_loop():
    global snapshot, last_success, healthy
    probe = None
    while True:
        started = time.monotonic()
        try:
            if probe is None: probe = KafkaProbe(os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'))
            current = probe.snapshot()
            with LOCK:
                snapshot, last_success, healthy = current, time.time(), True
        except Exception:
            with LOCK: healthy = False
            if probe:
                probe.close()
                probe = None
        time.sleep(max(1, 10 - (time.monotonic() - started)))


def exposition(current, success_time, success, now):
    fresh = success and now - success_time < 45
    lines = ['# TYPE platform_kafka_probe_up gauge', f'platform_kafka_probe_up {int(fresh)}',
             '# TYPE platform_kafka_probe_last_success_timestamp_seconds gauge',
             f'platform_kafka_probe_last_success_timestamp_seconds {success_time}']
    if not fresh or current is None:
        return '\n'.join(lines) + '\n'
    for row in current['partitions']:
        labels = f'topic="{row["topic"]}",partition="{row["partition"]}"'
        lines.append(f'platform_kafka_log_end_offset{{{labels}}} {row["end"]}')
        lines.append(f'platform_kafka_retained_records{{{labels}}} {row["end"]-row["start"]}')
        if row['lag'] is not None:
            labels += f',group="{GROUPS[row["topic"]]}"'
            lines.append(f'platform_kafka_committed_lag{{{labels}}} {row["lag"]}')
            lines.append(f'platform_kafka_oldest_uncommitted_age_seconds{{{labels}}} {row["oldestUncommittedAgeSeconds"] or 0}')
    return '\n'.join(lines) + '\n'


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if self.path == '/metrics/api':
                body = api_metrics()
            elif self.path == '/metrics':
                with LOCK: body = exposition(snapshot, last_success, healthy, time.time()).encode()
            elif self.path == '/health': body = b'collector running\n'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            self.send_error(503, 'Target unavailable')

    def log_message(self, *_): pass


if __name__ == '__main__':
    threading.Thread(target=kafka_loop, daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', 9800), Handler).serve_forever()
