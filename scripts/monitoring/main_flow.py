#!/usr/bin/env python3
"""Generate and verify traffic in the isolated main messaging monitoring stack."""

import argparse
import concurrent.futures
import json
import math
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "platform-messaging-monitoring", "-f", "compose.yml",
           "-f", "monitoring/compose.yml"]
API = "http://127.0.0.1:38080"
DEMO_JOBS = {"api", "pre-send", "skt-sender", "webhook-receive", "result-manager",
             "complete-manager", "webhook-sender"}


def request_json(url: str, payload: object | None = None, token: str | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, data=None if payload is None else json.dumps(payload).encode(), headers=headers)
    with urlopen(request, timeout=20) as response:
        return json.load(response)


def compose(*args: str) -> str:
    result = subprocess.run([*COMPOSE, *args], cwd=ROOT, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"Compose command failed: {result.stderr[-1000:]}")
    return result.stdout.strip()


def sql(statement: str) -> str:
    return compose("exec", "-T", "postgres", "psql", "-U", "delivery", "-d", "delivery",
                   "-v", "ON_ERROR_STOP=1", "-Atc", statement)


def wait_for_demo_services(timeout: int) -> None:
    deadline = time.monotonic() + timeout
    pending = DEMO_JOBS
    while time.monotonic() < deadline:
        try:
            response = request_json("http://127.0.0.1:19099/api/v1/query?" + urlencode({"query": "up"}))
            ready = {item["metric"]["job"] for item in response["data"]["result"]
                     if float(item["value"][1]) == 1}
            pending = DEMO_JOBS - ready
            if not pending:
                tables = sql("SELECT to_regclass('messaging_completion.tbl_msg_hist') IS NOT NULL "
                             "AND to_regclass('messaging_webhook.tbl_webhook_outbox') IS NOT NULL")
                if tables == "t":
                    return
                pending = {"database-migrations"}
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"Main flow APs did not become ready: {', '.join(sorted(pending))}")


def wait_for_job(job: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    query = "http://127.0.0.1:19099/api/v1/query?" + urlencode({"query": f'up{{job="{job}"}}'})
    while time.monotonic() < deadline:
        try:
            response = request_json(query)
            if any(float(item["value"][1]) == 1 for item in response["data"]["result"]):
                print(f"Ready: {job}")
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"Prometheus target did not become ready: {job}")


def submit(token: str, run_id: str, number: int, failure: bool) -> tuple[str, float, bool]:
    message_id = f"monitor-{run_id}-{number:05d}"
    body = {"messageId": message_id, "recipientNumber": "010" + f"{number % 100000000:08d}",
            "messageCategory": "GENERAL", "payload": {"text": "monitor-force-failure" if failure
                                                     else "monitoring main flow"}}
    start = time.monotonic()
    answer = request_json(API + "/api/v1/messages", body, token)
    return answer["data"]["clientMsgId"], (time.monotonic() - start) * 1000, failure


def demo(args: argparse.Namespace) -> None:
    if not 1 <= args.rate <= 100 or not 1 <= args.seconds <= 120:
        raise ValueError("--rate must be 1..100 and --seconds 1..120")
    wait_for_demo_services(args.startup_timeout)
    token = request_json(API + "/api/v1/auth/token",
                         {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
    run_id = uuid.uuid4().hex[:8]
    total = args.rate * args.seconds
    start = time.monotonic()
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(args.rate, 32)) as pool:
        for number in range(total):
            due = start + number / args.rate
            if due > time.monotonic():
                time.sleep(due - time.monotonic())
            futures.append(pool.submit(submit, token, run_id, number, False))
        if args.errors:
            futures.append(pool.submit(submit, token, run_id, total, True))
        receipts = [future.result() for future in futures]

    expected = {client_msg_id: failure for client_msg_id, _, failure in receipts}
    deadline = time.monotonic() + args.timeout
    rows = {}
    delivered = 0
    while time.monotonic() < deadline:
        raw = sql("SELECT client_msg_id || '|' || outcome || '|' || cleanup_status "
                  "FROM messaging_completion.tbl_msg_hist "
                  f"WHERE message_id LIKE 'monitor-{run_id}-%'")
        rows = {pieces[0]: pieces[1:] for line in raw.splitlines()
                if len(pieces := line.split("|")) == 3}
        delivered = int(sql("SELECT count(*) FROM messaging_webhook.tbl_webhook_outbox "
                            "WHERE status='DELIVERED' AND client_msg_id IN "
                            "(SELECT client_msg_id FROM messaging_completion.tbl_msg_hist "
                            f"WHERE message_id LIKE 'monitor-{run_id}-%')") or "0")
        if all(rows.get(client_msg_id) == ["FAILURE" if failure else "SUCCESS", "DONE"]
               for client_msg_id, failure in expected.items()) and delivered == len(expected):
            break
        time.sleep(1)
    else:
        raise TimeoutError(f"Main flow did not finish: finalized={len(rows)}/{len(expected)}, "
                           f"customerWebhooks={delivered}/{len(expected)}")

    billable = int(sql("SELECT count(*) FROM messaging_completion.tbl_cdr_hist "
                       f"WHERE message_id LIKE 'monitor-{run_id}-%'") or "0")
    if billable != total:
        raise AssertionError(f"Expected {total} primary billable rows; got {billable}")
    times = sorted(latency for _, latency, _ in receipts)
    report = {"runId": run_id, "admitted": len(receipts), "primarySuccess": total,
              "primaryFailure": int(args.errors), "billable": billable,
              "customerWebhooksDelivered": delivered,
              "admissionP95Ms": round(times[math.ceil(len(times) * 0.95) - 1], 2),
              "exampleClientMsgId": receipts[0][0]}
    print(json.dumps(report, ensure_ascii=False, indent=2))


def verify() -> None:
    jobs = {"api", "pre-send", "skt-sender", "kt-sender", "lgu-sender", "webhook-receive",
            "result-manager", "complete-manager", "webhook-sender", "tcp-sender",
            "kafka-observer", "redis", "postgres", "prometheus", "loki", "alloy"}
    response = request_json("http://127.0.0.1:19099/api/v1/query?" + urlencode({"query": "up"}))
    observed = {item["metric"]["job"]: float(item["value"][1])
                for item in response["data"]["result"]}
    errors = [f"{job}: Prometheus target absent/down" for job in sorted(jobs)
              if observed.get(job) != 1]
    probe = request_json("http://127.0.0.1:19099/api/v1/query?" +
                         urlencode({"query": "platform_kafka_probe_up"}))
    if not any(float(item["value"][1]) == 1 for item in probe["data"]["result"]):
        errors.append("Kafka offset probe is unavailable")
    boards = ["overview", "services", "providers", "customers", "errors", "infra", "trace"]
    for name in boards:
        try:
            request_json(f"http://127.0.0.1:13000/api/dashboards/uid/messaging-{name}")
        except Exception as failure:
            errors.append(f"Grafana dashboard {name}: {failure}")
    log_found = False
    for _ in range(15):
        try:
            log_query = request_json("http://127.0.0.1:13000/api/datasources/proxy/uid/platform-loki/loki/api/v1/query_range?" +
                                     urlencode({"query": '{service="api"}', "limit": 1, "since": "1h"}))
            if log_query["data"]["result"]:
                log_found = True
                break
        except Exception:
            pass
        time.sleep(2)
    if not log_found:
        errors.append("API structured log was not found in Loki")
    report = {"targets": observed, "dashboards": boards, "errors": errors, "pass": not errors}
    destination = ROOT / ".monitoring/verification.json"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Monitoring verification {'PASS' if not errors else 'FAIL'}: {destination}")
    if errors:
        raise AssertionError("; ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    demo_parser = subcommands.add_parser("demo")
    demo_parser.add_argument("--rate", type=int, default=1)
    demo_parser.add_argument("--seconds", type=int, default=1)
    demo_parser.add_argument("--errors", action="store_true")
    demo_parser.add_argument("--timeout", type=int, default=180)
    demo_parser.add_argument("--startup-timeout", type=int, default=300)
    subcommands.add_parser("verify")
    wait_parser = subcommands.add_parser("wait-job")
    wait_parser.add_argument("--job", required=True)
    wait_parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if args.command == "demo":
        demo(args)
    elif args.command == "verify":
        verify()
    else:
        wait_for_job(args.job, args.timeout)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Monitoring {sys.argv[1]} failed: {error}", file=sys.stderr)
        sys.exit(1)
