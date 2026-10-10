#!/usr/bin/env python3
"""Generate and verify traffic in the isolated main messaging monitoring stack."""

import argparse
import concurrent.futures
import json
import math
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "platform-messaging-monitoring", "-f", "compose.yml",
           "-f", "monitoring/compose.yml"]
API = "http://127.0.0.1:38080"
AP_HEALTH_ENDPOINTS = {"api": 19080, "pre-send": 19093, "publication-recovery": 19098,
                       "skt-sender": 19091, "kt-sender": 19091, "lgu-sender": 19091,
                       "webhook-receive": 19089, "result-manager": 19094, "complete-manager": 19095,
                       "webhook-sender": 19096, "tcp-sender": 19097}
DEMO_JOBS = {"api", "pre-send", "skt-sender", "webhook-receive", "result-manager",
             "complete-manager", "webhook-sender"}


def request_json(url: str, payload: object | None = None, token: str | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, data=None if payload is None else json.dumps(payload).encode(), headers=headers)
    with urlopen(request, timeout=20) as response:
        return json.load(response)


def compose(*args: str, override: Path | None = None, timeout: int = 30) -> str:
    command = [*COMPOSE, *([] if override is None else ["-f", str(override)]), *args]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
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


def submit(token: str, run_id: str, number: int, failure: bool,
           secondary: bool = False) -> tuple[str, float, str]:
    message_id = f"monitor-{run_id}-{number:05d}"
    body = {"messageId": message_id, "recipientNumber": "010" + f"{number % 100000000:08d}",
            "messageCategory": "GENERAL", "payload": {"text": "monitor-force-failure" if failure
                                                     else "monitoring main flow"}}
    if secondary:
        body["secondarySendPayload"] = {"text": "monitoring TCP fallback"}
    start = time.monotonic()
    answer = request_json(API + "/api/v1/messages", body, token)
    case = "secondary-success" if secondary else "primary-failure" if failure else "primary-success"
    return answer["data"]["clientMsgId"], (time.monotonic() - start) * 1000, case


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
        if args.secondary:
            futures.append(pool.submit(submit, token, run_id, total + int(args.errors), True, True))
        receipts = [future.result() for future in futures]

    expected = {client_msg_id: case for client_msg_id, _, case in receipts}
    destination = ROOT / ".monitoring/demo-latest.json"
    destination.parent.mkdir(exist_ok=True)
    report = {"runId": run_id, "admitted": len(receipts), "cases": expected, "pass": False}
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    deadline = time.monotonic() + args.timeout
    rows = {}
    delivered = 0
    while time.monotonic() < deadline:
        raw = sql("SELECT client_msg_id || '|' || outcome || '|' || cleanup_status || '|' || final_stage "
                  "FROM messaging_completion.tbl_msg_hist "
                  f"WHERE message_id LIKE 'monitor-{run_id}-%'")
        rows = {pieces[0]: pieces[1:] for line in raw.splitlines()
                if len(pieces := line.split("|")) == 4}
        delivered = int(sql("SELECT count(*) FROM messaging_webhook.tbl_webhook_outbox o "
                            "WHERE status='DELIVERED' AND client_msg_id IN "
                            "(SELECT client_msg_id FROM messaging_completion.tbl_msg_hist "
                            f"WHERE message_id LIKE 'monitor-{run_id}-%') "
                            "AND EXISTS (SELECT 1 FROM messaging_webhook.tbl_webhook_hist h "
                            "WHERE h.batch_id=o.batch_id AND h.acknowledged)") or "0")
        if all(rows.get(client_msg_id) == ["FAILURE" if case == "primary-failure" else "SUCCESS", "DONE",
                                          "SECONDARY" if case == "secondary-success" else "PRIMARY"]
               for client_msg_id, case in expected.items()) and delivered == len(expected):
            break
        time.sleep(1)
    else:
        raise TimeoutError(f"Main flow did not finish: finalized={len(rows)}/{len(expected)}, "
                           f"customerWebhooks={delivered}/{len(expected)}")

    billed_ids = set(sql("SELECT client_msg_id FROM messaging_completion.tbl_cdr_hist "
                         f"WHERE message_id LIKE 'monitor-{run_id}-%'").splitlines())
    primary_success_ids = {identifier for identifier, case in expected.items() if case == "primary-success"}
    if billed_ids != primary_success_ids:
        raise AssertionError("Billable message IDs do not match successful primary sends")
    billable = len(billed_ids)
    times = sorted(latency for _, latency, _ in receipts)
    report.update({"primarySuccess": total, "primaryFailure": int(args.errors) + int(args.secondary),
              "secondarySuccess": int(args.secondary), "billable": billable,
              "customerWebhooksDelivered": delivered,
              "admissionP95Ms": round(times[math.ceil(len(times) * 0.95) - 1], 2),
              "exampleClientMsgId": receipts[0][0], "pass": True})
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def verify() -> None:
    jobs = {"api", "pre-send", "skt-sender", "kt-sender", "lgu-sender", "webhook-receive",
            "result-manager", "complete-manager", "webhook-sender", "tcp-sender", "publication-recovery",
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
    scheduled_work = {}
    scheduled_panels = []
    try:
        counts = request_json("http://127.0.0.1:19099/api/v1/query?" + urlencode({
            "query": "messaging_scheduled_work_duration_seconds_count"}))
        scheduled_work = {item["metric"]["worker"] + "/" + item["metric"]["phase"]: float(item["value"][1])
                          for item in counts["data"]["result"]}
        expected = {
            "result_inbox": ("poll", "query", "item"),
            "result_outbox": ("poll", "query", "item", "handoff"),
            "complete_cleanup": ("poll", "query", "item", "load", "step_query", "ttl", "delete")}
        for worker, phases in expected.items():
            for phase in phases:
                if worker + "/" + phase not in scheduled_work:
                    errors.append(f"Scheduled work metric absent: {worker}/{phase}")
            if scheduled_work.get(worker + "/poll", 0) <= 0:
                errors.append(f"Scheduled worker has not completed a poll: {worker}")
        board = request_json("http://127.0.0.1:13000/api/dashboards/uid/messaging-services")
        for panel in board["dashboard"]["panels"]:
            expressions = [target["expr"] for target in panel.get("targets", [])
                           if "messaging_scheduled_work_" in target.get("expr", "")]
            if not expressions:
                continue
            for expression in expressions:
                result = request_json("http://127.0.0.1:19099/api/v1/query?" + urlencode({"query": expression}))
                if result.get("status") != "success":
                    errors.append(f"Scheduled work panel query failed: {panel['title']}")
            scheduled_panels.append(panel["title"])
        if len(scheduled_panels) != 6:
            errors.append(f"Expected six scheduled work panels, found {len(scheduled_panels)}")
    except Exception as failure:
        errors.append(f"Scheduled work monitoring: {failure}")
    log_evidence = {}
    for service in ("api", "publication-recovery"):
        log_found = False
        for _ in range(15):
            try:
                log_query = request_json("http://127.0.0.1:13000/api/datasources/proxy/uid/platform-loki/loki/api/v1/query_range?" +
                                         urlencode({"query": '{service="' + service + '"}', "limit": 1,
                                                    "since": "24h", "direction": "backward"}))
                if log_query["data"]["result"]:
                    timestamps = [int(value[0]) / 1e9 for stream in log_query["data"]["result"]
                                  for value in stream["values"]]
                    age = max(0, time.time() - max(timestamps))
                    log_evidence[service] = {"lookbackHours": 24, "lastLogAgeSeconds": round(age, 1),
                                             "seenWithinLastHour": age <= 3600}
                    log_found = True
                    break
            except Exception:
                pass
            time.sleep(2)
        if not log_found:
            errors.append(f"{service} structured log was not found in Loki within 24 hours")
    health = {}
    try:
        health = application_health()
        for service in AP_HEALTH_ENDPOINTS:
            if health.get(service) != "UP":
                errors.append(f"{service} application health: {health.get(service, 'missing')}")
    except Exception as failure:
        errors.append(f"Application health check unavailable: {failure}")
    report = {"targets": observed, "applicationHealth": health, "dashboards": boards,
              "logEvidence": log_evidence, "scheduledWork": scheduled_work,
              "scheduledWorkPanels": scheduled_panels, "errors": errors, "pass": not errors}
    destination = ROOT / ".monitoring/verification.json"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Monitoring verification {'PASS' if not errors else 'FAIL'}: {destination}")
    if errors:
        raise AssertionError("; ".join(errors))


def application_health() -> dict:
    code = """
import concurrent.futures, json, sys
from urllib.request import Request, urlopen
def check(entry):
    name, port = entry
    try:
        headers = {}
        if name == 'api':
            login = Request('http://api:8080/api/v1/auth/token',
                data=json.dumps({'username':'local-user','password':'local-password'}).encode(),
                headers={'Content-Type':'application/json'})
            with urlopen(login, timeout=5) as response:
                headers['Authorization'] = 'Bearer ' + json.load(response)['data']['accessToken']
        request = Request(f'http://{name}:{port}/actuator/health', headers=headers)
        with urlopen(request, timeout=5) as response:
            return name, json.load(response)['status']
    except Exception as failure:
        return name, str(failure)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    print(json.dumps(dict(pool.map(check, json.loads(sys.argv[1]).items()))))
"""
    return json.loads(compose("exec", "-T", "mock", "python", "-c", code, json.dumps(AP_HEALTH_ENDPOINTS)))


def diagnose() -> None:
    def docker(*args: str) -> str:
        result = subprocess.run(["docker", *args], cwd=ROOT, capture_output=True,
                                text=True, timeout=30, check=True)
        return result.stdout.strip()

    info = json.loads(docker("info", "--format", '{{json .}}'))
    containers = docker("ps", "--format",
                        '{{.Names}}|{{.Label "com.docker.compose.project"}}|'
                        '{{.Label "com.docker.compose.project.working_dir"}}')
    duplicates = []
    for line in containers.splitlines():
        name, project, directory = line.split("|", 2)
        if directory == str(ROOT) and project != "platform-messaging-monitoring":
            duplicates.append({"container": name, "project": project})
    report = {"dockerCpus": info["NCPU"], "dockerMemoryBytes": info["MemTotal"],
              "runningContainers": info["ContainersRunning"],
              "otherContainersFromThisRepository": duplicates}
    try:
        report["vmPressure"] = compose("exec", "-T", "redis", "sh", "-c",
            "cat /proc/loadavg /proc/pressure/cpu /proc/pressure/memory /proc/pressure/io; "
            "grep -E 'MemAvailable|SwapTotal|SwapFree' /proc/meminfo; "
            "grep -E 'pswp|pgmajfault' /proc/vmstat")
    except (subprocess.SubprocessError, RuntimeError) as failure:
        report["vmPressureError"] = str(failure)
    started = time.monotonic()
    try:
        targets = request_json("http://127.0.0.1:19099/api/v1/query?query=up")
        report["targets"] = {item["metric"]["job"]: float(item["value"][1])
                             for item in targets["data"]["result"]}
    except Exception as failure:
        report["prometheusError"] = str(failure)
    report["prometheusElapsedMs"] = round((time.monotonic() - started) * 1000, 2)
    destination = ROOT / ".monitoring/diagnostics.json"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def origin_item(client_msg_id: str) -> dict:
    payload = {"TableName": "ORIGIN", "Key": {"pk": {"S": "DELIVERY#" + client_msg_id},
               "sk": {"S": "META"}}, "ConsistentRead": True}
    response = subprocess.run(["curl", "--silent", "--show-error", "--fail-with-body", "--max-time", "10",
        "--aws-sigv4", "aws:amz:ap-northeast-2:dynamodb", "--user", "local:local",
        "-H", "X-Amz-Target: DynamoDB_20120810.GetItem", "-H", "Content-Type: application/x-amz-json-1.0",
        "--data", json.dumps(payload), "http://127.0.0.1:38000/"],
        capture_output=True, text=True, check=True, timeout=15)
    return json.loads(response.stdout).get("Item", {})


def wait_for_api(timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if request_json(API + "/readyz").get("status") == "UP":
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError("API did not become ready")


def recovery_test(timeout: int) -> None:
    """Break only this stack's API producer, then recover the durable admission without resubmitting."""
    wait_for_demo_services(180)
    wait_for_job("publication-recovery", 180)
    destination = ROOT / ".monitoring/recovery-latest.json"
    destination.parent.mkdir(exist_ok=True)
    report = {"pass": False, "stage": "starting"}

    def checkpoint(stage: str) -> None:
        report["stage"] = stage
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Recovery test: {stage}", flush=True)

    checkpoint("starting")
    try:
        with tempfile.TemporaryDirectory(prefix="messaging-recovery-") as directory:
            override = Path(directory) / "producer-down.yml"
            override.write_text("services:\n  api:\n    environment:\n"
                                "      KAFKA_BOOTSTRAP_SERVERS: 127.0.0.1:1\n", encoding="utf-8")
            try:
                compose("stop", "publication-recovery", timeout=90)
                compose("up", "-d", "--no-deps", "api", override=override, timeout=120)
                wait_for_api()
                checkpoint("api-producer-unavailable")
                token = request_json(API + "/api/v1/auth/token",
                                     {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
                run_id = uuid.uuid4().hex[:8]
                client_msg_id, latency, _ = submit(token, run_id, 0, False)
                report.update({"runId": run_id, "clientMsgId": client_msg_id, "admissionMs": round(latency, 2)})
                stored = origin_item(client_msg_id)
                if stored.get("message_id", {}).get("S") != f"monitor-{run_id}-00000":
                    raise AssertionError("Admission was not durably saved under the original ID")
                if "message_publication_due" not in stored:
                    raise AssertionError("Admission has no publication recovery index")
                logs = compose("logs", "--tail=1000", "--no-color", "api")
                if not any(client_msg_id in line and "Initial Kafka publication" in line for line in logs.splitlines()):
                    raise AssertionError("No evidence of the API publication failure")
                report["initialPublicationFailed"] = True
                if sql(f"SELECT count(*) FROM messaging_completion.tbl_msg_hist WHERE client_msg_id='{client_msg_id}'") != "0":
                    raise AssertionError("Message completed before recovery was started")
                checkpoint("origin-saved-publication-failed")
            finally:
                # Both services return to their normal configuration even if the injected failure test fails.
                try:
                    compose("up", "-d", "--no-deps", "api", timeout=120)
                    wait_for_api()
                finally:
                    compose("up", "-d", "--no-deps", "publication-recovery", timeout=120)

        checkpoint("waiting-for-recovery-without-resubmission")
        deadline = time.monotonic() + timeout
        expected = "PRIMARY|SUCCESS|DONE|1|1"
        while time.monotonic() < deadline:
            actual = sql("SELECT final_stage || '|' || outcome || '|' || cleanup_status || '|' || "
                         "(SELECT count(*) FROM messaging_completion.tbl_cdr_hist c WHERE c.client_msg_id=m.client_msg_id) || '|' || "
                         "(SELECT count(*) FROM messaging_webhook.tbl_webhook_outbox o WHERE o.client_msg_id=m.client_msg_id "
                         "AND o.status='DELIVERED' AND EXISTS (SELECT 1 FROM messaging_webhook.tbl_webhook_hist h "
                         "WHERE h.batch_id=o.batch_id AND h.acknowledged)) "
                         f"FROM messaging_completion.tbl_msg_hist m WHERE client_msg_id='{client_msg_id}'")
            if actual == expected and not origin_item(client_msg_id):
                break
            time.sleep(1)
        else:
            raise TimeoutError(f"Recovery did not complete: {actual!r}")
        logs = compose("logs", "--tail=1000", "--no-color", "publication-recovery")
        if f"ORIGIN publication recovery sent: clientMsgId={client_msg_id}" not in logs:
            raise AssertionError("No evidence of recovery publication for the original ID")
        report.update({"pass": True, "recoveryPublished": True, "messageHistory": 1, "billable": 1,
                       "customerWebhooksDelivered": 1, "originRemoved": True})
        checkpoint("complete")
        print(json.dumps(report, ensure_ascii=False, indent=2))
    except Exception as failure:
        report["error"] = str(failure)
        checkpoint("failed")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    demo_parser = subcommands.add_parser("demo")
    demo_parser.add_argument("--rate", type=int, default=1)
    demo_parser.add_argument("--seconds", type=int, default=1)
    demo_parser.add_argument("--errors", action="store_true")
    demo_parser.add_argument("--secondary", action="store_true",
                             help="Also verify HTTP failure followed by successful TCP fallback")
    demo_parser.add_argument("--timeout", type=int, default=180)
    demo_parser.add_argument("--startup-timeout", type=int, default=300)
    subcommands.add_parser("verify")
    subcommands.add_parser("diagnose")
    recovery_parser = subcommands.add_parser("recovery-test")
    recovery_parser.add_argument("--timeout", type=int, default=180)
    wait_parser = subcommands.add_parser("wait-job")
    wait_parser.add_argument("--job", required=True)
    wait_parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if args.command == "demo":
        demo(args)
    elif args.command == "verify":
        verify()
    elif args.command == "diagnose":
        diagnose()
    elif args.command == "recovery-test":
        recovery_test(args.timeout)
    else:
        wait_for_job(args.job, args.timeout)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Monitoring {sys.argv[1]} failed: {error}", file=sys.stderr)
        sys.exit(1)
