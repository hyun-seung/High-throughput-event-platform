#!/usr/bin/env python3
"""Run the real first-send APs against loopback carrier/customer endpoints."""

import argparse
import datetime as dt
import json
import os
import re
import secrets
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
MODULES = (
    "messaging-result-manager", "messaging-pre-send-manager", "messaging-carrier-http-sender",
    "messaging-webhook-receive-api", "messaging-complete-manager",
    "messaging-webhook-sender", "messaging-api",
)


def local_environment() -> dict[str, str]:
    command = ('set -a; [[ ! -f .env ]] || source .env; '
               '[[ -z ${LOCAL_ENV_OVERRIDE_FILE:-} ]] || source "$LOCAL_ENV_OVERRIDE_FILE"; env -0')
    result = subprocess.run(["bash", "-c", command], cwd=ROOT, env=os.environ,
                            capture_output=True, check=True)
    return dict(part.decode().split("=", 1) for part in result.stdout.split(b"\0") if part)


def run(command: list[str], env: dict[str, str], *, input_bytes: bytes | None = None) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, input=input_bytes,
                            capture_output=True, check=False)
    if result.returncode:
        tail = result.stderr.decode(errors="replace")[-1500:]
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command[:4])}\n{tail}")
    return result.stdout.decode().strip()


def psql(sql: str, env: dict[str, str], *, input_bytes: bytes | None = None) -> str:
    return run(["docker", "compose", "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
                "-U", "delivery", "-d", "delivery", "-Atc", sql], env, input_bytes=input_bytes)


def redis(command: list[str], env: dict[str, str]) -> str:
    database = env.get("REDIS_DATABASE", "0")
    return run(["docker", "compose", "exec", "-T", "redis", "redis-cli", "--raw",
                "-n", database, *command], env)


def post_json(url: str, value: object, token: str | None = None) -> tuple[int, dict]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, data=json.dumps(value).encode(), headers=headers, method="POST")
    with urlopen(request, timeout=20) as response:
        return response.status, json.load(response)


def ready(process: subprocess.Popen, port: str, log_file: Path, timeout: int = 60) -> None:
    url = f"http://127.0.0.1:{port}/readyz"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"AP exited before readiness; inspect {log_file}")
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except (HTTPError, URLError, TimeoutError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"AP readiness timed out; inspect {log_file}")


def ddb(target: str, payload: dict, endpoint: str, env: dict[str, str]) -> dict:
    value = run(["curl", "--silent", "--show-error", "--fail-with-body", "--aws-sigv4",
                 "aws:amz:ap-northeast-2:dynamodb", "--user", "dummy:dummy",
                 "-H", f"X-Amz-Target: DynamoDB_20120810.{target}",
                 "-H", "Content-Type: application/x-amz-json-1.0",
                 "--data", json.dumps(payload), endpoint], env)
    return json.loads(value)


def events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def verify_flow(env: dict[str, str], event_file: Path, client_id: int, timeout: int) -> str:
    api = f"http://127.0.0.1:{env.get('DELIVERY_API_PORT', '8080')}"
    status, auth = post_json(api + "/api/v1/auth/token",
                             {"username": "local-user", "password": "local-password"})
    if status != 200:
        raise AssertionError(f"Authentication returned {status}")
    token = auth["data"]["accessToken"]
    month = dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).strftime("%Y-%m")
    usage_key = f"message:usage:{{client:{client_id}}}:month:{month}:GENERAL"
    before = int(redis(["GET", usage_key], env) or "0")
    recipient = "010" + f"{secrets.randbelow(100000000):08d}"
    message_id = "primary-flow-" + uuid.uuid4().hex[:20]
    status, response = post_json(api + "/api/v1/messages", {
        "messageId": message_id, "recipientNumber": recipient,
        "messageCategory": "GENERAL", "payload": {"text": "first-send local flow"},
    }, token)
    if status != 202:
        raise AssertionError(f"Message admission returned {status}")
    tps_count = int(redis(["HGET", f"message:usage:{{client:{client_id}}}:10s", "count"], env) or "0")
    if tps_count < 1:
        raise AssertionError("TPS usage was not recorded after admission")
    message = response["data"]
    client_msg_id = message["clientMsgId"]
    if not re.fullmatch(r"[0-9a-f]{32}", client_msg_id):
        raise AssertionError("Invalid clientMsgId from admission")

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = events(event_file)
        requests = [event for event in found if event["kind"] == "carrier_request"
                    and event["clientMsgId"] == client_msg_id]
        callbacks = [event for event in found if event["kind"] == "carrier_webhook_accepted"
                     and event["clientMsgId"] == client_msg_id]
        customer = [event for event in found if event["kind"] == "customer_webhook"
                    and any(item.get("clientMsgId") == client_msg_id
                            for item in event["body"].get("results", []))]
        hist = psql(f"SELECT final_stage || '|' || outcome || '|' || cleanup_status "
                    f"FROM messaging_completion.tbl_msg_hist WHERE client_msg_id='{client_msg_id}'", env)
        cdr = psql(f"SELECT count(*) FROM messaging_completion.tbl_cdr_hist "
                   f"WHERE client_msg_id='{client_msg_id}'", env)
        webhook = psql(f"SELECT status FROM messaging_webhook.tbl_webhook_outbox "
                       f"WHERE client_msg_id='{client_msg_id}'", env)
        if hist == "PRIMARY|SUCCESS|DONE" and cdr == "1" and webhook == "DELIVERED" \
                and len(requests) == 1 and callbacks and customer:
            request = requests[0]["request"]
            if request["clientMsgId"] != client_msg_id or request["recipientNumber"] != recipient \
                    or request["tenantId"] != client_id or request["payload"]["text"] != "first-send local flow":
                raise AssertionError("Carrier request differs from admitted message")
            result = next(item for item in customer[0]["body"]["results"]
                          if item["clientMsgId"] == client_msg_id)
            if result["status"] != "success":
                raise AssertionError("Customer webhook did not report success")
            after = int(redis(["GET", usage_key], env) or "0")
            if after != before + 1:
                raise AssertionError(f"Message quota usage changed by {after - before}, expected 1")
            endpoint = f"http://127.0.0.1:{env.get('DYNAMODB_HOST_PORT', '8000')}/"
            origin = ddb("GetItem", {"TableName": "ORIGIN", "Key": {
                "pk": {"S": "DELIVERY#" + client_msg_id}, "sk": {"S": "META"}}}, endpoint, env)
            steps = ddb("Query", {"TableName": "STEP", "KeyConditionExpression": "pk = :pk",
                                  "ExpressionAttributeValues": {":pk": {"S": "DELIVERY#" + client_msg_id}}},
                        endpoint, env)
            if origin.get("Item") or steps.get("Count") != 0:
                raise AssertionError("DynamoDB rows remain after cleanup is DONE")
            return client_msg_id
        time.sleep(1)
    raise TimeoutError(f"First-send flow did not finish: clientMsgId={client_msg_id}, "
                       f"history={hist!r}, cdr={cdr!r}, webhook={webhook!r}, "
                       f"carrierRequests={len(requests)}, carrierCallbacks={len(callbacks)}, "
                       f"customerWebhooks={len(customer)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-infra", action="store_true")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    if args.timeout < 10:
        parser.error("--timeout must be at least 10 seconds")
    env = local_environment()
    original_override = os.environ.get("LOCAL_ENV_OVERRIDE_FILE")
    if original_override:
        original_override = str(Path(original_override).resolve())
    directory = Path(tempfile.mkdtemp(prefix="messaging-primary-flow-"))
    processes: list[subprocess.Popen] = []

    def override(name: str, values: dict[str, str]) -> str:
        path = directory / f"{name}.env"
        content = (f"source {shlex.quote(original_override)}\n" if original_override else "")
        content += "".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items())
        path.write_text(content, encoding="utf-8")
        path.chmod(0o600)
        return str(path)

    def start(command: list[str], name: str, custom: dict[str, str] | None = None) -> subprocess.Popen:
        app_env = env.copy()
        if custom:
            app_env.update(custom)
        log_path = directory / f"{name}.log"
        with log_path.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(command, cwd=ROOT, env=app_env, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        processes.append(process)
        return process

    try:
        if not args.skip_infra:
            run(["bash", "scripts/local.sh", "infra"], env)
        run(["bash", "scripts/local.sh", "init-db"], env)
        run(["bash", "scripts/local.sh", "init-policy"], env)
        run(["docker", "compose", "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
             "-U", "delivery", "-d", "delivery"], env,
            input_bytes=(ROOT / "scripts/cdc/init.sql").read_bytes())
        client_id = int(psql("SELECT id FROM public.users WHERE username='local-user'", env))
        psql(f"INSERT INTO delivery_results.client_message_contracts "
             f"(client_id, enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert) "
             f"VALUES ({client_id}, true, 100, 100000, 100000, 100000, 100000) "
             f"ON CONFLICT (client_id) DO NOTHING", env)
        if psql(f"SELECT enabled FROM delivery_results.client_message_contracts "
                f"WHERE client_id={client_id}", env) != "t":
            raise RuntimeError("local-user contract is inactive; test will not overwrite it")
        redis(["DEL", f"message:contract:{{client:{client_id}}}"], env)

        if not args.skip_build:
            run(["./mvnw", "-pl", ",".join(MODULES), "-am", "package", "-DskipTests", "-q"], env)
        for module in MODULES:
            if not (ROOT / module / "target" / f"{module}-1.0-SNAPSHOT.jar").is_file():
                raise RuntimeError(f"Build first: missing {module} JAR")

        secret = secrets.token_urlsafe(32)
        mock_ready = directory / "mock-ready.json"
        mock_events = directory / "mock-events.jsonl"
        webhook_port = env.get("MESSAGE_WEBHOOK_PORT", "8099")
        start([sys.executable, "scripts/primary-flow-mock.py", "--ready-file", str(mock_ready),
               "--events-file", str(mock_events), "--webhook-url",
               f"http://127.0.0.1:{webhook_port}/api/v1/message-webhooks/skt"],
              "mock", {"FLOW_WEBHOOK_SECRET": secret})
        for _ in range(100):
            if mock_ready.exists():
                break
            if processes[0].poll() is not None:
                raise RuntimeError(f"Mock endpoints exited; inspect {directory / 'mock.log'}")
            time.sleep(0.1)
        else:
            raise TimeoutError("Mock endpoints did not start")
        mock_urls = json.loads(mock_ready.read_text(encoding="utf-8"))

        app_ports = {
            "messaging-result-manager": env.get("MSG_RESULT_MANAGER_PORT", "8102"),
            "messaging-pre-send-manager": env.get("MESSAGING_PRE_SEND_PORT", "8100"),
            "messaging-carrier-http-sender": env.get("MSG_HTTP_SENDER_PORT", "8101"),
            "messaging-webhook-receive-api": webhook_port,
            "messaging-complete-manager": env.get("MSG_COMPLETE_MANAGER_PORT", "8103"),
            "messaging-webhook-sender": env.get("MSG_WEBHOOK_SENDER_PORT", "8104"),
            "messaging-api": env.get("DELIVERY_API_PORT", "8080"),
        }
        dynamic = {
            "messaging-carrier-http-sender": override("carrier", {
                "MSG_HTTP_CARRIER": "SKT", "MSG_HTTP_BASE_URL": mock_urls["carrierUrl"],
                "MSG_HTTP_NOT_OUR_CARRIER_CODES": "60001", "MSG_HTTP_TPS_EXCEEDED_CODES": "60002",
            }),
            "messaging-webhook-receive-api": override("webhook-receive", {
                "MESSAGE_WEBHOOK_SKT_SECRET": secret,
            }),
            "messaging-webhook-sender": override("webhook-sender", {
                "SPRING_APPLICATION_JSON": json.dumps({"messaging": {"webhook": {"sender": {
                    "customers": {str(client_id): {"url": mock_urls["customerUrl"]}}
                }}}}),
            }),
            "messaging-api": override("api", {"MESSAGING_ADMISSION_ENABLED": "true"}),
        }
        for module in MODULES:
            app_env = {"LOCAL_ENV_OVERRIDE_FILE": dynamic[module]} if module in dynamic else None
            process = start(["bash", "scripts/local.sh", "run", module], module, app_env)
            ready(process, app_ports[module], directory / f"{module}.log")
        time.sleep(2)
        client_msg_id = verify_flow(env, mock_events, client_id, args.timeout)
        print(f"1차 정상 경로 통과: clientMsgId={client_msg_id}")
        print("확인: TPS·Quota 사용량, SKT HTTP 200, 성공 웹훅, SQL 이력·CDR, 고객 웹훅, DynamoDB 정리")
        return 0
    except Exception as failure:
        print(f"1차 정상 경로 실패: {failure}", file=sys.stderr)
        return 1
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in reversed(processes):
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        for path in directory.glob("*.env"):
            path.unlink()
        print(f"검증 로그: {directory}")


if __name__ == "__main__":
    sys.exit(main())
