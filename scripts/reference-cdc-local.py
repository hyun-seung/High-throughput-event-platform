#!/usr/bin/env python3
"""Verify PostgreSQL -> Debezium -> Kafka -> Redis reference projection locally."""

import argparse
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parent.parent


def environment() -> dict[str, str]:
    command = ('set -a; [[ ! -f .env ]] || source .env; '
               '[[ -z ${LOCAL_ENV_OVERRIDE_FILE:-} ]] || source "$LOCAL_ENV_OVERRIDE_FILE"; env -0')
    result = subprocess.run(["bash", "-c", command], cwd=ROOT, capture_output=True, check=True)
    return dict(part.decode().split("=", 1) for part in result.stdout.split(b"\0") if part)


def run(command: list[str], env: dict[str, str]) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"{' '.join(command[:4])} failed: {result.stderr[-1200:]}")
    return result.stdout.strip()


def psql(sql: str, env: dict[str, str]) -> str:
    return run(["docker", "compose", "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
                "-U", "delivery", "-d", "delivery", "-Atc", sql], env)


def redis(command: list[str], env: dict[str, str]) -> str:
    return run(["docker", "compose", "exec", "-T", "redis", "redis-cli", "--raw",
                "-n", env.get("REDIS_DATABASE", "0"), *command], env)


def wait_for(description: str, predicate, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {description}")


def connector_running(port: str) -> bool:
    try:
        with urlopen(f"http://127.0.0.1:{port}/connectors/messaging-reference-postgres/status",
                     timeout=2) as response:
            status = json.load(response)
        return status["connector"]["state"] == "RUNNING" and bool(status["tasks"]) \
            and all(task["state"] == "RUNNING" for task in status["tasks"])
    except (HTTPError, URLError, TimeoutError, KeyError, ValueError):
        return False


def kafka_events(topic: str, key: dict, env: dict[str, str]) -> list[dict]:
    output = run(["docker", "compose", "exec", "-T", "kafka",
                  "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:29092",
                  "--topic", topic, "--from-beginning", "--timeout-ms", "2000",
                  "--property", "print.key=true", "--property", "key.separator=|"], env)
    events = []
    for line in output.splitlines():
        if "|" not in line:
            continue
        raw_key, raw_value = line.split("|", 1)
        try:
            if json.loads(raw_key) == key:
                events.append(json.loads(raw_value))
        except ValueError:
            continue
    return events


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--observe", action="store_true",
                        help="print PostgreSQL, Kafka and Redis evidence for the test rows")
    parser.add_argument("--pause-seconds", type=int, default=0,
                        help="pause between changes for inspection from another terminal")
    args = parser.parse_args()
    if args.pause_seconds < 0:
        parser.error("--pause-seconds must be at least zero")
    observe = args.observe or args.pause_seconds > 0
    env = environment()
    username = "cdc-" + uuid.uuid4().hex[:24]
    phone = None
    client_id = None
    process = None
    log_path = Path(tempfile.mkdtemp(prefix="messaging-reference-cdc-")) / "cache.log"
    try:
        run(["bash", "scripts/check-kafka-volume.sh", "docker", "compose"], env)
        run(["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "180",
             "postgres", "kafka", "redis"], env)
        run(["bash", "scripts/local.sh", "init-db"], env)
        run(["bash", "scripts/local.sh", "cdc"], env)
        if not args.skip_build:
            run(["./mvnw", "-pl", "messaging-reference-cache", "-am", "package", "-q"], env)
        jar = ROOT / "messaging-reference-cache/target/messaging-reference-cache-1.0-SNAPSHOT.jar"
        if not jar.is_file():
            raise RuntimeError("Build first: missing messaging-reference-cache JAR")

        with log_path.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(["bash", "scripts/local.sh", "run", "messaging-reference-cache"],
                                       cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True)
        def cache_ready() -> bool:
            if process.poll() is not None:
                raise RuntimeError(f"Cache AP exited; inspect {log_path}")
            return "Started MessageReferenceCacheApplication" in log_path.read_text(encoding="utf-8")

        wait_for("cache AP readiness", cache_ready)
        wait_for("Debezium connector and task RUNNING",
                 lambda: connector_running(env.get("DEBEZIUM_HOST_PORT", "18083")))
        if observe:
            print("[준비] Debezium connector=RUNNING, task=RUNNING; cache AP started", flush=True)

        # A new user and phone keep this run independent of real/local-user reference data.
        psql(f"INSERT INTO public.users (username, password, status, created_at, updated_at) "
             f"VALUES ('{username}', 'cdc-local-only', 'ACTIVE', now(), now())", env)
        client_id = int(psql(f"SELECT id FROM public.users WHERE username='{username}'", env))
        for _ in range(20):
            candidate = "010" + f"{secrets.randbelow(100000000):08d}"
            if psql("SELECT count(*) FROM delivery_results.phone_carrier_mappings "
                    f"WHERE phone_number='{candidate}'", env) == "0" \
                    and redis(["EXISTS", f"message:phone-carrier:{candidate}"], env) == "0":
                phone = candidate
                break
        if phone is None:
            raise RuntimeError("Could not find an unused test phone number")
        contract_key = f"message:contract:{{client:{client_id}}}"
        phone_key = f"message:phone-carrier:{phone}"
        if redis(["EXISTS", contract_key], env) != "0":
            raise RuntimeError("New client ID already has a cache entry")
        if observe:
            print(f"[대상] clientId={client_id}, phone={phone}", flush=True)
            print(f"[키] contract={contract_key}, carrier={phone_key}", flush=True)

        def show_stage(label: str) -> None:
            if not observe:
                return
            contract_sql = psql("SELECT enabled::text || ',' || tps_limit::text "
                                f"FROM delivery_results.client_message_contracts WHERE client_id={client_id}", env)
            carrier_sql = psql("SELECT carrier FROM delivery_results.phone_carrier_mappings "
                               f"WHERE phone_number='{phone}'", env)
            contract_cache = redis(["GET", contract_key], env)
            carrier_cache = redis(["GET", phone_key], env)
            print(f"[{label}] PostgreSQL contract={contract_sql or '(없음)'}, "
                  f"carrier={carrier_sql or '(없음)'}", flush=True)
            print(f"[{label}] Redis contract={contract_cache or '(없음)'}, "
                  f"carrier={carrier_cache or '(없음)'}", flush=True)
            if args.pause_seconds:
                print(f"[{label}] {args.pause_seconds}초 동안 외부 조회 가능", flush=True)
                time.sleep(args.pause_seconds)

        psql("INSERT INTO delivery_results.client_message_contracts "
             "(client_id, enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert) "
             f"VALUES ({client_id}, true, 100, 1000, 1000, 1000, 1000)", env)
        psql("INSERT INTO delivery_results.phone_carrier_mappings (phone_number, carrier) "
             f"VALUES ('{phone}', 'SKT')", env)

        def contract_is(enabled: bool, limit: int) -> bool:
            value = redis(["GET", contract_key], env)
            if not value:
                return False
            row = json.loads(value)
            return row["client_id"] == client_id and row["enabled"] is enabled \
                and row["tps_limit"] == limit

        wait_for("contract insert in Redis", lambda: contract_is(True, 100))
        wait_for("carrier insert in Redis", lambda: redis(["GET", phone_key], env) == "SKT")
        show_stage("생성")

        psql("UPDATE delivery_results.client_message_contracts "
             f"SET enabled=false, tps_limit=42, updated_at=now() WHERE client_id={client_id}", env)
        psql("UPDATE delivery_results.phone_carrier_mappings "
             f"SET carrier='KT', updated_at=now() WHERE phone_number='{phone}'", env)
        wait_for("contract update in Redis", lambda: contract_is(False, 42))
        wait_for("carrier update in Redis", lambda: redis(["GET", phone_key], env) == "KT")
        show_stage("수정")

        psql(f"DELETE FROM delivery_results.client_message_contracts WHERE client_id={client_id}", env)
        psql(f"DELETE FROM delivery_results.phone_carrier_mappings WHERE phone_number='{phone}'", env)
        wait_for("contract delete in Redis", lambda: redis(["EXISTS", contract_key], env) == "0")
        wait_for("carrier delete in Redis", lambda: redis(["EXISTS", phone_key], env) == "0")
        show_stage("삭제")
        if observe:
            topics = (
                ("messaging_reference.delivery_results.client_message_contracts", {"client_id": client_id}),
                ("messaging_reference.delivery_results.phone_carrier_mappings", {"phone_number": phone}),
            )
            for topic, key in topics:
                records = kafka_events(topic, key, env)
                summary = ["tombstone" if event is None else event.get("op", "?") for event in records]
                print(f"[Kafka] {topic}: {summary}", flush=True)
        print(f"CDC 생성·수정·삭제 검증 통과: clientId={client_id}, phone={phone}")
        print("경로: PostgreSQL → Debezium → Kafka → messaging-reference-cache → Redis")
        return 0
    except Exception as failure:
        print(f"CDC 통합 검증 실패: {failure}; cache log: {log_path}", file=sys.stderr)
        return 1
    finally:
        if client_id is not None:
            try:
                psql(f"DELETE FROM delivery_results.client_message_contracts WHERE client_id={client_id}; "
                     f"DELETE FROM public.users WHERE id={client_id} AND username='{username}'", env)
            except Exception as cleanup_failure:
                print(f"CDC test user cleanup failed: {cleanup_failure}", file=sys.stderr)
        if phone is not None:
            try:
                psql(f"DELETE FROM delivery_results.phone_carrier_mappings WHERE phone_number='{phone}'", env)
            except Exception as cleanup_failure:
                print(f"CDC test phone cleanup failed: {cleanup_failure}", file=sys.stderr)
        keys = ([f"message:contract:{{client:{client_id}}}"] if client_id is not None else [])
        if phone is not None:
            keys.append(f"message:phone-carrier:{phone}")
        if keys:
            try:
                redis(["DEL", *keys], env)
            except Exception as cleanup_failure:
                print(f"CDC test cache cleanup failed: {cleanup_failure}", file=sys.stderr)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        print(f"Cache AP log: {log_path}")


if __name__ == "__main__":
    sys.exit(main())
