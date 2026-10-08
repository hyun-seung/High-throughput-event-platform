#!/usr/bin/env python3
"""Verify Redis send-claim loss and Redis/DynamoDB outage recovery in the isolated monitoring stack."""

import json
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from main_flow import API, COMPOSE, ROOT, compose, request_json, wait_for_demo_services, wait_for_job
from policy_flow import mock_events
from webhook_flow import consumer_lag, dynamo, eventually, field, histories, key, name_uuid, post_webhook, read_items


HTTP_TOPIC = "message.skt.http.send.v1"
HTTP_GROUP = "messaging-skt-http-sender"


def redis(*args):
    return compose("exec", "-T", "redis", "redis-cli", "--raw", *args)


def redis_ready():
    try:
        return redis("PING") == "PONG"
    except RuntimeError:
        return False


def resume_service(service):
    container = compose("ps", "--all", "--quiet", service)
    state = subprocess.run(["docker", "inspect", "--format", "{{.State.Paused}}", container],
                           capture_output=True, text=True, check=True, timeout=15).stdout.strip()
    if state == "true":
        compose("unpause", service)


def dynamo_ready():
    try:
        return set(dynamo("ListTables", {})["TableNames"]) >= {"ORIGIN", "STEP"}
    except subprocess.SubprocessError:
        return False


def replay(identifier, command, count=1):
    # Feed the original frozen command through Kafka; never mutate the stored attempt or reset offsets.
    subprocess.run([*COMPOSE, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", "kafka:29092", "--topic", HTTP_TOPIC,
        "--property", "parse.key=true", "--property", "key.separator=\t",
        "--producer-property", "acks=all"], cwd=ROOT, input=(identifier + "\t" + command + "\n") * count,
        text=True, capture_output=True, check=True, timeout=30)


def http_item(identifier):
    attempt = name_uuid("primary-http:" + identifier + ":SKT")
    rows = read_items("STEP", [key(identifier, "HTTP#SKT#" + attempt + "#1")])
    return rows[0] if rows else None


def accepted_item(identifier):
    row = http_item(identifier)
    return row if row and field(row, "status") == "OBSERVED" and json.loads(field(row, "http_observation"))["status"] == "ACCEPTED" else None


def assert_calls(identifier, count):
    events = mock_events()
    calls = [e for e in events if e["kind"] == "carrier_request" and e["clientMsgId"] == identifier]
    assert len(calls) == count and all(e["carrier"] == "SKT" for e in calls), f"Unexpected HTTP sends: {identifier}: {len(calls)}"
    assert not any(e["kind"] == "tcp_request" and e["clientMsgId"] == identifier for e in events)


def run():
    wait_for_demo_services(180)
    report = {"runId": uuid.uuid4().hex[:8], "pass": False, "cases": {}}
    destination = ROOT / ".monitoring/storage-latest.json"
    destination.parent.mkdir(exist_ok=True)

    def checkpoint(stage):
        report["stage"] = stage
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Storage test: " + stage, flush=True)

    def rows():
        return histories(report["runId"], "storage")

    def admit(name):
        body = {"messageId": f"storage-{report['runId']}-{len(report['cases'])}", "recipientNumber": "01099990000",
                "messageCategory": "GENERAL", "payload": {"text": name, "simulatorScenario": "manual-webhook"}}
        identifier = request_json(API + "/api/v1/messages", body, token)["data"]["clientMsgId"]
        report["cases"][name] = {"clientMsgId": identifier}
        checkpoint(name + "-admitted")
        return identifier

    def complete(identifier):
        post_webhook([{"clientMsgId": identifier, "status": "success"}])
        wait_complete(identifier)

    def wait_complete(identifier):
        eventually("Final history/cleanup/customer webhook did not complete",
                   lambda: rows().get(identifier, {}).get("cleanup") == "DONE"
                   and rows().get(identifier, {}).get("delivered") == 1)

    def verify():
        final = rows()
        assert set(final) == {case["clientMsgId"] for case in report["cases"].values()}
        events = mock_events()
        for identifier, row in final.items():
            expected = {"stage": "PRIMARY", "outcome": "SUCCESS", "source": "WEBHOOK", "carrier": "SKT",
                        "invocation": 1, "error": None, "cleanup": "DONE", "billable": 1, "delivered": 1,
                        "cleanupError": None}
            assert all(row[k] == value for k, value in expected.items()), row
            assert_calls(identifier, 1)
            results = [r for e in events if e["kind"] == "customer_webhook" for r in e["body"]["results"]
                       if r["clientMsgId"] == identifier]
            assert len(results) == 1 and results[0]["status"] == "success", results
        assert not read_items("ORIGIN", [key(identifier, "META") for identifier in final])
        for group, topic in ((HTTP_GROUP, HTTP_TOPIC), ("messaging-result-manager", "MSG_RESULT"),
                             ("messaging-complete-manager", "MSG-RESULT-FINALIZED")):
            eventually("Consumer backlog did not clear", lambda: consumer_lag(group, topic) == 0)
        return {"messages": len(final), "billable": len(final), "customerResults": len(final),
                "externalHttpCalls": len(final), "originRemoved": len(final), "consumerLag": 0}

    checkpoint("starting")
    try:
        for group, topic in ((HTTP_GROUP, HTTP_TOPIC), ("messaging-result-manager", "MSG_RESULT")):
            assert consumer_lag(group, topic) == 0, "Run on an idle monitoring stack"
        token = request_json(API + "/api/v1/auth/token",
                             {"username": "local-user", "password": "local-password"})["data"]["accessToken"]

        identifier = admit("redis-claim-loss")
        observed = eventually("HTTP acceptance not recorded", lambda: accepted_item(identifier))
        command = field(observed, "command")
        attempt = json.loads(command)["attemptId"]
        claim = f"message:http:attempt:{identifier}:SKT:{attempt}:1"
        assert redis("DEL", claim) == "1", "Expected a live Redis claim to remove"
        assert redis("EXISTS", claim) == "0"
        replay(identifier, command, 3)
        eventually("Replayed HTTP commands were not committed", lambda: consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 0)
        assert_calls(identifier, 1)
        assert redis("EXISTS", claim) == "0", "Observed attempt reacquired Redis claim"
        assert field(http_item(identifier), "http_observation") == field(observed, "http_observation")
        complete(identifier)
        replay(identifier, command)
        eventually("Post-cleanup replay was not committed", lambda: consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 0)
        assert_calls(identifier, 1)
        report["cases"]["redis-claim-loss"].update({"deletedClaim": True, "replayedCommands": 4, "externalCalls": 1})
        checkpoint("redis-key-loss-and-post-cleanup-replay-passed")

        # Hold the sender until admission and PRE-SEND have frozen the command, then make Redis unavailable.
        try:
            compose("pause", "skt-sender")
            identifier = admit("redis-unavailable")
            eventually("PRE-SEND command was not published", lambda: consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 1)
            # Keep network endpoints stable: stopping both containers can swap their Docker IPs.
            compose("pause", "redis")
            compose("unpause", "skt-sender")
            assert not redis_ready(), "Sender startup unexpectedly restored Redis"
            eventually("Sender did not preserve its pending attempt", lambda: field(http_item(identifier) or {}, "status") == "PENDING")
            time.sleep(5)
            assert not redis_ready(), "Redis outage was not maintained"
            assert_calls(identifier, 0)
            assert field(http_item(identifier), "status") == "PENDING"
            assert consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 1
            report["cases"]["redis-unavailable"].update({"outageHttpCalls": 0, "outageLag": 1, "outageStepStatus": "PENDING"})
            checkpoint("redis-unavailable-send-blocked-offset-preserved")
        finally:
            try:
                resume_service("redis")
                eventually("Redis did not recover", redis_ready)
            finally:
                resume_service("skt-sender")
        eventually("Sender did not resume after Redis recovery", lambda: accepted_item(identifier))
        complete(identifier)
        checkpoint("redis-recovery-passed")

        identifier = admit("dynamo-unavailable")
        observed = eventually("HTTP acceptance not recorded", lambda: accepted_item(identifier))
        command = field(observed, "command")
        try:
            compose("stop", "dynamodb-local", timeout=60)
            report["cases"]["dynamo-unavailable"]["webhookTraceId"] = post_webhook([
                {"clientMsgId": identifier, "status": "success"}])
            replay(identifier, command)
            time.sleep(10)
            assert not dynamo_ready(), "DynamoDB outage was not maintained"
            assert consumer_lag("messaging-result-manager", "MSG_RESULT") == 1
            assert consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 1
            assert identifier not in rows(), "Completed without saving the result in DynamoDB"
            assert_calls(identifier, 1)
            report["cases"]["dynamo-unavailable"].update({"webhookAccepted": True, "outageResultLag": 1,
                "outageSenderLag": 1, "outageHistories": 0, "externalCalls": 1})
            checkpoint("dynamo-unavailable-webhook-retained-in-kafka-and-replay-blocked")
        finally:
            compose("up", "-d", "--no-deps", "dynamodb-local", timeout=60)
            eventually("DynamoDB did not recover", dynamo_ready)
        wait_complete(identifier)
        checkpoint("dynamo-recovery-without-provider-or-customer-resubmission-passed")

        # Break only completion cleanup's DynamoDB connection; SQL and other APs remain available.
        with tempfile.TemporaryDirectory(prefix="messaging-cleanup-outage-") as directory:
            override = Path(directory) / "dynamo-unavailable.yml"
            override.write_text("services:\n  complete-manager:\n    environment:\n"
                                "      DYNAMODB_ENDPOINT: http://127.0.0.1:1\n"
                                "      DYNAMODB_INITIALIZE_TABLES: 'false'\n", encoding="utf-8")
            try:
                compose("up", "-d", "--no-deps", "complete-manager", override=override, timeout=120)
                identifier = admit("cleanup-dynamo-unavailable")
                eventually("HTTP acceptance not recorded", lambda: accepted_item(identifier))
                post_webhook([{"clientMsgId": identifier, "status": "success"}])

                def cleanup_failed():
                    row = rows().get(identifier, {})
                    return row if row.get("cleanup") == "PENDING" and row.get("cleanupAttempts", 0) >= 1 else None

                pending = eventually("Cleanup failure was not retained for retry", cleanup_failed)
                assert pending["billable"] == 1 and pending["cleanupError"], pending
                assert read_items("ORIGIN", [key(identifier, "META")]), "Origin removed during cleanup failure"
                report["cases"]["cleanup-dynamo-unavailable"].update({"outageCleanup": pending["cleanup"],
                    "outageCleanupAttempts": pending["cleanupAttempts"], "outageCleanupError": pending["cleanupError"],
                    "outageBillable": 1, "originPreserved": True})
                checkpoint("sql-history-and-billing-preserved-cleanup-retry-pending")
            finally:
                compose("up", "-d", "--no-deps", "complete-manager", timeout=120)
                wait_for_job("complete-manager", 180)
        wait_complete(identifier)
        report["evidence"] = verify()
        checkpoint("all-storage-cases-matched; observing-70-seconds-for-extra-sends")
        until = time.monotonic() + 70
        while time.monotonic() < until:
            time.sleep(min(5, max(0, until - time.monotonic())))
        report["evidence"] = verify()
        report.update({"pass": True, "noExtraSendObservationSeconds": 70})
        checkpoint("complete")
        print(f"Storage verification PASS: {destination}", flush=True)
    except BaseException as failure:
        report["error"] = str(failure)
        checkpoint("failed")
        raise


if __name__ == "__main__":
    run()
