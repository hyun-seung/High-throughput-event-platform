#!/usr/bin/env python3
"""Kill the local SKT Sender after its HTTP response and before the observation reaches DynamoDB."""

import json
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path

from main_flow import API, AP_HEALTH_ENDPOINTS, ROOT, application_health, compose, request_json
from policy_flow import first_timeout, mock_events
from redis_dns_flow import inspect
from storage_flow import HTTP_GROUP, HTTP_TOPIC, http_item
from webhook_flow import consumer_lag, eventually, field, histories, key, name_uuid, post_webhook, read_items


def sender_ready():
    return application_health().get("skt-sender") == "UP"


def run():
    report = {"runId": uuid.uuid4().hex[:8], "pass": False, "cases": {}}
    destination = ROOT / ".monitoring/sender-crash-latest.json"
    destination.parent.mkdir(exist_ok=True)

    def checkpoint(stage):
        report["stage"] = stage
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Sender crash test: " + stage, flush=True)

    def rows():
        return histories(report["runId"], "crash")

    def calls(identifier):
        return [e for e in mock_events() if e["kind"] == "carrier_request" and e["clientMsgId"] == identifier]

    def verify():
        final = rows()
        assert set(final) == {case["clientMsgId"] for case in report["cases"].values()}, final
        events = mock_events()
        for name, case in report["cases"].items():
            identifier = case["clientMsgId"]
            invocation = 1 if name == "webhook-during-crash" else 2
            row = final[identifier]
            expected = {"outcome": "SUCCESS", "stage": "PRIMARY", "source": "WEBHOOK",
                        "carrier": "SKT", "invocation": invocation, "billable": 1,
                        "delivered": 1, "cleanup": "DONE", "error": None}
            assert all(row[k] == v for k, v in expected.items()), row
            sent = calls(identifier)
            assert len(sent) == invocation and all(e["carrier"] == "SKT" for e in sent), sent
            customer = [r for e in events if e["kind"] == "customer_webhook" for r in e["body"]["results"]
                        if r["clientMsgId"] == identifier]
            assert len(customer) == 1 and customer[0]["status"] == "success", customer
            assert not any(e["kind"] == "tcp_request" and e["clientMsgId"] == identifier for e in events)
        assert not read_items("ORIGIN", [key(identifier, "META") for identifier in final])
        for group, topic in ((HTTP_GROUP, HTTP_TOPIC), ("messaging-result-manager", "MSG_RESULT"),
                             ("messaging-complete-manager", "MSG-RESULT-FINALIZED"),
                             ("messaging-webhook-sender", "WEBHOOK-SEND")):
            eventually("Consumer lag did not clear: " + group, lambda: consumer_lag(group, topic) == 0)
        return {"histories": len(final), "billable": len(final), "customerResults": len(final),
                "originRemoved": len(final), "externalHttpCalls": 3, "consumerLag": 0}

    checkpoint("checking-idle-stack")
    health = application_health()
    assert len(health) == len(AP_HEALTH_ENDPOINTS) and all(v == "UP" for v in health.values()), health
    assert consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 0
    token = request_json(API + "/api/v1/auth/token",
                         {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
    with tempfile.TemporaryDirectory(prefix="sender-crash-") as directory:
        state = Path(directory)
        override = state / "compose.json"
        override.write_text(json.dumps({"services": {
            "sender-crash-proxy": {"image": "python:3.12-alpine", "restart": "no",
                "command": ["python", "/proxy.py"], "volumes": [
                    str(ROOT / "scripts/monitoring/sender_crash_proxy.py") + ":/proxy.py:ro",
                    str(state) + ":/state"]},
            "skt-sender": {"restart": "no", "environment": {
                "DYNAMODB_ENDPOINT": "http://sender-crash-proxy:8000"}}
        }}))
        try:
            compose("up", "-d", "--no-deps", "sender-crash-proxy", override=override, timeout=60)
            for name in ("webhook-during-crash", "missing-webhook-retry"):
                compose("up", "-d", "--no-deps", "skt-sender", override=override, timeout=90)
                eventually("Sender did not start with the fault proxy", sender_ready)
                body = {"messageId": f"crash-{report['runId']}-{len(report['cases'])}",
                        "recipientNumber": "01099990000", "messageCategory": "GENERAL",
                        "payload": {"text": name, "simulatorScenario": "manual-webhook"}}
                identifier = request_json(API + "/api/v1/messages", body, token)["data"]["clientMsgId"]
                case = report["cases"][name] = {"clientMsgId": identifier}
                checkpoint(name + "-admitted")

                def captured():
                    path = state / "captured.jsonl"
                    if not path.exists():
                        return None
                    for line in path.read_text().splitlines():
                        try:
                            observation = json.loads(line)
                        except json.JSONDecodeError:
                            continue  # A concurrent append may still be incomplete.
                        if observation["clientMsgId"] == identifier:
                            return observation
                    return None

                observation = eventually("Sender did not attempt to save the HTTP response", captured, timeout=30)
                assert observation["status"] == "ACCEPTED" and observation["httpStatus"] == 200, observation
                case["unpersistedObservation"] = observation
                pending = http_item(identifier)
                assert field(pending, "status") == "SENDING" and "http_observation" not in pending, pending
                assert len(calls(identifier)) == 1
                container = compose("ps", "--quiet", "skt-sender")
                compose("kill", "--signal", "SIGKILL", "skt-sender")
                killed = inspect(container)["State"]
                assert not killed["Running"] and killed["ExitCode"] == 137, killed
                assert consumer_lag(HTTP_GROUP, HTTP_TOPIC) == 1
                case.update({"exitCode": 137, "uncommittedCommands": 1, "persistedStatus": "SENDING"})
                if name == "webhook-during-crash":
                    case["webhookTraceId"] = post_webhook([{"clientMsgId": identifier, "status": "success"}])
                    inbox_key = key(identifier, "RESULT_INBOX#" + name_uuid(
                        "webhook-result:" + case["webhookTraceId"] + ":" + identifier))

                    def webhook_pending():
                        items = read_items("STEP", [inbox_key])
                        return items and field(items[0], "status") == "PENDING"

                    eventually("Webhook was not preserved while the Sender was down", webhook_pending)
                    assert identifier not in rows(), "Completed before recovering the HTTP observation"
                    case["webhookPendingBeforeRestart"] = True
                checkpoint(name + "-killed-before-observation-write")
                # Recreate only SKT with the normal endpoint and original restart policy.
                compose("up", "-d", "--no-deps", "skt-sender", timeout=90)
                eventually("Sender did not recover", sender_ready)
                if name == "missing-webhook-retry":
                    timeout = eventually("Stale invocation did not become a timeout", lambda: first_timeout(identifier))
                    assert timeout["status"] == "TIMEOUT" and timeout["source"] == "HTTP_TIMEOUT", timeout
                    case["recoveredObservation"] = timeout
                    eventually("Scheduled retry did not call the provider", lambda: len(calls(identifier)) == 2)
                    second_key = key(identifier, "HTTP#SKT#" + name_uuid("primary-http:" + identifier + ":SKT") + "#2")

                    def accepted_second():
                        items = read_items("STEP", [second_key])
                        return items and field(items[0], "status") == "OBSERVED" \
                            and json.loads(field(items[0], "http_observation"))["status"] == "ACCEPTED"

                    eventually("Retry HTTP acceptance not recorded", accepted_second)
                    gap = calls(identifier)[1]["receivedAt"] - datetime.fromisoformat(timeout["observedAt"].replace("Z", "+00:00")).timestamp()
                    assert gap >= 60, gap
                    case["retryAfterTimeoutSeconds"] = round(gap, 2)
                    post_webhook([{"clientMsgId": identifier, "status": "success"}])
                eventually("Message did not complete after Sender recovery",
                           lambda: rows().get(identifier, {}).get("cleanup") == "DONE"
                           and rows().get(identifier, {}).get("delivered") == 1)
                checkpoint(name + "-completed")
            report["verification"] = verify()
            checkpoint("checking-for-delayed-duplicates")
            deadline = time.monotonic() + 70
            while time.monotonic() < deadline:
                time.sleep(min(5, max(0, deadline - time.monotonic())))
            report["verification"] = verify()
            report["quietSeconds"] = 70
        except Exception as failure:
            report["failure"] = f"{type(failure).__name__}: {failure}"
            checkpoint("failed")
            raise
        finally:
            try:
                compose("up", "-d", "--no-deps", "skt-sender", timeout=90)
                eventually("Sender normal configuration was not restored", sender_ready)
                report["senderRestored"] = True
            finally:
                compose("rm", "--stop", "--force", "sender-crash-proxy", override=override, timeout=60)
                report["proxyRemoved"] = True
                destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["applicationHealth"] = application_health()
    assert all(v == "UP" for v in report["applicationHealth"].values())
    report["pass"] = True
    checkpoint("complete")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
