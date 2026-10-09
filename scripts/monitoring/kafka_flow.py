#!/usr/bin/env python3
"""Pause the isolated Kafka broker and verify admission, HTTP results and webhook recovery."""

import json
import time
import uuid

from main_flow import API, AP_HEALTH_ENDPOINTS, ROOT, application_health, compose, origin_item, request_json
from policy_flow import mock_events
from redis_dns_flow import inspect
from storage_flow import HTTP_GROUP, HTTP_TOPIC, accepted_item, assert_calls, http_item, resume_service
from webhook_flow import consumer_lag, eventually, field, histories, key, post_webhook, read_items


GROUPS = (("messaging-pre-send-manager", "message.received.v1"),
          (HTTP_GROUP, HTTP_TOPIC), ("messaging-result-manager", "MSG_RESULT"),
          ("messaging-complete-manager", "MSG-RESULT-FINALIZED"),
          ("messaging-webhook-sender", "WEBHOOK-SEND"))


def release_reply(identifier=""):
    compose("exec", "-T", "mock", "python", "-c",
            "from pathlib import Path; import sys; "
            "p=Path('/logs/http-failure-release'); "
            "p.write_text(sys.argv[1]) if sys.argv[1] else p.unlink(missing_ok=True)", identifier)


def unavailable_webhook(identifier):
    # Read the actual HTTP failure, using the mock's credential without exposing it.
    code = """
import json, os, sys, time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
request = Request('http://webhook-receive:8099/api/v1/message-webhooks/skt',
    data=json.dumps([{'clientMsgId': sys.argv[1], 'status': 'success'}]).encode(),
    headers={'Content-Type': 'application/json',
             'Authorization': 'Bearer ' + os.environ['FLOW_WEBHOOK_SECRET_SKT']})
start = time.monotonic()
try:
    response = urlopen(request, timeout=25)
except HTTPError as failure:
    response = failure
with response:
    print(json.dumps({'httpStatus': response.status, 'body': json.loads(response.read()),
                      'seconds': round(time.monotonic() - start, 2)}))
"""
    result = json.loads(compose("exec", "-T", "mock", "python", "-c", code, identifier, timeout=30))
    assert result["httpStatus"] == 503, result
    assert result["body"]["code"] == "MESSAGE_WEBHOOK_UNCONFIRMED", result
    return result


def run():
    report = {"runId": uuid.uuid4().hex[:8], "pass": False, "cases": {}}
    destination = ROOT / ".monitoring/kafka-latest.json"
    destination.parent.mkdir(exist_ok=True)

    def checkpoint(stage):
        report["stage"] = stage
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Kafka test: " + stage, flush=True)

    def rows():
        return histories(report["runId"], "kafka")

    def admit(name, scenario="manual-webhook"):
        body = {"messageId": f"kafka-{report['runId']}-{len(report['cases'])}",
                "recipientNumber": "01099990000", "messageCategory": "GENERAL",
                "payload": {"text": name, "simulatorScenario": scenario}}
        start = time.monotonic()
        result = request_json(API + "/api/v1/messages", body, token)["data"]
        assert result["status"] == "RECEIVED", result
        identifier = result["clientMsgId"]
        report["cases"][name] = {"clientMsgId": identifier,
                                 "admissionSeconds": round(time.monotonic() - start, 2)}
        checkpoint(name + "-admitted")
        return identifier

    def verify():
        final = rows()
        assert set(final) == {case["clientMsgId"] for case in report["cases"].values()}, final
        events = mock_events()
        for name, case in report["cases"].items():
            identifier = case["clientMsgId"]
            failed = name == "http-failure-publication"
            expected = {"stage": "PRIMARY", "outcome": "FAILURE" if failed else "SUCCESS",
                        "source": "HTTP_RESPONSE" if failed else "WEBHOOK", "carrier": "SKT",
                        "invocation": 1, "error": 61001 if failed else None,
                        "cleanup": "DONE", "billable": 0 if failed else 1, "delivered": 1}
            assert all(final[identifier][k] == v for k, v in expected.items()), final[identifier]
            assert_calls(identifier, 1)
            customer = [r for e in events if e["kind"] == "customer_webhook" for r in e["body"]["results"]
                        if r["clientMsgId"] == identifier]
            assert len(customer) == 1 and customer[0]["status"] == ("fail" if failed else "success"), customer
            assert not any(e["kind"] == "carrier_gate_timeout" and e["clientMsgId"] == identifier for e in events)
        assert not read_items("ORIGIN", [key(identifier, "META") for identifier in final]), "ORIGIN remains"
        for group, topic in GROUPS:
            eventually("Consumer backlog did not clear: " + group, lambda: consumer_lag(group, topic) == 0)
        assert all(inspect(container)["State"]["StartedAt"] == starts[service]
                   for service, container in apps.items()), "An AP restarted"
        return {"messages": len(final), "billable": 2, "customerResults": len(final),
                "externalHttpCalls": len(final), "originsDeleted": len(final),
                "consumerLag": 0, "applicationsRestarted": False}

    checkpoint("checking-idle-stack")
    broker = compose("ps", "--quiet", "kafka")
    before = inspect(broker)
    assert before["Config"]["Labels"]["com.docker.compose.project"] == "platform-messaging-monitoring"
    assert before["State"]["Running"] and not before["State"]["Paused"]
    health = application_health()
    assert len(health) == len(AP_HEALTH_ENDPOINTS) and all(s == "UP" for s in health.values()), health
    for group, topic in GROUPS:
        assert consumer_lag(group, topic) == 0, "Run on an idle monitoring stack"
    apps = {service: compose("ps", "--quiet", service) for service in AP_HEALTH_ENDPOINTS}
    starts = {service: inspect(container)["State"]["StartedAt"] for service, container in apps.items()}
    # Reload only the local provider fixture; production APs keep running throughout the test.
    compose("restart", "mock", timeout=60)
    release_reply()
    token = request_json(API + "/api/v1/auth/token",
                         {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
    try:
        webhook_id = admit("webhook-publication")
        eventually("HTTP 200 not recorded", lambda: accepted_item(webhook_id))
        failure_id = admit("http-failure-publication", "held-http-failure")
        # Poll quickly: the provider's response gate expires before the real 5-second HTTP timeout.
        deadline = time.monotonic() + 4
        while not any(e["kind"] == "carrier_request" and e["clientMsgId"] == failure_id for e in mock_events()):
            if time.monotonic() >= deadline:
                raise TimeoutError("Sender did not reach the provider gate")
            time.sleep(0.05)
        compose("pause", "kafka")
        outage_start = time.monotonic()
        try:
            assert inspect(broker)["State"]["Paused"]
            release_reply(failure_id)

            def failure_observed():
                item = http_item(failure_id)
                return item if field(item or {}, "status") == "OBSERVED" else None

            observed = eventually("HTTP failure not saved during Kafka outage", failure_observed)
            observation = json.loads(field(observed, "http_observation"))
            assert observation["status"] == "FAILED" and observation["httpStatus"] == 400, observation
            assert field(observed, "publish_state") == "PENDING", observed
            report["cases"]["http-failure-publication"]["outageObservation"] = observation
            admission_id = admit("admission-publication", "success")
            origin = origin_item(admission_id)
            assert "message_publication_due" in origin, origin
            report["cases"]["admission-publication"]["outageRecoveryIndexPresent"] = True
            report["cases"]["webhook-publication"]["outageResponse"] = unavailable_webhook(webhook_id)
            # Keep the broker down beyond both producers' 10-second delivery timeout.
            time.sleep(max(0, 22 - (time.monotonic() - outage_start)))
            logs = compose("logs", "--since", "2m", "api")
            assert any(admission_id in line and "Initial Kafka publication" in line for line in logs.splitlines())
            assert inspect(broker)["State"]["Paused"] and not rows()
            assert field(http_item(failure_id), "publish_state") == "PENDING"
            assert_calls(failure_id, 1)
            assert_calls(webhook_id, 1)
            assert_calls(admission_id, 0)
            report["outage"] = {"seconds": round(time.monotonic() - outage_start, 2),
                                "histories": 0, "httpFailurePublishState": "PENDING",
                                "admissionPublicationUnconfirmed": True, "brokerPaused": True}
            checkpoint("outage-admission-observation-and-webhook-503-verified")
        finally:
            resume_service("kafka")
            report["brokerRestored"] = not inspect(broker)["State"]["Paused"]
            checkpoint("broker-resumed")

        report["cases"]["webhook-publication"]["retryTraceId"] = post_webhook([
            {"clientMsgId": webhook_id, "status": "success"}])
        eventually("Results did not complete after broker recovery",
                   lambda: len(rows()) == 3 and all(r["cleanup"] == "DONE" and r["delivered"] == 1 for r in rows().values()))
        logs = compose("logs", "--since", "5m", "publication-recovery")
        # A paused broker can process a buffered request after the producer has timed out.
        # Publication is unconfirmed, not necessarily absent; either path must complete once.
        report["cases"]["admission-publication"].update({"completedWithoutResubmission": True,
            "recoveryPublicationLogged": f"ORIGIN publication recovery sent: clientMsgId={admission_id}" in logs})
        report["verification"] = verify()
        checkpoint("recovered-checking-for-delayed-duplicates")
        # Include the one-minute retry interval before checking for unintended sends again.
        quiet_until = time.monotonic() + 70
        while time.monotonic() < quiet_until:
            time.sleep(min(5, max(0, quiet_until - time.monotonic())))
        report["verification"] = verify()
        health = application_health()
        assert all(s == "UP" for s in health.values()) and len(health) == len(AP_HEALTH_ENDPOINTS), health
        report["applicationHealth"] = health
        report["quietSeconds"] = 70
    except Exception as failure:
        report["failure"] = f"{type(failure).__name__}: {failure}"
        checkpoint("failed")
        raise
    finally:
        resume_service("kafka")
        release_reply()
    report["pass"] = True
    checkpoint("complete")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
