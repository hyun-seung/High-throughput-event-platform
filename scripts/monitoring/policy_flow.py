#!/usr/bin/env python3
"""Exercise real carrier routing/retry policy against per-message local mock outcomes."""

import argparse
import json
import subprocess
import time
import uuid
from datetime import datetime

from main_flow import API, ROOT, compose, origin_item, request_json, sql, wait_for_demo_services, wait_for_job


def cases() -> list[dict]:
    result = []
    for source in ("", "webhook-"):
        for name, carriers, error in (
            ("carrier-mismatch", ["SKT", "KT"], None),
            ("carrier-exhausted", ["SKT", "KT", "LGU"], 40002),
            ("tps-retry", ["SKT"] * 2, None),
            ("tps-exhausted", ["SKT"] * 4, 40001),
        ):
            result.append({"name": source + name, "scenario": source + name,
                           "carriers": carriers, "error": error, "secondary": False})
    for name, count, error in (("no-response-retry", 2, None), ("no-response-exhausted", 4, 40003)):
        result.append({"name": name, "scenario": name, "carriers": ["SKT"] * count,
                       "error": error, "secondary": False})
    for name, carriers in (("carrier-exhausted", ["SKT", "KT", "LGU"]),
                            ("tps-exhausted", ["SKT"] * 4), ("no-response-exhausted", ["SKT"] * 4)):
        result.append({"name": name + "-secondary", "scenario": name,
                       "carriers": carriers, "error": None, "secondary": True})
    return result


def mock_events() -> list[dict]:
    raw = compose("exec", "-T", "mock", "cat", "/logs/mock-events.jsonl")
    events = []
    lines = raw.splitlines()
    for index, line in enumerate(lines):
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            if index != len(lines) - 1:
                raise
            # A concurrent append may be between write and close; read it on the next poll.
    return events


def first_timeout(client_msg_id: str) -> dict | None:
    payload = {"TableName": "STEP", "ConsistentRead": True,
               "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
               "ExpressionAttributeValues": {":pk": {"S": "DELIVERY#" + client_msg_id},
                                             ":prefix": {"S": "HTTP#SKT#"}}}
    response = subprocess.run(["curl", "--silent", "--show-error", "--fail-with-body", "--max-time", "10",
        "--aws-sigv4", "aws:amz:ap-northeast-2:dynamodb", "--user", "local:local",
        "-H", "X-Amz-Target: DynamoDB_20120810.Query", "-H", "Content-Type: application/x-amz-json-1.0",
        "--data", json.dumps(payload), "http://127.0.0.1:38000/"],
        capture_output=True, text=True, check=True, timeout=15)
    for item in json.loads(response.stdout).get("Items", []):
        if item.get("invocation", {}).get("N") == "1" and "http_observation" in item:
            return json.loads(item["http_observation"]["S"])
    return None


def histories(run_id: str) -> dict:
    raw = sql("SELECT COALESCE(json_agg(json_build_object('id',m.client_msg_id,'stage',m.final_stage,"
              "'outcome',m.outcome,'error',m.error_code,'cleanup',m.cleanup_status,'carrier',m.carrier,"
              "'invocation',m.invocation,'source',m.result_source,'billable',(SELECT count(*) FROM messaging_completion.tbl_cdr_hist c "
              "WHERE c.client_msg_id=m.client_msg_id),'delivered',(SELECT count(*) "
              "FROM messaging_webhook.tbl_webhook_outbox o WHERE o.client_msg_id=m.client_msg_id "
              "AND o.status='DELIVERED' AND EXISTS (SELECT 1 FROM messaging_webhook.tbl_webhook_hist h "
              "WHERE h.batch_id=o.batch_id AND h.acknowledged))))::text,'[]') "
              f"FROM messaging_completion.tbl_msg_hist m WHERE m.message_id LIKE 'policy-{run_id}-%'")
    return {item['id']: item for item in json.loads(raw)}


def verify_case(case: dict, events: list[dict], history: dict) -> dict:
    identifier = case["clientMsgId"]
    requests = [event for event in events if event["kind"] == "carrier_request" and event["clientMsgId"] == identifier]
    actual_carriers = [event["carrier"] for event in requests]
    if actual_carriers != case["carriers"]:
        raise AssertionError(f"{case['name']}: carrier/count mismatch: {actual_carriers}")
    expected_invocations = list(range(1, len(requests) + 1)) if len(set(actual_carriers)) == 1 else [1] * len(requests)
    if [event["invocation"] for event in requests] != expected_invocations:
        raise AssertionError(f"{case['name']}: invocation sequence differs")
    intervals = [round(later["receivedAt"] - earlier["receivedAt"], 3)
                 for earlier, later in zip(requests, requests[1:])]
    minimum_gap = 64 if case["scenario"].startswith("no-response-") else 60 if "tps-" in case["scenario"] else 0
    if any(gap < minimum_gap for gap in intervals):
        raise AssertionError(f"{case['name']}: retry was too early: {intervals}")
    if case["scenario"].startswith("no-response-") and "firstTimeoutSeconds" not in case:
        raise AssertionError(f"{case['name']}: five-second timeout observation was not captured")
    callbacks = [event for event in events if event["kind"] == "carrier_webhook_accepted" and event["clientMsgId"] == identifier]
    if case["scenario"].startswith("webhook-"):
        expected_codes = [66001 if "carrier-" in case["scenario"] else 66002] * len(requests)
        if case["error"] is None:
            expected_codes[-1] = None
        if [event.get("errorCode") for event in callbacks] != expected_codes:
            raise AssertionError(f"{case['name']}: webhook result sequence differs")
    else:
        expected_callbacks = 1 if case["error"] is None and not case["secondary"] else 0
        if len(callbacks) != expected_callbacks or any(event.get("status") != "success" for event in callbacks):
            raise AssertionError(f"{case['name']}: unexpected provider webhook")
    secondary = case["secondary"]
    success = case["error"] is None
    expected = {"stage": "SECONDARY" if secondary else "PRIMARY", "outcome": "SUCCESS" if success else "FAILURE",
                "error": case["error"], "cleanup": "DONE", "billable": int(success and not secondary), "delivered": 1}
    if any(history.get(key) != value for key, value in expected.items()):
        raise AssertionError(f"{case['name']}: final history differs: {history}")
    if history["carrier"] != actual_carriers[-1] or history["invocation"] != (None if secondary else expected_invocations[-1]):
        raise AssertionError(f"{case['name']}: final carrier/invocation differs: {history}")
    tcp = [event for event in events if event["kind"] == "tcp_request" and event["clientMsgId"] == identifier]
    if len(tcp) != int(secondary) or (secondary and history["source"] != "TCP_RESPONSE"):
        raise AssertionError(f"{case['name']}: unexpected TCP fallback")
    customer = [result for event in events if event["kind"] == "customer_webhook"
                for result in event["body"].get("results", []) if result.get("clientMsgId") == identifier]
    if len(customer) != 1 or customer[0]["status"] != ("success" if success else "fail"):
        raise AssertionError(f"{case['name']}: customer result differs or was delivered more than once")
    if not success and customer[0].get("errorCode") != case["error"]:
        raise AssertionError(f"{case['name']}: customer error code differs")
    if origin_item(identifier):
        raise AssertionError(f"{case['name']}: ORIGIN remains after completion")
    return {"carriers": actual_carriers, "invocations": expected_invocations,
            "intervalSeconds": intervals, "history": history}


def run(timeout: int) -> None:
    wait_for_demo_services(180)
    for job in ("kt-sender", "lgu-sender", "tcp-sender"):
        wait_for_job(job, 180)
    token = request_json(API + "/api/v1/auth/token",
                         {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
    run_id = uuid.uuid4().hex[:8]
    scenarios = cases()
    destination = ROOT / ".monitoring/policy-latest.json"
    destination.parent.mkdir(exist_ok=True)
    report = {"runId": run_id, "pass": False, "cases": scenarios}

    def save():
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    save()
    try:
        for number, case in enumerate(scenarios):
            body = {"messageId": f"policy-{run_id}-{number:02d}", "recipientNumber": "010" + f"{number:08d}",
                    "messageCategory": "GENERAL", "payload": {"text": "policy verification", "simulatorScenario": case["scenario"]}}
            if case["secondary"]:
                body["secondarySendPayload"] = {"text": "policy fallback verification"}
            case["clientMsgId"] = request_json(API + "/api/v1/messages", body, token)["data"]["clientMsgId"]
            save()
        print(f"Policy test: admitted {len(scenarios)} cases, runId={run_id}", flush=True)
        deadline = time.monotonic() + timeout
        previous = None
        while time.monotonic() < deadline:
            events = mock_events()
            rows = histories(run_id)
            progress = []
            for case in scenarios:
                identifier = case["clientMsgId"]
                requests = [item for item in events if item["kind"] == "carrier_request" and item["clientMsgId"] == identifier]
                progress.append(f"{case['name']}={len(requests)}/{len(case['carriers'])}")
                if len(requests) > len(case["carriers"]):
                    raise AssertionError(f"{case['name']}: too many external sends")
                if case["scenario"].startswith("no-response-") and requests and "firstTimeoutSeconds" not in case:
                    observed = first_timeout(identifier)
                    if observed:
                        elapsed = datetime.fromisoformat(observed["observedAt"].replace("Z", "+00:00")).timestamp() - requests[0]["receivedAt"]
                        if observed["source"] != "HTTP_TIMEOUT" or observed["status"] != "TIMEOUT" or not 4 <= elapsed <= 7:
                            raise AssertionError(f"{case['name']}: invalid five-second timeout observation: {elapsed:.3f}s")
                        case["firstTimeoutSeconds"] = round(elapsed, 3)
                        save()
            if progress != previous:
                print("Policy test: " + ", ".join(progress), flush=True)
                previous = progress
            if len(rows) == len(scenarios) and all(row["cleanup"] == "DONE" and row["delivered"] == 1 for row in rows.values()):
                for case in scenarios:
                    case["evidence"] = verify_case(case, events, rows[case["clientMsgId"]])
                save()
                break
            time.sleep(3)
        else:
            raise TimeoutError("Policy cases did not finish within the configured deadline")
        # Observe beyond a full retry interval to catch an erroneously scheduled fifth send.
        print("Policy test: all results matched; observing 70 seconds for unwanted extra sends", flush=True)
        quiet_until = time.monotonic() + 70
        while time.monotonic() < quiet_until:
            time.sleep(min(5, max(0, quiet_until - time.monotonic())))
        events, rows = mock_events(), histories(run_id)
        for case in scenarios:
            case["evidence"] = verify_case(case, events, rows[case["clientMsgId"]])
        report.update({"pass": True, "completed": len(scenarios), "noExtraSendObservationSeconds": 70})
        save()
        print(f"Policy verification PASS: {len(scenarios)} cases; {destination}", flush=True)
    except Exception as failure:
        report["error"] = str(failure)
        save()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=420)
    arguments = parser.parse_args()
    run(arguments.timeout)
