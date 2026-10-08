#!/usr/bin/env python3
"""Verify webhook batches, duplicates, ordering and SQL outage recovery in the isolated local stack."""

import hashlib
import json
import subprocess
import time
import uuid

from main_flow import API, ROOT, compose, request_json, sql, wait_for_demo_services
from policy_flow import mock_events


def eventually(description, probe, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(2)
    raise TimeoutError(description)


def name_uuid(value):
    return str(uuid.UUID(bytes=hashlib.md5(value.encode()).digest(), version=3))


def dynamo(operation, payload):
    response = subprocess.run(["curl", "--silent", "--show-error", "--fail-with-body", "--max-time", "10",
        "--aws-sigv4", "aws:amz:ap-northeast-2:dynamodb", "--user", "local:local",
        "-H", "X-Amz-Target: DynamoDB_20120810." + operation,
        "-H", "Content-Type: application/x-amz-json-1.0", "--data", json.dumps(payload),
        "http://127.0.0.1:38000/"], capture_output=True, text=True, check=True, timeout=15)
    return json.loads(response.stdout)


def read_items(table, keys):
    rows = []
    for start in range(0, len(keys), 100):
        pending = {table: {"Keys": keys[start:start + 100], "ConsistentRead": True}}
        for attempt in range(6):
            response = dynamo("BatchGetItem", {"RequestItems": pending})
            rows.extend(response.get("Responses", {}).get(table, []))
            pending = response.get("UnprocessedKeys", {})
            if not pending:
                break
            time.sleep(0.1 * 2 ** attempt)
        else:
            raise TimeoutError("DynamoDB batch read still has unprocessed keys")
    return rows


def key(identifier, sk):
    return {"pk": {"S": "DELIVERY#" + identifier}, "sk": {"S": sk}}


def field(row, name):
    return row.get(name, {}).get("S")


def post_webhook(results):
    # Use the mock's existing local credential and Docker network; no extra host port is needed.
    code = """
import json, os, sys
from urllib.request import Request, urlopen
request = Request('http://webhook-receive:8099/api/v1/message-webhooks/skt',
    data=sys.argv[1].encode(), headers={'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + os.environ['FLOW_WEBHOOK_SECRET_SKT']})
with urlopen(request, timeout=15) as response:
    assert response.status == 202, response.status
    print(response.read().decode())
"""
    accepted = json.loads(compose("exec", "-T", "mock", "python", "-c", code, json.dumps(results)))
    assert accepted["status"] == "RECEIVED"
    return accepted["traceId"]


def result_offset():
    raw = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                  "--bootstrap-server", "kafka:29092", "--topic", "MSG_RESULT")
    offsets = [int(line.rsplit(":", 1)[1]) for line in raw.splitlines() if line.startswith("MSG_RESULT:")]
    if not offsets:
        raise AssertionError("No MSG_RESULT partitions found")
    return sum(offsets)


def completion_lag():
    raw = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                  "--bootstrap-server", "kafka:29092", "--describe", "--group", "messaging-complete-manager")
    rows = [line.split() for line in raw.splitlines() if line.startswith("messaging-complete-manager ")]
    rows = [row for row in rows if row[1] == "MSG-RESULT-FINALIZED"]
    if not rows or any(row[5] == "-" for row in rows):
        raise AssertionError("No committed completion offsets found")
    return sum(int(row[5]) for row in rows)


def postgres_ready():
    try:
        return bool(compose("exec", "-T", "postgres", "pg_isready", "-U", "delivery"))
    except RuntimeError:
        return False


def histories(run_id):
    raw = sql("SELECT COALESCE(json_agg(json_build_object('id',m.client_msg_id,'outcome',m.outcome,"
              "'stage',m.final_stage,'source',m.result_source,'carrier',m.carrier,'invocation',m.invocation,"
              "'error',m.error_code,'cleanup',m.cleanup_status,'billable',(SELECT count(*) "
              "FROM messaging_completion.tbl_cdr_hist c WHERE c.client_msg_id=m.client_msg_id),"
              "'delivered',(SELECT count(*) FROM messaging_webhook.tbl_webhook_outbox o "
              "WHERE o.client_msg_id=m.client_msg_id AND o.status='DELIVERED' AND EXISTS "
              "(SELECT 1 FROM messaging_webhook.tbl_webhook_hist h WHERE h.batch_id=o.batch_id "
              "AND h.acknowledged))))::text,'[]') FROM messaging_completion.tbl_msg_hist m "
              f"WHERE m.message_id LIKE 'webhook-{run_id}-%'")
    return {row["id"]: row for row in json.loads(raw)}


def verify_final(report):
    identifiers = report["clientMsgIds"]
    rows = histories(report["runId"])
    expected_ids = identifiers + [report["earlyClientMsgId"]]
    assert set(rows) == set(expected_ids), "Missing or unexpected final histories"
    for number, identifier in enumerate(expected_ids):
        success = number >= 100 or number % 2 == 0
        expected = {"stage": "PRIMARY", "outcome": "SUCCESS" if success else "FAILURE", "source": "WEBHOOK",
                    "carrier": "SKT", "invocation": 1, "error": None if success else 66999,
                    "cleanup": "DONE", "billable": int(success), "delivered": 1}
        assert all(rows[identifier][k] == v for k, v in expected.items()), rows[identifier]
    assert not read_items("ORIGIN", [key(identifier, "META") for identifier in expected_ids]), "ORIGIN remains"
    events = mock_events()
    for identifier in expected_ids:
        sent = [e for e in events if e["kind"] == "carrier_request" and e["clientMsgId"] == identifier]
        assert len(sent) == 1 and sent[0]["carrier"] == "SKT", f"Unexpected send: {identifier}"
        assert not any(e["kind"] == "tcp_request" and e["clientMsgId"] == identifier for e in events)
        customer = [r for e in events if e["kind"] == "customer_webhook" for r in e["body"]["results"]
                    if r["clientMsgId"] == identifier]
        assert len(customer) == 1, f"Customer webhook count: {identifier}: {len(customer)}"
        assert customer[0]["status"] == rows[identifier]["outcome"].lower().replace("failure", "fail")
        if rows[identifier]["error"]:
            assert customer[0]["errorCode"] == rows[identifier]["error"]
    assert completion_lag() == 0, "Finalized results remain uncommitted"
    return {"messages": len(rows), "billable": sum(r["billable"] for r in rows.values()),
            "customerResults": len(rows), "originRemoved": len(rows), "completionLag": 0}


def run():
    wait_for_demo_services(180)
    report = {"runId": uuid.uuid4().hex[:8], "pass": False, "clientMsgIds": []}
    destination = ROOT / ".monitoring/webhook-latest.json"
    destination.parent.mkdir(exist_ok=True)

    def checkpoint(stage):
        report["stage"] = stage
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Webhook test: " + stage, flush=True)

    def admitted(token, number, scenario):
        body = {"messageId": f"webhook-{report['runId']}-{number:03d}", "recipientNumber": "010" + f"{number:08d}",
                "messageCategory": "GENERAL", "payload": {"text": "webhook verification", "simulatorScenario": scenario}}
        return request_json(API + "/api/v1/messages", body, token)["data"]["clientMsgId"]

    checkpoint("starting")
    try:
        assert completion_lag() == 0, "Run on an idle monitoring stack"
        token = request_json(API + "/api/v1/auth/token",
                             {"username": "local-user", "password": "local-password"})["data"]["accessToken"]
        early = admitted(token, 100, "webhook-before-response")
        report["earlyClientMsgId"] = early
        eventually("Early webhook did not complete", lambda: histories(report["runId"]).get(early, {}).get("cleanup") == "DONE")
        events = [e for e in mock_events() if e.get("clientMsgId") == early]
        accepted = next(e for e in events if e["kind"] == "carrier_webhook_accepted")
        response = next(e for e in events if e["kind"] == "carrier_response_sent")
        assert accepted["acceptedAt"] < response["sentAt"], "Webhook did not precede HTTP response"
        report["webhookBeforeHttpResponse"] = True
        for number in range(100):
            report["clientMsgIds"].append(admitted(token, number, "manual-webhook"))
        identifiers = report["clientMsgIds"]
        http_keys = [key(identifier, "HTTP#SKT#" + name_uuid("primary-http:" + identifier + ":SKT") + "#1")
                     for identifier in identifiers]

        def all_accepted():
            rows = read_items("STEP", http_keys)
            return len(rows) == 100 and all(field(row, "status") == "OBSERVED"
                and json.loads(field(row, "http_observation"))["status"] == "ACCEPTED" for row in rows)

        eventually("100 HTTP acceptances were not recorded", all_accepted)
        checkpoint("100-http-acceptances-recorded")
        results = [{"clientMsgId": identifier, "status": "success" if n % 2 == 0 else "fail",
                    **({} if n % 2 == 0 else {"error": {"code": 66999, "message": "simulated final failure"}})}
                   for n, identifier in enumerate(identifiers)]
        try:
            compose("stop", "postgres", timeout=60)
            checkpoint("postgres-stopped")
            before = result_offset()
            report["batchTraceId"] = post_webhook(results)
            after = result_offset()
            assert after - before == 1, "100 results did not produce exactly one Kafka record"
            report["batchKafkaRecords"] = after - before
            report["duplicateTraceId"] = post_webhook(results)
            assert result_offset() - after == 1, "Duplicate batch did not produce one Kafka record"

            def preserved():
                rows = read_items("ORIGIN", [key(identifier, "META") for identifier in identifiers])
                return len(rows) == 100 and all(field(row, "status") in ("PRIMARY_SUCCEEDED", "PRIMARY_FAILED") for row in rows)

            eventually("Final decisions were not preserved during SQL outage", preserved)
            # Wait for publication of every final record; the SQL consumer must not skip any of them.
            eventually("Completion consumer skipped or did not receive final results", lambda: completion_lag() == 100)
            report["outagePreservedOrigins"] = 100
            report["outageCompletionLag"] = 100
            checkpoint("100-results-preserved-uncommitted-during-sql-outage")
        finally:
            compose("start", "postgres", timeout=60)
            eventually("PostgreSQL did not recover", postgres_ready)
        checkpoint("postgres-restored")

        def all_completed():
            rows = histories(report["runId"])
            return len(rows) == 101 and all(row["cleanup"] == "DONE" and row["delivered"] == 1 for row in rows.values())

        eventually("SQL histories or customer results did not recover", all_completed)
        report["evidence"] = verify_final(report)
        report["lateDuplicateTraceId"] = post_webhook(results)
        unknown = uuid.uuid4().hex
        late = [{"clientMsgId": identifiers[0], "status": "fail", "error": {"code": 66002, "message": "late failure"}},
                {"clientMsgId": identifiers[1], "status": "success"}, {"clientMsgId": unknown, "status": "success"}]
        report["lateConflictTraceId"] = post_webhook(late)
        report["unknownClientMsgId"] = unknown
        late_keys = [key(identifier, "RESULT_INBOX#" + name_uuid("webhook-result:" + trace + ":" + identifier))
                     for trace, ids in ((report["lateDuplicateTraceId"], identifiers),
                                        (report["lateConflictTraceId"], [r["clientMsgId"] for r in late]))
                     for identifier in ids]

        def late_processed():
            rows = read_items("STEP", late_keys)
            return len(rows) == 103 and all(field(row, "status") == "PROCESSED"
                and int(row.get("ttl_epoch_seconds", {}).get("N", 0)) > time.time() + 6 * 86400 for row in rows)

        eventually("Late/unknown webhooks were not processed with retention", late_processed)
        checkpoint("late-duplicate-conflicting-and-unknown-results-observed; checking-70-seconds-for-extra-sends")
        until = time.monotonic() + 70
        while time.monotonic() < until:
            time.sleep(min(5, max(0, until - time.monotonic())))
        report["evidence"] = verify_final(report)
        assert not read_items("ORIGIN", [key(unknown, "META")]), "Unknown webhook created an origin"
        assert sql(f"SELECT count(*) FROM messaging_completion.tbl_msg_hist WHERE client_msg_id='{unknown}'") == "0"
        report.update({"pass": True, "lateProcessedItems": 103, "noExtraSendObservationSeconds": 70})
        checkpoint("complete")
        print(f"Webhook verification PASS: {destination}", flush=True)
    except BaseException as failure:
        report["error"] = str(failure)
        checkpoint("failed")
        raise


if __name__ == "__main__":
    run()
