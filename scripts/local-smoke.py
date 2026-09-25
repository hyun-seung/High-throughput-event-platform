#!/usr/bin/env python3
"""Verify the local API -> Kafka -> workers -> mock provider path."""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


API = "http://localhost:" + os.environ.get("DELIVERY_API_PORT", "8080")
DYNAMODB = "http://localhost:" + os.environ.get("DYNAMODB_HOST_PORT", "8000")
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
METRICS_PORTS = {
    "api": os.environ.get("DELIVERY_API_METRICS_PORT", "19080"),
    "ingress": os.environ.get("INGRESS_METRICS_PORT", "19081"),
    "dispatch": os.environ.get("DISPATCH_METRICS_PORT", "19082"),
    "simulator": os.environ.get("SIMULATOR_METRICS_PORT", "19090"),
}


def management(app, path, token=None):
    headers = {"Authorization": "Bearer " + token} if app == "api" and token else {}
    request = urllib.request.Request("http://127.0.0.1:" + METRICS_PORTS[app] + path, headers=headers)
    with HTTP.open(request, timeout=5) as response:
        return response.read().decode()


def metric_value(scrape, name, **labels):
    total = 0.0
    for line in scrape.splitlines():
        if not line.startswith(name + "{") and not line.startswith(name + " "):
            continue
        actual = dict(re.findall(r'([a-zA-Z_][a-zA-Z_0-9]*)="([^"\\]*)"', line))
        if all(actual.get(key) == value for key, value in labels.items()):
            total += float(line.rsplit(" ", 1)[1])
    return total


def metrics_snapshot(token):
    return {app: management(app, "/actuator/prometheus", token) for app in METRICS_PORTS}


def verify_metrics(token, before, attempt):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        after = metrics_snapshot(token)
        duplicate_delta = metric_value(after["dispatch"], "delivery_outcomes_total", outcome="dispatch_duplicate") \
            - metric_value(before["dispatch"], "delivery_outcomes_total", outcome="dispatch_duplicate")
        if duplicate_delta >= 1:
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("Duplicate replay was not observed by Dispatch metrics")
    required = [
        ("api", "delivery_stage_duration_seconds_count", {"stage": "api_publish", "result": "success"}, 2),
        ("api", "http_server_requests_seconds_count", {"uri": "/api/v1/deliveries", "status": "202"}, 2),
        ("ingress", "delivery_stage_duration_seconds_count", {"stage": "ingress_store", "result": "success"}, 2),
        ("ingress", "delivery_stage_duration_seconds_count", {"stage": "ingress_publish", "result": "success"}, 2),
        ("dispatch", "delivery_outcomes_total", {"outcome": "dispatch_accepted"}, 1),
        ("dispatch", "delivery_acceptance_latency_seconds_count", {}, 1),
        ("dispatch", "delivery_dynamodb_duration_seconds_count", {"operation": "update_item", "result": "success"}, 2),
        ("dispatch", "delivery_dynamodb_duration_seconds_count", {"operation": "update_item", "result": "condition_failed"}, 1),
    ]
    for app, name, labels, expected in required:
        delta = metric_value(after[app], name, **labels) - metric_value(before[app], name, **labels)
        if delta < expected:
            raise RuntimeError(f"{app} {name} {labels}: expected delta >= {expected}, got {delta}")
    for app in ("api", "ingress", "dispatch"):
        if "delivery_stage_duration_seconds_bucket{" not in after[app]:
            raise RuntimeError(f"{app}: latency histogram not exposed")
    counts = json.loads(management("simulator", "/actuator/simulator/" + attempt["attempt_id"]["S"]))
    if counts != {"calls": 1, "effects": 1}:
        raise RuntimeError(f"Expected exactly one provider call and effect without provider deduplication, got {counts}")
    print("PASS: stage/HTTP/DB metrics, latency histograms, duplicate skip and one provider effect without deduplication")


def post(path, payload, expected, headers=None):
    request = urllib.request.Request(
        API + path, json.dumps(payload).encode(),
        {"Content-Type": "application/json", **(headers or {})},
    )
    with HTTP.open(request, timeout=10) as response:
        if response.status != expected:
            raise RuntimeError(f"{path}: expected HTTP {expected}, got {response.status}")
        return json.load(response)["data"]


def state(delivery_id, table):
    query = {
        "TableName": table, "ConsistentRead": True,
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "DELIVERY#" + delivery_id}},
    }
    result = subprocess.run([
        "curl", "--noproxy", "*", "--max-time", "5", "--silent", "--show-error",
        "--fail-with-body", "--aws-sigv4", "aws:amz:ap-northeast-2:dynamodb",
        "--user", "local:local", "-H", "Content-Type: application/x-amz-json-1.0",
        "-H", "X-Amz-Target: DynamoDB_20120810.Query",
        "-d", json.dumps(query), DYNAMODB + "/",
    ], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)["Items"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", action="store_true", help="Also verify stage metrics and simulator effects")
    args = parser.parse_args()
    token = post("/api/v1/auth/token", {
        "username": "local-user", "password": "local-password",
    }, 200)["accessToken"]
    print("PASS: local-user authentication", flush=True)
    before = None
    if args.metrics:
        try:
            management("api", "/actuator/prometheus")
        except urllib.error.HTTPError as error:
            if error.code != 401:
                raise
        else:
            raise RuntimeError("API management endpoint unexpectedly accessible without authentication")
        if json.loads(management("simulator", "/actuator/simulator"))["deduplicate"]:
            raise RuntimeError("Metrics smoke requires SIMULATOR_DEDUPLICATE=false")
        before = metrics_snapshot(token)
    headers = {
        "Authorization": "Bearer " + token,
        "Idempotency-Key": "local-smoke-" + uuid.uuid4().hex,
    }
    payload = {"deliveryType": "EMAIL", "payload": {"message": "local smoke verification"}}
    first = post("/api/v1/deliveries", payload, 202, headers)
    second = post("/api/v1/deliveries", payload, 202, headers)
    if first["deliveryId"] != second["deliveryId"]:
        raise RuntimeError("Repeated idempotency key returned a different deliveryId")
    delivery_id = first["deliveryId"]
    print(f"PASS: two HTTP 202 responses with deliveryId={delivery_id}", flush=True)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        items = state(delivery_id, "ORIGIN")
        meta = [item for item in items if item["sk"]["S"] == "META"]
        execution_id = meta[0].get("delivery_id", {}).get("S", delivery_id) if meta else delivery_id
        execution_items = state(execution_id, "STEP")
        attempts = [item for item in execution_items if item["sk"]["S"].startswith("ATTEMPT#")]
        if len(attempts) > 1:
            raise RuntimeError("More than one dispatch attempt exists")
        if meta and attempts and attempts[0].get("status", {}).get("S") == "ACCEPTED":
            if "provider_processed_at" not in attempts[0]:
                raise RuntimeError("Accepted attempt has no provider timestamp")
            print("PASS: DynamoDB META and one ACCEPTED attempt with provider timestamp")
            if args.metrics:
                verify_metrics(token, before, attempts[0])
            return
        time.sleep(1)
    raise RuntimeError("Timed out waiting for META and ACCEPTED attempt; inspect worker logs")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
