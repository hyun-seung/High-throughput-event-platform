#!/usr/bin/env python3
"""Verify the local API -> Kafka -> workers -> mock provider path."""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


API = "http://localhost:" + os.environ.get("DELIVERY_API_PORT", "8080")
DYNAMODB = "http://localhost:" + os.environ.get("DYNAMODB_HOST_PORT", "8000")
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def post(path, payload, expected, headers=None):
    request = urllib.request.Request(
        API + path, json.dumps(payload).encode(),
        {"Content-Type": "application/json", **(headers or {})},
    )
    with HTTP.open(request, timeout=10) as response:
        if response.status != expected:
            raise RuntimeError(f"{path}: expected HTTP {expected}, got {response.status}")
        return json.load(response)["data"]


def state(delivery_id):
    query = {
        "TableName": "delivery_state", "ConsistentRead": True,
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
    token = post("/api/v1/auth/token", {
        "username": "local-user", "password": "local-password",
    }, 200)["accessToken"]
    print("PASS: local-user authentication", flush=True)
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
        items = state(delivery_id)
        meta = [item for item in items if item["sk"]["S"] == "META"]
        attempts = [item for item in items if item["sk"]["S"].startswith("ATTEMPT#")]
        if len(attempts) > 1:
            raise RuntimeError("More than one dispatch attempt exists")
        if meta and attempts and attempts[0].get("status", {}).get("S") == "ACCEPTED":
            if "provider_processed_at" not in attempts[0]:
                raise RuntimeError("Accepted attempt has no provider timestamp")
            print("PASS: DynamoDB META and one ACCEPTED attempt with provider timestamp")
            return
        time.sleep(1)
    raise RuntimeError("Timed out waiting for META and ACCEPTED attempt; inspect worker logs")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
