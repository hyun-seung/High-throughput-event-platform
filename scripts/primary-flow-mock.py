#!/usr/bin/env python3
"""Loopback carrier and customer endpoints for the first-send local flow."""

import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen


MAX_BODY_BYTES = 262144


class Events:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()

    def write(self, kind: str, **details):
        with self.lock, self.path.open("a", encoding="utf-8") as output:
            output.write(json.dumps({"kind": kind, **details}, ensure_ascii=False) + "\n")


def read_body(handler: BaseHTTPRequestHandler) -> bytes:
    if handler.headers.get("Transfer-Encoding", "").lower() == "chunked":
        chunks = []
        total = 0
        while True:
            size = int(handler.rfile.readline().strip().split(b";", 1)[0], 16)
            if size == 0:
                while handler.rfile.readline().strip():
                    pass
                break
            total += size
            if total > MAX_BODY_BYTES:
                raise ValueError("request too large")
            chunks.append(handler.rfile.read(size))
            if handler.rfile.read(2) != b"\r\n":
                raise ValueError("invalid chunk")
        return b"".join(chunks)
    length = int(handler.headers.get("Content-Length", "0"))
    if length < 1 or length > MAX_BODY_BYTES:
        raise ValueError("invalid content length")
    return handler.rfile.read(length)


def handler_for(kind: str, events: Events, webhook_base_url: str, secret: str,
                scenario: str, carrier: str = ""):
    counts = {}
    count_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            expected = "/api/v1/messages" if kind == "carrier" else "/hook"
            if self.path != expected:
                self.send_error(404)
                return
            try:
                payload = json.loads(read_body(self))
                if kind == "carrier":
                    client_msg_id = payload["clientMsgId"]
                    if not isinstance(client_msg_id, str) or not client_msg_id:
                        raise ValueError("missing clientMsgId")
            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                self.send_error(400)
                return

            if kind == "carrier":
                with count_lock:
                    invocation = counts.get(client_msg_id, 0) + 1
                    counts[client_msg_id] = invocation
                events.write("carrier_request", clientMsgId=client_msg_id, carrier=carrier,
                             invocation=invocation, receivedAt=time.time(), request=payload)
                no_response = carrier == "SKT" and (
                    scenario == "no-response-exhausted"
                    or (scenario == "no-response-retry" and invocation == 1)
                )
                if no_response:
                    time.sleep(7)
                    return
                mismatch = (scenario == "carrier-mismatch" and carrier == "SKT") \
                    or scenario == "carrier-exhausted"
                tps = carrier == "SKT" and (scenario == "tps-exhausted"
                    or (scenario == "tps-retry" and invocation == 1))
                if mismatch or tps:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    code = "41001" if mismatch else "42002"
                    self.wfile.write(json.dumps({"status": "4xx", "error": {
                        "code": code, "message": "not our carrier" if mismatch else "TPS exceeded",
                    }}).encode())
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")
                threading.Thread(target=send_webhook, args=(client_msg_id,), daemon=True).start()
            else:
                events.write("customer_webhook", body=payload)
                self.send_response(204)
                self.end_headers()

        def log_message(self, format_string, *args):
            return

    def send_webhook(client_msg_id: str):
        webhook_url = webhook_base_url + "/" + carrier.lower()
        body = json.dumps([{"clientMsgId": client_msg_id, "status": "success"}]).encode()
        for attempt in range(20):
            time.sleep(0.5 if attempt == 0 else 1)
            request = Request(webhook_url, data=body, method="POST", headers={
                "Content-Type": "application/json", "Authorization": "Bearer " + secret,
            })
            try:
                with urlopen(request, timeout=5) as response:
                    if response.status == 202:
                        events.write("carrier_webhook_accepted", clientMsgId=client_msg_id,
                                     carrier=carrier)
                        return
            except OSError:
                pass
        events.write("carrier_webhook_failed", clientMsgId=client_msg_id, carrier=carrier)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--events-file", type=Path, required=True)
    parser.add_argument("--webhook-base-url", required=True)
    parser.add_argument("--scenario", choices=("success", "carrier-mismatch",
                                                "carrier-exhausted", "tps-retry", "tps-exhausted",
                                                "no-response-retry", "no-response-exhausted"),
                        default="success")
    args = parser.parse_args()
    secrets_by_carrier = {carrier: os.environ.get(f"FLOW_WEBHOOK_SECRET_{carrier}", "")
                          for carrier in ("SKT", "KT", "LGU")}
    if any(len(secret) < 32 for secret in secrets_by_carrier.values()):
        parser.error("Each FLOW_WEBHOOK_SECRET_<CARRIER> must contain at least 32 characters")
    args.events_file.touch()
    events = Events(args.events_file)
    carriers = {name: ThreadingHTTPServer(("127.0.0.1", 0),
                handler_for("carrier", events, args.webhook_base_url, secrets_by_carrier[name],
                            args.scenario, name)) for name in ("SKT", "KT", "LGU")}
    customer = ThreadingHTTPServer(("127.0.0.1", 0),
                                   handler_for("customer", events, args.webhook_base_url,
                                               "", args.scenario))
    for server in carriers.values():
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
    customer.daemon_threads = True
    threading.Thread(target=customer.serve_forever, daemon=True).start()
    ready = {"carrierUrls": {name: f"http://127.0.0.1:{server.server_port}"
                              for name, server in carriers.items()},
             "customerUrl": f"http://127.0.0.1:{customer.server_port}/hook"}
    temporary = args.ready_file.with_suffix(".tmp")
    temporary.write_text(json.dumps(ready), encoding="utf-8")
    os.replace(temporary, args.ready_file)
    try:
        threading.Event().wait()
    finally:
        for server in carriers.values():
            server.shutdown()
        customer.shutdown()


if __name__ == "__main__":
    main()
