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


def handler_for(kind: str, events: Events, webhook_url: str, secret: str):
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
                events.write("carrier_request", clientMsgId=client_msg_id, request=payload)
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
        body = json.dumps([{"clientMsgId": client_msg_id, "status": "success"}]).encode()
        for attempt in range(20):
            time.sleep(0.5 if attempt == 0 else 1)
            request = Request(webhook_url, data=body, method="POST", headers={
                "Content-Type": "application/json", "Authorization": "Bearer " + secret,
            })
            try:
                with urlopen(request, timeout=5) as response:
                    if response.status == 202:
                        events.write("carrier_webhook_accepted", clientMsgId=client_msg_id)
                        return
            except OSError:
                pass
        events.write("carrier_webhook_failed", clientMsgId=client_msg_id)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--events-file", type=Path, required=True)
    parser.add_argument("--webhook-url", required=True)
    args = parser.parse_args()
    secret = os.environ.get("FLOW_WEBHOOK_SECRET", "")
    if len(secret) < 32:
        parser.error("FLOW_WEBHOOK_SECRET must contain at least 32 characters")
    args.events_file.touch()
    events = Events(args.events_file)
    carrier = ThreadingHTTPServer(("127.0.0.1", 0),
                                  handler_for("carrier", events, args.webhook_url, secret))
    customer = ThreadingHTTPServer(("127.0.0.1", 0),
                                   handler_for("customer", events, args.webhook_url, secret))
    carrier.daemon_threads = True
    customer.daemon_threads = True
    threading.Thread(target=carrier.serve_forever, daemon=True).start()
    threading.Thread(target=customer.serve_forever, daemon=True).start()
    ready = {"carrierUrl": f"http://127.0.0.1:{carrier.server_port}",
             "customerUrl": f"http://127.0.0.1:{customer.server_port}/hook"}
    temporary = args.ready_file.with_suffix(".tmp")
    temporary.write_text(json.dumps(ready), encoding="utf-8")
    os.replace(temporary, args.ready_file)
    try:
        threading.Event().wait()
    finally:
        carrier.shutdown()
        customer.shutdown()


if __name__ == "__main__":
    main()
