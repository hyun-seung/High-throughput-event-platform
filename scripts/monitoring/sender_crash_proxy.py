#!/usr/bin/env python3
"""Local-only fault proxy: capture HTTP observations without forwarding their DynamoDB writes."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


STATE = Path("/state")
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        payload = json.loads(body)
        target = self.headers.get("X-Amz-Target", "")
        if target.endswith(".UpdateItem") and payload.get("TableName") == "STEP" \
                and "http_observation" in payload.get("UpdateExpression", ""):
            # The AP has already parsed the provider response, but DynamoDB has not seen this write.
            observation = json.loads(payload["ExpressionAttributeValues"][":observation"]["S"])
            with LOCK, (STATE / "captured.jsonl").open("a") as output:
                output.write(json.dumps(observation) + "\n")
            # Never forward a captured write, even if the caller dies or retries it.
            threading.Event().wait(30)
            self.send_response(503)
            self.end_headers()
            return
        headers = {k: v for k, v in self.headers.items() if k.lower() not in
                   ("host", "connection", "content-length")}
        request = Request("http://dynamodb-local:8000/", data=body, headers=headers)
        try:
            response = urlopen(request, timeout=10)
        except HTTPError as error:
            response = error
        with response:
            result = response.read()
            self.send_response(response.status)
            self.send_header("Content-Type", response.headers.get("Content-Type", "application/x-amz-json-1.0"))
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            self.wfile.write(result)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
