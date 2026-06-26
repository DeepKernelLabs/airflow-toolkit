#!/usr/bin/env python3
"""
Minimal webhook receiver for demo purposes.

Accepts POST requests on any path and logs the payload to stdout,
so it appears in `docker compose logs webhook-logger`.

Used as a local stand-in for Slack, Teams, and Discord webhooks.
"""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")

        channel = self.path.lstrip("/").upper() or "UNKNOWN"
        separator = "─" * 60

        try:
            payload = json.loads(body)
            pretty = json.dumps(payload, indent=2, ensure_ascii=False)
        except json.JSONDecodeError:
            pretty = body

        logging.info(
            "\n%s\n  [%s] Webhook received\n%s\n%s\n%s",
            separator,
            channel,
            separator,
            pretty,
            separator,
        )

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, format, *args) -> None:  # suppress default access log
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8099), WebhookHandler)
    logging.info("Webhook logger ready on http://0.0.0.0:8099")
    logging.info("  POST /slack   → Slack payload")
    logging.info("  POST /teams   → Teams payload")
    logging.info("  POST /discord → Discord payload")
    server.serve_forever()
