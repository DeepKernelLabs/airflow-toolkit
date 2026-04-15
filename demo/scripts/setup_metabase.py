#!/usr/bin/env python3
"""
One-shot Metabase initialisation for the airflow-toolkit demo.

Creates:
  · Database connection → demo_dw (gold schema)
  · 3 questions: Overview table, Posts bar chart, Completion rate bar chart
  · 1 dashboard: airflow-toolkit · User Activity

Idempotent: safe to run on every `docker compose up`.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Config (from env vars set in docker-compose)
# ---------------------------------------------------------------------------
METABASE_URL   = os.getenv("METABASE_URL",       "http://metabase:3000")
MB_EMAIL       = os.getenv("MB_ADMIN_EMAIL",      "admin@demo.local")
MB_PASSWORD    = os.getenv("MB_ADMIN_PASSWORD",   "metabase123")
DB_HOST        = os.getenv("DW_HOST",             "postgres")
DB_PORT        = int(os.getenv("DW_PORT",         "5432"))
DB_NAME        = os.getenv("DW_DB",               "demo_dw")
DB_USER        = os.getenv("DW_USER",             "dw_user")
DB_PASSWORD    = os.getenv("DW_PASSWORD",         "dw_password")
DB_LABEL       = "demo_dw"
DASHBOARD_NAME = "airflow-toolkit · User Activity"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _request(method: str, path: str, data=None, token: str | None = None) -> dict:
    url = f"{METABASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get(path, token=None):        return _request("GET",  path, token=token)
def post(path, data, token=None): return _request("POST", path, data=data, token=token)
def put(path, data, token=None):  return _request("PUT",  path, data=data, token=token)

# ---------------------------------------------------------------------------
# Wait for Metabase to be ready
# ---------------------------------------------------------------------------

def wait_ready(timeout: int = 300):
    print("Waiting for Metabase …", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if get("/api/health").get("status") == "ok":
                print("Metabase is ready.", flush=True)
                return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError("Metabase did not become ready in time.")

# ---------------------------------------------------------------------------
# Setup / authentication
# ---------------------------------------------------------------------------

def get_session_token() -> str:
    """Complete first-time setup if needed, then return a session token."""
    props = get("/api/session/properties")
    setup_token = props.get("setup-token")

    if setup_token:
        print("Running first-time Metabase setup …", flush=True)
        try:
            post("/api/setup", {
                "token": setup_token,
                "user": {
                    "email":      MB_EMAIL,
                    "password":   MB_PASSWORD,
                    "first_name": "Admin",
                    "last_name":  "Demo",
                    "site_name":  "airflow-toolkit demo",
                },
                "prefs": {
                    "site_name":      "airflow-toolkit demo",
                    "allow_tracking": "false",
                },
            })
            print("Setup complete.", flush=True)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                print("Setup already done, continuing …", flush=True)
            else:
                raise

    resp = post("/api/session", {"username": MB_EMAIL, "password": MB_PASSWORD})
    return resp["id"]

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_or_create_database(token: str) -> int:
    result = get("/api/database", token=token)
    databases = result.get("data", result) if isinstance(result, dict) else result

    for db in databases:
        if db.get("name") == DB_LABEL:
            print(f"Database '{DB_LABEL}' already connected (id={db['id']}).", flush=True)
            return db["id"]

    print(f"Connecting database '{DB_LABEL}' …", flush=True)
    db = post("/api/database", {
        "engine": "postgres",
        "name":   DB_LABEL,
        "details": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "dbname":   DB_NAME,
            "user":     DB_USER,
            "password": DB_PASSWORD,
            "schema-filters-type":     "inclusion",
            "schema-filters-patterns": "gold",
        },
    }, token=token)
    db_id = db["id"]
    print(f"Database connected (id={db_id}). Syncing schema …", flush=True)
    post(f"/api/database/{db_id}/sync_schema", {}, token=token)
    time.sleep(8)
    return db_id

# ---------------------------------------------------------------------------
# Questions (native SQL — no dependency on dynamic field IDs)
# ---------------------------------------------------------------------------

QUESTIONS = [
    {
        "name": "User Activity — Overview",
        "display": "table",
        "sql": (
            "SELECT name, posts_count, comments_count, albums_count,\n"
            "       todos_count, todos_completed, todos_completion_pct\n"
            "FROM gold.gold__user_activity\n"
            "ORDER BY todos_completion_pct DESC"
        ),
        "viz": {},
    },
    {
        "name": "Posts by User",
        "display": "bar",
        "sql": (
            "SELECT name, posts_count\n"
            "FROM gold.gold__user_activity\n"
            "ORDER BY posts_count DESC"
        ),
        "viz": {
            "graph.dimensions":       ["name"],
            "graph.metrics":          ["posts_count"],
            "graph.x_axis.title_text": "User",
            "graph.y_axis.title_text": "Posts",
        },
    },
    {
        "name": "Todo Completion Rate (%)",
        "display": "bar",
        "sql": (
            "SELECT name, todos_completion_pct\n"
            "FROM gold.gold__user_activity\n"
            "ORDER BY todos_completion_pct DESC"
        ),
        "viz": {
            "graph.dimensions":       ["name"],
            "graph.metrics":          ["todos_completion_pct"],
            "graph.x_axis.title_text": "User",
            "graph.y_axis.title_text": "Completion %",
        },
    },
]


def get_or_create_questions(token: str, db_id: int) -> list[int]:
    existing = {c["name"]: c["id"] for c in get("/api/card", token=token)}
    card_ids = []
    for q in QUESTIONS:
        if q["name"] in existing:
            print(f"  · '{q['name']}' already exists.", flush=True)
            card_ids.append(existing[q["name"]])
            continue
        card = post("/api/card", {
            "name":    q["name"],
            "display": q["display"],
            "dataset_query": {
                "type":     "native",
                "native":   {"query": q["sql"], "template-tags": {}},
                "database": db_id,
            },
            "visualization_settings": q["viz"],
        }, token=token)
        print(f"  · Created '{q['name']}' (id={card['id']}).", flush=True)
        card_ids.append(card["id"])
    return card_ids

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def _add_cards_to_dashboard(dash_id: int, card_ids: list[int], token: str):
    """Add the three cards to a dashboard (layout: overview top, two charts below)."""
    cards = [
        {"id": -1, "card_id": card_ids[0], "row": 0, "col": 0,  "size_x": 24, "size_y": 8},
        {"id": -2, "card_id": card_ids[1], "row": 8, "col": 0,  "size_x": 12, "size_y": 8},
        {"id": -3, "card_id": card_ids[2], "row": 8, "col": 12, "size_x": 12, "size_y": 8},
    ]
    put(f"/api/dashboard/{dash_id}/cards", {"cards": cards}, token=token)


def get_or_create_dashboard(token: str, card_ids: list[int]):
    dashboards = get("/api/dashboard", token=token)
    for d in dashboards:
        if d.get("name") == DASHBOARD_NAME:
            # Check if cards were added (may have been empty from a previous failed run)
            detail = get(f"/api/dashboard/{d['id']}", token=token)
            if len(detail.get("dashcards", [])) >= len(QUESTIONS):
                print(f"Dashboard '{DASHBOARD_NAME}' already complete.", flush=True)
                return
            print(f"Dashboard '{DASHBOARD_NAME}' exists but has no cards — adding them …", flush=True)
            _add_cards_to_dashboard(d["id"], card_ids, token)
            print("Cards added.", flush=True)
            return

    print(f"Creating dashboard '{DASHBOARD_NAME}' …", flush=True)
    dash = post("/api/dashboard", {"name": DASHBOARD_NAME}, token=token)
    dash_id = dash["id"]
    _add_cards_to_dashboard(dash_id, card_ids, token)
    print(f"Dashboard created (id={dash_id}).", flush=True)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    wait_ready()
    token    = get_session_token()
    db_id    = get_or_create_database(token)
    print("Creating questions …", flush=True)
    card_ids = get_or_create_questions(token, db_id)
    get_or_create_dashboard(token, card_ids)
    print(f"\nDone — open {METABASE_URL} and go to Dashboards → '{DASHBOARD_NAME}'", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
