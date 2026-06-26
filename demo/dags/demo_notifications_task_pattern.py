"""
Demo — Notifications Pattern B: get_failure_notification_task
=============================================================

Shows how to add a dedicated notify_failure task to the DAG graph
using get_failure_notification_task(). The task has trigger_rule='one_failed'
so it fires automatically when any upstream task fails, and is skipped
when all tasks succeed.

This pattern is useful when you want the notification step to be
visible in the DAG graph and to have explicit dependencies.

Pipeline:
    pipeline_a ──┐
                 ├──► notify_failure  (fires if either pipeline fails)
    pipeline_b ──┘

Try it:
  • force_failure_a=true  → pipeline_a fails → notify fires
  • force_failure_b=true  → pipeline_b fails → notify fires
  • both false            → both succeed     → notify is SKIPPED
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.sdk import Param

from airflow_toolkit.notifications import get_failure_notification_task

_WEBHOOK_LOGGER_SLACK = "http://webhook-logger:8099/slack"
_WEBHOOK_LOGGER_TEAMS = "http://webhook-logger:8099/teams"
_WEBHOOK_LOGGER_DISCORD = "http://webhook-logger:8099/discord"
_EMAIL_TO = ["admin@demo.local"]


@dag(
    dag_id="demo_notifications_task_pattern",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["demo", "notifications", "v2.2.0"],
    description="Pattern B — notify_failure task in the graph with trigger_rule=one_failed",
    params={
        "force_failure_a": Param(
            True,
            type="boolean",
            description="Make pipeline_a fail.",
        ),
        "force_failure_b": Param(
            False,
            type="boolean",
            description="Make pipeline_b fail.",
        ),
        "environment": Param(
            "DEV",
            type="string",
            enum=["DEV", "STG", "PROD"],
            description="Environment label shown in the notification.",
        ),
    },
    default_args={"retries": 0},
)
def demo_notifications_task_pattern():
    # ── Pipeline A ──────────────────────────────────────────────
    @task(task_id="pipeline_a_extract")
    def pipeline_a_extract() -> dict:
        return {"pipeline": "A", "records": 50}

    @task(task_id="pipeline_a_load")
    def pipeline_a_load(data: dict, **context) -> None:
        if context["params"]["force_failure_a"]:
            raise RuntimeError("pipeline_a_load: intentional failure")
        print(f"[pipeline_a] Loaded {data['records']} records.")

    # ── Pipeline B ──────────────────────────────────────────────
    @task(task_id="pipeline_b_extract")
    def pipeline_b_extract() -> dict:
        return {"pipeline": "B", "records": 75}

    @task(task_id="pipeline_b_load")
    def pipeline_b_load(data: dict, **context) -> None:
        if context["params"]["force_failure_b"]:
            raise RuntimeError("pipeline_b_load: intentional failure")
        print(f"[pipeline_b] Loaded {data['records']} records.")

    # ── Notification task — fires if either pipeline fails ──────
    # environment is read from params at DAG parse time via conf; for a
    # dynamic value use a simpler default here.
    notify = get_failure_notification_task(
        channels=["slack", "email", "teams", "discord"],
        environment="DEV",
        slack_webhook_url=_WEBHOOK_LOGGER_SLACK,
        email_to=_EMAIL_TO,
        teams_webhook_url=_WEBHOOK_LOGGER_TEAMS,
        discord_webhook_url=_WEBHOOK_LOGGER_DISCORD,
    )

    # ── DAG wiring ───────────────────────────────────────────────
    a_done = pipeline_a_load(pipeline_a_extract())
    b_done = pipeline_b_load(pipeline_b_extract())

    [a_done, b_done] >> notify


demo_notifications_task_pattern()
