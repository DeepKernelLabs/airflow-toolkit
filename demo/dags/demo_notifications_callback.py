"""
Demo — Notifications Pattern A: on_failure_callback
=====================================================

Shows how to wire dag_failure_notification() as an on_failure_callback
so that any task failure automatically sends notifications to all channels.

Channels demonstrated:
  • Slack   → webhook-logger (http://localhost:8099/slack)
  • Email   → MailHog       (http://localhost:8025)
  • Teams   → webhook-logger (http://localhost:8099/teams)
  • Discord → webhook-logger (http://localhost:8099/discord)

Try it:
  1. Trigger with force_failure=true  → task fails → all channels fire
  2. Trigger with force_failure=false → all tasks pass → no notification
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.sdk import Param

from airflow_toolkit.notifications import dag_failure_notification

# Local webhook-logger URLs (resolved inside the Docker network)
_WEBHOOK_LOGGER_SLACK = "http://webhook-logger:8099/slack"
_WEBHOOK_LOGGER_TEAMS = "http://webhook-logger:8099/teams"
_WEBHOOK_LOGGER_DISCORD = "http://webhook-logger:8099/discord"
_EMAIL_TO = ["admin@demo.local"]


@dag(
    dag_id="demo_notifications_callback",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["demo", "notifications", "v2.2.0"],
    description="Pattern A — on_failure_callback triggers multi-channel notifications",
    params={
        "force_failure": Param(
            True,
            type="boolean",
            description="Set to true to make the transform task fail and trigger notifications.",
        ),
        "environment": Param(
            "DEV",
            type="string",
            enum=["DEV", "STG", "PROD"],
            description="Environment label shown in the notification.",
        ),
    },
    on_failure_callback=dag_failure_notification(
        channels=["slack", "email", "teams", "discord"],
        environment="DEV",
        slack_webhook_url=_WEBHOOK_LOGGER_SLACK,
        email_to=_EMAIL_TO,
        teams_webhook_url=_WEBHOOK_LOGGER_TEAMS,
        discord_webhook_url=_WEBHOOK_LOGGER_DISCORD,
    ),
    default_args={"retries": 0},
)
def demo_notifications_callback():
    @task
    def extract() -> dict:
        import time

        time.sleep(1)
        return {"records": 100, "source": "api"}

    @task
    def transform(data: dict, **context) -> dict:
        force_failure: bool = context["params"]["force_failure"]
        if force_failure:
            raise ValueError(
                "Intentional failure — set force_failure=false to let the pipeline succeed."
            )
        return {**data, "transformed": True}

    @task
    def load(data: dict) -> None:
        import time

        time.sleep(1)
        print(f"Loaded {data['records']} records.")

    raw = extract()
    transformed = transform(raw)
    load(transformed)


demo_notifications_callback()
