from __future__ import annotations

import typing

from airflow_toolkit.notifications.context import NotificationContext

_ENV_EMOJI: dict[str, str] = {
    "PROD": ":red_circle:",
    "STG": ":large_yellow_circle:",
    "DEV": ":large_green_circle:",
}


def build_slack_blocks(ctx: NotificationContext) -> list[dict[str, typing.Any]]:
    env = ctx["environment"]
    emoji = _ENV_EMOJI.get(env, ":red_circle:")

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} [{env}] DAG Failure — {ctx['dag_id']}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Run ID*\n`{ctx['run_id']}`"},
                {"type": "mrkdwn", "text": f"*Environment*\n`{env}`"},
                {"type": "mrkdwn", "text": f"*Logical Date*\n`{ctx['ds']}`"},
                {"type": "mrkdwn", "text": f"*Schedule*\n`{ctx['schedule']}`"},
                {
                    "type": "mrkdwn",
                    "text": f"*Interval Start*\n`{ctx['data_interval_start']}`",
                },
                {"type": "mrkdwn", "text": f"*Execution At*\n`{ctx['execution_at']}`"},
                {
                    "type": "mrkdwn",
                    "text": f"*Interval End*\n`{ctx['data_interval_end']}`",
                },
                {"type": "mrkdwn", "text": f"*Duration*\n`{ctx['duration']}`"},
            ],
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f":globe_with_meridians: {ctx['base_url']}"},
            ],
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View in Airflow",
                        "emoji": True,
                    },
                    "url": ctx["dag_url"],
                    "style": "danger",
                },
            ],
        },
    ]


def send_slack_notification(ctx: NotificationContext, webhook_url: str) -> None:
    import requests

    text = f':red_circle: DAG "{ctx["dag_id"]}" failed [{ctx["environment"]}]'
    requests.post(
        webhook_url, json={"text": text, "blocks": build_slack_blocks(ctx)}
    ).raise_for_status()
