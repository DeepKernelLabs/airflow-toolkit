from __future__ import annotations

import typing

from airflow_toolkit.notifications.context import NotificationContext

_ENV_COLOR: dict[str, str] = {
    "PROD": "Attention",
    "STG": "Warning",
    "DEV": "Good",
}


def build_teams_payload(ctx: NotificationContext) -> dict[str, typing.Any]:
    env = ctx["environment"]
    color = _ENV_COLOR.get(env, "Attention")

    facts = [
        {"title": "Run ID", "value": ctx["run_id"]},
        {"title": "Environment", "value": env},
        {"title": "Logical Date", "value": ctx["ds"]},
        {"title": "Schedule", "value": ctx["schedule"]},
        {"title": "Interval Start", "value": ctx["data_interval_start"]},
        {"title": "Interval End", "value": ctx["data_interval_end"]},
        {"title": "Execution At", "value": ctx["execution_at"]},
        {"title": "Duration", "value": ctx["duration"]},
    ]

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"\U0001f534 [{env}] DAG Failure — {ctx['dag_id']}",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": color,
                            "wrap": True,
                        },
                        {
                            "type": "FactSet",
                            "facts": facts,
                        },
                        {
                            "type": "TextBlock",
                            "text": ctx["base_url"],
                            "size": "Small",
                            "color": "Default",
                            "isSubtle": True,
                            "wrap": True,
                        },
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "View in Airflow",
                            "url": ctx["dag_url"],
                        }
                    ],
                },
            }
        ],
    }


def send_teams_notification(ctx: NotificationContext, webhook_url: str) -> None:
    import requests

    requests.post(webhook_url, json=build_teams_payload(ctx)).raise_for_status()
