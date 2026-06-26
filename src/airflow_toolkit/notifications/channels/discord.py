from __future__ import annotations

import typing

from airflow_toolkit.notifications.context import NotificationContext

# Discord embed colors (decimal RGB)
_ENV_COLOR: dict[str, int] = {
    "PROD": 15548997,  # red    #ED4245
    "STG": 16750592,  # orange #FF8C00
    "DEV": 5763719,  # green  #57F287
}


def build_discord_payload(ctx: NotificationContext) -> dict[str, typing.Any]:
    env = ctx["environment"]
    color = _ENV_COLOR.get(env, 15548997)

    return {
        "content": f"\U0001f534 DAG `{ctx['dag_id']}` failed",
        "embeds": [
            {
                "title": f"[{env}] DAG Failure — {ctx['dag_id']}",
                "url": ctx["dag_url"],
                "color": color,
                "fields": [
                    {"name": "Run ID", "value": ctx["run_id"], "inline": False},
                    {"name": "Environment", "value": env, "inline": True},
                    {"name": "Logical Date", "value": ctx["ds"], "inline": True},
                    {"name": "Schedule", "value": ctx["schedule"], "inline": True},
                    {
                        "name": "Interval Start",
                        "value": ctx["data_interval_start"],
                        "inline": True,
                    },
                    {
                        "name": "Interval End",
                        "value": ctx["data_interval_end"],
                        "inline": True,
                    },
                    {
                        "name": "Execution At",
                        "value": ctx["execution_at"],
                        "inline": True,
                    },
                    {"name": "Duration", "value": ctx["duration"], "inline": True},
                ],
                "footer": {"text": ctx["base_url"]},
            }
        ],
    }


def send_discord_notification(ctx: NotificationContext, webhook_url: str) -> None:
    import requests

    requests.post(webhook_url, json=build_discord_payload(ctx)).raise_for_status()
