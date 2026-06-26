from __future__ import annotations

import typing

from airflow_toolkit.notifications.context import (
    NotificationContext,
    build_notification_context,
)

__all__ = [
    "dag_failure_notification",
    "get_failure_notification_task",
    "build_notification_context",
    "NotificationContext",
]

_VALID_CHANNELS = {"slack", "email", "teams", "discord"}


def dag_failure_notification(
    channels: list[str],
    environment: str = "PROD",
    slack_webhook_url: str | None = None,
    email_to: list[str] | None = None,
    email_from: str | None = None,
    teams_webhook_url: str | None = None,
    discord_webhook_url: str | None = None,
) -> typing.Callable[[dict[str, typing.Any]], None]:
    """Return an on_failure_callback that sends DAG failure notifications.

    Usage::

        with DAG(..., on_failure_callback=dag_failure_notification(
            channels=["slack", "email"],
            environment="PROD",
            slack_webhook_url="https://hooks.slack.com/services/...",
            email_to=["ops@example.com"],
        )):
            ...
    """
    unknown = set(channels) - _VALID_CHANNELS
    if unknown:
        raise ValueError(
            f"Unknown notification channels: {unknown}. Valid: {_VALID_CHANNELS}"
        )

    def callback(context: dict[str, typing.Any]) -> None:
        ctx = build_notification_context(context, environment=environment)

        if "slack" in channels:
            if not slack_webhook_url:
                raise ValueError(
                    "slack_webhook_url is required when 'slack' is in channels"
                )
            from airflow_toolkit.notifications.channels.slack import (
                send_slack_notification,
            )

            send_slack_notification(ctx, webhook_url=slack_webhook_url)

        if "email" in channels:
            if not email_to:
                raise ValueError("email_to is required when 'email' is in channels")
            from airflow_toolkit.notifications.channels.email import (
                send_email_notification,
            )

            send_email_notification(ctx, to=email_to, from_email=email_from)

        if "teams" in channels:
            if not teams_webhook_url:
                raise ValueError(
                    "teams_webhook_url is required when 'teams' is in channels"
                )
            from airflow_toolkit.notifications.channels.teams import (
                send_teams_notification,
            )

            send_teams_notification(ctx, webhook_url=teams_webhook_url)

        if "discord" in channels:
            if not discord_webhook_url:
                raise ValueError(
                    "discord_webhook_url is required when 'discord' is in channels"
                )
            from airflow_toolkit.notifications.channels.discord import (
                send_discord_notification,
            )

            send_discord_notification(ctx, webhook_url=discord_webhook_url)

    return callback


def get_failure_notification_task(
    channels: list[str],
    environment: str = "PROD",
    slack_webhook_url: str | None = None,
    email_to: list[str] | None = None,
    email_from: str | None = None,
    teams_webhook_url: str | None = None,
    discord_webhook_url: str | None = None,
) -> typing.Any:
    """Return an @task with trigger_rule='one_failed' that sends failure notifications.

    Usage::

        notify = get_failure_notification_task(channels=["slack"], environment="PROD")
        [task_a, task_b] >> notify
    """
    from airflow.sdk import task as airflow_task

    _channels = channels
    _environment = environment
    _slack_webhook_url = slack_webhook_url
    _email_to = email_to
    _email_from = email_from
    _teams_webhook_url = teams_webhook_url
    _discord_webhook_url = discord_webhook_url

    @airflow_task(task_id="notify_failure", trigger_rule="one_failed")
    def notify_failure(**context: typing.Any) -> None:
        callback = dag_failure_notification(
            channels=_channels,
            environment=_environment,
            slack_webhook_url=_slack_webhook_url,
            email_to=_email_to,
            email_from=_email_from,
            teams_webhook_url=_teams_webhook_url,
            discord_webhook_url=_discord_webhook_url,
        )
        callback(context)

    return notify_failure()
