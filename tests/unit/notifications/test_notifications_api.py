from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow_toolkit.notifications import dag_failure_notification


def _make_context() -> dict:
    dag_run = MagicMock()
    dag_run.dag_id = "my_dag"
    dag_run.run_id = "scheduled__2024-01-15"
    dag_run.start_date = None
    dag_run.end_date = None
    dag_run.data_interval_start = None
    dag_run.data_interval_end = None

    dag = MagicMock()
    dag.schedule = "0 6 * * *"
    dag.schedule_interval = None
    dag.timezone = MagicMock(__str__=lambda self: "UTC")

    return {"dag_run": dag_run, "dag": dag, "ds": "2024-01-15"}


class TestDagFailureNotification:
    def test_returns_callable(self):
        callback = dag_failure_notification(
            channels=["slack"], slack_webhook_url="https://hooks.slack.com/test"
        )
        assert callable(callback)

    def test_slack_without_url_raises_on_call(self):
        callback = dag_failure_notification(channels=["slack"])
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with pytest.raises(ValueError, match="slack_webhook_url is required"):
                callback(_make_context())

    def test_unknown_channel_raises_on_creation(self):
        with pytest.raises(ValueError, match="Unknown notification channels"):
            dag_failure_notification(channels=["fax"])

    def test_email_without_email_to_raises_on_call(self):
        callback = dag_failure_notification(channels=["email"])
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with pytest.raises(ValueError, match="email_to is required"):
                callback(_make_context())

    def test_teams_without_url_raises_on_call(self):
        callback = dag_failure_notification(channels=["teams"])
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with pytest.raises(ValueError, match="teams_webhook_url is required"):
                callback(_make_context())

    def test_discord_without_url_raises_on_call(self):
        callback = dag_failure_notification(channels=["discord"])
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with pytest.raises(ValueError, match="discord_webhook_url is required"):
                callback(_make_context())

    def test_slack_channel_invoked(self):
        callback = dag_failure_notification(
            channels=["slack"],
            slack_webhook_url="https://hooks.slack.com/test",
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with patch(
                "airflow_toolkit.notifications.channels.slack.send_slack_notification"
            ) as mock_send:
                callback(_make_context())

        mock_send.assert_called_once()

    def test_email_channel_invoked(self):
        callback = dag_failure_notification(
            channels=["email"], email_to=["ops@example.com"]
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with patch(
                "airflow_toolkit.notifications.channels.email.send_email_notification"
            ) as mock_send:
                callback(_make_context())

        mock_send.assert_called_once()

    def test_teams_channel_invoked(self):
        callback = dag_failure_notification(
            channels=["teams"],
            teams_webhook_url="https://teams.webhook.example.com/hook",
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with patch(
                "airflow_toolkit.notifications.channels.teams.send_teams_notification"
            ) as mock_send:
                callback(_make_context())

        mock_send.assert_called_once()

    def test_discord_channel_invoked(self):
        callback = dag_failure_notification(
            channels=["discord"],
            discord_webhook_url="https://discord.com/api/webhooks/test",
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with patch(
                "airflow_toolkit.notifications.channels.discord.send_discord_notification"
            ) as mock_send:
                callback(_make_context())

        mock_send.assert_called_once()

    def test_multi_channel_all_invoked(self):
        callback = dag_failure_notification(
            channels=["slack", "email", "teams", "discord"],
            slack_webhook_url="https://hooks.slack.com/test",
            email_to=["ops@example.com"],
            teams_webhook_url="https://teams.webhook.example.com/hook",
            discord_webhook_url="https://discord.com/api/webhooks/test",
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with (
                patch(
                    "airflow_toolkit.notifications.channels.slack.send_slack_notification"
                ) as m_slack,
                patch(
                    "airflow_toolkit.notifications.channels.email.send_email_notification"
                ) as m_email,
                patch(
                    "airflow_toolkit.notifications.channels.teams.send_teams_notification"
                ) as m_teams,
                patch(
                    "airflow_toolkit.notifications.channels.discord.send_discord_notification"
                ) as m_discord,
            ):
                callback(_make_context())

        m_slack.assert_called_once()
        m_email.assert_called_once()
        m_teams.assert_called_once()
        m_discord.assert_called_once()

    def test_environment_propagated_to_context(self):
        captured = {}

        def fake_send(ctx, **kwargs):
            captured["env"] = ctx["environment"]

        callback = dag_failure_notification(
            channels=["slack"],
            environment="DEV",
            slack_webhook_url="https://hooks.slack.com/test",
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            with patch(
                "airflow_toolkit.notifications.channels.slack.send_slack_notification",
                side_effect=fake_send,
            ):
                callback(_make_context())

        assert captured["env"] == "DEV"
