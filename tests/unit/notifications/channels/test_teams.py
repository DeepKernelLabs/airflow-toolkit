from __future__ import annotations

from unittest.mock import MagicMock, patch

from airflow_toolkit.notifications.channels.teams import (
    build_teams_payload,
    send_teams_notification,
)
from airflow_toolkit.notifications.context import NotificationContext


def _ctx(**kwargs) -> NotificationContext:
    defaults: NotificationContext = {
        "dag_id": "my_dag",
        "run_id": "scheduled__2024-01-15",
        "ds": "2024-01-15",
        "schedule": "0 6 * * *",
        "environment": "PROD",
        "base_url": "http://airflow.example.com",
        "dag_url": "http://airflow.example.com/dags/my_dag/grid?dag_run_id=scheduled__2024-01-15",
        "data_interval_start": "2024-01-14 00:00:00 UTC",
        "data_interval_end": "2024-01-15 00:00:00 UTC",
        "execution_at": "2024-01-15 06:00:05 UTC",
        "duration": "3m 45s",
    }
    defaults.update(kwargs)
    return defaults


class TestBuildTeamsPayload:
    def test_adaptive_card_structure(self):
        payload = build_teams_payload(_ctx())
        assert payload["type"] == "message"
        card = payload["attachments"][0]["content"]
        assert card["type"] == "AdaptiveCard"

    def test_title_contains_dag_id_and_env(self):
        payload = build_teams_payload(_ctx())
        card = payload["attachments"][0]["content"]
        title_block = card["body"][0]
        assert "my_dag" in title_block["text"]
        assert "[PROD]" in title_block["text"]

    def test_prod_uses_attention_color(self):
        payload = build_teams_payload(_ctx(environment="PROD"))
        card = payload["attachments"][0]["content"]
        assert card["body"][0]["color"] == "Attention"

    def test_stg_uses_warning_color(self):
        payload = build_teams_payload(_ctx(environment="STG"))
        card = payload["attachments"][0]["content"]
        assert card["body"][0]["color"] == "Warning"

    def test_dev_uses_good_color(self):
        payload = build_teams_payload(_ctx(environment="DEV"))
        card = payload["attachments"][0]["content"]
        assert card["body"][0]["color"] == "Good"

    def test_factset_contains_all_fields(self):
        payload = build_teams_payload(_ctx())
        card = payload["attachments"][0]["content"]
        factset = next(b for b in card["body"] if b["type"] == "FactSet")
        titles = [f["title"] for f in factset["facts"]]
        assert "Run ID" in titles
        assert "Duration" in titles
        assert "Schedule" in titles

    def test_action_button_has_dag_url(self):
        ctx = _ctx(
            dag_url="http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )
        payload = build_teams_payload(ctx)
        card = payload["attachments"][0]["content"]
        action = card["actions"][0]
        assert action["type"] == "Action.OpenUrl"
        assert (
            action["url"]
            == "http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )


class TestSendTeamsNotification:
    def test_posts_to_webhook_url(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = MagicMock()
            send_teams_notification(
                _ctx(), webhook_url="https://teams.webhook.example.com/hook"
            )

        mock_post.assert_called_once_with(
            "https://teams.webhook.example.com/hook",
            json=build_teams_payload(_ctx()),
        )
