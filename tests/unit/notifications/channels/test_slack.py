from __future__ import annotations

import typing
from unittest.mock import MagicMock, patch

from airflow_toolkit.notifications.channels.slack import (
    build_slack_blocks,
    send_slack_notification,
)
from airflow_toolkit.notifications.context import NotificationContext


def _ctx(**kwargs: typing.Any) -> NotificationContext:
    base: dict[str, typing.Any] = {
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
    base.update(kwargs)
    return typing.cast(NotificationContext, base)


class TestBuildSlackBlocks:
    def test_header_contains_dag_id_and_env(self):
        blocks = build_slack_blocks(_ctx())
        header = blocks[0]
        assert header["type"] == "header"
        assert "my_dag" in header["text"]["text"]
        assert "[PROD]" in header["text"]["text"]

    def test_prod_uses_red_circle_emoji(self):
        blocks = build_slack_blocks(_ctx(environment="PROD"))
        assert ":red_circle:" in blocks[0]["text"]["text"]

    def test_stg_uses_yellow_circle_emoji(self):
        blocks = build_slack_blocks(_ctx(environment="STG"))
        assert ":large_yellow_circle:" in blocks[0]["text"]["text"]

    def test_dev_uses_green_circle_emoji(self):
        blocks = build_slack_blocks(_ctx(environment="DEV"))
        assert ":large_green_circle:" in blocks[0]["text"]["text"]

    def test_section_contains_all_fields(self):
        blocks = build_slack_blocks(_ctx())
        section = next(b for b in blocks if b["type"] == "section")
        field_texts = [f["text"] for f in section["fields"]]
        assert any("Run ID" in t for t in field_texts)
        assert any("Logical Date" in t for t in field_texts)
        assert any("Duration" in t for t in field_texts)

    def test_actions_button_has_dag_url(self):
        ctx = _ctx(
            dag_url="http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )
        blocks = build_slack_blocks(ctx)
        actions = next(b for b in blocks if b["type"] == "actions")
        button = actions["elements"][0]
        assert (
            button["url"]
            == "http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )

    def test_context_footer_contains_base_url(self):
        blocks = build_slack_blocks(_ctx(base_url="http://airflow.example.com"))
        context_block = next(b for b in blocks if b["type"] == "context")
        assert "http://airflow.example.com" in context_block["elements"][0]["text"]

    def test_unknown_env_falls_back_to_red_circle(self):
        blocks = build_slack_blocks(_ctx(environment="CUSTOM"))
        assert ":red_circle:" in blocks[0]["text"]["text"]


class TestSendSlackNotification:
    def test_sends_via_webhook_url(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = MagicMock()
            send_slack_notification(_ctx(), webhook_url="https://hooks.slack.com/test")

        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        assert "blocks" in kwargs["json"]

    def test_payload_includes_text_and_blocks(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = MagicMock()
            send_slack_notification(_ctx(), webhook_url="https://hooks.slack.com/test")

        _, kwargs = mock_post.call_args
        assert "text" in kwargs["json"]
        assert "blocks" in kwargs["json"]

    def test_posts_to_correct_url(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = MagicMock()
            send_slack_notification(
                _ctx(), webhook_url="https://hooks.slack.com/services/ABC/123"
            )

        mock_post.assert_called_once_with(
            "https://hooks.slack.com/services/ABC/123",
            json=mock_post.call_args[1]["json"],
        )
