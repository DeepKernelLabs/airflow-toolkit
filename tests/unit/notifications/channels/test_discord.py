from __future__ import annotations

import typing
from unittest.mock import MagicMock, patch

from airflow_toolkit.notifications.channels.discord import (
    build_discord_payload,
    send_discord_notification,
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


class TestBuildDiscordPayload:
    def test_content_contains_dag_id(self):
        payload = build_discord_payload(_ctx())
        assert "my_dag" in payload["content"]

    def test_embed_title_contains_dag_id_and_env(self):
        payload = build_discord_payload(_ctx())
        embed = payload["embeds"][0]
        assert "my_dag" in embed["title"]
        assert "[PROD]" in embed["title"]

    def test_prod_uses_red_color(self):
        payload = build_discord_payload(_ctx(environment="PROD"))
        assert payload["embeds"][0]["color"] == 15548997

    def test_stg_uses_orange_color(self):
        payload = build_discord_payload(_ctx(environment="STG"))
        assert payload["embeds"][0]["color"] == 16750592

    def test_dev_uses_green_color(self):
        payload = build_discord_payload(_ctx(environment="DEV"))
        assert payload["embeds"][0]["color"] == 5763719

    def test_unknown_env_falls_back_to_red(self):
        payload = build_discord_payload(_ctx(environment="CUSTOM"))
        assert payload["embeds"][0]["color"] == 15548997

    def test_embed_url_is_dag_url(self):
        ctx = _ctx(
            dag_url="http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )
        payload = build_discord_payload(ctx)
        assert (
            payload["embeds"][0]["url"]
            == "http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )

    def test_embed_footer_contains_base_url(self):
        payload = build_discord_payload(_ctx(base_url="http://airflow.internal"))
        assert payload["embeds"][0]["footer"]["text"] == "http://airflow.internal"

    def test_embed_fields_contain_all_metadata(self):
        payload = build_discord_payload(_ctx())
        fields = {f["name"]: f["value"] for f in payload["embeds"][0]["fields"]}
        assert "Run ID" in fields
        assert "Duration" in fields
        assert "Schedule" in fields
        assert "Logical Date" in fields

    def test_field_values_have_no_backticks(self):
        payload = build_discord_payload(_ctx())
        for field in payload["embeds"][0]["fields"]:
            assert not field["value"].startswith("`"), (
                f"{field['name']} value starts with backtick"
            )
            assert not field["value"].endswith("`"), (
                f"{field['name']} value ends with backtick"
            )


class TestSendDiscordNotification:
    def test_posts_to_webhook_url(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = MagicMock()
            send_discord_notification(
                _ctx(), webhook_url="https://discord.com/api/webhooks/test"
            )

        mock_post.assert_called_once_with(
            "https://discord.com/api/webhooks/test",
            json=build_discord_payload(_ctx()),
        )
