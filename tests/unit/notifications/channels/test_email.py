from __future__ import annotations

import typing
from unittest.mock import patch

from airflow_toolkit.notifications.channels.email import (
    build_email_html,
    send_email_notification,
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


class TestBuildEmailHtml:
    def test_contains_dag_id(self):
        html = build_email_html(_ctx())
        assert "my_dag" in html

    def test_contains_environment(self):
        html = build_email_html(_ctx(environment="STG"))
        assert "STG" in html

    def test_prod_uses_red_color(self):
        html = build_email_html(_ctx(environment="PROD"))
        assert "#c0392b" in html

    def test_stg_uses_orange_color(self):
        html = build_email_html(_ctx(environment="STG"))
        assert "#e67e22" in html

    def test_dev_uses_green_color(self):
        html = build_email_html(_ctx(environment="DEV"))
        assert "#27ae60" in html

    def test_contains_all_fields(self):
        html = build_email_html(_ctx())
        for label in (
            "Run ID",
            "Logical Date",
            "Schedule",
            "Interval Start",
            "Interval End",
            "Duration",
        ):
            assert label in html

    def test_contains_dag_url_link(self):
        ctx = _ctx(
            dag_url="http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123"
        )
        html = build_email_html(ctx)
        assert "http://airflow.example.com/dags/my_dag/grid?dag_run_id=run123" in html

    def test_contains_base_url(self):
        html = build_email_html(_ctx(base_url="http://airflow.internal"))
        assert "http://airflow.internal" in html

    def test_valid_html_structure(self):
        html = build_email_html(_ctx())
        assert "<!DOCTYPE html>" in html
        assert "<table" in html
        assert "</html>" in html


class TestSendEmailNotification:
    def test_calls_send_email_with_correct_args(self):
        with patch("airflow.utils.email.send_email") as mock_send:
            send_email_notification(_ctx(), to=["ops@example.com", "eng@example.com"])

        mock_send.assert_called_once()
        kwargs = mock_send.call_args.kwargs
        assert kwargs["to"] == ["ops@example.com", "eng@example.com"]
        assert "[PROD]" in kwargs["subject"]
        assert "my_dag" in kwargs["subject"]
        assert (
            "<html>" not in kwargs["html_content"]
            or "<!DOCTYPE html>" in kwargs["html_content"]
        )

    def test_subject_includes_environment_and_dag_id(self):
        with patch("airflow.utils.email.send_email") as mock_send:
            send_email_notification(
                _ctx(environment="STG", dag_id="etl_pipeline"), to=["a@b.com"]
            )

        subject = mock_send.call_args.kwargs["subject"]
        assert "[STG]" in subject
        assert "etl_pipeline" in subject

    def test_from_email_passed_when_provided(self):
        with patch("airflow.utils.email.send_email") as mock_send:
            send_email_notification(
                _ctx(), to=["a@b.com"], from_email="noreply@example.com"
            )

        kwargs = mock_send.call_args.kwargs
        assert kwargs.get("from_email") == "noreply@example.com"

    def test_from_email_not_passed_when_none(self):
        with patch("airflow.utils.email.send_email") as mock_send:
            send_email_notification(_ctx(), to=["a@b.com"])

        kwargs = mock_send.call_args.kwargs
        assert "from_email" not in kwargs
