from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pendulum

from airflow_toolkit.notifications.context import _fmt, build_notification_context


def _make_dag_run(
    dag_id: str = "my_dag",
    run_id: str = "scheduled__2024-01-15T00:00:00+00:00",
    start_date: datetime.datetime | None = None,
    end_date: datetime.datetime | None = None,
    data_interval_start: datetime.datetime | None = None,
    data_interval_end: datetime.datetime | None = None,
) -> MagicMock:
    dr = MagicMock()
    dr.dag_id = dag_id
    dr.run_id = run_id
    dr.start_date = start_date or pendulum.datetime(2024, 1, 15, 10, 0, 0)
    dr.end_date = end_date or pendulum.datetime(2024, 1, 15, 10, 3, 45)
    dr.data_interval_start = data_interval_start or pendulum.datetime(
        2024, 1, 14, 0, 0, 0
    )
    dr.data_interval_end = data_interval_end or pendulum.datetime(2024, 1, 15, 0, 0, 0)
    return dr


def _make_dag(schedule: str = "0 6 * * *", tz: str = "UTC") -> MagicMock:
    dag = MagicMock()
    dag.schedule_interval = None
    dag.timetable = None  # prevent MagicMock auto-creating a truthy timetable.summary
    dag.schedule = schedule
    dag.timezone = tz
    return dag


def _make_context(**kwargs) -> dict:
    dag_run = kwargs.pop("dag_run", _make_dag_run())
    dag = kwargs.pop("dag", _make_dag())
    return {"dag_run": dag_run, "dag": dag, "ds": "2024-01-15", **kwargs}


class TestFmt:
    def test_none_returns_na(self):
        assert _fmt(None) == "N/A"

    def test_datetime_formatted(self):
        dt = pendulum.datetime(2024, 1, 15, 10, 30, 0)
        assert _fmt(dt) == "2024-01-15 10:30:00 UTC"


class TestBuildNotificationContext:
    def test_basic_fields(self):
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://airflow.example.com"
            ctx = build_notification_context(_make_context(), environment="PROD")

        assert ctx["dag_id"] == "my_dag"
        assert ctx["run_id"] == "scheduled__2024-01-15T00:00:00+00:00"
        assert ctx["ds"] == "2024-01-15"
        assert ctx["environment"] == "PROD"

    def test_environment_passed_through(self):
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://airflow.example.com"
            ctx = build_notification_context(_make_context(), environment="STG")

        assert ctx["environment"] == "STG"

    def test_dag_url_format(self):
        dr = _make_dag_run(dag_id="etl_pipeline", run_id="run_abc123")
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://airflow.prod.internal"
            ctx = build_notification_context(_make_context(dag_run=dr))

        assert (
            ctx["dag_url"]
            == "http://airflow.prod.internal/dags/etl_pipeline/grid?dag_run_id=run_abc123"
        )

    def test_base_url_trailing_slash_stripped(self):
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://airflow.example.com/"
            ctx = build_notification_context(_make_context())

        assert ctx["base_url"] == "http://airflow.example.com"

    def test_duration_calculated(self):
        dr = _make_dag_run(
            start_date=pendulum.datetime(2024, 1, 15, 10, 0, 0),
            end_date=pendulum.datetime(2024, 1, 15, 10, 3, 45),
        )
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            ctx = build_notification_context(_make_context(dag_run=dr))

        assert ctx["duration"] == "3m 45s"

    def test_schedule_with_non_utc_timezone(self):
        dag = _make_dag(schedule="0 6 * * *", tz="America/New_York")
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            ctx = build_notification_context(_make_context(dag=dag))

        assert ctx["schedule"] == "0 6 * * * [America/New_York]"

    def test_schedule_utc_not_appended(self):
        dag = _make_dag(schedule="0 6 * * *", tz="UTC")
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            ctx = build_notification_context(_make_context(dag=dag))

        assert ctx["schedule"] == "0 6 * * *"

    def test_interval_start_shifted_when_equal_to_end(self):
        ts = pendulum.datetime(2024, 1, 15, 0, 0, 0)
        dr = _make_dag_run(data_interval_start=ts, data_interval_end=ts)
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            ctx = build_notification_context(_make_context(dag_run=dr))

        assert ctx["data_interval_start"] == "2024-01-14 00:00:00 UTC"
        assert ctx["data_interval_end"] == "2024-01-15 00:00:00 UTC"

    def test_no_start_date_gives_na_duration(self):
        dr = _make_dag_run()
        dr.start_date = None
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            ctx = build_notification_context(_make_context(dag_run=dr))

        assert ctx["duration"] == "N/A"
        assert ctx["execution_at"] == "N/A"

    def test_ds_defaults_to_na_when_missing(self):
        context = _make_context()
        del context["ds"]
        with patch("airflow_toolkit.notifications.context.conf") as mock_conf:
            mock_conf.get.return_value = "http://localhost:8080"
            ctx = build_notification_context(context)

        assert ctx["ds"] == "N/A"
