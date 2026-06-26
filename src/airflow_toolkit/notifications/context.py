from __future__ import annotations

import typing
from datetime import timedelta

import pendulum
from airflow.configuration import conf


class NotificationContext(typing.TypedDict):
    dag_id: str
    run_id: str
    ds: str
    schedule: str
    environment: str
    base_url: str
    dag_url: str
    data_interval_start: str
    data_interval_end: str
    execution_at: str
    duration: str


def _fmt(dt: typing.Any) -> str:
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def build_notification_context(
    context: dict[str, typing.Any],
    environment: str = "PROD",
) -> NotificationContext:
    dag_run = context["dag_run"]
    dag = context.get("dag")

    dag_id: str = dag_run.dag_id
    run_id: str = dag_run.run_id
    ds: str = context.get("ds", "N/A")

    data_interval_start = getattr(dag_run, "data_interval_start", None) or context.get(
        "data_interval_start"
    )
    data_interval_end = getattr(dag_run, "data_interval_end", None) or context.get(
        "data_interval_end"
    )

    # Manual/triggered runs often have start == end; shift start back one day to show a meaningful interval.
    if (
        data_interval_start
        and data_interval_end
        and data_interval_start == data_interval_end
    ):
        data_interval_start = data_interval_end - timedelta(days=1)

    start_date = getattr(dag_run, "start_date", None)

    if start_date:
        end = getattr(dag_run, "end_date", None) or pendulum.now("UTC")
        total_s = int((end - start_date).total_seconds())
        duration = f"{total_s // 60}m {total_s % 60}s"
    else:
        duration = "N/A"

    schedule = "N/A"
    if dag:
        raw = (
            getattr(dag, "schedule_interval", None)
            or getattr(getattr(dag, "timetable", None), "summary", None)
            or getattr(dag, "schedule", None)
        )
        if raw and str(raw) not in ("None", ""):
            schedule = str(raw)
            tz = getattr(dag, "timezone", None)
            tz_str = str(tz) if tz else "UTC"
            if tz_str != "UTC":
                schedule = f"{schedule} [{tz_str}]"

    base_url = conf.get("api", "base_url", fallback="http://localhost:8080").rstrip("/")
    dag_url = f"{base_url}/dags/{dag_id}/grid?dag_run_id={run_id}"

    return NotificationContext(
        dag_id=dag_id,
        run_id=run_id,
        ds=ds,
        schedule=schedule,
        environment=environment,
        base_url=base_url,
        dag_url=dag_url,
        data_interval_start=_fmt(data_interval_start),
        data_interval_end=_fmt(data_interval_end),
        execution_at=_fmt(start_date),
        duration=duration,
    )
