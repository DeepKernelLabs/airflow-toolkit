from __future__ import annotations

from airflow_toolkit.notifications.context import NotificationContext

_ENV_COLOR: dict[str, str] = {
    "PROD": "#c0392b",
    "STG": "#e67e22",
    "DEV": "#27ae60",
}

_ROW_TEMPLATE = """\
  <tr{bg}>
    <td style="padding:10px 16px;font-weight:bold;width:38%;border-bottom:1px solid #eee;color:#555">{label}</td>
    <td style="padding:10px 16px;font-family:monospace;font-size:13px;border-bottom:1px solid #eee">{value}</td>
  </tr>"""

_LAST_ROW_TEMPLATE = """\
  <tr{bg}>
    <td style="padding:10px 16px;font-weight:bold;width:38%;color:#555">{label}</td>
    <td style="padding:10px 16px;font-family:monospace;font-size:13px">{value}</td>
  </tr>"""


def _row(label: str, value: str, stripe: bool, last: bool = False) -> str:
    bg = ' style="background:#f9f9f9"' if stripe else ""
    tpl = _LAST_ROW_TEMPLATE if last else _ROW_TEMPLATE
    return tpl.format(bg=bg, label=label, value=value)


def build_email_html(ctx: NotificationContext) -> str:
    env = ctx["environment"]
    color = _ENV_COLOR.get(env, "#c0392b")

    fields = [
        ("Run ID", ctx["run_id"]),
        ("Environment", env),
        ("Logical Date", ctx["ds"]),
        ("Schedule", ctx["schedule"]),
        ("Interval Start", ctx["data_interval_start"]),
        ("Interval End", ctx["data_interval_end"]),
        ("Execution At", ctx["execution_at"]),
        ("Duration", ctx["duration"]),
    ]

    rows = "".join(
        _row(label, value, stripe=i % 2 == 0, last=(i == len(fields) - 1))
        for i, (label, value) in enumerate(fields)
    )

    return f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:20px;background:#f4f4f4;font-family:sans-serif">
  <div style="max-width:640px;margin:0 auto">

    <div style="background:{color};padding:16px 24px;border-radius:8px 8px 0 0">
      <h2 style="color:#fff;margin:0;font-size:18px">
        &#x1F534; [{env}] DAG Failure &mdash; {ctx["dag_id"]}
      </h2>
    </div>

    <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #ddd;border-top:none">
{rows}
    </table>

    <div style="background:#fff;border:1px solid #ddd;border-top:none;padding:16px 24px;border-radius:0 0 8px 8px">
      <a href="{ctx["dag_url"]}"
         style="display:inline-block;background:{color};color:#fff;padding:10px 20px;
                border-radius:4px;text-decoration:none;font-weight:bold;font-size:14px">
        View in Airflow
      </a>
      <p style="color:#aaa;font-size:11px;margin:12px 0 0">{ctx["base_url"]}</p>
    </div>

  </div>
</body>
</html>"""


def send_email_notification(
    ctx: NotificationContext,
    to: list[str],
    from_email: str | None = None,
) -> None:
    from airflow.utils.email import send_email

    html = build_email_html(ctx)
    subject = f"[{ctx['environment']}] DAG Failure — {ctx['dag_id']}"
    kwargs: dict = {"to": to, "subject": subject, "html_content": html}
    if from_email:
        kwargs["from_email"] = from_email
    send_email(**kwargs)
