import html
import os
from pathlib import Path

import dagster as dg
from dotenv import load_dotenv

# Explicit path, not a cwd-relative search - dagster/dg can be invoked from
# other working directories and would otherwise silently miss this file.
load_dotenv(Path(__file__).parents[3] / ".env")

# Left blank on purpose - fill these in later (see .env.example). Needs a
# Gmail App Password (16 chars, requires 2FA enabled on the Gmail account),
# not the regular account password.
GMAIL_USER = os.getenv("GMAIL_SMTP_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_SMTP_APP_PASSWORD")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")


WEBSERVER_BASE_URL = os.getenv("DAGSTER_WEBSERVER_BASE_URL", "http://localhost:3000")


def _failure_email_subject(context: dg.RunFailureSensorContext) -> str:
    partition = f" [{context.partition_key}]" if context.partition_key else ""
    return f"[Dagster] {context.dagster_run.job_name}{partition} failed"


def _failure_email_body(context: dg.RunFailureSensorContext) -> str:
    run = context.dagster_run
    step_errors = {
        event.step_key: event.event_specific_data.error.to_string()
        for event in context.get_step_failure_events()
        if event.event_specific_data and event.event_specific_data.error
    }

    if step_errors:
        rows = "".join(
            f"<tr>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;font-family:monospace;font-size:12px;'>{html.escape(step)}</td>"
            f"<td style='padding:6px 10px;border:1px solid #ddd;'>"
            f"<pre style='white-space:pre-wrap;margin:0;font-size:11px;max-height:220px;overflow:auto;'>{html.escape(err[:3000])}</pre>"
            f"</td></tr>"
            for step, err in step_errors.items()
        )
    else:
        rows = (
            "<tr><td colspan='2' style='padding:6px 10px;border:1px solid #ddd;'>"
            f"{html.escape(context.failure_event.message)}</td></tr>"
        )

    run_url = f"{WEBSERVER_BASE_URL}/runs/{run.run_id}"
    partition_row = (
        f"<tr><td style='padding:2px 10px;color:#666;'>Partition</td>"
        f"<td style='padding:2px 10px;'>{html.escape(context.partition_key)}</td></tr>"
        if context.partition_key
        else ""
    )

    return f"""
<div style="font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:720px;color:#1a1a1a;">
  <h2 style="color:#b91c1c;margin:0 0 12px 0;">Dagster run failed</h2>
  <table style="border-collapse:collapse;font-size:13px;margin-bottom:14px;">
    <tr><td style="padding:2px 10px;color:#666;">Job</td><td style="padding:2px 10px;"><b>{html.escape(run.job_name)}</b></td></tr>
    <tr><td style="padding:2px 10px;color:#666;">Run ID</td><td style="padding:2px 10px;font-family:monospace;">{run.run_id}</td></tr>
    {partition_row}
  </table>
  <p style="font-size:13px;margin:0 0 6px 0;"><b>Failed steps</b></p>
  <table style="border-collapse:collapse;width:100%;font-size:12px;margin-bottom:14px;">
    <tr>
      <th style="text-align:left;padding:6px 10px;border:1px solid #ddd;background:#f5f5f5;">Step</th>
      <th style="text-align:left;padding:6px 10px;border:1px solid #ddd;background:#f5f5f5;">Error</th>
    </tr>
    {rows}
  </table>
  <p style="font-size:13px;">
    <a href="{run_url}" style="background:#1a56db;color:#fff;padding:8px 14px;border-radius:4px;text-decoration:none;">View run in Dagster UI</a>
  </p>
</div>
""".strip()


def _build_failure_email_sensor() -> dg.SensorDefinition:
    if not (GMAIL_USER and GMAIL_APP_PASSWORD and ALERT_EMAIL_TO):

        @dg.run_failure_sensor(name="failure_email_alert", default_status=dg.DefaultSensorStatus.STOPPED)
        def _disabled(context: dg.RunFailureSensorContext) -> None:
            context.log.warning(
                "Email alert not configured (GMAIL_SMTP_USER / GMAIL_SMTP_APP_PASSWORD / "
                "ALERT_EMAIL_TO are unset) - run %s failed but no email was sent.",
                context.dagster_run.run_id,
            )

        return _disabled

    # Built-in Dagster helper, defaults to Gmail SMTP (smtp.gmail.com, SSL).
    # It already sends Content-type: text/html, so email_body_fn can return
    # real HTML - the default body just doesn't use that (plain <br> joins).
    return dg.make_email_on_run_failure_sensor(
        email_from=GMAIL_USER,
        email_password=GMAIL_APP_PASSWORD,
        email_to=[ALERT_EMAIL_TO],
        email_body_fn=_failure_email_body,
        email_subject_fn=_failure_email_subject,
        name="failure_email_alert",
        monitor_all_code_locations=True,
    )


failure_email_sensor = _build_failure_email_sensor()


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(sensors=[failure_email_sensor])
