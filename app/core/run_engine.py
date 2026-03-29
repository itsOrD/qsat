"""Run engine: orchestrates a full alert processing run.

This is the only module that imports from all other modules. It ties
together storage resolution, Parquet reading, alert logic, Slack
delivery, email notification, and database persistence.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

from app.core.alert_engine import (
    AlertRecord,
    build_alert_records,
    format_slack_message,
)
from app.core.config import Settings
from app.core.run_logger import (
    log_alert_progress,
    log_data_loaded,
    log_run_start,
    log_run_summary,
)
from app.data.parquet_reader import read_parquet_data
from app.data.storage import resolve_source_uri
from app.integrations.email_notifier import (
    build_unknown_region_email,
    get_notifier,
)
from app.integrations.slack_client import send_slack_message
from app.persistence.database import Database

log = logging.getLogger(__name__)


def execute_run(
    source_uri: str,
    month: date,
    dry_run: bool,
    settings: Settings,
    db: Database,
) -> dict:
    """Execute a full alert processing run.

    Args:
        source_uri: Parquet file URI.
        month: Target month (first-of-month date).
        dry_run: If True, compute alerts without sending to Slack.
        settings: Application settings.
        db: Database instance for persistence.

    Returns:
        Dict with run_id and processing statistics.

    Raises:
        ValueError: Invalid URI scheme.
        FileNotFoundError: Local file not found.
        Exception: Parquet read or DB errors abort the run.
    """
    run_id = str(uuid.uuid4())
    month_str = month.isoformat()
    t_start = time.monotonic()

    # Step 1-3: Create run record
    db.insert_run(
        run_id=run_id,
        source_uri=source_uri,
        month=month_str,
        dry_run=dry_run,
        config_snapshot=settings.snapshot(),
    )

    log_run_start(run_id, month_str, source_uri, dry_run)

    try:
        # Step 4: Resolve source URI
        resolved_path = resolve_source_uri(source_uri)

        # Step 5-7: Read and process Parquet data
        read_result = read_parquet_data(resolved_path, month)

        # Build alert records (duration, threshold, routing)
        alert_records, filtered_out = build_alert_records(
            accounts=read_result.at_risk_accounts,
            history=read_result.history,
            arr_threshold=settings.arr_threshold,
            region_channels=settings.region_channels,
        )

    except Exception:
        log.exception("Run %s failed during data processing", run_id)
        db.complete_run(run_id, status="failed")
        raise

    log_data_loaded(
        rows_scanned=read_result.rows_scanned,
        duplicates=read_result.duplicates_found,
        at_risk=len(read_result.at_risk_accounts),
        above_threshold=len(alert_records),
        below_threshold=len(filtered_out),
    )

    # Step 8: Process alerts concurrently
    counters = {"sent": 0, "skipped_replay": 0, "failed": 0}
    channel_counts: Counter = Counter()
    unroutable_accounts: list[dict] = []
    total_alerts = len(alert_records)
    progress_lock = threading.Lock()
    progress_counter = [0]  # mutable for closure

    def _process_one(alert: AlertRecord) -> None:
        outcome = "failed"
        try:
            outcome = _process_single_alert(
                alert=alert,
                run_id=run_id,
                month_str=month_str,
                dry_run=dry_run,
                settings=settings,
                db=db,
                counters=counters,
                unroutable_accounts=unroutable_accounts,
                channel_counts=channel_counts,
                lock=progress_lock,
            )
        except Exception:
            log.exception(
                "Unexpected error processing alert for %s", alert.account_id
            )
            with progress_lock:
                counters["failed"] += 1
            db.upsert_alert_outcome(
                run_id=run_id,
                account_id=alert.account_id,
                month=month_str,
                channel=alert.channel,
                status="failed",
                error="unexpected_error",
            )

        with progress_lock:
            progress_counter[0] += 1
            log_alert_progress(progress_counter[0], total_alerts, alert.account_id, outcome)

    max_workers = 1 if dry_run else 10
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_process_one, alert) for alert in alert_records]
        for future in as_completed(futures):
            future.result()  # propagate any uncaught exceptions

    # Step 9: Send unknown region notification
    if unroutable_accounts and not dry_run:
        _send_unknown_region_notification(
            run_id, month_str, unroutable_accounts, settings
        )

    # Step 10: Update run record
    db.complete_run(
        run_id=run_id,
        status="succeeded",
        rows_scanned=read_result.rows_scanned,
        duplicates_found=read_result.duplicates_found,
        alerts_sent=counters["sent"],
        skipped_replay=counters["skipped_replay"],
        failed_deliveries=counters["failed"],
    )

    elapsed_ms = int((time.monotonic() - t_start) * 1000)
    routable_count = sum(1 for a in alert_records if a.routable)
    unroutable_count = sum(1 for a in alert_records if not a.routable)

    log_run_summary(
        run_id=run_id,
        status="succeeded",
        counters=counters,
        channel_counts=channel_counts,
        unroutable_count=unroutable_count,
        dry_run=dry_run,
        elapsed_ms=elapsed_ms,
    )

    return {
        "run_id": run_id,
        "total_at_risk": len(read_result.at_risk_accounts),
        "below_threshold": len(filtered_out),
        "above_threshold": len(alert_records),
        "routable": routable_count,
        "unroutable": unroutable_count,
        "rows_scanned": read_result.rows_scanned,
        "duplicates_found": read_result.duplicates_found,
    }


def _process_single_alert(
    alert: AlertRecord,
    run_id: str,
    month_str: str,
    dry_run: bool,
    settings: Settings,
    db: Database,
    counters: dict[str, int],
    unroutable_accounts: list[dict],
    channel_counts: Counter | None = None,
    lock: threading.Lock | None = None,
) -> str:
    """Process a single alert through the idempotency gate and delivery.

    Returns the outcome string for progress logging.
    Thread-safe when lock is provided.
    """
    def _inc(key: str) -> None:
        if lock:
            with lock:
                counters[key] += 1
        else:
            counters[key] += 1

    # Step 8d: Check idempotency gate
    prior = db.get_prior_outcome(alert.account_id, month_str)
    if prior:
        prior_status = prior["status"]
        if prior_status == "sent":
            _inc("skipped_replay")
            db.upsert_alert_outcome(
                run_id=run_id,
                account_id=alert.account_id,
                month=month_str,
                channel=alert.channel,
                status="skipped_replay",
            )
            return "skipped_replay"
        # preview or failed -> proceed to retry/send

    # Step 8e: Dry run
    if dry_run:
        db.upsert_alert_outcome(
            run_id=run_id,
            account_id=alert.account_id,
            month=month_str,
            channel=alert.channel,
            status="preview",
        )
        return "preview"

    # Step 8f: Unroutable
    if not alert.routable:
        _inc("failed")
        if lock:
            with lock:
                unroutable_accounts.append({
                    "account_id": alert.account_id,
                    "account_name": alert.account_name,
                    "account_region": alert.account_region,
                    "arr": alert.arr,
                })
        else:
            unroutable_accounts.append({
                "account_id": alert.account_id,
                "account_name": alert.account_name,
                "account_region": alert.account_region,
                "arr": alert.arr,
            })
        db.upsert_alert_outcome(
            run_id=run_id,
            account_id=alert.account_id,
            month=month_str,
            channel=None,
            status="failed",
            error=alert.unroutable_reason,
        )
        return "failed"

    # Step 8g-h: Send Slack alert
    payload = format_slack_message(alert, settings.app_base_url)
    success, error = send_slack_message(
        payload=payload,
        channel=alert.channel,
        base_url=settings.slack_webhook_base_url,
        webhook_url=settings.slack_webhook_url,
    )

    now = datetime.now(timezone.utc).isoformat()
    if success:
        _inc("sent")
        if channel_counts is not None:
            if lock:
                with lock:
                    channel_counts[alert.channel] += 1
            else:
                channel_counts[alert.channel] += 1
        db.upsert_alert_outcome(
            run_id=run_id,
            account_id=alert.account_id,
            month=month_str,
            channel=alert.channel,
            status="sent",
            sent_at=now,
        )
        return "sent"
    else:
        _inc("failed")
        db.upsert_alert_outcome(
            run_id=run_id,
            account_id=alert.account_id,
            month=month_str,
            channel=alert.channel,
            status="failed",
            error=error,
        )
        return "failed"


def _send_unknown_region_notification(
    run_id: str,
    month_str: str,
    unroutable_accounts: list[dict],
    settings: Settings,
) -> None:
    """Send a single aggregated notification for all unroutable accounts."""
    notifier = get_notifier(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_from=settings.smtp_from,
    )
    subject, body = build_unknown_region_email(run_id, month_str, unroutable_accounts)
    if not notifier.send(to=settings.support_email, subject=subject, body=body):
        log.error(
            "Failed to send unknown-region notification for run %s (%d accounts)",
            run_id,
            len(unroutable_accounts),
        )
