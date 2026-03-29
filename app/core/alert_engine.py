"""Alert engine: duration calculation, ARR filtering, routing, and formatting.

This module contains the core business logic for determining which
accounts should be alerted and how to present them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

log = logging.getLogger(__name__)


@dataclass
class AlertRecord:
    """A processed alert ready for routing and delivery."""

    account_id: str
    account_name: str
    account_region: str | None
    month: date
    arr: int
    renewal_date: date | None
    account_owner: str | None
    duration_months: int
    risk_start_month: date
    channel: str | None
    routable: bool
    unroutable_reason: str | None = None


def compute_duration(
    account_id: str,
    target_month: date,
    history: dict[tuple[str, date], str],
) -> tuple[int, date]:
    """Count backward month-by-month while status is 'At Risk'.

    Args:
        account_id: The account to compute duration for.
        target_month: The current at-risk month (included in count).
        history: Lookup of (account_id, month) -> status.

    Returns:
        (duration_months, risk_start_month) tuple.
    """
    duration = 1
    current = target_month

    while True:
        # Step back one month
        if current.month == 1:
            prev = date(current.year - 1, 12, 1)
        else:
            prev = date(current.year, current.month - 1, 1)

        prev_status = history.get((account_id, prev))

        # Stop if missing or not At Risk
        if prev_status != "At Risk":
            break

        duration += 1
        current = prev

    risk_start = current
    return duration, risk_start


def filter_by_threshold(
    accounts: list[dict],
    arr_threshold: int,
) -> tuple[list[dict], list[dict]]:
    """Split accounts into above-threshold and below-threshold.

    Args:
        accounts: At-risk account dicts from the parquet reader.
        arr_threshold: Minimum ARR to include.

    Returns:
        (above, below) tuple of account lists.
    """
    above = []
    below = []
    for acct in accounts:
        arr = acct.get("arr") or 0
        if arr >= arr_threshold:
            above.append(acct)
        else:
            below.append(acct)
            log.info(
                "Filtered out %s (ARR=$%s below threshold $%s)",
                acct["account_id"],
                f"{arr:,}",
                f"{arr_threshold:,}",
            )
    return above, below


def route_to_channel(
    account_region: str | None,
    region_channels: dict[str, str],
) -> tuple[str | None, bool, str | None]:
    """Determine the Slack channel for an account's region.

    Args:
        account_region: The account's region string (may be None).
        region_channels: Mapping of region -> channel name.

    Returns:
        (channel, routable, reason) tuple.
    """
    if not account_region or account_region not in region_channels:
        return None, False, "unknown_region"
    return region_channels[account_region], True, None


def build_alert_records(
    accounts: list[dict],
    history: dict[tuple[str, date], str],
    arr_threshold: int,
    region_channels: dict[str, str],
) -> tuple[list[AlertRecord], list[dict]]:
    """Build AlertRecords from raw at-risk accounts.

    Applies ARR threshold filtering, duration calculation, and channel routing.

    Returns:
        (alert_records, filtered_out_accounts) tuple.
    """
    above, below = filter_by_threshold(accounts, arr_threshold)
    records: list[AlertRecord] = []

    for acct in above:
        target_month = acct["month"]
        duration, risk_start = compute_duration(
            acct["account_id"], target_month, history
        )
        channel, routable, reason = route_to_channel(
            acct.get("account_region"), region_channels
        )

        records.append(
            AlertRecord(
                account_id=acct["account_id"],
                account_name=acct["account_name"],
                account_region=acct.get("account_region"),
                month=target_month,
                arr=acct.get("arr") or 0,
                renewal_date=acct.get("renewal_date"),
                account_owner=acct.get("account_owner"),
                duration_months=duration,
                risk_start_month=risk_start,
                channel=channel,
                routable=routable,
                unroutable_reason=reason,
            )
        )

    return records, below


def format_slack_message(
    alert: AlertRecord,
    app_base_url: str,
) -> dict:
    """Build a Slack Block Kit payload for an alert.

    Args:
        alert: The processed alert record.
        app_base_url: Base URL for account detail links.

    Returns:
        Dict with 'text' (fallback) and 'blocks' (visual layout).
    """
    fallback = f"\U0001f6a9 At Risk: {alert.account_name} ({alert.account_id})"

    renewal_str = (
        str(alert.renewal_date) if alert.renewal_date else "Unknown"
    )

    lines = [
        f"*\U0001f6a9 At Risk: {alert.account_name} ({alert.account_id})*",
        f"*Region:* {alert.account_region or 'Unknown'}",
        f"*At Risk for:* {alert.duration_months} month(s) (since {alert.risk_start_month})",
        f"*ARR:* ${alert.arr:,}",
        f"*Renewal Date:* {renewal_str}",
    ]

    if alert.account_owner:
        lines.append(f"*Owner:* {alert.account_owner}")

    details_url = f"{app_base_url}/accounts/{alert.account_id}"
    lines.append(f"*Details:* <{details_url}>")

    return {
        "text": fallback,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(lines),
                },
            }
        ],
    }
