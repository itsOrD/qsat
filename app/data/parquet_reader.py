"""Parquet reading with column pruning, predicate pushdown, and dedup.

Two reads are performed:
  1. Target month — all columns needed for alert building.
  2. History — only columns needed for duration calculation.

Both reads use PyArrow filters for predicate pushdown (correct at scale
with multi-row-group files, even though the provided data has one group).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

TARGET_COLUMNS = [
    "account_id",
    "account_name",
    "account_region",
    "month",
    "status",
    "renewal_date",
    "account_owner",
    "arr",
    "updated_at",
]

HISTORY_COLUMNS = ["account_id", "month", "status", "updated_at"]


@dataclass
class ReadResult:
    """Container for Parquet read output."""

    at_risk_accounts: list[dict]
    history: dict[tuple[str, date], str]  # (account_id, month) -> status
    rows_scanned: int = 0
    duplicates_found: int = 0
    target_month_rows: int = 0
    history_rows: int = 0


def _dedup(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove duplicate (account_id, month) rows, keeping latest updated_at."""
    before = len(df)
    df = (
        df.sort_values("updated_at", ascending=False)
        .drop_duplicates(subset=["account_id", "month"], keep="first")
    )
    removed = before - len(df)
    return df, removed


def read_parquet_data(
    source_path: str,
    target_month: date,
) -> ReadResult:
    """Read and deduplicate Parquet data for alert processing.

    Args:
        source_path: Resolved path or URI for PyArrow.
        target_month: First-of-month date to process.

    Returns:
        ReadResult with at-risk accounts and history lookup.
    """
    # Read 1: target month — all columns for alert building
    target_table = pq.read_table(
        source_path,
        columns=TARGET_COLUMNS,
        filters=[("month", "=", target_month)],
    )
    target_df = target_table.to_pandas()
    target_rows = len(target_df)

    # Dedup target month first (need all statuses for correct dedup —
    # a Healthy row with a newer timestamp should beat an At Risk row)
    target_df, target_dupes = _dedup(target_df)

    # Filter to At Risk and extract IDs for narrowed history read
    at_risk_df = target_df[target_df["status"] == "At Risk"]
    at_risk_ids = at_risk_df["account_id"].tolist()

    # Read 2: history — only for at-risk accounts, only duration columns
    if at_risk_ids:
        history_table = pq.read_table(
            source_path,
            columns=HISTORY_COLUMNS,
            filters=[
                ("month", "<", target_month),
                ("account_id", "in", at_risk_ids),
            ],
        )
        history_df = history_table.to_pandas()
        history_rows = len(history_df)
        history_df, history_dupes = _dedup(history_df)
    else:
        history_df = pd.DataFrame(columns=HISTORY_COLUMNS)
        history_rows = 0
        history_dupes = 0

    total_scanned = target_rows + history_rows
    total_dupes = target_dupes + history_dupes

    if total_dupes > 0:
        log.warning(
            "Removed %d duplicate rows (%d target, %d history)",
            total_dupes,
            target_dupes,
            history_dupes,
        )

    # Convert at-risk accounts to list of dicts
    at_risk_accounts = []
    for _, row in at_risk_df.iterrows():
        account = {
            "account_id": row["account_id"],
            "account_name": row["account_name"],
            "account_region": row["account_region"],
            "month": row["month"],
            "status": row["status"],
            "renewal_date": row["renewal_date"],
            "account_owner": row["account_owner"],
            "arr": int(row["arr"]) if pd.notna(row["arr"]) else 0,
        }
        # Normalize pandas NaT/NaN to None
        for key in ("account_region", "renewal_date", "account_owner"):
            if pd.isna(account[key]):
                account[key] = None
        at_risk_accounts.append(account)

    # Build history lookup: (account_id, month) -> status
    # Vectorized extraction — avoids iterrows() overhead at scale
    history_lookup: dict[tuple[str, date], str] = dict(zip(
        zip(history_df["account_id"], history_df["month"]),
        history_df["status"],
    ))

    log.info(
        "Read %d rows (%d target, %d history for %d at-risk accounts), %d duplicates",
        total_scanned,
        target_rows,
        history_rows,
        len(at_risk_ids),
        total_dupes,
    )

    return ReadResult(
        at_risk_accounts=at_risk_accounts,
        history=history_lookup,
        rows_scanned=total_scanned,
        duplicates_found=total_dupes,
        target_month_rows=target_rows,
        history_rows=history_rows,
    )
