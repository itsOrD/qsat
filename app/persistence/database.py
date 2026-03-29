"""SQLite persistence for runs and alert outcomes.

Uses raw sqlite3 (not SQLAlchemy) because two tables and a handful of
queries don't benefit from an ORM. The SQL is visible and reviewable,
and sqlite3 is stdlib — zero additional dependency.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    source_uri TEXT NOT NULL,
    month TEXT NOT NULL,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'running',
    config_snapshot TEXT,
    rows_scanned INTEGER DEFAULT 0,
    duplicates_found INTEGER DEFAULT 0,
    alerts_sent INTEGER DEFAULT 0,
    skipped_replay INTEGER DEFAULT 0,
    failed_deliveries INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alert_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    month TEXT NOT NULL,
    alert_type TEXT NOT NULL DEFAULT 'at_risk',
    channel TEXT,
    status TEXT NOT NULL,
    error TEXT,
    sent_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, month, alert_type),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
"""

UPSERT_ALERT = """
INSERT INTO alert_outcomes (run_id, account_id, month, alert_type, channel, status, error, sent_at)
VALUES (?, ?, ?, 'at_risk', ?, ?, ?, ?)
ON CONFLICT(account_id, month, alert_type) DO UPDATE SET
    status = excluded.status,
    run_id = excluded.run_id,
    channel = excluded.channel,
    error = excluded.error,
    sent_at = excluded.sent_at;
"""


class Database:
    """Thin wrapper around SQLite for run and alert persistence."""

    def __init__(self, db_path: str) -> None:
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self._db_path = db_path
        self._init_schema()
        log.info("Database initialized at %s", db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    # ---- Runs ----

    def insert_run(
        self,
        run_id: str,
        source_uri: str,
        month: str,
        dry_run: bool,
        config_snapshot: dict | None = None,
    ) -> None:
        """Insert a new run record with status='running'."""
        snapshot_json = json.dumps(config_snapshot) if config_snapshot else None
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO runs (run_id, source_uri, month, dry_run, config_snapshot)
                   VALUES (?, ?, ?, ?, ?)""",
                (run_id, source_uri, month, dry_run, snapshot_json),
            )

    def complete_run(
        self,
        run_id: str,
        status: str,
        rows_scanned: int = 0,
        duplicates_found: int = 0,
        alerts_sent: int = 0,
        skipped_replay: int = 0,
        failed_deliveries: int = 0,
    ) -> None:
        """Update a run record with final counts and status."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """UPDATE runs SET
                       status = ?,
                       rows_scanned = ?,
                       duplicates_found = ?,
                       alerts_sent = ?,
                       skipped_replay = ?,
                       failed_deliveries = ?,
                       completed_at = ?
                   WHERE run_id = ?""",
                (
                    status,
                    rows_scanned,
                    duplicates_found,
                    alerts_sent,
                    skipped_replay,
                    failed_deliveries,
                    now,
                    run_id,
                ),
            )

    def get_run(self, run_id: str) -> dict | None:
        """Return run data with alert outcomes, or None if not found."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if not row:
                return None

            run = dict(row)
            if run.get("config_snapshot"):
                run["config_snapshot"] = json.loads(run["config_snapshot"])

            outcomes = conn.execute(
                "SELECT * FROM alert_outcomes WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            run["alert_outcomes"] = [dict(o) for o in outcomes]

        return run

    # ---- Alert Outcomes ----

    def upsert_alert_outcome(
        self,
        run_id: str,
        account_id: str,
        month: str,
        channel: str | None,
        status: str,
        error: str | None = None,
        sent_at: str | None = None,
    ) -> None:
        """Insert or update an alert outcome (idempotent on account_id + month)."""
        with self._connect() as conn:
            conn.execute(
                UPSERT_ALERT,
                (run_id, account_id, month, channel, status, error, sent_at),
            )

    def get_prior_outcome(
        self, account_id: str, month: str
    ) -> dict | None:
        """Check if an alert outcome already exists for this account+month."""
        with self._connect() as conn:
            row = conn.execute(
                """SELECT status, error FROM alert_outcomes
                   WHERE account_id = ? AND month = ? AND alert_type = 'at_risk'""",
                (account_id, month),
            ).fetchone()
        return dict(row) if row else None
