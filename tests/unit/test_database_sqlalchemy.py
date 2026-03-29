"""Smoke tests for the SQLAlchemy database backend.

Verifies the core operations work: insert, upsert, idempotency, and run lifecycle.
"""

import os
import tempfile

import pytest

from app.persistence.database import Database


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    d = Database(path)
    yield d
    os.unlink(path)


def test_full_run_lifecycle(db):
    """Insert a run, add outcomes, complete it, and retrieve everything."""
    db.insert_run("r1", "file://test.parquet", "2026-01-01", False, {"arr_threshold": 10000})

    db.upsert_alert_outcome("r1", "a1", "2026-01-01", "amer-risk-alerts", "sent")
    db.upsert_alert_outcome("r1", "a2", "2026-01-01", None, "failed", error="unknown_region")

    db.complete_run("r1", "succeeded", rows_scanned=100, alerts_sent=1, failed_deliveries=1)

    run = db.get_run("r1")
    assert run is not None
    assert run["status"] == "succeeded"
    assert run["rows_scanned"] == 100
    assert run["config_snapshot"]["arr_threshold"] == 10000
    assert run["completed_at"] is not None
    assert len(run["alert_outcomes"]) == 2


def test_upsert_idempotency(db):
    """UPSERT overwrites on (account_id, month, alert_type) — no duplicates."""
    db.insert_run("r1", "file://test.parquet", "2026-01-01", False)

    db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch", "failed", error="timeout")
    db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch", "sent")

    run = db.get_run("r1")
    assert len(run["alert_outcomes"]) == 1
    assert run["alert_outcomes"][0]["status"] == "sent"
    assert run["alert_outcomes"][0]["error"] is None

    prior = db.get_prior_outcome("a1", "2026-01-01")
    assert prior["status"] == "sent"
