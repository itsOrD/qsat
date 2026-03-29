"""Unit tests for SQLite database layer."""

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


class TestSchemaInit:
    def test_creates_tables_on_first_init(self, db):
        run = db.get_run("nonexistent")
        assert run is None  # No crash, tables exist

    def test_idempotent_on_second_init(self, db):
        # Re-init should not crash (IF NOT EXISTS)
        Database(db._db_path)


class TestUpsert:
    def test_new_outcome_creates_row(self, db):
        db.insert_run("r1", "file://test.parquet", "2026-01-01", False)
        db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch1", "sent")
        run = db.get_run("r1")
        assert len(run["alert_outcomes"]) == 1
        assert run["alert_outcomes"][0]["status"] == "sent"

    def test_duplicate_key_updates_not_duplicates(self, db):
        db.insert_run("r1", "file://test.parquet", "2026-01-01", False)
        db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch1", "failed", error="timeout")
        db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch1", "sent")
        run = db.get_run("r1")
        assert len(run["alert_outcomes"]) == 1
        assert run["alert_outcomes"][0]["status"] == "sent"

    def test_status_change_from_failed_to_sent(self, db):
        db.insert_run("r1", "file://test.parquet", "2026-01-01", False)
        db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch1", "failed", error="500")
        db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch1", "sent")
        run = db.get_run("r1")
        assert run["alert_outcomes"][0]["status"] == "sent"
        assert run["alert_outcomes"][0]["error"] is None


class TestRunLifecycle:
    def test_insert_and_get_run(self, db):
        db.insert_run("r1", "file://test.parquet", "2026-01-01", True, {"arr_threshold": 10000})
        run = db.get_run("r1")
        assert run["run_id"] == "r1"
        assert run["status"] == "running"
        assert run["config_snapshot"]["arr_threshold"] == 10000

    def test_complete_run(self, db):
        db.insert_run("r1", "file://test.parquet", "2026-01-01", False)
        db.complete_run("r1", "succeeded", rows_scanned=100, alerts_sent=5)
        run = db.get_run("r1")
        assert run["status"] == "succeeded"
        assert run["rows_scanned"] == 100
        assert run["alerts_sent"] == 5
        assert run["completed_at"] is not None

    def test_get_nonexistent_run(self, db):
        assert db.get_run("nope") is None


class TestPriorOutcome:
    def test_returns_none_when_no_prior(self, db):
        assert db.get_prior_outcome("a1", "2026-01-01") is None

    def test_returns_prior_status(self, db):
        db.insert_run("r1", "file://test.parquet", "2026-01-01", False)
        db.upsert_alert_outcome("r1", "a1", "2026-01-01", "ch1", "sent")
        prior = db.get_prior_outcome("a1", "2026-01-01")
        assert prior["status"] == "sent"


class TestForeignKey:
    def test_fk_enforced(self, db):
        with pytest.raises(Exception):
            db.upsert_alert_outcome("nonexistent_run", "a1", "2026-01-01", "ch1", "sent")
