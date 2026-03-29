"""SQLAlchemy persistence for runs and alert outcomes.

This branch uses SQLAlchemy instead of raw sqlite3 to demonstrate
comfort with ORM-based persistence. The public interface is identical
to the raw sqlite3 version on main.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    relationship,
    sessionmaker,
)

log = logging.getLogger(__name__)


# ---- Models ----


class Base(DeclarativeBase):
    pass


class Run(Base):
    __tablename__ = "runs"

    run_id = Column(String, primary_key=True)
    source_uri = Column(String, nullable=False)
    month = Column(String, nullable=False)
    dry_run = Column(Boolean, nullable=False, default=False)
    status = Column(String, nullable=False, default="running")
    config_snapshot = Column(Text)
    rows_scanned = Column(Integer, default=0)
    duplicates_found = Column(Integer, default=0)
    alerts_sent = Column(Integer, default=0)
    skipped_replay = Column(Integer, default=0)
    failed_deliveries = Column(Integer, default=0)
    created_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)

    outcomes = relationship("AlertOutcome", back_populates="run")


class AlertOutcome(Base):
    __tablename__ = "alert_outcomes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, ForeignKey("runs.run_id"), nullable=False)
    account_id = Column(String, nullable=False)
    month = Column(String, nullable=False)
    alert_type = Column(String, nullable=False, default="at_risk")
    channel = Column(String)
    status = Column(String, nullable=False)
    error = Column(Text)
    sent_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, default=func.now())

    run = relationship("Run", back_populates="outcomes")

    __table_args__ = (
        UniqueConstraint("account_id", "month", "alert_type"),
    )


# ---- Database class ----


class Database:
    """SQLAlchemy-backed persistence with the same interface as the raw sqlite3 version."""

    def __init__(self, db_path: str) -> None:
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self._db_path = db_path
        self._engine = create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False},
        )

        # Enable foreign keys for SQLite
        from sqlalchemy import event

        @event.listens_for(self._engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.close()

        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine)
        log.info("Database initialized at %s (SQLAlchemy)", db_path)

    def _session(self) -> Session:
        return self._session_factory()

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
        with self._session() as session:
            run = Run(
                run_id=run_id,
                source_uri=source_uri,
                month=month,
                dry_run=dry_run,
                config_snapshot=snapshot_json,
            )
            session.add(run)
            session.commit()

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
        now = datetime.now(timezone.utc)
        with self._session() as session:
            run = session.query(Run).filter_by(run_id=run_id).one()
            run.status = status
            run.rows_scanned = rows_scanned
            run.duplicates_found = duplicates_found
            run.alerts_sent = alerts_sent
            run.skipped_replay = skipped_replay
            run.failed_deliveries = failed_deliveries
            run.completed_at = now
            session.commit()

    def get_run(self, run_id: str) -> dict | None:
        """Return run data with alert outcomes, or None if not found."""
        with self._session() as session:
            run = session.query(Run).filter_by(run_id=run_id).first()
            if not run:
                return None

            result = {
                "run_id": run.run_id,
                "source_uri": run.source_uri,
                "month": run.month,
                "dry_run": run.dry_run,
                "status": run.status,
                "config_snapshot": (
                    json.loads(run.config_snapshot) if run.config_snapshot else None
                ),
                "rows_scanned": run.rows_scanned,
                "duplicates_found": run.duplicates_found,
                "alerts_sent": run.alerts_sent,
                "skipped_replay": run.skipped_replay,
                "failed_deliveries": run.failed_deliveries,
                "created_at": str(run.created_at) if run.created_at else None,
                "completed_at": str(run.completed_at) if run.completed_at else None,
            }

            outcomes = (
                session.query(AlertOutcome)
                .filter_by(run_id=run_id)
                .order_by(AlertOutcome.id)
                .all()
            )
            result["alert_outcomes"] = [
                {
                    "id": o.id,
                    "run_id": o.run_id,
                    "account_id": o.account_id,
                    "month": o.month,
                    "alert_type": o.alert_type,
                    "channel": o.channel,
                    "status": o.status,
                    "error": o.error,
                    "sent_at": str(o.sent_at) if o.sent_at else None,
                    "created_at": str(o.created_at) if o.created_at else None,
                }
                for o in outcomes
            ]

        return result

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
        with self._session() as session:
            existing = (
                session.query(AlertOutcome)
                .filter_by(account_id=account_id, month=month, alert_type="at_risk")
                .first()
            )
            if existing:
                existing.run_id = run_id
                existing.channel = channel
                existing.status = status
                existing.error = error
                existing.sent_at = sent_at
            else:
                outcome = AlertOutcome(
                    run_id=run_id,
                    account_id=account_id,
                    month=month,
                    alert_type="at_risk",
                    channel=channel,
                    status=status,
                    error=error,
                    sent_at=sent_at,
                )
                session.add(outcome)
            session.commit()

    def get_prior_outcome(
        self, account_id: str, month: str
    ) -> dict | None:
        """Check if an alert outcome already exists for this account+month."""
        with self._session() as session:
            outcome = (
                session.query(AlertOutcome)
                .filter_by(account_id=account_id, month=month, alert_type="at_risk")
                .first()
            )
            if not outcome:
                return None
            return {"status": outcome.status, "error": outcome.error}
