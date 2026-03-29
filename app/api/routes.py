"""FastAPI route definitions for the Risk Alert Service."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException

from app.api.schemas import (
    PreviewResponse,
    RunDetailResponse,
    RunRequest,
    RunResponse,
)
from app.core.config import Settings
from app.core.run_engine import execute_run
from app.persistence.database import Database

router = APIRouter()

# These are set during app lifespan (see main.py)
_settings: Settings | None = None
_db: Database | None = None


def init_dependencies(settings: Settings, db: Database) -> None:
    """Inject dependencies from the app lifespan."""
    global _settings, _db
    _settings = settings
    _db = db


def _validate_month(month_str: str) -> date:
    """Validate that month is a first-of-month ISO date."""
    try:
        d = date.fromisoformat(month_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid month format: '{month_str}'. Expected YYYY-MM-DD.",
        )
    if d.day != 1:
        raise HTTPException(
            status_code=400,
            detail=f"Month must be first day of month: '{month_str}'. Got day={d.day}.",
        )
    return d


@router.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@router.post("/runs", response_model=RunResponse)
def create_run(req: RunRequest):
    """Execute a full alert processing run (synchronous).

    Reads Parquet data, computes alerts for the specified month,
    sends Slack messages (unless dry_run), and persists all outcomes.
    """
    month = _validate_month(req.month)

    try:
        run_id = execute_run(
            source_uri=req.source_uri,
            month=month,
            dry_run=req.dry_run,
            settings=_settings,
            db=_db,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return RunResponse(run_id=run_id)


@router.post("/preview", response_model=PreviewResponse)
def preview(req: RunRequest):
    """Preview alerts without sending to Slack.

    Forces dry_run=true and returns full alert details inline.
    """
    month = _validate_month(req.month)

    try:
        run_id = execute_run(
            source_uri=req.source_uri,
            month=month,
            dry_run=True,
            settings=_settings,
            db=_db,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    run_data = _db.get_run(run_id)
    return PreviewResponse(
        run_id=run_id,
        month=req.month,
        dry_run=True,
        alerts=run_data.get("alert_outcomes", []),
        counts={
            "rows_scanned": run_data.get("rows_scanned", 0),
            "duplicates_found": run_data.get("duplicates_found", 0),
            "alerts_sent": run_data.get("alerts_sent", 0),
            "skipped_replay": run_data.get("skipped_replay", 0),
            "failed_deliveries": run_data.get("failed_deliveries", 0),
        },
    )


@router.get("/runs/{run_id}", response_model=RunDetailResponse)
def get_run(run_id: str):
    """Retrieve persisted run results including alert outcomes."""
    run_data = _db.get_run(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    return RunDetailResponse(
        run_id=run_data["run_id"],
        source_uri=run_data["source_uri"],
        month=run_data["month"],
        dry_run=bool(run_data["dry_run"]),
        status=run_data["status"],
        config_snapshot=run_data.get("config_snapshot"),
        counts={
            "rows_scanned": run_data.get("rows_scanned", 0),
            "duplicates_found": run_data.get("duplicates_found", 0),
            "alerts_sent": run_data.get("alerts_sent", 0),
            "skipped_replay": run_data.get("skipped_replay", 0),
            "failed_deliveries": run_data.get("failed_deliveries", 0),
        },
        alert_outcomes=run_data.get("alert_outcomes", []),
        created_at=run_data.get("created_at", ""),
        completed_at=run_data.get("completed_at"),
    )
