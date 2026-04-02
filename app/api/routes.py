"""FastAPI route definitions for the Risk Alert Service."""

from __future__ import annotations

from datetime import date
from secrets import compare_digest

from fastapi import APIRouter, Header, HTTPException

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

# Set during app lifespan (see main.py)
_settings: Settings | None = None
_db: Database | None = None


def init_dependencies(settings: Settings, db: Database) -> None:
    """Inject dependencies from the app lifespan."""
    global _settings, _db
    _settings = settings
    _db = db


def _deps() -> tuple[Settings, Database]:
    """Return initialized dependencies or raise 503."""
    if _settings is None or _db is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return _settings, _db


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


def _extract_bearer_token(authorization: str | None) -> str | None:
    """Extract Bearer token from Authorization header."""
    if not authorization:
        return None
    # Normalize whitespace and tolerate multiple spaces or other whitespace separators.
    header = authorization.strip()
    if not header:
        return None
    parts = header.split(None, 1)  # split on any whitespace, at most once
    if len(parts) != 2:
        return None
    scheme, token = parts[0], parts[1].strip()
    if scheme.lower() != "bearer" or not token:
        return None
    return token


def _require_role(
    settings: Settings,
    required_role: str,
    authorization: str | None,
) -> None:
    """Enforce RBAC for protected endpoints."""
    if not settings.auth_required():
        return

    token = _extract_bearer_token(authorization)
    if token is None:
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    runner_tokens = settings.runner_tokens()
    viewer_tokens_only = settings.viewer_tokens()
    viewer_tokens = viewer_tokens_only | runner_tokens

    # Global misconfiguration: RBAC enabled but no tokens of any kind configured
    if not runner_tokens and not viewer_tokens_only:
        raise HTTPException(status_code=503, detail="RBAC is enabled but no tokens configured")

    # Role-specific misconfiguration: runner endpoints require runner tokens
    if required_role == "runner" and not runner_tokens:
        raise HTTPException(
            status_code=503,
            detail="RBAC is enabled but no runner tokens configured",
        )
    token_set = runner_tokens if required_role == "runner" else viewer_tokens
    # Use a list comprehension (not a generator) so that compare_digest is
    # called for *every* configured token before any result is inspected.
    # A generator expression passed to any() short-circuits on the first True,
    # meaning the number of digest calls — and therefore wall-clock time —
    # varies with the matching token's position in the list.  Evaluating all
    # comparisons up front eliminates that timing side-channel.
    authorized = any([compare_digest(token, t) for t in token_set])
    if not authorized:
        raise HTTPException(status_code=403, detail="Forbidden")


@router.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@router.post("/runs", response_model=RunResponse)
def create_run(req: RunRequest, authorization: str | None = Header(default=None)):
    """Execute a full alert processing run (synchronous).

    Reads Parquet data, computes alerts for the specified month,
    sends Slack messages (unless dry_run), and persists all outcomes.
    """
    settings, db = _deps()
    _require_role(settings, "runner", authorization)
    month = _validate_month(req.month)

    try:
        result = execute_run(
            source_uri=req.source_uri,
            month=month,
            dry_run=req.dry_run,
            settings=settings,
            db=db,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return RunResponse(run_id=result["run_id"])


@router.post("/preview", response_model=PreviewResponse)
def preview(req: RunRequest, authorization: str | None = Header(default=None)):
    """Preview alerts without sending to Slack.

    Forces dry_run=true and returns full alert details inline.
    """
    settings, db = _deps()
    _require_role(settings, "runner", authorization)
    month = _validate_month(req.month)

    try:
        result = execute_run(
            source_uri=req.source_uri,
            month=month,
            dry_run=True,
            settings=settings,
            db=db,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    run_id = result["run_id"]
    run_data = db.get_run(run_id)
    return PreviewResponse(
        run_id=run_id,
        month=req.month,
        dry_run=True,
        alerts=run_data.get("alert_outcomes", []),
        counts={
            "rows_scanned": result["rows_scanned"],
            "duplicates_found": result["duplicates_found"],
            "total_at_risk": result["total_at_risk"],
            "above_threshold": result["above_threshold"],
            "routable": result["routable"],
            "unroutable": result["unroutable"],
        },
    )


@router.get("/runs/{run_id}", response_model=RunDetailResponse)
def get_run(run_id: str, authorization: str | None = Header(default=None)):
    """Retrieve persisted run results including alert outcomes."""
    settings, db = _deps()
    _require_role(settings, "viewer", authorization)
    run_data = db.get_run(run_id)
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
