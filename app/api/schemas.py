"""Pydantic request/response models with rich OpenAPI metadata."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RunRequest(BaseModel):
    """Request body for POST /runs and POST /preview."""

    source_uri: str = Field(
        ...,
        description="Parquet file URI. Supports file://, gs://, s3://",
        json_schema_extra={
            "examples": [
                "file://./monthly_account_status.parquet",
                "gs://fde-take-home/monthly_account_status.parquet",
            ]
        },
    )
    month: str = Field(
        ...,
        description="Target month as first-of-month ISO date (YYYY-MM-01)",
        json_schema_extra={"examples": ["2026-01-01"]},
    )
    dry_run: bool = Field(
        default=False,
        description="If true, compute alerts without sending to Slack",
    )


class RunResponse(BaseModel):
    """Response body for POST /runs."""

    run_id: str


class PreviewResponse(BaseModel):
    """Response body for POST /preview with inline alert details."""

    run_id: str
    month: str
    dry_run: bool = True
    alerts: list[dict]
    counts: dict


class RunDetailResponse(BaseModel):
    """Response body for GET /runs/{run_id}."""

    run_id: str
    source_uri: str
    month: str
    dry_run: bool
    status: str
    config_snapshot: dict | None = None
    counts: dict
    alert_outcomes: list[dict]
    created_at: str
    completed_at: str | None = None
