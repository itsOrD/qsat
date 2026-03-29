"""FastAPI application entry point.

Configures logging, initializes the database on startup,
and mounts all API routes.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import init_dependencies, router
from app.core.config import Settings
from app.persistence.database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize settings and database on startup."""
    settings = Settings()
    db = Database(settings.database_path)
    init_dependencies(settings, db)
    logging.getLogger(__name__).info("Risk Alert Service started")
    yield


app = FastAPI(
    title="Risk Alert Service",
    description=(
        "Batch service that identifies at-risk accounts from Parquet data "
        "and posts formatted alerts to region-specific Slack channels."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)
