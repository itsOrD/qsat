"""Shared test fixtures."""

from __future__ import annotations

import os
import tempfile

import pytest

from app.core.config import Settings
from app.persistence.database import Database

FIXTURE_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "test_accounts.parquet"
)


@pytest.fixture
def tmp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = Database(db_path)
    yield db
    os.unlink(db_path)


@pytest.fixture
def settings(tmp_db):
    """Test settings with in-memory-like config."""
    return Settings(
        slack_webhook_base_url="http://localhost:9000/slack/webhook",
        database_path=tmp_db._db_path,
        arr_threshold=10_000,
        app_mode="secure",
        rbac_runner_tokens="test-runner-token",
        rbac_viewer_tokens="test-viewer-token",
    )


@pytest.fixture
def fixture_path():
    """Path to the synthetic test Parquet file."""
    return FIXTURE_PATH
