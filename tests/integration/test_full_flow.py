"""Integration test: full pipeline validation in three modes.

Controlled by TEST_SLACK_MODE env var:
  - dry_run (default): in-process TestClient, no network
  - mock: requires mock Slack running on localhost:9000
  - live: requires real Slack webhook URL
"""

from __future__ import annotations

import os

import pytest
import requests
from fastapi.testclient import TestClient

from app.main import app

FIXTURE_URI = "file://./tests/fixtures/test_accounts.parquet"
MONTH = "2026-01-01"

MODE = os.getenv("TEST_SLACK_MODE", "dry_run")
RUNNER_HEADERS = {"Authorization": "Bearer test-runner-token"}
VIEWER_HEADERS = {"Authorization": "Bearer test-viewer-token"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("APP_MODE", "secure")
    monkeypatch.setenv("RBAC_RUNNER_TOKENS", "test-runner-token")
    monkeypatch.setenv("RBAC_VIEWER_TOKENS", "test-viewer-token")
    with TestClient(app) as c:
        yield c


class TestDryRunMode:
    """Full pipeline without Slack sends."""

    def test_preview_returns_alerts(self, client):
        resp = client.post("/preview", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is True
        assert data["run_id"]
        assert len(data["alerts"]) > 0
        assert data["counts"]["rows_scanned"] > 0

    def test_run_dry_creates_record(self, client):
        resp = client.post("/runs", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
            "dry_run": True,
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        detail = client.get(f"/runs/{run_id}", headers=VIEWER_HEADERS)
        assert detail.status_code == 200
        run = detail.json()
        assert run["status"] == "succeeded"
        assert run["dry_run"] is True
        assert run["counts"]["rows_scanned"] > 0

    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_preview_requires_runner_token(self, client):
        resp = client.post("/preview", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
        })
        assert resp.status_code == 401

    def test_invalid_month_format(self, client):
        resp = client.post("/preview", json={
            "source_uri": FIXTURE_URI,
            "month": "not-a-date",
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 400

    def test_invalid_month_not_first(self, client):
        resp = client.post("/preview", json={
            "source_uri": FIXTURE_URI,
            "month": "2026-01-15",
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 400

    def test_missing_file(self, client):
        resp = client.post("/preview", json={
            "source_uri": "file://./nonexistent.parquet",
            "month": MONTH,
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 400

    def test_unsupported_scheme(self, client):
        resp = client.post("/preview", json={
            "source_uri": "ftp://bad",
            "month": MONTH,
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 422  # Pydantic validation rejects unknown schemes

    def test_run_not_found(self, client):
        resp = client.get("/runs/nonexistent-id", headers=VIEWER_HEADERS)
        assert resp.status_code == 404


@pytest.mark.skipif(MODE != "mock", reason="Requires TEST_SLACK_MODE=mock")
class TestMockSlackMode:
    """Tests with mock Slack server running on localhost:9000."""

    def test_run_sends_to_mock_slack(self, client):
        resp = client.post("/runs", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
            "dry_run": False,
        }, headers=RUNNER_HEADERS)
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        detail = client.get(f"/runs/{run_id}", headers=VIEWER_HEADERS)
        run = detail.json()
        assert run["status"] == "succeeded"
        assert run["counts"]["alerts_sent"] > 0

        # Check mock Slack received messages
        logs = requests.get("http://localhost:9000/logs?limit=100", timeout=5)
        assert logs.status_code == 200
        records = logs.json()["records"]
        channels = {r["channel"] for r in records}
        # Should have sent to at least one known channel
        assert channels & {"amer-risk-alerts", "emea-risk-alerts", "apac-risk-alerts"}

    def test_replay_shows_skipped(self, client):
        # First run
        client.post("/runs", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
            "dry_run": False,
        }, headers=RUNNER_HEADERS)
        # Second run (same month)
        resp = client.post("/runs", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
            "dry_run": False,
        }, headers=RUNNER_HEADERS)
        run_id = resp.json()["run_id"]
        detail = client.get(f"/runs/{run_id}", headers=VIEWER_HEADERS)
        run = detail.json()
        assert run["counts"]["skipped_replay"] > 0

    def test_unknown_region_not_in_slack(self, client):
        client.post("/runs", json={
            "source_uri": FIXTURE_URI,
            "month": MONTH,
            "dry_run": False,
        }, headers=RUNNER_HEADERS)
        logs = requests.get("http://localhost:9000/logs?limit=200", timeout=5)
        records = logs.json()["records"]
        # No messages should go to a channel for null/unknown region
        for r in records:
            payloads_text = str(r.get("payload", {}))
            assert "test_004" not in payloads_text or r["channel"] in (
                "amer-risk-alerts", "emea-risk-alerts", "apac-risk-alerts"
            )
