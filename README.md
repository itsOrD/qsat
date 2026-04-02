# Risk Alert Service

A batch service that reads monthly account health data from Parquet, identifies at-risk accounts, computes how long they've been continuously at risk, and posts formatted alerts to region-specific Slack channels. Supports replay safety via idempotent alert persistence — the same month can be re-run without duplicate alerts.

Designed to run inside a customer's cloud environment (AWS/GCP) with a thin deployment footprint.

## Quick Start

```bash
docker compose up --build
```

Open [http://localhost:8000/docs](http://localhost:8000/docs) for the interactive Swagger UI. Pre-filled examples are ready to execute.

### Without Docker

```bash
pip install -r requirements.txt

# Terminal 1: mock Slack
make mock

# Terminal 2: service
make dev
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for component, sequence, and data flow diagrams.

```
app/
├── api/           HTTP layer (routes, Pydantic schemas)
├── core/          Business logic (alert engine, run orchestrator, config)
├── data/          Data access (storage URI resolution, Parquet reader)
├── integrations/  External systems (Slack webhook client, email notifier)
└── persistence/   Database (SQLite schema, queries)
```

The run engine (`app/core/run_engine.py`) is the only module that imports across all layers — everything else has a single responsibility and narrow dependency surface.

## Configuration

All settings are overridable via environment variables. See [`.env.example`](.env.example).

| Variable | Type | Default | Description |
|---|---|---|---|
| `SLACK_WEBHOOK_BASE_URL` | string | — | Base URL mode: POST to `{url}/{channel}` |
| `SLACK_WEBHOOK_URL` | string | — | Single webhook mode: POST to this URL |
| `DATABASE_PATH` | string | `./data/alerts.db` | SQLite database file path |
| `ARR_THRESHOLD` | int | `10000` | Minimum ARR to trigger alerts |
| `APP_BASE_URL` | string | `https://app.yourcompany.com` | Base URL for account detail links |
| `SUPPORT_EMAIL` | string | `support@quadsci.ai` | Recipient for unknown-region notifications |
| `SMTP_HOST` | string | — | SMTP server (enables real email delivery) |
| `SMTP_PORT` | int | `587` | SMTP port |
| `SMTP_FROM` | string | — | Sender email address |
| `APP_MODE` | enum | `demo` | `demo` disables RBAC for local demos; `secure` enforces RBAC |
| `RBAC_ENABLED` | bool | `null` | Optional override for RBAC behavior (normally inferred from `APP_MODE`) |
| `RBAC_RUNNER_TOKENS` | csv string | — | Tokens allowed to call `POST /runs` and `POST /preview` |
| `RBAC_VIEWER_TOKENS` | csv string | — | Tokens allowed to call `GET /runs/{run_id}` (runner tokens also work) |

**Slack mode precedence:** `SLACK_WEBHOOK_BASE_URL` > `SLACK_WEBHOOK_URL` > no Slack (logged warning).

## RBAC, Dev Mode, and Demo Mode

The service keeps **open-ended Parquet source selection** (`file://`, `s3://`, `gs://`) and secures execution with token RBAC.

### Recommended (customer/prod)

```bash
APP_MODE=secure
RBAC_RUNNER_TOKENS=runner-token-1
RBAC_VIEWER_TOKENS=viewer-token-1
```

Then call protected endpoints with:

```bash
-H "Authorization: Bearer <token>"
```

### Local dev / demo

For local-only demos, run:

```bash
make up-demo
```

For secure/prod-like compose runs, use:

```bash
RBAC_RUNNER_TOKENS=runner-token-1 RBAC_VIEWER_TOKENS=viewer-token-1 make up-secure
```

No file edits or comment/uncomment toggles are required; mode is selected by command-time flags/environment (`APP_MODE=demo` vs `APP_MODE=secure`).

## ARR Threshold Rationale

Default: **$10,000**

I analyzed the provided Parquet data before choosing a threshold. The ARR range is $0–$99,656. In the January 2026 at-risk population:

- 151 unique at-risk accounts after deduplication
- 10 have ARR = $0 (likely test or free-tier accounts)
- No accounts fall between $1 and $9,999
- 141 have ARR ≥ $10,000

A $10,000 threshold filters only the zero-ARR accounts — reducing noise without suppressing real risk. Configurable via `ARR_THRESHOLD` for different deployment contexts.

## API Reference

### POST /preview

Compute alerts without sending to Slack. Returns full alert details inline.

```bash
curl -s -X POST http://localhost:8000/preview \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer runner-token-1" \
  -d '{"source_uri": "file://./monthly_account_status.parquet", "month": "2026-01-01"}' \
  | python -m json.tool
```

Response:
```json
{
  "run_id": "c2fb4492-d0b5-4246-a520-7dac0392d0ed",
  "month": "2026-01-01",
  "dry_run": true,
  "alerts": [
    {
      "id": 1,
      "run_id": "c2fb4492-...",
      "account_id": "a00636",
      "month": "2026-01-01",
      "alert_type": "at_risk",
      "channel": "emea-risk-alerts",
      "status": "preview",
      "error": null,
      "sent_at": null,
      "created_at": "2026-03-29 17:31:10"
    }
  ],
  "counts": {
    "rows_scanned": 10587,
    "duplicates_found": 308,
    "total_at_risk": 151,
    "above_threshold": 141,
    "routable": 137,
    "unroutable": 4
  }
}
```

### POST /runs

Execute a full alert processing run synchronously. Blocks until all alerts are processed.

```bash
curl -s -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer runner-token-1" \
  -d '{"source_uri": "file://./monthly_account_status.parquet", "month": "2026-01-01"}' \
  | python -m json.tool
```

Response:
```json
{
  "run_id": "1455f5d8-8e66-46ea-a4e3-96f6c763a30b"
}
```

### GET /runs/{run_id}

Retrieve persisted run results including all alert outcomes.

```bash
curl -s \
  -H "Authorization: Bearer viewer-token-1" \
  http://localhost:8000/runs/1455f5d8-8e66-46ea-a4e3-96f6c763a30b | python -m json.tool
```

Response:
```json
{
  "run_id": "1455f5d8-8e66-46ea-a4e3-96f6c763a30b",
  "source_uri": "file://./monthly_account_status.parquet",
  "month": "2026-01-01",
  "dry_run": false,
  "status": "succeeded",
  "config_snapshot": {
    "arr_threshold": 10000,
    "app_base_url": "https://app.yourcompany.com",
    "region_channels": {
      "AMER": "amer-risk-alerts",
      "EMEA": "emea-risk-alerts",
      "APAC": "apac-risk-alerts"
    },
    "support_email": "support@quadsci.ai"
  },
  "counts": {
    "rows_scanned": 10587,
    "duplicates_found": 308,
    "alerts_sent": 137,
    "skipped_replay": 0,
    "failed_deliveries": 4
  },
  "alert_outcomes": ["... 141 entries"],
  "created_at": "2026-03-29 17:31:10",
  "completed_at": "2026-03-29T17:31:11.537229+00:00"
}
```

### GET /health

```json
{"status": "ok"}
```

## Design Decisions & Tradeoffs

### Raw `sqlite3` over SQLAlchemy

Two tables and five queries don't warrant an ORM. Raw `sqlite3` keeps the SQL visible and directly reviewable, and it's stdlib — zero additional dependencies. An SQLAlchemy implementation is available on the `feature/sqlalchemy-backend` branch for comparison.

At scale with concurrent writers, I'd migrate to Postgres.

### Idempotency keyed on business identity

The unique constraint is `(account_id, month, alert_type)` rather than `(run_id, account_id, ...)`. Re-running the same month with updated data correctly overwrites the previous outcome. Previously-sent alerts are skipped; previously-failed alerts are retried automatically.

### Partial Slack failures don't fail the run

A run with some failed deliveries still completes as `succeeded`. The `failed_deliveries` count communicates partial failure. One bad channel shouldn't block 136 good alerts.

### No default Slack channel

Accounts with unknown or missing regions are not sent to Slack — they're recorded as `failed` with reason `unknown_region`, and a single aggregated email notification is sent after the run. This forces data quality upstream rather than silently routing to a catch-all.

### Predicate pushdown despite single row group

The provided file has one row group, so pushdown doesn't physically skip data here. The code still uses PyArrow `filters` because it's the right pattern at scale — with multi-group files, entire row groups get skipped based on column statistics.

### Two Parquet reads, not one

Target month data needs all 9 columns for alert building. History needs only 4 (`account_id`, `month`, `status`, `updated_at`). Splitting into two reads enables column pruning on the larger history scan.

### Preview doesn't block real runs

`/preview` records outcomes with status `preview`. A subsequent `/runs` call treats previewed accounts as if no prior outcome exists — previews are informational, not commitments.

## Replay Safety

Re-running the same month is safe:

1. **Previously sent** → status becomes `skipped_replay`. No Slack message re-sent.
2. **Previously failed** → retried with current data. If Slack succeeds, outcome updates to `sent`.
3. **Previously previewed** → treated as no prior outcome. Real send proceeds normally.

The second run's `GET /runs/{id}` response shows `skipped_replay` counts reflecting how many alerts were already delivered.

## Testing

```bash
pip install -e ".[test]"   # Install test dependencies (pytest, httpx)
make test                  # Unit tests only (no network, no Docker)
make test-all              # Unit + integration (dry_run mode)
make test-mock             # Integration against mock Slack (requires: make mock)
```

See [tests/TEST_PLAN.md](tests/TEST_PLAN.md) for coverage details and future expansion.

Unit tests cover duration calculation (including Churned status, year boundaries, missing months), ARR threshold filtering, channel routing, Slack message formatting, SQLite UPSERT idempotency, Parquet dedup, and retry logic.

Integration tests run the full pipeline via FastAPI TestClient in three modes: `dry_run` (default, no network), `mock` (against the provided mock Slack server), and `live` (against a real Slack webhook).

## Docker

```bash
make up-demo                    # Start app + mock Slack (RBAC disabled)
RBAC_RUNNER_TOKENS=runner-token-1 RBAC_VIEWER_TOKENS=viewer-token-1 make up-secure
docker compose down             # Stop
```

The app runs at `localhost:8000`, mock Slack at `localhost:9000`. SQLite data persists across restarts via volume mount (`./data:/app/data`).

```bash
# Or run standalone
docker build -t quadsci-risk-alerts .
docker run -p 8000:8000 -v ./data:/app/data quadsci-risk-alerts
```

## Cloud Storage

| Scheme | Status | Notes |
|---|---|---|
| `file://` | Implemented, tested | Local filesystem |
| `gs://` | Implemented | PyArrow reads natively via `gcsfs`. Set `GOOGLE_APPLICATION_CREDENTIALS`. |
| `s3://` | Designed, not exercised | PyArrow reads via `s3fs` (requires `pip install s3fs`). |

## Unknown Region Handling

Accounts with null or unmapped regions are:
1. **Not sent to Slack** — recorded as `failed` with reason `unknown_region`
2. **Aggregated into a single email notification** after the run completes

The email backend is pluggable: `LoggingNotifier` (default — logs full content to stdout) or `SMTPNotifier` (when `SMTP_HOST` is configured). In local dev, the notification appears in container logs.

## Production Considerations

Things I'd change at scale but didn't build prematurely:

- **SQLite → Postgres:** SQLite is single-writer. Concurrent `/runs` requests serialize at the DB level. Postgres would support concurrent writers and distributed deployments.
- **LoggingNotifier → SMTP/SES:** The logging backend is a documented stub. Production would use real SMTP or AWS SES.
- **Synchronous → queue-based:** For large account volumes, a task queue (Celery, Cloud Tasks) would process alerts asynchronously.
- **Env vars → secrets management:** Production secrets should come from AWS Parameter Store, GCP Secret Manager, or Vault — not `.env` files.
- **Monitoring:** Alert on `failed_deliveries > 0`. Config snapshots stored per-run enable debugging threshold or routing changes over time.
