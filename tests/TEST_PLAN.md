# Test Plan

## Included

### Unit Tests
- **Alert engine:** duration calculation (all edge cases including Churned, year boundary, gaps), ARR threshold filtering, channel routing, Slack message formatting
- **Database:** schema creation (idempotent), UPSERT behavior, foreign key enforcement, run lifecycle
- **Slack client:** URL construction (base URL, webhook, precedence), retry logic (429, 500, connection errors), no-retry on 400
- **Parquet reader:** target month filtering, dedup (keeps latest updated_at), null field handling, history lookup

### Integration Test (three modes)
- **dry_run** (default): full pipeline via FastAPI TestClient, no network. Validates preview, run creation, error handling.
- **mock**: validates Slack HTTP delivery against provided mock server. Verifies correct channels, replay idempotency, unknown region exclusion.
- **live**: validates against real Slack (manual run for demo).

## Future Expansion

### Additional Integration Tests
- Error recovery: mock Slack returns 500 for specific channels, partial failure recorded
- Config changes between runs: different ARR threshold, different alert counts
- Multiple months: run Jan then Feb, separate idempotency tracking

### End-to-End
- Docker Compose stack: `docker compose up` + automated test suite
- GCS integration: read from real gs:// URI (requires credentials)

### Performance / Scale
- Property-based testing (Hypothesis) for duration edge cases
- Large Parquet file (100K+ rows) to validate memory behavior
- Concurrent run requests to verify SQLite locking behavior

### What Would Change for Production
- Move to Postgres for concurrent writers
- Add OpenTelemetry tracing for distributed debugging
- Contract tests for Slack webhook payload format
