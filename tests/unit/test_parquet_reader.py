"""Unit tests for Parquet reader: filtering, dedup, null handling."""

from datetime import date

from app.data.parquet_reader import read_parquet_data

FIXTURE = "tests/fixtures/test_accounts.parquet"


class TestReadTargetMonth:
    def test_reads_at_risk_accounts(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        account_ids = {a["account_id"] for a in result.at_risk_accounts}
        # Should include at-risk accounts in Jan 2026
        assert "test_001" in account_ids
        assert "test_002" in account_ids
        assert "test_003" in account_ids
        # Should NOT include healthy accounts
        assert "test_008" not in account_ids

    def test_rows_scanned_positive(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        assert result.rows_scanned > 0


class TestDedup:
    def test_dedup_keeps_latest(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        # test_009 has two rows — should appear once
        matches = [a for a in result.at_risk_accounts if a["account_id"] == "test_009"]
        assert len(matches) == 1

    def test_dedup_count_accurate(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        # test_009 has 1 duplicate in target month
        assert result.duplicates_found >= 1


class TestNullHandling:
    def test_null_region_is_none(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        acct = next(a for a in result.at_risk_accounts if a["account_id"] == "test_004")
        assert acct["account_region"] is None

    def test_null_renewal_date_is_none(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        acct = next(a for a in result.at_risk_accounts if a["account_id"] == "test_010")
        assert acct["renewal_date"] is None

    def test_null_owner_is_none(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        acct = next(a for a in result.at_risk_accounts if a["account_id"] == "test_010")
        assert acct["account_owner"] is None


class TestHistory:
    def test_history_populated(self):
        result = read_parquet_data(FIXTURE, date(2026, 1, 1))
        # test_001 has history in Oct, Nov, Dec
        assert ("test_001", date(2025, 10, 1)) in result.history
        assert result.history[("test_001", date(2025, 10, 1))] == "At Risk"
