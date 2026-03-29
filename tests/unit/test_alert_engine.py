"""Unit tests for alert engine: duration, threshold, routing, formatting."""

from datetime import date

from app.core.alert_engine import (
    AlertRecord,
    build_alert_records,
    compute_duration,
    filter_by_threshold,
    format_slack_message,
    route_to_channel,
)

REGION_CHANNELS = {
    "AMER": "amer-risk-alerts",
    "EMEA": "emea-risk-alerts",
    "APAC": "apac-risk-alerts",
}


# ---- Duration calculation ----


class TestComputeDuration:
    def test_previous_month_healthy(self):
        history = {("a1", date(2025, 12, 1)): "Healthy"}
        dur, start = compute_duration("a1", date(2026, 1, 1), history)
        assert dur == 1
        assert start == date(2026, 1, 1)

    def test_previous_month_churned(self):
        history = {("a1", date(2025, 12, 1)): "Churned"}
        dur, start = compute_duration("a1", date(2026, 1, 1), history)
        assert dur == 1
        assert start == date(2026, 1, 1)

    def test_previous_month_missing(self):
        dur, start = compute_duration("a1", date(2026, 1, 1), {})
        assert dur == 1
        assert start == date(2026, 1, 1)

    def test_four_consecutive_at_risk(self):
        history = {
            ("a1", date(2025, 10, 1)): "At Risk",
            ("a1", date(2025, 11, 1)): "At Risk",
            ("a1", date(2025, 12, 1)): "At Risk",
        }
        dur, start = compute_duration("a1", date(2026, 1, 1), history)
        assert dur == 4
        assert start == date(2025, 10, 1)

    def test_healthy_gap_resets_streak(self):
        """The spec example: Oct-Nov At Risk, Dec Healthy, Jan At Risk -> 1."""
        history = {
            ("a1", date(2025, 10, 1)): "At Risk",
            ("a1", date(2025, 11, 1)): "At Risk",
            ("a1", date(2025, 12, 1)): "Healthy",
        }
        dur, start = compute_duration("a1", date(2026, 1, 1), history)
        assert dur == 1
        assert start == date(2026, 1, 1)

    def test_churned_gap_resets_streak(self):
        history = {
            ("a1", date(2025, 10, 1)): "At Risk",
            ("a1", date(2025, 11, 1)): "At Risk",
            ("a1", date(2025, 12, 1)): "Churned",
        }
        dur, start = compute_duration("a1", date(2026, 1, 1), history)
        assert dur == 1
        assert start == date(2026, 1, 1)

    def test_year_boundary(self):
        """Jan target, Dec At Risk -> duration = 2."""
        history = {("a1", date(2025, 12, 1)): "At Risk"}
        dur, start = compute_duration("a1", date(2026, 1, 1), history)
        assert dur == 2
        assert start == date(2025, 12, 1)


# ---- ARR threshold ----


class TestFilterByThreshold:
    def test_below_threshold_excluded(self):
        accounts = [{"account_id": "a1", "arr": 5000}]
        above, below = filter_by_threshold(accounts, 10_000)
        assert len(above) == 0
        assert len(below) == 1

    def test_at_threshold_included(self):
        accounts = [{"account_id": "a1", "arr": 10_000}]
        above, below = filter_by_threshold(accounts, 10_000)
        assert len(above) == 1
        assert len(below) == 0

    def test_above_threshold_included(self):
        accounts = [{"account_id": "a1", "arr": 50_000}]
        above, below = filter_by_threshold(accounts, 10_000)
        assert len(above) == 1

    def test_zero_arr_excluded(self):
        accounts = [{"account_id": "a1", "arr": 0}]
        above, below = filter_by_threshold(accounts, 10_000)
        assert len(above) == 0
        assert len(below) == 1

    def test_none_arr_treated_as_zero(self):
        accounts = [{"account_id": "a1", "arr": None}]
        above, below = filter_by_threshold(accounts, 10_000)
        assert len(above) == 0


# ---- Channel routing ----


class TestRouteToChannel:
    def test_known_region(self):
        channel, routable, reason = route_to_channel("AMER", REGION_CHANNELS)
        assert channel == "amer-risk-alerts"
        assert routable is True
        assert reason is None

    def test_null_region(self):
        channel, routable, reason = route_to_channel(None, REGION_CHANNELS)
        assert channel is None
        assert routable is False
        assert reason == "unknown_region"

    def test_unmapped_region(self):
        channel, routable, reason = route_to_channel("LATAM", REGION_CHANNELS)
        assert channel is None
        assert routable is False
        assert reason == "unknown_region"


# ---- Message formatting ----


class TestFormatSlackMessage:
    def _make_alert(self, **overrides):
        defaults = dict(
            account_id="a1",
            account_name="Acme Corp",
            account_region="AMER",
            month=date(2026, 1, 1),
            arr=50_000,
            renewal_date=date(2026, 6, 1),
            account_owner="alice@example.com",
            duration_months=3,
            risk_start_month=date(2025, 11, 1),
            channel="amer-risk-alerts",
            routable=True,
        )
        defaults.update(overrides)
        return AlertRecord(**defaults)

    def test_has_text_and_blocks(self):
        msg = format_slack_message(self._make_alert(), "https://app.co")
        assert "text" in msg
        assert "blocks" in msg
        assert len(msg["blocks"]) > 0

    def test_null_renewal_date_shows_unknown(self):
        msg = format_slack_message(
            self._make_alert(renewal_date=None), "https://app.co"
        )
        block_text = msg["blocks"][0]["text"]["text"]
        assert "Unknown" in block_text

    def test_null_owner_omits_line(self):
        msg = format_slack_message(
            self._make_alert(account_owner=None), "https://app.co"
        )
        block_text = msg["blocks"][0]["text"]["text"]
        assert "Owner" not in block_text

    def test_owner_included_when_present(self):
        msg = format_slack_message(self._make_alert(), "https://app.co")
        block_text = msg["blocks"][0]["text"]["text"]
        assert "alice@example.com" in block_text

    def test_arr_formatted_with_commas(self):
        msg = format_slack_message(self._make_alert(), "https://app.co")
        block_text = msg["blocks"][0]["text"]["text"]
        assert "$50,000" in block_text

    def test_details_url(self):
        msg = format_slack_message(self._make_alert(), "https://app.co")
        block_text = msg["blocks"][0]["text"]["text"]
        assert "https://app.co/accounts/a1" in block_text
