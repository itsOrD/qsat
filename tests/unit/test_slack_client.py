"""Unit tests for Slack client: URL construction, retry logic."""

from unittest.mock import MagicMock, patch

import requests

from app.integrations.slack_client import send_slack_message


class TestUrlConstruction:
    @patch("app.integrations.slack_client.requests.post")
    def test_base_url_mode(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        send_slack_message(
            {"text": "hi"}, "amer-risk-alerts",
            base_url="http://localhost:9000/slack/webhook",
        )
        mock_post.assert_called_once()
        url = mock_post.call_args[0][0]
        assert url == "http://localhost:9000/slack/webhook/amer-risk-alerts"

    @patch("app.integrations.slack_client.requests.post")
    def test_webhook_mode(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        send_slack_message(
            {"text": "hi"}, "ignored",
            webhook_url="https://hooks.slack.com/services/T/B/x",
        )
        url = mock_post.call_args[0][0]
        assert url == "https://hooks.slack.com/services/T/B/x"

    @patch("app.integrations.slack_client.requests.post")
    def test_base_url_takes_precedence(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        send_slack_message(
            {"text": "hi"}, "ch",
            base_url="http://base/webhook",
            webhook_url="https://single",
        )
        url = mock_post.call_args[0][0]
        assert url == "http://base/webhook/ch"

    def test_neither_configured(self):
        success, error = send_slack_message({"text": "hi"}, "ch")
        assert success is False
        assert error == "no_slack_configured"


class TestRetryLogic:
    @patch("app.integrations.slack_client.time.sleep")
    @patch("app.integrations.slack_client.requests.post")
    def test_429_triggers_retry(self, mock_post, mock_sleep):
        resp_429 = MagicMock(status_code=429, text="rate limited", headers={"Retry-After": "1"})
        resp_200 = MagicMock(status_code=200)
        mock_post.side_effect = [resp_429, resp_200]

        success, error = send_slack_message(
            {"text": "hi"}, "ch", base_url="http://base"
        )
        assert success is True
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(1.0)

    @patch("app.integrations.slack_client.time.sleep")
    @patch("app.integrations.slack_client.requests.post")
    def test_500_triggers_retry(self, mock_post, mock_sleep):
        resp_500 = MagicMock(status_code=500, text="error", headers={})
        resp_200 = MagicMock(status_code=200)
        mock_post.side_effect = [resp_500, resp_200]

        success, error = send_slack_message(
            {"text": "hi"}, "ch", base_url="http://base"
        )
        assert success is True
        assert mock_post.call_count == 2

    @patch("app.integrations.slack_client.time.sleep")
    @patch("app.integrations.slack_client.requests.post")
    def test_400_no_retry(self, mock_post, mock_sleep):
        resp_400 = MagicMock(status_code=400, text="bad request", headers={})
        mock_post.return_value = resp_400

        success, error = send_slack_message(
            {"text": "hi"}, "ch", base_url="http://base"
        )
        assert success is False
        assert "400" in error
        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()

    @patch("app.integrations.slack_client.time.sleep")
    @patch("app.integrations.slack_client.requests.post")
    def test_retries_exhaust(self, mock_post, mock_sleep):
        resp_500 = MagicMock(status_code=500, text="error", headers={})
        mock_post.return_value = resp_500

        success, error = send_slack_message(
            {"text": "hi"}, "ch", base_url="http://base"
        )
        assert success is False
        assert mock_post.call_count == 4  # 1 initial + 3 retries

    @patch("app.integrations.slack_client.time.sleep")
    @patch("app.integrations.slack_client.requests.post")
    def test_connection_error_retries(self, mock_post, mock_sleep):
        mock_post.side_effect = [
            requests.ConnectionError("refused"),
            MagicMock(status_code=200),
        ]

        success, error = send_slack_message(
            {"text": "hi"}, "ch", base_url="http://base"
        )
        assert success is True
        assert mock_post.call_count == 2
