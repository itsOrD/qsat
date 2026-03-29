"""Slack webhook client with retry and exponential backoff.

Supports two modes:
  - Base URL mode: POST to {SLACK_WEBHOOK_BASE_URL}/{channel}
  - Single webhook mode: POST to SLACK_WEBHOOK_URL (channel ignored)

Base URL mode takes precedence when both are configured.
"""

from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)

INITIAL_BACKOFF = 1.0  # seconds
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10  # seconds


def send_slack_message(
    payload: dict,
    channel: str,
    base_url: str | None = None,
    webhook_url: str | None = None,
) -> tuple[bool, str | None]:
    """Post a message to Slack via webhook.

    Args:
        payload: Slack Block Kit message dict.
        channel: Target channel name (used in base URL mode).
        base_url: Base URL mode — POST to {base_url}/{channel}.
        webhook_url: Single webhook mode — POST to this URL.

    Returns:
        (success, error_message_or_none) tuple.
    """
    url = _resolve_url(channel, base_url, webhook_url)
    if url is None:
        return False, "no_slack_configured"

    last_error: str | None = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                return True, None

            if resp.status_code == 429 or resp.status_code >= 500:
                wait = _backoff_delay(attempt, resp)
                last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                if attempt < MAX_RETRIES:
                    log.warning(
                        "Slack %s (attempt %d/%d), retrying in %.1fs: %s",
                        resp.status_code,
                        attempt + 1,
                        MAX_RETRIES + 1,
                        wait,
                        channel,
                    )
                    time.sleep(wait)
                    continue

            # 4xx (non-429) — don't retry
            if 400 <= resp.status_code < 500:
                return False, f"HTTP {resp.status_code}: {resp.text[:200]}"

        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt < MAX_RETRIES:
                wait = INITIAL_BACKOFF * (2**attempt)
                log.warning(
                    "Slack connection error (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    MAX_RETRIES + 1,
                    wait,
                    exc,
                )
                time.sleep(wait)
                continue

    log.error("Slack send failed after %d attempts: %s", MAX_RETRIES + 1, last_error)
    return False, last_error


def _resolve_url(
    channel: str,
    base_url: str | None,
    webhook_url: str | None,
) -> str | None:
    """Determine the target URL based on configured mode."""
    if base_url:
        return f"{base_url.rstrip('/')}/{channel}"
    if webhook_url:
        return webhook_url
    log.warning("No Slack webhook configured — messages will not be delivered")
    return None


def _backoff_delay(attempt: int, resp: requests.Response) -> float:
    """Calculate retry delay, honoring Retry-After header if present."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return INITIAL_BACKOFF * (2**attempt)
