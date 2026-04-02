"""Application configuration via Pydantic BaseSettings.

Every field is overridable by an environment variable (uppercased name).
"""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Central configuration for the Risk Alert Service."""

    # --- Slack ---
    slack_webhook_base_url: str | None = None
    slack_webhook_url: str | None = None

    # --- Storage ---
    database_path: str = "./data/alerts.db"


    # --- API Surface Hardening ---
    app_mode: Literal["demo", "secure"] = "demo"
    rbac_enabled: bool | None = None
    rbac_runner_tokens: str | None = None
    rbac_viewer_tokens: str | None = None

    # --- Business Logic ---
    arr_threshold: int = Field(
        default=10_000,
        description=(
            "Minimum ARR to trigger alerts. "
            "Default $10,000 filters only zero-ARR accounts (test/free-tier)."
        ),
    )
    app_base_url: str = "https://app.yourcompany.com"
    region_channels: dict[str, str] = {
        "AMER": "amer-risk-alerts",
        "EMEA": "emea-risk-alerts",
        "APAC": "apac-risk-alerts",
    }

    # --- Notifications ---
    support_email: str = "support@quadsci.ai"

    # --- Email (SMTP) ---
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_from: str | None = None

    model_config = {"env_file": ".env", "extra": "ignore"}

    @staticmethod
    def _split_tokens(raw_tokens: str | None) -> set[str]:
        if not raw_tokens:
            return set()
        return {t.strip() for t in raw_tokens.split(",") if t.strip()}

    def runner_tokens(self) -> set[str]:
        return self._split_tokens(self.rbac_runner_tokens)

    def viewer_tokens(self) -> set[str]:
        return self._split_tokens(self.rbac_viewer_tokens)

    def auth_required(self) -> bool:
        """Resolve whether RBAC should be enforced."""
        if self.rbac_enabled is not None:
            return self.rbac_enabled
        return self.app_mode == "secure"

    def snapshot(self) -> dict:
        """Return an allowlist of config safe to persist. Never includes secrets."""
        return {
            "arr_threshold": self.arr_threshold,
            "app_base_url": self.app_base_url,
            "region_channels": self.region_channels,
            "support_email": self.support_email,
        }
