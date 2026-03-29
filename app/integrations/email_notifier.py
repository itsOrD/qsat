"""Email notification for accounts with unknown regions.

Two backends:
  - LoggingNotifier: logs the full email content (default for local dev).
  - SMTPNotifier: sends via Python stdlib smtplib (for production).

Backend selection: SMTP_HOST configured -> SMTPNotifier, otherwise LoggingNotifier.
"""

from __future__ import annotations

import logging
import smtplib
from abc import ABC, abstractmethod
from email.message import EmailMessage

log = logging.getLogger(__name__)


class NotifierBackend(ABC):
    """Base class for email notification backends."""

    @abstractmethod
    def send(self, to: str, subject: str, body: str) -> bool:
        """Send an email notification. Returns True on success."""
        ...


class LoggingNotifier(NotifierBackend):
    """Logs the full email content. Default for local dev."""

    def send(self, to: str, subject: str, body: str) -> bool:
        log.info(
            "Email notification (logged, not sent):\n"
            "  To: %s\n  Subject: %s\n\n%s",
            to,
            subject,
            body,
        )
        return True


class SMTPNotifier(NotifierBackend):
    """Sends via SMTP using Python's built-in smtplib."""

    def __init__(self, host: str, port: int, from_addr: str) -> None:
        self._host = host
        self._port = port
        self._from = from_addr

    def send(self, to: str, subject: str, body: str) -> bool:
        msg = EmailMessage()
        msg["From"] = self._from
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)

        try:
            with smtplib.SMTP(self._host, self._port) as server:
                server.starttls()
                server.send_message(msg)
            log.info("Email sent to %s: %s", to, subject)
            return True
        except (smtplib.SMTPException, OSError):
            log.exception("Failed to send email to %s", to)
            return False


def get_notifier(
    smtp_host: str | None = None,
    smtp_port: int = 587,
    smtp_from: str | None = None,
) -> NotifierBackend:
    """Return the appropriate notifier backend based on config."""
    if smtp_host and smtp_from:
        log.info("Using SMTP notifier: %s:%d", smtp_host, smtp_port)
        return SMTPNotifier(smtp_host, smtp_port, smtp_from)
    log.info("Using logging notifier (no SMTP configured)")
    return LoggingNotifier()


def build_unknown_region_email(
    run_id: str,
    month: str,
    unroutable_accounts: list[dict],
) -> tuple[str, str]:
    """Build the subject and body for an unknown-region notification.

    Args:
        run_id: The current run ID.
        month: Target month string.
        unroutable_accounts: List of account dicts with unknown/missing region.

    Returns:
        (subject, body) tuple.
    """
    n = len(unroutable_accounts)
    subject = f"[QuadSci Risk Alerts] {n} accounts with unknown region \u2014 {month}"

    lines = [
        f"Run ID: {run_id}",
        f"Month: {month}",
        "",
        "Accounts with unknown/missing region:",
        "",
    ]

    for acct in unroutable_accounts:
        region = acct.get("account_region") or "null"
        arr = acct.get("arr", 0)
        lines.append(
            f"  - {acct['account_name']} ({acct['account_id']}), "
            f"region={region}, ARR=${arr:,}"
        )

    lines.extend([
        "",
        "These accounts were not alerted because their region could not be",
        "mapped to a Slack channel. Please update account region data or",
        "add the region to the channel routing configuration.",
    ])

    return subject, "\n".join(lines)
