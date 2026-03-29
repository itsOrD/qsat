"""Formatted terminal output for run progress and summaries."""

from __future__ import annotations

import sys
from collections import Counter

# ANSI color codes
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _color_for_progress(current: int, total: int) -> str:
    """Red → Yellow → Green as progress approaches 100%."""
    if total == 0:
        return GREEN
    pct = current / total
    if pct < 0.33:
        return RED
    if pct < 0.66:
        return YELLOW
    return GREEN


def log_run_start(run_id: str, month: str, source_uri: str, dry_run: bool) -> None:
    """Print a clear header when a run begins."""
    mode = f"{YELLOW}DRY RUN{RESET}" if dry_run else f"{GREEN}LIVE{RESET}"
    print(flush=True)
    print(f"  {BOLD}{'─' * 60}{RESET}", flush=True)
    print(f"  {BOLD}Risk Alert Run{RESET}  {DIM}│{RESET}  {mode}", flush=True)
    print(f"  {BOLD}{'─' * 60}{RESET}", flush=True)
    print(f"  Run ID    {DIM}│{RESET}  {DIM}{run_id}{RESET}", flush=True)
    print(f"  Month     {DIM}│{RESET}  {month}", flush=True)
    print(f"  Source    {DIM}│{RESET}  {source_uri}", flush=True)
    print(f"  {DIM}{'─' * 60}{RESET}", flush=True)
    print(flush=True)


def log_data_loaded(
    rows_scanned: int,
    duplicates: int,
    at_risk: int,
    above_threshold: int,
    below_threshold: int,
) -> None:
    """Print data loading summary."""
    print(f"  {CYAN}Data{RESET}", flush=True)
    print(f"  Rows scanned      {BOLD}{rows_scanned:,}{RESET}", flush=True)
    print(f"  Duplicates removed {BOLD}{duplicates:,}{RESET}", flush=True)
    print(f"  At-risk accounts   {BOLD}{at_risk:,}{RESET}", flush=True)
    print(
        f"  Above ARR threshold{BOLD} {above_threshold:,}{RESET}"
        f"  {DIM}({below_threshold:,} filtered){RESET}",
        flush=True,
    )
    print(flush=True)


def log_alert_progress(current: int, total: int, account_id: str, outcome: str) -> None:
    """Print a single-line progress update."""
    color = _color_for_progress(current, total)
    pad = len(str(total))

    outcome_display = {
        "sent": f"{GREEN}sent{RESET}",
        "preview": f"{DIM}preview{RESET}",
        "skipped_replay": f"{CYAN}skipped{RESET}",
        "failed": f"{RED}failed{RESET}",
    }.get(outcome, outcome)

    print(
        f"  {color}{current:>{pad}}{RESET}"
        f" {DIM}of{RESET} {total}"
        f"  {account_id}"
        f"  {outcome_display}",
        flush=True,
    )


def log_run_summary(
    run_id: str,
    status: str,
    counters: dict[str, int],
    channel_counts: Counter,
    unroutable_count: int,
    dry_run: bool,
    elapsed_ms: int,
) -> None:
    """Print the final run summary box."""
    total = counters["sent"] + counters["skipped_replay"] + counters["failed"]
    status_color = GREEN if status == "succeeded" else RED
    elapsed_str = f"{elapsed_ms / 1000:.1f}s" if elapsed_ms >= 1000 else f"{elapsed_ms}ms"

    print(flush=True)
    print(f"  {BOLD}{'─' * 60}{RESET}", flush=True)
    print(f"  {BOLD}Run Complete{RESET}  {DIM}│{RESET}  {status_color}{status.upper()}{RESET}  {DIM}in {elapsed_str}{RESET}", flush=True)
    print(f"  {BOLD}{'─' * 60}{RESET}", flush=True)

    if dry_run:
        print(f"  Alerts previewed   {BOLD}{total:,}{RESET}  {DIM}(no Slack messages sent){RESET}", flush=True)
    else:
        print(f"  Alerts sent        {GREEN}{BOLD}{counters['sent']:,}{RESET}", flush=True)
        print(f"  Skipped (replay)   {CYAN}{counters['skipped_replay']:,}{RESET}", flush=True)
        print(f"  Failed             {RED}{counters['failed']:,}{RESET}", flush=True)

    if channel_counts and not dry_run:
        print(flush=True)
        print(f"  {CYAN}Slack Channels{RESET}", flush=True)
        for channel, count in sorted(channel_counts.items()):
            print(f"    {channel:<30} {BOLD}{count:,}{RESET}", flush=True)

    if unroutable_count > 0:
        print(flush=True)
        label = "would fail routing" if dry_run else "failed routing"
        print(f"  {YELLOW}Unroutable{RESET}  {unroutable_count:,} accounts {label} {DIM}(unknown region){RESET}", flush=True)

    print(f"  {DIM}{'─' * 60}{RESET}", flush=True)
    print(flush=True)
