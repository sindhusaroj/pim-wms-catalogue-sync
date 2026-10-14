"""Backoff with full jitter, and Retry-After parsing (section 7.1)."""
from __future__ import annotations

import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


def full_jitter_delay(attempt: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """sleep = random(0, min(cap, base * 2**attempt)); attempt is zero-based."""
    ceiling = min(cap, base * (2 ** attempt))
    return (rng or random).uniform(0, ceiling)


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Return the delay in seconds from a Retry-After header (delta-seconds or HTTP-date)."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())
