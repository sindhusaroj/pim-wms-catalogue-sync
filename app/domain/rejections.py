"""Rule-based classification of WMS rejections (section 7.2, assumption A7).

Rejections are business outcomes, not transport failures, and are never blindly
retried. Known rules are recorded and suppressed; anything else is unexpected
and raises the "unexpected rejection mix" alarm.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Classification:
    code: str
    expected: bool


WMS_RULE_SKU_999 = Classification("WMS_RULE_SKU_999", expected=True)
MISSING_SKU = Classification("MISSING_SKU", expected=False)
UNACCOUNTED = Classification("UNACCOUNTED", expected=False)
UNCLASSIFIED = Classification("UNCLASSIFIED", expected=False)


def classify(sku: str | None, reason: str | None) -> Classification:
    if not sku:
        return MISSING_SKU
    if str(sku).endswith("999") and (reason or "").strip().lower() == "invalid warehouse product":
        return WMS_RULE_SKU_999
    return UNCLASSIFIED
