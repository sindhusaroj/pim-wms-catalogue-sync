"""Content hashes and idempotency keys (section 5.4)."""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Iterable


def _default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"not serialisable: {type(value).__name__}")


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_default, ensure_ascii=False)


def content_hash(product: dict[str, Any]) -> str:
    """sha256 of the canonical WMS payload; drives change detection between runs."""
    return hashlib.sha256(canonical_json(product).encode("utf-8")).hexdigest()


def idempotency_key(run_id: str, items: Iterable[tuple[str, str]]) -> str:
    """Derived, not generated: sha256(run_id + SKU list + content hashes).

    A replay of the same logical work recomputes the same key; genuinely new
    work cannot collide with it.
    """
    digest = hashlib.sha256(run_id.encode("utf-8"))
    for sku, chash in items:
        digest.update(b"\n")
        digest.update(f"{sku}:{chash}".encode("utf-8"))
    return digest.hexdigest()
