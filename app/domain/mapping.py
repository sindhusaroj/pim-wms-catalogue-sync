"""Declarative PIM -> WMS mapping (Appendix A: transform kept as configuration)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from pydantic import ValidationError

from app.domain.models import PimProduct

MAPPING_PATH = Path(__file__).with_name("mapping.json")


@lru_cache(maxsize=1)
def load_mapping() -> dict:
    return json.loads(MAPPING_PATH.read_text(encoding="utf-8"))


class InvalidRecord(ValueError):
    def __init__(self, source_id: str | None, reason: str) -> None:
        super().__init__(reason)
        self.source_id = source_id
        self.reason = reason


def transform(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a PIM record and map it to the WMS shape. Raises InvalidRecord."""
    try:
        product = PimProduct.model_validate(dict(row))
    except ValidationError as exc:
        reason = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or 'record'}: {err['msg']}" for err in exc.errors()
        )
        source_id = row.get("id") if isinstance(row, Mapping) else None
        raise InvalidRecord(source_id or None, reason) from None
    source = product.model_dump()
    return {field["target"]: source[field["source"]] for field in load_mapping()["fields"]}


@dataclass(frozen=True)
class TransformResult:
    product: dict[str, Any] | None
    error: InvalidRecord | None


def try_transform(row: Mapping[str, Any]) -> TransformResult:
    try:
        return TransformResult(transform(row), None)
    except InvalidRecord as exc:
        return TransformResult(None, exc)


def to_wire(product: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe WMS payload. The WMS contract takes a JSON number, so the Decimal is
    converted only here, at the wire. Values are quantised to 2 dp beforehand and
    float's shortest repr round-trips them exactly (e.g. 249.50 -> 249.5)."""
    return {k: (float(v) if isinstance(v, Decimal) else v) for k, v in product.items()}


def to_message(product: dict[str, Any]) -> dict[str, Any]:
    """Lossless form for queue messages and state: Decimals as strings."""
    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in product.items()}


def from_message(product: dict[str, Any]) -> dict[str, Any]:
    out = dict(product)
    if out.get("selling_price") is not None:
        out["selling_price"] = Decimal(out["selling_price"])
    return out
