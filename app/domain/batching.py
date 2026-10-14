"""Batch partitioning. A SKU appears in exactly one batch per run (reading I1)."""
from __future__ import annotations

from typing import Iterable, Iterator, Sequence, TypeVar

T = TypeVar("T")

WMS_MAX_BATCH = 100


def chunked(items: Sequence[T], size: int) -> Iterator[list[T]]:
    if size < 1:
        raise ValueError("size must be >= 1")
    for start in range(0, len(items), size):
        yield list(items[start:start + size])


def partition_unique(products: Iterable[dict], size: int = WMS_MAX_BATCH, key: str = "sku") -> list[list[dict]]:
    """Deduplicate on ``key`` (first occurrence wins) and split into batches of ``size``."""
    if size > WMS_MAX_BATCH:
        raise ValueError(f"WMS hard limit is {WMS_MAX_BATCH} products per batch")
    seen: set[str] = set()
    unique = []
    for product in products:
        if product[key] in seen:
            continue
        seen.add(product[key])
        unique.append(product)
    return list(chunked(unique, size))
