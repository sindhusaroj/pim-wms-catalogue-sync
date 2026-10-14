import pytest

from app.domain.batching import partition_unique
from app.domain.hashing import content_hash, idempotency_key
from app.domain.mapping import transform
from app.domain.rejections import classify

ROW = {"id": "P0000001", "name": "A", "category": "HOME", "price": "10.00", "currency": "INR",
       "updated_at": "2026-09-25T08:30:00Z"}


def test_content_hash_is_stable_and_sensitive_to_every_field():
    product = transform(ROW)
    assert content_hash(product) == content_hash(transform(dict(ROW)))
    assert content_hash(transform({**ROW, "price": "10"})) == content_hash(product)  # 10 == 10.00
    for field, value in [("name", "B"), ("price", "10.01"), ("category", "SPORTS"), ("updated_at", "2026-09-26T00:00:00Z")]:
        assert content_hash(transform({**ROW, field: value})) != content_hash(product)


def test_idempotency_key_is_derived_from_run_skus_and_hashes():
    items = [("P1", "h1"), ("P2", "h2")]
    assert idempotency_key("run-1", items) == idempotency_key("run-1", list(items))
    assert idempotency_key("run-1", items) != idempotency_key("run-2", items)
    assert idempotency_key("run-1", items) != idempotency_key("run-1", [("P1", "h1"), ("P2", "h3")])
    assert len(idempotency_key("run-1", items)) == 64


def test_partition_never_exceeds_100_and_each_sku_appears_once():
    products = [{"sku": f"P{i % 750}"} for i in range(1000)]  # 250 duplicates
    batches = partition_unique(products)
    assert all(len(b) <= 100 for b in batches)
    skus = [p["sku"] for b in batches for p in b]
    assert len(skus) == len(set(skus)) == 750


def test_partition_rejects_batch_size_above_wms_limit():
    with pytest.raises(ValueError):
        partition_unique([], size=101)


@pytest.mark.parametrize("sku,reason,code,expected", [
    ("P0000999", "Invalid warehouse product", "WMS_RULE_SKU_999", True),
    (None, "sku is required", "MISSING_SKU", False),
    ("P0000123", "Invalid warehouse product", "UNCLASSIFIED", False),
    ("P0000999", "Warehouse closed", "UNCLASSIFIED", False),
])
def test_rejection_classifier(sku, reason, code, expected):
    result = classify(sku, reason)
    assert (result.code, result.expected) == (code, expected)
