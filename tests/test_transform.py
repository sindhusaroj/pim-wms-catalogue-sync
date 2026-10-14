from decimal import Decimal

import pytest

from app.domain.mapping import InvalidRecord, from_message, to_message, to_wire, transform, try_transform

ROW = {"id": "P0000001", "name": "Test Product", "category": "SPICES", "price": "249.50", "currency": "INR",
       "updated_at": "2026-09-25T08:30:00Z"}


def test_transform():
    result = transform(ROW)
    assert result["sku"] == "P0000001"
    assert result["selling_price"] == 249.50
    assert result["category_code"] == "SPICES"


def test_transform_maps_every_field_from_configuration():
    assert transform(ROW) == {
        "sku": "P0000001", "description": "Test Product", "selling_price": Decimal("249.50"), "currency": "INR",
        "category_code": "SPICES", "source_updated_at": "2026-09-25T08:30:00Z",
    }


def test_price_is_fixed_precision_decimal_not_float():
    result = transform({**ROW, "price": 0.1 + 0.2})  # 0.30000000000000004 as a float
    assert isinstance(result["selling_price"], Decimal)
    assert result["selling_price"] == Decimal("0.30")


@pytest.mark.parametrize("field,value", [
    ("id", ""), ("name", None), ("price", "not-a-number"), ("price", "-1"), ("currency", "rupees"),
    ("updated_at", "yesterday"),
])
def test_invalid_records_raise_invalid_record_instead_of_crashing(field, value):
    with pytest.raises(InvalidRecord) as exc:
        transform({**ROW, field: value})
    assert field in exc.value.reason


def test_missing_field_is_quarantinable():
    row = dict(ROW)
    del row["price"]
    result = try_transform(row)
    assert result.product is None and "price" in result.error.reason
    assert result.error.source_id == "P0000001"


def test_message_round_trip_is_lossless_and_wire_is_json_number():
    product = transform(ROW)
    assert from_message(to_message(product)) == product
    assert to_wire(product)["selling_price"] == 249.5
