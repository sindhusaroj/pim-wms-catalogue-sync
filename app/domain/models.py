"""Data contract validated at the boundary (section 7.4, finding F9)."""
from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator

MONEY_QUANTUM = Decimal("0.01")


def to_money(value) -> Decimal:
    """Fixed-precision money. Floats go through str() so 249.5 stays 249.50, not 249.4999..."""
    if isinstance(value, float):
        value = repr(value)
    try:
        amount = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"price is not a decimal: {value!r}") from exc
    if not amount.is_finite():
        raise ValueError(f"price is not finite: {value!r}")
    return amount.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_EVEN)


class PimProduct(BaseModel):
    """A product as the PIM publishes it."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True, frozen=True)

    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    category: str = Field(min_length=1)
    price: Decimal
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    updated_at: str

    @field_validator("price", mode="before")
    @classmethod
    def _price(cls, value):
        if value is None or value == "":
            raise ValueError("price is required")
        amount = to_money(value)
        if amount < 0:
            raise ValueError("price must not be negative")
        return amount

    @field_validator("updated_at")
    @classmethod
    def _iso8601(cls, value: str) -> str:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value
