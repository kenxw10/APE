from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Any

KALSHI_TAKER_FEE_SOURCE = "https://kalshi.com/docs/kalshi-fee-schedule.pdf"
KALSHI_TAKER_FEE_SCHEDULE_VERSION = "2026-07-07"
KALSHI_TAKER_FEE_RATE = Decimal("0.07")
KALSHI_FEE_PARAMETER_SNAPSHOT = {
    "default_maker_multiplier": "0",
    "default_taker_multiplier": "1",
    "document_title": "Fee Schedule for July 2026 - 7.7.26 Update",
    "effective_date": "2026-07-07",
    "kxbtc15m_listed_as_non_standard": False,
    "kxbtc15m_maker_multiplier": "0",
    "kxbtc15m_taker_multiplier": "1",
    "maker_formula": "round_up(M * 0.0175 * C * P * (1-P))",
    "rounding_rule_source_text": (
        "rounds up such that the fee + positionCost is rounded to a centicent"
    ),
    "settlement_fee": "0",
    "source_url": KALSHI_TAKER_FEE_SOURCE,
    "taker_formula": "round_up(M * 0.07 * C * P * (1-P))",
}
KALSHI_FEE_PARAMETER_SNAPSHOT_SHA256 = (
    "6d625f01b407d66a8f42c3df193ed750054df489bb075de63fc98608cfe1b823"
)


@dataclass(frozen=True)
class FeeModel:
    name: str
    schedule_version: str
    rate: Decimal
    source: str

    def fee_cents(self, *, price: Decimal, contracts: Decimal = Decimal("1")) -> Decimal:
        """Kalshi general taker fee, rounded up to the next whole cent."""
        dollars = (self.rate * contracts * price * (Decimal("1") - price)).quantize(
            Decimal("0.01"), rounding=ROUND_CEILING
        )
        return dollars * Decimal("100")

    def metadata(self) -> dict[str, Any]:
        source_identifier = hashlib.sha256(
            "|".join(
                (
                    KALSHI_FEE_PARAMETER_SNAPSHOT["source_url"],
                    KALSHI_FEE_PARAMETER_SNAPSHOT["document_title"],
                    KALSHI_FEE_PARAMETER_SNAPSHOT["effective_date"],
                )
            ).encode()
        ).hexdigest()
        return {
            "name": self.name,
            "schedule_version": self.schedule_version,
            **KALSHI_FEE_PARAMETER_SNAPSHOT,
            "parameter_snapshot_sha256": KALSHI_FEE_PARAMETER_SNAPSHOT_SHA256,
            "source_identifier": source_identifier,
        }


def verified_kalshi_taker_fee_model() -> FeeModel:
    """Return the authoritative general fee schedule verified for PR 11."""
    return FeeModel(
        name="kalshi_general_taker_fee",
        schedule_version=KALSHI_TAKER_FEE_SCHEDULE_VERSION,
        rate=KALSHI_TAKER_FEE_RATE,
        source=KALSHI_TAKER_FEE_SOURCE,
    )
