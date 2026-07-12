from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal

KALSHI_TAKER_FEE_SOURCE = "https://kalshi.com/docs/kalshi-fee-schedule.pdf"
KALSHI_TAKER_FEE_SCHEDULE_VERSION = "2026-02-05"
KALSHI_TAKER_FEE_RATE = Decimal("0.07")


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

    def metadata(self) -> dict[str, str]:
        payload = {
            "name": self.name,
            "schedule_version": self.schedule_version,
            "formula": "ceil(0.07 * contracts * price * (1 - price), $0.01)",
            "source": self.source,
        }
        payload["source_checksum"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return payload


def verified_kalshi_taker_fee_model() -> FeeModel:
    """Return the authoritative general fee schedule verified for PR 11."""
    return FeeModel(
        name="kalshi_general_taker_fee",
        schedule_version=KALSHI_TAKER_FEE_SCHEDULE_VERSION,
        rate=KALSHI_TAKER_FEE_RATE,
        source=KALSHI_TAKER_FEE_SOURCE,
    )
