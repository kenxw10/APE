from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from ape.research.fees import (
    KALSHI_FEE_PARAMETER_SNAPSHOT,
    KALSHI_FEE_PARAMETER_SNAPSHOT_SHA256,
    verified_kalshi_taker_fee_model,
)


def test_july_2026_fee_parameter_snapshot_is_canonical_and_not_a_pdf_hash() -> None:
    canonical = json.dumps(
        KALSHI_FEE_PARAMETER_SNAPSHOT, sort_keys=True, separators=(",", ":")
    )
    assert canonical == (
        '{"default_maker_multiplier":"0","default_taker_multiplier":"1",'
        '"document_title":"Fee Schedule for July 2026 - 7.7.26 Update",'
        '"effective_date":"2026-07-07","kxbtc15m_listed_as_non_standard":false,'
        '"kxbtc15m_maker_multiplier":"0","kxbtc15m_taker_multiplier":"1",'
        '"maker_formula":"round_up(M * 0.0175 * C * P * (1-P))",'
        '"rounding_rule_source_text":"rounds up such that the fee + positionCost '
        'is rounded to a centicent","settlement_fee":"0",'
        '"source_url":"https://kalshi.com/docs/kalshi-fee-schedule.pdf",'
        '"taker_formula":"round_up(M * 0.07 * C * P * (1-P))"}'
    )
    assert hashlib.sha256(canonical.encode()).hexdigest() == KALSHI_FEE_PARAMETER_SNAPSHOT_SHA256
    metadata = verified_kalshi_taker_fee_model().metadata()
    assert metadata["parameter_snapshot_sha256"] == KALSHI_FEE_PARAMETER_SNAPSHOT_SHA256
    assert "source_checksum" not in metadata


def test_july_2026_general_taker_fee_table_examples() -> None:
    model = verified_kalshi_taker_fee_model()
    examples = {
        ("1", "0.01"): "1",
        ("1", "0.20"): "2",
        ("1", "0.50"): "2",
        ("1", "0.85"): "1",
        ("1", "0.99"): "1",
        ("100", "0.01"): "7",
        ("100", "0.25"): "132",
        ("100", "0.50"): "175",
        ("100", "0.80"): "112",
        ("100", "0.99"): "7",
    }
    for (contracts, price), expected_cents in examples.items():
        assert model.fee_cents(
            contracts=Decimal(contracts), price=Decimal(price)
        ) == Decimal(expected_cents)
