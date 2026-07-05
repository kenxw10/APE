from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ape.kalshi.types import KalshiMarketPayload

PRICE_PATTERN = re.compile(r"\$?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{5,}(?:\.\d+)?)")
STRUCTURED_BOUNDARY_FIELDS = (
    "functional_strike",
    "floor_strike",
    "cap_strike",
    "custom_strike",
)
TEXT_BOUNDARY_FIELDS = (
    "title",
    "subtitle",
    "yes_sub_title",
    "no_sub_title",
    "rules_primary",
    "rules_secondary",
)


@dataclass(frozen=True)
class ParsedBoundary:
    functional_strike: Decimal | None
    floor_strike: Decimal | None
    cap_strike: Decimal | None
    custom_strike: Decimal | None
    source: str
    parse_status: str
    blockers: list[str]
    warnings: list[str]

    @property
    def is_parseable(self) -> bool:
        return self.parse_status in {"structured", "text_fallback"}


def parse_market_boundary(market: KalshiMarketPayload) -> ParsedBoundary:
    structured_values = {
        field: _decimal_or_none(market.get(field)) for field in STRUCTURED_BOUNDARY_FIELDS
    }
    present_structured_values = [value for value in structured_values.values() if value is not None]
    text_candidates = _extract_text_candidates(market)

    if present_structured_values:
        text_matches_structured = any(
            candidate in present_structured_values for candidate in text_candidates
        )
        if text_candidates and not text_matches_structured:
            return ParsedBoundary(
                functional_strike=structured_values["functional_strike"],
                floor_strike=structured_values["floor_strike"],
                cap_strike=structured_values["cap_strike"],
                custom_strike=structured_values["custom_strike"],
                source="structured_with_text_disagreement",
                parse_status="not_parseable",
                blockers=["structured_boundary_disagrees_with_text"],
                warnings=[],
            )

        warnings = []
        if not text_candidates:
            warnings.append("boundary_text_not_available_for_cross_check")

        return ParsedBoundary(
            functional_strike=structured_values["functional_strike"],
            floor_strike=structured_values["floor_strike"],
            cap_strike=structured_values["cap_strike"],
            custom_strike=structured_values["custom_strike"],
            source="structured",
            parse_status="structured",
            blockers=[],
            warnings=warnings,
        )

    if len(text_candidates) == 1:
        return ParsedBoundary(
            functional_strike=text_candidates[0],
            floor_strike=None,
            cap_strike=None,
            custom_strike=None,
            source="text_fallback",
            parse_status="text_fallback",
            blockers=[],
            warnings=["boundary_parsed_from_text_fallback"],
        )

    if len(text_candidates) > 1:
        return ParsedBoundary(
            functional_strike=None,
            floor_strike=None,
            cap_strike=None,
            custom_strike=None,
            source="text_fallback_ambiguous",
            parse_status="not_parseable",
            blockers=["multiple_text_boundary_candidates"],
            warnings=[],
        )

    return ParsedBoundary(
        functional_strike=None,
        floor_strike=None,
        cap_strike=None,
        custom_strike=None,
        source="none",
        parse_status="not_parseable",
        blockers=["boundary_not_found"],
        warnings=[],
    )


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None

    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _extract_text_candidates(market: KalshiMarketPayload) -> list[Decimal]:
    values: set[Decimal] = set()
    for field in TEXT_BOUNDARY_FIELDS:
        raw_value = market.get(field)
        if not isinstance(raw_value, str):
            continue
        for match in PRICE_PATTERN.findall(raw_value):
            parsed = _decimal_or_none(match)
            if parsed is not None and parsed >= Decimal("10000"):
                values.add(parsed)

    return sorted(values)
