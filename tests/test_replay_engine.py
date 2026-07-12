from __future__ import annotations

import copy
from datetime import timedelta
from decimal import Decimal

from tests.test_research_helpers import at_base, feature_event, orderbook_event, valid_vector

from ape.db.models import ResearchMarketOutcome
from ape.research.replay import (
    DeterministicReplayEngine,
    _lifecycle_inputs,
    _OpenPosition,
    _PendingEntry,
)
from ape.strategy.momentum_v2 import V2_PARAMETERS, evaluate_momentum_v2_feature_vector


def test_replay_is_order_deterministic_and_has_no_future_leakage() -> None:
    at = at_base()
    events = [
        feature_event(at=at),
        orderbook_event(at=at + timedelta(seconds=1), event_id="book-1"),
    ]
    first = DeterministicReplayEngine().replay(events)
    shuffled = DeterministicReplayEngine().replay(list(reversed(events)))
    changed_future = DeterministicReplayEngine().replay(
        events + [orderbook_event(at=at + timedelta(seconds=10), event_id="book-2", yes_ask="0.10")]
    )
    assert [decision.state for decision in first.decisions] == [
        decision.state for decision in shuffled.decisions
    ]
    assert first.decisions[0].state == changed_future.decisions[0].state


def test_first_in_window_book_cannot_be_rescued_by_a_later_book() -> None:
    at = at_base()
    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(at=at + timedelta(milliseconds=600), event_id="first", yes_ask="0.79"),
            orderbook_event(at=at + timedelta(milliseconds=900), event_id="later", yes_ask="0.60"),
        ]
    )
    assert [trade.status for trade in result.trades] == ["ENTRY_NO_FILL"]


def test_replay_orders_same_timestamp_books_by_sequence_before_source_id() -> None:
    at = at_base()
    source_id_ten = orderbook_event(
        at=at + timedelta(milliseconds=600),
        event_id="book-10",
        yes_ask="0.79",
    )
    source_id_ten.sequence_number = 10
    source_id_two = orderbook_event(
        at=at + timedelta(milliseconds=600),
        event_id="book-2",
        yes_ask="0.60",
    )
    source_id_two.sequence_number = 2
    outcome = ResearchMarketOutcome(
        outcome_id="sequence-order",
        market_ticker="M1",
        outcome_status="RESOLVED",
        result_side="YES",
        resolved_at=at + timedelta(minutes=15),
    )

    result = DeterministicReplayEngine().replay(
        [feature_event(at=at), source_id_ten, source_id_two], outcomes=[outcome]
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_fill_event_id == "book-2"
    assert result.trades[0].status == "CLOSED"


def test_first_in_window_exit_book_cannot_be_rescued_by_a_later_book() -> None:
    at = at_base()
    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(at=at + timedelta(milliseconds=600), event_id="entry"),
            orderbook_event(at=at + timedelta(seconds=61), event_id="exit-first", yes_bid_size="0"),
            orderbook_event(at=at + timedelta(seconds=62), event_id="exit-later", yes_bid="0.90"),
        ]
    )
    assert not [trade for trade in result.trades if trade.status == "CLOSED"]


def test_replay_preserves_exit_trigger_intent_and_fill_values() -> None:
    at = at_base()
    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(at=at + timedelta(milliseconds=600), event_id="entry"),
            feature_event(at=at + timedelta(seconds=61), event_id="exit-trigger"),
            orderbook_event(
                at=at + timedelta(seconds=62),
                event_id="exit-fill",
                yes_bid="0.65",
            ),
        ]
    )

    trade = next(trade for trade in result.trades if trade.status == "CLOSED")
    assert trade.exit_trigger_at == at + timedelta(seconds=61)
    assert trade.exit_intent_at == at + timedelta(seconds=61, milliseconds=500)
    assert trade.exit_limit == Decimal("0.57")
    assert trade.exit_fill_at == at + timedelta(seconds=62)
    assert trade.exit_fill_price == Decimal("0.65")
    assert trade.mfe_cents == Decimal("5")
    assert trade.time_to_mfe_ms == 61_400


def test_replay_closed_trade_keeps_the_source_feature_snapshot_id() -> None:
    at = at_base()
    feature = feature_event(at=at, event_id="replay-event-feature")
    feature.feature_snapshot_id = "immutable-feature-snapshot"
    outcome = ResearchMarketOutcome(
        outcome_id="feature-snapshot-source",
        market_ticker="M1",
        outcome_status="RESOLVED",
        result_side="YES",
        resolved_at=at + timedelta(minutes=15),
    )

    result = DeterministicReplayEngine().replay(
        [feature, orderbook_event(at=at + timedelta(milliseconds=600), event_id="entry")],
        outcomes=[outcome],
    )

    trade = next(trade for trade in result.trades if trade.status == "CLOSED")
    assert trade.measurements["entry_feature_snapshot_id"] == "immutable-feature-snapshot"


def test_zero_entry_audit_is_not_reported_as_healthy_selectivity() -> None:
    vector = valid_vector()
    vector["candidate_mode"] = "BOUNDARY_CROSS_HOLD"
    result = DeterministicReplayEngine().replay([feature_event(at=at_base(), vector=vector)])
    assert result.zero_entry_report["frequency_classification"] == "ZERO_ENTRY_UNVALIDATABLE"
    assert result.zero_entry_report["validation_status"] == "UNVALIDATABLE"


def test_persisted_vector_and_live_vector_evaluator_parity() -> None:
    vector = valid_vector()
    first = evaluate_momentum_v2_feature_vector(vector)
    second = evaluate_momentum_v2_feature_vector(dict(vector))
    assert (
        first.state,
        first.reason,
        first.blockers,
        first.score,
        first.edge_lower_bound_cents,
    ) == (second.state, second.reason, second.blockers, second.score, second.edge_lower_bound_cents)


def test_calibrated_impulse_keeps_a_flat_five_second_return() -> None:
    vector = valid_vector()
    vector.update(
        {
            "return_5s": Decimal("0"),
            "return_15s": Decimal("2"),
            "return_30s": Decimal("0"),
        }
    )
    parameters = copy.deepcopy(V2_PARAMETERS)
    parameters["calibration_overrides"] = {
        "fast_15": "1.25",
        "fast_30": "2",
        "adverse_5": "-0.5",
    }

    result = evaluate_momentum_v2_feature_vector(vector, parameters)

    assert result.candidate_mode == "CONTINUATION"
    assert "v2_fast_impulse_not_active" not in result.blockers


def test_replay_seeds_excursions_with_a_zero_bid() -> None:
    at = at_base()
    outcome = ResearchMarketOutcome(
        outcome_id="zero-bid-outcome",
        market_ticker="M1",
        outcome_status="RESOLVED",
        result_side="YES",
        resolved_at=at + timedelta(minutes=15),
    )

    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(
                at=at + timedelta(milliseconds=600),
                event_id="zero-bid-entry",
                yes_bid="0",
                yes_ask="0.60",
            ),
        ],
        outcomes=[outcome],
    )

    trade = next(trade for trade in result.trades if trade.status == "CLOSED")
    assert trade.mae_cents == Decimal("-60")


def test_replay_uses_settlement_time_for_a_worst_excursion() -> None:
    at = at_base()
    settled_at = at + timedelta(minutes=15)
    outcome = ResearchMarketOutcome(
        outcome_id="settlement-worst-excursion",
        market_ticker="M1",
        outcome_status="RESOLVED",
        result_side="NO",
        resolved_at=settled_at,
    )

    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(at=at + timedelta(milliseconds=600), event_id="entry"),
        ],
        outcomes=[outcome],
    )

    trade = next(trade for trade in result.trades if trade.status == "CLOSED")
    assert trade.mae_cents == Decimal("-60")
    assert trade.time_to_mae_ms == 899_400


def test_replay_lifecycle_inputs_use_candidate_tier_hold_windows() -> None:
    at = at_base()
    evaluation = evaluate_momentum_v2_feature_vector(valid_vector())
    pending = _PendingEntry(
        evaluation=evaluation,
        market_ticker="M1",
        event_id="entry",
        feature_snapshot_id="feature-entry",
        decision_at=at,
        effective_after=at,
        expires_at=at + timedelta(seconds=2),
    )
    position = _OpenPosition(
        pending=pending,
        entry_at=at,
        entry_price=Decimal("0.60"),
        entry_event_id="fill",
        best_bid=Decimal("0.60"),
        worst_bid=Decimal("0.60"),
        best_at=at,
        worst_at=at,
    )
    candidate_parameters = copy.deepcopy(V2_PARAMETERS)
    candidate_parameters["tiers"]["normal"].update({"time_stop": 71, "max_hold": 93})

    inputs = _lifecycle_inputs(
        position=position,
        evaluation=evaluation,
        features=valid_vector(),
        held_bid=Decimal("0.60"),
        market_matches=True,
        evaluated_at=at,
        parameters=candidate_parameters,
    )

    assert inputs["entry_time_stop_seconds"] == 71
    assert inputs["entry_max_hold_seconds"] == 93
