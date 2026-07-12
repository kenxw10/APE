from __future__ import annotations

from datetime import timedelta

from tests.test_research_helpers import at_base, feature_event, orderbook_event, valid_vector

from ape.research.replay import DeterministicReplayEngine
from ape.strategy.momentum_v2 import evaluate_momentum_v2_feature_vector


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
