from __future__ import annotations

import pytest

from ape.config import load_config
from ape.safety import assert_startup_safe, assess_startup_safety


def test_default_safety_is_observer_only_and_safe() -> None:
    assessment = assess_startup_safety(load_config({}))

    assert assessment.mode == "OBSERVER"
    assert assessment.trading_enabled is False
    assert assessment.execute is False
    assert assessment.is_safe is True
    assert assessment.blockers == []


def test_execute_true_is_blocked() -> None:
    assessment = assess_startup_safety(load_config({"EXECUTE": "true"}))

    assert assessment.is_safe is False
    assert any("EXECUTE=true" in blocker for blocker in assessment.blockers)


def test_trading_enabled_true_is_blocked() -> None:
    assessment = assess_startup_safety(load_config({"TRADING_ENABLED": "true"}))

    assert assessment.is_safe is False
    assert any("TRADING_ENABLED=true" in blocker for blocker in assessment.blockers)


@pytest.mark.parametrize("mode", ["DRY_RUN", "PAPER", "LIVE"])
def test_non_observer_modes_are_blocked_in_pr_1(mode: str) -> None:
    assessment = assess_startup_safety(load_config({"APP_MODE": mode}))

    assert assessment.is_safe is False
    assert any("APP_MODE=OBSERVER" in blocker for blocker in assessment.blockers)


def test_unsafe_assessment_raises_on_startup() -> None:
    assessment = assess_startup_safety(load_config({"EXECUTE": "true"}))

    with pytest.raises(RuntimeError, match="Unsafe startup configuration"):
        assert_startup_safe(assessment)

