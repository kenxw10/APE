from __future__ import annotations

from dataclasses import dataclass

from ape.config import AppConfig, AppMode


class SafetyError(RuntimeError):
    """Raised when startup safety checks block the process."""


@dataclass(frozen=True)
class SafetyAssessment:
    mode: str
    trading_enabled: bool
    execute: bool
    is_safe: bool
    blockers: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "trading_enabled": self.trading_enabled,
            "execute": self.execute,
            "is_safe": self.is_safe,
            "blockers": self.blockers,
            "warnings": self.warnings,
        }


def assess_startup_safety(config: AppConfig) -> SafetyAssessment:
    blockers: list[str] = []
    warnings: list[str] = []

    if config.app_mode is not AppMode.OBSERVER:
        blockers.append("Current safety policy only permits APP_MODE=OBSERVER.")

    if config.trading_enabled:
        blockers.append("TRADING_ENABLED=true is blocked in PR 1.")

    if config.execute:
        blockers.append("EXECUTE=true is blocked in PR 1.")

    if config.kalshi_api_key_id or config.kalshi_private_key:
        warnings.append("Kalshi credentials are configured for observer-only REST diagnostics.")

    return SafetyAssessment(
        mode=config.app_mode.value,
        trading_enabled=config.trading_enabled,
        execute=config.execute,
        is_safe=len(blockers) == 0,
        blockers=blockers,
        warnings=warnings,
    )


def assert_startup_safe(assessment: SafetyAssessment) -> None:
    if assessment.is_safe:
        return

    joined_blockers = "; ".join(assessment.blockers)
    raise SafetyError(f"Unsafe startup configuration: {joined_blockers}")
