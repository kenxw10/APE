from __future__ import annotations

from types import SimpleNamespace

from ape.research.repository import ResearchRepository


def test_postgres_challenger_admission_uses_architecture_transaction_lock() -> None:
    calls: list[tuple[object, dict[str, str]]] = []

    class FakeSession:
        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def execute(self, statement, values) -> None:
            calls.append((statement, values))

    ResearchRepository(FakeSession())._lock_challenger_architecture("momentum_v2_heuristic_v3")

    assert len(calls) == 1
    statement, values = calls[0]
    assert "pg_advisory_xact_lock" in str(statement)
    assert values == {"architecture_version": "momentum_v2_heuristic_v3"}
