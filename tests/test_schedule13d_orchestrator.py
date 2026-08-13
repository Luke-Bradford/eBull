from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from scripts import schedule13d_orchestrator as subject
from scripts.evaluate_2582_schedule13d_outcomes import OutcomeGate, SourceEvent


def _source() -> SourceEvent:
    return SourceEvent(
        accession_number="t-1",
        issuer_cik="issuer-1",
        instrument_id=42,
        public_filing_date=date(2026, 2, 2),
        maximum_percent_of_class=Decimal("7.5"),
        prior_active=False,
        prior_passive=False,
        same_public_date_peer=False,
        reporter_identity_complete=True,
        current_security_eligible=True,
        series_ids=(99,),
        series_adjustment_bases=("split_adjusted",),
    )


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Connection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object | None]] = []
        self.rolled_back = False

    def execute(self, query: str, params: object | None = None) -> _Result:
        self.statements.append((query, params))
        if "FROM instruments i" in query:
            return _Result([(42, "Technology"), (43, "provider_industry_id:9")])
        return _Result([])

    def rollback(self) -> None:
        self.rolled_back = True


def test_current_sector_labels_are_current_attribution_and_keep_provider_fallback() -> None:
    conn = _Connection()
    labels = subject.load_current_sector_labels(conn, (_source(),))  # type: ignore[arg-type]
    assert labels == {42: "Technology", 43: "provider_industry_id:9"}
    query, params = conn.statements[-1]
    assert "etoro_stocks_industries" in query
    assert params == {"instrument_ids": [42]}


def test_orchestrator_is_read_only_and_loads_every_arm_before_one_report(monkeypatch: Any) -> None:
    conn = _Connection()
    gate = OutcomeGate("hash", "register", "trial", 11, 22)
    source = (_source(),)
    calls: list[str] = []

    monkeypatch.setattr(subject, "load_source_events", lambda _conn: calls.append("sources") or source)
    monkeypatch.setattr(subject, "prepare_price_window_workspace", lambda _conn: calls.append("workspace"))
    monkeypatch.setattr(subject, "load_initial_13g_source_events", lambda _conn: calls.append("13g_sources") or ())

    def price(_conn: Any, _gate: Any, _events: Any, *, population: str) -> tuple[str, ...]:
        calls.append(population)
        return (population,)

    monkeypatch.setattr(subject, "load_price_windows", price)
    monkeypatch.setattr(
        subject,
        "load_random_time_price_windows",
        lambda _conn, _gate, _events: calls.append("random") or ("random",),
    )
    monkeypatch.setattr(
        subject,
        "load_initial_13g_price_windows",
        lambda _conn, _gate, _events: calls.append("13g_prices") or ("13g",),
    )
    monkeypatch.setattr(subject, "load_current_sector_labels", lambda _conn, _events: {42: "Technology"})

    captured: dict[str, Any] = {}

    def report(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "aggregate"

    monkeypatch.setattr(subject, "build_historical_falsification_report", report)

    result = subject.evaluate_historical_falsification(conn, gate)  # type: ignore[arg-type]

    assert result == "aggregate"
    assert conn.rolled_back
    assert conn.statements[0] == ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", None)
    assert calls == ["workspace", "sources", "13g_sources", "primary", "unfiltered", "random", "13g_prices"]
    assert captured["sector_by_instrument"] == {42: "Technology"}
    assert captured["primary_windows"] == ("primary",)
    assert captured["unfiltered_windows"] == ("unfiltered",)


def test_orchestrator_rolls_back_its_snapshot_when_evaluation_raises(monkeypatch: Any) -> None:
    conn = _Connection()
    monkeypatch.setattr(subject, "prepare_price_window_workspace", lambda _conn: None)
    monkeypatch.setattr(subject, "load_source_events", lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        subject.evaluate_historical_falsification(conn, OutcomeGate("hash", "register", "trial", 11, 22))  # type: ignore[arg-type]
    assert conn.rolled_back
