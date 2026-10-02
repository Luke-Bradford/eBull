"""#3543 slice 2: the endpoint's response model validates the read model's nested dataclasses."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from app.api.strategies import EngineBookRiskResponse
from app.services.ai_trial_status import JobFire
from app.services.engine_book_risk_status import EngineBookRiskStatus, LatestSnapshot, RecentSnapshot

_AT = datetime(2026, 10, 2, 9, tzinfo=UTC)
_JOB = JobFire("engine_book_risk_snapshot", _AT, _AT, _AT, "failure", "benchmark_not_ready")


def test_a_populated_status_serialises() -> None:
    latest = LatestSnapshot(
        session_date=date(2026, 10, 1),
        measured_at=_AT,
        pool_event_id=7,
        capital_usd=Decimal("40000"),
        gross_usd=Decimal("16078.026146"),
        position_count=1,
        instrument_count=1,
        open_trade_count=1,
        cost_marked_count=0,
        stale_count=0,
        largest_share_pct=Decimal("100"),
        top5_share_pct=Decimal("100"),
        hhi=Decimal("10000"),
        hist_vol_pct=Decimal("5.12"),
        ewma_vol_pct=Decimal("4.05"),
        beta=Decimal("0.39"),
        vol_n_obs=235,
        beta_n_obs=235,
        sample_first=date(2025, 10, 28),
        sample_last=date(2026, 10, 1),
        history_status="ok",
        beta_defaulted_count=0,
        beta_defaulted_weight_pct=Decimal("0"),
        stress_2020_pct=Decimal("-13.18"),
        stress_2022_pct=Decimal("-9.80"),
        checks={"stale_marks": {"status": "evaluated", "value": "0", "limit": "0", "flagged": False}},
        positions=[{"instrument_id": 3417, "symbol": "SPY.RTH", "beta": "defaulted"}],
    )
    recent = RecentSnapshot(
        session_date=date(2026, 10, 1),
        measured_at=_AT,
        capital_usd=Decimal("40000"),
        gross_usd=Decimal("16078.026146"),
        hist_vol_pct=None,
        ewma_vol_pct=None,
        beta=None,
        stress_2020_pct=Decimal("-13.18"),
        stress_2022_pct=Decimal("-9.80"),
        stale_count=0,
        history_status="insufficient_history",
        flagged=["stale_marks"],
    )
    body = EngineBookRiskResponse.model_validate(
        EngineBookRiskStatus("engine-book-risk-v1", _JOB, latest, [recent]), from_attributes=True
    ).model_dump(mode="json")
    assert body["job"]["last_note"] == "benchmark_not_ready"
    assert body["latest"]["stress_2020_pct"] == "-13.18"
    assert body["latest"]["positions"] == [{"instrument_id": 3417, "symbol": "SPY.RTH", "beta": "defaulted"}]
    assert body["recent"][0]["flagged"] == ["stale_marks"]


def test_no_snapshot_serialises_as_null() -> None:
    body = EngineBookRiskResponse.model_validate(
        EngineBookRiskStatus("engine-book-risk-v1", _JOB, None, []), from_attributes=True
    ).model_dump(mode="json")
    assert body["latest"] is None and body["recent"] == []
