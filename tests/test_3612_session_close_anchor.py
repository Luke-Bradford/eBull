"""#3612 slice 2 — which official snapshot may become a session's close anchor S, and the sync wiring.

Pure tier. The insert-once writer and the table CHECKs are in ``test_3612_session_close_anchor_db``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerDirectPositionInvestment,
    BrokerInstrumentInvestment,
)
from app.services.account_equity_evidence import session_close_snapshot_defect

# Monday 2026-10-12: regular close 16:00 New York = 20:00Z (EDT).
CLOSE = datetime(2026, 10, 12, 20, 0, tzinfo=UTC)


def position(
    position_id: int = 1,
    *,
    pnl_timestamp: datetime | None = CLOSE + timedelta(minutes=10),
    total_fees: Decimal | None = Decimal("0"),
    units: str = "2",
    pnl: str = "5",
) -> BrokerDirectPositionInvestment:
    return BrokerDirectPositionInvestment(
        position_id=position_id,
        instrument_id=100 + position_id,
        is_buy=True,
        units=Decimal(units),
        amount=Decimal("100"),
        unrealized_pnl=Decimal(pnl),
        market_value=Decimal("100") + Decimal(pnl),
        is_partially_altered=False,
        close_rate=Decimal("52.5"),
        close_conversion_rate=Decimal("1"),
        asset_currency_id=1,
        pnl_timestamp=pnl_timestamp,
        total_fees=total_fees,
    )


def snapshot(
    observed_at: datetime = CLOSE + timedelta(minutes=10, seconds=2),
    positions: tuple[BrokerDirectPositionInvestment, ...] | None = None,
    account_currency_id: int | None = 1,
) -> BrokerAccountRiskSnapshot:
    direct = (position(),) if positions is None else positions
    return BrokerAccountRiskSnapshot(
        available_cash=Decimal("500"),
        total_invested=Decimal("400"),
        unrealized_pnl=Decimal("100"),
        equity=Decimal("1000"),
        instrument_investments=(
            BrokerInstrumentInvestment(
                instrument_id=101,
                amount=Decimal("400"),
                direct_long_market_value=Decimal("105"),
                direct_long_positions=len(direct),
                direct_short_positions=0,
            ),
        ),
        observed_at=observed_at,
        raw_payload={},
        account_currency_id=account_currency_id,
        pending_order_amount=Decimal("0"),
        direct_positions=direct,
    )


def test_a_snapshot_after_the_close_on_the_session_date_qualifies() -> None:
    assert session_close_snapshot_defect(snapshot()) is None


def test_a_genuinely_empty_book_qualifies() -> None:
    assert session_close_snapshot_defect(snapshot(positions=())) is None


@pytest.mark.parametrize(
    ("observed_at", "why"),
    [
        (CLOSE - timedelta(seconds=1), "before the close: P is the previous session"),
        (datetime(2026, 10, 13, 4, 0, 1, tzinfo=UTC), "after New York midnight: the date is not P"),
        (datetime(2026, 10, 10, 18, 0, tzinfo=UTC), "Saturday: P is Friday, a different date"),
        (datetime(2026, 11, 26, 22, 0, tzinfo=UTC), "Thanksgiving, closed: P is the day before"),
    ],
)
def test_a_snapshot_off_the_session_date_after_its_close_is_refused(observed_at: datetime, why: str) -> None:
    observed = snapshot(observed_at=observed_at, positions=(position(pnl_timestamp=observed_at),))
    assert session_close_snapshot_defect(observed) is not None, why


def test_a_half_day_uses_the_13_00_close() -> None:
    # 2026-11-27, the day after Thanksgiving, closes at 13:00 New York = 18:00Z (EST).
    half_close = datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    after = snapshot(observed_at=half_close + timedelta(seconds=5), positions=(position(pnl_timestamp=half_close),))
    assert session_close_snapshot_defect(after) is None
    before = snapshot(
        observed_at=half_close - timedelta(seconds=1),
        positions=(position(pnl_timestamp=half_close - timedelta(seconds=2)),),
    )
    assert session_close_snapshot_defect(before) is not None


@pytest.mark.parametrize(
    "bad",
    [
        position(pnl_timestamp=None),
        position(pnl_timestamp=CLOSE - timedelta(seconds=1)),
        position(pnl_timestamp=CLOSE + timedelta(minutes=11)),  # after receipt
        position(total_fees=None),
        position(total_fees=Decimal("NaN")),
        position(units="Infinity"),
        position(pnl="NaN"),
    ],
)
def test_a_position_without_a_usable_mark_time_fees_or_values_refuses_the_whole_snapshot(
    bad: BrokerDirectPositionInvestment,
) -> None:
    observed = snapshot(positions=(position(2), replace(bad, position_id=3)))
    defect = session_close_snapshot_defect(observed)
    assert defect is not None and "position 3" in defect


def test_mark_time_bounds_are_inclusive() -> None:
    observed_at = CLOSE + timedelta(minutes=10)
    at_close = position(1, pnl_timestamp=CLOSE)
    at_receipt = position(2, pnl_timestamp=observed_at)
    assert session_close_snapshot_defect(snapshot(observed_at=observed_at, positions=(at_close, at_receipt))) is None


def test_a_snapshot_the_equity_validator_refuses_is_refused() -> None:
    assert session_close_snapshot_defect(snapshot(account_currency_id=None)) is not None


# --------------------------------------------------------------------------
# daily_portfolio_sync wiring: the anchor write is isolated from the sync
# --------------------------------------------------------------------------


class _Conn:
    def __init__(self) -> None:
        self.committed = False

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class _Broker:
    def __init__(self, **_: Any) -> None:
        pass

    def __enter__(self) -> _Broker:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def get_portfolio(self) -> object:
        return object()

    def get_account_risk_snapshot(self) -> BrokerAccountRiskSnapshot:
        return snapshot()


def test_an_anchor_write_failure_keeps_the_equity_snapshot_and_the_sync(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import app.providers.implementations.etoro_broker as etoro_broker
    import app.services.account_equity_evidence as evidence
    from app.workers import scheduler

    conn = _Conn()
    equity_writes: list[object] = []
    tracker = SimpleNamespace(row_count=None)

    @contextmanager
    def tracked(_name: str) -> Iterator[SimpleNamespace]:
        yield tracker

    def failing_anchor(*_a: object, **_k: object) -> bool:
        raise RuntimeError("anchor write failed")

    monkeypatch.setattr(scheduler, "_load_etoro_credentials", lambda _name: ("api", "user"))
    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", lambda: conn)
    monkeypatch.setattr(scheduler, "compute_history_min_date", lambda _conn: None)
    monkeypatch.setattr(scheduler, "fetch_trade_history_safely", lambda _broker, _since: None)
    monkeypatch.setattr(scheduler.settings, "etoro_env", "demo")
    monkeypatch.setattr(etoro_broker, "EtoroBrokerProvider", _Broker)
    monkeypatch.setattr(
        scheduler,
        "sync_portfolio",
        lambda _conn, _portfolio, trade_history: SimpleNamespace(
            positions_updated=1,
            positions_opened_externally=0,
            positions_closed_externally=0,
            mirrors_upserted=0,
            mirrors_closed=0,
            mirror_positions_upserted=0,
            broker_cash=0.0,
            local_cash=0.0,
            cash_delta=0.0,
        ),
    )
    monkeypatch.setattr(scheduler, "_promote_held_to_tier1", lambda _conn: 0)
    monkeypatch.setattr(
        evidence, "record_account_equity_snapshot", lambda _conn, **kwargs: equity_writes.append(kwargs) or True
    )
    monkeypatch.setattr(evidence, "record_session_close_snapshot", failing_anchor)

    with caplog.at_level("ERROR", logger=scheduler.logger.name):
        scheduler.daily_portfolio_sync()

    assert len(equity_writes) == 1
    assert conn.committed
    assert tracker.row_count == 1
    assert any("session-close anchor could not be stored" in r.getMessage() for r in caplog.records)
