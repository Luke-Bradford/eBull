"""Pure-logic tests for the trade-events ledger transforms (#1593).

No DB — the one genuinely-new SQL mechanism (partial-unique dedup) is
covered by tests/test_trade_events_db.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.providers.broker import BrokerClosedTrade, BrokerPosition
from app.providers.implementations.etoro_broker import (
    TradeHistoryParseError,
    _parse_closed_trade,
)
from app.services.trade_events import (
    POST_TRADE_SYNC_JOB,
    TradeEventCounters,
    _side,
    events_from_history,
    fetch_trade_history_safely,
    merge_events,
    open_events_from_positions,
)
from app.workers.scheduler import JOB_DAILY_PORTFOLIO_SYNC

_FIXTURE = Path(__file__).parent / "fixtures" / "etoro" / "trade_history_demo.json"


def _fixture_rows() -> list[dict[str, Any]]:
    return json.loads(_FIXTURE.read_text())["body"]


def _closed_trade(**overrides: Any) -> BrokerClosedTrade:
    base: dict[str, Any] = {
        "position_id": 100,
        "instrument_id": 4077,
        "is_buy": True,
        "units": Decimal("10"),
        "open_rate": Decimal("97.3"),
        "open_timestamp": datetime(2025, 8, 12, 16, 47, tzinfo=UTC),
        "close_rate": Decimal("120.56"),
        "close_timestamp": datetime(2025, 11, 14, 19, 24, tzinfo=UTC),
        "net_profit": Decimal("100.5"),
        "fees": Decimal("0"),
        "investment": Decimal("973"),
        "initial_investment": Decimal("973"),
        "leverage": 1,
        "order_id": 1,
        "social_trade_id": 0,
        "parent_position_id": 0,
        "raw_payload": {"positionId": 100},
    }
    base.update(overrides)
    return BrokerClosedTrade(**base)


def _broker_position(**overrides: Any) -> BrokerPosition:
    base: dict[str, Any] = {
        "instrument_id": 4077,
        "units": Decimal("5"),
        "open_price": Decimal("97.3"),
        "current_price": Decimal("97.3"),
        "raw_payload": {"positionID": 200},
        "position_id": 200,
        "is_buy": True,
        "initial_units": Decimal("8"),
        "initial_amount_in_dollars": Decimal("778.4"),
        "open_date_time": datetime(2025, 8, 12, tzinfo=UTC),
    }
    base.update(overrides)
    return BrokerPosition(**base)


class TestParseClosedTrade:
    def test_fixture_row_parses_to_exact_ilmn_figures(self) -> None:
        trade = _parse_closed_trade(_fixture_rows()[0])
        assert trade.position_id == 3308442654
        assert trade.instrument_id == 4077
        assert trade.is_buy is True
        assert trade.units == Decimal("82.135523")
        assert trade.open_rate == Decimal("97.3")
        assert trade.close_rate == Decimal("120.56")
        assert trade.net_profit == Decimal("1910.47")
        assert trade.fees == Decimal("0.0")
        assert trade.investment == Decimal("7991.79")
        assert trade.open_timestamp == datetime(2025, 8, 12, 16, 47, 12, 643000, tzinfo=UTC)
        assert trade.close_timestamp == datetime(2025, 11, 14, 19, 24, 35, 307000, tzinfo=UTC)
        assert trade.order_id == 272136682
        assert trade.social_trade_id == 0

    def test_missing_required_field_raises_key_error(self) -> None:
        row = dict(_fixture_rows()[0])
        del row["closeTimestamp"]
        with pytest.raises(KeyError):
            _parse_closed_trade(row)


class TestSideDerivation:
    @pytest.mark.parametrize(
        ("kind", "is_buy", "expected"),
        [
            ("open", True, "buy"),
            ("close", True, "sell"),
            ("open", False, "sell"),
            ("close", False, "buy"),
        ],
    )
    def test_side_table(self, kind: str, is_buy: bool, expected: str) -> None:
        assert _side(kind, is_buy) == expected  # type: ignore[arg-type]


class TestOpenEventsFromPositions:
    def test_emits_open_with_initial_units_not_current(self) -> None:
        counters = TradeEventCounters()
        events = open_events_from_positions([_broker_position()], counters)
        assert len(events) == 1
        assert events[0].event_kind == "open"
        assert events[0].units == Decimal("8")  # initialUnits, not the partial-close-reduced 5
        assert events[0].source == "etoro_sync"
        assert events[0].executed_at == datetime(2025, 8, 12, tzinfo=UTC)

    def test_synthetic_negative_id_excluded(self) -> None:
        counters = TradeEventCounters()
        events = open_events_from_positions([_broker_position(position_id=-42)], counters)
        assert events == []
        assert counters.skipped_other == 0  # silent by design: handoff artefact, not data loss

    def test_missing_position_id_excluded(self) -> None:
        counters = TradeEventCounters()
        assert open_events_from_positions([_broker_position(position_id=None)], counters) == []

    def test_missing_open_date_skipped_and_counted(self) -> None:
        counters = TradeEventCounters()
        events = open_events_from_positions([_broker_position(open_date_time=None)], counters)
        assert events == []
        assert counters.skipped_other == 1
        assert "no openDateTime" in counters.skip_reasons[0]

    def test_sentinel_open_price_lands_as_none(self) -> None:
        # null_price is counted at INGEST time (landed rows only), so the
        # transform just maps the sentinel to None — see test_trade_events_db.
        counters = TradeEventCounters()
        events = open_events_from_positions([_broker_position(open_price=Decimal("0"))], counters)
        assert events[0].price is None
        assert counters.null_price == 0

    def test_fees_not_stamped_on_open_events(self) -> None:
        counters = TradeEventCounters()
        events = open_events_from_positions([_broker_position(total_fees=Decimal("3.5"))], counters)
        assert events[0].fees_usd is None


class TestEventsFromHistory:
    def test_single_trade_yields_open_and_close(self) -> None:
        counters = TradeEventCounters()
        events = events_from_history([_closed_trade()], counters)
        kinds = sorted(e.event_kind for e in events)
        assert kinds == ["close", "open"]
        close = next(e for e in events if e.event_kind == "close")
        opened = next(e for e in events if e.event_kind == "open")
        assert opened.units == Decimal("10")
        assert opened.realized_pnl_usd is None
        assert close.units == Decimal("10")
        assert close.realized_pnl_usd == Decimal("100.5")
        assert close.executed_at == datetime(2025, 11, 14, 19, 24, tzinfo=UTC)
        assert close.side == "sell"

    def test_partial_slices_group_to_one_open_with_summed_units(self) -> None:
        counters = TradeEventCounters()
        slice_a = _closed_trade(units=Decimal("4"), close_timestamp=datetime(2025, 9, 1, tzinfo=UTC))
        slice_b = _closed_trade(units=Decimal("6"), close_timestamp=datetime(2025, 10, 1, tzinfo=UTC))
        events = events_from_history([slice_a, slice_b], counters)
        opens = [e for e in events if e.event_kind == "open"]
        closes = [e for e in events if e.event_kind == "close"]
        assert len(opens) == 1
        assert opens[0].units == Decimal("10")
        assert sorted(c.units for c in closes) == [Decimal("4"), Decimal("6")]

    def test_short_position_open_is_sell(self) -> None:
        counters = TradeEventCounters()
        events = events_from_history([_closed_trade(is_buy=False)], counters)
        opened = next(e for e in events if e.event_kind == "open")
        close = next(e for e in events if e.event_kind == "close")
        assert opened.side == "sell"
        assert close.side == "buy"

    def test_mirror_trade_carries_social_trade_id(self) -> None:
        counters = TradeEventCounters()
        events = events_from_history([_closed_trade(social_trade_id=98765)], counters)
        assert all(e.social_trade_id == 98765 for e in events)


class TestMergeEvents:
    def test_portfolio_open_beats_history_open_for_same_position(self) -> None:
        counters = TradeEventCounters()
        portfolio_opens = open_events_from_positions([_broker_position(position_id=100)], counters)
        history = events_from_history([_closed_trade(position_id=100)], counters)
        merged = merge_events(portfolio_opens, history)
        opens = [e for e in merged if e.event_kind == "open"]
        assert len(opens) == 1
        assert opens[0].source == "etoro_sync"
        # The history close survives the merge.
        assert any(e.event_kind == "close" for e in merged)

    def test_history_only_position_keeps_its_synthesized_open(self) -> None:
        counters = TradeEventCounters()
        merged = merge_events([], events_from_history([_closed_trade()], counters))
        assert any(e.event_kind == "open" and e.source == "etoro_history" for e in merged)


class _RaisingBroker:
    """Minimal BrokerProvider stand-in for the fetch error-posture table."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def get_trade_history(self, min_date: datetime, page_size: int = 200) -> list[BrokerClosedTrade]:
        raise self._exc


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.invalid")
    return httpx.HTTPStatusError("boom", request=request, response=httpx.Response(status, request=request))


class TestFetchTradeHistorySafely:
    @pytest.mark.parametrize(
        "exc",
        [
            _http_status_error(403),
            _http_status_error(429),
            _http_status_error(500),
            httpx.ConnectError("network down"),
            TradeHistoryParseError("bad row"),
            NotImplementedError(),  # provider without an override (ABC default)
        ],
    )
    def test_failures_return_none_so_positions_still_sync(self, exc: Exception) -> None:
        assert fetch_trade_history_safely(_RaisingBroker(exc), datetime(2020, 1, 1, tzinfo=UTC)) is None  # type: ignore[arg-type]


def test_post_trade_sync_job_name_matches_scheduler_constant() -> None:
    """POST_TRADE_SYNC_JOB is duplicated to avoid a circular import —
    this pin is the drift guard (#1593 spec §1.3)."""
    assert POST_TRADE_SYNC_JOB == JOB_DAILY_PORTFOLIO_SYNC
