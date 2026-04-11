"""Shared test fixtures for copy-trading ingestion (spec §8.0).

This module owns the canonical `_NOW` constant used by every test
that exercises the mirror sync soft-close path. The value is
pinned to a frozen UTC timestamp so that `_sync_mirrors`'s
`UPDATE ... closed_at = %(now)s` clause produces a deterministic
stored value and tests can assert the exact round-trip.

It also owns `_GUARD_INSTRUMENT_ID` and `_GUARD_INSTRUMENT_SECTOR`
— the deterministic instrument-row identifiers used by the
guard-path fixtures delivered in Track 1b (#187). They are
declared here in Track 1a so all callers import them from one
place once Track 1b lands.

Track 1a ships the constants and the parser/sync fixture
builders (`two_mirror_payload`, `parse_failure_payload`,
`two_mirror_seed_rows`). Track 1b adds `mirror_aum_fixture`,
`no_quote_mirror_fixture`, `mtm_delta_mirror_fixture` on top.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.types.json

from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerPortfolio,
)

# Frozen "now" for every sync-side test. Matches the value
# tests/test_portfolio_sync.py used locally before this refactor
# (bit-identical — no behaviour change).
_NOW: datetime = datetime(2026, 4, 10, 5, 30, tzinfo=UTC)

# Guard test instrument — chosen well above any seed data in
# sql/001_init.sql so it cannot collide with real instruments.
# Track 1b's guard-integration test fixtures reuse it.
_GUARD_INSTRUMENT_ID: int = 990001
_GUARD_INSTRUMENT_SECTOR: str = "technology"


def _make_mirror_position(
    position_id: int,
    instrument_id: int = 42,
    units: Decimal = Decimal("6.28927"),
    open_rate: Decimal = Decimal("1207.4994"),
    open_conversion_rate: Decimal = Decimal("0.01331"),
    amount: Decimal = Decimal("101.08"),
    is_buy: bool = True,
) -> BrokerMirrorPosition:
    return BrokerMirrorPosition(
        position_id=position_id,
        parent_position_id=position_id + 4000,
        instrument_id=instrument_id,
        is_buy=is_buy,
        units=units,
        amount=amount,
        initial_amount_in_dollars=amount,
        open_rate=open_rate,
        open_conversion_rate=open_conversion_rate,
        open_date_time=_NOW,
        take_profit_rate=None,
        stop_loss_rate=None,
        total_fees=Decimal("0"),
        leverage=1,
        raw_payload={
            "positionID": position_id,
            "instrumentID": instrument_id,
        },
    )


def _make_mirror(
    mirror_id: int,
    parent_cid: int,
    parent_username: str,
    positions: Sequence[BrokerMirrorPosition],
    available_amount: Decimal = Decimal("2800.33"),
    initial_investment: Decimal = Decimal("20000"),
    deposit_summary: Decimal = Decimal("0"),
    withdrawal_summary: Decimal = Decimal("0"),
    closed_positions_net_profit: Decimal = Decimal("-110.34"),
) -> BrokerMirror:
    return BrokerMirror(
        mirror_id=mirror_id,
        parent_cid=parent_cid,
        parent_username=parent_username,
        initial_investment=initial_investment,
        deposit_summary=deposit_summary,
        withdrawal_summary=withdrawal_summary,
        available_amount=available_amount,
        closed_positions_net_profit=closed_positions_net_profit,
        stop_loss_percentage=None,
        stop_loss_amount=None,
        mirror_status_id=None,
        mirror_calculation_type=None,
        pending_for_closure=False,
        started_copy_date=_NOW,
        positions=tuple(positions),
        raw_payload={"mirrorID": mirror_id, "parentCID": parent_cid},
    )


def two_mirror_payload() -> BrokerPortfolio:
    """Canonical 2 mirrors × 3 positions each BrokerPortfolio fixture.

    Derived from the real etoro_portfolio_20260411T053000Z.json
    payload — trimmed for test readability, includes at least one
    non-USD position (GBP conversion rate 1.158) so the
    openConversionRate round-trip is exercised in every test that
    uses this fixture.
    """
    mirror_a = _make_mirror(
        mirror_id=15712187,
        parent_cid=111,
        parent_username="thomaspj",
        available_amount=Decimal("2800.33"),
        initial_investment=Decimal("20000"),
        deposit_summary=Decimal("0"),
        withdrawal_summary=Decimal("0"),
        closed_positions_net_profit=Decimal("-110.34"),
        positions=[
            _make_mirror_position(
                position_id=1001,
                instrument_id=42,
                units=Decimal("6.28927"),
                open_rate=Decimal("1207.4994"),
                open_conversion_rate=Decimal("0.01331"),  # JPY
                amount=Decimal("101.08"),
            ),
            _make_mirror_position(
                position_id=1002,
                instrument_id=43,
                units=Decimal("2.0"),
                open_rate=Decimal("150.00"),
                open_conversion_rate=Decimal("1.158"),  # GBP
                amount=Decimal("347.40"),
            ),
            _make_mirror_position(
                position_id=1003,
                instrument_id=44,
                units=Decimal("10.0"),
                open_rate=Decimal("100.00"),
                open_conversion_rate=Decimal("1.0"),  # USD
                amount=Decimal("1000.00"),
            ),
        ],
    )
    mirror_b = _make_mirror(
        mirror_id=15714660,
        parent_cid=222,
        parent_username="triangulacapital",
        available_amount=Decimal("1724.11"),
        initial_investment=Decimal("17280"),
        deposit_summary=Decimal("2251"),
        withdrawal_summary=Decimal("0"),
        closed_positions_net_profit=Decimal("-140.13"),
        positions=[
            _make_mirror_position(
                position_id=2001,
                instrument_id=52,
                units=Decimal("1.0"),
                open_rate=Decimal("500.00"),
                open_conversion_rate=Decimal("1.0"),
                amount=Decimal("500.00"),
            ),
            _make_mirror_position(
                position_id=2002,
                instrument_id=53,
                units=Decimal("3.0"),
                open_rate=Decimal("200.00"),
                open_conversion_rate=Decimal("1.0"),
                amount=Decimal("600.00"),
            ),
            _make_mirror_position(
                position_id=2003,
                instrument_id=54,
                units=Decimal("5.0"),
                open_rate=Decimal("80.00"),
                open_conversion_rate=Decimal("1.0"),
                amount=Decimal("400.00"),
            ),
        ],
    )
    return BrokerPortfolio(
        positions=(),
        available_cash=Decimal("0"),
        raw_payload={},
        mirrors=(mirror_a, mirror_b),
    )


def parse_failure_payload() -> list[dict[str, Any]]:
    """Raw `clientPortfolio.mirrors[]` list with one malformed
    nested position. Used by §8.3 to prove the sync aborts before
    eviction / soft-close when the parser raises.

    Returns a raw list (not BrokerPortfolio) because the test
    exercises the parse step itself — `_parse_mirrors_payload`
    must raise on this input.
    """
    return [
        {
            "mirrorID": 15712187,
            "parentCID": 111,
            "parentUsername": "thomaspj",
            "initialInvestment": "20000",
            "depositSummary": "0",
            "withdrawalSummary": "0",
            "availableAmount": "2800.33",
            "closedPositionsNetProfit": "-110.34",
            "stopLossPercentage": None,
            "stopLossAmount": None,
            "mirrorStatusID": None,
            "mirrorCalculationType": None,
            "pendingForClosure": False,
            "startedCopyDate": "2025-01-01T00:00:00Z",
            "positions": [
                {
                    "positionID": 1001,
                    "parentPositionID": 5001,
                    "instrumentID": 42,
                    "isBuy": True,
                    "units": "bogus",  # <-- non-numeric → DecimalException
                    "amount": "101.08",
                    "initialAmountInDollars": "101.08",
                    "openRate": "1207.4994",
                    "openConversionRate": "0.01331",
                    "openDateTime": "2026-04-10T00:00:00Z",
                    "takeProfitRate": None,
                    "stopLossRate": None,
                    "totalFees": "0",
                    "leverage": 1,
                },
            ],
        }
    ]


def two_mirror_seed_rows(conn: psycopg.Connection[Any]) -> None:
    """INSERT the two_mirror_payload mirrors directly into
    copy_traders / copy_mirrors / copy_mirror_positions so
    disappearance and re-copy tests can seed the DB before
    calling sync_portfolio with a *different* payload.

    Caller is responsible for commit/rollback. Safe to run only
    against ebull_test — callers must enforce this themselves
    before calling (see _assert_test_db in test modules).
    """
    payload = two_mirror_payload()
    with conn.cursor() as cur:
        for mirror in payload.mirrors:
            cur.execute(
                """
                INSERT INTO copy_traders (parent_cid, parent_username,
                                          first_seen_at, updated_at)
                VALUES (%(cid)s, %(username)s, %(now)s, %(now)s)
                ON CONFLICT (parent_cid) DO NOTHING
                """,
                {
                    "cid": mirror.parent_cid,
                    "username": mirror.parent_username,
                    "now": _NOW,
                },
            )
            cur.execute(
                """
                INSERT INTO copy_mirrors (
                    mirror_id, parent_cid, initial_investment,
                    deposit_summary, withdrawal_summary,
                    available_amount, closed_positions_net_profit,
                    stop_loss_percentage, stop_loss_amount,
                    mirror_status_id, mirror_calculation_type,
                    pending_for_closure, started_copy_date,
                    active, closed_at, raw_payload, updated_at
                ) VALUES (
                    %(mirror_id)s, %(parent_cid)s, %(initial_investment)s,
                    %(deposit_summary)s, %(withdrawal_summary)s,
                    %(available_amount)s, %(closed_positions_net_profit)s,
                    NULL, NULL, NULL, NULL, FALSE, %(started_copy_date)s,
                    TRUE, NULL, %(raw_payload)s::jsonb, %(now)s
                )
                """,
                {
                    "mirror_id": mirror.mirror_id,
                    "parent_cid": mirror.parent_cid,
                    "initial_investment": mirror.initial_investment,
                    "deposit_summary": mirror.deposit_summary,
                    "withdrawal_summary": mirror.withdrawal_summary,
                    "available_amount": mirror.available_amount,
                    "closed_positions_net_profit": mirror.closed_positions_net_profit,
                    "started_copy_date": mirror.started_copy_date,
                    "raw_payload": psycopg.types.json.Jsonb(mirror.raw_payload),
                    "now": _NOW,
                },
            )
            for pos in mirror.positions:
                cur.execute(
                    """
                    INSERT INTO copy_mirror_positions (
                        mirror_id, position_id, parent_position_id,
                        instrument_id, is_buy, units, amount,
                        initial_amount_in_dollars, open_rate,
                        open_conversion_rate, open_date_time,
                        take_profit_rate, stop_loss_rate,
                        total_fees, leverage, raw_payload, updated_at
                    ) VALUES (
                        %(mirror_id)s, %(position_id)s, %(parent_position_id)s,
                        %(instrument_id)s, %(is_buy)s, %(units)s, %(amount)s,
                        %(initial_amount)s, %(open_rate)s,
                        %(open_conversion_rate)s, %(open_date_time)s,
                        %(take_profit_rate)s, %(stop_loss_rate)s,
                        %(total_fees)s, %(leverage)s, %(raw_payload)s::jsonb,
                        %(now)s
                    )
                    """,
                    {
                        "mirror_id": mirror.mirror_id,
                        "position_id": pos.position_id,
                        "parent_position_id": pos.parent_position_id,
                        "instrument_id": pos.instrument_id,
                        "is_buy": pos.is_buy,
                        "units": pos.units,
                        "amount": pos.amount,
                        "initial_amount": pos.initial_amount_in_dollars,
                        "open_rate": pos.open_rate,
                        "open_conversion_rate": pos.open_conversion_rate,
                        "open_date_time": pos.open_date_time,
                        "take_profit_rate": pos.take_profit_rate,
                        "stop_loss_rate": pos.stop_loss_rate,
                        "total_fees": pos.total_fees,
                        "leverage": pos.leverage,
                        "raw_payload": psycopg.types.json.Jsonb(pos.raw_payload),
                        "now": _NOW,
                    },
                )
