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


def mirror_aum_fixture(conn: psycopg.Connection[Any]) -> None:
    """Seed the load-bearing DB state for §8.4 AUM identity, §8.5
    guard integration, and §8.6 per-call-site delta tests.

    Seeds:
      1. Two mirrors in copy_mirrors: one active (#8001),
         one closed (#8002, active=FALSE, closed_at=_NOW).
      2. copy_mirror_positions: one long each, on distinct
         instrument_ids. The active mirror's position is on
         _GUARD_INSTRUMENT_ID so §8.5's sector-numerator
         resolution lands a valid row.
      3. quotes rows for both mirror positions (last prices set
         such that the MTM delta is non-zero but hand-computable).
      4. An instruments row for _GUARD_INSTRUMENT_ID with
         sector=_GUARD_INSTRUMENT_SECTOR.
      5. A scores row with model_version='v1-balanced', rank=1,
         total_score=0.5, instrument_id=_GUARD_INSTRUMENT_ID —
         required by run_portfolio_review's _load_ranked_scores
         WHERE rank IS NOT NULL clause (portfolio.py:203).
      6. Empty positions and cash_ledger — leaves the
         eBull-owned contribution at 0 so tests that call
         _load_mirror_equity get exactly the mirror_equity term.

    Numbers are chosen to be hand-computable:
      active_available = 1000.00
      active_amount    =  500.00  (cost basis)
      active_units     =   10.0
      active_open_rate =   50.00
      active_conv_rate =    1.00
      active_quote_last=   55.00  (delta = +5/unit)
      active_mtm_delta = 1 * 10.0 * (55.00 - 50.00) * 1.00 = 50.00
      active_equity    = 1000.00 + 500.00 + 50.00 = 1550.00

      closed_available =  200.00  (but WHERE m.active filters)
      closed_amount    =  100.00  (but WHERE m.active filters)
      Expected _load_mirror_equity(conn) = 1550.00

    Caller owns commit / rollback. Safe against ebull_test only
    — caller enforces via _assert_test_db.
    """
    with conn.cursor() as cur:
        # Parent trader rows (required by copy_mirrors.parent_cid FK)
        cur.execute(
            """
            INSERT INTO copy_traders (parent_cid, parent_username,
                                      first_seen_at, updated_at)
            VALUES
                (801, 'aum_fixture_active', %(now)s, %(now)s),
                (802, 'aum_fixture_closed', %(now)s, %(now)s)
            ON CONFLICT (parent_cid) DO NOTHING
            """,
            {"now": _NOW},
        )
        # Active mirror
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
                8001, 801, 5000.00, 0, 0,
                1000.00, 0,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                TRUE, NULL, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        # Closed mirror
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
                8002, 802, 5000.00, 0, 0,
                200.00, 0,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                FALSE, %(now)s, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        # Instruments row for guard test instrument.
        # NOTE: `instruments` columns per sql/001_init.sql:1-13 are
        # (instrument_id, symbol, company_name, exchange, currency,
        # sector, industry, country, is_tradable, first_seen_at,
        # last_seen_at). first_seen_at and last_seen_at both DEFAULT
        # NOW(). There is NO `tier`, NO `created_at`, NO `updated_at`.
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (%(iid)s, 'AUMTEST', 'AUM Fixture Instrument',
                    %(sector)s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            {
                "iid": _GUARD_INSTRUMENT_ID,
                "sector": _GUARD_INSTRUMENT_SECTOR,
            },
        )
        # A second instrument for the "sector numerator unchanged" §8.5 scenario
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (990002, 'AUMTEST2', 'AUM Fixture Instrument 2',
                    'healthcare', TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
        )
        # Active mirror's nested position — on guard test instrument
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
                8001, 80011, 10001,
                %(iid)s, TRUE, 10.0, 500.00,
                500.00, 50.00,
                1.00, %(now)s,
                NULL, NULL, 0, 1, '{}'::jsonb, %(now)s
            )
            """,
            {"iid": _GUARD_INSTRUMENT_ID, "now": _NOW},
        )
        # Closed mirror's nested position — would contribute 100 if not filtered
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
                8002, 80021, 10002,
                990002, TRUE, 5.0, 100.00,
                100.00, 20.00,
                1.00, %(now)s,
                NULL, NULL, 0, 1, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        # Quote for active mirror position — last=55.00 → delta=+5/unit
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask,
                                quoted_at)
            VALUES (%(iid)s, 55.00, 54.95, 55.05, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"iid": _GUARD_INSTRUMENT_ID, "now": _NOW},
        )
        # Quote for closed mirror's position (cosmetic — filter masks it)
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask,
                                quoted_at)
            VALUES (990002, 22.00, 21.95, 22.05, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"now": _NOW},
        )
        # Scores row so run_portfolio_review does NOT early-return
        # at portfolio.py:733 — required by §8.6 Test 2.
        cur.execute(
            """
            INSERT INTO scores (instrument_id, model_version,
                                total_score, rank, scored_at)
            VALUES (%(iid)s, 'v1-balanced', 0.5, 1, %(now)s)
            """,
            {"iid": _GUARD_INSTRUMENT_ID, "now": _NOW},
        )


def no_quote_mirror_fixture(conn: psycopg.Connection[Any]) -> None:
    """Seed the empirically-reconciled mirror 15712187 shape
    with no matching `quotes` rows. Used by §8.4's cost-basis
    fallback identity test.

    Expected _load_mirror_equity(conn) = 2800.33 + 50.00 + 17039.33
                                       = 19889.66

    Caller owns commit / rollback. ebull_test only.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO copy_traders (parent_cid, parent_username,
                                      first_seen_at, updated_at)
            VALUES (901, 'no_quote_fixture', %(now)s, %(now)s)
            ON CONFLICT (parent_cid) DO NOTHING
            """,
            {"now": _NOW},
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
                9001, 901, 20000.00, 0, 0,
                2800.33, -110.34,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                TRUE, NULL, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO copy_mirror_positions (
                mirror_id, position_id, parent_position_id,
                instrument_id, is_buy, units, amount,
                initial_amount_in_dollars, open_rate,
                open_conversion_rate, open_date_time,
                take_profit_rate, stop_loss_rate,
                total_fees, leverage, raw_payload, updated_at
            ) VALUES
                (9001, 90011, 90101, 4301, TRUE, 1.0, 50.00, 50.00,
                 50.00, 1.00, %(now)s,
                 NULL, NULL, 0, 1, '{}'::jsonb, %(now)s),
                (9001, 90012, 90102, 4302, TRUE, 1.0, 17039.33, 17039.33,
                 17039.33, 1.00, %(now)s,
                 NULL, NULL, 0, 1, '{}'::jsonb, %(now)s)
            """,
            {"now": _NOW},
        )
        # No quotes rows — the §3.4 query falls back to open_rate,
        # so each MTM delta term is zero and only `amount` contributes.


def mtm_delta_mirror_fixture(
    conn: psycopg.Connection[Any],
    *,
    is_buy: bool = True,
    quote_last: Decimal = Decimal("1400.0"),
) -> Decimal:
    """Seed one long (or short) position with a non-zero MTM
    delta and a matching quote, so §8.4 can assert FX-aware
    delta accounting. Returns the expected mirror equity as a
    Decimal so the test can assert an exact value.

    Computation (is_buy=True, quote_last=1400.0):
        delta_per_unit    = 1400.0 - 1207.4994 = 192.5006
        usd_delta_per_pos = +1 * 6.28927 * 192.5006 * 0.01331
                          ≈ +16.1122
        equity            = available + amount + usd_delta
                          = 2800.33  + 101.08 + 16.1122
                          ≈ 2917.5222

    Caller owns commit / rollback. ebull_test only.
    """
    open_rate = Decimal("1207.4994")
    units = Decimal("6.28927")
    conv_rate = Decimal("0.01331")
    amount = Decimal("101.08")
    available = Decimal("2800.33")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name)
            VALUES (4201, 'MTM_FIXTURE', 'MTM Fixture Instrument')
            ON CONFLICT (instrument_id) DO NOTHING
            """
        )
        cur.execute(
            """
            INSERT INTO copy_traders (parent_cid, parent_username,
                                      first_seen_at, updated_at)
            VALUES (911, 'mtm_fixture', %(now)s, %(now)s)
            ON CONFLICT (parent_cid) DO NOTHING
            """,
            {"now": _NOW},
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
                9101, 911, 20000.00, 0, 0,
                %(available)s, 0,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                TRUE, NULL, '{}'::jsonb, %(now)s
            )
            """,
            {"available": available, "now": _NOW},
        )
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
                9101, 91011, 91111,
                4201, %(is_buy)s, %(units)s, %(amount)s,
                %(amount)s, %(open_rate)s,
                %(conv_rate)s, %(now)s,
                NULL, NULL, 0, 1, '{}'::jsonb, %(now)s
            )
            """,
            {
                "is_buy": is_buy,
                "units": units,
                "amount": amount,
                "open_rate": open_rate,
                "conv_rate": conv_rate,
                "now": _NOW,
            },
        )
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask,
                                quoted_at)
            VALUES (4201, %(last)s, %(last)s, %(last)s, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"last": quote_last, "now": _NOW},
        )

    sign = Decimal("1") if is_buy else Decimal("-1")
    usd_delta = sign * units * (quote_last - open_rate) * conv_rate
    return available + amount + usd_delta
