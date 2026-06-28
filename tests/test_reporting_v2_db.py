"""DB-backed contract tests for the v2 report snapshot (#1596).

Pins the three things the pure-logic tests cannot:

1. **Valuation parity** (spec §3.3): the report cover's closing value
   equals `compute_portfolio_valuation().total_aum` on the same
   connection — by construction, since the builder calls the same
   helper as the dashboard endpoint, but this test guards against the
   paths re-forking.
2. **Flow-adjusted return end-to-end** (spec §3.4): a `capital_events`
   injection between two snapshots does NOT print as performance.
3. **The v2 fixture** consumed by child 2's `SnapshotV2` type test is
   backend-emitted, never handwritten: run with
   ``REPORT_FIXTURE_WRITE=1`` to regenerate
   ``tests/fixtures/report_snapshot_v2/*.json``; by default the test
   asserts the checked-in fixtures' key structure still matches what
   the builders emit.
"""

from __future__ import annotations

import json
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg

from app.services.reporting import (
    generate_monthly_report,
    generate_weekly_report,
    persist_report_snapshot,
)
from app.services.valuation import compute_portfolio_valuation
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "report_snapshot_v2"

_PERIOD_A = (date(2026, 5, 25), date(2026, 5, 31))
_PERIOD_B = (date(2026, 6, 1), date(2026, 6, 7))
_MONTH = (date(2026, 5, 1), date(2026, 5, 31))


def _seed_portfolio(conn: psycopg.Connection[tuple]) -> None:
    """One USD instrument, 10 units @ 100 cost, marked 110 via quote,
    1000 cash. total_aum = 10×110 + 1000 = 2100 (display ccy pinned
    to USD so no FX conversion enters the arithmetic)."""
    conn.execute("UPDATE runtime_config SET display_currency = 'USD' WHERE id = TRUE")
    # instruments.sector stores eToro's NUMERIC industry id (provider
    # contract); names live in etoro_stocks_industries (#1598). Seed the
    # production shape so the fixture exercises the id→name join.
    conn.execute(
        """
        INSERT INTO etoro_stocks_industries (industry_id, name)
        VALUES (42, 'Technology')
        ON CONFLICT (industry_id) DO UPDATE SET name = EXCLUDED.name
        """
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, sector, is_tradable)
        VALUES (789801, 'RPTV2A', 'Report Co', '4', 'USD', '42', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    conn.execute(
        """
        INSERT INTO positions (instrument_id, open_date, avg_cost, current_units,
                               cost_basis, realized_pnl, unrealized_pnl, source)
        VALUES (789801, '2026-05-01', 100, 10, 1000, 0, 100, 'ebull')
        ON CONFLICT (instrument_id) DO UPDATE
        SET current_units = EXCLUDED.current_units,
            cost_basis = EXCLUDED.cost_basis,
            realized_pnl = EXCLUDED.realized_pnl,
            unrealized_pnl = EXCLUDED.unrealized_pnl
        """
    )
    conn.execute(
        """
        INSERT INTO quotes (instrument_id, last, bid, ask, quoted_at)
        VALUES (789801, 110, 109.9, 110.1, NOW())
        ON CONFLICT (instrument_id) DO UPDATE SET last = EXCLUDED.last
        """
    )
    conn.execute(
        """
        INSERT INTO cash_ledger (event_time, event_type, amount, currency, note)
        VALUES ('2026-05-01T00:00:00Z', 'broker_sync', 1000, 'USD', 'test seed')
        """
    )
    conn.commit()


def test_weekly_v2_first_snapshot_parity(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_portfolio(conn)

    report = generate_weekly_report(conn, period_start=_PERIOD_A[0], period_end=_PERIOD_A[1])
    val = compute_portfolio_valuation(conn)

    assert report["schema_version"] == 2
    # Parity: cover closing value == the dashboard's valuation basis.
    assert Decimal(report["cover"]["closing_value"]) == Decimal(str(val.total_aum))
    assert Decimal(report["cover"]["closing_value"]) == Decimal("2100")
    assert report["cover"]["display_currency"] == "USD"
    # First v2 snapshot: no opening value, no return — never a fake 0%.
    assert report["cover"]["opening_value"] is None
    assert report["cover"]["period_return"] is None
    # Benchmark instrument not seeded → null-safe (spec §7).
    assert report["performance"]["benchmark"]["return_pct"] is None
    # Holdings row reflects the seeded mark.
    (holding,) = report["holdings"]
    assert holding["symbol"] == "RPTV2A"
    assert Decimal(holding["market_value"]) == Decimal("1100")
    assert Decimal(holding["since_entry_return_pct"]) == Decimal("0.1")
    assert holding["valuation_source"] == "quote"


def test_weekly_v2_flow_adjusted_return(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """A deposit between snapshots is NOT performance: value moves
    2100 → 3150 where 1000 is an injection on the first day of the
    period and 50 is genuine price P&L. Modified Dietz:
    (3150 − 2100 − 1000) / (2100 + 1000×1.0) = 50/3100."""
    conn = ebull_test_conn
    _seed_portfolio(conn)

    snap_a = generate_weekly_report(conn, period_start=_PERIOD_A[0], period_end=_PERIOD_A[1])
    persist_report_snapshot(
        conn,
        report_type="weekly",
        period_start=_PERIOD_A[0],
        period_end=_PERIOD_A[1],
        snapshot=snap_a,
    )
    conn.commit()

    # Period B: 1000 injection lands day 1 (w = 1.0), price 110 → 115
    # (+50 unrealised), cash +1000.
    conn.execute(
        """
        INSERT INTO capital_events (event_time, event_type, amount, currency, source, note)
        VALUES ('2026-06-01T09:00:00Z', 'injection', 1000, 'USD', 'operator', 'test')
        """
    )
    conn.execute(
        """
        INSERT INTO cash_ledger (event_time, event_type, amount, currency, note)
        VALUES ('2026-06-01T09:00:00Z', 'broker_sync', 1000, 'USD', 'mirror of injection')
        """
    )
    conn.execute("UPDATE quotes SET last = 115, bid = 114.9, ask = 115.1 WHERE instrument_id = 789801")
    conn.execute("UPDATE positions SET unrealized_pnl = 150 WHERE instrument_id = 789801")
    conn.commit()

    report = generate_weekly_report(conn, period_start=_PERIOD_B[0], period_end=_PERIOD_B[1])

    assert Decimal(report["cover"]["opening_value"]) == Decimal("2100")
    assert Decimal(report["cover"]["closing_value"]) == Decimal("3150")
    assert Decimal(report["cover"]["bridge"]["net_external_flows"]) == Decimal("1000")
    # (3150 − 2100 − 1000) / (2100 + 1000) = 50 / 3100 ≈ 0.016129 —
    # NOT (3150/2100 − 1) = 50%.
    assert Decimal(report["cover"]["period_return"]) == (Decimal("50") / Decimal("3100")).quantize(Decimal("0.000001"))
    # Bridge closes: unrealised moved +50, realised 0, residual 0
    # (the injection's cash twin is classified as a flow, not P&L).
    assert Decimal(report["cover"]["bridge"]["unrealized_delta"]) == Decimal("50")
    assert Decimal(report["cover"]["bridge"]["realized_delta"]) == Decimal("0")
    assert Decimal(report["cover"]["bridge"]["broker_adjustments_residual"]) == Decimal("0")


def test_benchmark_resolved_by_symbol(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """Benchmark closes resolve by symbol at generation; period return
    uses the last close strictly before period_start as baseline."""
    conn = ebull_test_conn
    _seed_portfolio(conn)
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (789802, 'SPX500', 'S&P 500 Index', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    conn.execute(
        """
        INSERT INTO price_daily (instrument_id, price_date, close)
        VALUES (789802, '2026-05-29', 5000), (789802, '2026-06-05', 5100)
        ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
        """
    )
    conn.commit()

    report = generate_weekly_report(conn, period_start=_PERIOD_B[0], period_end=_PERIOD_B[1])
    benchmark = report["performance"]["benchmark"]
    assert benchmark["symbol"] == "SPX500"
    assert Decimal(benchmark["close_start"]) == Decimal("5000")
    assert Decimal(benchmark["close_end"]) == Decimal("5100")
    # 5100/5000 − 1 = 0.02
    assert Decimal(benchmark["return_pct"]) == Decimal("0.02")


def test_benchmark_stale_no_in_window_close_returns_null(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When the latest benchmark close predates the whole period (stale
    data — #1818), close_end collapses onto the pre-period baseline and
    close_end/close_start - 1 == 0. That 0 is a fabricated "flat", not a
    real return: return_pct must be null so the FE shows "—" (#1817)."""
    conn = ebull_test_conn
    _seed_portfolio(conn)
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (789802, 'SPX500', 'S&P 500 Index', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    # Both closes land BEFORE _PERIOD_B (06-01 .. 06-07): no close inside
    # the window. close_start (strict < 06-01) and close_end (<= 06-07)
    # both resolve to the 05-29 row.
    conn.execute(
        """
        INSERT INTO price_daily (instrument_id, price_date, close)
        VALUES (789802, '2026-05-22', 5000), (789802, '2026-05-29', 5100)
        ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
        """
    )
    conn.commit()

    report = generate_weekly_report(conn, period_start=_PERIOD_B[0], period_end=_PERIOD_B[1])
    benchmark = report["performance"]["benchmark"]
    assert benchmark["symbol"] == "SPX500"
    # Closes still reported for transparency (both the stale 05-29 row)…
    assert Decimal(benchmark["close_start"]) == Decimal("5100")
    assert Decimal(benchmark["close_end"]) == Decimal("5100")
    # …but the return is null, NOT a spurious 0.00%.
    assert benchmark["return_pct"] is None
    # Cover + excess null-out too (no fabricated excess vs a flat line).
    assert report["cover"]["benchmark_return"] is None
    assert report["cover"]["excess_return"] is None


def test_benchmark_close_exactly_on_period_start_computes(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Boundary: a benchmark close landing exactly on period_start IS
    inside the window — the `end_row.date >= period_start` guard is
    inclusive, so the return computes (locks against a `>` regression)."""
    conn = ebull_test_conn
    _seed_portfolio(conn)
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (789802, 'SPX500', 'S&P 500 Index', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    # baseline before the window, plus a close ON period_start (06-01).
    conn.execute(
        """
        INSERT INTO price_daily (instrument_id, price_date, close)
        VALUES (789802, '2026-05-29', 5000), (789802, %(start)s, 5100)
        ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
        """,
        {"start": _PERIOD_B[0]},
    )
    conn.commit()

    report = generate_weekly_report(conn, period_start=_PERIOD_B[0], period_end=_PERIOD_B[1])
    benchmark = report["performance"]["benchmark"]
    assert Decimal(benchmark["close_start"]) == Decimal("5000")
    assert Decimal(benchmark["close_end"]) == Decimal("5100")
    assert Decimal(benchmark["return_pct"]) == Decimal("0.02")


def test_v2_fixture_is_backend_emitted(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """The SnapshotV2 fixture for child 2's FE type test is generated
    by the real builders. ``REPORT_FIXTURE_WRITE=1`` regenerates the
    files; the default run asserts the checked-in fixtures still carry
    exactly the keys the builders emit (drift = this test fails before
    the FE types rot)."""
    conn = ebull_test_conn
    _seed_portfolio(conn)

    weekly = generate_weekly_report(conn, period_start=_PERIOD_A[0], period_end=_PERIOD_A[1])
    monthly = generate_monthly_report(conn, period_start=_MONTH[0], period_end=_MONTH[1])

    if os.environ.get("REPORT_FIXTURE_WRITE") == "1":
        _FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
        (_FIXTURE_DIR / "weekly.json").write_text(json.dumps(weekly, indent=2, sort_keys=True) + "\n")
        (_FIXTURE_DIR / "monthly.json").write_text(json.dumps(monthly, indent=2, sort_keys=True) + "\n")

    # The id→name join must actually resolve (#1598): a broken
    # etoro_stocks_industries lookup would silently emit sector=None /
    # group everything as "Unknown" while the key-structure checks pass.
    assert monthly["holdings"][0]["sector"] == "Technology"
    assert "Technology" in monthly["risk"]["sector_exposure"]

    for name, generated in (("weekly", weekly), ("monthly", monthly)):
        fixture = json.loads((_FIXTURE_DIR / f"{name}.json").read_text())
        assert set(fixture.keys()) == set(generated.keys()), name
        assert set(fixture["cover"].keys()) == set(generated["cover"].keys()), name
        assert set(fixture["performance"].keys()) == set(generated["performance"].keys()), name
        assert set(fixture["cover"]["bridge"].keys()) == set(generated["cover"]["bridge"].keys()), name
