"""#3616 slice 3d-4a part B1 against the test database: ``resolve_tax_run``'s snapshot contract and ``GET /tax/view``.

Spec: docs/specs/metrics/2026-10-15-3616-slice3d4a-tax-year-view.md (Designs 1 and 6). Auto-marked ``db``. The run is
slice 3d-3c-3's SPY fixture (``tests.test_tax_matching_funds_db``): one fund disposal under the thirty-day rule, seven
acquisition balances, character ``oig_or_capital_unresolved``.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import IsolationLevel

from app.api.tax import _share_disposal, get_tax_view
from app.services.tax_matching import EffectiveMatching, TaxRunResolution, require_snapshot, resolve_tax_run
from app.services.tax_view_reader import ShareDisposalDetail, read_tax_run
from app.services.tax_year_view import ShareDisposal
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_tax_fund_determination_refresh_db import dets, fund_at, funds  # noqa: F401 - fixtures
from tests.test_tax_matching_funds_db import (  # noqa: F401 - `clock`, `determined` are fixtures
    SPY,
    _building,
    _run,
    _security,
    _spy_generation,
    clock,
    determined,
)

D = Decimal


def _conn(level: IsolationLevel | None = IsolationLevel.REPEATABLE_READ) -> psycopg.Connection[Any]:
    conn = psycopg.connect(test_database_url())
    if level is not None:
        conn.isolation_level = level
    return conn


def _view(tax_year: str) -> dict[str, Any]:
    with psycopg.connect(test_database_url()) as conn:
        return json.loads(get_tax_view(tax_year=tax_year, conn=conn).model_dump_json())


def test_resolution_requires_an_active_repeatable_read_or_serializable_transaction(funds: None) -> None:  # noqa: F811
    with _conn() as conn:
        with pytest.raises(RuntimeError, match="none is active"):
            require_snapshot(conn)
    with _conn(IsolationLevel.READ_COMMITTED) as conn, conn.transaction():
        with pytest.raises(RuntimeError, match="not read committed"):
            resolve_tax_run(conn)
    for level in (IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE):
        with _conn(level) as conn, conn.transaction():
            assert resolve_tax_run(conn).reason == "no_effective_run"


def test_resolution_neither_commits_nor_changes_isolation(funds: None) -> None:  # noqa: F811
    with _conn() as conn:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS t3d4a (x int)")
        conn.commit()
        conn.execute("INSERT INTO t3d4a VALUES (1)")  # opens the caller's REPEATABLE READ transaction
        resolve_tax_run(conn)
        assert conn.isolation_level == IsolationLevel.REPEATABLE_READ
        row = conn.execute("SELECT current_setting('transaction_isolation')").fetchone()
        assert row is not None and row[0] == "repeatable read"
        conn.rollback()
        row = conn.execute("SELECT count(*) FROM t3d4a").fetchone()
        assert row is not None and row[0] == 0


def test_a_run_published_after_the_snapshot_began_is_not_seen(determined: int) -> None:  # noqa: F811
    _spy_generation()
    with _conn() as conn, conn.transaction():
        assert resolve_tax_run(conn).reason == "no_effective_run"
        assert _run().outcome == "published"  # another connection publishes
        assert resolve_tax_run(conn).reason == "no_effective_run"
    with _conn() as conn, conn.transaction():
        assert resolve_tax_run(conn).run is not None


def test_a_non_demo_scope_is_unavailable(funds: None) -> None:  # noqa: F811
    with psycopg.connect(test_database_url(), autocommit=True) as conn:
        conn.execute("UPDATE trade_ledger_scope SET environment = 'real'")
    with _conn() as conn, conn.transaction():
        resolution = resolve_tax_run(conn)
    assert (resolution.environment, resolution.reason) == ("real", "ledger_scope_not_demo")


def test_unavailable_view_has_unknown_years_and_no_figure(funds: None) -> None:  # noqa: F811
    view = _view("2026/27")
    assert (view["status"], view["unavailable_reason"], view["year"]) == ("unavailable", "no_effective_run", None)
    assert view["years"] and all(y["has_disposal"] is None for y in view["years"])
    assert view["disposals"] == view["holdings"] == view["withheld"] == []


def test_view_of_the_spy_run(determined: int, fund_at: datetime, dets: Path) -> None:  # noqa: F811
    _spy_generation()
    report = _run()
    assert report.outcome == "published"
    view = _view("2026/27")
    assert (view["status"], view["matching_run_id"]) == ("available", report.matching_run_id)
    assert (view["matching_policy_id"], view["character_policy_id"]) == ("identification_v2", "tax_character_v2")
    assert view["labels"] == ["demo", "account_only_hypothetical"]
    assert {"tax_year": "2026/27", "has_disposal": True} in view["years"]
    [disposal] = view["disposals"]
    assert (disposal["kind"], disposal["gain_character"], disposal["member_of"]) == (
        "fund",
        "oig_or_capital_unresolved",
        "capital_reading_only",
    )
    assert disposal["offshore_income_gain_gbp"] is None and disposal["chargeable_gain_gbp"] is None
    assert disposal["uk_date"] == "2026-09-23" and len(disposal["identifications"]) == 1
    year = view["year"]
    basic = D(disposal["basic_gain_gbp"])
    assert D(year["unresolved_gains"]) == basic > 0
    assert D(year["income_reading"]["gains"]) == 0 and year["capital_reading"] is not None
    assert D(year["capital_reading"]["gains"]) == basic
    assert year["figure_class"] == "provisional" and year["complete"] is False
    codes = {f["code"] for f in year["flags"]}
    assert {"fund_income_not_computed", "income_tax_not_computed", "dividends_not_computed", "account_only"} <= codes
    assert set(view["assumption_definitions"]) == set(year["assumption_ids"])
    [holding] = [h for h in view["holdings"] if h["holding_basis"] == "relevant_securities"]
    assert len(holding["fund_balances"]) == 7 and holding["figure_class"] == "provisional"
    # Every amount is a plain decimal string.
    assert re.search(r"\d[eE][+-]?\d", json.dumps(view)) is None


def test_withheld_securities_keep_their_symbols_and_run_level_reasons(determined: int) -> None:  # noqa: F811
    """An unestablished identity stores no ``tax_security_id`` on its instrument row, and an ``unplaceable``
    dependency has none at all; the reader still names the security and its reasons (Codex checkpoint 2)."""
    generation = _spy_generation()
    with _conn() as conn:
        run = _building(conn, generation, "identification_v2", "tax_character_v2")
        _security(conn, run, SPY, "withheld", None)
        conn.execute(
            "INSERT INTO tax_matching_instruments (matching_run_id, instrument_id, symbol, component_id, "
            "tax_security_id) VALUES (%s, %s, 'SPY', %s, NULL)",
            (run, SPY, SPY),
        )
        conn.execute(
            "INSERT INTO tax_matching_dependencies (matching_run_id, seq, tax_security_id, reason, subject) "
            "VALUES (%s, 1, NULL, 'unplaceable', 'order:1'), (%s, 2, %s, 'unresolved_subject', 'order:2')",
            (run, run, SPY),
        )
        data = read_tax_run(conn, TaxRunResolution("demo", EffectiveMatching(run, generation), None))
        conn.rollback()
    assert data is not None
    [withheld] = data.rows.withheld
    assert withheld.symbols == ("SPY",)
    assert withheld.reasons == ("unplaceable", "unresolved_subject")


def test_a_zero_gain_share_disposal_is_in_neither_reading() -> None:
    row = ShareDisposal(1, date(2026, 6, 1), "2026/27", "computed", D("0"), (), ())
    detail = ShareDisposalDetail(row, ("lot",), D(1), D(5), D(5), ())
    assert _share_disposal(detail, []).member_of == "neither"
    gain = ShareDisposalDetail(
        ShareDisposal(1, date(2026, 6, 1), "2026/27", "computed", D("1"), (), ()), ("lot",), D(1), D(6), D(5), ()
    )
    assert _share_disposal(gain, []).member_of == "both_readings"
