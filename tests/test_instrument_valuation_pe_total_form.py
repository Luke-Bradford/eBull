"""#3903: ``instrument_valuation.pe_ratio`` is market cap / TTM net income (sql/506).

Frozen synthetic cases (spec "Acceptance" item 5). Every instrument here is on the complete-TTM arm: four adjacent
normalized quarters in ``financial_periods`` (``financial_periods_ttm`` is a view over them) and one #3841 PIT step
at the session (cap = 150 × 1e9 = 1.5e11 unless refused). The per-share EPS is seeded across a 25-for-1 split, as
BKNG's window was, so sql/505's ratio is about 1.2 while the total form is 37.5. ``net_income`` is NUMERIC(20,4),
which refuses ±Infinity on insert, so ``NaN`` is the one non-finite value a window can carry. The full-population
wiring is ``scripts/measure_3903_acceptance.py`` section 1.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows
import pytest

from tests.fixtures.pit_market_cap import seed_pit_step

SESSION = date(2026, 10, 16)
Q_ENDS = (date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31), date(2026, 6, 30))
Q_TYPES = ("Q3", "Q4", "Q1", "Q2")
#: Pre-split quarters then post-split ones (25-for-1): Σ = 125.2, so sql/505's P/E is 150 / 125.2.
EPS = (Decimal("60"), Decimal("62"), Decimal("1.6"), Decimal("1.6"))
SHARES = (Decimal("16e6"), Decimal("16e6"), Decimal("4e8"), Decimal("4e8"))
SQL_505_BODY = Path(__file__).resolve().parents[1] / "sql" / "rollback" / "506_instrument_valuation_pe_total_form.sql"

PUBLISHED, ZERO, NEGATIVE, NI_NULL, NAN, REFUSED, FPI, DUAL, ADS = range(301, 310)
NI_QUARTER: dict[int, list[str | None]] = {
    PUBLISHED: ["1e9"] * 4,
    ZERO: ["1e9", "-1e9", "1e9", "-1e9"],
    NEGATIVE: ["-1e9"] * 4,
    NI_NULL: ["1e9", None, "1e9", "1e9"],
    NAN: ["1e9", "NaN", "1e9", "1e9"],
    REFUSED: ["1e9"] * 4,
    FPI: ["1e9"] * 4,
    DUAL: ["1e9"] * 4,
    ADS: ["1e9"] * 4,
}


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    conn = ebull_test_conn
    for iid, quarters in NI_QUARTER.items():
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, f"T{iid}", f"Total Form {iid}"),
        )
        for i, (end, qt, ni) in enumerate(zip(Q_ENDS, Q_TYPES, quarters, strict=True)):
            conn.execute(
                "INSERT INTO financial_periods (instrument_id, period_end_date, period_type, fiscal_year, source, "
                " source_ref, reported_currency, revenue, net_income, eps_diluted, shares_diluted, "
                " shareholders_equity, superseded_at, normalization_status) "
                "VALUES (%s, %s, %s, %s, 'test', %s, 'USD', 1e10, %s::numeric, %s, %s, %s, NULL, 'normalized')",
                (iid, end, qt, end.year, f"t{iid}{i}", ni, EPS[i], SHARES[i], Decimal("3e10") if i == 3 else None),
            )
        if iid == REFUSED:
            seed_pit_step(conn, iid, session=SESSION, close=150, shares=1e9, refusal="no_left_anchor")
        else:
            seed_pit_step(conn, iid, session=SESSION, close=150, shares=1e9)
    conn.execute("INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (%s, 3, 'fpi')", (FPI,))
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', '0009993903', TRUE)",
        (DUAL,),
    )
    conn.execute(
        "INSERT INTO instrument_class_shares_outstanding "
        "(instrument_id, period_end, shares, class_member, source_cik, source_adsh, "
        " source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version, ingested_at) "
        "VALUES (%s, '2026-06-30', 1e9, 'CommonClassA', '0009993903', '0000000000-00-000000', "
        "'10-Q', '2026Q2', now(), 'curated', 1, now())",
        (DUAL,),
    )
    conn.execute("INSERT INTO ads_ratio (instrument_id, ratio, source_form) VALUES (%s, 2, 'F-6')", (ADS,))
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute("SELECT * FROM instrument_valuation WHERE instrument_id = %s", (iid,)).fetchone()
    assert row is not None
    return row


def _apply_sql_505(conn: psycopg.Connection[Any]) -> None:
    body = re.sub(r"^(BEGIN|COMMIT);$", "", SQL_505_BODY.read_text(), flags=re.M)
    conn.execute(body)  # type: ignore[arg-type]  # a reviewed file, not user input


@pytest.mark.db
def test_published_on_the_total_form(_seed: psycopg.Connection[Any]) -> None:
    row = _val(_seed, PUBLISHED)
    assert row["is_complete_ttm"] is True
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("1.5e11")
    assert row["net_income_ttm"] == Decimal("4e9")
    assert row["pe_ratio"] == Decimal("1.5e11") / Decimal("4e9")


@pytest.mark.db
def test_split_window_that_sql_505_published_near_one(_seed: psycopg.Connection[Any]) -> None:
    # The BKNG shape: sql/505 divides the price by a per-share sum across a 25-for-1 split.
    _apply_sql_505(_seed)
    assert float(_val(_seed, PUBLISHED)["pe_ratio"]) == pytest.approx(150 / 125.2)
    _seed.rollback()
    assert float(_val(_seed, PUBLISHED)["pe_ratio"]) == pytest.approx(37.5)


@pytest.mark.db
@pytest.mark.parametrize("iid", [ZERO, NEGATIVE, NI_NULL, NAN])
def test_non_positive_missing_or_non_finite_earnings_publish_no_pe(_seed: psycopg.Connection[Any], iid: int) -> None:
    row = _val(_seed, iid)
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("1.5e11")
    assert row["pe_ratio"] is None


@pytest.mark.db
@pytest.mark.parametrize(
    ("iid", "reason", "pe_505_positive"),
    [(REFUSED, "no_left_anchor", True), (DUAL, "dual_class", True), (ADS, "ads_ratio", True), (FPI, "fpi_adr", False)],
)
def test_refused_cap_refuses_pe_with_its_reason(
    _seed: psycopg.Connection[Any], iid: int, reason: str, pe_505_positive: bool
) -> None:
    row = _val(_seed, iid)
    assert row["market_cap_reason"] == reason
    assert row["pe_ratio"] is None
    # The control: sql/505 published a per-share P/E for each, except FPI, which it suppressed.
    _apply_sql_505(_seed)
    before = _val(_seed, iid)["pe_ratio"]
    _seed.rollback()
    assert (before is not None and before > 0) is pe_505_positive


@pytest.mark.db
def test_rollback_restores_sql_505(_seed: psycopg.Connection[Any]) -> None:
    columns_506 = [
        r[0]
        for r in _seed.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'instrument_valuation' "
            "ORDER BY ordinal_position"
        ).fetchall()
    ]
    _apply_sql_505(_seed)
    columns_505 = [
        r[0]
        for r in _seed.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'instrument_valuation' "
            "ORDER BY ordinal_position"
        ).fetchall()
    ]
    assert columns_505 == columns_506
    restored = {iid: _val(_seed, iid)["pe_ratio"] for iid in (PUBLISHED, FPI, NEGATIVE)}
    _seed.rollback()
    assert float(restored[PUBLISHED]) == pytest.approx(150 / 125.2)
    assert restored[FPI] is None  # sql/505's FPI suppression
    assert restored[NEGATIVE] is not None  # the per-share sum is positive beside a loss
