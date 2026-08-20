"""DB-backed tests for 13F-HR supersession in the ownership rollup (#2229).

Sibling of ``test_ownership_13f_nt_supersession.py`` (#1639), which covers the Notice
case. This covers the ORDINARY case: a filer that simply stopped holding the security.

``ownership_institutions_current`` keeps the latest row per (instrument, filer, nature,
exposure) and its MERGE deletes only ``NOT MATCHED BY SOURCE``, but 13F reports an exit
by OMISSION rather than a zero row — so a filer that sold out kept contributing its last
reported position forever. Form 13F Special Instruction 5b makes a "13F HOLDINGS REPORT"
a COMPLETE statement of the Manager's Section 13(f) holdings, so a later report from the
same filer that omits this security is affirmative evidence the position is closed.

The predicate lives in SQL (the second ``NOT (...)`` clause in
``_collect_canonical_holders_from_current`` + the lateral join in
``_read_hr_supersessions``), so it is exercised where it lives — against a real Postgres.

Instrument-id range 2_229_xxx is reserved for these scenarios.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services import ownership_rollup
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_FILER = "0000102909"
_OTHER_FILER = "0000789019"
_HR_PERIOD = date(2025, 12, 31)
_HR_ACC = "0000102909-26-000101"
_CUSIP = "037833100"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_outstanding(conn: psycopg.Connection[tuple], *, iid: int, shares: str) -> None:
    period_end = date(2026, 3, 31)
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            form_type, filed_date, accession_number, fiscal_year, fiscal_period
        ) VALUES (%s, 'dei', 'EntityCommonStockSharesOutstanding', 'shares', %s, %s,
                  '10-Q', %s, %s, %s, 'Q4')
        ON CONFLICT DO NOTHING
        """,
        (iid, period_end, Decimal(shares), period_end, f"OUT-{iid}", period_end.year),
    )


def _seed_institution_current(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    filer_cik: str,
    shares: str,
    period_end: date,
    accession: str,
) -> None:
    conn.execute(
        """
        INSERT INTO ownership_institutions_current (
            instrument_id, filer_cik, filer_name, ownership_nature, source,
            source_document_id, source_accession, filed_at, period_end, shares,
            exposure_kind
        ) VALUES (%s, %s, 'ACME CAPITAL', 'economic', '13f', %s, %s, %s, %s, %s, 'EQUITY')
        ON CONFLICT (instrument_id, filer_cik, ownership_nature, exposure_kind)
        DO UPDATE SET shares = EXCLUDED.shares, period_end = EXCLUDED.period_end
        """,
        (
            iid,
            filer_cik,
            f"{accession}#1",
            accession,
            datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
            period_end,
            Decimal(shares),
        ),
    )


def _seed_later_filing(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    period_of_report: date,
    other_iid: int,
) -> None:
    """Seed evidence that ``filer_cik`` filed a 13F covering ``period_of_report`` —
    against a DIFFERENT instrument, which is exactly the real shape: the filing exists
    and does not mention the instrument under test.

    The evidence lives in ``ownership_institutions_current`` itself, not in the
    ``institutional_holdings`` landing table: the two agree on 9,067 filers and
    ``_current`` is newer on 148 (it is fed by the continuous manifest parser as well
    as the quarterly bulk dataset), so the predicate reads the fresher source and
    stays self-consistent on one table."""
    _seed_institution_current(
        conn,
        iid=other_iid,
        filer_cik=filer_cik,
        shares="1000",
        period_end=period_of_report,
        accession=f"{filer_cik}-26-{period_of_report.month:02d}{period_of_report.day:02d}99",
    )


def _seed_unresolved_cusip(conn: psycopg.Connection[tuple], *, iid: int, last_period_end: date) -> None:
    """Bind a CUSIP to ``iid`` AND record it as unresolved at ``last_period_end`` —
    the case where absence from a later filing is OUR resolution failure, not an exit."""
    # ``is_primary`` listed explicitly (#1173): a SEC-sourced CUSIP is primary, and the
    # OpenFIGI fallback deliberately writes FALSE so the SEC row wins in
    # ``_load_cusip_map``. Never rely on the column DEFAULT here.
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, TRUE) ON CONFLICT DO NOTHING
        """,
        (iid, _CUSIP),
    )
    # ``unresolved_13f_cusips`` has no PK — only PARTIAL unique indexes
    # (``(cusip) WHERE source IS NULL`` and ``(cusip, source) WHERE source IS NOT NULL``),
    # so a bare ``ON CONFLICT (cusip)`` raises InvalidColumnReference. The test DB is
    # fresh per test, so a plain INSERT is correct here.
    conn.execute(
        """
        INSERT INTO unresolved_13f_cusips (cusip, name_of_issuer, observation_count, last_period_end)
        VALUES (%s, 'ACME', 1, %s)
        """,
        (_CUSIP, last_period_end),
    )


def _seed_notice(conn: psycopg.Connection[tuple], *, filer_cik: str, period_end: date) -> None:
    conn.execute(
        """
        INSERT INTO institutional_filer_13f_notices (
            filer_cik, accession_number, period_end, form, filed_at
        ) VALUES (%s, %s, %s, '13F-NT', %s)
        ON CONFLICT (accession_number) DO UPDATE SET period_end = EXCLUDED.period_end
        """,
        (filer_cik, f"{filer_cik}-26-00NT", period_end, datetime(2026, 5, 8, tzinfo=UTC)),
    )


def _institution_filer_ciks(rollup: ownership_rollup.OwnershipRollup) -> set[str | None]:
    out: set[str | None] = set()
    for slc in rollup.slices:
        if slc.category in ("institutions", "etfs"):
            out.update(h.filer_cik for h in slc.holders)
    return out


# ---------------------------------------------------------------------------
# Headline scenario
# ---------------------------------------------------------------------------


def test_later_hr_supersedes_exited_position_and_emits_correction(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A filer holding 400 of 1,000 shares at 2025-12-31 that filed again for
    2026-03-31 without this security has exited. Its stale row must leave the
    institutions wedge, and the removal must be explained in ``corrections_applied``
    — an operator seeing the wedge shrink needs the reason (#1639's contract)."""
    conn = ebull_test_conn
    iid, other = 2_229_001, 2_229_901
    _seed_instrument(conn, iid=iid, symbol="HREXIT")
    _seed_instrument(conn, iid=other, symbol="HROTHER")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_institution_current(conn, iid=iid, filer_cik=_FILER, shares="400", period_end=_HR_PERIOD, accession=_HR_ACC)
    _seed_later_filing(conn, filer_cik=_FILER, period_of_report=date(2026, 3, 31), other_iid=other)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="HREXIT", instrument_id=iid)

    assert _FILER not in _institution_filer_ciks(rollup)
    corrections = [c for c in rollup.corrections_applied if c.kind == "superseded_by_later_13f_hr"]
    assert len(corrections) == 1
    correction = corrections[0]
    assert correction.filer_cik == _FILER
    assert correction.shares_removed == Decimal(400)
    assert correction.superseded_period == _HR_PERIOD
    assert correction.source_channel == "13f"
    assert "2026-03-31" in correction.detail
    # ⚠ The WINNER is the later filing that proves the exit, not the stale row being
    # removed. Codex ckpt-2 caught this pointing at ``c.source_accession`` — an operator
    # auditing the correction would have been sent to the losing accession while it was
    # labelled the winning source.
    assert correction.winning_accession == f"{_FILER}-26-033199"
    assert correction.winning_accession != _HR_ACC
    # The shares return to the public residual rather than vanishing.
    assert rollup.residual.pct_outstanding == Decimal(1)
    assert rollup.residual.oversubscribed is False


def test_nt_and_hr_both_applicable_reports_once_under_nt(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A row superseded by BOTH mechanisms is reported once, under the Notice — the
    stronger evidence (the filer declared it holds nothing reportable at all). Without
    this the operator sees one removal counted twice."""
    conn = ebull_test_conn
    iid, other = 2_229_002, 2_229_902
    _seed_instrument(conn, iid=iid, symbol="HRBOTH")
    _seed_instrument(conn, iid=other, symbol="HRBOTHO")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_institution_current(conn, iid=iid, filer_cik=_FILER, shares="400", period_end=_HR_PERIOD, accession=_HR_ACC)
    _seed_later_filing(conn, filer_cik=_FILER, period_of_report=date(2026, 3, 31), other_iid=other)
    _seed_notice(conn, filer_cik=_FILER, period_end=date(2026, 3, 31))

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="HRBOTH", instrument_id=iid)

    assert _FILER not in _institution_filer_ciks(rollup)
    kinds = [c.kind for c in rollup.corrections_applied]
    assert kinds == ["suppressed_by_13f_nt"], kinds


# ---------------------------------------------------------------------------
# Predicate table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("iid", "case", "later_period", "unresolved_at", "notice_at", "expect_kept"),
    [
        # No later filing at all — the filer may still hold. Kept.
        (2_229_101, "no_later_filing", None, None, None, True),
        # Later filing omitting this security → exited. Dropped.
        (2_229_102, "later_filing", date(2026, 3, 31), None, None, False),
        # Filing for the SAME period as the row — that IS the row's own filing, so it
        # is not evidence of anything. Strict ``>`` keeps it.
        (2_229_103, "same_period", _HR_PERIOD, None, None, True),
        # Only EARLIER filings → says nothing about later holdings. Kept.
        (2_229_104, "earlier_only", date(2025, 9, 30), None, None, True),
        # ⚠ The guard: this instrument's CUSIP failed to resolve on a filing at or after
        # the row's period, so absence is OUR ingest gap, not an exit. Kept.
        (2_229_105, "unresolved_cusip_guard", date(2026, 3, 31), date(2026, 3, 31), None, True),
        # Guard must not over-apply: an unresolved sighting STRICTLY BEFORE the row's
        # period cannot explain absence from a filing after it. Dropped.
        (2_229_106, "unresolved_too_old", date(2026, 3, 31), date(2025, 6, 30), None, False),
    ],
)
def test_hr_supersession_predicate(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    iid: int,
    case: str,
    later_period: date | None,
    unresolved_at: date | None,
    notice_at: date | None,
    expect_kept: bool,
) -> None:
    conn = ebull_test_conn
    other = iid + 800
    symbol = f"HR{iid % 1000}"
    _seed_instrument(conn, iid=iid, symbol=symbol)
    _seed_instrument(conn, iid=other, symbol=f"HO{iid % 1000}")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_institution_current(conn, iid=iid, filer_cik=_FILER, shares="400", period_end=_HR_PERIOD, accession=_HR_ACC)
    if later_period is not None:
        _seed_later_filing(conn, filer_cik=_FILER, period_of_report=later_period, other_iid=other)
    if unresolved_at is not None:
        _seed_unresolved_cusip(conn, iid=iid, last_period_end=unresolved_at)
    if notice_at is not None:
        _seed_notice(conn, filer_cik=_FILER, period_end=notice_at)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol=symbol, instrument_id=iid)

    kept = _FILER in _institution_filer_ciks(rollup)
    assert kept is expect_kept, f"case={case}: filer kept={kept}, expected {expect_kept}"
    assert (len(rollup.corrections_applied) == 0) is expect_kept


def test_another_filers_later_filing_does_not_cross(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A DIFFERENT filer's later report says nothing about this filer's holdings.
    The join must be on ``filer_cik``, not on the instrument alone."""
    conn = ebull_test_conn
    iid, other = 2_229_003, 2_229_903
    _seed_instrument(conn, iid=iid, symbol="HRCROSS")
    _seed_instrument(conn, iid=other, symbol="HRCROSSO")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_institution_current(conn, iid=iid, filer_cik=_FILER, shares="400", period_end=_HR_PERIOD, accession=_HR_ACC)
    _seed_later_filing(conn, filer_cik=_OTHER_FILER, period_of_report=date(2026, 3, 31), other_iid=other)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="HRCROSS", instrument_id=iid)

    assert _FILER in _institution_filer_ciks(rollup)
    assert rollup.corrections_applied == ()
