"""DB-backed tests for 13F-NT supersession in the ownership rollup (#1639).

The supersession predicate lives in SQL (the ``NOT EXISTS`` clause in
``_collect_canonical_holders_from_current`` + the lateral join in
``_read_notice_suppressions``), so it is exercised where it lives — against a
real Postgres — rather than as a pure function. Seeds
``ownership_institutions_current`` + ``institutional_filer_13f_notices``
directly (this is a read-path test) plus a minimal shares-outstanding denominator
so the rollup does not short-circuit to ``no_data``.

Instrument-id range 1_639_xxx is reserved for these scenarios.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services import ownership_rollup
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


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


def _seed_outstanding(
    conn: psycopg.Connection[tuple], *, iid: int, shares: str, period_end: date = date(2026, 3, 31)
) -> None:
    """Seed the rollup denominator via ``financial_facts_raw`` DEI (the row the
    ``instrument_share_count_latest`` view reads)."""
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            form_type, filed_date, accession_number, fiscal_year, fiscal_period
        ) VALUES (%s, 'dei', 'EntityCommonStockSharesOutstanding', 'shares', %s, %s,
                  '10-Q', %s, %s, %s, 'Q4')
        ON CONFLICT DO NOTHING
        """,
        (iid, period_end, Decimal(shares), period_end, f"OUT-{iid}-{period_end}", period_end.year),
    )


def _seed_institution_current(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    filer_cik: str,
    filer_name: str,
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
        ) VALUES (%s, %s, %s, 'economic', '13f', %s, %s, %s, %s, %s, 'EQUITY')
        ON CONFLICT (instrument_id, filer_cik, ownership_nature, exposure_kind)
        DO UPDATE SET shares = EXCLUDED.shares, period_end = EXCLUDED.period_end
        """,
        (
            iid,
            filer_cik,
            filer_name,
            f"{accession}#1",
            accession,
            datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
            period_end,
            Decimal(shares),
        ),
    )


def _seed_notice(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    accession: str,
    period_end: date,
    form: str = "13F-NT",
    filed_at: datetime | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO institutional_filer_13f_notices (
            filer_cik, accession_number, period_end, form, filed_at
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (accession_number) DO UPDATE SET
            period_end = EXCLUDED.period_end, form = EXCLUDED.form
        """,
        (filer_cik, accession, period_end, form, filed_at or datetime(2026, 5, 8, tzinfo=UTC)),
    )


def _institution_filer_ciks(rollup: ownership_rollup.OwnershipRollup) -> set[str | None]:
    out: set[str | None] = set()
    for slc in rollup.slices:
        if slc.category in ("institutions", "etfs"):
            out.update(h.filer_cik for h in slc.holders)
    return out


def _institution_total(rollup: ownership_rollup.OwnershipRollup) -> Decimal:
    total = Decimal(0)
    for slc in rollup.slices:
        if slc.category in ("institutions", "etfs"):
            total += slc.total_shares
    return total


# ---------------------------------------------------------------------------
# Headline scenario — Vanguard-style parent-reorg double-count
# ---------------------------------------------------------------------------


def test_later_nt_supersedes_parent_and_emits_correction(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    iid = 1_639_001
    parent, sub = "0000102909", "0002100119"
    _seed_instrument(conn, iid=iid, symbol="VGTEST")
    _seed_outstanding(conn, iid=iid, shares="1000")
    # Stale parent HR (Q4'25) + post-reorg sub-entity HR (Q1'26): same ~book,
    # one quarter apart. Pre-#1639 the rollup summed both = 1200 (2x).
    _seed_institution_current(
        conn,
        iid=iid,
        filer_cik=parent,
        filer_name="VANGUARD GROUP INC",
        shares="600",
        period_end=date(2025, 12, 31),
        accession="0001029090-26-000031",
    )
    _seed_institution_current(
        conn,
        iid=iid,
        filer_cik=sub,
        filer_name="VANGUARD CAPITAL MGMT LLC",
        shares="600",
        period_end=date(2026, 3, 31),
        accession="0002100119-26-000100",
    )
    # Parent files a 13F-NT for the LATER quarter (Q1'26) → its stale HR is dead.
    _seed_notice(
        conn,
        filer_cik=parent,
        accession="0001029090-26-002707",
        period_end=date(2026, 3, 31),
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="VGTEST", instrument_id=iid)

    # Parent excluded; only the sub-entity's 600 remains (not 1200).
    assert _institution_filer_ciks(rollup) == {sub}
    assert _institution_total(rollup) == Decimal(600)

    # Structured correction telemetry (#1647 down-payment).
    assert rollup.corrections_applied == (
        ownership_rollup.CorrectionApplied(
            kind="suppressed_by_13f_nt",
            filer_cik=parent,
            filer_name="VANGUARD GROUP INC",
            shares_removed=Decimal(600),
            superseded_period=date(2025, 12, 31),
            winning_nt_period=date(2026, 3, 31),
            winning_nt_accession="0001029090-26-002707",
        ),
    )

    # CSV audit memo row, mirroring the #1640 ``__dropped:`` pattern.
    csv = ownership_rollup.build_rollup_csv(rollup)
    assert "__suppressed_by_13f_nt:0000102909__" in csv
    assert "0001029090-26-002707" in csv


# ---------------------------------------------------------------------------
# Predicate cases — does a single filer survive?
# ---------------------------------------------------------------------------

_FILER = "0000102909"
_HR_ACC = "0001029090-26-000031"
_HR_PERIOD = date(2026, 3, 31)


@pytest.mark.parametrize(
    ("iid", "case", "notice", "expect_kept"),
    [
        # No notice at all → kept.
        (1_639_101, "no_notice", None, True),
        # NT for a LATER quarter → superseded (dropped).
        (1_639_102, "later_nt", (_FILER, date(2026, 6, 30), "13F-NT"), False),
        # NT for an OLDER quarter (resume: filer went notice-only then came back)
        # → newer HR survives.
        (1_639_103, "older_nt_resume", (_FILER, date(2025, 12, 31), "13F-NT"), True),
        # NT/A amending an OLD quarter, filed after a resumed HR → period axis
        # keeps the newer HR (the load-bearing Codex HIGH #1 case).
        (1_639_104, "nt_a_old_quarter", (_FILER, date(2025, 9, 30), "13F-NT/A"), True),
        # Same-quarter NT (contradictory) → strict ``>`` keeps the HR.
        (1_639_105, "same_quarter", (_FILER, date(2026, 3, 31), "13F-NT"), True),
        # NT from a DIFFERENT filer → does not cross to this filer's HR.
        (1_639_106, "other_filer_nt", ("0000789019", date(2026, 6, 30), "13F-NT"), True),
    ],
)
def test_supersession_predicate(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    iid: int,
    case: str,
    notice: tuple[str, date, str] | None,
    expect_kept: bool,
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=iid, symbol=f"NT{case[:6].upper()}")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_institution_current(
        conn,
        iid=iid,
        filer_cik=_FILER,
        filer_name="ACME CAPITAL",
        shares="500",
        period_end=_HR_PERIOD,
        accession=_HR_ACC,
    )
    if notice is not None:
        nt_cik, nt_period, nt_form = notice
        _seed_notice(
            conn,
            filer_cik=nt_cik,
            accession=f"{nt_cik}-26-009999",
            period_end=nt_period,
            form=nt_form,
        )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="NT", instrument_id=iid)

    kept = _FILER in _institution_filer_ciks(rollup)
    assert kept is expect_kept, f"case={case}: filer kept={kept}, expected {expect_kept}"
    # A kept filer means no correction; a dropped one means exactly one.
    assert (len(rollup.corrections_applied) == 0) is expect_kept
