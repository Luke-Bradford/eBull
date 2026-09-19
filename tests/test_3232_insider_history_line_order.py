"""#3232 — the insider history CHART must pick the filing's last Table I line.

``ownership_history._insiders_history`` ended its ``DISTINCT ON`` ``ORDER BY`` with
``source_document_id ASC``. When one filing reports several Table I lines for the same
``(period_end, ownership_nature)``, every prior key ties, so the share value the operator sees
was decided by string order on a DERA surrogate key — which picks the filing's FIRST line.

Form 4 General Instruction 4(a)(i) wants the balance *"following the reported
transaction(s)"*, i.e. the LAST line. #3146 fixed exactly this in
``refresh_insiders_current`` and missed this reader; the fix shares
``_INSIDER_WINNER_ORDER_TAIL`` between them.

⚠ Every fixture here is built so the input **disagrees with the old rule** — a tie-break test
whose fixture the defect also satisfies pins nothing (prevention-log: "a fixture the defect
satisfies pins nothing"). The exception is the ``::numeric`` boundary case, which is labelled
as pinning the cast rather than the defect, because its live population is measured at ZERO
(``scripts/audit_3232_insider_history_line_order --gain``).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_history as oh
from app.services import ownership_observations as oo

_PERIOD_END = date(2025, 11, 14)
_FILED_AT = datetime(2025, 11, 17, tzinfo=UTC)
_HOLDER_CIK = "0001535399"


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _instrument(conn: psycopg.Connection[Any], instrument_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (instrument_id, f"T3232-{instrument_id}", f"Test 3232 Co {instrument_id}"),
        )
    conn.commit()


def _observe(conn: psycopg.Connection[Any], *, instrument_id: int, doc_id: str, shares: str) -> None:
    """One Form 4 Table I line, as ``sec_insider_dataset_ingest`` writes them."""
    oo.record_insider_observation(
        conn,
        instrument_id=instrument_id,
        holder_cik=_HOLDER_CIK,
        holder_name="TEST INSIDER 3232",
        ownership_nature="direct",  # type: ignore[arg-type]
        source="form4",
        source_document_id=doc_id,
        source_accession=doc_id.split(":")[0],
        source_field=None,
        source_url=None,
        filed_at=_FILED_AT,
        period_start=None,
        period_end=_PERIOD_END,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
    )


def _chart_shares(conn: psycopg.Connection[Any], instrument_id: int) -> Decimal | None:
    points = oh.get_ownership_history(
        conn, instrument_id=instrument_id, category="insiders", holder_id=_HOLDER_CIK
    )
    assert len(points) == 1, f"expected exactly one chart point, got {points}"
    return points[0].shares


@pytest.mark.db
def test_chart_point_is_the_filings_last_table_i_line(conn: psycopg.Connection[Any]) -> None:
    """Equal-width SKs, so text and numeric order agree and only DIRECTION is under test.

    The old tail (``source_document_id ASC``) picks ``:NDT:1000`` — 111, the balance BEFORE the
    filing's later line. Instruction 4(a)(i) wants 222. This is the whole live defect:
    693,492 of 693,492 moved buckets move to a LATER line.
    """
    iid = 932320
    _instrument(conn, iid)
    accn = "0001753926-25-003232"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:1000", shares="111")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:9999", shares="222")

    assert _chart_shares(conn, iid) == Decimal("222")


@pytest.mark.db
def test_chart_compares_the_surrogate_key_numerically_not_as_text(conn: psycopg.Connection[Any]) -> None:
    """The digit-length boundary that makes the ``::numeric`` cast load-bearing.

    Descending TEXT order puts ``'999'`` first (``'9' > '1'``); descending NUMERIC order puts
    ``1000`` first. ⚠ Measured live population of differing-width SKs in one filing: **0**. So
    this pins the cast against a future edit, and is NOT evidence of the shipped defect.
    """
    iid = 932321
    _instrument(conn, iid)
    accn = "0001753926-25-003233"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:999", shares="333")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:1000", shares="444")

    assert _chart_shares(conn, iid) == Decimal("444")


@pytest.mark.db
def test_chart_and_projection_name_the_same_line(conn: psycopg.Connection[Any]) -> None:
    """The invariant #3232 exists to restore: one filing, one answer.

    Before the fix these two readers used different tie-breaks over the same rows, so the
    ownership CARD and the ownership CHART could report different balances for the same
    holder on the same date. Sharing ``_INSIDER_WINNER_ORDER_TAIL`` is what makes this hold.
    """
    iid = 932322
    _instrument(conn, iid)
    accn = "0001753926-25-003234"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:1000", shares="111")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:9999", shares="222")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT shares FROM ownership_insiders_current WHERE instrument_id = %s",
            (iid,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1, f"expected exactly one current row, got {rows}"

    assert _chart_shares(conn, iid) == rows[0][0]
