"""#3146 — the insider ``_current`` projection must pick the filing's LAST Table I line.

Before this, ``refresh_insiders_current``'s ``DISTINCT ON`` ended its ``ORDER BY`` with
``source_document_id ASC``, so when one filing reported several Table I lines for the same
``(holder_identity_key, ownership_nature)`` on the same date — every earlier key tying — the
balance that became current was decided by string order on a DERA surrogate key.

Source rule + the measurement that establishes the SK carries document order:
``docs/proposals/ownership/2026-09-17-3146-insider-line-order.md``.

⚠ Every fixture here is built so the input **disagrees with the old rule**. A tie-break test
whose fixture the defect also satisfies pins nothing (prevention-log: "a fixture the defect
satisfies pins nothing" — the #2240 S-2 probe reported NOT CAUGHT twice for exactly this).
Each case therefore asserts a value that lexical ordering would NOT have produced, except the
two cases whose whole point is that they must NOT move.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo

_PERIOD_END = date(2025, 12, 11)
_FILED_AT = datetime(2025, 12, 12, tzinfo=UTC)


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _instrument(conn: psycopg.Connection[Any], instrument_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (instrument_id, f"T3146-{instrument_id}", f"Test 3146 Co {instrument_id}"),
        )
    conn.commit()


def _observe(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    doc_id: str,
    shares: str,
    holder_cik: str = "0001535326",
    holder_name: str = "TEST HOLDING SAS",
    nature: str = "direct",
) -> None:
    """One insider observation. ``source_accession`` is the doc id's prefix, as the writers do."""
    oo.record_insider_observation(
        conn,
        instrument_id=instrument_id,
        holder_cik=holder_cik,
        holder_name=holder_name,
        ownership_nature=nature,  # type: ignore[arg-type]
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


def _winner(conn: psycopg.Connection[Any], instrument_id: int) -> tuple[str, Decimal]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT source_document_id, shares FROM ownership_insiders_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1, f"expected exactly one current row, got {rows}"
    return rows[0][0], rows[0][1]


@pytest.mark.db
def test_last_table_i_line_wins_within_one_filing(conn: psycopg.Connection[Any]) -> None:
    """The IPAR shape: an option exercise then a same-day sale, both indirect.

    Lexical order picks ``:NDT:8823454`` — the PRE-sale balance the filing supersedes on its
    own page. Form 4 Instruction 4(a)(i) wants the balance following the reported
    transaction(s), which is the later line.
    """
    iid = 931460
    _instrument(conn, iid)
    accn = "0001753926-25-001883"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:8823454", shares="6871064")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:8823455", shares="6846064")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    doc, shares = _winner(conn, iid)
    assert doc == f"{accn}:NDT:8823455"
    assert shares == Decimal("6846064")


@pytest.mark.db
def test_the_last_line_wins_against_ascending_document_id(conn: psycopg.Connection[Any]) -> None:
    """Equal-width SKs, where the old rule's ascending doc id picks the FIRST line.

    ⚠ This case pins last-line-wins, NOT the ``::numeric`` cast — with equal digit counts,
    text and numeric ordering agree. The cast is pinned by the boundary case below. (The first
    version of this test claimed to be the digit-length boundary and was not; caught in review
    on PR #3148.)
    """
    iid = 931461
    _instrument(conn, iid)
    accn = "0001753926-25-000002"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:1000", shares="111")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:9999", shares="222")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    doc, shares = _winner(conn, iid)
    assert doc == f"{accn}:NDT:9999"
    assert shares == Decimal("222")


@pytest.mark.db
def test_surrogate_key_is_compared_numerically_not_as_text(conn: psycopg.Connection[Any]) -> None:
    """The digit-length boundary that makes the ``::numeric`` cast load-bearing.

    ``:NDT:999`` vs ``:NDT:1000``: descending TEXT order puts ``'999'`` first (``'9' > '1'``),
    descending NUMERIC order puts ``1000`` first. Dropping the cast therefore changes the
    answer here and nowhere in the equal-width cases — which is exactly why this fixture exists
    as well as, not instead of, the one above.
    """
    iid = 931467
    _instrument(conn, iid)
    accn = "0001753926-25-000008"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:999", shares="111")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:1000", shares="222")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    doc, shares = _winner(conn, iid)
    assert doc == f"{accn}:NDT:1000"
    assert shares == Decimal("222")


@pytest.mark.db
def test_ndh_holdings_rows_do_not_move(conn: psycopg.Connection[Any]) -> None:
    """``:NDH:`` is a Form 3 / holdings snapshot, not a sequence — no ordinal was established.

    Fixture discriminates: lexical picks ``:NDH:100`` (shares 100); had the tie-break leaked
    past ``:NDT:``, numeric-descending would pick ``:NDH:200`` (shares 200).
    """
    iid = 931462
    _instrument(conn, iid)
    accn = "0001753926-25-000003"
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDH:100", shares="100")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDH:200", shares="200")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    doc, shares = _winner(conn, iid)
    assert doc == f"{accn}:NDH:100"
    assert shares == Decimal("100")


@pytest.mark.db
def test_cross_accession_winner_does_not_move(conn: psycopg.Connection[Any]) -> None:
    """The change is within-filing only.

    Fixture discriminates: the LOWER accession carries the LOWER surrogate key, so a tie-break
    that compared SKs across filings would pick the other accession's row.
    """
    iid = 931463
    _instrument(conn, iid)
    _observe(conn, instrument_id=iid, doc_id="0001753926-25-000004:NDT:100", shares="100")
    _observe(conn, instrument_id=iid, doc_id="0001753926-25-000005:NDT:900", shares="200")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    doc, shares = _winner(conn, iid)
    assert doc == "0001753926-25-000004:NDT:100"
    assert shares == Decimal("100")


@pytest.mark.db
def test_xml_row_still_wins_a_mixed_provenance_tie(conn: psycopg.Connection[Any]) -> None:
    """An XML row has no ``:NDT:`` segment, so its ordering key is NULL.

    ``DESC`` is NULLS FIRST in Postgres, which preserves the pre-#3146 behaviour. ``NULLS
    LAST`` would hand the group to the DERA row, which #1805's de-collision then drops for
    sharing the accession and CIK — deleting the key outright via the MERGE's NOT MATCHED BY
    SOURCE prune.
    """
    iid = 931464
    _instrument(conn, iid)
    accn = "0001753926-25-000006"
    _observe(conn, instrument_id=iid, doc_id=accn, shares="500")
    _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:900", shares="100")
    oo.refresh_insiders_current(conn, instrument_id=iid)

    doc, shares = _winner(conn, iid)
    assert doc == accn
    assert shares == Decimal("500")


@pytest.mark.db
def test_single_and_batch_projections_agree(conn: psycopg.Connection[Any]) -> None:
    """The #2269 divergence check: the two forms must not disagree on tied rows.

    A per-row reader and its bulk twin get different plans, so a non-unique ORDER BY can pick
    a different arbitrary winner in each. Both carry ``_INSIDER_WINNER_ORDER_TAIL``.
    """
    single_iid, batch_iid = 931465, 931466
    for iid in (single_iid, batch_iid):
        _instrument(conn, iid)
        accn = "0001753926-25-000007"
        _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:10", shares="300")
        _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:20", shares="400")
        _observe(conn, instrument_id=iid, doc_id=f"{accn}:NDT:30", shares="500")

    oo.refresh_insiders_current(conn, instrument_id=single_iid)
    oo.refresh_insiders_current_batch(conn, instrument_ids=[batch_iid])

    single_doc, single_shares = _winner(conn, single_iid)
    batch_doc, batch_shares = _winner(conn, batch_iid)
    assert (single_doc, single_shares) == (batch_doc, batch_shares)
    # …and both must be the numerically-greatest SK. Lexical order would pick ":NDT:10".
    assert single_doc.endswith(":NDT:30")
    assert single_shares == Decimal("500")
