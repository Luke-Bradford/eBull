"""DB-tier tests for #2408's record-holder evidence read.

``_named_record_holder`` is pure and revert-probed in
``scripts/probe_2408_record_holder_rep.py``. ``_read_record_holder_evidence`` is SQL, and
a pure-logic suite cannot probe a column choice or a WHERE clause — the prevention log
already records that failure shape (2026-08-08, #2411: "a pure suite CANNOT revert-probe
a SQL column"). These four cases pin the parts of the query that a plausible edit would
silently get wrong, each against a real row shape rather than a mocked cursor.

The row content is the ``LCID`` Form 4 ``0001104659-24-113592`` and the ``TACO`` Form 3
``0001829126-25-003075``, both read off the dev corpus.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg

from app.services.ownership_rollup import _read_record_holder_evidence

_ACC_4 = "0001104659-24-113592"
_ACC_3 = "0001829126-25-003075"
_BLOCK = Decimal("2227072750.0000")
_OTHER = Decimal("374717927.0000")


def _seed_filing(conn: psycopg.Connection[tuple], accession: str, document_type: str) -> None:
    """``insider_transactions.accession_number`` carries an FK to ``insider_filings``."""
    conn.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type) VALUES (%s, 1, %s) "
        "ON CONFLICT DO NOTHING",
        (accession, document_type),
    )


def _seed_form4_row(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    post_shares: Decimal,
    direct_indirect: str,
    nature: str | None,
    footnote_refs: str,
    row_num: int,
) -> None:
    conn.execute(
        """
        INSERT INTO insider_transactions
            (instrument_id, accession_number, txn_row_num, filer_name, filer_cik, txn_date,
             txn_code, shares, direct_indirect, is_derivative, post_transaction_shares,
             nature_of_ownership, footnote_refs)
        VALUES (1, %s, %s, 'Ayar Third Investment Co', '0001874832', %s,
                'P', 1, %s, FALSE, %s, %s, %s)
        """,
        (accession, row_num, date(2024, 10, 30), direct_indirect, post_shares, nature, footnote_refs),
    )


def _seed_footnote(conn: psycopg.Connection[tuple], accession: str, footnote_id: str, text: str) -> None:
    conn.execute(
        "INSERT INTO insider_transaction_footnotes (accession_number, footnote_id, footnote_text) VALUES (%s,%s,%s)",
        (accession, footnote_id, text),
    )


def test_evidence_is_keyed_on_the_rows_own_amount(
    ebull_test_conn: psycopg.Connection[tuple], seeded_instrument_id: int
) -> None:
    """One accession, two indirect lines at DIFFERENT amounts naming different holders.

    This is the shape that makes per-accession pooling wrong: a Battery Ventures filing on
    dev names five distinct record holders across one accession's footnotes. Keying on the
    amount is what stops another row's holder deciding this row's representative."""
    _seed_filing(ebull_test_conn, _ACC_4, "4")
    _seed_form4_row(
        ebull_test_conn,
        accession=_ACC_4,
        post_shares=_BLOCK,
        direct_indirect="I",
        nature="By Ayar Third Investment Company",
        footnote_refs="[]",
        row_num=1,
    )
    _seed_form4_row(
        ebull_test_conn,
        accession=_ACC_4,
        post_shares=_OTHER,
        direct_indirect="I",
        nature="By Public Investment Fund",
        footnote_refs="[]",
        row_num=2,
    )
    ebull_test_conn.commit()
    evidence = _read_record_holder_evidence(ebull_test_conn, [_ACC_4])
    assert evidence[(_ACC_4, _BLOCK)] == ("By Ayar Third Investment Company",)
    assert evidence[(_ACC_4, _OTHER)] == ("By Public Investment Fund",)


def test_direct_lines_contribute_nothing(ebull_test_conn: psycopg.Connection[tuple], seeded_instrument_id: int) -> None:
    """``natureOfOwnership`` exists only on the INDIRECT lines — measured on the dev
    corpus, 138,380 of 138,380 ``I`` rows carry it and 914,567 of 914,567 ``D`` rows do
    not. Reading D rows would add nothing but would widen the key set, so the filter is
    pinned rather than assumed."""
    _seed_filing(ebull_test_conn, _ACC_4, "4")
    _seed_form4_row(
        ebull_test_conn,
        accession=_ACC_4,
        post_shares=_BLOCK,
        direct_indirect="D",
        nature="should never be read",
        footnote_refs="[]",
        row_num=1,
    )
    ebull_test_conn.commit()
    assert _read_record_holder_evidence(ebull_test_conn, [_ACC_4]) == {}


def test_derivative_rows_contribute_nothing(
    ebull_test_conn: psycopg.Connection[tuple], seeded_instrument_id: int
) -> None:
    """Table II derivative rows carry their OWN ``nature_of_ownership`` and their own
    post-transaction amount, and the evidence key is only ``(accession, shares)``. A
    derivative holding that happens to match the equity block's count would therefore name
    a record holder for a DIFFERENT security.

    The rollup's insider rows are Table I equity, so the evidence must be. Found by Codex
    at checkpoint 2, not by the A/B: 11 of 1,425 evidence keys on the fold population drew
    on a derivative row, and every one had a non-derivative row at the same amount — so the
    pollution was additive and invisible in any count."""
    _seed_filing(ebull_test_conn, _ACC_4, "4")
    ebull_test_conn.execute(
        """
        INSERT INTO insider_transactions
            (instrument_id, accession_number, txn_row_num, filer_name, filer_cik, txn_date,
             txn_code, shares, direct_indirect, is_derivative, post_transaction_shares,
             nature_of_ownership, footnote_refs)
        VALUES (1, %s, 1, 'Ayar Third Investment Co', '0001874832', %s,
                'P', 1, 'I', TRUE, %s, 'By Public Investment Fund', '[]')
        """,
        (_ACC_4, date(2024, 10, 30), _BLOCK),
    )
    ebull_test_conn.commit()
    assert _read_record_holder_evidence(ebull_test_conn, [_ACC_4]) == {}


def test_only_the_footnotes_this_row_references_are_attached(
    ebull_test_conn: psycopg.Connection[tuple], seeded_instrument_id: int
) -> None:
    """``footnote_refs`` is the row→footnote link, and it is per-row. Attaching every
    footnote on the accession would reintroduce the pooling defect through the back door,
    because one filing's footnote set covers many holdings."""
    _seed_filing(ebull_test_conn, _ACC_4, "4")
    _seed_form4_row(
        ebull_test_conn,
        accession=_ACC_4,
        post_shares=_BLOCK,
        direct_indirect="I",
        nature="See footnote.",
        footnote_refs='[{"field": "sharesOwnedFollowingTransaction", "footnote_id": "F3"}]',
        row_num=1,
    )
    _seed_footnote(ebull_test_conn, _ACC_4, "F3", "Ayar Third Investment Company is the record holder.")
    _seed_footnote(ebull_test_conn, _ACC_4, "F9", "Unrelated: shares held by The Lee Family Trust.")
    ebull_test_conn.commit()
    texts = _read_record_holder_evidence(ebull_test_conn, [_ACC_4])[(_ACC_4, _BLOCK)]
    assert "Ayar Third Investment Company is the record holder." in texts
    assert not any("Lee Family Trust" in t for t in texts)


def test_form_3_holdings_are_read_from_their_own_table(
    ebull_test_conn: psycopg.Connection[tuple], seeded_instrument_id: int
) -> None:
    """A Form 3 lands in ``insider_initial_holdings``, whose amount column is ``shares``
    (there is no ``post_transaction_shares``) and which carries NO ``footnote_refs``.
    Reading only ``insider_transactions`` would silently halve the evidence — 30,149 of the
    corpus's indirect rows live here."""
    ebull_test_conn.execute(
        """
        INSERT INTO insider_initial_holdings
            (instrument_id, accession_number, row_num, filer_cik, filer_name, as_of_date,
             security_title, shares, is_derivative, direct_indirect, nature_of_ownership)
        VALUES (1, %s, 1, '0002033953', 'Berto Acquisition Sponsor LLC', %s,
                'Ordinary Shares', %s, FALSE, 'I', 'See footnote.')
        """,
        (_ACC_3, date(2025, 6, 30), _BLOCK),
    )
    ebull_test_conn.commit()
    assert _read_record_holder_evidence(ebull_test_conn, [_ACC_3])[(_ACC_3, _BLOCK)] == ("See footnote.",)
