"""#3227 — an original Form 4 stops being a candidate once a 4/A restates every one of its lines.

``INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER`` (all three readers) and ``_INSIDER_SUPERSEDED_KEY_DROP``
(both ``_current`` refreshers). The base fixture is RWT ``0001104659-26-107867`` / ``-109010``: the
original reports one line ``I`` "by Trust", the 4/A restates it ``D``, and an older April filing
left 579 on the ``indirect`` key. Each refusal case changes exactly one thing the rule checks, so
the guard it pins is the only reason the original survives.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo
from app.services.ownership_history import _insiders_history

_IID = 932_275
_OWNER = "0001453409"
_ORIGINAL = "0001104659-26-107867"
_AMENDMENT = "0001104659-26-109010"
_APRIL = "0001453409-26-000005"
_PERIOD = date(2026, 9, 15)


@dataclass(frozen=True)
class _Case:
    amendment_lines: tuple[tuple[str, date], ...] = (("Common Stock", _PERIOD),)
    amendment_owner: str = _OWNER
    amendment_nature: str = "direct"
    amendment_shares: str = "679"
    amendment_filed: datetime = datetime(2026, 9, 18, tzinfo=UTC)
    holder_on_amendment: bool = True
    amendment_txn_shares: int = 100
    second_original_same_day: bool = False
    older_period: date = date(2026, 4, 14)


_BASE = _Case()


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _observation(
    conn: psycopg.Connection[Any], *, accession: str, nature: str, shares: str, period: date, filed: datetime
) -> None:
    oo.record_insider_observation(
        conn,
        instrument_id=_IID,
        holder_cik=_OWNER,
        holder_name="Abate Christopher J",
        ownership_nature=nature,  # type: ignore[arg-type]
        source="form4",
        source_document_id=accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=filed,
        period_start=None,
        period_end=period,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
    )


def _filing(
    cur: psycopg.Cursor[Any],
    accession: str,
    doc_type: str,
    owner: str,
    filed: datetime,
    lines: tuple[tuple[str, date], ...],
    original_date: date | None = None,
    txn_shares: int = 100,
) -> None:
    cur.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type, period_of_report,"
        " date_of_original_submission) VALUES (%s, %s, %s, %s, %s)",
        (accession, _IID, doc_type, _PERIOD, original_date),
    )
    cur.execute(
        "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, %s)",
        (accession, owner, f"Filer {owner}"),
    )
    cur.execute(
        "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id,"
        " instrument_id, filed_at, ingest_status) VALUES (%s, '0000930236', %s, 'sec_form4', 'issuer', %s, %s,"
        " %s, 'parsed')",
        (accession, doc_type, str(_IID), _IID, filed),
    )
    for row_num, (title, txn_date) in enumerate(lines):
        cur.execute(
            "INSERT INTO insider_transactions (accession_number, txn_row_num, instrument_id, filer_cik,"
            " filer_name, security_title, txn_date, txn_code, shares, post_transaction_shares, is_derivative)"
            " VALUES (%s, %s, %s, %s, 'Abate Christopher J', %s, %s, 'P', %s, 679, FALSE)",
            (accession, row_num, _IID, owner, title, txn_date, txn_shares),
        )


def _seed(conn: psycopg.Connection[Any], case: _Case) -> None:
    original_filed = datetime(2026, 9, 15, 21, tzinfo=UTC)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'T3227A', 'T', TRUE)",
            (_IID,),
        )
        _filing(cur, _ORIGINAL, "4", _OWNER, original_filed, (("Common Stock", _PERIOD),))
        _filing(
            cur,
            _AMENDMENT,
            "4/A",
            case.amendment_owner,
            case.amendment_filed,
            case.amendment_lines,
            original_date=original_filed.date(),
            txn_shares=case.amendment_txn_shares,
        )
        if case.second_original_same_day:
            _filing(cur, "0001104659-26-107868", "4", _OWNER, original_filed, (("Common Stock", _PERIOD),))
    april_filed = datetime(2026, 4, 15, tzinfo=UTC)
    _observation(conn, accession=_APRIL, nature="indirect", shares="579", period=case.older_period, filed=april_filed)
    _observation(conn, accession=_ORIGINAL, nature="indirect", shares="679", period=_PERIOD, filed=original_filed)
    if case.holder_on_amendment:
        _observation(
            conn,
            accession=_AMENDMENT,
            nature=case.amendment_nature,
            shares=case.amendment_shares,
            period=_PERIOD,
            filed=case.amendment_filed,
        )
    conn.commit()


def _current(conn: psycopg.Connection[Any]) -> dict[str, tuple[str, Decimal]]:
    rows = conn.execute(
        "SELECT ownership_nature, source_accession, shares FROM ownership_insiders_current WHERE instrument_id = %s",
        (_IID,),
    ).fetchall()
    return {r[0]: (r[1], Decimal(r[2])) for r in rows}


_KEPT_ORIGINAL = {"indirect": (_ORIGINAL, Decimal(679)), "direct": (_AMENDMENT, Decimal(679))}

_CASES = [
    pytest.param(_BASE, {"direct": (_AMENDMENT, Decimal(679))}, id="full_restatement_supersedes_and_drops_the_key"),
    pytest.param(
        replace(_BASE, amendment_lines=(("Common Stock", _PERIOD), ("Common Stock", _PERIOD))),
        _KEPT_ORIGINAL,
        id="an_added_line_is_partial_9a_and_refuses",
    ),
    pytest.param(
        replace(_BASE, amendment_lines=(("Common Stock", date(2026, 9, 14)),)),
        _KEPT_ORIGINAL,
        id="a_corrected_transaction_date_refuses",
    ),
    pytest.param(replace(_BASE, amendment_owner="0009999999"), _KEPT_ORIGINAL, id="no_shared_owner_refuses"),
    pytest.param(
        # Instr. 9(a): a one-line amendment ADDING a transaction of the same title and date; equal
        # line counts alone would read it as a restatement (Codex ckpt-2).
        replace(_BASE, amendment_txn_shares=250),
        _KEPT_ORIGINAL,
        id="an_added_transaction_of_equal_count_refuses",
    ),
    pytest.param(
        # dateOfOriginalSubmission names no accession: two same-day originals make the link ambiguous.
        replace(_BASE, second_original_same_day=True),
        _KEPT_ORIGINAL,
        id="two_candidate_originals_refuse",
    ),
    pytest.param(
        replace(_BASE, holder_on_amendment=False),
        {"indirect": (_ORIGINAL, Decimal(679))},
        id="holder_absent_from_the_amendment_refuses",
    ),
    pytest.param(
        # Same key, filed the same instant: the original used to win on accession order.
        replace(
            _BASE,
            amendment_nature="indirect",
            amendment_shares="700",
            amendment_filed=datetime(2026, 9, 15, 21, tzinfo=UTC),
        ),
        {"indirect": (_AMENDMENT, Decimal(700))},
        id="same_key_same_day_amendment_wins",
    ),
]


@pytest.mark.parametrize(("case", "expected"), _CASES)
@pytest.mark.parametrize("batch", [False, True], ids=["single", "batch"])
def test_current_projection(
    conn: psycopg.Connection[Any], case: _Case, expected: dict[str, tuple[str, Decimal]], batch: bool
) -> None:
    _seed(conn, case)
    if batch:
        oo.refresh_insiders_current_batch(conn, instrument_ids=[_IID])
    else:
        oo.refresh_insiders_current(conn, instrument_id=_IID)
    assert _current(conn) == expected


def test_history_chart_drops_the_superseded_original(conn: psycopg.Connection[Any]) -> None:
    _seed(conn, _BASE)
    points = _insiders_history(conn, instrument_id=_IID, holder_cik=_OWNER)
    assert [(p.period_end, p.ownership_nature, p.source_accession) for p in points] == [
        (date(2026, 4, 14), "indirect", _APRIL),
        (_PERIOD, "direct", _AMENDMENT),
    ]


def test_history_bucket_does_not_fall_back_to_an_older_filing_of_the_same_date(conn: psycopg.Connection[Any]) -> None:
    """Codex ckpt-2: an older filing on the original's own date and nature must not take its bucket."""
    _seed(conn, replace(_BASE, older_period=_PERIOD))
    points = _insiders_history(conn, instrument_id=_IID, holder_cik=_OWNER)
    assert [(p.period_end, p.ownership_nature, p.source_accession) for p in points] == [
        (_PERIOD, "direct", _AMENDMENT),
    ]
    oo.refresh_insiders_current(conn, instrument_id=_IID)
    assert _current(conn) == {"direct": (_AMENDMENT, Decimal(679))}
