"""#3227 statement-date selection against a real DB: the cover read and the observation read.

Spec: docs/specs/ownership/2026-10-22-3227-13f-statement-as-of.md (Acceptance, DB test). Auto-marked
``db`` (pulls ``ebull_test_conn``). A 13G and its observation filed after P, with date of event P, sit
in a later ``period_end`` partition than P; under a non-UTC session time zone they reach the fold
through ``_read_13f_covers`` and ``_read_13g_filers``, and the SQL bound agrees with
``_CoverRow.statement_date`` on rows with NULL, normal and after-filing dates.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg

from app.services.ownership_rollup import (
    _COVERS_13F_SQL,
    Holder,
    SliceCategory,
    _CoverRow,
    _fold_13f_duplicates,
    _read_13f_covers,
    _read_13g_filers,
)

_IID = 3_227_701
_P = date(2026, 6, 30)
_T = 2_520_562
_M, _M_NAME = "0000748054", "American Century Companies, Inc."
_K, _K_NAME = "0000052388", "American Century Investment Management, Inc."
# Accessions must match the observations table's accession-format check (sql/134).
_C = "0000052388-26-000256"
_FILED = datetime(2026, 8, 14, 20, tzinfo=UTC)


def _holder(cik: str, name: str, source: str, accession: str) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(_T),
        pct_outstanding=Decimal(0),
        winning_source=source,  # type: ignore[arg-type]
        winning_accession=accession,
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _filing(
    conn: psycopg.Connection[tuple],
    accession: str,
    name: str,
    filed: datetime | None,
    event: date | None,
    *,
    t: int = _T,
) -> None:
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status, instrument_id, issuer_cik, issuer_cusip,
            reporter_cik, reporter_no_cik, reporter_name, sole_dispositive_power, shared_dispositive_power,
            aggregate_amount_owned, filed_at, date_of_event
        )
        SELECT filer_id, %s, 'SCHEDULE 13G', 'passive', %s, '0000000789', '999999999',
               NULL, TRUE, %s, 0, %s, %s, %s, %s
          FROM blockholder_filers WHERE cik = %s
        """,
        (accession, _IID, name, t, t, filed, event, _K),
    )


def test_a_cover_filed_after_p_reaches_the_fold_under_a_non_utc_session(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, 'CALMT', 'CALM test co', '4', 'USD', 'US', TRUE)
        """,
        (_IID,),
    )
    conn.execute("INSERT INTO blockholder_filers (cik, name) VALUES (%s, %s)", (_K, _K_NAME))
    # C: non-uniform, filed after P (2026-Q3 partition) with date of event P.
    _filing(conn, _C, _K_NAME, _FILED, _P)
    _filing(conn, _C, _M_NAME, _FILED, _P)
    _filing(conn, _C, "American Century Mutual Funds", _FILED, _P, t=1_000)
    conn.execute(
        """
        INSERT INTO ownership_blockholders_observations (
            instrument_id, reporter_cik, reporter_name, ownership_nature, submission_type, status_flag,
            source, source_document_id, source_accession, source_url, filed_at, period_end,
            ingest_run_id, aggregate_amount_owned
        ) VALUES (%s, %s, %s, 'beneficial', 'SCHEDULE 13G', 'passive', '13g', %s, %s, NULL, %s, %s,
                  gen_random_uuid(), %s)
        """,
        (_IID, _K, _K_NAME, _C, _C, _FILED, _FILED.date(), _T),
    )
    # Statement-date edge rows on other accessions (one block each).
    edges = {
        "0000052388-26-000301": (datetime(2026, 7, 1, 2, tzinfo=UTC), None),  # 06-30 22:00 in New York
        "0000052388-26-000302": (datetime(2026, 6, 30, 23, tzinfo=UTC), None),
        "0000052388-26-000303": (datetime(2026, 6, 20, tzinfo=UTC), date(2026, 7, 5)),  # after filing
        "0000052388-26-000304": (datetime(2026, 7, 20, tzinfo=UTC), date(2026, 7, 5)),
        "0000052388-26-000305": (datetime(2026, 9, 1, tzinfo=UTC), date(2026, 6, 15)),
        # No filing timestamp: excluded, as the filing-date bound excluded it (``LEAST`` skips a NULL).
        "0000052388-26-000306": (None, date(2026, 6, 1)),
    }
    for accession, (filed, event) in edges.items():
        _filing(conn, accession, "Edge", filed, event, t=1)
    conn.execute("SET TIME ZONE 'America/New_York'")

    by_category: dict[SliceCategory, list[Holder]] = {
        "insiders": [],
        "blockholders": [_holder(_K, _K_NAME, "13g", _C)],
        "institutions": [_holder(_M, _M_NAME, "13f", "13F-M")],
    }
    covers = _read_13f_covers(conn, _IID, by_category)
    assert sorted({c.accession_number for c in covers}) == [
        _C,
        "0000052388-26-000302",
        "0000052388-26-000303",
        "0000052388-26-000305",
    ]
    filers = _read_13g_filers(conn, _IID, covers)
    assert filers[_C] == ((_K, _K_NAME, Decimal(_T)),)
    _, corrections = _fold_13f_duplicates(by_category, covers, filers)
    (c,) = corrections
    assert c.filer_cik == _K and "counted here as" in c.detail
    assert f"Schedule 13G {_C} (filed 2026-08-14, date of event 2026-06-30, filer of record 52388)" in c.detail

    # The SQL bound and ``statement_date`` agree on every row, at every period that splits them.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reporter_cik, accession_number, filed_at, sole_dispositive_power, shared_dispositive_power,"
            " aggregate_amount_owned, reporter_name, submission_type, date_of_event"
            " FROM blockholder_filings WHERE instrument_id = %s AND filed_at IS NOT NULL",
            (_IID,),
        )
        every = [_CoverRow(*row) for row in cur.fetchall()]
    for period in sorted({r.statement_date for r in every} | {date(2026, 6, 29)}):
        with conn.cursor() as cur:
            cur.execute(_COVERS_13F_SQL, {"iid": _IID, "period": period})
            got = sorted(cur.fetchall())
        want = sorted(
            (
                r.reporter_cik,
                r.accession_number,
                r.filed_at,
                r.sole_dispositive,
                r.shared_dispositive,
                r.aggregate,
                r.reporter_name,
                r.submission_type,
                r.date_of_event,
            )
            for r in every
            if r.statement_date <= period
        )
        assert got == want, period
