"""#2329 — `_upsert_filing_row`'s conditional conflict action.

`ON CONFLICT DO NOTHING` made a NULL `instrument_id` permanent: a later,
successful re-ingest that DID resolve the issuer could not repair the link
written by an earlier one that could not. The action is now a conditional
`DO UPDATE` restricted to exactly one transition — `instrument_id`
NULL -> non-NULL — with every other column still immutable on re-ingest.

These drive REAL PostgreSQL deliberately. The return contract rests on
`RETURNING (xmax = 0)` distinguishing an inserted tuple from one this
statement updated, and on a `WHERE`-suppressed conflict returning no row at
all. That is MVCC behaviour, not something a mocked cursor can pin
(Codex checkpoint 1 finding 2). Auto-marked ``db`` (pulls ``ebull_test_conn``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg

from app.providers.implementations.sec_13dg import BlockholderReportingPerson
from app.services.blockholders import _upsert_filer, _upsert_filing_row

_ACCESSION = "9999999999-26-000001"


def _person(
    *,
    cik: str | None = "0000123456",
    no_cik: bool = False,
    name: str = "ACME PARTNERS LP",
    aggregate: str | None = "1000",
) -> BlockholderReportingPerson:
    return BlockholderReportingPerson(
        cik=cik,
        no_cik=no_cik,
        name=name,
        member_of_group="b",
        type_of_reporting_person="CO",
        citizenship="DE",
        sole_voting_power=Decimal("1000"),
        shared_voting_power=None,
        sole_dispositive_power=Decimal("1000"),
        shared_dispositive_power=None,
        aggregate_amount_owned=None if aggregate is None else Decimal(aggregate),
        percent_of_class=Decimal("5.1234"),
    )


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', 'US', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _write(
    conn: psycopg.Connection[tuple],
    *,
    filer_id: int,
    instrument_id: int | None,
    person: BlockholderReportingPerson,
    accession: str = _ACCESSION,
    issuer_cusip: str = "111111111",
    securities_class_title: str | None = "COMMON STOCK",
) -> bool:
    return _upsert_filing_row(
        conn,
        filer_id=filer_id,
        accession_number=accession,
        submission_type="SCHEDULE 13G",
        status="passive",
        instrument_id=instrument_id,
        issuer_cik="0000999888",
        issuer_cusip=issuer_cusip,
        securities_class_title=securities_class_title,
        date_of_event=None,
        filed_at=datetime(2026, 6, 14, 12, 44, tzinfo=UTC),
        person=person,
    )


def _row(conn: psycopg.Connection[tuple], accession: str = _ACCESSION) -> dict[str, object]:
    cur = conn.execute(
        """
        SELECT filing_id, instrument_id, issuer_cusip, securities_class_title,
               aggregate_amount_owned, percent_of_class, submission_type, status,
               reporter_name, filed_at, fetched_at
        FROM blockholder_filings WHERE accession_number = %s
        """,
        (accession,),
    )
    got = cur.fetchall()
    assert len(got) == 1, f"expected exactly one row for {accession}, got {len(got)}"
    cols = [d.name for d in cur.description or []]
    return dict(zip(cols, got[0], strict=True))


def test_null_link_is_healed_by_a_later_resolved_reingest(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The #2329 defect itself: first ingest could not resolve, second could."""
    conn = ebull_test_conn
    _seed_instrument(conn, 930001, "HEAL")
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")
    person = _person()

    assert _write(conn, filer_id=filer_id, instrument_id=None, person=person) is True
    before = _row(conn)
    assert before["instrument_id"] is None

    # Second ingest, this time resolving. Reports False — a heal is NOT an
    # insertion, so `rows_inserted` must not count it.
    assert _write(conn, filer_id=filer_id, instrument_id=930001, person=person) is False

    after = _row(conn)
    assert after["instrument_id"] == 930001
    # Provenance and identity preserved: same row, not a replacement.
    assert after["filing_id"] == before["filing_id"]
    assert after["fetched_at"] == before["fetched_at"]


def test_heal_touches_no_other_column(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Immutability still holds for everything except the NULL link."""
    conn = ebull_test_conn
    _seed_instrument(conn, 930002, "IMMU")
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")

    assert _write(conn, filer_id=filer_id, instrument_id=None, person=_person()) is True
    before = _row(conn)

    # Re-ingest carrying DIFFERENT values for every mutable-looking column.
    assert (
        _write(
            conn,
            filer_id=filer_id,
            instrument_id=930002,
            person=_person(aggregate="999999"),
            issuer_cusip="222222222",
            securities_class_title="CLASS B",
        )
        is False
    )

    after = _row(conn)
    assert after["instrument_id"] == 930002  # the one permitted transition
    for column in (
        "issuer_cusip",
        "securities_class_title",
        "aggregate_amount_owned",
        "percent_of_class",
        "submission_type",
        "status",
        "reporter_name",
        "filed_at",
        "fetched_at",
        "filing_id",
    ):
        assert after[column] == before[column], f"{column} moved on a heal"


def test_existing_link_is_never_overwritten(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A resolved link is a value, not an absence — re-ingest must not move it."""
    conn = ebull_test_conn
    _seed_instrument(conn, 930003, "KEEP")
    _seed_instrument(conn, 930004, "OTHR")
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")
    person = _person()

    assert _write(conn, filer_id=filer_id, instrument_id=930003, person=person) is True
    # A later ingest resolving to a DIFFERENT instrument is suppressed.
    assert _write(conn, filer_id=filer_id, instrument_id=930004, person=person) is False
    assert _row(conn)["instrument_id"] == 930003


def test_unresolved_reingest_of_an_unresolved_row_is_a_no_op(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """NULL -> NULL: the WHERE suppresses the conflict, RETURNING yields no row."""
    conn = ebull_test_conn
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")
    person = _person()

    assert _write(conn, filer_id=filer_id, instrument_id=None, person=person) is True
    assert _write(conn, filer_id=filer_id, instrument_id=None, person=person) is False
    assert _row(conn)["instrument_id"] is None


def test_same_value_reingest_is_reported_as_a_conflict(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Re-ingest with the identical resolved link is not a second insertion."""
    conn = ebull_test_conn
    _seed_instrument(conn, 930005, "SAME")
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")
    person = _person()

    assert _write(conn, filer_id=filer_id, instrument_id=930005, person=person) is True
    assert _write(conn, filer_id=filer_id, instrument_id=930005, person=person) is False
    assert _row(conn)["instrument_id"] == 930005


def test_reporter_without_a_cik_heals_on_the_coalesced_key(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The arbiter is ``COALESCE(reporter_cik, '')`` — a natural person with no
    EDGAR CIK must conflict with itself, not insert a second row."""
    conn = ebull_test_conn
    _seed_instrument(conn, 930006, "NOCK")
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")
    person = _person(cik=None, no_cik=True, name="JANE DOE")

    assert _write(conn, filer_id=filer_id, instrument_id=None, person=person) is True
    assert _write(conn, filer_id=filer_id, instrument_id=930006, person=person) is False
    assert _row(conn)["instrument_id"] == 930006


def test_a_changed_reporter_identity_inserts_and_leaves_the_old_row_stranded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Documents a KNOWN limit rather than asserting a fix (Codex ckpt-1 finding 7).

    The conflict key includes ``reporter_name``. A re-ingest under a corrected
    name is a different key, so it INSERTS and the old NULL-linked row survives
    untouched. Healing cannot reach reporter-key drift; only a replace-then-insert
    rewash can. Pinned so the behaviour is a decision, not a surprise.
    """
    conn = ebull_test_conn
    _seed_instrument(conn, 930007, "DRFT")
    filer_id = _upsert_filer(conn, cik="0000999888", name="FILER CO")

    assert _write(conn, filer_id=filer_id, instrument_id=None, person=_person(cik=None, no_cik=True, name="ACME LP")) is True
    assert (
        _write(conn, filer_id=filer_id, instrument_id=930007, person=_person(cik=None, no_cik=True, name="ACME L.P."))
        is True
    )

    cur = conn.execute(
        "SELECT reporter_name, instrument_id FROM blockholder_filings WHERE accession_number = %s ORDER BY reporter_name",
        (_ACCESSION,),
    )
    assert cur.fetchall() == [("ACME L.P.", 930007), ("ACME LP", None)]
