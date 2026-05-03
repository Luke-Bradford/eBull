"""Tests for the re-wash workflow.

Pins the contract:

  * Registry: ``register_parser`` is idempotent / overwrite-on-kind.
  * Iter filter: rows already on the spec's ``current_version``
    are skipped — re-running after success is a no-op.
  * Apply contract: ``apply_fn`` returning ``False`` (typed row
    missing) skips without bumping parser_version.
  * Failure isolation: a single accession's apply_fn raising must
    not abort the sweep.
  * parser_version bump happens on success → second pass scans 0
    rows for that accession.
  * ``since`` filter scopes the cohort.
  * ``dry_run`` walks but writes nothing.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services import raw_filings, rewash_filings
from app.services.raw_filings import RawFilingDocument
from app.services.rewash_filings import (
    ParserSpec,
    register_parser,
    registered_specs,
    run_rewash,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Snapshot + restore the parser registry around each test."""
    saved = registered_specs()
    try:
        yield
    finally:
        rewash_filings._REGISTRY.clear()
        for spec in saved.values():
            register_parser(spec)


def _seed_raw(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    kind: str,
    payload: str = "<x/>",
    parser_version: str | None = None,
) -> None:
    raw_filings.store_raw(
        conn,
        accession_number=accession,
        document_kind=kind,  # type: ignore[arg-type]
        payload=payload,
        parser_version=parser_version,
    )
    conn.commit()


def test_register_parser_overwrites_on_same_kind(isolated_registry: None) -> None:
    def _apply_a(_conn: object, _doc: object) -> bool:
        return True

    def _apply_b(_conn: object, _doc: object) -> bool:
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v1", apply_fn=_apply_a)  # type: ignore[arg-type]
    )
    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v2", apply_fn=_apply_b)  # type: ignore[arg-type]
    )

    spec = registered_specs()["form4_xml"]
    assert spec.current_version == "v2"
    assert spec.apply_fn is _apply_b


def test_run_rewash_unknown_kind_raises(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    rewash_filings._REGISTRY.clear()
    with pytest.raises(ValueError, match="No parser registered"):
        run_rewash(ebull_test_conn, document_kind="form4_xml")


def test_run_rewash_skips_rows_on_current_version(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Idempotent: a row already on the spec's current_version must
    not be re-parsed. iter_raw filters them out."""
    conn = ebull_test_conn
    _seed_raw(conn, accession="0000-26-1", kind="form4_xml", parser_version="v1")
    _seed_raw(conn, accession="0000-26-2", kind="form4_xml", parser_version="v1")

    rewash_filings._REGISTRY.clear()
    apply_calls: list[str] = []

    def _apply(_conn: psycopg.Connection[tuple], doc: RawFilingDocument) -> bool:
        apply_calls.append(doc.accession_number)
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v1", apply_fn=_apply)  # type: ignore[arg-type]
    )

    result = run_rewash(conn, document_kind="form4_xml")

    assert apply_calls == []
    assert result.rows_scanned == 0
    assert result.rows_reparsed == 0


def test_run_rewash_reparses_old_versions_and_bumps(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Rows on older parser_version (or NULL) get re-parsed and
    their parser_version is bumped to the current spec — second
    pass is a no-op."""
    conn = ebull_test_conn
    _seed_raw(conn, accession="0000-26-3", kind="form4_xml", parser_version="v1")
    _seed_raw(conn, accession="0000-26-4", kind="form4_xml", parser_version=None)

    rewash_filings._REGISTRY.clear()
    apply_calls: list[str] = []

    def _apply(_conn: psycopg.Connection[tuple], doc: RawFilingDocument) -> bool:
        apply_calls.append(doc.accession_number)
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v2", apply_fn=_apply)  # type: ignore[arg-type]
    )

    first = run_rewash(conn, document_kind="form4_xml")
    assert sorted(apply_calls) == ["0000-26-3", "0000-26-4"]
    assert first.rows_reparsed == 2
    assert first.rows_failed == 0

    # Second pass: parser_version was bumped → iter_raw filters both rows out.
    apply_calls.clear()
    second = run_rewash(conn, document_kind="form4_xml")
    assert apply_calls == []
    assert second.rows_scanned == 0


def test_run_rewash_apply_returning_false_is_skipped_not_bumped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """apply_fn returning False (typed row missing) must NOT bump
    the parser_version — the row stays eligible for the next sweep
    so that fixing the typed-row gap and re-running picks it up."""
    conn = ebull_test_conn
    _seed_raw(conn, accession="0000-26-5", kind="form4_xml", parser_version="v1")

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="form4_xml",
            current_version="v2",
            apply_fn=lambda _c, _d: False,  # type: ignore[arg-type]
        )
    )

    result = run_rewash(conn, document_kind="form4_xml")

    assert result.rows_scanned == 1
    assert result.rows_skipped == 1
    assert result.rows_reparsed == 0

    # parser_version untouched
    with conn.cursor() as cur:
        cur.execute(
            "SELECT parser_version FROM filing_raw_documents WHERE accession_number = %s",
            ("0000-26-5",),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "v1"


def test_run_rewash_isolates_per_row_failure(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """One accession's apply_fn raising must not abort the sweep —
    later rows still get processed and the failure is counted."""
    conn = ebull_test_conn
    _seed_raw(conn, accession="0000-26-6", kind="form4_xml", parser_version="v1")
    _seed_raw(conn, accession="0000-26-7", kind="form4_xml", parser_version="v1")
    _seed_raw(conn, accession="0000-26-8", kind="form4_xml", parser_version="v1")

    rewash_filings._REGISTRY.clear()

    def _apply(_conn: psycopg.Connection[tuple], doc: RawFilingDocument) -> bool:
        if doc.accession_number == "0000-26-7":
            raise RuntimeError("simulated parse failure")
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v2", apply_fn=_apply)  # type: ignore[arg-type]
    )

    result = run_rewash(conn, document_kind="form4_xml")

    assert result.rows_scanned == 3
    assert result.rows_reparsed == 2
    assert result.rows_failed == 1


def test_run_rewash_dry_run_writes_nothing(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """dry_run walks the rows and counts but neither calls apply_fn
    nor bumps parser_version — useful for sizing the cohort before
    a real sweep."""
    conn = ebull_test_conn
    _seed_raw(conn, accession="0000-26-9", kind="form4_xml", parser_version="v1")

    rewash_filings._REGISTRY.clear()
    apply_calls: list[str] = []

    def _apply(_conn: psycopg.Connection[tuple], doc: RawFilingDocument) -> bool:
        apply_calls.append(doc.accession_number)
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v2", apply_fn=_apply)  # type: ignore[arg-type]
    )

    result = run_rewash(conn, document_kind="form4_xml", dry_run=True)

    assert apply_calls == []  # apply_fn not invoked
    assert result.rows_scanned == 1
    assert result.rows_reparsed == 1  # counts what WOULD be re-parsed

    with conn.cursor() as cur:
        cur.execute(
            "SELECT parser_version FROM filing_raw_documents WHERE accession_number = %s",
            ("0000-26-9",),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "v1"  # not bumped


def test_run_rewash_handles_cohort_larger_than_batch_size(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Cohort > batch_size must NOT trip the server-side-cursor +
    per-row-commit interaction (PostgreSQL closes cursors on
    commit). Eager-fetch of the cohort identifiers sidesteps it.
    Regression test for the high-severity Codex finding before
    push."""
    conn = ebull_test_conn
    cohort_size = 105  # > default batch_size=100
    for i in range(cohort_size):
        _seed_raw(
            conn,
            accession=f"0000-26-cohort-{i:03d}",
            kind="form4_xml",
            parser_version="v1",
        )

    rewash_filings._REGISTRY.clear()
    seen: list[str] = []

    def _apply(_conn: psycopg.Connection[tuple], doc: RawFilingDocument) -> bool:
        seen.append(doc.accession_number)
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v2", apply_fn=_apply)  # type: ignore[arg-type]
    )

    result = run_rewash(conn, document_kind="form4_xml", batch_size=10)

    assert result.rows_scanned == cohort_size
    assert result.rows_reparsed == cohort_size
    assert result.rows_failed == 0
    assert len(seen) == cohort_size  # every accession reached apply_fn


def test_run_rewash_since_filters_by_fetched_at(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """``since`` scopes the sweep to rows fetched on/after the
    cutoff date — operator only re-washes the cohort that a recent
    bug touched."""
    conn = ebull_test_conn
    _seed_raw(conn, accession="0000-26-10", kind="form4_xml", parser_version="v1")
    _seed_raw(conn, accession="0000-26-11", kind="form4_xml", parser_version="v1")

    # Backdate one row well before the cutoff.
    conn.execute(
        """
        UPDATE filing_raw_documents
        SET fetched_at = %s
        WHERE accession_number = %s
        """,
        (datetime.now(UTC) - timedelta(days=400), "0000-26-10"),
    )
    conn.commit()

    rewash_filings._REGISTRY.clear()
    apply_calls: list[str] = []

    def _apply(_conn: psycopg.Connection[tuple], doc: RawFilingDocument) -> bool:
        apply_calls.append(doc.accession_number)
        return True

    register_parser(
        ParserSpec(document_kind="form4_xml", current_version="v2", apply_fn=_apply)  # type: ignore[arg-type]
    )

    cutoff = (datetime.now(UTC) - timedelta(days=30)).date()
    result = run_rewash(conn, document_kind="form4_xml", since=cutoff)

    assert apply_calls == ["0000-26-11"]  # backdated row skipped
    assert result.rows_scanned == 2
    assert result.rows_reparsed == 1
    assert result.rows_skipped == 1


def test_form4_rewash_preserves_original_fetched_at(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Form 4 re-wash must NOT bump ``insider_filings.fetched_at``
    (it isn't a fresh SEC fetch; the body comes from the local raw
    store). Audit / recency logic depends on fetched_at meaning
    "when SEC last published this", not "when we last re-parsed
    it". Regression test for the medium-severity Codex finding."""
    from app.services.insider_transactions import (
        ParsedFiler,
        ParsedFiling,
        upsert_filing,
    )

    conn = ebull_test_conn
    accession = "0001234567-26-rewash-test"
    instrument_id = 950_001
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'RW', 'Rewash Inc', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.commit()

    parsed = ParsedFiling(
        document_type="4",
        period_of_report=None,
        date_of_original_submission=None,
        not_subject_to_section_16=None,
        form3_holdings_reported=None,
        form4_transactions_reported=None,
        issuer_cik="0000111000",
        issuer_name="Rewash Inc",
        issuer_trading_symbol="RW",
        remarks=None,
        signature_name=None,
        signature_date=None,
        filers=(
            ParsedFiler(
                filer_cik="0000222000",
                filer_name="Test Filer",
                street1=None,
                street2=None,
                city=None,
                state=None,
                zip_code=None,
                state_description=None,
                is_director=False,
                is_officer=False,
                officer_title=None,
                is_ten_percent_owner=False,
                is_other=False,
                other_text=None,
            ),
        ),
        footnotes=(),
        transactions=(),
    )
    upsert_filing(
        conn,
        instrument_id=instrument_id,
        accession_number=accession,
        primary_document_url="https://example.com/x",
        parsed=parsed,
    )
    conn.commit()

    # Backdate the row to a known prior timestamp, then call the
    # rewash variant and confirm fetched_at was preserved.
    backdate = datetime(2024, 6, 1, tzinfo=UTC)
    conn.execute(
        "UPDATE insider_filings SET fetched_at = %s WHERE accession_number = %s",
        (backdate, accession),
    )
    conn.commit()

    upsert_filing(
        conn,
        instrument_id=instrument_id,
        accession_number=accession,
        primary_document_url="https://example.com/x",
        parsed=parsed,
        is_rewash=True,
    )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT fetched_at FROM insider_filings WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == backdate  # preserved across rewash upsert
