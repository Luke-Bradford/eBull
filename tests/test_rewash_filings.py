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
from decimal import Decimal

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


def test_form4_apply_raises_on_parse_regression(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """``_apply_form4`` must RAISE (not return False) when the
    parser returns None on a body that has an existing typed-row.
    A returned False means "no typed row to update, legitimately
    skip", which is invisible in the operator's failure counter.
    Parser regressions must surface as rows_failed.

    Regression for the WARNING from PR #818 review."""
    conn = ebull_test_conn
    accession = "0001234567-26-parse-regress"
    instrument_id = 950_010
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'PR', 'Parse Regression', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type,
            primary_document_url, parser_version, is_tombstone
        ) VALUES (%s, %s, '4', 'https://example.com/x', 1, FALSE)
        """,
        (accession, instrument_id),
    )
    _seed_raw(conn, accession=accession, kind="form4_xml", parser_version="v1")

    monkeypatch.setattr(
        "app.services.insider_transactions.parse_form_4_xml",
        lambda _xml: None,
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="form4_xml",
            current_version="form4-v1",
            apply_fn=rewash_filings._apply_form4,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="form4_xml")

    assert result.rows_scanned == 1
    assert result.rows_failed == 1
    assert result.rows_skipped == 0
    assert result.rows_reparsed == 0


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


def test_form3_apply_raises_on_parse_regression(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """Form 3 rewash spec uses the same parse-regression contract
    as Form 4: parser returning None on a body that has an existing
    typed row must RAISE so the failure surfaces in rows_failed,
    not silently in rows_skipped."""
    conn = ebull_test_conn
    accession = "0001234567-26-form3-regress"
    instrument_id = 950_020
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'F3', 'Form 3 Regression', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type,
            primary_document_url, parser_version, is_tombstone
        ) VALUES (%s, %s, '3', 'https://example.com/x', 1, FALSE)
        """,
        (accession, instrument_id),
    )
    _seed_raw(conn, accession=accession, kind="form3_xml", parser_version="form3-v0")

    monkeypatch.setattr(
        "app.services.insider_transactions.parse_form_3_xml",
        lambda _xml: None,
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="form3_xml",
            current_version="form3-v1",
            apply_fn=rewash_filings._apply_form3,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="form3_xml")

    assert result.rows_scanned == 1
    assert result.rows_failed == 1
    assert result.rows_skipped == 0


def test_form3_rewash_preserves_original_fetched_at(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Form 3 rewash must NOT bump insider_filings.fetched_at —
    same audit contract as Form 4 (PR #818)."""
    from app.services.insider_form3_ingest import upsert_form_3_filing
    from app.services.insider_transactions import ParsedFiler
    from app.services.insider_transactions import (
        ParsedForm3 as ParsedForm3Data,
    )

    conn = ebull_test_conn
    accession = "0001234567-26-form3-rewash"
    instrument_id = 950_021
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'F3R', 'Form 3 Rewash', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.commit()

    parsed = ParsedForm3Data(
        document_type="3",
        period_of_report=None,
        date_of_original_submission=None,
        issuer_cik="0000111000",
        issuer_name="Form 3 Rewash Inc",
        issuer_trading_symbol="F3R",
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
        holdings=(),
        footnotes=(),
        no_securities_owned=False,
    )
    upsert_form_3_filing(
        conn,
        instrument_id=instrument_id,
        accession_number=accession,
        primary_document_url="https://example.com/x",
        parsed=parsed,
    )
    conn.commit()

    backdate = datetime(2024, 6, 1, tzinfo=UTC)
    conn.execute(
        "UPDATE insider_filings SET fetched_at = %s WHERE accession_number = %s",
        (backdate, accession),
    )
    conn.commit()

    upsert_form_3_filing(
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
    assert row[0] == backdate


def test_def14a_apply_raises_on_parse_failure(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """DEF 14A parser failure on a body with an existing typed row
    must raise so the failure surfaces in rows_failed."""
    conn = ebull_test_conn
    accession = "0001234567-26-def14a-regress"
    instrument_id = 950_050
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'D14', 'DEF 14A Regression', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, %s, '0000999000', 'Test Holder', 'officer', 100, 5.0, '2025-01-01')
        """,
        (instrument_id, accession),
    )
    _seed_raw(conn, accession=accession, kind="def14a_body", parser_version="def14a-v0")
    conn.commit()

    monkeypatch.setattr(
        "app.providers.implementations.sec_def14a.parse_beneficial_ownership_table",
        lambda _html: (_ for _ in ()).throw(ValueError("synthetic parse error")),
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="def14a_body",
            current_version="def14a-v1",
            apply_fn=rewash_filings._apply_def14a,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="def14a_body")

    assert result.rows_scanned == 1
    assert result.rows_failed == 1
    assert result.rows_skipped == 0


def test_def14a_apply_replaces_holders_on_rewash(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """DEF 14A rewash replaces all holders for the accession. A
    holder dropped by the new parser must not linger from the
    previous parse."""
    from app.providers.implementations.sec_def14a import (
        Def14ABeneficialHolder,
        Def14ABeneficialOwnershipTable,
    )

    conn = ebull_test_conn
    instrument_id = 950_051
    accession = "0001234567-26-def14a-replace"
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'D14R', 'DEF 14A Replace', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    # Two pre-existing holders.
    for holder in ("Holder A", "Holder B"):
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                instrument_id, accession_number, issuer_cik,
                holder_name, holder_role, shares, percent_of_class, as_of_date
            ) VALUES (%s, %s, '0000999000', %s, 'officer', 100, 5.0, '2025-01-01')
            """,
            (instrument_id, accession, holder),
        )
    _seed_raw(conn, accession=accession, kind="def14a_body", parser_version="def14a-v0")
    conn.commit()

    # New parse drops Holder B and adds Holder C.
    fake_table = Def14ABeneficialOwnershipTable(
        as_of_date=None,
        rows=[
            Def14ABeneficialHolder(
                holder_name="Holder A",
                holder_role="officer",
                shares=Decimal("100"),
                percent_of_class=Decimal("5.0"),
            ),
            Def14ABeneficialHolder(
                holder_name="Holder C",
                holder_role="director",
                shares=Decimal("200"),
                percent_of_class=Decimal("8.0"),
            ),
        ],
        raw_table_score=10,
    )
    monkeypatch.setattr(
        "app.providers.implementations.sec_def14a.parse_beneficial_ownership_table",
        lambda _html: fake_table,
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="def14a_body",
            current_version="def14a-v1",
            apply_fn=rewash_filings._apply_def14a,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="def14a_body")
    assert result.rows_reparsed == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT holder_name FROM def14a_beneficial_holdings WHERE accession_number = %s ORDER BY holder_name",
            (accession,),
        )
        rows = cur.fetchall()
    holders = [r[0] for r in rows]
    assert holders == ["Holder A", "Holder C"]


def test_def14a_apply_rescues_tombstoned_accession(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """Rescue cohort: original ingest tombstoned with zero typed
    rows (parser couldn't find table). New parser DOES find a
    table. Re-wash must populate the typed rows — not skip
    forever. Regression for the medium-severity Codex finding."""
    from app.providers.implementations.sec_def14a import (
        Def14ABeneficialHolder,
        Def14ABeneficialOwnershipTable,
    )

    conn = ebull_test_conn
    instrument_id = 950_060
    accession = "0001234567-26-def14a-rescue"
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'D14X', 'DEF 14A Rescue', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    # Tombstoned ingest_log row, zero typed rows.
    conn.execute(
        """
        INSERT INTO def14a_ingest_log (accession_number, issuer_cik, status)
        VALUES (%s, '0000999000', 'partial')
        """,
        (accession,),
    )
    # filing_events row carries instrument_id resolution.
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type, source_url,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, '2025-03-01', 'DEF 14A', 'https://example.com/x',
                  'sec', %s, 'https://example.com/x')
        """,
        (instrument_id, accession),
    )
    _seed_raw(conn, accession=accession, kind="def14a_body", parser_version="def14a-v0")
    conn.commit()

    fake_table = Def14ABeneficialOwnershipTable(
        as_of_date=None,
        rows=[
            Def14ABeneficialHolder(
                holder_name="Rescued Holder",
                holder_role="director",
                shares=Decimal("500"),
                percent_of_class=Decimal("3.0"),
            ),
        ],
        raw_table_score=15,
    )
    monkeypatch.setattr(
        "app.providers.implementations.sec_def14a.parse_beneficial_ownership_table",
        lambda _html: fake_table,
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="def14a_body",
            current_version="def14a-v1",
            apply_fn=rewash_filings._apply_def14a,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="def14a_body")
    assert result.rows_reparsed == 1  # rescued, not skipped

    with conn.cursor() as cur:
        cur.execute(
            "SELECT holder_name FROM def14a_beneficial_holdings WHERE accession_number = %s",
            (accession,),
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["Rescued Holder"]


def test_blockholders_apply_raises_on_parse_failure(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """13D/G rewash spec follows the same parse-regression contract:
    parser failure on a body with an existing typed row must raise
    so the failure surfaces in rows_failed."""
    conn = ebull_test_conn
    accession = "0001234567-26-13dg-regress"
    instrument_id = 950_030
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, '13DG', '13D/G Regression', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES ('0000111000', 'Test Filer') ON CONFLICT (cik) DO NOTHING",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM blockholder_filers WHERE cik = '0000111000'")
        result = cur.fetchone()
    assert result is not None
    filer_id = result[0]
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip, securities_class_title,
            reporter_no_cik, reporter_name, aggregate_amount_owned, percent_of_class
        ) VALUES (%s, %s, 'SCHEDULE 13G', 'passive', %s,
                  '0000999000', '00000099', 'Common Stock',
                  FALSE, 'Test Reporter', 1000, 5.5)
        """,
        (filer_id, accession, instrument_id),
    )
    _seed_raw(conn, accession=accession, kind="primary_doc_13dg", parser_version="13dg-primary-v0")
    conn.commit()

    monkeypatch.setattr(
        "app.providers.implementations.sec_13dg.parse_primary_doc",
        lambda _xml: (_ for _ in ()).throw(ValueError("synthetic parse error")),
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="primary_doc_13dg")

    assert result.rows_scanned == 1
    assert result.rows_failed == 1
    assert result.rows_skipped == 0


def test_blockholders_apply_returns_false_when_no_existing_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Re-wash isn't a first-time ingester. If there's no existing
    blockholder_filings row for the accession, _apply_blockholders
    returns False (skipped, not failed)."""
    conn = ebull_test_conn
    _seed_raw(
        conn,
        accession="0001234567-26-13dg-orphan",
        kind="primary_doc_13dg",
        parser_version="13dg-primary-v0",
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="primary_doc_13dg")

    assert result.rows_scanned == 1
    assert result.rows_skipped == 1
    assert result.rows_failed == 0


def test_blockholders_apply_re_resolves_instrument_from_fresh_cusip(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """If the new parser emits a corrected issuer_cusip, rewash
    must re-resolve the instrument_id from that CUSIP — not reuse
    the stale typed-row value. Otherwise the row ends up internally
    inconsistent (issuer_cusip from the new parse, instrument_id
    pointing at the old issuer). Regression for the high-severity
    Codex finding."""
    from app.providers.implementations.sec_13dg import (
        BlockholderFiling,
        BlockholderReportingPerson,
    )

    conn = ebull_test_conn
    old_iid = 950_040
    new_iid = 950_041
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'OLD', 'Old Issuer', '4', 'USD', TRUE),
                 (%s, 'NEW', 'New Issuer', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (old_iid, new_iid),
    )
    # external_identifiers maps CUSIPs to the right instruments.
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        ) VALUES
            (%s, 'sec', 'cusip', 'OLDCUSIP', FALSE),
            (%s, 'sec', 'cusip', 'NEWCUSIP', FALSE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (old_iid, new_iid),
    )
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES ('0000111000', 'Test Filer') ON CONFLICT (cik) DO NOTHING",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM blockholder_filers WHERE cik = '0000111000'")
        result = cur.fetchone()
    assert result is not None
    filer_id = result[0]

    accession = "0001234567-26-13dg-cusip-fix"
    # Seed a typed row with the OLD cusip + OLD instrument_id.
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip, securities_class_title,
            reporter_no_cik, reporter_name, aggregate_amount_owned, percent_of_class
        ) VALUES (%s, %s, 'SCHEDULE 13G', 'passive', %s,
                  '0000999000', 'OLDCUSIP', 'Common',
                  FALSE, 'Test Reporter', 1000, 5.5)
        """,
        (filer_id, accession, old_iid),
    )
    _seed_raw(conn, accession=accession, kind="primary_doc_13dg", parser_version="13dg-primary-v0")
    conn.commit()

    # Stub the parser to return the NEW cusip (simulating the bug fix).
    fake_filing = BlockholderFiling(
        submission_type="SCHEDULE 13G",
        status="passive",
        primary_filer_cik="0000111000",
        issuer_cik="0000999000",
        issuer_cusip="NEWCUSIP",  # parser fix
        issuer_name="New Issuer",
        securities_class_title="Common",
        date_of_event=None,
        filed_at=None,
        reporting_persons=[
            BlockholderReportingPerson(
                cik="0000111000",
                no_cik=False,
                name="Test Filer",
                member_of_group=None,
                type_of_reporting_person=None,
                citizenship=None,
                sole_voting_power=None,
                shared_voting_power=None,
                sole_dispositive_power=None,
                shared_dispositive_power=None,
                aggregate_amount_owned=None,
                percent_of_class=None,
            ),
        ],
    )
    monkeypatch.setattr(
        "app.providers.implementations.sec_13dg.parse_primary_doc",
        lambda _xml: fake_filing,
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="primary_doc_13dg")
    assert result.rows_reparsed == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id, issuer_cusip FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    instrument_id, issuer_cusip = row
    assert issuer_cusip == "NEWCUSIP"
    assert instrument_id == new_iid  # re-resolved, not reused stale value


def test_blockholders_apply_raises_on_empty_reporting_persons(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    isolated_registry: None,
) -> None:
    """If the new parser returns zero reporting_persons on a
    previously-populated 13D/G accession, the apply must RAISE —
    without the guard, DELETE would silently destroy every existing
    reporter row with no failure signal. Regression for the BLOCKING
    finding from PR #825 review."""
    from app.providers.implementations.sec_13dg import BlockholderFiling

    conn = ebull_test_conn
    accession = "0001234567-26-13dg-empty-persons"
    instrument_id = 950_110
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, '13DGE', '13D/G Empty', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES ('0000111000', 'Test') ON CONFLICT (cik) DO NOTHING",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM blockholder_filers WHERE cik = '0000111000'")
        result = cur.fetchone()
    assert result is not None
    filer_id = result[0]
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip, securities_class_title,
            reporter_no_cik, reporter_name, aggregate_amount_owned, percent_of_class
        ) VALUES (%s, %s, 'SCHEDULE 13G', 'passive', %s,
                  '0000999000', 'CSP1', 'Common',
                  FALSE, 'Existing', 1000, 5.0)
        """,
        (filer_id, accession, instrument_id),
    )
    _seed_raw(conn, accession=accession, kind="primary_doc_13dg", parser_version="13dg-primary-v0")
    conn.commit()

    fake_filing = BlockholderFiling(
        submission_type="SCHEDULE 13G",
        status="passive",
        primary_filer_cik="0000111000",
        issuer_cik="0000999000",
        issuer_cusip="CSP1",
        issuer_name="Issuer",
        securities_class_title="Common",
        date_of_event=None,
        filed_at=None,
        reporting_persons=[],  # parser regression: lost all reporters
    )
    monkeypatch.setattr(
        "app.providers.implementations.sec_13dg.parse_primary_doc",
        lambda _xml: fake_filing,
    )

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = rewash_filings.run_rewash(conn, document_kind="primary_doc_13dg")
    assert result.rows_failed == 1
    assert result.rows_skipped == 0

    # Existing rows must NOT have been deleted by the failed pass.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        )
        result_row = cur.fetchone()
    assert result_row is not None
    assert result_row[0] == 1  # original reporter still on file
