"""Tests for the sec_xbrl_facts synth no-op manifest-worker parser (G7).

Mirror of ``test_manifest_parser_sec_10q.py`` (#1168). Both adapters
adopt the sec-edgar §11.5.1 synth no-op pattern — the manifest row's
existence is the audit signal; the underlying data lands via the
Companyfacts bulk JSON path, not via manifest dispatch.

Covers:

- Happy path: manifest row transitions ``pending`` → ``parsed`` via
  ``run_manifest_worker`` with no DB writes outside the manifest itself.
- Form-agnostic: the synth no-op accepts whatever form value appears
  on the manifest row. ``sec_xbrl_facts`` rows are not produced via
  ``_FORM_TO_SOURCE`` form classification (no form maps to
  ``sec_xbrl_facts`` in ``app/services/sec_manifest.py``); the source
  is written directly by the Companyfacts ingest path. The parser
  therefore treats every row identically regardless of form.
- Registry wiring: ``register_all_parsers`` makes ``sec_xbrl_facts``
  discoverable via ``registered_parser_sources``, even after a
  registry wipe.
- Durability gate: the parser does NOT call ``conn.execute`` /
  ``conn.cursor`` / ``conn.transaction`` / ``store_raw`` /
  ``fetch_document_text``. A future contributor regressing this
  module into a fetcher fails this test loudly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    registered_parser_sources,
    run_manifest_worker,
)
from app.services.manifest_parsers.sec_xbrl_facts import _parse_sec_xbrl_facts
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    cik: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )
    if cik is not None:
        conn.execute(
            """
            INSERT INTO external_identifiers (
                instrument_id, provider, identifier_type, identifier_value,
                is_primary, last_verified_at
            )
            VALUES (%s, 'sec', 'cik', %s, TRUE, NOW())
            ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
                WHERE provider = 'sec' AND identifier_type = 'cik'
            DO NOTHING
            """,
            (iid, cik),
        )


def _seed_pending_xbrl_facts(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    cik: str = "0000999990",
    form: str = "10-K",
    filed_at: datetime | None = None,
    primary_doc_url: str | None = None,
) -> None:
    if filed_at is None:
        filed_at = datetime(2026, 4, 30, tzinfo=UTC)
    if primary_doc_url is None:
        primary_doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/primary_doc.htm"
        )
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_xbrl_facts",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=filed_at,
        primary_document_url=primary_doc_url,
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    """Pin the parser registry to ``register_all_parsers`` output for
    every test. Mirrors the sec_10q test fixture so cross-test registry
    leaks (a foreign parser registering ``sec_xbrl_facts``) cannot
    false-pass the registry-wiring test below."""
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def _count_filing_raw_documents(conn: psycopg.Connection[tuple], accession: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM filing_raw_documents WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _count_financial_facts_raw(conn: psycopg.Connection[tuple], instrument_id: int) -> int:
    """Cross-check: the Companyfacts bulk path writes ``financial_facts_raw``
    via ``upsert_facts_for_instrument``. The synth no-op MUST NOT touch
    that table, even by accident — the bulk path owns it."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_synth_no_op_marks_parsed(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """Happy path: pending sec_xbrl_facts row drains to ``parsed``.

    Post-assert: no filing_raw_documents row (synth no-op stores
    nothing); no financial_facts_raw row (the Companyfacts bulk path
    owns that table)."""
    conn = ebull_test_conn
    iid = 902_001
    accession = "0000000003-26-000001"
    _seed_instrument(conn, iid=iid, symbol="XBRLF", cik="0000999993")
    _seed_pending_xbrl_facts(conn, accession=accession, instrument_id=iid, cik="0000999993")

    stats = run_manifest_worker(conn, source="sec_xbrl_facts", max_rows=5)
    assert stats.parsed == 1
    assert stats.failed == 0
    assert stats.tombstoned == 0
    assert stats.skipped_no_parser == 0

    final = get_manifest_row(conn, accession)
    assert final is not None
    assert final.ingest_status == "parsed"
    assert final.parser_version == "xbrl-facts-noop-v1"

    assert _count_filing_raw_documents(conn, accession) == 0
    assert _count_financial_facts_raw(conn, iid) == 0


def test_synth_no_op_is_form_agnostic(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The synth no-op treats every row identically regardless of form.

    ``sec_xbrl_facts`` rows are produced by the Companyfacts ingest
    path, not by ``_FORM_TO_SOURCE`` form classification (no form maps
    to ``sec_xbrl_facts`` in ``app/services/sec_manifest.py``). Seed a
    row carrying a 10-K/A form value to confirm the parser's
    form-agnostic contract."""
    conn = ebull_test_conn
    iid = 902_002
    accession = "0000000003-26-000002"
    _seed_instrument(conn, iid=iid, symbol="XBRLFA", cik="0000999994")
    _seed_pending_xbrl_facts(
        conn,
        accession=accession,
        instrument_id=iid,
        cik="0000999994",
        form="10-K/A",
    )

    stats = run_manifest_worker(conn, source="sec_xbrl_facts", max_rows=5)
    assert stats.parsed == 1

    final = get_manifest_row(conn, accession)
    assert final is not None
    assert final.ingest_status == "parsed"
    assert final.parser_version == "xbrl-facts-noop-v1"

    assert _count_filing_raw_documents(conn, accession) == 0


def test_register_all_parsers_includes_sec_xbrl_facts() -> None:
    """Registry-wiring test: after ``clear_registered_parsers`` +
    ``register_all_parsers``, ``sec_xbrl_facts`` IS in the registry.

    The clear-first step is mandatory: without it a leaked registry
    entry from a prior test would false-pass even when ``__init__.py``
    does not wire ``sec_xbrl_facts``."""
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()

    sources = registered_parser_sources()
    assert "sec_xbrl_facts" in sources
    # Spot-check siblings — guards against a future refactor that
    # accidentally drops every parser except sec_xbrl_facts.
    assert "sec_10q" in sources
    assert "sec_10k" in sources


class _NoTouchConnection:
    """Sentinel connection — every DB-touch method raises.

    Used by the durability gate test below to prove the parser does
    not invoke conn.execute / conn.cursor / conn.transaction. If a
    future contributor adds DB writes, this raises and the test fails
    loudly."""

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.execute")

    def cursor(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.cursor")

    def transaction(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.transaction")


class _NoTouchRow:
    """Stub ManifestRow with the only attribute the synth no-op reads
    (accession_number — for the debug log)."""

    accession_number = "0000000099-26-000099"


def test_parser_does_not_touch_db_or_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Durability gate.

    The synth no-op parser MUST NOT call ``conn.execute`` /
    ``conn.cursor`` / ``conn.transaction`` / ``store_raw`` /
    ``fetch_document_text``. A future PR adding payload fetch / DB
    write / raw archival to the parser fails this test — forcing
    spec-revision rather than silently regressing the synth no-op
    contract that closes G7."""
    from app.providers.implementations import sec_edgar
    from app.services import raw_filings
    from app.services.manifest_parsers import sec_xbrl_facts as sec_xbrl_facts_module

    def _raise_store_raw(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call store_raw")

    def _raise_fetch(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call fetch_document_text")

    monkeypatch.setattr(raw_filings, "store_raw", _raise_store_raw)
    monkeypatch.setattr(sec_xbrl_facts_module, "store_raw", _raise_store_raw, raising=False)
    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _raise_fetch)

    outcome = _parse_sec_xbrl_facts(_NoTouchConnection(), _NoTouchRow())  # type: ignore[arg-type]
    assert outcome.status == "parsed"
    assert outcome.parser_version == "xbrl-facts-noop-v1"
    assert outcome.raw_status is None
    assert outcome.error is None
