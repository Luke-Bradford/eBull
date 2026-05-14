"""Tests for the N-CSR synth no-op manifest-worker parser adapter (#918).

Covers:

- Happy path: manifest row transitions ``pending`` -> ``parsed`` via
  ``run_manifest_worker`` with no DB writes outside the manifest itself.
- N-CSR/A amendment: same code path; no fallback semantics.
- Registry wiring: ``register_all_parsers`` makes ``sec_n_csr``
  discoverable via ``registered_parser_sources``, even after a registry
  wipe.
- Durability gate: the parser does NOT call ``conn.execute`` /
  ``conn.cursor`` / ``conn.transaction`` / ``store_raw`` /
  ``fetch_document_text``. A future contributor regressing this
  module into a fetcher fails this test loudly - forcing the
  spec-revision + Codex-review flow rather than silently regressing
  into a parser design the spike ruled out on product-visibility
  grounds.

Rationale: see ``docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md``.
Fund holdings already land via N-PORT-P at structured CUSIP grain.
N-CSR adds an audit credential whose operator-visible delta is marginal,
and no per-issuer machine-readable identifier exists anywhere in the
N-CSR payload (iXBRL or HTML). Synth no-op is the documented sec-edgar
§11.5.1 pattern; this test mirrors the #1168 ``sec_10q`` durability
shape exactly.
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
from app.services.manifest_parsers.sec_n_csr import _parse_sec_n_csr
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


def _seed_pending_n_csr(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    cik: str = "0000999990",
    form: str = "N-CSR",
    filed_at: datetime | None = None,
    primary_doc_url: str | None = None,
) -> None:
    if filed_at is None:
        filed_at = datetime(2026, 2, 27, tzinfo=UTC)
    if primary_doc_url is None:
        primary_doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/primary_doc.htm"
        )
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_n_csr",
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
    leaks (a foreign parser registering ``sec_n_csr``) cannot false-pass
    the registry-wiring test below."""
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


def _count_funds_observations(conn: psycopg.Connection[tuple], instrument_id: int) -> int:
    """Cross-check: a future real N-CSR parser would write to
    ``ownership_funds_observations``. The synth no-op MUST NOT, even
    by accident."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ownership_funds_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_synth_no_op_marks_parsed(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """Happy path: pending N-CSR row drains to ``parsed`` via the worker.

    Post-assert: no filing_raw_documents row (synth no-op stores
    nothing); no ownership_funds_observations row (the future-real-
    parser typed table must not be written by accident).
    """
    conn = ebull_test_conn
    iid = 902_001
    accession = "0001104659-26-021519"
    _seed_instrument(conn, iid=iid, symbol="NCSRF", cik="0000036405")
    _seed_pending_n_csr(conn, accession=accession, instrument_id=iid, cik="0000036405")

    stats = run_manifest_worker(conn, source="sec_n_csr", max_rows=5)
    assert stats.parsed == 1
    assert stats.failed == 0
    assert stats.tombstoned == 0
    assert stats.skipped_no_parser == 0

    final = get_manifest_row(conn, accession)
    assert final is not None
    assert final.ingest_status == "parsed"
    assert final.parser_version == "n-csr-noop-v1"

    assert _count_filing_raw_documents(conn, accession) == 0
    assert _count_funds_observations(conn, iid) == 0


def test_synth_no_op_handles_n_csr_a_amendment(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """N-CSR/A amendments parse the same way as plain N-CSR — no
    fallback semantics, no special-casing. The synth no-op is form-
    agnostic within the source."""
    conn = ebull_test_conn
    iid = 902_002
    accession = "0001104659-26-021502"
    _seed_instrument(conn, iid=iid, symbol="NCSRFA", cik="0000036406")
    _seed_pending_n_csr(
        conn,
        accession=accession,
        instrument_id=iid,
        cik="0000036406",
        form="N-CSR/A",
    )

    stats = run_manifest_worker(conn, source="sec_n_csr", max_rows=5)
    assert stats.parsed == 1

    final = get_manifest_row(conn, accession)
    assert final is not None
    assert final.ingest_status == "parsed"
    assert final.parser_version == "n-csr-noop-v1"

    assert _count_filing_raw_documents(conn, accession) == 0


def test_register_all_parsers_includes_sec_n_csr() -> None:
    """Registry-wiring test: after ``clear_registered_parsers`` +
    ``register_all_parsers``, ``sec_n_csr`` IS in the registry.

    The clear-first step is mandatory: without it a leaked registry
    entry from a prior test would false-pass even when ``__init__.py``
    does not wire ``sec_n_csr``. The autouse fixture does the same
    thing, but this test asserts the invariant directly."""
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()

    sources = registered_parser_sources()
    assert "sec_n_csr" in sources
    # Spot-check siblings — guards against a future refactor that
    # accidentally drops every parser except sec_n_csr.
    assert "sec_n_port" in sources
    assert "sec_10q" in sources


class _NoTouchConnection:
    """Sentinel connection — every DB-touch method raises.

    Used by the durability gate test below to prove the parser does
    not invoke conn.execute / conn.cursor / conn.transaction. If a
    future contributor adds DB writes, this raises and the test
    fails loudly."""

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.execute")

    def cursor(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.cursor")

    def transaction(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.transaction")


class _NoTouchRow:
    """Stub ManifestRow with the only attribute the synth no-op reads
    (accession_number — for the debug log)."""

    accession_number = "0001104659-26-099999"


def test_parser_does_not_touch_db_or_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Durability gate (#918 / spike `2026-05-14-n-csr-feasibility.md`).

    The synth no-op parser MUST NOT call ``conn.execute`` /
    ``conn.cursor`` / ``conn.transaction`` / ``store_raw`` /
    ``fetch_document_text``. Triple-block (mirrors #1168 sec_10q):

    1. Sentinel connection raises on any DB-touch method.
    2. ``app.services.raw_filings.store_raw`` is patched to raise on
       call (catches any future contributor importing it via the
       service-layer path).
    3. ``app.services.manifest_parsers.sec_n_csr.store_raw`` is patched
       with ``raising=False`` (catches the alternate import path
       where someone adds ``from app.services.raw_filings import
       store_raw`` to ``sec_n_csr.py`` and uses the module-local name).
    4. ``SecFilingsProvider.fetch_document_text`` is patched to raise
       on call (catches a future fetcher addition).

    A future PR that adds payload fetch / DB write / raw archival to
    the parser fails this test — forcing spec-revision + Codex-review
    rather than silently regressing into a parser the spike ruled out.
    """
    from app.providers.implementations import sec_edgar
    from app.services import raw_filings
    from app.services.manifest_parsers import sec_n_csr as sec_n_csr_module

    def _raise_store_raw(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call store_raw")

    def _raise_fetch(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call fetch_document_text")

    monkeypatch.setattr(raw_filings, "store_raw", _raise_store_raw)
    monkeypatch.setattr(sec_n_csr_module, "store_raw", _raise_store_raw, raising=False)
    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _raise_fetch)

    outcome = _parse_sec_n_csr(_NoTouchConnection(), _NoTouchRow())  # type: ignore[arg-type]
    assert outcome.status == "parsed"
    assert outcome.parser_version == "n-csr-noop-v1"
    assert outcome.raw_status is None
    assert outcome.error is None
