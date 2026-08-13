"""Tests for the ``finra_short_interest`` synth no-op manifest-worker
parser (G6/#915).

Mirror of ``test_manifest_parser_sec_xbrl_facts.py`` (G7) — third
adopter of the sec-edgar §11.5.1 synth no-op pattern. FINRA short
interest data lands via the ``finra_short_interest_refresh``
ScheduledJob (not via the manifest dispatch layer); the manifest row's
existence is the audit signal.

Covers:

- Happy path: a ``finra_short_interest`` manifest row transitions
  ``pending`` → ``parsed`` via ``run_manifest_worker``, with no
  ``filing_raw_documents`` row created by the synth no-op (the
  ScheduledJob is the sole writer of the raw payload).
- Registry wiring: ``register_all_parsers`` makes
  ``finra_short_interest`` discoverable via
  ``registered_parser_sources``, even after a registry wipe.
- Durability gate: parser does NOT call ``conn.execute`` /
  ``conn.cursor`` / ``conn.transaction`` / ``store_raw`` /
  ``fetch_document_text``.
- ParseOutcome contract: status='parsed', parser_version=
  ``'finra-si-bimonthly-v1'`` (unified with the ScheduledJob's
  write-side parser_version per Codex 1b r2 MED 3 — NO separate
  ``-noop-v1`` literal).
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
from app.services.manifest_parsers.finra_short_interest import (
    PARSER_VERSION,
    _parse_finra_short_interest,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_pending_finra_si(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filed_at: datetime | None = None,
    primary_doc_url: str | None = None,
) -> None:
    """Seed a ``finra_short_interest`` manifest row in ``pending`` state.

    Synthetic FINRA accession + CIK + subject per the documented
    ``finra_universe`` shape in sql/118 (subject_type='finra_universe',
    subject_id='FINRA_SI', instrument_id=NULL).
    """
    if filed_at is None:
        filed_at = datetime(2026, 4, 30, tzinfo=UTC)
    if primary_doc_url is None:
        primary_doc_url = f"https://cdn.finra.org/equity/otcmarket/biweekly/shrt{accession[len('FINRA_SI_') :]}.csv"
    record_manifest_entry(
        conn,
        accession,
        cik="FINRA_SI",
        form="SHRT",
        source="finra_short_interest",
        subject_type="finra_universe",
        subject_id="FINRA_SI",
        instrument_id=None,
        filed_at=filed_at,
        primary_document_url=primary_doc_url,
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    """Pin the parser registry to ``register_all_parsers`` output for
    every test. Mirrors the sec_xbrl_facts fixture so cross-test
    registry leaks cannot false-pass the registry-wiring test below."""
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


def test_synth_no_op_marks_parsed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Happy path: pending finra_short_interest row drains to ``parsed``.

    Post-assert: no ``filing_raw_documents`` row (synth no-op stores
    nothing; the ScheduledJob is the sole writer of the raw payload).
    """
    conn = ebull_test_conn
    accession = "FINRA_SI_20260430"
    _seed_pending_finra_si(conn, accession=accession)

    stats = run_manifest_worker(conn, source="finra_short_interest", max_rows=5)
    assert stats.parsed == 1
    assert stats.failed == 0
    assert stats.tombstoned == 0
    assert stats.skipped_no_parser == 0

    final = get_manifest_row(conn, accession)
    assert final is not None
    assert final.ingest_status == "parsed"
    assert final.parser_version == PARSER_VERSION == "finra-si-bimonthly-v1"
    assert _count_filing_raw_documents(conn, accession) == 0


def test_register_all_parsers_includes_finra_short_interest() -> None:
    """Registry-wiring: after ``clear_registered_parsers`` +
    ``register_all_parsers``, ``finra_short_interest`` IS in the
    registry. The clear-first step is mandatory."""
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()

    sources = registered_parser_sources()
    assert "finra_short_interest" in sources
    # Spot-check siblings — guards against a future refactor that
    # accidentally drops every parser except finra_short_interest.
    assert "sec_xbrl_facts" in sources
    assert "sec_10q" in sources


class _NoTouchConnection:
    """Sentinel connection — every DB-touch method raises."""

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.execute")

    def cursor(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.cursor")

    def transaction(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.transaction")


class _NoTouchRow:
    """Stub ManifestRow exposing only the field the synth no-op reads."""

    accession_number = "FINRA_SI_20260430"


def test_parser_does_not_touch_db_or_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Durability gate.

    The synth no-op MUST NOT call ``conn.execute`` / ``conn.cursor`` /
    ``conn.transaction`` / ``store_raw`` / ``fetch_document_text``.
    Forces spec-revision if a future PR adds payload fetch / DB write /
    raw archival to the manifest dispatch path.
    """
    from app.providers.implementations import sec_edgar
    from app.services import raw_filings
    from app.services.manifest_parsers import (
        finra_short_interest as finra_short_interest_module,
    )

    def _raise_store_raw(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call store_raw")

    def _raise_fetch(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call fetch_document_text")

    monkeypatch.setattr(raw_filings, "store_raw", _raise_store_raw)
    monkeypatch.setattr(finra_short_interest_module, "store_raw", _raise_store_raw, raising=False)
    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _raise_fetch)

    outcome = _parse_finra_short_interest(_NoTouchConnection(), _NoTouchRow())  # type: ignore[arg-type]
    assert outcome.status == "parsed"
    assert outcome.parser_version == PARSER_VERSION == "finra-si-bimonthly-v1"
    assert outcome.raw_status is None
    assert outcome.error is None
