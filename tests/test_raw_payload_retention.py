"""Pure-logic tests for the #1014 raw-payload retention sweep.

Spec: docs/specs/etl/2026-06-10-raw-payload-retention-sweep.md.

The structural tests are the #1013 prevention-log lesson applied: a
destructive operation keyed on a classification set must be coupled to
the producing/consuming code's own constants, never a hand-copied list.
The SQL mechanisms themselves are covered in
``tests/test_raw_payload_retention_db.py`` (db tier).
"""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, get_args

import pytest

from app.services import rewash_filings
from app.services.raw_filings import DocumentKind, _row_to_document
from app.services.raw_payload_retention import (
    SWEPT_DOCUMENT_KINDS,
    SWEPT_MANIFEST_SOURCES,
    RawPayloadIntegrityError,
    rehydrate_raw_document,
    sweep_raw_payloads,
)
from app.services.sec_manifest import ManifestSource

# ---------------------------------------------------------------------------
# Structural guards
# ---------------------------------------------------------------------------


def test_swept_kinds_have_no_rewash_parser() -> None:
    """A rewash parser reads stored bodies; the sweep nulls them. The
    two sets MUST stay disjoint — registering a ``primary_doc`` rewash
    parser requires reconciling it with the sweep first (re-fetch-
    capable rewash, or de-scoping the kind here)."""
    assert SWEPT_DOCUMENT_KINDS.isdisjoint(rewash_filings.registered_specs())


def test_swept_kinds_are_valid_document_kinds() -> None:
    assert SWEPT_DOCUMENT_KINDS <= set(get_args(DocumentKind))


def test_swept_sources_are_valid_manifest_sources() -> None:
    """Couple the drop-list to the manifest's own source vocabulary —
    a renamed/removed source fails here, not as a silent zero-match."""
    assert set(SWEPT_MANIFEST_SOURCES) <= set(get_args(ManifestSource))


def test_swept_sources_are_the_approved_drop_list() -> None:
    """Destruction is opt-in. Widening this set is a spec change —
    the new source's payload consumers must be audited first (see the
    def14a_body exclusion rationale in the spec)."""
    assert SWEPT_MANIFEST_SOURCES == frozenset({"sec_10k", "sec_8k"})


def test_batch_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        sweep_raw_payloads(database_url="postgresql://unused/unused", batch_size=0)


# ---------------------------------------------------------------------------
# Row mapping — swept rows carry NULL payload / byte_count
# ---------------------------------------------------------------------------


def _row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "accession_number": "0000000000-26-000001",
        "document_kind": "primary_doc",
        "payload": "<html>body</html>",
        "byte_count": 18,
        "parser_version": None,
        "fetched_at": datetime(2026, 6, 10, tzinfo=UTC),
        "source_url": "https://www.sec.gov/Archives/x.htm",
    }
    base.update(overrides)
    return base


def test_row_to_document_maps_null_payload_to_none_not_the_string_none() -> None:
    doc = _row_to_document(_row(payload=None, byte_count=None))
    assert doc.payload is None
    assert doc.byte_count is None


def test_row_to_document_maps_live_payload() -> None:
    doc = _row_to_document(_row())
    assert doc.payload == "<html>body</html>"
    assert doc.byte_count == 18


def test_require_payload_raises_on_swept_row() -> None:
    doc = _row_to_document(_row(payload=None, byte_count=None))
    with pytest.raises(RuntimeError, match="swept"):
        doc.require_payload()


# ---------------------------------------------------------------------------
# Rehydrate hash guard — pure interleaving with a fake connection
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def execute(self, sql: str, params: Any = None) -> None:
        del sql, params

    def fetchone(self) -> dict[str, Any] | None:
        return self._row


class _FakeConn:
    """Single-read fake: one dict_row SELECT + recorded UPDATEs.

    Deliberately minimal (prevention-log §mock cursor sequences are
    fragile) — the real SQL paths run in the db-tier test."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.executed: list[tuple[str, Any]] = []

    @contextmanager
    def cursor(self, *, row_factory: Any = None) -> Any:
        del row_factory
        yield _FakeCursor(self._row)

    def execute(self, sql: str, params: Any = None) -> Any:
        self.executed.append((sql, params))

        class _Result:
            rowcount = 1

        return _Result()


_BODY = "<html>10-K body</html>"
_BODY_SHA = hashlib.sha256(_BODY.encode("utf-8")).hexdigest()


def _swept_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "payload": None,
        "payload_sha256": _BODY_SHA,
        "source_url": "https://www.sec.gov/Archives/a.htm",
    }
    base.update(overrides)
    return base


def test_rehydrate_mismatch_fails_loud_and_writes_nothing() -> None:
    conn = _FakeConn(_swept_row())
    with pytest.raises(RawPayloadIntegrityError, match="does not match"):
        rehydrate_raw_document(
            conn,  # type: ignore[arg-type]
            accession_number="a",
            document_kind="primary_doc",
            fetch_text=lambda _url: "<html>SEC silently changed this</html>",
        )
    assert conn.executed == []  # never auto-overwrite on mismatch


def test_rehydrate_match_restores_payload() -> None:
    conn = _FakeConn(_swept_row())
    outcome = rehydrate_raw_document(
        conn,  # type: ignore[arg-type]
        accession_number="a",
        document_kind="primary_doc",
        fetch_text=lambda _url: _BODY,
    )
    assert outcome.status == "restored"
    # Two writes: raw-row restore + manifest compacted->stored.
    assert len(conn.executed) == 2
    restore_sql, restore_params = conn.executed[0]
    assert "payload_swept_at = NULL" in restore_sql
    assert restore_params[0] == _BODY
    manifest_sql, _ = conn.executed[1]
    assert "raw_status = 'stored'" in manifest_sql


def test_rehydrate_noop_when_payload_present() -> None:
    conn = _FakeConn(_swept_row(payload="<html>live</html>"))
    outcome = rehydrate_raw_document(
        conn,  # type: ignore[arg-type]
        accession_number="a",
        document_kind="primary_doc",
        fetch_text=lambda _url: pytest.fail("must not fetch when payload present"),  # type: ignore[arg-type]
    )
    assert outcome.status == "already_present"
    assert conn.executed == []


def test_rehydrate_missing_row_raises() -> None:
    conn = _FakeConn(None)
    with pytest.raises(ValueError, match="no filing_raw_documents row"):
        rehydrate_raw_document(
            conn,  # type: ignore[arg-type]
            accession_number="a",
            document_kind="primary_doc",
            fetch_text=lambda _url: _BODY,
        )


def test_rehydrate_swept_row_without_hash_raises() -> None:
    conn = _FakeConn(_swept_row(payload_sha256=None))
    with pytest.raises(RuntimeError, match="no payload_sha256"):
        rehydrate_raw_document(
            conn,  # type: ignore[arg-type]
            accession_number="a",
            document_kind="primary_doc",
            fetch_text=lambda _url: _BODY,
        )


def test_rehydrate_swept_row_without_source_url_raises() -> None:
    conn = _FakeConn(_swept_row(source_url=None))
    with pytest.raises(RuntimeError, match="no source_url"):
        rehydrate_raw_document(
            conn,  # type: ignore[arg-type]
            accession_number="a",
            document_kind="primary_doc",
            fetch_text=lambda _url: _BODY,
        )
