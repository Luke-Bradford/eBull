"""DB integration tests for the #1014 raw-payload retention sweep.

Spec: docs/specs/etl/2026-06-10-raw-payload-retention-sweep.md.

Two genuinely-new SQL mechanisms, one test each:

1. the chained-CTE sweep batch (eligibility + server-side SHA-256 +
   payload NULL + manifest stored->compacted, idempotent, dry-run);
2. the rehydrate / store_raw interleavings over a swept row (the two
   legal terminal states).

Structural drop-list coverage is pure-logic in
``tests/test_raw_payload_retention.py``.
"""

from __future__ import annotations

import hashlib

import psycopg
import pytest

from app.services.raw_filings import store_raw
from app.services.raw_payload_retention import (
    RawPayloadIntegrityError,
    rehydrate_raw_document,
    sweep_raw_payloads,
)
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url  # noqa: F401

_IID = 561014


def _seed_instrument(conn: psycopg.Connection[tuple], instrument_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            (instrument_id, f"SWP{instrument_id}", f"Raw sweep test {instrument_id}"),
        )


def _seed_manifest(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    source: str,
    form: str,
    ingest_status: str,
    raw_status: str,
    issuer: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_filing_manifest
                (accession_number, cik, form, source, subject_type, subject_id,
                 instrument_id, filed_at, ingest_status, raw_status)
            VALUES (%s, '0000561014', %s, %s, %s, %s, %s,
                    TIMESTAMPTZ '2026-01-01 00:00:00+00', %s, %s)
            """,
            (
                accession,
                form,
                source,
                "issuer" if issuer else "institutional_filer",
                str(_IID) if issuer else "0000561014",
                _IID if issuer else None,
                ingest_status,
                raw_status,
            ),
        )


def _seed_raw(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    kind: str,
    payload: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_raw_documents
                (accession_number, document_kind, payload, source_url)
            VALUES (%s, %s, %s, %s)
            """,
            (accession, kind, payload, f"https://www.sec.gov/Archives/{accession}.htm"),
        )


def _raw_row(conn: psycopg.Connection[tuple], *, accession: str, kind: str) -> tuple:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload, payload_sha256, payload_swept_at, byte_count
            FROM filing_raw_documents
            WHERE accession_number = %s AND document_kind = %s
            """,
            (accession, kind),
        )
        row = cur.fetchone()
        assert row is not None
        return row


def _manifest_raw_status(conn: psycopg.Connection[tuple], *, accession: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT raw_status FROM sec_filing_manifest WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
        assert row is not None
        return row[0]


_BODY_10K = "<html>annual report body — swept-eligible</html>"
_BODY_8K = "<html>8-K body — failed parse, must survive</html>"


@pytest.mark.integration
def test_sweep_nulls_only_parsed_swept_source_primary_docs(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)

    # A — parsed sec_10k primary_doc: THE sweep target.
    _seed_manifest(conn, accession="swp-a", source="sec_10k", form="10-K", ingest_status="parsed", raw_status="stored")
    _seed_raw(conn, accession="swp-a", kind="primary_doc", payload=_BODY_10K)
    # B — sec_8k but ingest_status='failed': keep-on-fail, payload survives.
    _seed_manifest(conn, accession="swp-b", source="sec_8k", form="8-K", ingest_status="failed", raw_status="stored")
    _seed_raw(conn, accession="swp-b", kind="primary_doc", payload=_BODY_8K)
    # C — parsed sec_def14a def14a_body: kind not swept, survives.
    _seed_manifest(
        conn, accession="swp-c", source="sec_def14a", form="DEF 14A", ingest_status="parsed", raw_status="stored"
    )
    _seed_raw(conn, accession="swp-c", kind="def14a_body", payload="<html>proxy</html>")
    # D — parsed sec_13f_hr primary_doc: source not in the drop-list, survives.
    _seed_manifest(
        conn,
        accession="swp-d",
        source="sec_13f_hr",
        form="13F-HR",
        ingest_status="parsed",
        raw_status="stored",
        issuer=False,
    )
    _seed_raw(conn, accession="swp-d", kind="primary_doc", payload="<xml>13f primary</xml>")
    # E — split row: parsed sec_10k but raw_status='absent' while payload
    # exists. Already an invariant violation — the sweep must not
    # compound it (Codex round 1 High).
    _seed_manifest(conn, accession="swp-e", source="sec_10k", form="10-K", ingest_status="parsed", raw_status="absent")
    _seed_raw(conn, accession="swp-e", kind="primary_doc", payload="<html>split row</html>")
    conn.commit()

    # Dry run first: counts only, zero writes.
    dry = sweep_raw_payloads(database_url=test_database_url(), dry_run=True)
    assert dry.dry_run is True
    assert dry.rows_swept == 1
    assert dry.by_source == {"sec_10k": 1}
    assert dry.bytes_reclaimed == len(_BODY_10K.encode("utf-8"))
    assert dry.batches == 0
    conn.commit()
    payload, sha, swept_at, _ = _raw_row(conn, accession="swp-a", kind="primary_doc")
    assert payload == _BODY_10K and sha is None and swept_at is None

    # Real run; batch_size=1 exercises the batching loop.
    summary = sweep_raw_payloads(database_url=test_database_url(), batch_size=1, dry_run=False)
    assert summary.rows_swept == 1
    assert summary.batches == 1
    assert summary.by_source == {"sec_10k": 1}
    assert summary.bytes_reclaimed == len(_BODY_10K.encode("utf-8"))

    conn.commit()  # end the idle tx so reads see the service's commits

    # A swept: payload + generated byte_count NULL, hash matches
    # hashlib over the same text, manifest compacted.
    payload, sha, swept_at, byte_count = _raw_row(conn, accession="swp-a", kind="primary_doc")
    assert payload is None
    assert byte_count is None
    assert swept_at is not None
    assert sha == hashlib.sha256(_BODY_10K.encode("utf-8")).hexdigest()
    assert _manifest_raw_status(conn, accession="swp-a") == "compacted"

    # B/C/D/E untouched.
    for accession, kind in (
        ("swp-b", "primary_doc"),
        ("swp-c", "def14a_body"),
        ("swp-d", "primary_doc"),
        ("swp-e", "primary_doc"),
    ):
        payload, sha, swept_at, _ = _raw_row(conn, accession=accession, kind=kind)
        assert payload is not None, accession
        assert sha is None and swept_at is None, accession
    assert _manifest_raw_status(conn, accession="swp-b") == "stored"
    assert _manifest_raw_status(conn, accession="swp-e") == "absent"

    # Idempotent: a drained DB sweeps 0.
    again = sweep_raw_payloads(database_url=test_database_url(), dry_run=False)
    assert again.rows_swept == 0
    assert again.batches == 0


@pytest.mark.integration
def test_rehydrate_and_store_raw_interleavings_over_a_swept_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)
    _seed_manifest(conn, accession="swp-r", source="sec_10k", form="10-K", ingest_status="parsed", raw_status="stored")
    _seed_raw(conn, accession="swp-r", kind="primary_doc", payload=_BODY_10K)
    conn.commit()

    summary = sweep_raw_payloads(database_url=test_database_url(), dry_run=False)
    assert summary.rows_swept == 1
    conn.commit()

    # Mismatching re-fetch fails loud and leaves the row swept.
    with pytest.raises(RawPayloadIntegrityError):
        rehydrate_raw_document(
            conn,
            accession_number="swp-r",
            document_kind="primary_doc",
            fetch_text=lambda _url: "<html>tampered</html>",
        )
    conn.rollback()
    payload, _, swept_at, _ = _raw_row(conn, accession="swp-r", kind="primary_doc")
    assert payload is None and swept_at is not None

    # Matching re-fetch restores: payload back, swept_at cleared, hash
    # kept (still true), manifest compacted -> stored.
    outcome = rehydrate_raw_document(
        conn,
        accession_number="swp-r",
        document_kind="primary_doc",
        fetch_text=lambda _url: _BODY_10K,
    )
    conn.commit()
    assert outcome.status == "restored"
    payload, sha, swept_at, byte_count = _raw_row(conn, accession="swp-r", kind="primary_doc")
    assert payload == _BODY_10K
    assert swept_at is None
    assert sha == hashlib.sha256(_BODY_10K.encode("utf-8")).hexdigest()
    assert byte_count == len(_BODY_10K.encode("utf-8"))
    assert _manifest_raw_status(conn, accession="swp-r") == "stored"

    # Re-sweep the restored row, then store_raw over it (amended
    # fetch): terminal state must be the live shape with BOTH sweep
    # columns cleared — a stale hash must never linger against new
    # bytes (prevention §ON CONFLICT covers all columns).
    summary = sweep_raw_payloads(database_url=test_database_url(), dry_run=False)
    assert summary.rows_swept == 1
    conn.commit()
    store_raw(
        conn,
        accession_number="swp-r",
        document_kind="primary_doc",
        payload="<html>amended body</html>",
    )
    conn.commit()
    payload, sha, swept_at, _ = _raw_row(conn, accession="swp-r", kind="primary_doc")
    assert payload == "<html>amended body</html>"
    assert sha is None and swept_at is None
    # Manifest stays 'compacted' until a parser outcome writes
    # 'stored' — and the 'compacted'-eligible predicate means the row
    # re-sweeps cleanly:
    assert _manifest_raw_status(conn, accession="swp-r") == "compacted"
    final = sweep_raw_payloads(database_url=test_database_url(), dry_run=False)
    assert final.rows_swept == 1
