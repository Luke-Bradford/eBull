"""DB integration tests for the #3899 verified sweep and ``verified_body``.

Spec: docs/specs/etl/2026-10-21-3899-def14a-pre14a-body-sweep.md (D0, D2).

The new SQL mechanisms:

1. ``verified_body`` — present / fetched / each unavailable reason;
2. the verified sweep's candidate selection + per-row compare-and-null, including
   the races and the keyset resume.
"""

from __future__ import annotations

import hashlib

import psycopg
import pytest

from app.services.raw_payload_retention import sweep_verified, verified_body
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url  # noqa: F401
from tests.test_raw_payload_retention_db import (
    _IID,
    _manifest_raw_status,
    _raw_row,
    _seed_instrument,
    _seed_manifest,
)

_DEF = "<html>definitive proxy body</html>"
_PRE = "<html>preliminary proxy body</html>"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _url(accession: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/561014/{accession}.htm"


def _seed_live(
    conn: psycopg.Connection[tuple], *, accession: str, kind: str, payload: str, url: str | None = None
) -> None:
    conn.execute(
        """
        INSERT INTO filing_raw_documents (accession_number, document_kind, payload, source_url)
        VALUES (%s, %s, %s, %s)
        """,
        (accession, kind, payload, url or _url(accession)),
    )


def _seed_swept(conn: psycopg.Connection[tuple], *, accession: str, kind: str, body: str) -> None:
    conn.execute(
        """
        INSERT INTO filing_raw_documents
            (accession_number, document_kind, payload, payload_sha256, payload_swept_at, source_url)
        VALUES (%s, %s, NULL, %s, NOW(), %s)
        """,
        (accession, kind, _sha(body), _url(accession)),
    )


def _proxy(conn: psycopg.Connection[tuple], accession: str, *, status: str = "parsed", raw: str = "stored") -> None:
    _seed_manifest(conn, accession=accession, source="sec_def14a", form="DEF 14A", ingest_status=status, raw_status=raw)


@pytest.mark.integration
def test_verified_body_outcomes(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_live(conn, accession="vb-live", kind="def14a_body", payload=_DEF)
    _seed_swept(conn, accession="vb-swept", kind="def14a_body", body=_DEF)
    _seed_live(
        conn,
        accession="vb-index",
        kind="def14a_body",
        payload="<html>an index page</html>",
        url="https://www.sec.gov/Archives/edgar/data/561014/000056101426000001/0000561014-26-000001-index.htm",
    )
    conn.commit()

    def boom(_url: str) -> str | None:
        raise RuntimeError("transport down")

    live = verified_body(conn, accession_number="vb-live", document_kind="def14a_body", fetch_text=boom)
    assert (live.status, live.text, live.sha256) == ("present", _DEF, _sha(_DEF))

    fetched = verified_body(conn, accession_number="vb-swept", document_kind="def14a_body", fetch_text=lambda _u: _DEF)
    assert (fetched.status, fetched.text, fetched.sha256) == ("fetched", _DEF, _sha(_DEF))

    cases = {
        "mismatch": ("vb-swept", lambda _u: "<html>changed</html>"),
        "not_found": ("vb-swept", lambda _u: None),
        "fetch_error": ("vb-swept", boom),
        "no_row": ("vb-absent", boom),
        "index_url": ("vb-index", boom),
    }
    for reason, (accession, fetch_text) in cases.items():
        got = verified_body(conn, accession_number=accession, document_kind="def14a_body", fetch_text=fetch_text)
        assert (got.status, got.reason, got.text) == ("unavailable", reason, None), reason

    # Never writes: the swept row is still swept after a successful fetch.
    payload, sha, swept_at, _ = _raw_row(conn, accession="vb-swept", kind="def14a_body")
    assert payload is None and sha == _sha(_DEF) and swept_at is not None


@pytest.mark.integration
def test_verified_sweep_nulls_only_reproduced_bodies(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)
    # Swept: parsed DEF 14A and parsed PRE 14A whose re-fetch reproduces the bytes;
    # vs-a2's manifest is already 'compacted' and still counts as swept.
    _proxy(conn, "vs-a1")
    _seed_live(conn, accession="vs-a1", kind="def14a_body", payload=_DEF)
    _proxy(conn, "vs-a2", raw="compacted")
    _seed_live(conn, accession="vs-a2", kind="def14a_body", payload=_DEF)
    _seed_manifest(
        conn, accession="vs-a3", source="sec_pre14a", form="PRE 14A", ingest_status="parsed", raw_status="stored"
    )
    _seed_live(conn, accession="vs-a3", kind="pre14a_body", payload=_PRE)
    # Attempted, left live: mismatch / not found / fetch error / race.
    for acc in ("vs-b1", "vs-b2", "vs-b3", "vs-b4"):
        _proxy(conn, acc)
        _seed_live(conn, accession=acc, kind="def14a_body", payload=_DEF)
    # Never selected: tombstoned (not named), pending, failed, split, index URL, 10-K.
    _proxy(conn, "vs-c1", status="tombstoned")
    _seed_live(conn, accession="vs-c1", kind="def14a_body", payload=_DEF)
    _proxy(conn, "vs-c2", status="pending")
    _seed_live(conn, accession="vs-c2", kind="def14a_body", payload=_DEF)
    _proxy(conn, "vs-c3", status="failed")
    _seed_live(conn, accession="vs-c3", kind="def14a_body", payload=_DEF)
    _proxy(conn, "vs-c4", raw="absent")
    _seed_live(conn, accession="vs-c4", kind="def14a_body", payload=_DEF)
    _proxy(conn, "vs-c5")
    _seed_live(
        conn,
        accession="vs-c5",
        kind="def14a_body",
        payload=_DEF,
        url="https://www.sec.gov/Archives/edgar/data/561014/000056101426000005/0000561014-26-000005-index.htm",
    )
    _seed_manifest(conn, accession="vs-c6", source="sec_10k", form="10-K", ingest_status="parsed", raw_status="stored")
    _seed_live(conn, accession="vs-c6", kind="primary_doc", payload="<html>10-K</html>")
    conn.commit()

    fetched: list[str] = []

    def fetch(url: str) -> str | None:
        acc = url.rsplit("/", 1)[-1].removesuffix(".htm")
        fetched.append(acc)
        if acc == "vs-b1":
            return "<html>SEC now serves other bytes</html>"
        if acc == "vs-b2":
            return None
        if acc == "vs-b3":
            raise RuntimeError("transport down")
        if acc == "vs-b4":
            # A concurrent writer replaces the body after the digest was read.
            with psycopg.connect(test_database_url(), autocommit=True) as other:
                other.execute(
                    "UPDATE filing_raw_documents SET payload = %s WHERE accession_number = 'vs-b4'",
                    ("<html>re-stored</html>",),
                )
            return _DEF
        return _PRE if acc == "vs-a3" else _DEF

    dry = sweep_verified(fetch_text=fetch, database_url=test_database_url(), dry_run=True)
    assert dry.eligible_rows == {"def14a_body/parsed": 6, "pre14a_body/parsed": 1}
    assert fetched == []

    summary = sweep_verified(fetch_text=fetch, database_url=test_database_url(), dry_run=False)
    conn.commit()
    assert summary.complete is True
    assert summary.outcomes == {"swept": 3, "mismatch": 1, "not_found": 1, "fetch_error": 1, "race": 1}
    assert sorted(summary.unswept) == [
        ("vs-b1", "def14a_body", "mismatch"),
        ("vs-b2", "def14a_body", "not_found"),
        ("vs-b3", "def14a_body", "fetch_error"),
        ("vs-b4", "def14a_body", "race"),
    ]
    assert sorted(fetched) == ["vs-a1", "vs-a2", "vs-a3", "vs-b1", "vs-b2", "vs-b3", "vs-b4"]

    for acc, kind, body in (
        ("vs-a1", "def14a_body", _DEF),
        ("vs-a2", "def14a_body", _DEF),
        ("vs-a3", "pre14a_body", _PRE),
    ):
        payload, sha, swept_at, _ = _raw_row(conn, accession=acc, kind=kind)
        assert payload is None and sha == _sha(body) and swept_at is not None, acc
        assert _manifest_raw_status(conn, accession=acc) == "compacted", acc
    for acc in ("vs-b1", "vs-b2", "vs-b3", "vs-b4", "vs-c1", "vs-c2", "vs-c3", "vs-c4", "vs-c5"):
        payload, sha, _, _ = _raw_row(conn, accession=acc, kind="def14a_body")
        assert payload is not None and sha is None, acc
    assert _raw_row(conn, accession="vs-c6", kind="primary_doc")[0] is not None
    assert _manifest_raw_status(conn, accession="vs-b1") == "stored"

    # The tombstoned cohort is swept only when named.
    tomb = sweep_verified(
        fetch_text=lambda _u: _DEF, database_url=test_database_url(), statuses=["tombstoned"], dry_run=False
    )
    conn.commit()
    assert tomb.outcomes == {"swept": 1}
    assert _raw_row(conn, accession="vs-c1", kind="def14a_body")[0] is None


@pytest.mark.integration
def test_verified_sweep_max_rows_resumes_from_the_cursor(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)
    for acc in ("vr-1", "vr-2", "vr-3"):
        _proxy(conn, acc)
        _seed_live(conn, accession=acc, kind="def14a_body", payload=_DEF)
    conn.commit()

    # A mismatching first row must not be re-attempted on resume.
    calls: list[str] = []

    def fetch(url: str) -> str | None:
        calls.append(url)
        return "<html>other</html>" if url.endswith("vr-1.htm") else _DEF

    first = sweep_verified(fetch_text=fetch, database_url=test_database_url(), max_rows=1, dry_run=False)
    assert (first.attempted, first.complete, first.last_accession) == (1, False, "vr-1")
    assert first.outcomes == {"mismatch": 1}

    rest = sweep_verified(
        fetch_text=fetch, database_url=test_database_url(), after=str(first.last_accession), dry_run=False
    )
    conn.commit()
    assert (rest.attempted, rest.complete) == (2, True)
    assert rest.outcomes == {"swept": 2}
    assert [u.rsplit("/", 1)[-1] for u in calls] == ["vr-1.htm", "vr-2.htm", "vr-3.htm"]
