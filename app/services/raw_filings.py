"""Raw-filings document store.

Single-source-of-truth helper for the ``filing_raw_documents`` table
(migration 107). Every ownership-side ingester (13F, 13D/G, Form 4,
Form 3, DEF 14A) writes the source XML / HTML body here at fetch
time so re-wash workflows can run against stored bodies instead of
re-fetching from SEC.

Operator audit 2026-05-03 found the prior pattern dropped raw bodies
after parsing — meaning a parser bug discovered later forced a full
re-fetch from SEC at 10 req/sec. This module is the foundation for
the audit-and-rewash workflow.

The helper is deliberately small (UPSERT + read by accession +
size-aggregate) so per-ingester wiring is a 2-line change at each
fetch site:

    from app.services.raw_filings import store_raw

    body = provider.fetch_xml(accession)
    store_raw(
        conn,
        accession_number=accession,
        document_kind="form4_xml",
        payload=body,
        parser_version="form4-v1",
        source_url=primary_doc_url,
    )
    parsed = parse_form4(body)
    # ... rest of ingester writes parsed rows ...

Re-wash flow (separate module, future PR):

    for row in iter_raw(conn, document_kind="form4_xml"):
        if row.parser_version == CURRENT_PARSER_VERSION:
            continue  # already on latest parser
        re_parse_and_upsert(conn, row.accession_number, row.payload)

The contract is: ``store_raw`` is idempotent; calling it twice for
the same ``(accession, document_kind)`` overwrites the body and
refreshes ``fetched_at`` + ``parser_version``. The new body is
authoritative — re-fetches from SEC are exactly the case where
overwriting is the right behaviour (the document may have been
amended).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
import psycopg.rows

DocumentKind = Literal[
    "primary_doc",
    "infotable_13f",
    "primary_doc_13dg",
    "form4_xml",
    "form3_xml",
    "form5_xml",
    "def14a_body",
    "nport_xml",
    # G6 / #915 — FINRA bimonthly short interest pipe-delim CSV
    # (sql/151). Keyed by synthetic accession ``FINRA_SI_{YYYYMMDD}``;
    # raw payload preserved per #1168 raw-payload-before-parse contract.
    "finra_short_interest_csv",
    # G6 / #916 — FINRA RegSHO daily short volume pipe-delim TXT
    # (sql/153). Keyed by synthetic accession
    # ``FINRA_REGSHO_{PREFIX}_{YYYYMMDD}`` (6 prefixes × per trade-date).
    "finra_regsho_daily_txt",
    # #1015 — SEC Form 12b-25 late-filing notice body (NT 10-K / NT 10-Q).
    # Small HTML (~3-8 KB); retained, not swept. sql/208.
    "nt_body",
    # #1892 — SEC PRE 14A / PRER14A proposal-signal body. Small HTML;
    # retained, not swept. sql/211.
    "pre14a_body",
]
# submissions.json / companyfacts.json are keyed by CIK, not by SEC
# accession number — they belong in their own per-CIK store, not in
# this per-filing table. Claude PR 808 review (BLOCKING) caught the
# prior overload that smuggled CIKs into the accession_number column.

# Kinds whose payload is WRITE-ONLY — no rewash parser reads them and
# the manifest rebuild re-fetches from EDGAR unconditionally (sql/190).
# ``store_raw`` persists these **born-compacted** (#1615): payload NULL +
# payload_sha256 + payload_swept_at, the same state the #1014 sweep
# produces, so ~22 GB of primary_doc bytes are never stored. MUST stay
# disjoint from the rewash registry (a rewash parser reads stored bodies,
# which a born-compacted row lacks). Canonical here (single source of
# truth); ``raw_payload_retention`` imports it.
SWEPT_DOCUMENT_KINDS: frozenset[DocumentKind] = frozenset({"primary_doc"})
# Future PR adds a sibling ``cik_raw_documents`` table for those.

# Write-only kinds we deliberately KEEP uncompacted because they are
# small enough that born-compaction (#1615) would buy nothing, yet they
# are NOT re-read by any rewash parser either. The operator retention
# rule (#1617, settled-decisions "Raw-payload retention"): a raw-store
# path is legitimate only if its payload is (a) re-read — REWASH, in
# ``rewash_filings.registered_specs()`` — OR (b) housekept-and-negligible
# — SWEPT, ``SWEPT_DOCUMENT_KINDS`` — OR (c) kept-and-negligible with an
# explicit justification, this map. Every ``DocumentKind`` MUST land in
# exactly one of those three buckets; a new unclassified kind fails the
# partition test in ``tests/test_raw_payload_retention.py``. The
# justification per kind was grep-verified at #1617: each was confirmed
# to have NO payload reader (the existence-only ``COUNT(*)`` diagnostics
# in ``ownership_drillthrough`` / ``instruments`` are satisfied by the
# row, not the bytes).
KEPT_NEGLIGIBLE_DOCUMENT_KINDS: dict[DocumentKind, str] = {
    # form5_xml PROMOTED to REWASH in #1731 — _parse_form5 now reuses the stored
    # body on re-drain + rewash_filings registers a Form 5 apply_fn, so it is a
    # payload reader and lives in registered_specs(), not here (the partition
    # test would fail if it stayed).
    # n_port: VERIFIED 2026-06-26 (#1731) — the "rewash re-fetches from EDGAR"
    # behaviour is DESCRIPTIVE (no payload reader wired), NOT a hard design
    # constraint: nport_xml is stored + retained, the filing is immutable per
    # accession, and parse_n_port_payload would produce identical output from the
    # stored bytes. Reuse is DEFERRED (not forbidden): 151 dev rows (~12× smaller
    # than Form 5's already-negligible 1,586) don't justify promoting — promotion
    # needs a full _apply_nport rewash spec (the series + per-holding equity-filter
    # + fund-observation ladder, far heavier than Form 5's upsert_filing mirror).
    # Revisit if the manifest-adapter nport population grows materially.
    "nport_xml": "write-only ~6MB; rewash re-fetches from EDGAR, no payload reader (reuse deferred by volume #1731)",
    # Parsed in-memory at ingest; the bimonthly job re-fetches fresh from
    # FINRA each cadence (finra_short_interest_refresh.py), never from the
    # store. Part of ~92 MB FINRA raw (dev).
    "finra_short_interest_csv": "write-only; steady-state re-fetches from FINRA, no payload reader",
    # Parsed in-memory at ingest; the daily job re-fetches fresh from FINRA
    # (finra_regsho_daily_refresh.py), never from the store.
    "finra_regsho_daily_txt": "write-only; steady-state re-fetches from FINRA, no payload reader",
    # #1015 — Form 12b-25 body. Stored for the #938 raw-before-parse invariant;
    # the sec_nt parser always re-fetches on re-drain (no stored-body reuse
    # wired), so it has no payload reader. Reuse-on-redrain deferred (negligible
    # volume), mirroring nport_xml's #1731 rationale.
    "nt_body": "write-only ~3-8KB; sec_nt re-fetches from EDGAR, no payload reader (reuse deferred by volume #1015)",
    # #1892 — PRE 14A / PRER14A proposal-signal body. Stored for the #938
    # raw-before-parse invariant; the sec_pre14a parser always re-fetches on
    # re-drain (no stored-body reuse wired), mirroring nt_body's rationale.
    "pre14a_body": ("write-only; sec_pre14a re-fetches from EDGAR, no payload reader (reuse deferred by volume #1892)"),
}


@dataclass(frozen=True)
class RawFilingDocument:
    accession_number: str
    document_kind: DocumentKind
    # ``payload`` / ``byte_count`` are None on swept rows (#1014): the
    # retention sweep nulls the bytes after recording payload_sha256;
    # byte_count is GENERATED from octet_length(payload) so it follows.
    payload: str | None
    byte_count: int | None
    parser_version: str | None
    fetched_at: datetime
    source_url: str | None

    def require_payload(self) -> str:
        """Return the payload, raising if this row has been swept.

        Rewash apply-fns call this instead of touching ``payload``
        directly — callers that can tolerate a swept row must check
        ``payload is None`` BEFORE handing the doc over. Explicit
        raise (not assert): production invariant."""
        if self.payload is None:
            raise RuntimeError(
                f"raw payload for accession={self.accession_number} "
                f"kind={self.document_kind} was swept "
                f"(payload_swept_at set); re-fetch via rehydrate before re-parsing"
            )
        return self.payload


def acquire_filing_accession_write_lock(conn: psycopg.Connection[Any], accession_number: str) -> None:
    """Serialise concurrent writers of ONE accession's typed ownership rows
    (#817 — generalises the 13F #1542 lock to Form 3/4/5, DEF 14A and 13D/G
    blockholders).

    Three writer classes target the same accession's typed tables: the live
    manifest drain (``manifest_parsers/*``), the operator rewash
    (``rewash_filings``), and the legacy manual ingesters. After
    ``POST /jobs/sec_rebuild/run`` requeues a parser-version cohort for the live
    worker, an operator-triggered ``scripts/rewash.py`` can target the same
    accession the worker is re-draining; #1274/#1591 also made the live drain
    concurrent. This transaction-scoped advisory lock makes those mutations
    mutually exclusive per accession.

    An advisory lock is cooperative: EVERY writer of the guarded rows MUST call
    this with the identical key (prevention-log "Advisory lock scope vs
    concurrent writers"). It lives inside the once-per-accession write path so a
    future caller cannot forget it (prevention-log "lock acquisition belongs
    inside the function").

    Transaction-scoped (``pg_advisory_xact_lock``): auto-releases on
    COMMIT/ROLLBACK/connection-close. MUST be called inside the same
    non-autocommit transaction as the write, AFTER any SEC fetch (never held
    across network I/O), and BEFORE any count/gate read that feeds a DELETE and
    BEFORE any per-instrument refresh lock (per-accession then per-instrument).

    Release granularity follows the CALLER's commit boundary, and ALL callers
    now commit per accession: rewash + legacy commit per accession, and the live
    manifest worker commits per row in ``_dispatch_rows`` (#1735) — it commits the
    implicit read-tx before the dispatch loop so each parser's
    ``with conn.transaction()`` is a TOP-LEVEL txn, then commits after each row's
    terminal ``transition_status``. So this lock releases at each accession's row
    boundary on every path. Mutual exclusion holds and the uniform-order acyclic
    argument (per-accession lock then per-instrument refresh lock, one accession
    in flight at a time) holds for all callers, so the pre-#1735 rare
    rewash-vs-live batch-txn deadlock no longer arises.

    Own namespace (``ingest_filing_accession``) — distinct from the 13F helper's
    ``ingest_13f_accession``; 13F filings carry their own kinds and keep their
    #1542 serialisation domain. Key derivation mirrors
    :func:`institutional_holdings.acquire_13f_accession_write_lock`."""
    conn.execute(
        "SELECT pg_advisory_xact_lock((hashtextextended('ingest_filing_accession', 0) # hashtextextended(%s, 0)))",
        (accession_number,),
    )


def store_raw(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
    payload: str,
    parser_version: str | None = None,
    source_url: str | None = None,
) -> None:
    """Idempotent UPSERT into ``filing_raw_documents``.

    Re-calling for the same ``(accession_number, document_kind)``
    overwrites the body and refreshes ``fetched_at`` +
    ``parser_version`` + ``source_url``. The new body is treated as
    authoritative — re-fetches from SEC are exactly when
    overwriting is correct (the document may have been amended).

    ``SWEPT_DOCUMENT_KINDS`` (write-only kinds) are stored
    **born-compacted** (#1615): the ``payload`` is hashed server-side
    and the column is written ``NULL`` so the bytes are never persisted.
    Such a row is identical to a #1014-swept row (payload NULL +
    payload_sha256 + payload_swept_at) and is rehydratable from
    ``source_url`` — which is therefore REQUIRED for these kinds.
    """
    if not accession_number:
        raise ValueError("accession_number is required")
    if not payload:
        raise ValueError("payload is required (empty payload would defeat re-wash / leave no hash)")

    if document_kind in SWEPT_DOCUMENT_KINDS:
        # Born-compacted: hash the bytes server-side (byte-identical to the
        # #1014 sweep + the rehydrate verifier, sql/190), store NULL
        # payload. source_url is the ONLY recovery path for a payload-less
        # row, so it is mandatory — the guard below makes EXCLUDED.source_url
        # non-NULL on every conflict, so it can never regress to NULL.
        if not source_url:
            raise ValueError(
                f"source_url is required for write-only kind {document_kind!r} "
                "(born-compacted rows are rehydrated from source_url)"
            )
        conn.execute(
            """
            INSERT INTO filing_raw_documents (
                accession_number, document_kind, payload, payload_sha256,
                payload_swept_at, parser_version, source_url, fetched_at
            ) VALUES (
                %(acc)s, %(kind)s, NULL,
                encode(sha256(convert_to(%(payload)s, 'UTF8')), 'hex'),
                NOW(), %(pv)s, %(url)s, NOW()
            )
            ON CONFLICT (accession_number, document_kind) DO UPDATE SET
                payload = NULL,
                payload_sha256 = encode(sha256(convert_to(%(payload)s, 'UTF8')), 'hex'),
                payload_swept_at = NOW(),
                parser_version = EXCLUDED.parser_version,
                source_url = EXCLUDED.source_url,
                fetched_at = NOW()
            """,
            {
                "acc": accession_number,
                "kind": document_kind,
                "payload": payload,
                "pv": parser_version,
                "url": source_url,
            },
        )
        return

    conn.execute(
        """
        INSERT INTO filing_raw_documents (
            accession_number, document_kind, payload, parser_version,
            source_url, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (accession_number, document_kind) DO UPDATE SET
            payload = EXCLUDED.payload,
            parser_version = EXCLUDED.parser_version,
            source_url = EXCLUDED.source_url,
            fetched_at = NOW(),
            -- #1014: a fresh body invalidates sweep state. The stored
            -- hash belongs to the bytes that were destroyed; letting it
            -- linger would fail a future verify against a legitimately
            -- re-stored (possibly amended) body.
            payload_sha256 = NULL,
            payload_swept_at = NULL
        """,
        (accession_number, document_kind, payload, parser_version, source_url),
    )


def read_raw(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
) -> RawFilingDocument | None:
    """Fetch the raw body for one (accession, kind) pair, or
    ``None`` when the row is missing. Read-only; safe to call
    inside a snapshot_read."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, document_kind, payload, byte_count,
                   parser_version, fetched_at, source_url
            FROM filing_raw_documents
            WHERE accession_number = %s AND document_kind = %s
            """,
            (accession_number, document_kind),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _row_to_document(row)


def stored_body(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
) -> str | None:
    """Return a stored body to REUSE in place of an upstream re-fetch, or
    ``None`` when the caller must fetch.

    ``None`` covers two cases the caller handles identically (fetch):

      * no ``filing_raw_documents`` row yet — first ingest.
      * the row exists but ``payload IS NULL`` — a SWEPT / born-compacted
        kind (#1014 / #1615). Reuse is impossible; the caller rehydrates
        from ``source_url`` (i.e. re-fetches).

    A non-``None`` return is always safe to reuse without a freshness
    check: SEC filings are **immutable once filed** (an amendment is a NEW
    accession with its own number), so for a fixed ``(accession_number,
    document_kind)`` a present body never goes stale — "present" ==
    "fresh". The manifest-worker parsers (#1591) call this BEFORE their
    fetch so a re-drain (parser-version bump → ``sec_rebuild`` resets rows
    to pending) re-parses the stored body instead of re-downloading it.

    Only valid for retained kinds (a payload reader on a SWEPT kind is
    barred by ``tests/test_raw_payload_retention.py``); SWEPT kinds always
    return ``None`` here and fall through to the fetch path."""
    doc = read_raw(conn, accession_number=accession_number, document_kind=document_kind)
    return doc.payload if doc is not None else None


def _row_to_document(row: dict[str, Any]) -> RawFilingDocument:
    """Map a dict_row to the dataclass. ``payload`` / ``byte_count``
    are NULL on swept rows (#1014) — must map to ``None``, never
    ``str(None)`` == the literal string ``"None"`` (prevention-log
    §str(row[N]) coerces SQL NULL)."""
    return RawFilingDocument(
        accession_number=str(row["accession_number"]),
        document_kind=row["document_kind"],
        payload=(str(row["payload"]) if row.get("payload") is not None else None),
        byte_count=(int(row["byte_count"]) if row.get("byte_count") is not None else None),
        parser_version=(str(row["parser_version"]) if row.get("parser_version") is not None else None),
        fetched_at=row["fetched_at"],
        source_url=(str(row["source_url"]) if row.get("source_url") is not None else None),
    )


def iter_raw(
    conn: psycopg.Connection[Any],
    *,
    document_kind: DocumentKind,
    parser_version_not_in: tuple[str, ...] = (),
    batch_size: int = 100,
) -> Iterator[RawFilingDocument]:
    """Yield raw documents of a kind, optionally filtered to those
    NOT already on a current parser version. Used by re-wash
    workflows to walk only the rows that need re-parsing.

    Server-side cursor avoids loading the full set into Python — the
    body column compresses but uncompressed sizes can be hundreds of
    KB per row.
    """
    where = ["document_kind = %s"]
    params: list[Any] = [document_kind]
    if parser_version_not_in:
        # Cover both NULL parser_version and any other value not in
        # the list. Using NOT IN doesn't catch NULLs, so explicit OR.
        placeholders = ",".join(["%s"] * len(parser_version_not_in))
        where.append(f"(parser_version IS NULL OR parser_version NOT IN ({placeholders}))")
        params.extend(parser_version_not_in)
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT accession_number, document_kind, payload, byte_count,
               parser_version, fetched_at, source_url
        FROM filing_raw_documents
        WHERE {where_sql}
        ORDER BY fetched_at DESC, accession_number
    """  # noqa: S608 — where_sql built from hardcoded enum + placeholders
    # Server-side cursor names are session-scoped in psycopg v3, so
    # a hardcoded name would collide between concurrent ``iter_raw``
    # calls on the same connection. Generate a unique name per call.
    # Claude PR 808 review (BLOCKING) caught the prior literal name.
    cursor_name = f"iter_raw_{uuid.uuid4().hex}"
    with conn.cursor(row_factory=psycopg.rows.dict_row, name=cursor_name) as cur:
        cur.itersize = batch_size
        cur.execute(sql, params)  # type: ignore[arg-type]  # f-string composed from closed enum
        for row in cur:
            yield _row_to_document(row)


@dataclass(frozen=True)
class StorageSummary:
    document_kind: DocumentKind
    row_count: int
    total_bytes: int
    avg_bytes: int


def storage_summary(conn: psycopg.Connection[Any]) -> list[StorageSummary]:
    """Per-kind row + byte summary. Drives the operator-visible
    storage chip on the ingest-health page."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT document_kind,
                   COUNT(*) AS row_count,
                   COALESCE(SUM(byte_count), 0) AS total_bytes,
                   COALESCE(AVG(byte_count)::int, 0) AS avg_bytes
            FROM filing_raw_documents
            GROUP BY document_kind
            ORDER BY total_bytes DESC
            """,
        )
        rows = cur.fetchall()
    return [
        StorageSummary(
            document_kind=row["document_kind"],  # type: ignore[arg-type]
            row_count=int(row["row_count"]),  # type: ignore[arg-type]
            total_bytes=int(row["total_bytes"]),  # type: ignore[arg-type]
            avg_bytes=int(row["avg_bytes"]),  # type: ignore[arg-type]
        )
        for row in rows
    ]
