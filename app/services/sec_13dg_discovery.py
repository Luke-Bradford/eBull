"""Universe-issuer-CIK-driven SC 13D / SC 13G discovery layer (#1233 PR11).

This module activates the dormant SEC Schedule 13D / 13G blockholder
pipeline by walking the universe of US tradable issuer CIKs and asking
EDGAR full-text search (``efts.sec.gov/LATEST/search-index``) for every
SC 13D / SC 13D/A / SC 13G / SC 13G/A filing against each CIK in a
bounded date window. Each hit becomes:

  1. One row in ``sec_filing_manifest`` (``subject_type='blockholder_filer'``,
     ``instrument_id=NULL`` per the table's CHECK constraint) for the
     existing ``sec_manifest_worker`` + ``manifest_parsers/sec_13dg.py``
     to drain.
  2. One row per universe-member sibling instrument in
     ``sec_13dg_discovery_issuer_hint`` so the parser can later
     cross-validate its CUSIP-resolved instrument_id and fall back to
     the hint for single-class issuers with unresolvable CUSIPs.

Cross-references
----------------
- Spec ``docs/superpowers/specs/2026-05-21-pr11-blockholders-activation-design.md``
  §3.1 (discovery responsibilities) + §3.5 (watermark helper + bootstrap stage).
- Hint table schema: ``sql/159_create_sec_13dg_discovery_issuer_hint.sql``.
- Manifest helper contract: ``app/services/sec_manifest.py:194-300``
  (returns ``None``; unconditional ``ON CONFLICT DO UPDATE``).
- Filing-agent defence: ``app/providers/implementations/sec_edgar.py``
  ``KNOWN_FILING_AGENT_CIKS`` — agent CIKs MUST be excluded from both
  the manifest ``cik`` field AND from ``blockholder_filers`` auto-seeding.
- Retention floor: ``app/services/blockholders.py::blockholders_retention_cutoff``
  — ``max(today - 3y, 2024-12-18)`` (SEC XBRL mandate effective date).

Why one file (not split discovery + ingest module)
--------------------------------------------------
The discovery layer is pure HTTP + SELECT + INSERT and does NOT call
the parser. It enqueues manifest rows; the existing
``sec_manifest_worker`` drains them. Keeping discovery in
``sec_13dg_discovery.py`` keeps the load-bearing live module
(``blockholders.py``) focused on parse + write helpers. Mirrors the
shape of the N-CSR discovery introduced under PR8.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import (
    KNOWN_FILING_AGENT_CIKS,
    SecFilingsProvider,
    _zero_pad_cik,
)
from app.services.blockholders import (
    _upsert_filer,
    blockholders_retention_cutoff,
)
from app.services.sec_manifest import record_manifest_entry

__all__ = [
    "DiscoveryResult",
    "discover_sec_13dg_for_universe",
]


logger = logging.getLogger(__name__)


# Forms enumerated by the discovery sweep (spec §3.1 step 2). SEC EDGAR
# treats originals and amendments as distinct ``form`` values; both are
# in-scope for the discovery layer.
_DISCOVERY_FORMS: tuple[str, ...] = ("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A")

# SEC efts.sec.gov max page size; the provider clamps any larger value
# to 100 server-side. Used both for the per-page fetch and the
# pagination loop termination condition (``len(hits) < _PAGE_SIZE``).
_PAGE_SIZE = 100


# 7-day safety overlap so a steady-state pass after a short outage still
# re-covers any filings whose ``filed_at`` predates the previous run's
# completion (SEC accepts filings 24/7; ``filed_at`` lags wall-clock by
# up to a business day on amendment chains).
_WATERMARK_SAFETY_OVERLAP_DAYS = 7


def _resolve_discovery_startdt(
    conn: psycopg.Connection[Any],
    *,
    mode: Literal["bootstrap", "steady_state"],
    issuer_cik: str | None = None,
) -> date:
    """Pick the discovery window start, with the 3y floor as the hard ceiling.

    Per spec §3.5:

    * ``mode='bootstrap'`` always returns the floor — the bootstrap
      stage performs the full-cohort 3y scan regardless of any prior
      ingest state.
    * ``mode='steady_state'`` narrows to
      ``MAX(blockholder_filings.filed_at) WHERE issuer_cik = ?`` minus
      a 7-day safety overlap, CLAMPED to the floor so a missing
      watermark (issuer with zero prior 13D/G ingest) does not
      silently shrink coverage. An ``issuer_cik`` of ``None`` is
      defensive — also falls back to the floor.

    Watermark source: the raw chain's own
    ``blockholder_filings.filed_at`` keyed by ``issuer_cik``. We do
    NOT consult ``data_freshness_index`` because DFI for
    ``sec_13d``/``sec_13g`` is keyed by
    ``(subject_type='blockholder_filer', subject_id=filer_cik)`` —
    that grain is filer-side and would not match the per-issuer scan
    PR11's discovery performs (Codex 1b HIGH watermark coherence).
    """
    floor = blockholders_retention_cutoff()
    if mode == "bootstrap":
        return floor
    if issuer_cik is None:
        return floor
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(filed_at)::date
            FROM blockholder_filings
            WHERE issuer_cik = %s
              AND filed_at IS NOT NULL
            """,
            (issuer_cik,),
        )
        row = cur.fetchone()
    watermark = row[0] if row and row[0] else floor
    return max(floor, watermark - timedelta(days=_WATERMARK_SAFETY_OVERLAP_DAYS))


@dataclass(frozen=True)
class DiscoveryResult:
    """Counters returned by :func:`discover_sec_13dg_for_universe`.

    Mirrors spec §3.1 step 6 field-for-field so the surrounding
    scheduler job (``sec_blockholders_discovery_job``) can populate a
    ``JobResult`` payload without translation. Frozen so the result is
    safe to hand to logging / metrics without defensive copies.

    Fields
    ------
    issuers_scanned
        Distinct universe issuer CIKs queried (one search-index call
        per issuer; one issuer CIK can map to multiple instruments
        e.g. GOOG + GOOGL on CIK 1652044).
    accessions_discovered
        Total ``hit._source`` records returned across all pages.
    manifest_rows_inserted
        New ``sec_filing_manifest`` rows written (i.e. accession not
        previously present). The helper ``record_manifest_entry`` uses
        an unconditional ``ON CONFLICT DO UPDATE``; insert-vs-update
        is decided by a ``SELECT 1 FROM sec_filing_manifest WHERE
        accession_number = %s`` pre-check inside the same
        ``conn.transaction()`` block.
    manifest_rows_skipped_existing
        Re-discoveries (accession already present). Bumps to confirm
        idempotency without re-fetching.
    filers_upserted
        Total ``blockholder_filers`` UPSERT invocations. Counts every
        seed call, not just net-new rows; the resolver semantic is
        idempotent on existing rows.
    hints_written
        New ``sec_13dg_discovery_issuer_hint`` rows. Idempotent UPSERT
        per the hint table comment in ``sql/159``; this counter
        increments only on NEW ``(accession_number, instrument_id)``
        pairs (detected via the ``RETURNING (xmax = 0)`` predicate),
        NOT on every UPSERT that refreshed ``discovered_at``.
    rows_skipped_outside_cap
        Accessions returned by efts whose ``file_date`` falls outside
        ``blockholders_retention_cutoff()``. Always ``0`` in normal
        operation because the discovery query is already bounded by
        ``startdt = _resolve_discovery_startdt(...)`` which is itself
        clamped to the cutoff. Surfaced explicitly as a tripwire so a
        future helper drift becomes operator-visible.
    elapsed_seconds
        Wall-clock duration of the whole universe sweep
        (``time.monotonic`` delta), useful for the bootstrap stage's
        runtime budget audit.
    """

    issuers_scanned: int
    accessions_discovered: int
    manifest_rows_inserted: int
    manifest_rows_skipped_existing: int
    filers_upserted: int
    hints_written: int
    rows_skipped_outside_cap: int
    elapsed_seconds: float


# ---------------------------------------------------------------------------
# Task 4.3 — universe walker + per-accession atomic ingest
# ---------------------------------------------------------------------------


def _list_universe_issuers(
    conn: psycopg.Connection[Any],
) -> list[tuple[int, str]]:
    """Return ``[(instrument_id, cik), ...]`` for every US tradable issuer.

    The SELECT is keyed on the post-PR1 universe filter
    (``country='US' AND is_tradable=TRUE``) joined to the SEC primary
    CIK row in ``external_identifiers`` (``provider='sec' AND
    identifier_type='cik' AND is_primary=TRUE``). Spec §3.1 step 1.

    Multiple instruments can map to the same CIK (share-class siblings
    GOOG + GOOGL on CIK 1652044, BRK.A + BRK.B on CIK 1067983); the
    caller is responsible for grouping by CIK so each issuer CIK is
    queried at efts.sec.gov only ONCE per sweep regardless of sibling
    count. ORDER BY ``identifier_value`` then ``instrument_id`` so
    siblings sort adjacent and the iteration order is deterministic
    across runs (testability + log readability).
    """
    rows: list[tuple[int, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT i.instrument_id, ei.identifier_value
            FROM instruments i
            INNER JOIN external_identifiers ei
                    ON ei.instrument_id = i.instrument_id
                   AND ei.provider = 'sec'
                   AND ei.identifier_type = 'cik'
                   AND ei.is_primary = TRUE
            WHERE i.country = 'US'
              AND i.is_tradable = TRUE
            ORDER BY ei.identifier_value, i.instrument_id
            """
        )
        for row in cur:
            rows.append((int(row[0]), str(row[1])))
    return rows


def _extract_filer_set(
    cik_list: list[str],
    name_list: list[str],
    issuer_cik: str,
) -> list[tuple[str, str]]:
    """Return ``[(cik_padded, name), ...]`` of filers from one efts hit.

    Spec §3.1 step 3 — defensive filer extraction:

    * Drops the issuer CIK itself (matched on the UNPADDED form;
      compare ``c.lstrip('0') == issuer_unpadded`` to tolerate both
      padded and unpadded forms in ``cik_list``).
    * Drops any known filing-agent CIK (matched on the PADDED form via
      ``_zero_pad_cik`` so a 7-digit ``"1193125"`` filtered out the
      same as the canonical ``"0001193125"``).
    * Deduplicates preserving first-occurrence order (efts can list
      the same filer CIK twice for joint filings; the per-cik UPSERT
      is idempotent but the spec-pinned counter shape depends on a
      stable iteration order).
    * Aligns each filer CIK to its display name positionally:
      ``name_list[i]`` for the CIK at position ``i`` in ``cik_list``,
      falling back to ``f"CIK {cik}"`` when ``name_list`` is shorter
      (rare malformed hits).
    """
    issuer_unpadded = issuer_cik.lstrip("0")
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for idx, raw_cik in enumerate(cik_list):
        if not raw_cik:
            continue
        if raw_cik.lstrip("0") == issuer_unpadded:
            continue
        try:
            padded = _zero_pad_cik(raw_cik)
        except TypeError, ValueError:
            continue
        if padded in KNOWN_FILING_AGENT_CIKS:
            continue
        if padded in seen:
            continue
        seen.add(padded)
        name = name_list[idx] if idx < len(name_list) and name_list[idx] else f"CIK {padded}"
        out.append((padded, name))
    return out


def _ingest_one_accession(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    issuer_cik: str,
    accession: str,
    form: str,
    file_date: date,
    filer_set: list[tuple[str, str]],
) -> tuple[bool, bool, int]:
    """Write one ``(accession, instrument_id)`` pair atomically.

    Returns ``(manifest_inserted, hint_inserted, filers_upserted)``:

    * ``manifest_inserted`` — ``True`` when the row was newly INSERTed
      into ``sec_filing_manifest`` (i.e. accession was previously
      absent), ``False`` on UPSERT-refresh of an existing row.
    * ``hint_inserted`` — ``True`` when the
      ``(accession_number, instrument_id)`` pair was newly INSERTed
      into ``sec_13dg_discovery_issuer_hint`` (detected via the
      ``RETURNING (xmax = 0)`` predicate), ``False`` when only
      ``discovered_at`` was refreshed.
    * ``filers_upserted`` — count of ``_upsert_filer`` calls inside
      this transaction (one per filer in ``filer_set``).

    Atomicity: every write happens inside a single
    ``conn.transaction()`` block so the manifest row never becomes
    worker-visible (``status='pending'``) until the matching hint row
    AND the filer-resolver seed rows are committed. This pins the
    close of the silent-gap window — the worker cannot race ahead of
    the hint write and re-introduce the CUSIP-only fallback for a
    universe-discovered accession (spec §3.1 step 4 atomicity clause).

    Skip semantics: ``filer_set == []`` (issuer-only efts result —
    anomalous; per spec §3.1 step 3 cardinality assertion) logs a
    warning and returns ``(False, False, 0)`` without entering the
    transaction. Writing a manifest row in this case would either need
    a synthetic ``cik`` (violates the schema's "filer of record"
    semantic) or fall back to the issuer's own CIK (would auto-seed
    the issuer into ``blockholder_filers`` which is wrong on both
    counts). Skipping preserves the audit trail without writing a
    nonsense row.
    """
    if not filer_set:
        logger.warning(
            "sec_13dg_discovery: skipping issuer-only efts hit "
            "(accession=%s instrument_id=%d issuer_cik=%s); no non-issuer "
            "non-agent filer CIK found in hit.ciks",
            accession,
            instrument_id,
            issuer_cik,
        )
        return (False, False, 0)

    # Archive-owner CIK = first non-issuer non-agent CIK from the hit.
    # Used as manifest.cik (the parser passes this to
    # ``_archive_file_url`` to build the SEC archive URL) AND as
    # manifest.subject_id (the resolver key for ``blockholder_filer``
    # rows). Per spec §3.1 step 3 "Correct derivation" + §3.1
    # archive-owner-CIK derivation.
    archive_owner_cik = filer_set[0][0]
    source = "sec_13d" if form.startswith("SC 13D") else "sec_13g"
    filed_at_utc = datetime.combine(file_date, datetime.min.time(), tzinfo=UTC)

    manifest_inserted = False
    hint_inserted = False
    filers_upserted = 0

    with conn.transaction():
        # Seed every filer CIK into the resolver BEFORE the manifest
        # row commits. The manifest worker keys on ``blockholder_filers``
        # for the legacy daily-index path; the upsert is idempotent so
        # re-seeding existing rows is cheap (refreshes ``fetched_at``).
        for filer_cik, filer_name in filer_set:
            _upsert_filer(conn, cik=filer_cik, name=filer_name)
            filers_upserted += 1

        # SELECT-1 pre-check inside the same transaction. Required because
        # ``record_manifest_entry`` returns ``None`` and uses an
        # unconditional ``ON CONFLICT DO UPDATE`` (per
        # ``app/services/sec_manifest.py:194-300``); the boolean
        # "this insert is net-new" cannot be inferred from the helper's
        # return value. The pre-check + UPSERT pair is atomic under the
        # surrounding transaction so concurrent discovery passes on the
        # same accession serialize on the PK and the second pass sees
        # ``already_present = True``.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sec_filing_manifest WHERE accession_number = %s",
                (accession,),
            )
            already_present = cur.fetchone() is not None

        record_manifest_entry(
            conn,
            accession,
            cik=archive_owner_cik,
            form=form,
            source=source,
            subject_type="blockholder_filer",
            subject_id=archive_owner_cik,
            instrument_id=None,
            filed_at=filed_at_utc,
            primary_document_url=None,
        )
        manifest_inserted = not already_present

        # Multi-row hint UPSERT keyed on (accession_number, instrument_id)
        # so share-class siblings (GOOG + GOOGL) each get their own hint
        # row for a single discovered accession. ``RETURNING (xmax = 0)``
        # discriminates net-new INSERTs from UPSERT-refreshes — Postgres
        # sets ``xmax = 0`` for an INSERTed tuple and to the deleting
        # transaction id for an UPDATEd tuple, so ``xmax = 0`` is True
        # only when the hint row was net-new.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sec_13dg_discovery_issuer_hint
                    (accession_number, instrument_id, issuer_cik)
                VALUES (%s, %s, %s)
                ON CONFLICT (accession_number, instrument_id)
                DO UPDATE SET
                    discovered_at = NOW(),
                    issuer_cik    = EXCLUDED.issuer_cik
                RETURNING (xmax = 0) AS inserted
                """,
                (accession, instrument_id, issuer_cik),
            )
            returning_row = cur.fetchone()
        hint_inserted = bool(returning_row and returning_row[0])

    return (manifest_inserted, hint_inserted, filers_upserted)


def discover_sec_13dg_for_universe(
    conn: psycopg.Connection[Any],
    *,
    mode: Literal["bootstrap", "steady_state"] = "steady_state",
) -> DiscoveryResult:
    """Universe-CIK-driven SC 13D/G discovery — module entry point.

    Walks the US tradable universe (one efts.sec.gov call per DISTINCT
    issuer CIK regardless of share-class sibling count), pages through
    the search-index in ``_PAGE_SIZE``-row chunks, and writes one
    manifest row + one hint row per universe-member sibling per
    discovered accession.

    Mode dispatch:
      * ``bootstrap`` — full-cohort 3y scan (bootstrap stage usage).
      * ``steady_state`` — per-issuer watermark via
        :func:`_resolve_discovery_startdt`, clamped to the floor.

    Pagination terminates when a page returns fewer than ``_PAGE_SIZE``
    hits (efts always returns exactly ``size`` rows when more exist).

    Returns a :class:`DiscoveryResult` populated with the spec §3.1
    step 6 counters.
    """
    started_at = time.monotonic()

    universe = _list_universe_issuers(conn)

    # Group instruments by CIK so each CIK is queried at efts AT MOST
    # ONCE per sweep (spec §3.1 §3.5 — share-class siblings collapse
    # to a single HTTP query, then fan back out to per-sibling hint rows
    # inside ``_ingest_one_accession``).
    cik_to_instruments: dict[str, list[int]] = {}
    for instrument_id, cik in universe:
        cik_to_instruments.setdefault(cik, []).append(instrument_id)

    enddt = datetime.now(tz=UTC).date()

    issuers_scanned = 0
    accessions_discovered = 0
    manifest_rows_inserted = 0
    manifest_rows_skipped_existing = 0
    filers_upserted_total = 0
    hints_written = 0
    rows_skipped_outside_cap = 0

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        for issuer_cik, sibling_ids in cik_to_instruments.items():
            issuers_scanned += 1
            startdt = _resolve_discovery_startdt(conn, mode=mode, issuer_cik=issuer_cik)

            from_offset = 0
            while True:
                payload = provider.fetch_search_index_json(
                    ciks=issuer_cik,
                    forms=_DISCOVERY_FORMS,
                    startdt=startdt,
                    enddt=enddt,
                    from_offset=from_offset,
                    size=_PAGE_SIZE,
                )
                hits = _extract_hits(payload)
                if not hits:
                    break

                for hit in hits:
                    source = hit.get("_source") or {}
                    if not isinstance(source, dict):
                        continue
                    adsh = source.get("adsh")
                    form = source.get("form")
                    file_date_raw = source.get("file_date")
                    cik_list = source.get("ciks") or []
                    name_list = source.get("display_names") or []
                    if not (
                        isinstance(adsh, str)
                        and isinstance(form, str)
                        and isinstance(file_date_raw, str)
                        and isinstance(cik_list, list)
                        and isinstance(name_list, list)
                    ):
                        continue
                    try:
                        file_date = date.fromisoformat(file_date_raw)
                    except ValueError:
                        continue

                    accessions_discovered += 1

                    # Belt-and-braces cap defense (chokepoint A2). The
                    # discovery query is ALREADY bounded by ``startdt =
                    # _resolve_discovery_startdt(...)`` so this branch
                    # should never fire under correct helper behaviour;
                    # surfacing it via ``rows_skipped_outside_cap``
                    # makes any future helper drift operator-visible
                    # (spec §3.1 step 6 tripwire semantics).
                    if file_date < blockholders_retention_cutoff():
                        rows_skipped_outside_cap += 1
                        continue

                    filer_set = _extract_filer_set(
                        [str(c) for c in cik_list],
                        [str(n) for n in name_list],
                        issuer_cik,
                    )

                    for sibling_iid in sibling_ids:
                        manifest_inserted, hint_inserted, filer_upserts = _ingest_one_accession(
                            conn,
                            instrument_id=sibling_iid,
                            issuer_cik=issuer_cik,
                            accession=adsh,
                            form=form,
                            file_date=file_date,
                            filer_set=filer_set,
                        )
                        if manifest_inserted:
                            manifest_rows_inserted += 1
                        elif filer_set:
                            # Only count "skipped existing" for accessions we
                            # actually attempted to write; pure-skip (no
                            # filer_set) leaves all manifest counters at 0.
                            manifest_rows_skipped_existing += 1
                        if hint_inserted:
                            hints_written += 1
                        filers_upserted_total += filer_upserts

                if len(hits) < _PAGE_SIZE:
                    break
                from_offset += _PAGE_SIZE

    return DiscoveryResult(
        issuers_scanned=issuers_scanned,
        accessions_discovered=accessions_discovered,
        manifest_rows_inserted=manifest_rows_inserted,
        manifest_rows_skipped_existing=manifest_rows_skipped_existing,
        filers_upserted=filers_upserted_total,
        hints_written=hints_written,
        rows_skipped_outside_cap=rows_skipped_outside_cap,
        elapsed_seconds=time.monotonic() - started_at,
    )


def _extract_hits(payload: dict[str, object] | None) -> list[dict[str, Any]]:
    """Pull the ``hits.hits[]`` envelope out of an efts response.

    Defensive: efts wraps results in ``{"hits": {"hits": [...]}}``.
    Returns ``[]`` on any missing key, non-list inner array, or
    non-dict element so the surrounding pagination loop terminates
    cleanly on an empty / malformed page.
    """
    if payload is None:
        return []
    hits_envelope = payload.get("hits")
    if not isinstance(hits_envelope, dict):
        return []
    inner = hits_envelope.get("hits")
    if not isinstance(inner, list):
        return []
    return [h for h in inner if isinstance(h, dict)]
