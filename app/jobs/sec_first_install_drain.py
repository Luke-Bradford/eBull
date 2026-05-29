"""First-install drain (#871).

Issue #871 / spec §"Mode 1 — First-install drain".

Operator-triggered job for new installs and explicit drain requests.

Three paths:

  - **filing_events seed (default fast path, #1044)**: SELECT every
    issuer row from ``filing_events`` (already populated by C1.a +
    C1.b in the bulk path) and seed ``sec_filing_manifest`` from the
    cached payloads. No HTTP. ~15s for ~12k issuer events vs ~21min
    of per-CIK fetches.
  - **In-universe HTTP fallback**: per-CIK submissions.json for every
    CIK in the tradable universe. Used for non-issuer subjects
    (institutional + blockholder filers) and the slow-connection
    bypass where the bulk path was skipped.
  - **Hybrid local-zip (#1277, bootstrap-only)**: when
    ``use_bulk_zip=True`` and S7's local ``submissions.zip`` is
    present + provenance-verified, PRIMARY ``CIK<10>.json`` reads
    route to the on-disk archive while secondary
    ``CIK<10>-submissions-<NNN>.json`` pages still hit HTTP (those
    are NOT in the bulk archive — see
    ``app/services/sec_submissions_files_walk.py:16-23``).

Crash-resume: idempotent — re-run drains the remaining pending /
unknown subjects. ``record_manifest_entry`` UPSERTs, so duplicate
discovery is a no-op.

Pagination: when a CIK's recent array doesn't cover its full history
(``has_more_in_files=True``), the drain follows the secondary pages
to capture older filings. The per-CIK steady-state poll (#870) does
NOT follow pagination; this is the dedicated batch-throughput path.
"""

from __future__ import annotations

import json
import logging
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from app.jobs.sec_atom_fast_lane import ResolvedSubject
from app.providers.implementations.sec_submissions import (
    HttpGet,
    check_freshness,
    parse_submissions_page,
)
from app.services.bootstrap_preconditions import BootstrapPhaseSkipped
from app.services.bootstrap_state import (
    BootstrapStageCancelled,
    resolve_progress_context,
    set_stage_processed,
    set_stage_target,
)
from app.services.institutional_holdings import thirteen_f_retention_cutoff
from app.services.manifest_parsers.sec_n_csr import n_csr_retention_cutoff
from app.services.processes.bootstrap_cancel_signal import (
    active_bootstrap_stage_key,
    bootstrap_cancel_requested,
)
from app.services.sec_manifest import is_amendment_form, map_form_to_source, record_manifest_entry
from app.services.sec_submissions_zip import PRIMARY_SUBMISSIONS_URL_RE

logger = logging.getLogger(__name__)

# #1277 — primary submissions URL pattern. Hybrid HttpGet routes these
# to the local ``submissions.zip`` archive; everything else (secondary
# pages, other paths) falls through to the real HTTP transport.
# Secondary pages ``CIK<10>-submissions-<NNN>.json`` are NOT in the bulk
# archive (canonical reference: app/services/sec_submissions_files_walk.py:16-23),
# so the regex deliberately excludes them.
# #1340 — single source of truth for the primary-URL contract, shared with
# S23's ``ZipBackedArchiveFetcher`` so the two consumers cannot drift.
_PRIMARY_SUBMISSIONS_URL_RE = PRIMARY_SUBMISSIONS_URL_RE


def _make_zip_http_get(
    archive_path: Path,
    *,
    fallback_http_get: HttpGet,
) -> tuple[HttpGet, zipfile.ZipFile]:
    """Return ``(hybrid HttpGet, open ZipFile)`` for caller-managed lifecycle.

    Routing:

    * Primary submissions URL ``data.sec.gov/submissions/CIK<10>.json``
      → read entry ``CIK<10>.json`` from ``archive_path``;
      ``(200, bytes)`` on hit, ``(404, b"")`` on miss (drain treats as
      ``not_found`` via the existing ``status != 200`` guards).
    * Anything else (secondary ``CIK<10>-submissions-<NNN>.json``
      pages, other paths) → delegate to ``fallback_http_get`` unchanged.

    Caller owns close — return tuple's second element is the open
    ZipFile, wrapped in caller's ``try/finally``. Lazy-callable contracts
    were rejected at spec time (NIT-1 fold of #1277) because they hide
    lifecycle and complicate test fixtures.

    Spec: docs/proposals/etl/1277-s16-local-zip.md §3.1.
    """
    zf = zipfile.ZipFile(archive_path)

    def _get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        match = _PRIMARY_SUBMISSIONS_URL_RE.match(url)
        if match is None:
            return fallback_http_get(url, headers)
        entry_name = f"CIK{match.group(1)}.json"
        try:
            with zf.open(entry_name) as fh:
                return (200, fh.read())
        except KeyError:
            return (404, b"")
        except (zipfile.BadZipFile, OSError) as exc:
            # Read-time zip error — corrupt member, bad CRC, truncated
            # archive surfaced mid-read. Don't abort S16 over one bad
            # entry: delegate to the real HTTP transport so the drain
            # still seeds this CIK's manifest. Codex 2 IMPORTANT fold.
            logger.warning(
                "first-install drain: zip entry %s unreadable (%s) — delegating to HTTP",
                entry_name,
                exc,
            )
            return fallback_http_get(url, headers)

    return _get, zf


@dataclass(frozen=True)
class DrainStats:
    ciks_processed: int
    ciks_skipped: int
    secondary_pages_fetched: int
    manifest_rows_upserted: int
    errors: int
    # Count of DISTINCT (subject_type, subject_id, source) triples
    # observed during the drain. Each triple corresponds to one
    # ``data_freshness_index`` row — the inline seed in
    # ``record_manifest_entry`` UPSERTs by triple (#956), so the
    # number of distinct triples == the number of scheduler rows
    # the drain caused to exist. Pre-#959 this was the count from
    # a separate post-drain ``seed_scheduler_from_manifest`` bulk
    # call, which was redundant with the inline seed and is now
    # removed.
    scheduler_rows_seeded: int = 0
    # #1044: count of manifest rows seeded from filing_events without
    # any HTTP. Bulk path populates this; fallback path leaves it 0.
    rows_seeded_from_filing_events: int = 0


def seed_manifest_from_filing_events(
    conn: psycopg.Connection[Any],
    *,
    seeded_triples: set[tuple[str, str, str]] | None = None,
) -> int:
    """Seed ``sec_filing_manifest`` from already-ingested ``filing_events``.

    Reads every issuer-scoped filing_events row joined to the best
    available SEC CIK identifier (primary preferred), calls
    ``record_manifest_entry`` per row.
    The bulk path (C1.a + C1.b) populates filing_events ahead of this
    drain stage, so the manifest can be seeded without ANY HTTP
    requests — replaces the ~21min per-CIK loop with a ~15s table
    walk. (#1044)

    Returns the number of manifest rows upserted.

    No-op if filing_events is empty (e.g. fallback mode bypassed the
    bulk path entirely); the caller should follow up with the per-CIK
    HTTP drain in that case.

    #959: when ``seeded_triples`` is supplied, every
    ``record_manifest_entry`` call also records its
    ``(subject_type, subject_id, source)`` triple into the set so the
    caller can report a faithful ``scheduler_rows_seeded`` count
    without re-querying ``data_freshness_index``.
    """
    upserted = 0
    skipped_unmapped_form = 0
    skipped_no_cik = 0
    skipped_non_issuer_source = 0
    with conn.cursor() as cur:
        # LATERAL JOIN with LIMIT 1 picks the highest-priority CIK
        # mapping per instrument: ORDER BY is_primary DESC selects the
        # primary mapping when one exists, else any non-primary one.
        # Codex pre-push MED for #1044 — naive `is_primary = TRUE`
        # would drop valid rows whose only SEC CIK mapping isn't
        # flagged primary.
        # Per-#1117 PR-B: filing_events fans out per share-class
        # sibling under sql/144, so two rows can carry the same
        # accession. sec_filing_manifest.accession_number is PK
        # (entity-level), so the seeder dedups by accession picking
        # the canonical (lowest instrument_id) sibling. Parser
        # fan-out at parse time covers per-sibling observations;
        # the manifest itself is one row per accession.
        cur.execute(
            """
            SELECT DISTINCT ON (fe.provider_filing_id)
                fe.instrument_id,
                fe.filing_date,
                fe.filing_type,
                fe.provider_filing_id,
                fe.primary_document_url,
                cik_map.identifier_value AS cik
            FROM filing_events fe
            JOIN LATERAL (
                SELECT identifier_value
                FROM external_identifiers ei
                WHERE ei.instrument_id = fe.instrument_id
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                ORDER BY ei.is_primary DESC, ei.external_identifier_id ASC
                LIMIT 1
            ) cik_map ON TRUE
            WHERE fe.provider = 'sec'
            ORDER BY fe.provider_filing_id, fe.instrument_id
            """
        )
        rows = cur.fetchall()
    for instrument_id, filing_date, filing_type, provider_filing_id, primary_doc_url, cik_raw in rows:
        if cik_raw is None or not str(cik_raw).strip():
            skipped_no_cik += 1
            continue
        # Use the canonical map_form_to_source from sec_manifest so
        # this drain stays in sync with the rest of the manifest
        # writers (Codex pre-push HIGH for #1044).
        source = map_form_to_source(filing_type) if filing_type else None
        if source is None:
            skipped_unmapped_form += 1
            continue
        # subject_type='issuer' is hard-coded below — must therefore
        # exclude non-issuer-scoped sources. 13F-HR / N-PORT / N-CSR
        # are filer-scoped (subject_id = filer CIK, instrument_id NULL)
        # and need a different code path. Filing_events does carry
        # 13F-HR / NPORT-P rows for instruments that ALSO have those
        # filings on file (e.g. fund families), so we must filter
        # them out at the seed boundary. PR #1051 review WARNING for
        # #1044.
        if source in ("sec_13f_hr", "sec_n_port", "sec_n_csr"):
            skipped_non_issuer_source += 1
            continue
        # Accession lives in fe.provider_filing_id — that's the
        # authoritative column. raw_payload_json mirrors it but can
        # legitimately drift on legacy rows. Codex pre-push MED.
        accession = provider_filing_id
        if not accession:
            continue
        cik_padded = str(cik_raw).strip().zfill(10)
        # filing_date is a date — record_manifest_entry takes filed_at
        # as datetime. Anchor at UTC midnight; the precise time is
        # carried only by the per-CIK HTTP path's accept_timestamp.
        filed_at = datetime.combine(filing_date, datetime.min.time(), tzinfo=UTC)
        # Use canonical is_amendment_form so DEFA14A and other non-/A
        # amendment proxies are flagged correctly. Codex pre-push MED.
        is_amendment = is_amendment_form(filing_type or "")
        try:
            record_manifest_entry(
                conn,
                str(accession),
                cik=cik_padded,
                form=str(filing_type or ""),
                source=source,
                subject_type="issuer",
                subject_id=str(int(instrument_id)),
                instrument_id=int(instrument_id),
                filed_at=filed_at,
                primary_document_url=primary_doc_url,
                is_amendment=is_amendment,
                # #1343 — defer 10-K Item 1 + 8-K item bodies to first user
                # view: seed these 'deferred' so the post-bootstrap
                # catch_up_on_boot manifest worker (iter_pending /
                # iter_retryable select only pending/failed) never eagerly
                # drains the body backlog. Other issuer sources stay
                # 'pending' for the worker to fetch eagerly.
                initial_ingest_status="deferred" if source in ("sec_10k", "sec_8k") else "pending",
            )
            upserted += 1
            if seeded_triples is not None:
                seeded_triples.add(("issuer", str(int(instrument_id)), source))
        except ValueError as exc:
            logger.debug(
                "seed_manifest_from_filing_events: rejected accession=%s: %s",
                accession,
                exc,
            )
    if skipped_unmapped_form or skipped_no_cik or skipped_non_issuer_source:
        logger.info(
            "seed_manifest_from_filing_events: upserted=%d skipped_no_cik=%d "
            "skipped_unmapped_form=%d skipped_non_issuer_source=%d",
            upserted,
            skipped_no_cik,
            skipped_unmapped_form,
            skipped_non_issuer_source,
        )
    return upserted


def _iter_in_universe_subjects(
    conn: psycopg.Connection[Any],
) -> Iterable[ResolvedSubject]:
    """Stream every (cik, subject) triple in the universe.

    Issuers from instrument_sec_profile, then institutional_filers,
    then blockholder_filers. Ordered for deterministic test runs;
    crash-resume relies on the manifest UPSERT idempotency, not
    iteration ordering.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik, instrument_id FROM instrument_sec_profile WHERE cik IS NOT NULL ORDER BY instrument_id"
        )
        for cik, instrument_id in cur.fetchall():
            yield (
                ResolvedSubject(
                    subject_type="issuer",
                    subject_id=str(int(instrument_id)),
                    instrument_id=int(instrument_id),
                ),
                cik,
            )  # type: ignore[misc]

        cur.execute("SELECT cik FROM institutional_filers ORDER BY filer_id")
        for (cik,) in cur.fetchall():
            yield (
                ResolvedSubject(
                    subject_type="institutional_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
                cik,
            )  # type: ignore[misc]

        cur.execute("SELECT cik FROM blockholder_filers ORDER BY filer_id")
        for (cik,) in cur.fetchall():
            yield (
                ResolvedSubject(
                    subject_type="blockholder_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
                cik,
            )  # type: ignore[misc]


def run_first_install_drain(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    use_bulk_zip: bool = False,
    archive_path: Path | None = None,
    follow_pagination: bool = True,
    max_subjects: int | None = None,
) -> DrainStats:
    """Drain manifest seeding from every CIK in the universe.

    ``use_bulk_zip=True`` (since #1277) wraps ``http_get`` with a
    hybrid that routes PRIMARY ``CIK<10>.json`` URLs to the local
    ``submissions.zip`` at ``archive_path``, while secondary
    ``CIK<10>-submissions-<NNN>.json`` pages still hit the real
    transport (those are not in the bulk archive). ``archive_path``
    must be provided and exist; otherwise the caller is responsible
    for downgrading ``use_bulk_zip`` to ``False`` and providing the
    HTTP-only ``http_get`` (the scheduler invoker handles this).

    ``max_subjects=None`` drains everything; pass an integer to bound
    a sample run. ``follow_pagination`` controls whether secondary
    submissions pages are fetched when ``has_more_in_files``.
    """
    zip_handle: zipfile.ZipFile | None = None
    effective_http_get = http_get
    if use_bulk_zip:
        if archive_path is None or not archive_path.exists():
            # Defensive — caller (scheduler invoker) should have
            # downgraded use_bulk_zip already. Belt-and-suspenders:
            # treat as HTTP-only rather than failing the stage.
            logger.warning(
                "first-install drain: use_bulk_zip=True but archive_path=%s — falling back to HTTP",
                archive_path,
            )
        else:
            # Codex 2 IMPORTANT fold: corrupt / truncated archives on
            # disk must downgrade rather than fail the stage. The
            # scheduler invoker has no signal to pre-detect this
            # (size + provenance pass but the inner zip CRC may be
            # bad). On open failure, log + fall back to HTTP.
            try:
                effective_http_get, zip_handle = _make_zip_http_get(archive_path, fallback_http_get=http_get)
            except (zipfile.BadZipFile, OSError) as exc:
                logger.warning(
                    "first-install drain: archive %s unreadable (%s) — falling back to HTTP",
                    archive_path,
                    exc,
                )
                # zip_handle stays None; effective_http_get stays as the
                # caller's http_get; per-CIK loop walks HTTP unchanged.

    try:
        return _run_first_install_drain_inner(
            conn,
            http_get=effective_http_get,
            follow_pagination=follow_pagination,
            max_subjects=max_subjects,
        )
    finally:
        if zip_handle is not None:
            zip_handle.close()


def _bulk_already_seeded_13f(
    conn: psycopg.Connection[Any],
    *,
    cik: str,
    filed_before: datetime,
) -> bool:
    """#1337 P2 — true iff bulk (S8) has already seeded this institutional
    filer's FULL parser-admissible 13F-HR window into ``sec_filing_manifest``.

    Coverage proof (do not weaken without re-deriving):
      * The ``sec_13f_hr`` manifest-worker parser tombstones any 13F-HR
        whose ``period_of_report`` predates ``thirteen_f_retention_cutoff``
        (the 8-quarter floor). So only 13F-HRs with
        ``period_of_report >= cutoff_quarter_end`` ever yield holdings.
      * A 13F-HR is filed AFTER its quarter ends, so ``filed_at`` >=
        ``period_of_report`` for every accession.
      * Therefore a seeded ``sec_13f_hr`` row with ``filed_at < cutoff``
        necessarily has ``period_of_report < cutoff`` — it is itself
        parser-INADMISSIBLE. Its presence proves S8's primary ``recent``
        block reached back PAST the admissible window, so every
        admissible 13F-HR for this CIK is already in the manifest.
      * This bridge ("one pre-cutoff row ⇒ admissible window fully
        present") relies on SEC's submissions ``recent`` block being the
        contiguous newest-first slice S8 walked — which holds for a
        first-install bulk write (S8 ran on a clean DB before this
        drain). The CALLER therefore gates this on bootstrap context
        (``progress_ctx is not None``); steady-state / manual / retry
        runs, where stray rows of mixed provenance could falsely satisfy
        the EXISTS, never take the skip and fall through to the full
        HTTP walk.

    Scoped to ``source='sec_13f_hr'`` (NOT just ``subject_type``) so an
    unrelated old row can't prove 13F coverage, and so the query rides
    ``idx_manifest_cik(cik, source, filed_at)`` directly. ``filed_at <``
    is strict: a filing exactly on the cutoff quarter-end could be
    admissible and must not count as proof of pre-window coverage.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM sec_filing_manifest
            WHERE cik = %(cik)s
              AND source = 'sec_13f_hr'
              AND filed_at < %(filed_before)s
            LIMIT 1
            """,
            {"cik": cik, "filed_before": filed_before},
        )
        return cur.fetchone() is not None


def _run_first_install_drain_inner(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    follow_pagination: bool,
    max_subjects: int | None,
) -> DrainStats:
    """Body of the drain. Split out so ``run_first_install_drain`` can
    own the hybrid ZipFile lifecycle via ``try/finally`` without
    re-indenting the existing loop. The split is purely structural —
    semantics match pre-#1277.
    """
    ciks_processed = 0
    ciks_skipped = 0
    secondary_pages_fetched = 0
    manifest_upserted = 0
    errors = 0
    # #959: track distinct (subject_type, subject_id, source) triples
    # observed inline so we can report scheduler_rows_seeded without
    # the redundant post-drain bulk seed.
    inline_seeded_triples: set[tuple[str, str, str]] = set()

    # Fast path (#1044): if filing_events has rows for the SEC
    # provider (populated by C1.a + C1.b in the bulk path), seed the
    # manifest from that table without any HTTP requests. The per-CIK
    # HTTP loop below still runs to cover non-issuer subjects
    # (institutional_filer + blockholder_filer) which filing_events
    # doesn't carry. Newer issuer filings published since the bulk
    # snapshot are picked up by the steady-state per-CIK poll
    # (#870), not this drain — running another full HTTP sweep here
    # would defeat the perf gain.
    rows_seeded_from_filing_events = seed_manifest_from_filing_events(conn, seeded_triples=inline_seeded_triples)
    manifest_upserted += rows_seeded_from_filing_events
    if rows_seeded_from_filing_events > 0:
        logger.info(
            "first-install drain: seeded %d manifest rows from filing_events (no HTTP)",
            rows_seeded_from_filing_events,
        )

    skip_issuer_http = rows_seeded_from_filing_events > 0

    # #1273 PR2 — long-pole stage instrumentation (S16 streaming).
    # MUST land here (not at function top) because `fast_path_seeded`
    # is only known after `seed_manifest_from_filing_events` returns
    # (Codex 1 IMPORTANT-2 fold). Streaming-style: target_count=None,
    # fingerprint only — `_iter_in_universe_subjects` is a streaming
    # cursor with no upfront cohort size. cadenced emits live in the
    # for-loop below; final emit after exit.
    import time as _time  # local — drain module otherwise time-free

    progress_ctx = resolve_progress_context()
    if progress_ctx is not None:
        # Spec §4 — booleans rendered lowercase `true`/`false`
        # (Python's default str(bool) is `True`/`False`); Codex 2 NIT.
        fingerprint = (
            f"max_subjects={max_subjects if max_subjects is not None else 'unbounded'};"
            f"follow_pagination={str(follow_pagination).lower()};"
            f"fast_path_seeded={str(skip_issuer_http).lower()}"
        )
        set_stage_target(
            run_id=progress_ctx.run_id,
            stage_key=progress_ctx.stage_key,
            target_count=None,
            cohort_fingerprint=fingerprint,
        )
    _last_progress_emit = _time.monotonic()

    # PR3d #1064 — cancel-poll cadence. The drain iterates ~12k CIKs
    # at 10 req/s, ~21 minutes wall-clock. Polling for the bootstrap
    # cancel signal every 50 CIKs keeps observation latency under
    # ~5 seconds without flooding the DB. Outside a bootstrap dispatch
    # the helper short-circuits to False (contextvar unset), so
    # scheduled / manual triggers of this job are unaffected.
    _CANCEL_POLL_EVERY_N = 50

    # #1337 P2 — institutional-filer fast-path cutoff. Computed once:
    # ``thirteen_f_retention_cutoff`` returns the 8-quarter-back quarter
    # END date; anchor it at UTC midnight as a tz-aware datetime so the
    # ``filed_at`` (TIMESTAMPTZ) comparison in ``_bulk_already_seeded_13f``
    # is offset-safe (no naive-datetime drift).
    _filer_skip_cutoff = datetime.combine(thirteen_f_retention_cutoff(), datetime.min.time(), tzinfo=UTC)
    _in_bootstrap = progress_ctx is not None

    for n, (subject, cik) in enumerate(_iter_in_universe_subjects(conn)):  # type: ignore[misc]
        if n % _CANCEL_POLL_EVERY_N == 0 and bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"first-install drain cancelled by operator after {ciks_processed} CIKs",
                stage_key=active_bootstrap_stage_key() or "",
            )
        if max_subjects is not None and ciks_processed >= max_subjects:
            break
        # #1044 fast-path: when filing_events seeded the issuer manifest
        # rows already, skip the per-CIK HTTP fetch for issuers. Non-
        # issuer subjects (institutional_filer + blockholder_filer)
        # still need the HTTP fetch — filing_events only carries
        # universe-mapped instruments.
        if skip_issuer_http and subject.subject_type == "issuer":
            ciks_skipped += 1
            continue

        # #1337 P2 — institutional-filer fast-path. When the bulk S8
        # writer already seeded this filer's full admissible 13F-HR
        # window (proven by a pre-cutoff ``sec_13f_hr`` manifest row,
        # see ``_bulk_already_seeded_13f``), skip the per-CIK HTTP walk
        # — the ~55-60 min critical-path win on a fresh install.
        #
        # Gated on ``_in_bootstrap``: the coverage proof relies on S8
        # having written the contiguous newest-first ``recent`` block
        # on a clean DB before this stage. Outside a bootstrap dispatch
        # (steady-state / manual / cron), manifest rows are of mixed
        # provenance and a stray pre-cutoff row could falsely satisfy
        # the EXISTS, so we never skip there — full HTTP walk, unchanged.
        #
        # blockholder_filer is deliberately NOT skipped: there is no
        # validated 13D/G analogue of the 13F 8-quarter retention proof,
        # so skipping would be unsound the moment that cohort is
        # non-empty. Conservative by construction, not merely "empty
        # today".
        if _in_bootstrap and subject.subject_type == "institutional_filer":
            if _bulk_already_seeded_13f(conn, cik=cik, filed_before=_filer_skip_cutoff):
                ciks_skipped += 1
                continue

        try:
            delta = check_freshness(
                http_get,
                cik=cik,
                last_known_filing_id=None,  # full drain — no watermark
            )
        except Exception as exc:
            logger.warning("first-install drain: check_freshness raised for cik=%s: %s", cik, exc)
            errors += 1
            continue

        ciks_processed += 1
        if not delta.new_filings:
            ciks_skipped += 1
        # #1273 PR2 — streaming-style cadenced emit. No cohort size
        # known up-front; emit every 30s wall-clock with the running
        # ciks_processed counter.
        if progress_ctx is not None:
            _now = _time.monotonic()
            if _now - _last_progress_emit > 30:
                set_stage_processed(
                    run_id=progress_ctx.run_id,
                    stage_key=progress_ctx.stage_key,
                    processed_count=ciks_processed,
                )
                _last_progress_emit = _now

        for row in delta.new_filings:
            if row.source is None:
                continue
            try:
                record_manifest_entry(
                    conn,
                    row.accession_number,
                    cik=row.cik,
                    form=row.form,
                    source=row.source,
                    subject_type=subject.subject_type,  # type: ignore[arg-type]
                    subject_id=subject.subject_id,
                    instrument_id=subject.instrument_id,
                    filed_at=row.filed_at,
                    accepted_at=row.accepted_at,
                    primary_document_url=row.primary_document_url,
                    is_amendment=row.is_amendment,
                )
                manifest_upserted += 1
                inline_seeded_triples.add((subject.subject_type, subject.subject_id, row.source))
            except ValueError as exc:
                logger.warning(
                    "first-install drain: rejected accession=%s for cik=%s: %s",
                    row.accession_number,
                    cik,
                    exc,
                )

        # Secondary-page pagination for full history
        if follow_pagination and delta.has_more_in_files:
            secondary_pages_fetched += _drain_secondary_pages(
                conn,
                http_get=http_get,
                cik=cik,
                subject=subject,
                seeded_triples=inline_seeded_triples,
            )

    # #959: post-#956 every ``record_manifest_entry`` call already
    # inline-seeds the (subject_type, subject_id, source) triple via
    # ``seed_freshness_for_manifest_row``. The post-drain bulk
    # ``seed_scheduler_from_manifest`` call (#937 / PR #957) was a
    # redundant second pass — on first-install drain it UPSERTed the
    # same ~12k * ~10 forms ≈ 120k rows the inline path had already
    # written. Dropping it.
    #
    # Counter wiring: every drain write path (per-CIK loop above,
    # ``seed_manifest_from_filing_events`` fast path, and
    # ``_drain_secondary_pages``) is threaded with the
    # ``inline_seeded_triples`` set so the counter stays accurate
    # across all paths (Codex pre-push round 1 — without these
    # threads the counter materially under-reports when the fast
    # path or pagination fires).
    scheduler_rows_seeded = len(inline_seeded_triples)

    # #1273 PR2 — final operator-progress emit on exit.
    if progress_ctx is not None:
        set_stage_processed(
            run_id=progress_ctx.run_id,
            stage_key=progress_ctx.stage_key,
            processed_count=ciks_processed,
        )

    logger.info(
        "first-install drain: ciks=%d skipped=%d errors=%d secondary_pages=%d upserted=%d scheduler_seeded=%d",
        ciks_processed,
        ciks_skipped,
        errors,
        secondary_pages_fetched,
        manifest_upserted,
        scheduler_rows_seeded,
    )
    return DrainStats(
        ciks_processed=ciks_processed,
        ciks_skipped=ciks_skipped,
        secondary_pages_fetched=secondary_pages_fetched,
        manifest_rows_upserted=manifest_upserted,
        errors=errors,
        scheduler_rows_seeded=scheduler_rows_seeded,
        rows_seeded_from_filing_events=rows_seeded_from_filing_events,
    )


def _drain_secondary_pages(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    cik: str,
    subject: ResolvedSubject,
    seeded_triples: set[tuple[str, str, str]] | None = None,
) -> int:
    """Walk every ``filings.files[]`` page for one CIK.

    The primary submissions.json carries up to ~1000 most-recent
    filings inline. Older filings live in secondary pages named in
    ``files[]``. The drain follows them all once per CIK.

    Returns the count of pages fetched.

    #959: when ``seeded_triples`` is supplied, every
    ``record_manifest_entry`` call records its triple so the caller's
    ``scheduler_rows_seeded`` counter stays accurate.
    """
    cik_padded = cik.zfill(10)
    primary_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    primary_status, primary_body = http_get(primary_url, _drain_headers())
    if primary_status != 200:
        return 0
    try:
        primary_payload = json.loads(primary_body)
    except json.JSONDecodeError:
        return 0

    files = (primary_payload.get("filings", {}) or {}).get("files", []) or []
    pages = 0
    for page_meta in files:
        name = page_meta.get("name") if isinstance(page_meta, dict) else None
        if not name:
            continue
        page_url = f"https://data.sec.gov/submissions/{name}"
        status, body = http_get(page_url, _drain_headers())
        if status != 200:
            continue
        rows, _ = parse_submissions_page(body, cik=cik_padded)
        pages += 1
        for row in rows:
            if row.source is None:
                continue
            try:
                record_manifest_entry(
                    conn,
                    row.accession_number,
                    cik=row.cik,
                    form=row.form,
                    source=row.source,
                    subject_type=subject.subject_type,  # type: ignore[arg-type]
                    subject_id=subject.subject_id,
                    instrument_id=subject.instrument_id,
                    filed_at=row.filed_at,
                    accepted_at=row.accepted_at,
                    primary_document_url=row.primary_document_url,
                    is_amendment=row.is_amendment,
                )
                if seeded_triples is not None:
                    seeded_triples.add((subject.subject_type, subject.subject_id, row.source))
            except ValueError as exc:
                logger.warning(
                    "first-install drain (secondary): rejected accession=%s: %s",
                    row.accession_number,
                    exc,
                )
    return pages


def _drain_headers() -> dict[str, str]:
    return {
        "User-Agent": "eBull research/1.0 contact@example.com",
        "Accept-Encoding": "gzip, deflate",
    }


# ---------------------------------------------------------------------------
# #1174 — N-CSR / N-CSRS fund-scoped bootstrap drain (T8 deferred from #1171).
#
# Walks distinct trust CIKs from ``cik_refresh_mf_directory`` (populated by
# the S25 ``mf_directory_sync`` bootstrap stage) + enqueues last-2-years
# N-CSR + N-CSRS accessions per trust to ``sec_filing_manifest`` so the
# manifest worker can drain them via the #1171 fund-metadata parser.
#
# Subject identity: ``subject_type='institutional_filer'`` with
# ``subject_id=trust_cik`` and ``instrument_id=None``. Matches N-PORT
# precedent + the manifest CHECK constraint ``chk_manifest_issuer_has_instrument``
# (institutional_filer rows must have ``instrument_id IS NULL``). The
# parser fans out per-(series, class) at parse time when writing
# fund_metadata_observations.
#
# This stage is OUT-OF-BAND vs the existing first-install drain at
# :167 (which explicitly excludes sec_n_csr from the issuer-scoped seed).
# See spec docs/superpowers/specs/2026-05-15-n-csr-bootstrap-drain.md.
# ---------------------------------------------------------------------------


_N_CSR_DRAIN_CANCEL_POLL_EVERY_N = 50
_N_CSR_SOURCE: str = "sec_n_csr"


@dataclass(frozen=True)
class NCsrDrainStats:
    trusts_processed: int
    trusts_skipped: int
    secondary_pages_fetched: int
    manifest_rows_upserted: int
    accessions_outside_horizon: int
    errors: int


@dataclass(frozen=True)
class _TrustDrainOutcome:
    rows_upserted: int
    accessions_outside_horizon: int
    secondary_pages_fetched: int
    skipped: bool
    errored: bool


def _iter_trust_ciks(conn: psycopg.Connection[Any]) -> Iterable[str]:
    """Yield distinct trust_cik values for trusts with at least one
    universe-mapped class (#1176).

    Filters via INNER JOIN against ``external_identifiers
    (provider='sec', identifier_type='class_id')`` — only trusts whose
    mf-directory class_id resolves to an in-universe instrument are
    walked. Non-universe trusts can never produce parseable
    fund-metadata observations (the parser would fetch their iXBRL +
    tombstone with ``INSTRUMENT_NOT_IN_UNIVERSE``), so enqueueing them
    burns SEC rate-budget + parser wall-clock for guaranteed-tombstone
    rows.

    Atomicity rationale: ``refresh_mf_directory`` populates
    ``cik_refresh_mf_directory`` AND
    ``external_identifiers (identifier_type='class_id')`` in the same
    transaction (``app/services/mf_directory.py``), so there is no
    race window where a class_id appears in the directory before its
    ext-id row lands. The JOIN is therefore exhaustive of every
    drain-relevant trust at any consistent read snapshot.

    Deterministic ORDER BY for crash-resume + test reproducibility;
    the manifest UPSERT idempotency carries actual safety across
    re-runs.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT mf.trust_cik
            FROM cik_refresh_mf_directory mf
            JOIN external_identifiers ei
              ON ei.identifier_value = mf.class_id
             AND ei.provider = 'sec'
             AND ei.identifier_type = 'class_id'
             AND ei.is_primary = TRUE
            WHERE mf.trust_cik IS NOT NULL
            ORDER BY mf.trust_cik
            """
        )
        for (cik,) in cur.fetchall():
            yield str(cik)


def _within_horizon(filed_at: datetime, cutoff: datetime) -> bool:
    """True iff ``filed_at`` is on or after ``cutoff``. Both must be tz-aware."""
    return filed_at >= cutoff


def _enqueue_n_csr_for_trust(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    trust_cik: str,
    cutoff: datetime,
) -> _TrustDrainOutcome:
    """Fetch ``submissions.json`` for ``trust_cik`` (primary + secondary
    pages), filter to ``source='sec_n_csr'`` rows within horizon, enqueue
    manifest rows with ``subject_type='institutional_filer'`` +
    ``subject_id=trust_cik`` + ``instrument_id=None``.

    Returns counters; caller aggregates into ``NCsrDrainStats``. 404 maps
    to ``skipped=True``; fetch / parse exception maps to ``errored=True``;
    neither bubbles.
    """
    rows_upserted = 0
    outside_horizon = 0
    pages_fetched = 0

    # Primary page via the shared freshness helper — it returns a
    # ``FreshnessDelta`` already filtered to ``source='sec_n_csr'`` plus
    # the ``filings.files[]`` page names for secondary pagination.
    try:
        delta = check_freshness(
            http_get,
            cik=trust_cik,
            last_known_filing_id=None,
            sources={_N_CSR_SOURCE},  # type: ignore[arg-type]
        )
    except Exception as exc:  # noqa: BLE001 — bubbling errored counter
        logger.warning("bootstrap_n_csr_drain: check_freshness raised cik=%s: %s", trust_cik, exc)
        return _TrustDrainOutcome(
            rows_upserted=0,
            accessions_outside_horizon=0,
            secondary_pages_fetched=0,
            skipped=False,
            errored=True,
        )

    primary_empty = not delta.new_filings
    no_pagination = not (delta.has_more_in_files and delta.files_pages)

    # 404 returns an empty delta + no pagination (sec_submissions.py:246).
    # Treat that as "skipped" so the caller distinguishes 'observed
    # but no in-horizon work' from a clean 0-N-CSR trust.
    if primary_empty and no_pagination:
        return _TrustDrainOutcome(
            rows_upserted=0,
            accessions_outside_horizon=0,
            secondary_pages_fetched=0,
            skipped=True,
            errored=False,
        )

    for row in delta.new_filings:
        # Defensive — check_freshness already filtered, but the contract
        # is "rows with the right source"; we re-assert at the
        # write boundary.
        if row.source != _N_CSR_SOURCE:
            continue
        if not _within_horizon(row.filed_at, cutoff):
            outside_horizon += 1
            continue
        try:
            record_manifest_entry(
                conn,
                row.accession_number,
                cik=row.cik,
                form=row.form,
                source=_N_CSR_SOURCE,  # type: ignore[arg-type]
                subject_type="institutional_filer",
                subject_id=trust_cik,
                instrument_id=None,
                filed_at=row.filed_at,
                accepted_at=row.accepted_at,
                primary_document_url=row.primary_document_url,
                is_amendment=row.is_amendment,
            )
            rows_upserted += 1
        except ValueError as exc:
            logger.warning(
                "bootstrap_n_csr_drain: rejected accession=%s for trust=%s: %s",
                row.accession_number,
                trust_cik,
                exc,
            )

    # Secondary-page walk — full traversal, filtered at row level.
    # Spec §3.3: submissions.json is keyed by accession (not date), so
    # secondary pages may carry both in-horizon and out-of-horizon rows;
    # we accept the full walk + row-filter.
    if delta.has_more_in_files and delta.files_pages:
        for name in delta.files_pages:
            page_url = f"https://data.sec.gov/submissions/{name}"
            try:
                status, body = http_get(page_url, _drain_headers())
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "bootstrap_n_csr_drain: secondary fetch raised cik=%s page=%s: %s",
                    trust_cik,
                    name,
                    exc,
                )
                continue
            if status != 200:
                logger.info(
                    "bootstrap_n_csr_drain: secondary non-200 cik=%s page=%s status=%s",
                    trust_cik,
                    name,
                    status,
                )
                continue
            try:
                page_rows, _ = parse_submissions_page(body, cik=trust_cik)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "bootstrap_n_csr_drain: secondary parse raised cik=%s page=%s: %s",
                    trust_cik,
                    name,
                    exc,
                )
                continue
            pages_fetched += 1
            for row in page_rows:
                # Explicit source filter — parse_submissions_page does NOT
                # filter, so we must apply ``source='sec_n_csr'`` per
                # spec §3.3 (Codex 1a WARNING).
                if row.source != _N_CSR_SOURCE:
                    continue
                if not _within_horizon(row.filed_at, cutoff):
                    outside_horizon += 1
                    continue
                try:
                    record_manifest_entry(
                        conn,
                        row.accession_number,
                        cik=row.cik,
                        form=row.form,
                        source=_N_CSR_SOURCE,  # type: ignore[arg-type]
                        subject_type="institutional_filer",
                        subject_id=trust_cik,
                        instrument_id=None,
                        filed_at=row.filed_at,
                        accepted_at=row.accepted_at,
                        primary_document_url=row.primary_document_url,
                        is_amendment=row.is_amendment,
                    )
                    rows_upserted += 1
                except ValueError as exc:
                    logger.warning(
                        "bootstrap_n_csr_drain: secondary rejected accession=%s trust=%s page=%s: %s",
                        row.accession_number,
                        trust_cik,
                        name,
                        exc,
                    )

    return _TrustDrainOutcome(
        rows_upserted=rows_upserted,
        accessions_outside_horizon=outside_horizon,
        secondary_pages_fetched=pages_fetched,
        skipped=False,
        errored=False,
    )


def bootstrap_n_csr_drain(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
) -> NCsrDrainStats:
    """Walk fund-trust CIKs from ``cik_refresh_mf_directory`` + enqueue
    N-CSR + N-CSRS accessions per trust to ``sec_filing_manifest``
    within the 730d retention window (#1233 §4.12 / PR8).

    Pre-condition: ``class_id_mapping_ready`` capability (S25
    ``mf_directory_sync`` populates ``cik_refresh_mf_directory``). Raises
    ``BootstrapPhaseSkipped`` if the directory is empty (manual-trigger
    guard — the capability gate catches this in the normal bootstrap
    path, but operators can trigger this stage directly).

    Cancel-cooperative: polls ``bootstrap_cancel_requested()`` every
    ``_N_CSR_DRAIN_CANCEL_POLL_EVERY_N`` trusts; raises
    ``BootstrapStageCancelled`` on observed cancel (cancel raises
    instead of returning stats — caller must wrap in ``try``).

    Subject identity at every ``record_manifest_entry`` call site:
    ``subject_type='institutional_filer'`` + ``subject_id=trust_cik`` +
    ``instrument_id=None``. The parser fans out per-(series, class) at
    parse time.

    Retention cap (PR8): cutoff resolved via ``n_csr_retention_cutoff``
    in ``app/services/manifest_parsers/sec_n_csr.py`` so this drain
    shares the single source of truth with the manifest-worker pre-
    fetch gate. The previous ``horizon_days`` parameter is removed —
    pre-cap deep dives use the parser gate's tombstone path (spec §8
    acceptance #6 N-CSR exception).
    """
    # Entry guard — manual-trigger before S25 has ever fired.
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cik_refresh_mf_directory")
        row = cur.fetchone()
        directory_count = int(row[0]) if row else 0
    if directory_count == 0:
        raise BootstrapPhaseSkipped("class_id_mapping_ready unsatisfied — cik_refresh_mf_directory empty")

    cutoff = n_csr_retention_cutoff()
    trusts_processed = 0
    trusts_skipped = 0
    secondary_pages_fetched = 0
    manifest_rows_upserted = 0
    accessions_outside_horizon = 0
    errors = 0

    for n, trust_cik in enumerate(_iter_trust_ciks(conn)):
        if n % _N_CSR_DRAIN_CANCEL_POLL_EVERY_N == 0 and bootstrap_cancel_requested():
            raise BootstrapStageCancelled(
                f"bootstrap_n_csr_drain cancelled by operator after {trusts_processed} trusts",
                stage_key=active_bootstrap_stage_key() or "",
            )

        outcome = _enqueue_n_csr_for_trust(
            conn,
            http_get=http_get,
            trust_cik=trust_cik,
            cutoff=cutoff,
        )

        trusts_processed += 1
        if outcome.errored:
            errors += 1
            continue
        if outcome.skipped:
            trusts_skipped += 1
            continue
        manifest_rows_upserted += outcome.rows_upserted
        accessions_outside_horizon += outcome.accessions_outside_horizon
        secondary_pages_fetched += outcome.secondary_pages_fetched

    logger.info(
        "bootstrap_n_csr_drain: trusts=%d skipped=%d errors=%d secondary_pages=%d upserted=%d outside_horizon=%d",
        trusts_processed,
        trusts_skipped,
        errors,
        secondary_pages_fetched,
        manifest_rows_upserted,
        accessions_outside_horizon,
    )

    return NCsrDrainStats(
        trusts_processed=trusts_processed,
        trusts_skipped=trusts_skipped,
        secondary_pages_fetched=secondary_pages_fetched,
        manifest_rows_upserted=manifest_rows_upserted,
        accessions_outside_horizon=accessions_outside_horizon,
        errors=errors,
    )
