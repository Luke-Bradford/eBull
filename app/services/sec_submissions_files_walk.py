"""C1.b — per-CIK ``filings.files[]`` secondary-pages walker (#1027,
rewritten in Stream A PR-B T1.3 #1233 to consume the
``sec_cik_submissions_files_index`` sidecar instead of re-fetching the
primary submissions.json).

The bulk ``submissions.zip`` archive published by SEC contains every
CIK's ``filings.recent`` block (last ~12 months / 1000 most-recent
filings). For deeper history, SEC paginates older filings under
``filings.files[]`` — secondary JSON URLs the bulk archive does NOT
include.

C1.a (bulk submissions ingester, ``sec_submissions_ingest``) reads the
bulk archive's ``recent`` block AND — since Stream A PR-B — populates
``sec_cik_submissions_files_index`` from the same in-memory payload.
C1.b walks the per-CIK page descriptors from the sidecar and fetches
each secondary page over HTTP, bounded by the per-CIK rate budget.

Pre-PR-B, this walker re-fetched the primary submissions.json for every
in-universe CIK JUST to read ``filings.files[]`` again — even though
C1.a already had that data minutes earlier. The sidecar eliminates
those ~5,105 redundant primary fetches (~12 min wall-clock at SEC's
7 req/s budget). Secondary pages are STILL fetched over HTTP (they
are NOT in the bulk archive — confirmed at the spec §0.5 grep proof
referencing this file's pre-PR-B docstring).

Sentinel-row behaviour (Codex v2 BLOCKING + spec §4):
  * CIK with ≥ 1 real sidecar page → walk those pages.
  * CIK with exactly the sentinel ``__no_overflow_pages__`` row → skip
    secondary walk; CIK is "processed with zero overflow" (e.g. AAPL
    whose ``recent`` array fits the 1000-cap).
  * CIK with zero sidecar rows (in-universe, not an agent CIK) →
    fail-closed; surface as a parse-error and continue with the rest
    of the cohort. Indicates an S8 ordering bug or a CIK added to
    universe after S8 ran.
  * CIK in ``KNOWN_FILING_AGENT_CIKS`` → not in the populated set;
    skipped at sidecar-populate time. S14 also skips them at consume
    so URL-construction never targets an agent CIK (would 404 every
    time per ``sec-edgar.md §3.7``).

Spec: docs/proposals/etl/stream-a-run-8-fixes.md v2.3 §1 T1.3 + §13 + §14
(post-Codex-1 re-pass + 3-lens code review 2026-05-24).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import (
    KNOWN_FILING_AGENT_CIKS,
    SecFilingsProvider,
    _normalise_submissions_block,
)
from app.services.filings import _upsert_filing

logger = logging.getLogger(__name__)


# Stream A PR-B T1.3 (#1233): sentinel page-name pattern written by
# the sidecar populate in sec_submissions_ingest._refresh_cik_sidecar.
# Distinguishes "CIK processed; no overflow pages" from "CIK not yet
# populated".
_SIDECAR_SENTINEL_PAGE_NAME: str = "__no_overflow_pages__"


@dataclass
class FilesWalkResult:
    """Per-CIK walk telemetry."""

    ciks_visited: int = 0
    secondary_pages_fetched: int = 0
    filings_upserted: int = 0
    parse_errors: int = 0
    # Stream A PR-B T1.3 (#1233): per-CIK sidecar telemetry.
    # ``ciks_with_no_overflow`` counts CIKs that had only the sentinel
    # row (zero secondary pages — skipped without HTTP). Closed-set
    # bookkeeping per spec §15 to keep ``parse_errors`` from
    # double-counting legitimate "no-overflow" CIKs as errors.
    ciks_with_no_overflow: int = 0
    ciks_with_empty_sidecar: int = 0


def _list_cik_secondary_pages(
    conn: psycopg.Connection[Any],
) -> list[tuple[int, str, str, list[str]]]:
    """Return ``[(instrument_id, cik_padded, symbol, sidecar_pages)]`` for every
    CIK-mapped universe instrument.

    Joins ``external_identifiers`` to ``instruments`` so the writer
    below threads the canonical ticker through to
    ``_normalise_submissions_block`` + ``_upsert_filing`` (Codex review
    BLOCKING for PR #1035, parity with the same fix in PR #1030 / C1.a).

    Stream A PR-B (#1233): also LEFT JOINs ``sec_cik_submissions_files_index``
    and aggregates the per-CIK page list. The aggregate preserves the
    sentinel page name when present so callers can distinguish:

      * ``sidecar_pages == []``                               — empty sidecar (fail-closed)
      * ``sidecar_pages == ['__no_overflow_pages__']``         — processed; no overflow
      * ``sidecar_pages == [<one or more real CIK*-...json>]`` — overflow exists; walk

    NOT NULL-safe: a row in ``sec_cik_submissions_files_index`` is
    always either a sentinel row OR a row with a non-NULL page_name
    matching the regex CHECK (sql/172). ``array_agg`` on a NOT NULL
    column never injects NULL into the aggregate.
    """
    out: list[tuple[int, str, str, list[str]]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ei.instrument_id,
                ei.identifier_value,
                i.symbol,
                COALESCE(
                    array_agg(s.page_name) FILTER (WHERE s.page_name IS NOT NULL),
                    ARRAY[]::TEXT[]
                )
            FROM external_identifiers ei
            JOIN instruments i
              ON i.instrument_id = ei.instrument_id
            LEFT JOIN sec_cik_submissions_files_index s
              -- LPAD is defensive: sec_identity.py:44 normalises CIKs to
              -- 10-digit zero-padded BEFORE writing to external_identifiers
              -- (and the sidecar's own CHECK enforces the 10-digit shape on
              -- the s.cik side), so the LPAD is a no-op for any row written
              -- by current code. Kept against the historical-data case
              -- where pre-sec_identity.py:44 inserts may carry unpadded
              -- values. Negligible perf cost at 12k-CIK cohort (hash-join).
              ON s.cik = LPAD(ei.identifier_value, 10, '0')
            WHERE ei.provider = 'sec'
              AND ei.identifier_type = 'cik'
              AND i.is_tradable = TRUE
            GROUP BY ei.instrument_id, ei.identifier_value, i.symbol
            """,
        )
        for row in cur.fetchall():
            instrument_id, identifier, symbol, sidecar_pages = row
            cik = str(identifier).zfill(10)
            pages_list: list[str] = list(sidecar_pages or [])
            out.append((int(instrument_id), cik, str(symbol or ""), pages_list))
    return out


def walk_files_pages(
    *,
    conn: psycopg.Connection[Any],
) -> FilesWalkResult:
    """Walk ``filings.files[]`` secondary pages for every CIK-mapped
    universe instrument and append discovered filings to
    ``filing_events``.

    Stream A PR-B (#1233): consumes the sidecar
    (``sec_cik_submissions_files_index``) instead of re-fetching each
    CIK's primary ``submissions/CIK<10>.json`` — eliminates ~5,105
    redundant primary HTTP calls per Run-#8.

    Secondary-page fetches over HTTP are UNCHANGED — they were never
    in the bulk archive.
    """
    result = FilesWalkResult()
    targets = _list_cik_secondary_pages(conn)

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        for instrument_id, cik, symbol, sidecar_pages in targets:
            # Agent CIKs are excluded by the populate path
            # (sec_submissions_ingest._refresh_cik_sidecar) so their
            # sidecar_pages list is empty by design. Skip them here
            # silently — they do NOT count toward ciks_visited (the
            # counter reflects "real CIKs we did or attempted work for"
            # per Architect IMPORTANT — pre-PR-B post-review fix).
            if cik in KNOWN_FILING_AGENT_CIKS:
                continue

            result.ciks_visited += 1

            if not sidecar_pages:
                # Empty sidecar for an in-universe CIK that is NOT an
                # agent. Indicates an S8 ordering bug or a CIK added
                # to the universe after S8 ran. Per-CIK log is DEBUG
                # to avoid stderr spam at scale (8.7k CIK cohort — a
                # systemic S8 failure would otherwise log 8,700
                # WARNING lines); a single end-of-walk summary
                # WARNING is emitted below (per Architect IMPORTANT).
                logger.debug(
                    "files walk: empty sidecar for in-universe CIK %s; "
                    "S8 must populate sec_cik_submissions_files_index first",
                    cik,
                )
                result.ciks_with_empty_sidecar += 1
                result.parse_errors += 1
                continue

            if sidecar_pages == [_SIDECAR_SENTINEL_PAGE_NAME]:
                # CIK processed with zero overflow pages (sentinel
                # row). No HTTP fetch needed.
                result.ciks_with_no_overflow += 1
                continue

            for page_name in sidecar_pages:
                # Defensive — the only sentinel allowed is
                # _SIDECAR_SENTINEL_PAGE_NAME; the schema CHECK already
                # rejects any other sentinel-shaped value. A real CIK
                # with overflow pages will never have the sentinel
                # mixed in (the populate path writes one or the other).
                # Skip sentinel rows defensively.
                if page_name == _SIDECAR_SENTINEL_PAGE_NAME:
                    continue
                try:
                    page = provider.fetch_submissions_page(page_name)
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "files walk: secondary fetch failed for CIK %s/%s: %s",
                        cik,
                        page_name,
                        exc,
                    )
                    result.parse_errors += 1
                    continue
                if page is None:
                    continue

                result.secondary_pages_fetched += 1
                try:
                    # ``symbol`` is the canonical ticker (e.g. "AAPL"),
                    # NOT a stringified instrument_id. Threaded from
                    # the universe lookup at the top of the walk so
                    # ``filing_events.raw_payload_json`` carries the
                    # right value. Codex review BLOCKING for PR #1035.
                    filings = _normalise_submissions_block(
                        page,
                        cik_padded=cik,
                        symbol=symbol or cik,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("files walk: normalise failed for %s: %s", page_name, exc)
                    result.parse_errors += 1
                    continue

                for filing in filings:
                    try:
                        with conn.transaction():
                            # ``_upsert_filing`` returns False when
                            # the 10y retention cap (#1233 §4.2) drops
                            # a pre-cutoff filing. Count only accepted
                            # rows.
                            if _upsert_filing(conn, str(instrument_id), "sec", filing):
                                result.filings_upserted += 1
                    except Exception as exc:  # noqa: BLE001
                        logger.debug(
                            "files walk: upsert failed for %s/%s: %s",
                            cik,
                            filing.provider_filing_id,
                            exc,
                        )
                        result.parse_errors += 1

    # End-of-walk SUMMARY warning when ≥ 1 in-universe non-agent CIK
    # had an empty sidecar. Single log line replaces the per-CIK spam
    # that pre-review-v2 emitted (Architect IMPORTANT).
    if result.ciks_with_empty_sidecar > 0:
        logger.warning(
            "files walk: %d in-universe CIK(s) had empty sidecar — "
            "sec_submissions_ingest (S8) must populate "
            "sec_cik_submissions_files_index before S14 runs. "
            "Counters: visited=%d empty=%d no_overflow=%d pages=%d filings=%d errors=%d",
            result.ciks_with_empty_sidecar,
            result.ciks_visited,
            result.ciks_with_empty_sidecar,
            result.ciks_with_no_overflow,
            result.secondary_pages_fetched,
            result.filings_upserted,
            result.parse_errors,
        )

    return result


# Job invoker registered in app/jobs/runtime.py:_INVOKERS.
JOB_SEC_SUBMISSIONS_FILES_WALK = "sec_submissions_files_walk"


def sec_submissions_files_walk_job() -> None:
    """Zero-arg job invoker for the runtime registry.

    Within an orchestrated bootstrap run, validates C1.a's
    rows_written > 0 before walking. Records its own per-run
    archive_result row so D-stages can verify provenance.
    """
    from app.services.bootstrap_preconditions import (
        assert_c1b_preconditions,
        record_archive_result,
    )

    # Find current bootstrap_run.
    run_id: int | None = None
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM bootstrap_runs WHERE status='running' ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            run_id = int(row[0]) if row else None
        if run_id is not None:
            from app.security.master_key import resolve_data_dir

            bulk_dir = resolve_data_dir() / "sec" / "bulk"
            assert_c1b_preconditions(conn, bootstrap_run_id=run_id, bulk_dir=bulk_dir)

    with psycopg.connect(settings.database_url) as conn:
        result = walk_files_pages(conn=conn)
        conn.commit()
    logger.info(
        "sec_submissions_files_walk: ciks=%d pages=%d filings=%d no_overflow=%d empty_sidecar=%d parse_errors=%d",
        result.ciks_visited,
        result.secondary_pages_fetched,
        result.filings_upserted,
        result.ciks_with_no_overflow,
        result.ciks_with_empty_sidecar,
        result.parse_errors,
    )
    if run_id is not None:
        # The walker has already committed its writes; a failure of
        # the post-commit audit row must NOT propagate to the
        # orchestrator (which would mark the stage `error` and a
        # later retry would re-run the walker AND fail
        # ``assert_c1b_preconditions`` because the audit row is
        # missing — permanently blocking C1.b on a transient DB
        # hiccup). PR review WARNING (bot, PR #1038).
        try:
            with psycopg.connect(settings.database_url) as conn:
                record_archive_result(
                    conn,
                    bootstrap_run_id=run_id,
                    stage_key="sec_submissions_files_walk",
                    archive_name="__job__",
                    rows_written=result.filings_upserted,
                    rows_skipped={
                        "parse_errors": result.parse_errors,
                        "no_overflow": result.ciks_with_no_overflow,
                        "empty_sidecar": result.ciks_with_empty_sidecar,
                    },
                )
                conn.commit()
        except Exception as exc:  # noqa: BLE001 — audit must not block stage
            logger.warning(
                "sec_submissions_files_walk: failed to record __job__ audit row (walker already committed): %s",
                exc,
            )
