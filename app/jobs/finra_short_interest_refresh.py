"""FINRA bimonthly short interest refresh (#915 — Phase 6 PR 11).

Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md.
Plan: docs/superpowers/plans/2026-05-18-finra-bimonthly-short-interest-plan.md.

ScheduledJob body. Per-fire flow:

  1. Build the preloaded ``symbol → instrument_id`` resolver
     (mirror G12 ``build_preloaded_subject_resolver``).
  2. Enumerate candidate settlement dates (15th + last-business-day per
     month, within the backfill window).
  3. Read sec_filing_manifest for already-parsed FINRA accessions.
  4. Compute targets = (candidates - already_parsed) ∪ revision_window,
     where revision_window = candidates[-2:] — the two most-recent
     candidates are always re-probed so FINRA in-place revisions
     (revisionFlag='Y') don't get masked.
  5. For each anchor in targets:
       a. ``_fetch_designated_file`` resolves the anchor to the
          settlement date FINRA actually DESIGNATED, walking the probe
          back up to ``_MAX_ANCHOR_WALKBACK_DAYS`` when the CDN does
          not serve the weekend-adjusted date (US market holidays —
          e.g. Good Friday 2022-04-15 → designated 2022-04-14).
          Nothing served in range = benign skip; other errors =
          per-file failure. Anchors younger than
          ``_DISSEMINATION_LAG_DAYS`` get a single probe with no
          walk-back — there a 403/404 means "not disseminated yet".
       b. Empty-file guard: 0 bytes → per-file failure.
       c. Phase 1: raw_filings.store_raw(...) + conn.commit() —
          raw payload durable BEFORE parse (#1168).
       d. Phase 2: ``with conn.transaction():`` wraps
          ingest_settlement_file. Clean exit commits observations +
          _current + manifest atomically. Exception triggers
          automatic rollback; raw payload stays durable.
  6. Match-rate WARNING log if < 50% (universe drift / FINRA shape
     regression sentinel).
  7. RuntimeError on partial failure so _tracked_job records
     job_runs.status='failure' (mirror G12 partial-failure contract).
"""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg

from app.providers.implementations.finra_short_interest import (
    FinraNotFound,
    FinraShortInterestProvider,
)
from app.services import raw_filings
from app.services.finra_short_interest_ingest import (
    SettlementIngestStats,
    build_preloaded_symbol_resolver,
    ingest_settlement_file,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FinraRefreshStats:
    settlement_files: list[SettlementIngestStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int:
        return sum(s.rows_upserted for s in self.settlement_files)

    @property
    def total_parsed(self) -> int:
        return sum(s.rows_parsed for s in self.settlement_files)

    @property
    def total_resolved(self) -> int:
        return sum(s.rows_resolved for s in self.settlement_files)

    @property
    def failed_files(self) -> int:
        return sum(1 for s in self.settlement_files if s.failed)


# Holiday walk-back bound (#2234). The settlement calendar is
# DESIGNATED by FINRA, not derivable — Rule 4560(a) requires reports
# "no later than the second business day after the reporting settlement
# date designated by FINRA", and FINRA publishes the designated dates
# as per-year tables rather than a formula. Every designated date
# observed is the 15th / last calendar day walked back to the preceding
# BUSINESS day, and ``_walk_back_to_weekday`` only knows about
# weekends, so US market holidays land the probe on a date the CDN does
# not serve.
#
# The bound is structural rather than a guess at the longest market
# closure: the two anchors in a month are >=13 days apart, so 5 days of
# walk-back can never cross into the adjacent half-month and mis-attribute
# one settlement date's file to the other's anchor. Every anchor from
# 2021-07 onward resolves within it — see the #2234 PR's full-population
# calendar scan.
_MAX_ANCHOR_WALKBACK_DAYS: int = 5

# Dissemination lag (#2234). FINRA publishes on a delay: Rule 4560(a)
# puts the member's report due "no later than the second business day
# after the reporting settlement date", and FINRA's own published
# schedule runs PUBLICATION about a week after that due date (the
# short-interest reporting-dates table pairs, e.g., a Nov 18 due date
# with a Nov 25 publication date). Inside that window a 403 means "not
# disseminated yet" and walking the probe back would spend
# _MAX_ANCHOR_WALKBACK_DAYS requests a day rediscovering a file that
# does not exist. Past it, a 403 means the date was never designated —
# which is the case worth probing for.
_DISSEMINATION_LAG_DAYS: int = 15


def _is_disseminated(anchor: date, now: datetime) -> bool:
    """Has FINRA had long enough to publish a file for ``anchor``?"""
    return (now.date() - anchor).days >= _DISSEMINATION_LAG_DAYS


def _walk_back_to_weekday(d: date) -> date:
    """If ``d`` falls on Saturday/Sunday, walk BACK to the prior Friday.

    FINRA publishes ``shrt{YYYYMMDD}.csv`` keyed by the last business
    day of the half-month, not the calendar day.

    **Federal holidays are NOT handled here** — this helper is pure
    weekend arithmetic. The holiday case is handled at fetch time by
    ``_fetch_designated_file``, which walks the probe back further when
    the CDN does not serve the derived date. Deriving holidays instead
    would need a US market calendar dependency, which
    settled-decisions #532 (minimal-dependency posture) gates; probing
    needs no dependency and is validated against the file's own
    ``settlementDate`` column at parse time.
    """
    while d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        d -= timedelta(days=1)
    return d


def _settlement_dates_to_fetch(
    now: datetime,
    backfill_window_days: int = 400,
) -> list[date]:
    """Enumerate business-day-adjusted (year, month, 15) +
    (year, month, last_business_day) settlement dates falling within
    ``[now - backfill_window_days, now]``. Sorted ASC.
    """
    earliest = (now - timedelta(days=backfill_window_days)).date()
    today = now.date()
    out: set[date] = set()
    y, m = earliest.year, earliest.month
    while (y, m) <= (today.year, today.month):
        mid = _walk_back_to_weekday(date(y, m, 15))
        last = _walk_back_to_weekday(date(y, m, calendar.monthrange(y, m)[1]))
        for d in (mid, last):
            if earliest <= d <= today:
                out.add(d)
        m += 1
        if m > 12:
            m = 1
            y += 1
    return sorted(out)


def _already_parsed_settlement_dates(conn: psycopg.Connection[Any]) -> set[date]:
    """Read manifest for FINRA short-interest rows with
    ``ingest_status='parsed'``; return the parsed settlement_date set
    (derived from the synthetic accession ``FINRA_SI_{YYYYMMDD}``).
    """
    out: set[date] = set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number
            FROM sec_filing_manifest
            WHERE source = 'finra_short_interest'
              AND ingest_status = 'parsed'
            """
        )
        for (accession,) in cur.fetchall():
            if not accession.startswith("FINRA_SI_"):
                continue
            tail = accession[len("FINRA_SI_") :]
            try:
                out.add(datetime.strptime(tail, "%Y%m%d").date())
            except ValueError:
                continue
    return out


def _fetch_designated_file(
    provider: FinraShortInterestProvider,
    anchor: date,
    *,
    allow_walkback: bool,
    skip_dates: frozenset[date] = frozenset(),
) -> tuple[date, bytes] | None:
    """Resolve ``anchor`` to the settlement date FINRA actually designated.

    Probes ``anchor`` first, then walks back one calendar day at a time
    up to ``_MAX_ANCHOR_WALKBACK_DAYS``. Returns ``(designated_date,
    payload)`` for the first date the CDN serves, or ``None`` when
    nothing in range is served (not yet published, before the archive
    floor, or already ingested under a shifted date).

    ``allow_walkback=False`` restricts this to a single probe. The job
    passes that for anchors younger than ``_DISSEMINATION_LAG_DAYS``,
    where a 403/404 means "not published yet" rather than "not the
    designated date". Walking back there would burn a request per day
    per anchor to re-discover a file that simply does not exist yet.

    Non-``FinraNotFound`` errors propagate; the caller records them as a
    per-file failure.
    """
    probe = anchor
    attempts = _MAX_ANCHOR_WALKBACK_DAYS + 1 if allow_walkback else 1
    for _ in range(attempts):
        if probe in skip_dates:
            return None
        try:
            return probe, provider.fetch_settlement_file(probe)
        except FinraNotFound:
            probe -= timedelta(days=1)
    return None


def _compute_targets(
    candidate_dates: list[date],
    already_parsed: set[date],
) -> list[date]:
    """Subtract parsed dates; UNION with revision window (the two most-
    recent candidates). Returns sorted ASC.
    """
    sorted_candidates = sorted(candidate_dates)
    revision_window = set(sorted_candidates[-2:]) if sorted_candidates else set()
    return sorted((set(candidate_dates) - already_parsed) | revision_window)


def run_finra_short_interest_refresh(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
    backfill_window_days: int = 400,
    provider: FinraShortInterestProvider | None = None,
) -> FinraRefreshStats:
    """Per-fire orchestration. See module docstring for the flow.

    Pre-conditions:
      - ``conn`` is a working DB connection (autocommit OR open-txn);
        the job owns ALL commit/rollback calls inside this body.
      - Test callers MAY inject ``provider`` (e.g. a fake) for test
        isolation.

    Raises ``RuntimeError`` on ``failed_files > 0`` so the caller's
    ``_tracked_job`` records ``job_runs.status='failure'``. Successful
    files commit BEFORE the raise — partial work is durable.
    """
    now_ = now or datetime.now(UTC)
    provider_ = provider if provider is not None else FinraShortInterestProvider()

    resolver = build_preloaded_symbol_resolver(conn)
    candidate_dates = _settlement_dates_to_fetch(now_, backfill_window_days)
    already_parsed = _already_parsed_settlement_dates(conn)
    targets = _compute_targets(candidate_dates, already_parsed)

    ingest_run_id = uuid4()
    stats_list: list[SettlementIngestStats] = []

    revision_window = set(sorted(candidate_dates)[-2:])
    # A holiday-shifted date already ingested must not be re-downloaded
    # every fire just because its unshifted anchor keeps failing to
    # resolve.
    skip_dates = frozenset(already_parsed - revision_window)

    for anchor in targets:
        # Inside the revision window the skip list is dropped entirely
        # (Codex ckpt 2, #2234). Otherwise a holiday-shifted date gets NO
        # revision coverage at all: its designated date is never itself an
        # anchor, so it only ever appears in ``skip_dates``, and the
        # walk-back that would reach it returns None before fetching. The
        # anchor being in the window is what makes its shifted date due a
        # re-fetch.
        in_revision_window = anchor in revision_window
        try:
            resolved = _fetch_designated_file(
                provider_,
                anchor,
                allow_walkback=_is_disseminated(anchor, now_),
                skip_dates=frozenset() if in_revision_window else skip_dates,
            )
        except Exception as exc:  # noqa: BLE001 — captured into stats
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=anchor,
                    failed=True,
                    error_detail=f"fetch: {type(exc).__name__}: {exc}",
                )
            )
            continue

        if resolved is None:
            logger.info(
                "finra_short_interest_refresh: skip unresolved anchor=%s",
                anchor.isoformat(),
            )
            continue

        settlement_date, raw_bytes = resolved
        if settlement_date != anchor:
            logger.info(
                "finra_short_interest_refresh: anchor=%s resolved to designated settlement=%s",
                anchor.isoformat(),
                settlement_date.isoformat(),
            )
        url = provider_.settlement_file_url(settlement_date)

        # Empty-file guard. raw_filings.store_raw rejects empty
        # payloads at app/services/raw_filings.py:105 ("payload is
        # required (empty payload would defeat re-wash)"). An empty
        # 200 from the FINRA CDN is most likely a CDN edge-case;
        # treat as per-file failure with no raw store.
        if not raw_bytes:
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail="empty file (0 bytes from FINRA CDN)",
                )
            )
            continue

        # Phase 1: raw payload durable BEFORE parse (#1168).
        # Wrapped in try so a UnicodeDecodeError / store_raw DB failure
        # records a per-file failure + continues to the next settlement
        # rather than aborting the whole refresh (Codex 2 r1 MED 2).
        try:
            raw_filings.store_raw(
                conn,
                accession_number=f"FINRA_SI_{settlement_date.strftime('%Y%m%d')}",
                document_kind="finra_short_interest_csv",
                payload=raw_bytes.decode("utf-8"),
                source_url=url,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — captured into stats
            # store_raw opens its own implicit transaction; on failure
            # roll back so the next iteration starts clean.
            conn.rollback()
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail=f"raw_store: {type(exc).__name__}: {exc}",
                )
            )
            continue

        # Phase 2: parse + upserts inside JOB-owned transaction.
        # Service body emits SQL only — commit/rollback is THIS scope.
        try:
            with conn.transaction():
                per_file = ingest_settlement_file(
                    conn,
                    settlement_date,
                    raw_bytes,
                    resolver,
                    ingest_run_id,
                )
            stats_list.append(per_file)
        except Exception as exc:  # noqa: BLE001
            # Catches HeaderCorruptionError + any DB / decode error.
            # `with conn.transaction()` rolled back automatically on the
            # raised exception; raw payload is durable from the earlier
            # conn.commit() so a future re-ingest can re-attempt parse
            # against the same raw row.
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail=f"parse: {type(exc).__name__}: {exc}",
                )
            )

    stats = FinraRefreshStats(settlement_files=stats_list)

    total_skipped_no_match = sum(s.skipped_no_instrument_match for s in stats_list)
    total_skipped_ambiguous = sum(s.skipped_ambiguous_symbol for s in stats_list)
    total_skipped_invalid = sum(s.skipped_invalid_row for s in stats_list)

    logger.info(
        "finra_short_interest_refresh: files=%d upserted=%d parsed=%d resolved=%d "
        "skipped_no_match=%d skipped_ambiguous=%d skipped_invalid=%d failed=%d",
        len(stats_list),
        stats.total_upserted,
        stats.total_parsed,
        stats.total_resolved,
        total_skipped_no_match,
        total_skipped_ambiguous,
        total_skipped_invalid,
        stats.failed_files,
    )

    if stats.total_parsed > 0:
        match_rate = stats.total_resolved / stats.total_parsed
        if match_rate < 0.50:
            logger.warning(
                "finra_short_interest_refresh: match rate %.2f%% below 50%% threshold "
                "(parsed=%d resolved=%d) — universe drift or FINRA column-shape "
                "regression suspected",
                100 * match_rate,
                stats.total_parsed,
                stats.total_resolved,
            )

    if stats.failed_files > 0:
        failed_details = [
            f"{s.settlement_date.isoformat()}: {s.error_detail or 'unknown'}" for s in stats_list if s.failed
        ]
        raise RuntimeError(
            f"finra_short_interest_refresh: {stats.failed_files} of "
            f"{len(stats_list)} files failed; "
            f"total_upserted={stats.total_upserted}; "
            f"failed: {'; '.join(failed_details)}"
        )

    return stats
