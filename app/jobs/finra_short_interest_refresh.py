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
  6. Per-file health sentinels (#2337) — ``evaluate_file_sentinels``:
     any row-shape failure, zero resolution on a non-empty file, or
     resolved-row retention below ``_RESOLVED_RETENTION_FLOOR`` against
     the previous stored settlement date. The aggregate match rate is
     logged at INFO as context and no longer carries an alarm; see the
     constant block for why an absolute floor on it detected nothing.
  7. RuntimeError on partial failure so _tracked_job records
     job_runs.status='failure' (mirror G12 partial-failure contract).
"""

from __future__ import annotations

import calendar
import logging
from collections.abc import Sequence
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

# Health sentinels (#2337). The arm these replace warned when
# ``rows_resolved / rows_parsed < 0.50``, described as a "universe drift /
# FINRA column-shape regression sentinel". It detected nothing: measured over
# every stored payload (``scripts/audit_2337_finra_match_rate.py``, 34 files /
# 715,915 rows), that ratio runs 25.47%-26.58% and is below the floor on
# 34 of 34 files, so the warning fired on every normal fire.
#
# It could not be re-baselined either, because its two sides are governed by
# different populations: the numerator is bounded by OUR universe
# (``build_preloaded_symbol_resolver`` selects ``instruments WHERE
# is_tradable``), the denominator by FINRA's — every US-reported symbol,
# including OTC, preferreds, ETFs and share-class siblings. The census shows
# the ratio drifting monotonically down (26.58% -> 25.47%) as FINRA's universe
# grows faster than ours, so any absolute floor expires on its own.
#
# So the ratio is now logged as context and the alarms sit on the three
# conditions whose healthy value is knowable rather than fitted:
#
# 1. ``skipped_invalid_row > 0`` — a required field missing or non-integer.
#    This is where a FINRA column-shape regression actually lands, and the
#    healthy value is zero BY CONSTRUCTION: a well-formed pipe-delimited file
#    carries every required field on every row. Census: 0 of 715,915 rows.
# 2. ``rows_resolved == 0`` on a non-empty file — the resolver or the tradable
#    universe is broken. Boundary at none-at-all; nothing to fit.
# 3. Retention against the previous stored settlement date, below.
#
# Arms 1 and 2 are boundaries. Arm 3 is the one that needs a number, and no
# published formulation exists for it — FINRA documents the file, not our
# match against it — so it is fixed BY CONSTRUCTION and frozen here.
# Consecutive-file retention ``resolved[i] / resolved[i-1]`` over the 33
# consecutive pairs in the corpus has min **0.99929**, max 1.05784: the
# largest drop the corpus has ever produced is 0.07%. The floor is placed two
# orders of magnitude beyond that, so it cannot fire on normal turnover (which
# is the defect #2337 exists to fix) while still catching partial universe
# damage — e.g. a universe sync flipping ``is_tradable`` on a slice of the
# table — long before arm 2's total failure would.
#
# ⚠ The comparison is against the newest STORED settlement date strictly
# before this file's, which during a backfill can be months rather than a
# fortnight away. A wider gap admits a larger legitimate move; the one such
# gap in the corpus (2024-02-15 -> 2025-04-30, 14 months) moved +5.8%, i.e.
# upward, and the floor is one-sided.
_RESOLVED_RETENTION_FLOOR: float = 0.90


@dataclass(frozen=True)
class SentinelFinding:
    settlement_date: date
    kind: str
    detail: str


def evaluate_file_sentinels(
    stats: SettlementIngestStats,
    previous: tuple[date, int] | None,
) -> list[SentinelFinding]:
    """Health arms for one ingested settlement file (#2337). Pure.

    ``previous`` is ``(settlement_date, rows_stored)`` for the newest stored
    settlement date strictly before this file's, or ``None`` when this is the
    oldest date held. See the ``_RESOLVED_RETENTION_FLOOR`` block above for
    each arm's derivation.

    A file that failed, or that parsed no rows at all, yields nothing — those
    are already surfaced as per-file failures and by the ``RuntimeError``
    partial-failure contract, and re-reporting them here would be noise.
    """
    if stats.failed or stats.rows_parsed == 0:
        return []

    findings: list[SentinelFinding] = []
    if stats.skipped_invalid_row > 0:
        findings.append(
            SentinelFinding(
                stats.settlement_date,
                "row_shape",
                f"{stats.skipped_invalid_row} of {stats.rows_parsed} rows failed the "
                "required-field check (healthy value is 0) — FINRA column-shape "
                "regression suspected",
            )
        )

    if stats.rows_resolved == 0:
        # Total resolution failure. Retention would fire too (0 / anything is
        # below any floor), so return here rather than reporting one fault twice.
        findings.append(
            SentinelFinding(
                stats.settlement_date,
                "no_resolution",
                f"0 of {stats.rows_parsed} rows resolved to an instrument — the "
                "symbol resolver or the tradable universe is broken",
            )
        )
        return findings

    if previous is not None and previous[1] > 0:
        retention = stats.rows_resolved / previous[1]
        if retention < _RESOLVED_RETENTION_FLOOR:
            findings.append(
                SentinelFinding(
                    stats.settlement_date,
                    "universe_drift",
                    f"resolved {stats.rows_resolved} against {previous[1]} stored at "
                    f"{previous[0].isoformat()} = {retention:.4f} retention, below the "
                    f"{_RESOLVED_RETENTION_FLOOR:.2f} floor",
                )
            )
    return findings


def _previous_stored_resolved(
    conn: psycopg.Connection[Any],
    settlement_dates: Sequence[date],
) -> dict[date, tuple[date, int]]:
    """Per input date, the newest STORED settlement date before it + its row count.

    Dates with no earlier stored date are absent from the result (the LATERAL
    yields no row), which ``evaluate_file_sentinels`` reads as "no baseline".
    """
    if not settlement_dates:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.settlement_date, p.settlement_date, p.n
              FROM unnest(%(dates)s::date[]) AS d(settlement_date)
             CROSS JOIN LATERAL (
                   SELECT o.settlement_date, count(*) AS n
                     FROM finra_short_interest_observations o
                    WHERE o.settlement_date < d.settlement_date
                    GROUP BY o.settlement_date
                    ORDER BY o.settlement_date DESC
                    LIMIT 1
             ) AS p
            """,
            {"dates": list(settlement_dates)},
        )
        return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


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
        # Context, not an alarm — see the _RESOLVED_RETENTION_FLOOR block for
        # why this ratio cannot carry one (#2337).
        logger.info(
            "finra_short_interest_refresh: match rate %.2f%% (parsed=%d resolved=%d)",
            100 * stats.total_resolved / stats.total_parsed,
            stats.total_parsed,
            stats.total_resolved,
        )

    previous_resolved = _previous_stored_resolved(conn, [s.settlement_date for s in stats_list if not s.failed])
    for s in stats_list:
        for finding in evaluate_file_sentinels(s, previous_resolved.get(s.settlement_date)):
            logger.warning(
                "finra_short_interest_refresh: %s at settlement=%s — %s",
                finding.kind,
                finding.settlement_date.isoformat(),
                finding.detail,
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
