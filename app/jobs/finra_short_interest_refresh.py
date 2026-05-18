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
  5. For each settlement_date in targets:
       a. provider.fetch_settlement_file(...) — 404 = benign skip;
          other errors = per-file failure.
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
    HeaderCorruptionError,
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


def _walk_back_to_weekday(d: date) -> date:
    """If ``d`` falls on Saturday/Sunday, walk BACK to the prior Friday.

    FINRA publishes ``shrt{YYYYMMDD}.csv`` keyed by the last business
    day of the half-month, not the calendar day.

    **Federal-holiday EOM/15th handling** (Codex 2 r1 MED 1): this
    helper handles weekends only — NOT US federal holidays (Good
    Friday, MLK day, Memorial Day, July 4, Labor Day, Thanksgiving,
    Christmas). On those rare cases, the probe lands on the holiday
    date itself and returns 404; the JOB's ``FinraNotFound`` catch
    treats it as a benign skip + the next-fire cron tries again
    (which will keep returning 404 until the operator runs the REPL
    backfill to pick up the actual prior-business-day file). This is
    an accepted v1 limitation — adding a US holiday calendar dep
    (pandas-market-calendars / exchange_calendars) is gated by
    settled-decisions #532 minimal-dependency posture and a tracked
    monitoring/alert path that doesn't exist yet.
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

    for settlement_date in targets:
        url = provider_.settlement_file_url(settlement_date)
        try:
            raw_bytes = provider_.fetch_settlement_file(settlement_date)
        except FinraNotFound:
            logger.info(
                "finra_short_interest_refresh: skip not-yet-published settlement=%s",
                settlement_date.isoformat(),
            )
            continue
        except Exception as exc:  # noqa: BLE001 — captured into stats
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail=f"fetch: {type(exc).__name__}: {exc}",
                )
            )
            continue

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
        except (HeaderCorruptionError, Exception) as exc:  # noqa: BLE001
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
