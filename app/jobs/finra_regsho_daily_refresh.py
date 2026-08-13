"""FINRA RegSHO daily short volume refresh (#916 — Phase 6 PR 12).

Spec: docs/superpowers/specs/2026-05-18-finra-regsho-daily.md.
Plan: docs/superpowers/plans/2026-05-18-finra-regsho-daily-plan.md.

ScheduledJob body. Per-fire flow:

  1. Build the preloaded ``symbol → instrument_id`` resolver (imported
     verbatim from the bimonthly sibling).
  2. Enumerate candidate weekday trade dates within the backfill
     window.
  3. Read sec_filing_manifest for already-parsed FINRA RegSHO
     accessions. Decode the synthetic accession back into
     ``(trade_date, prefix)`` via ``_parse_accession``.
  4. Compute targets = ((candidates × PREFIXES) - already_parsed_pairs)
     ∪ revision_window, where revision_window = (candidates[-2:] ×
     PREFIXES) — the two most-recent trade dates × all 6 prefixes are
     always re-probed so FINRA in-place revisions don't get masked.
  5. For each (trade_date, prefix) in targets:
       a. provider.fetch_regsho_daily_file(...) — 404 = benign skip;
          other errors = per-file failure.
       b. Empty-file guard: 0 bytes → per-file failure.
       c. Phase 1: raw_filings.store_raw(...) + conn.commit() — raw
          payload durable BEFORE parse (#1168). Wrapped in try/except
          + conn.rollback() so a UnicodeDecodeError / store_raw DB
          failure records a per-file failure + continues to the next
          pair rather than poisoning the connection (Codex 1a r1 MED).
       d. Phase 2: ``with conn.transaction():`` wraps
          ingest_regsho_daily_file. Clean exit commits observations +
          manifest + freshness atomically. Exception triggers
          automatic rollback; raw payload stays durable.
  6. Match-rate WARNING log if < 50% (universe drift / FINRA shape
     regression sentinel).
  7. RuntimeError on partial failure so _tracked_job records
     job_runs.status='failure' (mirror G6/#915 + G12).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg

from app.providers.implementations.finra_regsho import (
    PREFIXES,
    FinraRegShoProvider,
)
from app.providers.implementations.finra_short_interest import FinraNotFound
from app.services import raw_filings
from app.services.finra_regsho_ingest import (
    RegShoDailyIngestStats,
    ingest_regsho_daily_file,
)
from app.services.finra_short_interest_ingest import build_preloaded_symbol_resolver

logger = logging.getLogger(__name__)

# Synthetic accession prefix shared by all RegSHO daily manifest rows.
# Per spec §8.1 — `_parse_accession` reverses the
# ``FINRA_REGSHO_{PREFIX}_{YYYYMMDD}`` shape via rsplit on the
# trailing date suffix.
_ACCESSION_PREFIX = "FINRA_REGSHO_"


@dataclass(frozen=True)
class RegShoDailyRefreshStats:
    daily_files: list[RegShoDailyIngestStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int:
        return sum(s.rows_upserted for s in self.daily_files)

    @property
    def total_parsed(self) -> int:
        return sum(s.rows_parsed for s in self.daily_files)

    @property
    def total_resolved(self) -> int:
        return sum(s.rows_resolved for s in self.daily_files)

    @property
    def failed_files(self) -> int:
        return sum(1 for s in self.daily_files if s.failed)


def _parse_accession(accession: str) -> tuple[date, str] | None:
    """Reverse ``FINRA_REGSHO_{PREFIX}_{YYYYMMDD}``.

    Returns ``(trade_date, prefix)`` on clean parse; ``None`` on any
    malformation (unknown prefix, malformed date, missing root tag).

    Callers SKIP None results from the manifest filter — a malformed
    accession in the manifest table never causes the cron to re-fetch
    every file (Codex 1a r1 MED).
    """
    if not accession.startswith(_ACCESSION_PREFIX):
        return None
    tail = accession[len(_ACCESSION_PREFIX) :]
    if "_" not in tail:
        return None
    prefix_part, date_part = tail.rsplit("_", 1)
    if prefix_part not in PREFIXES:
        return None
    try:
        td = datetime.strptime(date_part, "%Y%m%d").date()
    except ValueError:
        return None
    return (td, prefix_part)


def _trade_dates_to_fetch(
    now: datetime,
    backfill_window_days: int = 30,
) -> list[date]:
    """Enumerate weekdays falling within ``[now - backfill_window_days, now]``.

    Weekend filter only (Saturday/Sunday excluded). US federal holidays
    are NOT filtered out at enumeration time — the 404 path returns
    ``FinraNotFound`` + the JOB skips silently. Same accepted v1
    limitation as #915 ``_walk_back_to_weekday``.
    """
    earliest = (now - timedelta(days=backfill_window_days)).date()
    today = now.date()
    out: list[date] = []
    d = earliest
    while d <= today:
        if d.weekday() < 5:  # 0-4 = Mon-Fri.
            out.append(d)
        d += timedelta(days=1)
    return out


def _already_parsed_pairs(conn: psycopg.Connection[Any]) -> set[tuple[date, str]]:
    """Read manifest for FINRA RegSHO daily rows with
    ``ingest_status='parsed'``; return the parsed ``(trade_date,
    prefix)`` pair set (derived from synthetic accession
    ``FINRA_REGSHO_{PREFIX}_{YYYYMMDD}``).
    """
    out: set[tuple[date, str]] = set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number
            FROM sec_filing_manifest
            WHERE source = 'finra_regsho_daily'
              AND ingest_status = 'parsed'
            """
        )
        for (accession,) in cur.fetchall():
            parsed = _parse_accession(accession)
            if parsed is not None:
                out.add(parsed)
    return out


def _compute_targets(
    candidate_dates: list[date],
    already_parsed: set[tuple[date, str]],
) -> list[tuple[date, str]]:
    """Cross-product candidate_dates × PREFIXES; subtract parsed pairs;
    UNION with revision window (last-2 dates × all PREFIXES). Returns
    sorted ASC by (date, prefix).
    """
    sorted_candidates = sorted(candidate_dates)
    revision_window: set[tuple[date, str]] = (
        {(d, p) for d in sorted_candidates[-2:] for p in PREFIXES} if sorted_candidates else set()
    )
    all_pairs = {(d, p) for d in candidate_dates for p in PREFIXES}
    return sorted(all_pairs - already_parsed | revision_window)


def run_finra_regsho_daily_refresh(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
    backfill_window_days: int = 30,
    provider: FinraRegShoProvider | None = None,
) -> RegShoDailyRefreshStats:
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
    provider_ = provider if provider is not None else FinraRegShoProvider()

    resolver = build_preloaded_symbol_resolver(conn)
    candidate_dates = _trade_dates_to_fetch(now_, backfill_window_days)
    already_parsed = _already_parsed_pairs(conn)
    targets = _compute_targets(candidate_dates, already_parsed)

    ingest_run_id = uuid4()
    stats_list: list[RegShoDailyIngestStats] = []

    for trade_date, prefix in targets:
        url = provider_.regsho_daily_url(trade_date, prefix)
        try:
            raw_bytes = provider_.fetch_regsho_daily_file(trade_date, prefix)
        except FinraNotFound:
            logger.info(
                "finra_regsho_daily_refresh: skip not-yet-published trade_date=%s prefix=%s",
                trade_date.isoformat(),
                prefix,
            )
            continue
        except Exception as exc:  # noqa: BLE001 — captured into stats
            stats_list.append(
                RegShoDailyIngestStats(
                    trade_date=trade_date,
                    prefix=prefix,
                    failed=True,
                    error_detail=f"fetch: {type(exc).__name__}: {exc}",
                )
            )
            continue

        # Empty-file guard. raw_filings.store_raw rejects empty
        # payloads at app/services/raw_filings.py:105 ("payload is
        # required (empty payload would defeat re-wash)"). An empty 200
        # from the FINRA CDN is most likely a CDN edge-case; treat as
        # per-file failure with no raw store.
        if not raw_bytes:
            stats_list.append(
                RegShoDailyIngestStats(
                    trade_date=trade_date,
                    prefix=prefix,
                    failed=True,
                    error_detail="empty file (0 bytes from FINRA CDN)",
                )
            )
            continue

        # Phase 1: raw payload durable BEFORE parse (#1168). Wrapped in
        # try so a UnicodeDecodeError / store_raw DB failure records a
        # per-file failure + continues to the next pair rather than
        # poisoning the connection (Codex 1a r1 MED — mirrors #915
        # finra_short_interest_refresh.py:235-255).
        try:
            raw_filings.store_raw(
                conn,
                accession_number=f"FINRA_REGSHO_{prefix}_{trade_date.strftime('%Y%m%d')}",
                document_kind="finra_regsho_daily_txt",
                payload=raw_bytes.decode("utf-8"),
                source_url=url,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — captured into stats
            # store_raw opens its own implicit transaction; on failure
            # roll back so the next iteration starts clean.
            conn.rollback()
            stats_list.append(
                RegShoDailyIngestStats(
                    trade_date=trade_date,
                    prefix=prefix,
                    failed=True,
                    error_detail=f"raw_store: {type(exc).__name__}: {exc}",
                )
            )
            continue

        # Phase 2: parse + upserts inside JOB-owned transaction.
        # Service body emits SQL only — commit/rollback is THIS scope.
        try:
            with conn.transaction():
                per_file = ingest_regsho_daily_file(
                    conn,
                    trade_date,
                    prefix,
                    raw_bytes,
                    resolver,
                    ingest_run_id,
                )
            stats_list.append(per_file)
        except Exception as exc:  # noqa: BLE001
            # Catches HeaderCorruptionError + any DB / decode error.
            # `with conn.transaction()` rolled back automatically on
            # the raised exception; raw payload is durable from the
            # earlier conn.commit() so a future re-ingest can re-
            # attempt parse against the same raw row.
            stats_list.append(
                RegShoDailyIngestStats(
                    trade_date=trade_date,
                    prefix=prefix,
                    failed=True,
                    error_detail=f"parse: {type(exc).__name__}: {exc}",
                )
            )

    stats = RegShoDailyRefreshStats(daily_files=stats_list)

    total_skipped_no_match = sum(s.skipped_no_instrument_match for s in stats_list)
    total_skipped_ambiguous = sum(s.skipped_ambiguous_symbol for s in stats_list)
    total_skipped_invalid = sum(s.skipped_invalid_row for s in stats_list)

    logger.info(
        "finra_regsho_daily_refresh: files=%d upserted=%d parsed=%d resolved=%d "
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

    # Match-rate WARNING — universe drift or FINRA shape regression
    # sentinel (#915 spec §4 + Codex 1b r2 MED on this plan). Skip on
    # zero-parsed so the FNRA-only / empty-day case doesn't false-fire.
    if stats.total_parsed > 0:
        match_rate = stats.total_resolved / stats.total_parsed
        if match_rate < 0.50:
            logger.warning(
                "finra_regsho_daily_refresh: match rate %.2f%% below 50%% threshold "
                "(parsed=%d resolved=%d) — universe drift or FINRA column-shape "
                "regression suspected",
                100 * match_rate,
                stats.total_parsed,
                stats.total_resolved,
            )

    if stats.failed_files > 0:
        failed_details = [
            f"{s.trade_date.isoformat()}/{s.prefix}: {s.error_detail or 'unknown'}" for s in stats_list if s.failed
        ]
        raise RuntimeError(
            f"finra_regsho_daily_refresh: {stats.failed_files} of "
            f"{len(stats_list)} files failed; "
            f"total_upserted={stats.total_upserted}; "
            f"failed: {'; '.join(failed_details)}"
        )

    return stats
