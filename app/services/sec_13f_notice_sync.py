"""13F-NT (Notice) supersession capture — #1639.

A 13F-NT declares the filer holds NOTHING reportable this quarter (its
holdings are reported by other managers, e.g. post-reorg sub-entity CIKs).
The parent's prior 13F-HR is thereby superseded. Our ingest pipeline handles
13F-HR only — 13F-NT is intentionally absent from ``_FORM_TO_SOURCE`` — so we
never learn the parent's stale HR is dead and the ownership rollup
double-counts it (Vanguard AAPL: 2.86B sh / 19.5% ≈ 2× the real ~9.8%).

This module captures NT filings into ``institutional_filer_13f_notices``; the
rollup read (``app/services/ownership_rollup.py``) excludes a filer's HR when
this table holds an NT for that filer with a LATER ``period_end``.

Discovery is via the SEC daily-index (the same ``read_daily_index`` the
manifest reconcile uses). ``periodOfReport`` is NOT on the index line — it
lives in the NT filing's ``primary_doc.xml`` — so capture must fetch + parse
each matched accession. Volume is low: most filers file HR, not NT, and NT
capture clusters on the four 45-day deadline days. A failed fetch / parse skips
that accession (logged) and is retried next run — self-healing, and
under-capture errs toward the EXISTING (non-suppressing) behaviour, never
toward wrongly dropping a holding.

Two entry points:

  * :func:`sync_13f_notices` — steady-state, default window = yesterday. Wired
    as the daily ``sec_13f_notice_sync`` ScheduledJob (lane ``sec_rate``).
  * :func:`backfill_13f_notices` — manual-only one-shot over the 8-quarter
    13F-HR retention horizon (the ``sec_rebuild`` triangle). Reuses the same
    per-day capture so the backfill and steady-state paths cannot drift.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg

from app.providers.implementations.sec_13f import parse_notice_primary_doc
from app.providers.implementations.sec_daily_index import read_daily_index
from app.services.institutional_holdings import THIRTEEN_F_HR_RETENTION_QUARTERS

logger = logging.getLogger(__name__)

HttpGet = Callable[[str, dict[str, str]], tuple[int, bytes]]

# Raw daily-index ``form`` values that are 13F Notices. NOT routed through
# ``map_form_to_source`` (which returns ``None`` for these by design — NT is
# deliberately out of the manifest source set); we match the raw form here.
_NOTICE_FORMS: frozenset[str] = frozenset({"13F-NT", "13F-NT/A"})

# Steady-state scans a trailing window, not just yesterday, so a transient
# fetch/parse failure on one day is retried by the next few runs (Codex ckpt-2):
# a failed NT is NOT in the table, so the re-scan re-fetches it; an
# already-captured NT is skipped (no re-fetch). NTs cluster on the four 45-day
# deadline days, so most days the window is a near-zero-cost no-op.
_STEADY_STATE_LOOKBACK_DAYS: int = 5

_UPSERT_SQL = """
    INSERT INTO institutional_filer_13f_notices
        (filer_cik, accession_number, period_end, form, filed_at)
    VALUES (%(filer_cik)s, %(accession)s, %(period_end)s, %(form)s, %(filed_at)s)
    ON CONFLICT (accession_number) DO UPDATE SET
        filer_cik  = EXCLUDED.filer_cik,
        period_end = EXCLUDED.period_end,
        form       = EXCLUDED.form,
        filed_at   = EXCLUDED.filed_at
"""


@dataclass
class NoticeSyncResult:
    """Outcome of one capture run, for the job_runs audit + operator log."""

    days_scanned: int = 0
    day_failures: int = 0  # daily-index days that raised (SEC 5xx etc.); retryable
    notices_seen: int = 0
    already_present: int = 0  # NT index rows already captured (skipped, no re-fetch)
    upserted: int = 0
    fetch_failures: int = 0
    parse_failures: int = 0
    window_since: date | None = None
    window_until: date | None = None
    skipped_accessions: list[str] = field(default_factory=list)

    def as_log_dict(self) -> dict[str, Any]:
        return {
            "days_scanned": self.days_scanned,
            "day_failures": self.day_failures,
            "notices_seen": self.notices_seen,
            "already_present": self.already_present,
            "upserted": self.upserted,
            "fetch_failures": self.fetch_failures,
            "parse_failures": self.parse_failures,
            "window_since": self.window_since.isoformat() if self.window_since else None,
            "window_until": self.window_until.isoformat() if self.window_until else None,
        }


def _yesterday_utc() -> date:
    """The most recent fully-published daily-index day (today's may not be
    published yet)."""
    return datetime.now(tz=UTC).date() - timedelta(days=1)


def _iter_dates(since: date, until: date) -> Iterator[date]:
    """Inclusive calendar-day range. Non-publish days (weekend / holiday /
    not-yet-published) yield no rows — ``read_daily_index`` tolerates them by
    returning an empty iterator, so we do not pre-filter."""
    day = since
    while day <= until:
        yield day
        day += timedelta(days=1)


def _notice_primary_doc_url(filer_cik: str, accession_number: str) -> str:
    """``https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dashes}/primary_doc.xml``.

    ``cik_int`` is the un-zero-padded CIK (the EDGAR archive path uses the
    integer form), ``acc_no_dashes`` the accession with dashes stripped.
    """
    cik_int = int(filer_cik)
    acc_no_dashes = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dashes}/primary_doc.xml"


def _capture_notice(
    conn: psycopg.Connection[Any],
    http_get: HttpGet,
    *,
    filer_cik: str,
    accession_number: str,
    form: str,
    filed_at: datetime,
    user_agent: str,
    result: NoticeSyncResult,
) -> None:
    """Fetch + parse one NT accession's primary_doc and upsert it. Any failure
    is logged + counted; the accession is retried on the next run."""
    url = _notice_primary_doc_url(filer_cik, accession_number)
    headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
    try:
        status, body = http_get(url, headers)
    except Exception:  # noqa: BLE001 — transport error → skip + retry next run.
        logger.warning("13f-notice fetch raised for %s (%s)", accession_number, url, exc_info=True)
        result.fetch_failures += 1
        result.skipped_accessions.append(accession_number)
        return
    if status != 200:
        logger.info("13f-notice fetch %s for %s — skipping", status, accession_number)
        result.fetch_failures += 1
        result.skipped_accessions.append(accession_number)
        return

    try:
        notice = parse_notice_primary_doc(body.decode("utf-8", errors="replace"))
    except Exception as exc:  # noqa: BLE001 — malformed NT → skip + retry next run.
        logger.warning("13f-notice parse failed for %s: %s", accession_number, exc)
        result.parse_failures += 1
        result.skipped_accessions.append(accession_number)
        return

    with conn.cursor() as cur:
        cur.execute(
            _UPSERT_SQL,
            {
                "filer_cik": notice.cik,
                "accession": accession_number,
                "period_end": notice.period_of_report,
                "form": form,
                "filed_at": filed_at,
            },
        )
    result.upserted += 1


def _already_captured(conn: psycopg.Connection[Any], accession_number: str) -> bool:
    """Whether this NT accession is already in the notices table — so the
    trailing-window re-scan re-fetches only NEW or previously-FAILED Notices,
    never a successfully-captured one."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM institutional_filer_13f_notices WHERE accession_number = %s",
            (accession_number,),
        )
        return cur.fetchone() is not None


def _process_day(
    conn: psycopg.Connection[Any],
    http_get: HttpGet,
    when: date,
    *,
    user_agent: str,
    result: NoticeSyncResult,
) -> None:
    for row in read_daily_index(http_get, when, user_agent=user_agent):
        if row.form not in _NOTICE_FORMS:
            continue
        result.notices_seen += 1
        if _already_captured(conn, row.accession_number):
            result.already_present += 1
            continue
        _capture_notice(
            conn,
            http_get,
            filer_cik=row.cik,
            accession_number=row.accession_number,
            form=row.form,
            filed_at=row.filed_at,
            user_agent=user_agent,
            result=result,
        )


def _scan_days(
    conn: psycopg.Connection[Any],
    http_get: HttpGet,
    days: Iterator[date],
    *,
    user_agent: str,
    result: NoticeSyncResult,
    commit_each_day: bool,
) -> None:
    """Walk each day's daily-index, capturing Notices. A day whose index fetch
    raises (non-tolerated SEC failure — 5xx, WAF 403 on a past business day) is
    logged + counted, NOT fatal: the scan continues and that day is retried on a
    future run (already-captured Notices are skipped, so re-runs are cheap).

    ``commit_each_day`` (backfill only): commit after each successful day so a
    late-scan failure on a multi-year backfill keeps every prior day's captures
    durable, instead of one giant transaction that a single bad day rolls back
    (Codex ckpt-2 round 2). Steady-state (5-day window) leaves the commit to the
    caller — one small transaction."""
    for when in days:
        result.days_scanned += 1
        try:
            _process_day(conn, http_get, when, user_agent=user_agent, result=result)
        except Exception:  # noqa: BLE001 — one bad day must not abort a long scan.
            logger.warning("13f-notice: day %s scan failed (retryable next run)", when.isoformat(), exc_info=True)
            result.day_failures += 1
            if commit_each_day:
                conn.rollback()  # drop the failed day's partial work; keep prior committed days
            continue
        if commit_each_day:
            conn.commit()


def sync_13f_notices(
    conn: psycopg.Connection[Any],
    http_get: HttpGet,
    *,
    user_agent: str,
    since: date | None = None,
    until: date | None = None,
) -> NoticeSyncResult:
    """Capture 13F-NT filings over ``[since, until]`` (inclusive).

    Default window = the trailing ``_STEADY_STATE_LOOKBACK_DAYS`` ending
    yesterday (NOT yesterday-only), so a transient fetch/parse failure on one
    day is retried by the next few daily runs — already-captured Notices in the
    window are skipped, so the re-scan only re-fetches the failed/new ones. The
    caller owns the transaction; this function does not commit.
    """
    until = until or _yesterday_utc()
    since = since or (until - timedelta(days=_STEADY_STATE_LOOKBACK_DAYS - 1))
    if since > until:
        raise ValueError(f"sync_13f_notices: since={since} after until={until}")

    result = NoticeSyncResult(window_since=since, window_until=until)
    _scan_days(conn, http_get, _iter_dates(since, until), user_agent=user_agent, result=result, commit_each_day=False)
    logger.info("13f-notice sync complete: %s", result.as_log_dict())
    return result


def _backfill_floor(conn: psycopg.Connection[Any]) -> date | None:
    """The oldest *quarter* (``period_end``) any ``ownership_institutions_current``
    row covers — the floor of the supersession-relevant scan window.

    MUST be `MIN(period_end)`, NOT `MIN(filed_at)` (Codex ckpt-2): a `13F-HR/A`
    amending an OLD quarter can be filed RECENTLY, so a `filed_at` floor can sit
    *after* an NT that supersedes that stale amended HR, and the NT would never
    be captured. The period floor is provably conservative — any NT that could
    supersede a current HR has `NT.period_end > HR.period_end >= MIN(period_end)`,
    and an NT is always filed after its own period, so `NT.filed_at > MIN(period_end)`;
    scanning daily indexes from `MIN(period_end)` therefore captures every
    relevant NT. `period_end` is NOT NULL on `_current`.

    Returns ``None`` when there are no institution rows on file (nothing to
    supersede → nothing to backfill)."""
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(period_end) FROM ownership_institutions_current")
        row = cur.fetchone()
    return row[0] if row else None


def backfill_13f_notices(
    conn: psycopg.Connection[Any],
    http_get: HttpGet,
    *,
    user_agent: str,
) -> NoticeSyncResult:
    """One-shot backfill over the 8-quarter 13F-HR retention horizon.

    Floor = ``MIN(ownership_institutions_current.period_end)`` (see
    :func:`_backfill_floor` for why the period axis, not filed_at), capped at the
    8-quarter retention horizon. Scans every daily-index day from the floor to
    yesterday via the shared :func:`_scan_days` (same per-day capture as
    :func:`sync_13f_notices`, so the two paths cannot drift) — but with
    ``commit_each_day`` so a multi-year scan is durable across a late-scan SEC
    failure. Manual-only (the ``sec_rebuild`` triangle).
    """
    floor = _backfill_floor(conn)
    if floor is None:
        logger.info("13f-notice backfill: no institution rows on file — nothing to backfill")
        return NoticeSyncResult()
    # Defensive floor: never scan further back than the retention horizon even
    # if a stale row's period_end predates it.
    horizon_floor = _yesterday_utc() - timedelta(days=int(THIRTEEN_F_HR_RETENTION_QUARTERS) * 92)
    since = max(floor, horizon_floor)
    until = _yesterday_utc()
    logger.info(
        "13f-notice backfill: floor=%s horizon_floor=%s → scanning %s..%s",
        floor.isoformat(),
        horizon_floor.isoformat(),
        since.isoformat(),
        until.isoformat(),
    )
    result = NoticeSyncResult(window_since=since, window_until=until)
    _scan_days(conn, http_get, _iter_dates(since, until), user_agent=user_agent, result=result, commit_each_day=True)
    logger.info("13f-notice backfill complete: %s", result.as_log_dict())
    return result
