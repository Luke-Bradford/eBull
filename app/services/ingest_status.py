"""Operator-facing ingest health rollup (#793, Batch 4 of #788).

The user's product intent (2026-05-03):

    "we also need to be mindful of the first start up for a user, that
    once they have got set up with at least one api key for etoro, we
    should have visibility of the data being ingested, so they know
    how far the updates are, how long it will take or anything, to
    make that a good user experience. So... why am I missing data, is
    there still a lot more to do or what?"

Single source of truth for the ``/admin/ingest-health`` page. Reads
from ``data_ingestion_runs`` (the universal audit trail since
migration 032), ``ingest_backfill_queue`` (this batch's new queue),
and the per-pipeline tombstone tables (institutional / blockholder /
def14a ingest logs). Returns a grouped-provider rollup the operator
can scan in 5 seconds.

Provider grouping is by ``source`` prefix — the ``data_ingestion_runs``
schema is free-form text, so the group mapping is a curated
dictionary in this module rather than an enum on the table. New
sources fall through to the ``other`` group until the curated list
catches up; the operator UI surfaces unmatched sources so the gap is
visible.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import psycopg.rows

ProviderGroupKey = Literal[
    "sec_fundamentals",
    "sec_ownership",
    "etoro",
    "fundamentals_other",
    "other",
]

GroupState = Literal["never_run", "green", "amber", "red"]


@dataclass(frozen=True)
class ProviderRunSummary:
    """One row per ``data_ingestion_runs.source`` value, summarised."""

    source: str
    last_success_at: datetime | None
    last_attempt_at: datetime | None
    last_attempt_status: str | None
    failures_24h: int
    rows_upserted_total: int


@dataclass(frozen=True)
class ProviderGroup:
    """Grouped provider rollup the operator UI renders as a card."""

    key: ProviderGroupKey
    label: str
    description: str
    state: GroupState
    sources: tuple[ProviderRunSummary, ...]
    backlog_pending: int
    backlog_running: int
    backlog_failed: int


@dataclass(frozen=True)
class IngestStatusReport:
    groups: tuple[ProviderGroup, ...]
    queue_total: int
    queue_running: int
    queue_failed: int
    computed_at: datetime


@dataclass(frozen=True)
class IngestFailure:
    """One recent failure surfaced on the operator page's "needs
    attention" list."""

    source: str
    started_at: datetime
    finished_at: datetime | None
    error: str | None
    rows_upserted: int


# ---------------------------------------------------------------------------
# Provider group taxonomy
# ---------------------------------------------------------------------------


# Maps ``source`` substrings to provider groups. Order matters — the
# first matching prefix wins. Curated rather than auto-detected so a
# new source going live shows up under ``other`` until the operator
# decides where it belongs.
_PROVIDER_GROUP_PREFIXES: tuple[tuple[str, ProviderGroupKey], ...] = (
    ("sec_edgar_13f", "sec_ownership"),
    ("sec_edgar_13d", "sec_ownership"),
    ("sec_edgar_13g", "sec_ownership"),
    ("sec_edgar_form3", "sec_ownership"),
    ("sec_edgar_form4", "sec_ownership"),
    ("sec_edgar_def14a", "sec_ownership"),
    ("sec_edgar_ncen", "sec_ownership"),
    ("sec_edgar_nport", "sec_ownership"),
    ("sec_edgar", "sec_fundamentals"),
    ("sec.companyfacts", "sec_fundamentals"),
    ("sec.submissions", "sec_fundamentals"),
    ("sec_xbrl", "sec_fundamentals"),
    ("etoro", "etoro"),
    ("finra", "fundamentals_other"),
    ("companies_house", "fundamentals_other"),
)


_GROUP_LABELS: dict[ProviderGroupKey, tuple[str, str]] = {
    "sec_fundamentals": (
        "SEC EDGAR — fundamentals",
        "Company facts (XBRL), submissions, 10-K / 10-Q filings.",
    ),
    "sec_ownership": (
        "SEC EDGAR — ownership",
        "13F-HR, 13D/G, Form 4, Form 3, DEF 14A proxy statements, N-CEN, N-PORT (when ingest pipelines are live).",
    ),
    "etoro": (
        "eToro broker",
        "Account, positions, candle history, instrument universe.",
    ),
    "fundamentals_other": (
        "Other regulated sources",
        "FINRA short interest, Companies House, future regulatory feeds.",
    ),
    "other": (
        "Uncategorised",
        "Sources not yet mapped into a provider group — the curated list catches up on each release.",
    ),
}


def group_for_source(source: str) -> ProviderGroupKey:
    """Map a ``data_ingestion_runs.source`` value to its group key.

    Returns ``"other"`` for unmapped sources so the operator UI can
    surface the gap explicitly.
    """
    for prefix, group in _PROVIDER_GROUP_PREFIXES:
        if source.startswith(prefix):
            return group
    return "other"


# ---------------------------------------------------------------------------
# Grouped status read
# ---------------------------------------------------------------------------


def get_ingest_status(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
) -> IngestStatusReport:
    """Build the operator-facing ingest health rollup.

    ``now`` is injectable for tests; production callers pass ``None``
    and the function uses ``NOW()`` server-side via the SQL.
    """
    sources = _read_source_summaries(conn)
    queue_counts = _read_queue_counts(conn)
    groups = _build_groups(sources, queue_counts)
    pending = sum(queue_counts.get(s, {}).get("pending", 0) for s in queue_counts)
    running = sum(queue_counts.get(s, {}).get("running", 0) for s in queue_counts)
    failed = sum(queue_counts.get(s, {}).get("failed", 0) for s in queue_counts)
    # ``queue_total`` is the full active-queue depth — anything not
    # ``complete``. Claude PR 801 review caught the prior version
    # that returned only the pending count, leaving the operator UI
    # to silently undercount running + failed rows.
    total = pending + running + failed
    computed_at = now if now is not None else datetime.now(tz=_UTC)
    return IngestStatusReport(
        groups=tuple(groups),
        queue_total=total,
        queue_running=running,
        queue_failed=failed,
        computed_at=computed_at,
    )


def get_recent_failures(
    conn: psycopg.Connection[Any],
    *,
    limit: int = 50,
) -> list[IngestFailure]:
    """Return the most recent failed runs for the operator's
    "needs attention" list. Bounded by ``limit`` so a flap-storm
    doesn't swamp the UI."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT source, started_at, finished_at, error,
                   COALESCE(rows_upserted, 0) AS rows_upserted
            FROM data_ingestion_runs
            WHERE status IN ('failed', 'partial')
              AND started_at > NOW() - INTERVAL '7 days'
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        IngestFailure(
            source=str(row["source"]),  # type: ignore[arg-type]
            started_at=row["started_at"],  # type: ignore[arg-type]
            finished_at=row.get("finished_at"),  # type: ignore[arg-type]
            error=(str(row["error"]) if row.get("error") is not None else None),
            rows_upserted=int(row["rows_upserted"]),  # type: ignore[arg-type]
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_UTC = UTC


def _read_source_summaries(
    conn: psycopg.Connection[Any],
) -> list[ProviderRunSummary]:
    """One row per distinct ``source`` value in ``data_ingestion_runs``,
    summarised over the last 24h + 7d windows."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH recent AS (
                SELECT source,
                       MAX(started_at) FILTER (WHERE status = 'success')
                           AS last_success_at,
                       MAX(started_at) AS last_attempt_at,
                       SUM(CASE WHEN status IN ('failed', 'partial')
                                 AND started_at > NOW() - INTERVAL '24 hours'
                                THEN 1 ELSE 0 END) AS failures_24h,
                       SUM(COALESCE(rows_upserted, 0)) AS rows_upserted_total
                FROM data_ingestion_runs
                GROUP BY source
            ),
            last_status AS (
                SELECT DISTINCT ON (source) source, status
                FROM data_ingestion_runs
                ORDER BY source, started_at DESC
            )
            SELECT r.source, r.last_success_at, r.last_attempt_at,
                   l.status AS last_attempt_status,
                   COALESCE(r.failures_24h, 0) AS failures_24h,
                   COALESCE(r.rows_upserted_total, 0) AS rows_upserted_total
            FROM recent r
            LEFT JOIN last_status l USING (source)
            ORDER BY r.source
            """,
        )
        rows = cur.fetchall()
    return [
        ProviderRunSummary(
            source=str(row["source"]),  # type: ignore[arg-type]
            last_success_at=row.get("last_success_at"),  # type: ignore[arg-type]
            last_attempt_at=row.get("last_attempt_at"),  # type: ignore[arg-type]
            last_attempt_status=(
                str(row["last_attempt_status"]) if row.get("last_attempt_status") is not None else None
            ),
            failures_24h=int(row["failures_24h"] or 0),  # type: ignore[arg-type]
            rows_upserted_total=int(row["rows_upserted_total"] or 0),  # type: ignore[arg-type]
        )
        for row in rows
    ]


def _read_queue_counts(
    conn: psycopg.Connection[Any],
) -> dict[str, dict[str, int]]:
    """``{pipeline_name: {status: count}}`` for the backfill queue.

    Pipeline names use the same convention as
    ``data_ingestion_runs.source`` so the operator UI can join the
    two cleanly when computing per-group backlog counts."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT pipeline_name, status, COUNT(*) AS n
            FROM ingest_backfill_queue
            GROUP BY pipeline_name, status
            """,
        )
        rows = cur.fetchall()
    out: dict[str, dict[str, int]] = {}
    for row in rows:
        pipeline = str(row["pipeline_name"])  # type: ignore[arg-type]
        status = str(row["status"])  # type: ignore[arg-type]
        out.setdefault(pipeline, {})[status] = int(row["n"])  # type: ignore[arg-type]
    return out


def _build_groups(
    summaries: Iterable[ProviderRunSummary],
    queue_counts: dict[str, dict[str, int]],
) -> list[ProviderGroup]:
    """Bucket source summaries by provider group + fold the queue
    counts in. Always emits the four canonical groups even when one
    is empty so the UI shows the full picture (a missing group ==
    "you have no activity here yet" which is useful info)."""
    by_group: dict[ProviderGroupKey, list[ProviderRunSummary]] = {key: [] for key in _GROUP_LABELS}
    for summary in summaries:
        by_group[group_for_source(summary.source)].append(summary)

    groups: list[ProviderGroup] = []
    canonical_order: tuple[ProviderGroupKey, ...] = (
        "sec_fundamentals",
        "sec_ownership",
        "etoro",
        "fundamentals_other",
        "other",
    )
    for key in canonical_order:
        sources = by_group[key]
        # Backlog counts: any pipeline whose group_for_source resolves
        # to this group contributes its queue rows.
        pending = 0
        running = 0
        failed = 0
        for pipeline, counts in queue_counts.items():
            if group_for_source(pipeline) == key:
                pending += counts.get("pending", 0)
                running += counts.get("running", 0)
                failed += counts.get("failed", 0)
        state = _derive_group_state(sources, failed)
        # ``other`` group is hidden from the canonical four when it's
        # empty (no source rows + no queue activity) — surface it only
        # when there's something to investigate.
        if key == "other" and not sources and pending == 0 and running == 0 and failed == 0:
            continue
        label, description = _GROUP_LABELS[key]
        groups.append(
            ProviderGroup(
                key=key,
                label=label,
                description=description,
                state=state,
                sources=tuple(sources),
                backlog_pending=pending,
                backlog_running=running,
                backlog_failed=failed,
            )
        )
    return groups


def _derive_group_state(sources: list[ProviderRunSummary], queue_failed: int) -> GroupState:
    """Worst-of fold across the group's sources.

    * ``never_run`` — no source has ever logged a successful run.
    * ``red`` — any source with > 3 failures in the last 24h, OR the
      queue has any failed rows for this group.
    * ``amber`` — any source whose last attempt failed but it had a
      success within the last 24h (recovery in progress), OR a
      source that hasn't successfully run in > 7 days.
    * ``green`` — every source has a recent successful run AND the
      queue is clean.
    """
    # Queue failures are decisive regardless of source state — a
    # backfill row stuck in ``failed`` is something the operator
    # needs to see even when ``data_ingestion_runs`` has no entries
    # for the group yet (e.g. ingest never started but a backfill
    # request was enqueued and failed).
    if queue_failed > 0:
        return "red"
    if not sources:
        return "never_run"
    # Failed-only sources are NOT ``never_run`` — they tried and
    # failed, which is exactly the state the operator needs to see
    # to answer "why is data missing?" Codex pre-push review (Batch 4
    # of #788) caught the prior shortcut that promoted them to
    # never_run before checking failure state.
    has_attempted = any(s.last_attempt_at is not None for s in sources)
    if all(s.last_success_at is None for s in sources):
        if has_attempted:
            return "red"
        return "never_run"
    has_amber = False
    now = datetime.now(tz=_UTC)
    for s in sources:
        if s.failures_24h > 3:
            return "red"
        if s.last_success_at is None:
            return "red"
        if s.last_attempt_status in {"failed", "partial"}:
            has_amber = True
        # Stale: more than 7 days since last successful run.
        delta = now - s.last_success_at
        if delta.days > 7:
            has_amber = True
    return "amber" if has_amber else "green"


# ---------------------------------------------------------------------------
# Backfill enqueue helper
# ---------------------------------------------------------------------------


def enqueue_backfill(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    pipeline_name: str,
    priority: int = 100,
    triggered_by: Literal["system", "operator", "migration", "consumer"] = "operator",
) -> None:
    """Enqueue a (instrument, pipeline) backfill request. Idempotent
    via the PK ON CONFLICT — re-queueing an already-pending row
    refreshes ``queued_at`` + ``priority`` + ``triggered_by`` so the
    drainer worker picks it up fresh."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_backfill_queue (
                instrument_id, pipeline_name, priority, status,
                triggered_by, queued_at
            )
            VALUES (%s, %s, %s, 'pending', %s, NOW())
            ON CONFLICT (instrument_id, pipeline_name) DO UPDATE SET
                priority = EXCLUDED.priority,
                triggered_by = EXCLUDED.triggered_by,
                queued_at = EXCLUDED.queued_at,
                status = 'pending',
                last_error = NULL,
                started_at = NULL,
                completed_at = NULL
            """,
            (instrument_id, pipeline_name, priority, triggered_by),
        )
