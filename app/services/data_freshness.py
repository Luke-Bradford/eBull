"""Data freshness scheduler — when to next ASK SEC for new filings.

Issue #865 / spec §"data_freshness_index"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

Distinct from ``sec_filing_manifest`` (#864):

  - ``sec_filing_manifest`` answers "is accession X already on file?"
  - ``data_freshness_index`` answers "should I poll subject Y for source Z?"

The scheduler is subject-polymorphic — 13F-HR is filer-centric (one
filer's 13F covers many issuers), so the row carries ``subject_type``
+ ``subject_id`` rather than always (instrument_id, source).

Per-source cadence (the ``_CADENCE`` map below) drives
``expected_next_at`` predictions from ``last_known_filed_at``. Calls
from the worker layer:

  - ``seed_scheduler_from_manifest``: bootstrap rows from manifest history
  - ``record_poll_outcome``: record after a poll completes
  - ``subjects_due_for_poll``: worker pulls due rows
  - ``subjects_due_for_recheck``: never_filed / error rechecks

The cadence map is hard-coded per the spec — adding a new source
means one edit here, not a sweep across the worker / providers.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.services.sec_manifest import ManifestSource, ManifestSubjectType

logger = logging.getLogger(__name__)


FreshnessState = Literal[
    "unknown",
    "current",
    "expected_filing_overdue",
    "never_filed",
    "error",
]

PollOutcome = Literal["current", "new_data", "error", "never"]


# ---------------------------------------------------------------------------
# Per-source cadence
# ---------------------------------------------------------------------------
#
# Each source's typical "next filing arrives at most N days after the
# last filing" cadence. Used to compute ``expected_next_at`` from
# ``last_known_filed_at``. Conservative ceilings — the worker still
# polls earlier sources (Atom feed, daily index) so over-prediction
# here just means a slightly delayed scheduled poll, never missed data.

# Cadence values match the spec table at lines 175-184. These are the
# Layer 3 per-CIK reconcile poll cadence — the Atom feed (every 5 min)
# and daily-index (daily) catch new filings within hours; this map is
# how often we re-poll submissions.json for amendments + safety net.
_CADENCE: dict[ManifestSource, timedelta] = {
    # Insider section 16 — Form 4 due within 2 business days of insider
    # txn. Spec says "24h after first known officer (event-driven)".
    # Layer 3 reconcile cadence: 30d ceiling — Atom feed catches the
    # individual events; this is the per-CIK safety-net poll.
    "sec_form3": timedelta(days=30),
    "sec_form4": timedelta(days=30),
    "sec_form5": timedelta(days=365),  # annual within 45 days of fiscal year-end
    # Beneficial owner — event-driven. Spec: "10 days after threshold
    # cross". Layer 3 reconcile cadence kept at 90d (event-driven; most
    # amendments within a quarter; Atom feed is primary path).
    "sec_13d": timedelta(days=90),
    "sec_13g": timedelta(days=90),
    # Institutional manager — quarterly within 45 days of quarter-end.
    # Spec: "45 days after quarter-end". Cadence 120d = filed_at +
    # ~90d (next quarter end) + 30d (filing window). Codex review
    # accepted the approximation; refining to next-quarter-end+45d
    # exact would need calendar logic.
    "sec_13f_hr": timedelta(days=120),
    # Proxy — annual. Spec: "365 days from last filed_at" (Codex
    # review v3: tighten from 395 to 365 to match spec exactly).
    "sec_def14a": timedelta(days=365),
    # Fund (Phase 3) — N-PORT 60 days after month-end; N-CSR semi-annual.
    "sec_n_port": timedelta(days=90),  # 60d window + buffer
    "sec_n_csr": timedelta(days=200),  # ~6mo
    # Periodic — 10-K within 60-90d of fiscal year-end; 10-Q within 40-45d
    # of quarter-end; 8-K within 4 business days of triggering event.
    "sec_10k": timedelta(days=120),
    "sec_10q": timedelta(days=60),
    "sec_8k": timedelta(days=14),
    "sec_xbrl_facts": timedelta(days=120),  # piggybacks on 10-K/10-Q
    # FINRA short interest — bimonthly settlement schedule.
    "finra_short_interest": timedelta(days=20),
}


def cadence_for(source: ManifestSource) -> timedelta:
    """Per-source cadence ceiling. Raises KeyError on an unknown source
    so a new source addition surfaces loudly instead of falling through
    to a default that would silently mis-schedule polls."""
    return _CADENCE[source]


def predict_next_at(
    source: ManifestSource,
    last_known_filed_at: datetime | None,
) -> datetime | None:
    """Compute ``expected_next_at`` from the last known filing.

    Returns ``last_known_filed_at + cadence(source)`` when known.
    ``None`` when never filed — caller decides whether to set
    ``next_recheck_at`` instead (``never_filed`` state) or leave the
    row in ``unknown`` for immediate poll."""
    if last_known_filed_at is None:
        return None
    return last_known_filed_at + cadence_for(source)


# ---------------------------------------------------------------------------
# Seeder
# ---------------------------------------------------------------------------


def seed_scheduler_from_manifest(conn: psycopg.Connection[Any]) -> int:
    """Bootstrap ``data_freshness_index`` rows from manifest history.

    For every distinct ``(subject_type, subject_id, source)`` triple
    in ``sec_filing_manifest``, derive:

      - ``last_known_filing_id``: max(filed_at)'s accession
      - ``last_known_filed_at``: max(filed_at)
      - ``cik``: from the manifest row
      - ``instrument_id``: from the manifest row (NULL for non-issuer)
      - ``state``: ``current`` (we know it has filed before)
      - ``expected_next_at``: ``last_known_filed_at + cadence(source)``

    Idempotent on re-run — uses ON CONFLICT DO UPDATE so a re-seed
    refreshes ``last_known_*`` from the latest manifest state.

    Returns the number of (subject, source) triples processed.
    """
    inserted = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (subject_type, subject_id, source)
                subject_type, subject_id, source, cik, instrument_id,
                accession_number, filed_at
            FROM sec_filing_manifest
            ORDER BY subject_type, subject_id, source, filed_at DESC
            """
        )
        rows = cur.fetchall()

    for (
        subject_type,
        subject_id,
        source,
        cik,
        instrument_id,
        accession_number,
        filed_at,
    ) in rows:
        expected_next_at = predict_next_at(source, filed_at)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_freshness_index (
                    subject_type, subject_id, source,
                    cik, instrument_id,
                    last_known_filing_id, last_known_filed_at,
                    last_polled_at, last_polled_outcome,
                    expected_next_at, state
                ) VALUES (
                    %(stype)s, %(sid)s, %(source)s,
                    %(cik)s, %(iid)s,
                    %(acc)s, %(filed_at)s,
                    NULL, 'never',
                    %(next_at)s, 'current'
                )
                ON CONFLICT (subject_type, subject_id, source) DO UPDATE SET
                    cik = COALESCE(EXCLUDED.cik, data_freshness_index.cik),
                    instrument_id = COALESCE(
                        EXCLUDED.instrument_id, data_freshness_index.instrument_id
                    ),
                    last_known_filing_id = EXCLUDED.last_known_filing_id,
                    last_known_filed_at = EXCLUDED.last_known_filed_at,
                    expected_next_at = EXCLUDED.expected_next_at,
                    -- Codex review: ALWAYS set state='current' when manifest
                    -- evidence shows the subject HAS filed. Preserving stale
                    -- 'never_filed' / 'error' / 'expected_filing_overdue'
                    -- from a prior cycle would leave a known-filed subject
                    -- out of the active poll queue or stuck in retry.
                    state = 'current',
                    state_reason = NULL,
                    next_recheck_at = NULL
                """,
                {
                    "stype": subject_type,
                    "sid": subject_id,
                    "source": source,
                    "cik": cik,
                    "iid": instrument_id,
                    "acc": accession_number,
                    "filed_at": filed_at,
                    "next_at": expected_next_at,
                },
            )
        inserted += 1

    return inserted


# ---------------------------------------------------------------------------
# Outcome recording
# ---------------------------------------------------------------------------


def record_poll_outcome(
    conn: psycopg.Connection[Any],
    *,
    subject_type: ManifestSubjectType,
    subject_id: str,
    source: ManifestSource,
    outcome: PollOutcome,
    last_known_filing_id: str | None = None,
    last_known_filed_at: datetime | None = None,
    new_filings_since: int = 0,
    error: str | None = None,
    next_recheck_at: datetime | None = None,
    cik: str | None = None,
    instrument_id: int | None = None,
) -> None:
    """Update the scheduler row after a poll cycle completes.

    The poll layer (Layer 3 in the spec — per-CIK submissions.json)
    calls this with the result. Subject row is created on demand
    (UPSERT) so the first poll for a never-seen subject lands cleanly.

    State transitions are derived from ``outcome`` + ``new_filings_since``:

      - outcome='new_data', new_filings_since>0  ->  state='current'
        (we just observed new filings, advance the watermark)
      - outcome='current', new_filings_since==0  ->  state='current'
        (still tracking; cadence not yet exceeded)
      - outcome='error'                          ->  state='error'
        (last poll failed; retry per ``next_recheck_at``)
      - outcome='never'                          ->  state='never_filed'
        (only used when seeding; not from a real poll)

    ``expected_next_at`` is recomputed from ``last_known_filed_at`` and
    the source cadence; for error / never_filed states the
    ``next_recheck_at`` field carries the recheck cadence instead.
    """
    if subject_type == "issuer":
        if instrument_id is None:
            raise ValueError(f"record_poll_outcome: issuer subject requires instrument_id (subject_id={subject_id})")
    else:
        if instrument_id is not None:
            raise ValueError(
                f"record_poll_outcome: non-issuer subject must have instrument_id=None"
                f" (subject_type={subject_type!r}, subject_id={subject_id})"
            )

    state: FreshnessState
    if outcome == "error":
        state = "error"
    elif outcome == "never":
        state = "never_filed"
    else:
        state = "current"

    # Codex review: ``outcome='current'`` with no fresh ``last_known_filed_at``
    # must STILL push ``expected_next_at`` forward — otherwise a row
    # polled with "no new data" stays immediately due forever and the
    # worker re-polls the same CIK every tick. Use NOW()-anchored
    # cadence as a fallback when no filed_at is supplied.
    poll_now = datetime.now(tz=UTC)
    if last_known_filed_at is not None:
        expected_next_at = predict_next_at(source, last_known_filed_at)
    elif outcome in ("current", "new_data"):
        expected_next_at = poll_now + cadence_for(source)
    else:
        expected_next_at = None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_freshness_index (
                subject_type, subject_id, source,
                cik, instrument_id,
                last_known_filing_id, last_known_filed_at,
                last_polled_at, last_polled_outcome,
                new_filings_since,
                expected_next_at, next_recheck_at,
                state, state_reason
            ) VALUES (
                %(stype)s, %(sid)s, %(source)s,
                %(cik)s, %(iid)s,
                %(acc)s, %(filed_at)s,
                NOW(), %(outcome)s,
                %(new_count)s,
                %(next_at)s, %(recheck_at)s,
                %(state)s, %(reason)s
            )
            ON CONFLICT (subject_type, subject_id, source) DO UPDATE SET
                cik = COALESCE(EXCLUDED.cik, data_freshness_index.cik),
                instrument_id = COALESCE(
                    EXCLUDED.instrument_id, data_freshness_index.instrument_id
                ),
                last_known_filing_id = COALESCE(
                    EXCLUDED.last_known_filing_id, data_freshness_index.last_known_filing_id
                ),
                last_known_filed_at = COALESCE(
                    EXCLUDED.last_known_filed_at, data_freshness_index.last_known_filed_at
                ),
                last_polled_at = EXCLUDED.last_polled_at,
                last_polled_outcome = EXCLUDED.last_polled_outcome,
                new_filings_since = data_freshness_index.new_filings_since
                    + EXCLUDED.new_filings_since,
                expected_next_at = COALESCE(
                    EXCLUDED.expected_next_at, data_freshness_index.expected_next_at
                ),
                next_recheck_at = EXCLUDED.next_recheck_at,
                state = EXCLUDED.state,
                state_reason = EXCLUDED.state_reason
            """,
            {
                "stype": subject_type,
                "sid": subject_id,
                "source": source,
                "cik": cik,
                "iid": instrument_id,
                "acc": last_known_filing_id,
                "filed_at": last_known_filed_at,
                "outcome": outcome,
                "new_count": new_filings_since,
                "next_at": expected_next_at,
                "recheck_at": next_recheck_at,
                "state": state,
                "reason": error,
            },
        )


# ---------------------------------------------------------------------------
# Worker iterators
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FreshnessRow:
    subject_type: ManifestSubjectType
    subject_id: str
    source: ManifestSource
    cik: str | None
    instrument_id: int | None
    last_known_filing_id: str | None
    last_known_filed_at: datetime | None
    last_polled_at: datetime | None
    last_polled_outcome: PollOutcome
    new_filings_since: int
    expected_next_at: datetime | None
    next_recheck_at: datetime | None
    state: FreshnessState


def subjects_due_for_poll(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
    now: datetime | None = None,
) -> Iterator[FreshnessRow]:
    """Yield scheduler rows whose ``expected_next_at`` has elapsed.

    Codex review v3 finding 4: includes ``state='unknown'`` so rows
    reset by a rebuild (or freshly seeded) drain immediately rather
    than sitting in the future-poll queue.

    Ordering: ``expected_next_at ASC NULLS FIRST`` — never-filed-but-
    unknown rows (NULL expected) come first; otherwise oldest due row
    first.
    """
    if now is None:
        now = datetime.now(tz=UTC)

    where = (
        "state IN ('unknown', 'current', 'expected_filing_overdue')"
        " AND (expected_next_at IS NULL OR expected_next_at <= %s)"
    )
    params: list[Any] = [now]
    if source is not None:
        where += " AND source = %s"
        params.append(source)
    params.append(limit)

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT subject_type, subject_id, source, cik, instrument_id,
                   last_known_filing_id, last_known_filed_at,
                   last_polled_at, last_polled_outcome, new_filings_since,
                   expected_next_at, next_recheck_at, state
            FROM data_freshness_index
            WHERE {where}
            ORDER BY expected_next_at ASC NULLS FIRST
            LIMIT %s
            """,
            params,
        )
        for row in cur.fetchall():
            yield FreshnessRow(**row)


def subjects_due_for_recheck(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
    now: datetime | None = None,
) -> Iterator[FreshnessRow]:
    """Yield ``never_filed`` / ``error`` rows past their recheck window.

    Separate iterator so the worker can rate-limit recheck polling
    independently from the main scheduled-poll path. NULL
    ``next_recheck_at`` is treated as immediately due (covers the
    case where an error row is created without an explicit recheck
    cadence).
    """
    if now is None:
        now = datetime.now(tz=UTC)

    where = "state IN ('never_filed', 'error') AND (next_recheck_at IS NULL OR next_recheck_at <= %s)"
    params: list[Any] = [now]
    if source is not None:
        where += " AND source = %s"
        params.append(source)
    params.append(limit)

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT subject_type, subject_id, source, cik, instrument_id,
                   last_known_filing_id, last_known_filed_at,
                   last_polled_at, last_polled_outcome, new_filings_since,
                   expected_next_at, next_recheck_at, state
            FROM data_freshness_index
            WHERE {where}
            ORDER BY next_recheck_at ASC NULLS FIRST
            LIMIT %s
            """,
            params,
        )
        for row in cur.fetchall():
            yield FreshnessRow(**row)


def get_freshness_row(
    conn: psycopg.Connection[Any],
    *,
    subject_type: ManifestSubjectType,
    subject_id: str,
    source: ManifestSource,
) -> FreshnessRow | None:
    """Fetch one scheduler row by PK; returns None if absent."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT subject_type, subject_id, source, cik, instrument_id,
                   last_known_filing_id, last_known_filed_at,
                   last_polled_at, last_polled_outcome, new_filings_since,
                   expected_next_at, next_recheck_at, state
            FROM data_freshness_index
            WHERE subject_type = %s AND subject_id = %s AND source = %s
            """,
            (subject_type, subject_id, source),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return FreshnessRow(**row)
