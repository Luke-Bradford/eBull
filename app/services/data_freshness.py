"""Data freshness scheduler — when to next ASK SEC for new filings.

Issue #865 / spec §"data_freshness_index"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

Distinct from ``sec_filing_manifest`` (#864):

  - ``sec_filing_manifest`` answers "is accession X already on file?"
  - ``data_freshness_index`` answers "should I poll subject Y for source Z?"

The scheduler is subject-polymorphic — 13F-HR is filer-centric (one
filer's 13F covers many issuers), so the row carries ``subject_type``
+ ``subject_id`` rather than always (instrument_id, source).

TWO clocks, deliberately (#3109):

  - ``expected_next_at`` — the reconciliation deadline, ``last_known_filed_at``
    + ``_CADENCE[source]``. A statement about the FILER.
  - ``next_poll_at`` — poll eligibility, advanced by ``POLL_REPOLL_INTERVAL``
    on every poll. A statement about OUR request budget.

The queue reads the second. It used to read the first, and a filer last seen
in 1994 therefore had a permanently-elapsed "deadline", won ``ORDER BY ... ASC``
on every hourly tick, and starved the other 55,000 rows — 95 of them had ever
been polled in 3.5 months. Keep the two apart.

Calls from the worker layer:

  - ``seed_scheduler_from_manifest``: bootstrap rows from manifest history
  - ``record_poll_outcome``: record after a poll completes
  - ``ciks_due_for_poll``: worker pulls due rows, GROUPED BY CIK (#3109)
  - ``ciks_due_for_recheck``: never_filed / error rechecks, same grouping
  - ``subjects_due_for_poll`` / ``subjects_due_for_recheck``: the
    row-denominated originals. No production caller since #3109; they
    remain the row-level statement of each lane's candidacy predicate,
    which both forms share via ``_POLL_LANE_STATES`` /
    ``_RECHECK_LANE_STATES``.

The cadence map is hard-coded per the spec — adding a new source
means one edit here, not a sweep across the worker / providers.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal

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
# Poll-eligibility clock (#3109)
# ---------------------------------------------------------------------------
#
# ⚠ This is NOT ``_CADENCE``. ``_CADENCE`` answers "when is this subject's
# reconciliation deadline, given its last filing" — a per-source figure
# anchored to ``last_known_filed_at``. These two answer "when may we spend
# another request on this row", which is a property of OUR budget and has
# nothing to do with the filer. Conflating them is the #3109 defect: a
# filer last seen in 1994 has a permanently-elapsed reconciliation deadline,
# so it won the queue's ``ORDER BY`` on every tick forever and 95 of 55,775
# rows had ever been polled.
#
# Source rule: ``.claude/skills/data-sources/sec-edgar.md`` §"Strategies"
# item 4 ("Three-tier polling") — *"Cold: per-CIK submissions JSON re-pull
# weekly or per-event."* ``sec_per_cik_poll`` IS the cold tier. The
# "per-event" arm is the hot (Atom) and warm (daily-index) tiers, which
# already exist; this layer is the weekly safety net behind them.
#
# ⚠ That is an in-repo settled convention, NOT an SEC requirement — SEC
# publishes a rate ceiling (10 req/s) and no re-poll cadence at all. Stated
# rather than dressed up as a regulation, per ``.claude/CLAUDE.md``.
#
# ONE constant, not per-source: ``submissions.json`` is entity-wide, one
# fetch answers every source of that CIK, and since #3109's batching the
# budget is denominated in CIKs. A per-source interval would be incoherent
# with the unit the request is actually spent in.
POLL_REPOLL_INTERVAL: Final = timedelta(days=7)

# Backoff for a FAILED poll, so an errored row rotates instead of pinning.
#
# Before #3109 ``_record_subject_error`` supplied no ``next_recheck_at``, so
# the UPSERT wrote NULL — and the recheck selector treats NULL as immediately
# due. An errored row therefore sat at the head of the recheck lane forever:
# the identical starvation defect in the other lane.
#
# One hour is BY CONSTRUCTION, not chosen: it is ``sec_per_cik_poll``'s own
# tick, the smallest interval at which a retry can actually happen. The value
# matters less than its finiteness — any advance converts starvation into
# rotation, because N errored CIKs then rotate by oldest-deadline instead of
# the lowest CIK monopolising every slot.
ERROR_RECHECK_INTERVAL: Final = timedelta(hours=1)


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
    # FINRA RegSHO daily short volume — daily EOD publication (#916).
    # 2-day cadence ceiling allows 1 weekend + 1 holiday slack before
    # the freshness panel paints the slot as overdue.
    "finra_regsho_daily": timedelta(days=2),
    # Late-filing notices (Form 12b-25) — EPISODIC, not periodic (#1015). A
    # healthy filer never files an NT, so there is no real "staleness"; a
    # generous ceiling keeps the freshness panel from painting NT overdue for
    # an instrument that simply has nothing to file. NT rows piggyback on the
    # per-CIK submissions poll for discovery, same as every other SEC form.
    "sec_nt": timedelta(days=400),
    # PRE 14A / PRER14A (#1892, #1015 item 3) — episodic like NT: a proxy
    # season fires once (or twice, if a preliminary revision is filed) a
    # year, so a generous ceiling avoids painting the source overdue between
    # meetings. Mirrors sec_nt's rationale.
    "sec_pre14a": timedelta(days=400),
    # 424B offerings (#1816) — episodic, event-driven: most issuers never
    # file one, so a generous ceiling keeps a non-issuing name from ever
    # reading "stale". Mirrors sec_nt's rationale.
    "sec_424b": timedelta(days=400),
    # Tender / going-private schedules (#1982) — episodic like NT/424B: a
    # never-tendered name must never read "stale". Mirrors sec_nt's rationale.
    "sec_tender": timedelta(days=400),
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


def seed_freshness_for_manifest_row(
    conn: psycopg.Connection[Any],
    *,
    subject_type: ManifestSubjectType,
    subject_id: str,
    source: ManifestSource,
    cik: str | None,
    instrument_id: int | None,
    accession_number: str,
    filed_at: datetime,
) -> None:
    """Single-row scheduler seed for one manifest write (#956).

    Companion to ``seed_scheduler_from_manifest`` (the bulk full-table
    rebuild). Called inline from ``record_manifest_entry`` so every
    manifest discovery write — Atom fast-lane, daily-index reconcile,
    per-CIK poll, targeted rebuild, first-install drain — leaves the
    scheduler queryable. Pre-#956 only the drain seeded; the others
    relied on the next full ``seed_scheduler_from_manifest`` to pick
    up new triples, leaving them invisible until that ran.

    Latest-row semantics: the freshness ``last_known_*`` columns must
    reflect the LATEST filing per (subject, source). Discovery writers
    can call this with an older accession (rebuild walking
    ``filings.files[]`` secondary pages, daily-index reconcile picking
    up a missed older row, etc.) — the conditional UPDATE preserves
    the existing newer values rather than letting ``EXCLUDED`` clobber
    them.
    """
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
                -- Latest-row preservation. The "newer" gate drives
                -- ALL watermark fields (filing_id + filed_at +
                -- expected_next_at) uniformly so they never diverge.
                -- Older discovery writes (rebuild's secondary-page
                -- walk, daily-index reconcile catching up) leave the
                -- existing watermark intact.
                last_known_filing_id = CASE
                    WHEN data_freshness_index.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at > data_freshness_index.last_known_filed_at
                    THEN EXCLUDED.last_known_filing_id
                    ELSE data_freshness_index.last_known_filing_id
                END,
                last_known_filed_at = CASE
                    WHEN data_freshness_index.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at > data_freshness_index.last_known_filed_at
                    THEN EXCLUDED.last_known_filed_at
                    ELSE data_freshness_index.last_known_filed_at
                END,
                expected_next_at = CASE
                    WHEN data_freshness_index.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at > data_freshness_index.last_known_filed_at
                    THEN EXCLUDED.expected_next_at
                    ELSE data_freshness_index.expected_next_at
                END,
                -- State: only ESCALATE from ``never_filed`` to
                -- ``current`` (manifest evidence shows the subject
                -- has filed). Don't clobber legitimate poll-outcome
                -- states like ``error`` / ``expected_filing_overdue``
                -- on a duplicate / older re-discovery write — those
                -- carry meaning from the per-CIK poll lifecycle and
                -- a noisy Atom replay shouldn't reset them.
                -- Codex pre-push catch.
                state = CASE
                    WHEN data_freshness_index.state = 'never_filed' THEN 'current'
                    ELSE data_freshness_index.state
                END,
                state_reason = CASE
                    WHEN data_freshness_index.state = 'never_filed' THEN NULL
                    ELSE data_freshness_index.state_reason
                END,
                next_recheck_at = CASE
                    WHEN data_freshness_index.state = 'never_filed' THEN NULL
                    ELSE data_freshness_index.next_recheck_at
                END
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

    # #3109 — a FAILED poll must still get a finite deadline. Before this,
    # ``_record_subject_error`` supplied no ``next_recheck_at`` and the UPSERT
    # wrote NULL; ``ciks_due_for_recheck`` treats NULL as immediately due, so
    # an errored row pinned the head of the recheck lane forever — the same
    # starvation defect this ticket fixes in the poll lane. Applied here rather
    # than at the call site so every caller inherits the invariant.
    #
    # An explicit deadline from the caller always wins (the 304 path supplies
    # ``cadence_for(source)`` for a ``never_filed`` subject).
    if outcome == "error" and next_recheck_at is None:
        next_recheck_at = poll_now + ERROR_RECHECK_INTERVAL

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_freshness_index (
                subject_type, subject_id, source,
                cik, instrument_id,
                last_known_filing_id, last_known_filed_at,
                last_polled_at, last_polled_outcome,
                new_filings_since,
                expected_next_at, next_recheck_at, next_poll_at,
                state, state_reason
            ) VALUES (
                %(stype)s, %(sid)s, %(source)s,
                %(cik)s, %(iid)s,
                %(acc)s, %(filed_at)s,
                NOW(), %(outcome)s,
                %(new_count)s,
                %(next_at)s, %(recheck_at)s, NOW() + %(repoll)s::interval,
                %(state)s, %(reason)s
            )
            ON CONFLICT (subject_type, subject_id, source) DO UPDATE SET
                cik = COALESCE(EXCLUDED.cik, data_freshness_index.cik),
                instrument_id = COALESCE(
                    EXCLUDED.instrument_id, data_freshness_index.instrument_id
                ),
                -- #1534 — monotonic watermark guard (mirrors the discovery
                -- seed path above). Before per_cik_poll got its own lane it
                -- serialised with the other sec_rate discovery producers
                -- (sec_atom_fast_lane / daily-index), so its read-snapshot →
                -- fetch → write-back could not race their freshness writes.
                -- Now that it runs concurrently, a poll that saw no newer
                -- filing must NOT regress a watermark a concurrent producer
                -- just advanced: gate last_known_filing_id + last_known_filed_at
                -- on filed_at so a stale snapshot leaves the newer value
                -- intact. expected_next_at deliberately stays EXCLUDED (the
                -- poll owns its own re-poll cadence — line ~394 — and must
                -- advance even on a no-new-data poll, unlike the seed path).
                last_known_filing_id = CASE
                    WHEN data_freshness_index.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at > data_freshness_index.last_known_filed_at
                    THEN EXCLUDED.last_known_filing_id
                    ELSE data_freshness_index.last_known_filing_id
                END,
                last_known_filed_at = CASE
                    WHEN data_freshness_index.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at > data_freshness_index.last_known_filed_at
                    THEN EXCLUDED.last_known_filed_at
                    ELSE data_freshness_index.last_known_filed_at
                END,
                last_polled_at = EXCLUDED.last_polled_at,
                last_polled_outcome = EXCLUDED.last_polled_outcome,
                new_filings_since = data_freshness_index.new_filings_since
                    + EXCLUDED.new_filings_since,
                -- #1534 — tie expected_next_at to the winning watermark so a
                -- STRICTLY-stale poll cannot leave the row too-soon-due while
                -- a newer watermark is preserved above. Uses ``>=`` (not the
                -- watermark's strict ``>``) on purpose: the common no-new-data
                -- re-poll arrives with EXCLUDED.filed_at == existing and MUST
                -- still advance its own next-poll cadence (line ~394) — only a
                -- strictly-older incoming filed_at keeps the existing value.
                expected_next_at = CASE
                    WHEN data_freshness_index.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at IS NULL
                      OR EXCLUDED.last_known_filed_at >= data_freshness_index.last_known_filed_at
                    THEN EXCLUDED.expected_next_at
                    ELSE data_freshness_index.expected_next_at
                END,
                next_recheck_at = EXCLUDED.next_recheck_at,
                -- #3109 — the poll-eligibility clock, advanced UNCONDITIONALLY.
                -- Deliberately NOT guarded like ``expected_next_at`` above: that
                -- guard is about watermark regression under concurrent discovery
                -- producers (#1534), whereas this records that WE JUST SPENT A
                -- REQUEST. A losing or stale poll consumed the fetch just as a
                -- winning one did, so the clock must move either way — otherwise
                -- the row returns to the head of the queue and starves the rest,
                -- which is the whole defect.
                next_poll_at = EXCLUDED.next_poll_at,
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
                "repoll": POLL_REPOLL_INTERVAL,
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
    # #3109 — the poll-eligibility clock. NOT NULL in the schema, so unlike
    # its two neighbours above this is never None on a row read from the DB.
    next_poll_at: datetime
    state: FreshnessState


# Each lane's candidacy predicate, named ONCE so the row-denominated readers
# (``subjects_due_for_*``) and the CIK-denominated ones (``ciks_due_for_*``)
# cannot drift about what "due" means. Two expressions of one rule is how the
# two diverge later, silently.
_POLL_LANE_STATES: Final[tuple[str, ...]] = ("unknown", "current", "expected_filing_overdue")
_RECHECK_LANE_STATES: Final[tuple[str, ...]] = ("never_filed", "error")


def subjects_due_for_poll(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
    now: datetime | None = None,
) -> Iterator[FreshnessRow]:
    """Yield scheduler rows whose ``next_poll_at`` has elapsed.

    Codex review v3 finding 4: includes ``state='unknown'`` so rows
    reset by a rebuild (or freshly seeded) drain immediately rather
    than sitting in the future-poll queue.

    ⚠ **#3109 — this lane is keyed on ``next_poll_at``, NOT on
    ``expected_next_at``.** The latter is the reconciliation deadline derived
    from ``last_known_filed_at``; for a filer last seen in 1994 it is
    permanently elapsed, so keying eligibility on it pinned the same rows to
    the head of the queue on every tick and 95 of 55,775 rows had ever been
    polled. ``next_poll_at`` is OUR budget's clock and advances on every poll,
    which turns the queue from a fixed head into a rotation.

    Ordering: ``next_poll_at ASC``. No ``NULLS FIRST`` — the column is
    ``NOT NULL``, which is what stops a stream of newly seeded rows from
    preempting the tail forever.

    ⚠ **No production caller since #3109** — ``sec_per_cik_poll`` moved to
    ``ciks_due_for_poll``, whose budget is denominated in CIKs. Kept because
    it is the row-level statement of this lane's candidacy predicate and its
    tests pin it; the predicate itself is shared via ``_POLL_LANE_STATES``
    so the two readers cannot disagree about what "due" means.
    """
    if now is None:
        now = datetime.now(tz=UTC)

    where = "state = ANY(%s) AND next_poll_at <= %s"
    params: list[Any] = [list(_POLL_LANE_STATES), now]
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
                   expected_next_at, next_recheck_at, next_poll_at, state
            FROM data_freshness_index
            WHERE {where}
            ORDER BY next_poll_at ASC, cik ASC
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

    where = "state = ANY(%s) AND (next_recheck_at IS NULL OR next_recheck_at <= %s)"
    params: list[Any] = [list(_RECHECK_LANE_STATES), now]
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
                   expected_next_at, next_recheck_at, next_poll_at, state
            FROM data_freshness_index
            WHERE {where}
            ORDER BY next_recheck_at ASC NULLS FIRST
            LIMIT %s
            """,
            params,
        )
        for row in cur.fetchall():
            yield FreshnessRow(**row)


# The CIK shape a SEC submissions poll can actually address: the URL is built
# from ``_zero_pad_cik`` (``sec_submissions.py``) and the manifest writer
# enforces the same 10-digit form (``_MANIFEST_CIK_RE``).
#
# ⚠⚠ This REPLACES the ``subject.cik is None`` guard the per-CIK poll used to
# carry, which was dead code for the wrong reason (#3109). That guard was
# written for the FINRA universe singletons — and those rows do not have a NULL
# cik, they have the literal strings ``FINRA_REGSHO`` / ``FINRA_SI``. Full
# population 2026-09-16: 0 NULL ciks, 2 non-numeric ones, both of them those
# singletons, both ``state='current'`` and therefore eligible. A poll of one
# fetches ``.../submissions/CIKFINRA_SI.json`` (``str.zfill`` leaves a
# 12-character string alone), takes the 404 branch, and writes
# ``outcome='current'`` — certifying a FINRA subject off a 404 from an endpoint
# that never served it.
#
# Narrowing gate, so state what it REJECTS rather than what it keeps. Exactly
# two rows today, enumerated by ``scripts/measure_3109_batching.py`` M0:
#   ('FINRA_REGSHO', 'finra_universe', 'finra_regsho_daily', 'current')
#   ('FINRA_SI',     'finra_universe', 'finra_short_interest', 'current')
_SEC_POLLABLE_CIK_RE = r"^[0-9]{1,10}$"

_FRESHNESS_COLUMNS = """
    subject_type, subject_id, source, cik, instrument_id,
    last_known_filing_id, last_known_filed_at,
    last_polled_at, last_polled_outcome, new_filings_since,
    expected_next_at, next_recheck_at, next_poll_at, state
"""


def _ciks_due(
    conn: psycopg.Connection[Any],
    *,
    deadline_column: Literal["next_poll_at", "next_recheck_at"],
    states: tuple[str, ...],
    source: ManifestSource | None,
    limit: int,
    now: datetime,
) -> list[list[FreshnessRow]]:
    """Shared body of ``ciks_due_for_poll`` / ``ciks_due_for_recheck``.

    Returns the due rows of the ``limit`` most urgent CIKs, **grouped by CIK**
    and ordered most-urgent-CIK first. One inner list is one batch: every row
    in it shares a zero-padded CIK, so one ``submissions.json`` fetch serves
    all of them (#3109).

    ``deadline_column`` is interpolated, not bound — it is one of two literals
    chosen by the caller, never user input.

    Three things in the SQL are load-bearing and each was a defect in the first
    draft of this query:

    1. ``COALESCE(<deadline>, '-infinity')`` sits INSIDE the ``MIN``. ``MIN``
       ignores NULLs, so a CIK with deadlines ``{NULL, yesterday}`` would rank
       on ``yesterday`` and fall behind an all-NULL CIK — inverting the
       NULL-is-most-urgent semantics the row-level readers get from
       ``ORDER BY ... NULLS FIRST``. Folding the NULL into the key makes the
       group rank agree with the row rank. ⚠ Since #3109 this is UNREACHABLE
       for the poll lane — ``next_poll_at`` is ``NOT NULL`` — but the recheck
       lane's ``next_recheck_at`` is still nullable, and this is one shared
       body. Do not remove it when reading the poll lane's schema.
    2. ``cik_padded`` is INSIDE the ``DENSE_RANK`` ordering, so the rank is one
       per CIK and the tie-break is deterministic — the ticket's explicit
       requirement. Ranking on the deadline alone collapses every CIK sharing a
       timestamp into a single rank and blows the budget.
    3. Grouping is on ``lpad(cik, 10, '0')``, the expression the fetch URL is
       built from, so padding variants of one entity cannot become two batches.
       The ``{1,10}`` bound in ``_SEC_POLLABLE_CIK_RE`` is what makes ``lpad``
       safe here — it cannot truncate a string it never sees longer than 10.
    """
    params: dict[str, Any] = {
        "now": now,
        "limit": limit,
        "states": list(states),
        "cik_shape": _SEC_POLLABLE_CIK_RE,
    }
    source_clause = ""
    if source is not None:
        # Applied BEFORE ranking: filtering after would let a scoped call spend
        # its CIK budget on CIKs whose only due rows are other sources.
        source_clause = " AND source = %(source)s"
        params["source"] = source

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            WITH due AS (
                SELECT {_FRESHNESS_COLUMNS},
                       lpad(cik, 10, '0') AS cik_padded,
                       MIN(COALESCE({deadline_column}, '-infinity'::timestamptz))
                           OVER (PARTITION BY lpad(cik, 10, '0')) AS cik_rank_key
                FROM data_freshness_index
                WHERE state = ANY(%(states)s)
                  AND ({deadline_column} IS NULL OR {deadline_column} <= %(now)s)
                  AND cik ~ %(cik_shape)s
                  {source_clause}
            ), ranked AS (
                SELECT *, DENSE_RANK() OVER (ORDER BY cik_rank_key, cik_padded) AS cik_rank
                FROM due
            )
            SELECT {_FRESHNESS_COLUMNS}, cik_padded
            FROM ranked
            WHERE cik_rank <= %(limit)s
            ORDER BY cik_rank, source, subject_type, subject_id
            """,
            params,
        )
        rows = cur.fetchall()

    batches: list[list[FreshnessRow]] = []
    current_cik: str | None = None
    for row in rows:
        cik_padded = row.pop("cik_padded")
        # ⚠⚠ Carry the PADDED cik onto the row, not the stored one. The SQL
        # groups on ``lpad(cik, 10, '0')``, so ``320193`` and ``0000320193``
        # correctly land in one batch — but returning each row's original
        # string would then hand ``_probe_cik`` a batch whose members disagree
        # about their own CIK, tripping its one-CIK assertion and aborting the
        # WHOLE run before any subject is fetched or any outcome written.
        # Padding is not constrained by the schema, and 10 digits is what both
        # the submissions URL and ``_MANIFEST_CIK_RE`` require, so the padded
        # form is the correct value to carry downstream in either case.
        # Measured 0 padding variants on 2026-09-16; this is a latent crash,
        # not a live one. Found by Codex checkpoint 2.
        row["cik"] = cik_padded
        if cik_padded != current_cik:
            batches.append([])
            current_cik = cik_padded
        batches[-1].append(FreshnessRow(**row))
    return batches


def ciks_due_for_poll(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
    now: datetime | None = None,
) -> list[list[FreshnessRow]]:
    """CIK-grouped counterpart of ``subjects_due_for_poll`` (#3109).

    Same candidate states and same due predicate; the budget is denominated in
    **CIKs** instead of rows, and every due row of a selected CIK comes back in
    that CIK's batch. ``submissions.json`` is entity-wide, so one fetch answers
    the whole batch — where the row-denominated reader spent one identical
    fetch per row.

    ⚠ Eligibility is an EXCLUSION on ``next_poll_at``, not merely an ordering:
    a polled row is gone from this lane for ``POLL_REPOLL_INTERVAL``. That is
    what makes the queue rotate. It is **per row**, not per CIK — a sibling row
    of the same CIK that becomes due later re-fetches that CIK, and a CIK with
    rows in both lanes costs two fetches (see ``ciks_due_for_recheck``). Both
    are bounded duplicate fetches, not starvation.

    Measured on the dev corpus 2026-09-16
    (``scripts/measure_3109_batching.py``): the row reader's 66-row prefix
    covers 48 distinct CIKs (in-prefix fan-out 1.375x); a 66-CIK budget covers
    127 rows across 66 CIKs (1.924x). ⚠ Those are a snapshot, not an invariant
    — and note the population fan-out is 3.60x, which does NOT describe an
    ordered prefix.
    """
    return _ciks_due(
        conn,
        deadline_column="next_poll_at",
        states=_POLL_LANE_STATES,
        source=source,
        limit=limit,
        now=now if now is not None else datetime.now(tz=UTC),
    )


def ciks_due_for_recheck(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
    now: datetime | None = None,
) -> list[list[FreshnessRow]]:
    """CIK-grouped counterpart of ``subjects_due_for_recheck`` (#3109).

    ⚠ The two lanes are selected INDEPENDENTLY and are not merged, so a CIK
    with both poll-lane and recheck-lane rows costs two fetches — exactly as it
    does today, where they are two separate probes. Merging them would change
    the budget contract #1155 G13's 2/3-1/3 split exists to enforce.
    """
    return _ciks_due(
        conn,
        deadline_column="next_recheck_at",
        states=_RECHECK_LANE_STATES,
        source=source,
        limit=limit,
        now=now if now is not None else datetime.now(tz=UTC),
    )


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
                   expected_next_at, next_recheck_at, next_poll_at, state
            FROM data_freshness_index
            WHERE subject_type = %s AND subject_id = %s AND source = %s
            """,
            (subject_type, subject_id, source),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return FreshnessRow(**row)
