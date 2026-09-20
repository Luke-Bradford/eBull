"""Whether a stored bar's LEVEL can be read as the level it traded at (#2840).

PROMOTED, NOT WRITTEN. Every rule here was already implemented and tested in
``scripts/census_2840_forward_daily_provenance.py``; this module is its new home
and that script now imports it. The move is the whole point: #2840's carrier
work needs an ADMISSION input, and a rule living in a census script is reachable
by a census and by nothing on a production path.

⚠ I reinvented this before finding it, and the reinvention was worse — a
"captured on the same New York calendar date" proxy, justified by not wanting to
resolve the session calendar. Measured against the real rule over the whole
store, the proxy refuses bars for which no session open intervened (a Friday
afternoon bar captured on the Saturday), and never the other way round. ⚠ The
count is deliberately NOT written here: it moves with every harvest. Reproduce
it with ``PYTHONPATH=. uv run python -m
scripts.census_2840_forward_daily_provenance --capture-certificate``, whose
output carries the population it was computed on. Recorded because the failure
mode is the reusable part: a proxy for a mechanism looks conservative and
silently discards evidence.

WHY A CAPTURE TIME CERTIFIES A LEVEL AT ALL
-------------------------------------------
A US corporate action takes effect at a US session OPEN. A bar observed with no
session open between its own completion and its capture therefore cannot have
been retroactively re-based: at capture time there had been no opportunity.

⚠⚠ AND THAT IS ONLY WORTH SOMETHING BECAUSE OF THE WRITER'S CONTRACT.
``strategy_observation_storage.store_intraday_bars`` writes each bar once:
``_INSERT_INTRADAY_BAR`` is a plain INSERT with NO ``ON CONFLICT`` against a
``(timeframe, bar_time, instrument_id)`` primary key, ``captured_at`` is a SQL
literal in that statement rather than a bound parameter so no caller can supply
it, and a bar at or behind the stored watermark RAISES rather than overwriting.
⚠ That is an APPLICATION convention, not a schema guarantee — ``sql/276``
carries no UPDATE/DELETE prohibition — and it makes the stored value the first
observation THIS WRITER ACCEPTED, not the first that ever existed. Add an upsert
there and ``captured_at`` silently becomes "last touched" while every caller
here keeps reading it as "first seen".

⚠⚠ THE STAMP MUST BE AN UPPER BOUND ON THE OBSERVATION, AND IT WAS NOT (#2840,
found at Codex checkpoint 2). ``sql/276`` defaulted the column to ``now()``,
which is ``transaction_timestamp()`` — the start of the ENCLOSING transaction.
``store_intraday_bars`` opens ``conn.transaction()``, a savepoint when the
caller already holds one, so a caller whose transaction began before the fetch
stamps a time EARLIER than the observation. This rule then places a post-open
observation before the open and certifies a bar it must refuse. ``sql/402``
moves the default to ``clock_timestamp()`` and the writer names it explicitly:
a statement time necessarily follows the fetch, so it can only cause a refusal.
⚠ Rows written before ``sql/402`` keep the old semantics and cannot be repaired
— the true observation instant of a stored bar is not recoverable.

WHAT THIS DOES NOT CLAIM
------------------------
Not that the provider serves nominal prices in general — ``91267518`` left
eToro's back-adjustment behaviour UNVERIFIED and this does not move it. Not that
an ``after_N_opens`` bar IS re-based; ``N`` is the number of opportunities a
split check would have to rule out, and the strong form would license reversing
an assumed factor, which could MANUFACTURE gate eligibility. Not that a
contemporaneous capture is unrevisable at SOURCE.

Refs #2840, #2437.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows

from app.services.market_calendar import RULE_SET_VERSION as CALENDAR_RULE_SET_VERSION
from app.services.market_calendar import us_market_status
from app.services.strategy_observation_storage import INTRADAY_TIERS, Timeframe

_NY: Final = ZoneInfo("America/New_York")

#: 09:30 ET. The instant a corporate action first shows in a traded price, which
#: is why this rule reads the OPEN and not the close.
_SESSION_OPEN: Final = time(9, 30)

#: The stable name of the rule. Bumped on a RULE change, never on a comment.
CAPTURE_CERTIFICATE_RULE_ID: Final = "bar-capture-certificate-v1"


def _tier_minutes_hash() -> str:
    """Hash of the per-tier bar lengths, which decide when a bar COMPLETED."""
    payload = repr(sorted((tier, INTRADAY_TIERS[tier].minutes_per_bar) for tier in INTRADAY_TIERS))
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


#: ⚠⚠ THE ID ALONE IS NOT THE VERSION, AND THAT WAS THE CHECKPOINT-2 FINDING.
#: This rule's verdict is a function of two things it does not own: the session
#: calendar (an added extraordinary closure moves ``next_session_open_utc``, so
#: a bar's bucket changes) and the tier bar lengths (which decide when a bar
#: completed). Under a bare id, two materially different admission decisions
#: would be indistinguishable in an audit record — the defect
#: ``strategy_registry.INPUT_RULE_SETS`` exists to prevent one layer up.
#:
#: Composed, not nested, following
#: ``strategy_forecast_outcome_resolution.RESOLVER_VERSION``
#: (``f"{RESOLVER_ID}+{_code_hash()}+path-{PATH_RULE_SET_VERSION}"``): a stable
#: id, this module's own source, then each dependency's version.
#:
#: ⚠ The cost is OVER-INVALIDATION — a comment edit in ``market_calendar`` moves
#: this string. That is the trade every rule-set version in this repo takes, and
#: the direction is right: a certificate that moves when nothing changed is
#: visibly stale, one that fails to move when the calendar did is silently wrong.
CAPTURE_CERTIFICATE_VERSION: Final = (
    f"{CAPTURE_CERTIFICATE_RULE_ID}"
    f"+{hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]}"
    f"+cal-{CALENDAR_RULE_SET_VERSION}"
    f"+tiers-{_tier_minutes_hash()}"
)

#: The one bucket in which no opportunity to re-base existed.
CERTIFIED_BUCKET: Final = "before_next_open"

#: The calendar search ran out of horizon before finding the next open, so
#: whether one intervened is UNKNOWN. A refusal, never a certification.
UNDETERMINABLE_BUCKET: Final = "undeterminable_next_open"

#: A capture that precedes its own bar's completion — a corrupt row, not an
#: early observation. Named rather than spelled out at each comparison because
#: it has to be EXCLUDED from any proxy comparison: such a row is same-session
#: by construction, so counting it as "refused by the rule" makes a must-be-zero
#: cell non-zero for a reason that has nothing to do with the proxy.
IMPOSSIBLE_BUCKET: Final = "impossible"


def next_session_open_utc(day: date, *, horizon_days: int = 30) -> datetime | None:
    """The next session's 09:30 ET in UTC — the first instant a split could re-base a bar.

    ``None`` when no session is found inside ``horizon_days``, which the caller must treat
    as "not determinable" rather than as "no open".

    ⚠ Reads ``us_market_status`` directly. The census original called
    ``expected_bars(cursor)`` and used its bar COUNT as a truthiness test for "is
    this a session". The two are equivalent — ``expected_bars`` returns 0 exactly
    when ``session_close`` is ``None``, which is exactly ``us_market_status ==
    "closed"`` — and ``tests/test_2840_bar_capture_certificate.py`` pins that
    equivalence over a year rather than asserting it. Dropping the hop keeps a
    13-bars-per-session ADMISSION rule out of a calendar question it has nothing
    to do with.
    """
    cursor = day + timedelta(days=1)
    limit = day + timedelta(days=horizon_days)
    while cursor <= limit:
        if us_market_status(cursor) != "closed":
            return datetime.combine(cursor, _SESSION_OPEN, tzinfo=_NY).astimezone(UTC)
        cursor += timedelta(days=1)
    return None


def nominality_bucket(bar_time: datetime, captured_at: datetime, *, timeframe: Timeframe = "30m") -> str:
    """Whether a split had an OPEN at which it could have re-based this bar before capture.

    ``before_next_open`` is the only bucket in which no opportunity existed. ``after_n_opens``
    counts how many session opens the capture sits beyond — it is NOT a defect label, it is
    the number of opportunities a split check would have to rule out.

    ⚠ ``impossible`` means the row is corrupt, not that the capture was early: a
    bar is written only after it completes, so a capture before ``bar_time +
    minutes_per_bar`` cannot have observed it. Whether the store holds any is a
    measurement, not a constant — the census arm named in the module docstring
    prints the bucket if it is ever non-empty.

    ⚠ The count SATURATES at 3. Beyond three opens the exact number stops
    informing the decision — the bar is not admissible either way — and an
    unbounded walk would scan the calendar to the end of the horizon for a bar
    captured months late.
    """
    minutes = INTRADAY_TIERS[timeframe].minutes_per_bar
    completed_at = bar_time.astimezone(UTC) + timedelta(minutes=minutes)
    captured = captured_at.astimezone(UTC)
    if captured < completed_at:
        return IMPOSSIBLE_BUCKET
    day = bar_time.astimezone(_NY).date()
    opens = 0
    cursor = day
    while opens < 3:
        next_open = next_session_open_utc(cursor)
        if next_open is None:
            # ⚠⚠ EXHAUSTING THE SEARCH IS NOT EVIDENCE THAT NO OPEN INTERVENED.
            # An earlier draft broke out here and fell through to
            # ``CERTIFIED_BUCKET``, i.e. the one branch where the rule has NO
            # information was also its most permissive — fail-open on absence,
            # which is the shape this whole module exists to refuse. It was
            # defended with "the horizon is longer than any real closure", an
            # argument rather than a guarantee, and an unverified one.
            #
            # Refusing here is also what lets ``horizon_days`` stay a plain
            # COMPUTE BOUND rather than a claim about market history: the
            # constant can be wrong without any verdict being wrong.
            return UNDETERMINABLE_BUCKET
        if captured < next_open:
            break
        opens += 1
        cursor = next_open.astimezone(_NY).date()
    return CERTIFIED_BUCKET if opens == 0 else f"after_{min(opens, 3)}_opens"


#: The stamping rule whose era ``sql/403`` records. Read from
#: ``intraday_capture_semantics`` rather than typed as an instant, because the
#: cutover is PER ENVIRONMENT — each database crosses over when its own
#: migrations run, and a literal would be right in exactly one of them.
CAPTURE_SEMANTICS_RULE_ID: Final = "clock_timestamp"

#: A bar whose capture semantics cannot be established. NOT a claim that the bar
#: is wrong — a claim that this rule cannot speak for it.
UNVERIFIABLE_BUCKET: Final = "unverifiable_capture_semantics"

#: ⚠⚠ THE PRECONDITION THIS RULE CANNOT SATISFY, AND THE REASON NOTHING IS
#: CERTIFIED TODAY (#2840, Codex checkpoint 2 P1).
#:
#: A session open bounds when a corporate action becomes ECONOMICALLY effective.
#: It does not bound when the PROVIDER rewrites its own history. If eToro
#: pre-adjusts a Friday candle ahead of a Monday split, ``captured_at`` is still
#: before the next open and the delivered level is already re-based — so the
#: open test is NECESSARY and not SUFFICIENT, and reading it as sufficient
#: converts an unverified premise into an admission.
#:
#: That premise is unverified on purpose: ``91267518`` narrowed eToro's
#: back-adjustment behaviour and did not resolve it, and the strong form would
#: license reversing an assumed factor, which could MANUFACTURE ≥$100 gate
#: eligibility. Until a confirmed split inside a reachable window settles it,
#: this flag stays ``False`` and ``capture_certificate`` refuses everything.
#:
#: ⚠ FLIPPING THIS IS A RULE CHANGE: bump ``CAPTURE_CERTIFICATE_RULE_ID`` in the
#: same commit, because every prior verdict was computed under the refusal.
PROVIDER_REWRITE_TIMING_VERIFIED: Final = False

#: What the refusal above is called when it fires. Distinct from
#: ``UNVERIFIABLE_BUCKET``: that one is about OUR stamp, this one is about the
#: PROVIDER's behaviour, and conflating them would hide which evidence is
#: missing when one of the two is eventually supplied.
UNVERIFIED_PROVIDER_BUCKET: Final = "unverified_provider_rewrite_timing"


def capture_semantics_cutover(conn: psycopg.Connection[Any]) -> datetime | None:
    """When this database started stamping ``captured_at`` from ``clock_timestamp()``.

    ``None`` when ``sql/403`` has not been applied here, which the caller must
    treat as "no row is certifiable" rather than as "every row is".

    ⚠ Reads ``intraday_capture_semantics`` and NOT ``schema_migrations.applied_at``.
    That column is ``now()`` — the migration transaction's START, recorded before
    its ``ALTER`` acquires ``ACCESS EXCLUSIVE`` — so a writer that inserted inside
    that window carries old semantics with a stamp at or after it, and the gate
    would admit it. ``sql/403`` records ``clock_timestamp()`` evaluated after the
    DDL instead. Caught at Codex checkpoint 2.
    """
    # ⚠ THE RELATION CHECK IS THE FAIL-CLOSED PATH, NOT BELT AND BRACES. On a
    # database that has not applied sql/403 — the exact case this function
    # documents as returning ``None`` — the table does not exist, so a bare
    # SELECT raises ``UndefinedTable`` and ABORTS THE CALLER'S TRANSACTION
    # instead of producing the refusal. ``to_regclass`` returns NULL rather than
    # raising, which is why it is used in place of a try/except: the exception
    # would already have poisoned the transaction by the time it was caught.
    # ⚠ AN EXPLICIT TUPLE ROW FACTORY, because this helper takes whatever
    # connection a caller has. Under ``row_factory=dict_row`` — which this repo
    # uses widely, including in the census that calls this — ``row[0]`` is a
    # ``KeyError``, so a gate written to fail closed would instead crash.
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        exists = cur.execute("SELECT to_regclass('intraday_capture_semantics')").fetchone()
        if exists is None or exists[0] is None:
            return None
        row = cur.execute(
            "SELECT effective_from FROM intraday_capture_semantics WHERE rule_id = %(rule_id)s",
            {"rule_id": CAPTURE_SEMANTICS_RULE_ID},
        ).fetchone()
    if row is None or row[0] is None:
        return None
    effective_from: datetime = row[0]
    return effective_from


def capture_certificate(
    bar_time: datetime,
    captured_at: datetime,
    *,
    timeframe: Timeframe = "30m",
    capture_semantics_from: datetime | None,
) -> str:
    """The ADMISSION verdict. ``nominality_bucket`` is only its arithmetic half.

    ⚠⚠ THE SPLIT IS THE POINT, AND IT IS A CHECKPOINT-2 FINDING. ``sql/402``'s
    own header says pre-migration rows cannot be repaired, and the first draft
    of this module then let them be certified anyway — the calendar arithmetic
    is identical for a row whose stamp is trustworthy and one whose stamp may
    precede its observation, so arithmetic alone cannot tell them apart.

    A bar stamped before this database applied ``sql/402`` carries a
    ``now()``-derived timestamp, which under a caller-held transaction can
    precede the fetch. Its verdict is ``unverifiable_capture_semantics``: not
    "this bar is re-based", but "this rule cannot speak for it".

    ⚠ ``capture_semantics_from`` is REQUIRED AND HAS NO DEFAULT, including no
    ``None`` default. A caller must state which database it is talking about;
    passing ``None`` is the explicit "the migration is not applied here" answer
    and refuses everything, which is the fail-closed direction.
    """
    if capture_semantics_from is None or captured_at < capture_semantics_from:
        return UNVERIFIABLE_BUCKET
    bucket = nominality_bucket(bar_time, captured_at, timeframe=timeframe)
    if bucket == CERTIFIED_BUCKET and not PROVIDER_REWRITE_TIMING_VERIFIED:
        # ⚠ Only the CERTIFYING verdict is downgraded. An ``after_n_opens`` bar is
        # already refused and relabelling it would destroy the count of
        # opportunities a later split check has to rule out.
        return UNVERIFIED_PROVIDER_BUCKET
    return bucket


__all__ = [
    "CAPTURE_SEMANTICS_RULE_ID",
    "PROVIDER_REWRITE_TIMING_VERIFIED",
    "UNVERIFIABLE_BUCKET",
    "UNVERIFIED_PROVIDER_BUCKET",
    "capture_certificate",
    "capture_semantics_cutover",
    "CAPTURE_CERTIFICATE_RULE_ID",
    "CAPTURE_CERTIFICATE_VERSION",
    "CERTIFIED_BUCKET",
    "next_session_open_utc",
    "nominality_bucket",
]
