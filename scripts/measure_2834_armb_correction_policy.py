"""ARM B §7 item 1 — the correction policy for DISPUTED split stamps. Refs #2834.

``b65abd9c`` (PR #3279) settled the signal basis: ``icyDenev/Intrader`` ships a
per-bar split ratio and the split-only series is ``close / scale``. ``8bb1443f``
(PR #3280) then measured that **no non-circular adjudicator is available** — SEC
XBRL covers 4.52% of stamps and returns a non-direct answer on 12.0% of a CONTROL
arm where nothing is in dispute, so it cannot settle the 30 disputed events it
reaches.

So the policy has to be chosen without an umpire. This script measures the four
candidates named in the verdict's §7 item 1 — apply / suppress / re-date /
quarantine — on the two things that decide between them: **how much of the signal
each one moves**, and **what each one costs on the half of the corpus that has no
second processing**.

Read-only. Writes nothing. One connection, one transaction.

Source rule, and where it runs out
----------------------------------

The APPLICATION rule is the archive's own reader and is already settled
(``IntraderEngine::SplitCheck()`` — the ratio is new-shares-per-old, stamped on
the bar where the split takes effect; ``back_adjust_scale`` implements it).

⚠ **There is NO published rule for what to do with a stamp a second processing
disputes**, and this script does not invent one. It reports what each candidate
does and what it costs; the choice is fixed **by construction** in the follow-on
spec and frozen there, per ``.claude/CLAUDE.md`` ("where a published formulation
genuinely does NOT exist … say so explicitly and fix the rule by construction").

What the arms are
-----------------

Every arm is a SCALE SET over the same stored raw closes. The uncorrected status
quo is not a special case — it is the arm whose scale is 1 everywhere, which is
why all four are computed in one panel pass and compared pairwise.

* ``suppress_all``  — today's s2. No stamp is applied; every split stays in the
  series as a raw level break.
* ``apply_all``     — every stamped event applies. Disputes are not adjudicated.
* ``apply_unless_refuted`` — apply, except where the second processing REFUTES
  the stamp (``split_error > tolerance``). Unadjudicable events still apply.
* ``apply_only_corroborated`` — apply ONLY where the second processing confirms
  the stamp. Both refuted and unadjudicable events are suppressed. This is the
  reference-gated policy, and §4 of the ``b65abd9c`` verdict is the reason to
  suspect it: the reference serves the SURVIVING half of the corpus.

``quarantine`` is the fifth candidate and is deliberately NOT an arm here. It
does not change a score, it removes SERIES, so its effect is a universe cost and
a displacement percentage computed against a different cross-section would not
mean the same thing as the other three. It is reported as what it deletes — in
ONE narrow form, "delete the series if any stamp is refuted". Event-level
quarantine, segmenting at the disputed bar and per-class treatment are unmeasured
alternatives, not refused ones.

``re_date`` is reported as a probe HIT COUNT rather than an arm. ⚠ It is not a
bound in either direction: false matches inflate it, and it only looks +/-3 bars
on the events the reference can adjudicate at all.

⚠⚠ Arm 5 exists because the other arms cannot see DIRECTION. s2 selects the TOP
decile, so an error that inflates a score buys a position and an error that
depresses one causes a miss — the two are not equally bad, and a set-distance
percentage treats them identically.

⚠ The eligibility floor stays on RAW ``close`` in every arm
-----------------------------------------------------------

``MIN_CLOSE`` is applied to the stored close, identically across arms, so the
admitted set is the same in all four and the only thing that moves is the score.
Letting the floor follow each arm's basis would confound membership with signal
and make the pairwise numbers uninterpretable. It also leaves §7 item 3 (what the
floor should read on a back-adjusted basis) open, which is where it belongs — it
is a strategy-definition question, not a correction-policy one.

Usage::

    PYTHONPATH=. uv run python -m scripts.measure_2834_armb_correction_policy
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Final, TextIO

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION, params_for
from app.services.research_corpus_ingest import ASSET_CLASS
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    SKIP_BARS,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import load_universe_selection

# The predecessor owns the stamp reader, the cross-vendor check and the
# back-adjustment. Re-deriving any of them here would be a second implementation
# of the same rule, and the two would drift.
from scripts.measure_2834_armb_split_only_basis import (
    ELIGIBILITY_BARS,
    WINDOW_END,
    WINDOW_START,
    EventCheck,
    SplitEvent,
    back_adjust_scale,
    check_events,
    find_offset_steps,
    read_split_events,
)

#: T3's trigger magnitude for THIS corpus, read from the rule set rather than
#: restated — ``research_corpus_ingest`` hands ``ASSET_CLASS`` to
#: ``evaluate_series`` (``:1014``) and ``params_for`` picks the row. A second
#: literal is how a threshold comes to mean two things.
_T3_TRIGGER_MAGNITUDE: Final[Decimal] = params_for(ASSET_CLASS).magnitude_threshold

#: Arm order is fixed so the pairwise table reads the same way every run.
_ARMS: Final[tuple[str, ...]] = (
    "suppress_all",
    "apply_all",
    "apply_unless_refuted",
    "apply_only_corroborated",
)


@dataclass(frozen=True)
class PairFormation:
    """One rebalance date's decile, and the overlap between every pair of arms."""

    bar_date: date
    cross_section: int
    decile_size: int
    #: ``(arm_i, arm_j) -> members of arm i's decile that are not in arm j's``.
    #: Symmetric at equal decile sizes, which they are: same cross-section.
    displaced: dict[tuple[str, str], int]


def _selection(conn: psycopg.Connection[tuple[Any, ...]]) -> Any:
    """The SETTLED ``survivorship_free`` admission — the rule, never a vendor filter."""
    validated = frozenset(load_validated_universe(conn))
    return load_universe_selection(conn, universe="survivorship_free", validated_ids=validated)


_CALENDAR_SQL = """
    CREATE TEMP TABLE cp_rebalance ON COMMIT DROP AS
    WITH weekdays AS (
        SELECT DISTINCT d.bar_date
        FROM research_price_daily d
        WHERE d.series_id = ANY(%(series_ids)s)
          AND extract(isodow FROM d.bar_date) < 6
    ),
    seq AS (
        SELECT bar_date, lag(bar_date) OVER (ORDER BY bar_date) AS previous
        FROM weekdays
    )
    SELECT bar_date
    FROM seq
    WHERE previous IS NOT NULL
      AND date_trunc('month', bar_date) <> date_trunc('month', previous)
      AND bar_date >= %(window_start)s
      AND bar_date < %(window_end)s
"""

#: ⚠ The quarantine join pins ``rule_set_version`` on BOTH the coverage and the
#: verdict table, per the predecessor. Joining verdicts alone fails OPEN outside
#: the rows the current rule set covers.
#:
#: ⚠⚠ The masking here is BAR-level (``research_bar_quarantine.return_usable``)
#: and that is the whole of the containment this corpus has. The rule set also
#: writes TRANSITION verdicts — T3 is "a level break is not a return", which is
#: precisely what an uncorrected split is — into ``research_transition_quarantine``,
#: and no reader in the backtest path consumes them (``backtest_run.py:4170-4175``
#: names its three reads and that table is not one). On the eToro corpus the same
#: verdicts ARE honoured, via ``price_series_break`` / ``price_segments``
#: (settled-decisions, 2026-09-15, clause 2). The research corpus has no
#: equivalent break layer at all. So on THIS corpus the correction policy is the
#: only thing standing between a split and the momentum score.
_PANEL_SQL = """
    CREATE TEMP TABLE cp_panel ON COMMIT DROP AS
    WITH bars AS (
        SELECT d.series_id,
               d.bar_date,
               CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.close END AS close,
               CASE WHEN COALESCE(q.return_usable, TRUE)
                    THEN d.close / COALESCE(s1.scale, 1) END AS close_1,
               CASE WHEN COALESCE(q.return_usable, TRUE)
                    THEN d.close / COALESCE(s2.scale, 1) END AS close_2,
               CASE WHEN COALESCE(q.return_usable, TRUE)
                    THEN d.close / COALESCE(s3.scale, 1) END AS close_3
        FROM research_price_daily d
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = d.series_id
         AND cov.rule_set_version = %(version)s
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(version)s
        LEFT JOIN cp_scale s1
          ON s1.arm = 'apply_all'
         AND s1.series_id = d.series_id
         AND d.bar_date >= s1.valid_from AND d.bar_date < s1.valid_to
        LEFT JOIN cp_scale s2
          ON s2.arm = 'apply_unless_refuted'
         AND s2.series_id = d.series_id
         AND d.bar_date >= s2.valid_from AND d.bar_date < s2.valid_to
        LEFT JOIN cp_scale s3
          ON s3.arm = 'apply_only_corroborated'
         AND s3.series_id = d.series_id
         AND d.bar_date >= s3.valid_from AND d.bar_date < s3.valid_to
        WHERE d.series_id = ANY(%(series_ids)s)
          AND extract(isodow FROM d.bar_date) < 6
    ),
    windowed AS (
        SELECT series_id,
               bar_date,
               close,
               lag(close,   %(skip)s)     OVER s AS c0_skip,
               lag(close,   %(lookback)s) OVER s AS c0_back,
               lag(close_1, %(skip)s)     OVER s AS c1_skip,
               lag(close_1, %(lookback)s) OVER s AS c1_back,
               lag(close_2, %(skip)s)     OVER s AS c2_skip,
               lag(close_2, %(lookback)s) OVER s AS c2_back,
               lag(close_3, %(skip)s)     OVER s AS c3_skip,
               lag(close_3, %(lookback)s) OVER s AS c3_back,
               row_number()               OVER s AS rn,
               count(*) OVER (PARTITION BY series_id) AS n_bars
        FROM bars
        WINDOW s AS (PARTITION BY series_id ORDER BY bar_date)
    )
    SELECT w.series_id,
           w.bar_date,
           w.c0_skip / w.c0_back - 1 AS score_0,
           w.c1_skip / w.c1_back - 1 AS score_1,
           w.c2_skip / w.c2_back - 1 AS score_2,
           w.c3_skip / w.c3_back - 1 AS score_3
    FROM windowed w
    JOIN cp_rebalance r ON r.bar_date = w.bar_date
    WHERE w.rn >= %(eligibility)s
      AND w.rn < w.n_bars
      AND w.close IS NOT NULL
      AND w.close >= %(floor)s
      AND w.c0_skip IS NOT NULL AND w.c0_skip > 0
      AND w.c0_back IS NOT NULL AND w.c0_back > 0
      AND w.c1_skip IS NOT NULL AND w.c1_skip > 0
      AND w.c1_back IS NOT NULL AND w.c1_back > 0
      AND w.c2_skip IS NOT NULL AND w.c2_skip > 0
      AND w.c2_back IS NOT NULL AND w.c2_back > 0
      AND w.c3_skip IS NOT NULL AND w.c3_skip > 0
      AND w.c3_back IS NOT NULL AND w.c3_back > 0
"""

#: Pairwise decile overlap. ⚠ ``pos_N <= decile_size`` on each arm SEPARATELY —
#: a single "is it in the decile" flag computed once and reused would silently
#: compare an arm against itself.
_OVERLAP_SQL = """
    WITH ranked AS (
        SELECT p.bar_date,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_0 DESC, nk.name_key) AS pos_0,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_1 DESC, nk.name_key) AS pos_1,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_2 DESC, nk.name_key) AS pos_2,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_3 DESC, nk.name_key) AS pos_3,
               count(*)     OVER (PARTITION BY p.bar_date) AS n
        FROM cp_panel p
        JOIN cp_name nk ON nk.series_id = p.series_id
    ),
    cut AS (
        SELECT bar_date,
               n,
               n / %(decile)s AS decile_size,
               pos_0 <= n / %(decile)s AS in_0,
               pos_1 <= n / %(decile)s AS in_1,
               pos_2 <= n / %(decile)s AS in_2,
               pos_3 <= n / %(decile)s AS in_3
        FROM ranked
        WHERE n >= %(min_cross_section)s
    )
    SELECT bar_date,
           max(n)                                          AS cross_section,
           max(decile_size)                                AS decile_size,
           count(*) FILTER (WHERE in_0 AND NOT in_1)       AS d01,
           count(*) FILTER (WHERE in_0 AND NOT in_2)       AS d02,
           count(*) FILTER (WHERE in_0 AND NOT in_3)       AS d03,
           count(*) FILTER (WHERE in_1 AND NOT in_2)       AS d12,
           count(*) FILTER (WHERE in_1 AND NOT in_3)       AS d13,
           count(*) FILTER (WHERE in_2 AND NOT in_3)       AS d23
    FROM cut
    GROUP BY bar_date
    ORDER BY bar_date
"""

_PAIR_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("suppress_all", "apply_all"),
    ("suppress_all", "apply_unless_refuted"),
    ("suppress_all", "apply_only_corroborated"),
    ("apply_all", "apply_unless_refuted"),
    ("apply_all", "apply_only_corroborated"),
    ("apply_unless_refuted", "apply_only_corroborated"),
)


def build_arms(checks: list[EventCheck]) -> dict[str, list[SplitEvent]]:
    """The three applying arms' event sets, from the predecessor's checks.

    ⚠ ``refuted`` is ``split_error is not None and not agrees`` — an event the
    reference CANNOT speak to is not refuted, it is unadjudicable, and the two
    arms below differ precisely in what they do with that population.
    """
    corroborated = [c.event for c in checks if c.agrees]
    not_refuted = [c.event for c in checks if c.split_error is None or c.agrees]
    return {
        "apply_all": [c.event for c in checks],
        "apply_unless_refuted": not_refuted,
        "apply_only_corroborated": corroborated,
    }


def measure_pairs(
    conn: psycopg.Connection[tuple[Any, ...]],
    names: dict[int, int],
    arms: dict[str, list[SplitEvent]],
) -> list[PairFormation]:
    """One panel pass; every arm scored, every pair compared."""
    params: dict[str, Any] = {
        "series_ids": list(names),
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "version": RULE_SET_VERSION,
        "skip": SKIP_BARS,
        "lookback": LOOKBACK_BARS,
        "eligibility": ELIGIBILITY_BARS,
        "floor": MIN_CLOSE,
        "decile": DECILE,
        "min_cross_section": MIN_CROSS_SECTION,
    }
    conn.execute("CREATE TEMP TABLE cp_name (series_id bigint PRIMARY KEY, name_key bigint) ON COMMIT DROP")
    conn.execute(
        "CREATE TEMP TABLE cp_scale ("
        " arm text, series_id bigint, valid_from date, valid_to date, scale numeric) ON COMMIT DROP"
    )
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO cp_name (series_id, name_key) VALUES (%s, %s)", list(names.items()))
        for arm, events in arms.items():
            rows = [(arm, *segment) for segment in back_adjust_scale(events)]
            if rows:
                cur.executemany(
                    "INSERT INTO cp_scale (arm, series_id, valid_from, valid_to, scale) VALUES (%s, %s, %s, %s, %s)",
                    rows,
                )
    conn.execute("CREATE INDEX ON cp_scale (arm, series_id, valid_from, valid_to)")
    conn.execute(_CALENDAR_SQL, params)
    conn.execute(_PANEL_SQL, params)
    out: list[PairFormation] = []
    for row in conn.execute(_OVERLAP_SQL, params).fetchall():
        counts = [int(value) for value in row[3:]]
        out.append(
            PairFormation(
                bar_date=row[0],
                cross_section=int(row[1]),
                decile_size=int(row[2]),
                displaced=dict(zip(_PAIR_COLUMNS, counts, strict=True)),
            )
        )
    return out


def containment_reachability(
    conn: psycopg.Connection[tuple[Any, ...]],
    admitted: dict[int, str],
    events: list[SplitEvent],
) -> dict[str, int]:
    """Can the eToro corpus's level-break containment be reused on THIS corpus?

    ⚠ This arm exists because ``strategy-catalogue-and-backtest-validity.md`` §4
    rule 10 states the containment corpus-wide — *"``price_series_break``
    segments (402 rows) are ``not_evaluable``, never spanned"* — and an
    uncorrected split IS a level break, so a reader could reasonably conclude
    the damage is already contained and the correction policy is cosmetic.

    Two facts say otherwise and both are checkable rather than argued:

    1. ``sql/246:103`` keys ``price_series_break`` on
       ``instrument_id REFERENCES instruments``. The research corpus is keyed on
       ``series_id`` *because* part of it has no ``instruments`` row at all — the
       count below is what that costs.
    2. The research rule set DOES write transition verdicts
       (``research_transition_quarantine``, ``sql/251:111``) and T3 is exactly
       "this level break is not a return" — but the only readers anywhere are the
       ingest writer and the census view, so nothing in the backtest read path
       consumes them. ``backtest_run.py:4170-4175`` names its three reads and
       that table is not among them.

    ⚠ Two limits on what these counts prove, both load-bearing:

    * ``instrument_id IS NULL`` is a MISSING STORED LINK, not proof that no
      matching instrument exists. It is sufficient for the claim being made
      (``price_series_break`` cannot key the row TODAY) and not for a stronger
      one.
    * Clause 2 is established by GREP, not by this query — ``rg
      research_transition_quarantine app sql`` returns the ingest writer and the
      ``research_quarantine_census`` view and nothing else. The counts below say
      how much the unread verdict would have had to say, not that it is unread.

    Coverage is returned alongside so the denominators are this run's own: a
    verdict row only exists inside an evaluated range, so "T3 fired N times"
    means nothing without the evaluated population beside it.
    """
    ids = sorted(admitted)
    coverage_row = conn.execute(
        "SELECT count(*), coalesce(sum(transitions_evaluated), 0)"
        " FROM research_price_quarantine_coverage"
        " WHERE series_id = ANY(%s) AND rule_set_version = %s",
        (ids, RULE_SET_VERSION),
    ).fetchone()
    covered = 0 if coverage_row is None else int(coverage_row[0])
    transitions_evaluated = 0 if coverage_row is None else int(coverage_row[1])
    linked_row = conn.execute(
        "SELECT count(*) FROM research_price_series WHERE series_id = ANY(%s) AND instrument_id IS NOT NULL",
        (ids,),
    ).fetchone()
    linked = 0 if linked_row is None else int(linked_row[0])
    t3_keys = {
        (int(series_id), bar_date)
        for series_id, bar_date in conn.execute(
            "SELECT series_id, bar_date FROM research_transition_quarantine"
            " WHERE series_id = ANY(%s) AND rule_set_version = %s AND 'T3' = ANY(rules)",
            (ids, RULE_SET_VERSION),
        ).fetchall()
    }
    # ⚠ T3 is keyed on the LATER bar (sql/251:113), and a split stamp sits on the
    # bar that first prints the post-split level — the same bar. So the join is
    # equality, not an offset.
    on_event = sum(1 for e in events if (e.series_id, e.bar_date) in t3_keys)
    # ⚠⚠ T3's predicate is on the OBSERVED ratio — ``max(close/prev, prev/close)
    # >= magnitude_threshold`` (``price_quarantine.py:478-495``) — NOT on the
    # stamped factor. So this is the LARGE-STAMPED-FACTOR subset and is only a
    # proxy for what T3 can see: a real move can push a small-factor event over
    # the trigger, and a stamped factor whose price step is absent stays under
    # it. The run demonstrates the first direction (T3 fires on a handful of
    # events outside this subset), which is why it is not called "reachable".
    large_factor = [e for e in events if max(e.factor, 1 / e.factor) >= _T3_TRIGGER_MAGNITUDE]
    return {
        "admitted_series": len(ids),
        "covered_series": covered,
        "transitions_evaluated": transitions_evaluated,
        "instrument_linked": linked,
        "t3_transitions": len(t3_keys),
        "t3_on_stamped_event": on_event,
        "stamped_events": len(events),
        "large_factor_events": len(large_factor),
        "t3_on_large_factor_event": sum(1 for e in large_factor if (e.series_id, e.bar_date) in t3_keys),
    }


def quarantine_cost(
    conn: psycopg.Connection[tuple[Any, ...]],
    checks: list[EventCheck],
    admitted: dict[int, str],
) -> dict[str, int]:
    """What the ``quarantine`` candidate DELETES, and whether the deletion is biased.

    ⚠ The bias question is the whole point. Quarantining a series because a
    second processing disputes one of its stamps can only fire where a second
    processing EXISTS — and the reference vendor serves the surviving half of
    this corpus (``research-price-corpus.md``: 0/382 on the delisted cohort). A
    rule that can only reject names it can see re-introduces exactly the
    selection this corpus exists to remove.
    """
    refuted_series = sorted({c.event.series_id for c in checks if c.split_error is not None and not c.agrees})
    stamped_series = sorted({c.event.series_id for c in checks})
    delisted = {
        int(row[0])
        for row in conn.execute(
            "SELECT series_id FROM research_price_series WHERE series_id = ANY(%s) AND delisting_source IS NOT NULL",
            (sorted(admitted),),
        ).fetchall()
    }
    checkable_series = sorted({c.event.series_id for c in checks if c.split_error is not None})
    unadjudicable_series = sorted({c.event.series_id for c in checks if c.split_error is None})
    return {
        "admitted_series": len(admitted),
        "admitted_delisted": len(delisted),
        "stamped_series": len(stamped_series),
        "refuted_series": len(refuted_series),
        "refuted_series_delisted": sum(1 for s in refuted_series if s in delisted),
        "checkable_series": len(checkable_series),
        "checkable_series_delisted": sum(1 for s in checkable_series if s in delisted),
        "unadjudicable_series": len(unadjudicable_series),
        "unadjudicable_series_delisted": sum(1 for s in unadjudicable_series if s in delisted),
    }


def harm_direction(checks: list[EventCheck]) -> dict[str, int]:
    """Which errors can put a name INTO the top decile, and how many are there?

    ⚠⚠ This is the asymmetry the pairwise displacement table cannot show. s2
    selects the TOP decile only, and the back-adjustment divides the bars BEFORE
    an event by the scale. So for a scoring window that spans the event:

    * applying a factor > 1 that is not really there divides ``c_back`` and
      INFLATES ``c_skip / c_back`` — manufactured positive momentum, and the name
      is selected IN;
    * failing to apply a real factor < 1 (a reverse split) leaves ``c_back`` too
      LOW and inflates the same ratio — also selected in;
    * the other two combinations DEPRESS the score, and a depressed score on a
      long top-decile strategy is a MISS, not a contaminated holding.

    So the two candidate policies are exposed to different populations, and the
    comparison is a count, not an argument:

    * ``apply_all`` is exposed to **refuted forward** stamps (factor > 1);
    * ``suppress_all`` is exposed to **real reverse** stamps (factor < 1).

    ⚠ This is NOT the *"the error has a sign"* mechanism story withdrawn on
    ``b65abd9c`` — that claim was withdrawn precisely because reverse splits move
    the error the other way. The partition below is what survives: no global
    sign, one direction per class, counted rather than reasoned.

    ⚠ "Exposed" is an upper bound on contamination, never a count of it. A
    spurious factor only reaches a score if a formation's lookback window spans
    the event date, and the magnitude decides whether the inflated score actually
    reaches the decile. Neither is established here.
    """
    refuted = [c for c in checks if c.split_error is not None and not c.agrees]
    # The scoring window can only reach an event if the event lies inside it:
    # LOOKBACK_BARS of trading days before the last formation, through the last
    # formation itself. Bounded in CALENDAR days at 7/5 the bar count — a loose
    # bound deliberately, because a tight one would need each series' own
    # calendar and would understate reach on a halted or thinly traded name.
    reach_start = WINDOW_START - timedelta(days=int(LOOKBACK_BARS * 7 / 5) + 14)
    in_reach = [c for c in checks if reach_start <= c.event.bar_date < WINDOW_END]
    refuted_in_reach = [c for c in refuted if reach_start <= c.event.bar_date < WINDOW_END]
    return {
        "checked": len(checks),
        "forward": sum(1 for c in checks if c.event.factor > 1),
        "reverse": sum(1 for c in checks if c.event.factor < 1),
        "refuted": len(refuted),
        "refuted_forward": sum(1 for c in refuted if c.event.factor > 1),
        "refuted_reverse": sum(1 for c in refuted if c.event.factor < 1),
        "in_reach": len(in_reach),
        "in_reach_reverse": sum(1 for c in in_reach if c.event.factor < 1),
        "refuted_in_reach": len(refuted_in_reach),
        "refuted_in_reach_forward": sum(1 for c in refuted_in_reach if c.event.factor > 1),
    }


def _report_harm(harm: dict[str, int], *, stream: TextIO) -> None:
    print("\nArm 5 — direction of harm: which errors can be SELECTED IN", file=stream)
    print(
        f"  checked events {harm['checked']:,}   forward (factor > 1) {harm['forward']:,}"
        f"   reverse (factor < 1) {harm['reverse']:,}",
        file=stream,
    )
    print(
        f"  inside the scoring reach of the window  {harm['in_reach']:,}"
        f"  ({_pct(harm['in_reach'], harm['checked']):.2f}%)",
        file=stream,
    )
    print(f"\n  {'policy':<16} {'exposed population':<38} {'events':>8} {'in reach':>9}", file=stream)
    print(
        f"  {'apply_all':<16} {'refuted FORWARD stamps applied':<38}"
        f" {harm['refuted_forward']:>8,} {harm['refuted_in_reach_forward']:>9,}",
        file=stream,
    )
    print(
        f"  {'suppress_all':<16} {'real REVERSE splits left uncorrected':<38}"
        f" {harm['reverse']:>8,} {harm['in_reach_reverse']:>9,}",
        file=stream,
    )
    print(
        "  ⚠ Upper bounds on exposure, not counts of contamination: a spurious factor only\n"
        "    reaches a score if a formation's lookback spans it, and only enters the decile if\n"
        "    the inflated score is large enough. Neither is established here.",
        file=stream,
    )


def _pct(numerator: int, denominator: int) -> float:
    return 0.0 if denominator <= 0 else 100.0 * numerator / denominator


def _report_containment(reach: dict[str, int], *, stream: TextIO) -> None:
    print("\nArm 0 — is the level-break containment §4 rule 10 cites reachable here?", file=stream)
    admitted = reach["admitted_series"]
    unlinked = admitted - reach["instrument_linked"]
    print(f"  admitted series                    {admitted:,}", file=stream)
    print(
        f"  ... with an instruments row        {reach['instrument_linked']:,}"
        f"  ({_pct(reach['instrument_linked'], admitted):.2f}%)",
        file=stream,
    )
    print(
        f"  ... with NONE                      {unlinked:,}  ({_pct(unlinked, admitted):.2f}%)"
        "  <- price_series_break cannot key these at all (sql/246:103)",
        file=stream,
    )
    print(
        f"\n  series with a coverage row           {reach['covered_series']:,}"
        f"   transitions evaluated {reach['transitions_evaluated']:,}",
        file=stream,
    )
    print(
        f"  T3 transitions stored, this universe {reach['t3_transitions']:,}"
        "   (research_transition_quarantine; no backtest reader)",
        file=stream,
    )
    print(
        f"  ... landing ON a stamped split bar   {reach['t3_on_stamped_event']:,}"
        f"  ({_pct(reach['t3_on_stamped_event'], reach['stamped_events']):.2f}% of stamped events)",
        file=stream,
    )
    print(
        f"  stamped events of large FACTOR       {reach['large_factor_events']:,}"
        f"  (|factor| >= {_T3_TRIGGER_MAGNITUDE}; a proxy — T3 tests the OBSERVED ratio)",
        file=stream,
    )
    print(
        f"  ... of which T3 fired on             {reach['t3_on_large_factor_event']:,}"
        f"  ({_pct(reach['t3_on_large_factor_event'], reach['large_factor_events']):.2f}%)",
        file=stream,
    )
    outside = reach["t3_on_stamped_event"] - reach["t3_on_large_factor_event"]
    print(
        f"  ... and T3 fired on                  {outside:,} stamped events OUTSIDE that subset,"
        "\n      which is why the subset is a proxy and not a reachability bound",
        file=stream,
    )


def _report_arms(checks: list[EventCheck], arms: dict[str, list[SplitEvent]], *, stream: TextIO) -> None:
    total = len(checks)
    refuted = sum(1 for c in checks if c.split_error is not None and not c.agrees)
    unadjudicable = sum(1 for c in checks if c.split_error is None)
    print("\nArm 1 — what each policy applies", file=stream)
    print(f"  checked events            {total:,}", file=stream)
    print(f"  refuted by the reference  {refuted:,}  ({_pct(refuted, total):.2f}%)", file=stream)
    print(f"  unadjudicable (no pair)   {unadjudicable:,}  ({_pct(unadjudicable, total):.2f}%)", file=stream)
    # ⚠ Not every refutation is about the stamp. Where the REFERENCE's own close
    # carries the raw step (CIVB 1996-05-09 — both vendors print 81 -> 20.25 on a
    # 4:1), the disagreement is the reference's basis, not the factor. Any policy
    # that suppresses on refutation suppresses these too, and they are the subset
    # most likely to be CORRECT.
    reference_fault = sum(1 for c in checks if c.split_error is not None and not c.agrees and c.reference_is_unadjusted)
    print(
        f"  ... where the REFERENCE is the unadjusted one  {reference_fault:,}"
        f"  ({_pct(reference_fault, refuted):.2f}% of refuted)",
        file=stream,
    )
    print(f"\n  {'arm':<26} {'events applied':>15} {'share':>8}", file=stream)
    print(f"  {'suppress_all':<26} {0:>15,} {0.0:>7.2f}%", file=stream)
    for arm in _ARMS[1:]:
        applied = len(arms[arm])
        print(f"  {arm:<26} {applied:>15,} {_pct(applied, total):>7.2f}%", file=stream)


def _report_pairs(formations: list[PairFormation], *, stream: TextIO) -> dict[tuple[str, str], float]:
    print("\nArm 2 — decile displacement between policies", file=stream)
    print(f"  window     {WINDOW_START} .. {WINDOW_END} (exclusive)", file=stream)
    print(f"  quarantine {RULE_SET_VERSION}", file=stream)
    print(f"  formations {len(formations):,}", file=stream)
    pooled: dict[tuple[str, str], float] = {}
    if not formations:
        return pooled
    decile_total = sum(f.decile_size for f in formations)
    print(f"\n  {'pair':<52} {'displaced':>10} {'pct':>8}", file=stream)
    for pair in _PAIR_COLUMNS:
        moved = sum(f.displaced[pair] for f in formations)
        share = _pct(moved, decile_total)
        pooled[pair] = share
        label = f"{pair[0]} -> {pair[1]}"
        print(f"  {label:<52} {moved:>10,} {share:>7.2f}%", file=stream)
    print(f"\n  decile slots {decile_total:,}", file=stream)
    print("\n  by decade, the two policy contrasts that decide §7 item 1:", file=stream)
    print(f"  {'decade':<8} {'formations':>10} {'apply→unrefuted':>17} {'apply→corrob':>14}", file=stream)
    for decade in sorted({f.bar_date.year // 10 * 10 for f in formations}):
        rows = [f for f in formations if f.bar_date.year // 10 * 10 == decade]
        slots = sum(f.decile_size for f in rows)
        a = _pct(sum(f.displaced[("apply_all", "apply_unless_refuted")] for f in rows), slots)
        b = _pct(sum(f.displaced[("apply_all", "apply_only_corroborated")] for f in rows), slots)
        print(f"  {decade:<8} {len(rows):>10,} {a:>16.2f}% {b:>13.2f}%", file=stream)
    return pooled


def _report_quarantine(cost: dict[str, int], *, stream: TextIO) -> None:
    print("\nArm 3 — what `quarantine` deletes, and whether the deletion is biased", file=stream)
    admitted = cost["admitted_series"]
    print(f"  admitted series                  {admitted:,}", file=stream)
    print(
        f"  ... carrying delisting evidence  {cost['admitted_delisted']:,}"
        f"  ({_pct(cost['admitted_delisted'], admitted):.2f}%)",
        file=stream,
    )
    print(f"  series carrying >=1 stamp        {cost['stamped_series']:,}", file=stream)
    print(
        f"  series quarantine would DROP     {cost['refuted_series']:,}"
        f"  ({_pct(cost['refuted_series'], admitted):.2f}% of admitted)",
        file=stream,
    )
    print(f"\n  {'population':<34} {'series':>8} {'delisted':>10} {'pct':>8}", file=stream)
    for label, key in (
        ("reference-checkable (stamped)", "checkable_series"),
        ("unadjudicable (stamped)", "unadjudicable_series"),
        ("refuted -> dropped", "refuted_series"),
    ):
        n = cost[key]
        d = cost[f"{key}_delisted"]
        print(f"  {label:<34} {n:>8,} {d:>10,} {_pct(d, n):>7.2f}%", file=stream)


def _report_redate(checks: list[EventCheck], offsets: dict[tuple[int, date], Decimal], *, stream: TextIO) -> int:
    """``re_date``'s reachable population — a BOUND, not an arm."""
    refuted = [c for c in checks if c.split_error is not None and not c.agrees]
    reachable = sum(
        1
        for c in refuted
        if offsets.get((c.event.series_id, c.event.bar_date)) is not None
        and offsets[(c.event.series_id, c.event.bar_date)] <= Decimal("0.01")
    )
    print("\nArm 4 — `re_date` reachability (a bound, not an arm)", file=stream)
    print(f"  refuted events                {len(refuted):,}", file=stream)
    print(
        f"  ... step found within +/-3 bars {reachable:,}  ({_pct(reachable, len(refuted)):.2f}% of refuted,"
        f" {_pct(reachable, len(checks)):.2f}% of checked)",
        file=stream,
    )
    print(
        "  ⚠ An upper bound on BOTH sides: a real move of the same size registers as a hit,\n"
        "    and the probe only reaches events the reference can adjudicate at all.",
        file=stream,
    )
    return reachable


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        # ⚠ REPEATABLE READ so every arm reads ONE snapshot. "One transaction" is
        # not the same guarantee: under READ COMMITTED each statement takes a
        # fresh snapshot, so a concurrent ingest could move the panel between the
        # arms' scale build and the overlap query, and the displacement between
        # two arms would silently include it.
        # ⚠ NOT `read_only` — a PostgreSQL read-only transaction forbids CREATE,
        # which includes the TEMP tables this measurement builds.
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SET statement_timeout = 1800000")
        selection = _selection(conn)
        ids = [row.series_id for row in selection.admitted]
        names = {row.series_id: row.name_key for row in selection.admitted}
        symbols = {
            int(series_id): str(symbol)
            for series_id, symbol in conn.execute(
                "SELECT series_id, vendor_symbol FROM research_price_series WHERE series_id = ANY(%s)",
                (ids,),
            ).fetchall()
        }
        events, missing, _dividend_bars = read_split_events(symbols)
        print(f"Admitted series {len(symbols):,}  (mirror CSV missing on {missing:,})", file=sys.stdout)
        print(f"Stamped events  {len(events):,}  on {len({e.series_id for e in events}):,} series", file=sys.stdout)
        if not events:
            print("\nREFUSED: no split evidence found — there is no policy to choose.", file=sys.stdout)
            return 2

        _report_containment(containment_reachability(conn, symbols, events), stream=sys.stdout)

        checks, unusable = check_events(conn, events)
        print(f"\nUnusable events {unusable:,}  (own bar pair missing or non-positive)", file=sys.stdout)
        arms = build_arms(checks)
        _report_arms(checks, arms, stream=sys.stdout)

        formations = measure_pairs(conn, names, arms)
        pooled = _report_pairs(formations, stream=sys.stdout)

        _report_quarantine(quarantine_cost(conn, checks, symbols), stream=sys.stdout)

        refuted_checks = [c for c in checks if c.split_error is not None and not c.agrees]
        _report_redate(checks, find_offset_steps(conn, refuted_checks), stream=sys.stdout)

        _report_harm(harm_direction(checks), stream=sys.stdout)

    if not formations:
        print("\nREFUSED: no formation cleared the cross-section floor; nothing was compared.", file=sys.stdout)
        return 2
    control = pooled[("suppress_all", "apply_all")]
    refine = pooled[("apply_all", "apply_unless_refuted")]
    gate = pooled[("apply_all", "apply_only_corroborated")]
    print(
        f"\nMEASURED. Correcting at all moves {control:.2f}% of the top decile against the status quo.\n"
        f"Adjudicating the disputes on top of that moves {refine:.2f}% (apply -> apply_unless_refuted)\n"
        f"and gating on corroboration moves {gate:.2f}% (apply -> apply_only_corroborated).\n"
        "⚠ These are SET DISTANCES, not benefits: a displacement percentage says how far two "
        "deciles differ, never which is right, and they are not additive. Arm 5 is the only "
        "thing here that speaks to the DIRECTION of harm, and it bounds exposure rather than "
        "counting contamination. No adjudicator has cleared a control arm (#3280 measured one "
        "candidate and it did not), so nothing in this output establishes correctness.",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
