"""#3046 residual 5 — the five arms that decide the verdict-aware read contract.

Spec: ``docs/proposals/ta/2026-09-15-3046-transition-verdict-contract.md``.
Read-only. Writes nothing. One ``REPEATABLE READ READ ONLY`` transaction.

WHAT THIS DECIDES. Residual 4 measured the exposure (21,872 instrument/metric pairs
on the ranked population) and struck the masked loader off as a carrier for
transitions. This script decides the CONTRACT: which of the four damage kinds each
existing carrier already covers, and what is genuinely missing.

    arm A   is T1 fully visible in price_bar_quarantine?      -> clause 1 covers T1
    arm B   is T3 fully carried by price_series_break?        -> clause 2 covers T3
    arm C   how big is the W1 population, and how much of it
            sits outside current coverage?                    -> clause 3's fail-closed half
    arm D   does rule_w2 fire on windows with ZERO
            quarantined transitions?                          -> W2 is a 4th damage kind
    arm E   how many breaks are RESOLVED (adjusted)?          -> what a naive W1 over-refuses

Each arm carries its acceptance condition in its own docstring, declared before the
run rather than read off the output.

⚠⚠ RECONCILIATION IS KEY-LEVEL, NEVER COUNTS. Arms A and B compare SETS of
``(instrument_id, date)`` in both directions. Two counts can agree while a missing
row and an unrelated row cancel — the prevention-log rule on key reconciliation.

⚠⚠ MAGNITUDE IS NOT A DAMAGE TEST AND IS NOT USED. ``price_quarantine``'s own header
says *"Magnitude is a trigger, not a verdict"*, and the class threshold is calibrated
on ADJACENT bars, so applying it to a T2 pair spanning a hole compares a daily
constant against a multi-month return. Codex killed that discriminator twice on this
ticket. Nothing below classifies a transition by its ratio.

⚠ WINDOW MACHINERY IS IMPORTED, NOT REBUILT. ``SPECS``, ``SLICES``, ``Anchors`` and
``load_anchors`` come from ``scripts.verify_3046_consumer_exposure``, which is where
the operand-span-vs-computability-floor corrections live. Re-deriving them here would
fork the definitions that Codex checkpoint 2 fixed four times on that script.
⚠ Its ``window_start`` is deliberately NOT used: it returns a spec's first operand
DATE, and ``rule_w2`` also needs the window's BAR COUNT, which only the rank anchors
supply exactly (see ``_w2_windows``).

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_contract_decision
"""

from __future__ import annotations

import argparse
import subprocess
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.price_quarantine import (
    RULE_SET_VERSION,
    ClassParams,
    params_for,
    rule_w1,
    rule_w2,
)
from scripts.verify_3046_consumer_exposure import (
    SLICES,
    SPECS,
    Anchors,
    load_anchors,
)

#: Bumped whenever an arm's definition changes, so two runs of this script are
#: comparable only when it matches. Not a rule constant — the rules all live in
#: ``price_quarantine`` and this script only calls them.
CONTRACT_ARM_VERSION = "contract-decision-v2"  # v2: arm D weekend test moved to trading-day units


def _git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    # Narrowed rather than bare: `git` missing is OSError, a non-zero exit is
    # CalledProcessError, and there is no third way this can fail. Run identity is
    # best-effort — the measurement is not — so it degrades rather than aborting.
    except OSError, subprocess.SubprocessError:
        return "unknown"


# ---------------------------------------------------------------------------
# Arm A — is T1 a restatement of a bar verdict?
# ---------------------------------------------------------------------------


def arm_a(conn: psycopg.Connection[Any]) -> list[str]:
    """T1 <-> ``price_bar_quarantine``, both directions, at key level.

    ACCEPT clause 1 as covering T1 only if BOTH directions are exact:

    - forward: every stored T1 transition has at least one endpoint carrying a
      ``return_usable = false`` bar row at the current rule-set version;
    - reverse: every adjacent stored bar pair whose later or earlier bar is
      ``return_usable = false`` carries a T1 transition row.

    The reverse direction is the one that matters and the one draft 1 omitted. A
    forward-only check passes vacuously when T1 rows are MISSING beside unusable
    bars, which is precisely the failure that would make clause 1 unsafe.

    ⚠ The reverse arm is bounded to adjacent pairs INSIDE coverage. A bar pair the
    quarantine never evaluated has no verdict owed, and counting it as a shortfall
    would report the ordinary ingest-vs-evaluation lag as a defect.
    """
    out: list[str] = []
    forward = conn.execute(
        """
        SELECT count(*) AS t1_rows,
               count(*) FILTER (WHERE a.iid IS NOT NULL OR b.iid IS NOT NULL) AS with_endpoint
          FROM price_transition_quarantine t
          LEFT JOIN (SELECT instrument_id AS iid, price_date AS d
                       FROM price_bar_quarantine
                      WHERE rule_set_version = %(ver)s AND NOT return_usable) a
                 ON a.iid = t.instrument_id AND a.d = t.prior_date
          LEFT JOIN (SELECT instrument_id AS iid, price_date AS d
                       FROM price_bar_quarantine
                      WHERE rule_set_version = %(ver)s AND NOT return_usable) b
                 ON b.iid = t.instrument_id AND b.d = t.price_date
         WHERE t.rule_set_version = %(ver)s AND 'T1' = ANY(t.rules)
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert forward is not None
    t1_rows, with_endpoint = int(forward[0]), int(forward[1])
    out.append(
        f"forward  {with_endpoint}/{t1_rows} T1 transitions have a return_usable=false endpoint"
        + ("   ✅ exact" if with_endpoint == t1_rows else "   ⛔ SHORTFALL")
    )

    # Reverse: adjacent stored pairs (via lag) inside coverage, either endpoint
    # return-unusable, with no T1 transition row.
    reverse = conn.execute(
        """
        WITH pairs AS (
            SELECT p.instrument_id,
                   p.price_date,
                   lag(p.price_date) OVER (PARTITION BY p.instrument_id ORDER BY p.price_date)
                       AS prior_date
              FROM price_daily p
              JOIN price_quarantine_coverage c
                ON c.instrument_id = p.instrument_id
               AND c.rule_set_version = %(ver)s
               AND p.price_date BETWEEN c.first_bar AND c.last_bar
        ),
        bad AS (
            SELECT instrument_id AS iid, price_date AS d
              FROM price_bar_quarantine
             WHERE rule_set_version = %(ver)s AND NOT return_usable
        ),
        owed AS (
            SELECT pr.instrument_id, pr.price_date
              FROM pairs pr
             WHERE pr.prior_date IS NOT NULL
               AND (EXISTS (SELECT 1 FROM bad WHERE bad.iid = pr.instrument_id AND bad.d = pr.price_date)
                 OR EXISTS (SELECT 1 FROM bad WHERE bad.iid = pr.instrument_id AND bad.d = pr.prior_date))
        )
        SELECT count(*) AS owed,
               count(*) FILTER (
                   WHERE EXISTS (
                       SELECT 1 FROM price_transition_quarantine t
                        WHERE t.rule_set_version = %(ver)s
                          AND t.instrument_id = owed.instrument_id
                          AND t.price_date = owed.price_date
                          AND 'T1' = ANY(t.rules))) AS present
          FROM owed
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert reverse is not None
    owed, present = int(reverse[0]), int(reverse[1])
    out.append(
        f"reverse  {present}/{owed} adjacent in-coverage pairs beside a return-unusable bar carry a T1 row"
        + ("   ✅ exact" if present == owed else "   ⛔ SHORTFALL")
    )
    out.append(
        "VERDICT  clause 1 covers T1"
        if (with_endpoint == t1_rows and present == owed and t1_rows > 0)
        else "VERDICT  clause 1 does NOT cover T1 — a transition read is still owed"
    )
    return out


# ---------------------------------------------------------------------------
# Arm B — is T3 fully carried by the operand price_segments loads?
# ---------------------------------------------------------------------------


def arm_b(conn: psycopg.Connection[Any]) -> list[str]:
    """T3 <-> ``price_series_break``, as SET differences on ``(instrument_id, date)``.

    ACCEPT clause 2 as covering T3 only if ``T3 \\ breaks`` is EMPTY. The opposite
    difference is reported and is not a failure: ``sql/246`` keeps a resolved break
    after its transition has been reclassified, and a break can outlive a re-run of
    the rules at a new version.

    ⚠ Three counts cannot establish this. A missing T3 row and an unrelated break
    row cancel exactly in a count comparison and the arm reports success.
    """
    out: list[str] = []
    t3 = {
        (int(i), d)
        for i, d in conn.execute(
            """
            SELECT instrument_id, price_date FROM price_transition_quarantine
             WHERE rule_set_version = %(ver)s AND 'T3' = ANY(rules)
            """,
            {"ver": RULE_SET_VERSION},
        ).fetchall()
    }
    breaks = {
        (int(i), d): resolved
        for i, d, resolved in conn.execute(
            "SELECT instrument_id, break_date, resolved_by IS NOT NULL FROM price_series_break"
        ).fetchall()
    }
    unresolved = {k for k, resolved in breaks.items() if not resolved}

    missing = t3 - set(breaks)
    extra = set(breaks) - t3
    not_loaded = t3 - unresolved  # what price_segments would NOT hand a consumer

    out.append(f"T3 transitions                       {len(t3)}")
    out.append(f"price_series_break rows              {len(breaks)}  (unresolved {len(unresolved)})")
    out.append(
        f"T3 \\ breaks (uncarried)               {len(missing)}"
        + ("   ✅ empty" if not missing else "   ⛔ T3 verdicts with no break row")
    )
    out.append(f"breaks \\ T3 (expected, see sql/246)   {len(extra)}")
    out.append(
        f"T3 not in the UNRESOLVED set          {len(not_loaded)}"
        "   <- price_segments filters resolved_by IS NULL, so these reach no consumer"
    )
    out.append("VERDICT  clause 2 covers T3" if not missing and t3 else "VERDICT  clause 2 does NOT cover T3")
    return out


# ---------------------------------------------------------------------------
# Arm C — the W1 population and its fail-closed half
# ---------------------------------------------------------------------------


def arm_c(conn: psycopg.Connection[Any]) -> list[str]:
    """Size clause 3, and establish whether its coverage contract is load-bearing.

    ACCEPT the fail-closed clause if the "no current coverage row" population is
    non-zero — a loader returning an empty tuple for those instruments would report
    "no transition crosses your window" for instruments nobody has checked, which is
    the exact ``sql/247:23-33`` failure the masked loader already refuses.

    The quarantine predicate is ``cardinality(rules) > 0``. Rows with empty ``rules``
    are ADMITTED or DEFERRED census evidence, not quarantined transitions, and
    counting them would inflate clause 3's population with rows the rule set has
    declined to condemn.
    """
    out: list[str] = []
    by_rule: Counter[str] = Counter()
    for rule, n in conn.execute(
        """
        SELECT unnest(rules) AS rule, count(*) FROM price_transition_quarantine
         WHERE rule_set_version = %(ver)s AND cardinality(rules) > 0
         GROUP BY 1
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchall():
        by_rule[str(rule)] = int(n)

    totals = conn.execute(
        """
        SELECT count(*) FILTER (WHERE cardinality(rules) > 0),
               count(DISTINCT instrument_id) FILTER (WHERE cardinality(rules) > 0),
               count(*) FILTER (WHERE cardinality(rules) = 0)
          FROM price_transition_quarantine WHERE rule_set_version = %(ver)s
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert totals is not None
    out.append(f"quarantined transitions              {totals[0]}  across {totals[1]} instruments")
    out.append(f"  by rule                            {dict(sorted(by_rule.items()))}")
    out.append(f"admitted/deferred rows (rules = [])  {totals[2]}   <- NOT clause 3's population")

    cov = conn.execute(
        """
        SELECT count(*) FILTER (WHERE c.instrument_id IS NULL)                     AS no_row,
               count(*) FILTER (WHERE c.rule_set_version IS DISTINCT FROM %(ver)s
                                  AND c.instrument_id IS NOT NULL)                 AS stale,
               count(*) FILTER (WHERE c.rule_set_version = %(ver)s)                AS current
          FROM (SELECT DISTINCT instrument_id FROM price_daily) p
          LEFT JOIN price_quarantine_coverage c USING (instrument_id)
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert cov is not None
    no_row, stale, current = int(cov[0]), int(cov[1]), int(cov[2])
    out.append(
        f"instruments with bars                {no_row + stale + current}"
        f"   (current coverage {current}, stale {stale}, none {no_row})"
    )
    out.append(
        "VERDICT  clause 3's loader MUST be fail-closed — "
        f"{no_row + stale} instruments would otherwise read as clean because they are unchecked"
        if (no_row + stale) > 0
        else "VERDICT  every instrument with bars has current coverage TODAY; the fail-closed "
        "clause is still required, because that is a fact about this instant"
    )
    return out


# ---------------------------------------------------------------------------
# Arm D — is W2 a separate damage kind on this corpus?
# ---------------------------------------------------------------------------


def _w2_windows(a: Anchors) -> list[tuple[str, date, date, int]]:
    """Every window whose BAR COUNT this corpus supplies exactly, per instrument.

    ⚠⚠ Rank windows only, and the rank IS the bar count. ``_SLICE_FILTER`` ranks with
    ``row_number() OVER (... ORDER BY price_date DESC)``, so ``rn = 1`` is the MOST
    RECENT bar and ``win_end = by_rank[1]``. The window ``[by_rank[r], win_end]``
    therefore holds exactly ``r`` stored bars BY CONSTRUCTION — no counting query and
    no estimate. ⚠ Draft 1 used ``r + 1``, which inflates ``rule_w2``'s ``nominal``
    by one bar's worth of calendar and makes W2 fire LESS; the first run under-counted
    because of it. The whole-slice window is exact for the same reason: ``depth`` is
    its bar count.

    Calendar-anchored windows are deliberately EXCLUDED. Their bar count is not
    implied by the anchor and estimating it would feed ``rule_w2`` a number this
    script invented, which is the failure mode the rule exists to detect.
    """
    windows: list[tuple[str, date, date, int]] = []
    for rank, start in a.by_rank.items():
        if start < a.win_end and rank >= 2:
            windows.append((f"rank_{rank}", start, a.win_end, rank))
    if a.oldest < a.win_end and a.depth >= 2:
        windows.append(("whole_slice", a.oldest, a.win_end, a.depth))
    return windows


def _weekend_explains(start: date, end: date, bar_count: int, days_per_bar: Decimal) -> bool:
    """Would W2 stop firing if the weekend days in the span were not counted?

    ⚠ The rule is CALLED, not mirrored: the span is shortened by the number of
    Saturdays and Sundays it contains and ``rule_w2`` is re-asked. Re-deriving its
    arithmetic here is the mistake ``verify_3046_consumer_exposure`` already made once
    with ``rule_w1`` and had deleted.

    ⚠ Only for classes the rule set itself declares 5-day (``calendar_days_per_bar``
    is not 1). On a 7-day class a Saturday IS a session, so removing it would invent
    a holiday the venue does not take. The 5-day/7-day fact is read from
    ``ClassParams``, never assumed from the symbol.

    ⚠⚠ BOTH SIDES MOVE TO TRADING-DAY UNITS, OR THE WEEKEND IS SUBTRACTED TWICE.
    ``calendar_days_per_bar = 1.4`` IS ``7/5`` — it already carries the weekend
    allowance. Stripping the weekend from the observed span while keeping 1.4 as the
    nominal deducts it on both sides and excuses genuinely stretched windows. Codex
    checkpoint 2's counterexample: 20 bars over 2026-01-05..2026-03-06 is 60 calendar
    days and **44 weekdays** for 19 intervals — stretched by any reading — yet the
    first version returned True. So the re-ask uses ``calendar_days_per_bar = 1``,
    which is the rule set's own ``_SEVEN_DAY`` value and is the correct nominal for a
    span that no longer contains weekends. Not a new constant; the same rule read in
    the units the shortened span is now expressed in.

    The weekend count spans ``(start, end]`` to match ``(end - start).days``, which
    counts the days strictly after ``start`` through ``end``.
    """
    if days_per_bar == 1:
        return False
    weekend = sum(
        1 for offset in range(1, (end - start).days + 1) if (start + timedelta(days=offset)).isoweekday() >= 6
    )
    # ⚠ `magnitude_threshold` and `hole_days` are UNREAD by `rule_w2` — it takes
    # `calendar_days_per_bar` alone — so the zeros are unused fields, not tuned values.
    # They are deliberately IMPOSSIBLE rather than plausible: if `rule_w2` ever grows a
    # dependency on either, a 0x magnitude gate and a 0-day hole tolerance fail loudly
    # instead of quietly re-classifying every window this arm counts.
    trading_day_params = ClassParams(magnitude_threshold=Decimal(0), hole_days=0, calendar_days_per_bar=Decimal(1))
    return not rule_w2(start, end - timedelta(days=weekend), bar_count, trading_day_params)


def arm_d(
    anchors: dict[str, dict[int, Anchors]],
    transitions: dict[int, list[date]],
    asset_class: dict[int, str | None],
) -> list[str]:
    """Count windows where ``rule_w2`` fires and ``rule_w1`` does NOT.

    ACCEPT the four-kind decomposition if that count is non-zero at a bar count where
    ``rule_w2``'s own premise holds. If it is zero, W2 adds nothing on this corpus that
    W1 does not already catch, and the contract has three clauses rather than four.

    ⚠⚠ THE ACCEPTANCE CARRIES A BAR-COUNT QUALIFIER, AND THE FIRST RUN IS WHY.
    ``calendar_days_per_bar`` is an AVERAGE (7/5 for exchange sessions). At
    ``bar_count = 2`` the nominal span is 1.4 days and the gate is 2.8 — BELOW an
    ordinary Friday-to-Monday gap of 3. So on a 5-day class every weekend-spanning
    PAIR fires W2 with nothing wrong. ``rule_w2``'s docstring is written around a
    20-bar window, where averaging over many gaps is what the constant is for. This
    arm therefore reports the bar-count distribution and measures the
    weekend-explainable share by construction rather than choosing a floor — a floor
    would be an invented constant, which is what the rule set refuses.

    ⚠ This is a LOWER BOUND on the W2 population in two directions, both disclosed:
    only rank and whole-slice windows are evaluated (see ``_w2_windows``), and only
    instruments the slice's anchor query returned.
    """
    out: list[str] = []
    fired = w2_only = w1_only = both = 0
    evaluated = 0
    w2_only_instruments: set[int] = set()
    per_slice: Counter[str] = Counter()
    by_bar_count: Counter[int] = Counter()
    weekend_explainable = 0

    for slice_, per_instrument in anchors.items():
        for iid, a in per_instrument.items():
            params = params_for(asset_class.get(iid))
            dates = transitions.get(iid, ())
            for label, start, end, bar_count in _w2_windows(a):
                evaluated += 1
                w2 = rule_w2(start, end, bar_count, params)
                w1 = rule_w1(start, end, dates)
                if w2:
                    fired += 1
                if w2 and not w1:
                    w2_only += 1
                    w2_only_instruments.add(iid)
                    per_slice[f"{slice_}/{label}"] += 1
                    by_bar_count[bar_count] += 1
                    if _weekend_explains(start, end, bar_count, params.calendar_days_per_bar):
                        weekend_explainable += 1
                elif w2 and w1:
                    both += 1
                elif w1:
                    w1_only += 1

    residue = w2_only - weekend_explainable
    out.append(f"windows evaluated (rank + whole-slice, exact bar counts)   {evaluated}")
    out.append(f"  W2 fires                                                {fired}")
    out.append(
        f"  W2 fires and W1 does NOT                                {w2_only}"
        f"   on {len(w2_only_instruments)} instruments"
    )
    out.append(f"  W2 and W1 both fire                                     {both}")
    out.append(f"  W1 only  <- clause 3's population, in window terms      {w1_only}")
    out.append("  W2-only by bar count:")
    for bars, n in sorted(by_bar_count.items())[:10]:
        out.append(f"    {bars:>4} bars   {n}")
    out.append(f"  of the {w2_only} W2-only windows, WEEKEND-EXPLAINABLE                {weekend_explainable}")
    out.append(f"  W2-only residue not explained by weekend days  <- the 4th kind  {residue}")
    for key, n in per_slice.most_common(6):
        out.append(f"    {key:<28} {n}")
    out.append(
        "VERDICT  W2 is a separate damage kind — a stretched horizon with no quarantined "
        "transition, not accounted for by weekend days"
        if residue > 0
        else "VERDICT  every W2-only window is explained by weekend days; W2 adds nothing W1 "
        "does not already catch on this corpus, and the contract is 3 clauses"
    )
    return out


# ---------------------------------------------------------------------------
# Arm E — what a naive W1 would over-refuse
# ---------------------------------------------------------------------------


def arm_e(conn: psycopg.Connection[Any]) -> list[str]:
    """Resolved breaks, and the transitions sitting on their dates.

    ``sql/246:4-10``: *"a quarantined transition that turns out to have an active
    adjustment row on its date is RECLASSIFIED from ``quarantined`` to ``adjusted``.
    That is a resolution step."* ``price_transition_quarantine`` carries no such
    column, so a loader reading it raw would refuse windows across transitions the
    repo has already resolved.

    The loader MUST exclude them whatever this count is — the rule is a rule, not a
    materiality judgement. The count sizes the error the exclusion prevents.
    """
    out: list[str] = []
    row = conn.execute(
        """
        SELECT count(*) FILTER (WHERE resolved_by IS NOT NULL),
               count(DISTINCT instrument_id) FILTER (WHERE resolved_by IS NOT NULL),
               count(*)
          FROM price_series_break
        """
    ).fetchone()
    assert row is not None
    out.append(f"price_series_break rows              {row[2]}  (resolved {row[0]} on {row[1]} instruments)")

    overlap = conn.execute(
        """
        SELECT count(*) FROM price_transition_quarantine t
          JOIN price_series_break b
            ON b.instrument_id = t.instrument_id AND b.break_date = t.price_date
         WHERE t.rule_set_version = %(ver)s AND cardinality(t.rules) > 0
           AND b.resolved_by IS NOT NULL
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert overlap is not None
    out.append(f"quarantined transitions on a RESOLVED break date   {overlap[0]}   <- a naive W1 over-refuses these")
    return out


# ---------------------------------------------------------------------------
# Population loads + main
# ---------------------------------------------------------------------------


def load_transitions(conn: psycopg.Connection[Any]) -> dict[int, list[date]]:
    """Quarantined transitions as their LATER dates, resolved breaks EXCLUDED.

    Two rules, neither invented here:

    - ``cardinality(rules) > 0`` — an admitted/deferred row is census evidence, not
      a quarantined transition (``sql/247`` sparse-table invariant).
    - the LATER date only — ``rule_w1``'s own docstring says it takes *"the LATER bar
      of each quarantined transition"*; passing the earlier date inverts its boundary.

    ⚠ This is the shape clause 3's loader would take, MINUS the fail-closed coverage
    half, which arm C measures rather than applies. Applying it here would remove the
    population arm C exists to count.
    """
    out: dict[int, list[date]] = {}
    for iid, price_date in conn.execute(
        """
        SELECT t.instrument_id, t.price_date
          FROM price_transition_quarantine t
          LEFT JOIN price_series_break b
                 ON b.instrument_id = t.instrument_id AND b.break_date = t.price_date
                AND b.resolved_by IS NOT NULL
         WHERE t.rule_set_version = %(ver)s AND cardinality(t.rules) > 0
           AND b.instrument_id IS NULL
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchall():
        out.setdefault(int(iid), []).append(price_date)
    return out


def load_asset_class(conn: psycopg.Connection[Any]) -> dict[int, str | None]:
    """Asset class as the quarantine SAW it, from its own coverage row.

    ⚠ Not from ``instruments``. ``price_quarantine_coverage.asset_class`` is stamped
    *"as seen at evaluation time"* (``sql/247:44``), so it is the class the stored
    verdicts were produced under. A later reclassification on ``instruments`` would
    silently re-parameterise rules that already ran.
    """
    return {
        int(iid): cls
        for iid, cls in conn.execute(
            "SELECT instrument_id, asset_class FROM price_quarantine_coverage WHERE rule_set_version = %(ver)s",
            {"ver": RULE_SET_VERSION},
        ).fetchall()
    }


def _section(title: str, lines: list[str]) -> None:
    print(f"\n{title}")
    print("-" * 100)
    for line in lines:
        print(f"  {line}")


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

        frontier_row = conn.execute("SELECT max(price_date), count(*) FROM price_daily").fetchone()
        assert frontier_row is not None
        print("=" * 100)
        print("#3046 residual 5 — the contract decision, five arms")
        print("=" * 100)
        print(f"  git                       {_git_sha()}")
        print(f"  arm version               {CONTRACT_ARM_VERSION}")
        print(f"  quarantine RULE_SET       {RULE_SET_VERSION}")
        print(f"  corpus                    {frontier_row[1]} bars, frontier {frontier_row[0]}")
        print("  crossing predicate        price_quarantine.rule_w1 / rule_w2 — called, never mirrored")

        _section("ARM A — is T1 a restatement of a bar verdict?  (clause 1)", arm_a(conn))
        _section("ARM B — is T3 carried by the operand price_segments loads?  (clause 2)", arm_b(conn))
        _section("ARM C — the W1 population and its fail-closed half  (clause 3)", arm_c(conn))
        _section("ARM E — what a naive W1 would over-refuse  (clause 3)", arm_e(conn))

        transitions = load_transitions(conn)
        asset_class = load_asset_class(conn)
        anchors = {slice_: load_anchors(conn, slice_) for slice_ in SLICES}
        print(
            f"\n  window machinery          {len(SPECS)} specs over {len(SLICES)} slices, "
            f"imported from verify_3046_consumer_exposure"
        )
        _section(
            "ARM D — does rule_w2 fire with ZERO quarantined transitions?  (clause 4)",
            arm_d(anchors, transitions, asset_class),
        )

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
