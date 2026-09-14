"""#3046 — adjudicate the suppressed-transition blind spot against an independent archive.

Spec: ``docs/proposals/ta/2026-09-14-3046-archive-continuity-discriminator.md``.
Read-only. Writes nothing.

WHY THIS EXISTS. ``verify_3046_break_minting_census`` established the SIZE of the
population ``price_segments`` cannot see — transitions that clear their class
magnitude threshold, carry T1 and/or T2, and therefore never reach T3
(``price_quarantine.py:486``). It deliberately declines to say whether any of them
is a real defect. This script asks the independent question the census could not:
over the SAME two dates, does a NON-eToro archive carry the same move our series
does — or does ours carry a scale factor the market never produced?

WHAT IS COMPARED IS THE **DISCREPANCY**, NOT THE TWO MAGNITUDES. The quantity
tested is ``d = our_ratio / archive_ratio`` over the same two dates, against
``params_for(asset_class).magnitude_threshold``.

⚠⚠ The obvious formulation — "does the archive ALSO clear the threshold" — is
wrong twice, and Codex checkpoint 1 killed it:

* ``price_quarantine``'s own header says *"Magnitude is a trigger, not a
  verdict"*. Asking whether the archive trips the same trigger promotes that
  trigger to a verdict on the other side of the comparison.
* The threshold is calibrated on a SAME-SCALE move between adjacent bars. A T2
  pair spans a hole — months, sometimes years — so a legitimate cumulative return
  across it can clear 5x with nothing wrong anywhere. Applying a daily-move
  constant to a multi-year return compares two different quantities.

The discrepancy has neither problem: it is dimensionless, it has no time
dimension (the market's own move divides out), and a scale artefact on our side
is exactly what it isolates. Direction falls out for free — an archive that moved
the same size the other way produces a discrepancy of ``ratio^2``, not 1.
No constant is invented; the class threshold is applied to the quantity it
describes (a same-span level mismatch) instead of to one it does not.

⚠⚠ THE SAME VERDICT MEANS OPPOSITE THINGS IN THE TWO RULE CLASSES, which is why
nothing here is pooled:

* **T1** — an endpoint bar is ``return_usable = false``. The classifier's own
  comment is *"the ratio across an unusable close is not a return regardless of
  its size"*. So ``sources_disagree`` here CORROBORATES the suppression: the
  magnitude is an artefact of a bar the quarantine already condemned, and the
  archive showing no such move is what that looks like from outside.
* **T2** — the pair spans a hole and both endpoints are ``return_usable``.
  Nothing masks it and nothing segments it, so ``sources_disagree`` here is a
  level shift that no consumer is told about.
* **T1+T2** — reads as T1. An unusable endpoint makes the ratio not-a-return
  whatever the calendar did, so the hole adds nothing to the reading. (The class
  is empty on today's corpus; the count is printed rather than assumed.)

⚠ A T1 verdict is about BREAK MINTING, not about consumer exposure. The bar is
still stored, and ``price_masked_bars`` masks the close on the return axis only —
B2/B3 range defects reach ``high``/``low`` untouched. "Correctly suppressed" here
means "no ``price_series_break`` is owed", never "nothing downstream is affected".

⚠ THE ASYMMETRY BETWEEN THE TWO BASES IS NOT COSMETIC. Our ``price_daily`` is
back-adjusted by the provider at fetch time (``market_data.detect_adjustment_event``
docstring). So a disagreement against a ``split_adjusted`` archive points at our
series; a disagreement against an ``unadjusted`` one can equally be the archive
showing a split we healed. The basis is printed on every adjudicated row and the
two are never summed.

⚠ INDEPENDENCE IS A PROPERTY OF THE UPSTREAM, NOT THE VENDOR. ``sql/249``'s own
comment: *"two vendors that both resolve to 'yahoo' are ONE observation, not two,
and any cross-source agreement between them is circular."* ``upstream_source =
'etoro'`` series are excluded outright (they are our own feed), and where two
vendor series reach the same pair the row yields ONE verdict — reported as
``vendor_disagreement`` when they differ, never resolved by majority.

⚠ WHAT THE ARCHIVE CANNOT REACH IS UNADJUDICATED, NOT ADJUDICATED-SAFE. The
archives are US-centric; the unadjudicable buckets are printed BESIDE the verdicts
and never under them, and no conclusion about the whole blind spot is drawn from
the adjudicable subset.

AND THEN: HOW BIG IS THE T2 BLIND SPOT ACTUALLY? (#3046 residual 1, added 2026-09-14.)
The T2 count above is the number the ticket quotes, and it is the wrong order of
magnitude. A T2 ratio is ``close(price_date) / close(prior_date)`` where
``prior_date`` is by construction the immediately preceding STORED bar — so it is
a claim about a level shift only if both closes were observed. eToro carries the
last close forward after a name stops quoting (zero range, ``volume IS NULL``,
repeating the previous close), and a ratio measured to one of those is arithmetic
over a level the market never set.

``T2 LEVEL PROVENANCE`` stratifies the class by where each operand's level came
from — see ``classify_level_provenance`` — into four tiers. A, B and C are
UNRESOLVED, NOT SAFE; only D is two observed levels. Spec addendum in the same
document.

⚠ The operand does NOT refuse, delete or re-classify any bar. The same predicate
was falsified as an INGEST-refusal rule on #3046 (2026-09-14) — its weekday
population is large and uncharacterised. Describing a stored ratio's operands and
refusing a bar are different acts.

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_archive_continuity
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, NamedTuple

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION, params_for
from scripts.verify_3046_break_minting_census import (
    BLIND_BUCKETS,
    bucket_for,
    magnitude_or_none,
)

#: Identity of the T2 level-provenance stratification below. ``RULE_SET_VERSION``
#: cannot move when only this classification changes, so without a version of its
#: own two runs reporting different residuals would be indistinguishable from a
#: corpus change.
STRATIFIER_VERSION = "t2-level-provenance-v1"

#: Rule-class label for a blind-spot transition. Derived from the census bucket
#: so the two scripts cannot drift apart on what "T1" means.
_CLASS_OF_BUCKET: dict[str, str] = {
    "t1_suppressed": "T1",
    "t2_suppressed": "T2",
    "t1_t2_suppressed": "T1+T2",
}

_VERDICTS: tuple[str, ...] = (
    "sources_agree",
    "sources_disagree",
    "vendor_disagreement",
    "provisional_deferred",
    "archive_bar_unusable",
    "archive_bar_missing",
    "pair_outside_span",
    "no_archive_series",
)

#: Verdicts that are an ANSWER. Everything else is unadjudicated, and the two
#: are printed apart — an unreachable transition is not a safe one.
_ADJUDICATED: tuple[str, ...] = ("sources_agree", "sources_disagree", "vendor_disagreement")

_VERDICT_NOTES: dict[str, str] = {
    "sources_agree": "discrepancy under T -> our series moved with the independent source",
    "sources_disagree": "discrepancy clears T -> the shift is OURS, not the market's",
    "vendor_disagreement": "two vendor series reach the pair and disagree",
    "provisional_deferred": "an endpoint bar is provisional; a part-session bar cannot adjudicate",
    "archive_bar_unusable": "series spans the pair, a close on it is non-positive",
    "archive_bar_missing": "series spans the pair, no bar on one/both dates",
    "pair_outside_span": "a series exists but does not span both dates",
    "no_archive_series": "no non-eToro series resolves to this instrument",
}


class Transition(NamedTuple):
    """One blind-spot transition, as the classifier stored it."""

    instrument_id: int
    price_date: date
    prior_date: date
    rule_class: str
    ratio: Decimal
    magnitude: Decimal
    threshold: Decimal
    asset_class: str | None
    provisional: bool


class Series(NamedTuple):
    """One archive series resolved to our instrument."""

    series_id: int
    instrument_id: int
    vendor: str
    upstream_source: str
    adjustment_basis: str
    first_bar: date
    last_bar: date


class Adjudication(NamedTuple):
    """One vendor series' reading of one transition."""

    vendor: str
    adjustment_basis: str
    archive_ratio: Decimal
    discrepancy: Decimal
    """``max(d, 1/d)`` for ``d = our_ratio / archive_ratio`` — the scale factor
    our series carries that the independent source does not."""
    disagrees: bool


class StoredBar(NamedTuple):
    """One ``price_daily`` row plus its stored RANGE verdict (never its return one)."""

    price_date: date
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None
    range_usable: bool | None


class Stratified(NamedTuple):
    """One T2 transition with the provenance of each of its two operands."""

    transition: Transition
    verdict: str
    prior_provenance: str
    resume_provenance: str
    prior_level_date: date | None
    """Where the prior operand's LEVEL was actually observed, when it was."""
    tier: str
    volume_ever_seen_by_prior: bool
    """Era-local: does any bar at or before ``prior_date`` carry positive volume?"""


def _fmt(n: int) -> str:
    return f"{n:,}"


def _load_blind(conn: psycopg.Connection[Any]) -> list[Transition]:
    """The blind spot, classified by the census's own bucket function."""
    rows = conn.execute(
        """
        SELECT t.instrument_id, t.price_date, t.prior_date, t.rules,
               t.observed_ratio, t.provisional, cov.asset_class
        FROM price_transition_quarantine t
        LEFT JOIN price_quarantine_coverage cov ON cov.instrument_id = t.instrument_id
        WHERE t.rule_set_version = %(ver)s
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchall()

    out: list[Transition] = []
    for instrument_id, price_date, prior_date, rules, ratio, provisional, asset_class in rows:
        magnitude = magnitude_or_none(ratio)
        if magnitude is None:
            continue
        threshold = params_for(asset_class).magnitude_threshold
        bucket = bucket_for(list(rules), magnitude >= threshold, bool(provisional))
        if bucket not in BLIND_BUCKETS:
            continue
        out.append(
            Transition(
                instrument_id=int(instrument_id),
                price_date=price_date,
                prior_date=prior_date,
                rule_class=_CLASS_OF_BUCKET[bucket],
                ratio=Decimal(ratio),
                magnitude=magnitude,
                threshold=threshold,
                asset_class=asset_class,
                provisional=bool(provisional),
            )
        )
    return out


def _load_series(conn: psycopg.Connection[Any], instrument_ids: list[int]) -> dict[int, list[Series]]:
    """Independent (non-eToro) archive series per instrument.

    ⚠ The exclusion is on ``upstream_source``, not on ``vendor``. A vendor name
    says who published the file; ``upstream_source`` says whose observation it
    is, and an eToro-derived comparator agreeing with eToro is circular.
    """
    if not instrument_ids:
        return {}
    rows = conn.execute(
        """
        SELECT series_id, instrument_id, vendor, upstream_source,
               adjustment_basis, first_bar, last_bar
        FROM research_price_series
        WHERE instrument_id = ANY(%(ids)s)
          AND upstream_source <> 'etoro'
          AND first_bar IS NOT NULL AND last_bar IS NOT NULL
        """,
        {"ids": sorted(set(instrument_ids))},
    ).fetchall()
    grouped: dict[int, list[Series]] = defaultdict(list)
    for series_id, instrument_id, vendor, upstream, basis, first_bar, last_bar in rows:
        grouped[int(instrument_id)].append(
            Series(
                series_id=int(series_id),
                instrument_id=int(instrument_id),
                vendor=vendor,
                upstream_source=upstream,
                adjustment_basis=basis,
                first_bar=first_bar,
                last_bar=last_bar,
            )
        )
    return dict(grouped)


def _load_closes(
    conn: psycopg.Connection[Any], series_ids: list[int], dates: list[date]
) -> dict[tuple[int, date], Decimal]:
    """Archive closes for exactly the (series, date) pairs under test.

    ⚠ EXACT DATES ONLY. Our ``prior_date`` is the previous STORED bar, so a T2
    pair spans a hole; sliding the archive endpoint to a nearest available date
    would compute a return over a different span than the one being adjudicated
    and would do it silently.
    """
    if not series_ids or not dates:
        return {}
    rows = conn.execute(
        """
        SELECT series_id, bar_date, close
        FROM research_price_daily
        WHERE series_id = ANY(%(sids)s) AND bar_date = ANY(%(dates)s)
        """,
        {"sids": sorted(set(series_ids)), "dates": sorted(set(dates))},
    ).fetchall()
    return {(int(sid), bar_date): Decimal(close) for sid, bar_date, close in rows if close is not None}


def _adjudicate(
    transition: Transition,
    series: list[Series],
    closes: dict[tuple[int, date], Decimal],
) -> tuple[str, list[Adjudication]]:
    """Verdict for one transition, plus every vendor reading behind it."""
    # ⚠ A provisional endpoint is refused BEFORE reachability, because the
    # refusal is a property of our own bar and does not depend on what the
    # archive holds. `price_quarantine` defers T3 on a provisional bar for the
    # same reason — a part-session bar is never verdict-bearing corroboration —
    # and adjudicating one here would be that rule broken from outside.
    if transition.provisional:
        return "provisional_deferred", []
    if not series:
        return "no_archive_series", []
    spanning = [s for s in series if s.first_bar <= transition.prior_date and transition.price_date <= s.last_bar]
    if not spanning:
        return "pair_outside_span", []

    readings: list[Adjudication] = []
    unusable = False
    for s in spanning:
        # Both endpoints come from ONE series_id. Pairing a prior close from one
        # vendor with a later close from another would manufacture a return out
        # of two different adjustment bases.
        prior = closes.get((s.series_id, transition.prior_date))
        later = closes.get((s.series_id, transition.price_date))
        if prior is None or later is None:
            continue
        # A non-positive close is not a price. `research_price_daily.close` is
        # NOT NULL but the corpus carries zero-valued sentinels (#2354), so the
        # guard is on value, not on nullability — and it is reported as its own
        # state rather than folded into "no bar", which would read as absence.
        if prior <= 0 or later <= 0:
            unusable = True
            continue
        archive_ratio = later / prior
        discrepancy_ratio = transition.ratio / archive_ratio
        discrepancy = max(discrepancy_ratio, Decimal(1) / discrepancy_ratio)
        readings.append(
            Adjudication(
                vendor=s.vendor,
                adjustment_basis=s.adjustment_basis,
                archive_ratio=archive_ratio,
                discrepancy=discrepancy,
                disagrees=discrepancy >= transition.threshold,
            )
        )

    if not readings:
        return ("archive_bar_unusable" if unusable else "archive_bar_missing"), []
    verdicts = {r.disagrees for r in readings}
    if len(verdicts) > 1:
        return "vendor_disagreement", readings
    return ("sources_disagree" if readings[0].disagrees else "sources_agree"), readings


def _print_identities(conn: psycopg.Connection[Any]) -> None:
    print("=" * 84)
    print("PINNED IDENTITIES")
    print("=" * 84)
    print(f"  price_quarantine.RULE_SET_VERSION  {RULE_SET_VERSION}")
    rows = conn.execute(
        "SELECT rule_set_version, count(*) FROM price_transition_quarantine GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    print("  transition versions                " + (", ".join(f"{v} ({_fmt(int(n))})" for v, n in rows) or "(empty)"))
    print()
    print("  archive corpus — the comparand, by upstream observation")
    print(f"    {'vendor':<38}{'upstream':<18}{'basis':<28}{'series':>7}{'resolved':>9}{'last bar':>12}")
    for vendor, upstream, basis, series, resolved, last_bar in conn.execute(
        """
        SELECT vendor, upstream_source, adjustment_basis, count(*),
               count(instrument_id), max(last_bar)
        FROM research_price_series GROUP BY 1,2,3 ORDER BY 4 DESC
        """
    ).fetchall():
        marker = "  <- EXCLUDED (our own feed)" if upstream == "etoro" else ""
        print(
            f"    {vendor[:37]:<38}{upstream:<18}{basis:<28}"
            f"{_fmt(int(series)):>7}{_fmt(int(resolved)):>9}{str(last_bar):>12}{marker}"
        )
    print()


def _print_population(blind: list[Transition]) -> None:
    by_class: Counter[str] = Counter(t.rule_class for t in blind)
    by_class_asset: Counter[tuple[str, str]] = Counter((t.rule_class, t.asset_class or "NULL") for t in blind)
    print("=" * 84)
    print(f"POPULATION — the blind spot at {RULE_SET_VERSION}")
    print("=" * 84)
    print(f"  transitions          {_fmt(len(blind))}")
    print(f"  instruments          {_fmt(len({t.instrument_id for t in blind}))}")
    for rule_class in sorted(by_class):
        ranked = sorted(by_class_asset.items(), key=lambda kv: -kv[1])
        assets = ", ".join(f"{asset} {_fmt(n)}" for (cls, asset), n in ranked if cls == rule_class)
        print(f"  {rule_class:<6} {_fmt(by_class[rule_class]):>6}   {assets}")
    print()


def _print_verdicts(results: list[tuple[Transition, str, list[Adjudication]]]) -> None:
    per_class: dict[str, Counter[str]] = defaultdict(Counter)
    for transition, verdict, _ in results:
        per_class[transition.rule_class][verdict] += 1

    print("=" * 84)
    print("ADJUDICATION — per rule class, never pooled")
    print("=" * 84)
    for rule_class in sorted(per_class):
        total = sum(per_class[rule_class].values())
        adjudicated = sum(per_class[rule_class][v] for v in _ADJUDICATED)
        print(f"\n  {rule_class}  ({_fmt(total)} transitions, {_fmt(adjudicated)} reachable by an independent source)")
        for verdict in _VERDICTS:
            count = per_class[rule_class][verdict]
            if count:
                print(f"    {verdict:<22}{_fmt(count):>7}   {_VERDICT_NOTES[verdict]}")
    print()


def _print_detail(results: list[tuple[Transition, str, list[Adjudication]]]) -> None:
    """Every adjudicated row. No top-N cut — a silent truncation reads as coverage."""
    adjudicated = [(t, v, r) for t, v, r in results if v in _ADJUDICATED]
    print("=" * 84)
    print(f"ADJUDICATED ROWS — all {_fmt(len(adjudicated))}, no cut")
    print("=" * 84)
    print(
        f"  {'cls':<6}{'instrument':>11}  {'prior -> date':<25}{'T':>3}{'ours':>11}"
        f"{'archive':>10}{'discrep':>10}  {'basis':<22}vendor"
    )
    for transition, verdict, readings in sorted(adjudicated, key=lambda r: (r[0].rule_class, -float(r[0].magnitude))):
        for reading in readings:
            flag = "  <- VENDORS DISAGREE" if verdict == "vendor_disagreement" else ""
            archive_magnitude = max(reading.archive_ratio, Decimal(1) / reading.archive_ratio)
            print(
                f"  {transition.rule_class:<6}{transition.instrument_id:>11}  "
                f"{str(transition.prior_date)} -> {str(transition.price_date):<11}"
                f"{float(transition.threshold):>3.0f}{float(transition.magnitude):>11.2f}"
                f"{float(archive_magnitude):>10.2f}{float(reading.discrepancy):>10.2f}  "
                f"{reading.adjustment_basis:<22}{reading.vendor.split('/')[0][:20]}{flag}"
            )
    print()


def is_observed(bar: StoredBar, *, range_verdict_known: bool) -> bool:
    """Did this date carry an observation, by our OWN rules?

    ⚠ POSITIVE volume, not merely non-NULL. ``price_quarantine._usable_volume``
    (``app/services/price_quarantine.py:302``) rejects ``<= 0``, and eToro's
    normaliser ``_int_or_none`` (``app/providers/implementations/etoro.py:861``)
    stores a ZERO volume AS NULL — so NULL and zero are already
    indistinguishable at rest, and reading NULL as anything but "no evidence"
    would be reading a normalisation artefact as a fact.

    ⚠ RANGE AND RETURN ARE SEPARATE VERDICT AXES AND ARE NOT CROSSED HERE.
    ``sql/247_price_quarantine.sql:10-17`` — B2 (containment) and B3 (phantom
    wick) set ``range_usable = false`` and leave ``return_usable`` true, because
    "one verdict class = one column". So ``high > low`` on a B2/B3 bar is a
    KNOWN-BAD range, not evidence that the session traded.

    ⚠ FAIL-CLOSED ON AN UNEVALUATED INSTRUMENT. ``sql/247``'s own header: the
    verdict tables are SPARSE, so absence of a row means "clean" only where a
    ``price_quarantine_coverage`` row says the instrument was evaluated. Without
    one, ``range_usable`` is UNKNOWN and range is not admitted as evidence —
    volume alone can carry the bar.
    """
    if bar.volume is not None and bar.volume > 0:
        return True
    if bar.range_usable is False or not range_verdict_known:
        return False
    return bar.high is not None and bar.low is not None and bar.high > bar.low


def classify_level_provenance(
    bars: Sequence[StoredBar], index: int, *, evaluated_span: tuple[date, date] | None
) -> tuple[str, date | None]:
    """Where did ``bars[index]``'s close come from? Pure; see the spec addendum.

    ``bars`` MUST be the instrument's complete stored history in ascending date
    order. Walking a filtered or joined subset would invent first bars and skip
    intervening rows, which is a different question wearing the same name.

    ⚠ ``stale_observed_level`` is the class revision 1 of this spec did not have,
    and its absence is what made that revision wrong: folding every
    carry-forward into "not a level claim" silently exonerates
    ``observed 100 -> carried 100 -> hole -> observed 10``, where the 10x can be
    a real scale error and only the earlier operand's DATE is wrong.

    ``evaluated_span`` is the instrument's ``price_quarantine_coverage`` interval
    at the current rule set, or ``None`` if it has none. It is checked PER BAR,
    not per instrument: a bar backfilled outside that interval carries no range
    verdict and never did.
    """

    def known(bar: StoredBar) -> bool:
        return evaluated_span is not None and evaluated_span[0] <= bar.price_date <= evaluated_span[1]

    if is_observed(bars[index], range_verdict_known=known(bars[index])):
        return "observed", bars[index].price_date
    cursor = index
    while cursor > 0 and bars[cursor].close is not None and bars[cursor].close == bars[cursor - 1].close:
        cursor -= 1
        if is_observed(bars[cursor], range_verdict_known=known(bars[cursor])):
            return "stale_observed_level", bars[cursor].price_date
    # ⚠ THE VERDICT IS WHERE THE WALK STOPPED, NOT WHETHER IT MOVED. Codex
    # checkpoint 2: ``cursor != index`` was reading ``observed 10 -> 12 -> 12``
    # as fabricated, because the run moved — but it stopped at a PRICE CHANGE,
    # not at the series start, so 12 is an undecided new level that happens to
    # have been repeated. Only reaching bar 0 exhausts the history behind the
    # level; anything else leaves a differing predecessor standing.
    return ("fabricated_level", None) if cursor == 0 else ("zero_range_new_level", None)


#: Tier precedence. First matching entry wins, so a transition is in exactly one.
_TIERS: tuple[tuple[str, frozenset[str]], ...] = (
    ("A_fabricated_prior_level", frozenset({"fabricated_level", "absent"})),
    ("B_stale_level", frozenset({"stale_observed_level"})),
    ("C_degenerate_endpoint", frozenset({"zero_range_new_level"})),
)

_TIER_NOTES: dict[str, str] = {
    "A_fabricated_prior_level": "an operand is a level the market never set",
    "B_stale_level": "the level is real; its DATE is not — the true span is longer",
    "C_degenerate_endpoint": "zero range at a NEW level — thin session or placeholder, undecided",
    "D_both_endpoints_observed": "the genuine residual: two observed levels",
}


def tier_for(prior_provenance: str, resume_provenance: str) -> str:
    """Tier from the two endpoint provenances. A, B and C are UNRESOLVED, not safe."""
    pair = {prior_provenance, resume_provenance}
    for tier, members in _TIERS:
        if pair & members:
            return tier
    return "D_both_endpoints_observed"


def _load_stored_bars(conn: psycopg.Connection[Any], instrument_ids: list[int]) -> dict[int, list[StoredBar]]:
    """Complete stored history for each instrument, ascending, with its range verdict."""
    if not instrument_ids:
        return {}
    rows = conn.execute(
        """
        SELECT p.instrument_id, p.price_date, p.high, p.low, p.close, p.volume, q.range_usable
        FROM price_daily p
        LEFT JOIN price_bar_quarantine q
          ON q.instrument_id = p.instrument_id
         AND q.price_date = p.price_date
         AND q.rule_set_version = %(ver)s
        WHERE p.instrument_id = ANY(%(ids)s)
        ORDER BY p.instrument_id, p.price_date
        """,
        {"ids": sorted(set(instrument_ids)), "ver": RULE_SET_VERSION},
    ).fetchall()
    grouped: dict[int, list[StoredBar]] = defaultdict(list)
    for instrument_id, price_date, high, low, close, volume, range_usable in rows:
        grouped[int(instrument_id)].append(
            StoredBar(
                price_date=price_date,
                high=None if high is None else Decimal(high),
                low=None if low is None else Decimal(low),
                close=None if close is None else Decimal(close),
                volume=None if volume is None else Decimal(volume),
                range_usable=None if range_usable is None else bool(range_usable),
            )
        )
    return dict(grouped)


def _load_evaluated_spans(conn: psycopg.Connection[Any], instrument_ids: list[int]) -> dict[int, tuple[date, date]]:
    """The DATE INTERVAL each instrument was evaluated over, at this rule set.

    ⚠ INSTRUMENT-LEVEL COVERAGE IS NOT ENOUGH, and Codex checkpoint 2 caught the
    difference. ``price_quarantine_coverage`` records ``first_bar``/``last_bar``
    precisely because a later backfill can add bars OUTSIDE the evaluated span:
    those carry no ``price_bar_quarantine`` row and never did, so reading their
    absence as "clean" would admit an unchecked range as evidence. The
    provenance walk visits arbitrarily old history, which is exactly where
    backfilled bars live.
    """
    if not instrument_ids:
        return {}
    return {
        int(instrument_id): (first_bar, last_bar)
        for instrument_id, first_bar, last_bar in conn.execute(
            """
            SELECT instrument_id, first_bar, last_bar FROM price_quarantine_coverage
            WHERE instrument_id = ANY(%(ids)s) AND rule_set_version = %(ver)s
              AND first_bar IS NOT NULL AND last_bar IS NOT NULL
            """,
            {"ids": sorted(set(instrument_ids)), "ver": RULE_SET_VERSION},
        ).fetchall()
    }


def _stratify(
    t2: list[tuple[Transition, str]],
    bars: dict[int, list[StoredBar]],
    evaluated_spans: dict[int, tuple[date, date]],
) -> list[Stratified]:
    out: list[Stratified] = []
    for transition, verdict in t2:
        series = bars.get(transition.instrument_id, [])
        span = evaluated_spans.get(transition.instrument_id)
        index_of = {bar.price_date: i for i, bar in enumerate(series)}
        provenances: list[tuple[str, date | None]] = []
        for endpoint in (transition.prior_date, transition.price_date):
            position = index_of.get(endpoint)
            if position is None:
                provenances.append(("absent", None))
                continue
            provenances.append(classify_level_provenance(series, position, evaluated_span=span))
        (prior_provenance, prior_level_date), (resume_provenance, _) = provenances
        # ⚠ Era-local, NOT lifetime. One populated bar years later would flip a
        # "never" instrument to "partial" and change nothing about this date.
        volume_seen = any(
            bar.volume is not None and bar.volume > 0 and bar.price_date <= transition.prior_date for bar in series
        )
        out.append(
            Stratified(
                transition=transition,
                verdict=verdict,
                prior_provenance=prior_provenance,
                resume_provenance=resume_provenance,
                prior_level_date=prior_level_date,
                tier=tier_for(prior_provenance, resume_provenance),
                volume_ever_seen_by_prior=volume_seen,
            )
        )
    return out


def _print_provenance(rows: list[Stratified]) -> None:
    print("=" * 84)
    print(f"T2 LEVEL PROVENANCE — stratifier {STRATIFIER_VERSION}")
    print("=" * 84)
    print("  A T2 ratio is a claim about a LEVEL SHIFT only if both its operands were")
    print("  observed. A, B and C below are UNRESOLVED, not safe.")
    print()
    print(f"  {'tier':<28}{'rows':>7}{'instruments':>13}{'no vol <= prior':>17}   note")
    for tier in (*(name for name, _ in _TIERS), "D_both_endpoints_observed"):
        members = [r for r in rows if r.tier == tier]
        novol = sum(1 for r in members if not r.volume_ever_seen_by_prior)
        print(
            f"  {tier:<28}{_fmt(len(members)):>7}"
            f"{_fmt(len({r.transition.instrument_id for r in members})):>13}{_fmt(novol):>17}   {_TIER_NOTES[tier]}"
        )
    print(f"  {'TOTAL':<28}{_fmt(len(rows)):>7}")
    print()
    print("  endpoint provenance x archive verdict, per tier")
    for tier in (*(name for name, _ in _TIERS), "D_both_endpoints_observed"):
        members = [r for r in rows if r.tier == tier]
        if not members:
            continue
        print(f"\n    {tier}")
        cross: Counter[tuple[str, str, str]] = Counter(
            (r.prior_provenance, r.resume_provenance, r.verdict) for r in members
        )
        for (prior, resume, verdict), count in sorted(cross.items()):
            print(f"      {prior:<22}{resume:<22}{verdict:<22}{_fmt(count):>6}")
    print()
    genuine = sorted(
        (r for r in rows if r.tier == "D_both_endpoints_observed"),
        key=lambda r: -float(r.transition.magnitude),
    )
    print(f"  THE GENUINE RESIDUAL — all {_fmt(len(genuine))} rows, no cut")
    print(f"    {'instrument':>11}  {'prior -> date':<26}{'T':>3}{'magnitude':>11}  {'asset':<12}verdict")
    for row in genuine:
        print(
            f"    {row.transition.instrument_id:>11}  "
            f"{str(row.transition.prior_date)} -> {str(row.transition.price_date):<12}"
            f"{float(row.transition.threshold):>3.0f}{float(row.transition.magnitude):>11.2f}  "
            f"{(row.transition.asset_class or 'NULL'):<12}{row.verdict}"
        )
    print()


def _reconcile_provenance(
    rows: list[Stratified], bars: dict[int, list[StoredBar]], t2_count: int
) -> tuple[bool, int, int, int]:
    """Arms 4 and 5. Returns (ok, absent_endpoints, resume_repeat_hits, ratio_mismatches).

    Arm 4 — PARTITION. Tiers must sum to the T2 population, and no endpoint may be
    ABSENT from ``price_daily``. ⚠ Counting "rows whose tier is not a known tier"
    would be a tautology — ``tier_for`` returns nothing else — and the review bot
    said so. The condition with content is the one the tautology was standing in
    for: an ``absent`` endpoint folds into tier A, where it is indistinguishable
    from a fabricated level, so it is counted and printed in its own right. A hit
    means a transition references a bar that is no longer stored.

    Arm 4b — STRUCTURAL. ``prior_date`` is by construction the immediately preceding
    stored bar, so the resume endpoint's predecessor IS the prior endpoint. A resume
    endpoint reading ``stale_observed_level``/``fabricated_level`` therefore requires
    ``close(price_date) == close(prior_date)`` — a ratio of exactly 1, which clears no
    magnitude threshold and cannot be in the blind spot. A hit means the stored
    transition and ``price_daily`` have drifted apart.

    Arm 5 — STORED RATIO. ``REPEATABLE READ`` gives one snapshot; it does NOT give
    agreement between a quarantine row minted at an earlier refresh and the
    ``price_daily`` this classifier reads now. Recompute
    ``close(price_date) / close(prior_date)`` (``_usable_close`` semantics: positive
    only) and compare against the stored ``observed_ratio``. A mismatch voids the
    residual counts — the bars classified are not the bars that minted the transition.
    """
    absent_endpoints = sum(
        1 for r in rows for provenance in (r.prior_provenance, r.resume_provenance) if provenance == "absent"
    )
    resume_repeat_hits = sum(1 for r in rows if r.resume_provenance in {"stale_observed_level", "fabricated_level"})
    mismatches = 0
    for row in rows:
        closes = {
            bar.price_date: bar.close
            for bar in bars.get(row.transition.instrument_id, [])
            if bar.close is not None and bar.close > 0
        }
        prior, later = closes.get(row.transition.prior_date), closes.get(row.transition.price_date)
        if prior is None or later is None:
            mismatches += 1
            continue
        # Stored at the column's own precision (NUMERIC(24,12)); compare at it
        # rather than exactly, or every row fails on the last digit of a stored
        # quotient. ⚠ ROUND_HALF_UP, not Decimal's default ROUND_HALF_EVEN:
        # Postgres numeric rounds half AWAY FROM ZERO, so an exact tie at the
        # twelfth decimal would fail reconciliation on data that never moved and
        # void the whole report (Codex checkpoint 2).
        recomputed = (later / prior).quantize(row.transition.ratio, rounding=ROUND_HALF_UP)
        if recomputed != row.transition.ratio:
            mismatches += 1
    ok = len(rows) == t2_count and absent_endpoints == 0 and resume_repeat_hits == 0 and mismatches == 0
    return ok, absent_endpoints, resume_repeat_hits, mismatches


def _reconcile(conn: psycopg.Connection[Any], blind: list[Transition]) -> tuple[bool, int, int, int]:
    """Three arms. Returns (ok, unresolved_break_hits, resolved_break_hits, coverage_off_version).

    Arm 1 — the DEFINITIONAL claim, and it must be scoped to UNRESOLVED breaks.
    ``price_segments.load_unresolved_breaks`` reads ``resolved_by IS NULL``, and
    ``price_quarantine_store.py:138`` deletes only the unresolved rows on
    refresh — so a RESOLVED break row legitimately survives a re-evaluation that
    no longer mints its transition, and keying against the whole table would cry
    mismatch on perfectly valid data. (Codex checkpoint 1; the sibling census
    carries the same correction for the opposite direction.) Today's corpus has
    zero resolved rows, which is exactly why the scoping has to be written rather
    than observed to pass.

    Arm 2 — resolved hits, reported not failed. A resolved break keyed to a
    now-blind transition is an operator decision that outlived its evidence, and
    it is worth seeing; it is not this script measuring the wrong population.

    Arm 3 — version skew. ``price_quarantine_coverage`` supplies the asset class
    that picks every threshold above, so a coverage row written under an older
    rule set silently changes which transitions are "blind".
    """
    keys = [(t.instrument_id, t.price_date) for t in blind]
    unresolved_hits = resolved_hits = 0
    if keys:
        row = conn.execute(
            """
            SELECT count(*) FILTER (WHERE b.resolved_by IS NULL),
                   count(*) FILTER (WHERE b.resolved_by IS NOT NULL)
            FROM price_series_break b
            JOIN unnest(%(ids)s::bigint[], %(dates)s::date[]) AS u(i, d)
              ON u.i = b.instrument_id AND u.d = b.break_date
            """,
            {"ids": [k[0] for k in keys], "dates": [k[1] for k in keys]},
        ).fetchone()
        assert row is not None
        unresolved_hits, resolved_hits = int(row[0]), int(row[1])
    row = conn.execute(
        "SELECT count(*) FROM price_quarantine_coverage WHERE rule_set_version <> %(ver)s",
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert row is not None
    off_version = int(row[0])
    return (unresolved_hits == 0 and off_version == 0), unresolved_hits, resolved_hits, off_version


def run(conn: psycopg.Connection[Any]) -> int:
    _print_identities(conn)
    blind = _load_blind(conn)
    _print_population(blind)

    series = _load_series(conn, [t.instrument_id for t in blind])
    series_ids = [s.series_id for group in series.values() for s in group]
    dates = [d for t in blind for d in (t.prior_date, t.price_date)]
    closes = _load_closes(conn, series_ids, dates)

    results = [(t, *_adjudicate(t, series.get(t.instrument_id, []), closes)) for t in blind]
    _print_verdicts(results)
    _print_detail(results)

    # T2 ONLY, deliberately. A T1 endpoint is already `return_usable = false`, so
    # its ratio is not a return whatever the calendar did — asking where its level
    # came from would pool two rule classes the parent section keeps apart.
    t2 = [(t, v) for t, v, _ in results if t.rule_class == "T2"]
    stored_bars = _load_stored_bars(conn, [t.instrument_id for t, _ in t2])
    evaluated_spans = _load_evaluated_spans(conn, [t.instrument_id for t, _ in t2])
    stratified = _stratify(t2, stored_bars, evaluated_spans)
    _print_provenance(stratified)

    ok, unresolved_hits, resolved_hits, off_version = _reconcile(conn, blind)
    prov_ok, absent_endpoints, resume_repeats, ratio_mismatches = _reconcile_provenance(
        stratified, stored_bars, len(t2)
    )
    print("=" * 84)
    print("RECONCILIATION")
    print("=" * 84)
    print(f"  blind-spot keys carrying an UNRESOLVED break row    {_fmt(unresolved_hits)}   <- must be 0")
    print(f"  …carrying a RESOLVED one                           {_fmt(resolved_hits)}   (reported, not failed)")
    print(f"  coverage rows at another rule-set version           {_fmt(off_version)}   <- must be 0")
    print(f"  T2 transitions stratified                          {_fmt(len(stratified))}   of {_fmt(len(t2))}")
    print(f"  endpoints absent from price_daily                  {_fmt(absent_endpoints)}   <- must be 0")
    print(f"  resume endpoints reading as a repeat               {_fmt(resume_repeats)}   <- must be 0 (ratio 1)")
    print(f"  stored observed_ratio vs recomputed mismatches     {_fmt(ratio_mismatches)}   <- must be 0")
    if ok and prov_ok:
        print("  OK — the adjudicated population IS the blind spot, at one rule-set version,")
        print("  and the bars stratified are the bars that minted its transitions.")
    else:
        print("  MISMATCH — do not quote this run's verdicts or its residual counts: either")
        print("  a break row exists for a supposedly suppressed transition, the asset classes")
        print("  that picked the thresholds are stale, or price_daily has moved under the")
        print("  quarantine rows being classified.")
    print()
    return 0 if (ok and prov_ok) else 1


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="#3046 archive-continuity discriminator (read-only)").parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        # ⚠ READ-ONLY IS NOT ISOLATION — same reason as the sibling census:
        # `price_quarantine_refresh` rewrites all three quarantine tables on a
        # 24h cadence, so without REPEATABLE READ the transitions, the coverage
        # classes and the break table could each come from a different committed
        # state and the reconciliation would fail on a snapshot that never existed.
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        return run(conn)


if __name__ == "__main__":
    sys.exit(main())
