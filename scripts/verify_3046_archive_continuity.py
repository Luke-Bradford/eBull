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

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_archive_continuity
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, NamedTuple

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION, params_for
from scripts.verify_3046_break_minting_census import (
    BLIND_BUCKETS,
    bucket_for,
    magnitude_or_none,
)

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

    ok, unresolved_hits, resolved_hits, off_version = _reconcile(conn, blind)
    print("=" * 84)
    print("RECONCILIATION")
    print("=" * 84)
    print(f"  blind-spot keys carrying an UNRESOLVED break row    {_fmt(unresolved_hits)}   <- must be 0")
    print(f"  …carrying a RESOLVED one                           {_fmt(resolved_hits)}   (reported, not failed)")
    print(f"  coverage rows at another rule-set version           {_fmt(off_version)}   <- must be 0")
    if ok:
        print("  OK — the adjudicated population IS the blind spot, at one rule-set version.")
    else:
        print("  MISMATCH — this run is not adjudicating the census's population. Do not")
        print("  quote its verdicts: either a break row exists for a supposedly suppressed")
        print("  transition, or the asset classes that picked the thresholds are stale.")
    print()
    return 0 if ok else 1


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
