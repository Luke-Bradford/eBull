"""#3046 — how many large-magnitude transitions mint no ``price_series_break``.

Spec: ``docs/proposals/ta/2026-09-14-3046-raw-price-consumer-exposure.md``.
Read-only. Writes nothing.

WHY THIS EXISTS. ``price_quarantine.py:486`` evaluates T3 only when neither T1 nor
T2 has already fired::

    if magnitude >= params.magnitude_threshold and not rules:

The rationale for the T2 half is written directly above it and is sound on its own
terms — *"a price_series_break minted from a gap would strand history behind a break
that never happened."* Nothing addresses the other direction. A transition whose
magnitude clears its class threshold but which also spans a hole, or sits beside a
return-unusable close, is recorded as ``{T2}`` or ``{T1}``, mints NO break, and is
therefore invisible to ``app/services/price_segments.py`` — the model
``outcome_resolver`` and every strategy consumer segment on.

⚠⚠ THIS IS A SUPPRESSION CENSUS, NOT A MISSED-SPLIT COUNT. Whether a large move
across a three-month hole is a scale change or a real move is exactly the question
the classifier declines to answer, and this script does not answer it either. What
it establishes is the SIZE of the population the segment model cannot see.

⚠ It also says nothing about B2/B3 range defects, which set ``range_usable = false``
with no transition row at all (``sql/247_price_quarantine.sql:14-16``). A zero here
is silence about them, not safety.

⚠ FIXING THE SUPPRESSION IS NOT A SCRIPT-SIZED CHANGE. ``price_quarantine`` sits in
``INPUT_RULE_SETS`` (#3031), so editing the rules rotates strategy identity AND
empties the 76M-bar backtest substrate, and only the first is visible.

THE RECONCILIATION ARM IS THE ONE THAT EARNS ITS KEEP. ``t3_minted`` must equal the
``price_series_break`` row count. If it does not, this script is not measuring what
it claims, and it says so rather than printing a suppression figure nobody can
trust — the ``stored`` arm discipline from ``full-population-ab.md``.

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_break_minting_census
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION, params_for

#: Trailing window for the "does this bound anything live?" split. Not a rule
#: constant — it is the widest lookback any listed raw consumer carries
#: (``risk_metrics.TRAILING_LOOKBACK_DAYS['1y']`` = 365) rounded up to leave room
#: for a stale anchor, and it is reported as a cut, never as a gate.
RECENT_DAYS = 400


def _fmt(n: int) -> str:
    return f"{n:,}"


def _print_identities(conn: psycopg.Connection[Any]) -> tuple[bool, date | None]:
    """Print what this run measured under. Returns (coverage_is_uniform, corpus_max)."""
    print("=" * 78)
    print("PINNED IDENTITIES")
    print("=" * 78)
    print(f"  price_quarantine.RULE_SET_VERSION  {RULE_SET_VERSION}")

    # ⚠ Two LITERAL queries rather than one f-string over a table name. The
    # table names are ours and not user input, so this is not an injection
    # question — but psycopg's typed `execute` takes a LiteralString, and
    # interpolating defeats it for no gain at two call sites.
    for label, rows in (
        (
            "coverage",
            conn.execute(
                "SELECT rule_set_version, count(*) FROM price_quarantine_coverage GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall(),
        ),
        (
            "transitions",
            conn.execute(
                "SELECT rule_set_version, count(*) FROM price_transition_quarantine GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall(),
        ),
    ):
        versions = ", ".join(f"{v} ({_fmt(int(n))})" for v, n in rows) or "(empty)"
        print(f"  {label + ' versions':<34} {versions}")

    corpus_max_row = conn.execute("SELECT max(price_date) FROM price_daily").fetchone()
    today_row = conn.execute("SELECT current_date").fetchone()
    assert corpus_max_row is not None and today_row is not None
    corpus_max, today = corpus_max_row[0], today_row[0]
    print(f"  {'corpus max(price_date)':<34} {corpus_max}")
    print(f"  {'current_date':<34} {today}")

    # Coverage completeness. A census over stale verdicts is a DIFFERENT
    # measurement and must not be reported as this one.
    #
    # ⚠ `last_bar` ALONE IS NOT THE CHECK, and this is not hypothetical:
    # `refresh_market_data(force_backfill=True)` adds OLDER bars without moving
    # the newest date. Coverage then still matches on `last_bar`, the stale
    # transition and break tables still reconcile perfectly, and the census exits
    # 0 having never evaluated the backfilled history. `first_bar` and
    # `bars_evaluated` are the terms that see it. (Codex checkpoint 2.)
    coverage_row = conn.execute(
        """
        WITH anchor AS (
            SELECT instrument_id,
                   min(price_date) AS first_bar,
                   max(price_date) AS last_bar,
                   count(*)        AS bars
            FROM price_daily WHERE close IS NOT NULL GROUP BY 1
        )
        SELECT count(*),
               count(*) FILTER (WHERE cov.rule_set_version = %(ver)s),
               count(*) FILTER (WHERE cov.last_bar  < a.last_bar),
               count(*) FILTER (WHERE cov.first_bar > a.first_bar),
               count(*) FILTER (WHERE cov.bars_evaluated < a.bars)
        FROM anchor a
        LEFT JOIN price_quarantine_coverage cov ON cov.instrument_id = a.instrument_id
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert coverage_row is not None
    total, at_version, trailing, preceding, short = coverage_row
    uniform = int(at_version) == int(total) and int(trailing) == 0 and int(preceding) == 0 and int(short) == 0
    verdict = "uniform" if uniform else "NOT UNIFORM — census below is not comparable"
    print(f"  {'instruments with a close':<34} {_fmt(int(total))}")
    print(f"  {'…covered at current version':<34} {_fmt(int(at_version))}")
    print(f"  {'…coverage.last_bar behind newest':<34} {_fmt(int(trailing))}")
    print(f"  {'…coverage.first_bar after oldest':<34} {_fmt(int(preceding))}  (a backfill coverage cannot see)")
    print(f"  {'…bars_evaluated under bar count':<34} {_fmt(int(short))}")
    print(f"  {'verdict':<34} {verdict}")
    print()
    return uniform, corpus_max


def _reconcile(conn: psycopg.Connection[Any]) -> tuple[int, int, int]:
    """Key-match T3 verdicts against break rows. Returns (matched, t3_only, break_only).

    ⚠ COUNT-MATCHING AGAINST THE UNRESOLVED BREAKS IS WRONG, and it passes today
    by luck (nothing is resolved yet). ``price_quarantine_store.py:142`` deletes
    only ``resolved_by IS NULL`` rows on refresh, so an operator- or #2231-set
    resolution SURVIVES a re-evaluation while its T3 transition is rewritten —
    the two counts then differ on perfectly valid data and the script would cry
    mismatch. The sound comparison is on the key the writer uses:
    ``(instrument_id, break_date)`` where ``break_date`` is the transition's own
    ``price_date`` (``price_quarantine_store.py:195``). Caught by Codex
    checkpoint 2.

    ``break_only`` is therefore EXPECTED to be non-zero once anything is
    resolved, and is reported rather than failed. ``t3_only`` is the one that
    means the census is not measuring production: a current T3 verdict with no
    break row.
    """
    row = conn.execute(
        """
        WITH t3 AS (
            SELECT instrument_id, price_date AS break_date
            FROM price_transition_quarantine
            WHERE rule_set_version = %(ver)s AND rules @> ARRAY['T3']
        ),
        brk AS (
            SELECT instrument_id, break_date FROM price_series_break
        )
        SELECT count(*) FILTER (WHERE t3.instrument_id IS NOT NULL AND brk.instrument_id IS NOT NULL),
               count(*) FILTER (WHERE brk.instrument_id IS NULL),
               count(*) FILTER (WHERE t3.instrument_id IS NULL)
        FROM t3 FULL OUTER JOIN brk
          ON brk.instrument_id = t3.instrument_id AND brk.break_date = t3.break_date
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), int(row[2])


def _print_break_table(conn: psycopg.Connection[Any]) -> int:
    """Print the comparand. Returns the UNRESOLVED break count."""
    break_row = conn.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE resolved_by IS NULL),
               count(*) FILTER (WHERE resolved_by IS NOT NULL),
               count(DISTINCT instrument_id)
        FROM price_series_break
        """
    ).fetchone()
    assert break_row is not None
    total, unresolved, resolved, instruments = break_row
    print("=" * 78)
    print("COMPARAND — price_series_break, the operand price_segments reads")
    print("=" * 78)
    print(f"  rows                {_fmt(int(total))}   ({_fmt(int(instruments))} instruments)")
    # ⚠ price_segments.load_unresolved_breaks filters `resolved_by IS NULL`, and
    # price_quarantine_store.py:142 deletes only the unresolved rows on refresh —
    # so a resolved row SURVIVES a re-evaluation and is not a subset of today's
    # T3 verdicts. The two must be reported apart.
    print(f"  unresolved          {_fmt(int(unresolved))}   <- what price_segments segments on")
    print(f"  resolved            {_fmt(int(resolved))}   <- survives a verdict refresh")
    print()
    return int(unresolved)


def _load_transitions(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return conn.execute(
        """
        SELECT t.instrument_id,
               t.price_date,
               t.rules,
               t.observed_ratio,
               t.provisional,
               t.corroboration,
               cov.asset_class
        FROM price_transition_quarantine t
        LEFT JOIN price_quarantine_coverage cov
               ON cov.instrument_id = t.instrument_id
        WHERE t.rule_set_version = %(ver)s
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchall()


def _bucket(rules: list[str], clears: bool, provisional: bool) -> str:
    """Which census bucket a transition falls in.

    ⚠ Order matters: ``T3 ∈ rules`` wins outright because that is the arm the
    break table has to reconcile against, and a T3 row can never also carry T1 or
    T2 (line 486 refuses to evaluate it when either fired).
    """
    if "T3" in rules:
        return "t3_minted"
    if not clears:
        return "below_threshold"
    if not rules:
        return "deferred" if provisional else "admitted_back"
    has_t1, has_t2 = "T1" in rules, "T2" in rules
    if has_t1 and has_t2:
        return "t1_t2_suppressed"
    if has_t1:
        return "t1_suppressed"
    if has_t2:
        return "t2_suppressed"
    return "below_threshold"


_BUCKET_NOTES: dict[str, str] = {
    "t3_minted": "T3 fired -> a price_series_break exists",
    "t1_suppressed": "clears T, no T3: an endpoint close is return-unusable",
    "t2_suppressed": "clears T, no T3: the pair spans a hole",
    "t1_t2_suppressed": "clears T, no T3: both",
    "admitted_back": "clears T, T3 triggered, _corroboration admitted it (a real move)",
    "deferred": "clears T, provisional bar — verdict deferred, not declined",
    "below_threshold": "magnitude under its class threshold",
}
#: The buckets the segment model cannot see AND which were never adjudicated.
#: ``admitted_back`` is excluded deliberately: T3 did run and said "real move",
#: which is an answer, not a blind spot.
_BLIND: tuple[str, ...] = ("t1_suppressed", "t2_suppressed", "t1_t2_suppressed")


def run_census(conn: psycopg.Connection[Any]) -> int:
    uniform, corpus_max = _print_identities(conn)
    unresolved_breaks = _print_break_table(conn)

    rows = _load_transitions(conn)
    recent_cut = (corpus_max - timedelta(days=RECENT_DAYS)) if corpus_max else None

    counts: Counter[str] = Counter()
    instruments: dict[str, set[int]] = {}
    recent: Counter[str] = Counter()
    per_class: Counter[tuple[str, str]] = Counter()
    unmeasurable = 0
    unmeasurable_instruments: set[int] = set()

    for instrument_id, price_date, rules, ratio, provisional, _corr, asset_class in rows:
        iid = int(instrument_id)
        if ratio is None:
            # Unmeasurable, not clean. Reported on its own line so it cannot be
            # read as either side of the census.
            unmeasurable += 1
            unmeasurable_instruments.add(iid)
            continue
        ratio = Decimal(ratio)
        # Symmetric magnitude with an inclusive bound, mirroring the classifier.
        magnitude = max(ratio, Decimal(1) / ratio)
        threshold = params_for(asset_class).magnitude_threshold
        bucket = _bucket(list(rules), magnitude >= threshold, bool(provisional))
        counts[bucket] += 1
        instruments.setdefault(bucket, set()).add(iid)
        if recent_cut is not None and price_date >= recent_cut:
            recent[bucket] += 1
        if bucket in _BLIND or bucket == "t3_minted":
            per_class[(asset_class or "NULL", bucket)] += 1

    print("=" * 78)
    print(f"CENSUS — {_fmt(len(rows))} transitions at {RULE_SET_VERSION}")
    print("=" * 78)
    print(f"  {'bucket':<20}{'rows':>9}{'instr':>8}{'last ' + str(RECENT_DAYS) + 'd':>12}   note")
    for bucket in (
        "t3_minted",
        "t1_suppressed",
        "t2_suppressed",
        "t1_t2_suppressed",
        "admitted_back",
        "deferred",
        "below_threshold",
    ):
        print(
            f"  {bucket:<20}{_fmt(counts[bucket]):>9}{_fmt(len(instruments.get(bucket, ()))):>8}"
            f"{_fmt(recent[bucket]):>12}   {_BUCKET_NOTES[bucket]}"
        )
    if unmeasurable:
        print(
            f"  {'ratio IS NULL':<20}{_fmt(unmeasurable):>9}{_fmt(len(unmeasurable_instruments)):>8}"
            f"{'-':>12}   unmeasurable; excluded from both sides"
        )
    print()

    blind_rows = sum(counts[b] for b in _BLIND)
    blind_instruments = set().union(*(instruments.get(b, set()) for b in _BLIND)) if blind_rows else set()
    blind_recent = sum(recent[b] for b in _BLIND)
    print("-" * 78)
    print("  THE BLIND SPOT — clears its class threshold, no T3, so no break row")
    print(f"    rows                {_fmt(blind_rows)}")
    print(f"    instruments         {_fmt(len(blind_instruments))}")
    print(f"    in the last {RECENT_DAYS}d    {_fmt(blind_recent)}")
    print(f"    unresolved breaks   {_fmt(unresolved_breaks)}   <- what the segment model DOES see")
    print("-" * 78)
    print()

    classes = sorted({c for c, _ in per_class})
    print("  per asset class (coverage-time value — sql/247:45 'as seen at evaluation time')")
    print(f"    {'class':<14}{'T':>5}{'t3_minted':>12}{'blind':>9}")
    for asset_class in classes:
        threshold = params_for(None if asset_class == "NULL" else asset_class).magnitude_threshold
        blind = sum(per_class[(asset_class, b)] for b in _BLIND)
        minted = _fmt(per_class[(asset_class, "t3_minted")])
        print(f"    {asset_class:<14}{float(threshold):>5.0f}{minted:>12}{_fmt(blind):>9}")
    print()

    # ---- The reconciliation arm -------------------------------------------
    print("=" * 78)
    print("RECONCILIATION")
    print("=" * 78)
    matched, t3_only, break_only = _reconcile(conn)
    reconciled = t3_only == 0 and matched == counts["t3_minted"]
    print(f"  T3 verdicts key-matched to a break row   {_fmt(matched)}")
    print(f"  T3 verdict with NO break row             {_fmt(t3_only)}   <- must be 0")
    print(f"  break row with no current T3 verdict     {_fmt(break_only)}   (expected once any is resolved)")
    print(f"  census t3_minted bucket                  {_fmt(counts['t3_minted'])}")
    print(f"  unresolved breaks (segmentation operand) {_fmt(unresolved_breaks)}")
    if reconciled:
        print("  OK — the census reproduces the stored break table.")
    else:
        print("  MISMATCH — this census does not reproduce production. The suppression")
        print("  figure above is NOT trustworthy; do not quote it. Likely causes: a")
        print("  version skew between the transition and break tables, or a T3 verdict")
        print("  whose break row was never written.")
    if not uniform:
        print("  COVERAGE NOT UNIFORM — see identities above; counts span rule-set versions.")
    print()
    return 0 if (reconciled and uniform) else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="#3046 break-minting census (read-only)")
    ap.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        # ⚠ READ-ONLY IS NOT ISOLATION. `price_quarantine_refresh` runs on a 24 h
        # cadence and rewrites all three tables per instrument; under the default
        # READ COMMITTED the identities, the break count and the transition rows
        # could each come from a different committed state, and the run would
        # report a reconciliation failure that never existed in any one snapshot.
        # REPEATABLE READ pins them to one. (Codex checkpoint 2.)
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        return run_census(conn)


if __name__ == "__main__":
    sys.exit(main())
