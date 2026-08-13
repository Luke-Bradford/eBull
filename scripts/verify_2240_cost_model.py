"""Full-population verification of the phase-5b cost model (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_cost_model.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

THE ARMS
--------
``--calibrate`` — recompute the band table from ``quotes`` and report it beside
the frozen one. Session membership comes from ``market_calendar``, never from
§5.1's ``14–19 UTC`` literal.

  ⚠⚠ THE DRIFT IS REPORTED, NOT GATED, AND THAT IS A DECISION.
  ``quotes`` holds one row per instrument and is overwritten on every refresh
  (``market_data._upsert_quote``), so a gate on "today's p75 equals the frozen
  one" fails whenever the quote job runs. A gate that fails on schedule trains
  you to skip it — the #2224 argument, applied here. What this arm DOES gate on
  is structural: every band still carries a calibration quote, every priced
  quote lands in a band, and the frozen table is total over positive prices.
  The per-band verdict line says whether the frozen model is currently
  OPTIMISTIC (charging less than today's measurement) so a real recalibration
  decision has the number in front of it.

  ⚠ It also prints a SECOND, bounding table — p95 and the sample max, with the
  freeze-ready (ROUND_CEILING, 0.001 pp) form of each. #2598 scope 5 asks for
  the banded model to become the declared execution-side conservative bound,
  and a p75 cannot be one: a quarter of its population exceeds it by
  construction. Reported, never adopted — adopting a column is a new
  ``COST_MODEL_ID``. See ``_report_bounding_statistics``.

``--positions`` — cost every position S-1 and S-3 produce over the §4.0
validated universe, through the real path (``s1_signals`` / ``s3_signals`` →
``signal_ledger.resolve_fills`` → ``build_positions`` → ``cost_positions``), and
assert four properties plus report the census.

  P1  **Every position is charged.** Net entry strictly exceeds the gross fill,
      for every position, on the full population — acceptance C2(a)'s *"costs
      are non-zero on every position"*.
  P2  **Costs never improve a trade.** Net return strictly below gross, every
      priced position. This is the one that would catch a sign error in either
      side of the adjustment.
  P3  **The band is the ENTRY band.** Every costed row's ``half_spread`` equals
      the half-spread of its own entry fill price — §5.1's *"fixed for the life
      of the position"*, asserted against the SOURCE PRICE rather than against a
      second implementation of the lookup.
  P4  **Conservation.** Every position produces exactly one costed row, and each
      is either priced or carries a reason.

  ⚠ WHAT IS NOT COVERED, STATED SO THE GAP IS A DECISION. Only S-1 and S-3 run
  here, for phase 5a's reason: C2 (``level``) needs the resolver over the whole
  corpus and C4 (``calendar``) needs S-2's panel resident at once. So the
  ``ambiguous_close`` costing path — reachable only through an outcome row —
  appears in this arm never, and is covered by ``tests/test_position_costing.py``.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from datetime import time as clock_time
from decimal import ROUND_CEILING, Decimal
from zoneinfo import ZoneInfo

import psycopg

from app.config import settings
from app.services.cost_model import (
    BANDS,
    CALIBRATION_QUOTES_IN_SESSION,
    CALIBRATION_RUN_DATE,
    CARRY_BPS,
    CARRY_UNMODELLED,
    COST_MODEL_ID,
    FX_BPS,
    FX_UNMODELLED,
    SESSION_RULE,
    UNKNOWN_NOMINAL_PRICE_BAND,
    _check_bands_are_total,
    band_for,
)
from app.services.market_calendar import us_market_status
from app.services.position_builder import Window, build_positions
from app.services.position_costing import CostedPosition, cost_positions
from app.services.research_price_structure_store import load_masked_series
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe

# ⚠ REUSED, not re-derived. `scripts` is a package and phase 5a already built
# the corpus→positions path; a second copy of it here would be a second place
# for the fill rule to drift. Only the costing layer is new.
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    EVALUATION_END,
    HOLDOUT_BOUNDARY,
    UNIVERSE,
    _fills,
    _quantile,
    _stamped_versions,
    _to_series,
)

#: The NYSE regular session in ``America/New_York``. ⚠ 13:00 on a half day —
#: ``market_calendar`` supplies which days those are, which is the whole reason
#: the literal UTC window §5.1 shipped is not used here.
NY = ZoneInfo("America/New_York")
SESSION_OPEN = clock_time(9, 30)
SESSION_CLOSE = clock_time(16, 0)
HALF_DAY_CLOSE = clock_time(13, 0)

#: Return buckets for the census, in HUNDREDTHS OF A PERCENT (1 bp).
#: ⚠ A Counter over integer buckets, never a list: S-1 produces millions of
#: positions and a list of that many Decimals is gigabytes for a statistic whose
#: support is small. Phase 5a's `holds` Counter, applied to a continuous value.
_BP = Decimal("0.01")

#: ⚠ NO JOIN TO ``instruments``. An earlier draft joined it to reach
#: ``exchanges.asset_class``; the calibration now selects on the §4.0 validated
#: universe's own id list, which already carries that predicate, so the join was
#: dead weight over 1,500 rows and — worse — read as a filter that was doing
#: something. ``quotes.instrument_id`` is an FK to ``instruments``, so it could
#: never have dropped a row.
_QUOTES_SQL = """
    SELECT q.instrument_id, q.quoted_at, q.last, q.spread_pct
    FROM quotes q
    WHERE q.instrument_id = ANY(%(ids)s)
"""


def _in_session(quoted_at: datetime) -> bool:
    """Whether a capture stamp lands inside a real NYSE regular session.

    ⚠ THE CALENDAR, NOT A UTC LITERAL. §5.1 shipped ``14–19 UTC`` and flagged it
    as wrong in both DST regimes — under EDT it misses the opening 30 minutes
    (the widest-spread part of the day, so the omission biases the model
    OPTIMISTIC), and under EST it admits the pre-open and drops the closing
    hour. Resolving the local session removes both errors and removes the DST
    dependence entirely.
    """
    local = quoted_at.astimezone(NY)
    status = us_market_status(local.date())
    if status == "closed":
        return False
    close = HALF_DAY_CLOSE if status == "half_day" else SESSION_CLOSE
    return SESSION_OPEN <= local.time() < close


def _percentile_disc(values: Sequence[Decimal], *, percentile: int) -> Decimal:
    """A discrete percentile — an OBSERVED spread, never an interpolation.

    ⚠ Stated because it is a choice with no published rule, so it is fixed by
    construction and frozen with the model: ``percentile_disc`` semantics, the
    smallest observed value whose cumulative share reaches ``percentile/100``.
    An interpolated percentile is a number no instrument was ever quoted at, and
    the model claims to charge a spread somebody actually saw.

    ⚠ INTEGER ARITHMETIC, not ``q * n`` in float. ``ceil(n × p / 100)`` is exact
    here; the float form is off by one wherever ``q * n`` lands a ulp below an
    integer, and the value it then selects is the neighbouring observation
    rather than an approximation of the right one. ``percentile=100`` is the
    sample maximum, which is the same rule at its limit rather than a special
    case.
    """
    if not 1 <= percentile <= 100:
        raise ValueError(f"percentile must be in 1..100, got {percentile}")
    if not values:
        # ⚠ Explicit, because the index arithmetic below lands on -1 for an
        # empty sequence and would return the LAST element of nothing — i.e.
        # IndexError, from a function whose job is to answer a question about a
        # sample that does not exist. Callers band their quotes first and must
        # report "no sample" themselves.
        raise ValueError(f"cannot take the p{percentile} of an empty sample")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, -(-len(ordered) * percentile // 100) - 1))
    return ordered[index]


#: The freeze quantum and its direction, restated from ``cost_model``'s BANDS
#: note so a recalibration candidate is printed in the form it would be frozen
#: in: 0.001 percentage points (0.1 bp), ROUND_CEILING, so the frozen model is
#: never CHEAPER than the measurement it came from.
_FREEZE_QUANTUM = Decimal("0.001")


def _freeze_ready(value: Decimal) -> Decimal:
    """The value as it would enter ``BANDS`` — quantised AWAY from zero cost."""
    return value.quantize(_FREEZE_QUANTUM, rounding=ROUND_CEILING)


def _report_bounding_statistics(by_band: dict[str, list[Decimal]], *, top_hour: int | None, top_count: int) -> None:
    """Print the BOUNDING statistics beside the frozen p75 (#2598 scope 5, step 1).

    ⚠⚠ WHY THIS TABLE EXISTS, IN ONE LINE: **a p75 is not a bound.** #2598 scope
    5 asks for the banded model to become *"the declared execution-side
    conservative bound"* for unleveraged long stock, and a 75th percentile is
    exceeded by a quarter of its own population BY CONSTRUCTION. That is a
    property of the statistic, not of the sample, so no amount of extra
    calibration data repairs it — only a different statistic does. This arm
    computes the two candidates the ticket names (p95, or the sample max) over
    the same population, the same session rule and the same freeze discipline,
    so the recalibration decision has its numbers in front of it.

    ⚠ NOTHING IS FROZEN HERE. Adopting a column ships a NEW ``COST_MODEL_ID``
    (``cost_model``'s own rule — a change to what is charged is a new model, not
    a silent improvement), which moves every strategy version and supersedes
    every stored result under the current id. The freeze-ready column is printed
    so that decision is a copy, not a hand-quantisation.

    ⚠ THE MAX IS A SAMPLE MAX. ``quotes`` holds one row per instrument,
    overwritten on every refresh, so this is the widest spread in ONE snapshot
    of a fraction of the universe — an upper bound on what was observed, never
    on what can occur. The caveat line prints the n and the capture-hour
    concentration it rests on, because that is what decides how much of the
    trading day it has ever seen.
    """
    print("\n  ⚠ p75 is not a bound — a quarter of the population exceeds it by construction (#2598 scope 5).")
    print("  band          n   p75 today   p95 today   max today   p95 frozen-ready   max frozen-ready   p95/frozen")
    for band in BANDS:
        values = by_band[band.label]
        if not values:
            print(f"  {band.label:<10} {0:>4}   {'—':>9}   {'—':>9}   {'—':>9}   {'—':>16}   {'—':>16}   {'—':>10}")
            continue
        p75 = _percentile_disc(values, percentile=75)
        p95 = _percentile_disc(values, percentile=95)
        widest = _percentile_disc(values, percentile=100)
        uplift = (p95 / band.p75_spread_pct).quantize(Decimal("0.01"))
        print(
            f"  {band.label:<10} {len(values):>4}   {p75:>9}   {p95:>9}   {widest:>9}   "
            f"{_freeze_ready(p95):>16}   {_freeze_ready(widest):>16}   {uplift:>9}x"
        )
    priced = sum(len(values) for values in by_band.values())
    hour = f"UTC {top_hour}" if top_hour is not None else "—"
    print(
        f"  ⚠ sample max over {priced:,} priced in-session quotes, {top_count:,} of them at {hour} — "
        "one hour of one snapshot, not a population maximum"
    )


def calibrate() -> int:
    """Recompute the band table from today's ``quotes``; report the drift."""
    started = time.monotonic()
    print(f"\n[calibrate] cost model {COST_MODEL_ID}   frozen {CALIBRATION_RUN_DATE}", flush=True)
    print(f"            session rule: {SESSION_RULE}", flush=True)
    _check_bands_are_total(BANDS)

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        rows = conn.execute(_QUOTES_SQL, {"ids": list(universe)}).fetchall()
    print(f"  validated universe        {len(universe):>7,} instruments (US stocks ex-ETF, §4.0)")
    print(f"  quotes in it              {len(rows):>7,}")

    in_session = [row for row in rows if _in_session(row[1])]
    print(
        f"  in-session by the calendar{len(in_session):>7,}   "
        f"(frozen calibration used {CALIBRATION_QUOTES_IN_SESSION:,})"
    )

    # ⚠ The literal is measured BESIDE the calendar rule rather than replaced
    # silently: on the frozen snapshot the two selected the same rows, and a
    # future refresh that makes them diverge is exactly what this line reveals.
    literal = [row for row in rows if 14 <= row[1].astimezone(ZoneInfo("UTC")).hour <= 19]
    calendar_ids = {id(row) for row in in_session}
    literal_ids = {id(row) for row in literal}
    print(
        f"  §5.1's 14–19 UTC literal  {len(literal):>7,}   symmetric difference vs the calendar: "
        f"{len(calendar_ids ^ literal_ids):,}"
    )

    hours = Counter(row[1].astimezone(ZoneInfo("UTC")).hour for row in in_session)
    top_hour, top_count = hours.most_common(1)[0] if hours else (None, 0)
    print(f"  capture-hour concentration  UTC {top_hour}: {top_count:,} of {len(in_session):,}")
    ny_dates = {row[1].astimezone(NY).date() for row in in_session}
    span = f"{min(ny_dates)} … {max(ny_dates)}" if ny_dates else "—"
    print(f"  distinct NY capture dates {len(ny_dates):>7,}   {span}")

    problems: list[str] = []
    by_band: dict[str, list[Decimal]] = {band.label: [] for band in BANDS}
    unpriced = 0
    for _instrument_id, _quoted_at, last, spread_pct in in_session:
        if last is None or last <= 0 or spread_pct is None:
            unpriced += 1
            continue
        by_band[band_for(last).label].append(spread_pct)
    print(f"  in-session rows with no usable price/spread  {unpriced:,}")

    print("\n  band        n today   p75 today    p75 frozen   half frozen   verdict")
    for band in BANDS:
        values = by_band[band.label]
        if not values:
            problems.append(
                f"band {band.label} has no in-session calibration quote today — the frozen "
                f"sample_size={band.sample_size} rests on a population that is no longer observable"
            )
            print(
                f"  {band.label:<10} {0:>7}   {'—':>10}   {band.p75_spread_pct:>10}   "
                f"{band.half_spread_pct:>11}   NO SAMPLE"
            )
            continue
        today = _percentile_disc(values, percentile=75)
        verdict = "OPTIMISTIC" if band.p75_spread_pct < today else "conservative"
        print(
            f"  {band.label:<10} {len(values):>7}   {today:>10}   {band.p75_spread_pct:>10}   "
            f"{band.half_spread_pct:>11}   {verdict}"
        )

    _report_bounding_statistics(by_band, top_hour=top_hour, top_count=top_count)

    print(
        f"\n  carry_bps {CARRY_BPS}   fx_bps {FX_BPS}   "
        f"carry_unmodelled {CARRY_UNMODELLED}   fx_unmodelled {FX_UNMODELLED}"
    )
    print(f"  problems: {len(problems)}")
    for problem in problems:
        print(f"    {problem}")
    print(f"  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    return 1 if problems else 0


class _CostTally:
    """One strategy's costed census."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.positions = 0
        self.priced = 0
        self.uncosted: Counter[str] = Counter()
        self.basis: Counter[str] = Counter()
        self.entry_bands: Counter[str] = Counter()
        self.crossings = 0
        self.gross_wins = 0
        self.net_wins = 0
        self.closed_priced = 0
        #: Return distributions, in 1 bp buckets. See ``_BP``.
        self.gross_bp: Counter[int] = Counter()
        self.net_bp: Counter[int] = Counter()
        self.cost_bp: Counter[int] = Counter()
        self.in_sample = 0
        self.hold_out = 0

    def absorb(self, rows: Sequence[CostedPosition]) -> list[str]:
        problems: list[str] = []
        for row in rows:
            position = row.position
            self.positions += 1
            self.entry_bands[row.band_label] += 1

            # P1 — every position is charged.
            if row.entry_price_net <= position.entry_fill_price:
                problems.append(
                    f"{self.label}/{position.instrument_id}: P1 uncharged entry — net {row.entry_price_net} "
                    f"does not exceed gross {position.entry_fill_price}"
                )
            # P3 — adjusted research prices receive the declared adverse band;
            # they never pretend their numeric value is a nominal price.
            expected = UNKNOWN_NOMINAL_PRICE_BAND.half_spread
            if row.price_basis != "split_adjusted" or row.half_spread != expected:
                problems.append(
                    f"{self.label}/{position.instrument_id}: P3 basis — {(row.price_basis, row.half_spread)!r} against "
                    f"split_adjusted/{expected} for an entry at {position.entry_fill_price}"
                )
            # P4 — priced or reasoned, never both and never neither. (The row's
            # own __post_init__ already refuses the mixed state; this counts it.)
            if (row.exit_basis is None) == (row.uncosted_reason is None):
                problems.append(f"{self.label}/{position.instrument_id}: P4 conservation — neither/both priced")

            if row.uncosted_reason is not None:
                self.uncosted[row.uncosted_reason] += 1
            else:
                assert row.exit_basis is not None
                self.basis[row.exit_basis] += 1
                self.priced += 1
                assert row.gross_return_pct is not None and row.net_return_pct is not None
                # P2 — costs never improve a trade.
                if row.net_return_pct >= row.gross_return_pct:
                    problems.append(
                        f"{self.label}/{position.instrument_id}: P2 net {row.net_return_pct} is not below gross "
                        f"{row.gross_return_pct}"
                    )
                assert row.exit_price_gross is not None
                if row.price_basis == "as_traded" and band_for(row.exit_price_gross).label != row.band_label:
                    self.crossings += 1
                if row.exit_basis == "close":
                    self.closed_priced += 1
                    if row.gross_return_pct > 0:
                        self.gross_wins += 1
                    if row.net_return_pct > 0:
                        self.net_wins += 1
                    self.gross_bp[int(row.gross_return_pct / _BP)] += 1
                    self.net_bp[int(row.net_return_pct / _BP)] += 1
                    # ⚠⚠ `gross − net` SCALES WITH THE WINNER, and its maximum
                    # looks like a defect if that is not said. Algebraically
                    # `gross − net = (exit / entry) × 100 × 2h/(1+h)`, so a
                    # 270-fold gain carries a "cost" of ~390 percentage points
                    # while a flat trade carries ~2h. The measured max on this
                    # corpus is 39,064 bp for S-1 and it is arithmetic, not a
                    # mis-charge: the half-spread is a fraction of the EXIT
                    # value expressed against the ENTRY basis. Read the median
                    # (53 bp), not the max.
                    self.cost_bp[int((row.gross_return_pct - row.net_return_pct) / _BP)] += 1

            if position.entry_fill_bar_date < HOLDOUT_BOUNDARY:
                self.in_sample += 1
            else:
                self.hold_out += 1
        return problems

    def report(self) -> None:
        print(f"\n  [{self.label}]")
        print(f"      positions              {self.positions:>12,}")
        print(f"      priced                 {self.priced:>12,}")
        for basis, count in self.basis.most_common():
            print(f"        exit basis {basis:<10} {count:>10,}")
        for reason, count in self.uncosted.most_common():
            print(f"        uncosted {reason:<12} {count:>10,}")
        print("      nominal band crossings          n/a   (split-adjusted price basis)")
        for label, count in self.entry_bands.most_common():
            share = 100.0 * count / self.positions if self.positions else 0.0
            print(f"        entry band {label:<10} {count:>10,}   {share:6.3f}%")
        if self.closed_priced:
            gross_rate = 100.0 * self.gross_wins / self.closed_priced
            net_rate = 100.0 * self.net_wins / self.closed_priced
            print(f"      realised closes        {self.closed_priced:>12,}")
            print(f"        win rate GROSS       {gross_rate:>12.3f}%")
            print(f"        win rate NET         {net_rate:>12.3f}%   ({net_rate - gross_rate:+.3f} points)")
            print(
                f"        gross return bp      p25 {_quantile(self.gross_bp, 0.25)} · median "
                f"{_quantile(self.gross_bp, 0.5)} · p75 {_quantile(self.gross_bp, 0.75)}"
            )
            print(
                f"        net return bp        p25 {_quantile(self.net_bp, 0.25)} · median "
                f"{_quantile(self.net_bp, 0.5)} · p75 {_quantile(self.net_bp, 0.75)}"
            )
            print(
                f"        cost charged bp      p25 {_quantile(self.cost_bp, 0.25)} · median "
                f"{_quantile(self.cost_bp, 0.5)} · p75 {_quantile(self.cost_bp, 0.75)} · max "
                f"{max(self.cost_bp)}"
            )
        print(f"      in-sample / hold-out   {self.in_sample:>12,} / {self.hold_out:,}   (boundary {HOLDOUT_BOUNDARY})")


def positions(*, limit: int | None) -> int:
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    print(f"\n[positions] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"            {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"            builder {builder_version}", flush=True)
    print(
        f"            cost model {COST_MODEL_ID}   carry_unmodelled {CARRY_UNMODELLED}   fx_unmodelled {FX_UNMODELLED}",
        flush=True,
    )
    window = Window(start=date(1900, 1, 1), end=EVALUATION_END)
    print(f"            window {window.start} … {window.end}   (the whole corpus; nothing purged)", flush=True)

    tallies = {"S-1": _CostTally("S-1"), "S-3": _CostTally("S-3")}
    problems: list[str] = []
    empty = 0

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        print(f"  validated universe {len(universe)} instruments (US stocks ex-ETF, §4.0)", flush=True)
        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        instruments = {int(row[0]) for row in pairs}
        if len(instruments) != len(pairs):
            print(f"  *** {len(pairs) - len(instruments)} instruments carry more than one research series — refusing")
            return 1
        print(f"  research series in it {len(pairs)}", flush=True)
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)

        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            series, _masked_opens = _to_series(masked.bars)
            for label, identity, signals, regime, strategy_id, version in (
                (
                    "S-1",
                    s1_identity,
                    s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S1_REGIME,
                    S1_STRATEGY_ID,
                    s1_version,
                ),
                (
                    "S-3",
                    s3_identity,
                    s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S3_REGIME,
                    S3_STRATEGY_ID,
                    s3_version,
                ),
            ):
                rows = resolve_fills(
                    signals,
                    series=series,
                    identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
                    instrument_id=int(instrument_id),
                )
                entries, exits = _fills(rows, int(instrument_id))
                built = build_positions(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    entries=entries,
                    exits=exits,
                    outcomes=[],
                    outcome_pin=None,
                    series={int(instrument_id): series},
                    regime=regime,
                    window=window,
                )
                costed = cost_positions(built.positions, price_basis="split_adjusted")
                # P4 — conservation across the layer boundary.
                if len(costed) != len(built.positions):
                    problems.append(
                        f"{label}/{instrument_id}: P4 conservation — {len(built.positions)} positions produced "
                        f"{len(costed)} costed rows"
                    )
                problems.extend(tallies[label].absorb(costed))
            if n % 250 == 0:
                print(
                    f"  {n}/{len(pairs)} series, {len(problems)} problems ({time.monotonic() - started:.0f}s)",
                    flush=True,
                )

    print(f"\n  series with bars  {len(pairs) - empty}   (fail-closed empties: {empty})")
    for tally in tallies.values():
        tally.report()
    print(f"\n  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    if len(problems) > 20:
        print(f"    … and {len(problems) - 20} more")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    # ⚠ Re-checked AFTER the sweep as well as before — phase 5a's reason: a
    # probe harness that mutated and restored a source file mid-run would pass
    # an entry check alone.
    _stamped_versions()
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibrate", action="store_true", help="recompute the band table and report the drift")
    parser.add_argument("--positions", action="store_true", help="cost every S-1/S-3 position; assert P1-P4")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.calibrate or args.positions or args.all):
        parser.error("pick at least one arm: --calibrate, --positions or --all")
    status = 0
    if args.calibrate or args.all:
        status |= calibrate()
    if args.positions or args.all:
        status |= positions(limit=args.limit)
    return status


if __name__ == "__main__":
    sys.exit(main())
