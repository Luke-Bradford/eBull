"""Full-population verification of phase-5a position construction (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_position_builder.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

WHY A FULL-POPULATION ARM EXISTS FOR A PURE FUNCTION
----------------------------------------------------
``build_positions`` reads no database, so a table test can pin every rule. What
a table test cannot produce is the SHAPE of the real corpus: spec §3.1 says
entries are STATES and predicts that a naive "one position per fired entry"
multiplies every statistic by the length of a trend. That prediction is a
measurement, and the ``superseded_open_position`` share below is it — on 5,266
series, not on a fixture.

⚠ The ledgers hold ZERO rows (spec M10) and phase 5b will move all four
strategy versions, so storing signals now would be storing rows nobody can use.
The signals here are therefore GENERATED in memory through the real path —
``s1_signals`` / ``s3_signals`` → ``signal_ledger.resolve_fills`` — and thrown
away. The arms measure the builder against the writer's own output, which is
what the stored path will hand it.

THE ARMS
--------
``--invariants`` — four properties asserted over every position built from the
§4.0 validated universe. ⚠ They are checked against the SOURCE DATA, not
against a second implementation of the same algorithm: a reference that
re-derives the rule agrees with a shared misreading (prevention log, #2240 S-3).

  I1  **Non-overlap** — one position per instrument at a time (§3.1). Consecutive
      positions never straddle, and an open position is always the last.
  I2  **Suppression is justified** — every in-window fired entry that produced no
      position falls strictly inside some position's hold. This is I1's converse
      and is the one that catches OVER-suppression, which I1 alone cannot see.
  I3  **Conservation** — in-window fired entries == positions + superseded.
  I4  **Close provenance and the declared bound** — every close is a stored
      value, per spec §2.1's equality: a ``signal_pair`` close equals some exit
      row's ``(fill_bar_date, fill_price)`` exactly, a ``max_hold`` close equals
      the OPEN of ``entry index + max_hold_bars``, and every entry price equals
      the open of its own fill bar. ⚠ Plus ``bars_held <= max_hold_bars``
      wherever one is declared — the property that would have caught the
      ceiling defect Codex found, and that I1-I3 are all silent on.

``--census`` — the distribution, reported rather than asserted: the collapse
ratio, close sources, open-position reasons, holding periods, and the realised
in-sample / hold-out trade split at the spec's frozen boundary (§5.2).

⚠ WHAT IS NOT COVERED HERE, STATED SO THE GAP IS A DECISION. Only C1
(``signal_pair``) and C3 (``max_hold``) are exercised on the full population.
C2 (``level``) needs the resolver run over the corpus and C4 (``calendar``)
needs S-2's whole panel resident at once; both are a different run's cost, and
both are covered by ``tests/test_position_builder.py`` on fixtures. The
``ambiguous`` close source is reachable only through C2 and so appears here
never.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from collections.abc import Sequence
from datetime import date

import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.position_builder import RULE_SET_VERSION as BUILDER_RULE_SET_VERSION
from app.services.position_builder import (
    EntryFill,
    ExitFill,
    ExitRegime,
    Position,
    Window,
    build_positions,
)
from app.services.price_structure import StructureBar
from app.services.research_price_structure_store import load_masked_series
from app.services.signal_ledger import LedgerRow, resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import MAX_HOLD_BARS as S3_MAX_HOLD_BARS
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe
from app.services.technical_analysis import OHLCVRow

#: The research corpus is survivor-only (#2284) and every figure below inherits
#: that label (#2288).
UNIVERSE = "survivor_only"

#: ⚠ IMPORTED, never restated. Stage 5b froze the model, so the identity hash
#: records a real cost basis; a local literal here would be a second source of
#: truth for a value that is hashed into every stored strategy version.
#: ⚠ Prices in this run are still GROSS — costing is `position_costing`, and
#: this constant only names the model the identity declares.

#: Spec §5.2's frozen bar-weighted 75/25 boundary and evaluation end. ⚠ Frozen
#: LITERALS, deliberately: recomputing them walks the split forward as the
#: corpus grows and silently re-admits hold-out data into training.
HOLDOUT_BOUNDARY = date(2021, 6, 29)
EVALUATION_END = date(2026, 7, 8)

#: ⚠ Read at IMPORT, so it is the hash of the code this process actually runs.
#: The probe harness mutates a source file, runs a test and restores it, so a
#: concurrent run stamps clean figures with an INJECTED hash and a start-vs-end
#: comparison does NOT catch it. The check has to bracket the read itself
#: (prevention log, #2240 S-1).
_BUILDER_VERSION_AT_IMPORT = BUILDER_RULE_SET_VERSION

_S1_REGIME = ExitRegime(signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=None)
_S3_REGIME = ExitRegime(signal_pair=True, level_based=False, max_hold_bars=S3_MAX_HOLD_BARS, rebalance_dates=None)


def _stamped_versions() -> tuple[str, str, str]:
    """The three versions this run reports under, with the builder's re-checked."""
    from app.services.position_builder import _code_hash  # noqa: PLC0415 — read late on purpose

    live = f"position-builder-v1+{_code_hash()}"
    if live != _BUILDER_VERSION_AT_IMPORT:
        raise RuntimeError(
            f"position_builder source moved since import (expected {_BUILDER_VERSION_AT_IMPORT}, "
            f"now {live}) — refusing to report figures stamped with a version that is not the one that ran"
        )
    return (
        s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version,
        s3_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version,
        live,
    )


def _to_series(bars: Sequence[StructureBar]) -> tuple[BarSeries, int]:
    """The masked bars, with any NON-POSITIVE open masked too. Returns the count.

    ⚠⚠ SINCE #2354 THIS IS A CROSS-CHECK, NOT THE FIX. ``load_masked_series``
    now masks a non-positive open itself, so through that loader the count this
    returns is **0** and the print below is what says so. It is kept rather than
    deleted for the reason the fix has two layers at all: this function takes
    ``Sequence[StructureBar]`` and does not know which loader produced them, and
    a raw read of ``research_price_daily`` still carries the zero.

    Measured on the dev corpus 2026-08-08, reproduced by::

        select count(*), count(distinct series_id) from research_price_daily where open <= 0;
        select q.rules, q.range_usable, q.return_usable, count(*)
          from research_price_daily d
          left join research_bar_quarantine q
                 on q.series_id = d.series_id and q.bar_date = d.bar_date
         where d.open <= 0 group by 1, 2, 3;

    **16 bars across 9 series carry ``open = 0``**, none negative, and all 16
    are ``rules = ['B1']`` with `range_usable = false` AND `return_usable =
    false` — the quarantine had condemned every one of them on both axes while
    the loader handed its open to the fill path.

    A bar reaching here still masked turns into ``unusable_fill_price`` inside
    ``signal_ledger.resolve_fills`` — the tenth reason code, which is the split
    that branch's docstring pre-registered as *"if the measured count ever
    leaves zero, split it"*.
    """
    masked_opens = 0
    rows: list[OHLCVRow] = []
    for bar in bars:
        bar_open = bar.open
        if bar_open is not None and bar_open <= 0:
            bar_open = None
            masked_opens += 1
        rows.append(
            {"open": bar_open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume}  # type: ignore[typeddict-item]  # noqa: E501
        )
    return BarSeries(dates=tuple(b.bar_date for b in bars), rows=tuple(rows)), masked_opens


def _fills(rows: list[LedgerRow], instrument_id: int) -> tuple[list[EntryFill], list[ExitFill]]:
    """Project the writer's own output onto the builder's inputs.

    ⚠ ``verdict == "fired"`` alone, exactly as ``outcome_ledger.select_pending_fills``
    filters: a ``not_fired`` or ``not_evaluable`` row has no fill, and the
    builder must never see one.
    """
    entries: list[EntryFill] = []
    exits: list[ExitFill] = []
    for index, row in enumerate(rows):
        if row.verdict != "fired":
            continue
        assert row.fill_bar_date is not None and row.fill_price is not None
        if row.signal_kind == "entry":
            entries.append(
                EntryFill(
                    # ⚠ A stand-in for the BIGSERIAL the table would assign;
                    # nothing is stored, so the id only has to be unique within
                    # this instrument's batch.
                    signal_id=index,
                    instrument_id=instrument_id,
                    signal_bar_date=row.signal_bar_date,
                    fill_bar_date=row.fill_bar_date,
                    fill_price=row.fill_price,
                )
            )
        else:
            exits.append(
                ExitFill(
                    instrument_id=instrument_id,
                    signal_bar_date=row.signal_bar_date,
                    fill_bar_date=row.fill_bar_date,
                    fill_price=row.fill_price,
                )
            )
    return entries, exits


def _check_invariants(
    *,
    label: str,
    instrument_id: int,
    series: BarSeries,
    entries: list[EntryFill],
    exits: list[ExitFill],
    positions: tuple[Position, ...],
    superseded: int,
    window: Window,
    max_hold_bars: int | None,
) -> list[str]:
    """I1-I4 for one instrument. Returns the violations, named."""
    problems: list[str] = []
    index_of = {when: i for i, when in enumerate(series.dates)}
    in_window = [entry for entry in entries if window.contains(entry.fill_bar_date)]

    # I3 — conservation.
    if len(in_window) != len(positions) + superseded:
        problems.append(
            f"{label}/{instrument_id}: I3 conservation — {len(in_window)} in-window entries != "
            f"{len(positions)} positions + {superseded} superseded"
        )

    # I1 — non-overlap, and an open position is terminal.
    for earlier, later in zip(positions, positions[1:], strict=False):
        if earlier.close_bar_date is None:
            problems.append(
                f"{label}/{instrument_id}: I1 non-overlap — an open position at "
                f"{earlier.entry_fill_bar_date} is not the last"
            )
        elif earlier.close_bar_date > later.entry_fill_bar_date:
            problems.append(
                f"{label}/{instrument_id}: I1 non-overlap — hold closing {earlier.close_bar_date} straddles the "
                f"entry filling {later.entry_fill_bar_date}"
            )

    # I2 — every suppressed entry falls strictly inside some hold. The converse
    # of I1, and the half that catches OVER-suppression.
    #
    # ⚠ A LINEAR MERGE, not a scan per entry. Both lists are already in fill
    # order, and the obvious `any(...)` is quadratic: measured on 60 series it
    # was tolerable and it does not scale — one 16,236-bar instrument carries
    # thousands of positions and tens of thousands of suppressed entries.
    kept = {position.entry_signal_id for position in positions}
    cursor = 0
    for entry in in_window:
        while cursor < len(positions):
            closed_at = positions[cursor].close_bar_date
            if closed_at is None or closed_at > entry.fill_bar_date:
                break
            cursor += 1
        if entry.signal_id in kept:
            continue
        covered = cursor < len(positions) and positions[cursor].entry_fill_bar_date <= entry.fill_bar_date
        if not covered:
            problems.append(
                f"{label}/{instrument_id}: I2 suppression — the entry filling {entry.fill_bar_date} produced no "
                "position and sits inside no hold"
            )

    # I4 — close provenance, and §2.1's entry equality.
    exit_pairs = {(fill.fill_bar_date, fill.fill_price) for fill in exits}
    for position in positions:
        entry_index = index_of[position.entry_fill_bar_date]
        if series.rows[entry_index].get("open") != position.entry_fill_price:
            problems.append(
                f"{label}/{instrument_id}: I4 entry price — position at {position.entry_fill_bar_date} priced "
                f"{position.entry_fill_price}, series open {series.rows[entry_index].get('open')}"
            )
        if position.entry_fill_bar_date <= position.entry_signal_bar_date:
            problems.append(
                f"{label}/{instrument_id}: I4 fill order — {position.entry_fill_bar_date} is not after its signal"
            )
        if position.close_source is None:
            continue
        assert position.close_bar_date is not None and position.bars_held is not None
        close_index = index_of[position.close_bar_date]
        if position.bars_held != close_index - entry_index:
            problems.append(
                f"{label}/{instrument_id}: I4 bars_held — stored {position.bars_held}, series gives "
                f"{close_index - entry_index}"
            )
        # ⚠ THE DECLARED HOLDING BOUND, asserted rather than assumed. Added
        # after Codex found a hold running past its own `max_hold_bars` when
        # the expiry bar's open was masked (prevention log, #2240 5a): I1-I4
        # all passed on that build, because overlap, conservation and
        # provenance say nothing about how LONG a position ran. A property no
        # assertion states is a property the full population cannot falsify.
        if max_hold_bars is not None and position.bars_held > max_hold_bars:
            problems.append(
                f"{label}/{instrument_id}: I4 holding bound — {position.bars_held} bars held against a declared "
                f"maximum of {max_hold_bars}, closing {position.close_bar_date} via {position.close_source}"
            )
        if position.close_source == "signal_pair":
            if (position.close_bar_date, position.close_price) not in exit_pairs:
                problems.append(
                    f"{label}/{instrument_id}: I4 provenance — signal_pair close "
                    f"({position.close_bar_date}, {position.close_price}) is not a stored exit fill"
                )
            if position.close_bar_date <= position.entry_fill_bar_date:
                problems.append(
                    f"{label}/{instrument_id}: I4 same-bar — a signal_pair close on {position.close_bar_date} "
                    "is not strictly after the entry fill"
                )
        elif position.close_source == "max_hold":
            assert max_hold_bars is not None
            expected = entry_index + max_hold_bars
            if close_index != expected or series.rows[expected].get("open") != position.close_price:
                problems.append(
                    f"{label}/{instrument_id}: I4 provenance — max_hold close at index {close_index} priced "
                    f"{position.close_price}; entry {entry_index} + {max_hold_bars} is index {expected} at open "
                    f"{series.rows[expected].get('open') if expected < len(series) else 'past end'}"
                )
        else:
            problems.append(f"{label}/{instrument_id}: unexpected close source {position.close_source!r} for this arm")
    return problems


def _quantile(counts: Counter[int], q: float) -> int:
    """The ``q``-quantile of a value->frequency map, by nearest rank.

    ⚠ Nearest rank, stated rather than left implicit: it returns an OBSERVED
    hold length, never an interpolated one. A "median hold of 1.5 bars" is not a
    thing this corpus can produce.
    """
    total = sum(counts.values())
    target = max(1, min(total, int(-(-total * q // 1))))
    seen = 0
    for value in sorted(counts):
        seen += counts[value]
        if seen >= target:
            return value
    raise AssertionError("unreachable: the loop covers every element")


class _Tally:
    """One strategy's running census."""

    def __init__(self, label: str, max_hold_bars: int | None) -> None:
        self.label = label
        self.max_hold_bars = max_hold_bars
        self.fired_entries = 0
        self.fired_exits = 0
        self.positions = 0
        self.superseded = 0
        self.marks_unavailable = 0
        self.close_bar_unfillable = 0
        self.sources: Counter[str] = Counter()
        self.open_reasons: Counter[str] = Counter()
        # ⚠ A Counter, not a list: the full sweep builds millions of
        # positions and a list of that many ints is hundreds of MB for a
        # statistic whose support is small.
        self.holds: Counter[int] = Counter()
        self.in_sample = 0
        self.hold_out = 0
        self.instruments_with_positions = 0

    def absorb(self, entries: int, exits: int, built, problems: list[str]) -> list[str]:
        self.fired_entries += entries
        self.fired_exits += exits
        self.positions += len(built.positions)
        self.superseded += built.superseded_open_position
        self.marks_unavailable += built.marks_unavailable
        self.close_bar_unfillable += built.close_bar_unfillable
        if built.positions:
            self.instruments_with_positions += 1
        for position in built.positions:
            if position.close_source is not None:
                self.sources[position.close_source] += 1
                assert position.bars_held is not None
                self.holds[position.bars_held] += 1
            else:
                assert position.open_reason is not None
                self.open_reasons[position.open_reason] += 1
            if position.entry_fill_bar_date < HOLDOUT_BOUNDARY:
                self.in_sample += 1
            else:
                self.hold_out += 1
        return problems

    def report(self) -> None:
        print(f"\n  [{self.label}]  max_hold_bars={self.max_hold_bars}")
        print(f"      fired entries        {self.fired_entries:>12,}")
        print(f"      fired exits          {self.fired_exits:>12,}")
        print(f"      positions            {self.positions:>12,}   ({self.instruments_with_positions:,} instruments)")
        share = 100.0 * self.superseded / self.fired_entries if self.fired_entries else 0.0
        print(f"      superseded_open_position {self.superseded:>8,}   {share:6.3f}% of fired entries")
        if self.positions:
            print(
                f"      collapse ratio       {self.fired_entries / self.positions:>12.2f}   fired entries per position"
            )
        for source, count in self.sources.most_common():
            print(f"        close {source:<14} {count:>10,}")
        for reason, count in self.open_reasons.most_common():
            print(f"        open  {reason:<14} {count:>10,}")
        print(f"      marks unavailable    {self.marks_unavailable:>12,}")
        print(f"      close bar unfillable {self.close_bar_unfillable:>12,}")
        if self.holds:
            print(
                f"      bars_held            min {min(self.holds)} · p25 {_quantile(self.holds, 0.25)} · median "
                f"{_quantile(self.holds, 0.5)} · p75 {_quantile(self.holds, 0.75)} · p99 "
                f"{_quantile(self.holds, 0.99)} · max {max(self.holds)}"
            )
        print(f"      in-sample / hold-out {self.in_sample:>12,} / {self.hold_out:,}   (boundary {HOLDOUT_BOUNDARY})")


def sweep(*, check: bool, report: bool, limit: int | None) -> int:
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    print(f"\n[sweep] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"        {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"        builder {builder_version}", flush=True)
    window = Window(start=date(1900, 1, 1), end=EVALUATION_END)
    print(f"        window {window.start} … {window.end}   (the whole corpus; nothing purged)", flush=True)

    tallies = {"S-1": _Tally("S-1", None), "S-3": _Tally("S-3", S3_MAX_HOLD_BARS)}
    problems: list[str] = []
    bars = 0
    empty = 0
    non_positive_opens = 0

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        print(f"  validated universe {len(universe)} instruments (US stocks ex-ETF, §4.0)", flush=True)
        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        # ⚠ One series per instrument is ASSERTED, not assumed. The ledger keys
        # on instrument_id while the corpus keys on series_id, so a second
        # series would split one instrument's hold state in two and I1 would be
        # measuring nothing.
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
            series, masked_opens = _to_series(masked.bars)
            non_positive_opens += masked_opens
            bars += len(series)
            for label, identity, signals in (
                ("S-1", s1_identity, s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar")),
                ("S-3", s3_identity, s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar")),
            ):
                rows = resolve_fills(
                    signals,
                    series=series,
                    identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
                    instrument_id=int(instrument_id),
                )
                entries, exits = _fills(rows, int(instrument_id))
                regime = _S1_REGIME if label == "S-1" else _S3_REGIME
                built = build_positions(
                    strategy_id=S1_STRATEGY_ID if label == "S-1" else S3_STRATEGY_ID,
                    strategy_version=s1_version if label == "S-1" else s3_version,
                    entries=entries,
                    exits=exits,
                    outcomes=[],
                    outcome_pin=None,
                    series={int(instrument_id): series},
                    regime=regime,
                    window=window,
                )
                tallies[label].absorb(len(entries), len(exits), built, problems)
                if check:
                    problems.extend(
                        _check_invariants(
                            label=label,
                            instrument_id=int(instrument_id),
                            series=series,
                            entries=entries,
                            exits=exits,
                            positions=built.positions,
                            superseded=built.superseded_open_position,
                            window=window,
                            max_hold_bars=regime.max_hold_bars,
                        )
                    )
            if n % 250 == 0:
                print(
                    f"  {n}/{len(pairs)} series, {bars:,} bars, {len(problems)} problems "
                    f"({time.monotonic() - started:.0f}s)",
                    flush=True,
                )

    print(f"\n  series with bars  {len(pairs) - empty}   (fail-closed empties: {empty})")
    print(f"  bars              {bars:,}")
    print(f"  non-positive opens masked by this caller  {non_positive_opens}   (see _to_series)")
    if report:
        for tally in tallies.values():
            tally.report()
    if check:
        print(f"\n  invariant violations: {len(problems)}")
        for problem in problems[:20]:
            print(f"    {problem}")
        if len(problems) > 20:
            print(f"    … and {len(problems) - 20} more")
    print(f"\n  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    # ⚠ Re-checked AFTER the sweep as well as before: a probe harness that
    # mutated and restored the file mid-run would pass the entry check alone.
    _stamped_versions()
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invariants", action="store_true", help="assert I1-I4 over every position built")
    parser.add_argument("--census", action="store_true", help="report the distribution")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.invariants or args.census or args.all):
        parser.error("pick at least one arm: --invariants, --census or --all")

    failures = sweep(check=args.invariants or args.all, report=args.census or args.all, limit=args.limit)
    print(f"\nverdict: {'*** FAIL ***' if failures else 'PASS'}", flush=True)
    return failures


if __name__ == "__main__":
    sys.exit(main())
