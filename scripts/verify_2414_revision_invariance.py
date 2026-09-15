"""#2414 — WHICH corpus revisions can change a stored strategy verdict. Measured, not argued.

Read-only. One ``REPEATABLE READ READ ONLY`` transaction for the loads, so every
instrument below comes from the same snapshot.

    PYTHONPATH=. uv run python -m scripts.verify_2414_revision_invariance

## The question this answers

#2414 item 1 records its own gating question: *"which revisions make a stored
verdict wrong. A retroactive split adjustment does not — the strategy saw the
unadjusted prices and so did a live trader — whereas a corrected bad print
does. §12 does not distinguish them and neither does the parent spec."*

That framing has the right instinct and the wrong axis, and this script exists
to establish which by running the real rule functions rather than reasoning
about them.

## Why "split vs bad print" is the wrong axis

A back-adjustment does NOT rescale the stored series uniformly. eToro
back-adjusts at FETCH time, so for a λ:1 split on date S:

* bars fetched before S sit at pre-split nominal scale;
* bars fetched on or after S come back rescaled by 1/λ;
* so between S and the heal, the STORED series carries a fabricated λ-fold
  jump at S — and `adjustment_heal` is what removes it.

So there are three revision classes, not two:

1. **uniform** — the whole decision window is rescaled (heal deeper than the
   window). Every predicate in S-4 / S-8 / S-11 is homogeneous (ranks, ratios,
   ADX, band comparisons, ATR multiples), so the verdict should be unchanged.
2. **boundary-inside-window** — the adjustment boundary falls inside the
   window. The pre-heal window contained a λ-fold move that never happened.
   This is STRONGER than "decided on stale data": the stored verdict was wrong
   on the data as it should have been at the time.
3. **point** — one bar corrected. Nothing is homogeneous with respect to a
   single-bar change.

Class 1 needs no supersession. Classes 2 and 3 do.

## ⚠⚠ The tidy answer this script was written to confirm is FALSE

The hypothesis was: class 2's reach is bounded by the strategy's own window, so
a supersession can be scoped to `WARMUP_BARS` bars after the adjustment
boundary. **Measured, it is not.** S-4's largest observed radius is **151 bars
against a `WARMUP_BARS` of 113** (14 + 100 − 1), and S-8's is **81 against 27**
(2 × 14 − 1).

The reason is structural and is in `indicator_series.py`: `atr_series` and
`adx_series` are **Wilder recursions** — seeded once with a simple average at
bar `period`, then `current = (current × (period − 1) + tr) / period` for every
later bar. So the indicator at bar *t* depends on EVERY bar from the series
start, with geometric decay `((period − 1) / period) ** k`, not on a rolling
window. Only S-8's Bollinger leg has finite memory.

So there is **no structural cut-off to scope a supersession on.** What bounds
the radius is the decay falling below the magnitude needed to cross a
threshold, which is data-dependent. A ledger fix must either re-evaluate the
affected instrument's whole series or adopt an explicitly stated, measured
decay bound — and if it states one, this script is how it is re-measured.

## What this does NOT do

⚠ It does not locate affected rows in the stored ledger. `bars_revised_by_cause`
is a run-level SUM and, per this ticket's own recorded lesson, an aggregate
carries no per-entity identity. This measures the RULES' sensitivity, which is
a property of code and is therefore knowable exactly.

⚠ The regime series is held FIXED across arms, and that is the faithful model
rather than a simplification: S-8's and S-11's regime is computed on the
BENCHMARK, which an instrument-level split does not touch. It is pinned to a
permitted regime per strategy so the price predicates are actually exercised —
a regime-refused bar would report invariance for the wrong reason.

⚠ None of the three strategies reads volume (S-4 / S-8 / S-11 are OHLC), so
volume is left alone. A split scales share volume by 1/λ; that matters for the
quarantine turnover rule, whose predicate is a RATIO of turnovers and so is
invariant anyway.

⚠ λ = 3, deliberately not a power of two: ×4 and ×0.25 are exact in binary
floating point, so a power-of-two factor would report perfect invariance
without testing whether the comparisons survive rounding.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.indicator_series import BarSeries
from app.services.market_regime import Regime, unconstrained_regime
from app.services.price_masked_bars import MASKED_REASON, load_masked_bars
from app.services.strategies.s4_volatility_compression_breakout import s4_signals
from app.services.strategies.s8_range_mean_reversion import s8_signals
from app.services.strategies.s11_volatile_regime_gated_breakout import s11_signals
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: The split factor. NOT a power of two — see the module docstring.
LAMBDA = Decimal(3)

#: The point-correction factor for class 3. A 50% bad print is the order of
#: magnitude the quarantine rules' own reverting-spike band is written around
#: (`_REVERSION_LO` 0.8 / `_REVERSION_HI` 1.25 on the RETURN, not the level).
POINT_FACTOR = Decimal("1.5")

#: `masked_reason` is the caller's to supply; the scan's own value is reused
#: verbatim (`price_masked_bars.MASKED_REASON`) so the arms differ only in
#: prices — a locally-invented code would be a second model of maskedness.

#: Each runnable strategy with a regime its gate permits. S-4 has no regime
#: gate and is called directly.
_RUNNABLE = (
    ("s4-volatility-compression-breakout", None),
    ("s8-range-mean-reversion", Regime.BULL_QUIET),
    ("s11-volatile-regime-gated-breakout", Regime.BULL_VOLATILE),
)

_INSTRUMENTS_SQL = """
SELECT instrument_id, count(*) AS signals
  FROM strategy_signals
 GROUP BY 1
 ORDER BY count(*) DESC, instrument_id
 LIMIT %(n)s
"""


def _scaled(series: BarSeries, factor: Decimal, *, start: int = 0, end: int | None = None) -> BarSeries:
    """``series`` with OHLC multiplied by ``factor`` over ``[start, end)``.

    A masked field stays masked: rescaling ``None`` would invent an observation,
    and the mask is a verdict the loader already reached.
    """
    stop = len(series.rows) if end is None else end
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(series.rows):
        if start <= index < stop:
            rows.append(
                {
                    "open": None if row["open"] is None else row["open"] * factor,
                    "high": None if row["high"] is None else row["high"] * factor,
                    "low": None if row["low"] is None else row["low"] * factor,
                    "close": None if row["close"] is None else row["close"] * factor,
                    "volume": row["volume"],
                }
            )
        else:
            rows.append(dict(row))
    return BarSeries(dates=series.dates, rows=tuple(rows))  # type: ignore[arg-type]


def _verdicts(strategy_id: str, regime: Regime | None, series: BarSeries) -> tuple[frozenset[int], frozenset[int]]:
    """``(fired indices, not_evaluable indices)`` for the strategy's ENTRY leg.

    Evaluability is returned alongside the firing set because a changed
    ``not_evaluable`` is a changed verdict too — the ledger stores the refusal
    and its reason code, not only the fires. Reporting the firing set alone
    would under-count exactly the rows §12 is about.
    """
    n = len(series.rows)
    if strategy_id.startswith("s4-"):
        signals = s4_signals(series, universe=SCAN_UNIVERSE, masked_reason=MASKED_REASON)
    else:
        assert regime is not None
        pinned = unconstrained_regime(n, regime=regime)
        fn = s8_signals if strategy_id.startswith("s8-") else s11_signals
        signals = fn(series, universe=SCAN_UNIVERSE, masked_reason=MASKED_REASON, regime=pinned)
    fired = frozenset(s.signal_index for s in signals if s.verdict == "fired")
    refused = frozenset(s.signal_index for s in signals if s.verdict == "not_evaluable")
    return fired, refused


def _radius(differing: frozenset[int], boundary: int) -> int | None:
    """How far past ``boundary`` a differing verdict reaches, in bars."""
    after = [index - boundary for index in differing if index >= boundary]
    return max(after) if after else None


#: Where the adjustment boundary is placed, as a fraction through the series.
#: ⚠ THREE positions, not one. A single position is a sample, and the radius is
#: the number the ledger decision would be scoped on — so it is exactly the
#: figure that must not rest on one draw.
BOUNDARY_FRACTIONS = (0.4, 0.6, 0.8)

#: Below this a strategy cannot warm up and the arms are all-refusal.
MIN_BARS = 200


def main(*, instruments: int = 20) -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            now = conn.execute("SELECT now()").fetchone()
            assert now is not None
            print(f"snapshot: {now[0]:%Y-%m-%d %H:%M:%SZ}   lambda={LAMBDA}  point_factor={POINT_FACTOR}")
            print(f"boundary fractions: {BOUNDARY_FRACTIONS}   instruments requested: {instruments}\n")
            targets = conn.execute(_INSTRUMENTS_SQL, {"n": instruments}).fetchall()
            loaded = [(int(iid), int(count), load_masked_bars(conn, int(iid))) for iid, count in targets]

    per_strategy: dict[str, dict[str, int]] = {
        sid: {"uniform": 0, "boundary": 0, "point": 0, "max_radius": -1, "base": 0, "pairs": 0} for sid, _ in _RUNNABLE
    }
    skipped = 0
    bars_total = 0
    for _instrument_id, _stored, masked in loaded:
        series = masked.series
        n = len(series.rows)
        if n < MIN_BARS:
            skipped += 1
            continue
        bars_total += n
        arm_uniform = _scaled(series, LAMBDA)
        for strategy_id, regime in _RUNNABLE:
            stats = per_strategy[strategy_id]
            base, base_refused = _verdicts(strategy_id, regime, series)
            stats["base"] += len(base)
            stats["pairs"] += 1
            u_fired, u_refused = _verdicts(strategy_id, regime, arm_uniform)
            stats["uniform"] += len((base ^ u_fired) | (base_refused ^ u_refused))
            for fraction in BOUNDARY_FRACTIONS:
                boundary = int(n * fraction)
                b_fired, b_refused = _verdicts(strategy_id, regime, _scaled(series, LAMBDA, end=boundary))
                d_boundary = (base ^ b_fired) | (base_refused ^ b_refused)
                stats["boundary"] += len(d_boundary)
                radius = _radius(d_boundary, boundary)
                if radius is not None and radius > stats["max_radius"]:
                    stats["max_radius"] = radius
                p_fired, p_refused = _verdicts(
                    strategy_id, regime, _scaled(series, POINT_FACTOR, start=boundary, end=boundary + 1)
                )
                stats["point"] += len((base ^ p_fired) | (base_refused ^ p_refused))

    print(
        f"instruments evaluated: {len(loaded) - skipped} ({skipped} skipped under {MIN_BARS} bars), "
        f"{bars_total} bars total\n"
    )
    print(f"{'strategy':<38} {'pairs':>6} {'base':>6} {'uniform':>8} {'boundary':>9} {'radius':>7} {'point':>7}")
    for strategy_id, _ in _RUNNABLE:
        s = per_strategy[strategy_id]
        radius = "-" if s["max_radius"] < 0 else s["max_radius"]
        print(
            f"{strategy_id:<38} {s['pairs']:>6} {s['base']:>6} {s['uniform']:>8} "
            f"{s['boundary']:>9} {radius!s:>7} {s['point']:>7}"
        )

    print("\nHOW TO READ THIS")
    print("  uniform  — verdict flips when the WHOLE series is rescaled by lambda.")
    print("             MUST be 0: every predicate is homogeneous. Non-zero here would be")
    print("             a floating-point finding, not a strategy one.")
    print("  boundary — verdict flips when the adjustment boundary sits INSIDE the series,")
    print("             i.e. the pre-heal shape. Summed over the boundary fractions.")
    print("  radius   — the largest number of bars PAST the boundary at which a verdict")
    print("             flipped. ⚠⚠ This is a DECAY bound, not a window bound: `atr_series`")
    print("             and `adx_series` are Wilder recursions seeded once at bar `period`,")
    print("             so every value depends on EVERY earlier bar. There is no structural")
    print("             cut-off to scope a supersession on.")
    print("  point    — verdict flips from correcting ONE bar by point_factor.")
    print("\n⚠ S-11 is NOT independent evidence here. Its price legs ARE S-4's, and the")
    print("  regime is pinned to a permitted value so the gate always passes — so S-11")
    print("  reproducing S-4 exactly is arithmetic, not corroboration.")
    print("⚠ These are the RULES' sensitivity. They do not locate affected stored rows —")
    print("  `bars_revised_by_cause` is a run-level sum and carries no per-entity identity.")


if __name__ == "__main__":
    main()
