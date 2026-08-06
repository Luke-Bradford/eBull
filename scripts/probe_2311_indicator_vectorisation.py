"""Revert-probe the vectorised window indicators (#2311).

    uv run python scripts/probe_2311_indicator_vectorisation.py

Sister to ``scripts/probe_2240_s3_mean_reversion.py``, whose two guards apply
unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected. Two anchors here
   would have matched twice without a longer form — ``if n >= period:`` appears
   in both rewritten functions — which is precisely the failure guard 1 exists
   to catch.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

⚠ THIS HARNESS MUTATES ``app/services/indicator_series.py``, whose source hash
IS ``RULE_SET_VERSION``. Do not run it concurrently with the corpus sweep
(``scripts/verify_2240_indicator_series.py``) or with any signal write: a
concurrent reader stamps its rows with the INJECTED hash, and a start-vs-end
check misses it because the probe restores the file (phase 4b's lesson).

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **The recursive indicators.** #2311 left EMA / RSI / ATR / MACD as Python
loops, so this branch injects no defect into them and probing them here would
be probing ticket 2a's work a second time.

⚠ **CORRUPTING the float -> ndarray cache** — returning the wrong field. That
fails essentially every test in the file, so a probe would report ``CAUGHT``
without discriminating anything. The prefix-equivalence probes below cover the
same ground with a fixture that says which value moved.

⚠ REMOVING that cache is the opposite shape and IS probed (the last three
entries). It breaks no value and fails no other test — the only symptom is the
corpus sweep drifting back towards the 305.6 s that made ticket 2a add the
cache. A performance invariant with no failing test is the kind that survives
review, so it gets a counting test and a probe like any other.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path("app/services/indicator_series.py")
TESTS = "tests/test_indicator_series.py"

#: (what the injected defect IS, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        # ⚠ THE ONE THE REWRITE INTRODUCED. The Python form checked
        # `close is None` before it could reach the flat-range convention, so
        # the ordering was structural; `np.where` evaluates both branches and
        # has to be told.
        "the flat-range 50.0 convention applied to a bar with a NULL close",
        [
            (
                "np.where((span == 0.0) & ~np.isnan(close_at), 50.0, k_at)",
                "np.where(span == 0.0, 50.0, k_at)",
            )
        ],
        "test_a_flat_window_with_a_null_close_is_unevaluable_not_fifty",
    ),
    (
        # NaN is the internal missing-marker. Leaking it past the boundary
        # gives a value that is False against every comparison in both
        # directions — invisible to any strategy that reads it.
        "NaN left in the result contract instead of None",
        [("    for i in np.flatnonzero(np.isnan(values)).tolist():\n        out[i] = None\n", "")],
        "test_no_nan_ever_reaches_the_result_contract",
    ),
    (
        # ⚠ `sliding_window_view` RAISES when the window exceeds the array. The
        # Python loops it replaced simply never entered the body, so this guard
        # is new surface, not inherited surface.
        "bollinger's short-series guard removed (window longer than the array)",
        [
            (
                "    if n >= period:\n        windows = sliding_window_view(closes, period)",
                "    if True:\n        windows = sliding_window_view(closes, period)",
            )
        ],
        "test_a_series_shorter_than_the_window_is_warm_up_not_a_crash",
    ),
    (
        "stochastic's short-series guard removed (window longer than the array)",
        [
            (
                "    if n >= period:\n        window_high = sliding_window_view(highs, period).max(axis=1)",
                "    if True:\n        window_high = sliding_window_view(highs, period).max(axis=1)",
            )
        ],
        "test_a_series_shorter_than_the_window_is_warm_up_not_a_crash",
    ),
    (
        "stochastic's %D short-series guard removed",
        [("    if n >= d_period:", "    if True:")],
        "test_a_series_shorter_than_the_window_is_warm_up_not_a_crash",
    ),
    (
        # The window offset is where an off-by-one enters: every band shifts by
        # one bar and the LAST value still matches, which is why the fixtures
        # compare every prefix rather than the tail.
        "bollinger's unevaluable indices off by one",
        [("np.flatnonzero(np.isnan(means)) + (period - 1)", "np.flatnonzero(np.isnan(means)) + period")],
        "test_bollinger_lists_every_window_containing_the_null_and_no_warm_up",
    ),
    (
        # Acceptance 3 of the ticket: the reverted one-pass variance must still
        # be caught after the rewrite. This is that form wearing a numpy hat.
        "the two-pass variance replaced by the reverted one-pass form",
        [
            (
                "        stds = np.sqrt(windows.var(axis=1))",
                "        stds = np.sqrt((windows**2).mean(axis=1) - means**2)",
            )
        ],
        "test_matches_the_batch_form_where_one_pass_variance_fails",
    ),
    (
        "the lower band computed with the upper band's sign",
        [
            (
                "        lower[period - 1 :] = means - num_std * stds",
                "        lower[period - 1 :] = means + num_std * stds",
            )
        ],
        "test_bollinger_every_prefix_matches_the_batch_form",
    ),
    (
        # A window min computed as a max inverts the oscillator's denominator
        # and its floor at once.
        "stochastic's window low computed as a max",
        [
            (
                "        window_low = sliding_window_view(lows, period).min(axis=1)",
                "        window_low = sliding_window_view(lows, period).max(axis=1)",
            )
        ],
        "test_stochastic_every_prefix_matches_the_batch_form",
    ),
    (
        # [C2] %D inheriting %K's unevaluability is the contract clause a prior
        # round of this module already got wrong once.
        "%D no longer inherits %K's unevaluability",
        [
            (
                "        unevaluable_d[d_period - 1 :] = sliding_window_view(unevaluable_k, d_period).any(axis=1)",
                "        unevaluable_d[d_period - 1 :] = False",
            )
        ],
        "test_stochastic_d_inherits_k_unevaluability",
    ),
    (
        # ⚠ THE ONLY DEFECT HERE THAT BREAKS NO VALUE. Dropping the memoisation
        # leaves every number in this module correct and every other test in the
        # file green — the sole symptom is the corpus sweep drifting back
        # towards the 305.6 s that made ticket 2a add the cache. That is why the
        # counting test exists, and why the harness docstring's "not probed"
        # note does not cover this: it declines to probe CORRUPTING the cache
        # (returns the wrong field, fails everything, discriminates nothing).
        # Removing it is the opposite shape.
        "the float view converts on every access instead of once",
        [
            (
                "    @cached_property\n    def float_closes(self) -> list[float | None]:",
                "    @property\n    def float_closes(self) -> list[float | None]:",
            )
        ],
        "test_seven_indicators_convert_each_field_exactly_once",
    ),
    (
        # The ndarray view re-entering the uncached builder rather than reading
        # the float view. Values identical; conversions doubled.
        "the ndarray view re-converts the Decimals instead of reusing the float view",
        [
            (
                "        return np.array(self.float_closes, dtype=float)",
                '        return np.array(self._floats("close"), dtype=float)',
            )
        ],
        "test_the_ndarray_views_reuse_the_float_views",
    ),
    (
        # ⚠ Two-line anchor because `@dataclass(frozen=True)` also decorates
        # `IndicatorSeries` — guard 1's exact failure mode.
        #
        # `slots=True` removes the instance `__dict__` that `cached_property`
        # writes into. It is legal at class-definition time and fails at first
        # ACCESS, so nothing but an access-path test catches it.
        "slots=True added, leaving cached_property no __dict__ to write into",
        [
            (
                "@dataclass(frozen=True)\nclass BarSeries:",
                "@dataclass(frozen=True, slots=True)\nclass BarSeries:",
            )
        ],
        "test_the_cache_survives_on_a_frozen_instance",
    ),
]


def run(selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def main() -> int:
    original = SRC.read_text()
    failures: list[str] = []
    try:
        for name, edits, selector in PROBES:
            mutated = original
            bad_anchor = False
            for old, new in edits:
                count = mutated.count(old)
                if count != 1:
                    failures.append(f"{name}: anchor occurs {count} times, expected exactly 1 — probe proves nothing")
                    bad_anchor = True
                    break
                mutated = mutated.replace(old, new)
            if bad_anchor:
                print(f"  *** BAD ANCHOR ***    {name}", flush=True)
                continue
            SRC.write_text(mutated)
            rc = run(selector)
            SRC.write_text(original)
            verdict = "CAUGHT" if rc != 0 else "*** NOT CAUGHT ***"
            print(f"  {verdict:<20} {name}", flush=True)
            if rc == 0:
                failures.append(name)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        SRC.write_text(original)

    print(f"\n{len(PROBES) - len(failures)}/{len(PROBES)} defects caught", flush=True)
    for failure in failures:
        print(f"  UNGUARDED: {failure}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
