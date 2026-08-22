"""S-11 — S-4 gated to the two volatile regimes (#2840, R5 candidate S-H).

Spec: ``docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md``.

⚠ WHAT IS ACTUALLY AT RISK HERE, AND WHY THE NEGATIVE TESTS DOMINATE.
Conjoining a gate onto an existing rule is three lines; the things that go wrong
are all invisible to a test that only asks "did it fire":

* the gate collapsing ``not_evaluable`` into ``not_fired`` — the S-6 bug, and the
  reason a post-filter over ``s4_signals`` is forbidden;
* the manifest adapter dropping the regime (``_s4_signals`` does exactly that,
  deliberately, and copying it would silently un-gate S-11 while every test of
  the strategy module still passed);
* S-11's identity not moving when S-4's source moves, since S-11 IMPORTS S-4's
  rule rather than copying it.

One test each, below, and they are the point of the file.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import app.services.strategies.s4_volatility_compression_breakout as s4_module
import app.services.strategies.s11_volatile_regime_gated_breakout as s11_module
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.market_regime import Regime, RegimeSeries
from app.services.outcome_resolver import ExitLevels
from app.services.strategies.s4_volatility_compression_breakout import s4_exit_bracket, s4_signals
from app.services.strategies.s11_volatile_regime_gated_breakout import (
    PERMITTED_REGIMES,
    S11_PARAMS,
    S11_STRATEGY_ID,
    s11_exit_bracket,
    s11_identity,
    s11_signals,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_registry import StrategySignal
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"

#: The bar S-4 fires on in ``_firing_series`` below. Written out rather than
#: computed, so a fixture that stops firing fails loudly instead of vacuously
#: passing every "does not fire" assertion in the file.
FIRING_INDEX = 170

#: ⚠ TRANSCRIBED, not imported. #2840's declared hypothesis is the two volatile
#: regimes; importing ``PERMITTED_REGIMES`` for the expected value would make the
#: assertion agree with whatever the module happens to say, including a set
#: somebody narrowed to ``bear_volatile`` after seeing a result.
SPEC_PERMITTED = {"bear_volatile", "bull_volatile"}


def _bars(closes: Sequence[float | None], half_ranges: Sequence[float]) -> BarSeries:
    """One bar per close, ``high = close + h``, ``low = close - h``.

    ``None`` is a MASKED bar — every field present and empty, as
    ``load_masked_series`` produces. Same helper shape as ``test_strategy_s4``'s.
    """
    assert len(half_ranges) == len(closes), "half_ranges must align with closes"
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(c)),
            "high": None if c is None else Decimal(str(c + h)),
            "low": None if c is None else Decimal(str(c - h)),
            "close": None if c is None else Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c, h in zip(closes, half_ranges, strict=True)
    ]
    start = date(2020, 1, 1)
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _firing_closes() -> tuple[list[float | None], list[float]]:
    """130 wide-range bars, then 40 compressed flat ones, then a small breakout.

    ⚠ THE BREAKOUT IS DELIBERATELY TINY (100.0 -> 100.2). A large gap would blow
    up the breakout bar's own true range, and S-4 reads compression at ``t``
    INCLUDING that bar — so a dramatic-looking fixture never fires and the
    reason is easy to misread as a broken gate.
    """
    closes: list[float | None] = [*(100.0 + (i % 7) for i in range(130)), *([100.0] * 40), *([100.2] * 5)]
    spans: list[float] = [3.0] * 130 + [0.05] * 45
    return closes, spans


def _firing_series() -> BarSeries:
    closes, spans = _firing_closes()
    return _bars(closes, spans)


def _regime(n: int, regime: Regime | None, *, holes: tuple[int, ...] = ()) -> RegimeSeries:
    values: tuple[Regime | None, ...] = tuple(None if i in holes else regime for i in range(n))
    return RegimeSeries(values=values, not_evaluable_indices=holes)


def _verdict_at(signals: Sequence[StrategySignal], index: int) -> tuple[str, str | None]:
    signal = signals[index]
    return signal.verdict, signal.reason


# --------------------------------------------------------------------- the rule


def test_the_fixture_fires_s4_ungated() -> None:
    """The sentinel. Every negative assertion below is vacuous without it."""
    series = _firing_series()
    fired = [
        s.signal_index for s in s4_signals(series, universe=UNIVERSE, masked_reason=REASON) if s.verdict == "fired"
    ]
    assert fired == [FIRING_INDEX]


@pytest.mark.parametrize("regime", [Regime.BEAR_VOLATILE, Regime.BULL_VOLATILE])
def test_a_permitted_regime_lets_the_s4_signal_through(regime: Regime) -> None:
    series = _firing_series()
    signals = s11_signals(series, universe=UNIVERSE, masked_reason=REASON, regime=_regime(len(series), regime))
    assert [s.signal_index for s in signals if s.verdict == "fired"] == [FIRING_INDEX]


@pytest.mark.parametrize("regime", [Regime.BEAR_QUIET, Regime.BULL_QUIET])
def test_a_quiet_regime_suppresses_the_same_bar_as_not_fired(regime: Regime) -> None:
    """⚠ ``not_fired``, NOT ``not_evaluable``. The bar WAS judged — the regime is
    known and simply is not one the strategy permits (``RegimeSeries`` docstring:
    *"spec §0 rule 2 makes firing outside a declared domain the defect"*)."""
    series = _firing_series()
    signals = s11_signals(series, universe=UNIVERSE, masked_reason=REASON, regime=_regime(len(series), regime))
    assert not [s for s in signals if s.verdict == "fired"]
    assert _verdict_at(signals, FIRING_INDEX) == ("not_fired", None)


def test_all_four_regimes_are_exercised_by_the_two_tests_above() -> None:
    """Guards against an asymmetric permitted set slipping through.

    Without this, a module that permitted only ``bear_volatile`` would still pass
    one half of each parametrised pair and the file would look green.
    """
    assert {r.value for r in Regime} == SPEC_PERMITTED | {"bear_quiet", "bull_quiet"}
    assert {r.value for r in PERMITTED_REGIMES} == SPEC_PERMITTED


def test_s11_fires_on_a_subset_of_s4s_bars() -> None:
    """The gate can only remove. Checked across all four regimes at once."""
    series = _firing_series()
    s4_fired = {
        s.signal_index for s in s4_signals(series, universe=UNIVERSE, masked_reason=REASON) if s.verdict == "fired"
    }
    for regime in Regime:
        s11_fired = {
            s.signal_index
            for s in s11_signals(series, universe=UNIVERSE, masked_reason=REASON, regime=_regime(len(series), regime))
            if s.verdict == "fired"
        }
        assert s11_fired <= s4_fired, f"{regime} produced a signal S-4 did not"


# ------------------------------------------------- the refusal distinction (S-6)


def test_a_benchmark_hole_on_a_firing_bar_is_missing_market_context() -> None:
    """The bug this design exists to prevent, stated on the bar that matters."""
    series = _firing_series()
    regime = _regime(len(series), Regime.BULL_VOLATILE, holes=(FIRING_INDEX,))
    signals = s11_signals(series, universe=UNIVERSE, masked_reason=REASON, regime=regime)
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", "missing_market_context")


def test_a_benchmark_hole_refuses_even_where_s4_would_not_fire() -> None:
    """⚠ THE ONE A SHORT-CIRCUITING IMPLEMENTATION GETS WRONG.

    A post-filter over ``s4_signals`` — or a body that checks the S-4 predicate
    before the regime — reports ``not_fired`` here, because S-4's rule already
    said no. The bar is still one we could not judge.
    """
    series = _firing_series()
    quiet_bar = FIRING_INDEX - 1
    assert [s for s in s4_signals(series, universe=UNIVERSE, masked_reason=REASON)][quiet_bar].verdict == "not_fired"
    regime = _regime(len(series), Regime.BULL_VOLATILE, holes=(quiet_bar,))
    signals = s11_signals(series, universe=UNIVERSE, masked_reason=REASON, regime=regime)
    assert _verdict_at(signals, quiet_bar) == ("not_evaluable", "missing_market_context")


def test_a_masked_ohlc_bar_reports_s4s_reason_not_the_regimes() -> None:
    """Input ORDER is load-bearing: the regime is declared last, so a bar S-4
    cannot evaluate is refused for S-4's reason rather than the benchmark's."""
    closes, spans = _firing_closes()
    closes[FIRING_INDEX] = None
    series = _bars(closes, spans)
    regime = _regime(len(series), Regime.BULL_VOLATILE, holes=(FIRING_INDEX,))
    signals = s11_signals(series, universe=UNIVERSE, masked_reason=REASON, regime=regime)
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", REASON)


def test_a_misaligned_regime_series_is_rejected() -> None:
    series = _firing_series()
    with pytest.raises(ValueError, match="must align"):
        s11_signals(
            series, universe=UNIVERSE, masked_reason=REASON, regime=_regime(len(series) - 1, Regime.BULL_VOLATILE)
        )


def test_an_unknown_masked_reason_is_rejected() -> None:
    series = _firing_series()
    with pytest.raises(ValueError, match="unknown reason code"):
        s11_signals(
            series,
            universe=UNIVERSE,
            masked_reason="not_a_reason",  # type: ignore[arg-type]
            regime=_regime(len(series), Regime.BULL_VOLATILE),
        )


# ------------------------------------------------------------------- the identity


def test_s11_params_carry_s4s_live_source_hash() -> None:
    """⚠⚠ THE DRIFT GUARD. S-11 imports S-4's rule, so S-4's bytes are part of
    what S-11 does. Recomputed here by a third construction rather than by
    calling either module's private helper."""
    expected = hashlib.sha256(Path(s4_module.__file__).read_bytes()).hexdigest()[:12]
    assert S11_PARAMS["s4_source_hash"] == expected


def test_the_local_close_input_matches_s4s() -> None:
    """The one helper copied rather than imported — see the module docstring."""
    series = _firing_series()
    assert s11_module._close_input(series, universe=UNIVERSE) == s4_module._close_input(series, universe=UNIVERSE)


def test_moving_s4s_source_moves_s11s_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proved, not assumed: without this the params key could be inert."""
    before = s11_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    moved = dict(S11_PARAMS) | {"s4_source_hash": "deadbeefcafe"}
    monkeypatch.setattr(s11_module, "S11_PARAMS", moved)
    assert s11_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version != before


def test_moving_the_permitted_set_moves_the_version(monkeypatch: pytest.MonkeyPatch) -> None:
    before = s11_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    narrowed = dict(S11_PARAMS) | {"permitted_regimes": ("bear_volatile",)}
    monkeypatch.setattr(s11_module, "S11_PARAMS", narrowed)
    assert s11_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version != before


def test_the_permitted_set_is_serialised_canonically() -> None:
    """A ``frozenset`` has no canonical JSON form; the params must carry a sorted
    tuple of values, as S-5's do."""
    assert S11_PARAMS["permitted_regimes"] == ("bear_volatile", "bull_volatile")


def test_a_blank_cost_model_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="cost_model_id"):
        s11_identity(universe=UNIVERSE, cost_model_id="  ")


def test_s11_and_s4_are_distinct_identities() -> None:
    s11 = s11_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    s4 = s4_module.s4_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    assert s11.strategy_id != s4.strategy_id
    assert s11.version != s4.version


# ------------------------------------------------------------------ the manifest


def test_the_manifest_entry_is_wired_and_not_retired() -> None:
    entry = STRATEGY_MANIFEST[S11_STRATEGY_ID]
    assert entry.strategy_id == S11_STRATEGY_ID
    assert entry.retired_reason is None, "S-11 exists to produce evidence; retirement forbids that"
    assert entry.purpose == "harness_validation"
    assert entry.strategy_class == "per_series"
    assert entry.signal_kinds == frozenset({"entry"})
    assert entry.decision_calendar is not None and entry.decision_calendar([]) is None
    regime = entry.exit_regime(None)
    assert (regime.signal_pair, regime.level_based, regime.max_hold_bars) == (False, True, 40)


def test_the_manifest_adapter_passes_the_regime_through() -> None:
    """⚠⚠ The un-gating bug. ``_s4_signals`` discards its ``regime`` argument by
    design; a copy-paste of it here would leave S-11 firing in quiet regimes with
    every test in the sections above still green, because they call
    ``s11_signals`` directly."""
    series = _firing_series()
    entry = STRATEGY_MANIFEST[S11_STRATEGY_ID]
    assert entry.signals is not None
    quiet = entry.signals(
        series, universe=UNIVERSE, masked_reason=REASON, regime=_regime(len(series), Regime.BULL_QUIET)
    )
    volatile = entry.signals(
        series, universe=UNIVERSE, masked_reason=REASON, regime=_regime(len(series), Regime.BULL_VOLATILE)
    )
    assert not [s for s in quiet if s.verdict == "fired"]
    assert [s.signal_index for s in volatile if s.verdict == "fired"] == [FIRING_INDEX]


def test_the_scalar_and_batch_exit_levels_agree_and_equal_s4s() -> None:
    series = _firing_series()
    entry_price = Decimal("100.20")
    entry = STRATEGY_MANIFEST[S11_STRATEGY_ID]
    assert entry.exit_levels is not None
    # The manifest adapter raises if scalar and batch disagree, so a clean return
    # IS the parity assertion; the equality below is the S-4 half.
    levels = entry.exit_levels(series, signal_index=FIRING_INDEX, entry_price=entry_price, universe=UNIVERSE)
    target, stop, max_hold = s4_exit_bracket(
        series, signal_index=FIRING_INDEX, entry_price=entry_price, universe=UNIVERSE
    )
    assert levels == ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)
    assert s11_exit_bracket(series, signal_index=FIRING_INDEX, entry_price=entry_price, universe=UNIVERSE) == (
        target,
        stop,
        max_hold,
    )
