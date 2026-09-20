"""S-12 — S-4 gated to the cheapest charged cost band (#2840 arm 2).

Spec: ``docs/proposals/ta/2026-09-20-sh-top-band-price-gated-breakout.md``.

⚠ WHAT IS ACTUALLY AT RISK HERE. Conjoining a price gate onto an existing rule is
two lines; the failures are the ones a "did it fire" test cannot see:

* the gate turning a REFUSAL into a decline — S-4 refuses a bar, the gate reports
  ``not_fired``, and a whole class of unevaluable bars is silently reclassified;
* the manifest adapter not gating at all, which leaves every test of the module
  itself green while the registered strategy is plain S-4;
* the threshold drifting away from the band table it is supposed to BE, either by
  a literal creeping in or by a recalibration that the identity does not follow;
* S-12's identity not moving when S-4's source moves, since S-12 IMPORTS S-4's rule.

One test each, below, and they are the point of the file.

⚠ The fixtures put the firing bar deliberately at 100.2 / 100.0 / 99.9 — either
side of, and exactly on, the band edge. They are written as LEVELS rather than as a
scale factor applied to one fixture, because a float scale lands near the edge
rather than on it and the inclusive-boundary test is the one that needs to be exact.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import app.services.strategies.s12_cheapest_band_price_gated_breakout as s12_module
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import BANDS, COST_MODEL_ID, PriceBand, band_for, cost_price_basis
from app.services.indicator_series import BarSeries
from app.services.market_regime import Regime, RegimeSeries
from app.services.outcome_resolver import ExitLevels
from app.services.research_corpus_ingest import RESEARCH_ARCHIVES
from app.services.strategies.s4_volatility_compression_breakout import s4_exit_bracket, s4_signals
from app.services.strategies.s12_cheapest_band_price_gated_breakout import (
    CHEAPEST_BAND,
    PRICE_BASIS_REFUSAL_REASON,
    PRICE_FLOOR,
    S12_PARAMS,
    S12_STRATEGY_ID,
    _check_gate_is_expressible,
    s12_exit_bracket,
    s12_identity,
    s12_signals,
)
from app.services.strategy_entry_liquidity import archive_policy_for
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_price_basis import from_archive_basis, from_undeclared_source
from app.services.strategy_registry import StrategySignal
from app.services.technical_analysis import OHLCVRow
from app.services.universe_selection import vendor_for

UNIVERSE = "survivorship_free"
REASON = "quarantined_bar"

#: The bar S-4 fires on in every fixture below. Written out rather than computed,
#: so a fixture that stops firing fails loudly instead of vacuously passing every
#: "does not fire" assertion in the file.
FIRING_INDEX = 170

#: ⚠ TRANSCRIBED from the spec, not imported. Importing ``CHEAPEST_BAND.lower`` for
#: the expected value would make the assertion agree with whatever the module says,
#: including an edge somebody moved after seeing a result. If the cost model is ever
#: recalibrated this constant is what fails, which is the intended alarm.
SPEC_EDGE = Decimal("100")


def _bars(closes: Sequence[float | None], half_ranges: Sequence[float]) -> BarSeries:
    """One bar per close, ``high = close + h``, ``low = close - h``.

    ``None`` is a MASKED bar — every field present and empty, as
    ``load_masked_series`` produces. Same helper shape as S-11's test file.
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


def _firing_series(*, plateau: float, breakout: float) -> BarSeries:
    """130 wide-range bars, 40 compressed flat ones at ``plateau``, then ``breakout``.

    ⚠ THE BREAKOUT IS DELIBERATELY TINY. A large gap blows up the breakout bar's own
    true range, and S-4 reads compression at ``t`` INCLUDING that bar — so a
    dramatic-looking fixture never fires and the reason reads as a broken gate.
    """
    closes: list[float | None] = [
        *(plateau + (i % 7) for i in range(130)),
        *([plateau] * 40),
        *([breakout] * 5),
    ]
    spans: list[float] = [3.0] * 130 + [0.05] * 45
    return _bars(closes, spans)


def _above_edge() -> BarSeries:
    """Firing close 100.2 — inside the cheapest band."""
    return _firing_series(plateau=100.0, breakout=100.2)


def _on_edge() -> BarSeries:
    """Firing close exactly 100.0 — the inclusive boundary."""
    return _firing_series(plateau=99.8, breakout=100.0)


def _below_edge() -> BarSeries:
    """Firing close 99.9 — one tick outside the cheapest band."""
    return _firing_series(plateau=99.7, breakout=99.9)


def _fired(signals: Sequence[StrategySignal]) -> list[int]:
    return [signal.signal_index for signal in signals if signal.verdict == "fired"]


def _verdict_at(signals: Sequence[StrategySignal], index: int) -> tuple[str, str | None]:
    signal = signals[index]
    return signal.verdict, signal.reason


# ------------------------------------------------------------------- the sentinel


@pytest.mark.parametrize("series_factory", [_above_edge, _on_edge, _below_edge])
def test_every_fixture_fires_s4_ungated(series_factory: object) -> None:
    """The sentinel. Every gate assertion below is vacuous without it.

    ⚠ Run on ALL THREE price levels, not just one. S-4's rule is scale-free, but
    that is the claim the below-edge test depends on: if moving the fixture down
    two dollars ALSO stopped S-4 firing, ``test_below_the_edge_is_not_fired`` would
    pass while proving nothing about the gate.
    """
    series = series_factory()  # type: ignore[operator]
    assert _fired(s4_signals(series, universe=UNIVERSE, masked_reason=REASON)) == [FIRING_INDEX]


# ---------------------------------------------------------------------- the rule


def test_above_the_edge_the_s4_signal_passes_through() -> None:
    series = _above_edge()
    signals = s12_signals(
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert _fired(signals) == [FIRING_INDEX]


def test_the_edge_is_inclusive() -> None:
    """``close == PRICE_FLOOR`` fires, matching ``PriceBand.contains``.

    The band a charge would select on exactly 100.00 IS the cheapest band, so the
    gate that claims to admit that band has to admit that price.
    """
    assert band_for(SPEC_EDGE) is CHEAPEST_BAND
    series = _on_edge()
    signals = s12_signals(
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert _fired(signals) == [FIRING_INDEX]


def test_below_the_edge_is_not_fired_and_not_refused() -> None:
    """⚠ ``not_fired``, NOT ``not_evaluable``. The bar WAS judged — its price is
    known and simply is not one the strategy trades. Collapsing the two is the S-6
    bug, one level down from S-11's regime case."""
    series = _below_edge()
    signals = s12_signals(
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert not _fired(signals)
    assert _verdict_at(signals, FIRING_INDEX) == ("not_fired", None)


def test_s12_equals_s4_exactly_when_every_close_clears_the_edge() -> None:
    """The other half of the subset property — without it, a never-firing strategy
    satisfies "subset of S-4" perfectly."""
    series = _above_edge()
    assert all(row["close"] is not None and row["close"] >= SPEC_EDGE for row in series.rows), (
        "fixture no longer sits entirely above the edge; the equality below would be vacuous"
    )
    s4 = s4_signals(series, universe=UNIVERSE, masked_reason=REASON)
    s12 = s12_signals(
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert [(s.signal_index, s.verdict, s.reason) for s in s12] == [(s.signal_index, s.verdict, s.reason) for s in s4]


def test_s12_fired_set_is_a_subset_of_s4s_on_a_mixed_series() -> None:
    """A series straddling the edge: the gate can only ever remove."""
    series = _firing_series(plateau=99.0, breakout=101.0)
    s4 = set(_fired(s4_signals(series, universe=UNIVERSE, masked_reason=REASON)))
    s12 = set(
        _fired(
            s12_signals(
                series,
                universe=UNIVERSE,
                masked_reason=REASON,
                price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
            )
        )
    )
    assert s12 <= s4


# ------------------------------------------------------------------- the refusals


def test_a_masked_bar_keeps_s4s_reason_even_above_the_edge() -> None:
    """S-4's refusal wins. The gate must not report a priceless bar as ``not_fired``."""
    series = _above_edge()
    masked = list(series.rows)
    masked[FIRING_INDEX] = {"open": None, "high": None, "low": None, "close": None, "volume": 1_000}  # type: ignore[typeddict-item]
    series = BarSeries(dates=series.dates, rows=tuple(masked))
    signals = s12_signals(
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", REASON)


def test_a_warmup_bar_is_refused_whatever_the_price() -> None:
    """Index 0 is inside S-4's warm-up on any fixture, above the edge or below."""
    for series in (_above_edge(), _below_edge()):
        signals = s12_signals(
            series,
            universe=UNIVERSE,
            masked_reason=REASON,
            price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
        )
        verdict, reason = _verdict_at(signals, 0)
        assert verdict == "not_evaluable"
        assert reason == "insufficient_warmup"


def test_a_masked_bar_below_the_edge_is_still_a_refusal() -> None:
    """The case a short-circuiting gate gets wrong: price-check first, then refuse.

    Written as its own test because the two conditions agree on ``do not fire`` and
    only the REASON separates them — which is exactly the distinction the cohort
    reporting is built on.
    """
    series = _below_edge()
    masked = list(series.rows)
    masked[FIRING_INDEX] = {"open": None, "high": None, "low": None, "close": None, "volume": 1_000}  # type: ignore[typeddict-item]
    series = BarSeries(dates=series.dates, rows=tuple(masked))
    signals = s12_signals(
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", REASON)


def test_an_unknown_masked_reason_is_rejected() -> None:
    series = _above_edge()
    with pytest.raises(ValueError, match="unknown reason code"):
        s12_signals(
            series,
            universe=UNIVERSE,
            masked_reason="not_a_reason",  # type: ignore[arg-type]
            price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
        )


# -------------------------------------------------------------------- the threshold


def test_the_gate_reads_the_cheapest_charged_band_not_the_last_one() -> None:
    """⚠ The whole hypothesis. ``BANDS[-1]`` is the highest-PRICED band; today it
    is also the cheapest-CHARGED one, and this asserts the coincidence explicitly
    rather than relying on it."""
    assert CHEAPEST_BAND is min(BANDS, key=lambda band: band.p75_spread_pct)
    assert CHEAPEST_BAND.lower == SPEC_EDGE
    assert all(band.p75_spread_pct >= CHEAPEST_BAND.p75_spread_pct for band in BANDS)


def test_the_gate_the_params_and_the_band_are_one_number() -> None:
    """Ties the executed threshold to the hashed one.

    ⚠ Without this, a test that monkeypatches ``S12_PARAMS`` proves only that the
    digest reads that dict — not that the dict describes what the rule DOES.
    """
    assert Decimal(str(S12_PARAMS["price_band_lower"])) == CHEAPEST_BAND.lower
    assert Decimal(repr(PRICE_FLOOR)) == CHEAPEST_BAND.lower
    assert S12_PARAMS["price_band_label"] == CHEAPEST_BAND.label


@pytest.mark.parametrize("price", ["0.01", "4.99", "5", "19.99", "20", "99.99", "100", "100.01", "1e6"])
def test_the_float_gate_agrees_with_band_for(price: str) -> None:
    """The float comparison and the Decimal band table answer the same question.

    ⚠ What licenses a float compare at all is a MEASUREMENT, not this test: 0 of
    75,972,669 corpus open/close values sit within 1e-9 of a band edge without
    equalling it (#3238, recorded at ``backtest_run.py:1479``). This pins the two
    implementations together; the census is what says no stored price can land
    between them.
    """
    decimal_price = Decimal(price)
    assert (float(decimal_price) >= PRICE_FLOOR) is (band_for(decimal_price) is CHEAPEST_BAND)


def test_the_threshold_follows_a_recalibrated_band_table() -> None:
    """The "never a literal" rule, proven rather than asserted.

    ⚠ A source grep for ``100`` was the first draft of this and it is the weaker
    check twice over: it passes on a technicality the moment the literal is spelled
    differently, and it says nothing about whether a MOVED table would be followed.
    Handing ``_cheapest_band`` a recalibrated table answers the actual question.
    """
    recalibrated = (
        PriceBand(label="<$250", lower=None, upper=Decimal("250"), p75_spread_pct=Decimal("0.9"), sample_size=10),
        PriceBand(label=">=$250", lower=Decimal("250"), upper=None, p75_spread_pct=Decimal("0.2"), sample_size=10),
    )
    chosen = s12_module._cheapest_band(recalibrated)
    assert chosen.lower == Decimal("250")
    assert chosen is not CHEAPEST_BAND


def test_the_cheapest_band_is_not_assumed_to_be_the_last_one() -> None:
    """A table whose cheapest band is NOT the open-above one is selected correctly —
    and then refused by the expressibility check, which is the honest pair."""
    inverted = (
        PriceBand(label="<$5", lower=None, upper=Decimal("5"), p75_spread_pct=Decimal("0.1"), sample_size=10),
        PriceBand(label=">=$5", lower=Decimal("5"), upper=None, p75_spread_pct=Decimal("2.0"), sample_size=10),
    )
    chosen = s12_module._cheapest_band(inverted)
    assert chosen.label == "<$5"
    with pytest.raises(ValueError, match="open BELOW"):
        _check_gate_is_expressible(chosen)


@pytest.mark.parametrize(
    "band",
    [
        PriceBand(label="open-below", lower=None, upper=Decimal("5"), p75_spread_pct=Decimal("0.1"), sample_size=1),
        PriceBand(
            label="bounded", lower=Decimal("100"), upper=Decimal("500"), p75_spread_pct=Decimal("0.1"), sample_size=1
        ),
    ],
)
def test_a_band_that_cannot_express_the_gate_refuses_at_import(band: PriceBand) -> None:
    """A recalibration that made a BOUNDED band cheapest would leave ``close >= lower``
    admitting the dearer bands above it — a different rule wearing this one's name."""
    with pytest.raises(ValueError):
        _check_gate_is_expressible(band)


# --------------------------------------------------------------------- the identity


def test_s12_params_carry_s4s_live_source_hash() -> None:
    live = hashlib.sha256(Path(s12_module.s4.__file__).read_bytes()).hexdigest()[:12]
    assert S12_PARAMS["s4_source_hash"] == live


def test_moving_s4s_source_moves_s12s_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """S-12 IMPORTS S-4's rule, so an edit to S-4 changes what S-12 does.

    Monkeypatched rather than asserted, so the dependency is PROVEN rather than
    assumed from the presence of a key.
    """
    before = s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    monkeypatch.setattr(s12_module, "S12_PARAMS", {**S12_PARAMS, "s4_source_hash": "deadbeefcafe"})
    after = s12_module.s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    assert before != after


def test_moving_the_band_edge_moves_s12s_version(monkeypatch: pytest.MonkeyPatch) -> None:
    before = s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    monkeypatch.setattr(s12_module, "S12_PARAMS", {**S12_PARAMS, "price_band_lower": "150"})
    after = s12_module.s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    assert before != after


def test_the_cost_model_id_is_part_of_the_identity() -> None:
    """The primary recalibration protection — the band keys are belt-and-braces."""
    a = s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    b = s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID + "-x").version
    assert a != b


def test_a_blank_cost_model_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="cost_model_id must be a non-empty declaration"):
        s12_identity(universe=UNIVERSE, cost_model_id="   ")


def test_s12_close_input_matches_s4s() -> None:
    """The local copy has not drifted from the private original it duplicates."""
    series = _above_edge()
    mine = s12_module._close_input(series, universe=UNIVERSE)
    theirs = s12_module.s4._close_input(series, universe=UNIVERSE)
    assert mine == theirs


# ------------------------------------------------------------------ the price basis


def test_the_backtest_universes_pinned_archive_is_as_traded() -> None:
    """Guard 1 of 2 (the other is a run-time assertion in the step-3 measurement).

    A ``>= $100`` gate is a nominal-price gate only on an as-traded corpus. If the
    backtest corpus ever moves to a split-adjusted archive, this reds instead of the
    strategy quietly becoming a gate on adjusted prices.

    ⚠ It checks CONFIGURATION, not the run: ``load_corpus`` resolves the basis from
    the series actually selected and fails closed to ``split_adjusted`` when the
    provenance is missing. A run can still charge the maximum band with this green.
    """
    policy = archive_policy_for(vendor_for(BACKTEST_UNIVERSE))
    assert policy is not None, "the backtest universe's pinned vendor has no declared provenance"
    assert cost_price_basis(policy.adjustment_basis) == "as_traded"
    assert any(archive.adjustment_basis == "unadjusted" for archive in RESEARCH_ARCHIVES)


def test_a_series_with_no_declared_basis_refuses_every_bar() -> None:
    """⚠ REFUSES, never judges. An uncertified close may be back-adjusted, so judging
    it against a nominal edge would record a verdict about a price that never traded
    — and scan rows are terminal, so there is no later correction.

    ⚠⚠ THE GATE IS THE BASIS, NOT THE UNIVERSE (#2840 §6 item 3). This test used to
    pass a CERTIFIED carrier on ``survivor_only`` and assert a refusal, because the
    removed ``AS_TRADED_UNIVERSES`` token refused the universe by name. The universe
    here is now the backtest one and the refusal comes from the carrier, which is the
    honest statement of what the rule checks. ⚠ The last bar is ``no_fill_bar`` — the
    short-circuit routes through ``evaluate``, which the universe token never did.
    """
    series = _above_edge()
    signals = s12_signals(
        series,
        universe=BACKTEST_UNIVERSE,
        masked_reason=REASON,
        price_basis=from_undeclared_source(n_bars=len(series)),
    )
    assert len(signals) == len(series)
    assert {(s.verdict, s.reason) for s in signals[:-1]} == {("not_evaluable", PRICE_BASIS_REFUSAL_REASON)}
    assert (signals[-1].verdict, signals[-1].reason) == ("not_evaluable", "no_fill_bar")
    assert [s.signal_index for s in signals] == list(range(len(series)))
    assert {s.kind for s in signals} == {"entry"}


def test_the_refusal_is_not_a_decline_even_where_s4_would_fire() -> None:
    """The distinction the cohort counts are built on: this is an absent CONTEXT, not
    a rule that looked and said no. S-4 fires on this bar under the same universe.

    ⚠ The absent context is now the BASIS rather than the universe (#2840 §6 item 3) —
    S-4 reads no basis at all, so it fires on the very bar S-12 refuses.
    """
    series = _above_edge()
    assert _fired(s4_signals(series, universe=BACKTEST_UNIVERSE, masked_reason=REASON)) == [FIRING_INDEX]
    signals = s12_signals(
        series,
        universe=BACKTEST_UNIVERSE,
        masked_reason=REASON,
        price_basis=from_undeclared_source(n_bars=len(series)),
    )
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", PRICE_BASIS_REFUSAL_REASON)


def test_no_universe_token_survives_in_the_rule_or_its_identity() -> None:
    """⚠⚠ THE DE-OVERLOADING, asserted rather than trusted (#2840 §6 item 3).

    The rule used to declare ``AS_TRADED_UNIVERSES`` and hash ``as_traded_universes``
    into its params — a universe NAME standing in for a per-bar price-basis FACT. Both
    are gone, and the params carry the basis RULE VERSION instead. A reader coming back
    to the module needs one place that says the token is not merely unused but absent.
    """
    assert not hasattr(s12_module, "AS_TRADED_UNIVERSES")
    assert "as_traded_universes" not in S12_PARAMS
    assert "price_basis_rule" in S12_PARAMS


def test_moving_the_price_basis_rule_moves_the_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠ The identity must follow the rule that now does the token's job.

    Replaces ``test_widening_the_declared_universes_moves_the_version``: the params key
    whose movement matters is no longer the universe set but the basis rule's version,
    which composes this module's bytes with the pinned archive provenances.
    """
    before = s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    monkeypatch.setattr(
        s12_module,
        "S12_PARAMS",
        {**S12_PARAMS, "price_basis_rule": "price-basis-carrier-v1+ffffffffffff+archives-ffffffffffff"},
    )
    after = s12_module.s12_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    assert before != after


# --------------------------------------------------------------- the manifest wiring


def _entry() -> object:
    return STRATEGY_MANIFEST[S12_STRATEGY_ID]


def test_the_manifest_registers_s12_as_an_entry_only_per_series_strategy() -> None:
    entry = _entry()
    assert entry.strategy_id == S12_STRATEGY_ID  # type: ignore[attr-defined]
    assert entry.strategy_class == "per_series"  # type: ignore[attr-defined]
    assert entry.signal_kinds == frozenset({"entry"})  # type: ignore[attr-defined]
    assert entry.retired_reason is None  # type: ignore[attr-defined]
    assert entry.purpose == "harness_validation"  # type: ignore[attr-defined]
    assert entry.exit_levels is not None and entry.exit_levels_batch is not None  # type: ignore[attr-defined]


def test_the_manifest_carries_s4s_exit_regime() -> None:
    regime = _entry().exit_regime(None)  # type: ignore[attr-defined]
    assert regime.level_based is True
    assert regime.signal_pair is False
    assert regime.max_hold_bars == s12_module.MAX_HOLD_BARS
    assert regime.rebalance_dates is None


def test_the_manifest_adapter_actually_gates_on_price() -> None:
    """⚠ THE POINT OF THIS FILE. The adapter takes a ``regime`` it discards, which
    makes it textually identical to ``_s4_signals`` — the copy that would leave the
    registered strategy un-gated while every test above still passed."""
    series = _below_edge()
    signals = _entry().signals(  # type: ignore[attr-defined]
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        regime=RegimeSeries(values=tuple([Regime.BULL_QUIET] * 175), not_evaluable_indices=()),
        price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
    )
    assert not _fired(signals)
    assert _verdict_at(signals, FIRING_INDEX) == ("not_fired", None)


def test_the_manifest_adapter_ignores_the_regime() -> None:
    """S-12 gates on price alone: the same bars fire under every regime."""
    series = _above_edge()
    verdicts = {
        regime: _fired(
            _entry().signals(  # type: ignore[attr-defined]
                series,
                universe=UNIVERSE,
                masked_reason=REASON,
                regime=RegimeSeries(values=tuple([regime] * len(series)), not_evaluable_indices=()),
                price_basis=from_archive_basis("unadjusted", n_bars=len(series)),
            )
        )
        for regime in Regime
    }
    assert set(map(tuple, verdicts.values())) == {(FIRING_INDEX,)}


def test_scalar_and_batch_exit_levels_agree_and_equal_s4s() -> None:
    series = _above_edge()
    entry_price = Decimal("100.2")
    target, stop, max_hold = s12_exit_bracket(
        series, signal_index=FIRING_INDEX, entry_price=entry_price, universe=UNIVERSE
    )
    assert (target, stop, max_hold) == s4_exit_bracket(
        series, signal_index=FIRING_INDEX, entry_price=entry_price, universe=UNIVERSE
    )
    through_manifest = _entry().exit_levels(  # type: ignore[attr-defined]
        series, signal_index=FIRING_INDEX, entry_price=entry_price, universe=UNIVERSE
    )
    assert through_manifest == ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)
