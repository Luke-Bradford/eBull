"""#2840 — S-12's declared price-basis input, and the two paths that must agree.

Separate from ``test_2840_price_basis_carrier.py`` on purpose: that file pins the
CARRIER, this one pins the CONSUMER. The distinction matters because the carrier
is general and the gate is S-12's alone.
"""

from __future__ import annotations

import inspect
from datetime import date
from pathlib import Path

import pytest

from app.services.indicator_series import BarSeries
from app.services.market_regime import unconstrained_regime
from app.services.price_segments import series_segment_bounds
from app.services.strategies.s12_cheapest_band_price_gated_breakout import (
    PRICE_BASIS_REFUSAL_REASON,
    S12_PARAMS,
    S12_STRATEGY_ID,
    s12_signals,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_price_basis import PRICE_BASIS_RULE_VERSION, PriceBasisSeries, from_archive_basis
from app.services.strategy_registry import StrategySignal
from app.services.strategy_segmented_evaluation import segmented_signals
from tests.test_2840_cheapest_band_price_gated_breakout import (
    FIRING_INDEX,
    REASON,
    UNIVERSE,
    _above_edge,
    _verdict_at,
)


def _certified(series: BarSeries) -> PriceBasisSeries:
    return from_archive_basis("unadjusted", n_bars=len(series))


def _withheld(series: BarSeries) -> PriceBasisSeries:
    return from_archive_basis(None, n_bars=len(series))


def test_a_withheld_archive_policy_refuses_every_bar() -> None:
    """The case the universe token cannot see.

    ``_resolve_liquidity_policy`` returns ``None`` on a provenance disagreement and
    evaluation CONTINUES — it withholds a diagnostic, it does not block. Before
    this change a ``survivorship_free`` run under a withheld policy reached the
    ``>= $100`` nominal gate with no refusal anywhere.
    """
    series = _above_edge()
    signals = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=_withheld(series))
    assert {s.verdict for s in signals} == {"not_evaluable"}
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", PRICE_BASIS_REFUSAL_REASON)


def test_a_certified_archive_leaves_the_verdicts_byte_identical() -> None:
    """⚠ The regression half, and the one that licenses the identity rotation.

    S-12's stored evidence is reproducible across the rotation only if a certified
    carrier changes nothing. Asserted against the full verdict list, not against a
    fired-count — a count would hide a moved reason code.
    """
    series = _above_edge()
    certified = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=_certified(series))
    # The pre-change behaviour, reconstructed: every bar certified is exactly the
    # state in which the new input can never refuse.
    assert [(s.signal_index, s.verdict, s.reason) for s in certified] == [
        (s.signal_index, s.verdict, s.reason)
        for s in s12_signals(
            series,
            universe=UNIVERSE,
            masked_reason=REASON,
            price_basis=PriceBasisSeries(values=("observed_unadjusted",) * len(series)),
        )
    ]
    assert any(s.verdict == "fired" for s in certified), "fixture must still fire, or the equality is vacuous"


def test_the_short_circuit_matches_the_declared_input_path() -> None:
    """⚠⚠ A FAST PATH THAT DISAGREES WITH ITS RULE IS WORSE THAN THE COST IT SAVES.

    The all-refused short-circuit exists because ``_unevaluable_reason_at`` does a
    tuple membership test per bar, which is O(n²) when every bar is refused. It is
    only legitimate if it reproduces what ``evaluate`` would have returned — and
    the subtle half is the LAST bar, which ``evaluate`` answers ``no_fill_bar``
    before reading any input at all.

    Reconstructed here by refusing all but one bar, which forces the declared-input
    path, and comparing the shared prefix.
    """
    series = _above_edge()
    n = len(series)
    short_circuited = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=_withheld(series))

    # One certified bar is enough to defeat ``certifies_nothing`` and route through
    # ``evaluate`` with the carrier declared as an input.
    nearly_all = PriceBasisSeries(
        values=tuple("observed_unadjusted" if index == 0 else None for index in range(n)),
        not_evaluable_indices=tuple(range(1, n)),
    )
    declared = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=nearly_all)

    assert [(s.verdict, s.reason) for s in short_circuited[1:]] == [(s.verdict, s.reason) for s in declared[1:]]
    assert short_circuited[-1].reason == "no_fill_bar", "evaluate answers the last bar before reading any input"


def test_the_basis_refusal_wins_over_a_masked_bar() -> None:
    """⚠ Input ORDER is the rule. ``_unevaluable_reason_at`` returns the FIRST
    declared input's reason among competing data reasons, and the basis is declared
    first: the others say *this bar's data is unusable*, this one says *this rule
    may not read this corpus at all*.
    """
    series = _above_edge()
    rows = list(series.rows)
    rows[FIRING_INDEX] = {"open": None, "high": None, "low": None, "close": None, "volume": 1_000}  # type: ignore[typeddict-item]
    masked_series = BarSeries(dates=series.dates, rows=tuple(rows))

    basis = PriceBasisSeries(
        values=tuple(None if index == FIRING_INDEX else "observed_unadjusted" for index in range(len(masked_series))),
        not_evaluable_indices=(FIRING_INDEX,),
    )
    signals = s12_signals(masked_series, universe=UNIVERSE, masked_reason=REASON, price_basis=basis)
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", PRICE_BASIS_REFUSAL_REASON)


@pytest.mark.parametrize("delta", [-1, 1])
def test_a_misaligned_carrier_raises_in_both_directions(delta: int) -> None:
    """⚠⚠ The SHORT case is the dangerous one and is why this check exists.

    ``evaluate`` answers the last bar ``no_fill_bar`` before reading any input, so
    a carrier one element short is never looked up: without this raise it passes
    silently while every certification sits one bar off. Codex checkpoint 1 probed
    it directly.
    """
    series = _above_edge()
    with pytest.raises(ValueError, match="price_basis has .* bars against"):
        s12_signals(
            series,
            universe=UNIVERSE,
            masked_reason=REASON,
            price_basis=from_archive_basis("unadjusted", n_bars=len(series) + delta),
        )


def test_a_certified_carrier_evaluates_on_the_scan_universe() -> None:
    """⚠⚠ THE INVERSION, and the single test the whole of #2840 §6 item 3 reads off.

    Until this change ``AS_TRADED_UNIVERSES`` refused every ``survivor_only`` bar by
    NAME, whatever the carrier said. The verdict now follows the BASIS: a certified
    carrier fires here exactly as it would on the backtest universe.

    ⚠ This does NOT mean the live scan is certified. The production scan builds its
    carrier with ``from_undeclared_source`` and still refuses every bar — see
    ``test_the_scan_declares_no_source``. What moved is which fact decides.
    """
    series = _above_edge()
    signals = s12_signals(series, universe="survivor_only", masked_reason=REASON, price_basis=_certified(series))
    assert _verdict_at(signals, FIRING_INDEX) == ("fired", None)
    on_backtest_universe = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=_certified(series))
    assert [(s.verdict, s.reason) for s in signals] == [(s.verdict, s.reason) for s in on_backtest_universe]


def test_an_uncertified_carrier_refuses_on_the_backtest_universe() -> None:
    """⚠ The other half of the pair, and together they prove the verdict follows the
    BASIS and not the universe: same universe as the fire above, opposite carrier,
    opposite verdict.

    ⚠ The last bar is ``no_fill_bar`` rather than the basis refusal — the
    short-circuit routes through ``evaluate``, which stamps a segment's final bar
    before reading any input. The removed universe token refused uniformly and so
    disagreed with ``evaluate`` on exactly that bar.
    """
    series = _above_edge()
    signals = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=_withheld(series))
    assert _verdict_at(signals, FIRING_INDEX) == ("not_evaluable", PRICE_BASIS_REFUSAL_REASON)
    assert (signals[-1].verdict, signals[-1].reason) == ("not_evaluable", "no_fill_bar")


def test_the_scan_declares_no_source() -> None:
    """⚠⚠ THE TRIPWIRE §3 OF THE SPEC RESTS ON, and its limit is stated there.

    Removing the universe token left ``SCAN_ARCHIVE_ADJUSTMENT_BASIS`` as the only
    thing between the live scan and a nominal gate on back-adjusted ``price_daily``
    levels — one token edit away. That constant is gone; the scan reaches the carrier
    through ``from_undeclared_source``, which has no token to flip.

    ⚠ It does NOT prevent an inline ``PriceBasisSeries(values=("observed_unadjusted",)
    * n)``, which is public and structural (Codex checkpoint 1). This catches the
    cheap mistake, not a determined one.
    """
    source = Path("app/services/strategy_signal_scan.py").read_text()
    assert "from_undeclared_source" in source
    assert "from_archive_basis" not in source, (
        "the scan has no pinned archive; building its carrier from an archive basis is the "
        "misdeclaration #2840 §6 item 3 removed"
    )
    assert "SCAN_ARCHIVE_ADJUSTMENT_BASIS" not in source


def test_no_member_adapter_forwards_the_carrier() -> None:
    """⚠ Keeps the CROSS-SECTIONAL path out of scope by test rather than by inspection.

    S-12 is ``per_series`` with no ``member``, so ``segmented_member`` never dispatches
    it, and every cross-sectional adapter discards ``price_basis``. If one starts
    forwarding it, ``stage_cross_sectional_member``'s pre-input terminal refusal becomes
    reachable for a basis reason and needs its own analysis — plus that is a second
    consumer, so ``INPUT_RULE_SETS`` is owed its sixth entry.
    """
    entry = STRATEGY_MANIFEST[S12_STRATEGY_ID]
    assert entry.strategy_class == "per_series"
    assert entry.member is None
    forwarding = sorted(
        strategy_id
        for strategy_id, other in STRATEGY_MANIFEST.items()
        if other.member is not None and "price_basis=price_basis" in inspect.getsource(other.member)
    )
    assert forwarding == [], f"{forwarding} now forward the carrier through a MemberStager"


def test_a_carrier_built_under_another_rule_version_raises() -> None:
    """⚠⚠ The identity CLAIMS a rule version; the input must be that version.

    ``S12_PARAMS["price_basis_rule"]`` hashes ``PRICE_BASIS_RULE_VERSION`` in, so a
    stale carrier would have its verdicts stored under a version asserting a rule that
    did not produce them. It RAISES rather than refusing: a caller holding a stale
    carrier has a wiring bug, and a refusal would write that bug into the ledger as a
    data condition.
    """
    series = _above_edge()
    stale = PriceBasisSeries(
        values=("observed_unadjusted",) * len(series),
        rule_set_version="price-basis-carrier-v0+deadbeefcafe+archives-deadbeefcafe",
    )
    with pytest.raises(ValueError, match="stale carrier cannot certify"):
        s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=stale)


def test_the_rule_version_is_carried_in_the_identity_params() -> None:
    assert S12_PARAMS["price_basis_rule"] == PRICE_BASIS_RULE_VERSION


# ----------------------------------------------------------------- segment mechanics


def _segmented(series: BarSeries, *, breaks: tuple[date, ...], certified: bool) -> list[StrategySignal]:
    """S-12 through the production dispatcher, which slices per price-scale segment."""
    basis = _certified(series) if certified else _withheld(series)
    return segmented_signals(
        STRATEGY_MANIFEST[S12_STRATEGY_ID],
        series,
        universe=UNIVERSE,
        masked_reason=REASON,
        unresolved_breaks=breaks,
        regime=unconstrained_regime(len(series)),
        price_basis=basis,
    )


def _terminus_indices(series: BarSeries, breaks: tuple[date, ...]) -> list[int]:
    return [end - 1 for _, end in series_segment_bounds(series, unresolved_breaks=breaks)]


@pytest.mark.parametrize(
    "break_offsets",
    [
        pytest.param((1,), id="singleton-first-segment"),
        pytest.param((40, 120), id="two-boundaries"),
        pytest.param((len(_above_edge()) - 1,), id="singleton-last-segment"),
    ],
)
def test_every_segment_terminus_is_no_fill_bar_not_the_basis_refusal(break_offsets: tuple[int, ...]) -> None:
    """⚠⚠ "LAST BAR" MEANS THE SEGMENT'S LAST BAR, and that is the one verdict this
    change moves (#2840 §6 item 3).

    The removed universe token refused uniformly; the carrier short-circuit routes
    through ``evaluate``, which stamps a segment terminus ``no_fill_bar`` before reading
    any input. A segment terminus is a MID-SERIES bar, so unlike the series end it can
    fall inside the scan's write window. Measured before the removal: 0 of the 5,791
    stored S-12 observations sit at one.
    """
    series = _above_edge()
    breaks = tuple(series.dates[offset] for offset in break_offsets)
    signals = _segmented(series, breaks=breaks, certified=False)
    termini = _terminus_indices(series, breaks)
    assert len(termini) == len(break_offsets) + 1
    for index, signal in enumerate(signals):
        expected = "no_fill_bar" if index in termini else PRICE_BASIS_REFUSAL_REASON
        assert (signal.verdict, signal.reason) == ("not_evaluable", expected), f"bar {index}"


def test_a_break_between_delivered_dates_cuts_at_the_bisect_position() -> None:
    """⚠ ``series_segment_bounds`` cuts at ``bisect_left(dates, break_date)``, so a break
    on a NON-TRADING day still lands — at the first delivered bar at or after it. Pinned
    because the terminus is then the bar BEFORE a date that is not in the series at all,
    which is the case a reader is most likely to get wrong.
    """
    # ⚠ The fixture's dates are CONSECUTIVE, so an absent date has to be made: drop
    # bar 60 and break on the date it used to hold. Without the drop, "the day before
    # bar 60" is itself bar 59 and the test would be the ordinary aligned case wearing
    # this test's name.
    full = _above_edge()
    absent = full.dates[60]
    gapped = BarSeries(
        dates=full.dates[:60] + full.dates[61:],
        rows=full.rows[:60] + full.rows[61:],
    )
    assert absent not in gapped.dates
    assert _terminus_indices(gapped, (absent,)) == [59, len(gapped) - 1]
    signals = _segmented(gapped, breaks=(absent,), certified=False)
    assert (signals[59].verdict, signals[59].reason) == ("not_evaluable", "no_fill_bar")
    assert (signals[58].verdict, signals[58].reason) == ("not_evaluable", PRICE_BASIS_REFUSAL_REASON)
    assert (signals[60].verdict, signals[60].reason) == ("not_evaluable", PRICE_BASIS_REFUSAL_REASON)
