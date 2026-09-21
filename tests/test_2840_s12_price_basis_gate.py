"""#2840 — S-12's declared price-basis input, and the two paths that must agree.

Separate from ``test_2840_price_basis_carrier.py`` on purpose: that file pins the
CARRIER, this one pins the CONSUMER. The distinction matters because the carrier
is general and the gate is S-12's alone.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

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
from app.services.strategy_manifest import PRICE_BASIS_CONSUMERS, STRATEGY_MANIFEST
from app.services.strategy_price_basis import (
    PRICE_BASIS_RULE_VERSION,
    PriceBasisSeries,
    bindings_for,
    from_archive_basis,
    from_undeclared_source,
)
from app.services.strategy_registry import StrategySignal
from app.services.strategy_segmented_evaluation import segmented_signals
from app.services.technical_analysis import OHLCVRow
from tests.test_2840_cheapest_band_price_gated_breakout import (
    FIRING_INDEX,
    REASON,
    UNIVERSE,
    _above_edge,
    _verdict_at,
)


def _certified(series: BarSeries) -> PriceBasisSeries:
    return from_archive_basis("unadjusted", series=series)


def _withheld(series: BarSeries) -> PriceBasisSeries:
    return from_archive_basis(None, series=series)


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
            price_basis=PriceBasisSeries(
                values=("observed_unadjusted",) * len(series), bar_bindings=bindings_for(series)
            ),
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
        bar_bindings=bindings_for(series),
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
        bar_bindings=bindings_for(masked_series),
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
    # ⚠ The carrier is built from a DIFFERENT-LENGTH series, because the constructor
    # now takes the series rather than a count (#2840 §8b) — a miscounted ``n_bars``
    # is no longer expressible, so the misalignment has to come from a genuinely
    # different series. The length check must still fire BEFORE the binding check,
    # whose message would otherwise mask it.
    end = len(series) + delta
    misaligned = (
        BarSeries(dates=series.dates[:end], rows=series.rows[:end])
        if delta < 0
        else BarSeries(
            dates=(*series.dates, series.dates[-1] + timedelta(days=1)), rows=(*series.rows, series.rows[-1])
        )
    )
    with pytest.raises(ValueError, match="price_basis has .* bars against"):
        s12_signals(
            series,
            universe=UNIVERSE,
            masked_reason=REASON,
            price_basis=from_archive_basis("unadjusted", series=misaligned),
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


def test_no_member_adapter_reads_the_carrier() -> None:
    """⚠ Keeps the CROSS-SECTIONAL path out of scope by test rather than by inspection.

    S-12 is ``per_series`` with no ``member``, so ``segmented_member`` never dispatches
    it, and every cross-sectional adapter discards ``price_basis``. If one starts reading
    it, ``stage_cross_sectional_member``'s pre-input terminal refusal becomes reachable
    for a basis reason and needs its own analysis — plus that is a second consumer, so
    ``INPUT_RULE_SETS`` is owed its sixth entry.

    ⚠⚠ ASSERTED BEHAVIOURALLY, NOT BY A SOURCE SUBSTRING (review bot NITPICK on the
    first version, and it was right). That version looked for the literal
    ``"price_basis=price_basis"`` in ``inspect.getsource``, which a multiline call or a
    reordered kwarg defeats — a textual gate that passes on a technicality is worse than
    no gate, because it reads as coverage. Here every member adapter is CALLED twice,
    once with a fully certified carrier and once with one that certifies nothing, and the
    two results must be identical. An adapter that reads the carrier cannot satisfy that
    however it is formatted.

    ⚠ Detection power probed rather than assumed, in both directions:
    ``CrossSectionalMember`` equality is structural (same input → equal, so the assertion
    is not identity-vacuous), both real adapters compare equal across the two carriers,
    and a wrapper that stages from a different view when ``certifies_nothing()`` IS
    flagged.

    ⚠ Residual limit: an adapter that reads the carrier and happens to produce an
    identical ``CrossSectionalMember`` either way is undetected. That is far narrower
    than the substring version's limit, and such an adapter is not yet a consumer in the
    sense ``INPUT_RULE_SETS`` cares about — its verdicts do not depend on the rule.
    """
    entry = STRATEGY_MANIFEST[S12_STRATEGY_ID]
    assert entry.strategy_class == "per_series"
    assert entry.member is None

    series = _above_edge()
    panel_dates = frozenset(series.dates)
    members = {
        strategy_id: other.member for strategy_id, other in STRATEGY_MANIFEST.items() if other.member is not None
    }
    assert members, "no MemberStager in the manifest — this test would be vacuous"
    reading: list[str] = []
    for strategy_id, member in members.items():
        staged = [
            member(
                series,
                panel_decision_dates=panel_dates,
                universe=UNIVERSE,
                masked_reason=REASON,
                regime=unconstrained_regime(len(series)),
                price_basis=basis,
            )
            for basis in (_certified(series), _withheld(series))
        ]
        if staged[0] != staged[1]:
            reading.append(strategy_id)
    assert reading == [], f"{reading} now read the carrier through a MemberStager"


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
        bar_bindings=bindings_for(series),
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


# ------------------------------------------------- the binding (#2840 §8 obligation b)


def _foreign(series: BarSeries, *, at: int) -> BarSeries:
    """``series`` with ONE close changed — same length, same dates, different bars."""
    rows = list(series.rows)
    rows[at] = cast(OHLCVRow, {**rows[at], "close": Decimal("7.77")})
    return BarSeries(dates=series.dates, rows=tuple(rows))


def test_a_carrier_built_for_another_series_of_the_same_length_raises() -> None:
    """⚠⚠ THE DEFECT #2840 §8 RECORDED, closed here, and it was REACHABLE.

    Codex refused "not reachable today" by constructing it. Measured before this
    change, with the carrier built at ``n_bars=len(series_B)`` and handed to
    series A: ``Counter({'not_evaluable': 114, 'not_fired': 60, 'fired': 1})`` —
    accepted, and S-12 FIRED on a certificate that was never about these bars.

    ⚠ Asserted through BOTH consumers. ``s12_signals`` is the direct call; the
    scan reaches it through ``segmented_signals``, which slices the carrier and is
    the last point that still holds the series to check against.
    """
    mine = _above_edge()
    theirs = _foreign(mine, at=FIRING_INDEX - 40)
    carrier = from_archive_basis("unadjusted", series=theirs)
    assert len(carrier) == len(mine), "the counterexample is a SAME-LENGTH carrier, not a misaligned one"

    with pytest.raises(ValueError, match="built for different bars"):
        s12_signals(mine, universe=UNIVERSE, masked_reason=REASON, price_basis=carrier)
    with pytest.raises(ValueError, match="built for different bars"):
        segmented_signals(
            STRATEGY_MANIFEST[S12_STRATEGY_ID],
            mine,
            universe=UNIVERSE,
            masked_reason=REASON,
            unresolved_breaks=(),
            regime=unconstrained_regime(len(mine)),
            price_basis=carrier,
        )


def test_an_UNCERTIFIED_bar_is_bound_too_because_it_feeds_the_certified_verdict() -> None:
    """⚠⚠ THE EXPLOIT THAT KILLED "BIND ONLY THE CERTIFIED BARS" (Codex checkpoint 1).

    An uncertified bar's OHLC still feeds ATR, compression and prior-high for later
    bars — this module's own header says so — so binding only certifications leaves
    the inputs that PRODUCE a certified verdict unbound. Reproduced on this fixture:
    changing bar 169's close alone flips bar 170 from ``fired`` to ``not_fired``,
    and under the narrower rule every binding still passed.

    The fix is that a carrier binds EVERY bar whenever it certifies ANY, so this
    now raises instead of returning a different verdict.
    """
    mine = _above_edge()
    hole = FIRING_INDEX - 1
    built_for = _foreign(mine, at=hole)
    partial = PriceBasisSeries(
        values=tuple(None if index == hole else "observed_unadjusted" for index in range(len(mine))),
        not_evaluable_indices=(hole,),
        bar_bindings=bindings_for(built_for),
    )
    with pytest.raises(ValueError, match="built for different bars"):
        s12_signals(mine, universe=UNIVERSE, masked_reason=REASON, price_basis=partial)


def test_a_retained_row_dict_can_no_longer_move_the_series_under_the_carrier() -> None:
    """⚠⚠ THIS TEST CHANGED MEANING DELIBERATELY — it was not repaired (#2840).

    It used to mutate a retained row ``dict`` after construction and assert the
    carrier REJECTED the series, because ``OHLCVRow`` is a plain mutable
    ``dict`` and ``BarSeries`` aliased the caller's objects. ``BarSeries`` now
    copies each row and wraps it in a ``MappingProxyType``, so the mutation
    cannot reach the series at all and there is nothing left to reject.

    ⚠ Rewriting it to assert "binding passes" alone would DEFANG it — that
    assertion holds even if the binding check were deleted. So the old content
    was split and both halves are still asserted:

    * the caller's retained dicts cannot reach the series, cold and warm —
      ``tests/test_2840_barseries_row_immutability.py``;
    * the binding is a VALUE SNAPSHOT and not a live reference — pinned below
      against the independently computed encoding.

    ⚠⚠ The obvious replacement for the second half — "build a changed series
    separately and check the carrier rejects it" — does NOT work, and Codex
    checkpoint 1 refused it by construction: a carrier holding a live REFERENCE
    to the original series also rejects a different series, so that assertion
    cannot tell the two designs apart. Comparing against the independently
    recomputed encoding can, because a reference-backed carrier has no encoding
    to compare.
    """
    rows = [dict(row) for row in _above_edge().rows]
    series = BarSeries(dates=_above_edge().dates, rows=cast(tuple[OHLCVRow, ...], tuple(rows)))
    carrier = from_archive_basis("unadjusted", series=series)
    assert carrier.binding_mismatch(series) is None

    # The snapshot is VALUES, taken at construction, and equal to what an
    # independent encoding of the same bars produces.
    assert carrier.bar_bindings == bindings_for(series)

    before = series.rows[FIRING_INDEX]["close"]
    rows[FIRING_INDEX]["close"] = Decimal("7.77")
    assert series.rows[FIRING_INDEX]["close"] == before
    assert carrier.bar_bindings == bindings_for(series)
    assert carrier.binding_mismatch(series) is None
    s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=carrier)


def test_the_scans_undeclared_carrier_is_unbound_and_still_refuses_every_bar() -> None:
    """⚠ The hot path pays NOTHING, and that is safe rather than an omission: a
    carrier that certifies no bar cannot certify a foreign one. Asserted as the
    pair — unbound AND uniformly refusing — because either half alone is vacuous.
    """
    series = _above_edge()
    carrier = from_undeclared_source(series=series)
    assert carrier.bar_bindings == ()
    signals = s12_signals(series, universe=UNIVERSE, masked_reason=REASON, price_basis=carrier)
    assert signals, "an empty verdict list would make the set assertion below vacuous"
    assert {s.verdict for s in signals} == {"not_evaluable"}


def test_only_the_declared_consumers_verdicts_depend_on_the_carrier() -> None:
    """⚠⚠ THE PIN UNDER ``PRICE_BASIS_CONSUMERS``, asserted BEHAVIOURALLY.

    ``backtest_run._signals_for`` builds a CERTIFYING carrier only for the
    strategies in that set, because binding every bar costs ~0.8 µs to encode plus
    ~0.8 µs to re-check (measured) and eleven of the twelve adapters discard the
    carrier. That routing is only sound while the set is exactly the strategies
    whose verdicts move with it.

    So every per-series adapter is CALLED TWICE — once with a fully certified
    carrier, once with one that certifies nothing — and:

    * a strategy IN the set must DISAGREE (or the set has a passenger, and the
      assertion below would be vacuous);
    * a strategy OUTSIDE it must AGREE (or it is an undeclared consumer that the
      backtest is now silently handing an all-refusing carrier).

    ⚠ Not a source substring. The cross-sectional sibling above records why: a
    textual gate is defeated by a multiline call or a reordered kwarg, and one
    that passes on a technicality is worse than none because it reads as coverage.
    """
    series = _above_edge()
    regime = unconstrained_regime(len(series))
    certified = _certified(series)
    undeclared = from_undeclared_source(series=series)

    disagreed: set[str] = set()
    checked = 0
    for strategy_id, entry in sorted(STRATEGY_MANIFEST.items()):
        if entry.signals is None:
            continue
        checked += 1
        both = [
            entry.signals(series, universe=UNIVERSE, masked_reason=REASON, regime=regime, price_basis=carrier)
            for carrier in (certified, undeclared)
        ]
        if [(s.signal_index, s.verdict, s.reason) for s in both[0]] != [
            (s.signal_index, s.verdict, s.reason) for s in both[1]
        ]:
            disagreed.add(strategy_id)

    assert checked >= 10, f"only {checked} per-series adapters were exercised; the comparison must not be near-empty"
    assert disagreed == set(PRICE_BASIS_CONSUMERS), (
        f"{sorted(disagreed)} have verdicts that move with the carrier but PRICE_BASIS_CONSUMERS is "
        f"{sorted(PRICE_BASIS_CONSUMERS)}; backtest_run routes the certifying carrier by that set, so a "
        "strategy missing from it is handed an all-refusing carrier and refuses every bar"
    )
