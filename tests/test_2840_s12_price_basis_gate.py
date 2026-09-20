"""#2840 — S-12's declared price-basis input, and the two paths that must agree.

Separate from ``test_2840_price_basis_carrier.py`` on purpose: that file pins the
CARRIER, this one pins the CONSUMER. The distinction matters because the carrier
is general and the gate is S-12's alone.
"""

from __future__ import annotations

import pytest

from app.services.indicator_series import BarSeries
from app.services.strategies.s12_cheapest_band_price_gated_breakout import (
    PRICE_BASIS_REFUSAL_REASON,
    S12_PARAMS,
    s12_signals,
)
from app.services.strategy_price_basis import PRICE_BASIS_RULE_VERSION, PriceBasisSeries, from_archive_basis
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


def test_the_universe_gate_still_fires_first_on_the_scan_universe() -> None:
    """``AS_TRADED_UNIVERSES`` stays until #2840 §6 item 3 removes it.

    That is what keeps the 5,791 stored ``survivor_only`` observations reproducible
    across the identity rotation: their content is this refusal, and it is unmoved.
    """
    series = _above_edge()
    signals = s12_signals(series, universe="survivor_only", masked_reason=REASON, price_basis=_certified(series))
    assert {(s.verdict, s.reason) for s in signals} == {("not_evaluable", PRICE_BASIS_REFUSAL_REASON)}


def test_the_rule_version_is_carried_in_the_identity_params() -> None:
    assert S12_PARAMS["price_basis_rule"] == PRICE_BASIS_RULE_VERSION
