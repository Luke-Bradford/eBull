"""#2840 §8 obligation (a) — the carrier's SOURCE SELECTION is outside every identity hash.

``s12_signals`` already checks the carrier's ``rule_set_version`` against the one
``S12_PARAMS`` hashes, so a STALE carrier raises. Nothing checks which CONSTRUCTOR
built it, and the two constructors carry the same ``rule_set_version``. Measured at
``69c17204`` on ``_above_edge()`` (175 bars), ``universe=SCAN_UNIVERSE``,
``masked_reason=MASKED_REASON``, everything else held::

    s12_identity(universe="survivor_only").version  = strategy-registry-v1+6b4b504f4fdf
    carrier rule_set_version, undeclared vs archive: IDENTICAL
    from_undeclared_source: not_evaluable/missing_market_context 174, no_fill_bar 1
    from_archive_basis:     insufficient_warmup 113, not_fired 60, fired 1, no_fill_bar 1

⇒ **61 of 175 verdict LABELS move, 174 of 175 (verdict, reason) pairs move**, and no
hashed input moves. ⚠ Not "every bar": the terminal ``no_fill_bar`` is unchanged,
because ``evaluate`` returns it before reading any input. ``corpus_generation`` does
not move either — it digests the bar values a pass read plus the regime, the
unresolved breaks, the quarantine rule version and the frontier date, never a
constructor choice. So a one-line edit at ``strategy_signal_scan.py:987`` would write
certified S-12 decisions under a ``(strategy_id, strategy_version,
corpus_generation)`` a reader cannot tell from the uncertified ones already stored.

⚠ What that measures is carrier SENSITIVITY under a fixed identity, and not more:
swapping constructors also changes the carrier's values and routes S-12 through its
all-refused short-circuit. Sufficient for the claim made here; not for a stronger one.

The live trigger is a code edit, so the guard is a test.

⚠⚠ WHY THIS IS NOT THE OBVIOUS TEST, AND THE OBVIOUS TEST IS A TAUTOLOGY.
``PRICE_BASIS_REFUSAL_REASON`` is ``"missing_market_context"`` — the SAME code an
absent regime produces. A test that drove the scan and asserted that reason would
pass just as happily on a fixture whose regime was simply missing, which is this
repo's own recorded defect shape (prevention log: *"a test of a refusal must
construct the fixture so that the refusal's output differs from every plausible
wrong path's output"*). So the refusal is pinned by its VERDICT SET and paired with
:func:`test_the_same_fixture_fires_under_a_certified_carrier`, which shows the same
bars and the same regime reaching ``fired`` when only the carrier changes. The pair
is the discriminator; neither half is one alone.

⚠ NOT A SPY ON THE DISPATCHERS. Monkeypatching ``segmented_signals`` to capture the
``price_basis`` argument was the first design and Codex checkpoint 1 refused it:
replacing the dispatcher removes the forwarding, the segment remap and S-12's own
consumption — the whole behaviour the carrier participates in — so it can pass while
an adapter discards the carrier entirely. This drives the real ``_scan_per_series``.

⚠ ONE SITE, NOT TWO. ``_stage_cross_sectional`` is not reachable for S-12:
``run_signal_scan`` dispatches on ``entry.strategy_class == "per_series"``
(``strategy_signal_scan.py:882``) and S-12 is per-series with no member function, so
calling the cross-sectional path with it raises rather than certifying anything.
Claiming this covers both scan call sites would be false.

Spec: ``docs/proposals/ta/2026-09-21-2840-carrier-source-selection-tripwire.md``.
Refs #2840, #2437.
"""

from __future__ import annotations

from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.market_regime import Regime
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.strategies.s12_cheapest_band_price_gated_breakout import S12_STRATEGY_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_price_basis import from_archive_basis
from app.services.strategy_segmented_evaluation import segmented_signals
from app.services.strategy_signal_scan import (
    MASKED_REASON,
    SCAN_UNIVERSE,
    LedgerRow,
    _Plan,
    _scan_per_series,
)
from tests.test_2840_cheapest_band_price_gated_breakout import _above_edge


def _plan() -> _Plan:
    entry = STRATEGY_MANIFEST[S12_STRATEGY_ID]
    identity = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID)
    return _Plan(entry=entry, identity=identity, version=identity.version, watermark=None)


def _provider(series: BarSeries) -> MarketRegimeProvider:
    """A regime CLASSIFIED on every bar.

    ⚠ Load-bearing, not scaffolding. ``for_dates`` returns ``not_evaluable`` for a
    date absent from this map, and that refusal carries the same reason code as the
    price-basis refusal. Populating every date is what leaves the carrier as the only
    thing in the fixture that can produce ``missing_market_context``.
    """
    return MarketRegimeProvider(regime_by_date={when: Regime.BULL_QUIET for when in series.dates})


def test_the_scan_path_certifies_no_bar() -> None:
    """The tripwire. Swapping ``from_undeclared_source`` at the call site fails this.

    ⚠ ``assert out`` first, and it is not decoration: every other assertion here is
    over a comprehension, and ``all(...)`` / a set comparison over an EMPTY row list
    passes vacuously. A fixture that stopped producing rows would otherwise turn this
    tripwire off silently, which is worse than not having it.
    """
    series = _above_edge()
    out: list[LedgerRow] = []
    _scan_per_series(
        _plan(),
        series,
        instrument_id=1,
        window=range(len(series)),
        out=out,
        regime_provider=_provider(series),
    )

    assert out, "the fixture produced no ledger rows; every assertion below would pass vacuously"
    # ⚠ The VERDICT set, not the reason set. A certified carrier produces `fired` and
    # `not_fired`; the reason code alone cannot separate a price-basis refusal from a
    # regime one, so the reason is checked for PRESENCE below rather than used as the
    # discriminator.
    assert {row.verdict for row in out} == {"not_evaluable"}
    assert any(row.not_evaluable_reason == "missing_market_context" for row in out)


def test_the_same_fixture_fires_under_a_certified_carrier() -> None:
    """The other half of the discriminator — and the reason the first test means something.

    Same bars, same regime, same universe, same masked reason. Only the carrier's
    SOURCE differs, and the verdicts move. Without this, the first test is satisfied
    by any fixture that refuses for any reason at all.
    """
    series = _above_edge()
    signals = segmented_signals(
        STRATEGY_MANIFEST[S12_STRATEGY_ID],
        series,
        universe=SCAN_UNIVERSE,
        masked_reason=MASKED_REASON,
        unresolved_breaks=(),
        regime=_provider(series).for_dates(series.dates),
        price_basis=from_archive_basis("unadjusted", series=series),
    )

    assert signals, "the fixture produced no signals; the comparison below would be vacuous"
    assert any(signal.verdict == "fired" for signal in signals)
    assert {signal.verdict for signal in signals} != {"not_evaluable"}
