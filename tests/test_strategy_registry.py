"""Phase 3a — the registry contract.

Pure, no DB. Spec:
`docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md`.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import (
    BarSeries,
    IndicatorSeries,
    sma_series,
)
from app.services.strategy_registry import (
    PARENT_REASON_CODES,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)
from app.services.technical_analysis import OHLCVRow

U = "survivor_only"


def _bars(closes: list[float]) -> BarSeries:
    rows: list[OHLCVRow] = [
        {
            "open": Decimal(str(c)),
            "high": Decimal(str(c + 1)),
            "low": Decimal(str(c - 1)),
            "close": Decimal(str(c)),
            "volume": 100,
        }
        for c in closes
    ]
    start = date(2024, 1, 1)
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


_CLOSES = [100 + (i % 7) - 3 + i * 0.1 for i in range(60)]


class TestSignalShapeForbidsSameBarFill:
    """⚠ The fill rule is enforced by the SHAPE of this API, not a constraint.

    The spec's first draft claimed a `CHECK (fill_bar_date > signal_bar_date)`
    was "the whole mechanism". It is not — a writer can record
    `signal_bar_date = t-1`, fill on `t`, and use bar `t`'s data with every
    constraint passing. What actually makes a same-bar fill impossible is that
    a strategy has no way to express one.
    """

    def test_a_signal_carries_no_fill_field_at_all(self) -> None:
        signal = StrategySignal(verdict="fired", signal_index=5)
        fields = set(signal.__dataclass_fields__)
        assert fields == {"verdict", "signal_index", "kind", "reason"}
        # No fill_price, fill_date, fill_bar, fill_index — the capability is
        # absent, not guarded.
        assert not any("fill" in f for f in fields)


class TestReasonCodeContract:
    """Parent criterion 8: `not_evaluable` carries a reason CODE, not text."""

    def test_not_evaluable_requires_a_reason(self) -> None:
        with pytest.raises(ValueError, match="requires a reason code"):
            StrategySignal(verdict="not_evaluable", signal_index=3)

    def test_a_reason_on_a_decided_verdict_is_rejected(self) -> None:
        """A reason beside `fired` would make the code uncountable — criterion
        9 needs to count them to 'measure what you reject'."""
        with pytest.raises(ValueError, match="meaningless"):
            StrategySignal(verdict="fired", signal_index=3, reason="missing_volume")

    def test_the_seven_parent_codes_are_carried_verbatim(self) -> None:
        assert PARENT_REASON_CODES == {
            "missing_volume",
            "missing_spread",
            "insufficient_warmup",
            "quarantined_bar",
            "series_break",
            "not_listed",
            "ambiguous_intrabar",
        }

    def test_no_fill_bar_is_ours_and_is_not_claimed_as_the_parents(self) -> None:
        """⚠ An addition, flagged rather than smuggled. If it silently joined
        PARENT_REASON_CODES a reader would attribute it to criterion 8."""
        assert "no_fill_bar" not in PARENT_REASON_CODES


class TestEvaluabilityBeatsShortCircuit:
    """⚠⚠ The hole Codex found at checkpoint 1.

    Python's `and` short-circuits, so a strategy returns False on the first
    failing condition WITHOUT touching a later unevaluable input — reporting
    `not_fired` for a bar it could not judge. That is decision 5's corruption
    re-entering after being closed at the indicator layer.
    """

    @staticmethod
    def _inputs_with_hole(hole: int) -> tuple[IndicatorSeries, IndicatorSeries]:
        series = _bars(_CLOSES)
        good = sma_series(series, universe=U, period=5)
        holed = IndicatorSeries(
            values=tuple(None if i == hole else 1.0 for i in range(len(series))),
            universe=U,
            not_evaluable_indices=(hole,),
        )
        return good, holed

    def test_short_circuiting_body_still_yields_not_evaluable(self) -> None:
        hole = 30
        good, holed = self._inputs_with_hole(hole)

        # A body that short-circuits: it never reads `holed` when the first
        # condition fails. Under the old contract this returned not_fired.
        calls: list[int] = []

        def body(i: int) -> bool:
            calls.append(i)
            return False  # first condition always fails; second never reached

        signals = evaluate(
            body,
            inputs=[StrategyInput(good, "missing_volume"), StrategyInput(holed, "quarantined_bar")],
            n_bars=len(good),
        )

        assert signals[hole].verdict == "not_evaluable"
        # ⚠ the reason is the INPUT's, not a blanket warm-up code
        assert signals[hole].reason == "quarantined_bar"
        # ⚠ The body was never invoked on the holed bar — that is the mechanism.
        assert hole not in calls

    def test_body_never_sees_a_none(self) -> None:
        """Inside a body a `None` is impossible by construction, so a strategy
        author cannot write the bug even carelessly."""
        good, holed = self._inputs_with_hole(30)

        def body(i: int) -> bool:
            assert good.values[i] is not None
            assert holed.values[i] is not None
            return True

        evaluate(
            body,
            inputs=[StrategyInput(good, "missing_volume"), StrategyInput(holed, "quarantined_bar")],
            n_bars=len(good),
        )


class TestLastBarHasNoFill:
    def test_final_bar_is_no_fill_bar(self) -> None:
        """A signal on the last bar has no t+1 to fill at. Reporting it as
        `fired` would hand the backtester an unenterable trade."""
        series = _bars(_CLOSES)
        sma = sma_series(series, universe=U, period=5)
        signals = evaluate(lambda i: True, inputs=[StrategyInput(sma, "missing_volume")], n_bars=len(series))

        assert signals[-1].verdict == "not_evaluable"
        assert signals[-1].reason == "no_fill_bar"
        # ...and the bar before it is a normal decision.
        assert signals[-2].verdict == "fired"


class TestIdentityCoversMoreThanSource:
    """Parent criterion 11: identity covers code, params, universe and cost
    model. Hashing module source alone (the first draft, copied from
    `indicator_series`) misses universe and cost model entirely — two different
    strategies would share a version and collide on the ledger key."""

    @staticmethod
    def _identity(**overrides: object) -> StrategyIdentity:
        base: dict[str, object] = {
            "strategy_id": "S-1",
            "params": {"period": 200},
            "universe": "survivor_only",
            "cost_model_id": "etoro-v1",
            "source_hash": "abc123",
        }
        base.update(overrides)
        return StrategyIdentity(**base)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "override",
        [
            {"params": {"period": 50}},
            {"universe": "survivorship_free"},
            {"cost_model_id": "etoro-v2"},
            {"source_hash": "def456"},
            {"strategy_id": "S-2"},
        ],
    )
    def test_every_component_changes_the_version(self, override: dict[str, object]) -> None:
        assert self._identity().version != self._identity(**override).version

    def test_identical_identities_agree(self) -> None:
        assert self._identity().version == self._identity().version

    def test_param_ordering_does_not_change_the_version(self) -> None:
        """Canonical JSON — otherwise a dict literal reordering would look like
        a new strategy and orphan every stored signal."""
        a = self._identity(params={"a": 1, "b": 2})
        b = self._identity(params={"b": 2, "a": 1})
        assert a.version == b.version


class TestReasonCodesAreNotCollapsed:
    """[C2] The defect Codex found: `evaluate` recorded ONE `warmup_reason` for
    every unevaluable bar, collapsing quarantined bars, series breaks and data
    gaps into `insufficient_warmup` — destroying exactly what criterion 8
    exists for. Each input now carries its own code."""

    def test_each_input_contributes_its_own_reason(self) -> None:
        series = _bars(_CLOSES)
        n = len(series)
        vol_gap = IndicatorSeries(
            values=tuple(None if i == 20 else 1.0 for i in range(n)),
            universe=U,
            not_evaluable_indices=(20,),
        )
        quarantined = IndicatorSeries(
            values=tuple(None if i == 30 else 1.0 for i in range(n)),
            universe=U,
            not_evaluable_indices=(30,),
        )
        signals = evaluate(
            lambda i: True,
            inputs=[StrategyInput(vol_gap, "missing_volume"), StrategyInput(quarantined, "quarantined_bar")],
            n_bars=n,
        )
        assert signals[20].reason == "missing_volume"
        assert signals[30].reason == "quarantined_bar"

    def test_warm_up_is_distinguished_from_a_data_reason(self) -> None:
        """⚠ Warm-up is structural, not caller-supplied: a leading None that is
        NOT in not_evaluable_indices is the indicator warming up."""
        series = _bars(_CLOSES)
        sma = sma_series(series, universe=U, period=10)
        signals = evaluate(lambda i: True, inputs=[StrategyInput(sma, "quarantined_bar")], n_bars=len(series))
        assert signals[0].reason == "insufficient_warmup"
        assert signals[5].reason == "insufficient_warmup"


class TestClosedVocabulariesEnforcedAtRuntime:
    """[C2] `Literal` enforces nothing at runtime. This class is the contract
    that keeps verdicts and reason codes countable, so it checks."""

    def test_free_text_reason_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown reason code"):
            StrategySignal(verdict="not_evaluable", signal_index=0, reason="free text")  # type: ignore[arg-type]

    def test_unknown_verdict_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown verdict"):
            StrategySignal(verdict="maybe", signal_index=0)  # type: ignore[arg-type]

    def test_unknown_kind_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown signal kind"):
            StrategySignal(verdict="fired", signal_index=0, kind="hedge")  # type: ignore[arg-type]
