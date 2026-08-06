"""Phase 3a — the registry contract.

Pure, no DB. Spec:
`docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md`.
"""

from __future__ import annotations

import ast
import importlib
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.services import strategies, strategy_registry
from app.services.indicator_series import RULE_SET_VERSION as INDICATOR_SERIES_RULE_SET_VERSION
from app.services.indicator_series import (
    BarSeries,
    IndicatorSeries,
    sma_series,
)
from app.services.strategy_registry import (
    INPUT_RULE_SETS,
    NOT_EVALUABLE_REASONS,
    OUR_ADDITIONAL_REASON_CODES,
    PARENT_REASON_CODES,
    SIGNAL_KINDS,
    VERDICTS,
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

    def test_the_indicator_rule_set_changes_the_version(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """#2333. A strategy IS its indicators — S-1 is
        ``sma_series(fast) > sma_series(slow)`` and has no other content — so a
        change to how the SMA is COMPUTED is changed filter logic under
        criterion 11. Before this, it produced different signals under an
        unchanged version, and the ledger's uniqueness key treated the old and
        new rows as the same row."""
        before = self._identity().version
        monkeypatch.setattr(
            strategy_registry,
            "INPUT_RULE_SETS",
            {"indicator_series": "indicator-series-v1+ffffffffffff"},
        )
        assert self._identity().version != before

    def test_the_stored_mapping_is_the_hashed_one(self) -> None:
        """The writer stores ``identity.input_rule_set_versions`` and the hash
        is built from the same object, so a disagreement is not expressible."""
        assert self._identity().input_rule_set_versions is strategy_registry.INPUT_RULE_SETS
        assert dict(INPUT_RULE_SETS) == {"indicator_series": INDICATOR_SERIES_RULE_SET_VERSION}

    def test_the_registry_constant_is_read_only(self) -> None:
        """A plain dict would let any importer mutate the identity of every
        strategy in the process."""
        with pytest.raises(TypeError):
            INPUT_RULE_SETS["indicator_series"] = "tampered"  # type: ignore[index]

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


class TestInputRuleSetsAreComplete:
    """#2333 — the coverage of ``INPUT_RULE_SETS`` is CHECKED, not promised.

    The defect being prevented is an omission: a strategy reads a versioned
    pipeline whose version is not in the identity hash, so a change to that
    pipeline reuses the old ``strategy_version``. A per-strategy ``inputs=[…]``
    declaration would move the omission rather than remove it, so the registry
    keeps one constant and this test walks the strategies package to prove it
    covers what is actually imported.

    S-5/S-6 are the live case: they are specced against ``price_structure``,
    which carries its own ``RULE_SET_VERSION``. The day one of them imports it,
    this test fails until the registry names it.

    ⚠ DIRECT imports only. A strategy importing a helper that itself reads a
    versioned pipeline is not caught, and no static rule short of a full import
    graph would catch it. Stated rather than implied — a guard whose blind spot
    is undocumented reads as covering more than it does.
    """

    _STRATEGIES_DIR = Path(strategies.__file__).parent

    @classmethod
    def _imported_service_modules(cls) -> dict[str, set[str]]:
        """``{strategy module: {app.services module it imports}}``."""
        found: dict[str, set[str]] = {}
        for path in sorted(cls._STRATEGIES_DIR.glob("*.py")):
            tree = ast.parse(path.read_text())
            names: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("app.services."):
                    names.add(node.module)
                elif isinstance(node, ast.Import):
                    names.update(alias.name for alias in node.names if alias.name.startswith("app.services."))
            found[path.name] = names
        return found

    def test_the_walk_finds_the_strategies(self) -> None:
        """⚠ A completeness test that silently matched nothing would pass
        forever. Pin that it is actually reading the modules it claims to."""
        imports = self._imported_service_modules()
        assert {"s1_time_series_momentum.py", "s3_mean_reversion_in_trend.py"} <= set(imports)
        assert "app.services.indicator_series" in imports["s1_time_series_momentum.py"]

    def test_every_versioned_pipeline_a_strategy_reads_is_in_the_hash(self) -> None:
        missing: list[str] = []
        for strategy_module, imported in sorted(self._imported_service_modules().items()):
            for dotted in sorted(imported):
                module = importlib.import_module(dotted)
                if not hasattr(module, "RULE_SET_VERSION"):
                    continue
                if dotted.rsplit(".", 1)[-1] not in INPUT_RULE_SETS:
                    missing.append(f"{strategy_module} reads {dotted}")
        assert not missing, (
            "these versioned rule sets are read by a strategy but absent from "
            f"strategy_registry.INPUT_RULE_SETS, so a change to them would reuse the old "
            f"strategy_version (#2333): {missing}"
        )


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


class TestVocabularyIsDefinedOnce:
    """[review NITPICK] The reason vocabulary was written out three times in
    Python, and sql/255's CHECK is a fourth place. That is the
    closed-vocabulary-in-N-places defect the prevention log carries from #2218:
    a member added in one place and missed in the others writes rows nothing
    reads. The Python sets are now derived via `get_args`; this pins the SQL.
    """

    @staticmethod
    def _check_values(constraint: str, column: str) -> set[str]:
        import re
        from pathlib import Path

        sql = (Path(__file__).resolve().parents[1] / "sql" / "255_strategy_signals.sql").read_text()
        # The CHECK bodies are multi-line; grab from the column name to the
        # closing paren of its IN (...) list.
        match = re.search(rf"{column}[^;]*?IN \(([^)]*)\)", sql, re.DOTALL)
        assert match is not None, f"no IN-list found for {column}"
        return {value.strip().strip("'") for value in match.group(1).split(",") if value.strip()}

    def test_sql_reason_codes_match_the_python_vocabulary(self) -> None:
        assert self._check_values("strategy_signals", "not_evaluable_reason") == NOT_EVALUABLE_REASONS

    def test_sql_verdicts_match(self) -> None:
        assert self._check_values("strategy_signals", "verdict") == VERDICTS

    def test_sql_signal_kinds_match(self) -> None:
        assert self._check_values("strategy_signals", "signal_kind") == SIGNAL_KINDS

    def test_parent_codes_are_the_derived_set_minus_ours(self) -> None:
        assert PARENT_REASON_CODES == NOT_EVALUABLE_REASONS - OUR_ADDITIONAL_REASON_CODES
        assert len(PARENT_REASON_CODES) == 7
