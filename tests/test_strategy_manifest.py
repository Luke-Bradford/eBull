"""The strategy manifest — enumeration that cannot be forgotten (#2394 §2).

Spec: ``docs/proposals/ta/2026-08-08-strategy-runner-and-manifest.md`` §2.
Module under test: ``app/services/strategy_manifest.py``.

⚠ THE EXPECTED VALUES ARE LITERALS, NOT IMPORTS. Spec §3's exit-regime table
and the four strategy ids are written out below rather than imported from the
modules they describe. A reference that imports the constant it validates is a
tautology — it agrees with any value the source happens to hold, including a
wrong one. The bridge between the literals and the source is
``test_the_spec_ids_are_the_modules_ids``, which is the ONE place the two are
compared, and which fails loudly if a module renames itself.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.services import strategies
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.market_regime import unconstrained_regime
from app.services.position_builder import ExitRegime
from app.services.strategies.s1_time_series_momentum import s1_identity, s1_signals
from app.services.strategies.s2_cross_sectional_momentum import rebalance_dates, s2_member
from app.services.strategies.s3_mean_reversion_in_trend import s3_signals
from app.services.strategies.s4_volatility_compression_breakout import s4_signals
from app.services.strategy_manifest import (
    STRATEGY_MANIFEST,
    StrategyEntry,
)
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "series_break"

#: §4's four catalogue strategies, written out. See the module docstring.
SPEC_S1 = "s1-time-series-momentum"
SPEC_S2 = "s2-cross-sectional-momentum"
SPEC_S3 = "s3-mean-reversion-in-trend"
SPEC_S4 = "s4-volatility-compression-breakout"
#: §3 of the S-5..S-10 set. Written out for the same reason as the four above.
SPEC_S5 = "s5-support-bounce"
SPEC_S6 = "s6-resistance-breakout"
SPEC_S9 = "s9-squeeze-expansion"

#: Spec §3's table, verbatim, as ``(signal_pair, level_based, max_hold_bars,
#: has_rebalance_dates)``. ⚠ Written out for the reason in the module docstring:
#: importing ``S3_MAX_HOLD_BARS`` here would make the assertion agree with
#: whatever the module says, which is not a check.
SPEC_EXIT_REGIMES: dict[str, tuple[bool, bool, int | None, bool]] = {
    SPEC_S1: (True, False, None, False),
    SPEC_S2: (False, False, None, True),
    SPEC_S3: (True, False, 10, False),
    SPEC_S4: (False, True, 40, False),
    SPEC_S5: (False, True, 30, False),
    SPEC_S6: (False, True, 40, False),
    SPEC_S9: (False, True, 40, False),
}

#: The legs each strategy emits — §4: S-1 and S-3 have an exit rule, S-2 closes
#: on the calendar and S-4 on its levels, so neither emits an exit SIGNAL.
SPEC_SIGNAL_KINDS: dict[str, frozenset[str]] = {
    SPEC_S1: frozenset({"entry", "exit"}),
    SPEC_S2: frozenset({"entry"}),
    SPEC_S3: frozenset({"entry", "exit"}),
    SPEC_S4: frozenset({"entry"}),
    SPEC_S5: frozenset({"entry"}),
    SPEC_S6: frozenset({"entry"}),
    SPEC_S9: frozenset({"entry"}),
}

SPEC_CLASSES: dict[str, str] = {
    SPEC_S1: "per_series",
    SPEC_S2: "cross_sectional",
    SPEC_S3: "per_series",
    SPEC_S4: "per_series",
    SPEC_S5: "per_series",
    SPEC_S6: "per_series",
    SPEC_S9: "per_series",
}

SPEC_PURPOSES = {
    strategy_id: "harness_validation" for strategy_id in (SPEC_S1, SPEC_S2, SPEC_S3, SPEC_S4, SPEC_S5, SPEC_S6, SPEC_S9)
}


def _bars(closes: Sequence[float | None], *, start: date = date(2020, 1, 1)) -> BarSeries:
    """One bar per close. ``None`` is a MASKED field, as ``load_masked_series``
    produces — present and empty, not absent."""
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(c)),
            "high": None if c is None else Decimal(str(c + 1)),
            "low": None if c is None else Decimal(str(c - 1)),
            "close": None if c is None else Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


#: Long enough that S-1's and S-3's 200-bar inputs warm up, so the per-series
#: entries are exercised past their warm-up and not only inside it.
_CLOSES: list[float | None] = [100.0 + (i % 17) for i in range(260)]
_HOLED_CLOSES: list[float | None] = [None, *_CLOSES[1:]]


class TestManifestIsComplete:
    """⚠⚠ COVERAGE IS CHECKED, NOT PROMISED — the pattern
    ``TestInputRuleSetsAreComplete`` already proves for ``INPUT_RULE_SETS``.

    The defect being prevented is an omission with no symptom: a strategy exists
    in the tree, nobody adds it to the manifest, and every runner and every
    verify script silently covers a smaller population than it reports. That is
    criterion 9's *"exclusion is visible rather than assumed harmless"* failing
    one layer up from where it was fixed.
    """

    _STRATEGIES_DIR = Path(strategies.__file__).parent

    #: Modules in the package that are deliberately not strategies. Listed
    #: rather than pattern-matched: a new helper module must be named here on
    #: purpose, so "not a strategy" stays a decision instead of an accident.
    _NOT_STRATEGIES = frozenset({"__init__.py", "validated_universe.py"})

    @classmethod
    def _declared_strategy_ids(cls) -> dict[str, str]:
        """``{module file: the string its *_STRATEGY_ID is assigned}``.

        Read with ``ast`` rather than by importing, so a module that fails to
        import is a missing entry here and therefore a test failure, not an
        error that could be mistaken for unrelated breakage.
        """
        found: dict[str, str] = {}
        for path in sorted(cls._STRATEGIES_DIR.glob("*.py")):
            if path.name in cls._NOT_STRATEGIES:
                continue
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if not isinstance(node, ast.Assign):
                    continue
                for target in node.targets:
                    if (
                        isinstance(target, ast.Name)
                        and target.id.endswith("_STRATEGY_ID")
                        and isinstance(node.value, ast.Constant)
                        and isinstance(node.value.value, str)
                    ):
                        found[path.name] = node.value.value
        return found

    def test_the_walk_finds_the_strategies(self) -> None:
        """⚠ A completeness test that silently matched nothing would pass
        forever. Pin that it is actually reading the modules it claims to — the
        prevention-log lesson from a probe that matched nothing."""
        declared = self._declared_strategy_ids()
        assert len(declared) == 7, f"expected the seven catalogue modules, walked {sorted(declared)}"
        assert declared["s1_time_series_momentum.py"] == SPEC_S1

    def test_every_strategy_module_is_registered(self) -> None:
        missing = {
            module: strategy_id
            for module, strategy_id in sorted(self._declared_strategy_ids().items())
            if strategy_id not in STRATEGY_MANIFEST
        }
        assert not missing, (
            "these strategies exist in the tree but are absent from STRATEGY_MANIFEST, so a runner "
            f"iterating it covers a smaller population than it reports (#2394 §2): {missing}"
        )

    def test_no_entry_is_registered_for_a_module_that_does_not_exist(self) -> None:
        """The other direction. A key with no module is a runner that raises at
        import — or worse, a strategy id that outlives its code and keeps
        appearing in a stored ledger."""
        declared = set(self._declared_strategy_ids().values())
        orphans = sorted(set(STRATEGY_MANIFEST) - declared)
        assert not orphans, f"manifest keys with no strategy module: {orphans}"

    def test_the_spec_ids_are_the_modules_ids(self) -> None:
        """⚠ THE ONE BRIDGE between this file's literals and the source. Every
        other assertion here is written against the literals, so without this
        the whole file could agree with a renamed strategy."""
        assert set(self._declared_strategy_ids().values()) == {
            SPEC_S1,
            SPEC_S2,
            SPEC_S3,
            SPEC_S4,
            SPEC_S5,
            SPEC_S6,
            SPEC_S9,
        }


class TestEntriesDescribeTheirStrategy:
    def test_key_equals_entry_id(self) -> None:
        mismatched = {key: entry.strategy_id for key, entry in STRATEGY_MANIFEST.items() if key != entry.strategy_id}
        assert not mismatched, f"manifest keyed differently from the entry it holds: {mismatched}"

    def test_identity_factory_returns_that_strategy(self) -> None:
        """⚠ Invoked, not inspected. A copy-paste of the wrong ``s*_identity``
        into an entry is invisible to any structural check and would file every
        S-3 signal under S-1's identity."""
        for key, entry in STRATEGY_MANIFEST.items():
            identity = entry.identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
            assert identity.strategy_id == key

    def test_classes_are_the_spec_classes(self) -> None:
        assert {key: entry.strategy_class for key, entry in STRATEGY_MANIFEST.items()} == SPEC_CLASSES

    def test_all_catalogue_rules_are_explicit_harness_controls(self) -> None:
        assert {key: entry.purpose for key, entry in STRATEGY_MANIFEST.items()} == SPEC_PURPOSES

    def test_declared_signal_kinds_are_the_spec_legs(self) -> None:
        assert {key: set(entry.signal_kinds) for key, entry in STRATEGY_MANIFEST.items()} == {
            key: set(kinds) for key, kinds in SPEC_SIGNAL_KINDS.items()
        }


class TestExitRegimeTableIsExecutable:
    """Spec §3's table is prose in ``ExitRegime``'s docstring and every caller
    hand-builds it. These assert the manifest reproduces it."""

    @staticmethod
    def _regime(strategy_id: str) -> ExitRegime:
        entry = STRATEGY_MANIFEST[strategy_id]
        calendar = [date(2020, 1, 1) + timedelta(days=i) for i in range(90)]
        return entry.exit_regime(entry.decision_calendar(calendar))

    @pytest.mark.parametrize("strategy_id", sorted(SPEC_EXIT_REGIMES))
    def test_regime_matches_the_spec_table(self, strategy_id: str) -> None:
        signal_pair, level_based, max_hold, has_calendar = SPEC_EXIT_REGIMES[strategy_id]
        regime = self._regime(strategy_id)
        assert regime.signal_pair is signal_pair
        assert regime.level_based is level_based
        assert regime.max_hold_bars == max_hold
        assert (regime.rebalance_dates is not None) is has_calendar

    def test_a_per_series_strategy_refuses_a_calendar(self) -> None:
        """Ignoring it would let a caller believe S-1 rebalances monthly."""
        with pytest.raises(ValueError, match="declares no decision calendar"):
            STRATEGY_MANIFEST[SPEC_S1].exit_regime(frozenset({date(2020, 2, 3)}))

    def test_the_calendar_strategy_refuses_no_calendar(self) -> None:
        with pytest.raises(ValueError, match="cannot be built without one"):
            STRATEGY_MANIFEST[SPEC_S2].exit_regime(None)

    def test_per_series_decision_calendar_is_none_not_empty(self) -> None:
        """ "No calendar" and "a calendar with no dates" must stay distinct —
        ``ExitRegime`` refuses the empty set for exactly that reason."""
        for strategy_id in (SPEC_S1, SPEC_S3, SPEC_S4):
            entry = STRATEGY_MANIFEST[strategy_id]
            assert entry.decision_calendar([date(2020, 1, 1), date(2020, 2, 1)]) is None

    def test_the_calendar_strategy_computes_rebalances(self) -> None:
        entry = STRATEGY_MANIFEST[SPEC_S2]
        calendar = [date(2020, 1, 1) + timedelta(days=i) for i in range(90)]
        assert entry.decision_calendar(calendar) == rebalance_dates(calendar)


class TestUniformInvocationEqualsTheDirectCall:
    """⚠⚠ THE ADAPTERS ARE THE RISK, AND ONLY A CALL FINDS IT.

    ``s1_signals``/``s3_signals`` take ``close_reason``; ``s4_signals`` takes
    ``masked_reason``. The manifest presents one signature, so each entry wraps
    its module's. A wrapper passing the wrong keyword, or the wrong module's
    function, type-checks fine and fails at call time — which for a nightly scan
    means the first failure is in production.
    """

    @pytest.mark.parametrize(
        ("strategy_id", "direct"),
        [(SPEC_S1, s1_signals), (SPEC_S3, s3_signals)],
    )
    def test_close_reason_strategies_match(self, strategy_id: str, direct: object) -> None:
        entry = STRATEGY_MANIFEST[strategy_id]
        assert entry.signals is not None
        via_manifest = entry.signals(
            _bars(_CLOSES), universe=UNIVERSE, masked_reason=REASON, regime=unconstrained_regime(len(_bars(_CLOSES)))
        )
        expected = direct(_bars(_CLOSES), universe=UNIVERSE, close_reason=REASON)  # type: ignore[operator]
        assert via_manifest == expected
        assert any(signal.verdict == "fired" for signal in via_manifest), (
            "the fixture never fires, so an adapter returning refusals for every bar would pass"
        )

    def test_masked_reason_strategy_matches(self) -> None:
        entry = STRATEGY_MANIFEST[SPEC_S4]
        assert entry.signals is not None
        via_manifest = entry.signals(
            _bars(_CLOSES), universe=UNIVERSE, masked_reason=REASON, regime=unconstrained_regime(len(_bars(_CLOSES)))
        )
        assert via_manifest == s4_signals(_bars(_CLOSES), universe=UNIVERSE, masked_reason=REASON)

    @pytest.mark.parametrize("strategy_id", [SPEC_S1, SPEC_S3, SPEC_S4, SPEC_S5, SPEC_S6, SPEC_S9])
    def test_the_reason_code_reaches_the_verdict(self, strategy_id: str) -> None:
        """⚠ An adapter could accept ``masked_reason`` and drop it, defaulting
        the module's own argument. Equality against the direct call would still
        pass if BOTH sides were wrong, so this asserts the caller's code is what
        comes back."""
        entry = STRATEGY_MANIFEST[strategy_id]
        assert entry.signals is not None
        signals = entry.signals(
            _bars(_HOLED_CLOSES),
            universe=UNIVERSE,
            masked_reason=REASON,
            regime=unconstrained_regime(len(_bars(_HOLED_CLOSES))),
        )
        assert signals[0].verdict == "not_evaluable"
        assert signals[0].reason == REASON

    @pytest.mark.parametrize("strategy_id", [SPEC_S1, SPEC_S3, SPEC_S4, SPEC_S5, SPEC_S6, SPEC_S9])
    def test_emitted_kinds_are_the_declared_kinds(self, strategy_id: str) -> None:
        """⚠ ``signal_kinds`` is a claim about the strategy, so it is measured
        from what it emits rather than trusted."""
        entry = STRATEGY_MANIFEST[strategy_id]
        assert entry.signals is not None
        emitted = {
            signal.kind
            for signal in entry.signals(
                _bars(_CLOSES),
                universe=UNIVERSE,
                masked_reason=REASON,
                regime=unconstrained_regime(len(_bars(_CLOSES))),
            )
        }
        assert emitted == set(entry.signal_kinds)

    def test_cross_sectional_member_matches_the_direct_call(self) -> None:
        entry = STRATEGY_MANIFEST[SPEC_S2]
        assert entry.member is not None and entry.select is not None
        series = _bars(_CLOSES)
        dates = entry.decision_calendar(series.dates)
        assert dates is not None
        via_manifest = entry.member(series, panel_decision_dates=dates, universe=UNIVERSE, masked_reason=REASON)
        expected = s2_member(series, panel_rebalance_dates=dates, universe=UNIVERSE, close_reason=REASON)
        assert via_manifest.dates == expected.dates
        assert via_manifest.decision_indices == expected.decision_indices
        assert via_manifest.score.values == expected.score.values

    def test_cross_sectional_select_ranks(self) -> None:
        entry = STRATEGY_MANIFEST[SPEC_S2]
        assert entry.select is not None and entry.min_participants is not None
        scores = {key: float(key) for key in range(entry.min_participants)}
        winners = entry.select(date(2020, 3, 2), scores)
        assert winners and set(winners) <= set(scores)


class TestEntryRefusesAContradictoryRegistration:
    """The tagged union is checked, not documented — an entry whose two halves
    disagree would be honoured by whichever half the runner read."""

    @staticmethod
    def _kwargs(**overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "strategy_id": "s9-test",
            "purpose": "capital_candidate",
            "identity": s1_identity,
            "strategy_class": "per_series",
            "signal_kinds": frozenset({"entry"}),
            "exit_regime": lambda decision_dates: ExitRegime(
                signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=None
            ),
            "decision_calendar": lambda calendar: None,
            "signals": STRATEGY_MANIFEST[SPEC_S1].signals,
        }
        base.update(overrides)
        return base

    def test_the_baseline_registration_is_valid(self) -> None:
        """⚠ Without this, every refusal below could be passing for the wrong
        reason — a baseline that raises makes them all vacuous."""
        assert StrategyEntry(**self._kwargs()).strategy_id == "s9-test"  # type: ignore[arg-type]

    def test_per_series_without_a_signals_function(self) -> None:
        with pytest.raises(ValueError, match="declares no signals function"):
            StrategyEntry(**self._kwargs(signals=None))  # type: ignore[arg-type]

    def test_per_series_carrying_cross_sectional_fields(self) -> None:
        with pytest.raises(ValueError, match="carries cross-sectional fields"):
            StrategyEntry(**self._kwargs(min_participants=10))  # type: ignore[arg-type]

    def test_cross_sectional_missing_half_its_contract(self) -> None:
        with pytest.raises(ValueError, match="must declare member, select and min_participants together"):
            StrategyEntry(
                **self._kwargs(
                    strategy_class="cross_sectional",
                    signals=None,
                    member=STRATEGY_MANIFEST[SPEC_S2].member,
                )  # type: ignore[arg-type]
            )

    def test_cross_sectional_carrying_a_per_series_function(self) -> None:
        with pytest.raises(ValueError, match="carries a per-series signals function"):
            StrategyEntry(
                **self._kwargs(
                    strategy_class="cross_sectional",
                    member=STRATEGY_MANIFEST[SPEC_S2].member,
                    select=STRATEGY_MANIFEST[SPEC_S2].select,
                    min_participants=10,
                )  # type: ignore[arg-type]
            )

    def test_an_unknown_class(self) -> None:
        with pytest.raises(ValueError, match="unknown strategy class"):
            StrategyEntry(**self._kwargs(strategy_class="per_bar"))  # type: ignore[arg-type]

    def test_an_unknown_purpose(self) -> None:
        with pytest.raises(ValueError, match="unknown strategy purpose"):
            StrategyEntry(**self._kwargs(purpose="looks_promising"))  # type: ignore[arg-type]

    def test_an_unknown_signal_kind(self) -> None:
        with pytest.raises(ValueError, match="unknown signal kinds"):
            StrategyEntry(**self._kwargs(signal_kinds=frozenset({"entry", "hedge"})))  # type: ignore[arg-type]

    def test_an_exit_only_strategy(self) -> None:
        """Outcome resolution consumes fired ENTRIES, so an exit-only
        registration writes rows nothing can resolve."""
        with pytest.raises(ValueError, match="no entry leg"):
            StrategyEntry(**self._kwargs(signal_kinds=frozenset({"exit"})))  # type: ignore[arg-type]

    def test_a_blank_strategy_id(self) -> None:
        with pytest.raises(ValueError, match="non-empty declaration"):
            StrategyEntry(**self._kwargs(strategy_id="  "))  # type: ignore[arg-type]

    def test_a_cross_section_of_zero(self) -> None:
        with pytest.raises(ValueError, match="min_participants must be at least 1"):
            StrategyEntry(
                **self._kwargs(
                    strategy_class="cross_sectional",
                    signals=None,
                    member=STRATEGY_MANIFEST[SPEC_S2].member,
                    select=STRATEGY_MANIFEST[SPEC_S2].select,
                    min_participants=0,
                )  # type: ignore[arg-type]
            )
