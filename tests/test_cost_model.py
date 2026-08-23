"""Phase 5b — the static cost model, as pure logic.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.1, acceptance
C2. The calibration figures and their drift against the live ``quotes``
snapshot live in ``scripts/verify_2240_cost_model.py`` — a hand-copied
statistic goes stale in the place a reader trusts most.

⚠ DB-free by design. ``cost_model`` reads no database; it is a frozen table and
four functions over it.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
from decimal import Decimal
from unittest import mock

import pytest

from app.services import cost_model
from app.services.cost_model import (
    BANDS,
    CARRY_CLOSURE,
    CARRY_EVIDENCE,
    CARRY_UNMODELLED,
    COST_COMPONENT_CLOSURES,
    COST_MODEL_ID,
    FX_CLOSURE,
    FX_EVIDENCE,
    FX_UNMODELLED,
    PRICE_BASES,
    STRUCTURAL_ZERO_LANE,
    UNKNOWN_NOMINAL_PRICE_BAND,
    PriceBand,
    _check_bands_are_total,
    _check_half_spread,
    band_for,
    buy_price,
    cost_band_for,
    half_spread_for,
    sell_price,
)

#: ⚠⚠ THE FROZEN TABLE, RESTATED HERE AS LITERALS AND NOT IMPORTED.
#:
#: Acceptance C2(c): *"the band table is … pinned by test"*. A test that
#: imported ``BANDS`` and compared it to itself is a tautology — the exact
#: defect the S-3 run hit (prevention log, #2240 S-3: *"a reference that IMPORTS
#: the constant it validates"*). These literals are the second copy, on purpose,
#: so a recalibration cannot land without a test failing and a human deciding
#: whether the ``cost_model_id`` should have moved with it.
SPEC_BANDS: tuple[tuple[str, str | None, str | None, str, int], ...] = (
    ("<$5", None, "5", "1.450", 76),
    ("$5-20", "5", "20", "0.571", 244),
    ("$20-100", "20", "100", "0.509", 625),
    (">=$100", "100", None, "0.322", 210),
)

SPEC_COST_MODEL_ID = "static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd"

#: ⚠ THE LANE, RESTATED AS LITERALS AND NOT IMPORTED — the same second-copy
#: rule as ``SPEC_BANDS``. These are also, field for field, the literals
#: ``EtoroBrokerProvider.place_demo_strategy_order`` puts on the wire
#: (``transaction: buy`` ⇔ long, ``leverage: 1``, ``settlementType: "real"``,
#: ``orderCurrency: "usd"``) — ``tests/test_broker_provider.py``'s
#: ``test_the_order_payload_is_the_cost_model_lane`` captures that payload
#: independently, so an executor change that leaves the lane fails there and a
#: lane change that leaves the executor fails here.
SPEC_LANE = ("long", 1, "real", "USD", "USD", "USD")


class TestTheFrozenTable:
    def test_the_cost_model_id_is_the_frozen_one(self) -> None:
        """⚠ A recalibration is a NEW id (§5.1), and a new id moves every
        strategy version. This assertion is what makes that a deliberate act."""
        assert COST_MODEL_ID == SPEC_COST_MODEL_ID

    def test_every_band_matches_the_frozen_calibration(self) -> None:
        assert len(BANDS) == len(SPEC_BANDS)
        for band, (label, lower, upper, p75, n) in zip(BANDS, SPEC_BANDS, strict=True):
            assert band.label == label
            assert band.lower == (None if lower is None else Decimal(lower))
            assert band.upper == (None if upper is None else Decimal(upper))
            assert band.p75_spread_pct == Decimal(p75)
            assert band.sample_size == n

    def test_the_half_spread_is_half_the_round_trip(self) -> None:
        for band in BANDS:
            assert band.half_spread_pct == band.p75_spread_pct / 2
            assert band.half_spread == band.p75_spread_pct / 200

    def test_the_bands_get_cheaper_as_the_price_rises(self) -> None:
        """Not a law of nature — a property of THIS calibration, asserted so a
        recalibration that inverts it is noticed rather than shipped. A penny
        stock costing less to trade than a $200 one would mean the measurement
        was of something other than liquidity."""
        spreads = [band.p75_spread_pct for band in BANDS]
        assert spreads == sorted(spreads, reverse=True)


class TestBandLookup:
    @pytest.mark.parametrize(
        ("price", "label"),
        [
            ("0.01", "<$5"),
            ("4.999999", "<$5"),
            ("5", "$5-20"),  # lower bound INCLUSIVE
            ("19.999999", "$5-20"),
            ("20", "$20-100"),
            ("99.999999", "$20-100"),
            ("100", ">=$100"),
            ("100000", ">=$100"),
        ],
    )
    def test_a_price_lands_in_the_band_that_claims_it(self, price: str, label: str) -> None:
        assert band_for(Decimal(price)).label == label

    @pytest.mark.parametrize("price", ["0", "-1", "-0.000001"])
    def test_a_non_positive_price_has_no_band(self, price: str) -> None:
        """⚠ It RAISES rather than defaulting to the cheapest or the dearest
        band. Both fills already require ``> 0``, so arriving here with one
        means the caller assembled a position from something that is not a
        price — and a silent default would price that trade anyway."""
        with pytest.raises(ValueError, match="must be > 0"):
            band_for(Decimal(price))

    def test_half_spread_for_reads_the_entry_price_band(self) -> None:
        assert half_spread_for(Decimal("50")) == Decimal("0.509") / 200

    def test_split_adjusted_price_uses_the_maximum_band_not_its_numeric_band(self) -> None:
        assert cost_band_for(Decimal("500"), price_basis="split_adjusted") is UNKNOWN_NOMINAL_PRICE_BAND
        assert UNKNOWN_NOMINAL_PRICE_BAND.p75_spread_pct == max(b.p75_spread_pct for b in BANDS)

    def test_as_traded_price_can_select_its_nominal_band(self) -> None:
        assert cost_band_for(Decimal("500"), price_basis="as_traded").label == ">=$100"

    def test_an_unknown_basis_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown price basis"):
            cost_band_for(Decimal("50"), price_basis="raw")  # type: ignore[arg-type]

    def test_split_adjusted_still_requires_a_positive_price(self) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            cost_band_for(Decimal("0"), price_basis="split_adjusted")

    def test_the_basis_vocabulary_is_closed(self) -> None:
        assert PRICE_BASES == {"as_traded", "split_adjusted"}


class TestTheBandTableIsTotal:
    """``_check_bands_are_total`` runs at IMPORT, so these exercise it directly
    against tables the module would refuse."""

    def _band(self, label: str, lower: str | None, upper: str | None) -> PriceBand:
        return PriceBand(
            label=label,
            lower=None if lower is None else Decimal(lower),
            upper=None if upper is None else Decimal(upper),
            p75_spread_pct=Decimal("1"),
            sample_size=10,
        )

    def test_a_gap_between_two_bands_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="not contiguous"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("b", "6", None)))

    def test_an_overlap_between_two_bands_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="not contiguous"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("b", "4", None)))

    def test_the_lowest_band_must_be_open_below(self) -> None:
        with pytest.raises(ValueError, match="open below"):
            _check_bands_are_total((self._band("a", "1", "5"), self._band("b", "5", None)))

    def test_the_highest_band_must_be_open_above(self) -> None:
        with pytest.raises(ValueError, match="open above"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("b", "5", "100")))

    def test_an_empty_table_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="no price bands"):
            _check_bands_are_total(())

    def test_a_duplicate_label_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate band label"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("a", "5", None)))

    def test_the_shipped_table_passes_its_own_check(self) -> None:
        _check_bands_are_total(BANDS)


class TestBandInvariants:
    def _band(self, **overrides: object) -> PriceBand:
        fields: dict[str, object] = {
            "label": "b",
            "lower": Decimal("5"),
            "upper": Decimal("20"),
            "p75_spread_pct": Decimal("0.5"),
            "sample_size": 10,
        }
        fields.update(overrides)
        return PriceBand(**fields)  # type: ignore[arg-type]

    def test_a_zero_cost_band_is_rejected(self) -> None:
        """Criterion 2's *"honest model, not a fictional one"* — a band charging
        nothing is a free trade, which is the assumption the criterion exists to
        remove."""
        with pytest.raises(ValueError, match="must be > 0"):
            self._band(p75_spread_pct=Decimal("0"))

    def test_a_band_with_no_sample_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="sample_size"):
            self._band(sample_size=0)

    def test_an_inverted_band_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="does not exceed"):
            self._band(lower=Decimal("20"), upper=Decimal("5"))

    def test_a_non_positive_lower_bound_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="lower bound must be > 0"):
            self._band(lower=Decimal("0"))


class TestTheArithmetic:
    """§5.1 verbatim: *"a buy fills at ``fill_price × (1 + h)`` and a sell at
    ``fill_price × (1 − h)``"*."""

    def test_a_buy_pays_the_half_spread(self) -> None:
        assert buy_price(Decimal("100"), half_spread=Decimal("0.01")) == Decimal("101")

    def test_a_sell_pays_the_half_spread(self) -> None:
        assert sell_price(Decimal("100"), half_spread=Decimal("0.01")) == Decimal("99")

    def test_the_two_sides_are_symmetric_about_the_gross_price(self) -> None:
        gross, h = Decimal("37.5"), Decimal("0.0025")
        assert buy_price(gross, half_spread=h) - gross == gross - sell_price(gross, half_spread=h)

    def test_a_round_trip_at_an_unchanged_price_loses_money(self) -> None:
        """⚠ THE PROPERTY THE WHOLE STAGE EXISTS FOR. A flat trade is a losing
        trade once the spread is charged, and S-1's median hold is ONE BAR
        (measured, phase 5a), so this is not a rounding effect for it."""
        gross, h = Decimal("100"), half_spread_for(Decimal("100"))
        assert sell_price(gross, half_spread=h) < buy_price(gross, half_spread=h)

    @pytest.mark.parametrize("half_spread", ["0", "-0.01"])
    def test_a_non_positive_half_spread_is_rejected(self, half_spread: str) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            _check_half_spread(Decimal(half_spread))

    @pytest.mark.parametrize("half_spread", ["1", "1.5"])
    def test_a_half_spread_at_or_above_one_is_rejected(self, half_spread: str) -> None:
        """⚠ At ``h >= 1`` the sell side is zero or negative. A return computed
        from it is not a loss — it is nonsense that still divides cleanly and
        would be averaged into a result."""
        with pytest.raises(ValueError, match="must be < 1"):
            _check_half_spread(Decimal(half_spread))

    def test_the_price_functions_enforce_the_same_bound(self) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            buy_price(Decimal("100"), half_spread=Decimal("0"))
        with pytest.raises(ValueError, match="must be < 1"):
            sell_price(Decimal("100"), half_spread=Decimal("1"))


class TestTheStructuralClosure:
    """#2720: carry and FX close as STRUCTURAL ZERO for the declared lane —
    a closure state, never a ``Decimal("0")`` pretending to be a measurement
    (#2286's shape)."""

    def test_both_closures_are_structural_zero(self) -> None:
        assert CARRY_CLOSURE == "structural_zero"
        assert FX_CLOSURE == "structural_zero"

    def test_the_closure_vocabulary_is_closed(self) -> None:
        assert COST_COMPONENT_CLOSURES == {"unmodelled", "structural_zero"}

    def test_the_markers_are_cleared(self) -> None:
        """⚠ The markers the promotion gate refuses on (§6 clause 4). DERIVED
        from the closures, so the closure IS the single source — a hand-written
        ``False`` would have to be remembered."""
        assert CARRY_UNMODELLED is False
        assert FX_UNMODELLED is False

    def test_the_lane_is_the_frozen_one(self) -> None:
        """The second copy, as literals — a lane edit (short, leveraged,
        non-USD) must fail a test and force a human to decide whether the
        ``cost_model_id`` should have moved with it. It should: either edit is
        a NEW model by the module rule."""
        lane = STRUCTURAL_ZERO_LANE
        assert (
            lane.direction,
            lane.leverage,
            lane.settlement,
            lane.order_currency,
            lane.account_currency,
            lane.instrument_denomination,
        ) == SPEC_LANE

    def test_the_lane_account_currency_matches_the_deployment_currency(self) -> None:
        """Literal vs literal — ``strategy_base_currency`` states USD in its own
        constant (enforced at the sql/290 + sql/338 CHECKs and the executor
        currency checks); the lane states it independently. Divergence means
        one of the two contracts moved without the other."""
        from app.services.strategy_base_currency import DEPLOYMENT_CURRENCY

        assert STRUCTURAL_ZERO_LANE.account_currency == DEPLOYMENT_CURRENCY
        assert STRUCTURAL_ZERO_LANE.order_currency == DEPLOYMENT_CURRENCY

    def test_the_evidence_is_present_and_dated(self) -> None:
        """A structural claim with no dated record is ceremony (§ckpt-1 #21)."""
        for evidence in (CARRY_EVIDENCE, FX_EVIDENCE):
            assert evidence
            assert any("2026-08-14" in item or "#2698" in item or "#2605" in item for item in evidence)

    @pytest.mark.parametrize(
        "carry_closure,fx_closure,expected",
        [
            ("unmodelled", "unmodelled", (True, True)),
            ("structural_zero", "unmodelled", (False, True)),
            ("unmodelled", "structural_zero", (True, False)),
            ("structural_zero", "structural_zero", (False, False)),
        ],
    )
    def test_each_marker_reads_only_its_own_closure(
        self, carry_closure: str, fx_closure: str, expected: tuple[bool, bool]
    ) -> None:
        """⚠⚠ THE #2363 DE-COUPLING, preserved across #2720's input change: the
        two mixed rows are the ones that matter — under a re-coupled rule each
        would return ``(True, ...)``."""
        assert cost_model.unmodelled_markers(carry_closure, fx_closure) == expected

    @pytest.mark.parametrize("bad", ["charged", "zero", "", "STRUCTURAL_ZERO"])
    def test_an_unknown_closure_is_refused_by_the_markers(self, bad: str) -> None:
        """⚠⚠ THE FALSE-PROMOTION TRIPWIRE'S NEW SHAPE. ``== "unmodelled"``
        alone would read a typo as "modelled" — clearing a promotion refusal
        while modelling nothing. Unknown values must RAISE, never default."""
        with pytest.raises(ValueError, match="unknown carry closure"):
            cost_model.unmodelled_markers(bad, "unmodelled")
        with pytest.raises(ValueError, match="unknown fx closure"):
            cost_model.unmodelled_markers("unmodelled", bad)

    def test_the_closure_guard_refuses_an_unknown_value(self) -> None:
        with pytest.raises(ValueError, match="not in the closure vocabulary"):
            with mock.patch.object(cost_model, "CARRY_CLOSURE", "charged"):
                cost_model._check_closures()

    def test_the_closure_guard_requires_evidence(self) -> None:
        """A structural_zero with an empty evidence tuple is the flag-clearing
        shortcut wearing a new name; the guard refuses it beside the literal."""
        with pytest.raises(ValueError, match="no evidence"):
            with mock.patch.object(cost_model, "FX_EVIDENCE", ()):
                cost_model._check_closures()

    def test_the_closure_guard_refuses_a_short_or_leveraged_lane(self) -> None:
        """⚠ Risk posture (`.claude/CLAUDE.md`): a short is a CFD and accrues
        financing by construction; leverage above x1 the same. Either lane
        needs a NEW cost model, never this one relabelled."""
        with pytest.raises(ValueError, match="must be long and unleveraged"):
            with mock.patch.object(
                cost_model, "STRUCTURAL_ZERO_LANE", cost_model.CostLane("short", 1, "cfd", "USD", "USD", "USD")
            ):
                cost_model._check_closures()
        with pytest.raises(ValueError, match="must be long and unleveraged"):
            with mock.patch.object(
                cost_model, "STRUCTURAL_ZERO_LANE", cost_model.CostLane("long", 2, "cfd", "USD", "USD", "USD")
            ):
                cost_model._check_closures()

    def test_the_closure_guard_refuses_a_non_usd_instrument_on_a_usd_path(self) -> None:
        """⚠⚠ THE #2833 HOLE. The beta-sleeve candidates (CSPX.L, IUSA.L,
        IUMO.L, IUQA.L, R1VL.L) are stored ``instruments.currency = 'GBP'`` and
        their eligibility proofs answer ``response_currency = 'usd'`` from a USD
        account — so order and account currency BOTH read USD while a
        conversion event happens on unit sizing. Under the pre-#2833 lane
        (order + account only) that sleeve inherits an FX ``structural_zero``
        measured on an all-USD universe, silently and with every test green.

        The discriminating case is the mixed one: same order and account
        currency, different instrument denomination."""
        with pytest.raises(ValueError, match="names more than one currency"):
            with mock.patch.object(
                cost_model, "STRUCTURAL_ZERO_LANE", cost_model.CostLane("long", 1, "real", "USD", "USD", "GBP")
            ):
                cost_model._check_closures()

    def test_the_currency_guard_is_scoped_to_a_structural_zero_claim(self) -> None:
        """An ``unmodelled`` FX closure claims nothing about conversion, so a
        mixed-currency lane is not a contradiction there — it is the honest
        state. Guarding it regardless would refuse the very lane a non-USD
        sleeve has to declare on its way to measuring the fee."""
        with mock.patch.object(cost_model, "FX_CLOSURE", "unmodelled"):
            with mock.patch.object(
                cost_model, "STRUCTURAL_ZERO_LANE", cost_model.CostLane("long", 1, "real", "USD", "USD", "GBP")
            ):
                cost_model._check_closures()


class TestTheImportTimeGuardsActuallyRun:
    """⚠⚠ Both module-level guards were UNPROVEN until #2699.

    ``_check_closures`` (#2720; previously ``_check_unmodelled_components_are_not_charged``)
    and ``_check_bands_are_total``
    each end their definition with a module-level call, and each docstring says why:
    the thing they guard is *an edit somebody makes to the literal above*, so the
    check belongs beside the literal rather than in a test file that person may never
    run. That placement IS the guard's whole value.

    Every existing test reaches them by CALLING THEM DIRECTLY. Delete either
    invocation and the entire file still passes -- the guards become dead code that
    reads as live, which is the #2437 R4 shape (*a control on a path the decision
    does not take*) arriving through a missing call rather than a missing branch.

    These tests execute the SHIPPED SOURCE in a fresh interpreter with one literal
    substituted, which is exactly what the maintainer making that edit would see.
    A warm interpreter cannot answer the question -- ``app.services.cost_model`` is
    already in ``sys.modules`` and ``importlib.reload`` re-reads the same literals --
    so the subprocess is load-bearing, per ``audit_probe_anchors._launch_failure``.
    """

    SOURCE = pathlib.Path(cost_model.__file__)

    def _run_with_substitution(self, tmp_path: pathlib.Path, old: str, new: str) -> subprocess.CompletedProcess[str]:
        """Execute the real module source in a cold interpreter, one literal changed.

        ⚠ Anchored on the literal's exact text and asserted to have matched, so a
        reshaped declaration fails HERE rather than silently testing an unmodified
        copy -- the dead-anchor class #2695 spent a session on.
        """
        source = self.SOURCE.read_text()
        assert source.count(old) == 1, f"anchor {old!r} matched {source.count(old)} times, expected 1"
        probe = tmp_path / "cost_model_probe.py"
        probe.write_text(source.replace(old, new))
        return subprocess.run(  # noqa: S603 — fixed argv, path from tmp_path
            [sys.executable, str(probe)],
            capture_output=True,
            text=True,
            timeout=60,
        )

    def test_the_shipped_source_imports_cleanly(self) -> None:
        """The control arm: an UNMODIFIED copy must exit 0.

        Without it, a probe that fails for an unrelated reason (a syntax error in the
        substitution, a missing import) reads as the guard firing, and both tests
        below would pass while proving nothing.
        """
        result = subprocess.run(  # noqa: S603 — fixed argv, no user input
            [sys.executable, str(self.SOURCE)],
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0, result.stderr

    def test_an_unknown_closure_fails_the_import_itself(self, tmp_path: pathlib.Path) -> None:
        """Not merely `the function raises when called` -- THE MODULE WILL NOT LOAD.

        Someone who widens a closure literal without widening the vocabulary
        (and the arithmetic, and the model id) is stopped by their next import,
        whether or not they run ``tests/test_cost_model.py``. The marker
        derivation fires first (it sits before ``_check_closures()``), so the
        message asserted is its.
        """
        result = self._run_with_substitution(
            tmp_path,
            'CARRY_CLOSURE: CostComponentClosure = "structural_zero"',
            'CARRY_CLOSURE: CostComponentClosure = "charged"',
        )

        assert result.returncode != 0
        assert "unknown carry closure" in result.stderr

    def test_a_short_lane_fails_the_import_itself(self, tmp_path: pathlib.Path) -> None:
        """⚠⚠ THE LANE HALF OF THE GUARD, reachable only through the module-level
        ``_check_closures()`` call — the markers cannot see the lane, so deleting
        that invocation would let a short/leveraged lane import silently. This
        subprocess is what the probe harness's guard-deletion probe selects.
        """
        result = self._run_with_substitution(
            tmp_path,
            '    direction="long",\n    leverage=1,',
            '    direction="short",\n    leverage=1,',
        )

        assert result.returncode != 0
        assert "must be long and unleveraged" in result.stderr

    def test_a_non_usd_instrument_denomination_fails_the_import_itself(self, tmp_path: pathlib.Path) -> None:
        """⚠ #2833's half, and the reason it needs the subprocess arm too: the
        edit this guards is somebody pointing the lane at a GBP-denominated
        UCITS ETF because its eligibility proof answered ``usd``. Order and
        account currency both still read USD, so nothing else in the module
        objects — only the three-way comparison does, and only the module-level
        call reaches it."""
        result = self._run_with_substitution(
            tmp_path,
            '    instrument_denomination="USD",',
            '    instrument_denomination="GBP",',
        )

        assert result.returncode != 0
        assert "names more than one currency" in result.stderr

    def test_empty_carry_evidence_fails_the_import_itself(self, tmp_path: pathlib.Path) -> None:
        """The evidence half, same reasoning: only the module-level call sees it."""
        result = self._run_with_substitution(
            tmp_path,
            "CARRY_EVIDENCE: tuple[str, ...] = (",
            "CARRY_EVIDENCE: tuple[str, ...] = ()\n_UNUSED_EVIDENCE = (",
        )

        assert result.returncode != 0
        assert "no evidence" in result.stderr

    def test_a_gap_in_the_band_table_fails_the_import_itself(self, tmp_path: pathlib.Path) -> None:
        """``_check_bands_are_total`` had the same asymmetry, for the same reason.

        Its existing tests all pass hand-built tuples, so the ``_check_bands_are_total(BANDS)``
        call at module level was equally deletable. Widening the lowest band's upper
        bound without moving its neighbour's lower bound opens a hole that
        ``half_spread_for`` would raise on for an ordinary price.
        """
        result = self._run_with_substitution(
            tmp_path,
            'label="<$5", lower=None, upper=Decimal("5")',
            'label="<$5", lower=None, upper=Decimal("4")',
        )

        assert result.returncode != 0
        assert "not contiguous" in result.stderr
