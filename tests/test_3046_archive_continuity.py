"""#3046 — the archive-continuity discriminator's verdict function.

Pure-logic. The script's SQL is exercised by running it against dev; what is
pinned here is the decision, because the decision is where the invented-constant
risk lives — the verdict must be driven by the DISCREPANCY between our stored
ratio and the archive's, measured against ``params_for(asset_class)
.magnitude_threshold``, and by nothing else.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from scripts.verify_3046_archive_continuity import (
    Series,
    StoredBar,
    Transition,
    _adjudicate,
    classify_level_provenance,
    is_observed,
    tier_for,
)

PRIOR = date(2025, 6, 2)
LATER = date(2025, 8, 26)


def _transition(*, ratio: str, threshold: str = "5", rule_class: str = "T2", provisional: bool = False) -> Transition:
    value = Decimal(ratio)
    return Transition(
        instrument_id=42,
        price_date=LATER,
        prior_date=PRIOR,
        rule_class=rule_class,
        ratio=value,
        magnitude=max(value, Decimal(1) / value),
        threshold=Decimal(threshold),
        asset_class="us_equity",
        provisional=provisional,
    )


def _series(series_id: int = 1, *, vendor: str = "vendorA", basis: str = "split_adjusted") -> Series:
    return Series(
        series_id=series_id,
        instrument_id=42,
        vendor=vendor,
        upstream_source="yahoo_derivative",
        adjustment_basis=basis,
        first_bar=date(2000, 1, 1),
        last_bar=date(2026, 1, 1),
    )


def _bar(
    price_date: date = PRIOR,
    *,
    high: str = "10",
    low: str = "10",
    close: str = "10",
    volume: str | None = None,
    range_usable: bool | None = None,
) -> StoredBar:
    """Zero-range, volume-less by default — the shape carry-forward takes."""
    return StoredBar(
        price_date=price_date,
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=None if volume is None else Decimal(volume),
        range_usable=range_usable,
    )


class TestReachability:
    def test_no_series_at_all_is_not_a_verdict(self) -> None:
        verdict, readings = _adjudicate(_transition(ratio="10"), [], {})
        assert (verdict, readings) == ("no_archive_series", [])

    def test_a_series_that_does_not_span_the_pair_is_not_a_verdict(self) -> None:
        narrow = _series()._replace(first_bar=date(2025, 7, 1))
        verdict, _ = _adjudicate(_transition(ratio="10"), [narrow], {})
        assert verdict == "pair_outside_span"

    def test_spanning_series_missing_a_bar_is_not_a_verdict(self) -> None:
        """⚠ Exact dates only — a T2 pair spans a hole, so a nearest-date
        substitution would compute a return over a different span."""
        closes = {(1, PRIOR): Decimal("10")}
        verdict, _ = _adjudicate(_transition(ratio="10"), [_series()], closes)
        assert verdict == "archive_bar_missing"

    def test_a_non_positive_archive_close_is_its_own_state_not_an_absence(self) -> None:
        """Folding it into ``archive_bar_missing`` would report a bar we hold and
        cannot use as a bar we do not hold."""
        closes = {(1, PRIOR): Decimal("0"), (1, LATER): Decimal("100")}
        verdict, _ = _adjudicate(_transition(ratio="10"), [_series()], closes)
        assert verdict == "archive_bar_unusable"

    def test_a_provisional_endpoint_is_refused_before_any_archive_lookup(self) -> None:
        """``price_quarantine`` defers T3 on a provisional bar because a
        part-session bar is never verdict-bearing corroboration. Adjudicating one
        from outside would be that rule broken through the side door."""
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("100")}
        verdict, readings = _adjudicate(_transition(ratio="10", provisional=True), [_series()], closes)
        assert (verdict, readings) == ("provisional_deferred", [])


class TestVerdict:
    def test_an_archive_that_made_the_same_move_agrees(self) -> None:
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("100")}
        verdict, readings = _adjudicate(_transition(ratio="10"), [_series()], closes)
        assert verdict == "sources_agree"
        assert readings[0].discrepancy == Decimal(1)

    def test_a_continuous_archive_makes_the_shift_ours(self) -> None:
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("10")}
        verdict, readings = _adjudicate(_transition(ratio="10"), [_series()], closes)
        assert verdict == "sources_disagree"
        assert readings[0].discrepancy == Decimal(10)

    def test_a_near_miss_on_the_trigger_is_agreement_not_a_finding(self) -> None:
        """⚠ The discarded "does the archive ALSO clear T" formulation called
        ours=6x / archive=4.9x a finding. The two series moved within 22% of each
        other; the only thing that differed was which side of a daily-move
        trigger they each landed on."""
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("49")}
        verdict, _ = _adjudicate(_transition(ratio="6"), [_series()], closes)
        assert verdict == "sources_agree"

    def test_a_real_move_with_a_scale_error_on_top_is_still_a_finding(self) -> None:
        """⚠ And the same discarded formulation called ours=25x / archive=5x
        corroborated, because both cleared T — leaving a 5x scale error
        unreported on the grounds that something real also happened."""
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("50")}
        verdict, readings = _adjudicate(_transition(ratio="25"), [_series()], closes)
        assert verdict == "sources_disagree"
        assert readings[0].discrepancy == Decimal(5)

    def test_the_threshold_is_the_transitions_own_and_not_a_constant(self) -> None:
        """Identical bars: a 3x discrepancy clears `commodity`'s T=2 and not
        `us_equity`'s T=5."""
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("20")}
        assert _adjudicate(_transition(ratio="6", threshold="2"), [_series()], closes)[0] == "sources_disagree"
        assert _adjudicate(_transition(ratio="6", threshold="5"), [_series()], closes)[0] == "sources_agree"

    def test_the_bound_is_inclusive_like_the_classifiers(self) -> None:
        """``price_quarantine`` triggers on ``magnitude >= threshold``; exactly
        at the bound must be a finding, not fall through."""
        closes = {(1, PRIOR): Decimal("10"), (1, LATER): Decimal("20")}
        verdict, readings = _adjudicate(_transition(ratio="10"), [_series()], closes)
        assert readings[0].discrepancy == Decimal(5)
        assert verdict == "sources_disagree"

    def test_an_equal_move_the_OTHER_way_disagrees_without_a_direction_rule(self) -> None:
        """Direction needs no separate clause: opposing moves square the
        discrepancy rather than cancelling into agreement."""
        closes = {(1, PRIOR): Decimal("100"), (1, LATER): Decimal("10")}
        verdict, readings = _adjudicate(_transition(ratio="10"), [_series()], closes)
        assert verdict == "sources_disagree"
        assert readings[0].discrepancy == Decimal(100)

    def test_a_downward_transition_agrees_with_a_downward_archive(self) -> None:
        closes = {(1, PRIOR): Decimal("100"), (1, LATER): Decimal("10")}
        verdict, _ = _adjudicate(_transition(ratio="0.1"), [_series()], closes)
        assert verdict == "sources_agree"


class TestOneUpstreamOneObservation:
    """⚠ Both vendors are yahoo derivatives — ONE observation, never two votes."""

    def test_two_vendors_that_agree_yield_one_verdict_with_both_readings(self) -> None:
        closes = {
            (1, PRIOR): Decimal("10"),
            (1, LATER): Decimal("100"),
            (2, PRIOR): Decimal("20"),
            (2, LATER): Decimal("200"),
        }
        verdict, readings = _adjudicate(
            _transition(ratio="10"), [_series(1), _series(2, vendor="vendorB", basis="unadjusted")], closes
        )
        assert verdict == "sources_agree"
        assert {r.vendor for r in readings} == {"vendorA", "vendorB"}

    def test_disagreement_is_reported_and_never_resolved_by_majority(self) -> None:
        closes = {
            (1, PRIOR): Decimal("10"),
            (1, LATER): Decimal("100"),
            (2, PRIOR): Decimal("20"),
            (2, LATER): Decimal("21"),
        }
        verdict, readings = _adjudicate(_transition(ratio="10"), [_series(1), _series(2, vendor="vendorB")], closes)
        assert verdict == "vendor_disagreement"
        assert {r.disagrees for r in readings} == {True, False}

    def test_endpoints_are_never_mixed_across_series(self) -> None:
        """One series holds the prior close, the other the later one. Pairing
        them would manufacture a return out of two adjustment bases."""
        closes = {(1, PRIOR): Decimal("10"), (2, LATER): Decimal("100")}
        verdict, readings = _adjudicate(_transition(ratio="10"), [_series(1), _series(2, vendor="vendorB")], closes)
        assert (verdict, readings) == ("archive_bar_missing", [])


class TestIsObserved:
    """⚠ Every clause here is a SOURCE RULE, not a preference. See the spec addendum."""

    def test_positive_volume_is_an_observation(self) -> None:
        assert is_observed(_bar(volume="1000"), range_verdict_known=True)

    def test_zero_volume_is_not(self) -> None:
        """``price_quarantine._usable_volume`` rejects ``<= 0``; ``IS NOT NULL``
        would admit a zero as evidence of trading against our own rule set."""
        assert not is_observed(_bar(volume="0"), range_verdict_known=True)

    def test_a_usable_intrabar_range_is_an_observation(self) -> None:
        assert is_observed(_bar(high="11", low="10"), range_verdict_known=True)

    def test_a_range_the_quarantine_condemned_is_not(self) -> None:
        """sql/247: B2/B3 set ``range_usable=false`` and leave the return axis
        alone. A known phantom wick is not evidence that the session traded."""
        assert not is_observed(_bar(high="11", low="10", range_usable=False), range_verdict_known=True)

    def test_range_is_not_evidence_on_an_unevaluated_instrument(self) -> None:
        """Fail-closed: the verdict tables are SPARSE, so absence of a row means
        'clean' only where a coverage row says the instrument was evaluated."""
        assert not is_observed(_bar(high="11", low="10"), range_verdict_known=False)

    def test_volume_still_carries_an_unevaluated_bar(self) -> None:
        assert is_observed(_bar(high="10", low="10", volume="5"), range_verdict_known=False)


class TestLevelProvenance:
    def test_an_observed_bar_is_its_own_level(self) -> None:
        bars = [_bar(date(2025, 1, 2), close="10", volume="5")]
        assert classify_level_provenance(bars, 0, range_verdict_known=True) == ("observed", date(2025, 1, 2))

    def test_a_repeat_run_reaching_an_observed_bar_is_STALE_not_fabricated(self) -> None:
        """⚠ The class revision 1 of the spec did not have. ``observed 100 ->
        carried 100 -> hole -> observed 10`` can be a real scale error; only the
        earlier operand's DATE is wrong, so exonerating it would be the defect."""
        bars = [
            _bar(date(2025, 1, 2), close="100", volume="7"),
            _bar(date(2025, 1, 3), close="100"),
            _bar(date(2025, 1, 6), close="100"),
        ]
        assert classify_level_provenance(bars, 2, range_verdict_known=True) == (
            "stale_observed_level",
            date(2025, 1, 2),
        )

    def test_a_repeat_run_reaching_the_series_start_is_fabricated(self) -> None:
        bars = [_bar(date(2025, 6, 28), close="20"), _bar(date(2025, 6, 29), close="20")]
        assert classify_level_provenance(bars, 1, range_verdict_known=True) == ("fabricated_level", None)

    def test_a_lone_unobserved_first_bar_is_fabricated_too(self) -> None:
        bars = [_bar(date(2025, 6, 28), close="20")]
        assert classify_level_provenance(bars, 0, range_verdict_known=True) == ("fabricated_level", None)

    def test_a_zero_range_bar_at_a_NEW_level_is_undecided(self) -> None:
        """A one-quote session on a thin name has this shape and so does a
        placeholder. Neither reading is asserted."""
        bars = [_bar(date(2025, 1, 2), close="10", volume="7"), _bar(date(2025, 1, 3), close="12")]
        assert classify_level_provenance(bars, 1, range_verdict_known=True) == ("zero_range_new_level", None)

    def test_the_walk_stops_at_the_first_observed_bar_not_the_oldest(self) -> None:
        bars = [
            _bar(date(2025, 1, 2), close="100", volume="9"),
            _bar(date(2025, 1, 3), close="100", volume="9"),
            _bar(date(2025, 1, 6), close="100"),
        ]
        assert classify_level_provenance(bars, 2, range_verdict_known=True)[1] == date(2025, 1, 3)


class TestTier:
    def test_fabricated_wins_over_every_other_shape(self) -> None:
        assert tier_for("fabricated_level", "observed") == "A_fabricated_prior_level"
        assert tier_for("observed", "absent") == "A_fabricated_prior_level"

    def test_stale_outranks_degenerate(self) -> None:
        assert tier_for("stale_observed_level", "zero_range_new_level") == "B_stale_level"

    def test_one_degenerate_endpoint_is_enough_to_leave_the_residual(self) -> None:
        assert tier_for("observed", "zero_range_new_level") == "C_degenerate_endpoint"

    def test_only_two_observed_endpoints_are_the_genuine_residual(self) -> None:
        assert tier_for("observed", "observed") == "D_both_endpoints_observed"
