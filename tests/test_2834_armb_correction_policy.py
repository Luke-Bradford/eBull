"""Pure-logic tests for #2834 §7 item 1's arm partition. Refs #2834.

No DB. The full-population output is the measurement's evidence; what is pinned
here is the one piece of logic the verdict's conclusion actually turns on —
which events each policy applies. Every failure direction is silent:

* **unadjudicable is not refuted.** An event the reference vendor cannot speak
  to has ``split_error is None``. If ``apply_unless_refuted`` dropped those too
  it would collapse into ``apply_only_corroborated``, and the verdict's decisive
  contrast (0.50% against 9.51% of the decile) would be measuring one arm twice.
  That is the precise confusion the two arms exist to separate, and both would
  still produce a plausible-looking percentage.
* **corroboration is a positive test, not the absence of refutation.** Building
  ``apply_only_corroborated`` from ``not refuted`` would silently include the
  47.69% of events with no second processing — the exact population whose
  survivorship asymmetry (1.67% delisted served, 17.33% unserved) is the reason
  that arm is rejected.
* **the arms are nested and must stay so**: corroborated ⊆ not-refuted ⊆ all.
  A partition bug that broke the nesting would make the pairwise displacement
  table incomparable without changing its shape.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from scripts.measure_2834_armb_correction_policy import build_arms
from scripts.measure_2834_armb_split_only_basis import EventCheck, SplitEvent


def _event(series_id: int, day: int) -> SplitEvent:
    return SplitEvent(series_id=series_id, vendor_symbol="X", bar_date=date(2020, 1, day), factor=Decimal(2))


def _check(series_id: int, day: int, split_error: str | None) -> EventCheck:
    return EventCheck(
        event=_event(series_id, day),
        internal_error=None,
        raw_error=None,
        split_error=None if split_error is None else Decimal(split_error),
    )


#: One of each kind. ``0.001`` is inside the predecessor's 1% tolerance and
#: ``0.5`` is far outside it, so neither is a boundary case in disguise.
_CORROBORATED = _check(1, 1, "0.001")
_REFUTED = _check(2, 2, "0.5")
_UNADJUDICABLE = _check(3, 3, None)
_CHECKS = [_CORROBORATED, _REFUTED, _UNADJUDICABLE]


class TestBuildArms:
    def test_apply_all_takes_every_checked_event(self) -> None:
        assert build_arms(_CHECKS)["apply_all"] == [c.event for c in _CHECKS]

    def test_unless_refuted_keeps_the_unadjudicable(self) -> None:
        arm = build_arms(_CHECKS)["apply_unless_refuted"]
        assert _UNADJUDICABLE.event in arm
        assert _CORROBORATED.event in arm
        assert _REFUTED.event not in arm

    def test_only_corroborated_drops_the_unadjudicable_too(self) -> None:
        arm = build_arms(_CHECKS)["apply_only_corroborated"]
        assert arm == [_CORROBORATED.event]

    def test_arms_are_nested(self) -> None:
        arms = build_arms(_CHECKS)
        corroborated = set(arms["apply_only_corroborated"])
        not_refuted = set(arms["apply_unless_refuted"])
        every = set(arms["apply_all"])
        assert corroborated <= not_refuted <= every

    def test_empty_input_yields_empty_arms(self) -> None:
        assert build_arms([]) == {
            "apply_all": [],
            "apply_unless_refuted": [],
            "apply_only_corroborated": [],
        }
