"""#2232 — the partial-class denominator guard, as pure policy + payload shape.

No DB. The two DB-touching halves have their own homes: the SQL predicate lives in
``ownership_rollup._read_has_dei_cover_share_count`` and is exercised end-to-end by
``tests/test_ownership_rollup.py::TestPartialClassDenominator``; everything below is
the decision itself and the ``no_data`` payload it produces.

Why the policy is shaped this way (the two conditions are impossibility arguments,
not thresholds):

* The issue's own worked examples rule magnitude out. ``PKG`` drops 1,001x and the
  DROP is the correction; ``AVAL`` drops 1.08e9x and the drop is the error. A
  size/ratio cut cannot separate them.
* The previous attempt's ``denominator_cross_check`` was ``unavailable`` for 24 of
  the 47 instruments in the live cohort, because those issuers file only ONE shares
  concept — so a cross-concept comparison is structurally blind to exactly the
  population the ticket is about.
"""

from __future__ import annotations

from dataclasses import fields
from datetime import date
from decimal import Decimal

import pytest

from app.services.ownership_rollup import OwnershipRollup, denominator_is_partial_class


class TestDenominatorIsPartialClass:
    """Truth table for the pure policy. Both conditions required; the per-class
    swap short-circuits."""

    @pytest.mark.parametrize(
        ("has_dei", "largest_pct", "per_class", "expected"),
        [
            # The defect: no cover-page count on file AND a single holder exceeds
            # the figure we would divide by (AEVEX: 1,000 stored, 57,564,830 held).
            (False, Decimal("57564.830"), False, True),
            # Barely over is still over — Rule 13d-3 admits no >100% single owner.
            (False, Decimal("1.0001"), False, True),
            # Exactly 100% is possible (a wholly-owned registrant), so not the defect.
            (False, Decimal("1"), False, False),
            # A cover-page count EXISTS → sec-edgar §7.17's "every cover value was
            # dimensional" argument does not hold, so the over-100% holder is a
            # numerator problem (#2231 split staleness / #2230 deemed attribution),
            # not this one. Leaving it alone is what keeps those tickets measurable.
            (True, Decimal("57564.830"), False, False),
            # No cover count but every holder fits — Alphabet's shape. The
            # undimensioned us-gaap figure IS the combined total there.
            (False, Decimal("0.0289"), False, False),
            (True, Decimal("0.0289"), False, False),
            # A verified FSDS per-class denominator already ran this exact
            # plausibility guard (``_should_use_class_denominator`` condition 3)
            # before it was taken, so it can never be the partial one.
            (False, Decimal("57564.830"), True, False),
            (True, Decimal("57564.830"), True, False),
        ],
    )
    def test_truth_table(self, has_dei: bool, largest_pct: Decimal, per_class: bool, expected: bool) -> None:
        assert (
            denominator_is_partial_class(
                has_dei_cover_share_count=has_dei,
                largest_single_holder_pct=largest_pct,
                per_class_denominator_applied=per_class,
            )
            is expected
        )

    def test_zero_holders_never_fires(self) -> None:
        """An instrument with no holders at all has ``largest_single_holder_pct``
        of exactly 0. Absence of evidence must not suppress the card — the
        ordinary ``unknown_universe`` empty state is the honest one there."""
        assert (
            denominator_is_partial_class(
                has_dei_cover_share_count=False,
                largest_single_holder_pct=Decimal(0),
                per_class_denominator_applied=False,
            )
            is False
        )


class TestPartialClassNoDataPayload:
    """The payload the guard returns. The FE discriminator is the point of the
    change, so it is asserted directly."""

    def _payload(self, as_of: date | None = date(2026, 3, 31)) -> OwnershipRollup:
        return OwnershipRollup.no_data(
            symbol="ZAVEX",
            instrument_id=999_001,
            reason="partial_class_denominator",
            stale_as_of=as_of,
        )

    def test_reason_is_carried_explicitly(self) -> None:
        assert self._payload().no_data_reason == "partial_class_denominator"

    def test_as_of_survives_and_is_fresh(self) -> None:
        """⚠ This is exactly why ``no_data_reason`` had to be added. The payload
        keeps a FRESH ``shares_outstanding_as_of``, so the pre-#2232 frontend
        inference ("no_data + non-null as_of ⇒ stale") would have labelled a
        2026 figure "too stale to use"."""
        rollup = self._payload()
        assert rollup.shares_outstanding_as_of == date(2026, 3, 31)
        assert rollup.shares_outstanding is None

    def test_banner_names_the_cause_and_never_advises_a_sync(self) -> None:
        """Unlike #1581's cause-agnostic copy this one CAN name the cause. It must
        still not tell the operator to trigger a fundamentals sync: re-fetching
        returns the same dimension-stripped companyfacts payload (§7.17), so the
        instruction can never work."""
        banner = self._payload().banner
        assert banner.state == "no_data"
        assert banner.variant == "error"
        assert "does not cover the whole company" in banner.body
        assert "too stale" not in banner.body.lower()
        assert "fundamentals sync" not in banner.body.lower()

    def test_other_reasons_keep_their_own_copy_and_tag(self) -> None:
        absent = OwnershipRollup.no_data(symbol="ZNONE", instrument_id=999_002)
        assert absent.no_data_reason == "absent"
        assert absent.shares_outstanding_as_of is None
        assert "does not cover the whole company" not in absent.banner.body

        stale = OwnershipRollup.no_data(
            symbol="ZOLD",
            instrument_id=999_003,
            reason="stale_denominator",
            stale_as_of=date(2011, 4, 29),
        )
        assert stale.no_data_reason == "stale_denominator"
        assert "too stale" in stale.banner.body

    def test_rendering_payloads_default_to_no_reason(self) -> None:
        """``no_data_reason`` must default to ``None``, so a consumer can use it as
        the SOLE discriminator: only the ``no_data`` classmethod ever sets it, and
        every ordinary construction in ``get_ownership_rollup`` leaves it unset.
        (The rendering path is asserted end-to-end in the DB suite.)"""
        field = next(f for f in fields(OwnershipRollup) if f.name == "no_data_reason")
        assert field.default is None
