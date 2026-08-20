"""#2232 — the partial-class denominator guard, as pure policy + payload shape.

No DB. The two DB-touching halves have their own homes: the SQL predicate lives in
``ownership_rollup._read_has_dei_cover_share_count`` and is exercised end-to-end by
``tests/test_ownership_rollup.py::TestPartialClassDenominator``; everything below is
the decision itself and the ``no_data`` payload it produces.

Why the policy is shaped this way (every condition is an impossibility argument,
not a threshold):

* The issue's own worked examples rule magnitude out. ``PKG`` drops 1,001x and the
  DROP is the correction; ``AVAL`` drops 1.08e9x and the drop is the error. A
  size/ratio cut cannot separate them.
* The previous attempt's ``denominator_cross_check`` was ``unavailable`` for 24 of
  the 47 instruments in the live cohort, because those issuers file only ONE shares
  concept — so a cross-concept comparison is structurally blind to exactly the
  population the ticket is about.
* Arm 3 (the pigeonhole) reaches what the scope arm cannot: a cover-page count that
  EXISTS and is still wrong. Its floor is not chosen — it is however many 13F
  managers filed at all, which is an independent observation per instrument. It
  counts ONLY 13F, because that is the only channel where each holder occupies a
  share no other holder occupies (Form 13F Special Instruction 5) and reports whole
  shares (Column 5); Section 16 and 13D/G fail both tests.
"""

from __future__ import annotations

from dataclasses import fields
from datetime import date
from decimal import Decimal

import pytest

from app.services.ownership_rollup import (
    Holder,
    OwnershipRollup,
    OwnershipSlice,
    _build_slice,
    count_additive_institutional_holders,
    denominator_is_partial_class,
)

# Arm 3 is inert unless the caller passes a holder count that exceeds the
# denominator, so the scope-arm cases below pin it OFF explicitly rather than
# relying on a default the function does not have.
_ARM3_OFF = {"additive_institutional_holders": 0, "outstanding": Decimal(1000)}


class TestDenominatorIsPartialClass:
    """Truth table for the pure policy. The scope arm needs both its conditions and
    is short-circuited by the per-class swap; the pigeonhole arm stands alone."""

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
    def test_scope_arm_truth_table(self, has_dei: bool, largest_pct: Decimal, per_class: bool, expected: bool) -> None:
        assert (
            denominator_is_partial_class(
                has_dei_cover_share_count=has_dei,
                largest_single_holder_pct=largest_pct,
                per_class_denominator_applied=per_class,
                **_ARM3_OFF,
            )
            is expected
        )

    @pytest.mark.parametrize(
        ("holders", "outstanding", "expected"),
        [
            # AVAL: the FY2025 20-F cover reports 7 shares while 88 distinct 13F
            # managers file a whole-share position against it (dev DB, 2026-08-20).
            # The cover fact EXISTS, so the scope arm is off and only this reaches it.
            (88, Decimal(7), True),
            # GLXY / MWH — same shape, already suppressed by arm 1. Kept because
            # arm 3 must fire on them INDEPENDENTLY: if arm 1's SQL predicate ever
            # regresses, this is the arm that still catches them.
            (493, Decimal(100), True),
            (199, Decimal(100), True),
            # One manager, one share — possible (a wholly-owned registrant), so the
            # boundary is strict ``>``, matching arm 2's treatment of exactly 100%.
            (1, Decimal(1), False),
            # The real small-float issuers the falsified magnitude cut would have
            # taken with it. Measured on the dev DB 2026-08-20: NVR 2,664,860 shares
            # / 1,060 managers is the closest non-firing instrument arm 1 does not
            # already suppress, at 0.000398; SEB.US 957,794 / 19; CVR 966,132 / 42.
            (1_060, Decimal(2_664_860), False),
            (19, Decimal(957_794), False),
            # ⚠ HQ is the residual arm 3 does NOT reach: its cover, balance-sheet
            # issued and balance-sheet outstanding all report 1 for a predecessor
            # entity, but it has ZERO 13F managers — all nine disclosed holders are
            # Section 16 / 13D-G, which this arm must exclude. Pinned so that
            # widening the count back across all channels is a visible decision.
            (0, Decimal(1), False),
        ],
    )
    def test_pigeonhole_arm_stands_alone(self, holders: int, outstanding: Decimal, expected: bool) -> None:
        """Arm 3 fires with the scope arm fully satisfied in the NEGATIVE direction
        — cover fact present, no over-100% holder — which is the configuration the
        residual actually has."""
        assert (
            denominator_is_partial_class(
                has_dei_cover_share_count=True,
                largest_single_holder_pct=Decimal("0.5"),
                per_class_denominator_applied=False,
                additive_institutional_holders=holders,
                outstanding=outstanding,
            )
            is expected
        )

    def test_pigeonhole_arm_survives_the_per_class_short_circuit(self) -> None:
        """The short-circuit's justification is that the FSDS swap already ran the
        plausibility guard against the class count. That guard is a MAGNITUDE test
        and never ran a count test, so the claim does not extend to arm 3 and the
        short-circuit must not swallow it."""
        assert (
            denominator_is_partial_class(
                has_dei_cover_share_count=True,
                largest_single_holder_pct=Decimal("0.5"),
                per_class_denominator_applied=True,
                additive_institutional_holders=88,
                outstanding=Decimal(7),
            )
            is True
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
                additive_institutional_holders=0,
                outstanding=Decimal(1000),
            )
            is False
        )


def _holder(name: str, shares: str) -> Holder:
    return Holder(
        filer_cik=None,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source="13f",
        winning_accession="acc-1",
        winning_edgar_url=None,
        as_of_date=date(2025, 12, 31),
        filer_type=None,
        dropped_sources=(),
    )


def _slice(category: str, holders: list[Holder], basis: str = "pie_wedge") -> OwnershipSlice:
    """Built through the module's own ``_build_slice`` rather than by hand, so the
    fixture cannot drift from the shape the rollup actually produces."""
    return _build_slice(category, holders, Decimal(1000), denominator_basis=basis)  # type: ignore[arg-type]


class TestCountAdditiveInstitutionalHolders:
    """The count arm 3 divides against. Wrong here and the guard is wrong — every
    exclusion below is a channel whose holders do NOT each occupy a distinct whole
    share, so counting it would make the pigeonhole unsound rather than merely
    generous."""

    def test_counts_managers_across_the_institutional_slices(self) -> None:
        """``etfs`` is the same 13F book split by filer type, so both count."""
        slices = (
            _slice("institutions", [_holder("A", "10"), _holder("B", "5")]),
            _slice("etfs", [_holder("C", "1")]),
        )
        assert count_additive_institutional_holders(slices) == 3

    def test_section_16_and_blockholders_are_excluded(self) -> None:
        """Rule 13d-5(b)(1) deems every member of a group to own ALL the group's
        securities, so N insiders / blockholders legitimately restate ONE block —
        that is #2230's mechanism, and it is exactly what the pigeonhole may not
        assume away."""
        slices = (
            _slice("insiders", [_holder("A", "10"), _holder("B", "10")]),
            _slice("blockholders", [_holder("C", "10")]),
            _slice("institutions", [_holder("D", "4")]),
        )
        assert count_additive_institutional_holders(slices) == 1

    def test_memo_overlays_are_excluded(self) -> None:
        """A DEF 14A holder is a non-additive restatement of somebody already
        counted (I21); an N-PORT fund is a subset of its manager's 13F book.

        ⚠ The last slice is the one that makes this test able to fail. Today's
        memo overlays all carry a category OUTSIDE ``_INSTITUTIONAL_CATEGORIES``,
        so the category filter alone happens to exclude them and a revert-probe
        that drops the ``denominator_basis`` check comes back NOT CAUGHT. That is
        a coincidence of the current taxonomy, not a guarantee: this is a pure
        function over an arbitrary slice sequence, and an ``institutions``-category
        overlay is precisely what the basis check exists for. Constructing one
        pins the contract instead of the coincidence."""
        slices = (
            _slice("institutions", [_holder("A", "10")]),
            _slice("def14a_unmatched", [_holder("A", "10")], basis="proxy_disclosure"),
            _slice("funds", [_holder("D", "3")], basis="institution_subset"),
            _slice("institutions", [_holder("E", "7")], basis="institution_subset"),
        )
        assert count_additive_institutional_holders(slices) == 1

    def test_sub_one_share_holders_do_not_occupy_a_whole_share(self) -> None:
        """``>= 1``, not ``> 0``. ``_build_slice`` drops zeros (#1916 Finding A) but
        keeps fractions, and fractions are real — ``ownership_insiders_current``
        held 13,017 of 170,941 fractional rows on 2026-08-20, smallest positive
        0.0015. The institutions table has none today, so this bound makes the
        whole-share premise hold by construction rather than by current data."""
        slices = (_slice("institutions", [_holder("A", "0.5"), _holder("B", "0.9999"), _holder("C", "1")]),)
        assert count_additive_institutional_holders(slices) == 1

    def test_no_slices_is_zero(self) -> None:
        assert count_additive_institutional_holders(()) == 0
        assert count_additive_institutional_holders((_slice("institutions", []),)) == 0


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
