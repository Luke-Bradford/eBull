"""#3947 DPS inline-scale decisions and where they act (spec §The correction; acceptance 3, decision fixtures).

Expected values are written by hand: one case per decision-table row, scale 3 and scale 6. The Amendment 1 A3 group
gate has its own module, ``tests/test_3947_dps_group_gate.py``.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.fundamentals import (
    DpsScaleDecision,
    FactRow,
    PeriodRow,
    _derive_periods_from_facts,
    _dps_fact_inline_scale_decisions,
)
from app.services.inline_xbrl_dps import Occurrence

CONCEPT = "CommonStockDividendsPerShareDeclared"
ACC = "0001411342-25-000001"
CIKS = frozenset({"0001411342"})


def _fact(
    val: str = "390",
    *,
    start: date | None = date(2025, 7, 1),
    end: date = date(2025, 9, 30),
    accession: str = ACC,
    filed: date = date(2025, 11, 1),
    unit: str = "USD/shares",
    fiscal_period: str = "Q3",
    form_type: str = "10-Q",
    concept: str = CONCEPT,
) -> FactRow:
    return FactRow(
        concept=concept,
        unit=unit,
        period_start=start,
        period_end=end,
        val=Decimal(val),
        frame=None,
        form_type=form_type,
        fiscal_year=2025,
        fiscal_period=fiscal_period,
        accession_number=accession,
        filed_date=filed,
    )


def _occ(
    instance_value: str | None = "390",
    scale: int | None = 3,
    *,
    ordinal: int = 0,
    start: date | None = date(2025, 7, 1),
    end: date | None = date(2025, 9, 30),
    dimensional: bool | None = False,
    unit: str | None = "USD/shares",
    cik: str | None = "0001411342",
    unresolved: str | None = None,
) -> Occurrence:
    return Occurrence(
        document="a.htm",
        element_ordinal=ordinal,
        fact_id=None,
        context_ref_raw="c1",
        unit_ref_raw="u",
        scale_raw=None if scale is None else str(scale),
        sign_raw=None,
        format_raw=None,
        decimals_raw=None,
        display_text="0.39",
        entity_cik=cik,
        period_start=start,
        period_end=end,
        dimensional=dimensional,
        unit=unit,
        scale=scale,
        instance_value=None if instance_value is None else Decimal(instance_value),
        unresolved=unresolved,
    )


def _decide(
    fact: FactRow, occurrences: list[Occurrence], *, outcome: str | None = "inline", ciks: frozenset[str] = CIKS
) -> DpsScaleDecision:
    scans = {} if outcome is None else {fact.accession_number: outcome}
    return _dps_fact_inline_scale_decisions([fact], scans, ciks, {fact.accession_number: occurrences})[fact]


@pytest.mark.parametrize(
    ("fact", "occurrences", "outcome", "reason"),
    [
        # 1: filed before the 2019-06-15 cutoff
        (_fact(filed=date(2019, 6, 14)), [_occ()], "inline", "out_of_scope"),
        # 2: not USD/shares
        (_fact(unit="USD"), [_occ()], "inline", "unsupported_unit"),
        # 3: no active generation
        (_fact(), [], None, "untested"),
        # 4: not inline
        (_fact(), [], "not_inline", "not_inline"),
        # 5: an occurrence of another entity, anywhere in the accession
        (_fact(), [_occ(), _occ("1", 0, ordinal=1, cik="0000000001")], "inline", "entity_mismatch"),
        # 6: an unresolved occurrence, anywhere in the accession
        (_fact(), [_occ(), _occ(None, None, ordinal=1, unresolved="unit_ref")], "inline", "unresolved"),
        # 7: no occurrence with this period and value (dimensional and unsupported-unit ones never match)
        (
            _fact(),
            [_occ(dimensional=True), _occ(ordinal=1, unit="unsupported_unit"), _occ("391", ordinal=2)],
            "inline",
            "no_match",
        ),
        # 10: matched at scale 0
        (_fact(), [_occ(scale=0)], "inline", "unscaled"),
        # 10: matched at a negative scale only (a display in cents encodes the right instance value)
        (_fact(), [_occ(scale=-2)], "inline", "unscaled"),
    ],
)
def test_keep_rows(fact: FactRow, occurrences: list[Occurrence], outcome: str | None, reason: str) -> None:
    decision = _decide(fact, occurrences, outcome=outcome)
    assert (decision.action, decision.reason, decision.value) == ("keep", reason, fact.val)


def test_row_8_mixed_scales_keep_unverified() -> None:
    # `0.39` at scale 3 and `390` at scale 0 encode the same instance value but imply different repairs.
    decision = _decide(_fact(), [_occ(scale=3), _occ(scale=0, ordinal=1)])
    # Amendment 1 A3: the fact stays in both derivations at its raw value; its group falls back (unverified).
    assert decision == DpsScaleDecision("keep", "mixed_scale", None, Decimal("390"), (("a.htm", 0), ("a.htm", 1)))


@pytest.mark.parametrize(("val", "scale", "restored"), [("390", 3, "0.39"), ("1750000", 6, "1.75")])
def test_row_9_one_positive_scale_restores(val: str, scale: int, restored: str) -> None:
    decision = _decide(_fact(val), [_occ(val, scale), _occ(val, scale, ordinal=1)])
    assert decision == DpsScaleDecision("restore", "scaled", scale, Decimal(restored), (("a.htm", 0), ("a.htm", 1)))


def test_only_the_dps_concept_is_decided() -> None:
    other = _fact(concept="DistributionMadeToLimitedPartnerDistributionsPaidPerUnit")
    assert _dps_fact_inline_scale_decisions([other], {ACC: "inline"}, CIKS, {ACC: [_occ()]}) == {}


# --- where decisions act --------------------------------------------------------------------------------------


def _quarter(n: int, val: str, accession: str) -> FactRow:
    ends = {1: date(2025, 3, 31), 2: date(2025, 6, 30), 3: date(2025, 9, 30)}
    starts = {1: date(2025, 1, 1), 2: date(2025, 4, 1), 3: date(2025, 7, 1)}
    return _fact(
        val, start=starts[n], end=ends[n], accession=accession, fiscal_period=f"Q{n}", filed=ends[n] + timedelta(30)
    )


def _year(val: str, accession: str = "acc-fy", filed: date = date(2026, 2, 20)) -> FactRow:
    return _fact(
        val,
        start=date(2025, 1, 1),
        end=date(2025, 12, 31),
        accession=accession,
        fiscal_period="FY",
        form_type="10-K",
        filed=filed,
    )


def _by_type(periods: list[PeriodRow]) -> dict[str, PeriodRow]:
    return {p.period_type: p for p in periods}


def test_a_restore_changes_dps_only_and_feeds_the_derived_q4() -> None:
    quarters = [_quarter(1, "0.10", "acc-q1"), _quarter(2, "0.10", "acc-q2"), _quarter(3, "0.10", "acc-q3")]
    revenue = replace(_year("1000"), concept="Revenues", unit="USD")
    fy = _year("520")  # displayed 0.52 at scale 3
    facts = [*quarters, fy, revenue]
    off = _by_type(_derive_periods_from_facts(facts))
    on = _by_type(_derive_periods_from_facts(facts, dps_values={fy: Decimal("0.52")}))
    assert off["FY"].dps_declared == Decimal("520") and off["Q4"].dps_declared == Decimal("519.70")
    assert on["FY"].dps_declared == Decimal("0.52") and on["Q4"].dps_declared == Decimal("0.22")
    # Nothing else moves: same rows, same dates, same other columns.
    for kind in off:
        assert replace(on[kind], dps_declared=None) == replace(off[kind], dps_declared=None)


def test_a_restore_reaches_the_ytd_pool() -> None:
    # Q3 has no discrete DPS fact, only the nine-month YTD; Q3 = YTD9 − YTD6 after the restore.
    ytd6 = _fact(
        "200",
        start=date(2025, 1, 1),
        end=date(2025, 6, 30),
        accession="acc-q2",
        fiscal_period="Q2",
        filed=date(2025, 7, 28),
    )
    q2 = _fact(
        "0.10",
        start=date(2025, 4, 1),
        end=date(2025, 6, 30),
        accession="acc-q2",
        fiscal_period="Q2",
        filed=date(2025, 7, 28),
    )
    ytd9 = _fact("300", start=date(2025, 1, 1), end=date(2025, 9, 30), accession="acc-q3", fiscal_period="Q3")
    revenue_q3 = _fact("1000", start=date(2025, 7, 1), unit="USD", concept="Revenues", accession="acc-q3")
    facts = [ytd6, q2, ytd9, revenue_q3]
    off = _by_type(_derive_periods_from_facts(facts))
    on = _by_type(_derive_periods_from_facts(facts, dps_values={ytd6: Decimal("0.20"), ytd9: Decimal("0.30")}))
    assert off["Q3"].dps_declared == Decimal("100")
    assert on["Q3"].dps_declared == Decimal("0.10")
