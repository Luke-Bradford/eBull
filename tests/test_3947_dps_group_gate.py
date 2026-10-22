"""#3947 Amendment 1 A3: decisions reach a fiscal-year group only when every leaf is verified.

Spec ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §A3 and the amendment's fixtures. Expected values
are written by hand. Each case derives both arms through ``_derive_periods_from_facts`` and gates them with
``_gate_dps_groups``, as ``derive_gated_dps_periods`` does after reading its inputs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

from app.services.fundamentals import (
    DpsGroupOutcome,
    FactRow,
    PeriodRow,
    _derive_periods_from_facts,
    _dps_fact_inline_scale_decisions,
    _gate_dps_groups,
)
from app.services.inline_xbrl_dps import Occurrence

CONCEPT = "CommonStockDividendsPerShareDeclared"
CIKS = frozenset({"0001411342"})
_Q = {
    1: (date(2025, 1, 1), date(2025, 3, 31)),
    2: (date(2025, 4, 1), date(2025, 6, 30)),
    3: (date(2025, 7, 1), date(2025, 9, 30)),
}


def _fact(val: str, start: date, end: date, accession: str, fp: str, *, concept: str = CONCEPT) -> FactRow:
    return FactRow(
        concept=concept,
        unit="USD/shares",
        period_start=start,
        period_end=end,
        val=Decimal(val),
        frame=None,
        form_type="10-K" if fp == "FY" else "10-Q",
        fiscal_year=2025,
        fiscal_period=fp,
        accession_number=accession,
        filed_date=date(2026, 2, 20) if fp == "FY" else end + timedelta(30),
    )


def _quarter(n: int, val: str = "0.10", *, concept: str = CONCEPT) -> FactRow:
    return _fact(val, *_Q[n], f"acc-q{n}", f"Q{n}", concept=concept)


def _fy(val: str = "520") -> FactRow:
    return _fact(val, date(2025, 1, 1), date(2025, 12, 31), "acc-fy", "FY")


def _revenue(fact: FactRow) -> FactRow:
    return replace(fact, concept="Revenues", unit="USD", val=Decimal("1000"))


def _occ(fact: FactRow, scale: int, *, ordinal: int = 0) -> Occurrence:
    return Occurrence(
        document="a.htm",
        element_ordinal=ordinal,
        fact_id=None,
        context_ref_raw="c",
        unit_ref_raw="u",
        scale_raw=str(scale),
        sign_raw=None,
        format_raw=None,
        decimals_raw=None,
        display_text=str(fact.val.scaleb(-scale)),
        entity_cik="0001411342",
        period_start=fact.period_start,
        period_end=fact.period_end,
        dimensional=False,
        unit="USD/shares",
        scale=scale,
        instance_value=fact.val,
        unresolved=None,
    )


def _gated(
    facts: Sequence[FactRow],
    scanned: Mapping[str, Sequence[Occurrence]],
    stored: Mapping[tuple[int, str, date], Decimal | None] | None = None,
) -> tuple[dict[str, PeriodRow], dict[str, PeriodRow], DpsGroupOutcome]:
    """``(gated rows, decisions-off rows, the 2025 group's outcome)``; an accession absent from ``scanned`` is
    untested."""
    decisions = _dps_fact_inline_scale_decisions(facts, dict.fromkeys(scanned, "inline"), CIKS, scanned)
    restored = {f: d.value for f, d in decisions.items() if d.action == "restore"}
    off = _derive_periods_from_facts(facts)
    on = _derive_periods_from_facts(facts, dps_values=restored)
    outcomes = _gate_dps_groups(off, on, decisions, stored or {})
    fresh_off = {p.period_type: p for p in _derive_periods_from_facts(facts)}
    return {p.period_type: p for p in off}, fresh_off, outcomes[2025]


def _year_facts(fy: FactRow) -> list[FactRow]:
    return [_quarter(1), _quarter(2), _quarter(3), fy, _revenue(fy)]


def _all_unscaled(facts: Sequence[FactRow]) -> dict[str, list[Occurrence]]:
    return {f.accession_number: [_occ(f, 0)] for f in facts if f.concept == CONCEPT}


def test_a_fully_verified_group_takes_the_restored_values_and_its_q4_follows() -> None:
    fy = _fy()
    facts = _year_facts(fy)
    rows, _, outcome = _gated(facts, {**_all_unscaled(facts), "acc-fy": [_occ(fy, 3)]})
    assert outcome == DpsGroupOutcome(2025, "on", "verified")
    assert rows["FY"].dps_declared == Decimal("0.52")
    assert rows["Q4"].dps_declared == Decimal("0.22")  # 0.52 − 0.10 × 3, over the cells shown beside it
    assert rows["FY"].revenue == Decimal("1000")
    assert len(rows["Q4"].dps_leaves) == 4  # FY and Q1–Q3


def test_an_untested_quarter_holds_the_whole_group_at_decisions_off() -> None:
    fy = _fy()
    facts = _year_facts(fy)
    scanned = {**_all_unscaled(facts), "acc-fy": [_occ(fy, 3)]}
    del scanned["acc-q3"]  # Q3's accession was never scanned
    rows, off, outcome = _gated(facts, scanned)
    assert outcome == DpsGroupOutcome(2025, "off", "unverified_leaf")
    assert rows["FY"].dps_declared == Decimal("520") == off["FY"].dps_declared
    assert rows["Q4"].dps_declared == Decimal("519.70") == off["Q4"].dps_declared


def test_a_q4_whose_q2_is_ytd_filled_from_an_unverified_leaf_keeps_decisions_off() -> None:
    fy = _fy()
    ytd6 = _fact("0.20", date(2025, 1, 1), date(2025, 6, 30), "acc-q2", "Q2")
    q2_revenue = _revenue(_quarter(2))  # the Q2 row exists; its DPS comes only from YTD6 − Q1
    facts = [_quarter(1), ytd6, q2_revenue, _quarter(3), fy, _revenue(fy)]
    scanned = {"acc-q1": [_occ(_quarter(1), 0)], "acc-q3": [_occ(_quarter(3), 0)], "acc-fy": [_occ(fy, 3)]}
    rows, off, outcome = _gated(facts, scanned)  # acc-q2 untested
    assert rows["Q2"].dps_leaves == (ytd6, _quarter(1))
    assert outcome == DpsGroupOutcome(2025, "off", "unverified_leaf")
    assert rows["FY"].dps_declared == Decimal("520")
    assert rows["Q4"].dps_declared == off["Q4"].dps_declared == Decimal("519.70")


def test_a_mixed_scale_fact_keeps_its_group_at_decisions_off() -> None:
    fy = _fy()
    facts = _year_facts(fy)
    rows, off, outcome = _gated(facts, {**_all_unscaled(facts), "acc-fy": [_occ(fy, 3), _occ(fy, 0, ordinal=1)]})
    assert outcome == DpsGroupOutcome(2025, "off", "unverified_leaf")
    assert {k: r.dps_declared for k, r in rows.items()} == {k: r.dps_declared for k, r in off.items()}


def test_a_negative_quarter_introduced_by_a_restore_falls_the_group_back() -> None:
    # Q3 = YTD9 − YTD6. Restoring YTD9 (150 at scale 3 → 0.15) below the unscaled YTD6 (0.20) would make Q3 negative.
    fy = _fy("0.40")
    ytd6 = _fact("0.20", date(2025, 1, 1), date(2025, 6, 30), "acc-q2", "Q2")
    ytd9 = _fact("150", date(2025, 1, 1), date(2025, 9, 30), "acc-q3", "Q3")
    facts = [_quarter(1), _quarter(2), ytd6, ytd9, _revenue(_quarter(3)), fy, _revenue(fy)]
    scanned = {
        "acc-q1": [_occ(_quarter(1), 0)],
        "acc-q2": [_occ(_quarter(2), 0), _occ(ytd6, 0, ordinal=1)],
        "acc-q3": [_occ(ytd9, 3)],
        "acc-fy": [_occ(fy, 0)],
    }
    rows, _, outcome = _gated(facts, scanned)
    assert outcome == DpsGroupOutcome(2025, "off", "negative_derived")
    assert rows["Q3"].dps_declared == Decimal("149.80")
    # The displayed Q4 still equals FY − Q1 − Q2 − Q3 over the cells beside it.
    fy_, q1, q2, q3, q4 = (rows[k].dps_declared or Decimal("NaN") for k in ("FY", "Q1", "Q2", "Q3", "Q4"))
    assert q4 == fy_ - q1 - q2 - q3


def test_an_alias_concept_leaf_keeps_decisions_off() -> None:
    fy = _fy()
    alias = "DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit"
    facts = [_quarter(1, concept=alias), _quarter(2), _quarter(3), fy, _revenue(fy)]
    rows, _, outcome = _gated(facts, {**_all_unscaled(facts), "acc-fy": [_occ(fy, 3)]})
    assert outcome == DpsGroupOutcome(2025, "off", "other_concept")
    assert rows["FY"].dps_declared == Decimal("520")


def test_an_incomplete_group_keeps_its_stored_dps() -> None:
    # Over swept raw only the FY survives; the stored quarters were derived from aged-out facts.
    fy = _fy()
    stored = {
        (2025, "FY", date(2025, 12, 31)): Decimal("520"),
        (2025, "Q1", date(2025, 3, 31)): Decimal("0.10"),
        (2025, "Q4", date(2025, 12, 31)): Decimal("519.70"),
    }
    rows, _, outcome = _gated([fy, _revenue(fy)], {"acc-fy": [_occ(fy, 3)]}, stored)
    assert outcome == DpsGroupOutcome(2025, "kept_stored", "incomplete_group")
    assert set(rows) == {"FY"}
    assert rows["FY"].dps_declared == Decimal("520")  # not the restored 0.52 beside stored quarters of the raw regime
    assert rows["FY"].revenue == Decimal("1000")  # every other column is written as today


def test_a_complete_group_is_gated_even_with_stored_rows() -> None:
    fy = _fy()
    facts = _year_facts(fy)
    stored = {(2025, k, date(2025, 12, 31)): Decimal("9") for k in ("FY", "Q1", "Q2", "Q3", "Q4")}
    rows, _, outcome = _gated(facts, {**_all_unscaled(facts), "acc-fy": [_occ(fy, 3)]}, stored)
    assert outcome.branch == "on"
    assert rows["FY"].dps_declared == Decimal("0.52")


def test_rows_that_differ_between_the_derivations_keep_decisions_off() -> None:
    fy = _fy()
    off = _derive_periods_from_facts(_year_facts(fy))
    on = _derive_periods_from_facts(_year_facts(fy))[:-1]
    outcomes = _gate_dps_groups(off, on, {}, {})
    assert outcomes == {2025: DpsGroupOutcome(2025, "off", "structure_mismatch")}
    assert {r.period_type: r.dps_declared for r in off}["FY"] == Decimal("520")


def test_an_incomplete_group_copies_a_stored_value_only_onto_the_same_row() -> None:
    # A #1914/#541 collision row (another period end under the same label) keeps its decisions-off value.
    def fy_row(end: date, dps: str) -> PeriodRow:
        return PeriodRow(end, "FY", 2025, None, None, 12, dps_declared=Decimal(dps))

    rows = [fy_row(date(2025, 12, 31), "520"), fy_row(date(2026, 2, 28), "0.30")]
    stored = {(2025, "FY", date(2025, 12, 31)): Decimal("0.52"), (2025, "Q1", date(2025, 3, 31)): Decimal("0.10")}
    outcomes = _gate_dps_groups(rows, list(rows), {}, stored)
    assert outcomes == {2025: DpsGroupOutcome(2025, "kept_stored", "incomplete_group")}
    assert [r.dps_declared for r in rows] == [Decimal("0.52"), Decimal("0.30")]
