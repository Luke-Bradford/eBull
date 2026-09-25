"""#2901 PR A: fixtures for the quality-arm construction rules 2-7.

Fundamentals fixtures build a real #3360 bundle through its builder and loader (the helpers
of ``tests/test_3360_pit_fundamentals.py``) and classify through ``r6_quality_universe``.
Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Construction", "Delivery").
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest

from app.services import r6_quality_universe as q
from app.services import security_linkage as sl
from scripts import build_2901_quality_input as build
from scripts import census_2901_quality as census
from tests.test_3360_pit_fundamentals import CIK, T1, T2, _build, _facts, _row, _subs

D = date(2020, 6, 30)  # a June formation: the fiscal year must end in 2019
MANUFACTURER = 3571

REV = ("us-gaap", "Revenues", "USD")
RCC = ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax", "USD")
COR = ("us-gaap", "CostOfRevenue", "USD")
CGS = ("us-gaap", "CostOfGoodsSold", "USD")
AST = ("us-gaap", "Assets", "USD")
EQ = ("us-gaap", "StockholdersEquity", "USD")
GP = ("us-gaap", "GrossProfit", "USD")

Rows = dict[tuple[str, str, str], list[Any]]


def _accounts(
    accn: str = "a1",
    *,
    rev: Any = 100,
    cogs: Any = 40,
    assets: Any = 200,
    equity: Any = 50,
    start: str = "2019-01-01",
    end: str = "2019-12-31",
) -> Rows:
    return {
        REV: [_row(rev, accn, start=start, end=end)],
        COR: [_row(cogs, accn, start=start, end=end)],
        AST: [_row(assets, accn, end=end)],
        EQ: [_row(equity, accn, end=end)],
    }


def _merge(*parts: Rows) -> Rows:
    merged: Rows = {}
    for part in parts:
        for key, rows in part.items():
            merged.setdefault(key, []).extend(rows)
    return merged


def _bundle(tmp_path: Path, rows: Rows, filings: list[tuple[str, str, str]]) -> Any:
    tmp_path.mkdir(parents=True, exist_ok=True)
    forms = {accn: form for accn, _, form in filings}
    for unit_rows in rows.values():
        for row in unit_rows:
            row.setdefault("form", forms.get(row["accn"], "10-K"))
    bundle, _ = _build(tmp_path, {f"CIK{CIK}.json": _facts(rows)}, {f"CIK{CIK}.json": _subs(filings)})
    return bundle


def _classify(
    tmp_path: Path, rows: Rows, filings: list[tuple[str, str, str]], *, sic: int | None = MANUFACTURER, d: date = D
) -> q.Classification:
    return q.classify(_bundle(tmp_path, rows, filings), CIK, sic, d)


# --------------------------------------------------------------------------- rules 3-7 on one issuer


def test_baseline_issuer_is_eligible_with_an_exact_gpa(tmp_path: Path) -> None:
    result = _classify(tmp_path, _accounts(rev="100.1", cogs="40.2", assets=200), [("a1", T1, "10-K")])
    assert result.rung == q.ELIGIBLE
    assert result.period == ("2019-01-01", "2019-12-31")
    assert result.fields["signs"] == "ok" and result.fields["winning_accessions"] == "one"
    assert q.gpa(result) == Fraction(599, 2000)  # (100.1 - 40.2) / 200, no float on the way


def test_restated_revenue_is_read_from_the_amendment_once_public(tmp_path: Path) -> None:
    rows = _merge(_accounts(), {REV: [_row(120, "a2", start="2019-01-01")]})
    filings = [("a1", T1, "10-K"), ("a2", T2, "10-K/A")]
    before = _classify(tmp_path / "before", rows, filings, d=date(2020, 5, 29))
    after = _classify(tmp_path / "after", rows, filings)
    assert before.components["revenue"].value == "100"
    assert (after.components["revenue"].value, after.components["revenue"].accns) == ("120", ("a2",))
    assert after.fields["winning_accessions"] == "several"
    assert q.gpa(after) == Fraction(120 - 40, 200)


def test_alias_recency_beats_declared_order(tmp_path: Path) -> None:
    rows = _merge(_accounts(), {RCC: [_row(90, "a2", start="2019-01-01")]})
    result = _classify(tmp_path, rows, [("a1", T1, "10-K"), ("a2", T2, "10-K/A")])
    revenue = result.components["revenue"]
    assert (revenue.alias, revenue.value, revenue.aliases_disagree) == (RCC[1], "90", False)


def test_same_filing_alias_disagreement_takes_the_declared_order_and_is_flagged(tmp_path: Path) -> None:
    rows = _merge(_accounts(), {RCC: [_row(90, "a1", start="2019-01-01")]})
    result = _classify(tmp_path, rows, [("a1", T1, "10-K")])
    revenue = result.components["revenue"]
    assert (revenue.alias, revenue.value, revenue.aliases_disagree) == ("Revenues", "100", True)
    assert result.fields["revenue_aliases_disagree_at_latest"] == "true"
    assert result.rung == q.ELIGIBLE


def test_ambiguous_winning_alias_never_falls_back_to_the_next_alias(tmp_path: Path) -> None:
    rows = _merge(
        _accounts(),
        {REV: [_row(105, "a1", start="2019-01-01")], RCC: [_row(90, "a1", start="2019-01-01")]},
    )
    result = _classify(tmp_path, rows, [("a1", T1, "10-K")])
    assert result.components["revenue"].status == "ambiguous"
    assert result.components["revenue"].alias == "Revenues"
    assert result.rung == "gross_profit_unavailable"


def test_several_annual_starts_for_the_latest_end(tmp_path: Path) -> None:
    rows = _merge(_accounts(), {CGS: [_row(40, "a1", start="2018-12-30", end="2019-12-31")]})
    result = _classify(tmp_path, rows, [("a1", T1, "10-K")])
    assert result.rung == "several_annual_starts"


def test_rejection_only_later_period_blocks_instead_of_falling_back(tmp_path: Path) -> None:
    earlier = _accounts("a1", start="2018-07-01", end="2019-06-30")
    later = {REV: [_row("bad", "a2", start="2019-01-01")], COR: [_row("bad", "a2", start="2019-01-01")]}
    result = _classify(tmp_path, _merge(earlier, later), [("a1", T1, "10-K"), ("a2", T2, "10-K")])
    assert result.period == ("2019-01-01", "2019-12-31")
    assert result.components["revenue"].status == "blocked_by_rejection"
    assert result.rung == "gross_profit_unavailable"


@pytest.mark.parametrize("bad", [{"start": "2019-13-45"}, {"end": "not-a-date"}, {"end": None}])
def test_rejection_with_an_unparseable_period_key_is_skipped_not_fatal(tmp_path: Path, bad: dict[str, Any]) -> None:
    rejected = _row("bad", "a2", start="2019-01-01")
    rejected.update(bad)
    rows = _merge(_accounts(), {REV: [rejected]})
    result = _classify(tmp_path, rows, [("a1", T1, "10-K"), ("a2", T2, "10-K")])
    assert result.rung == q.ELIGIBLE


@pytest.mark.parametrize(
    ("start", "rung"),
    [
        ("2018-12-30", q.ELIGIBLE),  # 52 weeks + 1 day = 367 days
        ("2018-12-23", q.ELIGIBLE),  # 53-week year ending 2019-12-28: 371 days
        ("2019-01-17", "no_public_annual_period"),  # 349 days
    ],
)
def test_annual_duration_window(tmp_path: Path, start: str, rung: str) -> None:
    end = "2019-12-28" if start == "2018-12-23" else "2019-12-31"
    result = _classify(tmp_path, _accounts(start=start, end=end), [("a1", T1, "10-K")])
    assert result.rung == rung


def test_fiscal_year_change_takes_the_last_full_year_never_the_transition(tmp_path: Path) -> None:
    full = _accounts("a1", start="2018-07-01", end="2019-06-30")
    transition = _accounts("a2", start="2019-07-01", end="2019-12-31")  # 184 days, on a 10-KT
    result = _classify(tmp_path, _merge(full, transition), [("a1", T1, "10-K"), ("a2", T2, "10-KT")])
    assert result.period == ("2018-07-01", "2019-06-30")
    assert result.rung == q.ELIGIBLE


def test_late_filer_has_no_period_and_no_fallback_to_the_prior_year(tmp_path: Path) -> None:
    prior = _accounts("a0", start="2018-01-01", end="2018-12-31")
    late = _accounts("a1")
    filings = [("a0", "2019-02-11T21:00:00.000Z", "10-K"), ("a1", "2020-07-15T20:00:00.000Z", "10-K")]
    result = _classify(tmp_path, _merge(prior, late), filings)
    assert result.rung == "no_public_annual_period"


@pytest.mark.parametrize(
    ("second", "second_rows", "fpi"),
    [
        ("20-F/A", {}, False),  # an amendment is never the "latest original"
        ("20-F/A", {AST: [_row(210, "a2")]}, True),  # ...unless it supplies a winning component
        ("20-F", {}, True),  # the latest original annual report is a 20-F
        # an AMBIGUOUS winning component still names its accessions: FPI precedes the GP rung
        ("20-F/A", {REV: [_row(105, "a2", start="2019-01-01"), _row(106, "a2", start="2019-01-01")]}, True),
    ],
)
def test_foreign_private_issuer(tmp_path: Path, second: str, second_rows: Rows, fpi: bool) -> None:
    filings = [("a1", T1, "10-K"), ("a2", T2, second)]
    result = _classify(tmp_path, _merge(_accounts(), second_rows), filings)
    assert result.fields["fpi"] == str(fpi).lower()
    assert result.rung == ("foreign_private_issuer" if fpi else q.ELIGIBLE)


@pytest.mark.parametrize(
    ("overrides", "rung", "signs"),
    [
        ({"equity": 0}, q.ELIGIBLE, "ok"),  # the quoted rule excludes NEGATIVE book equity only
        ({"equity": -1}, "equity_negative", "equity"),
        ({"cogs": -5}, "component_sign_invalid", "cogs"),
        ({"rev": -5}, "component_sign_invalid", "revenue"),
        ({"assets": 0}, "assets_nonpositive", "assets"),
    ],
)
def test_sign_screens(tmp_path: Path, overrides: dict[str, Any], rung: str, signs: str) -> None:
    result = _classify(tmp_path, _accounts(**overrides), [("a1", T1, "10-K")])
    assert (result.rung, result.fields["signs"]) == (rung, signs)


def test_missing_cogs_is_gross_profit_unavailable_even_with_a_gross_profit_tag(tmp_path: Path) -> None:
    rows = _accounts()
    del rows[COR]
    rows[GP] = [_row(60, "a1", start="2019-01-01")]
    result = _classify(tmp_path, rows, [("a1", T1, "10-K")])
    assert (result.rung, result.fields["gp_tag"]) == ("gross_profit_unavailable", "value")


@pytest.mark.parametrize(
    ("raw", "parsed"),
    [
        ("3571", 3571),
        (3571, 3571),
        ("6021", 6021),
        ("99", None),  # below the SIC range
        ("12345", None),
        ("", None),
        ("35.7", None),
        ("３５７１", None),  # full-width digits: isdigit() but not ASCII
        (True, None),
        (None, None),
        (3571.0, None),
    ],
)
def test_parse_sic(raw: object, parsed: int | None) -> None:
    assert q.parse_sic(raw) == parsed


@pytest.mark.parametrize(("sic", "rung"), [(None, "sic_missing"), (6021, "sic_financial"), (5999, q.ELIGIBLE)])
def test_sic_rungs_still_record_every_field(tmp_path: Path, sic: int | None, rung: str) -> None:
    result = _classify(tmp_path, _accounts(), [("a1", T1, "10-K")], sic=sic)
    assert result.rung == rung
    assert result.fields["revenue"] == "value"  # counted independently of the ladder


def test_gated_prefix_read_aborts_instead_of_reading_as_no_period(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, _accounts(), [("a1", T1, "10-K")])
    with pytest.raises(q.QualityUniverseError, match="after_capture"):
        q.classify(bundle, CIK, MANUFACTURER, date(2031, 6, 30))


def test_bundle_screens_come_first(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, _accounts(), [("a1", T1, "10-K")])
    assert q.classify(bundle, "0000000002", MANUFACTURER, D).rung == "no_companyfacts_entry"


def test_gpa_refuses_a_non_eligible_issuer(tmp_path: Path) -> None:
    result = _classify(tmp_path, _accounts(equity=-1), [("a1", T1, "10-K")])
    with pytest.raises(q.QualityUniverseError):
        q.gpa(result)


# --------------------------------------------------------------------------- rule 2


def _link(reason: sl.Reason, grammar: str | None = "plain", *, cik: str | None = CIK, q_alias: bool = False) -> Any:
    return sl.LinkResult(reason, grammar, cik=cik, basis="single_cik" if cik else None, q_alias=q_alias)


@pytest.mark.parametrize(
    ("result", "candidate"),
    [
        (_link(sl.Reason.LINKED), True),
        (_link(sl.Reason.LINKED, "class:A"), True),
        (_link(sl.Reason.LINKED, q_alias=True), True),  # bankrupt ...Q names stay in
        (_link(sl.Reason.LINKED, "non_common:WS"), False),
        (_link(sl.Reason.VENDOR_SYMBOL_COLLISION, cik=None), False),
        (_link(sl.Reason.CONFLICTING_EVIDENCE, cik=None), False),
        (_link(sl.Reason.NO_RECENT_EVIDENCE, cik=None), False),
    ],
)
def test_is_candidate(result: sl.LinkResult, candidate: bool) -> None:
    assert q.is_candidate(result) is candidate


def test_choose_series_by_liquidity_then_lowest_id_before_executability() -> None:
    assert q.choose_series({7: Decimal(5), 3: Decimal(9), 1: None}) == 3
    assert q.choose_series({7: Decimal(9), 3: Decimal(9)}) == 3
    assert q.choose_series({7: None, 3: None}) == 3
    # The winner is chosen on liquidity alone; a non-executable winner is NOT swapped for
    # the executable runner-up (the caller records the CIK as not_executable).
    liquidity = {1: Decimal(100), 2: Decimal(1)}
    x_rows = {1: (Decimal(10), Decimal(10), Decimal(10), 0), 2: (Decimal(5), Decimal(5), Decimal(5), 100)}
    winner = q.choose_series(liquidity)
    assert winner == 1 and not q.is_executable(*x_rows[winner])
    with pytest.raises(q.QualityUniverseError):
        q.choose_series({})


@pytest.mark.parametrize(
    ("row", "executable"),
    [
        ((Decimal(10), Decimal(11), Decimal(5), 1), True),
        ((Decimal(10), Decimal(11), Decimal(5), 0), False),  # zero-volume X(D) row
        ((Decimal(10), Decimal(11), Decimal(5), None), False),
        ((None, Decimal(11), Decimal(5), 1), False),
        ((Decimal(0), Decimal(11), Decimal(5), 1), False),
        ((Decimal("NaN"), Decimal(11), Decimal(5), 1), False),
        ((Decimal(10), Decimal("Infinity"), Decimal(5), 1), False),
        ((Decimal(10), Decimal(11), Decimal(-5), 1), False),
    ],
)
def test_is_executable(row: tuple[Any, Any, Any, Any], executable: bool) -> None:
    assert q.is_executable(*row) is executable


# --------------------------------------------------------------------------- rule 7


def _signal(values: list[int]) -> dict[str, Fraction]:
    return {f"{i:010d}": Fraction(v, 7) for i, v in enumerate(values)}


def test_top_decile_is_exactly_n_over_10_without_ties() -> None:
    result = q.top_decile(_signal(list(range(105))))
    assert len(result.arm) == 10 and result.boundary_ties == 0
    assert result.arm == {f"{i:010d}" for i in range(95, 105)}


def test_boundary_ties_join_the_arm_whole() -> None:
    values = list(range(100))
    values[87:93] = [90] * 6  # ranks 87-92 tie; decile 9 starts at rank 90
    signal = _signal(values)
    result = q.top_decile(signal)
    assert result.boundary_ties == 3
    assert len(result.arm) == 13
    assert {cik for cik, v in signal.items() if v == Fraction(90, 7)} <= result.arm
    # The tie-break by CIK orders ranks but never splits a boundary tie.
    assert sum(1 for d in result.decile.values() if d == q.TOP_DECILE) == 10


def test_fewer_than_100_eligible_refuses() -> None:
    with pytest.raises(q.QualityUniverseError, match="below 100"):
        q.top_decile(_signal(list(range(99))))


# --------------------------------------------------------------------------- builder + census pieces (PR A2)


def test_execution_date_skips_weekends_and_holidays() -> None:
    assert build.execution_date(date(2020, 6, 30)) == date(2020, 7, 1)
    assert build.execution_date(date(2020, 7, 2)) == date(2020, 7, 6)  # 2020-07-03 observed holiday
    assert build.execution_date(date(2024, 6, 28)) == date(2024, 7, 1)


def test_read_mirror_series_parses_and_refuses_unordered_dates(tmp_path: Path) -> None:
    good = tmp_path / "ABC.csv"
    good.write_text("2020-07-01,10,11,9,10.5,1000,1,0,5.25\n2020-07-02,10,11,9,nan,12.5,1,0,5\n")
    bars = build.read_mirror_series(good)
    assert bars[0] == build.MirrorBar(date(2020, 7, 1), Decimal(10), Decimal("10.5"), Decimal("5.25"), 1000)
    assert (bars[1].raw_close, bars[1].volume) == (None, None)  # non-finite close, fractional volume
    assert not build.x_row_executable(bars, date(2020, 7, 2))
    assert build.x_row_executable(bars, date(2020, 7, 1))
    assert not build.x_row_executable(bars, date(2020, 7, 3))  # no row at all
    bad = tmp_path / "XYZ.csv"
    bad.write_text("2020-07-02,1,1,1,1,1,1,0,1\n2020-07-02,1,1,1,1,1,1,0,1\n")
    with pytest.raises(build.QualityInputError, match="strictly increasing"):
        build.read_mirror_series(bad)


def test_form25_horizon_is_strictly_after_d_and_on_the_linked_cik() -> None:
    def row(accession: str, filed: str, cik: str = CIK, symbol: str | None = "ABC") -> dict[str, Any]:
        return {"accession": accession, "filed_date": filed, "issuer_cik": cik, "resolved_symbol": symbol}

    rows = [
        row("on-d", "2020-06-30"),
        row("in", "2020-07-01"),
        row("last", "2022-06-30"),  # D + 730
        row("after", "2022-07-01"),
        row("other-cik", "2021-01-01", cik="0000000002"),
        row("other-symbol", "2021-01-02", symbol="XYZ"),
    ]
    got = build.form25_in_horizon(rows, CIK, frozenset({"ABC"}), D, {"in": "(b)"})
    assert [(r["accession"], r["match"], r["rule_provision"]) for r in got] == [
        ("in", "symbol_match", "(b)"),
        ("last", "symbol_match", None),
        ("other-symbol", "symbol_other", None),
    ]


@pytest.mark.parametrize(
    ("decision", "last_bar", "alive", "outcome"),
    [
        (date(2013, 6, 28), date(2014, 1, 2), False, "exchange_failure"),  # ended inside the horizon
        (date(2013, 6, 28), date(2020, 1, 2), False, "alive_730"),
        (date(2013, 6, 28), date(2024, 9, 27), True, "alive_730"),
        (date(2023, 6, 30), date(2024, 9, 27), True, "censored_alive"),  # horizon passes the capture
    ],
)
def test_price_outcome(decision: date, last_bar: date, alive: bool, outcome: str) -> None:
    assert census.price_outcome(decision, last_bar, alive, "exchange_failure") == outcome


def test_form25_outcome_channels() -> None:
    span = ["2010-01-04", "2024-09-20"]
    hit = [{"match": "symbol_other", "filed_date": "2014-01-01", "accession": "x", "rule_provision": "(a)(3)"}]
    assert census.form25_outcome(date(2013, 6, 28), hit, span) == "no_form25_observed"
    hit.append({"match": "symbol_match", "filed_date": "2014-02-01", "accession": "y", "rule_provision": "(b)"})
    assert census.form25_outcome(date(2013, 6, 28), hit, span) == "form25:(b)"
    assert census.form25_outcome(date(2023, 6, 30), [], span) == "unobserved_horizon"
    assert census.form25_outcome(date(2013, 6, 28), [], None) == "unobserved_horizon"


def test_size_deciles_keep_unavailable_separate_and_need_ten_values() -> None:
    values: dict[str, Decimal | None] = {f"{i:010d}": Decimal(i) for i in range(20)}
    values["x"] = None
    got = census.deciles(values)
    assert got["x"] == census.UNAVAILABLE
    assert got["0000000000"] == "0" and got["0000000019"] == "9"
    assert set(census.deciles({"a": Decimal(1), "b": None}).values()) == {census.NO_DECILES, census.UNAVAILABLE}


def test_sic_division() -> None:
    assert [census.sic_division(s) for s in (None, 100, 3571, 6021, 7372, 9999, 1900)] == [
        "missing",
        "A_agriculture",
        "D_manufacturing",
        "H_finance",
        "I_services",
        "K_nonclassifiable",
        "unassigned",
    ]


def test_loader_refuses_a_moved_document_and_a_foreign_policy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "artefact"
    document = {"schema": build.FORMATION_SCHEMA, "formation": "2013-06-28", "counts": {}, "candidates": [], "rows": []}
    build._write_exclusive(root / "formations" / "2013-06-28.json", document)
    digest = hashlib.sha256((root / "formations" / "2013-06-28.json").read_bytes()).hexdigest()
    manifest = {
        "schema": build.MANIFEST_SCHEMA,
        "policy": build.policy_sha256(),
        "formations": [{"formation": "2013-06-28", "path": "formations/2013-06-28.json", "sha256": digest}],
    }
    build._write_exclusive(root / build.MANIFEST_FILENAME, manifest)
    sha = hashlib.sha256((root / build.MANIFEST_FILENAME).read_bytes()).hexdigest()
    assert build.load_quality_input(root, expected_manifest_sha256=sha)[1] == [document]
    with pytest.raises(build.QualityInputError, match="pinned"):
        build.load_quality_input(root, expected_manifest_sha256="0" * 64)
    monkeypatch.setattr(build, "policy_sha256", lambda: "other")
    with pytest.raises(build.QualityInputError, match="policy"):
        build.load_quality_input(root, expected_manifest_sha256=sha)


def test_census_counts_only_true_alias_disagreements() -> None:
    def row(cik: str, fields: dict[str, str]) -> dict[str, Any]:
        return {
            "cik": cik,
            "liquidity": None,
            "components": {},
            "fields": fields,
            "rung": "no_public_annual_period",
            "last_bar": "2020-01-02",
            "alive_at_capture": False,
            "termination_class": "unknown_termination",
            "form25_horizon": [],
        }

    document = {
        "formation": "2013-06-28",
        "counts": {},
        "rows": [
            row("1", {"revenue_aliases_disagree_at_latest": "true"}),
            row("2", {"revenue_aliases_disagree_at_latest": "false"}),
            row("3", {"cogs": "value"}),
        ],
    }
    assert census.formation_census(document, None)["alias_disagreement"] == {"revenue": 1}
    # every status field reconciles to P(D), early ladder exits counted as not_evaluated
    counted = census.formation_census(document, None)["fields"]
    for key in census.STATUS_FIELDS:
        assert sum(n for k, n in counted.items() if k.startswith(f"{key}=")) == 3, key
    assert counted["signs=not_evaluated"] == 3 and counted["cogs=value"] == 1


def test_census_module_is_policy_bound() -> None:
    assert "scripts/census_2901_quality.py" in build.POLICY_FILES


def test_mirror_bounds_must_equal_the_stored_series(tmp_path: Path) -> None:
    good = tmp_path / "abc.csv"
    good.write_text("2020-07-01,10,11,9,10,100,0,0,10\n2020-07-02,10,11,9,10,100,0,0,10\n")
    row = {"vendor_symbol": "ABC", "first_bar": "2020-07-01", "last_bar": "2020-07-03", "symbol_unique": True}
    document = {"vendor_symbol": "ABC", "first_bar": "2020-07-01", "last_bar": "2020-07-03", "form25": None}
    (tmp_path / "s1.json").write_text(json.dumps(document))
    with pytest.raises(build.QualityInputError, match="mirror bar bounds differ"):
        build._series_inputs(
            1,
            {1: {"path": "s1.json", "sha256": build.read_verified_document(tmp_path / "s1.json")[0]}},
            tmp_path,
            {1: row},
            {"ABC": [good]},
            [],
        )


@pytest.mark.parametrize("operands", [(None, "1", "2"), ("3", None, "2"), ("3", "1", None)])
def test_gpa_of_refuses_a_missing_operand_with_a_named_error(operands: tuple[str | None, ...]) -> None:
    # the builder calls gpa_of directly on artefact rows; the guard must not live only in gpa()
    with pytest.raises(q.QualityUniverseError, match="without all GP/A operands"):
        q.gpa_of(*operands)
