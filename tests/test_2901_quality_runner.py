"""#2901 runner stage 1: the artefact's books and the census-time assertions (no price, no return).

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Portfolios", "Schedule assertions").
Documents are synthetic: 100 eligible issuers with GP/A (i + 1)/100 plus named edge rows.
"""

from __future__ import annotations

import copy
from datetime import date, datetime
from fractions import Fraction
from typing import Any

import pytest

from app.services import r6_quality_universe as quality
from app.services.r6_monthly_trial import Portfolio
from scripts.run_2901_quality_trial import (
    BUY_HOLD,
    ArtefactRefusal,
    annual_schedule,
    books_from_artefact,
    complete_case_census,
    exclusion_reasons,
    formation_books,
    schedules,
)

YEARS = (2013, 2014)
X_DATES = {2013: "2013-07-01", 2014: "2014-07-01"}
FORMATIONS = {2013: "2013-06-28", 2014: "2014-06-30"}


def _row(
    cik: str,
    *,
    rung: str = quality.ELIGIBLE,
    executable: bool = True,
    sic: int | None = 3571,
    fpi: str = "false",
    period: str = "found",
    assets: str | None = "100",
    equity: str | None = "50",
    x_date: str = X_DATES[2013],
) -> dict[str, Any]:
    fields: dict[str, Any] = {"period": period, "fpi": fpi}
    if rung not in {"no_companyfacts_entry", "fundamentals_integrity_excluded"}:
        fields["sic"] = "missing" if sic is None else "financial" if 6000 <= sic <= 6999 else "other"
    else:
        sic = None
    components: dict[str, Any] = {}
    for name, value in (("assets", assets), ("equity", equity)):
        if value is not None:
            components[name] = {"status": "value", "value": value}
    return {
        "cik": cik,
        "vendor_symbol": f"S{cik}",
        "x_date": x_date,
        "executable": executable,
        "rung": rung,
        "sic": sic,
        "fields": fields,
        "components": components,
        "gpa": None,
        "decile": None,
        "in_arm": False,
    }


def _document(year: int, extra: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    x_date = X_DATES[year]
    eligible = [_row(f"{index:010d}", x_date=x_date) for index in range(100)]
    signal = {row["cik"]: Fraction(index + 1, 100) for index, row in enumerate(eligible)}
    deciles = quality.top_decile(signal)
    for row in eligible:
        value = signal[row["cik"]]
        row["gpa"] = [str(value.numerator), str(value.denominator)]
        row["decile"] = deciles.decile[row["cik"]]
        row["in_arm"] = row["cik"] in deciles.arm
    others = [{**row, "x_date": x_date} for row in (extra or [])]
    return {"formation": FORMATIONS[year], "x_date": x_date, "rows": eligible + others}


#: Rows outside E(D). The first four are admitted to C′; the rest carry the named exclusion reasons.
EDGE = [
    _row("9000000001", rung="no_public_annual_period", period="no_public_annual_period", assets=None, equity=None),
    _row("9000000002", rung="several_annual_starts", period="several_annual_starts", fpi="not_evaluated"),
    _row("9000000003", rung="assets_unavailable", assets=None),
    _row("9000000004", rung="gross_profit_unavailable", equity="0"),  # zero equity is kept
    _row("9000000005", rung="no_companyfacts_entry"),
    _row("9000000006", rung="fundamentals_integrity_excluded"),
    _row("9000000007", rung="sic_missing", sic=None),
    _row("9000000008", rung="sic_financial", sic=6021, executable=False),
    _row("9000000009", rung="foreign_private_issuer", fpi="true"),
    _row("9000000010", rung="assets_nonpositive", assets="0"),
    _row("9000000011", rung="equity_negative", equity="-1.5"),
    _row("9000000012", rung="not_executable", executable=False),
]


def test_books_follow_the_spec_definitions() -> None:
    books = formation_books(_document(2013, EDGE))
    eligible = {f"S{index:010d}" for index in range(100)}
    assert books.control == eligible
    assert books.arm == {f"S{index:010d}" for index in range(90, 100)}
    assert books.gate_control == {f"S{index:010d}" for index in range(10)}  # r < n/10
    assert books.complete_case == eligible | {f"S900000000{i}" for i in range(1, 5)}
    assert books.gate_boundary_ties == 0
    assert (books.formation, books.x_date) == (date(2013, 6, 28), date(2013, 7, 1))


def test_exclusion_reasons_are_ordered_and_overlap() -> None:
    reasons = {row["cik"]: exclusion_reasons(row) for row in EDGE}
    assert reasons["9000000001"] == ()  # an issuer missing a period is admitted
    assert reasons["9000000008"] == ("sic_financial", "not_executable")
    assert reasons["9000000005"] == ("no_companyfacts_entry",)
    assert reasons["9000000011"] == ("equity_negative",)
    census = complete_case_census(_document(2013, EDGE))
    assert (census["population"], census["complete_case"]) == (112, 104)
    assert census["excluded"]["sic_financial"] == {
        "exclusive": 1,
        "exclusive_share": 1 / 112,
        "overlapping": 1,
        "overlapping_share": 1 / 112,
    }
    assert census["excluded"]["not_executable"]["exclusive"] == 1
    assert census["excluded"]["not_executable"]["overlapping"] == 2
    assert sum(cell["count"] for cell in census["cells"]) == 104
    assert sum(cell["target_weight"] for cell in census["cells"]) == pytest.approx(1.0)
    no_period = next(cell for cell in census["cells"] if cell["period"] == "no_public_annual_period")
    assert (no_period["assets"], no_period["equity"], no_period["count"]) == ("not_evaluated", "not_evaluated", 1)


def test_ties_at_the_decile_zero_boundary_are_counted() -> None:
    document = _document(2013)
    rows = {row["cik"]: row for row in document["rows"]}
    # Give rank 10 (decile 1) the same GP/A as rank 9 (D₀'s highest): rule 7 splits it by CIK.
    rows["0000000010"]["gpa"] = list(rows["0000000009"]["gpa"])
    assert formation_books(document).gate_boundary_ties == 1


def _mutated(path: tuple[Any, ...], value: Any, cik: str = "0000000050") -> dict[str, Any]:
    document = copy.deepcopy(_document(2013))
    row = next(row for row in document["rows"] if row["cik"] == cik)
    target = row
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return document


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        (("executable",), "yes", "executable"),
        (("fields", "fpi"), "maybe", "fpi"),
        (("sic",), 99999, "SIC range"),
        (("sic",), True, "SIC range"),
        (("fields", "sic"), "financial", "disagrees"),
        (("fields", "sic"), "unknown", "fields.sic"),
        (("components", "assets", "status"), "stale", "status"),
        (("components", "assets", "value"), "100.0", "canonical"),
        (("components", "assets", "value"), "NaN", "canonical"),
        (("components", "assets", "value"), 100, "canonical"),
        (("components", "assets", "status"), "absent", "exactly when"),
        (("decile",), 3, "rule 7"),
        (("in_arm",), True, "rule 7"),
        (("x_date",), "2013-07-02", "x_date"),
    ],
)
def test_a_schema_violation_refuses(path: tuple[Any, ...], value: Any, match: str) -> None:
    with pytest.raises(ArtefactRefusal, match=match):
        formation_books(_mutated(path, value))


def test_a_missing_recorded_sic_refuses_outside_the_no_sic_rungs() -> None:
    document = _document(2013)
    del document["rows"][0]["fields"]["sic"]
    with pytest.raises(ArtefactRefusal, match="no-SIC rungs"):
        formation_books(document)


def test_duplicate_symbols_in_a_book_refuse() -> None:
    document = _document(2013)
    document["rows"][1]["vendor_symbol"] = document["rows"][0]["vendor_symbol"]
    with pytest.raises(ArtefactRefusal, match="duplicate vendor symbols"):
        formation_books(document)


def test_fewer_than_the_minimum_eligible_refuses() -> None:
    document = _document(2013)
    document["rows"] = document["rows"][:99]
    with pytest.raises(ArtefactRefusal, match="rule 7 refuses"):
        formation_books(document)


def test_the_schedule_assertions() -> None:
    documents = [_document(2013), _document(2014)]
    books = books_from_artefact(documents, years=YEARS, window_end=date(2014, 9, 26))
    assert [book.x_date for book in books] == [date(2013, 7, 1), date(2014, 7, 1)]
    with pytest.raises(ArtefactRefusal, match="one per year"):
        books_from_artefact(documents[::-1], years=YEARS, window_end=date(2014, 9, 26))
    with pytest.raises(ArtefactRefusal, match="one per year"):
        books_from_artefact(documents[:1], years=YEARS, window_end=date(2014, 9, 26))
    with pytest.raises(ArtefactRefusal, match="outside"):
        books_from_artefact(documents, years=YEARS, window_end=date(2014, 7, 1))
    late = copy.deepcopy(documents)
    late[1]["x_date"] = "2014-07-02"
    for row in late[1]["rows"]:
        row["x_date"] = "2014-07-02"
    with pytest.raises(ArtefactRefusal, match="first NYSE session of July"):
        books_from_artefact(late, years=YEARS, window_end=date(2014, 9, 26))


def test_schedules_and_the_annual_mirror() -> None:
    books = books_from_artefact([_document(2013), _document(2014)], years=YEARS, window_end=date(2014, 9, 26))
    out = schedules(books)
    assert set(out) == {Portfolio.ARM, Portfolio.CONTROL, Portfolio.COMPLETE_CASE, Portfolio.GATE_CONTROL, BUY_HOLD}
    assert out[BUY_HOLD] == ((date(2013, 7, 1), books[0].control),)
    assert [day for day, _ in out[Portfolio.ARM]] == [date(2013, 7, 1), date(2014, 7, 1)]
    annual = annual_schedule(out[Portfolio.CONTROL], books)
    assert [formation for formation, _ in annual] == [datetime(2013, 6, 28), datetime(2014, 6, 30)]
    assert [targets for _, targets in annual] == [targets for _, targets in out[Portfolio.CONTROL]]
