"""#3740 (#3609 step 3): the historical path's formation assembly (``scripts.report_3740_step3``).

Fixtures only: nothing here reads a published artefact or the database.
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from typing import Any

import pytest

from app.services.avoidance_filters import MaxMissing, MaxReading
from app.services.factor_book import UNIVERSE_SIZE
from app.services.factor_book_path import HoldingReturn
from app.services.factor_panel_prices import HoldingStatus
from scripts.build_3609_factor_panel import VerifiedArtefact
from scripts.report_3609_step2 import TABLE9_SIGNS, PanelMonth, PanelName, ReportError
from scripts.report_3740_step3 import EXTRA_MEMBERS, check_members, formation_inputs, read_members, score

M = date(2019, 1, 31)
ALL = ("value", "GP/A", "investment")
ROWS = "rows.jsonl.gz"
#: Table 9's own signs for the extra members (gp_at +1, at_gr1 -1).
SIGNS_CSV = b"name,sign\ngp_at,1\nat_gr1,-1\nbe_me,1\n"


def _verified(rows: list[dict[str, Any]], signs: bytes = SIGNS_CSV) -> VerifiedArtefact:
    payload = gzip.compress(b"".join(json.dumps(r).encode() + b"\n" for r in rows))
    return VerifiedArtefact(manifest={"rows": {"path": ROWS}}, files={ROWS: payload, TABLE9_SIGNS: signs})


def _row(name: int, *, admitted: bool = True, **values: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "M": M.isoformat(),
        "name_key": name,
        "exclusion": None if admitted else "no_recent_evidence",
    }
    if admitted:
        row["characteristics"] = {c: {"value": values.get(c, 0.5)} for c in EXTRA_MEMBERS}
    return row


# --------------------------------------------------------------------------- the member pass


def test_members_are_signed_by_table9_and_none_is_no_input() -> None:
    got = read_members(_verified([_row(1, gp_at=0.2, at_gr1=0.3), _row(2, at_gr1=None), _row(3, admitted=False)]))
    assert got == {M: {1: {"gp_at": 0.2, "at_gr1": -0.3}, 2: {"gp_at": 0.5}}}


@pytest.mark.parametrize(
    ("rows", "signs", "match"),
    [
        ([_row(1), _row(1)], SIGNS_CSV, "repeated"),
        ([_row(1, gp_at="0.2")], SIGNS_CSV, "not a number"),
        ([_row(1, gp_at=True)], SIGNS_CSV, "not a number"),
        ([_row(1)], b"name,sign\ngp_at,1\n", "no sign"),
        ([_row(1) | {"characteristics": {"gp_at": {"value": 0.1}}}], SIGNS_CSV, "no at_gr1 characteristic"),
    ],
)
def test_member_pass_refuses(rows: list[dict[str, Any]], signs: bytes, match: str) -> None:
    with pytest.raises(ReportError, match=match):
        read_members(_verified(rows, signs))


# --------------------------------------------------------------------------- fixtures for scoring


def _name(name: int, *, industry: str = "Manuf", value: float | None = None, holding: float = 0.01) -> PanelName:
    signed = {} if value is None else {"be_me": value, "ni_me": value, "ocf_me": value}
    returns = HoldingReturn(HoldingStatus.OBSERVED, {"best_case": holding, "worst_case": holding})
    return PanelName(
        series_id=10_000 + name, me=1e9 + name, industry=industry, signed=signed, holding=returns, sic=None
    )


def _month(n: int = UNIVERSE_SIZE + 5, *, no_value: frozenset[int] = frozenset()) -> PanelMonth:
    admitted = {
        i: _name(i, industry="Manuf" if i % 2 else "Shops", value=None if i in no_value else float(i % 37))
        for i in range(n)
    }
    return PanelMonth(
        formation=M,
        session=M,
        admitted=admitted,
        close={i: 10.0 + i % 3 for i in admitted},
        first_bar={i: date(2010, 1, 4) if i % 5 else date(2018, 6, 1) for i in admitted},
    )


def _members(month: PanelMonth, *, missing: frozenset[int] = frozenset()) -> dict[int, dict[str, float]]:
    return {i: {} if i in missing else {"gp_at": float(i % 11), "at_gr1": -float(i % 13)} for i in month.admitted}


def _readings(month: PanelMonth) -> dict[int, MaxReading]:
    out = {i: MaxReading(0.01 * (i % 50), 20, 0, None) for i in month.admitted}
    out[0] = MaxReading(None, 3, 0, MaxMissing.SHORT)
    out[1] = MaxReading(None, 20, 0, MaxMissing.SCREENED)
    return out


# --------------------------------------------------------------------------- score and formation inputs


def test_extra_members_feed_the_composite() -> None:
    # The top 1,000 by ME are names 5..1004; the largest 100 have no value-family input.
    no_value = frozenset(range(905, UNIVERSE_SIZE + 5))
    month = _month(no_value=no_value)
    members = _members(month)
    value_only = score(month, members, ("value",))
    three = score(month, members, ALL)
    assert value_only.universe == three.universe
    assert len(three.universe) == UNIVERSE_SIZE
    # Value alone: those names have no composite. GP/A and investment: 2 of 3 families, so they do.
    assert not no_value & set(value_only.scores.composite)
    assert no_value <= set(three.scores.composite)
    assert "gp_at" not in value_only.scores.by_operation
    assert {"gp_at", "at_gr1", "GP/A", "investment"} <= set(three.scores.by_operation)


def test_score_refuses_a_universe_name_without_a_member_row() -> None:
    month = _month()
    members = _members(month)
    del members[UNIVERSE_SIZE]
    with pytest.raises(ReportError, match=f"no member row: \\[{UNIVERSE_SIZE}\\]"):
        score(month, members, ALL)


def test_one_of_three_families_is_no_composite() -> None:
    no_value = frozenset(range(905, UNIVERSE_SIZE + 5))
    month = _month(no_value=no_value)
    # No value input and no investment input: 1 of 3 families, short of the strict majority.
    members = {i: ({"gp_at": 1.0 + i % 7} if i in no_value else v) for i, v in _members(month).items()}
    assert not no_value & set(score(month, members, ALL).scores.composite)


def test_formation_inputs_follow_the_historical_path_rules() -> None:
    month = _month()
    scored = score(month, _members(month), ALL)
    formation = formation_inputs(month, scored, _readings(month))
    universe = set(scored.universe)
    assert formation.universe == universe
    assert set(formation.me) == universe and set(formation.group) == universe and set(formation.returns) == universe
    assert formation.me[7] == month.admitted[7].me and formation.group[7] == "Manuf"
    assert formation.tradable is None
    # Archive seasoning: names first admitted 2018-06-01 are under 36 months old at 2019-01-31.
    assert formation.aged == frozenset(i for i in month.admitted if i % 5)
    # The flag ranks every admitted name, not only the universe: a screened name flags, a short one does not.
    assert 1 in formation.max_flagged and 0 not in formation.max_flagged
    assert {i for i in month.admitted if i % 50 == 49} <= formation.max_flagged
    assert not formation.eligible & formation.max_flagged


def test_formation_inputs_refuse_partial_readings_and_unaged_scored_names() -> None:
    month = _month()
    scored = score(month, _members(month), ALL)
    readings = _readings(month)
    del readings[3]
    with pytest.raises(ReportError, match="MAX readings"):
        formation_inputs(month, scored, readings)
    named = scored.bands.order[0]
    unaged = PanelMonth(M, M, month.admitted, month.close, {i: d for i, d in month.first_bar.items() if i != named})
    with pytest.raises(ReportError, match="no first admitted bar"):
        formation_inputs(unaged, scored, _readings(month))


def test_member_pass_must_match_step2_admitted_names() -> None:
    month = _month(n=12)
    members = _members(month)
    check_members([month], {M: members})
    del members[4]
    with pytest.raises(ReportError, match="differing \\[4\\]"):
        check_members([month], {M: members})
    with pytest.raises(ReportError, match="did not read"):
        check_members([month], {M: _members(month), date(2019, 2, 28): {}})
