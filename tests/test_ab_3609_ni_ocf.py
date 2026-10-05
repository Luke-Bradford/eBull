"""#3609 slice 3d-iv part 2: the ni*/ocf* A/B must refuse every difference it exists to refuse."""

from __future__ import annotations

import copy
from typing import Any

from scripts import ab_3609_ni_ocf as ab


def _c(value: float | None, missing: str | None = None) -> dict[str, Any]:
    return {"value": value, "missing": missing, "period_end": "2016-12-31", "kind": "annual", "facts": []}


def _row(**chars: dict[str, Any]) -> dict[str, Any]:
    base = {name: _c(0.1) for name in ("at_gr1", "be_me", "gp_at", "ni_me", "ocf_me", "ope_be")}
    return {"M": "2017-04-30", "cik": "0000000001", "symbol": "AAA", "exclusion": None, "characteristics": base | chars}


def _oracle(ni: dict[str, Any], ocf: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {("2017-04-30", "0000000001", "AAA"): {"ni_adopted": ni, "ocf_adopted": ocf}}


def test_a_change_matching_the_oracle_passes_and_is_counted() -> None:
    a = _row(ni_me=_c(None, "no_period"))
    b = _row(ni_me=_c(0.3))
    failures, summary = ab.compare([a], [b], _oracle(_c(0.3), _c(0.1)))
    assert failures == []
    assert summary["transitions"] == {"ni_me: no_period -> value": 1}


def test_a_change_the_oracle_does_not_predict_refuses() -> None:
    failures, _ = ab.compare([_row()], [_row(ocf_me=_c(0.2))], _oracle(_c(0.1), _c(0.1)))
    assert failures == [
        "('2017-04-30', '0000000001', 'AAA') ocf_me: (0.2, None, '2016-12-31', 'annual') != oracle "
        "(0.1, None, '2016-12-31', 'annual')"
    ]


def test_any_other_change_refuses() -> None:
    oracle = _oracle(_c(0.1), _c(0.1))
    assert ab.compare([_row()], [_row(be_me=_c(0.2))], oracle)[0] == [
        "('2017-04-30', '0000000001', 'AAA'): a characteristic other than ni_me/ocf_me differs"
    ]
    moved = copy.deepcopy(_row())
    moved["sic"] = 1311
    assert ab.compare([_row()], [moved], oracle)[0] == [
        "('2017-04-30', '0000000001', 'AAA'): fields outside the characteristics differ"
    ]
    assert ab.compare([_row()], [], oracle)[0] == ["row counts differ", "1 oracle rows have no admitted row"]


def test_an_admitted_row_without_an_oracle_row_refuses() -> None:
    failures, _ = ab.compare([_row()], [_row()], {})
    assert failures == ["('2017-04-30', '0000000001', 'AAA'): admitted row has no oracle row"]


def test_an_adjudicated_departure_passes_and_a_stale_one_refuses() -> None:
    listed = {("2017-04-30", "0000000001", "AAA", "ni_me"): "a witness the measurement could not see"}
    departed = ab.compare([_row()], [_row(ni_me=_c(None, "no_period"))], _oracle(_c(0.1), _c(0.1)), listed)
    assert departed[0] == [] and departed[1]["counts"]["adjudicated"] == 1
    stale = ab.compare([_row()], [_row()], _oracle(_c(0.1), _c(0.1)), listed)
    assert stale[0] == ["('2017-04-30', '0000000001', 'AAA', 'ni_me'): adjudicated but equal to the oracle"]
