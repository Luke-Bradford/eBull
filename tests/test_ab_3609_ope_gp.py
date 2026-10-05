"""#3609 slice 3d-v part 2: the ope*/gp* A/B refuses every difference outside ``ope_be`` / ``gp_at``."""

from __future__ import annotations

from typing import Any

from scripts import ab_3609_ope_gp as ab


def _c(value: float | None, missing: str | None = None) -> dict[str, Any]:
    return {"value": value, "missing": missing, "period_end": "2016-12-31", "kind": "annual", "facts": []}


def _row(**chars: dict[str, Any]) -> dict[str, Any]:
    base = {name: _c(0.1) for name in ("at_gr1", "be_me", "gp_at", "ni_me", "ocf_me", "ope_be")}
    return {"M": "2017-04-30", "cik": "0000000001", "symbol": "AAA", "exclusion": None, "characteristics": base | chars}


def _oracle(ope: dict[str, Any], gp: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {("2017-04-30", "0000000001", "AAA"): {"ope:adopted": ope, "gp:r5": gp}}


def test_ope_and_gp_changes_matching_the_oracle_pass_and_a_new_guards_key_is_allowed() -> None:
    b = _row(ope_be=_c(0.3) | {"guards": [{"concept": "OperatingExpenses"}]}, gp_at=_c(None, "no_period"))
    failures, summary = ab.compare([_row()], [b], _oracle(_c(0.3), _c(None, "no_period")), changed=ab.CHANGED)
    assert failures == []
    assert summary["transitions"] == {"gp_at: value -> no_period": 1, "ope_be: value -> value": 1}


def test_an_unpredicted_change_and_a_change_to_ni_me_refuse() -> None:
    oracle = _oracle(_c(0.1), _c(0.1))
    assert ab.compare([_row()], [_row(ope_be=_c(0.2))], oracle, changed=ab.CHANGED)[0] == [
        "('2017-04-30', '0000000001', 'AAA') ope_be: (0.2, None, '2016-12-31', 'annual') != oracle "
        "(0.1, None, '2016-12-31', 'annual')"
    ]
    assert ab.compare([_row()], [_row(ni_me=_c(0.2))], oracle, changed=ab.CHANGED)[0] == [
        "('2017-04-30', '0000000001', 'AAA'): a characteristic other than ope_be/gp_at differs"
    ]
