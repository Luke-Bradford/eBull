"""#3609 slice 3d-iv census clauses: the veto A/B must refuse every difference it exists to refuse."""

from __future__ import annotations

from typing import Any

from scripts import ab_3609_vetoes as ab

KEY = "('2017-04-30', '0000000001', 'AAA')"
VETO = "veto_do:2016-01-01:2016-12-31:DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax"


def _c(value: float | None, missing: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"value": value, "missing": missing, "period_end": "2016-12-31", "kind": "annual", "branches": []} | extra


def _row(**chars: dict[str, Any]) -> dict[str, Any]:
    base = {name: _c(0.1) for name in ("at_gr1", "be_me", "gp_at", "ni_me", "ocf_me", "ope_be")}
    return {"M": "2017-04-30", "cik": "0000000001", "symbol": "AAA", "exclusion": None, "characteristics": base | chars}


def _b(**chars: dict[str, Any]) -> dict[str, Any]:
    row = _row(**chars)
    for c in row["characteristics"].values():
        c.setdefault("vetoes", [])
    return row


ORACLE = {("2017-04-30", "0000000001", "AAA"): {"ni_adopted": _c(0.1), "ocf_adopted": _c(0.1)}}


def test_veto_labels_alone_pass_and_are_counted() -> None:
    a = _row(ni_me=_c(None, "blocked_by_rejection", branches=["ni_minus_xido"]))
    b = _b(ni_me=_c(None, "blocked_by_rejection", branches=["ni_minus_xido", VETO], vetoes=[VETO, VETO]))
    oracle = {
        ("2017-04-30", "0000000001", "AAA"): {"ni_adopted": _c(None, "blocked_by_rejection"), "ocf_adopted": _c(0.1)}
    }
    failures, summary = ab.compare([a], [b], oracle, {"do": 2})
    assert failures == []
    assert summary["counts"]["veto evaluations: do"] == 2


def test_any_other_change_refuses() -> None:
    assert ab.compare([_row()], [_b(be_me=_c(0.2))], ORACLE, {})[0] == [f"{KEY}: differs beyond the veto labels"]
    assert ab.compare([_row()], [], ORACLE, {})[0] == ["row counts differ", "1 oracle rows have no admitted row"]


def test_a_veto_outside_the_fallback_characteristics_refuses() -> None:
    failures, _ = ab.compare([_row()], [_b(be_me=_c(0.1, vetoes=[VETO]))], ORACLE, {"do": 1})
    assert failures == [f"{KEY} be_me: a veto outside ni_me/ocf_me"]


def test_a_reading_off_the_oracle_refuses_with_no_adjudication() -> None:
    failures, _ = ab.compare([_row(ocf_me=_c(0.2))], [_b(ocf_me=_c(0.2))], ORACLE, {})
    assert failures == [
        f"{KEY} ocf_me: (0.2, None, '2016-12-31', 'annual') != oracle (0.1, None, '2016-12-31', 'annual')"
    ]


def test_a_veto_count_other_than_the_measurements_refuses() -> None:
    b = _b(ni_me=_c(0.1, vetoes=[VETO]))
    assert ab.compare([_row()], [b], ORACLE, {"do": 2})[0] == ["veto evaluations for do: 1 != oracle 2"]
    assert ab.compare([_row()], [_b()], ORACLE, {"ocf_disc": 1})[0] == ["veto evaluations for ocf_disc: 0 != oracle 1"]


def test_the_oracle_veto_counts_are_read_per_companion() -> None:
    summary = {
        "period_evaluations": {
            "ni_adopted: zero refused by a witness (do)": 3,
            "ni_zero: periods evaluated": 9,
            "ocf_adopted: zero refused by a witness (ocf_disc)": 4,
        }
    }
    assert ab.refused_by_witness(summary) == {"do": 3, "ocf_disc": 4}
