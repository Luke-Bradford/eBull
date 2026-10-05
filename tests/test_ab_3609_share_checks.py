"""#3609 slice 3d: the share-check A/B's row classifier."""

from __future__ import annotations

from typing import Any

from scripts.ab_3609_share_checks import classify


def _row(**me: Any) -> dict[str, Any]:
    base = {"value": "100", "missing": None, "shares": "50", "shares_scope": "cover", "basis": "2017-02-10"}
    return {
        "M": "2017-04-30",
        "series_id": 7,
        "exclusion": None,
        "prices": {"adj_close": 2.0},
        "me": {**base, **me},
        "characteristics": {"be_me": {"value": 0.4}, "gp_at": {"value": 0.1}},
    }


def test_new_keys_alone_leave_a_row_unchanged() -> None:
    after = _row(raw=None, checks={"basis": "pass"}, verified=True)
    after["prices"] = {"adj_close": 2.0, "liquidity_screened": False}
    assert classify(_row(), after) == "unchanged"


def test_a_removed_me_must_move_admission_to_its_reason_and_nothing_else() -> None:
    after = {**_row(value=None, missing="shares_discontinuity", raw="100"), "exclusion": "shares_discontinuity"}
    del after["characteristics"]
    assert classify(_row(), after) == "removed:shares_discontinuity"
    assert classify(_row(), {**after, "characteristics": {}}) == "other:removed_row_kept_characteristics"
    after["prices"] = {"adj_close": 3.0}
    assert classify(_row(), after) == "other:fields_outside_me_changed"


def test_a_recovered_me_may_change_only_me_denominated_characteristics() -> None:
    after = _row(value="0.1", shares="0.05", shares_scope="dqc_recovered:balance_sheet")
    after["characteristics"] = {"be_me": {"value": 400.0}, "gp_at": {"value": 0.1}}
    assert classify(_row(), after) == "recovered"
    after["characteristics"]["gp_at"] = {"value": 0.2}
    assert classify(_row(), after) == "other:recovered_non_me_characteristic_changed"
