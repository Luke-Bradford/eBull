"""#3609 slice 3d: the share-check A/B's row classifier."""

from __future__ import annotations

from typing import Any

from scripts.ab_3609_share_checks import classify, csv_row


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


def test_a_recovered_row_may_still_be_removed_by_a_later_check() -> None:
    after = {
        **_row(value=None, missing="shares_turnover_implausible", shares="1", shares_scope="dqc_recovered:cover"),
        "exclusion": "shares_turnover_implausible",
    }
    del after["characteristics"]
    assert classify(_row(), after) == "removed_after_recovery:shares_turnover_implausible"


def test_a_restored_me_must_be_admitted_with_its_characteristics() -> None:
    # 3d-ii: a newer reference lifts the check-4 failure.
    before = {**_row(value=None, missing="shares_discontinuity", raw="100"), "exclusion": "shares_discontinuity"}
    del before["characteristics"]
    assert classify(before, _row()) == "restored:shares_discontinuity"
    assert classify(before, {**_row(), "exclusion": "shares_discontinuity"}) == "other:restored_but_not_admitted"
    earlier = {**before, "exclusion": "not_filer"}
    assert classify(earlier, {**_row(), "exclusion": "not_filer"}) == "restored:shares_discontinuity"
    assert classify(earlier, _row()) == "other:earlier_exclusion_changed"
    assert classify(before, _row(shares="100")) == "other:restored_me_provenance_changed"
    assert classify(before, _row(missing="shares_discontinuity")) == "other:restored_with_a_missing_reason"
    recovered = _row(shares="0.05", shares_scope="dqc_recovered:balance_sheet")
    assert classify(before, recovered) == "restored:shares_discontinuity"


def test_the_evidence_line_carries_the_count_used_and_its_cause() -> None:
    after = _row(
        value=None,
        missing="shares_basis_ambiguous",
        raw="1000",
        split_product="0.0018",
        checks={"turnover": "pass", "basis": "fail"},
        facts=[{"concept": "EntityCommonStockSharesOutstanding", "value": "42757664"}],
    )
    after["prices"] = {"dollar_volume": 0.0}
    line = csv_row("removed:shares_basis_ambiguous", after)
    assert (line["fact_used"], line["filed_value"], line["raw_me"]) == (
        "EntityCommonStockSharesOutstanding",
        "42757664",
        "1000",
    )
    assert (line["dollar_volume_over_raw_me"], line["cause"]) == ("0", "split stamp beyond [1/100,100]")
    assert line["checks"] == '{"basis": "fail", "turnover": "pass"}'
    assert csv_row("restored:shares_discontinuity", _row())["cause"].startswith("shares_discontinuity no longer")
    recovered = _row(checks={"scale": "recovered"})
    assert csv_row("restored:shares_scale_conflict", recovered)["cause"].startswith("DQC_0095 conflict now resolved")
    passing = _row(checks={"scale": "pass"})
    assert csv_row("restored:shares_scale_conflict", passing)["cause"].startswith("DQC_0095 conflict no longer found")


def test_a_lifted_reason_may_give_way_to_a_later_check() -> None:
    # 3d-iii: check 2 no longer fails, check 3 does; the row stays out under its new reason.
    before = {**_row(value=None, missing="shares_scale_conflict", raw="100"), "exclusion": "shares_scale_conflict"}
    del before["characteristics"]
    after = {**before, "me": {**before["me"], "missing": "shares_turnover_implausible"}}
    after["exclusion"] = "shares_turnover_implausible"
    verdict = "reason_changed:shares_scale_conflict->shares_turnover_implausible"
    assert classify(before, after) == verdict
    assert csv_row(verdict, after)["cause"].startswith("shares_scale_conflict no longer applies")
    assert (
        classify(before, {**after, "exclusion": "shares_scale_conflict"}) == "other:reason_changed_admitted_differently"
    )
    kept = {**after, "characteristics": {"be_me": {"value": 1.0}}}
    assert classify(before, kept) == "other:reason_changed_row_kept_characteristics"
    moved = {**after, "me": {**after["me"], "shares": "7"}}
    assert classify(before, moved) == "other:reason_changed_me_provenance_changed"
