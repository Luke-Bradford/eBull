"""#3385 slice 2a — pure contract of the hunt harness: canonical JSON, TrialSpec, hashes, timeline."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from datetime import date
from typing import Any

import pytest

from app.services import hunt_harness as hh
from app.services.hunt_harness import TrialSpec, UniverseIdentity

_SHA = "a" * 64


def universe_identity(**overrides: Any) -> UniverseIdentity:
    fields: dict[str, Any] = {
        "universe": "survivorship_free",
        "selection_rule_version": "universe-selection-v1+test",
        "validated_ids_sha256": "1" * 64,
        "archive_sha256": "2" * 64,
        "termination_identity_sha256": "3" * 64,
    }
    fields.update(overrides)
    return UniverseIdentity(**fields)


def trial_spec(**overrides: Any) -> TrialSpec:
    fields: dict[str, Any] = {
        "hunt_id": "hunt-1",
        "family": "overnight_intraday",
        "split": "discovery",
        "signal_id": "overnight_intraday:gap_score",
        "signal_code_sha256": _SHA,
        "sign": 1,
        "selection": 0.1,
        "lag": 1,
        "h": 5,
        "entry_point": "open",
        "exit_point": "close",
        "constants": {"window": 20, "threshold": 0.25, "names": ["a", "b"]},
        "weighting": "equal",
        "lane": "real_stock_long_x1",
        "universe_identity": universe_identity(),
        "calendar_identity": "nyse-market-calendar-v1+test",
        "cost_model_id": "hunt-cost-v1+test",
        "harness_model_id": hh.HUNT_HARNESS_MODEL_ID,
        "survivor_bias_direction": "unknown",
        "survivor_bias_reason": "admitted set is today's validated universe",
        "mechanism": "overnight gaps revert intraday",
        "competing_explanation": "bid-ask bounce at the open",
    }
    fields.update(overrides)
    return TrialSpec(**fields)


# --- canonical JSON -------------------------------------------------------------------------


def test_floats_are_tagged_so_they_never_collide_with_strings_or_ints() -> None:
    assert hh.canonical_form(0.1) == {"__float__": "0.1"}
    assert hh.sha256_form(hh.canonical_form(1.0)) != hh.sha256_form(hh.canonical_form(1))
    assert hh.sha256_form(hh.canonical_form(0.5)) != hh.sha256_form(hh.canonical_form("0.5"))


def test_canonical_json_is_sorted_and_compact() -> None:
    assert hh.dumps_form(hh.canonical_form({"b": 1, "a": [2, 3]})) == '{"a":[2,3],"b":1}'


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_non_finite_floats_are_refused(bad: float) -> None:
    with pytest.raises(ValueError, match="non-finite"):
        hh.canonical_form({"x": bad})


def test_the_float_tag_is_a_reserved_key() -> None:
    with pytest.raises(ValueError, match="reserved"):
        hh.canonical_form({"__float__": "1"})


def test_decode_inverts_the_float_tag() -> None:
    value = {"a": 0.1, "b": [1, 2.5], "c": "x"}
    assert hh.decode_form(hh.canonical_form(value)) == value


def test_a_stored_form_round_trips_through_json_to_the_same_hash() -> None:
    """jsonb stores integers exactly and tagged floats as strings, so a stored spec rehashes."""
    spec = trial_spec()
    stored = json.loads(json.dumps(spec.form()))
    assert hh.candidate_sha256_of(stored) == spec.candidate_sha256
    assert hh.spec_sha256_of(stored) == spec.spec_sha256


# --- TrialSpec ------------------------------------------------------------------------------


def test_constants_are_frozen() -> None:
    spec = trial_spec()
    with pytest.raises(TypeError):
        spec.constants["window"] = 5  # type: ignore[index]
    assert spec.constants["names"] == ("a", "b")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"hunt_id": "hunt-0"}, "hunt_id"),
        ({"hunt_id": "c6-x"}, "hunt_id"),
        ({"family": "Bad-Family"}, "family"),
        ({"split": "in_sample"}, "split"),
        ({"signal_id": "no_colon"}, "signal_id"),
        ({"signal_code_sha256": "abc"}, "signal_code_sha256"),
        ({"sign": 0}, "sign"),
        ({"sign": True}, "sign"),
        ({"selection": 0.0}, "selection"),
        ({"selection": 0.51}, "selection"),
        ({"selection": 1}, "selection"),
        ({"lag": 0}, "lag"),
        ({"lag": 1.0}, "lag"),
        ({"h": 0}, "h"),
        ({"h": True}, "h"),
        ({"entry_point": "mid"}, "entry_point"),
        ({"weighting": "cap"}, "weighting"),
        ({"lane": "crypto"}, "lane"),
        ({"survivor_bias_direction": "none"}, "survivor_bias_direction"),
        ({"mechanism": " "}, "mechanism"),
        ({"cost_model_id": ""}, "cost_model_id"),
    ],
)
def test_field_domains_are_validated_on_construction(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        trial_spec(**overrides)


def test_constants_with_a_non_finite_value_are_refused() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        trial_spec(constants={"x": float("nan")})


def test_universe_identity_digests_are_validated() -> None:
    with pytest.raises(ValueError, match="archive_sha256"):
        universe_identity(archive_sha256="nope")


# --- hashes ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"hunt_id": "hunt-2"},
        {"family": "other_family"},
        {"split": "validation"},
        {"mechanism": "a different story"},
        {"competing_explanation": "another rival"},
        {"survivor_bias_reason": "reworded"},
        {"survivor_bias_direction": "favours_arm"},
    ],
)
def test_relabelling_cannot_make_a_candidate_new(overrides: dict[str, Any]) -> None:
    assert trial_spec(**overrides).candidate_sha256 == trial_spec().candidate_sha256


@pytest.mark.parametrize("overrides", [{"hunt_id": "hunt-2"}, {"family": "other_family"}, {"split": "validation"}])
def test_spec_sha_adds_hunt_family_and_split(overrides: dict[str, Any]) -> None:
    assert trial_spec(**overrides).spec_sha256 != trial_spec().spec_sha256


@pytest.mark.parametrize(
    "overrides",
    [
        {"signal_id": "overnight_intraday:other"},
        {"signal_code_sha256": "b" * 64},
        {"sign": -1},
        {"selection": 0.2},
        {"lag": 2},
        {"h": 6},
        {"entry_point": "close"},
        {"exit_point": "open"},
        {"constants": {"window": 21, "threshold": 0.25, "names": ["a", "b"]}},
        {"lane": "stock_cfd_long_x1"},
        {"universe_identity": universe_identity(archive_sha256="4" * 64)},
        {"calendar_identity": "other"},
        {"cost_model_id": "other"},
        {"harness_model_id": "other"},
    ],
)
def test_every_defining_field_moves_the_candidate(overrides: dict[str, Any]) -> None:
    assert trial_spec(**overrides).candidate_sha256 != trial_spec().candidate_sha256


def test_the_hash_field_lists_cover_every_spec_field_but_prose() -> None:
    names = {f.name for f in dataclasses.fields(TrialSpec) if not f.name.startswith("_")}
    prose = {"survivor_bias_direction", "survivor_bias_reason", "mechanism", "competing_explanation"}
    assert set(hh.SPEC_FIELDS) == names - prose
    assert set(hh.SPEC_FIELDS) - set(hh.CANDIDATE_FIELDS) == {"hunt_id", "family", "split"}


# --- timeline -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("lag", "h", "entry", "exit_", "refused"),
    [
        (1, 1, "open", "close", False),
        (1, 1, "close", "close", True),
        (1, 1, "open", "open", True),
        (1, 1, "close", "open", True),
        (1, 2, "close", "open", False),
        (1, 63, "close", "close", False),
        (2, 62, "open", "close", False),
        (2, 63, "open", "close", True),
        (63, 1, "open", "close", False),
        (64, 1, "open", "close", True),
    ],
)
def test_timeline_refusals(lag: int, h: int, entry: str, exit_: str, refused: bool) -> None:
    spec = trial_spec(lag=lag, h=h, entry_point=entry, exit_point=exit_)
    assert (hh.timeline_refusal(spec) is not None) is refused


# --- constants ------------------------------------------------------------------------------


def test_every_budget_is_a_positive_integer() -> None:
    for hunt, budget in hh.HUNT_BUDGETS.items():
        assert hh._HUNT_ID.match(hunt), hunt
        assert isinstance(budget, int) and not isinstance(budget, bool) and budget > 0, (hunt, budget)


def test_closed_hunts_are_hunt_ids_with_a_readout() -> None:
    for hunt, readout in hh.HUNT_CLOSED.items():
        assert hh._HUNT_ID.match(hunt), hunt
        assert readout.strip(), hunt


def test_the_model_id_is_the_hash_of_the_model_constants() -> None:
    form = hh.canonical_form(hh._model_constants())
    assert hh.HUNT_HARNESS_MODEL_ID == f"hunt-harness-v1+{hh.sha256_form(form)[:16]}"
    assert hh._model_constants()["splits"]["holdout"][0] == "2021-06-29"


def test_operational_constants_do_not_enter_the_model_id() -> None:
    assert "budget" not in json.dumps(hh._model_constants())
    assert "closed" not in json.dumps(hh._model_constants())


def test_the_pinned_tariff_prices_only_the_real_stock_lane() -> None:
    assert hh.HUNT_TARIFF is not None
    assert hh.running_cost_model_id("real_stock_long_x1") == hh.HUNT_TARIFF.cost_model_id()
    assert all(hh.running_cost_model_id(lane) is None for lane in hh.LANES if lane != "real_stock_long_x1")


def test_the_tariff_hash_is_the_recorded_fees_text() -> None:
    assert hh.HUNT_TARIFF is not None
    digest = hashlib.sha256(hh.HUNT_TARIFF_EVIDENCE.encode("utf-8")).hexdigest()
    assert digest == hh.HUNT_TARIFF.text_sha256
    # The UK row of the stock-commission table: $0 on every exchange, so nothing is fixed-fee.
    assert "All other exchanges\n$0\t$0" in hh.HUNT_TARIFF_EVIDENCE
    assert hh.HUNT_TARIFF.proportional_commission_per_side == 0.0


def test_a_tariff_rejects_a_malformed_record() -> None:
    with pytest.raises(ValueError):
        _tariff(text_sha256="not-a-hash")
    with pytest.raises(ValueError):
        _tariff(proportional_commission_per_side=1)


def test_nothing_registers_while_no_hunt_has_a_budget() -> None:
    assert dict(hh.HUNT_BUDGETS) == {}


def _tariff(**overrides: Any) -> hh.HuntTariff:
    fields: dict[str, Any] = {
        "url": "https://www.etoro.com/trading/fees/",
        "fetched_on": date(2026, 9, 26),
        "text_sha256": "5" * 64,
        "residence_country": "United Kingdom",
        "account_currency": "USD",
        "proportional_commission_per_side": 0.0,
    }
    fields.update(overrides)
    return hh.HuntTariff(**fields)


def test_only_the_real_stock_lane_is_priced_and_only_in_usd(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hh, "HUNT_TARIFF", _tariff())
    assert hh.running_cost_model_id("real_stock_long_x1") == _tariff().cost_model_id()
    assert hh.running_cost_model_id("stock_cfd_long_x1") is None
    monkeypatch.setattr(hh, "HUNT_TARIFF", _tariff(account_currency="GBP"))
    assert hh.running_cost_model_id("real_stock_long_x1") is None


def test_the_cost_identity_moves_with_the_fetched_page() -> None:
    assert _tariff().cost_model_id().startswith("hunt-cost-v1+")
    assert _tariff(text_sha256="6" * 64).cost_model_id() != _tariff().cost_model_id()


# --- signal code hash -----------------------------------------------------------------------


def test_signal_code_hash_is_the_module_file_and_needs_the_callable(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = b"def gap_score(t, view, constants):\n    return {}\n"
    (tmp_path / "overnight_intraday.py").write_bytes(source)
    monkeypatch.setattr(hh, "SIGNAL_PACKAGE_DIR", tmp_path)
    assert hh.signal_code_sha256("overnight_intraday:gap_score") == hashlib.sha256(source).hexdigest()
    assert hh.signal_code_sha256("overnight_intraday:missing") is None
    assert hh.signal_code_sha256("absent_module:gap_score") is None


# --- obligation 83 --------------------------------------------------------------------------


def _row(trial: TrialSpec, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "hunt_id": trial.hunt_id,
        "family": trial.family,
        "split": trial.split,
        "candidate_sha256": trial.candidate_sha256,
        "spec_sha256": trial.spec_sha256,
        "spec": json.loads(json.dumps(trial.form())),
        "harness_model_id": trial.harness_model_id,
        "note": None,
    }
    row.update(overrides)
    return row


def test_a_consistent_row_verifies() -> None:
    assert hh.verify_trial_row(**_row(trial_spec())) == []


def test_a_tampered_spec_fails_both_hashes() -> None:
    spec = trial_spec()
    stored = json.loads(json.dumps(spec.form()))
    stored["lag"] = 2
    problems = hh.verify_trial_row(**_row(spec, spec=stored))
    assert any("candidate_sha256" in p for p in problems)
    assert any("spec_sha256" in p for p in problems)


def test_a_column_disagreeing_with_its_spec_is_reported() -> None:
    assert hh.verify_trial_row(**_row(trial_spec(), family="other")) != []


def test_an_unreconstructed_recording_hashes_its_note() -> None:
    note_sha = hashlib.sha256(b"chart 1, variant 1").hexdigest()
    base = {"hunt_id": "hunt-1", "family": "f", "split": "discovery", "spec": None, "harness_model_id": "m"}
    assert hh.verify_trial_row(**base, candidate_sha256=note_sha, spec_sha256=note_sha, note="chart 1, variant 1") == []
    assert hh.verify_trial_row(**base, candidate_sha256=_SHA, spec_sha256=note_sha, note="chart 1, variant 1") != []


def test_form_is_a_detached_copy_so_the_hashes_cannot_be_mutated() -> None:
    spec = trial_spec()
    before = (spec.candidate_sha256, spec.spec_sha256)
    spec.form()["constants"]["window"] = 999
    spec.form()["constants"]["names"].append("c")
    assert (spec.candidate_sha256, spec.spec_sha256) == before
