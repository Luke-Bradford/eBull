"""#3515 slice 4 — fund-v1's freeze, pure half: the terms, the document, the budget-fixture and
measurement checks, v1 hashed-module parity, and the script's three phases (fund-v1 spec §6, §7)."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

import pytest

import scripts.ai_trial_fund_freeze as script
from app.services.ai_trial_freeze import (
    V1_TERMS,
    FreezeReport,
    Provenance,
    TrialFreezeError,
    build_declaration,
    config_refusals,
    freeze_trial,
)
from app.services.ai_trial_fund_blocks import INPUT_TOKEN_CEILING
from app.services.ai_trial_fund_freeze import (
    EXPECTED_FUND_REGISTER_ENTRY,
    FUND_TERMS,
    budget_fixture_refusal,
    measurements_refusal,
    v1_module_parity,
)
from app.services.ai_trial_fund_pack import fixture_bytes
from app.services.ai_trial_fund_policy import FUND_POLICY_HASH, fund_policy_manifest
from app.services.ai_trial_guard import decision_json_schema
from app.services.ai_trial_intent import declaration_digest
from app.services.ai_trial_invocation import TRIAL_MODEL_ID
from app.services.ai_trial_pack import canonical_sha256
from app.services.ai_trial_policy import POLICY_MODULES, policy_manifest
from app.services.ai_trial_prompt import SYSTEM_PROMPT_SHA256
from app.services.ai_trial_version import FUND_V1
from app.services.trial_register import TRIAL_REGISTER

PROVENANCE = Provenance(code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0", refusals=())
#: Declaration 16's top-level keys on dev (2026-10-01): v1's document shape must not move.
V1_DOC_KEYS = {
    "kind",
    "prereg",
    "model_id",
    "draw_rule",
    "spec_path",
    "policy_hash",
    "spec_sha256",
    "strategy_id",
    "code_git_sha",
    "policy_modules",
    "python_version",
    "frozen_constants",
    "strategy_version",
    "control_strategy_id",
    "system_prompt_sha256",
    "decision_schema_sha256",
    "prompt_template_sha256",
    "house_functions_by_name",
}


def _doc(terms: Any = None) -> dict[str, Any]:
    return build_declaration(
        manifest=(terms or V1_TERMS).manifest(),
        code_git_sha="a" * 40,
        spec_sha256="b" * 64,
        python_version="3.12.0",
        terms=terms,
    )


def test_v1_terms_reproduce_v1s_document() -> None:
    doc = _doc()
    assert set(doc) == V1_DOC_KEYS
    assert doc["kind"] == "ai-trial-declaration-v1"
    assert doc["system_prompt_sha256"] == SYSTEM_PROMPT_SHA256
    assert doc["spec_path"] == "docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md"
    assert V1_TERMS.builder == "ai_trial_freeze.build_declaration"
    assert V1_TERMS.capital_limit is None


def test_fund_document_carries_fund_v1s_identity_and_hash() -> None:
    doc = _doc(FUND_TERMS)
    assert set(doc) == V1_DOC_KEYS
    assert (doc["kind"], doc["strategy_id"], doc["control_strategy_id"]) == (
        "ai-trial-declaration-fund-v1",
        "ai-discretionary-fund-v1",
        "ai-discretionary-fund-v1-control",
    )
    assert doc["policy_hash"] == FUND_POLICY_HASH == fund_policy_manifest().digest()
    assert doc["system_prompt_sha256"] == FUND_V1.system_prompt_sha256 != SYSTEM_PROMPT_SHA256
    assert doc["spec_path"] == "docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md"
    # v1's stamps (§7): the same #2599 terms.
    assert doc["prereg"] == _doc()["prereg"]


def test_the_register_charges_fund_v1_once_with_the_pinned_entry() -> None:
    entries = [t for t in TRIAL_REGISTER.trials if t.declared_for == EXPECTED_FUND_REGISTER_ENTRY.declared_for]
    assert entries == [EXPECTED_FUND_REGISTER_ENTRY]
    assert TRIAL_REGISTER.trial_for_declaration("ai-discretionary-fund-v1", "v1") == EXPECTED_FUND_REGISTER_ENTRY
    assert EXPECTED_FUND_REGISTER_ENTRY.searches == 1


def _leg(capital: str = "3000.000000", **overrides: Any) -> dict[str, Any]:
    return {"deployment_id": 1, "enabled": True, "capital_limit": capital, "currency": "USD", "policy": {}, **overrides}


def test_fund_capital_is_the_spec_value() -> None:
    arm, control = FUND_V1.leg_strategy_ids
    assert config_refusals({arm: _leg(), control: _leg()}, FUND_TERMS) == []
    assert config_refusals({arm: _leg("1000"), control: _leg("1000")}, FUND_TERMS) == ["trial_capital_not_spec"]
    # v1 keeps the value the supervisor's.
    v1_arm, v1_control = "ai-discretionary-v1", "ai-discretionary-v1-control"
    assert config_refusals({v1_arm: _leg("1000"), v1_control: _leg("1000")}) == []


def _fixture(**overrides: Any) -> dict[str, Any]:
    probe = {
        "n": 28,
        "attempt": 1,
        "outcome": "pass",
        "input_tokens": 831_920,
        "rendered_prompt_bytes": 1_531_589,
        "pack_sha256": "p" * 64,
        "rendered_prompt_sha256": "r" * 64,
        "refusal_reason": None,
        "detail": "",
        "cost_usd": "6.63",
    }
    fixture = {
        "n": 28,
        "n0": 30,
        "bytes_per_name": 54_121,
        "start_base_tokens": 14_527,
        "start_bytes_per_token": "1.95",
        "attempt": 1,
        "pack_sha256": "p" * 64,
        "rendered_prompt_sha256": "r" * 64,
        "rendered_prompt_bytes": 1_531_589,
        "input_tokens": 831_920,
        "cli_version": "2.1.285 (Claude Code)",
        "model_id": TRIAL_MODEL_ID,
        "system_prompt_sha256": FUND_V1.system_prompt_sha256,
        "decision_schema_sha256": canonical_sha256(decision_json_schema()),
        "probes": [{**probe, "n": 30, "outcome": "over_ceiling", "input_tokens": 890_436}, probe],
    }
    return {**fixture, **overrides}


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, None),
        ({"input_tokens": INPUT_TOKEN_CEILING + 1}, "budget_fixture_invalid"),
        ({"input_tokens": 0}, "budget_fixture_invalid"),
        ({"input_tokens": True}, "budget_fixture_invalid"),
        ({"rendered_prompt_bytes": 1}, "budget_fixture_invalid"),  # not the chosen probe's
        ({"attempt": 2}, "budget_fixture_invalid"),  # no such probe
        ({"n": 30}, "budget_fixture_invalid"),  # the probe at 30 is not a pass
        ({"model_id": "other"}, "budget_fixture_invalid"),
        ({"system_prompt_sha256": "x"}, "budget_fixture_invalid"),
        ({"decision_schema_sha256": "x"}, "budget_fixture_invalid"),
        ({"cli_version": "unavailable: OSError"}, "budget_fixture_invalid"),
        ({"bytes_per_name": 1.5}, "budget_fixture_invalid"),  # a float breaks the JSONB digest
        ({"probes": "nope"}, "budget_fixture_invalid"),
    ],
)
def test_budget_fixture_refusal(overrides: dict[str, Any], expected: str | None) -> None:
    assert budget_fixture_refusal(_fixture(**overrides)) == expected


def test_a_missing_fixture_is_not_run() -> None:
    assert budget_fixture_refusal(None) == "budget_fixture_not_run"
    assert budget_fixture_refusal([]) == "budget_fixture_invalid"


def test_measurements_refusal() -> None:
    good = {"coverage": {"output_lines": ["as_of: x"]}, "prompt_budget": {"output_lines": ["y"]}}
    assert measurements_refusal(good) is None
    assert measurements_refusal(None) == "measurements_missing"
    assert measurements_refusal({**good, "coverage": {"output_lines": []}}) == "measurements_missing"
    assert measurements_refusal({"coverage": good["coverage"]}) == "measurements_missing"


def test_the_document_round_trips_and_the_runtime_reads_its_byte_gate() -> None:
    doc = {**_doc(FUND_TERMS), "budget_fixture": _fixture()}
    assert declaration_digest(doc) == declaration_digest(dict(doc))
    assert fixture_bytes(doc) == 1_531_589


def test_v1_module_parity_per_declaration() -> None:
    v1 = policy_manifest().module_sha256
    fund = fund_policy_manifest().module_sha256
    intact = {"policy_modules": dict(v1)}
    edited = {"policy_modules": {**v1, "ai_trial_guard.py": "0" * 64}}
    lacking = {"policy_modules": {k: v for k, v in v1.items() if k != "ai_trial_plan.py"}}
    docs = [
        (16, intact, declaration_digest(intact)),
        (17, edited, declaration_digest(edited)),
        (18, lacking, declaration_digest(lacking)),
        (19, intact, "not-the-digest"),
    ]
    parity = v1_module_parity(docs, fund)
    assert set(parity["16"]) == set(POLICY_MODULES)
    assert all(parity["16"].values())  # fund-v1 hashes v1's modules as they are
    assert parity["17"]["ai_trial_guard.py"] is False and parity["17"]["ai_trial_plan.py"] is True
    assert parity["18"]["ai_trial_plan.py"] is None
    assert set(parity["19"].values()) == {None}


def test_freeze_refuses_an_unregistered_descriptor_and_a_key_overwrite() -> None:
    class Idle:
        class info:  # noqa: N801
            from psycopg.pq import TransactionStatus

            transaction_status = TransactionStatus.IDLE

    forged = dataclasses.replace(FUND_TERMS, version=dataclasses.replace(FUND_V1, policy_hash="x"))
    with pytest.raises(TrialFreezeError, match="not a registered descriptor"):
        freeze_trial(Idle(), provenance=PROVENANCE, apply=False, terms=forged)  # type: ignore[arg-type]
    with pytest.raises(TrialFreezeError, match="may not be overwritten"):
        freeze_trial(Idle(), provenance=PROVENANCE, apply=False, terms=FUND_TERMS, outside={"policy_hash": "x"})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# The script's three phases (freeze_trial stubbed)
# ---------------------------------------------------------------------------
class _Freezes:
    """Stands in for ``freeze_trial``: records each call; the precheck's refusals are given."""

    def __init__(self, pre_refusals: tuple[str, ...]) -> None:
        self.pre_refusals = pre_refusals
        self.calls: list[dict[str, Any]] = []

    def __call__(self, conn: Any, **kwargs: Any) -> FreezeReport:
        self.calls.append(kwargs)
        first = len(self.calls) == 1
        return FreezeReport(
            applied=False,
            refusals=self.pre_refusals if first else (),
            doc={"code_git_sha": "a" * 40, "policy_hash": "h", **kwargs["outside"]},
            doc_sha256="d",
            config={},
            config_sha256="c",
        )


@contextmanager
def _conn() -> Iterator[None]:
    yield None


def _summary(**overrides: Any) -> dict[str, Any]:
    fixture = _fixture()
    return {**fixture, "freeze_refusal": None, **overrides}


def _steps(
    walks: list[int], *, coverage: Any = None, provenance_shas: tuple[str, str] = ("a", "a"), summary: Any = None
) -> script.Steps:
    shas = iter(provenance_shas)

    def walk() -> dict[str, Any]:
        walks.append(1)
        return summary if summary is not None else _summary()

    def budget() -> int:
        print("fund-v1 rendered user prompt bytes: 1,185,479")
        return 0

    return script.Steps(
        connect=_conn,
        provenance=lambda: dataclasses.replace(PROVENANCE, code_git_sha=next(shas)),
        walk=walk,
        coverage=coverage or (lambda: print("as_of: 2026-10-01")),
        prompt_budget=budget,
    )


def test_a_precheck_refusal_spends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    freezes = _Freezes(("v1_not_wound_down:trial_active", "budget_fixture_not_run", "measurements_missing"))
    monkeypatch.setattr(script, "freeze_trial", freezes)
    walks: list[int] = []
    report, summary = script.run(_steps(walks), apply=False, measure=False)
    assert (walks, summary, len(freezes.calls)) == ([], None, 1)
    assert "v1_not_wound_down:trial_active" in report.refusals


def test_apply_gaps_refuse_before_any_spend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(script, "freeze_trial", _Freezes(("budget_fixture_not_run", "measurements_missing")))
    walks: list[int] = []
    report, _ = script.run(_steps(walks), apply=True, measure=False, declared_by="s", expect_config_sha256="other")
    assert walks == []
    assert {"wake_evidence_missing", "config_changed"} <= set(report.refusals)


def test_only_measurement_refusals_measure_then_freeze_with_them(monkeypatch: pytest.MonkeyPatch) -> None:
    freezes = _Freezes(("budget_fixture_not_run", "measurements_missing"))
    monkeypatch.setattr(script, "freeze_trial", freezes)
    walks: list[int] = []
    report, summary = script.run(_steps(walks), apply=False, measure=False)
    assert walks == [1] and summary is not None and report.refusals == ()
    outside = freezes.calls[1]["outside"]
    assert budget_fixture_refusal(outside["budget_fixture"]) is None
    assert measurements_refusal(outside["measurements"]) is None
    assert (outside["measurements"]["real_prompt_bytes"], outside["measurements"]["fixture_prompt_bytes"]) == (
        1_185_479,
        1_531_589,
    )
    assert freezes.calls[1]["terms"] is FUND_TERMS


def test_a_refused_walk_is_the_freezes_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(script, "freeze_trial", _Freezes(("budget_fixture_not_run",)))
    summary = _summary(n=None, freeze_refusal="prompt_budget_exceeded")
    report, _ = script.run(_steps([], summary=summary), apply=False, measure=False)
    assert "prompt_budget_exceeded" in report.refusals


def test_a_failed_measurement_and_a_moved_head_refuse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(script, "freeze_trial", _Freezes(("budget_fixture_not_run",)))

    def exits() -> None:
        raise SystemExit(2)

    report, _ = script.run(_steps([], coverage=exits, provenance_shas=("a", "b")), apply=False, measure=False)
    assert "measurement_failed:scripts.measure_3515_fund_pack_coverage:SystemExit" in report.refusals
    assert "provenance_changed" in report.refusals


def test_the_real_bytes_line_must_appear_exactly_once() -> None:
    assert script.real_prompt_bytes({"output_lines": ["fund-v1 rendered user prompt bytes: 1,185,479"]}) == 1_185_479
    assert script.real_prompt_bytes({"output_lines": []}) is None
    assert script.real_prompt_bytes({"output_lines": ["fund-v1 rendered user prompt bytes: 1"] * 2}) is None


def test_the_walk_summary_becomes_a_json_exact_fixture() -> None:
    summary = _summary()
    summary["probes"] = [{**p, "cost_usd": 6.63} for p in summary["probes"]]
    fixture = script.budget_fixture(summary)
    assert budget_fixture_refusal(fixture) is None
    assert all(isinstance(p["cost_usd"], str) for p in fixture["probes"])
    assert Decimal(fixture["probes"][0]["cost_usd"]) == Decimal("6.63")
