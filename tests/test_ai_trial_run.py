"""#3471 slice 2c-iv-b — the run publisher's pure parts: declaration checks, entry cap, stored
output cap, the policy hash and the step-5 pairing it shares with the synthetic CLI.

The claim / publish / refusal paths against real Postgres are ``test_ai_trial_run_db.py``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.services.ai_trial_decision import (
    DecisionVerdict,
    TrialDecision,
    decision_metrics,
    measure_atr,
    plan_pairs,
)
from app.services.ai_trial_intent import DECLARATION_CONTRACT_PREFIX, declaration_digest
from app.services.ai_trial_invocation import InvocationResult
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH, FROZEN_CONSTANTS, POLICY_MODULES, policy_hash
from app.services.ai_trial_run import (
    STORED_OUTPUT_CAP_BYTES,
    Declaration,
    LegBook,
    _stored_output,
    declaration_refusal,
    max_new_entries,
)

SHA = "c" * 64
SESSION = date(2026, 10, 5)


def _declaration(doc: object, *, state: str | None = "active", sha: str | None = None) -> Declaration:
    digest = declaration_digest(doc) if sha is None else sha
    return Declaration(1, doc, digest, DECLARATION_CONTRACT_PREFIX + digest, state)


def test_declaration_refusal_order() -> None:
    doc = {"strategy_id": "ai-discretionary-v1", "strategy_version": "v1", "policy_hash": AI_TRIAL_POLICY_HASH}
    assert declaration_refusal(_declaration(doc)) is None
    # Digest-intact first: a stored sha that is not the document's.
    assert declaration_refusal(_declaration(doc, sha="d" * 64)) == "trial_declaration_not_intact"
    wrong_contract = Declaration(1, doc, declaration_digest(doc), "ai-trial-declaration-v1:" + "e" * 64, "active")
    assert declaration_refusal(wrong_contract) == "trial_declaration_not_intact"
    # O6: a document with no policy hash, or another one, is drift.
    assert declaration_refusal(_declaration({**doc, "policy_hash": "f" * 64})) == "policy_drift"
    no_hash = {k: v for k, v in doc.items() if k != "policy_hash"}
    assert declaration_refusal(_declaration(no_hash)) == "policy_drift"
    # No state event at all is not active (fail closed), nor is a halt.
    assert declaration_refusal(_declaration(doc, state=None)) == "trial_not_active"
    assert declaration_refusal(_declaration(doc, state="halted_operator")) == "trial_not_active"


def _book(n: int) -> LegBook:
    positions = tuple({"instrument_id": i} for i in range(n))
    return LegBook(frozenset(range(n)), positions)


@pytest.mark.parametrize(
    ("arm", "control", "expected"),
    [(0, 0, 2), (3, 0, 1), (0, 3, 1), (4, 0, 0), (2, 2, 2), (5, 1, 0)],
)
def test_max_new_entries_is_min_of_two_and_both_legs_free_slots(arm: int, control: int, expected: int) -> None:
    assert max_new_entries({"arm": _book(arm), "control": _book(control)}) == expected


def _result(stdout: bytes, stderr: bytes) -> InvocationResult:
    return InvocationResult(None, "", None, None, None, 0, stdout, stderr, 0)


def test_stored_output_fits_the_shared_cap_after_decoding() -> None:
    # An invalid byte decodes to U+FFFD (3 bytes): a raw-length check would overrun the cap.
    stdout, stderr = _stored_output(_result(b"\xff" * STORED_OUTPUT_CAP_BYTES, b"tail"))
    assert len(stdout.encode()) + len(stderr.encode()) <= STORED_OUTPUT_CAP_BYTES
    assert len(stderr) < len("tail")  # stdout took the budget; stderr gets only the remainder
    # NUL cannot be stored in text.
    out, err = _stored_output(_result(b"a\x00b", b"e\x00"))
    assert "\x00" not in out + err and out.startswith("a") and err.startswith("e")


def test_policy_hash_moves_with_any_policy_module_byte(tmp_path: Path) -> None:
    services = Path(__file__).resolve().parents[1] / "app" / "services"
    for name in POLICY_MODULES:
        (tmp_path / name).write_bytes((services / name).read_bytes())
    assert policy_hash(tmp_path) == AI_TRIAL_POLICY_HASH
    target = tmp_path / "ai_trial_decision.py"
    target.write_bytes(target.read_bytes() + b"\n")
    assert policy_hash(tmp_path) != AI_TRIAL_POLICY_HASH


def test_policy_hash_moves_with_a_frozen_term_defined_outside_the_hashed_modules() -> None:
    # e.g. the per-leg slot cap lives in the (unhashed) executor; the start gate imports it.
    moved = {**FROZEN_CONSTANTS, "ai_trial_executor.TRIAL_MAX_CONCURRENT_PER_LEG": 5}
    assert policy_hash(constants=moved) != AI_TRIAL_POLICY_HASH
    assert policy_hash(constants=dict(FROZEN_CONSTANTS)) == AI_TRIAL_POLICY_HASH


def _verdict(position: int, iid: int | None, reason: str | None, stop: float = 8.0) -> DecisionVerdict:
    decision = TrialDecision(
        action="enter_long",
        symbol=f"S{iid}",
        stop_pct=stop,
        target_pct=2 * stop,
        horizon_days=10,
        size_tier="half",
        confidence=3,
        thesis="Thesis.",
    )
    atr = measure_atr(4.0, 100.0) if iid is not None else None
    return DecisionVerdict(position, decision, iid, reason, decision_metrics(stop, 2 * stop, atr))  # type: ignore[arg-type]


def test_plan_pairs_draws_without_replacement_and_numbers_only_created_pairs() -> None:
    atr = {i: measure_atr(4.0, 100.0) for i in (1, 2, 3)}
    verdicts = (_verdict(0, 1, None), _verdict(1, None, "not_in_shortlist"), _verdict(2, 2, None))
    planned = plan_pairs(
        verdicts,
        shortlist_instrument_ids=[1, 2, 3],
        atr_by_instrument=atr,
        control_held_instrument_ids=frozenset({3}),
        declaration_sha256_hex=SHA,
        session_date=SESSION,
        first_pair_seq=7,
    )
    assert [p.reason_code for p in planned] == [None, "not_in_shortlist", None]
    first, second = planned[0].pair, planned[2].pair
    assert first is not None and second is not None
    assert (first.pair_seq, second.pair_seq) == (7, 8)
    # The control leg's holding never enters a pool; the first draw is excluded from the second.
    assert 3 not in first.draw.pool
    assert second.draw.pool == tuple(i for i in (1, 2) if i != first.draw.instrument_id)


def test_plan_pairs_exhaustion_refuses_the_arm_and_consumes_no_seq() -> None:
    atr = {1: measure_atr(4.0, 100.0), 2: None}
    verdicts = (_verdict(0, 1, None), _verdict(1, 1, None, stop=12.0))
    planned = plan_pairs(
        verdicts,
        shortlist_instrument_ids=[1, 2],
        atr_by_instrument=atr,
        control_held_instrument_ids=frozenset(),
        declaration_sha256_hex=SHA,
        session_date=SESSION,
        first_pair_seq=0,
    )
    # Name 2 has no valid measurement (never placeable); name 1 is drawn by the first pair.
    assert planned[0].pair is not None and planned[0].pair.pair_seq == 0
    assert planned[1].reason_code == "control_pool_exhausted" and planned[1].pair is None
