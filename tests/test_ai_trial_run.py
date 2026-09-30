"""#3471 slice 2c-iv-b — the run publisher's pure parts: declaration checks, entry cap, stored
output cap and the policy hash. The step-5 pairing is ``test_ai_trial_guard.py``.

The claim / publish / refusal paths against real Postgres are ``test_ai_trial_run_db.py``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

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
    assert declaration_refusal(_declaration(doc), policy_hash=AI_TRIAL_POLICY_HASH) is None
    # Digest-intact first: a stored sha that is not the document's.
    assert (
        declaration_refusal(_declaration(doc, sha="d" * 64), policy_hash=AI_TRIAL_POLICY_HASH)
        == "trial_declaration_not_intact"
    )
    wrong_contract = Declaration(1, doc, declaration_digest(doc), "ai-trial-declaration-v1:" + "e" * 64, "active")
    assert declaration_refusal(wrong_contract, policy_hash=AI_TRIAL_POLICY_HASH) == "trial_declaration_not_intact"
    # O6: a document with no policy hash, or another one, is drift.
    assert (
        declaration_refusal(_declaration({**doc, "policy_hash": "f" * 64}), policy_hash=AI_TRIAL_POLICY_HASH)
        == "policy_drift"
    )
    no_hash = {k: v for k, v in doc.items() if k != "policy_hash"}
    assert declaration_refusal(_declaration(no_hash), policy_hash=AI_TRIAL_POLICY_HASH) == "policy_drift"
    # No state event at all is not active (fail closed), nor is a halt.
    assert declaration_refusal(_declaration(doc, state=None), policy_hash=AI_TRIAL_POLICY_HASH) == "trial_not_active"
    assert (
        declaration_refusal(_declaration(doc, state="halted_operator"), policy_hash=AI_TRIAL_POLICY_HASH)
        == "trial_not_active"
    )


def _book(n: int) -> LegBook:
    positions = tuple({"instrument_id": i} for i in range(n))
    return LegBook(frozenset(range(n)), positions)


@pytest.mark.parametrize(
    ("arm", "control", "expected"),
    # 12 slots per leg (§8 answer, 2026-09-30).
    [(0, 0, 2), (11, 0, 1), (0, 11, 1), (12, 0, 0), (2, 2, 2), (13, 1, 0)],
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
