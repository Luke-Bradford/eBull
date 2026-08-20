from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from scripts.evaluate_2582_schedule13d_outcomes import OutcomeGate
from scripts.schedule13d_artifact import build_artifact, verify_artifact


class _Report:
    decision = "inconclusive"

    def as_dict(self) -> dict[str, object]:
        return {"decision": self.decision, "primary": {"event_count": 10}}


def test_artifact_pins_gate_corpus_implementation_and_report_digest() -> None:
    gate = OutcomeGate("c" * 64, "trial-register-test", "trial-test", 11, 22)
    artifact = build_artifact(
        gate,
        _Report(),  # type: ignore[arg-type]
        generated_at=datetime(2026, 8, 12, 9, 0, tzinfo=UTC),
    )
    assert artifact.trial_id == "trial-test"
    assert artifact.contract_sha256 == "c" * 64
    assert len(artifact.implementation_sha256) == 64
    # #2614 — the envelope names the declaration and the access that authorised it.
    assert (artifact.declaration_id, artifact.access_id) == (11, 22)
    assert artifact.source_first_date == "2024-12-18"
    assert artifact.source_last_complete_filing_date == "2026-06-18"
    assert json.loads(artifact.to_json())["report"]["primary"]["event_count"] == 10
    verify_artifact(artifact)


def test_artifact_verifier_refuses_report_or_decision_tampering() -> None:
    artifact = build_artifact(
        OutcomeGate("c" * 64, "register", "trial", 11, 22),
        _Report(),  # type: ignore[arg-type]
        generated_at=datetime(2026, 8, 12, 9, 0, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="digest"):
        verify_artifact(replace(artifact, report={"decision": "inconclusive"}))
    with pytest.raises(ValueError, match="decision"):
        verify_artifact(replace(artifact, decision="pass"))
    with pytest.raises(ValueError, match="implementation"):
        verify_artifact(replace(artifact, implementation_sha256="0" * 64))


def test_artifact_requires_an_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        build_artifact(
            OutcomeGate("c" * 64, "register", "trial", 11, 22),
            _Report(),  # type: ignore[arg-type]
            generated_at=datetime(2026, 8, 12, 9, 0),
        )
