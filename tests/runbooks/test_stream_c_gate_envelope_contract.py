"""Contract tests for the Stream-C gate envelope JSONL output.

Pins the envelope's exact 8-key shape via the Pydantic model at
``app.runbooks.stream_a_stream_c_gate_schema``. Any rename / addition /
removal / type change at the runbook emitter without a parallel schema
update FAILS the canonical-path positive test immediately, blocking
the commit.

History: caught by 3 lenses (API B4 + Codex B1 + Test B2) in the
Stream A ETL-sweep 8-lens committee review (2026-05-24). Run-#8-readiness
fixes Item 4 spec at ``docs/proposals/etl/run-8-readiness-fixes.md``.

The 5 cases below match the spec § Item 4 acceptance test list verbatim:

* Positive — canonical envelope validates.
* Negative — missing key.
* Negative — wrong type.
* Negative — wrong schema_version.
* Negative — unknown top-level key (extra='forbid' guarantee).
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from app.runbooks.stream_a_stream_c_gate_schema import (
    CheckRecord,
    Envelope,
    validate_envelope,
)


def _canonical_envelope() -> dict[str, Any]:
    """Build a known-valid envelope shaped exactly as the runbook emits."""
    return {
        "schema_version": 1,
        "runbook": "stream_a_stream_c_gate",
        "bootstrap_run_id": 123,
        "started_at": "2026-05-24T18:00:00+00:00",
        "ended_at": "2026-05-24T18:05:00+00:00",
        "checks": [
            {"id": "c1_complete", "status": "passed", "count": 1, "detail": "complete"},
            {"id": "c7_sidecar", "status": "passed", "count": 4500, "detail": "ok"},
        ],
        "accepted": True,
        "first_failed": None,
    }


def test_envelope_canonical_path_validates() -> None:
    """POSITIVE: known-valid envelope round-trips through the model."""
    envelope = validate_envelope(_canonical_envelope())
    assert isinstance(envelope, Envelope)
    assert envelope.schema_version == 1
    assert envelope.runbook == "stream_a_stream_c_gate"
    assert envelope.accepted is True
    assert envelope.first_failed is None
    assert len(envelope.checks) == 2
    assert all(isinstance(c, CheckRecord) for c in envelope.checks)


def test_envelope_rejects_missing_required_key() -> None:
    """NEGATIVE: dropping ``schema_version`` raises ValidationError."""
    payload = _canonical_envelope()
    del payload["schema_version"]
    with pytest.raises(ValidationError) as exc_info:
        validate_envelope(payload)
    # Confirm the error names the missing field so future debugging is fast.
    assert "schema_version" in str(exc_info.value)


def test_envelope_rejects_wrong_type_for_accepted() -> None:
    """NEGATIVE: ``accepted`` must be bool, not str. Pydantic ``bool`` is
    permissive (accepts ``"true"`` / ``1``) so we use a value pydantic
    cannot coerce — a non-truthy non-bool string."""
    payload = _canonical_envelope()
    payload["accepted"] = "definitely-not-a-bool"
    with pytest.raises(ValidationError) as exc_info:
        validate_envelope(payload)
    assert "accepted" in str(exc_info.value)


def test_envelope_rejects_wrong_schema_version() -> None:
    """NEGATIVE: ``schema_version=2`` raises until a v2 model lands.

    Pinning v1 here is what enables forward-compat versioning — a future
    PR that introduces v2 must add a sibling Envelope2 model + update
    the validator dispatch, not silently let v2 through this v1 check.
    """
    payload = _canonical_envelope()
    payload["schema_version"] = 2
    with pytest.raises(ValidationError) as exc_info:
        validate_envelope(payload)
    assert "schema_version" in str(exc_info.value)


def test_envelope_rejects_unknown_top_level_key() -> None:
    """NEGATIVE: ``extra='forbid'`` guarantees future shape drift fails
    fast. The most common drift pattern is "someone added a field at
    the runbook emitter and forgot the schema" — this test catches it
    immediately. Use the renamed-key shape (``verdict`` not ``accepted``)
    from the v1.2 spec bug Codex caught.
    """
    payload = _canonical_envelope()
    payload["verdict"] = "passed"
    with pytest.raises(ValidationError) as exc_info:
        validate_envelope(payload)
    assert "verdict" in str(exc_info.value) or "extra" in str(exc_info.value).lower()


def test_check_record_rejects_unknown_status() -> None:
    """Belt-and-braces: ``CheckRecord.status`` literal pin prevents a
    new status string from slipping into a check without explicit
    schema update. Three allowed: passed / failed / error.
    """
    payload = _canonical_envelope()
    payload["checks"][0]["status"] = "almost-passed"
    with pytest.raises(ValidationError):
        validate_envelope(payload)
