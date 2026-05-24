"""Pydantic envelope schema for the Stream-C acceptance gate JSONL output.

Pins the exact shape the operator posts to #1233 as the bootstrap-run
attestation. Drift in shape (renamed key, added field, removed field,
wrong type) is rejected at emit-time so a malformed envelope CAN NOT
reach the operator's comment.

The envelope is currently emitted at
:func:`app.runbooks.stream_a_stream_c_gate._build_envelope` (formerly
inline). The runbook calls :func:`validate_envelope` immediately before
printing to stdout + writing the JSONL log file.

History: caught by 3 lenses (API B4 + Codex B1 + Test B2) in the
Stream A ETL-sweep 8-lens committee review (2026-05-24). Run-#8-readiness
fixes Item 4 (spec at ``docs/proposals/etl/run-8-readiness-fixes.md``)
folded the finding into an app-layer validator after Codex 1 round 1
caught that ``bootstrap_runs.stream_c_gate_status`` is a TEXT column
(holds lifecycle state only) — a JSONB CHECK is impossible by column
type. Codex 1 diff re-pass (round 2) re-pinned the model to the exact
8-key emitter shape.

Model uses ``extra='forbid'`` so any new field added to the runbook
emitter without a parallel schema update fails the canonical-path
positive test immediately, blocking the commit.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class CheckRecord(BaseModel):
    """One per-check row inside :attr:`Envelope.checks`.

    Matches :func:`app.runbooks.stream_a_stream_c_gate._check_record`
    output exactly. Status enum pinned to the three values the runbook
    actually emits (``passed`` / ``failed`` / ``error``).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    status: Literal["passed", "failed", "error"]
    count: int
    detail: str


class Envelope(BaseModel):
    """Top-level Stream-C gate envelope.

    **Exactly 8 top-level keys** per current emitter at
    ``app/runbooks/stream_a_stream_c_gate.py:325-334``:

    1. ``schema_version`` — `int`, pinned to 1.
    2. ``runbook`` — `str`, pinned to ``"stream_a_stream_c_gate"``.
    3. ``bootstrap_run_id`` — `int`, FK to ``bootstrap_runs.id``.
    4. ``started_at`` — ISO-8601 UTC timestamp.
    5. ``ended_at`` — ISO-8601 UTC timestamp.
    6. ``checks`` — list of :class:`CheckRecord`.
    7. ``accepted`` — `bool`, overall verdict (NOT ``verdict``).
    8. ``first_failed`` — `str | None`, check ``id`` of the first
       failure or ``None`` if all passed.

    ``extra='forbid'`` rejects unknown top-level keys — prevents silent
    shape drift via accidental new fields.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    runbook: Literal["stream_a_stream_c_gate"]
    bootstrap_run_id: int
    started_at: str
    ended_at: str
    checks: list[CheckRecord]
    accepted: bool
    first_failed: str | None


def validate_envelope(payload: dict[str, object]) -> Envelope:
    """Validate a payload dict against :class:`Envelope` and return the
    parsed model.

    Raises :class:`pydantic.ValidationError` on shape mismatch. Caller
    (the runbook) does NOT need to catch — pydantic errors propagate as
    ``ValueError`` subclasses, which the runbook's outer ``try/except``
    surfaces with exit code 1 (gate-side failure) per
    ``stream_a_run_8_verify.py`` exit-code conventions.
    """
    return Envelope.model_validate(payload)


__all__ = ["CheckRecord", "Envelope", "validate_envelope"]
