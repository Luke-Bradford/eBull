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

import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints

# CheckRecord.status accepts the three terminal verdicts AND the
# C6-emitted warning_category_quiescent_<category> sentinel from
# stream_a_stream_c_gate.py:181. Codex CTO BLOCKING (final committee
# 2026-05-24): the prior schema's Literal["passed", "failed", "error"]
# would have raised ValidationError on its own documented happy-path
# quiescence output. The pattern below allows any quiescent category
# name (alphanumeric + underscore) so adding a new category to
# _CATEGORIES at app/jobs/ownership_observations_repair.py doesn't
# require a parallel schema update.
_STATUS_PATTERN = r"^(passed|failed|error|warning_category_quiescent_[a-z0-9_]+)$"
_STATUS_RE = re.compile(_STATUS_PATTERN)

CheckStatus = Annotated[str, StringConstraints(pattern=_STATUS_PATTERN)]


class CheckRecord(BaseModel):
    """One per-check row inside :attr:`Envelope.checks`.

    Matches :func:`app.runbooks.stream_a_stream_c_gate._check_record`
    output exactly. Status accepts the three terminal verdicts
    (``passed`` / ``failed`` / ``error``) AND the C6-emitted
    ``warning_category_quiescent_<category>`` sentinel.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    status: CheckStatus
    count: int
    detail: str


class Envelope(BaseModel):
    """Top-level Stream-C gate envelope.

    **Exactly 9 top-level keys** matching the emitted JSONL at
    ``app/runbooks/stream_a_stream_c_gate.py:438`` (validated AFTER
    ``exit_code`` is appended per Codex CTO BLOCKING 2026-05-24):

    1. ``schema_version`` — `int`, pinned to 1.
    2. ``runbook`` — `str`, pinned to ``"stream_a_stream_c_gate"``.
    3. ``bootstrap_run_id`` — `int`, FK to ``bootstrap_runs.id``.
    4. ``started_at`` — ISO-8601 UTC timestamp.
    5. ``ended_at`` — ISO-8601 UTC timestamp.
    6. ``checks`` — list of :class:`CheckRecord`.
    7. ``accepted`` — `bool`, overall verdict.
    8. ``first_failed`` — `str | None`, check ``id`` of the first
       failure or ``None`` if all passed.
    9. ``exit_code`` — `int` (0 or 1), runbook process exit code.

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
    exit_code: int


def validate_envelope(payload: dict[str, object]) -> Envelope:
    """Validate a payload dict against :class:`Envelope` and return the
    parsed model.

    Raises :class:`pydantic.ValidationError` on shape mismatch. Caller
    (the runbook) does NOT need to catch — pydantic errors propagate as
    ``ValueError`` subclasses, which the runbook's outer ``try/except``
    surfaces with exit code 1 (gate-side failure) per
    ``stream_a_run_8_verify.py`` exit-code conventions.

    NOTE: caller MUST add ``exit_code`` to the payload BEFORE calling
    this function. Validating a pre-``exit_code`` payload (the v1
    pattern) fails the ``extra='forbid'`` clause; validating a payload
    that omits ``exit_code`` fails the required-field clause. Both fail
    at the same place (here) so the contract is enforced exactly once.
    """
    return Envelope.model_validate(payload)


__all__ = ["CheckRecord", "CheckStatus", "Envelope", "validate_envelope"]
