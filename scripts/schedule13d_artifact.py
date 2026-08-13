"""Auditable, bounded artifact envelope for the sealed Schedule 13D study."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from scripts.evaluate_2582_schedule13d_outcomes import (
    FIRST_SOURCE_DATE,
    LAST_COMPLETE_FILING_DATE,
    RESEARCH_VENDOR,
    OutcomeGate,
)
from scripts.schedule13d_report import HistoricalFalsificationReport

#: ⚠ v2 (#2614) — the envelope now carries the #2599 declaration and the access
#: row that authorised the look. Bumped rather than added silently because a
#: consumer reading a v1 artifact cannot tell "no declaration existed" from "this
#: schema does not record one". No migration is owed: C-4 has never run, so no v1
#: artifact exists.
ARTIFACT_SCHEMA_VERSION: Final = "schedule13d-historical-falsification-artifact-v2"
IMPLEMENTATION_FILES: Final = (
    "scripts/evaluate_2582_schedule13d_outcomes.py",
    "scripts/schedule13d_challengers.py",
    "scripts/schedule13d_statistics.py",
    "scripts/schedule13d_report.py",
    "scripts/schedule13d_orchestrator.py",
    "scripts/schedule13d_artifact.py",
    "scripts/run_2582_schedule13d_outcomes.py",
)


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def implementation_sha256(root: Path = Path(".")) -> str:
    digest = hashlib.sha256()
    for relative in IMPLEMENTATION_FILES:
        source = root / relative
        payload = source.read_bytes()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(str(len(payload)).encode())
        digest.update(b"\0")
        digest.update(payload)
    return digest.hexdigest()


@dataclass(frozen=True)
class HistoricalFalsificationArtifact:
    artifact_schema_version: str
    generated_at: str
    trial_id: str
    trial_register_version: str
    contract_sha256: str
    #: #2614 — which frozen declaration authorised this study, and which access
    #: row recorded the look. An artifact that cannot name its own authorisation
    #: leaves an auditor to take the study's word for it.
    declaration_id: int
    access_id: int
    implementation_sha256: str
    source_first_date: str
    source_last_complete_filing_date: str
    research_vendor: str
    decision: str
    report_sha256: str
    report: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), allow_nan=False, indent=2, sort_keys=True) + "\n"


def build_artifact(
    gate: OutcomeGate,
    report: HistoricalFalsificationReport,
    *,
    root: Path = Path("."),
    generated_at: datetime | None = None,
) -> HistoricalFalsificationArtifact:
    timestamp = generated_at or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("artifact timestamp must be timezone-aware")
    report_payload = report.as_dict()
    return HistoricalFalsificationArtifact(
        artifact_schema_version=ARTIFACT_SCHEMA_VERSION,
        generated_at=timestamp.astimezone(UTC).isoformat(),
        trial_id=gate.trial_id,
        trial_register_version=gate.trial_register_version,
        contract_sha256=gate.contract_sha256,
        declaration_id=gate.declaration_id,
        access_id=gate.access_id,
        implementation_sha256=implementation_sha256(root),
        source_first_date=FIRST_SOURCE_DATE.isoformat(),
        source_last_complete_filing_date=LAST_COMPLETE_FILING_DATE.isoformat(),
        research_vendor=RESEARCH_VENDOR,
        decision=report.decision,
        report_sha256=canonical_sha256(report_payload),
        report=report_payload,
    )


def verify_artifact(artifact: HistoricalFalsificationArtifact, *, root: Path = Path(".")) -> None:
    if artifact.artifact_schema_version != ARTIFACT_SCHEMA_VERSION:
        raise ValueError("unknown Schedule 13D artifact schema")
    if artifact.decision != artifact.report.get("decision"):
        raise ValueError("artifact decision does not match report")
    if canonical_sha256(artifact.report) != artifact.report_sha256:
        raise ValueError("artifact report digest mismatch")
    if implementation_sha256(root) != artifact.implementation_sha256:
        raise ValueError("artifact implementation digest does not match this checkout")


__all__ = [
    "ARTIFACT_SCHEMA_VERSION",
    "HistoricalFalsificationArtifact",
    "build_artifact",
    "canonical_sha256",
    "implementation_sha256",
    "verify_artifact",
]
