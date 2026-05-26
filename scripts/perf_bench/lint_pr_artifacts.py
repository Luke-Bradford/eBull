"""perf-claim-lint: gate every perf-claim PR against the §4 evidence contract.

Per-PR plan #1356. Spec: docs/proposals/etl/phase-0-instrumentation.md
§2.6 NEW-A.2 + master plan docs/proposals/etl/bootstrap-sub-1h-plan.md §4.

Trigger: PR labeled ``perf`` OR body contains ``## Performance impact`` header.

Validates (when triggered):
    * 3 artifacts under ``var/perf_baselines/<ticket>-<sha>.{txt,json,manifest.yaml}``
      where ``<ticket>`` is the sole ``Closes #<N>`` in the body and ``<sha>`` is
      ``GITHUB_PR_HEAD_SHA``
    * ``.txt`` first non-blank line starts with ``EXPLAIN (ANALYZE,``
    * ``.json`` schema (trials >= 3 each with numeric wall_ms,
      median_ms numeric, fingerprint.{pg_version,host,shared_buffers})
    * ``.manifest.yaml`` row counts for any referenced floored table meet
      ``scripts/perf_bench/floors.yaml`` minima
    * Body contains 3 sections: ``## Sibling-shape audit``,
      ``## Rollback criteria``, ``## Post-deploy SLO``

Gated bypass (requires ALL of):
    * PR label ``emergency``
    * Body section ``## Bypass justification`` containing ``Operator: <name>``
      and ``Reason: <text>`` (both non-empty)
    * Env ``PERF_CLAIM_LINT_BYPASS=true``

On full bypass: emit a GitHub ``::warning::`` annotation and exit 0.

Exit codes:
    0 — no claim detected, OR all checks pass, OR bypass fully gated
    1 — claim detected and any check failed
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Final, NoReturn

import yaml

from scripts.perf_bench._floors import load_floors

REPO_ROOT: Final[Path] = Path(__file__).parent.parent.parent
ARTIFACT_DIR: Final[Path] = REPO_ROOT / "var" / "perf_baselines"

PERF_LABEL: Final[str] = "perf"
EMERGENCY_LABEL: Final[str] = "emergency"
HEADER_PERF: Final[str] = "## Performance impact"
HEADER_BYPASS: Final[str] = "## Bypass justification"
REQUIRED_SECTIONS: Final[tuple[str, ...]] = (
    "## Sibling-shape audit",
    "## Rollback criteria",
    "## Post-deploy SLO",
)
EXPLAIN_PREFIX: Final[str] = "EXPLAIN (ANALYZE,"

CLOSES_RE: Final[re.Pattern[str]] = re.compile(r"^Closes\s+#(\d+)\s*$", re.MULTILINE)
OPERATOR_RE: Final[re.Pattern[str]] = re.compile(r"^Operator:\s*(\S.*)$", re.MULTILINE)
REASON_RE: Final[re.Pattern[str]] = re.compile(r"^Reason:\s*(\S.*)$", re.MULTILINE)
HTML_COMMENT_RE: Final[re.Pattern[str]] = re.compile(r"<!--.*?-->", re.DOTALL)


def _annotation_escape(text: str) -> str:
    """Escape characters that would corrupt a GitHub Actions annotation.

    Per the workflow-commands docs, ``%``, ``\\r``, ``\\n`` must be
    URL-encoded so a malicious bypass-justification ``Reason: ... %0A...``
    cannot inject a second annotation or break the parser. The bypass
    path is already triple-gated, but defence-in-depth is cheap.
    """
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _err(msg: str) -> NoReturn:
    print(f"::error::{_annotation_escape(msg)}", file=sys.stderr)
    sys.exit(1)


def _warn(msg: str) -> None:
    print(f"::warning::{_annotation_escape(msg)}", file=sys.stderr)


def _strip_html_comments(body: str) -> str:
    return HTML_COMMENT_RE.sub("", body)


def _parse_labels(raw: str) -> list[str]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError, TypeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, str)]


def _detect_claim(labels: list[str], body: str) -> bool:
    if PERF_LABEL in labels:
        return True
    return HEADER_PERF in body


def _slice_bypass_section(body: str) -> str | None:
    if HEADER_BYPASS not in body:
        return None
    after = body.split(HEADER_BYPASS, 1)[1]
    next_header = re.search(r"^## ", after, re.MULTILINE)
    if next_header is None:
        return after
    return after[: next_header.start()]


def _detect_bypass(labels: list[str], body: str, env_set: bool) -> tuple[bool, str | None]:
    """Return ``(fully_gated, warn_msg)``.

    ``fully_gated`` is True only when every gate condition holds.
    ``warn_msg`` is the operator-facing annotation text or None.
    """
    if not env_set:
        return False, None
    if EMERGENCY_LABEL not in labels:
        return False, None
    section = _slice_bypass_section(body)
    if section is None:
        return False, None
    op_match = OPERATOR_RE.search(section)
    reason_match = REASON_RE.search(section)
    if op_match is None or reason_match is None:
        return False, None
    operator = op_match.group(1).strip()
    reason = reason_match.group(1).strip()
    if not operator or not reason:
        return False, None
    return True, f"bypass-engaged by {operator}: {reason}"


def _resolve_ticket(body: str) -> int:
    stripped = _strip_html_comments(body)
    matches = CLOSES_RE.findall(stripped)
    if not matches:
        _err("no `Closes #<N>` line found in PR body")
    if len(matches) > 1:
        _err(f"multiple `Closes #<N>` lines in PR body: {matches}; expected exactly one")
    return int(matches[0])


def _validate_explain(txt_path: Path) -> None:
    first_non_blank: str | None = None
    for line in txt_path.read_text().splitlines():
        if line.strip():
            first_non_blank = line
            break
    if first_non_blank is None or not first_non_blank.startswith(EXPLAIN_PREFIX):
        _err(f"{txt_path.relative_to(REPO_ROOT)}: first non-blank line must start with {EXPLAIN_PREFIX!r}")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_json(json_path: Path) -> None:
    try:
        doc = json.loads(json_path.read_text())
    except json.JSONDecodeError as exc:
        _err(f"{json_path.relative_to(REPO_ROOT)}: invalid JSON: {exc}")
    if not isinstance(doc, dict):
        _err(f"{json_path.relative_to(REPO_ROOT)}: top-level must be an object")
    trials = doc.get("trials")
    if not isinstance(trials, list) or len(trials) < 3:
        _err(f"{json_path.relative_to(REPO_ROOT)}: 'trials' must be a list with >= 3 entries")
    for index, trial in enumerate(trials):
        if not isinstance(trial, dict) or not _is_number(trial.get("wall_ms")):
            _err(f"{json_path.relative_to(REPO_ROOT)}: trials[{index}] missing numeric 'wall_ms'")
    if not _is_number(doc.get("median_ms")):
        _err(f"{json_path.relative_to(REPO_ROOT)}: missing numeric 'median_ms'")
    fingerprint = doc.get("fingerprint")
    if not isinstance(fingerprint, dict):
        _err(f"{json_path.relative_to(REPO_ROOT)}: missing 'fingerprint' object")
    for key in ("pg_version", "host", "shared_buffers"):
        value = fingerprint.get(key)
        if not isinstance(value, str) or not value:
            _err(f"{json_path.relative_to(REPO_ROOT)}: fingerprint missing string '{key}'")


_MISSING: Final[object] = object()


def _validate_manifest(manifest_path: Path) -> None:
    rel = manifest_path.relative_to(REPO_ROOT)
    try:
        doc = yaml.safe_load(manifest_path.read_text())
    except yaml.YAMLError as exc:
        _err(f"{rel}: invalid YAML: {exc}")
    if not isinstance(doc, dict):
        _err(f"{rel}: top-level must be a mapping")
    row_counts = doc.get("row_counts")
    if not isinstance(row_counts, dict):
        _err(f"{rel}: missing 'row_counts' mapping")
    # ``target_table`` MUST be present (string or null). Otherwise an
    # empty ``row_counts: {}`` would silently satisfy the lint and
    # bypass the §4 floor-proof contract (Codex 2 IMPORTANT-1).
    target_table = doc.get("target_table", _MISSING)
    if target_table is _MISSING:
        _err(f"{rel}: missing required key 'target_table' (string or null)")
    if target_table is not None and not isinstance(target_table, str):
        _err(f"{rel}: 'target_table' must be string or null, got {target_table!r}")
    floors = load_floors()
    # If the perf claim names a floored table, the manifest MUST prove
    # it meets the floor.
    if isinstance(target_table, str) and target_table in floors:
        actual = row_counts.get(target_table)
        floor = floors[target_table]
        if actual is None:
            _err(
                f"{rel}: target_table '{target_table}' is floored but "
                f"absent from row_counts; harness must record COUNT(*)"
            )
        if not isinstance(actual, int) or isinstance(actual, bool) or actual < floor:
            _err(f"{rel}: target_table '{target_table}' row_count={actual!r} below floor {floor}")
    # Any other reported counts must also honour their floors.
    for table, actual in row_counts.items():
        if table not in floors or table == target_table:
            continue
        floor = floors[table]
        if not isinstance(actual, int) or isinstance(actual, bool) or actual < floor:
            _err(f"{rel}: '{table}' row_count={actual!r} below floor {floor}")


def _validate_artifacts(ticket: int, sha: str) -> None:
    base = ARTIFACT_DIR / f"{ticket}-{sha}"
    txt_path = base.with_suffix(".txt")
    json_path = base.with_suffix(".json")
    manifest_path = ARTIFACT_DIR / f"{ticket}-{sha}.manifest.yaml"

    missing = [p for p in (txt_path, json_path, manifest_path) if not p.exists()]
    if missing:
        rel = [str(p.relative_to(REPO_ROOT)) for p in missing]
        _err(f"missing perf artifacts: {rel}. Run `scripts/perf_bench/run_explain.sh {ticket}` on the bench DB.")

    _validate_explain(txt_path)
    _validate_json(json_path)
    _validate_manifest(manifest_path)


def _validate_sections(body: str) -> None:
    missing = [section for section in REQUIRED_SECTIONS if section not in body]
    if missing:
        _err("PR description missing required section(s): " + ", ".join(repr(s) for s in missing))


def main() -> int:
    body = os.environ.get("GITHUB_PR_BODY", "") or ""
    labels = _parse_labels(os.environ.get("GITHUB_PR_LABELS", "[]"))

    if not _detect_claim(labels, body):
        return 0

    bypass_env = os.environ.get("PERF_CLAIM_LINT_BYPASS", "").lower() == "true"
    fully_gated, warn_msg = _detect_bypass(labels, body, bypass_env)
    if fully_gated:
        if warn_msg:
            _warn(warn_msg)
        return 0
    if bypass_env:
        _err(
            "PERF_CLAIM_LINT_BYPASS=true set but bypass not fully gated. "
            "Required: label 'emergency' + body '## Bypass justification' "
            "section with 'Operator: <name>' + 'Reason: <text>' (both non-empty)."
        )

    sha = os.environ.get("GITHUB_PR_HEAD_SHA")
    if not sha:
        _err("GITHUB_PR_HEAD_SHA env var unset")

    ticket = _resolve_ticket(body)
    _validate_artifacts(ticket, sha)
    _validate_sections(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
