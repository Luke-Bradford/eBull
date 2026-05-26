"""Self-tests for ``scripts/perf_bench/lint_pr_artifacts.py``.

Scenarios mirror the per-PR plan #1356 acceptance grid:

1.  clean-no-claim → exit 0
2.  label-only-missing-artifact → exit non-zero
3.  header-only-missing-artifact → exit non-zero
4.  floor-fail → exit non-zero
5.  explain-shape-fail → exit non-zero
6.  json-schema-fail → exit non-zero
7.  bypass-no-label → exit non-zero
8.  bypass-no-justification-section → exit non-zero
9.  bypass-no-operator-regex → exit non-zero
10. bypass-fully-gated → exit 0 with ``::warning::``
11. floors-yaml-malformed → exit non-zero
12. closes-multi-match → exit non-zero
13. sha-mismatch → exit non-zero
14. invariant: floors.yaml keys + values match master plan §4 table
    (skipped until the master plan is committed)
15. manifest-target-table-missing → exit non-zero (Codex 2 IMPORTANT-1)
16. manifest-floored-target-absent-from-row-counts → exit non-zero
17. manifest-target-table-null-empty-row-counts-ok → exit 0
18. header-in-backticks-is-not-a-claim → exit 0 (CI iter-1 regression)
19. header-on-own-line-does-trigger → exit non-zero (sanity)
20. workflow-perf-claim-lint-has-same-repo-guard → fork-pwn lint (iter-2)
21. bypass-html-comment-trailing-header-does-not-misslice (iter-2 WARNING)

The lint is invoked via subprocess to match CI's invocation path (so we
catch import-time failures that ``import + call main()`` would mask).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Final

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LINT_MODULE = "scripts.perf_bench.lint_pr_artifacts"
ARTIFACT_DIR = REPO_ROOT / "var" / "perf_baselines"
MASTER_PLAN = REPO_ROOT / "docs" / "proposals" / "etl" / "bootstrap-sub-1h-plan.md"
REAL_FLOORS_YAML = REPO_ROOT / "scripts" / "perf_bench" / "floors.yaml"

VALID_SECTIONS = (
    "## Sibling-shape audit",
    "## Rollback criteria",
    "## Post-deploy SLO",
)


def _run_lint(env_overrides: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Invoke lint via subprocess with a controlled env."""
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
    }
    # Allow the floors-yaml override to flow through for the malformed-yaml test.
    for inheritable in ("PERF_BENCH_FLOORS_YAML_OVERRIDE",):
        if inheritable in os.environ:
            env[inheritable] = os.environ[inheritable]
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-m", LINT_MODULE],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


def _write_artifacts(
    ticket: int,
    sha: str,
    *,
    explain_bad: bool = False,
    json_bad: bool = False,
    floor_bad: bool = False,
    target_table: str | None = "ownership_institutions_current",
    omit_target_row_count: bool = False,
) -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    base = ARTIFACT_DIR / f"{ticket}-{sha}"
    if explain_bad:
        base.with_suffix(".txt").write_text("Plan rows=...\n")
    else:
        base.with_suffix(".txt").write_text("EXPLAIN (ANALYZE, BUFFERS, COSTS, FORMAT TEXT)\nSeq Scan ...\n")
    if json_bad:
        # Only one trial — fails the ``>= 3 entries`` assertion.
        doc = {
            "trials": [{"wall_ms": 100.0}],
            "median_ms": 100.0,
            "fingerprint": {
                "pg_version": "17.0",
                "host": "h",
                "shared_buffers": "1GB",
            },
        }
    else:
        doc = {
            "trials": [
                {"wall_ms": 100.0},
                {"wall_ms": 110.0},
                {"wall_ms": 105.0},
            ],
            "median_ms": 105.0,
            "fingerprint": {
                "pg_version": "17.0",
                "host": "h",
                "shared_buffers": "1GB",
            },
        }
    base.with_suffix(".json").write_text(json.dumps(doc))
    row_counts: dict[str, int] = {}
    if isinstance(target_table, str) and not omit_target_row_count:
        row_counts[target_table] = 500_000 if floor_bad else 1_500_000
    (ARTIFACT_DIR / f"{ticket}-{sha}.manifest.yaml").write_text(
        yaml.safe_dump(
            {
                "fixture_label": "bench-dev",
                "target_table": target_table,
                "row_counts": row_counts,
            }
        )
    )


def _body(
    *,
    sections: tuple[str, ...] = VALID_SECTIONS,
    closes: int | None = 8_000_000,
    perf_header: bool = False,
    bypass_section: str | None = None,
) -> str:
    lines: list[str] = []
    if closes is not None:
        lines.extend([f"Closes #{closes}", ""])
    for section in sections:
        lines.extend([section, "details", ""])
    if perf_header:
        lines.extend(["## Performance impact", "claim body", ""])
    if bypass_section is not None:
        lines.extend(["## Bypass justification", bypass_section, ""])
    return "\n".join(lines)


# Test-only ticket-number range. Real eBull ticket numbers are <100_000 today
# and the planning horizon never grows past 8 digits; using an 8M+ prefix means
# cleanup cannot collide with a real ticket's artifact (Codex 2 IMPORTANT-2).
TEST_TICKET_PREFIX: Final = "8000"


@pytest.fixture(autouse=True)
def _cleanup_test_artifacts() -> Iterator[None]:
    """Remove only artifacts written by this test module.

    Tests use 7-digit ticket IDs starting with ``8000`` (e.g. ``8000004``)
    so the purge cannot delete any artifact whose ticket number a real
    GitHub issue could ever take. Cleanup runs before AND after each
    test to defend against a prior crashed run.
    """

    def purge() -> None:
        if not ARTIFACT_DIR.exists():
            return
        for entry in ARTIFACT_DIR.iterdir():
            if entry.name.startswith(TEST_TICKET_PREFIX):
                entry.unlink()

    purge()
    yield
    purge()


def test_1_clean_no_claim() -> None:
    result = _run_lint({})
    assert result.returncode == 0, result.stderr


def test_2_label_only_missing_artifact() -> None:
    body = _body()
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
        }
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "missing perf artifacts" in result.stderr


def test_3_header_only_missing_artifact() -> None:
    body = _body(perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": "[]",
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
        }
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "missing perf artifacts" in result.stderr


def test_4_floor_fail() -> None:
    sha = "sha4"
    _write_artifacts(8_000_004, sha, floor_bad=True)
    body = _body(closes=8_000_004, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
        }
    )
    assert result.returncode == 1
    assert "below floor" in result.stderr


def test_5_explain_shape_fail() -> None:
    sha = "sha5"
    _write_artifacts(8_000_005, sha, explain_bad=True)
    body = _body(closes=8_000_005, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
        }
    )
    assert result.returncode == 1
    assert "EXPLAIN (ANALYZE," in result.stderr


def test_6_json_schema_fail() -> None:
    sha = "sha6"
    _write_artifacts(8_000_006, sha, json_bad=True)
    body = _body(closes=8_000_006, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
        }
    )
    assert result.returncode == 1
    assert ">= 3 entries" in result.stderr


def test_7_bypass_no_label() -> None:
    body = _body(perf_header=True, bypass_section="Operator: alice\nReason: outage")
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
            "PERF_CLAIM_LINT_BYPASS": "true",
        }
    )
    assert result.returncode == 1
    assert "bypass not fully gated" in result.stderr


def test_8_bypass_no_justification_section() -> None:
    body = _body(perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf", "emergency"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
            "PERF_CLAIM_LINT_BYPASS": "true",
        }
    )
    assert result.returncode == 1
    assert "bypass not fully gated" in result.stderr


def test_9_bypass_no_operator_regex() -> None:
    body = _body(perf_header=True, bypass_section="Reason: outage")
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf", "emergency"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
            "PERF_CLAIM_LINT_BYPASS": "true",
        }
    )
    assert result.returncode == 1
    assert "bypass not fully gated" in result.stderr


def test_10_bypass_fully_gated() -> None:
    body = _body(perf_header=True, bypass_section="Operator: alice\nReason: outage")
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf", "emergency"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
            "PERF_CLAIM_LINT_BYPASS": "true",
        }
    )
    assert result.returncode == 0, result.stderr
    assert "::warning::bypass-engaged by alice" in result.stderr


def test_11_floors_yaml_malformed(tmp_path: Path) -> None:
    bad = tmp_path / "floors.yaml"
    bad.write_text(":\n:\nbad\n")
    sha = "sha11"
    _write_artifacts(8_000_011, sha)
    body = _body(closes=8_000_011, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
            "PERF_BENCH_FLOORS_YAML_OVERRIDE": str(bad),
        }
    )
    assert result.returncode != 0


def test_12_closes_multi_match() -> None:
    body = "Closes #100\nCloses #200\n\n## Performance impact\nclaim\n\n" + "\n".join(VALID_SECTIONS)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
        }
    )
    assert result.returncode == 1
    assert "multiple `Closes" in result.stderr


def test_13_sha_mismatch() -> None:
    _write_artifacts(8_000_013, "actual-sha")
    body = _body(closes=8_000_013, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "other-sha",
        }
    )
    assert result.returncode == 1
    assert "missing perf artifacts" in result.stderr


def test_14_floors_yaml_matches_master_plan() -> None:
    """floors.yaml is the SoT — assert the master-plan table mirrors it.

    Skipped when the master plan is not yet committed: at the time of this
    PR the doc is staged separately (per session hand-over) and CI cannot
    diff against it. The invariant activates automatically once the master
    plan lands on main.
    """
    if not MASTER_PLAN.exists():
        pytest.skip(
            f"master plan not yet committed: {MASTER_PLAN.relative_to(REPO_ROOT)} — invariant activates when it lands"
        )

    sys.path.insert(0, str(REPO_ROOT))
    try:
        from scripts.perf_bench._floors import load_floors
    finally:
        sys.path.pop(0)

    floors = load_floors()
    plan_text = MASTER_PLAN.read_text()
    table_pattern = re.compile(r"^\|\s*`([a-z_]+)`\s*\|\s*≥\s*([\d,]+)\s*\|", re.MULTILINE)
    matches = table_pattern.findall(plan_text)
    assert matches, "master plan §4 row-count-floors table not found"
    plan_floors = {table: int(num.replace(",", "")) for table, num in matches}
    assert floors == plan_floors, f"floors drift: floors.yaml={floors!r} vs master plan §4={plan_floors!r}"


def test_15_manifest_target_table_missing() -> None:
    """Empty ``row_counts: {}`` with no ``target_table`` key MUST fail.

    Without this guard a perf PR could ship a manifest that reports zero
    floored tables and silently bypass §4 (Codex 2 IMPORTANT-1).
    """
    sha = "sha15"
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    base = ARTIFACT_DIR / f"8000015-{sha}"
    base.with_suffix(".txt").write_text("EXPLAIN (ANALYZE, BUFFERS, COSTS, FORMAT TEXT)\nSeq Scan ...\n")
    base.with_suffix(".json").write_text(
        json.dumps(
            {
                "trials": [{"wall_ms": 1.0}, {"wall_ms": 1.0}, {"wall_ms": 1.0}],
                "median_ms": 1.0,
                "fingerprint": {"pg_version": "17", "host": "h", "shared_buffers": "1GB"},
            }
        )
    )
    (ARTIFACT_DIR / f"8000015-{sha}.manifest.yaml").write_text(
        yaml.safe_dump({"fixture_label": "bench-dev", "row_counts": {}})
    )
    body = _body(closes=8_000_015, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
        }
    )
    assert result.returncode == 1
    assert "target_table" in result.stderr


def test_16_manifest_floored_target_absent_from_row_counts() -> None:
    """A floored ``target_table`` MUST be present in ``row_counts``."""
    sha = "sha16"
    _write_artifacts(8_000_016, sha, omit_target_row_count=True)
    body = _body(closes=8_000_016, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
        }
    )
    assert result.returncode == 1
    assert "absent from row_counts" in result.stderr


def test_18_header_in_backticks_is_not_a_claim() -> None:
    """Regression: PR body that *documents* the perf-claim pattern in
    backticks (e.g. this PR's own description) MUST NOT trigger detection.

    First-iteration of this PR failed CI on its own description because
    the detection was a substring match. Detection is now line-exact;
    a backticked occurrence inside a bullet must NOT count.
    """
    body = (
        "Closes #8000018\n\n"
        "## Summary\n\n"
        "claim detection (PR label `perf` OR body `## Performance impact` header)\n\n"
        "- PR-template `## Performance impact` boilerplate — owned by NEW-B\n"
    )
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": "[]",
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
        }
    )
    assert result.returncode == 0, result.stderr


def test_20_workflow_perf_claim_lint_has_same_repo_guard() -> None:
    """Bot review iter-2 PREVENTION: any CI job that checks out
    ``pull_request.head.sha`` AND executes the checked-out code MUST
    also carry an ``if:`` guard restricting to the operator's own
    repository. Without it a fork-PR contributor's Python runs in CI
    with access to repo variables.
    """
    ci_yml = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    job_idx = ci_yml.find("perf-claim-lint:")
    assert job_idx >= 0, "perf-claim-lint job missing from ci.yml"
    # Slice job body (up to next top-level job or EOF).
    next_job = re.search(r"^  [a-zA-Z][\w-]*:\s*$", ci_yml[job_idx + 1 :], re.MULTILINE)
    job_body = ci_yml[job_idx : job_idx + 1 + next_job.start()] if next_job else ci_yml[job_idx:]
    assert "github.event.pull_request.head.repo.full_name == github.repository" in job_body, (
        "perf-claim-lint job missing same-repo head.sha guard "
        "(see docs/review-prevention-log.md 'fork-pwn via pull_request + head-SHA checkout')"
    )


def test_22_harness_table_ident_regex_rejects_sql_fragments() -> None:
    """Bot review iter-3 WARNING regression: ``_row_count`` shells out to
    ``psql -c "SELECT COUNT(*) FROM {table}"`` so ``table`` is interpolated
    unquoted. ``TABLE_IDENT_RE`` MUST reject anything that does not match
    a strict lowercase Postgres identifier shape so a malformed value
    cannot reach the command line.
    """
    sys.path.insert(0, str(REPO_ROOT))
    try:
        from scripts.perf_bench._run_explain import TABLE_IDENT_RE
    finally:
        sys.path.pop(0)

    valid = [
        "ownership_institutions_current",
        "financial_facts_raw",
        "x",
        "table_1",
        "_underscore_first",
    ]
    for name in valid:
        assert TABLE_IDENT_RE.fullmatch(name), f"valid identifier rejected: {name!r}"

    malicious = [
        "x; DROP TABLE financial_facts_raw",
        "x' OR 1=1 --",
        "x.y",
        "x; SELECT pg_sleep(60)",
        "X_UPPERCASE",
        "1leading_digit",
        "table-with-dash",
        'x" --',
        '"quoted_ident"',
        "x\nDROP",
        "",
    ]
    for name in malicious:
        assert not TABLE_IDENT_RE.fullmatch(name), f"malicious identifier accepted: {name!r}"


def test_21_bypass_html_comment_trailing_header_does_not_misslice() -> None:
    """Bot review iter-2 WARNING regression: a bypass-justification
    header line with a trailing HTML comment (``## Bypass justification<!-- x -->``)
    must still slice the section correctly. Previously ``_has_header_line``
    operated on the stripped body while ``split`` operated on the raw
    body, opening a divergence between guard + slice.
    """
    body = (
        "Closes #8000021\n\n"
        "## Performance impact\n\n"
        "claim\n\n" + "\n".join(VALID_SECTIONS) + "\n\n"
        "## Bypass justification<!-- trailing comment -->\n"
        "Operator: alice\n"
        "Reason: legit\n"
    )
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf", "emergency"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
            "PERF_CLAIM_LINT_BYPASS": "true",
        }
    )
    # Bypass fully gated → exit 0 with warning annotation.
    assert result.returncode == 0, result.stderr
    assert "::warning::bypass-engaged by alice" in result.stderr


def test_19_header_on_own_line_does_trigger() -> None:
    """Sanity: same string on its own line DOES trigger detection."""
    body = "Closes #8000019\n\n## Performance impact\n\nclaim text here\n\n" + "\n".join(VALID_SECTIONS)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": "[]",
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": "deadbeef",
        }
    )
    # No artifacts → fails. Confirms detection fired.
    assert result.returncode == 1
    assert "missing perf artifacts" in result.stderr


def test_17_manifest_target_table_null_empty_row_counts_ok() -> None:
    """Non-floor perf claims (target_table=null) MAY have empty row_counts."""
    sha = "sha17"
    _write_artifacts(8_000_017, sha, target_table=None)
    body = _body(closes=8_000_017, perf_header=True)
    result = _run_lint(
        {
            "GITHUB_PR_LABELS": json.dumps(["perf"]),
            "GITHUB_PR_BODY": body,
            "GITHUB_PR_HEAD_SHA": sha,
        }
    )
    assert result.returncode == 0, result.stderr
