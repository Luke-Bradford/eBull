"""#3515 slice 4 — freeze the AI-discretionary-fund-v1 declaration (fund-v1 spec §0, §6, §7, §9).

Three phases, so a paid call is never spent on a freeze that would be refused anyway:

1. **Precheck** — ``freeze_trial`` under ``FUND_TERMS`` with no measurements (rolled back). Any
   refusal other than the two only the measurements clear (``budget_fixture_not_run``,
   ``measurements_missing``) stops here: no model call. While v1 runs, this is where it stops
   (``v1_not_wound_down:...``). ``--measure`` forces the next phase anyway (paid).
2. **Measure, in this process** — the §6 probe walk (``run_fixture``: production ``invoke_model``,
   fund system prompt, decision schema, asserted model; about $7 a probe), then §9's coverage
   script and §6's real-prompt-size script with their output captured. A walk refusal
   (``prompt_budget_exceeded`` / ``fixture_probe_nonmonotone``) is the freeze's refusal; the whole
   walk prints, for posting on #3515.
3. **Freeze** — ``freeze_trial`` again with the measurements, after re-reading provenance
   (``provenance_changed`` if HEAD moved during the walk).

Dry run (default) rolls back:

    PYTHONPATH=. uv run python -m scripts.ai_trial_fund_freeze [--measure] [--json out.json]

⚠ ``--apply`` STARTS fund-v1 — its decision job reads ``active`` at its next fire. Supervisor only,
from ``~/Dev/eBull`` at ``origin/main``, once v1 is wound down and fund-v1's two paper deployments
($3,000 each, O9 policy parity) exist:

    PYTHONPATH=. uv run python -m scripts.ai_trial_fund_freeze --apply --declared-by <who> \\
        --wake-evidence <url> --expect-config-sha256 <sha from the dry run>

Every apply re-runs the walk and the measurements, so the committed document is never byte-equal to
a dry run's; on a lost commit, ``already_frozen`` on a dry run is the answer, not the document sha.

Exit status: 0 when the freeze would succeed (dry run) or did (apply), 1 on any refusal.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import io
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.ai_trial_freeze import FreezeReport, Provenance, TrialFreezeError, freeze_trial, read_provenance
from app.services.ai_trial_fund_freeze import FUND_SPEC_PATH, FUND_TERMS, MEASUREMENT_REFUSALS

#: The walk summary's keys the document keeps (``run_fixture``); probes are kept whole, cost as text.
_FIXTURE_KEYS = (
    "n",
    "n0",
    "bytes_per_name",
    "start_base_tokens",
    "start_bytes_per_token",
    "attempt",
    "pack_sha256",
    "rendered_prompt_sha256",
    "rendered_prompt_bytes",
    "input_tokens",
    "cli_version",
    "model_id",
    "system_prompt_sha256",
    "decision_schema_sha256",
)
_REAL_BYTES = re.compile(r"^fund-v1 rendered user prompt bytes: ([0-9,]+)$")


def budget_fixture(summary: Mapping[str, Any]) -> dict[str, Any]:
    """The document's ``budget_fixture`` from one passing walk: its figures and every probe."""
    fixture = {key: summary.get(key) for key in _FIXTURE_KEYS}
    fixture["probes"] = [
        {**probe, "cost_usd": None if probe.get("cost_usd") is None else str(probe["cost_usd"])}
        for probe in summary.get("probes", [])
    ]
    return fixture


def capture(main: Callable[[], object], command: str) -> tuple[dict[str, Any] | None, str | None]:
    """Run a measurement script's ``main`` with stdout captured. Any exception, ``SystemExit``
    included, or a return other than 0/``None`` is ``measurement_failed:<command>``."""
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            status = main()
    except (Exception, SystemExit) as exc:
        print(out.getvalue(), file=sys.stderr)
        return None, f"measurement_failed:{command}:{type(exc).__name__}"
    if status not in (0, None):
        print(out.getvalue(), file=sys.stderr)
        return None, f"measurement_failed:{command}:exit_{status}"
    return {"command": command, "output_lines": out.getvalue().splitlines()}, None


def real_prompt_bytes(prompt_budget: Mapping[str, Any]) -> int | None:
    """The grouped fund-v1 rendered bytes of the real pack, from exactly one output line, or ``None``."""
    found = [m for line in prompt_budget["output_lines"] if (m := _REAL_BYTES.match(line))]
    return int(found[0].group(1).replace(",", "")) if len(found) == 1 else None


@dataclasses.dataclass(frozen=True)
class Steps:
    """The side effects, injectable for tests."""

    connect: Callable[[], Any]
    provenance: Callable[[], Provenance]
    walk: Callable[[], dict[str, Any]]
    coverage: Callable[[], object]
    prompt_budget: Callable[[], object]


def run(
    steps: Steps,
    *,
    apply: bool,
    measure: bool,
    declared_by: str = "",
    wake_evidence: str = "",
    expect_config_sha256: str | None = None,
) -> tuple[FreezeReport, dict[str, Any] | None]:
    """The three phases. Returns the freeze report and the walk summary (``None`` when no walk ran)."""
    provenance = steps.provenance()
    common = {
        "apply": apply,
        "declared_by": declared_by,
        "wake_evidence": wake_evidence,
        "expect_config_sha256": expect_config_sha256,
        "terms": FUND_TERMS,
    }
    with steps.connect() as conn:
        pre = freeze_trial(conn, provenance=provenance, outside={}, **{**common, "apply": False})
    # The precheck runs as a dry run but must still refuse the apply-only gaps before any spend.
    apply_gaps = [
        code
        for code, missing in (
            ("declared_by_missing", apply and not declared_by.strip()),
            ("wake_evidence_missing", apply and not wake_evidence.strip()),
            ("config_sha_unconfirmed", apply and expect_config_sha256 is None),
            (
                "config_changed",
                apply and expect_config_sha256 is not None and expect_config_sha256 != pre.config_sha256,
            ),
        )
        if missing
    ]
    blocking = [code for code in pre.refusals if code not in MEASUREMENT_REFUSALS] + apply_gaps
    if blocking and not measure:
        return dataclasses.replace(pre, refusals=tuple(pre.refusals) + tuple(apply_gaps)), None

    summary = steps.walk()
    refusals: list[str] = []
    if summary.get("freeze_refusal") is not None or summary.get("n") is None:
        refusals.append(str(summary.get("freeze_refusal") or "budget_fixture_not_run"))
    coverage, failed_coverage = capture(steps.coverage, "scripts.measure_3515_fund_pack_coverage")
    prompt_budget, failed_budget = capture(steps.prompt_budget, "scripts.measure_3515_prompt_budget")
    refusals += [r for r in (failed_coverage, failed_budget) if r is not None]
    outside: dict[str, Any] = {}
    if not refusals:
        assert coverage is not None and prompt_budget is not None
        real = real_prompt_bytes(prompt_budget)
        if real is None:
            refusals.append("measurement_failed:scripts.measure_3515_prompt_budget:no_real_bytes_line")
        else:
            fixture = budget_fixture(summary)
            outside = {
                "budget_fixture": fixture,
                "measurements": {
                    "coverage": coverage,
                    "prompt_budget": prompt_budget,
                    # §6: the fixture's bytes against the real pack's (grouped encoding).
                    "real_prompt_bytes": real,
                    "fixture_prompt_bytes": fixture["rendered_prompt_bytes"],
                },
            }
    again = steps.provenance()
    if again.code_git_sha != provenance.code_git_sha:
        refusals.append("provenance_changed")
    with steps.connect() as conn:
        report = freeze_trial(conn, provenance=again, outside=outside, **common)
    return dataclasses.replace(report, refusals=tuple(report.refusals) + tuple(refusals)), summary


def render(report: FreezeReport, summary: Mapping[str, Any] | None) -> str:
    mode = "APPLIED — fund-v1 is active" if report.applied else "dry run (rolled back)"
    lines = [
        f"AI-discretionary-fund-v1 freeze: {mode}",
        f"document sha256: {report.doc_sha256}  code: {report.doc['code_git_sha']}",
        f"policy_hash: {report.doc['policy_hash']}",
        f"config_sha256: {report.config_sha256}",
    ]
    for strategy_id, leg in sorted(report.config.items()):
        lines.append(f"  {strategy_id}: {json.dumps(leg, sort_keys=True, default=str)}")
    if "v1_hashed_module_parity" in report.doc:
        lines.append(f"v1 hashed-module parity: {json.dumps(report.doc['v1_hashed_module_parity'], sort_keys=True)}")
    if summary is None:
        lines.append("probe walk: not run (the precheck refused; --measure forces it)")
    else:
        lines.append("probe walk:\n" + json.dumps(summary, indent=2, sort_keys=True, default=str))
    measurements = report.doc.get("measurements")
    if isinstance(measurements, Mapping):
        lines.append(
            f"fixture bytes {measurements['fixture_prompt_bytes']:,} vs real pack {measurements['real_prompt_bytes']:,}"
        )
    if report.existing_doc_sha256 is not None:
        lines.append(
            f"already frozen (stored document {report.existing_doc_sha256}); a re-measured document never "
            "equals it, so `already_frozen` itself is the answer"
        )
    lines.append(f"refusals: {list(report.refusals) or 'none'}")
    if report.declaration_id is not None:
        lines.append(f"declaration_id: {report.declaration_id}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="freeze and START fund-v1 (supervisor only)")
    parser.add_argument("--measure", action="store_true", help="run the paid walk even if the precheck refuses")
    parser.add_argument("--declared-by", default="")
    parser.add_argument("--wake-evidence", default="", help="URL of the supervisor's go-live answer")
    parser.add_argument("--expect-config-sha256", default=None, help="config_sha256 printed by the dry run")
    parser.add_argument("--claude-bin", help="absolute path to the claude CLI (default: resolved from PATH)")
    parser.add_argument("--no-fetch", action="store_true", help="skip `git fetch origin main` (tests only)")
    parser.add_argument("--json", type=Path, default=None, help="also write the report as JSON")
    args = parser.parse_args(argv)

    # Imported here: the measurement scripts pull in the broker and fixture modules only when used.
    from scripts import measure_3515_fund_pack_coverage, measure_3515_prompt_budget
    from scripts.ai_trial_fund_budget_fixture import run_fixture
    from scripts.ai_trial_synthetic import _resolve_executable

    steps = Steps(
        connect=lambda: psycopg.connect(settings.database_url),
        provenance=lambda: read_provenance(fetch=not args.no_fetch, spec_path=FUND_SPEC_PATH),
        walk=lambda: run_fixture(executable=_resolve_executable(args.claude_bin), source_env=os.environ),
        coverage=measure_3515_fund_pack_coverage.main,
        prompt_budget=lambda: measure_3515_prompt_budget.main([]),
    )
    try:
        report, summary = run(
            steps,
            apply=args.apply,
            measure=args.measure,
            declared_by=args.declared_by,
            wake_evidence=args.wake_evidence,
            expect_config_sha256=args.expect_config_sha256,
        )
    except (TrialFreezeError, psycopg.Error, OSError) as exc:
        print(
            f"freeze FAILED: {type(exc).__name__}: {exc}\n"
            "Before retrying, run the dry run: `already_frozen` means the freeze DID commit."
        )
        return 1
    print(render(report, summary))
    if args.json is not None:
        try:
            args.json.write_text(
                json.dumps({**report.__dict__, "walk": summary}, sort_keys=True, indent=2, default=str)
            )
        except OSError as exc:
            print(f"--json not written (the outcome above stands): {type(exc).__name__}: {exc}")
    return 0 if not report.refusals else 1


if __name__ == "__main__":
    raise SystemExit(main())
