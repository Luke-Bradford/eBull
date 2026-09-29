"""#3471 slice 3d — freeze the AI-discretionary-v1 declaration (spec §9 "Freeze").

Dry run (default) executes the whole freeze and rolls back, printing the document, its sha, both
legs' deployment + policy values and ``config_sha256``, and every refusal:

    PYTHONPATH=. uv run python -m scripts.ai_trial_freeze [--json out.json]

⚠ ``--apply`` STARTS THE TRIAL — both ai_trial jobs read ``active`` at their next fire. It is a
supervisor go-live action, run from ``~/Dev/eBull`` at ``origin/main`` once the §8 wake condition
holds, never by the autonomy loop:

    PYTHONPATH=. uv run python -m scripts.ai_trial_freeze --apply --declared-by <who> \\
        --wake-evidence <url of the supervisor's §8 answer> --expect-config-sha256 <sha from the dry run>

Exit status: 0 when the freeze would succeed (dry run) or did (apply), 1 on any refusal.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg

from app.config import settings
from app.services.ai_trial_freeze import FreezeReport, freeze_trial, read_provenance


def render(report: FreezeReport) -> str:
    mode = "APPLIED — the trial is active" if report.applied else "dry run (rolled back)"
    lines = [
        f"AI-discretionary-v1 freeze: {mode}",
        f"document sha256: {report.doc_sha256}  code: {report.doc['code_git_sha']}",
        f"policy_hash: {report.doc['policy_hash']}",
        f"config_sha256: {report.config_sha256}",
    ]
    for strategy_id, leg in sorted(report.config.items()):
        lines.append(f"  {strategy_id}: {json.dumps(leg, sort_keys=True, default=str)}")
    if report.existing_doc_sha256 is not None:
        same = report.existing_doc_sha256 == report.doc_sha256
        lines.append(
            f"already frozen: stored document {report.existing_doc_sha256} ({'same' if same else 'DIFFERENT'})"
        )
    lines.append(f"refusals: {list(report.refusals) or 'none'}")
    if report.declaration_id is not None:
        lines.append(f"declaration_id: {report.declaration_id}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="freeze and START the trial (supervisor only)")
    parser.add_argument("--declared-by", default="")
    parser.add_argument("--wake-evidence", default="", help="URL of the supervisor's §8 answer")
    parser.add_argument("--expect-config-sha256", default=None, help="config_sha256 printed by the dry run")
    parser.add_argument("--no-fetch", action="store_true", help="skip `git fetch origin main` (tests only)")
    parser.add_argument("--json", type=Path, default=None, help="also write the report as JSON")
    args = parser.parse_args(argv)

    provenance = read_provenance(fetch=not args.no_fetch)
    with psycopg.connect(settings.database_url) as conn:
        report = freeze_trial(
            conn,
            provenance=provenance,
            apply=args.apply,
            declared_by=args.declared_by,
            wake_evidence=args.wake_evidence,
            expect_config_sha256=args.expect_config_sha256,
        )
    print(render(report))
    if args.json is not None:
        args.json.write_text(json.dumps(report.__dict__, sort_keys=True, indent=2, default=str))
    return 0 if not report.refusals else 1


if __name__ == "__main__":
    raise SystemExit(main())
