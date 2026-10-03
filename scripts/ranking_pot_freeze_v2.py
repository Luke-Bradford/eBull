"""#3592 slice 3c — freeze the ranking-pot-v2 declaration (spec §8).

Dry run (default) executes the whole freeze and rolls back, printing the document's sha, S₀, the measured frozen terms
(strata sizes, the DTC baseline, the insider history floor) and every refusal:

    PYTHONPATH=. uv run python -m scripts.ranking_pot_freeze_v2 [--json out.json]

⚠ ``--apply`` freezes S₀ and lands v2 in ``shadow_only`` (no executed book exists for v2; the shadow, controls and
reference start at the first rebalance). It is the supervisor's step after slice 4 sets ``BUILD_COMPLETE`` and the
freeze prerequisites on #3592 hold (the #3590 drain at zero pending, both premise scripts re-run), from
``~/Dev/eBull`` at ``origin/main``, never by the autonomy loop:

    PYTHONPATH=. uv run python -m scripts.ranking_pot_freeze_v2 --apply --declared-by <who>

Exit status: 0 when the freeze would succeed (dry run) or did (apply), 1 on any refusal.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings
from app.services.ai_trial_freeze import read_provenance
from app.services.ranking_pot_freeze import FreezeReport, PotFreezeError
from app.services.ranking_pot_freeze_v2 import SPEC_PATH, freeze_pot_v2


def render(report: FreezeReport) -> str:
    mode = "APPLIED — v2 is shadow_only" if report.applied else "dry run (rolled back)"
    lines = [f"ranking-pot-v2 freeze: {mode}"]
    if report.doc is not None:
        s0, frozen = report.doc["s0"], report.doc["v2"]
        sizes = Counter(s for _, s in frozen["strata"])
        floor = frozen["history_floor"]
        counts = floor["month_counts"]
        lines += [
            f"document sha256: {report.doc_sha256}  code: {report.doc['code_git_sha']}",
            f"policy_hash: {report.doc['policy_hash']}",
            f"family {report.doc['family']} seq {report.doc['family_seq']}: {report.doc['terms']}",
            f"S0: {s0['count']} ids from the scores run {s0['scored_at']} (sha256 {s0['sha256']})",
            f"strata sizes: {dict(sorted(sizes.items()))}",
            f"DTC baseline: {frozen['dtc_baseline']}",
            f"history floor {floor['floor']} (threshold {floor['threshold']}); {len(counts)} months "
            f"{counts[0][0]}..{counts[-1][0]}, min {min(n for _, n in counts)}",
        ]
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
    parser.add_argument("--apply", action="store_true", help="freeze S0 and enter shadow_only (supervisor only)")
    parser.add_argument("--declared-by", default="")
    parser.add_argument("--no-fetch", action="store_true", help="skip `git fetch origin main` (tests only)")
    parser.add_argument("--json", type=Path, default=None, help="also write the report as JSON")
    args = parser.parse_args(argv)

    try:
        provenance = read_provenance(fetch=not args.no_fetch, spec_path=SPEC_PATH)
        with psycopg.connect(settings.database_url) as conn:
            report = freeze_pot_v2(conn, provenance=provenance, apply=args.apply, declared_by=args.declared_by)
    except (PotFreezeError, psycopg.Error, OSError) as exc:
        print(
            f"freeze FAILED: {type(exc).__name__}: {exc}\n"
            "Before retrying, run the dry run: `already_frozen` means the freeze DID commit."
        )
        return 1
    print(render(report))
    if args.json is not None:
        try:
            args.json.write_text(json.dumps(report.__dict__, sort_keys=True, indent=2, default=str))
        except OSError as exc:
            print(f"--json not written (the outcome above stands): {type(exc).__name__}: {exc}")
    return 0 if not report.refusals else 1


if __name__ == "__main__":
    raise SystemExit(main())
