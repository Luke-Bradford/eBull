"""#3609 step 1 slice 3d-v part 2: full-population A/B of Amendment 2c's ``ope*`` and ``gp*`` reads.

Spec §"Slices" 3d-v. Arm A is the canonical stage-A rows (``2026-10-05-191d7b07-stageA``), arm B the rebuild of the
same frozen inputs under this code, with the ``BUNDLE`` and ``LINKAGE`` pins moved to slice 3d-v part 1's artefacts.
Only ``ope_be`` and ``gp_at`` may change. On every admitted row, arm B's (value, missing, period end, kind) for each
must equal ``ope:adopted`` / ``gp:r5`` in the rows of the Amendment 2c measurement re-run, adopted variant only,
against the built bundle (the oracle), exactly: no reading is adjudicated. Arm A and the oracle are bound by the
sha256 of their decompressed content. Any difference exits 1. The comparison is ``ab_3609_ni_ocf.compare``'s.

    PYTHONPATH=. uv run python -m scripts.ab_3609_ope_gp --a <rows A> --b <rows B> --oracle <measurement rows> \\
        --out <summary.json>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Final

from app.services.factor_panel_artefact import gz_content_sha256, read_gz_lines
from scripts.ab_3609_ni_ocf import compare, index_oracle

#: Canonical stage-A rows (``factor_panel_3609/2026-10-05-191d7b07-stageA/rows.jsonl.gz``; its manifest's
#: ``rows.content_sha256``).
A_CONTENT_SHA256: Final = "b9724baa8932d4fa70cf865a3b72ae4a91d3772c45240e9a7d5298eacb157805"
#: The measurement re-run against bundle B (``var/research/3609_step1/a2c/m5B/rows.jsonl.gz`` in the loop worktree),
#: adopted variant only, from a clean ``origin/main`` worktree at ``eeec97b2``; equal to m4's rows.
ORACLE_CONTENT_SHA256: Final = "9d7c05ec86a27c243ff9eb299efd6be80802704d3b518c5191148c7a4f460cba"
CHANGED: Final = {"ope_be": "ope:adopted", "gp_at": "gp:r5"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for arm in ("a", "b", "oracle", "out"):
        parser.add_argument(f"--{arm}", type=Path, required=True)
    args = parser.parse_args()
    for path, digest in ((args.a, A_CONTENT_SHA256), (args.oracle, ORACLE_CONTENT_SHA256)):
        if gz_content_sha256(path) != digest:
            raise SystemExit(f"{path}: content sha256 does not match its pin")
    oracle = index_oracle(read_gz_lines(args.oracle))
    failures, summary = compare(read_gz_lines(args.a), read_gz_lines(args.b), oracle, changed=CHANGED)
    result = {
        "b_content_sha256": gz_content_sha256(args.b),
        **summary,
        "failures": failures[:200],
        "failure_count": len(failures),
    }
    args.out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "failures"}, indent=1, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
