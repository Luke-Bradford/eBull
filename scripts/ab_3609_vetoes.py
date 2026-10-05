"""#3609 step 1 slice 3d-iv, the two open clauses: full-population A/B of the veto labels against the m6 oracle.

Spec §"Slices" 3d-iv. Arm A is the 3d-iv part 2 rows, arm B the rebuild of the same frozen inputs under this code.
With each characteristic's ``vetoes`` removed and every ``veto_`` label dropped from its ``branches``, B must equal A
row for row; ``vetoes`` must be empty outside ``ni_me`` / ``ocf_me``. On every admitted row, B's ``ni_me`` /
``ocf_me`` (value, missing, period end, kind) must equal ``ni_adopted`` / ``ocf_adopted`` in the m6 measurement rows
exactly: no reading is adjudicated. B's veto labels, per companion, must number the measurement's own count of zeros
refused by a witness. Arm A and the oracle rows are bound by the sha256 of their decompressed content, the oracle
summary by the rows digest it records. Any difference exits 1.

    PYTHONPATH=. uv run python -m scripts.ab_3609_vetoes --a <rows A> --b <rows B> --oracle <m6 rows> \\
        --oracle-summary <m6 summary.json> --out <summary.json>
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from itertools import zip_longest
from pathlib import Path
from typing import Any

from app.services.factor_panel import FALLBACK_LABELS, VETO
from app.services.factor_panel_artefact import gz_content_sha256, read_gz_lines
from scripts.ab_3609_ni_ocf import CHANGED, _key, _reading, index_oracle

#: Slice 3d-iv part 2 rows (``var/research/3609_step1/s3div2/rows3.jsonl.gz`` in the loop worktree).
A_CONTENT_SHA256 = "5f0c153f57104f0d056a602883a998462f7de05f8376469db774f9ffb2df55a3"
#: Amendment 2b m6 measurement rows (``var/research/3609_step1/a2b/m6/rows.jsonl.gz``).
ORACLE_CONTENT_SHA256 = "TODO"


def without_vetoes(row: Mapping[str, Any]) -> dict[str, Any]:
    """Arm B in arm A's shape: no ``vetoes`` field and no ``veto_`` branch label."""
    chars = row.get("characteristics")
    if chars is None:
        return dict(row)
    stripped = {
        name: {
            **{k: v for k, v in c.items() if k != "vetoes"},
            "branches": [b for b in c["branches"] if not b.startswith(VETO)],
        }
        for name, c in chars.items()
    }
    return {**row, "characteristics": stripped}


def compare(
    a_rows: Iterable[Mapping[str, Any]],
    b_rows: Iterable[Mapping[str, Any]],
    oracle: Mapping[tuple[str, str, str], Mapping[str, Any]],
    refused: Mapping[str, int],
) -> tuple[list[str], dict[str, Any]]:
    """Pair A and B row by row (the builder's order is deterministic); ``refused`` is the oracle's veto count per
    companion."""
    failures: list[str] = []
    counts: Counter[str] = Counter()
    used: set[tuple[str, str, str]] = set()
    for a, b in zip_longest(a_rows, b_rows):
        if a is None or b is None:
            failures.append("row counts differ")
            break
        counts["rows"] += 1
        if a.get("exclusion") is None:
            used.add(_key(a))
        if without_vetoes(b) != a:
            failures.append(f"{_key(a)}: differs beyond the veto labels")
            continue
        chars = b.get("characteristics")
        if chars is None:
            continue
        for name, c in chars.items():
            if not c["vetoes"]:
                continue
            if name not in FALLBACK_LABELS:
                failures.append(f"{_key(a)} {name}: a veto outside ni_me/ocf_me")
            counts[f"{name} name-months with a veto"] += 1
            counts.update(f"veto evaluations: {v.split(':', 1)[0].removeprefix(VETO)}" for v in c["vetoes"])
        if a.get("exclusion") is not None:
            continue
        counts["admitted"] += 1
        expected = oracle.get(_key(a))
        if expected is None:
            failures.append(f"{_key(a)}: admitted row has no oracle row")
            continue
        for name, variant in CHANGED.items():
            if (got := _reading(chars[name])) != (want := _reading(expected[variant])):
                failures.append(f"{_key(a)} {name}: {got} != oracle {want}")
    if unmatched := len(set(oracle) - used):
        failures.append(f"{unmatched} oracle rows have no admitted row")
    for companion in sorted({c for _, _, names in FALLBACK_LABELS.values() for c in names}):
        if (got := counts[f"veto evaluations: {companion}"]) != (want := refused.get(companion, 0)):
            failures.append(f"veto evaluations for {companion}: {got} != oracle {want}")
    return failures, {"counts": dict(sorted(counts.items()))}


def refused_by_witness(summary: Mapping[str, Any]) -> dict[str, int]:
    """The measurement's ``<variant>_adopted: zero refused by a witness (<companion>)`` counts, per companion."""
    prefixes = tuple(f"{v}: zero refused by a witness (" for v in CHANGED.values())
    out: dict[str, int] = {}
    for key, n in summary["period_evaluations"].items():
        for prefix in prefixes:
            if key.startswith(prefix):
                out[key.removeprefix(prefix).removesuffix(")")] = n
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for arm in ("a", "b", "oracle", "oracle-summary", "out"):
        parser.add_argument(f"--{arm}", type=Path, required=True)
    args = parser.parse_args()
    for path, digest in ((args.a, A_CONTENT_SHA256), (args.oracle, ORACLE_CONTENT_SHA256)):
        if gz_content_sha256(path) != digest:
            raise SystemExit(f"{path}: content sha256 does not match its pin")
    summary = json.loads(args.oracle_summary.read_text())
    if summary["output_rows_sha256"] != ORACLE_CONTENT_SHA256:
        raise SystemExit(f"{args.oracle_summary}: not the summary of the pinned oracle rows")
    failures, result = compare(
        read_gz_lines(args.a),
        read_gz_lines(args.b),
        index_oracle(read_gz_lines(args.oracle)),
        refused_by_witness(summary),
    )
    out = {"b_content_sha256": gz_content_sha256(args.b), **result, "failures": failures[:200]}
    out["failure_count"] = len(failures)
    args.out.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in out.items() if k != "failures"}, indent=1, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
