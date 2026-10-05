"""#3609 step 1 slice 3d-iv part 2: full-population A/B of Amendment 2b's ``ni*`` and ``ocf*`` branches.

Spec §"Slices" 3d-iv. Arm A is the 3d-iii rows, arm B the rebuild of the same frozen inputs under this code. Only
``ni_me`` and ``ocf_me`` may change. On every admitted row, arm B's (value, missing, period end, kind) for each must
equal ``ni_adopted`` / ``ocf_adopted`` in the Amendment 2b measurement rows (the oracle). Every input is bound by the
sha256 of its decompressed content. Any difference exits 1.

    PYTHONPATH=. uv run python -m scripts.ab_3609_ni_ocf --a <rows A> --b <rows B> --oracle <measurement rows> \\
        --out <summary.json>
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from app.services.factor_panel_artefact import gz_content_sha256, read_gz_lines

#: Slice 3d-iii rows (``var/research/3609_step1/ab3d22/rows.jsonl.gz`` in the loop worktree).
A_CONTENT_SHA256 = "defe34aaaaaccab0a3c50260817947c647af093a7393b620f218197244914deb"
#: Amendment 2b measurement rows (``var/research/3609_step1/a2b/m5/rows.jsonl.gz``).
ORACLE_CONTENT_SHA256 = "068afe2cef29782f45456463488234a360fce05572c5a9ef95d8718a8992dbe3"
CHANGED = {"ni_me": "ni_adopted", "ocf_me": "ocf_adopted"}
COMPARED = ("value", "missing", "period_end", "kind")


def _key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (row["M"], row["cik"], row["symbol"])


def _reading(c: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(c[k] for k in COMPARED)


def _state(reading: tuple[Any, ...]) -> str:
    return "value" if reading[1] is None else str(reading[1])


def index_oracle(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    out: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in rows:
        if _key(row) in out:
            raise SystemExit(f"oracle key {_key(row)} repeats")
        out[_key(row)] = row
    return out


def compare(
    a_rows: Iterable[Mapping[str, Any]],
    b_rows: Iterable[Mapping[str, Any]],
    oracle: Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    """Pair A and B row by row (the builder's order is deterministic) and check each pair against the oracle."""
    failures: list[str] = []
    transitions: Counter[str] = Counter()
    counts: Counter[str] = Counter()
    used: set[tuple[str, str, str]] = set()
    a_iter, b_iter = iter(a_rows), iter(b_rows)
    sentinel = object()
    while True:
        a, b = next(a_iter, sentinel), next(b_iter, sentinel)
        if a is sentinel or b is sentinel:
            if a is not b:
                failures.append("row counts differ")
            break
        assert isinstance(a, Mapping) and isinstance(b, Mapping)
        counts["rows"] += 1
        a_chars, b_chars = a.get("characteristics"), b.get("characteristics")
        if {k: v for k, v in a.items() if k != "characteristics"} != {
            k: v for k, v in b.items() if k != "characteristics"
        }:
            failures.append(f"{_key(a)}: fields outside the characteristics differ")
            continue
        if a_chars is None or b_chars is None:
            if a_chars != b_chars:
                failures.append(f"{_key(a)}: characteristics present on one arm only")
            continue
        if set(a_chars) != set(b_chars) or any(a_chars[n] != b_chars[n] for n in a_chars if n not in CHANGED):
            failures.append(f"{_key(a)}: a characteristic other than ni_me/ocf_me differs")
            continue
        if a.get("exclusion") is not None:
            if any(a_chars[n] != b_chars[n] for n in CHANGED):
                failures.append(f"{_key(a)}: an excluded row's ni_me/ocf_me changed")
            continue
        counts["admitted"] += 1
        expected = oracle.get(_key(a))
        if expected is None:
            failures.append(f"{_key(a)}: admitted row has no oracle row")
            continue
        used.add(_key(a))
        for name, variant in CHANGED.items():
            before, after, want = _reading(a_chars[name]), _reading(b_chars[name]), _reading(expected[variant])
            if after != want:
                failures.append(f"{_key(a)} {name}: {after} != oracle {want}")
            if before != after:
                counts[f"{name} changed"] += 1
                transitions[f"{name}: {_state(before)} -> {_state(after)}"] += 1
            if after[1] is None:
                counts[f"{name} value"] += 1
    if len(used) != len(oracle):
        failures.append(f"{len(oracle) - len(used)} oracle rows have no admitted row")
    return failures, {"counts": dict(counts), "transitions": dict(sorted(transitions.items()))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for arm in ("a", "b", "oracle", "out"):
        parser.add_argument(f"--{arm}", type=Path, required=True)
    args = parser.parse_args()
    for path, digest in ((args.a, A_CONTENT_SHA256), (args.oracle, ORACLE_CONTENT_SHA256)):
        if gz_content_sha256(path) != digest:
            raise SystemExit(f"{path}: content sha256 does not match its pin")
    oracle = index_oracle(read_gz_lines(args.oracle))
    failures, summary = compare(read_gz_lines(args.a), read_gz_lines(args.b), oracle)
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
