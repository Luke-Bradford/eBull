"""Open only #2908's preregistered published-factor identity gate."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import subprocess
from pathlib import Path

from app.services.r6_exclusion_trial import (
    constructed_nsi_factor,
    load_required_prices,
    read_global_q_nsi,
    validate_factor,
)
from app.services.r6_pit_bundle import load_r6_pit_bundle


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _verify_mirror(root: Path, expected_commit: str) -> None:
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if commit != expected_commit or dirty:
        raise RuntimeError(f"price mirror is not the declared clean commit: commit={commit}, dirty={bool(dirty)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--price-mirror", type=Path, required=True)
    parser.add_argument("--price-mirror-commit", required=True)
    parser.add_argument("--global-q", type=Path, required=True)
    parser.add_argument("--global-q-sha256", required=True)
    args = parser.parse_args()
    if _sha256(args.global_q) != args.global_q_sha256:
        raise RuntimeError("global-q source digest moved")
    _verify_mirror(args.price_mirror, args.price_mirror_commit)
    bundle = load_r6_pit_bundle(args.manifest, expected_manifest_sha256=args.manifest_sha256)
    prices = load_required_prices(bundle, args.price_mirror / "Data" / "Day")
    ours = constructed_nsi_factor(bundle, prices)
    reference = read_global_q_nsi(args.global_q)
    result = validate_factor(ours, reference)
    print(
        json.dumps(
            {
                "factor_validation": dataclasses.asdict(result),
                "global_q_sha256": args.global_q_sha256,
                "invalid_price_rows_skipped": sum(series.invalid_rows for series in prices.values()),
                "manifest_sha256": args.manifest_sha256,
                "price_mirror_commit": args.price_mirror_commit,
                "result": "PASS" if result.passed else "FAIL_CONSTRUCTION_BUG",
                "series": len(prices),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
