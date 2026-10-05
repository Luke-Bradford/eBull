"""#3609 step 1 slice 2: the full-population A/B for the ``CONCEPT_SET`` extension (spec Amendment 1).

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Concept-set extension", Amendment 1.

* **#3360 bundle.** Arm A is the 2026-09-24 bundle, arm B the new build from arm A's own ``inputs/``.
  Compared per CIK, row for row with payload and multiplicity: B's rows for the pre-existing concepts must
  equal A's, and its only extra rows must be for the added concepts. Manifest integrity failures,
  ``supported_through``, inputs and every pre-existing ledger key must be equal. Each added concept must
  have stored events.
* **#3361 linkage.** The replay rebuild against the 2026-09-24 build: every series document and the ledger
  byte-identical; the manifest equal except ``policy`` and ``input_sha256.pit_manifest``.

Both arms are read as raw JSON with every file checked against its manifest digest: arm A's policy no
longer matches this code, so its loader would refuse it. Any difference exits non-zero. Usage::

    PYTHONPATH=. uv run python scripts/ab_3609_concept_extension.py \\
        --bundle-a <dir> --bundle-a-sha256 <digest> --bundle-b <dir> --bundle-b-sha256 <digest> \\
        --linkage-a <dir> --linkage-a-sha256 <digest> --linkage-b <dir> --linkage-b-sha256 <digest>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from app.services.pit_fundamentals import CONCEPT_SET, load_pit_fundamentals
from app.services.security_linkage import load_security_linkage

#: Spec §"Concept-set extension": exactly these nine are added.
ADDED_CONCEPTS = frozenset(
    {
        ("us-gaap", "Liabilities"),
        ("us-gaap", "SellingGeneralAndAdministrativeExpense"),
        ("us-gaap", "InterestExpense"),
        ("us-gaap", "IncomeLossFromContinuingOperations"),
        ("us-gaap", "DeferredTaxLiabilitiesNoncurrent"),
        ("us-gaap", "DeferredIncomeTaxLiabilitiesNet"),
        ("us-gaap", "PreferredStockRedemptionAmount"),
        ("us-gaap", "PreferredStockLiquidationPreferenceValue"),
        ("us-gaap", "PreferredStockValue"),
    }
)
STORED = "stored"


def _read(path: Path, expected_sha256: str | None = None) -> tuple[bytes, Any]:
    data = path.read_bytes()
    if expected_sha256 is not None and hashlib.sha256(data).hexdigest() != expected_sha256:
        raise SystemExit(f"{path}: sha256 does not match its pin")
    return data, json.loads(data)


def _added_ledger_key(key: str) -> bool:
    taxonomy, concept, _ = key.split("/", 2)
    return (taxonomy, concept) in ADDED_CONCEPTS


def compare_bundles(a_root: Path, a_sha: str, b_root: Path, b_sha: str) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    _, a = _read(a_root / "manifest.json", a_sha)
    _, b = _read(b_root / "manifest.json", b_sha)
    for field in ("schema", "input_sha256", "supported_through", "snapshot_integrity_failures"):
        if a[field] != b[field]:
            failures.append(f"bundle manifest {field} differs")
    a_rows, b_rows = a["ledger"]["rows"], b["ledger"]["rows"]
    for key, counts in a_rows.items():
        if b_rows.get(key) != counts:
            failures.append(f"ledger {key}: {counts} -> {b_rows.get(key)}")
    extra_keys = sorted(set(b_rows) - set(a_rows))
    failures += [f"ledger key {key} is not an added concept" for key in extra_keys if not _added_ledger_key(key)]
    a_variants, b_variants = a["ledger"]["form_label_variants"], b["ledger"]["form_label_variants"]
    for key in sorted(set(a_variants) | set(b_variants)):
        if a_variants.get(key) != b_variants.get(key) and not (key not in a_variants and _added_ledger_key(key)):
            failures.append(f"ledger form_label_variants {key} differs")
    if a["ledger"]["ignored_companyfacts_members"] != b["ledger"]["ignored_companyfacts_members"]:
        failures.append("ledger ignored_companyfacts_members differs")
    # NO_ACCEPTANCE is counted per CIK over every concept, so an added concept may only raise it.
    for cik, count in a["ledger"]["no_acceptance_by_cik"].items():
        if b["ledger"]["no_acceptance_by_cik"].get(cik, 0) < count:
            failures.append(f"no_acceptance_by_cik {cik} fell")

    a_shards = {entry["cik"]: entry for entry in a["shards"]}
    b_shards = {entry["cik"]: entry for entry in b["shards"]}
    if set(a_shards) != set(b_shards):
        failures.append(f"shard CIKs differ: {len(a_shards)} vs {len(b_shards)}")
    added_rows: Counter[str] = Counter()
    added_multiplicity: Counter[str] = Counter()
    for cik in sorted(set(a_shards) & set(b_shards)):
        _, shard_a = _read(a_root / a_shards[cik]["path"], a_shards[cik]["sha256"])
        _, shard_b = _read(b_root / b_shards[cik]["path"], b_shards[cik]["sha256"])
        if (shard_a["schema"], shard_a["cik"], shard_a["accessions"]) != (
            shard_b["schema"],
            shard_b["cik"],
            shard_b["accessions"],
        ):
            failures.append(f"{cik}: shard header or accessions differ")
        for section in ("events", "rejections"):
            # Ordered on purpose: the builder sorts shard rows by (taxonomy, concept, unit, …)
            # (``pit_fundamentals._event_order`` / ``_rejection_order``), so removing the added concepts
            # leaves A's rows in A's order, and a reorder is a difference too.
            kept = [row for row in shard_b[section] if (row["taxonomy"], row["concept"]) not in ADDED_CONCEPTS]
            if kept != shard_a[section]:
                failures.append(f"{cik}: {section} differ on pre-existing concepts")
            if any((row["taxonomy"], row["concept"]) in ADDED_CONCEPTS for row in shard_a[section]):
                failures.append(f"{cik}: arm A already holds an added concept")
            for row in shard_b[section]:
                if (row["taxonomy"], row["concept"]) in ADDED_CONCEPTS:
                    added_rows[f"{section}:{row['concept']}"] += 1
                    if section == "events":
                        added_multiplicity[row["concept"]] += row["multiplicity"]

    stored_by_concept: Counter[str] = Counter()
    for key in extra_keys:
        stored_by_concept[key.split("/")[1]] += b_rows[key].get(STORED, 0)
    for _, concept in sorted(ADDED_CONCEPTS):
        if stored_by_concept[concept] == 0 or added_rows[f"events:{concept}"] == 0:
            failures.append(f"added concept {concept} has no stored events")
        elif added_multiplicity[concept] != stored_by_concept[concept]:
            # Each STORED raw row adds 1 to its event's multiplicity and 1 to the ledger (build_shard).
            failures.append(f"added concept {concept}: event multiplicities do not sum to the ledger's stored")
    # Arm B must also load under this code's policy, every shard verified by its own loader.
    load_pit_fundamentals(b_root, expected_manifest_sha256=b_sha).verify_all()
    summary = {
        "shards": len(b_shards),
        "added_ledger": {key: b_rows[key] for key in extra_keys},
        "added_rows": dict(sorted(added_rows.items())),
        "supported_through": b["supported_through"],
    }
    return failures, summary


def compare_linkage(a_root: Path, a_sha: str, b_root: Path, b_sha: str) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    _, a = _read(a_root / "manifest.json", a_sha)
    _, b = _read(b_root / "manifest.json", b_sha)
    allowed = {"policy"}
    for field in sorted(set(a) | set(b)):
        if field in allowed or field == "input_sha256":
            continue
        if a.get(field) != b.get(field):
            failures.append(f"linkage manifest {field} differs")
    a_inputs = {k: v for k, v in a["input_sha256"].items() if k != "pit_manifest"}
    b_inputs = {k: v for k, v in b["input_sha256"].items() if k != "pit_manifest"}
    if a_inputs != b_inputs:
        failures.append("linkage input_sha256 differs outside pit_manifest")
    if (a_root / "ledger.json").read_bytes() != (b_root / "ledger.json").read_bytes():
        failures.append("linkage ledger.json differs")
    documents = 0
    for entry in a["series"]:
        if entry["path"] is None:
            continue
        documents += 1
        if (a_root / entry["path"]).read_bytes() != (b_root / entry["path"]).read_bytes():
            failures.append(f"series {entry['series_id']} document differs")
    # The rebuilt bundle must also pass its own loader under this code, every series verified.
    load_security_linkage(b_root, expected_manifest_sha256=b_sha).verify_all()
    return failures, {
        "series": len(a["series"]),
        "documents_compared": documents,
        "policy_changed": a["policy"] != b["policy"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for arm in ("bundle-a", "bundle-b", "linkage-a", "linkage-b"):
        parser.add_argument(f"--{arm}", type=Path, required=True)
        parser.add_argument(f"--{arm}-sha256", required=True)
    args = parser.parse_args()
    if not ADDED_CONCEPTS <= set(CONCEPT_SET):
        raise SystemExit("this code's CONCEPT_SET lacks an added concept")
    bundle_failures, bundle = compare_bundles(args.bundle_a, args.bundle_a_sha256, args.bundle_b, args.bundle_b_sha256)
    linkage_failures, linkage = compare_linkage(
        args.linkage_a, args.linkage_a_sha256, args.linkage_b, args.linkage_b_sha256
    )
    failures = bundle_failures + linkage_failures
    json.dump(
        {"bundle": bundle, "linkage": linkage, "failures": failures[:200], "failure_count": len(failures)},
        sys.stdout,
        indent=1,
        sort_keys=True,
    )
    print()
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
