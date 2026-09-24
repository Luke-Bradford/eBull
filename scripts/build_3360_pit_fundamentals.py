"""Build the #3360 point-in-time fundamentals bundle from companyfacts + submissions.

Spec: ``docs/proposals/ta/2026-09-24-3360-companyfacts-pit-bundle.md`` ("Artefact").
Admission and reading live in ``app/services/pit_fundamentals.py``; this script only
snapshots the inputs, walks the archives and publishes.

Publish protocol: ``mkdir <bundle>`` exclusively (refuses an existing directory); both
archives copied into ``<bundle>/inputs/`` and hashed from the copy, which is the only
thing read; each shard written with fsync and the no-replace link, then re-read and
validated; the manifest is written LAST and the directory fsynced. No manifest = not a
bundle. A crashed build is deleted, never resumed.

Usage::

    PYTHONPATH=. uv run python scripts/build_3360_pit_fundamentals.py \\
        --companyfacts ~/Library/Application\\ Support/eBull/sec/bulk/companyfacts.zip \\
        --submissions ~/Library/Application\\ Support/eBull/sec/bulk/submissions.zip \\
        --out <new bundle directory>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import zipfile
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.services.pit_fundamentals import (
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA,
    SHARDS_DIRNAME,
    Outcome,
    SubmissionsIndex,
    acceptance_ny_date,
    build_shard,
    cik_int,
    count_raw_rows,
    iter_raw_rows,
    ledger_key,
    load_shard,
    parse_submissions,
    policy_sha256,
)
from scripts.build_2900_pit_bundle import _write_exclusive

_MEMBER = re.compile(r"CIK(\d{10})\.json")
INTEGRITY_EXCLUDED = "integrity_excluded"


def _snapshot(source: Path, destination: Path) -> str:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())
        handle.seek(0)
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _unique_names(archive: zipfile.ZipFile, label: str) -> set[str]:
    names = archive.namelist()
    if len(names) != len(set(names)):
        raise RuntimeError(f"{label}: duplicate zip members")
    return set(names)


def build(companyfacts: Path, submissions: Path, out: Path) -> dict[str, Any]:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing bundle directory is refused, never resumed
    try:
        inputs = out / "inputs"
        inputs.mkdir()
        input_sha = {
            "companyfacts": _snapshot(companyfacts, inputs / "companyfacts.zip"),
            "submissions": _snapshot(submissions, inputs / "submissions.zip"),
        }
        with (
            zipfile.ZipFile(inputs / "companyfacts.zip") as facts_zip,
            zipfile.ZipFile(inputs / "submissions.zip") as subs_zip,
        ):
            return _build(facts_zip, subs_zip, out, input_sha)
    except BaseException:
        # A crashed build is deleted, never resumed. ``out`` was created by the exclusive
        # mkdir above, so it holds nothing but this build's own partial output.
        shutil.rmtree(out, ignore_errors=True)
        raise


def _build(
    facts_zip: zipfile.ZipFile, subs_zip: zipfile.ZipFile, out: Path, input_sha: dict[str, str]
) -> dict[str, Any]:
    fact_names = _unique_names(facts_zip, "companyfacts")
    sub_names = _unique_names(subs_zip, "submissions")
    ledger: Counter[tuple[str, str]] = Counter()
    label_variants: Counter[str] = Counter()
    no_acceptance: dict[str, int] = {}
    failures: list[dict[str, str]] = []
    shards: list[dict[str, Any]] = []
    max_fact: str | None = None
    max_sub: str | None = None
    ignored_members = 0

    def read_page(name: str) -> object | None:
        return json.loads(subs_zip.read(name)) if name in sub_names else None

    for name in sorted(fact_names):
        match = _MEMBER.fullmatch(name)
        if match is None:
            ignored_members += 1
            continue
        cik10 = match.group(1)
        payload = json.loads(facts_zip.read(name), parse_float=Decimal, parse_int=Decimal)
        index: SubmissionsIndex | str
        if not isinstance(payload, dict) or cik_int(payload.get("cik")) != int(cik10):
            index = "companyfacts_cik_mismatch"
        elif not isinstance(payload.get("facts", {}), dict):
            index = "companyfacts_malformed"
        elif name not in sub_names:
            index = "no_submissions_entry"
        else:
            index = parse_submissions(cik10, json.loads(subs_zip.read(name)), read_page)
        if isinstance(index, str):
            failures.append({"cik": cik10, "reason": index})
            if isinstance(payload, dict):
                for key, count in count_raw_rows(payload).items():
                    ledger[(key, "raw")] += count
                for taxonomy, concept, unit, _, _ in iter_raw_rows(payload):
                    ledger[(ledger_key(taxonomy, concept, unit), INTEGRITY_EXCLUDED)] += 1
            continue

        for key, count in count_raw_rows(payload).items():
            ledger[(key, "raw")] += count
        built = build_shard(cik10, payload, index)
        ledger.update(built.ledger)
        label_variants.update(built.form_label_variants)
        missing = sum(n for (_, outcome), n in built.ledger.items() if outcome == Outcome.NO_ACCEPTANCE)
        if missing:
            no_acceptance[cik10] = missing
        if built.max_fact_acceptance is not None:
            max_fact = max(max_fact or "", built.max_fact_acceptance)
        accepted = [f.acceptance for f in index.filings.values() if f.acceptance is not None]
        if accepted:
            max_sub = max(max_sub or "", *accepted)

        relative = f"{SHARDS_DIRNAME}/CIK{cik10}.json"
        _write_exclusive(out / relative, built.shard)
        with (out / relative).open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        entry = {"cik": cik10, "path": relative, "sha256": digest, "events": len(built.shard["events"])}
        load_shard(out / relative, entry)  # re-read through the loader's own checks
        shards.append(entry)

    # Horizons: companyfacts = latest acceptance of ANY accession its rows cite (any
    # outcome); submissions = latest acceptance in every index this build parsed. A CIK the
    # build never reads can only make the true archive maximum later, so this bound errs
    # toward ``after_capture``.
    if max_fact is None or max_sub is None:
        raise RuntimeError("no admitted fact or no parseable acceptance: nothing to support")
    rows = _reconcile(ledger)
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "policy": policy_sha256(),
        "input_sha256": input_sha,
        "supported_through": min(acceptance_ny_date(max_fact), acceptance_ny_date(max_sub)).isoformat(),
        "snapshot_integrity_failures": failures,
        "shards": shards,
        "ledger": {
            "rows": rows,
            "no_acceptance_by_cik": no_acceptance,
            "form_label_variants": dict(sorted(label_variants.items())),
            "ignored_companyfacts_members": ignored_members,
        },
    }
    _write_exclusive(out / MANIFEST_FILENAME, manifest)
    descriptor = os.open(out, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return manifest


def _reconcile(ledger: Counter[tuple[str, str]]) -> dict[str, dict[str, int]]:
    """Raw rows in = Σ outcomes + integrity-excluded rows, per (taxonomy, concept, unit)."""
    rows: dict[str, dict[str, int]] = {}
    for (key, outcome), count in sorted(ledger.items()):
        rows.setdefault(key, {})[outcome] = count
    for key, counts in rows.items():
        accounted = sum(n for outcome, n in counts.items() if outcome != "raw")
        if accounted != counts.get("raw", 0):
            raise RuntimeError(f"ledger does not reconcile for {key}: {counts}")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--companyfacts", type=Path, required=True)
    parser.add_argument("--submissions", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    # The chokepoint WARNs once per out-of-window row; every one is counted in the ledger.
    logging.getLogger("app.providers.implementations.sec_fundamentals").setLevel(logging.ERROR)
    manifest = build(args.companyfacts, args.submissions, args.out)
    with (args.out / MANIFEST_FILENAME).open("rb") as handle:
        manifest_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    sizes = sorted((args.out / entry["path"]).stat().st_size for entry in manifest["shards"])
    totals: Counter[str] = Counter()
    for counts in manifest["ledger"]["rows"].values():
        totals.update(counts)
    json.dump(
        {
            "manifest_sha256": manifest_sha,
            "built_on": date.today().isoformat(),
            "supported_through": manifest["supported_through"],
            "shards": len(manifest["shards"]),
            "events": sum(entry["events"] for entry in manifest["shards"]),
            "integrity_failures": Counter(f["reason"] for f in manifest["snapshot_integrity_failures"]),
            "row_outcomes": totals,
            "shard_bytes": {"max": sizes[-1] if sizes else 0, "total": sum(sizes)},
        },
        sys.stdout,
        indent=1,
        sort_keys=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
