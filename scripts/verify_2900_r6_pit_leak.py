"""Run the adversarial #2900 leak test against one frozen R6 bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from app.services.r6_dilution_exclusion import NsiInput, assign_nsi_portfolios
from app.services.r6_pit_bundle import R6PitBundleError, load_r6_pit_bundle


def _rank(manifest: Path, expected_sha256: str, formation: datetime) -> tuple[str, dict[str, int | None]]:
    bundle = load_r6_pit_bundle(manifest, expected_manifest_sha256=expected_sha256)
    rows = tuple(
        NsiInput(
            symbol=row.symbol,
            exchange=row.exchange,
            current_shares=row.current_shares,
            prior_shares=row.prior_shares,
            red_flag_scores=row.red_flag_scores,
            red_flag_history_complete=row.red_flag_history_complete,
        )
        for row in bundle.records_at(formation)
    )
    ranks = assign_nsi_portfolios(rows, nyse_exchange_names=frozenset({"NYSE"}))
    return bundle.ranking_input_hash(formation), ranks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--formation", type=datetime.fromisoformat, required=True)
    args = parser.parse_args()

    before_hash, before_ranks = _rank(args.manifest, args.manifest_sha256, args.formation)
    with tempfile.TemporaryDirectory(prefix="r6-2900-leak-") as raw_temp:
        temporary = Path(raw_temp)
        copied_manifest = temporary / args.manifest.name
        manifest_document = json.loads(args.manifest.read_text(encoding="utf-8"))
        payload_name = str(manifest_document["payload"]["filename"])
        copied_payload = temporary / payload_name
        shutil.copyfile(args.manifest, copied_manifest)
        shutil.copyfile(args.manifest.parent / payload_name, copied_payload)

        # Simulate a later ingest arriving beside the evidence path.  The read
        # API has no discovery path to it and must return byte-identical ranks.
        (temporary / "later-ingest.json").write_text(
            json.dumps(
                {
                    "accepted_at": "2099-01-01T00:00:00",
                    "current_shares": "999999999999",
                    "symbol": next(iter(sorted(before_ranks))),
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        after_hash, after_ranks = _rank(copied_manifest, args.manifest_sha256, args.formation)
        if (after_hash, after_ranks) != (before_hash, before_ranks):
            raise RuntimeError("FAIL: historical ranking moved after later ingest")

        payload = json.loads(copied_payload.read_text(encoding="utf-8"))
        payload["records"][0]["current_shares"] = "999999999999"
        copied_payload.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        manifest_document["payload"]["sha256"] = hashlib.sha256(copied_payload.read_bytes()).hexdigest()
        copied_manifest.write_text(json.dumps(manifest_document, sort_keys=True), encoding="utf-8")
        overwrite_refused = False
        try:
            _rank(copied_manifest, args.manifest_sha256, args.formation)
        except R6PitBundleError:
            overwrite_refused = True
        if not overwrite_refused:
            raise RuntimeError("FAIL: rewritten bundle was accepted under the frozen manifest hash")

    assigned = sum(rank is not None for rank in before_ranks.values())
    print(
        json.dumps(
            {
                "formation_close": args.formation.isoformat(),
                "later_ingest_rank_unchanged": True,
                "manifest_sha256": args.manifest_sha256,
                "ranked_securities": assigned,
                "ranking_input_sha256": before_hash,
                "result": "PASS",
                "rewritten_bundle_refused": True,
                "unranked_neutral_securities": len(before_ranks) - assigned,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
