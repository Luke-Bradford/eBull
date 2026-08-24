"""Build the immutable, outcome-blind R6 #2900 ranking bundle.

Only retained census JSON is accepted.  This builder never opens a price
value: exact formation-session availability was established by the cover
census from CSV date columns alone.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Final, cast
from uuid import uuid4

from app.services.r6_pit_bundle import MANIFEST_SCHEMA, PAYLOAD_SCHEMA
from app.services.r6_pit_universe import common_equity_reason

BUILDER_VERSION: Final = "r6-2900-pit-builder-v1"
PAYLOAD_FILENAME: Final = "r6-2900-pit-payload.json"
MANIFEST_FILENAME: Final = "r6-2900-pit-manifest.json"


def _sha256(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        raise RuntimeError(f"source must be a regular non-symlink file: {path}")
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _load_pinned(path: Path, expected_sha256: str) -> dict[str, Any]:
    measured = _sha256(path)
    if measured != expected_sha256:
        raise RuntimeError(f"source digest moved for {path}: expected {expected_sha256}, measured {measured}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"source must contain a JSON object: {path}")
    return value


def _formation_map(document: dict[str, Any], *, collection: str) -> dict[str, dict[str, Any]]:
    raw = document.get(collection)
    valid = isinstance(raw, dict) and all(
        isinstance(key, str) and isinstance(value, dict) for key, value in raw.items()
    )
    if not valid:
        raise RuntimeError(f"{collection} must be an object of formation objects")
    return cast(dict[str, dict[str, Any]], raw)


def _write_exclusive(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            raise RuntimeError(f"refusing to overwrite frozen evidence: {path}")
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_bundle(
    *,
    cover: dict[str, Any],
    shares: dict[str, Any],
    red_flags: dict[str, Any],
    source_hashes: dict[str, str],
    code_hashes: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    identities = _formation_map(cover, collection="formation_identity_census")
    share_formations = _formation_map(shares, collection="formations")
    flag_formations = _formation_map(red_flags, collection="formations")
    if set(identities) != set(share_formations) or set(identities) != set(flag_formations):
        raise RuntimeError("cover, share, and red-flag formation sets differ")

    records: list[dict[str, object]] = []
    formation_census: dict[str, object] = {}
    for formation in sorted(identities):
        identity = identities[formation]
        raw_records = identity.get("records")
        duplicates = identity.get("duplicate_normalized_symbol_ciks")
        if not isinstance(raw_records, list) or not isinstance(duplicates, dict):
            raise RuntimeError(f"malformed cover census at {formation}")
        share_rows = share_formations[formation].get("complete_pairs")
        flag_rows = flag_formations[formation].get("records")
        if not isinstance(share_rows, list) or not isinstance(flag_rows, list):
            raise RuntimeError(f"malformed signal census at {formation}")
        share_by_cik = {str(row["cik"]): row for row in share_rows}
        flag_by_cik = {str(row["cik"]): row for row in flag_rows}
        if len(flag_by_cik) != len(flag_rows):
            raise RuntimeError(f"duplicate red-flag CIK at {formation}")

        candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
        reasons: Counter[str] = Counter()
        for raw in raw_records:
            if not isinstance(raw, dict):
                raise RuntimeError(f"non-object identity row at {formation}")
            reason = common_equity_reason(
                security_title=str(raw.get("security_title", "")),
                exchange=str(raw.get("exchange", "")),
            )
            if reason != "included_common_equity":
                reasons[reason] += 1
                continue
            if raw.get("price_filename_match") is not True or raw.get("exact_formation_session_bar") is not True:
                reasons["missing_exact_formation_bar"] += 1
                continue
            symbol = str(raw.get("normalized_price_symbol", ""))
            if symbol in duplicates:
                reasons["symbol_shared_across_ciks"] += 1
                continue
            candidates[symbol].append(raw)

        included = 0
        with_shares = 0
        for symbol, rows in sorted(candidates.items()):
            triples = {(str(row.get("cik")), str(row.get("security_title")), str(row.get("exchange"))) for row in rows}
            if len(triples) != 1:
                reasons["ambiguous_same_symbol_context"] += len(rows)
                continue
            raw = rows[0]
            cik = str(raw["cik"])
            flag = flag_by_cik.get(cik)
            if flag is None:
                raise RuntimeError(f"missing red-flag census for {cik} at {formation}")
            pair = share_by_cik.get(cik)
            share_accepted = None
            current_shares = None
            prior_shares = None
            if pair is not None:
                share_accepted = str(pair["accepted"])
                current_shares = str(pair["current_shares"])
                prior_shares = str(pair["prior_shares"])
                with_shares += 1
            scores = flag.get("red_flag_scores")
            complete = flag.get("complete_recent_history")
            if not isinstance(scores, list) or type(complete) is not bool:
                raise RuntimeError(f"malformed red-flag row for {cik} at {formation}")
            records.append(
                {
                    "cik": cik,
                    "current_shares": current_shares,
                    "exchange": str(raw["exchange"]),
                    "formation_close": formation,
                    "identity_accepted_at": str(raw["accepted_at"]),
                    "prior_shares": prior_shares,
                    "red_flag_history_complete": complete,
                    "red_flag_scores": scores,
                    "security_title": str(raw["security_title"]),
                    "share_accepted_at": share_accepted,
                    "symbol": symbol,
                }
            )
            included += 1
        formation_census[formation] = {
            "included_common_equities": included,
            "included_with_share_pairs": with_shares,
            "neutral_missing_share_pairs": included - with_shares,
            "exclusion_reasons": dict(sorted(reasons.items())),
        }

    records.sort(key=lambda row: (str(row["formation_close"]), str(row["symbol"])))
    payload: dict[str, Any] = {
        "builder_version": BUILDER_VERSION,
        "code_sha256": dict(sorted(code_hashes.items())),
        "formation_census": formation_census,
        "records": records,
        "schema_version": PAYLOAD_SCHEMA,
        "source_sha256": dict(sorted(source_hashes.items())),
    }
    return payload, formation_census


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cover", type=Path, required=True)
    parser.add_argument("--cover-sha256", required=True)
    parser.add_argument("--shares", type=Path, required=True)
    parser.add_argument("--shares-sha256", required=True)
    parser.add_argument("--red-flags", type=Path, required=True)
    parser.add_argument("--red-flags-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    pinned = {
        "cover": (args.cover, args.cover_sha256),
        "red_flags": (args.red_flags, args.red_flags_sha256),
        "shares": (args.shares, args.shares_sha256),
    }
    documents = {name: _load_pinned(path, digest) for name, (path, digest) in pinned.items()}
    code_paths = {
        "builder": Path(__file__),
        "bundle_loader": Path("app/services/r6_pit_bundle.py"),
        "universe_classifier": Path("app/services/r6_pit_universe.py"),
    }
    code_hashes = {name: _sha256(path) for name, path in code_paths.items()}
    payload, census = build_bundle(
        cover=documents["cover"],
        shares=documents["shares"],
        red_flags=documents["red_flags"],
        source_hashes={name: digest for name, (_, digest) in pinned.items()},
        code_hashes=code_hashes,
    )
    payload_path = args.output_dir / PAYLOAD_FILENAME
    _write_exclusive(payload_path, payload)
    payload_sha = _sha256(payload_path)
    manifest = {
        "builder_version": BUILDER_VERSION,
        "payload": {"filename": PAYLOAD_FILENAME, "sha256": payload_sha},
        "schema_version": MANIFEST_SCHEMA,
    }
    manifest_path = args.output_dir / MANIFEST_FILENAME
    _write_exclusive(manifest_path, manifest)
    print(
        json.dumps(
            {
                "formation_census": census,
                "manifest": str(manifest_path),
                "manifest_sha256": _sha256(manifest_path),
                "payload": str(payload_path),
                "payload_sha256": payload_sha,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
