from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from app.services.r6_pit_bundle import (
    MANIFEST_SCHEMA,
    PAYLOAD_SCHEMA,
    R6PitBundleError,
    load_r6_pit_bundle,
)


def _write_bundle(root: Path) -> tuple[Path, str]:
    payload = {
        "schema_version": PAYLOAD_SCHEMA,
        "records": [
            {
                "cik": "0000000001",
                "current_shares": "120",
                "exchange": "NYSE",
                "formation_close": "2022-06-30T16:00:00",
                "identity_accepted_at": "2022-02-01T12:00:00",
                "prior_shares": "100",
                "red_flag_history_complete": True,
                "red_flag_scores": [],
                "security_title": "Common Stock",
                "share_accepted_at": "2022-02-01T12:00:00",
                "symbol": "TEST",
            }
        ],
    }
    payload_path = root / "payload.json"
    payload_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    digest = hashlib.sha256(payload_path.read_bytes()).hexdigest()
    manifest = {
        "payload": {"filename": payload_path.name, "sha256": digest},
        "schema_version": MANIFEST_SCHEMA,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path, hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def test_later_external_ingest_cannot_move_historical_ranking(tmp_path: Path) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    before = load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)
    mutable_ingest = tmp_path / "later-ingest.json"
    mutable_ingest.write_text(
        json.dumps({"identity_accepted_at": "2024-01-01T12:00:00", "symbol": "TEST"}),
        encoding="utf-8",
    )

    after = load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)

    formation = before.records[0].formation_close
    assert before.ranking_input_hash(formation) == after.ranking_input_hash(formation)


def test_overwriting_pinned_payload_after_later_ingest_fails_loudly(tmp_path: Path) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)
    payload = tmp_path / "payload.json"
    payload.write_text(payload.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(R6PitBundleError, match="payload digest moved"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)


def test_postdated_record_is_refused_even_with_a_matching_digest(tmp_path: Path) -> None:
    manifest, _ = _write_bundle(tmp_path)
    payload = tmp_path / "payload.json"
    document = json.loads(payload.read_text(encoding="utf-8"))
    document["records"][0]["identity_accepted_at"] = "2022-07-01T12:00:00"
    payload.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    manifest_document = json.loads(manifest.read_text(encoding="utf-8"))
    manifest_document["payload"]["sha256"] = hashlib.sha256(payload.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(manifest_document, sort_keys=True), encoding="utf-8")

    changed_manifest_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
    with pytest.raises(R6PitBundleError, match="identity accepted at .* after formation"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=changed_manifest_sha)


def test_repointing_manifest_to_rewritten_payload_fails_frozen_manifest_hash(tmp_path: Path) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    payload = tmp_path / "payload.json"
    document = json.loads(payload.read_text(encoding="utf-8"))
    document["records"][0]["current_shares"] = "999"
    payload.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    manifest_document = json.loads(manifest.read_text(encoding="utf-8"))
    manifest_document["payload"]["sha256"] = hashlib.sha256(payload.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(manifest_document, sort_keys=True), encoding="utf-8")

    with pytest.raises(R6PitBundleError, match="manifest digest moved"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)
