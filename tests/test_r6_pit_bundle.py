from __future__ import annotations

import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

import app.services.r6_pit_bundle as bundle_module
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


def _replace_after_verification(monkeypatch: pytest.MonkeyPatch, target: str, mutate) -> None:
    """Land a writer in the window the loader used to have (#2945).

    The injection point is the same one the audit used: the instant the digest
    for ``target`` has been taken. Before the fix a replacement here was parsed
    under the old digest; after it the bytes are already in hand, so it cannot be.
    """
    original = bundle_module.read_verified_document

    def racing(path: Path) -> tuple[str, bytes]:
        result = original(path)
        if path.name == target:
            mutate(path)
        return result

    monkeypatch.setattr(bundle_module, "read_verified_document", racing)


def test_payload_replaced_after_its_digest_is_taken_cannot_change_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    payload = tmp_path / "payload.json"
    verified_bytes = payload.read_bytes()

    def swap(path: Path) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        document["records"][0]["current_shares"] = "240"
        path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    _replace_after_verification(monkeypatch, "payload.json", swap)

    loaded = load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)

    assert loaded.records[0].current_shares == Decimal("120")
    # The reported digest must describe the bytes that produced the records --
    # not the bytes now sitting at the pathname.
    assert loaded.payload_sha256 == hashlib.sha256(verified_bytes).hexdigest()
    assert loaded.payload_sha256 != hashlib.sha256(payload.read_bytes()).hexdigest()


def test_manifest_replaced_after_its_digest_is_taken_cannot_repoint_the_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    decoy = tmp_path / "decoy.json"
    decoy_document = json.loads((tmp_path / "payload.json").read_text(encoding="utf-8"))
    decoy_document["records"][0]["current_shares"] = "999"
    decoy.write_text(json.dumps(decoy_document, sort_keys=True), encoding="utf-8")

    def repoint(path: Path) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        document["payload"] = {"filename": decoy.name, "sha256": hashlib.sha256(decoy.read_bytes()).hexdigest()}
        path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    _replace_after_verification(monkeypatch, "manifest.json", repoint)

    loaded = load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)

    assert loaded.records[0].current_shares == Decimal("120")
    assert loaded.manifest_sha256 == manifest_sha


def test_symlinked_evidence_is_refused(tmp_path: Path) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    payload = tmp_path / "payload.json"
    real = tmp_path / "elsewhere.json"
    real.write_bytes(payload.read_bytes())
    payload.unlink()
    payload.symlink_to(real)

    with pytest.raises(R6PitBundleError, match="regular non-symlink file"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)


def test_document_over_the_memory_ceiling_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, manifest_sha = _write_bundle(tmp_path)
    monkeypatch.setattr(bundle_module, "MAX_EVIDENCE_BYTES", 8)

    with pytest.raises(R6PitBundleError, match="exceeds 8 bytes"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)


def test_non_json_evidence_refuses_as_a_bundle_error(tmp_path: Path) -> None:
    manifest, _ = _write_bundle(tmp_path)
    manifest.write_bytes(b"\xff\xfe not json")
    changed = hashlib.sha256(manifest.read_bytes()).hexdigest()

    with pytest.raises(R6PitBundleError, match="manifest must be a UTF-8 JSON document"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=changed)


def test_read_verified_document_returns_the_digest_of_the_bytes_it_returns(tmp_path: Path) -> None:
    document = tmp_path / "evidence.json"
    document.write_bytes(b'{"a":1}')

    digest, data = bundle_module.read_verified_document(document)

    assert data == b'{"a":1}'
    assert digest == hashlib.sha256(data).hexdigest()


def test_fifo_evidence_is_refused_instead_of_blocking(tmp_path: Path) -> None:
    """A FIFO with no writer must refuse, not hang.

    ``os.open`` on a FIFO blocks until a writer appears, and that happens before
    the ``fstat`` that rejects it -- so the non-blocking flag is what keeps a
    planted FIFO from stalling the loader indefinitely.
    """
    manifest, manifest_sha = _write_bundle(tmp_path)
    payload = tmp_path / "payload.json"
    payload.unlink()
    os.mkfifo(payload)

    with pytest.raises(R6PitBundleError, match="regular non-symlink file"):
        load_r6_pit_bundle(manifest, expected_manifest_sha256=manifest_sha)
