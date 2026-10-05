"""#3609 step 1: the A/B comparator must catch the differences it exists to refuse."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from scripts import ab_3609_concept_extension as ab


class _Verified:
    def verify_all(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _loaders(monkeypatch: pytest.MonkeyPatch) -> None:
    # The fixtures are not real bundles; the loaders' own checks are covered by their own tests.
    monkeypatch.setattr(ab, "load_pit_fundamentals", lambda *_a, **_k: _Verified())
    monkeypatch.setattr(ab, "load_security_linkage", lambda *_a, **_k: _Verified())


def _event(concept: str, value: str = "1") -> dict[str, Any]:
    return {
        "taxonomy": "us-gaap",
        "concept": concept,
        "unit": "USD",
        "start": None,
        "end": "2019-12-31",
        "accn": "a1",
        "acceptance": "2020-02-01T21:00:00.000Z",
        "value": value,
        "multiplicity": 1,
    }


def _write(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value).encode()
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _bundle(root: Path, events: list[dict[str, Any]], ledger: dict[str, dict[str, int]]) -> str:
    shard = {"schema": "s", "cik": "0000000001", "events": events, "rejections": [], "accessions": []}
    sha = _write(root / "shards/CIK0000000001.json", shard)
    manifest = {
        "schema": "m",
        "input_sha256": {"companyfacts": "x"},
        "supported_through": "2026-09-23",
        "snapshot_integrity_failures": [],
        "shards": [{"cik": "0000000001", "path": "shards/CIK0000000001.json", "sha256": sha}],
        "ledger": {
            "rows": ledger,
            "no_acceptance_by_cik": {},
            "form_label_variants": {},
            "ignored_companyfacts_members": 0,
        },
    }
    return _write(root / "manifest.json", manifest)


_OLD_LEDGER = {"us-gaap/Assets/USD": {"raw": 1, "stored": 1}}
_NEW_LEDGER = {**_OLD_LEDGER, "us-gaap/OperatingExpenses/USD": {"raw": 1, "stored": 1}}


def _all_added(ledger: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    return {**ledger, **{f"us-gaap/{c}/USD": {"raw": 1, "stored": 1} for _, c in ab.ADDED_CONCEPTS}}


def test_identical_old_rows_plus_added_concepts_pass(tmp_path: Path) -> None:
    a = _bundle(tmp_path / "a", [_event("Assets")], _OLD_LEDGER)
    added = [_event(concept) for _, concept in sorted(ab.ADDED_CONCEPTS)]
    b = _bundle(tmp_path / "b", [_event("Assets"), *added], _all_added(_OLD_LEDGER))
    failures, summary = ab.compare_bundles(tmp_path / "a", a, tmp_path / "b", b)
    assert failures == []
    assert summary["added_rows"]["events:OperatingExpenses"] == 1


def test_a_changed_old_value_and_an_unlisted_concept_are_refused(tmp_path: Path) -> None:
    a = _bundle(tmp_path / "a", [_event("Assets")], _OLD_LEDGER)
    b = _bundle(
        tmp_path / "b",
        [_event("Assets", value="2"), _event("Goodwill")],
        {**_all_added(_OLD_LEDGER), "us-gaap/Goodwill/USD": {"raw": 1, "stored": 1}},
    )
    failures, _ = ab.compare_bundles(tmp_path / "a", a, tmp_path / "b", b)
    assert "0000000001: events differ on pre-existing concepts" in failures
    assert "ledger key us-gaap/Goodwill/USD is not an added concept" in failures


def test_an_added_concept_with_no_stored_events_is_refused(tmp_path: Path) -> None:
    a = _bundle(tmp_path / "a", [_event("Assets")], _OLD_LEDGER)
    b = _bundle(tmp_path / "b", [_event("Assets"), _event("OperatingExpenses")], _NEW_LEDGER)
    failures, _ = ab.compare_bundles(tmp_path / "a", a, tmp_path / "b", b)
    assert "added concept InterestPaid has no stored events" in failures
    assert not any("OperatingExpenses has no stored" in f for f in failures)
    # The ledger claiming stored rows is not enough: the shards must hold them.
    c = _bundle(tmp_path / "c", [_event("Assets")], _all_added(_OLD_LEDGER))
    failures, _ = ab.compare_bundles(tmp_path / "a", a, tmp_path / "c", c)
    assert "added concept OperatingExpenses has no stored events" in failures
    # Rows present but fewer than the ledger stored: multiplicities must account for every stored row.
    short = _all_added(_OLD_LEDGER) | {"us-gaap/OperatingExpenses/USD": {"raw": 2, "stored": 2}}
    added = [_event(concept) for _, concept in sorted(ab.ADDED_CONCEPTS)]
    d = _bundle(tmp_path / "d", [_event("Assets"), *added], short)
    failures, _ = ab.compare_bundles(tmp_path / "a", a, tmp_path / "d", d)
    assert failures == ["added concept OperatingExpenses: event multiplicities do not sum to the ledger's stored"]


def test_a_new_form_variant_key_on_an_old_concept_is_refused(tmp_path: Path) -> None:
    a = _bundle(tmp_path / "a", [_event("Assets")], _OLD_LEDGER)
    added = [_event(concept) for _, concept in sorted(ab.ADDED_CONCEPTS)]
    b = _bundle(tmp_path / "b", [_event("Assets"), *added], _all_added(_OLD_LEDGER))
    manifest_path = tmp_path / "b/manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["ledger"]["form_label_variants"] = {"us-gaap/Assets/USD": 1, "us-gaap/OperatingExpenses/USD": 1}
    b = _write(manifest_path, manifest)
    failures, _ = ab.compare_bundles(tmp_path / "a", a, tmp_path / "b", b)
    assert failures == ["ledger form_label_variants us-gaap/Assets/USD differs"]


def test_a_pin_mismatch_stops_the_comparison(tmp_path: Path) -> None:
    a = _bundle(tmp_path / "a", [_event("Assets")], _OLD_LEDGER)
    with pytest.raises(SystemExit, match="does not match its pin"):
        ab.compare_bundles(tmp_path / "a", "0" * 64, tmp_path / "a", a)


def _linkage(root: Path, document: bytes, *, policy: str, pit: str) -> str:
    (root / "series").mkdir(parents=True)
    (root / "series/1.json").write_bytes(document)
    (root / "ledger.json").write_bytes(b"{}")
    manifest = {
        "schema": "m",
        "policy": policy,
        "input_sha256": {"pit_manifest": pit, "submissions": "s"},
        "supported_through": "2024-09-27",
        "series": [{"series_id": 1, "path": "series/1.json"}, {"series_id": 2, "path": None}],
    }
    return _write(root / "manifest.json", manifest)


def test_linkage_allows_only_policy_and_pit_manifest_to_move(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ab, "load_security_linkage", lambda *_a, **_k: type("B", (), {"verify_all": lambda s: None})())
    a = _linkage(tmp_path / "a", b'{"x":1}', policy="p1", pit="m1")
    b = _linkage(tmp_path / "b", b'{"x":1}', policy="p2", pit="m2")
    failures, summary = ab.compare_linkage(tmp_path / "a", a, tmp_path / "b", b)
    assert failures == [] and summary["documents_compared"] == 1
    c = _linkage(tmp_path / "c", b'{"x":2}', policy="p2", pit="m2")
    failures, _ = ab.compare_linkage(tmp_path / "a", a, tmp_path / "c", c)
    assert failures == ["series 1 document differs"]


def _prior(root: Path) -> str:
    from scripts.build_3361_security_linkage import MANIFEST_SCHEMA

    quarter = root / "inputs/form345/insider_2006q1.zip"
    quarter.parent.mkdir(parents=True)
    quarter.write_bytes(b"zip")
    recorded: dict[str, Any] = {"quarters": {quarter.name: hashlib.sha256(b"zip").hexdigest()}}
    recorded["series_inventory"] = _write(root / "inputs/series_inventory.json", [[1, "v", "AAA"]])
    manifest = {"schema": MANIFEST_SCHEMA, "form25_mode": False, "input_sha256": recorded}
    return _write(root / "manifest.json", manifest)


def test_replay_reads_the_prior_inputs_and_refuses_tampering(tmp_path: Path) -> None:
    from scripts.build_3361_security_linkage import replay_inputs

    digest = _prior(tmp_path)
    quarters, db, form25 = replay_inputs(tmp_path, digest)
    assert [q.name for q in quarters] == ["insider_2006q1.zip"]
    assert db == {"series_inventory": [[1, "v", "AAA"]]} and form25 is False
    with pytest.raises(RuntimeError, match="prior manifest digest"):
        replay_inputs(tmp_path, "0" * 64)
    (tmp_path / "inputs/form345/insider_2006q1.zip").write_bytes(b"other")
    with pytest.raises(RuntimeError, match="insider_2006q1.zip does not match"):
        replay_inputs(tmp_path, digest)


def test_added_concepts_are_the_measurements_less_xopr() -> None:
    from scripts import measure_3609_amendment_2c as m

    named = {("us-gaap", c) for c in (*m.RD_WITNESSES, *m.GA, *m.SELL_WITNESSES, *m.XINT_WITNESSES, *m.COGS_WITNESSES)}
    named |= {("us-gaap", c) for c in (*m.OPEX, *m.XOPR)}
    before = set(ab.CONCEPT_SET) - ab.ADDED_CONCEPTS
    # Pinned independently of ADDED_CONCEPTS (arm A's ledger at 6993911f): the only measurement tags held before 2c.
    # Without it, swapping an added concept for one of these would leave every assertion below true.
    held = {"InterestExpense", "CostOfGoodsAndServicesSold", "CostOfGoodsSold", "CostOfRevenue"}
    assert named & before == {("us-gaap", c) for c in held}
    # What the scratch bundle added on the pre-2c set, less the one concept only the rejected XOPR branch reads.
    assert ab.ADDED_CONCEPTS == named - before - {("us-gaap", "CostsAndExpenses")}
    assert len(ab.ADDED_CONCEPTS) == 23 and ab.ADDED_CONCEPTS <= set(ab.CONCEPT_SET)
    assert m.EXTRA_CONCEPTS == (("us-gaap", "CostsAndExpenses"),)


def test_scratch_manifest_may_differ_only_in_policy(tmp_path: Path) -> None:
    added = [_event(concept) for _, concept in sorted(ab.ADDED_CONCEPTS)]
    b = _bundle(tmp_path / "b", [_event("Assets"), *added], _all_added(_OLD_LEDGER))
    manifest = json.loads((tmp_path / "b/manifest.json").read_bytes())
    shutil.copytree(tmp_path / "b", tmp_path / "scratch")
    scratch = _write(tmp_path / "scratch/manifest.json", {**manifest, "policy": "scratch"})
    assert ab.compare_scratch(tmp_path / "scratch", scratch, tmp_path / "b", b) == []
    # A shard file on disk that no longer matches the scratch manifest refuses.
    shard_path = manifest["shards"][0]["path"]
    (tmp_path / "scratch" / shard_path).write_bytes(b"{}")
    assert ab.compare_scratch(tmp_path / "scratch", scratch, tmp_path / "b", b) == [
        f"scratch shard {shard_path} is missing or does not match its digest"
    ]
    (tmp_path / "scratch" / shard_path).unlink()
    assert ab.compare_scratch(tmp_path / "scratch", scratch, tmp_path / "b", b) == [
        f"scratch shard {shard_path} is missing or does not match its digest"
    ]
    # A different shard digest is a different shard byte.
    shutil.copy(tmp_path / "b" / shard_path, tmp_path / "scratch" / shard_path)
    manifest["shards"][0]["sha256"] = "0" * 64
    scratch = _write(tmp_path / "scratch/manifest.json", {**manifest, "policy": "scratch"})
    assert ab.compare_scratch(tmp_path / "scratch", scratch, tmp_path / "b", b) == [
        "scratch manifest shards differs",
        f"scratch shard {shard_path} is missing or does not match its digest",
    ]
