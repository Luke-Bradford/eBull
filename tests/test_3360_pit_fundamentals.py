"""#3360 acceptance item 1: pure leak fixtures for the PIT fundamentals bundle.

Each test builds a tiny companyfacts + submissions archive pair, runs the real builder,
loads the bundle through the policy-bound loader and reads it. Spec:
``docs/proposals/ta/2026-09-24-3360-companyfacts-pit-bundle.md``.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.services import pit_fundamentals as pf
from app.services.pit_fundamentals import FactKey, ReadStatus, canonical_decimal, load_pit_fundamentals
from app.services.r6_pit_bundle import R6PitBundleError
from scripts.build_3360_pit_fundamentals import build

CIK = "0000000001"
ASSETS = FactKey("us-gaap", "Assets", "USD", None, "2019-12-31")
REVENUE = FactKey("us-gaap", "Revenues", "USD", "2019-01-01", "2019-12-31")
# 2020-02-10 21:00Z = 16:00 New York (EST): NY date 2020-02-10.
T1 = "2020-02-10T21:00:00.000Z"
T2 = "2020-06-01T20:00:00.000Z"


def _row(val: Any, accn: str, *, end: str = "2019-12-31", start: str | None = None, **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"end": end, "val": val, "accn": accn, "filed": "2020-02-10"}
    if start is not None:
        row["start"] = start
    row.update(extra)
    return row


def _facts(rows: dict[tuple[str, str, str], list[Any]], cik: str = CIK) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for (taxonomy, concept, unit), unit_rows in rows.items():
        for row in unit_rows:
            row.setdefault("form", "10-K")
        facts.setdefault(taxonomy, {}).setdefault(concept, {"units": {}})["units"][unit] = unit_rows
    return {"cik": int(cik), "entityName": "X", "facts": facts}


def _block(filings: list[tuple[str, str, str]]) -> dict[str, list[str]]:
    return {
        "accessionNumber": [f[0] for f in filings],
        "acceptanceDateTime": [f[1] for f in filings],
        "form": [f[2] for f in filings],
        "items": ["" for _ in filings],
    }


def _subs(filings: list[tuple[str, str, str]], cik: str = CIK, pages: list[str] | None = None) -> dict[str, Any]:
    return {
        "cik": cik,
        "filings": {"recent": _block(filings), "files": [{"name": name} for name in pages or []]},
    }


def _zip(path: Path, members: dict[str, Any]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, body in members.items():
            text = body if isinstance(body, str) else json.dumps(body)
            archive.writestr(name, text)
    return path


#: A second issuer with a far-future filing, so ``supported_through`` (the earlier of the
#: two archives' maximum acceptance date) does not clip the reads under test.
FILLER = "0000000099"


def _build(
    tmp_path: Path,
    facts: dict[str, Any],
    subs: dict[str, Any],
    name: str = "bundle",
    *,
    filler: bool = True,
) -> tuple[pf.PitFundamentalsBundle, dict[str, Any]]:
    src = tmp_path / f"src-{name}"
    src.mkdir()
    if filler:
        facts = {**facts, f"CIK{FILLER}.json": _facts({A: [_row(1, "f1", form="10-K")]}, FILLER)}
        subs = {**subs, f"CIK{FILLER}.json": _subs([("f1", "2030-01-02T15:00:00.000Z", "10-K")], FILLER)}
    companyfacts = _zip(src / "companyfacts.zip", facts)
    submissions = _zip(src / "submissions.zip", subs)
    manifest = build(companyfacts, submissions, tmp_path / name)
    return load_pit_fundamentals(tmp_path / name, expected_manifest_sha256=_sha(tmp_path / name)), manifest


def _sha(root: Path) -> str:
    return hashlib.sha256((root / pf.MANIFEST_FILENAME).read_bytes()).hexdigest()


def _one(tmp_path: Path, rows: dict[tuple[str, str, str], list[Any]], filings: list[tuple[str, str, str]]):
    # A row's companyfacts ``form`` defaults to its accession's submissions form; tests
    # that exercise ``form_mismatch`` set it explicitly.
    forms = {accn: form for accn, _, form in filings}
    for unit_rows in rows.values():
        for row in unit_rows:
            row.setdefault("form", forms.get(row["accn"], "10-K"))
    return _build(tmp_path, {f"CIK{CIK}.json": _facts(rows)}, {f"CIK{CIK}.json": _subs(filings)})


A: tuple[str, str, str] = ("us-gaap", "Assets", "USD")
R: tuple[str, str, str] = ("us-gaap", "Revenues", "USD")


def test_restatement_is_public_only_from_the_next_ny_date(tmp_path: Path) -> None:
    bundle, _ = _one(tmp_path, {A: [_row(100, "a1"), _row(110, "a2")]}, [("a1", T1, "10-K"), ("a2", T2, "10-K/A")])
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 2, 10)).status is ReadStatus.ABSENT
    first = bundle.value_as_of(CIK, ASSETS, date(2020, 2, 11))
    assert (first.status, first.values, first.accns) == (ReadStatus.VALUE, ("100",), ("a1",))
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 6, 1)).values == ("100",)
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 6, 2)).values == ("110",)


def test_partial_amendment_leaves_untouched_keys_on_the_original(tmp_path: Path) -> None:
    bundle, _ = _one(
        tmp_path,
        {A: [_row(100, "a1")], R: [_row(50, "a1", start="2019-01-01"), _row(55, "a2", start="2019-01-01")]},
        [("a1", T1, "10-K"), ("a2", T2, "10-K/A")],
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 7, 1)).values == ("100",)
    assert bundle.value_as_of(CIK, REVENUE, date(2020, 7, 1)).values == ("55",)


def test_comparative_only_period_appears_with_its_carrier(tmp_path: Path) -> None:
    prior = FactKey("us-gaap", "Assets", "USD", None, "2018-12-31")
    bundle, _ = _one(tmp_path, {A: [_row(90, "a1", end="2018-12-31")]}, [("a1", T1, "10-K")])
    assert bundle.value_as_of(CIK, prior, date(2019, 6, 1)).status is ReadStatus.ABSENT
    assert bundle.value_as_of(CIK, prior, date(2020, 2, 11)).values == ("90",)


def test_same_day_later_timestamp_wins_and_exact_tie_is_ambiguous(tmp_path: Path) -> None:
    early, late = "2020-06-01T14:00:00.000Z", "2020-06-01T19:00:00.000Z"
    bundle, _ = _one(
        tmp_path,
        {
            A: [_row(100, "a1"), _row(101, "a2")],
            R: [_row(1, "a1", start="2019-01-01"), _row(2, "a3", start="2019-01-01")],
        },
        [("a1", early, "10-K"), ("a2", late, "10-K/A"), ("a3", early, "10-K/A")],
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 6, 2)).values == ("101",)
    tie = bundle.value_as_of(CIK, REVENUE, date(2020, 6, 2))
    assert (tie.status, tie.values, tie.accns) == (ReadStatus.AMBIGUOUS, ("1", "2"), ("a1", "a3"))


def test_frame_fy_fp_and_filed_are_inert(tmp_path: Path) -> None:
    plain: dict[tuple[str, str, str], list[Any]] = {A: [_row(100, "a1")]}
    decorated: dict[tuple[str, str, str], list[Any]] = {
        A: [_row(100, "a1", frame="CY2019Q4I", fy=2019, fp="FY", filed="2020-01-01")]
    }
    filings = [("a1", T1, "10-K")]
    one = _build(tmp_path, {f"CIK{CIK}.json": _facts(plain)}, {f"CIK{CIK}.json": _subs(filings)}, "one")[1]
    two = _build(tmp_path, {f"CIK{CIK}.json": _facts(decorated)}, {f"CIK{CIK}.json": _subs(filings)}, "two")[1]
    assert one["shards"][0]["sha256"] == two["shards"][0]["sha256"]


def test_filed_before_acceptance_is_public_only_after_acceptance(tmp_path: Path) -> None:
    bundle, _ = _one(
        tmp_path,
        {A: [_row(7, "a1", filed="2010-05-04", end="2010-03-31")]},
        [("a1", "2010-06-09T20:00:00.000Z", "10-Q")],
    )
    key = FactKey("us-gaap", "Assets", "USD", None, "2010-03-31")
    assert bundle.value_as_of(CIK, key, date(2010, 5, 10)).status is ReadStatus.ABSENT
    assert bundle.value_as_of(CIK, key, date(2010, 6, 10)).values == ("7",)


def test_ambiguity_then_recovery_by_a_later_single_value(tmp_path: Path) -> None:
    bundle, _ = _one(
        tmp_path, {A: [_row(100, "a1"), _row(105, "a1"), _row(110, "a2")]}, [("a1", T1, "10-K"), ("a2", T2, "10-K/A")]
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 3, 1)).status is ReadStatus.AMBIGUOUS
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 6, 2)).values == ("110",)


def test_blocking_rejection_at_latest_and_on_a_tie_never_falls_back(tmp_path: Path) -> None:
    bundle, manifest = _one(
        tmp_path,
        {
            A: [_row(100, "a1"), _row("not-a-number", "a2")],
            R: [_row(50, "a1", start="2019-01-01"), _row("bad", "a1", start="2019-01-01")],
        },
        [("a1", T1, "10-K"), ("a2", T2, "10-K/A")],
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 3, 1)).values == ("100",)
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 6, 2)).status is ReadStatus.BLOCKED_BY_REJECTION
    assert bundle.value_as_of(CIK, REVENUE, date(2020, 3, 1)).status is ReadStatus.BLOCKED_BY_REJECTION
    assert manifest["ledger"]["rows"]["us-gaap/Assets/USD"]["chokepoint_reject"] == 1


def test_rejected_only_key_is_blocked_not_absent(tmp_path: Path) -> None:
    bundle, _ = _one(tmp_path, {A: [_row(None, "a1")]}, [("a1", T1, "10-K")])
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 3, 1)).status is ReadStatus.BLOCKED_BY_REJECTION


def test_out_of_scope_8k_row_does_not_block(tmp_path: Path) -> None:
    bundle, manifest = _one(
        tmp_path, {A: [_row(100, "a1"), _row(999, "e1", form="8-K")]}, [("a1", T1, "10-K"), ("e1", T2, "8-K")]
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 7, 1)).values == ("100",)
    assert manifest["ledger"]["rows"]["us-gaap/Assets/USD"]["out_of_scope_form"] == 1
    assert [a["accn"] for a in bundle.public_events(CIK, "us-gaap", "Assets", date(2020, 7, 1)).accessions] == ["a1"]


def test_prospective_period_is_a_blocking_rejection(tmp_path: Path) -> None:
    future = FactKey("us-gaap", "Assets", "USD", None, "2020-03-31")
    bundle, manifest = _one(tmp_path, {A: [_row(1, "a1", end="2020-03-31")]}, [("a1", T1, "10-K")])
    assert bundle.value_as_of(CIK, future, date(2020, 7, 1)).status is ReadStatus.BLOCKED_BY_REJECTION
    assert manifest["ledger"]["rows"]["us-gaap/Assets/USD"]["prospective_period"] == 1


def test_form_mismatch_between_archives_blocks(tmp_path: Path) -> None:
    bundle, manifest = _one(
        tmp_path, {A: [_row(100, "a1"), _row(120, "a2", form="10-K")]}, [("a1", T1, "10-K"), ("a2", T2, "10-Q")]
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 7, 1)).status is ReadStatus.BLOCKED_BY_REJECTION
    assert manifest["ledger"]["rows"]["us-gaap/Assets/USD"]["form_mismatch"] == 1


def test_base_form_label_on_an_amendment_is_stored_not_blocked(tmp_path: Path) -> None:
    # Measured 2026-09-24: 3,214 amendment/transition accessions carry their base form as the
    # companyfacts label. Same family, so the restated value is stored and ledgered.
    bundle, manifest = _one(
        tmp_path,
        {A: [_row(100, "a1"), _row(110, "a2", form="10-K"), _row(120, "a3", form="10-K")]},
        [("a1", T1, "10-K"), ("a2", T2, "10-K/A"), ("a3", "2020-09-01T20:00:00.000Z", "10-KT")],
    )
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 7, 1)).values == ("110",)
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 9, 2)).values == ("120",)
    assert manifest["ledger"]["form_label_variants"] == {"us-gaap/Assets/USD": 2}


def test_crashed_build_leaves_nothing_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.build_3360_pit_fundamentals as builder

    def boom(*_: Any, **__: Any) -> Any:
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(builder, "build_shard", boom)
    with pytest.raises(RuntimeError, match="simulated crash"):
        _one(tmp_path, {A: [_row(100, "a1")]}, [("a1", T1, "10-K")])
    assert not (tmp_path / "bundle").exists()


def test_no_acceptance_and_unit_outside_policy_are_ledgered_not_stored(tmp_path: Path) -> None:
    eur = ("us-gaap", "Assets", "EUR")
    bundle, manifest = _one(
        tmp_path,
        {A: [_row(100, "a1"), _row(5, "ghost")], eur: [_row(3, "a1"), _row("bad", "a1")]},
        [("a1", T1, "10-K")],
    )
    rows = manifest["ledger"]["rows"]
    assert rows["us-gaap/Assets/USD"] == {"raw": 3, "stored": 2, "no_acceptance": 1}  # incl. FILLER
    assert rows["us-gaap/Assets/EUR"] == {"raw": 2, "unit_outside_policy": 1, "chokepoint_reject": 1}
    assert manifest["ledger"]["no_acceptance_by_cik"] == {CIK: 1}
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 3, 1)).values == ("100",)


@pytest.mark.parametrize(
    ("subs", "reason"),
    [
        (_subs([("a1", T1, "10-K")], pages=["CIK0000000001-submissions-001.json"]), "submissions_page_missing"),
        (_subs([("a1", T1, "10-K"), ("a1", T2, "10-K")]), "submissions_accession_conflict"),
        (_subs([("a1", T1, "10-K")], cik="0000000002"), "submissions_cik_mismatch"),
        (
            {"cik": CIK, "filings": {"recent": {**_block([("a1", T1, "10-K")]), "items": []}, "files": []}},
            "submissions_column_length_mismatch",
        ),
    ],
)
def test_snapshot_integrity_excludes_the_whole_cik(tmp_path: Path, subs: dict[str, Any], reason: str) -> None:
    bundle, manifest = _build(
        tmp_path,
        {f"CIK{CIK}.json": _facts({A: [_row(100, "a1")]}), "CIK0000000003.json": _facts({A: [_row(1, "b1")]}, "3")},
        {f"CIK{CIK}.json": subs, "CIK0000000003.json": _subs([("b1", T1, "10-K")], cik="3")},
    )
    assert manifest["snapshot_integrity_failures"] == [{"cik": CIK, "reason": reason}]
    assert manifest["ledger"]["rows"]["us-gaap/Assets/USD"]["integrity_excluded"] == 1
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 3, 1)).status is ReadStatus.CIK_INTEGRITY_EXCLUDED
    assert bundle.value_as_of("0000000009", ASSETS, date(2020, 3, 1)).status is ReadStatus.CIK_NOT_IN_BUNDLE


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("1", "1"),
        ("1.0", "1"),
        ("-0", "0"),
        ("-0.00", "0"),
        ("1E+3", "1000"),
        ("0.0012300", "0.00123"),
        ("-12345678901234567890.1230", "-12345678901234567890.123"),
        ("100", "100"),
        ("12.5", "12.5"),
    ],
)
def test_canonical_decimal(raw: str, canonical: str) -> None:
    assert canonical_decimal(Decimal(raw)) == canonical


def test_decimal_spellings_collapse_to_one_event_with_multiplicity(tmp_path: Path) -> None:
    text = json.dumps(_facts({A: [_row("@1", "a1"), _row("@2", "a1"), _row("@3", "a1")]}))
    text = text.replace('"@1"', "1").replace('"@2"', "1.0").replace('"@3"', "1.00000000000000000000000001")
    bundle, _ = _build(tmp_path, {f"CIK{CIK}.json": text}, {f"CIK{CIK}.json": _subs([("a1", T1, "10-K")])})
    read = bundle.value_as_of(CIK, ASSETS, date(2020, 3, 1))
    assert read.status is ReadStatus.AMBIGUOUS
    assert read.values == ("1", "1.00000000000000000000000001")
    events = bundle.public_events(CIK, "us-gaap", "Assets", date(2020, 3, 1)).events
    assert [(e["value"], e["multiplicity"]) for e in events] == [("1", 2), ("1.00000000000000000000000001", 1)]


def test_gates_after_capture_and_concept_not_in_policy(tmp_path: Path) -> None:
    bundle, manifest = _build(
        tmp_path,
        {f"CIK{CIK}.json": _facts({A: [_row(100, "a1")]})},
        {f"CIK{CIK}.json": _subs([("a1", T1, "10-K")])},
        filler=False,
    )
    assert manifest["supported_through"] == "2020-02-10"
    assert bundle.value_as_of(CIK, ASSETS, date(2020, 2, 11)).status is ReadStatus.AFTER_CAPTURE
    other = FactKey("us-gaap", "Liabilities", "USD", None, "2019-12-31")
    assert bundle.value_as_of(CIK, other, date(2020, 2, 1)).status is ReadStatus.CONCEPT_NOT_IN_POLICY


def test_public_events_is_the_unranked_public_prefix(tmp_path: Path) -> None:
    bundle, _ = _one(
        tmp_path,
        {A: [_row(100, "a1"), _row(90, "a1", end="2018-12-31"), _row("x", "a2"), _row(110, "a3")]},
        [("a1", T1, "10-K"), ("a2", T2, "10-K/A"), ("a3", "2021-01-05T15:00:00.000Z", "10-K/A")],
    )
    prefix = bundle.public_events(CIK, "us-gaap", "Assets", date(2020, 6, 2))
    assert prefix.status is ReadStatus.OK
    assert [(e["end"], e["value"]) for e in prefix.events] == [("2018-12-31", "90"), ("2019-12-31", "100")]
    assert [(r["accn"], r["reason"]) for r in prefix.rejections] == [("a2", "chokepoint_reject")]
    assert [a["accn"] for a in prefix.accessions] == ["a1", "a2"]


def test_rebuild_is_byte_deterministic(tmp_path: Path) -> None:
    facts = {f"CIK{CIK}.json": _facts({A: [_row(100, "a1"), _row(1, "a2")], R: [_row(5, "a1", start="2019-01-01")]})}
    subs = {f"CIK{CIK}.json": _subs([("a2", T2, "10-K/A"), ("a1", T1, "10-K")])}
    _build(tmp_path, facts, subs, "one")
    _build(tmp_path, facts, subs, "two")
    for relative in (pf.MANIFEST_FILENAME, f"shards/CIK{CIK}.json"):
        assert (tmp_path / "one" / relative).read_bytes() == (tmp_path / "two" / relative).read_bytes()


def test_existing_bundle_directory_is_refused(tmp_path: Path) -> None:
    _one(tmp_path, {A: [_row(100, "a1")]}, [("a1", T1, "10-K")])
    with pytest.raises(FileExistsError):
        build(
            tmp_path / "src-bundle" / "companyfacts.zip",
            tmp_path / "src-bundle" / "submissions.zip",
            tmp_path / "bundle",
        )


# ------------------------------------------------------------------ loader refusals


@pytest.fixture
def built(tmp_path: Path) -> Path:
    _one(tmp_path, {A: [_row(100, "a1")]}, [("a1", T1, "10-K")])
    return tmp_path / "bundle"


def _rewrite_manifest(root: Path, edit: Any) -> str:
    path = root / pf.MANIFEST_FILENAME
    manifest = json.loads(path.read_text())
    edit(manifest)
    path.write_text(json.dumps(manifest))
    return _sha(root)


def test_corrupt_shard_is_refused(built: Path) -> None:
    shard = built / "shards" / f"CIK{CIK}.json"
    shard.write_bytes(shard.read_bytes().replace(b'"100"', b'"101"'))
    bundle = load_pit_fundamentals(built, expected_manifest_sha256=_sha(built))
    with pytest.raises(pf.PitFundamentalsError, match="digest moved"):
        bundle.value_as_of(CIK, ASSETS, date(2020, 2, 10))


def test_wrong_cik_shard_is_refused(built: Path) -> None:
    shard = json.loads((built / "shards" / f"CIK{CIK}.json").read_text())
    with pytest.raises(pf.PitFundamentalsError, match="cik mismatch"):
        pf.validate_shard(shard, cik10="0000000002", expected_events=1)


def test_bad_path_is_refused(built: Path) -> None:
    sha = _rewrite_manifest(built, lambda m: m["shards"][0].update(path="../elsewhere.json"))
    with pytest.raises(pf.PitFundamentalsError, match="shard path"):
        load_pit_fundamentals(built, expected_manifest_sha256=sha)


def test_moved_manifest_is_refused(built: Path, tmp_path: Path) -> None:
    moved = tmp_path / "moved"
    moved.mkdir()
    shutil.copy(built / pf.MANIFEST_FILENAME, moved / pf.MANIFEST_FILENAME)
    bundle = load_pit_fundamentals(moved, expected_manifest_sha256=_sha(built))
    with pytest.raises(R6PitBundleError):
        bundle.value_as_of(CIK, ASSETS, date(2020, 2, 10))


def test_unpinned_manifest_digest_is_refused(built: Path) -> None:
    with pytest.raises(pf.PitFundamentalsError, match="pinned"):
        load_pit_fundamentals(built, expected_manifest_sha256="0" * 64)


def test_policy_mismatch_is_refused(built: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pf, "policy_sha256", lambda: "f" * 64)
    with pytest.raises(pf.PitFundamentalsError, match="POLICY"):
        load_pit_fundamentals(built, expected_manifest_sha256=_sha(built))


def test_event_count_mismatch_is_refused(built: Path) -> None:
    sha = _rewrite_manifest(built, lambda m: m["shards"][0].update(events=2))
    bundle = load_pit_fundamentals(built, expected_manifest_sha256=sha)
    with pytest.raises(pf.PitFundamentalsError, match="events"):
        bundle.verify_all()


def test_policy_files_cover_every_repo_module_the_builder_and_reader_import() -> None:
    # Drift guard (review #3367): logic moved into a new repo module must join POLICY_FILES,
    # or an edit to it would escape the bundle's POLICY pin.
    import ast

    root = Path(pf.__file__).resolve().parents[2]
    imported: set[str] = set()
    for relative in ("app/services/pit_fundamentals.py", "scripts/build_3360_pit_fundamentals.py"):
        for node in ast.walk(ast.parse((root / relative).read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            elif isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            else:
                continue
            imported |= {m.replace(".", "/") + ".py" for m in modules if m.split(".")[0] in {"app", "scripts"}}
    assert imported <= set(pf.POLICY_FILES)


# ------------------------------------------------------------------ causal reference (item 2)


def _causal(tmp_path: Path) -> tuple[pf.PitFundamentalsBundle, dict[str, Any], dict[str, Any]]:
    rows: dict[tuple[str, str, str], list[Any]] = {
        A: [_row(100, "a1"), _row(105, "a1"), _row(110, "a2"), _row("bad", "a3"), _row(1, "a1", end="2020-06-30")],
        R: [_row(5, "a1", start="2019-01-01"), _row(6, "a3", start="2019-01-01"), _row(7, "e1", start="2019-01-01")],
    }
    filings = [
        ("a1", T1, "10-K"),
        ("a2", T2, "10-K/A"),
        ("a3", "2020-06-01T21:00:00.000Z", "10-K/A"),
        ("e1", T2, "8-K"),
    ]
    bundle, _ = _one(tmp_path, rows, filings)
    return bundle, _facts(rows), _subs(filings)


def test_causal_reference_agrees_with_the_bundle(tmp_path: Path) -> None:
    from scripts.causal_3360_pit_fundamentals import verify_cik

    bundle, facts, subs = _causal(tmp_path)
    tally = verify_cik(bundle, CIK, facts, subs, lambda _: None)
    assert tally.mismatches == []
    assert tally.counts["value_compared"] > 0 and tally.counts["prefix_compared"] > 0
    assert tally.counts["decisions"] == 3  # before the first date, after each of the two dates


def test_causal_reference_catches_a_reader_that_leaks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Negative control: a reader that looks one day ahead must be caught.
    from scripts.causal_3360_pit_fundamentals import verify_cik

    bundle, facts, subs = _causal(tmp_path)
    honest = bundle.value_as_of
    monkeypatch.setattr(bundle, "value_as_of", lambda cik, key, d: honest(cik, key, d + timedelta(days=1)))
    assert verify_cik(bundle, CIK, facts, subs, lambda _: None).counts["value_mismatch"] > 0
