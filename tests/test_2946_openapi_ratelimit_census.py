"""The lane map, cross-checked against eToro's OpenAPI document (#2946 item 4).

Pure logic, offline. Both inputs are committed: the raw portal response
(``tests/fixtures/etoro/openapi_v1.375.0.json``) and the extract derived from it
(``openapi_ratelimit_census.json``). Nothing here touches the network — a test that
fetches the portal fails in CI and trains people to skip it.

⚠⚠ **This is a CROSS-CHECK and must stay one.** The two sides are separately authored:
the artefact is eToro's document, ``etoro_quota_lanes`` is ours. If a future session
generates the lane map FROM the artefact, every assertion below becomes an identity and
the census is unguarded again — the failure mode recorded in
``docs/review-prevention-log.md`` under *"a derivation can silently turn a comparison
test into an identity test"*.

⚠ What this does NOT establish:

* **Not enforcement.** The document says what is DOCUMENTED. #2946 item 3's request
  artefact is the measurement.
* **Not an independent witness.** The rendered portal pages and this JSON are plausibly
  generated from one upstream annotation, the same caveat ``QuotaLane.witnesses`` already
  carries. What it rules out is OUR transcription error.
* **Not our runtime inventory.** The AST guards in ``test_etoro_quota_lanes.py`` count
  call expressions, so an equal-count URL substitution passes them; comparing the document
  against an unchanged hand-maintained table cannot see that either.
* **Not the general rate-limit page.** ``CallSite.general_tier_per_minute`` comes from a
  page that classifies by OPERATION and is deliberately not compared here — history and
  the TP/SL edit would fail by design.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

from app.providers.implementations.etoro_quota_lanes import (
    CALL_SITES,
    KNOWN_UNTHROTTLED,
    LANES,
)
from scripts.refresh_2946_openapi_census import (
    ARTEFACT,
    SOURCE_FIXTURE,
    CensusError,
    build_extract,
    extract_sha256,
    load_operations,
    operation_key,
    read_rate_limit,
)

_EXTRACT_KEYS = ("call_sites", "unthrottled", "unresolved", "orphan_peer_references", "population")


@pytest.fixture(scope="module")
def source_bytes() -> bytes:
    return pathlib.Path(SOURCE_FIXTURE).read_bytes()


@pytest.fixture(scope="module")
def spec(source_bytes: bytes) -> dict[str, Any]:
    return json.loads(source_bytes)


@pytest.fixture(scope="module")
def artefact() -> dict[str, Any]:
    return json.loads(pathlib.Path(ARTEFACT).read_text())


@pytest.fixture(scope="module")
def resolved(artefact: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Every resolved operation from both inventories, as ``(key, record)`` PAIRS.

    ⚠⚠ Deliberately a list, not ``{**call_sites, **unthrottled}``. The two inventories
    OVERLAP — the credential-validation probe reaches ``/api/v1/trading/info/{env}/pnl``,
    which is also ``get_account_risk_snapshot``'s lane-E call site — so a dict merge keeps
    one record per key. A wrong lane on the throttled side is then overwritten by the
    correct one on the raw side, and every assertion below passes on a record that is not
    the one under test. That is not hypothetical: it was demonstrated by reassigning
    ``get_account_risk_snapshot`` to lane D and watching the suite stay green.
    """
    return [(key, record) for section in ("call_sites", "unthrottled") for key, record in artefact[section].items()]


# ---------------------------------------------------------------------------
# Integrity — the artefact must be derivable from the committed source
# ---------------------------------------------------------------------------


def test_the_extract_is_reproducible_from_the_committed_source(spec: dict[str, Any], artefact: dict[str, Any]) -> None:
    """Re-derive the extract and compare field for field.

    ⚠ This, not ``extract_sha256``, is what makes a hand-edited artefact fail. A hash
    stored next to the data it hashes proves only that nobody forgot to re-run the
    hasher — editing the extract and recomputing the adjacent hash passes. Committing the
    SOURCE is what turns integrity into something checkable.
    """
    rebuilt = build_extract(spec)
    for key in _EXTRACT_KEYS:
        assert artefact[key] == rebuilt[key], (
            f"artefact['{key}'] does not match a fresh extraction from "
            f"{SOURCE_FIXTURE.name}. Re-run scripts/refresh_2946_openapi_census.py "
            "--offline, and read the diff before committing it."
        )
    assert artefact["extract_sha256"] == extract_sha256(rebuilt)


def test_the_document_hash_matches_the_committed_bytes(source_bytes: bytes, artefact: dict[str, Any]) -> None:
    """The pin item 4 actually asked for: a content hash over the source."""
    import hashlib

    assert artefact["document_sha256"] == hashlib.sha256(source_bytes).hexdigest()
    assert artefact["provenance"]["byte_length"] == len(source_bytes)


def test_the_provenance_envelope_says_what_was_hashed(artefact: dict[str, Any]) -> None:
    """Without this, an extractor change and a source change are indistinguishable."""
    provenance = artefact["provenance"]
    for field in (
        "source_url",
        "fetched_at",
        "extracted_at",
        "byte_length",
        "info_version",
        "extractor_version",
        "hashed_bytes",
    ):
        assert provenance.get(field), f"provenance is missing {field}"
    assert provenance["source_fixture"] == str(SOURCE_FIXTURE.relative_to(pathlib.Path(__file__).resolve().parents[1]))
    # The bytes cannot have been extracted before they were taken.
    assert provenance["fetched_at"] <= provenance["extracted_at"]


def test_an_offline_re_extraction_does_not_forge_a_fetch_date(tmp_path: pathlib.Path, monkeypatch) -> None:
    """``--offline`` performs no fetch, so it must not stamp one.

    ⚠ The first draft wrote ``datetime.now()`` into ``fetched_at`` on every run. A
    re-extraction of a months-old fixture would then read as freshly acquired evidence and
    destroy the only record of when the bytes were actually taken — in the one file whose
    entire job is provenance. The two dates answer different questions.
    """
    from scripts import refresh_2946_openapi_census as module

    source = tmp_path / "openapi.json"
    source.write_bytes(pathlib.Path(SOURCE_FIXTURE).read_bytes())
    artefact_path = tmp_path / "census.json"
    monkeypatch.setattr(module, "SOURCE_FIXTURE", source)
    monkeypatch.setattr(module, "ARTEFACT", artefact_path)
    monkeypatch.setattr(module, "_fetch", lambda: source.read_bytes())

    assert module.main([]) == 0  # a real "fetch", stamping both dates
    first = json.loads(artefact_path.read_text())["provenance"]

    assert module.main(["--offline"]) == 0
    second = json.loads(artefact_path.read_text())["provenance"]

    assert second["fetched_at"] == first["fetched_at"], "an offline re-extraction moved the FETCH date"
    assert second["extracted_at"] > first["extracted_at"], "the extraction date did not move"


def test_an_offline_run_with_no_prior_artefact_refuses_rather_than_inventing_a_date(
    tmp_path: pathlib.Path, monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """There is nothing to preserve, and inventing one would forge provenance."""
    from scripts import refresh_2946_openapi_census as module

    source = tmp_path / "openapi.json"
    source.write_bytes(pathlib.Path(SOURCE_FIXTURE).read_bytes())
    monkeypatch.setattr(module, "SOURCE_FIXTURE", source)
    monkeypatch.setattr(module, "ARTEFACT", tmp_path / "absent.json")

    assert module.main(["--offline"]) == 1
    assert "forge provenance" in capsys.readouterr().err


def test_check_mode_reports_the_document_hash_not_only_the_extract_diff(
    tmp_path: pathlib.Path, monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``document_sha256`` answers "did the portal change at all" — in check mode too.

    ⚠ Without this, a release that touched nothing we extract (a new ``info.version``, a
    new endpoint, a reworded description) prints exactly what an unchanged fetch prints,
    in the mode the script's own header documents for periodic checking.
    """
    from scripts import refresh_2946_openapi_census as module

    source = tmp_path / "openapi.json"
    source.write_bytes(pathlib.Path(SOURCE_FIXTURE).read_bytes())
    artefact_path = tmp_path / "census.json"
    monkeypatch.setattr(module, "SOURCE_FIXTURE", source)
    monkeypatch.setattr(module, "ARTEFACT", artefact_path)
    monkeypatch.setattr(module, "_fetch", lambda: source.read_bytes())
    assert module.main([]) == 0
    capsys.readouterr()

    # Same extracted blocks, different bytes: bump a field the extract does not read.
    spec = json.loads(source.read_bytes())
    spec["info"]["title"] = "changed upstream"
    monkeypatch.setattr(module, "_fetch", lambda: json.dumps(spec).encode())

    assert module.main(["--check"]) == 0
    out = capsys.readouterr().out
    assert "document: CHANGED" in out, out
    assert "no semantic change in the extracted blocks" in out, "the extract diff should still say it is unchanged"
    assert json.loads(artefact_path.read_text())["provenance"]["fetched_at"], "--check must not have written"


# ---------------------------------------------------------------------------
# Coverage — every call site is accounted for, in exactly one state
# ---------------------------------------------------------------------------


def _expected_keys(verb: str, path: str, known_paths: set[str]) -> list[str]:
    from scripts.refresh_2946_openapi_census import _candidate_keys

    return _candidate_keys(verb, path, known_paths)


def test_every_call_site_is_either_resolved_or_pinned_as_unresolved(
    spec: dict[str, Any], artefact: dict[str, Any]
) -> None:
    """No third state. A new eToro call site fails here until it is recorded.

    ⚠ Asserting only "every artefact entry matches a call site" would be vacuous in the
    direction that matters — a call site missing from the artefact would escape every
    comparison below while looking fully checked.
    """
    known_paths = set(spec["paths"])
    accounted = set(artefact["call_sites"]) | set(artefact["unthrottled"]) | set(artefact["unresolved"])

    entries = [(s.verb, s.demo_path, s.method) for s in CALL_SITES]
    entries += [(u.verb, u.path, f"{u.module}:{u.line_hint}") for u in KNOWN_UNTHROTTLED]

    for verb, path, label in entries:
        for key in _expected_keys(verb, path, known_paths):
            assert key in accounted, (
                f"{label} reaches {key}, which the census does not account for. "
                "Re-run scripts/refresh_2946_openapi_census.py --offline."
            )


def test_the_unresolved_list_is_pinned_and_self_cleaning(spec: dict[str, Any], artefact: dict[str, Any]) -> None:
    """An entry that now resolves must be removed; a path that stops resolving must fail.

    ⚠ The point of pinning rather than regenerating: a list computed from "whatever failed
    today" silently blesses an endpoint that disappeared from the document, which is
    exactly the event worth seeing.
    """
    ops = load_operations(spec)
    for key in artefact["unresolved"]:
        assert key not in ops, (
            f"{key} now has an operation in the document — remove it from `unresolved` "
            "by re-running the refresh script, and check what else moved."
        )


def test_the_unresolved_paths_are_named_by_other_endpoints_pools(artefact: dict[str, Any]) -> None:
    """These are not merely undocumented — the document assigns them to pools.

    Pins the distinction the census turns on: an orphan is an endpoint the spec KNOWS
    about and declines to define, not one it has never heard of. All three of ours are
    orphans, so if one ever became a true unknown that is a different fact.
    """
    orphans = set(artefact["orphan_peer_references"])
    for key in artefact["unresolved"]:
        assert key in orphans, f"{key} is unresolved AND unreferenced — a genuinely unknown endpoint, not an orphan"


# ---------------------------------------------------------------------------
# The cross-check itself
# ---------------------------------------------------------------------------


def test_documented_limits_and_windows_match_the_lane_map(resolved: list[tuple[str, dict[str, Any]]]) -> None:
    """``documented_per_minute`` and ``window_s`` against the document's own numbers."""
    for key, record in resolved:
        lane = LANES[record["lane"]]
        assert record["limit"] == lane.documented_per_minute, (
            f"{key}: document says {record['limit']}/min, lane {lane.key} records "
            f"{lane.documented_per_minute}. Do NOT edit the lane to match without reading "
            f"the prose: {record['rate_limit_prose']!r}"
        )
        assert record["window"] == lane.window_s, f"{key}: document window {record['window']}s vs lane {lane.window_s}s"


def test_documented_scope_matches_the_lane_scope(resolved: list[tuple[str, dict[str, Any]]]) -> None:
    """``shared``/``defaultPool`` against ``LaneScope``.

    This is the half a limit comparison misses: lanes B and C are 20/min like lane A, and
    only the ``shared: false`` flag distinguishes "dedicated" from "one of eleven".
    """
    for key, record in resolved:
        scope = LANES[record["lane"]].scope
        assert (record["shared"] is False) == (scope == "dedicated"), (
            f"{key}: document shared={record['shared']}, lane scope is {scope!r}"
        )
        assert record["defaultPool"] == (scope == "default"), (
            f"{key}: document defaultPool={record['defaultPool']}, lane scope is {scope!r}"
        )


def _pools_by_lane_and_env(
    resolved: list[tuple[str, dict[str, Any]]],
) -> dict[tuple[str, str], set[tuple[str, ...]]]:
    """``(lane, environment) -> the distinct pool membership sets observed``.

    Excludes the default pool: its members carry no peer list (that is what
    ``defaultPool: true`` means here), so each reports a pool of itself alone and
    comparing them says nothing.
    """
    out: dict[tuple[str, str], set[tuple[str, ...]]] = {}
    for key, record in resolved:
        if record["defaultPool"]:
            continue
        out.setdefault((record["lane"], record["environment"]), set()).add(tuple(record["pool"]))
    return out


def test_call_sites_on_one_lane_and_environment_report_the_same_pool(
    resolved: list[tuple[str, dict[str, Any]]],
) -> None:
    """Pool IDENTITY, not pool size.

    ⚠ Sizes are not enough, and the failure is concrete: moving a lane-D call into lane E
    passes every limit, window, scope and member-COUNT check, because both are shared
    60/min three-member pools. Only the membership set separates them.

    ⚠ Partitioned by ENVIRONMENT, because one lane legitimately holds two pools — which
    this test discovered rather than assumed. Lane E's demo and real trios are separate
    documented pools, and the first draft of this assertion failed on exactly that. The
    lane map already said so (``env_separation="witnessed"``); the next test is what now
    holds the document to it.
    """
    for (lane, env), pools in _pools_by_lane_and_env(resolved).items():
        assert len(pools) == 1, (
            f"lane {lane} ({env}) groups call sites the document puts in {len(pools)} different pools: "
            + "; ".join(f"{len(pool)} members" for pool in sorted(pools))
        )


def test_witnessed_lanes_have_disjoint_demo_and_real_pools(resolved: list[tuple[str, dict[str, Any]]]) -> None:
    """``env_separation="witnessed"`` is a CLAIM — here is the document checking it.

    The lane map's own wording is *"demo and real memberships were enumerated and are
    disjoint"*. Step 1 enumerated them by reading two rendered pages; this reads the
    membership lists. A lane whose two environments shared even one operation would mean
    one budget, not two, and every per-environment rate we derive would be double-counted.

    ⚠ Only asserted where both environments are actually reached — we do not call the real
    counterpart of most endpoints, and asserting over an absent side would pass vacuously
    while looking covered.
    """
    pools = _pools_by_lane_and_env(resolved)
    checked = 0
    for lane in LANES.values():
        if lane.env_separation != "witnessed":
            continue
        demo = pools.get((lane.key, "demo"))
        real = pools.get((lane.key, "real"))
        if not demo or not real:
            continue
        demo_members, real_members = set(next(iter(demo))), set(next(iter(real)))
        assert not (demo_members & real_members), (
            f"lane {lane.key} claims witnessed env separation, but the document puts "
            f"{sorted(demo_members & real_members)} in both environments' pools"
        )
        checked += 1
    assert checked, "no witnessed lane had both environments resolved — the assertion above never ran"


# ---------------------------------------------------------------------------
# Full-population invariants — all 177 operations, not our 17
# ---------------------------------------------------------------------------


def test_shared_with_is_reciprocal_across_the_whole_document(spec: dict[str, Any]) -> None:
    """If A names B as a pool peer, B must name A.

    The document's own invariant, checked on the FULL population rather than on the
    operations we happen to call. A violation means the document is the thing to re-read,
    not our map — and it would make "the pool" ambiguous for whichever side we believed.
    Orphans are skipped: a name with no operation cannot name anyone back.
    """
    ops = load_operations(spec)
    limits = {key: read_rate_limit(key, op) for key, op in ops.items()}
    breaks = [
        (key, peer)
        for key, record in limits.items()
        for peer in set(record["pool"]) - {key}
        if peer in limits and key not in limits[peer]["pool"]
    ]
    assert not breaks, f"non-reciprocal sharedWith pairs: {breaks[:5]}"


def test_no_operation_belongs_to_two_different_non_default_pools(spec: dict[str, Any]) -> None:
    """An operation in two pools would make "the budget for this call" undefined."""
    ops = load_operations(spec)
    pools = {
        tuple(record["pool"])
        for record in (read_rate_limit(key, op) for key, op in ops.items())
        if record["shared"] and not record["defaultPool"]
    }
    memberships: dict[str, int] = {}
    for pool in pools:
        for member in pool:
            memberships[member] = memberships.get(member, 0) + 1
    offenders = {member: count for member, count in memberships.items() if count > 1}
    assert not offenders, f"operations in more than one non-default pool: {offenders}"


def test_the_population_counts_are_recomputed_not_recalled(spec: dict[str, Any], artefact: dict[str, Any]) -> None:
    """Every count the census quotes is a measurement, re-run here.

    Guards the prose in ``docs/proposals/execution/2026-09-13-openapi-ratelimit-census-pin.md``,
    which cites these figures — a hand-written statistic goes stale in the place a reader
    trusts most.
    """
    from scripts.refresh_2946_openapi_census import population_counts

    assert artefact["population"] == population_counts(load_operations(spec), spec)
    assert artefact["population"]["with_x_ratelimit"] == artefact["population"]["operations"], (
        "an operation without a rate-limit annotation appeared — the extractor raises on "
        "one, so this failing means the counts were written by something else"
    )


# ---------------------------------------------------------------------------
# Extractor validation — "absent" and "present but unreadable" are different facts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (None, "no x-ratelimit"),
        ("60/min", "not an object"),
        ({"limit": 0, "window": 60, "shared": True, "defaultPool": False}, "limit"),
        ({"limit": "60", "window": 60, "shared": True, "defaultPool": False}, "limit"),
        # bool is an int subclass — `True` would read as limit 1 without an explicit guard
        ({"limit": True, "window": 60, "shared": True, "defaultPool": False}, "limit"),
        ({"limit": 60, "window": -1, "shared": True, "defaultPool": False}, "window"),
        ({"limit": 60, "window": 60, "shared": "yes", "defaultPool": False}, "shared"),
        ({"limit": 60, "window": 60, "shared": True, "defaultPool": None}, "defaultPool"),
        ({"limit": 60, "window": 60, "shared": True, "defaultPool": False, "sharedWith": "GET /x"}, "not a list"),
        ({"limit": 60, "window": 60, "shared": True, "defaultPool": False, "sharedWith": ["nospace"]}, "peer identity"),
        # Contradiction: a dedicated limit cannot have pool peers.
        (
            {"limit": 20, "window": 60, "shared": False, "defaultPool": False, "sharedWith": ["GET /x"]},
            "shared=false but lists",
        ),
    ],
)
def test_a_malformed_annotation_raises_rather_than_reading_as_absent(annotation: object, expected: str) -> None:
    """A malformed annotation must never fall through into the unresolved list.

    "Absent" means the document defines no operation; "present but unreadable" means it
    does and we could not parse it. Collapsing the second into the first would record a
    documented endpoint as undocumented — and the unresolved list is exactly where a
    reader looks to find out what the portal does not cover.
    """
    with pytest.raises(CensusError, match=expected):
        read_rate_limit("GET /api/v1/example", {"x-ratelimit": annotation})


def test_a_prefix_template_matching_several_paths_is_an_error(spec: dict[str, Any]) -> None:
    """Zero and ambiguous are distinct errors — never a silent first hit.

    ``KNOWN_UNTHROTTLED`` writes the debug candle probe as a prefix template, so a
    too-loose prefix would quietly assign one quota lane to whichever path sorted first.
    """
    from scripts.refresh_2946_openapi_census import _candidate_keys

    known_paths = set(spec["paths"])
    with pytest.raises(CensusError, match="matched 0 spec paths"):
        _candidate_keys("GET", "/api/v1/no-such-prefix/...", known_paths)
    with pytest.raises(CensusError, match="matched .* spec paths"):
        _candidate_keys("GET", "/api/v1/market-data/...", known_paths)


def test_an_offline_rewrite_refuses_when_the_source_fixture_no_longer_matches(
    tmp_path: pathlib.Path, monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The recorded fetch date may only be reused for the bytes it describes.

    ⚠ Otherwise editing the source fixture and re-running ``--offline`` mints a fully
    self-consistent artefact — both hashes recomputed, every integrity test green —
    carrying an acquisition date for a document nobody fetched. The hashes agree with each
    other precisely because they were both computed from the edit.
    """
    from scripts import refresh_2946_openapi_census as module

    source = tmp_path / "openapi.json"
    source.write_bytes(pathlib.Path(SOURCE_FIXTURE).read_bytes())
    artefact_path = tmp_path / "census.json"
    monkeypatch.setattr(module, "SOURCE_FIXTURE", source)
    monkeypatch.setattr(module, "ARTEFACT", artefact_path)
    monkeypatch.setattr(module, "_fetch", lambda: source.read_bytes())
    assert module.main([]) == 0
    original = json.loads(artefact_path.read_text())
    capsys.readouterr()

    spec = json.loads(source.read_bytes())
    spec["info"]["title"] = "edited on disk"
    source.write_bytes(json.dumps(spec).encode())

    assert module.main(["--offline"]) == 1
    assert "does not match the artefact's document_sha256" in capsys.readouterr().err
    assert json.loads(artefact_path.read_text()) == original, "the artefact was rewritten despite the refusal"


def test_an_operation_reached_from_both_inventories_must_agree_on_its_lane(spec: dict[str, Any], monkeypatch) -> None:
    """The overlap is real, so a disagreement across it has to be an error.

    ``/api/v1/trading/info/{env}/pnl`` is both ``get_account_risk_snapshot``'s lane-E call
    site and the credential-validation probe's raw bypass. If the two inventories ever
    disagree about its lane, one of them is wrong — and without this, the extract would
    simply record whichever block was built last.
    """
    from dataclasses import replace

    import scripts.refresh_2946_openapi_census as module

    mutated = tuple(
        replace(site, lane="D_order_info") if site.method == "get_account_risk_snapshot" else site
        for site in CALL_SITES
    )
    monkeypatch.setattr(module, "CALL_SITES", mutated)

    with pytest.raises(CensusError, match="different lanes"):
        build_extract(spec)


def test_operation_key_uses_the_documents_own_syntax() -> None:
    """The key has to match ``sharedWith`` entries verbatim, or pool membership never
    resolves and every pool silently reads as a pool of one."""
    assert operation_key("get", "/api/v1/me") == "GET /api/v1/me"
