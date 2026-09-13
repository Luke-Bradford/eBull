"""Pin the eToro quota census against the portal's OpenAPI document (#2946 item 4).

CAVEMAN: portal change spec -> rerun me -> artefact match again. Read diff BEFORE commit.

WHEN TO RUN
    * A test in ``tests/test_2946_openapi_ratelimit_census.py`` failed and you have
      decided the PORTAL changed rather than our map.
    * A new eToro call site was added to ``etoro_quota_lanes`` and needs its documented
      limit recorded.
    * Periodically, to answer "did the portal change at all" — that is what
      ``document_sha256`` is for.

WHEN **NOT** TO RUN
    * A test failed and you do not know why. STOP. The cross-check compares OUR lane map
      against ETORO'S document; re-pinning makes the disagreement disappear without
      anyone reading it, which is the entire failure this artefact exists to prevent.
    * To "fix" a floor. This script records what is DOCUMENTED. Changing a throttle is a
      behavioural change with its own evidence bar — see
      ``docs/proposals/execution/2026-09-13-lane-g-history-floor.md``.

⚠ Two files are written together and must be committed together: the raw document and
the extract. Committing 1.79 MB of JSON is deliberate — without the source,
``document_sha256`` can never be recomputed, the extract cannot be shown to have come
from those bytes, and "absent from the spec" is unprovable offline (an absence
recomputed from an extract of the same absence is circular).

⚠ ``curl`` is Cloudflare-blocked against this portal (``.claude/skills/data-sources/
etoro-api.md``), so the fetch uses ``httpx`` with a browser user-agent. Read-only,
public, unauthenticated — this is NOT a broker call and touches no credential.

Invoke:
    PYTHONPATH=. uv run python -m scripts.refresh_2946_openapi_census            # fetch
    PYTHONPATH=. uv run python -m scripts.refresh_2946_openapi_census --offline  # re-extract

Exit code:
    0 — artefact matches, or was rewritten after the diff was printed.
    1 — fetch, validation or extraction failed. Nothing was written.

Spec: docs/proposals/execution/2026-09-13-openapi-ratelimit-census-pin.md
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.providers.implementations.etoro_quota_lanes import CALL_SITES, KNOWN_UNTHROTTLED

_REPO_ROOT = Path(__file__).resolve().parent.parent
_FIXTURES = _REPO_ROOT / "tests" / "fixtures" / "etoro"

SOURCE_URL = "https://api-portal.etoro.com/api-reference/openapi.json"

#: Bumped whenever the EXTRACT's shape changes. Without it, an extractor change and a
#: source change are indistinguishable in the artefact.
EXTRACTOR_VERSION = 1

#: Committed alongside the extract. Named by the version it carries so a refresh against
#: a new spec version lands as an add + delete rather than an invisible overwrite.
SOURCE_FIXTURE = _FIXTURES / "openapi_v1.375.0.json"
ARTEFACT = _FIXTURES / "openapi_ratelimit_census.json"

_VERBS = ("get", "post", "put", "patch", "delete")

#: A ``KNOWN_UNTHROTTLED`` path ending in this is a PREFIX template.  Resolution requires
#: EXACTLY ONE spec path to match: zero and ambiguous are distinct errors, never a silent
#: first hit.
_PREFIX_MARKER = "..."

#: The rate-limit rule is stated in prose on each operation's ``description``, ahead of a
#: ``---`` separator.  The extension carries the numbers; this carries the SEMANTICS, and
#: the semantics are the part a field name cannot establish on its own.
_DESCRIPTION_SEPARATOR = "\n\n---\n\n"


class CensusError(RuntimeError):
    """Extraction or validation failed. Nothing is written when this is raised."""


def _repo_relative(path: Path) -> str:
    """Repo-relative when it can be, absolute otherwise.

    ``Path.relative_to`` RAISES on a path outside the repo, which a test redirecting
    these constants at a ``tmp_path`` hits immediately — a crash in provenance
    bookkeeping, for a cosmetic string.
    """
    try:
        return str(path.relative_to(_REPO_ROOT))
    except ValueError:
        return str(path)


# ---------------------------------------------------------------------------
# Spec reading
# ---------------------------------------------------------------------------


def operation_key(verb: str, path: str) -> str:
    """``"GET /api/v1/..."`` — the spec's OWN ``sharedWith`` syntax.

    Deliberately not keyed by our method name: ``place_order`` reaches two paths, so
    method-only keys collide; and one path carries several verbs, so path-only keys lose
    the operation.
    """
    return f"{verb.upper()} {path}"


def load_operations(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Every operation in the document, keyed by ``VERB path``."""
    out: dict[str, dict[str, Any]] = {}
    for path, item in spec.get("paths", {}).items():
        if not isinstance(item, dict):
            raise CensusError(f"path item for {path} is not an object")
        for verb, op in item.items():
            if verb not in _VERBS:
                continue
            if not isinstance(op, dict):
                raise CensusError(f"operation {verb.upper()} {path} is not an object")
            out[operation_key(verb, path)] = op
    return out


def operation_environment(key: str, op: dict[str, Any]) -> str:
    """``demo`` / ``real`` / ``neutral``, read from the operation's ``tags``.

    ⚠ The STRUCTURED field, not a path heuristic. The document tags operations
    ``"Trading - Demo"`` / ``"Trading - Real"`` / ``"Market Data"`` (and 23 other groups),
    so the environment is stated rather than inferred — a `/demo/` substring test would
    additionally have to guess for every path with no environment segment at all.

    This is the three-way distinction ``QuotaLane.env_separation`` makes, which is what
    lets the document CONFIRM that field instead of merely coexisting with it.
    """
    tags = op.get("tags") or []
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise CensusError(f"{key}: tags is not a list of strings")
    suffixes = {tag.rsplit(" - ", 1)[-1].strip().lower() for tag in tags if " - " in tag}
    if "demo" in suffixes:
        return "demo"
    if "real" in suffixes:
        return "real"
    return "neutral"


def _rate_limit_sentence(op: dict[str, Any]) -> str:
    description = op.get("description") or ""
    if not isinstance(description, str):
        raise CensusError("operation description is not a string")
    return description.split(_DESCRIPTION_SEPARATOR, 1)[0].strip()


def read_rate_limit(key: str, op: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalise one ``x-ratelimit``.

    ⚠ Raises rather than returning ``None`` on a malformed annotation. "Absent" and
    "present but unreadable" are different facts, and letting the second fall through
    into the unresolved list would record a documented endpoint as undocumented.
    """
    raw = op.get("x-ratelimit")
    if raw is None:
        raise CensusError(f"{key}: no x-ratelimit annotation")
    if not isinstance(raw, dict):
        raise CensusError(f"{key}: x-ratelimit is not an object")

    limit, window = raw.get("limit"), raw.get("window")
    for name, value in (("limit", limit), ("window", window)):
        # bool is an int subclass in Python — excluded explicitly, or `True` reads as 1.
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise CensusError(f"{key}: x-ratelimit.{name} is not a positive integer ({value!r})")

    shared, default_pool = raw.get("shared"), raw.get("defaultPool")
    for name, value in (("shared", shared), ("defaultPool", default_pool)):
        if not isinstance(value, bool):
            raise CensusError(f"{key}: x-ratelimit.{name} is not a boolean ({value!r})")

    peers_raw = raw.get("sharedWith") or []
    if not isinstance(peers_raw, list):
        raise CensusError(f"{key}: x-ratelimit.sharedWith is not a list")
    for peer in peers_raw:
        if not isinstance(peer, str) or len(peer.split(" ", 1)) != 2:
            raise CensusError(f"{key}: malformed peer identity {peer!r}")

    if not shared and peers_raw:
        raise CensusError(f"{key}: x-ratelimit says shared=false but lists {len(peers_raw)} peers")

    return {
        "limit": limit,
        "window": window,
        "shared": shared,
        "defaultPool": default_pool,
        # Sorted, de-duplicated, self removed: `sharedWith` ordering carries no meaning
        # and every list already omits its own endpoint, so including self here makes the
        # membership SET directly comparable between two members of one pool.
        "pool": sorted({key} | set(peers_raw)),
        "environment": operation_environment(key, op),
        "rate_limit_prose": _rate_limit_sentence(op),
    }


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def _candidate_keys(verb: str, path: str, known_paths: set[str]) -> list[str]:
    """Expand ``{env}`` and a trailing prefix marker into concrete operation keys.

    ``{env}`` expands to BOTH environments: the raw pnl probe in ``KNOWN_UNTHROTTLED``
    builds the real path too, and demo-only substitution would hide half of it.
    """
    paths = [path.replace("{env}", env) for env in ("demo", "real")] if "{env}" in path else [path]

    resolved: list[str] = []
    for candidate in paths:
        if not candidate.endswith(_PREFIX_MARKER):
            resolved.append(candidate)
            continue
        prefix = candidate[: -len(_PREFIX_MARKER)]
        matches = sorted(p for p in known_paths if p.startswith(prefix))
        if len(matches) != 1:
            raise CensusError(
                f"prefix template {candidate!r} matched {len(matches)} spec paths; "
                "exactly one is required (zero = the endpoint moved, several = the "
                "template is too loose to name one quota lane)"
            )
        resolved.append(matches[0])
    return [operation_key(verb, p) for p in resolved]


def _extract_block(
    entries: list[tuple[str, str, str, str]],
    ops: dict[str, dict[str, Any]],
    known_paths: set[str],
) -> tuple[dict[str, Any], list[str]]:
    """Resolve ``(label, lane, verb, path)`` entries against the spec.

    Returns the per-operation block plus the operation keys that have no operation in the
    document. An entry may resolve to several keys (``{env}`` expansion); each is recorded
    separately, because the two environments are separate operations with separate
    annotations.
    """
    block: dict[str, Any] = {}
    unresolved: list[str] = []
    for label, lane, verb, path in entries:
        for key in _candidate_keys(verb, path, known_paths):
            op = ops.get(key)
            if op is None:
                unresolved.append(key)
                continue
            record = block.setdefault(key, {**read_rate_limit(key, op), "lane": lane, "reached_by": []})
            if record["lane"] != lane:
                raise CensusError(f"{key}: reached from two different lanes ({record['lane']} and {lane})")
            if label not in record["reached_by"]:
                record["reached_by"].append(label)
    for record in block.values():
        record["reached_by"].sort()
    return block, sorted(set(unresolved))


def population_counts(ops: dict[str, dict[str, Any]], spec: dict[str, Any]) -> dict[str, int]:
    """Whole-document counts, so a claim about the census cites the population it was
    measured on rather than the 17 operations we happen to call."""
    limits = [read_rate_limit(key, op) for key, op in ops.items()]
    pools = {tuple(r["pool"]) for r in limits if r["shared"] and not r["defaultPool"]}
    return {
        "paths": len(spec.get("paths", {})),
        "operations": len(ops),
        "with_x_ratelimit": sum(1 for _ in limits),
        "default_pool": sum(1 for r in limits if r["defaultPool"]),
        "dedicated": sum(1 for r in limits if not r["shared"]),
        "distinct_non_default_pools": len(pools),
    }


def orphan_peer_references(ops: dict[str, dict[str, Any]]) -> list[str]:
    """Names that appear only inside ``sharedWith`` lists, with no operation of their own.

    The spec assigns these to pools while defining nothing for them — three of them are
    endpoints this repo calls. An orphan that later acquires an operation is a portal
    change worth seeing, which is why they are stored rather than counted.
    """
    named: set[str] = set()
    for key, op in ops.items():
        named |= set(read_rate_limit(key, op)["pool"]) - {key}
    return sorted(named - set(ops))


def build_extract(spec: dict[str, Any]) -> dict[str, Any]:
    """The whole artefact minus provenance and hashes — a pure function of the document."""
    ops = load_operations(spec)
    known_paths = set(spec.get("paths", {}))

    call_sites, call_unresolved = _extract_block(
        [(s.method, s.lane, s.verb, s.demo_path) for s in CALL_SITES], ops, known_paths
    )
    unthrottled, raw_unresolved = _extract_block(
        [(f"{u.module}:{u.line_hint}", u.lane, u.verb, u.path) for u in KNOWN_UNTHROTTLED], ops, known_paths
    )

    # ⚠ The two inventories OVERLAP: the credential-validation probe in
    # `KNOWN_UNTHROTTLED` reaches `/api/v1/trading/info/{env}/pnl`, which is also
    # `get_account_risk_snapshot`'s lane-E call site. A reader merging the blocks
    # (`{**call_sites, **unthrottled}`) silently keeps ONE record per key, so a wrong lane
    # on the throttled side is overwritten by the correct one on the raw side and every
    # downstream check passes. Caught here instead, where the two are still separate.
    for key in set(call_sites) & set(unthrottled):
        if call_sites[key]["lane"] != unthrottled[key]["lane"]:
            raise CensusError(
                f"{key}: reached from both inventories with different lanes "
                f"({call_sites[key]['lane']} as a throttled call site, "
                f"{unthrottled[key]['lane']} as a raw bypass) — one of them is wrong"
            )

    return {
        "call_sites": call_sites,
        "unthrottled": unthrottled,
        "unresolved": sorted(set(call_unresolved) | set(raw_unresolved)),
        "orphan_peer_references": orphan_peer_references(ops),
        "population": population_counts(ops, spec),
    }


def canonical(extract: dict[str, Any]) -> str:
    """Stable text for hashing: sorted keys, no incidental whitespace, UTF-8."""
    return json.dumps(extract, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def extract_sha256(extract: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(extract).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _fetch() -> bytes:
    """Fetch the document, or raise ``CensusError``.

    ⚠ Every transport failure is wrapped HERE rather than caught in ``main``. Two reasons:
    ``httpx`` is imported lazily so an ``--offline`` run needs no network stack at all,
    and ``main``'s handler would otherwise have to name an exception type from a module it
    does not import. A Cloudflare 403, a timeout or a 5xx would then surface as an
    unhandled traceback instead of the "nothing was written" line this script promises.
    """
    import httpx  # imported here so --offline needs no network stack at all

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
    try:
        response = httpx.get(
            SOURCE_URL,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=60.0,
            follow_redirects=True,
        )
        response.raise_for_status()
        body = response.content
    except httpx.HTTPStatusError as exc:
        raise CensusError(f"GET {SOURCE_URL} returned HTTP {exc.response.status_code}") from exc
    except httpx.HTTPError as exc:
        raise CensusError(f"GET {SOURCE_URL} failed ({type(exc).__name__}: {exc})") from exc
    if not body.lstrip().startswith(b"{"):
        # Cloudflare serves an HTML interstitial with a 200 — a JSON parse error here
        # would be reported as "malformed spec" and send the reader to the wrong place.
        raise CensusError(f"response body is not JSON (starts {body[:40]!r}) — Cloudflare interstitial?")
    return body


#: Compared as a one-line "changed" rather than printed: the prose is a paragraph, and a
#: wrapped diff of it buries every numeric change above it.
_BULKY_FIELDS = ("rate_limit_prose",)


def _diff(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    """Human-readable semantic changes. A silent overwrite is the failure mode here.

    ⚠ Field comparison is driven by the UNION of both records' keys, never by a
    hand-written list. The first draft listed the fields explicitly and reported "no
    semantic change" on a run that had added a whole new field to every record — a diff
    that cannot see a new field is worse than no diff, because it is read as reassurance.
    """
    if previous is None:
        return ["no previous artefact — this is the first pin"]
    lines: list[str] = []
    for section in ("call_sites", "unthrottled"):
        before, after = previous.get(section, {}), current.get(section, {})
        for key in sorted(set(before) | set(after)):
            if key not in after:
                lines.append(f"{section}: REMOVED {key}")
            elif key not in before:
                lines.append(f"{section}: ADDED   {key}")
            else:
                for field in sorted(set(before[key]) | set(after[key])):
                    old, new = before[key].get(field), after[key].get(field)
                    if old == new:
                        continue
                    if field in _BULKY_FIELDS:
                        lines.append(f"{section}: {key} {field}: CHANGED (read the artefact)")
                    else:
                        lines.append(f"{section}: {key} {field}: {old!r} -> {new!r}")
    for section in ("unresolved", "orphan_peer_references"):
        gone = sorted(set(previous.get(section, [])) - set(current.get(section, [])))
        new = sorted(set(current.get(section, [])) - set(previous.get(section, [])))
        lines += [f"{section}: no longer listed: {k}" for k in gone]
        lines += [f"{section}: newly listed:     {k}" for k in new]
    if previous.get("population") != current.get("population"):
        lines.append(f"population: {previous.get('population')} -> {current.get('population')}")
    return lines or ["no semantic change in the extracted blocks"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--offline",
        action="store_true",
        help="re-extract from the committed source fixture instead of fetching the portal",
    )
    parser.add_argument("--check", action="store_true", help="report differences and write nothing")
    args = parser.parse_args(argv)

    try:
        body = SOURCE_FIXTURE.read_bytes() if args.offline else _fetch()
        spec = json.loads(body)
        extract = build_extract(spec)
    except (CensusError, json.JSONDecodeError, OSError) as exc:
        print(f"refresh_2946_openapi_census: FAILED — {type(exc).__name__}: {exc}", file=sys.stderr)
        print("nothing was written.", file=sys.stderr)
        return 1

    previous = json.loads(ARTEFACT.read_text()) if ARTEFACT.exists() else None

    # ⚠ The DOCUMENT hash first, and before any early return. It answers a different
    # question from the semantic diff — "did the portal change AT ALL" — and that is the
    # whole reason it exists. Printing only the extract diff makes a release that touched
    # nothing we extract (a new `info.version`, a new endpoint, a reworded description)
    # indistinguishable from an unchanged fetch, in the mode documented for exactly that
    # periodic check.
    document_sha256 = hashlib.sha256(body).hexdigest()
    if previous is None:
        print("  document: no previous artefact to compare against")
    elif previous.get("document_sha256") == document_sha256:
        print(f"  document: UNCHANGED ({document_sha256[:12]}…)")
    else:
        print(f"  document: CHANGED {str(previous.get('document_sha256'))[:12]}… -> {document_sha256[:12]}…")
        print(f"  document: info.version {previous.get('provenance', {}).get('info_version')!r} -> ", end="")
        print(
            f"{spec.get('info', {}).get('version')!r}, {previous.get('provenance', {}).get('byte_length')} -> ", end=""
        )
        print(f"{len(body)} bytes")

    for line in _diff(previous, extract):
        print(f"  {line}")

    if args.check:
        print("--check: nothing written.")
        return 0

    # ⚠ An offline run performs no fetch, so it must NOT stamp a fetch date. Overwriting
    # `fetched_at` with now() would make a re-extraction of a months-old fixture read as
    # freshly acquired evidence, and destroy the only record of when it was actually
    # taken. The two dates answer different questions and are stored separately.
    if args.offline:
        if previous is None:
            print(
                "refresh_2946_openapi_census: FAILED — --offline has no previous artefact to take "
                "`fetched_at` from, and inventing one would forge provenance. Run a real fetch first.",
                file=sys.stderr,
            )
            return 1
        # ⚠ And the date may only be reused if the BYTES are the ones it describes.
        # Otherwise editing the source fixture and re-running --offline mints a
        # self-consistent artefact — both hashes recomputed, every integrity test green —
        # carrying an acquisition date for a document that was never fetched.
        if previous.get("document_sha256") != document_sha256:
            print(
                "refresh_2946_openapi_census: FAILED — the source fixture does not match the "
                f"artefact's document_sha256 ({str(previous.get('document_sha256'))[:12]}… vs "
                f"{document_sha256[:12]}…). --offline reuses the recorded fetch date, which would "
                "then describe bytes nobody fetched. Run a real fetch.",
                file=sys.stderr,
            )
            return 1
        fetched_at = previous["provenance"]["fetched_at"]
    else:
        fetched_at = datetime.now(UTC).isoformat()

    artefact = {
        "provenance": {
            "source_url": SOURCE_URL,
            "fetched_at": fetched_at,
            "extracted_at": datetime.now(UTC).isoformat(),
            "byte_length": len(body),
            "info_version": spec.get("info", {}).get("version"),
            "extractor_version": EXTRACTOR_VERSION,
            "hashed_bytes": (
                "document_sha256 is over the RAW response body as received, before any "
                "decode. extract_sha256 is over canonical(extract) — sorted keys, compact "
                "separators, UTF-8 — and covers the blocks below and nothing else. "
                "fetched_at is when the bytes were taken from the portal; extracted_at is "
                "when this extract was last derived from them, which an --offline re-run "
                "moves and a fetch date must not."
            ),
            "source_fixture": _repo_relative(SOURCE_FIXTURE),
        },
        # Reuses the local computed above — recomputing it is not merely redundant, it is
        # a second chance for the stored hash to disagree with the one the offline-rewrite
        # guard just compared against.
        "document_sha256": document_sha256,
        "extract_sha256": extract_sha256(extract),
        **extract,
    }

    # Write the source first: an artefact referring to bytes that are not on disk is the
    # one state in which none of the offline checks can run.
    if not args.offline:
        SOURCE_FIXTURE.write_bytes(body)
    ARTEFACT.write_text(json.dumps(artefact, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    print(f"wrote {_repo_relative(ARTEFACT)} (extract_sha256 {artefact['extract_sha256'][:12]}…)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
