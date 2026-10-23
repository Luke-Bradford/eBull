"""#3947 Amendment 2 B4.1-B4.4: the v2 replay A/B, run before v2 is deployed.

Spec: ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §Amendment 2, B4. Read-only: one REPEATABLE READ
snapshot of the dev DB, then EDGAR through ``SecFilingsProvider`` (its shared limiter). Nothing is written to the DB.

* ``freeze DIR`` (B4.1) writes ``DIR/manifest.json`` with the git SHA, before any network work: the baseline (every
  accession with an active generation, its scan row and every stored occurrence field), the seeded supplement (100
  each of unscanned ``raw``, ``history`` and failed-at-v1 accessions; all of an arm when it has fewer), and the
  [Z3] accessions frozen with their instance counts.
* ``run DIR`` replays each manifest accession not yet in ``DIR/results.jsonl`` and appends one line per accession,
  so a run resumes. Each accession reads both sources (the archive and the listing path), runs B1 over each with the
  v2 code (:func:`document_set`, :func:`parse_document_set`), and fetches SEC's extracted instance as the oracle. A
  transport error (connection, timeout, 429 or 5xx) is retried 3 times, then the accession is ``incomplete``.
* ``report DIR`` prints the B4.2-B4.4 readout and exits 1 on any blocking difference or ``incomplete`` accession.

    PYTHONPATH=. uv run python -m scripts.replay_3947_v2 freeze var/3947-replay
    PYTHONPATH=. uv run python -m scripts.replay_3947_v2 run var/3947-replay
    PYTHONPATH=. uv run python -m scripts.replay_3947_v2 report var/3947-replay
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import html
import json
import random
import re
import statistics
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

import httpx
import lxml.etree as ET
import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider, archive_dir_url
from app.services.dps_inline_scale_scan import (
    _POPULATION_SQL,
    RESPONSE_MAX_BYTES,
    SCOPE_START,
    DocumentSource,
    Target,
    _archive_candidates,
    _ArchiveFallback,
    document_set,
    listing_candidates,
    parse_header_records,
)
from app.services.inline_xbrl_dps import (
    CONCEPT,
    IX_NS,
    XSI_NIL,
    DocumentSetError,
    Occurrence,
    classify,
    parse_document_set,
)

V1 = "dps-inline-scale-v1"
PER_ARM = 100
SEED = 3947
RETRIES = 3
# [Z3] (spec Amendment 2): the 19 accessions holding header-less Inline XBRL documents in the 2026-10-23 sample, with
# the non-nil facts of the concept in SEC's extracted instance; plus the EFC control (4). From that run's log.
FROZEN: dict[str, int] = {
    "0000037785-19-000101": 3,
    "0000040987-20-000048": 3,
    "0000051253-20-000026": 3,
    "0000066740-22-000038": 15,
    "0000107263-22-000016": 4,
    "0000815097-20-000022": 12,
    "0000874761-23-000043": 10,
    "0000896878-25-000050": 4,
    "0001020569-22-000169": 18,
    "0001130310-20-000049": 20,
    "0001133421-20-000020": 8,
    "0001140361-24-034098": 4,
    "0001140859-26-000012": 4,
    "0001333986-23-000017": 3,
    "0001345126-19-000057": 3,
    "0001476204-21-000128": 12,
    "0001628280-21-011732": 3,
    "0001628280-23-037944": 8,
    "0001666700-21-000050": 11,
    "0001628280-26-055148": 4,  # the EFC control
}
# The stored occurrence columns (sql/514) are the Occurrence fields, in order.
_OCCURRENCE_COLUMNS: Final = (
    "document, element_ordinal, fact_id, context_ref_raw, unit_ref_raw, scale_raw, sign_raw, format_raw,"
    " decimals_raw, display_text, entity_cik, period_start, period_end, dimensional, unit, scale, instance_value,"
    " unresolved"
)
_OCCURRENCE_FIELDS = tuple(f.name for f in dataclasses.fields(Occurrence))
assert _OCCURRENCE_COLUMNS.replace(" ", "").split(",") == list(_OCCURRENCE_FIELDS)
# The issuer CIK an accession's archive lives under (#736): the manifest's, else a population instrument's primary.
_CIK_SQL = """
COALESCE((SELECT m.cik FROM sec_filing_manifest m WHERE m.accession_number = {acc}),
         (SELECT ei.identifier_value FROM sec_inline_scale_population sp
          JOIN external_identifiers ei ON ei.instrument_id = sp.instrument_id
           AND ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary
          WHERE sp.accession_number = {acc} ORDER BY sp.instrument_id LIMIT 1))
"""
_SGML_DOCUMENT = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.S | re.I)
_SGML_SEQUENCE = re.compile(r"<SEQUENCE>([^\n<]*)", re.I)
_SGML_FILENAME = re.compile(r"<FILENAME>([^\n<]*)", re.I)
_DECLARED_ENCODING = re.compile(rb"""\s*<\?xml[^>]*\bencoding\s*=\s*["']([A-Za-z0-9._-]+)["']""")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


# --- B4.1 manifest ------------------------------------------------------------------------------------------------


def freeze(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.json"
    if manifest_path.exists():
        sys.exit(f"{manifest_path} exists: the manifest is frozen once")
    sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        with conn.transaction():
            baseline_rows = conn.execute(
                "SELECT s.accession_number, s.outcome, s.documents, s.parser_version, "
                + _CIK_SQL.format(acc="s.accession_number")
                + " FROM sec_inline_scale_scan s WHERE s.outcome IS NOT NULL ORDER BY 1"
            ).fetchall()
            stored: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
            for row in conn.execute(
                "SELECT accession_number, "
                + _OCCURRENCE_COLUMNS
                + " FROM sec_inline_dps_occurrence ORDER BY accession_number, document, element_ordinal"
            ):
                stored[row[0]].append({k: _jsonable(v) for k, v in zip(_OCCURRENCE_FIELDS, row[1:], strict=True)})
            arms = conn.execute(
                _POPULATION_SQL
                + """
                SELECT p.accession_number, """
                + _CIK_SQL.format(acc="p.accession_number")
                + """,
                       CASE WHEN s.accession_number IS NULL AND p.in_raw THEN 'unscanned_raw'
                            WHEN s.accession_number IS NULL THEN 'history'
                            WHEN s.outcome IS NULL AND s.attempt_version = %(v1)s AND s.attempts > 0
                                THEN 'failed_v1' END
                FROM pop p LEFT JOIN sec_inline_scale_scan s USING (accession_number)
                ORDER BY 1
                """,
                {"concept": CONCEPT, "scope_start": SCOPE_START, "v1": V1},
            ).fetchall()
            frozen_ciks = dict(
                conn.execute(
                    "SELECT a, " + _CIK_SQL.format(acc="a") + " FROM unnest(%s::text[]) AS a",
                    (list(FROZEN),),
                ).fetchall()
            )
    baseline = [
        {
            "accession": a,
            "cik": cik,
            "outcome": outcome,
            "documents": documents,
            "parser_version": version,
            "occurrences": stored.get(a, []),
        }
        for a, outcome, documents, version, cik in baseline_rows
    ]
    by_arm: dict[str, list[tuple[str, str | None]]] = collections.defaultdict(list)
    for accession, cik, arm in arms:
        if arm is not None:
            by_arm[arm].append((accession, cik))
    rng = random.Random(SEED)
    supplement = {
        arm: [{"accession": a, "cik": c} for a, c in sorted(rng.sample(by_arm[arm], min(PER_ARM, len(by_arm[arm]))))]
        for arm in ("unscanned_raw", "history", "failed_v1")
    }
    manifest = {
        "git_sha": sha,
        "frozen_at": datetime.now(UTC).isoformat(),
        "baseline": baseline,
        "supplement": supplement,
        "arm_sizes": {arm: len(rows) for arm, rows in sorted(by_arm.items())},
        "frozen": [{"accession": a, "cik": frozen_ciks.get(a), "instance_facts": n} for a, n in FROZEN.items()],
    }
    manifest_path.write_text(json.dumps(manifest, indent=1))
    occurrences = sum(len(b["occurrences"]) for b in baseline)
    print(
        f"manifest {manifest_path} at {sha}: baseline {len(baseline)} accessions ({occurrences} occurrences);"
        f" supplement { ({k: len(v) for k, v in supplement.items()}) } of {manifest['arm_sizes']};"
        f" frozen {len(FROZEN)}"
    )


def _work(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Every manifest accession once, with its roles."""
    work: dict[str, dict[str, Any]] = {}
    for row in manifest["baseline"]:
        work.setdefault(row["accession"], {"cik": row["cik"], "roles": []})["roles"].append("baseline")
    for arm, rows in manifest["supplement"].items():
        for row in rows:
            work.setdefault(row["accession"], {"cik": row["cik"], "roles": []})["roles"].append(arm)
    for row in manifest["frozen"]:
        work.setdefault(row["accession"], {"cik": row["cik"], "roles": []})["roles"].append("frozen")
    return work


# --- the replay of one accession ----------------------------------------------------------------------------------


def _read(source: DocumentSource) -> dict[str, Any]:
    """B1 over one source with the v2 code: the set, its header-less members, the outcome and the occurrences; plus
    each candidate's state for the v1 comparison's listed exceptions."""
    states: dict[str, str] = {}
    result: dict[str, Any] = {"candidates": len(source.candidates)}
    try:
        members = document_set(source)
    except DocumentSetError as exc:
        return {**result, "outcome": "error", "error": str(exc)}
    names = {name for name, _ in members}
    for name, body in source.candidates:
        if name in names:
            states[name] = "member"
        else:
            try:
                states[name] = "non_member" if classify(name, body) is None else "separate_set"
            except DocumentSetError:
                states[name] = "undecidable"
    result |= {
        "set": [name for name, _ in members],
        "headerless": [name for name, root in members if next(root.iter(f"{{{IX_NS}}}header"), None) is None],
        "states": states,
        "declared": {
            name: (m.group(1).decode("ascii") if (m := _DECLARED_ENCODING.match(body)) else None)
            for name, body in source.candidates
            if name in names
        },
    }
    if not members:
        return {**result, "outcome": "not_inline", "occurrences": []}
    try:
        occurrences = parse_document_set(members)
    except DocumentSetError as exc:
        return {**result, "outcome": "error", "error": str(exc)}
    return {
        **result,
        "outcome": "inline",
        "occurrences": [{k: _jsonable(getattr(o, k)) for k in _OCCURRENCE_FIELDS} for o in occurrences],
    }


def _primary(page: str) -> list[str]:
    """The sequence-1 filenames of the SGML header records."""
    found = []
    for block in _SGML_DOCUMENT.findall(html.unescape(page)):
        sequence = _SGML_SEQUENCE.search(block)
        name = _SGML_FILENAME.search(block)
        if sequence and name and sequence.group(1).strip() == "1":
            found.append(name.group(1).strip())
    return found


def _instance(provider: SecFilingsProvider, base: str, primary: list[str]) -> list[list[str]] | str:
    """(contextRef, unitRef, value) of each non-nil fact of the concept in SEC's extracted instance
    (``{primary}_htm.xml``, PDS §2.3.7), or why it is unavailable."""
    if len(primary) != 1 or not primary[0].lower().endswith(".htm"):
        return f"oracle_unavailable:primary {primary}"
    body = provider.fetch_document_bytes(f"{base}{primary[0][: -len('.htm')]}_htm.xml", max_bytes=RESPONSE_MAX_BYTES)
    if body is None:
        return "oracle_unavailable:instance_not_found"
    root = ET.fromstring(body, ET.XMLParser(huge_tree=True, resolve_entities=False, no_network=True))
    facts = []
    for element in root.iter():
        if not isinstance(element.tag, str) or not element.tag.endswith("}" + CONCEPT):
            continue
        if element.get(XSI_NIL) in ("true", "1") or not (element.text or "").strip():
            continue
        try:
            value = Decimal(element.text.strip())  # type: ignore[union-attr]
        except InvalidOperation:
            return f"oracle_unavailable:not_decimal {element.text!r}"
        facts.append([element.get("contextRef", "").strip(), element.get("unitRef", "").strip(), str(value)])
    return facts


def _replay(provider: SecFilingsProvider, accession: str, cik: str) -> dict[str, Any]:
    target = Target(accession, date.min, cik, (), None, None)
    base = archive_dir_url(accession.replace("-", ""), int(cik))
    record: dict[str, Any] = {"accession": accession}
    try:
        archive: list[tuple[str, bytes]] | None = _archive_candidates(provider, f"{base}{accession}-xbrl.zip")
        record["archive"] = "read"
    except _ArchiveFallback as exc:
        archive = None
        record["archive"] = str(exc.args[0])
    try:
        listing: list[tuple[str, bytes]] | None = listing_candidates(provider, target)[0]
    except DocumentSetError as exc:
        listing = None
        record["listing_error"] = str(exc)
    page = provider.fetch_document_text(f"{base}{accession}-index-headers.html")
    if page is None:
        record["error"] = "header_not_found"
        return record
    header = parse_header_records(page)
    for name, candidates in (("archive", archive), ("listing", listing)):
        if candidates is not None:
            record[f"read_{name}"] = _read(DocumentSource(name, None, tuple(candidates), header, 0))
    primary = record.get("read_archive") or record.get("read_listing")
    if primary is not None and primary["outcome"] == "inline":
        record["instance"] = _instance(provider, base, _primary(page))
    return record


def _transient(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and (
        exc.response.status_code == 429 or exc.response.status_code >= 500
    )


def _replay_with_retries(provider: SecFilingsProvider, accession: str, cik: str) -> dict[str, Any]:
    """B4.4: a transport error is retried :data:`RETRIES` times, then the accession is ``incomplete``; any other error
    is recorded as one."""
    for attempt in range(RETRIES + 1):
        try:
            return _replay(provider, accession, cik)
        except Exception as exc:  # noqa: BLE001 — a transport error is retried; anything else is recorded
            if _transient(exc) and attempt < RETRIES:
                time.sleep(5 * (attempt + 1))
                continue
            return {"accession": accession, ("incomplete" if _transient(exc) else "error"): repr(exc)[:500]}
    raise AssertionError("unreachable")


def run(out: Path) -> None:
    manifest = json.loads((out / "manifest.json").read_text())
    results_path = out / "results.jsonl"
    done = set()
    if results_path.exists():
        done = {json.loads(line)["accession"] for line in results_path.read_text().splitlines() if line.strip()}
    work = _work(manifest)
    todo = [(a, w) for a, w in work.items() if a not in done]
    print(f"{len(work)} manifest accessions, {len(done)} done, {len(todo)} to replay", flush=True)
    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider, results_path.open("a") as sink:
        for index, (accession, item) in enumerate(todo):
            started = time.monotonic()
            cik = item["cik"]
            record: dict[str, Any] = (
                _replay_with_retries(provider, accession, str(cik))
                if cik is not None and str(cik).isdigit()
                else {"accession": accession, "error": f"no_numeric_cik:{cik}"}
            )
            record["seconds"] = round(time.monotonic() - started, 3)
            sink.write(json.dumps(record) + "\n")
            sink.flush()
            if index % 50 == 0:
                print(f"{index}/{len(todo)} {accession} {record.get('archive')} {record['seconds']}s", flush=True)


# --- B4.2-B4.4 readout --------------------------------------------------------------------------------------------


def _fields_equal(stored: dict[str, Any], replayed: dict[str, Any]) -> list[str]:
    differ = []
    for key in _OCCURRENCE_FIELDS:
        a, b = stored.get(key), replayed.get(key)
        if key == "instance_value" and a is not None and b is not None:
            if Decimal(a) != Decimal(b):
                differ.append(key)
        elif a != b:
            differ.append(key)
    return differ


def _canonical(occurrences: list[dict[str, Any]]) -> list[str]:
    return sorted(json.dumps(o, sort_keys=True) for o in occurrences)


def report(out: Path) -> int:
    manifest = json.loads((out / "manifest.json").read_text())
    results = {}
    for line in (out / "results.jsonl").read_text().splitlines():
        if line.strip():
            record = json.loads(line)
            results[record["accession"]] = record
    work = _work(manifest)
    baseline = {row["accession"]: row for row in manifest["baseline"]}
    frozen = {row["accession"]: row["instance_facts"] for row in manifest["frozen"]}
    counts: collections.Counter[str] = collections.Counter()
    blocking: list[str] = []
    listed: list[str] = []
    seconds: list[float] = []

    for accession, item in sorted(work.items()):
        record = results.get(accession)
        if record is None or "incomplete" in record:
            counts["incomplete"] += 1
            blocking.append(f"incomplete {accession}: {record and record.get('incomplete')}")
            continue
        seconds.append(record["seconds"])
        if "error" in record:
            counts["error"] += 1
            blocking.append(f"error {accession}: {record['error']}")
            continue
        counts[f"archive:{record['archive']}"] += 1
        archive, listing = record.get("read_archive"), record.get("read_listing")
        primary = archive or listing

        # B4.2 path invariance.
        undecidable = [
            f"{name}:{r['error']}"
            for name, r in (("archive", archive), ("listing", listing))
            if r and r["outcome"] == "error" and r["error"].startswith("undecidable:")
        ]
        if undecidable:
            counts["invariance_undecidable"] += 1
            listed.append(f"undecidable {accession}: {undecidable}")
        elif archive is None:
            counts["invariance_not_evidence"] += 1
            listed.append(
                f"archive {record['archive']} {accession}: listing "
                f"{listing['outcome'] if listing else record.get('listing_error')}"
            )
        elif listing is None:
            counts["invariance_fail"] += 1
            blocking.append(f"invariance {accession}: listing path failed ({record.get('listing_error')})")
        elif (
            archive.get("set") == listing.get("set")
            and archive["outcome"] == listing["outcome"]
            and archive.get("error") == listing.get("error")
            and _canonical(archive.get("occurrences", [])) == _canonical(listing.get("occurrences", []))
        ):
            counts["invariance_pass"] += 1
        else:
            counts["invariance_fail"] += 1
            blocking.append(
                f"invariance {accession}: archive {archive['outcome']} {archive.get('set')} vs listing"
                f" {listing['outcome']} {listing.get('set')}"
            )
        if primary is None:
            blocking.append(f"no source {accession}: {record.get('listing_error')}")
            continue

        # B4.3 against v1, baseline accessions.
        if accession in baseline:
            stored = baseline[accession]
            replayed = {(o["document"], o["element_ordinal"]): o for o in primary.get("occurrences", [])}
            if primary["outcome"] == "error":
                reason = primary["error"]
                if reason.startswith("target:") and reason.split(":", 1)[1] in primary.get("headerless", []):
                    listed.append(f"v1 exception (new member carries ix target) {accession}: {reason}")
                else:
                    blocking.append(f"v1 outcome {accession}: {stored['outcome']} -> error {reason}")
                counts["v1_error"] += 1
            else:
                if primary["outcome"] != stored["outcome"]:
                    blocking.append(f"v1 outcome {accession}: {stored['outcome']} -> {primary['outcome']}")
                lost = 0
                for occurrence in stored["occurrences"]:
                    key = (occurrence["document"], occurrence["element_ordinal"])
                    state = primary.get("states", {}).get(occurrence["document"])
                    if key not in replayed:
                        lost += 1
                        if state == "non_member":
                            listed.append(f"v1 exception (v1 document not a B1 member) {accession}: {key}")
                        elif state == "separate_set":
                            listed.append(f"v1 exception (separate set, accept on #3947) {accession}: {key}")
                            blocking.append(f"separate-set occurrence awaits acceptance {accession}: {key}")
                        else:
                            blocking.append(f"v1 loss {accession}: {key} (document state {state})")
                    elif differ := _fields_equal(occurrence, replayed[key]):
                        lost += 1
                        declared = primary.get("declared", {}).get(occurrence["document"])
                        blocking.append(
                            f"v1 field change {accession} {key} {differ} declared {declared}:"
                            f" {[(occurrence[d], replayed[key][d]) for d in differ]}"
                        )
                counts["v1_no_loss" if lost == 0 else "v1_loss"] += 1
                stored_keys = {(o["document"], o["element_ordinal"]) for o in stored["occurrences"]}
                added = [o for k, o in replayed.items() if k not in stored_keys]
                if added:
                    counts["v1_accessions_with_additions"] += 1
                    counts["v1_added_occurrences"] += len(added)
                for o in added:
                    if o["document"] not in primary.get("headerless", []):
                        blocking.append(f"v1 addition outside a header-less member {accession}: {o['document']}")
                    if o["unresolved"] is not None or (o["scale"] is not None and o["scale"] > 0):
                        counts["v1_added_scale_or_unresolved"] += 1
                        listed.append(
                            f"added scale>0/unresolved {accession} {o['document']}#{o['element_ordinal']}:"
                            f" scale {o['scale_raw']} unresolved {o['unresolved']}"
                        )

        # B4.3 the oracle, every accession with a set.
        if primary["outcome"] == "inline":
            instance = record.get("instance")
            if not isinstance(instance, list):
                counts["oracle_unavailable"] += 1
                blocking.append(f"oracle {accession}: {instance}")
            else:
                sec = {(c, u, Decimal(v)) for c, u, v in instance}
                ours = {
                    (
                        (o["context_ref_raw"] or "").strip(),
                        (o["unit_ref_raw"] or "").strip(),
                        Decimal(o["instance_value"]),
                    )
                    for o in primary["occurrences"]
                    if o["unresolved"] is None and o["instance_value"] is not None
                }
                if sec == ours:
                    counts["oracle_pass"] += 1
                else:
                    counts["oracle_fail"] += 1
                    unresolved = [
                        f"{o['document']}#{o['element_ordinal']}:{o['unresolved']}"
                        for o in primary["occurrences"]
                        if o["unresolved"] is not None
                    ]
                    blocking.append(
                        f"oracle {accession}: SEC-only {sorted(map(str, sec - ours))[:6]} ({len(sec - ours)}),"
                        f" v2-only {sorted(map(str, ours - sec))[:6]} ({len(ours - sec)}); unresolved {unresolved}"
                    )
                if accession in frozen:
                    if len(sec) != frozen[accession]:
                        blocking.append(f"frozen {accession}: instance {len(sec)} facts, frozen {frozen[accession]}")
                    counts["frozen_checked"] += 1
                    listed.append(
                        f"frozen {accession}: instance {len(sec)} (frozen {frozen[accession]}), v2 {len(ours)}"
                    )
        elif accession in frozen:
            blocking.append(f"frozen {accession}: v2 outcome {primary['outcome']} {primary.get('error')}")

    print(f"manifest {len(work)} accessions at {manifest['git_sha']} (frozen {manifest['frozen_at']})")
    for key, value in sorted(counts.items()):
        print(f"  {key}: {value}")
    if seconds:
        print(f"  seconds per accession: mean {statistics.mean(seconds):.2f}, median {statistics.median(seconds):.2f}")
    print(f"listed ({len(listed)}):")
    for line in listed:
        print(f"  {line}")
    print(f"blocking ({len(blocking)}):")
    for line in blocking:
        print(f"  {line}")
    return 1 if blocking else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("freeze", "run", "report"))
    parser.add_argument("dir", type=Path)
    args = parser.parse_args()
    if args.command == "freeze":
        freeze(args.dir)
    elif args.command == "run":
        run(args.dir)
    else:
        sys.exit(report(args.dir))


if __name__ == "__main__":
    main()
