"""#3624 slice 2: measure 8-K item metadata in SEC's bulk submissions archive, under the producer's own rules.

Read-only. Streams one ``submissions.zip`` (every ``CIK##########.json`` and its ``-submissions-NNN.json`` history
pages, for every CIK) and keeps every Form 8-K submission type (:data:`FORMS`) filed on or after :data:`FIRST`.
Identity is the producer's: a row is ``(cik, accession)``; an accession's *variants* are its distinct
``(form, filing_date, items)`` values over every appearance; an accession is a *conflict* when it has more than one
variant, detected over the whole archive and then reported for our CIKs. It reports:

* ``identity``: appearances, ``(cik, accession)`` rows, distinct accessions, multi-CIK accessions, same-CIK repeats,
  and conflicting accessions with the fields that differ (ours listed with every variant);
* ``validity``: item strings checked against the Form 8-K item vocabulary (:data:`VOCABULARY`), with the
  non-vocabulary tokens seen;
* ``by_year``: rows and accessions per form, candidates per reason (a target code, or items that are not valid),
  all CIKs and ours;
* ``labels``: per label and form, ``(cik, accession)`` rows and distinct accessions, ours and all, split by conflict;
* ``filing_events``: our CIKs' accessions in the archive against ``filing_events`` (joined through
  ``external_identifiers``), by year; the events-only accessions with every field and their archive membership; item
  agreement with mixed-NULL accessions compared on their non-NULL rows; form and filing-date agreement;
* ``cross_source``: Items 4.01 / 4.02 from the typed body parser, on non-tombstone ``eight_k_filings`` accessions the
  archive also holds;
* ``headers``: with ``--headers``, the EDGAR header of every candidate accession of our CIKs, plus a seeded sample of
  non-candidate accessions, each parsed for acceptance, type, filing date, filing-date change, items and filer CIKs
  and compared with the archive (body sha256 kept), and each of our ``(cik, accession)`` rows classed under the
  spec's production field contract (:func:`field_outcomes`, :func:`row_class`);
* ``previous_snapshot``: with ``--previous``, membership and per-field changes against an earlier archive.

The CIK list itself is written out, so a re-run can reproduce "our CIKs". Usage::

    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive <zip> [--previous <zip>] \\
        [--headers] [--negative-sample N] [--out results.json]

With ``--index-cache DIR`` it instead reconciles the archive against every 8-K-family row of EDGAR's quarterly
``full-index/master.gz`` files from :data:`FIRST` (:func:`index_reconciliation`).
With ``--daily-index-cache DIR`` it measures EDGAR's daily index files against both (:func:`daily_index`). Every
index file is read under one parse contract (:func:`index_rows`), which counts what it rejects.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import random
import re
import subprocess
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg

from app.config import settings

ARCHIVE: Final = Path.home() / "Library/Application Support/eBull/sec/bulk/submissions.zip"
#: Every EDGAR submission type of Form 8-K; all eight occur in the archive from :data:`FIRST`.
FORMS: Final = frozenset({"8-K", "8-K/A", "8-K12B", "8-K12B/A", "8-K12G3", "8-K12G3/A", "8-K15D5", "8-K15D5/A"})
#: Effective date of the current Form 8-K item numbering (Release 33-8400); earlier rows use other codes.
FIRST: Final = date(2004, 8, 23)
LABEL_ITEMS: Final = ("4.01", "4.02")
#: The item headings of Form 8-K, SEC 873 (02-25), sha256 730ab1de…: ``grep -o 'Item [0-9]\\.[0-9][0-9]'``.
VOCABULARY: Final = frozenset(
    "1.01 1.02 1.03 1.04 1.05 2.01 2.02 2.03 2.04 2.05 2.06 3.01 3.02 3.03 4.01 4.02 5.01 5.02 5.03 5.04 5.05 "
    "5.06 5.07 5.08 6.01 6.02 6.03 6.04 6.05 6.06 7.01 8.01 9.01".split()
)
SEED: Final = 3624
#: Archive item lists this long are header-checked: round 2 found 13-code lists truncated against the header, and
#: checking from 10 measures whether shorter lists are.
ITEM_CAP: Final = 10
#: Inventory fields every listed page must hold as arrays; ``items`` may be absent (its own ``missing`` policy).
_REQUIRED: Final = ("accessionNumber", "filingDate", "form")
#: Patterns of the consumer audit, run with ``git grep`` over ``app/`` and ``scripts/`` at the measured revision.
_AUDIT_PATTERNS: Final = (r"4\.0[12]", "sec_8k_item_codes", "red_flag_score")
_SEVERITY: Final = "SELECT code, severity FROM sec_8k_item_codes WHERE code = ANY(%(codes)s) ORDER BY code"

_CIKS: Final = """
    SELECT DISTINCT ei.identifier_value FROM external_identifiers ei
    WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik'
"""
#: ``filing_events`` rows for our CIKs: one row per (event row, CIK the instrument maps to).
_EVENTS: Final = """
    SELECT fe.provider_filing_id, fe.filing_type, fe.filing_date, fe.items, ei.identifier_value,
           (fe.created_at AT TIME ZONE 'America/New_York')::date - fe.filing_date
    FROM filing_events fe
    JOIN external_identifiers ei
      ON ei.instrument_id = fe.instrument_id AND ei.provider = 'sec' AND ei.identifier_type = 'cik'
    WHERE fe.provider = 'sec' AND fe.filing_type = ANY(%(forms)s)
"""
#: Item codes the typed body parser stored per non-tombstone accession; no item rows gives an empty array.
_TYPED: Final = """
    SELECT f.accession_number, coalesce(array_agg(i.item_code) FILTER (WHERE i.item_code IS NOT NULL), '{}')
    FROM eight_k_filings f LEFT JOIN eight_k_items i ON i.accession_number = f.accession_number
    WHERE NOT f.is_tombstone
    GROUP BY 1
"""
_AMENDMENT_LINKS: Final = "SELECT count(*), count(amends_accession) FROM sec_filing_manifest WHERE form = '8-K/A'"
#: Stored XBRL concepts, and those naming going concern or substantial doubt.
_FACT_CONCEPTS: Final = """
    SELECT count(DISTINCT concept),
           coalesce(array_agg(DISTINCT concept) FILTER (WHERE concept ~* 'going|substantial.?doubt'), '{}')
    FROM financial_facts_raw
"""

Items = tuple[str, ...] | str
#: One appearance: (cik, form, filing_date, items, page name).
Appearance = tuple[str, str, str, Items, str]
_NON_VOCABULARY: Counter[str] = Counter()
#: Pre-FIRST ``(cik, accession)`` rows seen by :func:`read_archive`, and those with a target token.
_PRE_FIRST: dict[str, set[tuple[str, str]]] = {"rows": set(), "target_rows": set()}


def parse_items(raw: Any, count: bool = True) -> Items:
    """Sorted vocabulary codes, or ``"missing"`` / ``"empty"`` / ``"invalid:<raw>"`` (raw kept, so two invalid
    lists stay distinguishable). Codes are matched exactly (ASCII); nothing is normalised. ``count`` adds the
    non-vocabulary tokens to the reported tally."""
    if raw is None:
        return "missing"
    if not isinstance(raw, str):
        return f"invalid:{raw!r}"
    if raw == "":
        return "empty"
    codes = raw.split(",")
    unknown = [code for code in codes if code not in VOCABULARY]
    if unknown:
        if count:
            _NON_VOCABULARY.update(unknown)
        return f"invalid:{raw}"
    return tuple(sorted(set(codes)))


def item_state(items: Items) -> str:
    return "valid" if isinstance(items, tuple) else items.split(":", 1)[0]


def valid(items: Items) -> bool:
    return isinstance(items, tuple)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 24), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_archive(path: Path, quality: Counter[str]) -> tuple[dict[str, list[Appearance]], dict[str, bool]]:
    """Accession -> every appearance (all CIKs), and CIK -> whether all its listed pages are present and aligned.

    Reads each ``CIK##########.json`` and exactly the pages it lists; every listed page is checked for presence and
    alignment before any form filter. A block without an ``items`` array gives ``"missing"`` on each row."""
    appearances: dict[str, list[Appearance]] = defaultdict(list)
    pages_complete: dict[str, bool] = {}
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        listed: set[str] = set()
        for name in sorted(n for n in names if n.startswith("CIK") and "-submissions-" not in n):
            quality["ciks_read"] += 1
            document = json.loads(archive.read(name))
            cik = name[3:13]
            recent = document["filings"]["recent"]
            quality["recent_keys_naming_amend"] += sum(1 for key in recent if "amend" in key.lower())
            blocks: list[tuple[str, dict[str, Any]]] = [("recent", recent)]
            complete = True
            listing = document["filings"].get("files")
            if not isinstance(listing, list) or any(
                not isinstance(e, dict) or not isinstance(e.get("name"), str) for e in listing
            ):
                quality["history_listing_missing" if listing is None else "history_listing_invalid"] += 1
                complete = False
                listing = []
            for entry in listing:
                listed.add(entry["name"])
                quality["history_pages_listed"] += 1
                if entry["name"] not in names:
                    quality["history_pages_missing"] += 1
                    complete = False
                    continue
                blocks.append((entry["name"], json.loads(archive.read(entry["name"]))))
            for page, block in blocks:
                if any(not isinstance(block.get(field), list) for field in _REQUIRED):
                    quality["pages_missing_required_fields"] += 1
                    complete = False
                    continue
                if len({len(value) for value in block.values() if isinstance(value, list)}) > 1:
                    quality["misaligned_pages"] += 1
                    complete = False
                    continue
                items = block.get("items")
                if items is None:
                    quality["blocks_without_items"] += 1
                for i, form in enumerate(block["form"]):
                    if form not in FORMS:
                        continue
                    filed = block["filingDate"][i]
                    if date.fromisoformat(filed) < FIRST:
                        # Release 33-8400's domain boundary: what pre-FIRST rows carry, not read further.
                        raw = None if items is None else items[i]
                        before = "missing" if raw is None else parse_items(raw, count=False)
                        quality[f"pre_first_appearances_{item_state(before)}"] += 1
                        row = (cik, block["accessionNumber"][i])
                        _PRE_FIRST["rows"].add(row)
                        # Exact target tokens, whatever the validity of the rest of the list.
                        if isinstance(raw, str) and set(raw.split(",")) & set(LABEL_ITEMS):
                            quality["pre_first_appearances_with_target_token"] += 1
                            _PRE_FIRST["target_rows"].add(row)
                        continue
                    parsed = "missing" if items is None else parse_items(items[i])
                    appearances[block["accessionNumber"][i]].append((cik, form, filed, parsed, page))
            pages_complete[cik] = complete
        quality["unlisted_pages"] = sum(1 for n in names if "-submissions-" in n and n not in listed)
    return appearances, pages_complete


def variants(apps: list[Appearance]) -> set[tuple[str, str, Items]]:
    return {(form, filed, items) for _, form, filed, items, _ in apps}


def candidate_reasons(apps: list[Appearance]) -> set[str]:
    reasons: set[str] = {"conflict"} if len(variants(apps)) > 1 else set()
    for _, _, _, items, _ in apps:
        if not valid(items):
            reasons.add(f"items_{item_state(items)}")
        else:
            reasons.update(code for code in LABEL_ITEMS if code in items)
            if len(items) >= ITEM_CAP:
                reasons.add("items_at_cap")
    return reasons


def identity(arch: dict[str, list[Appearance]], ours: set[str]) -> dict[str, Any]:
    """``all``: every appearance. ``ours``: appearances and rows restricted to our CIKs; accessions are those with
    at least one of our rows. Conflicts are detected over every appearance of the accession."""
    out: Counter[str] = Counter()
    differing: Counter[str] = Counter()
    listed: dict[str, list[list[Any]]] = {}
    for accession, apps in arch.items():
        mine = [x for x in apps if x[0] in ours]
        vs = variants(apps)
        for scope, scoped in (("all", apps), ("ours", mine)):
            if not scoped:
                continue
            rows = {x[0] for x in scoped}
            out[f"{scope}_appearances"] += len(scoped)
            out[f"{scope}_rows"] += len(rows)
            out[f"{scope}_accessions"] += 1
            out[f"{scope}_accessions_under_several_ciks"] += len({x[0] for x in apps}) > 1
            out[f"{scope}_excess_appearances"] += len(scoped) - len(rows)
            for n in Counter(x[0] for x in scoped).values():
                if n > 1:
                    out[f"{scope}_rows_appearing_{n}_times"] += 1
            if len(vs) > 1:
                out[f"{scope}_conflict_accessions"] += 1
                for field, index in (("form", 0), ("filing_date", 1), ("items", 2)):
                    if len({v[index] for v in vs}) > 1:
                        differing[f"{scope}_{field}"] += 1
        if mine and len(vs) > 1:
            listed[accession] = [list(a) for a in sorted(apps, key=str)]
    return {"counts": dict(sorted(out.items())), "conflict_fields": dict(sorted(differing.items())), "ours": listed}


def by_year(arch: dict[str, list[Appearance]], ours: set[str]) -> dict[str, dict[str, int]]:
    years: dict[str, Counter[str]] = defaultdict(Counter)
    for apps in arch.values():
        year = min(filed for _, _, filed, _, _ in apps)[:4]
        mine = [x for x in apps if x[0] in ours]
        reasons = candidate_reasons(apps)
        for scope, scoped in (("all", apps), ("ours", mine)):
            if not scoped:
                continue
            for form in {x[1] for x in scoped}:
                years[year][f"{scope}_accessions_{form}"] += 1
            years[year][f"{scope}_rows"] += len({x[0] for x in scoped})
            if reasons:
                years[year][f"{scope}_candidates"] += 1
                for reason in reasons:
                    years[year][f"{scope}_candidates_{reason}"] += 1
    return {year: dict(sorted(counts.items())) for year, counts in sorted(years.items())}


def labels(arch: dict[str, list[Appearance]], ours: set[str]) -> dict[str, Any]:
    out: dict[str, Counter[str]] = {code: Counter() for code in LABEL_ITEMS}
    for apps in arch.values():
        conflict = "conflict" if len(variants(apps)) > 1 else "single"
        for code in LABEL_ITEMS:
            holders = [a for a in apps if valid(a[3]) and code in a[3]]
            if not holders:
                continue
            mine = [a for a in holders if a[0] in ours]
            for scope, scoped in (("all", holders), ("ours", mine)):
                if not scoped:
                    continue
                for form in {a[1] for a in scoped}:
                    out[code][f"{scope}_{form}_accessions_{conflict}"] += 1
                for _, form in {(a[0], a[1]) for a in scoped}:
                    out[code][f"{scope}_{form}_rows_{conflict}"] += 1
    return {code: dict(sorted(counts.items())) for code, counts in out.items()}


def filing_events(arch: dict[str, list[Appearance]], ours: set[str], events: list[tuple[Any, ...]]) -> dict[str, Any]:
    """Our CIKs' archive accessions against ``filing_events``, from :data:`FIRST` to the archive's last date.

    Items are compared per distinct non-NULL ``filing_events`` item set (a contributor), not as a union: ``equal``
    means every contributor equals the archive's set. ``captured_within_3_days`` restricts to accessions whose
    earliest ``filing_events`` row was written 0 to 3 days (Eastern dates) after its filing date."""
    last = max(filed for apps in arch.values() for _, _, filed, _, _ in apps)
    db: dict[str, dict[str, Any]] = {}
    for accession, form, filed, items, cik, lag in events:
        entry = db.setdefault(
            accession, {"forms": set(), "dates": set(), "sets": set(), "nulls": 0, "rows": 0, "ciks": set()}
        )
        entry["forms"].add(form)
        entry["dates"].add(filed.isoformat())
        entry["rows"] += 1
        entry["ciks"].add(str(cik).zfill(10))
        if lag is not None:
            entry["lag"] = min(entry.get("lag", lag), lag)
        if items is None:
            entry["nulls"] += 1
        else:
            entry["sets"].add(frozenset(items))
    mine = {a for a, apps in arch.items() if any(cik in ours for cik, *_ in apps)}
    years: dict[str, Counter[str]] = defaultdict(Counter)
    agreement: Counter[str] = Counter()
    fields: Counter[str] = Counter()
    events_only: list[dict[str, Any]] = []
    for accession in mine | db.keys():
        a = arch.get(accession) if accession in mine else None
        o = db.get(accession)
        filed = min(x[2] for x in a) if a else min(o["dates"]) if o else ""
        if filed < FIRST.isoformat():
            continue
        year = filed[:4]
        if filed > last:  # outside the comparison window: counted apart, never in an in-window denominator
            years[year]["after_archive_accessions"] += 1
            years[year]["after_archive_event_rows"] += o["rows"] if o else 0
            continue
        if o:
            years[year]["event_rows"] += o["rows"]
            years[year]["event_accessions"] += 1
        if a and o:
            years[year]["both"] += 1
            archive_items = frozenset(code for x in a if valid(x[3]) for code in x[3])
            if any(not valid(x[3]) for x in a):
                category = "archive_not_valid"
            elif not o["sets"]:
                category = "events_all_null"
            else:
                kind = "mixed_null" if o["nulls"] else "no_null"
                category = f"{kind}_{'equal' if o['sets'] == {archive_items} else 'differ'}"
            agreement[category] += 1
            years[year][f"items_{category}"] += 1
            if o.get("lag") is not None and 0 <= o["lag"] <= 3:
                agreement[f"captured_within_3_days_{category}"] += 1
            fields["form_equal" if {x[1] for x in a} == o["forms"] else "form_differ"] += 1
            fields["date_equal" if {x[2] for x in a} == o["dates"] else "date_differ"] += 1
            fields["events_multi_form_or_date"] += len(o["forms"]) > 1 or len(o["dates"]) > 1
        elif a:
            years[year]["archive_only"] += 1
            years[year][f"archive_only_{'recent' if any(x[4] == 'recent' for x in a) else 'history'}"] += 1
        elif o:
            years[year]["events_only"] += 1
            elsewhere = arch.get(accession)
            events_only.append(
                {
                    "accession": accession,
                    "forms": sorted(o["forms"]),
                    "dates": sorted(o["dates"]),
                    "items": sorted(sorted(x) for x in o["sets"]),
                    "nulls": o["nulls"],
                    "event_ciks": sorted(o["ciks"]),
                    "archive_appearances": [list(x) for x in elsewhere] if elsewhere else None,
                }
            )
    return {
        "last_filing_date": last,
        "by_year": {y: dict(sorted(c.items())) for y, c in sorted(years.items())},
        "item_agreement": dict(sorted(agreement.items())),
        "field_agreement": dict(sorted(fields.items())),
        "events_only": events_only,
    }


def cross_source(arch: dict[str, list[Appearance]], typed: dict[str, list[str]]) -> dict[str, Any]:
    """Typed parser against the archive on non-tombstone ``eight_k_filings`` accessions the archive holds, kept only
    when every archive appearance has valid items (the excluded count is reported)."""
    held = [a for a in typed if a in arch]
    shared = [a for a in held if all(valid(x[3]) for x in arch[a])]
    out: dict[str, Any] = {
        "typed_accessions": len(typed),
        "held_by_archive": len(held),
        "excluded_not_valid": len(held) - len(shared),
        "shared": len(shared),
        "typed_with_no_item_rows": sum(1 for a in shared if not typed[a]),
    }
    for code in LABEL_ITEMS:
        in_archive = {a for a in shared if any(code in x[3] for x in arch[a])}
        in_typed = {a for a in shared if code in typed[a]}
        out[code] = {
            "archive_positive": len(in_archive),
            "typed_positive": len(in_typed),
            "both": len(in_archive & in_typed),
            "archive_only": len(in_archive - in_typed),
            "archive_only_typed_has_no_item_rows": sum(1 for a in in_archive - in_typed if not typed[a]),
            "typed_only": sorted(in_typed - in_archive),
        }
    return out


#: The SGML header file in the filing's folder.
_HEADER_URL: Final = "https://www.sec.gov/Archives/edgar/data/{cik}/{bare}/{accession}.hdr.sgml"
_SGML: Final = re.compile(r"<SEC-HEADER>(.*?)</SEC-HEADER>", re.S)
_CIK: Final = re.compile(r"^[ \t]*<CIK>(.*)$", re.M)
#: Header tags that must occur at most once.
_SCALAR_TAGS: Final = ("ACCESSION-NUMBER", "ACCEPTANCE-DATETIME", "TYPE", "FILING-DATE", "DATE-OF-FILING-DATE-CHANGE")
#: PDS markers of a non-ordinary submission: paper, private-to-public release, confirming copy of a paper filing.
#: Each is its own field outcome, and any one makes the row ``items_unestablished``.
MARKERS: Final = ("PAPER", "PRIVATE-TO-PUBLIC", "CONFIRMING-COPY")
#: PDS post-acceptance correction evidence (pp. 41-42): any one present gives ``correction_evidence``, which also
#: makes the row ``items_unestablished``.
CORRECTION_TAGS: Final = ("CORRECTION", "DELETION", "TIMESTAMP")


#: An opening tag anywhere, and one that starts its line after optional blanks (the layout the grammar accepts).
_ANY_TAG: Final = re.compile(r"<([A-Z][A-Z0-9-]*)>")
_LINE_TAG: Final = re.compile(r"^[ \t]*<([A-Z][A-Z0-9-]*)>", re.M)


def parse_header(text: str) -> dict[str, Any]:
    """The SGML header's acceptance (Eastern wall clock), type, filing date, filing-date change, items, filer CIKs.

    Tags are counted wherever they occur in the retrieved body, inside the header block or not; a value is read only
    from a tag that starts its line (after blanks). ``misplaced_tags`` counts opening tags that do not, and any makes
    the parse ``malformed``, so an unexpected layout fails closed instead of hiding a tag."""
    found = _SGML.search(text)
    body = found.group(1) if found else text

    def one(tag: str) -> str | None:
        match = re.search(rf"^[ \t]*<{tag}>(.*)$", body, re.M)
        return match.group(1).strip() if match else None

    def anywhere(tag: str) -> int:
        return len(re.findall(rf"<{tag}>", text))

    return {
        "header_blocks": len(_SGML.findall(text)),
        "misplaced_tags": len(_ANY_TAG.findall(text)) - len(_LINE_TAG.findall(text)),
        # Every opening tag name anywhere in the raw body, for the tag-presence census.
        "raw_tags": sorted(set(_ANY_TAG.findall(text))),
        "repeated_tags": sorted(t for t in _SCALAR_TAGS if anywhere(t) > 1),
        "accession_numbers": [m.strip() for m in re.findall(r"^[ \t]*<ACCESSION-NUMBER>(.*)$", body, re.M)],
        "markers": sorted(t for t in MARKERS if anywhere(t)),
        "correction_tags": sorted(t for t in CORRECTION_TAGS if anywhere(t)),
        "acceptance_et": one("ACCEPTANCE-DATETIME"),
        "type": one("TYPE"),
        "filing_date": one("FILING-DATE"),
        "filing_date_change": one("DATE-OF-FILING-DATE-CHANGE"),
        "items": sorted(m.strip() for m in re.findall(r"^[ \t]*<ITEMS>(.*)$", body, re.M)),
        "filer_ciks": sorted(
            {m.strip() for block in re.findall(r"<FILER>(.*?)</FILER>", body, re.S) for m in _CIK.findall(block)}
        ),
        "other_ciks": sorted({m.strip() for m in _CIK.findall(re.sub(r"<FILER>.*?</FILER>", "", body, flags=re.S))}),
    }


def fetch_header(client: httpx.Client, cik: str, accession: str, cache: Path) -> str | None:
    """Store the header body under ``cache`` (kept across runs); the error after three attempts, else ``None``."""
    path = cache / f"{accession}.sgml"
    if path.exists():
        return None
    url = _HEADER_URL.format(cik=int(cik), bare=accession.replace("-", ""), accession=accession)
    error = None
    for attempt, pause in enumerate((2, 8, 0)):
        try:
            response = client.get(url)
            response.raise_for_status()
            path.with_suffix(".tmp").write_bytes(response.content)
            path.with_suffix(".tmp").rename(path)
            return None
        except httpx.HTTPError as exc:
            error = f"attempt {attempt + 1}: {exc!r}"
            time.sleep(pause)
        finally:
            time.sleep(0.8)  # four workers: at most 5 req/s, half SEC's 10, which the manifest worker shares
    return error


def read_header(cache: Path, accession: str) -> dict[str, Any]:
    body = (cache / f"{accession}.sgml").read_bytes()
    return {**parse_header(body.decode("latin-1")), "sha256": hashlib.sha256(body).hexdigest()}


def _ymd(value: str | None) -> str | None:
    """``YYYY-MM-DD`` of a header date or datetime that is a real calendar value, else ``None``."""
    if not value or not value.isdigit() or len(value) not in (8, 14):
        return None
    try:
        parsed = datetime.strptime(value, "%Y%m%d%H%M%S" if len(value) == 14 else "%Y%m%d")
    except ValueError:
        return None
    return parsed.date().isoformat()


#: The spec's class decision table, first match wins: a row is ``items_unestablished`` if any of these fields is not
#: ok, else ``timing_in_doubt`` if any of the next, else ``label_in_doubt`` if any of the last, else ``ok``.
CLASS_FIELDS: Final = (
    (
        "items_unestablished",
        (
            "retrieval",
            "parse",
            "accession",
            "paper",
            "private_to_public",
            "confirming_copy",
            "correction",
            "correction_evidence",
        ),
    ),
    ("timing_in_doubt", ("acceptance", "filing_date", "date_order")),
    ("label_in_doubt", ("type", "items", "filer")),
)
_CORRECTION_OUTCOME: Final = {
    "equal": "ok",
    "later": "corrected",
    "earlier": "correction_before_acceptance",
    "missing": "no_correction_date",
    "invalid": "correction_invalid",
    "acceptance_unusable": "not_evaluated",
}


def field_outcomes(header: dict[str, Any] | None, accession: str, cik: str) -> dict[str, str]:
    """The spec's field contract for one ``(cik, accession)`` row; a field whose inputs are not ok is
    ``not_evaluated``. ``header`` is ``None`` when no body was retrieved."""
    if header is None:
        return {"retrieval": "no_header"}
    raw_accepted, raw_filed = header["acceptance_et"], header["filing_date"]
    accepted = _ymd(raw_accepted) if len(raw_accepted or "") == 14 else None
    filed = _ymd(raw_filed) if len(raw_filed or "") == 8 else None
    numbers, items = header["accession_numbers"], header["items"]
    out = {
        "retrieval": "ok",
        "parse": "ok"
        if header["header_blocks"] == 1 and not header["repeated_tags"] and not header["misplaced_tags"]
        else "malformed",
        "accession": "ok" if numbers == [accession] else "no_accession" if not numbers else "accession_mismatch",
        **{
            m.lower().replace("-", "_"): m.lower().replace("-", "_") if m in header["markers"] else "ok"
            for m in MARKERS
        },
        "acceptance": "ok" if accepted else "no_acceptance" if raw_accepted is None else "acceptance_invalid",
        "filing_date": "ok" if filed else "no_filing_date" if raw_filed is None else "filing_date_invalid",
        "type": "ok" if header["type"] in FORMS else "type_out_of_scope",
        "items": "items_empty"
        if not items
        else "items_invalid"
        if any(code not in VOCABULARY for code in items) or len(set(items)) != len(items)
        else "ok",
        "filer": "ok" if cik in header["filer_ciks"] else "not_a_filer",
        "date_order": "not_evaluated"
        if not (accepted and filed)
        else "ok"
        if filed >= accepted
        else "date_before_acceptance",
        "correction": _CORRECTION_OUTCOME[correction_category(header)],
        "correction_evidence": "correction_evidence" if header["correction_tags"] else "ok",
    }
    return out


def row_class(outcomes: dict[str, str]) -> str:
    """First matching class of :data:`CLASS_FIELDS`; an absent field (not evaluated) is not ok."""
    for name, fields in CLASS_FIELDS:
        if any(outcomes.get(field, "not_evaluated") != "ok" for field in fields):
            return name
    return "ok"


def correction_category(header: dict[str, Any]) -> str:
    """The PDS correction field against the acceptance date: ``later``, ``equal``, ``earlier``, ``missing``,
    ``invalid``, or ``acceptance_unusable`` when there is no valid acceptance date to compare with."""
    accepted = _ymd(header["acceptance_et"]) if len(header["acceptance_et"] or "") == 14 else None
    raw = header["filing_date_change"]
    if accepted is None:
        return "acceptance_unusable"
    if raw is None:
        return "missing"
    change = _ymd(raw) if len(raw) == 8 else None
    if change is None:
        return "invalid"
    return "later" if change > accepted else "equal" if change == accepted else "earlier"


def compare_header(apps: list[Appearance], header: dict[str, Any]) -> list[str]:
    """Where the archive and the header disagree, and header states the producer treats as uncertain, as tags."""
    tags: list[str] = ["repeated_tag"] if header["repeated_tags"] else []
    if header["header_blocks"] != 1:
        tags.append("malformed")
    if any(not valid(x[3]) or list(x[3]) != header["items"] for x in apps):
        tags.append("items")
    if not header["items"]:
        tags.append("header_items_empty")
    if any(code not in VOCABULARY for code in header["items"]):
        tags.append("header_items_not_vocabulary")
    if any(x[1] != header["type"] for x in apps):
        tags.append("form")
    if any(x[2] != _ymd(header["filing_date"]) for x in apps):
        tags.append("filing_date")
    archive_ciks = {x[0] for x in apps}
    if archive_ciks - set(header["filer_ciks"]):
        tags.append("archive_cik_not_filer")
    if set(header["filer_ciks"]) - archive_ciks:
        tags.append("filer_not_in_archive")
    raw_accepted, raw_filed = header["acceptance_et"], header["filing_date"]
    accepted = _ymd(raw_accepted) if len(raw_accepted or "") == 14 else None
    filed = _ymd(raw_filed) if len(raw_filed or "") == 8 else None
    if raw_accepted is None:
        tags.append("no_acceptance")
    elif accepted is None:
        tags.append("acceptance_invalid")
    if raw_filed is None:
        tags.append("no_filing_date")
    elif filed is None:
        tags.append("filing_date_invalid")
    if accepted and filed:
        if filed < accepted:
            tags.append("filing_date_before_acceptance")
        elif filed > accepted:
            tags.append("filing_date_after_acceptance")
    correction = correction_category(header)  # PDS: "Date when the last Post Acceptance occurred"
    tag = {
        "later": "post_acceptance_change",
        "missing": "no_filing_date_change",
        "invalid": "filing_date_change_invalid",
        "earlier": "correction_before_acceptance",
    }.get(correction)
    if tag:
        tags.append(tag)
    return tags


def headers(arch: dict[str, list[Appearance]], ours: set[str], negatives: int, cache: Path) -> dict[str, Any]:
    """Every candidate accession of our CIKs, plus a seeded sample of our non-candidate accessions."""
    mine = sorted(a for a, apps in arch.items() if any(cik in ours for cik, *_ in apps))
    candidates = [a for a in mine if candidate_reasons(arch[a])]
    pool = [a for a in mine if not candidate_reasons(arch[a])]
    sample = random.Random(SEED).sample(pool, negatives) if negatives else []
    work = [("candidate", a) for a in candidates] + [("negative", a) for a in sample]
    cache.mkdir(parents=True, exist_ok=True)
    errors: dict[str, str] = {}
    with (
        httpx.Client(timeout=30, headers={"User-Agent": settings.sec_user_agent}) as client,
        ThreadPoolExecutor(max_workers=4) as pool_,
    ):
        futures = {
            pool_.submit(fetch_header, client, next(c for c, *_ in arch[a] if c in ours), a, cache): a for _, a in work
        }
        for done, future in enumerate(as_completed(futures), 1):
            if (error := future.result()) is not None:
                errors[futures[future]] = error
            if done % 500 == 0:
                print(f"headers {done}/{len(work)}", flush=True)
    out: dict[str, Any] = {"candidates": len(candidates), "negatives": len(sample), "seed": SEED}
    tags: dict[str, Counter[str]] = {"candidate": Counter(), "negative": Counter()}
    lag: Counter[str] = Counter()
    presence: Counter[str] = Counter()  # headers carrying each raw tag at least once
    rows: list[dict[str, Any]] = []
    failures: list[list[str]] = []
    for kind, accession in work:
        if accession in errors:
            failures.append([kind, accession, errors[accession]])
            continue
        header = read_header(cache, accession)
        presence.update(header.pop("raw_tags"))
        found = compare_header(arch[accession], header)
        tags[kind].update(found or ["agree"])
        accepted, filed = _ymd(header["acceptance_et"]), _ymd(header["filing_date"])
        if accepted and filed:
            lag[str((date.fromisoformat(filed) - date.fromisoformat(accepted)).days)] += 1
        archive_rows = [[x[0], x[1], x[2], list(x[3]) if valid(x[3]) else x[3]] for x in arch[accession]]
        # Production predicates, per (cik, accession) row of our CIKs.
        outcomes = {cik: field_outcomes(header, accession, cik) for cik in sorted({x[0] for x in arch[accession]})}
        classes = {cik: row_class(o) for cik, o in outcomes.items() if cik in ours}
        rows.append(
            {
                "kind": kind,
                "accession": accession,
                "tags": found,
                "archive": archive_rows,
                "classes": classes,
                "field_outcomes": outcomes,
                **header,
            }
        )
    later = sorted(r["acceptance_et"][8:] for r in rows if "filing_date_after_acceptance" in r["tags"])
    same_late = sum(
        1
        for r in rows
        if r["acceptance_et"]
        and not r["tags"].count("filing_date_after_acceptance")
        and _ymd(r["acceptance_et"]) == _ymd(r["filing_date"])
        and r["acceptance_et"][8:] > "173000"
    )
    corrections = Counter(f"{r['kind']}_{correction_category(r)}" for r in rows)
    # Class census under the production predicates, in both units: (cik, accession) rows of our CIKs, and
    # accessions (counted once under each class any of its rows has). Field outcomes are per row.
    class_rows: Counter[str] = Counter()
    class_accessions: Counter[str] = Counter()
    outcome_rows: Counter[str] = Counter()
    for r in rows:
        class_rows[f"{r['kind']}_rows"] += len(r["classes"])
        class_accessions[f"{r['kind']}_accessions"] += 1
        class_rows.update(f"{r['kind']}_{c}" for c in r["classes"].values())
        class_accessions.update(f"{r['kind']}_{c}" for c in set(r["classes"].values()))
        for cik in r["classes"]:
            outcome_rows.update(f"{field}={value}" for field, value in r["field_outcomes"][cik].items())
    marker_presence = Counter(m for r in rows for m in r["markers"])
    for kind, accession, _ in failures:
        n_rows = len({x[0] for x in arch[accession] if x[0] in ours})
        class_rows[f"{kind}_rows"] += n_rows
        class_rows[f"{kind}_items_unestablished"] += n_rows
        class_accessions[f"{kind}_accessions"] += 1
        class_accessions[f"{kind}_items_unestablished"] += 1
    lengths = Counter(
        f"{len(next(x for x in r['archive'] if isinstance(x[3], list))[3])}_{'items' in r['tags']}"
        for r in rows
        if any(isinstance(x[3], list) for x in r["archive"])
    )
    out.update(
        {
            "later_dated_acceptance_range": [later[0], later[-1]] if later else None,
            "same_day_dated_accepted_after_1730": same_late,
            "correction_field": dict(sorted(corrections.items())),
            "raw_tag_presence": dict(sorted(presence.items())),
            "header_block_counts": dict(Counter(str(r["header_blocks"]) for r in rows)),
            "class_rows": dict(sorted(class_rows.items())),
            "class_accessions": dict(sorted(class_accessions.items())),
            "field_outcome_rows": dict(sorted(outcome_rows.items())),
            "marker_presence_in_header_block": dict(sorted(marker_presence.items())),
            "archive_item_count_by_disagreement": dict(sorted(lengths.items(), key=lambda kv: kv[0])),
            "tags": {k: dict(sorted(v.items())) for k, v in tags.items()},
            "filing_minus_acceptance_days": dict(sorted(lag.items(), key=lambda kv: int(kv[0]))),
            "failures": failures,
            "rows": rows,
        }
    )
    return out


def compare_snapshots(new: dict[str, list[Appearance]], old: dict[str, list[Appearance]]) -> dict[str, Any]:
    """Accessions filed by the older archive's last date: membership and per-field changes, all CIKs."""
    last_old = max(filed for apps in old.values() for _, _, filed, _, _ in apps)
    out: Counter[str] = Counter()
    for accession in old.keys() | new.keys():
        o, n = old.get(accession), new.get(accession)
        if n is not None and o is None:
            out["new_only_by_old_last_date" if min(x[2] for x in n) <= last_old else "new_after_old_last_date"] += 1
            continue
        if n is None or o is None:
            out["old_only"] += 1
            continue
        out["shared"] += 1
        if {x[0] for x in o} != {x[0] for x in n}:
            out["membership_changed"] += 1
        for cik in {x[0] for x in o}:
            before = variants([x for x in o if x[0] == cik])
            after = variants([x for x in n if x[0] == cik])
            if after and before != after:
                for field, index in (("form", 0), ("filing_date", 1), ("items", 2)):
                    if {v[index] for v in before} != {v[index] for v in after}:
                        out[f"row_{field}_changed"] += 1
    return {"old_last_filing_date": last_old, **dict(sorted(out.items()))}


_INDEX_URL: Final = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.gz"
#: ``edgar/data/<cik>/<accession>.txt``, the accession in its PDS format (10-digit CIK, 2-digit year, 6-digit
#: sequence).
_INDEX_FILE: Final = re.compile(r"edgar/data/[0-9]{1,10}/[0-9]{10}-[0-9]{2}-[0-9]{6}\.txt")


def index_rows(text: str, audit: Counter[str], daily: bool) -> list[tuple[str, str, str, str]]:
    """Data rows ``(cik, form, date_filed, accession)`` of an EDGAR ``master`` index, date as ``YYYY-MM-DD``.

    Quarterly files must date rows ``YYYY-MM-DD``; daily files may use ``YYYYMMDD`` or ``YYYY-MM-DD`` (each format
    counted). Data rows are the lines after the dashed separator under the ``CIK|…`` column header. A name holding
    ``|`` is recovered from the four fixed fields around it (counted). Any other non-blank line that is not a valid
    row is counted in ``audit`` as ``rejected_<reason>``, never skipped silently; a missing column header or
    separator rejects the whole file."""
    lines = text.splitlines()
    head = next((i for i, line in enumerate(lines) if line.startswith("CIK|")), None)
    if head is None or head + 1 >= len(lines) or not lines[head + 1].startswith("---"):
        audit["rejected_file_without_column_header_or_separator"] += 1
        return []
    out: list[tuple[str, str, str, str]] = []
    for line in lines[head + 2 :]:
        if not line.strip():
            audit["blank_lines"] += 1
            continue
        parts = line.split("|")
        if len(parts) > 5:
            audit["recovered_name_with_separator"] += 1
            parts = [parts[0], "|".join(parts[1:-3]), *parts[-3:]]
        if len(parts) != 5:
            audit["rejected_field_count"] += 1
            continue
        cik, _, form, raw, path = parts
        compact = daily and re.fullmatch(r"[0-9]{8}", raw) is not None
        filed = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}" if compact else raw
        if not re.fullmatch(r"[0-9]{1,10}", cik):  # ASCII only: latin-1 superscripts pass ``isdigit``
            audit["rejected_cik"] += 1
        elif not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", filed) or _ymd(filed.replace("-", "")) is None:
            audit["rejected_date"] += 1
        elif not _INDEX_FILE.fullmatch(path):
            audit["rejected_file_name"] += 1
        else:
            audit[f"date_format_{'compact' if compact else 'iso'}"] += 1
            out.append((cik.zfill(10), form, filed, path.rsplit("/", 1)[-1].removesuffix(".txt")))
    return out


def quarters(first: date, last: date) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    year, quarter = first.year, (first.month - 1) // 3 + 1
    while (year, quarter) <= (last.year, (last.month - 1) // 3 + 1):
        out.append((year, quarter))
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return out


def index_reconciliation(archive_path: Path, cache: Path, ours: set[str]) -> dict[str, Any]:
    """Every 8-K-family row of EDGAR's quarterly ``full-index/master.gz`` from :data:`FIRST` against the archive.

    The master files are cached under ``cache`` and named in the output by sha256 with their ``Last Data Received``
    line. A row is ``(cik, accession)``; ``index_only`` rows are split by whether the archive holds the CIK's file."""
    arch, _ = read_archive(archive_path, Counter())
    archive_rows = {(cik, accession) for accession, apps in arch.items() for cik, *_ in apps}
    last = max(filed for apps in arch.values() for _, _, filed, _, _ in apps)
    with zipfile.ZipFile(archive_path) as archive:
        cik_files = {name[3:13] for name in archive.namelist() if name.startswith("CIK") and "-" not in name}
    cache.mkdir(parents=True, exist_ok=True)
    (cache / "headers").mkdir(exist_ok=True)
    files: dict[str, dict[str, Any]] = {}
    quarterly: dict[tuple[str, str], tuple[str, str]] = {}
    parse_total: Counter[str] = Counter()
    # Whether a quarter file is organised by date filed: every row, all forms, against its file's quarter.
    placement: Counter[str] = Counter()
    with httpx.Client(timeout=120, headers={"User-Agent": settings.sec_user_agent}) as client:
        for year, quarter in quarters(FIRST, date.today()):
            path = cache / f"master_{year}_QTR{quarter}.gz"
            if not path.exists():
                response = client.get(_INDEX_URL.format(year=year, quarter=quarter))
                response.raise_for_status()
                path.write_bytes(response.content)
                time.sleep(0.2)
            text = gzip.decompress(path.read_bytes()).decode("latin-1")
            received = next((line for line in text.splitlines()[:5] if line.startswith("Last Data Received")), "")
            files[f"{year}Q{quarter}"] = {"sha256": sha256_file(path), "last_data_received": received[19:].strip()}
            start = date(year, 3 * quarter - 2, 1).isoformat()
            end = (date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)).isoformat()
            audit: Counter[str] = Counter()
            for cik, form, filed, accession in index_rows(text, audit, daily=False):
                placement["before_quarter" if filed < start else "after_quarter" if filed >= end else "in"] += 1
                if form in FORMS and filed < FIRST.isoformat():
                    placement["family_rows_dated_before_first"] += 1
                if form not in FORMS or filed < FIRST.isoformat():
                    continue
                quarterly[(cik, accession)] = (form, filed)
            files[f"{year}Q{quarter}"]["parse"] = dict(sorted(audit.items()))
            parse_total.update(audit)
    by_quarter: dict[str, Counter[str]] = defaultdict(Counter)
    stale: list[tuple[str, str, str, str]] = []
    for key, (form, filed) in quarterly.items():
        bucket = by_quarter[f"{filed[:4]}Q{(int(filed[5:7]) - 1) // 3 + 1}"]
        bucket["index_rows"] += 1
        if key in archive_rows:
            continue
        where = "after_archive" if filed > last else "cik_file_present" if key[0] in cik_files else "cik_file_absent"
        bucket[f"index_only_{where}"] += 1
        if filed <= last:
            stale.append((*key, form, filed))
    for cik, accession in archive_rows - quarterly.keys():
        filed = min(x[2] for x in arch[accession])
        by_quarter[f"{filed[:4]}Q{(int(filed[5:7]) - 1) // 3 + 1}"]["archive_only"] += 1
    reconciled: list[dict[str, Any]] = []
    with (
        zipfile.ZipFile(archive_path) as archive,
        httpx.Client(timeout=30, headers={"User-Agent": settings.sec_user_agent}) as client,
    ):
        for cik, accession, form, filed in sorted(stale):
            name = f"CIK{cik}.json"
            latest = None
            if name in archive.namelist():
                recent = json.loads(archive.read(name))["filings"]["recent"]
                latest = max(recent["filingDate"], default=None)
            error = fetch_header(client, cik, accession, cache / "headers")
            reconciled.append(
                {
                    "cik": cik,
                    "accession": accession,
                    "index_form": form,
                    "index_date": filed,
                    "ours": cik in ours,
                    "archive_file_latest_filing_date": latest,
                    "header": read_header(cache / "headers", accession) if error is None else None,
                    "header_error": error,
                }
            )
    return {
        "archive_sha256": sha256_file(archive_path),
        "archive_last_filing_date": last,
        "index_files": files,
        "index_parse": dict(sorted(parse_total.items())),
        "row_placement_by_date_filed": dict(sorted(placement.items())),
        "by_quarter": {q: dict(sorted(c.items())) for q, c in sorted(by_quarter.items())},
        "index_only_by_archive_last_date": reconciled,
    }


_DAILY_URL: Final = "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/{name}"
#: Some quarters (2011 Q3-Q4, 2013 Q1, Q3-Q4, 2014 Q2) publish only ``.idx.gz``.
_DAILY_NAME: Final = re.compile(r"master\.(\d{8})\.idx(\.gz)?")


def _fetch_cached(client: httpx.Client, url: str, path: Path) -> bytes:
    """``url``'s body, read from ``path`` when cached there, else fetched (three attempts) and cached."""
    if path.exists():
        return path.read_bytes()
    for attempt in range(3):
        try:
            response = client.get(url)
            response.raise_for_status()
            path.with_suffix(".tmp").write_bytes(response.content)
            path.with_suffix(".tmp").rename(path)
            return response.content
        except httpx.HTTPError:
            if attempt == 2:
                raise
            time.sleep((2, 8)[attempt])
        finally:
            time.sleep(0.8)  # four workers: at most 5 req/s, half SEC's 10, which the manifest worker shares
    raise AssertionError("unreachable")


def daily_index(archive_path: Path, cache: Path, index_cache: Path, measurement: Path) -> dict[str, Any]:
    """EDGAR's daily ``master.YYYYMMDD.idx`` files from :data:`FIRST`: what each says was disseminated that day.

    Per file: its day and its listing's ``last-modified`` (the write lag). Per 8-K-family ``(cik, accession)`` row:
    the days it appears on. Reconciled with the cached quarterly ``master.gz`` rows and the archive, and, for each
    header row of ``measurement``, the first daily-index day against the header's acceptance and filing dates."""
    arch, _ = read_archive(archive_path, Counter())
    archive_rows = {(cik, accession) for accession, apps in arch.items() for cik, *_ in apps}
    cache.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, Any]] = []
    listings: dict[str, dict[str, str]] = {}
    unmatched: list[str] = []
    with httpx.Client(timeout=120, headers={"User-Agent": settings.sec_user_agent}) as client:
        for year, quarter in quarters(FIRST, date.today()):
            listing_url = _DAILY_URL.format(year=year, quarter=quarter, name="index.json")
            listing_path = cache / f"index_{year}_QTR{quarter}.json"
            raw_listing = _fetch_cached(client, listing_url, listing_path)
            listings[listing_path.name] = {
                "sha256": hashlib.sha256(raw_listing).hexdigest(),
                "cached_at": _mtime(listing_path),
            }
            listing = json.loads(raw_listing)
            for item in listing["directory"]["item"]:
                name = item["name"]
                if not name.startswith("master."):
                    continue
                found = _DAILY_NAME.fullmatch(name)
                if not found:
                    unmatched.append(f"{year}Q{quarter}/{name}")
                    continue
                day = datetime.strptime(found.group(1), "%Y%m%d").date()
                if day < FIRST:
                    continue
                stamp = item["last-modified"]  # some listings give a date only; no time zone is stated
                precise = " " in stamp
                modified = datetime.strptime(stamp, "%m/%d/%Y %I:%M:%S %p" if precise else "%m/%d/%Y")
                files.append(
                    {
                        "day": day.isoformat(),
                        "year": year,
                        "quarter": quarter,
                        "name": name,
                        "listing": listing_path.name,
                        "last_modified_raw": stamp,
                        "last_modified_precision": "second" if precise else "date",
                        "lag_days": (modified.date() - day).days,
                    }
                )
        with ThreadPoolExecutor(max_workers=4) as pool:
            bodies = dict(
                zip(
                    [f["name"] for f in files],
                    pool.map(
                        lambda f: _fetch_cached(
                            client,
                            _DAILY_URL.format(year=f["year"], quarter=f["quarter"], name=f["name"]),
                            cache / f["name"],
                        ),
                        files,
                    ),
                    strict=True,
                )
            )
    days: dict[tuple[str, str], list[str]] = defaultdict(list)
    dated: dict[tuple[str, str], str] = {}
    shape: Counter[str] = Counter()
    parse_total: Counter[str] = Counter()
    for f in files:
        body = bodies[f["name"]]
        text = (gzip.decompress(body) if f["name"].endswith(".gz") else body).decode("latin-1")
        received = next((line for line in text.splitlines()[:5] if line.startswith("Last Data Received")), "")
        f["last_data_received"] = received[19:].strip()
        # The listing and body are bound by their sha256s and cache times (the listing names the body).
        f["sha256"] = hashlib.sha256(body).hexdigest()
        f["listing_sha256"] = listings[f["listing"]]["sha256"]
        f["cached_at"] = _mtime(cache / f["name"])
        audit: Counter[str] = Counter()
        for cik, form, filed, accession in index_rows(text, audit, daily=True):
            if form not in FORMS:
                continue
            key = (cik, accession)
            days[key].append(f["day"])
            dated.setdefault(key, filed)
            shape["rows"] += 1
        if audit:
            f["parse"] = dict(sorted(audit.items()))
        parse_total.update(audit)
    # Quarterly rows of every date (8-K family), so pre-FIRST daily rows are reconciled against unfiltered sets.
    quarterly: dict[tuple[str, str], str] = {}
    quarterly_files: dict[str, str] = {}
    quarterly_parse: Counter[str] = Counter()
    for path in sorted(index_cache.glob("master_*.gz")):
        quarterly_files[path.name] = sha256_file(path)
        text = gzip.decompress(path.read_bytes()).decode("latin-1")
        for cik, form, filed, accession in index_rows(text, quarterly_parse, daily=False):
            if form in FORMS:
                quarterly[(cik, accession)] = filed
    archive_all = archive_rows | _PRE_FIRST["rows"]
    first_day = {key: min(v) for key, v in days.items()}
    lag_of = {f["day"]: f["lag_days"] for f in files}
    recon: Counter[str] = Counter()
    neither: dict[str, Counter[str]] = {"all": Counter(), "from_first": Counter()}  # by year of first listing
    unexplained: list[tuple[str, str, str]] = []
    examples: dict[str, list[str]] = defaultdict(list)
    for key, day in first_day.items():
        scopes = ("all", "before_first" if dated[key] < FIRST.isoformat() else "from_first")
        offset = (date.fromisoformat(day) - date.fromisoformat(dated[key])).days
        recon[f"first_day_minus_date_filed_{offset if -3 <= offset <= 5 else 'other'}"] += 1
        lag = lag_of[day]
        recon[f"first_file_lag_{lag if lag <= 7 else 'over_7'}"] += 1
        for scope in scopes:
            recon[f"{scope}_daily_rows"] += 1
            recon[f"{scope}_daily_rows_on_several_days"] += len(set(days[key])) > 1
            recon[f"{scope}_in_quarterly" if key in quarterly else f"{scope}_not_in_quarterly"] += 1
            recon[f"{scope}_in_archive" if key in archive_all else f"{scope}_not_in_archive"] += 1
            if key not in quarterly and key not in archive_all:
                recon[f"{scope}_in_neither"] += 1
                if scope in neither:
                    neither[scope][day[:4]] += 1
        if key not in quarterly and key not in archive_all:
            unexplained.append((*key, day))
            if len(examples["daily_in_neither"]) < 20:
                examples["daily_in_neither"].append(f"{key[0]} {key[1]} {day}")
    # The unexplained rows against the quarterly files unfiltered by form or CIK: is the accession listed at all?
    wanted = {accession for _, accession, _ in unexplained}
    elsewhere: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for path in sorted(index_cache.glob("master_*.gz")):
        text = gzip.decompress(path.read_bytes()).decode("latin-1")
        for cik, form, filed, accession in index_rows(text, Counter(), daily=False):
            if accession in wanted:
                elsewhere[accession].add((cik, form, filed))
    # Rows and distinct accessions apart: an accession can be unexplained under several CIKs.
    status: dict[str, set[str]] = defaultdict(set)
    for cik, accession, _ in unexplained:
        listed = elsewhere.get(accession, set())
        if not listed:
            where = "absent_from_quarterly"
        elif any(c == cik for c, _, _ in listed):
            where = "in_quarterly_same_cik_other_form"
        else:
            where = "in_quarterly_other_cik"
        recon[f"in_neither_rows_{where}"] += 1
        status[where].add(accession)
    recon["in_neither_accessions"] = len(wanted)
    for where, accessions in status.items():
        recon[f"in_neither_accessions_{where}"] = len(accessions)
    for key, filed in quarterly.items():
        if filed >= FIRST.isoformat() and key not in first_day:
            recon["quarterly_not_in_daily"] += 1
    for key in archive_rows - first_day.keys():
        recon["archive_not_in_daily"] += 1
    rows = json.loads(measurement.read_text())["headers"]["rows"]
    against: Counter[str] = Counter()
    for r in rows:
        keys = [(x[0], r["accession"]) for x in r["archive"] if (x[0], r["accession"]) in first_day]
        if not keys:
            against[f"{r['kind']}_not_in_daily"] += 1
            examples["not_in_daily"].append(r["accession"])
            continue
        day = min(first_day[k] for k in keys)
        for field, raw in (("acceptance", r["acceptance_et"]), ("filing_date", r["filing_date"])):
            value = _ymd(raw[:8]) if raw else None
            if value is None:
                against[f"{field}_unusable"] += 1
                continue
            offset = (date.fromisoformat(day) - date.fromisoformat(value)).days
            against[f"day_minus_{field}_{offset if -3 <= offset <= 5 else 'other'}"] += 1
            if offset < 0 or offset > 5:
                examples[f"day_minus_{field}_{offset}"].append(r["accession"])
        for name in sorted(set(r["classes"].values()) - {"ok"}):
            lag = lag_of[day]
            against[f"{name}_first_file_lag_{lag if lag <= 7 else 'over_7'}"] += 1
    lags = Counter(f["lag_days"] if f["lag_days"] <= 7 else "over_7" for f in files)
    return {
        "inputs": {
            "archive_sha256": sha256_file(archive_path),
            "measurement": str(measurement),
            "measurement_sha256": sha256_file(measurement),
            "quarterly_files": quarterly_files,
            "listings": listings,
        },
        "files": len(files),
        "unmatched_master_names": unmatched,
        "index_parse": {"daily": dict(sorted(parse_total.items())), "quarterly": dict(sorted(quarterly_parse.items()))},
        "write_lag_days": dict(sorted(lags.items(), key=str)),
        "files_written_over_7_days_late": [f for f in files if f["lag_days"] > 7],
        "rows": dict(shape),
        "reconciliation": dict(sorted(recon.items())),
        # Zero-filled from FIRST's year to the last file's year, so a year with none is shown.
        "daily_in_neither_by_year": {
            k: {str(y): v.get(str(y), 0) for y in range(FIRST.year, int(max(f["day"] for f in files)[:4]) + 1)}
            for k, v in neither.items()
        },
        "headers_against_daily": dict(sorted(against.items())),
        "examples": {k: v[:20] for k, v in sorted(examples.items())},
        "file_list": files,
    }


def _mtime(path: Path) -> str:
    """A cached file's write time (UTC), the time this run's copy was captured."""
    return datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(timespec="seconds")


def provenance() -> dict[str, Any]:
    """The executing code: this file's sha256, ``HEAD``, and whether the file differs from ``HEAD``."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    source = Path(__file__)
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, env=env)
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", str(source)], capture_output=True, text=True, check=True, env=env
    )
    return {
        "script_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "head": head.stdout.strip(),
        "script_clean_at_head": dirty.stdout.strip() == "",
    }


def consumer_audit() -> dict[str, Any]:
    """Every line of ``app/`` and ``scripts/`` at ``HEAD`` naming a target code or the item-severity table."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}  # a hook's GIT_DIR must not leak in
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, env=env
    ).stdout.strip()
    out: dict[str, Any] = {"revision": revision}
    for pattern in _AUDIT_PATTERNS:
        command = ["git", "grep", "-n", "-E", pattern, revision, "--", "app", "scripts"]
        found = subprocess.run(command, capture_output=True, text=True, check=False, env=env)
        if found.returncode not in (0, 1):  # 1 = no match
            raise RuntimeError(found.stderr)
        out[pattern] = {"command": " ".join(command), "hits": found.stdout.splitlines()}
    return out


def measure(
    archive_path: Path, with_headers: bool, negatives: int, previous: Path | None, cache: Path
) -> dict[str, Any]:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        ours = {str(value).zfill(10) for (value,) in conn.execute(_CIKS).fetchall()}
        events = conn.execute(_EVENTS, {"forms": sorted(FORMS)}).fetchall()
        typed = {accession: list(codes) for accession, codes in conn.execute(_TYPED).fetchall()}
        links = conn.execute(_AMENDMENT_LINKS).fetchone()
        concepts = conn.execute(_FACT_CONCEPTS).fetchone()
        severity = conn.execute(_SEVERITY, {"codes": list(LABEL_ITEMS)}).fetchall()
    quality: Counter[str] = Counter()
    arch, complete = read_archive(archive_path, quality)
    tokens = dict(_NON_VOCABULARY.most_common(40))  # before the previous archive adds its own
    pre_first = {"rows": len(_PRE_FIRST["rows"]), "target_rows": sorted(_PRE_FIRST["target_rows"])}
    events = [e for e in events if str(e[4]).zfill(10) in ours]
    previous_result = None
    if previous is not None:
        old, _ = read_archive(previous, Counter())
        previous_result = {"path": str(previous), "sha256": sha256_file(previous), **compare_snapshots(arch, old)}
    return {
        "archive": str(archive_path),
        "archive_sha256": sha256_file(archive_path),
        "first": FIRST.isoformat(),
        "forms": sorted(FORMS),
        "ciks_sha256": hashlib.sha256("\n".join(sorted(ours)).encode()).hexdigest(),
        "ciks": sorted(ours),
        "quality": dict(sorted(quality.items())),
        "pre_first": pre_first,
        "ciks_pages_incomplete": sorted(c for c, ok in complete.items() if not ok),
        "ours_absent_from_archive": sorted(ours - complete.keys()),
        "validity": {
            "non_vocabulary_tokens": tokens,
            "rows_by_state": dict(
                Counter(item_state(x[3]) + ("_ours" if x[0] in ours else "") for apps in arch.values() for x in apps)
            ),
        },
        "identity": identity(arch, ours),
        "by_year": by_year(arch, ours),
        "labels": labels(arch, ours),
        "filing_events": filing_events(arch, ours, events),
        "cross_source": cross_source(arch, typed),
        "amendment_links": {"manifest_8ka_rows": links[0], "with_amends_accession": links[1]} if links else None,
        "financial_facts_raw_concepts": (
            {"query": " ".join(_FACT_CONCEPTS.split()), "distinct": concepts[0], "matching": concepts[1]}
            if concepts
            else None
        ),
        "headers": headers(arch, ours, negatives, cache) if with_headers else None,
        "consumer_audit": {
            **consumer_audit(),
            "severity_query": " ".join(_SEVERITY.split()),
            "severity": [list(r) for r in severity],
        },
        "previous_snapshot": previous_result,
        "measured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--previous", type=Path, help="an earlier submissions.zip snapshot to compare against")
    parser.add_argument("--headers", action="store_true", help="fetch EDGAR headers for our candidate accessions")
    parser.add_argument("--negative-sample", type=int, default=0, help="non-candidate accessions also fetched")
    parser.add_argument("--header-cache", type=Path, default=Path("var/research/3624/headers"))
    parser.add_argument("--index-cache", type=Path, help="reconcile against EDGAR full-index instead (cache dir)")
    parser.add_argument("--daily-index-cache", type=Path, help="measure EDGAR's daily indexes instead (cache dir)")
    parser.add_argument("--measurement", type=Path, help="the first command's output, for --daily-index-cache")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.daily_index_cache:
        if not (args.index_cache and args.measurement):
            parser.error("--daily-index-cache needs --index-cache and --measurement")
        result = daily_index(args.archive, args.daily_index_cache, args.index_cache, args.measurement)
    elif args.index_cache:
        with psycopg.connect(settings.database_url) as conn:
            ours = {str(value).zfill(10) for (value,) in conn.execute(_CIKS).fetchall()}
        result = index_reconciliation(args.archive, args.index_cache, ours)
    else:
        result = measure(args.archive, args.headers, args.negative_sample, args.previous, args.header_cache)
    result["provenance"] = provenance()
    text = json.dumps(result, indent=1, default=str)
    if args.out:
        args.out.write_text(text + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("ciks", "headers", "identity")}, default=str)[:4000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
