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
  and compared with the archive (body sha256 kept);
* ``previous_snapshot``: with ``--previous``, membership and per-field changes against an earlier archive.

The CIK list itself is written out, so a re-run can reproduce "our CIKs". Usage::

    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive <zip> [--previous <zip>] \\
        [--headers] [--negative-sample N] [--out results.json]

With ``--index-cache DIR`` it instead reconciles the archive against every 8-K-family row of EDGAR's quarterly
``full-index/master.gz`` files from :data:`FIRST` (:func:`index_reconciliation`).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import re
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
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


def parse_items(raw: Any) -> Items:
    """Sorted vocabulary codes, or ``"missing"`` / ``"empty"`` / ``"invalid:<raw>"`` (raw kept, so two invalid
    lists stay distinguishable). Codes are matched exactly (ASCII); nothing is normalised."""
    if raw is None:
        return "missing"
    if not isinstance(raw, str):
        return f"invalid:{raw!r}"
    if raw == "":
        return "empty"
    codes = raw.split(",")
    unknown = [code for code in codes if code not in VOCABULARY]
    if unknown:
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
            for entry in document["filings"].get("files", []):
                listed.add(entry["name"])
                quality["history_pages_listed"] += 1
                if entry["name"] not in names:
                    quality["history_pages_missing"] += 1
                    complete = False
                    continue
                blocks.append((entry["name"], json.loads(archive.read(entry["name"]))))
            for page, block in blocks:
                if len({len(value) for value in block.values() if isinstance(value, list)}) > 1:
                    quality["misaligned_pages"] += 1
                    complete = False
                    continue
                items = block.get("items")
                if items is None:
                    quality["blocks_without_items"] += 1
                for i, form in enumerate(block.get("form", [])):
                    if form not in FORMS:
                        continue
                    filed = block["filingDate"][i]
                    if date.fromisoformat(filed) < FIRST:
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
            out[f"{scope}_row_repeats"] += len(scoped) - len(rows)
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
        if o:
            years[year]["event_rows"] += o["rows"]
            years[year]["event_accessions"] += 1
        if filed > last:
            years[year]["events_after_archive"] += 1
            continue
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


#: The SGML header file; ``-index-headers.html`` is absent for older filings (404 on 2006 accessions).
_HEADER_URL: Final = "https://www.sec.gov/Archives/edgar/data/{cik}/{bare}/{accession}.hdr.sgml"
_SGML: Final = re.compile(r"<SEC-HEADER>(.*?)</SEC-HEADER>", re.S)
_CIK: Final = re.compile(r"^<CIK>(.*)$", re.M)


def parse_header(text: str) -> dict[str, Any]:
    """The SGML header's acceptance (Eastern wall clock), type, filing date, filing-date change, items, filer CIKs."""
    found = _SGML.search(text)
    body = found.group(1) if found else text

    def one(tag: str) -> str | None:
        match = re.search(rf"^<{tag}>(.*)$", body, re.M)
        return match.group(1).strip() if match else None

    return {
        "acceptance_et": one("ACCEPTANCE-DATETIME"),
        "type": one("TYPE"),
        "filing_date": one("FILING-DATE"),
        "filing_date_change": one("DATE-OF-FILING-DATE-CHANGE"),
        "items": sorted(m.strip() for m in re.findall(r"^<ITEMS>(.*)$", body, re.M)),
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
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}" if value and len(value) >= 8 else None


def compare_header(apps: list[Appearance], header: dict[str, Any]) -> list[str]:
    """Where the archive and the header disagree, and header states the producer treats as uncertain, as tags."""
    tags: list[str] = []
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
    if not header["acceptance_et"]:
        tags.append("no_acceptance")
    accepted = _ymd(header["acceptance_et"])
    filed = _ymd(header["filing_date"])
    if accepted and filed:
        if filed < accepted:
            tags.append("filing_date_before_acceptance")
        elif filed > accepted:
            tags.append("filing_date_after_acceptance")
    change = _ymd(header["filing_date_change"])
    if change and accepted and change != accepted:
        tags.append("post_acceptance_change")  # PDS: "Date when the last Post Acceptance occurred"
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
    rows: list[dict[str, Any]] = []
    failures: list[list[str]] = []
    for kind, accession in work:
        if accession in errors:
            failures.append([kind, accession, errors[accession]])
            continue
        header = read_header(cache, accession)
        found = compare_header(arch[accession], header)
        tags[kind].update(found or ["agree"])
        accepted, filed = _ymd(header["acceptance_et"]), _ymd(header["filing_date"])
        if accepted and filed:
            lag[str((date.fromisoformat(filed) - date.fromisoformat(accepted)).days)] += 1
        archive_rows = [[x[0], x[1], x[2], list(x[3]) if valid(x[3]) else x[3]] for x in arch[accession]]
        rows.append({"kind": kind, "accession": accession, "tags": found, "archive": archive_rows, **header})
    later = sorted(r["acceptance_et"][8:] for r in rows if "filing_date_after_acceptance" in r["tags"])
    same_late = sum(
        1
        for r in rows
        if r["acceptance_et"]
        and not r["tags"].count("filing_date_after_acceptance")
        and _ymd(r["acceptance_et"]) == _ymd(r["filing_date"])
        and r["acceptance_et"][8:] > "173000"
    )
    lengths = Counter(
        f"{len(next(x for x in r['archive'] if isinstance(x[3], list))[3])}_{'items' in r['tags']}"
        for r in rows
        if any(isinstance(x[3], list) for x in r["archive"])
    )
    out.update(
        {
            "later_dated_acceptance_range": [later[0], later[-1]] if later else None,
            "same_day_dated_accepted_after_1730": same_late,
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
    files: dict[str, dict[str, str]] = {}
    index_rows: dict[tuple[str, str], tuple[str, str]] = {}
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
            for line in text.splitlines():
                parts = line.split("|")
                if len(parts) != 5 or parts[2] not in FORMS or parts[3] < FIRST.isoformat():
                    continue
                accession = parts[4].rsplit("/", 1)[-1].removesuffix(".txt")
                index_rows[(parts[0].zfill(10), accession)] = (parts[2], parts[3])
    by_quarter: dict[str, Counter[str]] = defaultdict(Counter)
    stale: list[tuple[str, str, str, str]] = []
    for key, (form, filed) in index_rows.items():
        bucket = by_quarter[f"{filed[:4]}Q{(int(filed[5:7]) - 1) // 3 + 1}"]
        bucket["index_rows"] += 1
        if key in archive_rows:
            continue
        where = "after_archive" if filed > last else "cik_file_present" if key[0] in cik_files else "cik_file_absent"
        bucket[f"index_only_{where}"] += 1
        if filed <= last:
            stale.append((*key, form, filed))
    for cik, accession in archive_rows - index_rows.keys():
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
        "by_quarter": {q: dict(sorted(c.items())) for q, c in sorted(by_quarter.items())},
        "index_only_by_archive_last_date": reconciled,
    }


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
    quality: Counter[str] = Counter()
    arch, complete = read_archive(archive_path, quality)
    tokens = dict(_NON_VOCABULARY.most_common(40))  # before the previous archive adds its own
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
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.index_cache:
        with psycopg.connect(settings.database_url) as conn:
            ours = {str(value).zfill(10) for (value,) in conn.execute(_CIKS).fetchall()}
        result = index_reconciliation(args.archive, args.index_cache, ours)
    else:
        result = measure(args.archive, args.headers, args.negative_sample, args.previous, args.header_cache)
    text = json.dumps(result, indent=1, default=str)
    if args.out:
        args.out.write_text(text + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("ciks", "headers", "identity")}, default=str)[:4000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
