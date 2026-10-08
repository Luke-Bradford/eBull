"""#3624 slice 2: measure 8-K item metadata in SEC's bulk submissions archive against what we store.

Read-only. Streams one ``submissions.zip`` (every ``CIK##########.json`` and its ``-submissions-NNN.json`` history
pages) for the CIKs in ``external_identifiers`` and keeps forms ``8-K`` and ``8-K/A`` filed on or after
:data:`FIRST`. It reports:

* ``quality``: rows per form, items that are missing, empty or invalid, accessions seen twice (same CIK or another,
  identical or conflicting), history pages the zip lacks and misaligned pages;
* ``by_year``: accessions per form in the archive, and against ``filing_events``: in both, archive only (split by
  page and by whether the CIK has any matched event), ``filing_events`` only (listed);
* ``filing_events_nulls`` and ``item_agreement``: NULL ``items`` as raw rows and as accessions, and item-set
  agreement on shared accessions (``filing_events`` items unioned per accession);
* ``labels``: Item 4.01 / 4.02 accessions per form;
* ``cross_source``: Items 4.01 / 4.02 from the typed body parser (``eight_k_items``) against the archive, on
  accessions both hold, with per-label denominators;
* ``amendment_links``: ``sec_filing_manifest`` 8-K/A rows with ``amends_accession`` set, and archive ``recent``
  keys naming an amendment;
* ``acceptance``: the JSON ``acceptanceDateTime`` hour histogram and its offset from ``sec_filing_manifest``;
  with ``--header-sample N``, a seeded random sample checked against each filing's EDGAR header
  ``<ACCEPTANCE-DATETIME>`` (Eastern wall clock); with ``--previous``, the change between two snapshots.

The CIK list's sha256 is printed, so a re-run can tell whether "our CIKs" moved. Usage::

    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive <zip> [--previous <zip>] \\
        [--header-sample N] [--out results.json]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import time
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import httpx
import psycopg

from app.config import settings

ARCHIVE: Final = Path.home() / "Library/Application Support/eBull/sec/bulk/submissions.zip"
FORMS: Final = ("8-K", "8-K/A")
FIRST: Final = date(2016, 6, 3)  # filing_events' earliest 8-K, measured 2026-10-08
LABEL_ITEMS: Final = ("4.01", "4.02")

_CIKS: Final = """
    SELECT DISTINCT ei.identifier_value FROM external_identifiers ei
    WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik'
"""
_EVENTS: Final = """
    SELECT provider_filing_id, filing_type, filing_date, items FROM filing_events
    WHERE provider = 'sec' AND filing_type = ANY(%(forms)s)
"""
_EVENT_NULLS: Final = """
    SELECT count(*) FILTER (WHERE items IS NULL), count(*) FROM filing_events
    WHERE provider = 'sec' AND filing_type = ANY(%(forms)s)
"""
#: Item codes the typed body parser stored per non-tombstone accession; no item rows gives an empty array.
_TYPED: Final = """
    SELECT f.accession_number, coalesce(array_agg(i.item_code) FILTER (WHERE i.item_code IS NOT NULL), '{}')
    FROM eight_k_filings f LEFT JOIN eight_k_items i ON i.accession_number = f.accession_number
    WHERE NOT f.is_tombstone
    GROUP BY 1
"""
_AMENDMENT_LINKS: Final = "SELECT count(*), count(amends_accession) FROM sec_filing_manifest WHERE form = '8-K/A'"
_MANIFEST: Final = """
    SELECT accession_number, accepted_at FROM sec_filing_manifest
    WHERE form = ANY(%(forms)s) AND accepted_at IS NOT NULL
"""
_ITEM_LIST: Final = re.compile(r"\d{1,2}\.\d{2}(,\d{1,2}\.\d{2})*")
_FILING_FIELDS: Final = ("form", "filing_date", "acceptance", "items")


def parse_items(raw: Any) -> tuple[str, ...] | str:
    """The archive's ``items`` as a sorted tuple of codes, or ``"missing"``, ``"empty"`` or ``"invalid"``."""
    if raw is None:
        return "missing"
    if not isinstance(raw, str):
        return "invalid"
    if raw == "":
        return "empty"
    if not _ITEM_LIST.fullmatch(raw):
        return "invalid"
    return tuple(sorted(raw.split(",")))


def known(items: tuple[str, ...] | str) -> bool:
    return isinstance(items, tuple)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 24), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_archive(path: Path, ciks: set[str], quality: Counter[str]) -> dict[str, dict[str, Any]]:
    """Accession -> the first archive row seen, for our CIKs' 8-K and 8-K/A filings on or after :data:`FIRST`.

    ``quality`` counts what a producer must refuse or keep apart (module docstring)."""
    rows: dict[str, dict[str, Any]] = {}
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        for name in sorted(names):
            if not name.startswith("CIK") or name[3:13] not in ciks:
                continue
            document = json.loads(archive.read(name))
            primary = "filings" in document
            block = document["filings"]["recent"] if primary else document
            if primary:
                quality["ciks_read"] += 1
                pages = [entry["name"] for entry in document["filings"].get("files", [])]
                quality["history_pages_listed"] += len(pages)
                quality["history_pages_missing"] += sum(1 for page in pages if page not in names)
                quality["recent_keys_naming_amend"] += sum(1 for key in block if "amend" in key.lower())
            if len({len(value) for value in block.values() if isinstance(value, list)}) != 1:
                quality["misaligned_pages"] += 1
                continue
            for i, form in enumerate(block["form"]):
                if form not in FORMS or date.fromisoformat(block["filingDate"][i]) < FIRST:
                    continue
                row = {
                    "cik": name[3:13],
                    "form": form,
                    "filing_date": block["filingDate"][i],
                    "acceptance": block["acceptanceDateTime"][i] or None,
                    "items": parse_items(block["items"][i]),
                    "page": "recent" if primary else "history",
                }
                quality[f"rows_{form}"] += 1
                if not known(row["items"]):
                    quality[f"items_{row['items']}"] += 1
                seen = rows.get(block["accessionNumber"][i])
                if seen is None:
                    rows[block["accessionNumber"][i]] = row
                    continue
                identical = all(seen[key] == row[key] for key in _FILING_FIELDS)
                where = "same_cik" if seen["cik"] == row["cik"] else "other_cik"
                quality[f"duplicate_{where}_{'identical' if identical else 'conflict'}"] += 1
    return rows


_HEADER_URL: Final = "https://www.sec.gov/Archives/edgar/data/{cik}/{bare}/{accession}-index-headers.html"
_HEADER_ACCEPTANCE: Final = re.compile(r"ACCEPTANCE-DATETIME(?:>|&gt;)(\d{14})")
_NEW_YORK: Final = ZoneInfo("America/New_York")


def header_acceptance(client: httpx.Client, cik: str, accession: str) -> datetime | None:
    """The EDGAR header's acceptance time (Eastern wall clock) as UTC; ``None`` if the header has none."""
    url = _HEADER_URL.format(cik=int(cik), bare=accession.replace("-", ""), accession=accession)
    response = client.get(url)
    response.raise_for_status()
    found = _HEADER_ACCEPTANCE.search(response.text)
    if found is None:
        return None
    return datetime.strptime(found.group(1), "%Y%m%d%H%M%S").replace(tzinfo=_NEW_YORK).astimezone(UTC)


def _stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _hours(delta: Any) -> str:
    return f"{delta.total_seconds() / 3600:+g}h"


def header_sample(
    archive: dict[str, dict[str, Any]], manifest: dict[str, datetime], n: int, seed: int = 3624
) -> dict[str, Any]:
    """Offsets (source minus header) for a seeded random sample of archive accessions, with every row kept."""
    accessions = random.Random(seed).sample(sorted(archive), n)
    json_offsets: Counter[str] = Counter()
    manifest_offsets: Counter[str] = Counter()
    rows: list[list[str | None]] = []
    with httpx.Client(timeout=30, headers={"User-Agent": settings.sec_user_agent}) as client:
        for accession in accessions:
            row = archive[accession]
            truth = header_acceptance(client, row["cik"], accession)
            time.sleep(0.15)  # well under SEC's 10 req/s
            rows.append([accession, row["acceptance"], truth.isoformat() if truth else None])
            if truth is None or row["acceptance"] is None:
                json_offsets["not_comparable"] += 1
                continue
            json_offsets[_hours(_stamp(row["acceptance"]) - truth)] += 1
            if accession in manifest:
                manifest_offsets[_hours(manifest[accession] - truth)] += 1
    return {
        "n": n,
        "seed": seed,
        "json_minus_header": dict(json_offsets.most_common()),
        "manifest_minus_header": dict(manifest_offsets.most_common()),
        "rows": rows,
    }


def compare_snapshots(new: dict[str, dict[str, Any]], old: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Accessions both snapshots hold, filed by the older one's last date: which fields changed."""
    last_old = max(row["filing_date"] for row in old.values())
    shared = old.keys() & new.keys()
    changed = {key: sum(1 for a in shared if old[a][key] != new[a][key]) for key in ("cik", *_FILING_FIELDS)}
    return {
        "old_last_filing_date": last_old,
        "old_only": sum(1 for a in old if a not in new),
        "new_only_by_old_last_date": sum(1 for a, r in new.items() if a not in old and r["filing_date"] <= last_old),
        "shared": len(shared),
        "changed": changed,
        "acceptance_new_minus_old": dict(
            Counter(
                _hours(_stamp(new[a]["acceptance"]) - _stamp(old[a]["acceptance"]))
                for a in shared
                if old[a]["acceptance"] != new[a]["acceptance"] and old[a]["acceptance"] and new[a]["acceptance"]
            ).most_common(6)
        ),
    }


def cross_source(archive: dict[str, dict[str, Any]], typed: dict[str, list[str]]) -> dict[str, Any]:
    """Items 4.01 / 4.02, typed body parser against the archive, on accessions both hold with known archive items."""
    shared = [a for a in typed if a in archive and known(archive[a]["items"])]
    out: dict[str, Any] = {
        "shared": len(shared),
        "typed_with_no_item_rows": sum(1 for a in shared if not typed[a]),
    }
    for code in LABEL_ITEMS:
        in_archive = {a for a in shared if code in archive[a]["items"]}
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


def measure(archive_path: Path, header_n: int = 0, previous: Path | None = None) -> dict[str, Any]:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        ciks = {str(value).zfill(10) for (value,) in conn.execute(_CIKS).fetchall()}
        events = conn.execute(_EVENTS, {"forms": list(FORMS)}).fetchall()
        null_rows, all_rows = conn.execute(_EVENT_NULLS, {"forms": list(FORMS)}).fetchone() or (0, 0)
        typed = {accession: list(codes) for accession, codes in conn.execute(_TYPED).fetchall()}
        links = conn.execute(_AMENDMENT_LINKS).fetchone()
        manifest = dict(conn.execute(_MANIFEST, {"forms": list(FORMS)}).fetchall())
    ours: dict[str, dict[str, Any]] = {}
    for accession, form, filed, items in events:
        entry = ours.setdefault(accession, {"form": form, "filing_date": filed.isoformat(), "items": set(), "nulls": 0})
        if items is None:
            entry["nulls"] += 1
        else:
            entry["items"].update(items)
    quality: Counter[str] = Counter()
    archive = read_archive(archive_path, ciks, quality)
    event_ciks = {row["cik"] for accession, row in archive.items() if accession in ours}
    last = max(row["filing_date"] for row in archive.values())

    by_year: dict[str, Counter[str]] = defaultdict(Counter)
    agreement: Counter[str] = Counter()
    events_only: list[str] = []
    for accession in archive.keys() | ours.keys():
        a, o = archive.get(accession), ours.get(accession)
        row = a or o or {}
        year = row["filing_date"][:4]
        if a:
            by_year[year][f"archive_{a['form']}"] += 1
        if row["filing_date"] > last:
            by_year[year]["events_after_archive"] += 1
            continue
        if a and o:
            by_year[year]["both"] += 1
        elif a:
            by_year[year]["archive_only"] += 1
            by_year[year][f"archive_only_{a['page']}"] += 1
            by_year[year][
                "archive_only_cik_has_events" if a["cik"] in event_ciks else "archive_only_cik_no_events"
            ] += 1
        else:
            by_year[year]["events_only"] += 1
            events_only.append(accession)
        if a and o:
            if o["nulls"]:
                agreement[f"events_null_{'all' if not o['items'] else 'mixed'}"] += 1
            elif not known(a["items"]):
                agreement[f"archive_{a['items']}"] += 1
            else:
                agreement["equal" if set(a["items"]) == o["items"] else "differ"] += 1

    labels = {
        code: dict(Counter(row["form"] for row in archive.values() if known(row["items"]) and code in row["items"]))
        for code in LABEL_ITEMS
    }
    hours: Counter[int] = Counter()
    offsets: Counter[str] = Counter()
    for accession, row in archive.items():
        if row["acceptance"] is None:
            quality["acceptance_missing"] += 1
            continue
        hours[_stamp(row["acceptance"]).hour] += 1
        if accession in manifest:
            offsets[_hours(_stamp(row["acceptance"]) - manifest[accession])] += 1
    previous_rows = None if previous is None else read_archive(previous, ciks, Counter())
    return {
        "archive": str(archive_path),
        "archive_sha256": sha256_file(archive_path),
        "archive_last_filing_date": last,
        "ciks": len(ciks),
        "ciks_sha256": hashlib.sha256("\n".join(sorted(ciks)).encode()).hexdigest(),
        "window_first": FIRST.isoformat(),
        "quality": dict(sorted(quality.items())),
        "by_year": {year: dict(sorted(counts.items())) for year, counts in sorted(by_year.items())},
        "events_only": sorted(events_only),
        "filing_events_nulls": {"null_rows": null_rows, "rows": all_rows},
        "item_agreement": dict(agreement),
        "labels": labels,
        "cross_source": cross_source(archive, typed),
        "amendment_links": {"manifest_8ka_rows": links[0], "with_amends_accession": links[1]} if links else None,
        "acceptance": {
            "hour_of_json_value": {str(h): hours[h] for h in sorted(hours)},
            "json_minus_manifest": dict(offsets.most_common(8)),
            "manifest_overlap": sum(offsets.values()),
            "header_sample": header_sample(archive, manifest, header_n) if header_n else None,
        },
        "previous_snapshot": (
            None
            if previous is None or previous_rows is None
            else {"path": str(previous), "sha256": sha256_file(previous), **compare_snapshots(archive, previous_rows)}
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--previous", type=Path, help="an earlier submissions.zip snapshot to compare against")
    parser.add_argument("--header-sample", type=int, default=0, help="random accessions checked against EDGAR")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    result = measure(args.archive, args.header_sample, args.previous)
    text = json.dumps(result, indent=1, default=str)
    if args.out:
        args.out.write_text(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
