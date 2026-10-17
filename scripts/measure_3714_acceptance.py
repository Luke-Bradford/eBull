"""#3714: measure how far each SEC acceptance-time surface sits from the EDGAR header.

Read-only. Reference = ``<ACCEPTANCE-DATETIME>`` in ``<accession>.hdr.sgml``: 14 digits of EDGAR acceptance time,
"Time (EST) at which the submission was accepted by EDGAR" (PDS Technical Specification, control block), i.e.
Eastern local time. Offsets are reported in SECONDS as ``surface - header``; negative = the surface claims an
EARLIER instant (look-ahead).

Every header fetch is classified, never collapsed to "absent": ``found`` (one valid tag in one header block for
exactly that accession), ``absent`` (a valid block without the tag), ``invalid_value`` (the tag's value is not 14
digits, not a calendar time, or a DST gap/fold wall time), ``bad_response`` (a 200 that is not exactly one header
block for that accession, or carries a duplicated tag), ``http_<status>`` or ``transport_error``.

Subcommands (accession-level rows go to ``--out`` as JSON lines; a counter goes to stdout):

``manifest``  seeded random sample of ``sec_filing_manifest`` rows with ``accepted_at`` set (``--since`` limits
              it to rows created after a timestamp), vs header.
``captures``  for the accessions in a ``manifest`` output: each ``submissions.zip`` capture's
              ``acceptanceDateTime`` vs header (``recent`` plus every ``files[]`` page).
``cik``       the live ``data.sec.gov`` submissions JSON for one CIK, ``--n`` accessions spread over its history.
``atom``      the live getcurrent Atom feed's ``<updated>`` (explicit offset), vs header.
``hours``     full population: stored ``accepted_at`` outside EDGAR's 06:00-22:00 ET window, per source.

    PYTHONPATH=. uv run python -m scripts.measure_3714_acceptance manifest --n 300 --out /tmp/m.jsonl
    PYTHONPATH=. uv run python -m scripts.measure_3714_acceptance captures /tmp/m.jsonl A.zip B.zip C.zip
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import re
import time
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg

from app.config import Settings
from app.providers.implementations.sec_header import classify_header, header_url

#: This script's own pacing between SEC requests. It bounds only this loop; it does not coordinate with the jobs
#: process, so run it off the manifest worker's busy windows.
_PAUSE_S: Final = 0.15


def header_acceptance(client: httpx.Client, cik: int, accession: str) -> tuple[str, datetime | None]:
    """Classify one header fetch with the job's own rule (``sec_header.classify_header``); a non-200 reads
    ``http_<status>`` here for the sample counters."""
    url = header_url(str(cik), accession)
    try:
        response = client.get(url)
    except httpx.HTTPError:
        return "transport_error", None
    finally:
        time.sleep(_PAUSE_S)
    check = classify_header(accession, url, response.status_code, response.text)
    if check.outcome == "http_error":
        return f"http_{response.status_code}", None
    return check.outcome, check.acceptance_utc


def _seconds(a: datetime, b: datetime) -> int:
    return int((a - b).total_seconds())


def _client(settings: Settings) -> httpx.Client:
    return httpx.Client(headers={"User-Agent": settings.sec_user_agent}, timeout=30)


def _write(out: Path | None, rows: list[dict[str, Any]]) -> None:
    if out is not None:
        out.write_text("".join(json.dumps(row, default=str) + "\n" for row in rows))


def manifest(settings: Settings, n: int, seed: float, since: str | None, out: Path | None) -> None:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            """
            SELECT accession_number, cik, source, accepted_at, created_at FROM sec_filing_manifest
            WHERE accepted_at IS NOT NULL AND left(source, 4) = 'sec_'
              AND (%(since)s::timestamptz IS NULL OR created_at > %(since)s::timestamptz)
            ORDER BY accession_number
            """,
            {"since": since},
        ).fetchall()
    # Sampling in Python over an accession-ordered list: reproducible for a fixed population and seed.
    sample = random.Random(seed).sample(rows, min(n, len(rows)))
    counts: collections.Counter[tuple[str, str, int | None]] = collections.Counter()
    output: list[dict[str, Any]] = []
    with _client(settings) as client:
        for accession, cik, source, accepted, created in sample:
            status, header = header_acceptance(client, int(cik), accession)
            delta = None if header is None else _seconds(accepted, header)
            counts[(source, status, delta)] += 1
            output.append(
                {
                    "accession": accession,
                    "cik": cik,
                    "source": source,
                    "accepted_at": accepted,
                    "created_at": created,
                    "header_status": status,
                    "header": header,
                    "delta_s": delta,
                }
            )
    _write(out, output)
    print(f"population={len(rows)} sampled={len(sample)}")
    for key, value in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(key, value)


def _blocks(archive: zipfile.ZipFile, cik: str) -> Iterator[dict[str, Any]]:
    try:
        main = json.loads(archive.read(f"CIK{cik}.json"))
    except KeyError:
        return
    yield main["filings"]["recent"]
    for page in main["filings"].get("files", []):
        try:
            yield json.loads(archive.read(page["name"]))
        except KeyError:
            continue


def _capture_value(archive: zipfile.ZipFile, cik: str, accession: str) -> str | None:
    for block in _blocks(archive, cik):
        accessions = block["accessionNumber"]
        values = block["acceptanceDateTime"]
        if len(accessions) != len(values):
            raise ValueError(f"CIK{cik}: unequal accessionNumber/acceptanceDateTime arrays")
        if accession in accessions:
            return values[accessions.index(accession)]
    return None


def captures(sample: Path, archives: list[Path]) -> None:
    rows = [json.loads(line) for line in sample.read_text().splitlines()]
    counts: collections.Counter[tuple[Any, ...]] = collections.Counter()
    zips = [zipfile.ZipFile(path) for path in archives]
    for row in rows:
        if row["header"] is None:
            counts[("no_header", row["header_status"])] += 1
            continue
        header = datetime.fromisoformat(row["header"])
        cells: list[str | int] = []
        for archive in zips:
            value = _capture_value(archive, row["cik"], row["accession"])
            cells.append(
                "absent" if not value else _seconds(datetime.fromisoformat(value.replace("Z", "+00:00")), header)
            )
        counts[(row["delta_s"], *cells)] += 1
    print("key = (manifest - header, *[capture - header]) in seconds; captures in argument order")
    for key, value in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(key, value)


def cik(settings: Settings, cik_number: int, n: int, out: Path | None) -> None:
    padded = f"{cik_number:010d}"
    with _client(settings) as client:
        recent = client.get(f"https://data.sec.gov/submissions/CIK{padded}.json").json()["filings"]["recent"]
        if len(recent["accessionNumber"]) != len(recent["acceptanceDateTime"]):
            raise ValueError(f"CIK{padded}: unequal accessionNumber/acceptanceDateTime arrays")
        total = len(recent["accessionNumber"])
        picks = range(0, total, max(1, total // n))
        counts: collections.Counter[tuple[str, int | None]] = collections.Counter()
        output: list[dict[str, Any]] = []
        for i in list(picks)[:n]:
            accession, value = recent["accessionNumber"][i], recent["acceptanceDateTime"][i]
            status, header = header_acceptance(client, cik_number, accession)
            json_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
            delta = None if header is None else _seconds(json_at, header)
            counts[(status, delta)] += 1
            output.append(
                {
                    "accession": accession,
                    "form": recent["form"][i],
                    "json": value,
                    "header": header,
                    "header_status": status,
                    "delta_s": delta,
                }
            )
    _write(out, output)
    print(f"CIK{padded} accessions_in_recent={total} sampled={len(output)}")
    for key, value in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(key, value)


def atom(settings: Settings, n: int) -> None:
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&owner=include&count=100&output=atom"
    counts: collections.Counter[tuple[str, int | None]] = collections.Counter()
    seen: set[str] = set()
    with _client(settings) as client:
        for entry in re.findall(r"<entry>.*?</entry>", client.get(url).text, re.S):
            updated = re.search(r"<updated>(.*?)</updated>", entry)
            link = re.search(r"/Archives/edgar/data/(\d+)/(\d{18})/", entry)
            if updated is None or link is None or link.group(2) in seen:
                continue
            seen.add(link.group(2))
            bare = link.group(2)
            status, header = header_acceptance(client, int(link.group(1)), f"{bare[:10]}-{bare[10:12]}-{bare[12:]}")
            stamp = datetime.fromisoformat(updated.group(1))
            counts[(status, None if header is None or stamp.tzinfo is None else _seconds(stamp, header))] += 1
            if len(seen) >= n:
                break
    print(f"checked={len(seen)} at={datetime.now(tz=UTC).isoformat()}")
    for key, value in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(key, value)


def hours(settings: Settings) -> None:
    with psycopg.connect(settings.database_url) as conn:
        for row in conn.execute(
            """
            SELECT source, count(*),
                   count(*) FILTER (WHERE (accepted_at AT TIME ZONE 'America/New_York')::time < '06:00:00'),
                   count(*) FILTER (WHERE (accepted_at AT TIME ZONE 'America/New_York')::time > '22:00:00')
            FROM sec_filing_manifest WHERE accepted_at IS NOT NULL GROUP BY 1 ORDER BY 2 DESC
            """
        ):
            print(f"source={row[0]} rows={row[1]} before_0600_et={row[2]} after_2200_et={row[3]}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    m = sub.add_parser("manifest")
    m.add_argument("--n", type=int, default=300)
    m.add_argument("--seed", type=float, default=0.3714)
    m.add_argument("--since", help="only rows created after this ISO timestamp")
    m.add_argument("--out", type=Path)
    c = sub.add_parser("captures")
    c.add_argument("sample", type=Path)
    c.add_argument("archives", type=Path, nargs="+")
    k = sub.add_parser("cik")
    k.add_argument("cik", type=int)
    k.add_argument("--n", type=int, default=40)
    k.add_argument("--out", type=Path)
    a = sub.add_parser("atom")
    a.add_argument("--n", type=int, default=100)
    sub.add_parser("hours")
    args = parser.parse_args()
    settings = Settings()
    if args.command == "manifest":
        manifest(settings, args.n, args.seed, args.since, args.out)
    elif args.command == "captures":
        captures(args.sample, args.archives)
    elif args.command == "cik":
        cik(settings, args.cik, args.n, args.out)
    elif args.command == "atom":
        atom(settings, args.n)
    else:
        hours(settings)


if __name__ == "__main__":
    main()
