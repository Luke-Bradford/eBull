"""Build the outcome-blind SEC cover-identity census for R6 #2900.

This script reads pinned SEC Financial Statement Data Set ZIPs, selects the
latest annual filing public by each R6 formation close, and optionally caches
the accession-addressed extracted XBRL instances. It never reads prices or
returns.

Examples::

    PYTHONPATH=. uv run python -m scripts.census_2900_sec_cover_identity \
      --fsds-dir /tmp/r6-fsds --manifest-only

    PYTHONPATH=. uv run python -m scripts.census_2900_sec_cover_identity \
      --fsds-dir /tmp/r6-fsds --instance-cache /path/to/cache
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import hashlib
import io
import json
import os
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final
from uuid import uuid4

import httpx
import lxml.etree as ET

from app.config import settings
from app.services.sec_pipelined_fetcher import FetchTask, PipelinedSecFetcher
from app.services.xbrl_instance import SAFE_XML_PARSER, context_dimensions, context_period

ANNUAL_FORMS: Final = frozenset({"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"})
FORMATION_CLOSES: Final = (
    datetime(2022, 6, 30, 16, 0, 0),
    datetime(2023, 6, 30, 16, 0, 0),
    datetime(2024, 6, 28, 16, 0, 0),
)
PARSER_VERSION: Final = "r6-sec-cover-identity-census-v2"
_SECURITY_FACTS: Final = frozenset({"Security12bTitle", "TradingSymbol", "SecurityExchangeName"})
_COVER_FACTS: Final = _SECURITY_FACTS | {"DocumentPeriodEndDate"}


@dataclass(frozen=True)
class Submission:
    accession: str
    cik: str
    form: str
    accepted: datetime
    period: str
    instance: str

    @property
    def url(self) -> str:
        accession_path = self.accession.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{int(self.cik)}/{accession_path}/{self.instance}"


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _archive_member(zf: zipfile.ZipFile, basename: str) -> str:
    matches = [name for name in zf.namelist() if name == basename or name.endswith("/" + basename)]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one {basename!r} in archive, found {matches!r}")
    return matches[0]


def load_submissions(fsds_dir: Path) -> tuple[dict[str, Submission], dict[str, str]]:
    archives = sorted(fsds_dir.glob("20??q?.zip"))
    if not archives:
        raise RuntimeError(f"no quarterly FSDS ZIPs found in {fsds_dir}")
    submissions: dict[str, Submission] = {}
    digests: dict[str, str] = {}
    for archive in archives:
        digests[archive.name] = _sha256(archive)
        with zipfile.ZipFile(archive) as zf:
            name = _archive_member(zf, "sub.txt")
            with zf.open(name) as raw:
                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"), delimiter="\t")
                for row in reader:
                    if row.get("form") not in ANNUAL_FORMS:
                        continue
                    accession = row.get("adsh", "").strip()
                    cik = row.get("cik", "").strip()
                    instance = row.get("instance", "").strip()
                    accepted_raw = row.get("accepted", "").strip()
                    if not accession or not cik.isdigit() or not instance or not accepted_raw:
                        continue
                    if Path(instance).name != instance:
                        raise RuntimeError(f"unsafe FSDS instance filename for {accession}: {instance!r}")
                    candidate = Submission(
                        accession=accession,
                        cik=cik.zfill(10),
                        form=str(row["form"]),
                        accepted=datetime.fromisoformat(accepted_raw),
                        period=row.get("period", "").strip(),
                        instance=instance,
                    )
                    incumbent = submissions.get(accession)
                    if incumbent is not None and incumbent != candidate:
                        raise RuntimeError(f"conflicting FSDS submission metadata for {accession}")
                    submissions[accession] = candidate
    return submissions, digests


def select_formation_submissions(submissions: dict[str, Submission]) -> dict[datetime, tuple[Submission, ...]]:
    by_cik: dict[str, list[Submission]] = defaultdict(list)
    for submission in submissions.values():
        by_cik[submission.cik].append(submission)
    selected: dict[datetime, tuple[Submission, ...]] = {}
    for cutoff in FORMATION_CLOSES:
        rows: list[Submission] = []
        for values in by_cik.values():
            public = [row for row in values if row.accepted <= cutoff and len(row.period) == 8 and row.period.isdigit()]
            if public:
                rows.append(max(public, key=lambda row: (row.period, row.accepted, row.accession)))
        selected[cutoff] = tuple(sorted(rows, key=lambda row: (row.cik, row.accession)))
    return selected


def _cache_path(cache_root: Path, submission: Submission) -> Path:
    return cache_root / submission.accession[:4] / f"{submission.accession}.xml.gz"


def _write_gzip_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
                compressed.write(payload)
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def _download_chunk(rows: list[Submission], cache_root: Path) -> Counter[str]:
    result: Counter[str] = Counter()
    headers = {"User-Agent": settings.sec_user_agent, "Accept-Encoding": "gzip, deflate"}
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=False) as client:
        fetcher = PipelinedSecFetcher(
            client=client,
            target_rps=7.0,
            concurrency=16,
        )
        tasks = [FetchTask(key=row.accession, url=row.url, headers=headers) for row in rows]
        lookup = {row.accession: row for row in rows}
        async for fetched in fetcher.fetch_many(tasks):
            accession = str(fetched.key)
            if fetched.error is not None or fetched.response is None:
                result["transport_error"] += 1
                continue
            response = fetched.response
            if response.status_code != 200:
                result[f"http_{response.status_code}"] += 1
                continue
            payload = response.content
            if not payload.lstrip().startswith(b"<"):
                result["non_xml"] += 1
                continue
            _write_gzip_atomic(_cache_path(cache_root, lookup[accession]), payload)
            result["downloaded"] += 1
    return result


def download_instances(rows: tuple[Submission, ...], cache_root: Path, *, chunk_size: int = 100) -> Counter[str]:
    pending = [row for row in rows if not _cache_path(cache_root, row).is_file()]
    totals: Counter[str] = Counter(cached=len(rows) - len(pending))
    for start in range(0, len(pending), chunk_size):
        chunk = pending[start : start + chunk_size]
        totals.update(asyncio.run(_download_chunk(chunk, cache_root)))
        print(
            json.dumps(
                {
                    "download_progress": min(start + len(chunk), len(pending)),
                    "download_total": len(pending),
                    "status": dict(sorted(totals.items())),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
    return totals


def parse_cover_contexts(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rb") as handle:
        root = ET.parse(handle, parser=SAFE_XML_PARSER).getroot()
    contexts: dict[str, ET._Element] = {}
    for element in root.iter():
        if not isinstance(element.tag, str) or ET.QName(element.tag).localname != "context":
            continue
        context_id = element.get("id")
        if context_id:
            contexts[context_id] = element

    values: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        local = ET.QName(element.tag).localname
        context_ref = element.get("contextRef")
        text = " ".join("".join(element.itertext()).split())
        if local in _COVER_FACTS and context_ref and text:
            values[context_ref][local].append(text)

    document_periods = sorted({value for facts in values.values() for value in facts.get("DocumentPeriodEndDate", [])})
    if len(document_periods) != 1:
        return []

    out: list[dict[str, Any]] = []
    for context_ref, facts in sorted(values.items()):
        # The three security facts identify one listed class through their
        # shared context. DocumentPeriodEndDate is document-level and may use
        # the default context, so require one unique value across the instance.
        if not _SECURITY_FACTS.issubset(facts):
            continue
        context = contexts.get(context_ref)
        if context is None:
            continue
        out.append(
            {
                "context_ref": context_ref,
                "dimensions": context_dimensions(context),
                "period": context_period(context),
                "facts": {
                    "DocumentPeriodEndDate": document_periods,
                    **{key: sorted(set(facts[key])) for key in sorted(_SECURITY_FACTS)},
                },
            }
        )
    return out


def parse_cover_cache(
    rows: tuple[Submission, ...],
    cache_root: Path,
) -> dict[str, list[dict[str, Any]] | None]:
    parsed: dict[str, list[dict[str, Any]] | None] = {}
    for row in rows:
        path = _cache_path(cache_root, row)
        if not path.is_file():
            parsed[row.accession] = None
            continue
        try:
            parsed[row.accession] = parse_cover_contexts(path)
        except ET.XMLSyntaxError, OSError, EOFError:
            parsed[row.accession] = None
    return parsed


def cover_census(
    rows: tuple[Submission, ...],
    cache_root: Path,
    parsed: dict[str, list[dict[str, Any]] | None],
) -> dict[str, Any]:
    counters: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        path = _cache_path(cache_root, row)
        if not path.is_file():
            counters["missing_cache"] += 1
            continue
        contexts = parsed.get(row.accession)
        if contexts is None:
            counters["parse_error"] += 1
            if len(examples["parse_error"]) < 10:
                examples["parse_error"].append(row.accession)
            continue
        counters["accessions_parsed"] += 1
        counters["complete_contexts"] += len(contexts)
        if not contexts:
            counters["without_complete_context"] += 1
            if len(examples["without_complete_context"]) < 10:
                examples["without_complete_context"].append(row.accession)
        elif len(contexts) == 1:
            counters["one_complete_context"] += 1
        else:
            counters["multiple_complete_contexts"] += 1
    return {"counts": dict(sorted(counters.items())), "examples": dict(sorted(examples.items()))}


def _price_symbol(value: str) -> str:
    """Map only SEC class punctuation to Intrader's filename convention."""
    return value.strip().upper().replace(".", "_").replace("-", "_")


def price_sessions_by_symbol(
    price_series_dir: Path,
    sessions: frozenset[date],
) -> dict[str, frozenset[date]]:
    """Read only the date column and report exact observed formation sessions."""
    result: dict[str, frozenset[date]] = {}
    for path in sorted(price_series_dir.glob("*.csv")):
        observed: set[date] = set()
        with path.open(encoding="utf-8", errors="strict") as handle:
            for line in handle:
                raw_date = line.partition(",")[0].strip()
                try:
                    candidate = date.fromisoformat(raw_date)
                except ValueError:
                    continue
                if candidate in sessions:
                    observed.add(candidate)
        result[path.stem.strip().upper()] = frozenset(observed)
    return result


def formation_identity_census(
    selected: dict[datetime, tuple[Submission, ...]],
    cover_submissions: dict[datetime, dict[str, Submission]],
    cache_root: Path,
    price_symbols: frozenset[str],
    price_sessions: dict[str, frozenset[date]] | None = None,
    parsed: dict[str, list[dict[str, Any]] | None] | None = None,
) -> dict[str, Any]:
    """Reconcile dated SEC cover identities to filenames without reading returns."""
    parsed = {} if parsed is None else parsed
    report: dict[str, Any] = {}
    for cutoff, rows in selected.items():
        counters: Counter[str] = Counter(population_ciks=len(rows))
        titles: Counter[str] = Counter()
        exchanges: Counter[str] = Counter()
        symbols: Counter[str] = Counter()
        symbol_ciks: dict[str, set[str]] = defaultdict(set)
        exact_bar_ciks: set[str] = set()
        examples: dict[str, list[str]] = defaultdict(list)
        records: list[dict[str, Any]] = []
        for row in rows:
            cover_row = cover_submissions.get(cutoff, {}).get(row.cik)
            if cover_row is None:
                counters["without_any_complete_security_context"] += 1
                continue
            if cover_row.accession != row.accession:
                counters["ciks_using_earlier_complete_cover_filing"] += 1
            row = cover_row
            if row.accession not in parsed:
                path = _cache_path(cache_root, row)
                if not path.is_file():
                    parsed[row.accession] = None
                else:
                    try:
                        parsed[row.accession] = parse_cover_contexts(path)
                    except ET.XMLSyntaxError, OSError, EOFError:
                        parsed[row.accession] = None
            contexts = parsed[row.accession]
            if contexts is None:
                counters["missing_or_unparseable_accession"] += 1
                continue
            if not contexts:
                counters["without_complete_security_context"] += 1
                continue
            counters["ciks_with_security_context"] += 1
            counters["security_contexts"] += len(contexts)
            cik_matched = False
            seen_triples: set[tuple[str, str, str]] = set()
            for context in contexts:
                facts = context["facts"]
                title_values = facts["Security12bTitle"]
                symbol_values = facts["TradingSymbol"]
                exchange_values = facts["SecurityExchangeName"]
                if len(title_values) != 1 or len(symbol_values) != 1 or len(exchange_values) != 1:
                    counters["contexts_with_non_singleton_fact"] += 1
                    continue
                title = str(title_values[0]).strip()
                symbol = str(symbol_values[0]).strip().upper()
                exchange = str(exchange_values[0]).strip()
                triple = (title, symbol, exchange)
                if triple in seen_triples:
                    counters["duplicate_identical_security_contexts"] += 1
                    continue
                seen_triples.add(triple)
                counters["distinct_security_triples"] += 1
                titles[title] += 1
                symbols[symbol] += 1
                exchanges[exchange] += 1
                normalized = _price_symbol(symbol)
                symbol_ciks[normalized].add(row.cik)
                price_filename_match = normalized in price_symbols
                exact_formation_bar = price_sessions is not None and cutoff.date() in price_sessions.get(
                    normalized, frozenset()
                )
                records.append(
                    {
                        "accession": row.accession,
                        "accepted_at": row.accepted.isoformat(),
                        "cik": row.cik,
                        "document_period": row.period,
                        "exact_formation_session_bar": exact_formation_bar,
                        "exchange": exchange,
                        "normalized_price_symbol": normalized,
                        "price_filename_match": price_filename_match,
                        "security_title": title,
                        "trading_symbol": symbol,
                    }
                )
                if price_filename_match:
                    counters["contexts_matching_price_filename"] += 1
                    cik_matched = True
                    if exact_formation_bar:
                        counters["contexts_with_exact_formation_session_bar"] += 1
                        exact_bar_ciks.add(row.cik)
                elif len(examples["unmatched_security_context"]) < 25:
                    examples["unmatched_security_context"].append(
                        f"{row.cik}:{row.accession}:{title}|{symbol}|{exchange}"
                    )
            if cik_matched:
                counters["ciks_matching_price_filename"] += 1
        if price_sessions is not None:
            counters["ciks_with_exact_formation_session_bar"] = len(exact_bar_ciks)
        duplicates = {symbol: sorted(ciks) for symbol, ciks in symbol_ciks.items() if len(ciks) > 1}
        counters["normalized_symbols_shared_across_ciks"] = len(duplicates)
        report[cutoff.isoformat()] = {
            "counts": dict(sorted(counters.items())),
            "security_titles": dict(sorted(titles.items())),
            "exchange_names": dict(sorted(exchanges.items())),
            "trading_symbols": dict(sorted(symbols.items())),
            "duplicate_normalized_symbol_ciks": duplicates,
            "examples": dict(sorted(examples.items())),
            "records": records,
        }
    return report


def resolve_cover_submissions(
    submissions: dict[str, Submission],
    selected: dict[datetime, tuple[Submission, ...]],
    cache_root: Path,
    parsed: dict[str, list[dict[str, Any]] | None] | None = None,
) -> tuple[dict[datetime, dict[str, Submission]], Counter[str]]:
    """Choose the latest public annual accession carrying a complete cover."""
    by_cik: dict[str, list[Submission]] = defaultdict(list)
    for submission in submissions.values():
        by_cik[submission.cik].append(submission)
    parsed = {} if parsed is None else parsed
    attempted_downloads: set[str] = set()
    download_counts: Counter[str] = Counter()
    resolved: dict[datetime, dict[str, Submission]] = {}

    while True:
        pending: dict[str, Submission] = {}
        resolved = {}
        for cutoff, latest_rows in selected.items():
            formation: dict[str, Submission] = {}
            for latest in latest_rows:
                candidates = sorted(
                    (
                        candidate
                        for candidate in by_cik[latest.cik]
                        if candidate.accepted <= cutoff and candidate.period == latest.period
                    ),
                    key=lambda candidate: (candidate.accepted, candidate.accession),
                    reverse=True,
                )
                for candidate in candidates:
                    if candidate.accession not in parsed:
                        path = _cache_path(cache_root, candidate)
                        if not path.is_file():
                            if candidate.accession not in attempted_downloads:
                                pending[candidate.accession] = candidate
                            # An unfetched newer accession cannot be skipped in
                            # favour of an older one without guessing its facts.
                            break
                        try:
                            parsed[candidate.accession] = parse_cover_contexts(path)
                        except ET.XMLSyntaxError, OSError, EOFError:
                            parsed[candidate.accession] = None
                    contexts = parsed.get(candidate.accession)
                    if contexts:
                        formation[latest.cik] = candidate
                        break
            resolved[cutoff] = formation
        if not pending:
            return resolved, download_counts
        rows = tuple(sorted(pending.values(), key=lambda row: row.accession))
        attempted_downloads.update(pending)
        download_counts.update(download_instances(rows, cache_root))


def _unique_selected(selected: dict[datetime, tuple[Submission, ...]]) -> tuple[Submission, ...]:
    unique = {row.accession: row for rows in selected.values() for row in rows}
    return tuple(sorted(unique.values(), key=lambda row: row.accession))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fsds-dir", type=Path, required=True)
    parser.add_argument("--instance-cache", type=Path)
    parser.add_argument(
        "--price-series-dir",
        type=Path,
        help="optional Intrader Day directory; reads CSV filenames only",
    )
    parser.add_argument("--manifest-only", action="store_true")
    args = parser.parse_args()

    submissions, digests = load_submissions(args.fsds_dir)
    selected = select_formation_submissions(submissions)
    unique = _unique_selected(selected)
    report: dict[str, Any] = {
        "parser_version": PARSER_VERSION,
        "script_sha256": _sha256(Path(__file__)),
        "archives": digests,
        "annual_accessions": len(submissions),
        "annual_ciks": len({row.cik for row in submissions.values()}),
        "formation_latest_accessions": {cutoff.isoformat(): len(rows) for cutoff, rows in selected.items()},
        "unique_selected_accessions": len(unique),
    }
    if args.manifest_only:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    if args.instance_cache is None:
        parser.error("--instance-cache is required unless --manifest-only is set")

    report["download"] = dict(sorted(download_instances(unique, args.instance_cache).items()))
    parsed = parse_cover_cache(unique, args.instance_cache)
    report["cover_census"] = cover_census(unique, args.instance_cache, parsed)
    cover_submissions, fallback_download = resolve_cover_submissions(
        submissions,
        selected,
        args.instance_cache,
        parsed,
    )
    report["fallback_download"] = dict(sorted(fallback_download.items()))
    report["formation_complete_cover_accessions"] = {
        cutoff.isoformat(): len(rows) for cutoff, rows in cover_submissions.items()
    }
    if args.price_series_dir is not None:
        price_symbols = frozenset(path.stem.strip().upper() for path in args.price_series_dir.glob("*.csv"))
        price_sessions = price_sessions_by_symbol(
            args.price_series_dir,
            frozenset(cutoff.date() for cutoff in selected),
        )
        report["price_namespace"] = {
            "directory": str(args.price_series_dir),
            "csv_filenames": len(price_symbols),
        }
        report["formation_identity_census"] = formation_identity_census(
            selected,
            cover_submissions,
            args.instance_cache,
            price_symbols,
            price_sessions,
            parsed,
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
