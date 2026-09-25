"""#3384 slice 2a — admissibility probe for the two SEC per-security sources we do not hold (read-only).

Sources: SEC fails-to-deliver (FTD) and SEC MIDAS "Metrics by Individual Security and Exchange". Nothing
is ingested and no outcome return is computed. Per source:

1. **File inventory** from the source's own download page: every file, its period, and the cadence.
2. **Publication clock** from an HTTP ``HEAD`` per file: ``Last-Modified`` minus the period end. This is
   an UPPER BOUND on the first-publication lag (a later re-post moves Last-Modified forward, never
   back), so it is the conservative side for an availability rule. Re-posts show up as many files
   sharing one Last-Modified date and are reported as such.
3. **Identity route**: one sampled file per year, one settlement/trade date per file. The source's
   ticker on that date is matched onto Intrader series whose stored bounds contain the date,
   separators unified (``.``/``-``/``/``/``_`` -> ``.``). Both directions are reported: the share of
   source tickers that land on a series, and the share of admitted series with a bar on that date
   that appear in the source. A ticker match is a SYMBOL match: ticker reuse and renames are not
   resolved here, which is exactly the gap the identity route has to close.

    SEC_UA="<Name> <email>" PYTHONPATH=. uv run python -m scripts.probe_3384_sec_sources \\
        --out <path.json> --cache var/census/src

``SEC_UA`` overrides ``settings.sec_user_agent`` (SEC refuses generic agents). Requests are paced at
``REQUEST_INTERVAL_S`` (well under SEC's 10 req/s fair-access limit). Downloads are cached.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import IO, Any, Final

import httpx
import psycopg

from app.config import settings
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import (
    INTRADER_CAPTURE_DATE,
    SURVIVORSHIP_FREE_VENDOR,
    load_universe_selection,
)

PROBE_VERSION: Final = "sec-sources-probe-v1"
SEC: Final = "https://www.sec.gov"
FTD_PAGE: Final = f"{SEC}/data-research/sec-markets-data/fails-deliver-data"
MIDAS_PAGE: Final = f"{SEC}/data-research/sec-markets-data/market-structure-data-security-exchange"
REQUEST_INTERVAL_S: Final = 0.25

_FTD_HALF: Final = re.compile(r"cnsfails(\d{4})(\d{2})([ab])(?:_\d+)?\.zip$")
_FTD_QUARTER: Final = re.compile(r"cnsp_sec_fails_(\d{4})q([1-4])\.zip$")
_MIDAS: Final = re.compile(r"individual_security_exchange_(\d{4})_q([1-4])\d*\.zip$")


@dataclass(frozen=True)
class SourceFile:
    url: str
    period_start: date
    period_end: date
    label: str


def _quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    start = date(year, 3 * quarter - 2, 1)
    nxt = date(year + (quarter == 4), 1 if quarter == 4 else 3 * quarter + 1, 1)
    return start, nxt - timedelta(days=1)


def _month_end(year: int, month: int) -> date:
    return date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)


def parse_ftd_href(href: str) -> SourceFile | None:
    """FTD file -> its settlement period. Half-month ``a`` = 1st-15th, ``b`` = 16th-month end."""
    if m := _FTD_HALF.search(href):
        year, month, half = int(m[1]), int(m[2]), m[3]
        start = date(year, month, 1 if half == "a" else 16)
        end = date(year, month, 15) if half == "a" else _month_end(year, month)
        return SourceFile(SEC + href, start, end, f"{year}{month:02d}{half}")
    if m := _FTD_QUARTER.search(href):
        start, end = _quarter_bounds(int(m[1]), int(m[2]))
        return SourceFile(SEC + href, start, end, f"{m[1]}q{m[2]}")
    return None


def parse_midas_href(href: str) -> SourceFile | None:
    if m := _MIDAS.search(href):
        start, end = _quarter_bounds(int(m[1]), int(m[2]))
        return SourceFile(SEC + href, start, end, f"{m[1]}q{m[2]}")
    return None


def unify_symbol(symbol: str) -> str:
    return re.sub(r"[-/_.]", ".", symbol.strip().upper())


class _Client:
    def __init__(self, user_agent: str, cache: Path) -> None:
        self._http = httpx.Client(headers={"User-Agent": user_agent}, timeout=300, follow_redirects=True)
        self._cache = cache
        self._last = 0.0

    def _pace(self) -> None:
        wait = REQUEST_INTERVAL_S - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def get_text(self, url: str) -> str:
        self._pace()
        response = self._http.get(url)
        response.raise_for_status()
        return response.text

    def head(self, url: str) -> dict[str, Any]:
        self._pace()
        response = self._http.head(url)
        modified = response.headers.get("last-modified")
        return {
            "status": response.status_code,
            "last_modified": None if modified is None else parsedate_to_datetime(modified).astimezone(UTC).date(),
            "bytes": int(response.headers.get("content-length", 0)),
        }

    def download(self, url: str) -> Path:
        path = self._cache / url.rsplit("/", 1)[1]
        if not path.exists():
            self._pace()
            with self._http.stream("GET", url) as response, path.with_suffix(".part").open("wb") as out:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    out.write(chunk)
            path.with_suffix(".part").rename(path)
        return path


def _inventory(client: _Client, page: str, parse: Any) -> list[SourceFile]:
    html = client.get_text(page)
    files = {f.url: f for href in re.findall(r'href="([^"]+\.zip)"', html) if (f := parse(href))}
    return sorted(files.values(), key=lambda f: (f.period_end, f.url))


def _clock(client: _Client, files: list[SourceFile]) -> dict[str, Any]:
    rows = []
    for f in files:
        h = client.head(f.url)
        lag = None if h["last_modified"] is None else (h["last_modified"] - f.period_end).days
        rows.append({"label": f.label, "url": f.url, "period_end": str(f.period_end), **h, "lag_days_upper": lag})
    by_modified = Counter(str(r["last_modified"]) for r in rows)
    reposts = {d: n for d, n in by_modified.items() if n >= 5}
    fresh = sorted(
        r["lag_days_upper"] for r in rows if r["lag_days_upper"] is not None and str(r["last_modified"]) not in reposts
    )
    return {
        "files": rows,
        "repost_dates": reposts,
        "lag_days_upper_excluding_reposts": _quantiles(fresh),
    }


def _quantiles(values: list[int]) -> dict[str, Any]:
    if not values:
        return {"n": 0}
    pick = lambda q: values[min(len(values) - 1, int(q * len(values)))]  # noqa: E731
    return {"n": len(values), "min": values[0], "p50": pick(0.5), "p90": pick(0.9), "max": values[-1]}


def _intrader(conn: psycopg.Connection[Any]) -> tuple[list[tuple[int, str, date, date]], set[int]]:
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset(validated))
    rows = conn.execute(
        "SELECT series_id, vendor_symbol, first_bar, last_bar FROM research_price_series "
        "WHERE vendor = %(v)s AND bar_count IS NOT NULL",
        {"v": SURVIVORSHIP_FREE_VENDOR},
    ).fetchall()
    return [(int(a), str(b), c, d) for a, b, c, d in rows], {s.series_id for s in selection.admitted}


def _match(
    conn: psycopg.Connection[Any],
    series: list[tuple[int, str, date, date]],
    admitted: set[int],
    on: date,
    tickers: set[str],
) -> dict[str, Any]:
    spanning = {unify_symbol(sym): sid for sid, sym, first, last in series if first <= on <= last}
    with_bar = {
        int(r[0])
        for r in conn.execute(
            "SELECT d.series_id FROM research_price_daily d JOIN research_price_series s USING (series_id) "
            "WHERE s.vendor = %(v)s AND d.bar_date = %(d)s",
            {"v": SURVIVORSHIP_FREE_VENDOR, "d": on},
        )
    }
    admitted_with_bar = with_bar & admitted
    matched = {t for t in tickers if t in spanning}
    matched_admitted = {t for t in matched if spanning[t] in admitted}
    symbol_of = {sid: unify_symbol(sym) for sid, sym, _, _ in series}
    return {
        "date": str(on),
        "source_tickers": len(tickers),
        "matched_to_spanning_series": len(matched),
        "matched_to_admitted_series": len(matched_admitted),
        "admitted_series_with_bar": len(admitted_with_bar),
        "admitted_with_bar_in_source": sum(symbol_of[sid] in tickers for sid in admitted_with_bar),
    }


def _ftd_sample(path: Path) -> tuple[date, set[str], int, list[str]]:
    """First settlement date in the file: its tickers, CUSIP count, and the header."""
    with zipfile.ZipFile(path) as z:
        name = z.namelist()[0]
        text = z.read(name).decode("latin-1")
    lines = text.splitlines()
    header = lines[0].split("|")
    rows = [line.split("|") for line in lines[1:] if line.count("|") >= 5]
    first = min(r[0] for r in rows if r[0].isdigit())
    day = [r for r in rows if r[0] == first]
    return (
        datetime.strptime(first, "%Y%m%d").date(),
        {unify_symbol(r[2]) for r in day if r[2].strip()},
        len({r[1] for r in day}),
        header,
    )


def _midas_sample(path: Path) -> tuple[date, set[str], list[str]]:
    """First trade date in the file and its tickers (all exchange partitions pooled)."""
    with _first_csv(zipfile.ZipFile(path)) as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="latin-1"))
        header = next(reader)
        i_date, i_ticker = header.index("Date"), header.index("Ticker")
        # Sort order is not documented, so scan the whole member for the first row's date.
        first: str | None = None
        tickers: set[str] = set()
        for row in reader:
            if first is None:
                first = row[i_date]
            if row[i_date] == first:
                tickers.add(unify_symbol(row[i_ticker]))
    if first is None:
        raise RuntimeError(f"{path} has no rows")
    return _parse_day(first), tickers, header


def _first_csv(z: zipfile.ZipFile) -> IO[bytes]:
    """The first CSV member, descending into nested zips (MIDAS ships zip -> zip -> monthly CSVs)."""
    for name in z.namelist():
        if name.lower().endswith(".csv"):
            return z.open(name)
        if name.lower().endswith(".zip"):
            return _first_csv(zipfile.ZipFile(io.BytesIO(z.read(name))))
    raise RuntimeError(f"no CSV member in {z.filename}")


def _parse_day(text: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unrecognised date {text!r}")


def _git_provenance() -> dict[str, Any]:
    def git(*argv: str) -> str:
        return subprocess.run(["git", *argv], capture_output=True, text=True, check=True).stdout.strip()

    status = git("status", "--porcelain")
    return {"git_head": git("rev-parse", "HEAD"), "git_dirty": bool(status), "git_status": status.splitlines()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--midas-years", default="2012,2016,2020,2024", help="MIDAS sample years (Q2 file)")
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)
    client = _Client(os.environ.get("SEC_UA") or settings.sec_user_agent, args.cache)

    ftd_files = _inventory(client, FTD_PAGE, parse_ftd_href)
    midas_files = _inventory(client, MIDAS_PAGE, parse_midas_href)
    ftd_clock = _clock(client, ftd_files)
    midas_clock = _clock(client, midas_files)

    ftd_samples: list[dict[str, Any]] = []
    midas_samples: list[dict[str, Any]] = []
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        series, admitted = _intrader(conn)
        for year in range(2004, INTRADER_CAPTURE_DATE.year + 1):
            wanted = f"{year}q2" if year < 2009 else ("200907a" if year == 2009 else f"{year}06a")
            pick = [f for f in ftd_files if f.label == wanted]
            if not pick:
                ftd_samples.append({"year": year, "file": wanted, "missing": True})
                continue
            on, tickers, cusips, header = _ftd_sample(client.download(pick[0].url))
            ftd_samples.append(
                {"year": year, "file": pick[0].label, "header": header, "cusips": cusips}
                | _match(conn, series, admitted, on, tickers)
            )
        for year in (int(y) for y in args.midas_years.split(",")):
            pick = [f for f in midas_files if f.label == f"{year}q2"]
            if not pick:
                midas_samples.append({"year": year, "missing": True})
                continue
            on, tickers, header = _midas_sample(client.download(pick[0].url))
            midas_samples.append(
                {"year": year, "file": pick[0].label, "header": header} | _match(conn, series, admitted, on, tickers)
            )

    probe = {
        "probe_version": PROBE_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        **_git_provenance(),
        "ftd": {"page": FTD_PAGE, "clock": ftd_clock, "samples": ftd_samples},
        "midas": {"page": MIDAS_PAGE, "clock": midas_clock, "samples": midas_samples},
    }
    body = json.dumps(probe, indent=1, sort_keys=True, default=str) + "\n"
    args.out.write_text(body)
    print(f"wrote {args.out} sha256 {hashlib.sha256(body.encode()).hexdigest()}")
    for name in ("ftd", "midas"):
        clock = probe[name]["clock"]
        print(name, len(clock["files"]), "files; reposts", clock["repost_dates"])
        print("  lag (upper, excl. reposts)", clock["lag_days_upper_excluding_reposts"])
        for s in probe[name]["samples"]:
            print("  ", {k: v for k, v in s.items() if k != "header"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
