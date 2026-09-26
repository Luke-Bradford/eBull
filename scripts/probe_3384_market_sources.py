"""#3384 slice 2b — admissibility probe for the market-level sources we do not hold (read-only).

Sources: Cboe volatility indices and put/call ratios, FRED/ALFRED, CFTC Commitments of Traders, and
Wikipedia pageviews (+ the Wikidata ticker map it would need). Nothing is ingested and no outcome return
is computed. Per source: history start, cadence, publication clock, identity key.

- **Cboe indices:** each served CSV's first/last row and row count, against the FIRST VALUE DATE / LAUNCH
  DATE rows in Cboe's own methodology documents (``CBOE_LAUNCH``, frozen with their source URLs). Rows
  before the launch were back-calculated, not published at the time. Served-vs-stored VIX overlap is
  compared against our ``research_price_series`` copy.
- **Cboe put/call:** the frozen archive CSVs (their preamble notes are kept verbatim) and the per-day JSON
  that continues them; the JSON's ``Last-Modified`` minus the trade date is the only clock measured.
- **FRED/ALFRED:** keyless ``fredgraph.csv`` for the current vintage and ``alfredgraph.csv`` with a
  ``vintage_date``. ALFRED answers 404 for a date before a series' first vintage, so the first vintage is
  found by bisection. Revisions = observations whose value in a fixed old vintage differs from today's.
- **COT:** the CFTC historical compressed files (legacy futures-only and TFF futures-only). The report
  "as of" dates are counted per year, by weekday and by gap, against CFTC's stated cadence history. The
  files carry no release date.
- **Wikipedia:** the pageviews REST floor and latest day, and a Wikidata census of exchange/ticker
  statements (P414 with P249) and their start/end qualifiers, matched onto Intrader symbols by stratum.

    PROBE_UA="<Name> <email>" PYTHONPATH=. uv run python -m scripts.probe_3384_market_sources \\
        --out <path.json> --cache <dir>

The cache is keyed by file name and never revalidated: pass an EMPTY ``--cache`` for a fresh
measurement. Every served input here is mutable (Cboe CSVs, FRED, Wikidata), and only the COT zips and
the Cboe CSV hashes are retained, so a re-run measures today's sources, not the committed run's.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import time
import zipfile
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg

from app.config import settings
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import SURVIVORSHIP_FREE_VENDOR, load_universe_selection
from scripts.probe_3384_sec_sources import _git_provenance, _sha256, unify_symbol

PROBE_VERSION: Final = "market-sources-probe-v2"
REQUEST_INTERVAL_S: Final = 0.5

CBOE_INDEX_CSV: Final = "https://cdn.cboe.com/api/global/us_indices/daily_prices/{}_History.csv"
CBOE_PC_ARCHIVE: Final = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/{}.csv"
CBOE_PC_DAILY: Final = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{}_daily_options"
PC_ARCHIVES: Final = ("totalpc", "equitypc", "indexpc", "vixpc", "etppc")

_VIX_DOC: Final = (
    "https://cdn.cboe.com/api/global/us_indices/governance/Volatility_Index_Methodology_Cboe_Volatility_Index.pdf"
)
_TERM_DOC: Final = (
    "https://cdn.cboe.com/api/global/us_indices/governance/"
    "Volatility_Index_Methodology_Selected_SPX_Target_Expected_Volatility_Term_Indices.pdf"
)
_SKEW_DOC: Final = "https://cdn.cboe.com/resources/indices/documents/SKEWwhitepaperjan2011.pdf"
_VVIX_DOC: Final = "https://cdn.cboe.com/resources/indices/documents/vvix-termstructure.pdf"


@dataclass(frozen=True)
class Launch:
    """One row of a Cboe methodology document's index-information table, as printed.

    ``published_from`` is the first day of the launch month (or year, where only a year is printed):
    rows dated before it are back-calculated by the document's own disclaimer. Rows inside the launch
    month/year are not classified.
    """

    first_value: str
    launch: str
    published_from: date
    source: str


#: Frozen from the methodology documents on 2026-09-25 (the SKEW and VVIX white papers print no table:
#: SKEW's is dated January 2011 and "introduc[es] a new benchmark"; VVIX's is (c) 2012, "a new index").
CBOE_LAUNCH: Final[dict[str, Launch]] = {
    "VIX": Launch("January 1990", "September 22, 2003", date(2003, 9, 1), _VIX_DOC),
    "VIX9D": Launch("January 2011", "October 2013", date(2013, 10, 1), _TERM_DOC),
    "VIX3M": Launch("September 2009", "October 2013", date(2013, 10, 1), _TERM_DOC),
    "VIX6M": Launch("January 2008", "November 2013", date(2013, 11, 1), _TERM_DOC),
    "VIX1Y": Launch("January 2007", "2018", date(2018, 1, 1), _TERM_DOC),
    "SKEW": Launch("1990 (white paper chart)", "2011 (white paper dated January 2011)", date(2011, 1, 1), _SKEW_DOC),
    "VVIX": Launch("June 2006 (white paper chart)", "2012 (white paper (c) 2012)", date(2012, 1, 1), _VVIX_DOC),
}

FREDGRAPH: Final = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"
ALFREDGRAPH: Final = "https://alfred.stlouisfed.org/graph/alfredgraph.csv?id={}&vintage_date={}"
#: Candidate regime-conditioning series: credit spreads, curve, rates, financial-conditions indices and
#: the macro releases most often used as regime inputs. A probe list, not a feature decision.
FRED_SERIES: Final = (
    "BAA10Y",
    "AAA10Y",
    "BAMLH0A0HYM2",
    "BAMLC0A0CM",
    "T10Y2Y",
    "T10Y3M",
    "DGS10",
    "DTB3",
    "NFCI",
    "STLFSI4",
    "ICSA",
    "UNRATE",
    "PAYEMS",
    "INDPRO",
    "CPIAUCSL",
)
#: The old vintage each series' revisions are measured against, and the observation cut inside it.
REVISION_VINTAGE: Final = date(2015, 6, 1)
REVISION_OBS_CUT: Final = date(2014, 12, 31)
ALFRED_FLOOR: Final = date(1940, 1, 1)
#: The programme's discovery window ends here (docs/proposals/ta/2026-09-25-pattern-hunt-programme.md).
DISCOVERY_END: Final = date(2008, 12, 31)

COT_PAGE: Final = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm"
COT_BASE: Final = "https://www.cftc.gov"
_COT_LEGACY: Final = re.compile(r"/files/dea/history/deacot(\d{4})(?:_(\d{4}))?\.zip$")
_COT_TFF: Final = re.compile(r"/files/dea/history/(?:fut_fin_txt|fin_fut_txt)_(\d{4})(?:_(\d{4}))?\.zip$")
#: E-mini S&P 500, the equity-market contract a regime input would use.
COT_EMINI_SP: Final = "13874A"
#: CFTC's stated history (About the COT Reports): monthly from 1962, mid-month + month-end in 1990,
#: every two weeks in 1992, weekly in 2000.
COT_ERAS: Final = ((1986, 1989), (1990, 1991), (1992, 1999), (2000, 2100))

PAGEVIEWS: Final = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{}/daily/{}/{}"
)
PAGEVIEW_PROBE_ARTICLES: Final = ("Apple_Inc.", "General_Electric", "Sears")
WIKIDATA_SPARQL: Final = "https://query.wikidata.org/sparql"
#: Wikidata items for the New York Stock Exchange and Nasdaq.
WIKIDATA_US_EXCHANGES: Final = {"NYSE": "Q13677", "Nasdaq": "Q82059"}


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

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        self._pace()
        return self._http.get(url, **kwargs)

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


def _last_modified(response: httpx.Response) -> datetime | None:
    raw = response.headers.get("last-modified")
    return None if raw is None else parsedate_to_datetime(raw).astimezone(UTC)


def _us_date(text: str) -> date:
    return datetime.strptime(text.strip(), "%m/%d/%Y").date()


#: In a DAILY series, a calendar gap longer than this is listed with its dates, not only counted.
LONG_GAP_DAYS: Final = 5


def cadence(dates: Iterable[date], list_gaps_over: int | None = LONG_GAP_DAYS) -> dict[str, Any]:
    """Rows per year, weekday mix, the day-gap histogram and, unless ``list_gaps_over`` is None (weekly
    or monthly series, where every gap would qualify), each gap longer than it, dated."""
    days = sorted(set(dates))
    pairs = list(zip(days, days[1:], strict=False))
    gaps = Counter((b - a).days for a, b in pairs)
    out: dict[str, Any] = {
        "n": len(days),
        "first": str(days[0]) if days else None,
        "last": str(days[-1]) if days else None,
        "per_year": dict(sorted(Counter(str(d.year) for d in days).items())),
        "weekdays": dict(sorted(Counter(d.strftime("%a") for d in days).items())),
        "gap_days": dict(sorted(gaps.items())),
    }
    if list_gaps_over is not None:
        out["long_gaps"] = [
            {"after": str(a), "next": str(b), "days": (b - a).days} for a, b in pairs if (b - a).days > list_gaps_over
        ]
    return out


def parse_cboe_index_csv(text: str) -> dict[date, tuple[str, ...]]:
    """Cboe history CSV (``DATE,OPEN,HIGH,LOW,CLOSE`` or ``DATE,<NAME>``) -> date -> value fields."""
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    if header[0].strip().upper() != "DATE":
        raise ValueError(f"unexpected Cboe header {header}")
    return {_us_date(row[0]): tuple(v.strip() for v in row[1:]) for row in reader if row and row[0].strip()}


def parse_putcall_archive(text: str) -> tuple[list[str], list[date]]:
    """Put/call archive CSV -> (the preamble lines above the ``DATE`` header, verbatim; the row dates)."""
    lines = text.splitlines()
    at = next((i for i, line in enumerate(lines) if line.strip().upper().startswith("DATE")), None)
    if at is None:
        raise ValueError("put/call archive has no DATE header row")
    notes = [line.strip().strip(",").strip() for line in lines[:at] if line.strip().strip(",").strip()]
    days = [_us_date(line.split(",", 1)[0]) for line in lines[at + 1 :] if line.split(",", 1)[0].strip()]
    return notes, days


def bisect_first_vintage(exists: Callable[[date], bool], lo: date, hi: date) -> date | None:
    """Earliest date in ``[lo, hi]`` for which ``exists`` holds, ASSUMING it is monotone (False..True).

    The assumption is not verified: an earlier island of served vintages would be missed. The caller
    records every probed date so the boundary can be audited.
    """
    if not exists(hi):
        return None
    if exists(lo):
        return lo
    while (hi - lo).days > 1:
        mid = lo + (hi - lo) // 2
        if exists(mid):
            hi = mid
        else:
            lo = mid
    return hi


def parse_fred_csv(text: str) -> dict[date, str]:
    """FRED/ALFRED graph CSV -> observation date -> value text ('.' = missing is kept as-is)."""
    reader = csv.reader(io.StringIO(text))
    next(reader)
    return {date.fromisoformat(row[0]): row[1].strip() for row in reader if len(row) >= 2 and row[0].strip()}


def count_revisions(old: dict[date, str], new: dict[date, str], cut: date) -> dict[str, int]:
    """Compare two vintages over observation dates up to ``cut``.

    ``common_observations`` counts dates NUMERIC in both, and ``changed`` those whose value differs.
    A date missing or non-numeric in either vintage is counted apart, never as a change, and dates only
    one vintage carries are counted per side. A difference is a changed number, whatever its cause.
    """
    old_cut = {d: _num(v) for d, v in old.items() if d <= cut}
    new_cut = {d: _num(v) for d, v in new.items() if d <= cut}
    both = old_cut.keys() & new_cut.keys()
    common = [d for d in both if old_cut[d] is not None and new_cut[d] is not None]
    return {
        "common_observations": len(common),
        "changed": sum(1 for d in common if old_cut[d] != new_cut[d]),
        "non_numeric_in_either": len(both) - len(common),
        "only_in_old": len(old_cut.keys() - new_cut.keys()),
        "only_in_new": len(new_cut.keys() - old_cut.keys()),
    }


def _num(text: str) -> float | None:
    """A finite number, or None (FRED writes '.' for a missing observation)."""
    try:
        value = float(text)
    except ValueError:
        return None
    return value if value == value and value not in (float("inf"), float("-inf")) else None


# ---- Cboe ------------------------------------------------------------------------------------------


def _cboe_indices(client: _Client, stored_vix: dict[date, tuple[str, ...]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for symbol, launch in CBOE_LAUNCH.items():
        response = client.get(CBOE_INDEX_CSV.format(symbol))
        response.raise_for_status()
        rows = parse_cboe_index_csv(response.text)
        days = sorted(rows)
        modified = _last_modified(response)
        entry: dict[str, Any] = {
            "url": CBOE_INDEX_CSV.format(symbol),
            "header": response.text.splitlines()[0],
            "sha256": hashlib.sha256(response.content).hexdigest(),
            "launch": launch.__dict__ | {"published_from": str(launch.published_from)},
            "rows": len(days),
            "first_row": str(days[0]),
            "last_row": str(days[-1]),
            # A LOWER bound on back-calculated rows: the launch month/year itself is not classified.
            "rows_before_published_from": sum(d < launch.published_from for d in days),
            "rows_published_from_to_discovery_end": sum(launch.published_from <= d <= DISCOVERY_END for d in days),
            "last_modified_utc": None if modified is None else modified.isoformat(),
            "cadence": cadence(days),
        }
        if symbol == "VIX":
            overlap = [d for d in stored_vix if d in rows]
            entry["stored_vs_served"] = {
                "stored_rows": len(stored_vix),
                "stored_first": str(min(stored_vix)) if stored_vix else None,
                "stored_last": str(max(stored_vix)) if stored_vix else None,
                "compared_at_decimals": 4,
                "overlap": len(overlap),
                "stored_not_served": len(stored_vix) - len(overlap),
                "ohlc_differs": sum(_nums(stored_vix[d]) != _nums(rows[d]) for d in overlap),
            }
        out[symbol] = entry
    return out


def _nums(values: tuple[str, ...]) -> tuple[float | None, ...]:
    return tuple(round(v, 4) if (v := _num(x)) is not None else None for x in values)


def _cboe_putcall(client: _Client, archive_last: date | None) -> dict[str, Any]:
    archives: dict[str, Any] = {}
    last_rows: list[date] = []
    for name in PC_ARCHIVES:
        response = client.get(CBOE_PC_ARCHIVE.format(name))
        response.raise_for_status()
        notes, days = parse_putcall_archive(response.text)
        modified = _last_modified(response)
        last_rows.append(max(days))
        archives[name] = {
            "url": CBOE_PC_ARCHIVE.format(name),
            "sha256": hashlib.sha256(response.content).hexdigest(),
            "preamble": notes,
            "last_modified_utc": None if modified is None else modified.isoformat(),
            "cadence": cadence(days),
        }
    last = archive_last or max(last_rows)
    # The daily JSON: the weekdays either side of the archive's end, then one date a year (first weekday
    # on or after 1 June), each with its HTTP status and Last-Modified.
    probes = [last + timedelta(days=k) for k in range(-3, 6)]
    for year in range(last.year, datetime.now(UTC).year + 1):
        d = date(year, 6, 1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        probes.append(d)
    daily = []
    for d in sorted(set(p for p in probes if p.weekday() < 5)):
        response = client.get(CBOE_PC_DAILY.format(d.isoformat()))
        modified = _last_modified(response)
        daily.append(
            {
                "trade_date": str(d),
                "status": response.status_code,
                "last_modified_utc": None if modified is None else modified.isoformat(),
                # An upper bound on the release lag ONLY if the header is not back-dated; a negative value
                # (header before the trade date) would falsify that and is left visible, not dropped.
                "lag_days_upper": None if modified is None else (modified.date() - d).days,
                "ratio_names": (
                    [r.get("name") for r in response.json().get("ratios", [])] if response.status_code == 200 else None
                ),
            }
        )
    return {"archives": archives, "archive_last_row": str(last), "daily_json": daily}


# ---- FRED / ALFRED ---------------------------------------------------------------------------------


def alfred_served(response: httpx.Response, series: str, on: date) -> bool:
    """True for a served vintage (200 with at least one observation), False for 404 (none yet).

    Any other status (429, 403, 5xx), or a 200 without observations, raises: a transient error read as
    "no vintage" would silently move the bisection boundary.
    """
    if response.status_code == 404:
        return False
    if response.status_code != 200:
        raise RuntimeError(f"ALFRED {series} vintage {on}: HTTP {response.status_code}")
    if not parse_fred_csv(response.text):
        raise RuntimeError(f"ALFRED {series} vintage {on}: 200 with no observations")
    return True


def _fred(client: _Client) -> dict[str, Any]:
    today = datetime.now(UTC).date()
    out: dict[str, Any] = {}
    for series in FRED_SERIES:
        current = client.get(FREDGRAPH.format(series))
        current.raise_for_status()
        now_obs = parse_fred_csv(current.text)
        cache: dict[date, httpx.Response] = {}

        def alfred(on: date, series: str = series, cache: dict[date, httpx.Response] = cache) -> httpx.Response:
            if on not in cache:
                cache[on] = client.get(ALFREDGRAPH.format(series, on.isoformat()))
            return cache[on]

        first_vintage = bisect_first_vintage(lambda d: alfred_served(alfred(d), series, d), ALFRED_FLOOR, today)
        first_obs = None if first_vintage is None else parse_fred_csv(alfred(first_vintage).text)
        entry: dict[str, Any] = {
            "current": {
                "observations": len(now_obs),
                "first_observation": str(min(now_obs)) if now_obs else None,
                "last_observation": str(max(now_obs)) if now_obs else None,
                "cadence": cadence(now_obs, list_gaps_over=None),
            },
            "alfred_first_vintage": None if first_vintage is None else str(first_vintage),
            "alfred_first_vintage_observations": (
                None
                if first_obs is None
                else {"n": len(first_obs), "first": str(min(first_obs)), "last": str(max(first_obs))}
            ),
            "alfred_probes": {str(d): r.status_code for d, r in sorted(cache.items())},
        }
        if first_vintage is not None and first_vintage <= REVISION_VINTAGE:
            old = alfred(REVISION_VINTAGE)
            old.raise_for_status()
            entry["revisions_vs_vintage"] = {"vintage": str(REVISION_VINTAGE), "cut": str(REVISION_OBS_CUT)} | (
                count_revisions(parse_fred_csv(old.text), now_obs, REVISION_OBS_CUT)
            )
        out[series] = entry
    return out


# ---- COT ---------------------------------------------------------------------------------------------


def _cot_links(client: _Client) -> dict[str, list[str]]:
    response = client.get(COT_PAGE)
    response.raise_for_status()
    html = response.text
    hrefs = sorted(set(re.findall(r"""href=["']([^"']+\.zip)["']""", html, flags=re.IGNORECASE)))
    return {
        "legacy_futures": [h for h in hrefs if _COT_LEGACY.search(h)],
        "tff_futures": [h for h in hrefs if _COT_TFF.search(h)],
        "all": hrefs,
    }


def _cot_day(text: str) -> date:
    # The column is named "...YYYY-MM-DD" everywhere, but fin_fut_txt_2006_2016 writes it as MM/DD/YYYY.
    # Month and day are not zero-padded there, and a time may follow.
    cleaned = text.strip().split(" ")[0][:10]
    return _us_date(cleaned) if "/" in cleaned else date.fromisoformat(cleaned)


def _cot_dates(path: Path) -> tuple[set[date], set[date], str, int]:
    """All report 'as of' dates in a COT zip, those for the E-mini S&P 500 code, the date column, and
    how many rows were skipped as short or blank-dated (counted, not silently dropped)."""
    every: set[date] = set()
    emini: set[date] = set()
    column = ""
    skipped = 0
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            with z.open(name) as raw:
                reader = csv.reader(io.TextIOWrapper(raw, encoding="latin-1"))
                header = [h.strip() for h in next(reader)]
                # Named Report_Date_as_MM_DD_YYYY in some files (see each file's ``date_column`` in the
                # output), ...YYYY-MM-DD in the rest.
                i_date = next((i for i, h in enumerate(header) if "YYYY-MM-DD" in h or "MM_DD_YYYY" in h), None)
                i_code = next((i for i, h in enumerate(header) if "Contract" in h and "Code" in h), None)
                if i_date is None or i_code is None:
                    raise ValueError(f"{path.name}/{name}: no report-date or contract-code column in {header}")
                column = header[i_date]
                for row in reader:
                    if len(row) <= max(i_date, i_code) or not row[i_date].strip():
                        skipped += 1
                        continue
                    d = _cot_day(row[i_date])
                    every.add(d)
                    if row[i_code].strip() == COT_EMINI_SP:
                        emini.add(d)
    return every, emini, column, skipped


def _cot(client: _Client) -> dict[str, Any]:
    links = _cot_links(client)
    out: dict[str, Any] = {"page": COT_PAGE, "zip_links": len(links["all"])}
    for family in ("legacy_futures", "tff_futures"):
        every: set[date] = set()
        emini: set[date] = set()
        files = []
        for href in links[family]:
            path = client.download(COT_BASE + href)
            dates, sp, column, skipped = _cot_dates(path)
            every |= dates
            emini |= sp
            files.append(
                {
                    "href": href,
                    "sha256": _sha256(path),
                    "date_column": column,
                    "report_dates": len(dates),
                    "first": str(min(dates)) if dates else None,
                    "last": str(max(dates)) if dates else None,
                    "rows_skipped": skipped,
                }
            )
        eras = {}
        for lo, hi in COT_ERAS:
            span = [d for d in every if lo <= d.year <= hi]
            if span:
                c = cadence(span, list_gaps_over=None)
                era = f"{lo}-{min(hi, datetime.now(UTC).year)}"
                eras[era] = {k: c[k] for k in ("n", "first", "last", "weekdays", "gap_days")} | {
                    "non_tuesday_dates": [str(d) for d in sorted(span) if d.weekday() != 1]
                }
        out[family] = {
            "files": files,
            "all_markets": cadence(every, list_gaps_over=None),
            "eras": eras,
            "emini_sp_500": cadence(emini, list_gaps_over=14),
        }
    return out


# ---- Wikipedia / Wikidata ----------------------------------------------------------------------------


def _sparql(client: _Client, query: str) -> list[dict[str, Any]]:
    response = client.get(
        WIKIDATA_SPARQL, params={"query": query}, headers={"Accept": "application/sparql-results+json"}
    )
    response.raise_for_status()
    return [{k: v["value"] for k, v in b.items()} for b in response.json()["results"]["bindings"]]


def _wikipedia(client: _Client, symbols_by_stratum: dict[str, set[str]]) -> dict[str, Any]:
    today = datetime.now(UTC).date()
    pageviews = []
    for article in PAGEVIEW_PROBE_ARTICLES:
        response = client.get(PAGEVIEWS.format(article, "20100101", today.strftime("%Y%m%d")))
        items = response.json().get("items", []) if response.status_code == 200 else []
        days = [datetime.strptime(i["timestamp"][:8], "%Y%m%d").date() for i in items]
        pageviews.append(
            {
                "article": article,
                "status": response.status_code,
                "days": len(days),
                "first_day": str(min(days)) if days else None,
                "last_day": str(max(days)) if days else None,
                "missing_days_in_span": (max(days) - min(days)).days + 1 - len(set(days)) if days else None,
            }
        )
    exchanges: dict[str, Any] = {}
    tickers: dict[str, dict[str, bool]] = {}
    for name, qid in WIKIDATA_US_EXCHANGES.items():
        rows = _sparql(
            client,
            "SELECT ?st ?item ?ticker (SAMPLE(?s) AS ?start) (SAMPLE(?e) AS ?end) (SAMPLE(?enwiki) AS ?article) "
            f"WHERE {{ ?item p:P414 ?st . ?st ps:P414 wd:{qid} ; pq:P249 ?ticker ; wikibase:rank ?rank . "
            "FILTER(?rank != wikibase:DeprecatedRank) "
            "OPTIONAL { ?st pq:P580 ?s } OPTIONAL { ?st pq:P582 ?e } "
            "OPTIONAL { ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> } } "
            "GROUP BY ?st ?item ?ticker",
        )
        exchanges[name] = {
            "qid": qid,
            # Non-deprecated statements; a statement with several ticker qualifiers is one statement here
            # and several (statement, ticker) rows below.
            "ticker_statements": len({r["st"] for r in rows}),
            "statement_ticker_rows": len(rows),
            "items": len({r["item"] for r in rows}),
            "with_start_time": len({r["st"] for r in rows if "start" in r}),
            "with_end_time": len({r["st"] for r in rows if "end" in r}),
            "items_with_enwiki_article": len({r["item"] for r in rows if "article" in r}),
        }
        for r in rows:
            seen = tickers.setdefault(unify_symbol(r["ticker"]), {"any": False, "ended": False, "article": False})
            seen["any"] = True
            seen["ended"] |= "end" in r
            seen["article"] |= "article" in r
    # SYMBOL-level: flags are OR-ed across every statement and item carrying the symbol, so an end date
    # and an article may come from different companies. Not an identity rate.
    matches: dict[str, Any] = {
        "symbols_in_both_strata": len(symbols_by_stratum["alive"] & symbols_by_stratum["terminating"])
    }
    for stratum, symbols in symbols_by_stratum.items():
        matches[stratum] = {
            "series_symbols": len(symbols),
            "symbol_in_wikidata": sum(s in tickers for s in symbols),
            "symbol_with_end_dated_statement": sum(tickers.get(s, {}).get("ended", False) for s in symbols),
            "symbol_with_enwiki_article": sum(tickers.get(s, {}).get("article", False) for s in symbols),
        }
    return {"pageviews": pageviews, "wikidata_exchanges": exchanges, "intrader_symbol_match": matches}


def _db_inputs(conn: psycopg.Connection[Any]) -> tuple[dict[date, tuple[str, ...]], dict[str, set[str]]]:
    stored = {
        d: tuple(str(v) for v in rest)
        for d, *rest in conn.execute(
            "SELECT d.bar_date, d.open, d.high, d.low, d.close FROM research_price_daily d "
            "JOIN research_price_series s USING (series_id) WHERE s.vendor = 'cboe' AND s.vendor_symbol = 'VIX'"
        )
    }
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset(validated))
    symbol_of = {
        int(sid): str(sym)
        for sid, sym in conn.execute(
            "SELECT series_id, vendor_symbol FROM research_price_series WHERE vendor = %(v)s",
            {"v": SURVIVORSHIP_FREE_VENDOR},
        )
    }
    strata: dict[str, set[str]] = {"alive": set(), "terminating": set()}
    for s in selection.admitted:
        strata["alive" if s.termination is None else "terminating"].add(unify_symbol(symbol_of[s.series_id]))
    return stored, strata


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)
    client = _Client(os.environ.get("PROBE_UA") or settings.sec_user_agent, args.cache)

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        stored_vix, strata = _db_inputs(conn)

    probe = {
        "probe_version": PROBE_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        **_git_provenance(),
        "cboe_indices": _cboe_indices(client, stored_vix),
        "cboe_putcall": _cboe_putcall(client, None),
        "fred": _fred(client),
        "cot": _cot(client),
        "wikipedia": _wikipedia(client, strata),
    }
    body = json.dumps(probe, indent=1, sort_keys=True, default=str) + "\n"
    args.out.write_text(body)
    print(f"wrote {args.out} sha256 {hashlib.sha256(body.encode()).hexdigest()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
