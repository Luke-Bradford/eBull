"""#3619 slice 1: does the research corpus's ``adj_close`` carry total return? Measured against non-Yahoo references.

Both price archives we hold are Yahoo derivatives, so agreement between them is circular. This report checks the
Intrader ``adj_close`` against two references that are not Yahoo, plus one internal identity:

1. **ETFs vs the fund's own reported total return.** Form N-PORT Item B.5.a requires each registered fund to report
   its monthly total return for each of the three months ending the report date, computed under Form N-1A
   Item 26(b)(1): NAV-based, distributions reinvested. SEC publishes these in the quarterly N-PORT data sets
   (``MONTHLY_TOTAL_RETURN.tsv``). Reported returns are NAV-based and the corpus is a market close, so premium or
   discount moves add noise but no drift. The control arm is the price-only return: if ``adj_close`` carries
   distributions, its tracking difference should sit near zero while the price-only arm lags by the distribution
   yield. UITs (SPY, and QQQ before its 2025 conversion) do not file N-PORT; each is checked against a registered
   fund tracking the same index (IVV, QQQM) and labelled as a proxy. Commodity pools (GLD, IAU, SLV, USO, DBC) file
   no N-PORT and are not covered.
2. **Stocks vs SEC XBRL dividends.** Per-share dividends declared (``us-gaap:CommonStockDividendsPerShareDeclared``,
   ``financial_periods.dps_declared``) per fiscal quarter, against the payments implied by each vendor's own
   ``adj_close`` in the same window (PWB has no dividend stamps, so both arms are measured the same way). Full
   population of Intrader series linked to an instrument and reporting in USD, plus PWB, the candidate source past
   Intrader's end, on the instruments both carry. Declaration precedes the ex-date by weeks, so single quarters
   shift: ratios use each instrument's total over its quarters, and the by-year miss rate looks across the quarter
   and the one after. ``financial_periods`` restates comparatives in place, so a split can leave XBRL
   split-adjusted against nominal vendor amounts; that stratum is reported separately.
3. **Internal identity.** On every dividend bar (no split that day) of the series above, is the ``adj_close`` return
   closer to the total-return formula ``(close + dividend) / prev_close`` than to the price-only ``close /
   prev_close``? A threshold-free test that the stamped dividends are applied.

    PYTHONPATH=. uv run python -m scripts.report_3619_total_return_fidelity

Read-only against the DB. N-PORT tables are read from the cached bulk ZIPs under the app data dir when present,
otherwise fetched with HTTP range requests (only the two small tables, not the ~450 MB archive) and cached under
``var/research_corpus/nport_returns/``.
"""

from __future__ import annotations

import argparse
import csv
import io
import math
import statistics
import zipfile
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import httpx
import psycopg

from app.config import settings
from app.providers.sec_rate_gate_holder import get_sec_rate_gate
from app.security.master_key import resolve_data_dir

INTRADER_VENDOR = "icyDenev/Intrader"
PWB_VENDOR = "paperswithbacktest/Stocks-Daily-Price"
#: Intrader's last bar. Every comparison stops here; later months are the extension window this ticket fills.
INTRADER_END = date(2024, 9, 27)
NPORT_URL = "https://www.sec.gov/files/dera/data/form-n-port-data-sets/{q}_nport.zip"
#: First quarter SEC published an N-PORT data set for.
FIRST_NPORT_QUARTER = (2019, 4)
CACHE_DIR = Path("var/research_corpus/nport_returns")
NPORT_TABLES = ("MONTHLY_TOTAL_RETURN.tsv", "SUBMISSION.tsv")

#: corpus symbol -> the registered fund whose N-PORT return is the reference. Same symbol unless a proxy.
ETF_REFERENCES: dict[str, str] = {
    **{s: s for s in ("IWM EFA EEM XLK XLF XLE XLRE XLC TLT IEF SHY AGG LQD HYG TIP VNQ VGK EWJ VTI BND".split())},
    "SPY": "IVV",
    "QQQ": "QQQM",
}

Month = tuple[int, int]


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def add_months(m: Month, k: int) -> Month:
    n = m[0] * 12 + (m[1] - 1) + k
    return (n // 12, n % 12 + 1)


def month_end_levels(bars: Iterable[tuple[date, float]]) -> dict[Month, float]:
    """Last level in each calendar month, from bars in ascending date order."""
    out: dict[Month, float] = {}
    for d, level in bars:
        out[(d.year, d.month)] = level
    return out


def monthly_returns(levels: dict[Month, float]) -> dict[Month, float]:
    """Simple return for each month whose previous calendar month also has a level."""
    return {
        m: levels[m] / levels[prev] - 1.0 for m in levels if (prev := add_months(m, -1)) in levels and levels[prev] > 0
    }


def without_month(returns: dict[Month, float], pick: Callable[[Iterable[Month]], Month]) -> dict[Month, float]:
    """Drop one edge month that may be partial.

    The vendor's final month (``max``): a frozen archive stops mid-month (Intrader's last bar is 2024-09-27, a
    Friday, before the 30th). The reference's first month (``min``): Form N-1A Item 26(b)(1) reports a fund's
    first period from inception, so a fund launched mid-month reports a partial month (QQQM, 2020-10-13). For a
    fund already running when N-PORT began, this drops one valid month, which costs nothing.
    """
    if not returns:
        return returns
    edge = pick(returns)
    return {m: v for m, v in returns.items() if m != edge}


def split_adjusted_closes(bars: Sequence[tuple[date, float, float]]) -> list[tuple[date, float]]:
    """Raw closes rescaled into the latest bar's share basis, from ``(date, close, split_factor)`` ascending.

    ``split_factor`` is new shares per old share on the split bar (4 for a 4:1), so every earlier close divides
    by it. Dividends are not touched: this is the price-only control arm.
    """
    factor = 1.0
    out: list[tuple[date, float]] = []
    for d, close, split in reversed(bars):
        out.append((d, close / factor))
        factor *= split
    out.reverse()
    return out


def nport_months(report_date: date) -> tuple[Month, Month, Month]:
    """Months that MONTHLY_TOTAL_RETURN1..3 describe: the three ending at the report date (Item B.5.a)."""
    end = (report_date.year, report_date.month)
    return (add_months(end, -2), add_months(end, -1), end)


@dataclass(frozen=True)
class NportReturn:
    class_id: str
    month: Month
    return_pct: float
    filing_date: date
    accession: str


def latest_per_month(rows: Iterable[NportReturn]) -> tuple[dict[tuple[str, Month], float], int]:
    """One return per (class, month): the latest filing wins. Also counts months where filings disagree."""
    best: dict[tuple[str, Month], NportReturn] = {}
    seen: dict[tuple[str, Month], set[float]] = {}
    for r in rows:
        key = (r.class_id, r.month)
        seen.setdefault(key, set()).add(round(r.return_pct, 6))
        cur = best.get(key)
        if cur is None or (r.filing_date, r.accession) > (cur.filing_date, cur.accession):
            best[key] = r
    disagreements = sum(1 for v in seen.values() if len(v) > 1)
    return {k: v.return_pct / 100.0 for k, v in best.items()}, disagreements


@dataclass(frozen=True)
class Tracking:
    months: int
    correlation: float | None
    #: Annualised mean monthly difference (arm − reference), in percentage points.
    adj_td_pp: float
    price_td_pp: float
    reference_annual_pct: float


def tracking(adj: dict[Month, float], price: dict[Month, float], ref: dict[Month, float]) -> Tracking | None:
    months = sorted(set(adj) & set(price) & set(ref))
    if len(months) < 2:
        return None
    a = [adj[m] for m in months]
    p = [price[m] for m in months]
    r = [ref[m] for m in months]
    try:
        corr: float | None = statistics.correlation(a, r)
    except statistics.StatisticsError:
        corr = None
    return Tracking(
        months=len(months),
        correlation=corr,
        adj_td_pp=12 * 100 * statistics.fmean(x - y for x, y in zip(a, r, strict=True)),
        price_td_pp=12 * 100 * statistics.fmean(x - y for x, y in zip(p, r, strict=True)),
        reference_annual_pct=12 * 100 * statistics.fmean(r),
    )


def quantiles(values: Sequence[float]) -> list[float]:
    """p5, p25, p50, p75, p95 (inclusive method)."""
    if len(values) < 2:
        return list(values) * 5 if values else []
    cuts = statistics.quantiles(values, n=20, method="inclusive")
    return [cuts[0], cuts[4], cuts[9], cuts[14], cuts[18]]


# ---------------------------------------------------------------------------
# N-PORT data sets
# ---------------------------------------------------------------------------


class _RangeFile(io.RawIOBase):
    """A seekable read-only view of a remote file over HTTP Range, so ``zipfile`` reads only what it needs."""

    def __init__(self, client: httpx.Client, url: str) -> None:
        self._client = client
        self._url = url
        self._pos = 0
        first = self._get(0, 0)
        self._size = int(first.headers["content-range"].rsplit("/", 1)[1])

    def _get(self, start: int, end: int) -> httpx.Response:
        get_sec_rate_gate().acquire()
        resp = self._client.get(self._url, headers={"Range": f"bytes={start}-{end}"})
        resp.raise_for_status()
        if resp.status_code != 206:
            raise RuntimeError(f"{self._url}: expected 206 Partial Content, got {resp.status_code}")
        return resp

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        self._pos = base + offset
        return self._pos

    def readinto(self, buffer: memoryview) -> int:  # type: ignore[override]
        if self._pos >= self._size or len(buffer) == 0:
            return 0
        end = min(self._pos + len(buffer), self._size) - 1
        data = self._get(self._pos, end).content
        buffer[: len(data)] = data
        self._pos += len(data)
        return len(data)


def _quarters(today: date) -> list[str]:
    out = []
    y, q = FIRST_NPORT_QUARTER
    while (y, q) <= (today.year, (today.month - 1) // 3 + 1):
        out.append(f"{y}q{q}")
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


def _cached_tables(quarter: str, client: httpx.Client) -> Path | None:
    """Directory holding this quarter's two tables, extracting or fetching them on first use."""
    target = CACHE_DIR / quarter
    if all((target / t).exists() for t in NPORT_TABLES):
        return target
    local = resolve_data_dir() / "sec" / "bulk" / f"nport_{quarter}.zip"
    try:
        source: io.IOBase = (
            local.open("rb")
            if local.exists()
            else io.BufferedReader(_RangeFile(client, NPORT_URL.format(q=quarter)), buffer_size=1 << 20)
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (403, 404):
            return None  # not published yet
        raise
    target.mkdir(parents=True, exist_ok=True)
    with source, zipfile.ZipFile(source) as zf:  # type: ignore[arg-type]
        for table in NPORT_TABLES:
            (target / table).write_bytes(zf.read(table))
    return target


def _tsv(path: Path) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        yield from csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)


def _nport_date(raw: str) -> date:
    return datetime.strptime(raw, "%d-%b-%Y").date()


def load_nport_returns(class_ids: set[str], today: date) -> tuple[dict[tuple[str, Month], float], int, list[str]]:
    rows: list[NportReturn] = []
    quarters: list[str] = []
    with httpx.Client(headers={"User-Agent": settings.sec_user_agent}, timeout=120, follow_redirects=True) as client:
        for quarter in _quarters(today):
            folder = _cached_tables(quarter, client)
            if folder is None:
                continue
            quarters.append(quarter)
            subs = {
                s["ACCESSION_NUMBER"]: s
                for s in _tsv(folder / "SUBMISSION.tsv")
                if s["SUB_TYPE"] in ("NPORT-P", "NPORT-P/A") and s["REPORT_DATE"]
            }
            for r in _tsv(folder / "MONTHLY_TOTAL_RETURN.tsv"):
                sub = subs.get(r["ACCESSION_NUMBER"])
                if r["CLASS_ID"] not in class_ids or sub is None:
                    continue
                months = nport_months(_nport_date(sub["REPORT_DATE"]))
                for month, col in zip(months, ("1", "2", "3"), strict=True):
                    value = r[f"MONTHLY_TOTAL_RETURN{col}"]
                    if value:
                        rows.append(
                            NportReturn(
                                class_id=r["CLASS_ID"],
                                month=month,
                                return_pct=float(value),
                                filing_date=_nport_date(sub["FILING_DATE"]),
                                accession=r["ACCESSION_NUMBER"],
                            )
                        )
    returns, disagreements = latest_per_month(rows)
    return returns, disagreements, quarters


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


def _fmt(x: float | None, nd: int = 2) -> str:
    return "—" if x is None or math.isnan(x) else f"{x:.{nd}f}"


def etf_report(conn: psycopg.Connection, today: date) -> list[int]:
    refs = sorted(set(ETF_REFERENCES.values()))
    class_by_symbol = dict(
        conn.execute("SELECT symbol, class_id FROM cik_refresh_mf_directory WHERE symbol = ANY(%s)", (refs,)).fetchall()
    )
    missing = [s for s in refs if s not in class_by_symbol]
    returns, disagreements, quarters = load_nport_returns(set(class_by_symbol.values()), today)
    print(f"\n## 1. ETFs: Intrader adj_close vs N-PORT Item B.5.a monthly total return (to {INTRADER_END})")
    print(f"N-PORT data sets read: {quarters[0]}..{quarters[-1]} ({len(quarters)}); classes missing from the SEC")
    print(f"fund directory: {missing or 'none'}; (class, month) cells where filings disagree: {disagreements}")
    print("TD = annualised mean monthly (arm − reference), percentage points. Reference annual = mean × 12.")
    print(
        "symbol | reference | months | corr | adj TD pp | price-only TD pp | reference annual % | carried"
        " | N-PORT last month"
    )
    series_ids: list[int] = []
    for symbol, ref in ETF_REFERENCES.items():
        row = conn.execute(
            "SELECT series_id FROM research_price_series WHERE vendor = %s AND vendor_symbol = %s",
            (INTRADER_VENDOR, symbol),
        ).fetchone()
        class_id = class_by_symbol.get(ref)
        if row is None or class_id is None:
            print(f"{symbol} | {ref} | no corpus series or no class id")
            continue
        series_ids.append(row[0])
        bars = conn.execute(
            "SELECT bar_date, close::float8, adj_close::float8, COALESCE(split_factor, 1)::float8"
            " FROM research_price_daily WHERE series_id = %s AND close > 0 AND adj_close > 0 ORDER BY bar_date",
            (row[0],),
        ).fetchall()
        adj = without_month(monthly_returns(month_end_levels((d, a) for d, _c, a, _s in bars)), max)
        price = without_month(
            monthly_returns(month_end_levels(split_adjusted_closes([(d, c, s) for d, c, _a, s in bars]))), max
        )
        ref_returns = without_month({m: v for (cid, m), v in returns.items() if cid == class_id}, min)
        last = "{}-{:02d}".format(*max(ref_returns)) if ref_returns else None
        t = tracking(adj, price, ref_returns)
        label = ref if ref == symbol else f"{ref} (proxy)"
        if t is None:
            print(f"{symbol} | {label} | 0 | no overlapping months | | | | | {last}")
            continue
        carried = abs(t.adj_td_pp) < abs(t.price_td_pp)
        print(
            f"{symbol} | {label} | {t.months} | {_fmt(t.correlation, 4)} | {_fmt(t.adj_td_pp)} | "
            f"{_fmt(t.price_td_pp)} | {_fmt(t.reference_annual_pct)} | {carried} | {last}"
        )
    return series_ids


#: An implied dividend below this fraction of the close is treated as rounding, not a payment. PWB stores
#: float32 prices (31.329999923706055), relative precision 2**-24 ≈ 6e-8; the implied amount combines three
#: such values, so its noise stays well under 1e-6 of the close. 1e-5 keeps a tenfold margin, and a payment
#: smaller than 0.001% of the price does not move total return.
IMPLIED_DIVIDEND_FLOOR = 1e-5

_STOCK_SQL = """
WITH s AS (
    SELECT series_id, vendor, instrument_id, first_bar,
           CASE WHEN vendor = %(intrader)s THEN LEAST(last_bar, %(end)s) ELSE last_bar END AS last_bar
      FROM research_price_series
     WHERE vendor = ANY(%(vendors)s) AND instrument_id IS NOT NULL
       AND instrument_id IN (SELECT instrument_id FROM research_price_series WHERE vendor = %(intrader)s)
),
b AS (
    SELECT d.series_id, d.bar_date, COALESCE(d.split_factor, 1) AS split,
           d.close::float8 AS c, d.adj_close::float8 AS a,
           lag(d.close::float8) OVER w AS pc, lag(d.adj_close::float8) OVER w AS pa
      FROM research_price_daily d
     WHERE d.series_id IN (SELECT series_id FROM s)
    WINDOW w AS (PARTITION BY d.series_id ORDER BY d.bar_date)
),
dv AS (
    SELECT series_id, bar_date, pc * a / pa - c AS amount
      FROM b
     WHERE split = 1 AND pc > 0 AND pa > 0 AND c > 0 AND pc * a / pa - c > %(floor)s * c
),
splits AS (
    SELECT s.instrument_id, d.bar_date
      FROM research_price_daily d JOIN s USING (series_id)
     WHERE s.vendor = %(intrader)s AND d.split_factor <> 1
),
q AS (
    -- One row per instrument and period end: fiscal-year-rekey duplicates (two quarterly period_type rows
    -- sharing a period_end_date) would otherwise join the same payments twice. Latest-filed wins, the same
    -- tiebreak as fundamentals' _SNAPSHOT_WRITE_THROUGH_SQL.
    SELECT DISTINCT ON (fp.instrument_id, fp.period_end_date)
           fp.instrument_id, fp.period_start_date, fp.period_end_date, fp.dps_declared::float8 AS dps
      FROM financial_periods fp
     WHERE fp.superseded_at IS NULL
       AND fp.period_type IN ('Q1', 'Q2', 'Q3', 'Q4')
       AND fp.reported_currency = 'USD'
       AND fp.instrument_id IN (SELECT instrument_id FROM s)
     ORDER BY fp.instrument_id, fp.period_end_date, fp.filed_date DESC NULLS LAST
),
p AS (
    SELECT s.vendor, s.series_id, s.instrument_id, q.period_start_date, q.period_end_date, q.dps
      FROM q
      JOIN s ON s.instrument_id = q.instrument_id
     WHERE q.dps IS NOT NULL
       AND q.period_start_date >= s.first_bar
       AND q.period_end_date + interval '3 months' <= s.last_bar
)
SELECT p.vendor, p.instrument_id, p.period_end_date, p.dps,
       COALESCE(sum(dv.amount) FILTER (WHERE dv.bar_date <= p.period_end_date), 0) AS quarter_sum,
       COALESCE(sum(dv.amount), 0) AS wide_sum,
       EXISTS (SELECT 1 FROM splits x WHERE x.instrument_id = p.instrument_id
                  AND x.bar_date BETWEEN p.period_start_date AND p.period_end_date + interval '3 months') AS split
  FROM p
  LEFT JOIN dv ON dv.series_id = p.series_id
              AND dv.bar_date BETWEEN p.period_start_date AND p.period_end_date + interval '3 months'
 GROUP BY p.vendor, p.series_id, p.instrument_id, p.period_start_date, p.period_end_date, p.dps
"""


@dataclass(frozen=True)
class DividendCell:
    """One fiscal quarter of one instrument on one vendor: XBRL dividend declared vs the vendor's implied payments.

    ``quarter_sum`` covers the quarter itself; ``wide_sum`` adds the following quarter, because a dividend
    declared late in a quarter goes ex in the next one.
    """

    vendor: str
    instrument_id: int
    period_end: date
    dps: float
    quarter_sum: float
    wide_sum: float
    split: bool


def is_miss(cell: DividendCell) -> bool:
    """A declared dividend the vendor shows no payment for, across the quarter and the one after.

    Half the declared amount separates a dropped payment (~0) from timing or rounding (≥ ~1x): the wide window
    already absorbs declaration-to-ex lag, so a captured payment lands at one times the declared amount or more.
    """
    return cell.dps > 0 and cell.wide_sum < 0.5 * cell.dps


def instrument_ratios(cells: Iterable[DividendCell]) -> dict[tuple[int, bool], float]:
    """Per instrument: vendor quarter sums over XBRL sums, keyed by (instrument, any split in window)."""
    totals: dict[int, list[float]] = {}
    split: dict[int, bool] = {}
    for c in cells:
        t = totals.setdefault(c.instrument_id, [0.0, 0.0])
        t[0] += c.quarter_sum
        t[1] += c.dps
        split[c.instrument_id] = split.get(c.instrument_id, False) or c.split
    return {(i, split[i]): v / x for i, (v, x) in totals.items() if x > 0 and v > 0}


def stock_report(conn: psycopg.Connection) -> list[int]:
    vendors = [INTRADER_VENDOR, PWB_VENDOR]
    cells = [
        DividendCell(*row)
        for row in conn.execute(
            _STOCK_SQL,
            {"end": INTRADER_END, "intrader": INTRADER_VENDOR, "vendors": vendors, "floor": IMPLIED_DIVIDEND_FLOOR},
        ).fetchall()
    ]
    print("\n## 2. Stocks: implied vendor dividends vs SEC XBRL dps_declared (instruments with an Intrader series)")
    print("implied dividend per bar = prev_close * adj_t / adj_{t-1} - close; split days excluded.")
    print(f"Intrader capped at {INTRADER_END}; PWB runs to its own last bar.")
    both = {c.instrument_id for c in cells if c.vendor == PWB_VENDOR} & {
        c.instrument_id for c in cells if c.vendor == INTRADER_VENDOR
    }
    arms = (
        ("Intrader, all", [c for c in cells if c.vendor == INTRADER_VENDOR]),
        ("Intrader, matched", [c for c in cells if c.vendor == INTRADER_VENDOR and c.instrument_id in both]),
        ("PWB, matched", [c for c in cells if c.vendor == PWB_VENDOR and c.instrument_id in both]),
    )
    print(f"matched = instruments with cells on both vendors ({len(both)}); 'all' keeps Intrader-only names too.")
    print("ratio = vendor / XBRL over the instrument's covered quarters | arm | split | n | p5 | p25 | p50 | p75 | p95")
    for label, arm in arms:
        ratios = instrument_ratios(arm)
        for has_split in (False, True):
            values = [r for (_i, sp), r in ratios.items() if sp == has_split]
            print(f"{label} | {has_split} | {len(values)} | " + " | ".join(_fmt(x, 3) for x in quantiles(values)))
    print("\nmisses by fiscal-quarter-end year (cells with XBRL declared > 0):")
    print("miss = the vendor's payments over that quarter and the next < half the declared amount")
    print("year | " + " | ".join(f"{label} cells | miss %" for label, _arm in arms))
    for year in sorted({c.period_end.year for c in cells}):
        out = []
        for _label, arm in arms:
            sub = [c for c in arm if c.period_end.year == year and c.dps > 0]
            out.append(f"{len(sub)} | {_fmt(100 * sum(map(is_miss, sub)) / len(sub), 1) if sub else '—'}")
        print(f"{year} | " + " | ".join(out))
    return sorted(
        r[0]
        for r in conn.execute(
            "SELECT series_id FROM research_price_series WHERE vendor = %s AND instrument_id = ANY(%s)",
            (INTRADER_VENDOR, sorted({c.instrument_id for c in cells})),
        ).fetchall()
    )


_IDENTITY_SQL = """
WITH b AS (
    SELECT d.dividend::float8 AS dividend, COALESCE(d.split_factor, 1) AS split,
           d.close::float8 AS close, d.adj_close::float8 AS adj,
           lag(d.close::float8) OVER w AS pc, lag(d.adj_close::float8) OVER w AS pa
      FROM research_price_daily d
     WHERE d.series_id = ANY(%(ids)s)
    WINDOW w AS (PARTITION BY d.series_id ORDER BY d.bar_date)
)
SELECT count(*) FILTER (WHERE split <> 1) AS split_day,
       count(*) FILTER (WHERE split = 1) AS tested,
       count(*) FILTER (WHERE split = 1
                          AND abs(adj / pa - (close + dividend) / pc) < abs(adj / pa - close / pc)) AS carried
  FROM b
 WHERE dividend > 0 AND pc > 0 AND pa > 0 AND adj > 0
"""


def identity_report(conn: psycopg.Connection, series_ids: list[int]) -> None:
    split_day, tested, carried = conn.execute(_IDENTITY_SQL, {"ids": series_ids}).fetchone() or (0, 0, 0)
    print("\n## 3. Internal identity: is the adj_close return on each dividend bar closer to (close+div)/prev_close?")
    print(
        f"series: {len(series_ids)}; dividend bars tested: {tested}; carried: {carried}; not carried: "
        f"{tested - carried}; skipped (split the same day): {split_day}"
    )


def main() -> None:
    argparse.ArgumentParser(description="#3619 total-return fidelity report (read-only).").parse_args()
    today = date.today()
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        etf_ids = etf_report(conn, today)
        stock_ids = stock_report(conn)
        identity_report(conn, sorted(set(etf_ids) | set(stock_ids)))


if __name__ == "__main__":
    main()
