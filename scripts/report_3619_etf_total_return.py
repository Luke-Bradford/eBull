"""#3619 slice 2b acceptance matrix: the ETF total-return panel on every #3620 / #3609 ETF.

Per symbol: the verdict, the identity gate on the overlap (paired months, median |Intrader − source|), the
months covered and the gaps inside them, and the distribution check slice 1 ran, restricted to
2022-01..2024-08 where stock dividend capture degraded: Intrader ``adj_close`` and the price-only arm,
each against the symbol's N-PORT return (annualised mean difference, pp). If ``adj_close`` still carries
distributions there, its difference sits near zero and the price-only arm lags by the yield.

Then the figures behind the spec's rules: Intrader dividend stamps per year on four distributing ETFs, the
N-PORT resolution census over every class-month in the cached data sets (slice 1's cache, blank cells
included, which the table does not store), and the wrong-fund identity control.

    PYTHONPATH=. uv run python -m scripts.report_3619_etf_total_return

Read-only.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

import psycopg

from app.config import settings
from app.services.etf_total_return_reader import (
    ETORO_SOURCE,
    NPORT_IDENTITY_MAX_MEDIAN,
    NPORT_SOURCE,
    NportFiling,
    identity_gate,
    load_etf_total_return_panel,
    resolve_nport_months,
)
from app.services.total_return_reader import INTRADER_VENDOR, SWITCH_MONTH, load_month_ends, month_of
from app.services.total_return_reader import monthly_returns as period_returns
from scripts.report_3619_total_return_fidelity import (
    CACHE_DIR,
    _nport_date,
    _tsv,
    month_end_levels,
    monthly_returns,
    nport_months,
    split_adjusted_closes,
    tracking,
)

#: #3620's universe (its issue body) plus the remaining sector SPDRs and #3609 step 0's broad-market legs.
SYMBOLS = (
    "SPY QQQ IWM EFA EEM VGK EWJ XLK XLF XLE XLV XLI XLY XLP XLU XLB XLRE XLC "
    "TLT IEF SHY AGG LQD HYG TIP GLD IAU SLV DBC USO VNQ VTI BND"
).split()

_DEGRADED_WINDOW = ((2022, 1), (2024, 8))

#: (Intrader symbol, a neighbouring fund's N-PORT): how far apart a DIFFERENT fund sits on the identity statistic.
CONTROL_PAIRS = (
    ("EFA", "VGK"), ("VGK", "EFA"), ("EEM", "EFA"), ("EWJ", "EFA"), ("SPY", "VTI"), ("SPY", "IWM"),
    ("TLT", "IEF"), ("IEF", "AGG"), ("LQD", "AGG"), ("HYG", "LQD"), ("XLK", "QQQM"), ("XLY", "XLK"),
    ("VNQ", "XLRE"), ("AGG", "BND"), ("SHY", "IEF"), ("IWM", "VTI"),
)  # fmt: skip

_STAMPS_SQL = """
SELECT s.vendor_symbol, extract(year FROM d.bar_date)::int, count(*) FILTER (WHERE d.dividend > 0)
FROM research_price_daily d JOIN research_price_series s USING (series_id)
WHERE s.vendor = %(vendor)s AND s.vendor_symbol = ANY(%(symbols)s::text[]) AND d.bar_date >= '2019-01-01'
GROUP BY 1, 2 ORDER BY 1, 2
"""


def _ym(d: object) -> str:
    return f"{d:%Y-%m}" if d is not None else "—"  # type: ignore[str-format]


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        panel = load_etf_total_return_panel(conn, SYMBOLS, include_price_return=True)
        print(f"version {panel.version}; N-PORT {panel.nport_snapshot}")
        print(
            "symbol | verdict | paired | median |diff| bp | first | last Intrader | last extension | source | "
            "gaps | 2022-24 months | adj TD pp | price-only TD pp"
        )
        for symbol in SYMBOLS:
            rows = [r for r in panel.rows if r.symbol == symbol]
            months = sorted(r.month for r in rows)
            gaps = 0
            for a, b in zip(months, months[1:], strict=False):
                gaps += (b.year * 12 + b.month) - (a.year * 12 + a.month) - 1
            ext = [r for r in rows if r.source in (NPORT_SOURCE, ETORO_SOURCE)]
            last_intrader = max((r.month for r in rows if r.source == INTRADER_VENDOR), default=None)
            check = panel.identity.get(symbol)
            median = (
                f"{check.median_abs_diff * 1e4:.1f}" if check is not None and check.median_abs_diff is not None else "—"
            )
            nport = {
                (r.month.year, r.month.month): r.total_return
                for r in panel.rows
                if r.symbol == symbol and r.source == NPORT_SOURCE
            }
            fidelity = "— | — | —"
            if check is not None and ext and ext[0].source == NPORT_SOURCE:
                fidelity = _degraded_window_fidelity(conn, rows[0].source_key, nport)
            print(
                f"{symbol} | {panel.verdicts[symbol]} | {check.paired_months if check else '—'} | {median} | "
                f"{_ym(months[0] if months else None)} | {_ym(last_intrader)} | "
                f"{_ym(ext[-1].month if ext else None)} | {ext[0].source if ext else '—'} | {gaps} | {fidelity}"
            )
        dividend_stamps(conn)
        wrong_fund_control(conn)
    resolution_census()


def _degraded_window_fidelity(conn: psycopg.Connection, series_id: str, extension: dict[tuple[int, int], float]) -> str:
    """Slice 1's arms on 2022-01..2024-08 only, against the reader's own resolved N-PORT months."""
    bars = conn.execute(
        "SELECT bar_date, close::float8, adj_close::float8, COALESCE(split_factor, 1)::float8"
        " FROM research_price_daily WHERE series_id = %s AND close > 0 AND adj_close > 0 ORDER BY bar_date",
        (int(series_id),),
    ).fetchall()
    adj = monthly_returns(month_end_levels((d, a) for d, _c, a, _s in bars))
    price = monthly_returns(month_end_levels(split_adjusted_closes([(d, c, s) for d, c, _a, s in bars])))
    lo, hi = _DEGRADED_WINDOW
    t = tracking(adj, price, {m: v for m, v in extension.items() if lo <= m <= hi})
    if t is None:
        return "0 | — | —"
    return f"{t.months} | {t.adj_td_pp:.2f} | {t.price_td_pp:.2f}"


def dividend_stamps(conn: psycopg.Connection) -> None:
    print("\n## Intrader dividend stamps per year")
    rows = conn.execute(_STAMPS_SQL, {"vendor": INTRADER_VENDOR, "symbols": ["HYG", "AGG", "LQD", "VNQ"]}).fetchall()
    by_symbol: dict[str, list[str]] = defaultdict(list)
    for symbol, year, count in rows:
        by_symbol[symbol].append(f"{year}:{count}")
    for symbol, cells in sorted(by_symbol.items()):
        print(f"{symbol} | {' '.join(cells)}")


def resolution_census() -> None:
    """Every class-month in the cached data sets, blank cells included (the table stores none)."""
    cells: dict[tuple[str, tuple[int, int]], list[tuple[date, str, str | None, str]]] = defaultdict(list)
    for folder in sorted(CACHE_DIR.iterdir()):
        subs = {s["ACCESSION_NUMBER"]: s for s in _tsv(folder / "SUBMISSION.tsv")}
        for r in _tsv(folder / "MONTHLY_TOTAL_RETURN.tsv"):
            if not r["CLASS_ID"]:
                continue
            sub = subs[r["ACCESSION_NUMBER"]]
            filed = _nport_date(sub["FILING_DATE"])
            for position, month in enumerate(nport_months(_nport_date(sub["REPORT_DATE"])), start=1):
                value = r[f"MONTHLY_TOTAL_RETURN{position}"].strip() or None
                cells[(r["CLASS_ID"], month)].append((filed, r["ACCESSION_NUMBER"], value, sub["SUB_TYPE"]))
    multi = blank_wins = blank_wins_amendment = same_day_conflict = 0
    for filings in cells.values():
        multi += len({f[1] for f in filings}) > 1
        latest = max(f[0] for f in filings)
        winners = [f for f in filings if f[0] == latest]
        values = {f[2] for f in winners}
        same_day_conflict += len({f[1] for f in winners}) > 1 and len(values) > 1
        if None in values and any(f[2] is not None for f in filings if f[0] < latest):
            blank_wins += 1
            blank_wins_amendment += any(f[3] == "NPORT-P/A" for f in winners)
    print(f"\n## N-PORT resolution census ({CACHE_DIR})")
    print(f"class-months {len(cells)}; reported by >1 filing {multi}; latest filing blank over an earlier value")
    print(
        f"{blank_wins} (amendments {blank_wins_amendment}); "
        f"two accessions, two values on the latest date {same_day_conflict}"
    )


def wrong_fund_control(conn: psycopg.Connection) -> None:
    print("\n## Wrong-fund control: Intrader adj_close vs a neighbouring fund's N-PORT, before the switch")
    for symbol, other in CONTROL_PAIRS:
        series = conn.execute(
            "SELECT series_id FROM research_price_series WHERE vendor = %s AND vendor_symbol = %s",
            (INTRADER_VENDOR, symbol),
        ).fetchone()
        cls = conn.execute("SELECT class_id FROM cik_refresh_mf_directory WHERE symbol = %s", (other,)).fetchone()
        if series is None or cls is None:
            print(f"{symbol} vs {other} | missing")
            continue
        filings = [
            NportFiling(month_of(m), pct, filed, accession)
            for m, pct, filed, accession in conn.execute(
                "SELECT month, return_pct, filing_date, accession_number FROM sec_nport_monthly_returns"
                " WHERE class_id = %s",
                (cls[0],),
            ).fetchall()
        ]
        ends = load_month_ends(conn, [series[0]]).get(series[0], {})
        before = {
            m: r for m, r in period_returns(ends, field="adj_close", through=SWITCH_MONTH).items() if m < SWITCH_MONTH
        }
        check = identity_gate(
            before, {m: v.value for m, v in resolve_nport_months(filings).items()}, max_median=NPORT_IDENTITY_MAX_MEDIAN
        )
        median = f"{check.median_abs_diff * 1e4:.1f}" if check.median_abs_diff is not None else "—"
        print(f"{symbol} vs {other} | paired {check.paired_months} | median {median}bp | passes {check.passed}")


if __name__ == "__main__":
    main()
