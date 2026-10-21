"""#3942: measure /summary key_stats against instrument_valuation over the full population.

Read-only. For every instrument with a primary SEC CIK, rebuilds the key_stats ratios the summary
endpoint published before #3942 (``price / snapshot eps|book_value``, the latest ``financial_periods``
row's net income over its equity / assets, ``snapshot.debt`` over that equity, the dividend
summary's ``ttm_yield_pct``, with P/E, P/B and the yield nulled for an FPI ADR/ADS without a
curated ratio as the endpoint did, read here as the view's ``market_cap_reason = 'fpi_adr'``, which
resolves dual-class, then ADS ratio, then FPI in ``resolve_market_cap_basis``'s order) and compares
each with the same-named ``instrument_valuation`` column.

    PYTHONPATH=. uv run python -m scripts.measure_3942_key_stats_divergence
"""

from __future__ import annotations

from decimal import Decimal

import psycopg

from app.config import settings

_SQL = """
WITH cik AS (
    SELECT DISTINCT instrument_id FROM external_identifiers
    WHERE provider = 'sec' AND identifier_type = 'cik' AND is_primary = TRUE
),
q AS (
    SELECT instrument_id,
           COALESCE(NULLIF(GREATEST(last, 0), 0),
                    CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END) AS price
    FROM quotes
)
SELECT c.instrument_id,
       q.price, fs.eps, fs.book_value, fs.debt,
       fp.net_income, fp.shareholders_equity, fp.total_assets,
       ids.ttm_yield_pct,
       v.pe_ratio, v.pb_ratio, v.roe, v.roa, v.debt_equity_ratio, v.dividend_yield,
       v.market_cap_reason = 'fpi_adr' AS fpi_suppressed
FROM cik c
LEFT JOIN q USING (instrument_id)
LEFT JOIN LATERAL (
    SELECT eps, book_value, debt FROM fundamentals_snapshot s
    WHERE s.instrument_id = c.instrument_id ORDER BY as_of_date DESC LIMIT 1
) fs ON TRUE
LEFT JOIN LATERAL (
    SELECT net_income, shareholders_equity, total_assets FROM financial_periods p
    WHERE p.instrument_id = c.instrument_id AND superseded_at IS NULL
      AND period_type IN ('Q1','Q2','Q3','Q4','FY')
    ORDER BY period_end_date DESC LIMIT 1
) fp ON TRUE
LEFT JOIN instrument_dividend_summary ids USING (instrument_id)
LEFT JOIN instrument_valuation v USING (instrument_id)
"""


def _div(n: Decimal | None, d: Decimal | None) -> Decimal | None:
    if n is None or d is None or d == 0:
        return None
    return n / d


def _cmp(old: Decimal | None, new: Decimal | None) -> str:
    if old is None and new is None:
        return "both_null"
    if old is None:
        return "view_only"
    if new is None:
        return "old_only"
    if new == 0:
        return "differ" if old != 0 else "agree"
    return "agree" if abs(old / new - 1) <= Decimal("0.01") else "differ"


def main() -> None:
    fields = ("pe_ratio", "pb_ratio", "roe", "roa", "debt_to_equity", "dividend_yield")
    tally: dict[str, dict[str, int]] = {f: {} for f in fields}
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SQL).fetchall()
    for r in rows:
        (_, price, eps, bv, debt, ni, eq, ta, ttm_yield, v_pe, v_pb, v_roe, v_roa, v_de, v_dy, fpi) = r
        old = {
            "pe_ratio": _div(price, eps),
            "pb_ratio": _div(price, bv),
            "roe": _div(ni, eq),
            "roa": _div(ni, ta),
            "debt_to_equity": _div(debt, eq),
            "dividend_yield": ttm_yield,
        }
        if fpi:
            old["pe_ratio"] = old["pb_ratio"] = old["dividend_yield"] = None
        new = {
            "pe_ratio": v_pe,
            "pb_ratio": v_pb,
            "roe": v_roe,
            "roa": v_roa,
            "debt_to_equity": v_de,
            "dividend_yield": v_dy,
        }
        for f in fields:
            k = _cmp(old[f], new[f])
            tally[f][k] = tally[f].get(k, 0) + 1
    print(f"instruments with a primary SEC CIK: {len(rows)}")
    for f in fields:
        t = tally[f]
        print(f, {k: t.get(k, 0) for k in ("agree", "differ", "view_only", "old_only", "both_null")})


if __name__ == "__main__":
    main()
