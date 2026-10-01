"""#2842 premise measurements for the ranking-pot spec (read-only).

    PYTHONPATH=. uv run python -m scripts.measure_2842_pot_premise [--n 25]

Applies spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §5.0 rules to the latest
``v1.5-balanced`` scores run at or before ``as_of`` and the current ``quotes`` snapshot, and prints:

1. the ranking universe R: tradable ``us_equity``, finite positive score, completeness present and not
   ``insufficient_data``, ``coverage.filings_status = 'analysable'``, overlaid market cap (fail closed) ≥ the
   20th percentile of the overlaid caps of every scored tradable NYSE name (Fama-French 2008 microcap cut);
2. the entry-feasible set F ⊆ R: the v1 trial's quote rule (``ai_trial_pack.is_eligible``), MAX valid (22
   closes on exactly the last 22 NYSE sessions) and ≤ the nearest-rank 90th percentile of valid MAX over
   R ∩ quote-eligible (Bali/Cakici/Whitelaw), stored ATR14 finite and ``0 < 3 × ATR14 < ask``;
3. the entrants under §5.1 (F-rank ≤ N and R-rank ≤ 2N): thesis share and age (latest thesis written at or
   before the run — a proxy: the score row does not record which thesis it used);
4. stored-score entrants on the first stored run of each calendar month, applying TODAY's R and F (labelled
   approximation: past quotes, caps and MAX are not reconstructable from current-row tables);
5. a power input: cross-sectional SD of ≈1-year (365–372 days) price returns over F, with the finite-population
   correction for an N-name draw without replacement, and a normal-tail power figure (an approximation).

Nothing is written. Every figure prints with its denominator. Facts read from current-row tables (``quotes``,
``coverage``, ``instrument_valuation``) are as of the read, not ``as_of``; the output is a dated snapshot and is
not reproducible after those tables move.
"""

from __future__ import annotations

import argparse
import math
import statistics
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.ai_trial_pack import ShortlistCandidate, is_eligible
from app.services.market_calendar import latest_completed_us_session, us_market_status
from app.services.scoring import _apply_market_cap_basis
from app.services.xbrl_derived_stats import resolve_market_cap_basis

NYSE_DESCRIPTION = "NYSE"
MAX_WINDOW = 21
ATR_MULTIPLE_STOP = 3
Z_ONE_SIDED_025 = 1.96


def _finite_positive(value: object) -> bool:
    if value is None:
        return False
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return d.is_finite() and d > 0


def _nearest_rank(values: list[Decimal], pct: int) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(1, -(-pct * len(ordered) // 100)) - 1]


def _last_sessions(last: date, count: int) -> list[date]:
    out: list[date] = []
    d = last
    while len(out) < count:
        if us_market_status(d) != "closed":
            out.append(d)
        d -= timedelta(days=1)
    return out


def _overlaid_cap(conn: psycopg.Connection[Any], instrument_id: int, cap: object) -> Decimal | None:
    """The scorer's #1664 overlay; any failure to resolve the basis fails closed (no cap)."""
    try:
        with conn.transaction():
            resolution = resolve_market_cap_basis(conn, instrument_id=instrument_id)
    except psycopg.Error:
        return None
    overlaid = _apply_market_cap_basis({"market_cap_live": cap, "fcf_ttm": None}, resolution)
    value = None if overlaid is None else overlaid["market_cap_live"]
    if not _finite_positive(value):
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=25)
    n = ap.parse_args().n
    if n <= 0:
        raise SystemExit("--n must be positive")
    as_of = datetime.now(UTC)
    last_session = latest_completed_us_session(as_of)
    max_sessions = _last_sessions(last_session, MAX_WINDOW + 1)
    with psycopg.connect(settings.database_url) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        nyse = cur.execute(
            "SELECT exchange_id FROM exchanges WHERE description = %(d)s", {"d": NYSE_DESCRIPTION}
        ).fetchall()
        if len(nyse) != 1:
            raise SystemExit(f"expected one {NYSE_DESCRIPTION} exchange row, found {len(nyse)}")
        nyse_id = nyse[0]["exchange_id"]
        run = cur.execute(
            "SELECT max(scored_at) AS t FROM scores WHERE model_version = 'v1.5-balanced' AND scored_at <= %(a)s",
            {"a": as_of},
        ).fetchone()
        if run is None or run["t"] is None:
            raise SystemExit("no v1.5-balanced run at or before as_of")
        scored_at = run["t"]
        rows = cur.execute(
            """
            SELECT i.instrument_id, i.symbol, i.exchange, q.bid, q.ask, q.quoted_at, s.total_score,
                   s.completeness_tier, c.filings_status, p.atr_14,
                   (SELECT max(t.created_at) FROM theses t
                     WHERE t.instrument_id = i.instrument_id AND t.created_at <= s.scored_at) AS thesis_at
              FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              JOIN scores s ON s.instrument_id = i.instrument_id
                           AND s.model_version = 'v1.5-balanced' AND s.scored_at = %(t)s
              LEFT JOIN coverage c ON c.instrument_id = i.instrument_id
              LEFT JOIN quotes q ON q.instrument_id = i.instrument_id
              LEFT JOIN price_daily p ON p.instrument_id = i.instrument_id AND p.price_date = %(ls)s
             WHERE i.is_tradable AND e.asset_class = 'us_equity'
            """,
            {"t": scored_at, "ls": last_session},
        ).fetchall()
        # By id list, as the scorer does: a join of the view does not finish (ai_trial_pack_reader.read_shortlist).
        view_caps = {
            int(r["instrument_id"]): r["market_cap_live"]
            for r in cur.execute(
                "SELECT instrument_id, market_cap_live FROM instrument_valuation "
                "WHERE instrument_id = ANY(%(ids)s::bigint[])",
                {"ids": [r["instrument_id"] for r in rows]},
            ).fetchall()
            if r["market_cap_live"] is not None
        }
        caps = {iid: cap for iid, raw in view_caps.items() if (cap := _overlaid_cap(conn, iid, raw)) is not None}
        nyse_caps = [caps[r["instrument_id"]] for r in rows if r["exchange"] == nyse_id and r["instrument_id"] in caps]
        bp = _nearest_rank(nyse_caps, 20)
        if bp is None:
            raise SystemExit("no NYSE caps: breakpoint unavailable (a rebalance refuses)")

        ranking = [
            r
            for r in rows
            if _finite_positive(r["total_score"])
            and r["completeness_tier"] is not None
            and r["completeness_tier"] != "insufficient_data"
            and r["filings_status"] == "analysable"
            and r["instrument_id"] in caps
            and caps[r["instrument_id"]] >= bp
        ]
        ranking.sort(key=lambda r: (-Decimal(r["total_score"]), r["instrument_id"]))
        r_rank = {r["instrument_id"]: k + 1 for k, r in enumerate(ranking)}

        def quote_ok(r: dict[str, Any]) -> bool:
            c = ShortlistCandidate(
                instrument_id=int(r["instrument_id"]),
                symbol=str(r["symbol"]),
                bid=r["bid"],
                ask=r["ask"],
                quoted_at=r["quoted_at"],
                total_score=float(r["total_score"]),
                market_cap_usd=None,
            )
            return is_eligible(c, as_of=as_of)

        quoted = [r for r in ranking if quote_ok(r)]
        bars = cur.execute(
            """
            SELECT instrument_id, array_agg(close ORDER BY price_date DESC) AS closes,
                   array_agg(price_date ORDER BY price_date DESC) AS days
              FROM price_daily
             WHERE instrument_id = ANY(%(ids)s::bigint[]) AND price_date = ANY(%(days)s::date[])
             GROUP BY instrument_id
            """,
            {"ids": [r["instrument_id"] for r in quoted], "days": max_sessions},
        ).fetchall()
        mx: dict[int, Decimal] = {}
        for b in bars:
            cl = b["closes"]
            if list(b["days"]) == max_sessions and all(_finite_positive(x) for x in cl):
                mx[int(b["instrument_id"])] = max(cl[i] / cl[i + 1] - 1 for i in range(MAX_WINDOW))
        cut = _nearest_rank(list(mx.values()), 90)
        if cut is None:
            raise SystemExit("no valid MAX: guard unavailable (a rebalance refuses)")

        def placeable(r: dict[str, Any]) -> bool:
            atr = r["atr_14"]
            return _finite_positive(atr) and _finite_positive(r["ask"]) and ATR_MULTIPLE_STOP * atr < r["ask"]

        feasible_rows = [
            r for r in quoted if r["instrument_id"] in mx and mx[r["instrument_id"]] <= cut and placeable(r)
        ]
        feasible = {r["instrument_id"] for r in feasible_rows}
        print(f"scores run {scored_at.isoformat()}  as_of {as_of.isoformat()}  last session {last_session}")
        run_rows = cur.execute(
            "SELECT count(*) AS n FROM scores WHERE model_version = 'v1.5-balanced' AND scored_at = %(t)s",
            {"t": scored_at},
        ).fetchone()
        print(f"run rows (all instruments): {run_rows['n'] if run_rows else 0}")
        print(f"scored tradable us_equity: {len(rows)}; with an overlaid cap: {len(caps)}")
        print(f"NYSE (exchange {nyse_id}) names with an overlaid cap: {len(nyse_caps)}; 20th pct ${bp:,.0f}")
        print(f"ranking universe R: {len(ranking)}")
        print(f"R with a passing quote: {len(quoted)}; valid MAX({MAX_WINDOW}): {len(mx)}; cut {float(cut):.4f}")
        print(f"entry-feasible F: {len(feasible)}")

        top = [r for r in feasible_rows if r_rank[r["instrument_id"]] <= 2 * n][:n]
        ages = sorted((scored_at - r["thesis_at"]).days for r in top if r["thesis_at"] is not None)
        print(
            f"entrants (F-rank ≤ {n}, R-rank ≤ {2 * n}): {len(top)}; "
            f"deepest R-rank {r_rank[top[-1]['instrument_id']] if top else '-'}; "
            f"with a thesis on or before the run (proxy): {len(ages)}; age days min/median/max: "
            f"{ages[0] if ages else '-'}/{statistics.median(ages) if ages else '-'}/{ages[-1] if ages else '-'}"
        )
        print("  symbols:", " ".join(r["symbol"] for r in top))

        runs = [
            r["t"]
            for r in cur.execute(
                "SELECT DISTINCT scored_at AS t FROM scores "
                "WHERE model_version = 'v1.5-balanced' AND scored_at <= %(a)s ORDER BY 1",
                {"a": as_of},
            ).fetchall()
        ]
        held: set[int] = set()
        formed = False
        history: list[str] = []
        seen_month: str | None = None
        for t in runs:
            if t.strftime("%Y-%m") == seen_month:
                continue
            seen_month = t.strftime("%Y-%m")
            scored = [
                (Decimal(r["total_score"]), r["instrument_id"])
                for r in cur.execute(
                    "SELECT instrument_id, total_score FROM scores WHERE model_version = 'v1.5-balanced' "
                    "AND scored_at = %(t)s AND total_score > 0",
                    {"t": t},
                ).fetchall()
                if r["instrument_id"] in r_rank and _finite_positive(r["total_score"])
            ]
            ordered = [iid for _, iid in sorted(scored, key=lambda x: (-x[0], x[1]))]
            hold_band = set(ordered[: 2 * n])
            kept = held & hold_band
            new = [i for i in ordered[: 2 * n] if i in feasible and i not in kept][: n - len(kept)]
            if formed:
                history.append(
                    f"{t.date()} held {len(held)} kept {len(kept)} exited {len(held - kept)} entered {len(new)}"
                )
            held, formed = kept | set(new), True
        print("first stored run of each month (today's R and F; approximation):")
        for line in history:
            print("  " + line)

        rets = [
            Decimal(r["r"])
            for r in cur.execute(
                """
                SELECT p1.close / p0.close - 1 AS r
                  FROM unnest(%(ids)s::bigint[]) AS u(instrument_id)
                  JOIN price_daily p1 ON p1.instrument_id = u.instrument_id AND p1.price_date = %(ls)s
                  JOIN LATERAL (SELECT close FROM price_daily WHERE instrument_id = u.instrument_id
                                   AND price_date BETWEEN %(ls)s::date - 372 AND %(ls)s::date - 365
                                 ORDER BY price_date DESC LIMIT 1) p0 ON TRUE
                 WHERE p0.close > 0 AND p1.close > 0
                """,
                {"ids": sorted(feasible), "ls": last_session},
            ).fetchall()
            if r["r"] is not None
        ]
        rets = [x for x in rets if x.is_finite()]
        population = len(rets)
        if population < 2 or n >= population:
            print(f"power input unavailable: {population} one-year returns for N = {n}")
            return
        sd = statistics.pstdev([float(x) for x in rets])
        fpc = math.sqrt((population - n) / (population - 1))
        se = sd / math.sqrt(n) * fpc
        print(
            f"≈1-year price return over F: n={population} of {len(feasible)}; "
            f"median {float(statistics.median(rets)):.3f} sd {sd:.3f}; "
            f"random {n}-name basket SE {se:.3f} (FPC {fpc:.3f}); one-sided 2.5% bar ≈ +{Z_ONE_SIDED_025 * se:.3f}"
        )
        if se == 0:
            print("power unavailable: zero dispersion")
            return
        for delta in (0.03, 0.06, 0.10):
            power = 0.5 * (1 + math.erf((delta / se - Z_ONE_SIDED_025) / math.sqrt(2)))
            print(f"  normal-tail power at a true +{delta:.0%}/yr edge (static-basket approximation): {power:.2f}")


if __name__ == "__main__":
    main()
