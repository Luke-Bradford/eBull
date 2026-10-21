"""#3919 A/B: ``financial_periods_ttm``'s stock items under JKP's annual/quarterly combine (sql/512).

Spec: ``docs/specs/fundamentals/2026-10-21-3919-ttm-stock-items-jkp-combine.md`` ("Acceptance", A/B items 1-8). The
control is sql/508's ``financial_periods_ttm`` with sql/509's ``instrument_valuation``; the treatment is sql/512's two
bodies. Each is created from its file as a TEMPORARY view under its own name (the treatment valuation reading the
treatment TTM view, the binding asserted through ``pg_depend``), because the pre-push smoke applies sql/512 to the dev
database before merge. Everything runs in one REPEATABLE READ transaction that is rolled back: nothing is written.

Prints, and exits non-zero on any failure:

1. wiring: ordered columns and types of both views, the ``instrument_id`` sets, every non-stock TTM column, and the
   deployed views against whichever arm ``schema_migrations`` says is applied;
2. the stock oracle: every treatment stock item and ``shareholders_equity_end`` against an independent Python
   evaluation over ``financial_periods`` (bound and currency guard included); a latest quarter tied on its greatest
   ``filed_date`` passes only when its whole output equals one candidate row;
3. the valuation oracle: every ``instrument_valuation`` column that reads a stock item recomputed in Python from the
   treatment inputs, both arms (NULL masks and values within relative 1e-12); every other column equal to control;
4. the A/B: every changed TTM row with its cause (newer FY / fill / tie), every changed ``pb_ratio`` with its cause
   (equity / refusal lifted / bound from the FY end);
5. the named figures and the cross-source check (NTNX FY2026 ``StockholdersEquity`` on SEC companyconcept);
6. latency: a bulk read and 50 per-instrument reads of each view, the treatment within 2x the control;
7. the AAPL/GME/MSFT/JPM/HD panel;
8. ``fair_value_band`` on frozen inputs: the module's own pass-1 and pass-2 run with ``financial_periods_ttm`` shadowed
   by a TEMPORARY view of each arm: the isolated arm (treatment inputs, control cohort, changed-input instruments) and
   both full arms. ``--fvb-out`` saves the full treatment arm for corpus item 7.

``--post-deploy FILE --since ISO`` runs corpus item 7 alone: every saved instrument's ``fair_value_band_current`` row
computed after ``--since`` against the saved full treatment arm.

    PYTHONPATH=. uv run python scripts/ab_3919_stock_items_combine.py --fvb-out var/3919_fvb.json
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any, Final, LiteralString, cast

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services import fair_value_band as fvb

SQL_DIR: Final = Path(__file__).resolve().parents[1] / "sql"
CONTROL_TTM: Final = SQL_DIR / "508_financial_periods_ttm_dividend_window_guard.sql"
CONTROL_IV: Final = SQL_DIR / "509_instrument_valuation_pb_total_form.sql"
TREATMENT: Final = SQL_DIR / "512_financial_periods_ttm_stock_items_combine.sql"
STOCK_ITEMS: Final = (
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash",
    "long_term_debt",
    "short_term_debt",
    "shares_outstanding",
    "inventory",
    "receivables",
    "payables",
    "goodwill",
    "ppe_net",
)
# instrument_valuation columns that read a stock item (spec A/B item 3); every other column must equal the control.
IV_STOCK_COLUMNS: Final = (
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash",
    "long_term_debt",
    "short_term_debt",
    "shares_outstanding",
    "enterprise_value",
    "pb_ratio",
    "debt_equity_ratio",
    "roa",
    "roe",
    "ev_revenue",
    "ev_ebitda",
)
# fair_value_band's stock-item inputs (fair_value_band.py:938-953 and :1305-1330).
FVB_INPUTS: Final = (
    "total_assets",
    "shareholders_equity",
    "shares_outstanding",
    "long_term_debt",
    "short_term_debt",
    "cash",
)
NAMED: Final = ("NTNX", "HRB", "MCFT", "PURR")
PANEL: Final = ("AAPL", "GME", "MSFT", "JPM", "HD")
REL_TOL: Final = Decimal("1e-12")


def ttm_body(path: Path) -> str:
    m = re.search(r"VIEW financial_periods_ttm AS\n(.*?);\n\nCOMMENT", path.read_text(), re.S)
    if m is None:
        raise RuntimeError(f"no financial_periods_ttm body in {path}")
    return m.group(1)


def iv_body(path: Path, ttm_view: str) -> str:
    m = re.search(r"VIEW instrument_valuation AS\n(.*?);\n\nCOMMENT", path.read_text(), re.S)
    if m is None:
        raise RuntimeError(f"no instrument_valuation body in {path}")
    return re.sub(r"\bfinancial_periods_ttm\b", ttm_view, m.group(1))


_DEPENDS_SQL: Final = """
SELECT DISTINCT d.refobjid::regclass::text
FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid
WHERE d.classid = 'pg_rewrite'::regclass AND r.ev_class = %s::regclass AND d.refobjid <> r.ev_class
  AND d.refobjid::regclass::text LIKE '%%ttm%%'
"""

_QUARTER_CANDIDATES_SQL: Final = f"""
WITH q AS (
    SELECT * FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
),
latest AS (SELECT instrument_id, max(period_end_date) AS pe FROM q GROUP BY instrument_id),
at_end AS (SELECT q.* FROM q JOIN latest l ON l.instrument_id = q.instrument_id AND q.period_end_date = l.pe),
top AS (
    -- The view's dedup orders filed_date DESC NULLS LAST: the greatest non-NULL date wins, else every NULL row ties.
    SELECT instrument_id, max(filed_date) AS f FROM at_end GROUP BY instrument_id
)
SELECT a.instrument_id, a.period_end_date AS pe, a.reported_currency AS cur, {", ".join(STOCK_ITEMS)}
FROM at_end a JOIN top t USING (instrument_id)
WHERE a.filed_date = t.f OR (t.f IS NULL AND a.filed_date IS NULL)
"""

_LATEST_FY_SQL: Final = f"""
SELECT DISTINCT ON (instrument_id) instrument_id, period_end_date AS pe, reported_currency AS cur,
       {", ".join(STOCK_ITEMS)}
FROM financial_periods
WHERE period_type = 'FY' AND superseded_at IS NULL AND normalization_status = 'normalized'
ORDER BY instrument_id, period_end_date DESC
"""

_LAST_PERIOD_SQL: Final = """
SELECT instrument_id, max(period_end_date) AS last_end
FROM financial_periods
WHERE period_type IN ('Q1','Q2','Q3','Q4','FY') AND superseded_at IS NULL AND normalization_status = 'normalized'
GROUP BY instrument_id
"""


def bound(pe: date) -> date:
    """The month-end 18 months after ``pe``: ``(date_trunc('month', pe + 18 months) + 1 month - 1 day)::date``."""
    idx = pe.year * 12 + pe.month - 1 + 19
    return date(idx // 12, idx % 12 + 1, 1) - timedelta(days=1)


def annual_usable(q_cur: str | None, a: dict[str, Any] | None, today: date) -> bool:
    """Departures 4 and 6: within the 18-month bound at CURRENT_DATE, both currencies known and equal."""
    return (
        a is not None and a["cur"] is not None and q_cur is not None and a["cur"] == q_cur and today <= bound(a["pe"])
    )


def oracle(q: dict[str, Any], a: dict[str, Any] | None, today: date) -> dict[str, Any]:
    """Spec "Construction" 1, independently of the SQL: per item, JKP combine_ann_qtr_chars over two latest rows."""
    usable = annual_usable(q["cur"], a, today)
    out: dict[str, Any] = {}
    for c in STOCK_ITEMS:
        if a is None or not usable or a[c] is None or (q[c] is not None and q["pe"] > a["pe"]):
            out[c] = q[c]
            if c == "shareholders_equity":
                out["shareholders_equity_end"] = q["pe"] if q[c] is not None else None
        else:
            out[c] = a[c]
            if c == "shareholders_equity":
                out["shareholders_equity_end"] = a["pe"]
    return out


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, Decimal) and isinstance(b, Decimal) and a.is_nan() and b.is_nan():
        return True
    return a == b


def _close(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    a, b = Decimal(a), Decimal(b)
    if a == b:
        return True
    return abs(a - b) <= REL_TOL * max(abs(a), abs(b))


def _finite_pos(x: Any) -> bool:
    return x is not None and x.is_finite() and x > 0


def _div(n: Any, d: Any) -> Decimal | None:
    with localcontext() as ctx:
        ctx.prec = 40
        return Decimal(n) / Decimal(d)


def valuation_oracle(
    v: dict[str, Any], t: dict[str, Any] | None, control: dict[str, Any], currency: str | None
) -> dict[str, Any]:
    """Every stock-reading ``instrument_valuation`` column, from the treatment TTM row and the row's own inputs.

    The legacy arm reads its displayed stock columns and D/E from ``fundamentals_snapshot``, so only its ``pb_ratio``
    may move; the rest must equal the control."""
    out: dict[str, Any] = {c: control[c] for c in IV_STOCK_COLUMNS if c != "pb_ratio"}
    complete = v["is_complete_ttm"] is True
    cap = v["market_cap_live"]
    if complete:
        assert t is not None
        for c in IV_STOCK_COLUMNS[:7]:
            out[c] = t[c]
        ltd, std, cash = (t[c] or Decimal(0) for c in ("long_term_debt", "short_term_debt", "cash"))
        ev = None if cap is None else cap + ltd + std - cash
        out["enterprise_value"] = ev
        eq = t["shareholders_equity"]
        out["debt_equity_ratio"] = _div(ltd + std, eq) if eq is not None and eq > 0 else None
        ta, ni, rev, ebitda = t["total_assets"], v["net_income_ttm"], v["revenue_ttm"], v["ebitda_ttm"]
        out["roa"] = _div(ni, ta) if ta is not None and ta > 0 and ni is not None else None
        out["roe"] = _div(ni, eq) if eq is not None and eq > 0 and ni is not None else None
        out["ev_revenue"] = _div(ev, rev) if ev is not None and rev is not None and rev > 0 else None
        out["ev_ebitda"] = _div(ev, ebitda) if ev is not None and ebitda is not None and ebitda > 0 else None
    eq, end = (None, None) if t is None else (t["shareholders_equity"], t["shareholders_equity_end"])
    out["pb_ratio"] = (
        _div(cap, eq)
        if cap is not None
        and _finite_pos(eq)
        and end is not None
        and v["session"] is not None
        and v["session"] <= bound(end)
        and currency == "USD"
        else None
    )
    return out


def _columns(conn: psycopg.Connection[Any], view: str) -> list[tuple[str, str]]:
    return [
        (str(r[0]), str(r[1]))
        for r in conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            (view,),
        ).fetchall()
    ]


def _rows(conn: psycopg.Connection[Any], sql: str) -> dict[int, dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        return {int(r["instrument_id"]): r for r in cur.execute(cast(LiteralString, sql)).fetchall()}


def _median(conn: psycopg.Connection[Any], sql: str, params: list[dict[str, Any]]) -> float:
    """One warm-up pass, then the median of 5 timed passes."""
    for p in params:
        conn.execute(cast(LiteralString, sql), p).fetchall()
    passes = []
    for _ in range(5):
        start = time.perf_counter()
        for p in params:
            conn.execute(cast(LiteralString, sql), p).fetchall()
        passes.append(time.perf_counter() - start)
    return statistics.median(passes)


def _shadow(conn: psycopg.Connection[Any], body: str) -> None:
    """Shadow public.financial_periods_ttm for fair_value_band's unqualified reads (pg_temp is searched first)."""
    conn.execute("DROP VIEW IF EXISTS pg_temp.financial_periods_ttm")
    conn.execute(cast(LiteralString, f"CREATE TEMP VIEW financial_periods_ttm AS {body}"))


def _bands(conn: psycopg.Connection[Any], ids: list[int], as_of: date) -> dict[int, tuple[Any, ...]]:
    out: dict[int, tuple[Any, ...]] = {}
    for iid in ids:
        try:
            with conn.transaction():
                b = fvb.compute_band_for_instrument(conn, iid, as_of)
            out[iid] = (b.bear, b.base, b.bull, b.quality_status, b.reason)
        except (psycopg.errors.DataError, ValueError) as exc:
            out[iid] = ("failed", type(exc).__name__)
    return out


def _qualifies(b: tuple[Any, ...]) -> bool:
    return len(b) == 5 and b[4] == "ok"


def _band_tuple(g: tuple[Any, ...]) -> tuple[Any, ...]:
    return (_f(g[1]), _f(g[2]), _f(g[3]), g[4], g[5])


def _band_equal(a: tuple[Any, ...], b: tuple[Any, ...]) -> bool:
    return a[3:] == b[3:] and all(_fclose(x, y) for x, y in zip(a[:3], b[:3], strict=True))


def post_deploy(path: Path, since: datetime) -> int:
    """Corpus item 7. A current row at the saved as-of must equal the saved band. A row at a later as-of is attributed
    to that as-of's prices only if the deployed module, recomputed at that as-of in a rolled-back transaction, gives the
    stored row; any other difference fails."""
    saved = json.loads(path.read_text())
    saved_as_of = date.fromisoformat(saved["as_of"])
    want = {int(k): tuple(v) for k, v in saved["treatment"].items()}
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")
        rows = conn.execute(
            "SELECT instrument_id, bear_value, base_value, bull_value, quality_status, reason, computed_at, as_of_date "
            "FROM fair_value_band_current WHERE method_version = %s AND instrument_id = ANY(%s)",
            (fvb.METHOD_VERSION, list(want)),
        ).fetchall()
        got = {int(r[0]): r for r in rows}
        stale = [i for i in want if i not in got or got[i][6] <= since]
        fresh = [i for i in want if i not in stale]
        later: dict[date, list[int]] = {}
        for iid in fresh:
            if got[iid][7] != saved_as_of:
                later.setdefault(got[iid][7], []).append(iid)
        recomputed: dict[int, tuple[Any, ...]] = {}
        for as_of, ids in later.items():
            fvb.materialize_cohort_members(conn, as_of)
            recomputed.update(_bands(conn, ids, as_of))
        conn.rollback()
    unexplained = []
    for iid in fresh:
        g = got[iid]
        current = _band_tuple(g)
        ref = want[iid] if g[7] == saved_as_of else recomputed[iid]
        if not _band_equal(current, ref):
            unexplained.append((iid, want[iid], ref, current, g[7]))
    print(f"7. post-deploy fair_value_band_current vs the saved full treatment arm (as of {saved_as_of}):")
    print(f"   saved instruments {len(want)}; not recomputed after {since.isoformat()}: {len(stale)} {stale[:10]}")
    n_same = sum(1 for i in fresh if got[i][7] == saved_as_of)
    print(f"   at the saved as-of: {n_same}; at another as-of: {sum(map(len, later.values()))} (recomputed there)")
    print(f"   unexplained differences: {len(unexplained)}")
    for iid, w, ref, current, as_of in unexplained:
        print(f"     {iid}: saved {w}, reference {ref} -> current {current} (current as_of {as_of})")
    return 1 if stale or unexplained else 0


def _f(x: Any) -> float | None:
    return None if x is None else float(x)


def _fclose(a: Any, b: Any) -> bool:
    """fair_value_band_* store bands as NUMERIC(18,6): equal at six decimals."""
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= 5e-7 + 1e-12 * max(abs(a), abs(b))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fvb-out", type=Path, help="save item 8's full treatment arm here for corpus item 7")
    ap.add_argument("--post-deploy", type=Path, help="run corpus item 7 alone against this saved file")
    ap.add_argument("--since", type=datetime.fromisoformat, help="the deploy time, ISO with an offset")
    args = ap.parse_args()
    if args.post_deploy:
        if args.since is None:
            ap.error("--post-deploy needs --since")
        return post_deploy(args.post_deploy, args.since)

    failures: list[str] = []
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")
        conn.execute("SET LOCAL statement_timeout = '1800s'")
        today_row = conn.execute("SELECT CURRENT_DATE").fetchone()
        assert today_row is not None
        today: date = today_row[0]
        c_ttm, t_ttm = ttm_body(CONTROL_TTM), ttm_body(TREATMENT)
        conn.execute(cast(LiteralString, f"CREATE TEMP VIEW ttm_control AS {c_ttm}"))
        conn.execute(cast(LiteralString, f"CREATE TEMP VIEW ttm_treatment AS {t_ttm}"))
        conn.execute(
            cast(LiteralString, f"CREATE TEMP VIEW iv_control AS {iv_body(CONTROL_IV, 'pg_temp.ttm_control')}")
        )
        conn.execute(
            cast(LiteralString, f"CREATE TEMP VIEW iv_treatment AS {iv_body(TREATMENT, 'pg_temp.ttm_treatment')}")
        )
        binds = {
            arm: [r[0] for r in conn.execute(_DEPENDS_SQL, (f"pg_temp.iv_{arm}",)).fetchall()]
            for arm in ("control", "treatment")
        }
        cols = {
            v: _columns(conn, f"pg_temp.{v}") for v in ("ttm_control", "ttm_treatment", "iv_control", "iv_treatment")
        }
        ttm_c, ttm_t = _rows(conn, "SELECT * FROM ttm_control"), _rows(conn, "SELECT * FROM ttm_treatment")
        iv_c, iv_t = _rows(conn, "SELECT * FROM iv_control"), _rows(conn, "SELECT * FROM iv_treatment")
        applied_row = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE filename = %s", (TREATMENT.name,)
        ).fetchone()
        applied = bool(applied_row and applied_row[0])
        dep_ttm = _rows(conn, "SELECT * FROM public.financial_periods_ttm")
        dep_iv = _rows(conn, "SELECT * FROM public.instrument_valuation")
        with conn.cursor(row_factory=dict_row) as cur:
            q_cands: dict[int, list[dict[str, Any]]] = {}
            for r in cur.execute(cast(LiteralString, _QUARTER_CANDIDATES_SQL)).fetchall():
                q_cands.setdefault(int(r["instrument_id"]), []).append(r)
            fy = {int(r["instrument_id"]): r for r in cur.execute(cast(LiteralString, _LATEST_FY_SQL)).fetchall()}
            last_end = {int(r["instrument_id"]): r["last_end"] for r in cur.execute(_LAST_PERIOD_SQL).fetchall()}
            meta = {
                int(r["instrument_id"]): r
                for r in cur.execute(
                    "SELECT i.instrument_id, i.symbol, i.currency, "
                    "(SELECT identifier_value FROM external_identifiers e WHERE e.instrument_id = i.instrument_id "
                    " AND e.provider = 'sec' AND e.identifier_type = 'cik' AND e.is_primary LIMIT 1) AS cik "
                    "FROM instruments i"
                ).fetchall()
            }
        sym = {iid: str(m["symbol"]) for iid, m in meta.items()}
        by_sym = {s: i for i, s in sym.items()}

        # 6. latency.
        probe = sorted(ttm_c)[:50]
        latency = {
            arm: {
                "ttm bulk": _median(conn, f"SELECT * FROM ttm_{arm}", [{}]),
                "ttm x50": _median(
                    conn, f"SELECT * FROM ttm_{arm} WHERE instrument_id = %(id)s", [{"id": i} for i in probe]
                ),
                "iv bulk": _median(conn, f"SELECT * FROM iv_{arm}", [{}]),
                "iv x50": _median(
                    conn, f"SELECT * FROM iv_{arm} WHERE instrument_id = %(id)s", [{"id": i} for i in probe]
                ),
            }
            for arm in ("control", "treatment")
        }

        # 8. fair_value_band on frozen inputs.
        as_of = fvb.resolve_batch_as_of_date(conn)
        assert as_of is not None
        ids = fvb._universe_ids(conn)
        # The pass covers the tradable universe only; a changed non-tradable instrument has no band to compare.
        universe = set(ids)
        own_changed = sorted(
            iid for iid in ttm_t if iid in universe and any(not _same(ttm_c[iid][c], ttm_t[iid][c]) for c in FVB_INPUTS)
        )
        _shadow(conn, c_ttm)
        fvb.materialize_cohort_members(conn, as_of)
        band_c = _bands(conn, ids, as_of)
        _shadow(conn, t_ttm)  # the cohort is still the control's
        band_iso = _bands(conn, own_changed, as_of)
        fvb.materialize_cohort_members(conn, as_of)
        band_t = _bands(conn, ids, as_of)
        cohort_peers = conn.execute(
            "SELECT count(*) FROM fair_value_cohort_members WHERE as_of_date = %s", (as_of,)
        ).fetchone()
        conn.rollback()

    # 1. wiring.
    print("\n1. wiring")
    checks = {
        "iv_control reads ttm_control only": binds["control"] == ["ttm_control"],
        "iv_treatment reads ttm_treatment only": binds["treatment"] == ["ttm_treatment"],
        "ttm columns = control + shareholders_equity_end date": cols["ttm_treatment"]
        == [*cols["ttm_control"], ("shareholders_equity_end", "date")],
        "iv columns unchanged": cols["iv_control"] == cols["iv_treatment"],
        "ttm instrument_id sets equal": set(ttm_c) == set(ttm_t),
        "iv instrument_id sets equal": set(iv_c) == set(iv_t),
    }
    non_stock = [c for c, _ in cols["ttm_control"] if c not in STOCK_ITEMS]
    off = [iid for iid in ttm_t if any(not _same(ttm_c[iid][c], ttm_t[iid][c]) for c in non_stock)]
    checks[f"non-stock ttm columns equal ({len(non_stock)} columns)"] = not off
    arm_ttm, arm_iv = (ttm_t, iv_t) if applied else (ttm_c, iv_c)
    checks[f"deployed views equal the {'treatment' if applied else 'control'} (sql/512 applied: {applied})"] = (
        dep_ttm.keys() == arm_ttm.keys()
        and all(all(_same(dep_ttm[i][c], arm_ttm[i][c]) for c in arm_ttm[i]) for i in arm_ttm)
        and dep_iv.keys() == arm_iv.keys()
        and all(all(_same(dep_iv[i][c], arm_iv[i][c]) for c in arm_iv[i]) for i in arm_iv)
    )
    print(
        f"   rows: ttm {len(ttm_t)}, iv {len(iv_t)};"
        f" columns: ttm {len(cols['ttm_treatment'])}, iv {len(cols['iv_treatment'])}"
    )
    for name, ok in checks.items():
        print(f"   {name}: {ok}")
        if not ok:
            failures.append(f"wiring: {name}")

    # 2. stock oracle.
    print("\n2. stock oracle (CURRENT_DATE", today, ")")
    bad: list[int] = []
    tied: list[int] = []
    for iid, t in ttm_t.items():
        cands = q_cands.get(iid, [])
        outs = [oracle(q, fy.get(iid), today) for q in cands]
        got = {c: t[c] for c in (*STOCK_ITEMS, "shareholders_equity_end")}
        if len(cands) > 1:
            tied.append(iid)
        if not any(all(_same(got[c], o[c]) for c in got) for o in outs):
            bad.append(iid)
    print(f"   rows {len(ttm_t)}; not equal to the oracle: {len(bad)} {bad[:10]}")
    print(f"   latest quarters tied on their greatest filed_date: {len(tied)}")
    for iid in tied:
        print(f"     {sym.get(iid)} ({iid}): output equals one whole candidate row: {iid not in bad}")
    if bad:
        failures.append("stock oracle")

    # 3. valuation oracle.
    print("\n3. valuation oracle")
    vbad: dict[str, list[int]] = {}
    for iid, v in iv_t.items():
        want = valuation_oracle(v, ttm_t.get(iid), iv_c[iid], meta[iid]["currency"] if iid in meta else None)
        for c, w in want.items():
            if not _close(v[c], w):
                vbad.setdefault(c, []).append(iid)
        for c in v:
            if c not in IV_STOCK_COLUMNS and not _same(v[c], iv_c[iid][c]):
                vbad.setdefault(f"{c} != control", []).append(iid)
    print(f"   rows {len(iv_t)}; failing columns: {({c: (len(v), v[:5]) for c, v in vbad.items()}) or 'none'}")
    if vbad:
        failures.append("valuation oracle")

    # 4. the A/B.
    print("\n4. the A/B")
    ttm_changed: dict[str, list[int]] = {"newer_fy": [], "fill": [], "tie": []}
    item_changes: dict[str, int] = dict.fromkeys(STOCK_ITEMS, 0)
    for iid, t in ttm_t.items():
        diff = [c for c in STOCK_ITEMS if not _same(t[c], ttm_c[iid][c])]
        if not diff:
            continue
        for c in diff:
            item_changes[c] += 1
        a, qpe = fy.get(iid), ttm_c[iid]["ttm_end"]
        if a is None or not annual_usable(t["reported_currency"], a, today):
            failures.append(f"ttm change without a usable FY row: {iid}")
            continue
        cause = "newer_fy" if a["pe"] > qpe else "tie" if a["pe"] == qpe else "fill"
        ttm_changed[cause].append(iid)
        to_null = [c for c in diff if t[c] is None]
        print(
            f"   ttm {sym.get(iid)} ({iid}) {cause}: {', '.join(diff)}{' TO NULL ' + str(to_null) if to_null else ''}"
        )
        if to_null:
            failures.append(f"ttm item to NULL: {iid}")
    print(
        f"   ttm instruments changed: {sum(map(len, ttm_changed.values()))}"
        f" = { {k: len(v) for k, v in ttm_changed.items()} }"
    )
    print(f"   per item: {item_changes}")
    pb_causes: dict[str, list[int]] = {"equity": [], "refusal_lifted": [], "bound_from_fy_end": []}
    for iid, v in iv_t.items():
        before, after = iv_c[iid]["pb_ratio"], v["pb_ratio"]
        if _same(before, after):
            continue
        c, t = ttm_c.get(iid), ttm_t.get(iid)
        if c is None or t is None:
            failures.append(f"pb change without a ttm row: {iid}")
            continue
        if not _same(c["shareholders_equity"], t["shareholders_equity"]):
            cause = "equity"
        elif t["shareholders_equity_end"] != c["ttm_end"]:
            cause = "bound_from_fy_end"
        elif before is None and last_end.get(iid) is not None and last_end[iid] > c["ttm_end"]:
            cause = "refusal_lifted"
        else:
            failures.append(f"pb change without a cause: {iid}")
            print(f"   pb {sym.get(iid)} ({iid}) UNATTRIBUTED: {before} -> {after}")
            continue
        pb_causes[cause].append(iid)
        print(f"   pb {sym.get(iid)} ({iid}) {cause}: {before} -> {after}")
    pub = sum(1 for v in iv_t.values() if v["pb_ratio"] is not None)
    pub_c = sum(1 for v in iv_c.values() if v["pb_ratio"] is not None)
    print(f"   pb changed: {sum(map(len, pb_causes.values()))} = { {k: len(v) for k, v in pb_causes.items()} }")
    print(f"   pb published: control {pub_c}, treatment {pub}")

    # 5. named figures and cross-source.
    print("\n5. named figures (dated evidence,", today, ")")
    for s in NAMED:
        iid = by_sym.get(s)
        if iid is None or iid not in ttm_t:
            failures.append(f"named {s} missing")
            continue
        a, t = fy.get(iid), ttm_t[iid]
        newer_usable = a is not None and a["pe"] > t["ttm_end"] and today <= bound(a["pe"])
        ok = not newer_usable or (a is not None and t["shareholders_equity"] == a["shareholders_equity"])
        print(
            f"   {s}: quarter {t['ttm_end']} -> FY {a and a['pe']} newer+usable {newer_usable}; "
            f"equity {ttm_c[iid]['shareholders_equity']} -> {t['shareholders_equity']}"
            f" (end {t['shareholders_equity_end']})"
        )
        if not ok:
            failures.append(f"named {s}")
    ntnx = by_sym.get("NTNX")
    cik = meta[ntnx]["cik"] if ntnx in meta else None
    if ntnx is None or cik is None:
        failures.append("NTNX cik")
    else:
        payload = SecFundamentalsProvider(user_agent=settings.sec_user_agent).fetch_concept(
            cik, "us-gaap", "StockholdersEquity"
        )
        facts = [
            f
            for f in (payload or {}).get("units", {}).get("USD", [])
            if f.get("end") == "2026-07-31" and f.get("form") == "10-K" and f.get("fp") == "FY"
        ]
        sec_vals = sorted({Decimal(str(f["val"])) for f in facts})
        ours = ttm_t[ntnx]["shareholders_equity"]
        ok = sec_vals == [ours]
        print(
            f"   NTNX cross-source: SEC companyconcept StockholdersEquity 2026-07-31 10-K {sec_vals}"
            f" vs ours {ours}: {ok}"
        )
        if not ok:
            failures.append("NTNX cross-source")

    # 6. latency.
    print("\n6. latency (median of 5, seconds)")
    for k in latency["control"]:
        c, t = latency["control"][k], latency["treatment"][k]
        ok = t <= 2 * c
        print(f"   {k}: control {c:.3f}, treatment {t:.3f}, ratio {t / c:.2f}: {ok}")
        if not ok:
            failures.append(f"latency {k}")

    # 7. panel.
    print("\n7. panel")
    changed_ids = {i for v in ttm_changed.values() for i in v}
    for s in PANEL:
        iid = by_sym.get(s)
        if iid is None or iid not in ttm_t:
            print(f"   {s}: no TTM row")
            continue
        c, t = ttm_c[iid], ttm_t[iid]
        cause = next((k for k, v in ttm_changed.items() if iid in v), None)
        print(
            f"   {s}: equity {c['shareholders_equity']} -> {t['shareholders_equity']}"
            f" (end {t['shareholders_equity_end']});"
            f" pb {iv_c.get(iid, {}).get('pb_ratio')} -> {iv_t.get(iid, {}).get('pb_ratio')};"
            f" changed {iid in changed_ids} cause {cause}"
        )

    # 8. fair_value_band.
    print(
        f"\n8. fair_value_band on frozen inputs (as_of {as_of}, universe {len(ids)},"
        f" own-input changed {len(own_changed)})"
    )
    print(f"   treatment cohort rows at as_of: {cohort_peers and cohort_peers[0]}")
    iso_changed = [i for i in own_changed if band_iso[i] != band_c[i]]
    print(f"   isolated arm: {len(iso_changed)} band changes, each on an own-input change:")
    for iid in iso_changed:
        inputs = [c for c in FVB_INPUTS if not _same(ttm_c[iid][c], ttm_t[iid][c])]
        print(f"     {sym.get(iid)} ({iid}) inputs {inputs}: {band_c[iid]} -> {band_iso[iid]}")
    full_changed = [i for i in ids if band_t.get(i) != band_c.get(i)]
    own_set = set(own_changed)
    attribution = {"own": 0, "cohort": 0, "both": 0}
    print(f"   full arms: {len(full_changed)} band changes:")
    for iid in full_changed:
        # own: the own inputs alone (control cohort) give the treatment band; cohort: they move nothing (or did not
        # change); both: they move the band, and the cohort moves it further.
        own_moves = iid in own_set and band_iso.get(iid) != band_c.get(iid)
        kind = "cohort" if not own_moves else "own" if band_iso.get(iid) == band_t.get(iid) else "both"
        attribution[kind] += 1
        print(f"     {sym.get(iid)} ({iid}) {kind}: {band_c.get(iid)} -> {band_t.get(iid)}")
    print(f"   attribution: {attribution}")
    start = [i for i in ids if not _qualifies(band_c[i]) and _qualifies(band_t[i])]
    stop = [i for i in ids if _qualifies(band_c[i]) and not _qualifies(band_t[i])]
    print(f"   start qualifying: {len(start)} {[sym.get(i) for i in start]}")
    print(f"   stop qualifying: {len(stop)} {[sym.get(i) for i in stop]}")
    failed = {
        arm: sum(1 for b in bands.values() if b[0] == "failed")
        for arm, bands in (("control", band_c), ("treatment", band_t))
    }
    failed["isolated"] = sum(1 for b in band_iso.values() if b[0] == "failed")
    print(f"   per-instrument failures: {failed}")
    if any(failed.values()):
        failures.append(f"fair_value_band computation failures {failed}")
    if args.fvb_out:
        args.fvb_out.parent.mkdir(parents=True, exist_ok=True)
        args.fvb_out.write_text(
            json.dumps(
                {"as_of": as_of.isoformat(), "code": head, "treatment": {str(i): band_t[i] for i in full_changed}},
                default=str,
            )
        )
        print(f"   saved {len(full_changed)} changed full-treatment bands to {args.fvb_out}")

    print("\nFAILURES:", failures or "none")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
