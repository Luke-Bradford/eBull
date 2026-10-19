"""#3850 acceptance: ``instrument_valuation``'s count-based columns on the #3841 point-in-time row.

Spec: ``docs/specs/fundamentals/2026-10-19-3850-live-cap-share-basis.md`` ("Acceptance"). Each arm is a TEMPORARY
view named ``instrument_valuation``, which shadows the deployed one for this connection only (``pg_temp`` is searched
first), so every reader the decision gates call reads it unmodified, whichever body is deployed. The control is
sql/241's body (``sql/rollback/505_…``), the treatment sql/505's. Both arms are read in one REPEATABLE READ
transaction, which is rolled back: nothing is written.

Prints:

1. wiring over the full population: row set, the independently derived final reason, published values against PIT
   and the item-4 formulas in ``Decimal``, refused rows NULL, every other column equal to the control;
2. the A/B per count-based column: kept, gained by arm, lost by reason, changed by more than 0.5% (count-driven or
   price-date-only, the premise script's section-3 rule);
3. decision gates, both arms, through the consumers' own readers: ``ranking_pot`` v1's §5.0 universes
   (``read_snapshot_inputs`` + ``universes_of``: the NYSE breakpoint and each S₀ name's first failed hold rule), the
   ``ai_trial_pack`` shortlist (``read_step1`` + ``read_shortlist``), and the scorer (its bulk load, overlay and
   ``_score_from_data``, without persistence): ``fcf_yield``, value-family scores, and rank moves attributed to the
   instrument's own cap input or to peers;
4. the independent-value sample (the panel + the decile picks pinned in ``REFERENCE``); a sample name that is not
   published, or whose session is not the reference's date, fails;
5. live expectations (MNST, APH, SPTX, AMZN, XOM);
6. freshness: for each scorer run in the last 30 days, whether a complete PIT build of that UTC date had finished;
7. latency of the per-instrument and bulk reads, both arms.

Exits non-zero when a wiring check, the own-input attribution, a missing or missed reference (beyond ±5%) or the 2x
latency bound fails.

    PYTHONPATH=. uv run python scripts/measure_3850_acceptance.py
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, LiteralString, cast

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services import ranking_pot_rebalance
from app.services.ai_trial_pack_reader import read_scores_run, read_shortlist, read_step1
from app.services.instrument_analytics import assemble_instrument_analytics_bulk
from app.services.ranking_pot import Universes
from app.services.scoring import (
    _DEFAULT_MODEL_VERSION,
    _WEIGHT_MODES,
    _analytics_inputs,
    _bulk_load_instrument_data,
    _score_from_data,
)

_SQL: Final = Path(__file__).resolve().parents[1] / "sql"
MIGRATION: Final = _SQL / "505_instrument_valuation_pit_market_cap.sql"
ROLLBACK: Final = _SQL / "rollback" / "505_instrument_valuation_pit_market_cap.sql"
#: Spec "Construction" item 4.
ITEM4: Final = (
    "market_cap_live",
    "enterprise_value",
    "pb_ratio",
    "price_sales",
    "p_fcf_ratio",
    "fcf_yield",
    "ev_revenue",
    "ev_ebitda",
)
HALF_PCT: Final = Decimal("0.005")
#: numeric division in Postgres rounds to its own scale; float8 → numeric keeps 15 significant digits.
REL_TOL: Final = Decimal("1e-12")
LATENCY_BOUND: Final = 2.0
NAMED: Final = ("MNST", "APH", "SPTX", "AMZN", "XOM")

#: Acceptance 4. companiesmarketcap's per-company chart series (``data = [...]`` on ``/<company>/marketcap/``,
#: retrieved 2026-10-19 ~18:00Z) is WEEKLY: each point is stamped Saturday 00:00 UTC and holds the Friday close, in
#: units of $100k (#3841 slice 3). The 2026-10-17 point is the 2026-10-16 close, every sample row's session. The
#: sample is the panel plus one published instrument per cap decile, picked by section 4's decile rule on 2026-10-19 and
#: pinned here; section 4 prints today's picks beside it.
REFERENCE_SOURCE: Final = "companiesmarketcap.com/<company>/marketcap/ weekly chart series, retrieved 2026-10-19"
_FRIDAY: Final = date(2026, 10, 16)
REFERENCE: Final[dict[str, tuple[date, Decimal]]] = {
    symbol: (_FRIDAY, Decimal(units) * 100_000)
    for symbol, units in (
        ("AAPL", 50125168),
        ("GME", 135257),
        ("MSFT", 39284850),
        ("JPM", 9021604),
        ("HD", 3089147),
        ("PROP", 385),
        ("SKYX", 1300),
        ("GEVO", 2893),
        ("WTI", 5736),
        ("MCB", 10816),
        ("INSP", 19887),
        ("FSLY", 35795),
        ("LNTH", 65104),
        ("ROL", 160943),
        ("CI", 798799),
    )
}
REFERENCE_TOLERANCE: Final = Decimal("0.05")

_DERIVED_SQL = """
    WITH fpi_adr AS (
        SELECT instrument_id FROM coverage WHERE filings_status = 'fpi'
        UNION
        SELECT instrument_id FROM instruments WHERE company_name ~* '\\y(ADR|ADS)\\y'
    ),
    dual_class AS (
        SELECT DISTINCT ei.instrument_id
        FROM external_identifiers ei
        JOIN instrument_class_shares_outstanding c ON c.source_cik = lpad(ei.identifier_value, 10, '0')
        WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary
    ),
    frontier AS (
        SELECT i.instrument_id,
               (SELECT max(p.price_date) FROM price_daily p WHERE p.instrument_id = i.instrument_id AND p.close > 0)
                   AS latest_close,
               (SELECT max(s.bars_through) FROM instrument_share_count_pit s WHERE s.instrument_id = i.instrument_id)
                   AS frontier
        FROM instruments i WHERE i.instrument_id = ANY(%(ids)s::bigint[])
    ),
    ses AS (
        -- Spec Amendment 1: the latest positive close on or before the frontier, else the latest positive close.
        SELECT f.instrument_id,
               coalesce(
                   (SELECT max(p.price_date) FROM price_daily p
                     WHERE p.instrument_id = f.instrument_id AND p.close > 0 AND p.price_date <= f.frontier),
                   f.latest_close
               ) AS session
        FROM frontier f
    )
    SELECT s.instrument_id, s.session, d.instrument_id IS NOT NULL AS is_dual_class,
           a.instrument_id IS NOT NULL AS has_ratio, f.instrument_id IS NOT NULL AS is_fpi_adr,
           p.price_date IS NOT NULL AS has_pit, p.reason AS pit_reason, p.market_cap AS pit_market_cap,
           p.close AS pit_close, p.shares_reported, p.split_factor
    FROM ses s
    LEFT JOIN dual_class d USING (instrument_id)
    LEFT JOIN ads_ratio a USING (instrument_id)
    LEFT JOIN fpi_adr f USING (instrument_id)
    LEFT JOIN instrument_market_cap_pit p ON p.instrument_id = s.instrument_id AND p.price_date = s.session
"""
_ELIGIBLE_SQL = """
    SELECT DISTINCT i.instrument_id
    FROM instruments i
    JOIN coverage c ON c.instrument_id = i.instrument_id
    WHERE i.is_tradable = TRUE
      AND c.filings_status = 'analysable'
      AND (
          EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
          OR EXISTS (SELECT 1 FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id)
          OR EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
      )
    ORDER BY i.instrument_id
"""
_ONE_SQL = (
    "SELECT pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield, debt_equity_ratio, market_cap_live, current_price, fcf_ttm "
    "FROM instrument_valuation WHERE instrument_id = %(id)s"
)
_BULK_SQL = (
    "SELECT instrument_id, pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield, debt_equity_ratio, market_cap_live, "
    "current_price, fcf_ttm FROM instrument_valuation WHERE instrument_id = ANY(%(ids)s::bigint[])"
)


def view_body(path: Path) -> str:
    """The SELECT a migration file creates ``instrument_valuation`` as."""
    m = re.search(r"VIEW instrument_valuation AS\n(.*?);\n\n(?:COMMENT|COMMIT)", path.read_text(), re.S)
    if m is None:
        raise RuntimeError(f"no view body in {path}")
    return m.group(1)


def expected_reason(r: dict[str, Any]) -> str | None:
    """Spec "Construction" item 7, derived from the inputs; None means published."""
    if r["is_dual_class"]:
        return "dual_class"
    if r["has_ratio"]:
        return "ads_ratio"
    if r["is_fpi_adr"]:
        return "fpi_adr"
    if not r["has_pit"]:
        return "no_pit_row"
    return None if r["pit_reason"] is None else str(r["pit_reason"])


def _dec(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _close(a: Decimal | None, b: Decimal | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if a == b:
        return True
    return abs(a - b) <= REL_TOL * max(abs(a), abs(b))


def expected_item4(t: dict[str, Any]) -> dict[str, Decimal | None]:
    """Spec "Construction" item 4 recomputed in Decimal from the treatment row's own operands."""
    cap = _dec(t["market_cap_live"])
    ttm = t["is_complete_ttm"] is True
    fcf, ebitda = _dec(t["fcf_ttm"]), _dec(t["ebitda_ttm"])
    revenue, equity = _dec(t["revenue_ttm"]), _dec(t["shareholders_equity"])
    zero = Decimal(0)
    ev = None
    if cap is not None and ttm:
        ev = (
            cap + (_dec(t["long_term_debt"]) or zero) + (_dec(t["short_term_debt"]) or zero) - (_dec(t["cash"]) or zero)
        )
    return {
        "market_cap_live": cap,
        "enterprise_value": ev,
        "pb_ratio": (cap / equity if cap is not None and equity is not None and equity > 0 else None)
        if ttm
        else _dec(t["pb_ratio"]),
        "price_sales": cap / revenue if ttm and cap is not None and revenue is not None and revenue > 0 else None,
        "p_fcf_ratio": cap / fcf if cap is not None and fcf is not None and fcf > 0 else None,
        "fcf_yield": fcf / cap if cap is not None and cap > 0 and fcf is not None else None,
        "ev_revenue": ev / revenue if ev is not None and revenue is not None and revenue > 0 else None,
        "ev_ebitda": ev / ebitda if ev is not None and ebitda is not None and ebitda > 0 else None,
    }


def _read_view(conn: psycopg.Connection[Any]) -> dict[int, dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        return {int(r["instrument_id"]): r for r in cur.execute("SELECT * FROM instrument_valuation").fetchall()}


def _score(conn: psycopg.Connection[Any], ids: list[int], now: datetime) -> tuple[dict[int, Any], dict[int, Any]]:
    """compute_rankings' scoring, without its persistence. Returns (results by id, bulk data by id)."""
    weights = _WEIGHT_MODES[_DEFAULT_MODEL_VERSION]
    bulk = _bulk_load_instrument_data(conn, ids, now)
    gics: dict[int, str | None] = {}
    shares: dict[int, float | None] = {}
    filed: dict[int, date | None] = {}
    for iid in ids:
        try:
            gics[iid], shares[iid], filed[iid] = _analytics_inputs(bulk[iid])
        except Exception:
            gics[iid], shares[iid], filed[iid] = None, None, None
    analytics = assemble_instrument_analytics_bulk(
        conn,
        ids,
        gics_sector_by_id=gics,
        shares_outstanding_by_id=shares,
        shares_outstanding_filed_by_id=filed,
        today=now.date(),
    )
    results: dict[int, Any] = {}
    for iid in ids:
        try:
            results[iid] = _score_from_data(iid, bulk[iid], weights, _DEFAULT_MODEL_VERSION, now, analytics[iid])
        except Exception:
            continue
    return results, bulk


def _ranks(results: dict[int, Any]) -> dict[int, int]:
    order = sorted(results.values(), key=lambda r: r.total_score, reverse=True)
    return {r.instrument_id: pos for pos, r in enumerate(order, start=1)}


def _pot_universes(conn: psycopg.Connection[Any], now: datetime) -> Universes | str | None:
    """ranking_pot v1's §5.0 universes as its rebalance would derive them now, from the latest scores run."""
    decl = ranking_pot_rebalance.load_declaration(conn)
    run = read_scores_run(conn, as_of=now)
    if decl is None or run is None:
        return None
    read = ranking_pot_rebalance.read_snapshot_inputs(conn, decl, as_of=now, scored_at=run.scored_at, theses={})
    return ranking_pot_rebalance.universes_of(read.inputs)


def _shortlist(conn: psycopg.Connection[Any], now: datetime) -> dict[int, str] | None:
    """ai_trial_pack's §3.1 shortlist, ``{instrument_id: slice}``. Step 1's other refusals do not bear on it."""
    step1 = read_step1(conn, as_of=now)
    if step1.scores_run is None:
        return None
    return {n.instrument_id: n.slice for n in read_shortlist(conn, step1=step1).names}


def _arm(conn: psycopg.Connection[Any], body: str, *, now: datetime, eligible: list[int], probe: int) -> dict[str, Any]:
    """Every reading of one arm, with ``body`` as the session's ``instrument_valuation``."""
    conn.execute("DROP VIEW IF EXISTS pg_temp.instrument_valuation")
    conn.execute(cast(LiteralString, "CREATE TEMP VIEW instrument_valuation AS " + body))
    rows = _read_view(conn)
    latency = (_best(conn, _ONE_SQL, {"id": probe}, 5), _best(conn, _BULK_SQL, {"ids": eligible}, 3))
    scored, bulk = _score(conn, eligible, now)
    return {
        "rows": rows,
        "latency": latency,
        "scored": scored,
        "bulk": bulk,
        "pot": _pot_universes(conn, now),
        "shortlist": _shortlist(conn, now),
    }


def _best(conn: psycopg.Connection[Any], sql: LiteralString, params: dict[str, Any], n: int) -> float:
    best = float("inf")
    for _ in range(n):
        start = time.perf_counter()
        conn.execute(sql, params).fetchall()
        best = min(best, time.perf_counter() - start)
    return best


def main() -> int:
    failures: list[str] = []
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
        build = conn.execute(
            "SELECT build_id, build_date, finished_at FROM market_cap_pit_builds ORDER BY build_id DESC LIMIT 1"
        ).fetchone()
        print("latest complete PIT build", build)
        symbols = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
        eligible = [int(r[0]) for r in conn.execute(_ELIGIBLE_SQL).fetchall()]
        probe = conn.execute(
            "SELECT instrument_id FROM instruments WHERE symbol = 'AAPL' ORDER BY is_primary_listing DESC, "
            "instrument_id LIMIT 1"
        ).fetchone()
        now = datetime.now(UTC)

        # The rebalance reader asserts its own SHARE lock on ranking_pot_state_events; this read-only measurement
        # takes none, so it cannot block the live jobs. Its reads are otherwise the rebalance's own.
        ranking_pot_rebalance._assert_rebalance_transaction = lambda _conn: None
        arms = {
            name: _arm(conn, view_body(path), now=now, eligible=eligible, probe=probe[0] if probe else 0)
            for name, path in (("control", ROLLBACK), ("treatment", MIGRATION))
        }
        control, treat = arms["control"]["rows"], arms["treatment"]["rows"]
        scored_c, bulk_c = arms["control"]["scored"], arms["control"]["bulk"]
        scored_t, bulk_t = arms["treatment"]["scored"], arms["treatment"]["bulk"]

        with conn.cursor(row_factory=dict_row) as cur:
            derived = {int(r["instrument_id"]): r for r in cur.execute(_DERIVED_SQL, {"ids": list(treat)}).fetchall()}
        anchors = {
            iid: row["market_cap_pit"]
            for iid, row in treat.items()
            if symbols.get(iid) in NAMED and row["market_cap_pit"] is not None
        }
        runs = conn.execute(
            "SELECT DISTINCT scored_at FROM scores WHERE scored_at > now() - interval '30 days' ORDER BY 1"
        ).fetchall()
        builds = conn.execute("SELECT build_date, finished_at FROM market_cap_pit_builds ORDER BY build_id").fetchall()
        conn.rollback()

    # 1. wiring
    print("\n1. wiring")
    if set(control) != set(treat):
        failures.append("row set differs")
    print("   rows control", len(control), "treatment", len(treat), "| same id set", set(control) == set(treat))
    bad_reason = bad_value = bad_refused = bad_other = bad_invariant = 0
    for iid, t in treat.items():
        c, d = control.get(iid), derived[iid]
        want = expected_reason(d)
        if t["market_cap_reason"] != want or t["session"] != d["session"]:
            bad_reason += 1
            if bad_reason <= 5:
                print("   reason mismatch", symbols.get(iid), t["market_cap_reason"], want, t["session"], d["session"])
        if (t["market_cap_live"] is None) != (t["market_cap_reason"] is not None):
            bad_invariant += 1
        if t["market_cap_reason"] is None:
            pit = _dec(d["pit_market_cap"])
            if not (
                _close(_dec(t["market_cap_live"]), pit) and _close(_dec(t["valuation_price"]), _dec(d["pit_close"]))
            ):
                bad_value += 1
            exp = expected_item4(t)
            for col in ITEM4:
                if not _close(_dec(t[col]), exp[col]):
                    bad_value += 1
                    if bad_value <= 5:
                        print("   value mismatch", symbols.get(iid), col, t[col], exp[col])
        else:
            for col in ITEM4:
                if col == "pb_ratio" and t["is_complete_ttm"] is not True:
                    continue  # legacy per-share P/B: item 8, compared to control below
                if t[col] is not None:
                    bad_refused += 1
        if c is not None:
            for col, value in c.items():
                if col in ITEM4 and not (col == "pb_ratio" and t["is_complete_ttm"] is not True):
                    continue
                if value != t[col]:
                    bad_other += 1
                    if bad_other <= 5:
                        print("   unchanged-column mismatch", symbols.get(iid), col, value, t[col])
    for name, n in (
        ("reason/session mismatches", bad_reason),
        ("published value mismatches", bad_value),
        ("refused rows with an item-4 value", bad_refused),
        ("other columns differing from control", bad_other),
        ("NULL-iff-reason violations", bad_invariant),
    ):
        print("  ", name, n)
        if n:
            failures.append(name)

    # 2. A/B per count-based column
    print("\n2. A/B per count-based column (kept | gained by arm | lost by reason | changed > 0.5%: count, price date)")
    for col in ITEM4:
        kept = 0
        gained: Counter[str] = Counter()
        lost: Counter[str] = Counter()
        moved: Counter[str] = Counter()
        for iid, t in treat.items():
            old, new = _dec(control[iid][col]), _dec(t[col])
            if old is not None and new is not None:
                kept += 1
                if old != 0 and abs(new / old - 1) > HALF_PCT:
                    d = derived[iid]
                    count = _dec(control[iid]["shares_outstanding"])
                    eff = _dec(d["shares_reported"])
                    eff = eff * Decimal(str(d["split_factor"])) if eff is not None and d["split_factor"] else None
                    same = count is not None and count > 0 and eff is not None and abs(count / eff - 1) <= HALF_PCT
                    moved["price_date" if same else "count"] += 1
            elif new is not None:
                gained["ttm" if t["is_complete_ttm"] else "legacy"] += 1
            elif old is not None:
                lost[str(t["market_cap_reason"] or "operand_guard")] += 1
        print(
            f"   {col}: kept {kept} | gained {dict(gained)} | lost {dict(lost.most_common())} | changed {dict(moved)}"
        )

    # 3. decision gates
    print("\n3. decision gates (each consumer's own reader, both arms)")
    pot_c, pot_t = arms["control"]["pot"], arms["treatment"]["pot"]
    if isinstance(pot_c, Universes) and isinstance(pot_t, Universes):
        print(
            f"   ranking_pot v1: NYSE breakpoint {pot_c.breakpoint:.0f} -> {pot_t.breakpoint:.0f} "
            f"(NYSE caps {pot_c.nyse_cap_population} -> {pot_t.nyse_cap_population}) | "
            f"R {len(pot_c.r_ids)} -> {len(pot_t.r_ids)} | F {len(pot_c.f_ids)} -> {len(pot_t.f_ids)}"
        )
        moves: Counter[str] = Counter()
        for iid in pot_c.r_ids | pot_t.r_ids | set(pot_c.hold_failure) | set(pot_t.hold_failure):
            before = "member" if iid in pot_c.r_ids else str(pot_c.hold_failure.get(iid))
            after = "member" if iid in pot_t.r_ids else str(pot_t.hold_failure.get(iid))
            if before != after:
                moves[f"{before} -> {after}"] += 1
        print("   ranking_pot v1 S0 membership moves", dict(moves.most_common()))
    else:
        print("   ranking_pot v1 universes:", pot_c, "->", pot_t)
    sl_c, sl_t = arms["control"]["shortlist"], arms["treatment"]["shortlist"]
    if sl_c is not None and sl_t is not None:
        print(
            "   ai_trial_pack shortlist: size",
            len(sl_c),
            "->",
            len(sl_t),
            "| left",
            sorted(f"{symbols.get(i)}:{sl_c[i]}" for i in sl_c.keys() - sl_t.keys()),
            "| joined",
            sorted(f"{symbols.get(i)}:{sl_t[i]}" for i in sl_t.keys() - sl_c.keys()),
        )
    else:
        print("   ai_trial_pack shortlist: no scores run")
    dropped, added = scored_c.keys() - scored_t.keys(), scored_t.keys() - scored_c.keys()
    print("   scorer: scored", len(scored_c), "->", len(scored_t), "| dropped", len(dropped), "| added", len(added))
    if dropped or added:
        failures.append("scorer result set differs")
        print("   ", sorted(str(symbols.get(i)) for i in dropped | added)[:20])
    fy: Counter[str] = Counter()
    value_moved = 0
    for iid in scored_c.keys() & scored_t.keys():
        a = _dec(((bulk_c.get(iid) or {}).get("valuation_row") or {}).get("fcf_yield"))
        b = _dec(((bulk_t.get(iid) or {}).get("valuation_row") or {}).get("fcf_yield"))
        if a is None and b is not None:
            fy["gained"] += 1
        elif a is not None and b is None:
            fy["lost"] += 1
        elif a is not None and b is not None and a != b:
            fy["changed"] += 1
        if scored_c[iid].family_scores.value != scored_t[iid].family_scores.value:
            value_moved += 1
    print("   scorer fcf_yield", dict(fy), "| value-family scores changed", value_moved)
    rc, rt = _ranks(scored_c), _ranks(scored_t)
    attributed: Counter[str] = Counter()
    unexplained: list[str] = []
    for iid in rc.keys() & rt.keys():
        if rc[iid] == rt[iid] and scored_c[iid].total_score == scored_t[iid].total_score:
            continue
        own_changed = scored_c[iid].total_score != scored_t[iid].total_score
        vc = (bulk_c.get(iid) or {}).get("valuation_row") or {}
        vt = (bulk_t.get(iid) or {}).get("valuation_row") or {}
        input_changed = any(
            vc.get(col) != vt.get(col) for col in ("market_cap_live", "fcf_yield", "pb_ratio", "p_fcf_ratio")
        )
        if not own_changed:
            attributed["peers"] += 1
        elif not input_changed:
            unexplained.append(str(symbols.get(iid)))
        elif (vc.get("market_cap_live") is None) != (vt.get("market_cap_live") is None):
            attributed["own_refusal_or_gain"] += 1
        else:
            t, d = treat.get(iid), derived.get(iid)
            count = _dec(control[iid]["shares_outstanding"]) if iid in control else None
            eff = _dec(d["shares_reported"]) if d else None
            eff = eff * Decimal(str(d["split_factor"])) if d and eff is not None and d["split_factor"] else None
            same = count is not None and count > 0 and eff is not None and abs(count / eff - 1) <= HALF_PCT
            attributed["own_price_date" if same and t is not None else "own_count"] += 1
    print(
        "   rank moves",
        dict(attributed.most_common()),
        "| own-score moves with no own cap-input change",
        len(unexplained),
    )
    if unexplained:
        failures.append("unexplained own-input rank moves")
        print("   ", unexplained[:20])

    # 4. independent values
    print("\n4. independent values, reference:", REFERENCE_SOURCE)
    published = sorted(
        ((iid, _dec(t["market_cap_live"])) for iid, t in treat.items() if t["market_cap_reason"] is None),
        key=lambda x: x[1] or 0,
    )
    picks = [published[min(len(published) - 1, (k * len(published)) // 10 + len(published) // 20)] for k in range(10)]
    print("   today's decile picks:", [symbols.get(iid) for iid, _ in picks])
    by_symbol = {symbols.get(iid): iid for iid, _ in published}
    for symbol, (ref_date, ref_cap) in REFERENCE.items():
        iid = by_symbol.get(symbol)
        if iid is None:
            print("  ", symbol, "not published")
            failures.append(f"reference sample {symbol} not published")
            continue
        t = treat[iid]
        cap = _dec(t["market_cap_live"]) or Decimal(0)
        line = f"   {symbol} session {t['session']} cap {cap:.0f} | reference {ref_date} {ref_cap:.0f}"
        if ref_date != t["session"]:
            print(line, "| reference is not at the session")
            failures.append(f"reference for {symbol} not at its session {t['session']}")
            continue
        miss = cap / ref_cap - 1
        print(line, f"| miss {miss:+.2%}")
        if abs(miss) > REFERENCE_TOLERANCE:
            failures.append(f"reference miss {symbol}")

    # 5. live expectations
    print("\n5. live expectations")
    for symbol in NAMED:
        iid = next((i for i, s in symbols.items() if s == symbol and i in treat and treat[i]["session"]), None)
        if iid is None:
            print("  ", symbol, "no view row")
            continue
        t = treat[iid]
        window = [
            (w.get("q"), w.get("status"), round(w["f_hat"], 4) if w.get("f_hat") else None)
            for w in ((((anchors.get(iid) or {}).get("evidence") or {}).get("basis") or {}).get("window") or [])
        ]
        pit = anchors.get(iid) or {}
        print(
            "  ",
            symbol,
            "| reason",
            t["market_cap_reason"],
            "| cap",
            t["market_cap_live"],
            "| count",
            pit.get("shares_reported"),
            pit.get("shares_concept"),
            "context",
            pit.get("context_date"),
            "| window",
            window,
        )

    # 6. freshness
    print("\n6. scorer runs (scores.scored_at, 30 days) against complete PIT builds of the same UTC date")
    for (run_at,) in runs:
        ok = any(bd == run_at.astimezone(UTC).date() and fin <= run_at for bd, fin in builds)
        prior = [bd for bd, fin in builds if fin <= run_at]
        print(
            "  ",
            run_at.isoformat(timespec="minutes"),
            "| same-date build finished before:",
            ok,
            "| latest build date then:",
            max(prior) if prior else None,
        )

    # 7. latency
    print("\n7. latency (best of n)")
    lat_c, lat_t = arms["control"]["latency"], arms["treatment"]["latency"]
    for name, c_s, t_s in (("per-instrument", lat_c[0], lat_t[0]), ("compute_rankings bulk read", lat_c[1], lat_t[1])):
        ratio = t_s / c_s if c_s else float("inf")
        print(f"   {name}: control {c_s * 1000:.1f} ms, treatment {t_s * 1000:.1f} ms, ratio {ratio:.2f}")
        if ratio > LATENCY_BOUND:
            failures.append(f"latency {name}")

    print("\nRESULT:", "PASS" if not failures else "FAIL " + "; ".join(failures))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
