"""#3923 A/B: ``instrument_valuation`` refuses a cap whose session is a calendar month or more before the build date.

Spec: ``docs/specs/fundamentals/2026-10-19-3850-live-cap-share-basis.md`` Amendment 2 ("Acceptance" items 1 and 4).
The control is sql/512's ``instrument_valuation`` body and the treatment is sql/513's. Each is created from its file
as a TEMPORARY view, because the pre-push smoke applies sql/513 to the dev database before merge. Everything runs in
one REPEATABLE READ transaction that is rolled back: nothing is written.

Prints, and exits non-zero on any failure:

1. wiring: both arms' ordered columns and types, their ``instrument_id`` sets, every step's build being the latest
   complete build, and the deployed view against whichever arm ``schema_migrations`` says is applied;
2. the oracle: the changed rows are exactly the control's published rows whose session is on or before the latest
   complete build's ``build_date`` minus one calendar month (derived here, not from the view's step ``build_id``);
   on them the reason is ``session_stale``, the cap and every count-based column are NULL, and every other column
   equals control; every other row equals control in every column;
3. the refused rows with their age and, as a cross-source, any SEC Form 25 in ``sec_form25_common_equity_delistings``
   for the primary CIK (reported, not gated);
4. latency: a bulk read and 50 per-instrument reads of each arm, the treatment within 2x the control, with plans;
5. the AAPL/GME/MSFT/JPM/HD panel.

    PYTHONPATH=. uv run python scripts/ab_3923_session_stale.py
"""

from __future__ import annotations

import calendar
import re
import statistics
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final, LiteralString, cast

import psycopg
from psycopg.rows import dict_row

from app.config import settings

SQL_DIR: Final = Path(__file__).resolve().parents[1] / "sql"
CONTROL: Final = SQL_DIR / "512_financial_periods_ttm_stock_items_combine.sql"
TREATMENT: Final = SQL_DIR / "513_instrument_valuation_session_stale.sql"
COUNT_BASED: Final = (
    "market_cap_live",
    "enterprise_value",
    "pe_ratio",
    "pb_ratio",
    "price_sales",
    "p_fcf_ratio",
    "fcf_yield",
    "ev_revenue",
    "ev_ebitda",
    "dividend_yield",
)
PANEL: Final = ("AAPL", "GME", "MSFT", "JPM", "HD")
_LATEST_BUILD_SQL: Final = (
    "SELECT build_id, build_date FROM market_cap_pit_builds WHERE status = 'complete' ORDER BY build_id DESC LIMIT 1"
)


def iv_body(path: Path) -> str:
    m = re.search(r"VIEW instrument_valuation AS\n(.*?);\n\nCOMMENT", path.read_text(), re.S)
    if m is None:
        raise RuntimeError(f"no instrument_valuation body in {path}")
    return m.group(1)


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


def month_before(d: date) -> date:
    """Postgres ``date - interval '1 month'``: the same day a month earlier, clamped to that month's last day."""
    year, month = (d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12)
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def main() -> int:
    failures: list[str] = []
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")
        conn.execute("SET LOCAL statement_timeout = '1800s'")
        conn.execute(cast(LiteralString, f"CREATE TEMP VIEW iv_control AS {iv_body(CONTROL)}"))
        conn.execute(cast(LiteralString, f"CREATE TEMP VIEW iv_treatment AS {iv_body(TREATMENT)}"))

        # 1. Wiring.
        cols_c, cols_t = _columns(conn, "pg_temp.iv_control"), _columns(conn, "pg_temp.iv_treatment")
        if cols_c != cols_t:
            failures.append("wiring: column lists differ")
        ctl, trt = _rows(conn, "SELECT * FROM iv_control"), _rows(conn, "SELECT * FROM iv_treatment")
        if set(ctl) != set(trt):
            failures.append(f"wiring: id sets differ by {len(set(ctl) ^ set(trt))}")
        build = conn.execute(_LATEST_BUILD_SQL).fetchone()
        assert build is not None
        build_id, build_date = int(build[0]), build[1]
        step_builds = [r[0] for r in conn.execute("SELECT DISTINCT build_id FROM instrument_share_count_pit")]
        if step_builds != [build_id]:
            failures.append(f"wiring: step builds {step_builds} are not the latest complete build {build_id}")
        cutoff = month_before(build_date)
        pg_cutoff = conn.execute("SELECT (%s::date - interval '1 month')::date", (build_date,)).fetchone()
        if pg_cutoff is None or pg_cutoff[0] != cutoff:
            failures.append(f"wiring: cutoff {cutoff} disagrees with Postgres {pg_cutoff}")
        applied_row = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE filename = %s", (TREATMENT.name,)
        ).fetchone()
        applied = bool(applied_row and applied_row[0])
        deployed = _rows(conn, "SELECT * FROM public.instrument_valuation")
        if deployed != (trt if applied else ctl):
            failures.append(f"wiring: deployed view differs from the {'treatment' if applied else 'control'} arm")
        print(
            f"1. columns {len(cols_t)} | ids {len(trt)} | build {build_id} dated {build_date}, cutoff {cutoff} "
            f"| sql/513 applied {applied}"
        )

        # 2. The oracle.
        expected = {
            iid
            for iid, r in ctl.items()
            if r["market_cap_reason"] is None and r["session"] is not None and r["session"] <= cutoff
        }
        changed = {iid for iid in ctl if ctl[iid] != trt.get(iid)}
        if changed != expected:
            failures.append(
                f"oracle: changed {len(changed)} vs expected {len(expected)}; "
                f"unexpected {sorted(changed - expected)[:20]}, missing {sorted(expected - changed)[:20]}"
            )
        for iid in expected:
            c, t = ctl[iid], trt[iid]
            if t["market_cap_reason"] != "session_stale":
                failures.append(f"oracle: {iid} reason {t['market_cap_reason']}")
            if any(t[col] is not None for col in COUNT_BASED):
                failures.append(f"oracle: {iid} publishes a count-based column")
            other = [k for k in c if k not in COUNT_BASED and k != "market_cap_reason" and c[k] != t[k]]
            if other:
                failures.append(f"oracle: {iid} differs on {other}")
        published = sum(1 for r in ctl.values() if r["market_cap_reason"] is None)
        older = sum(
            1 for r in ctl.values() if r["market_cap_reason"] is not None and r["session"] and r["session"] <= cutoff
        )
        print(
            f"2. control published {published}; refused on age {len(expected)}; changed {len(changed)}; "
            f"already refused and at or before the cutoff {older} (unchanged)"
        )

        # 3. The refused rows, with the Form 25 cross-source.
        with conn.cursor(row_factory=dict_row) as cur:
            detail = cur.execute(
                """
                SELECT i.instrument_id, i.symbol, f.form, f.filed_date, f.rule_provision
                FROM instruments i
                LEFT JOIN external_identifiers ei
                  ON ei.instrument_id = i.instrument_id AND ei.provider = 'sec' AND ei.identifier_type = 'cik'
                 AND ei.is_primary
                LEFT JOIN LATERAL (
                    SELECT d.form, d.filed_date, d.rule_provision FROM sec_form25_common_equity_delistings d
                    WHERE lpad(d.issuer_cik::text, 10, '0') = lpad(ei.identifier_value, 10, '0')
                    ORDER BY d.filed_date DESC LIMIT 1
                ) f ON TRUE
                WHERE i.instrument_id = ANY(%s)
                """,
                (sorted(expected),),
            ).fetchall()
        with_form25 = 0
        print("3. refused on age (symbol, session, age days, latest Form 25):")
        for d in sorted(detail, key=lambda r: ctl[r["instrument_id"]]["session"]):
            s = ctl[d["instrument_id"]]["session"]
            f25 = f"{d['form']} {d['filed_date']} {d['rule_provision']}" if d["form"] else "none in register"
            with_form25 += d["form"] is not None
            print(f"   {d['symbol']:<8} {s} {(build_date - s).days:>4}  {f25}")
        print(f"   with a Form 25: {with_form25} of {len(detail)}")

        # 4. Latency.
        ids = sorted(trt)[:: max(1, len(trt) // 50)][:50]
        params = [{"id": i} for i in ids]
        one = {
            arm: _median(conn, f"SELECT * FROM iv_{arm} WHERE instrument_id = %(id)s", params)
            for arm in ("control", "treatment")
        }
        bulk = {arm: _median(conn, f"SELECT * FROM iv_{arm}", [{}]) for arm in ("control", "treatment")}
        for name, m in (("50 single reads", one), ("bulk read", bulk)):
            print(f"4. {name}: control {m['control']:.3f}s, treatment {m['treatment']:.3f}s")
            if m["treatment"] > 2 * m["control"]:
                failures.append(f"latency: {name} treatment over 2x control")
        plan = conn.execute(
            "EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM iv_treatment WHERE instrument_id = %(id)s", params[0]
        ).fetchall()
        mb = [str(r[0]).strip() for r in plan if "market_cap_pit_builds" in str(r[0])]
        print("   treatment single-read plan, market_cap_pit_builds nodes:", mb)
        print("  ", str(plan[-1][0]).strip())

        # 5. Panel.
        with conn.cursor(row_factory=dict_row) as cur:
            panel = cur.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE symbol = ANY(%s) ORDER BY symbol", (list(PANEL),)
            ).fetchall()
        print("5. panel (symbol, session, control cap/reason -> treatment cap/reason):")
        for p in panel:
            c, t = ctl.get(p["instrument_id"]), trt.get(p["instrument_id"])
            if c is None or t is None:
                print(f"   {p['symbol']}: no valuation row")
                continue
            print(
                f"   {p['symbol']:<5} {c['session']} {c['market_cap_live']}/{c['market_cap_reason']} -> "
                f"{t['market_cap_live']}/{t['market_cap_reason']}"
            )
        conn.rollback()

    for f in failures:
        print("FAIL", f)
    print("RESULT", "FAIL" if failures else "PASS")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
