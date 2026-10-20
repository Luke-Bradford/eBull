"""#3903 acceptance: ``instrument_valuation.pb_ratio`` as market cap / the latest quarter's equity, both arms (sql/509).

Spec: ``docs/specs/fundamentals/2026-10-20-3903-pb-total-form.md`` ("Acceptance"). The control is sql/507's
``instrument_valuation`` and the treatment sql/509's, each created from its file as a TEMPORARY view, because the
pre-push smoke applies sql/509 to the dev database before this runs; while sql/509 is not yet applied, the deployed view
must equal the file control row for row. Everything is read in one REPEATABLE READ transaction that is rolled back:
nothing is written. Run it after the 08:20Z PIT build and before the evening candle refresh (spec).

Prints:

1. wiring over the full population: ordered columns and types; the ``instrument_id`` multiset; every column other
   than ``pb_ratio`` against the control; treatment ``pb_ratio`` against an independent evaluation over
   ``financial_periods`` (NULL mask and values within relative 1e-12); currency; latest-quarter ties;
2. the A/B: the publication-state table, both arms, and every row whose state or value moved with its attribution;
3. the factor decomposition of every legacy row publishing both ways whose value changed, with provenance for each
   factor outside [0.99, 1.01];
4. external values: ``REFERENCE`` per slot, the identity ours ÷ external = (cap ÷ cap_x) × (E_x ÷ E), the ±15% rule
   and the coverage rule;
5. latency of the bulk read and of 50 per-instrument reads, both views;
6. corpus evidence: the smoke panel, and the theses written since sql/507's apply by prompt version.

Exits non-zero when a wiring check, an unattributed row, an untraced factor, the identity, the reference rule, the
coverage rule or the 2x latency bound fails.

    PYTHONPATH=. uv run python scripts/measure_3903_pb_acceptance.py
"""

from __future__ import annotations

import statistics
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
from scripts.measure_3850_acceptance import _close, _dec, view_body
from scripts.measure_3903_pb_premise import _COUNT_FACTS_SQL, _STORED_COUNTS_SQL, _TIES_SQL

_SQL: Final = Path(__file__).resolve().parents[1] / "sql"
CONTROL: Final = _SQL / "507_instrument_valuation_dividend_yield_total_form.sql"
TREATMENT: Final = _SQL / "509_instrument_valuation_pb_total_form.sql"
FACTOR_BAND: Final = (Decimal("0.99"), Decimal("1.01"))
IDENTITY_TOL: Final = Decimal("1e-9")
REFERENCE_TOLERANCE: Final = Decimal("0.15")
#: #3911 Amendment 1 rule 2: the PIT count must equal its filed fact within 0.5%.
FILED_COUNT_TOL: Final = Decimal("0.005")
LATENCY_BOUND: Final = 2.0
PANEL: Final = ("AAPL", "GME", "MSFT", "JPM", "HD")
#: Spec acceptance item 4's slots by category. The "largest kept legacy" category is filled at run time.
SLOTS: Final[dict[str, tuple[str, ...]]] = {
    "panel": ("AAPL", "MSFT", "JPM", "HD"),
    "gained": ("DVN", "FERG", "CNMD"),
    "count factor": ("PURR", "MTD"),
}
MANDATORY: Final = ("PURR", "MTD")
LARGEST_KEPT: Final = 2
MAX_UNMEASURED: Final = 2
#: Per symbol: (page date, the page's P/B, its market cap in USD, its price), read from stockanalysis.com
#: ``/stocks/<symbol>/statistics/``. A slot absent here is unmeasured, with the reason in ``UNPUBLISHED``.
REFERENCE_SOURCE: Final = "stockanalysis.com/stocks/<symbol>/statistics/"
#: Retrieved 2026-10-20 ~12:20Z; every page showed the 2026-10-19 regular-session close ("Last updated: Oct 20, 2026").
_CLOSE: Final = date(2026, 10, 19)
REFERENCE: Final[dict[str, tuple[date, Decimal, Decimal, Decimal]]] = {
    symbol: (_CLOSE, Decimal(pb), Decimal(cap), Decimal(price))
    for symbol, pb, cap, price in (
        ("AAPL", "46.60", "5.00e12", "342.94"),
        ("MSFT", "8.96", "3.96e12", "533.52"),
        ("JPM", "2.52", "885.72e9", "333.53"),
        ("HD", "18.61", "309.16e9", "309.88"),
        ("DVN", "1.40", "58.55e9", "50.92"),
        ("FERG", "7.06", "43.79e9", "226.36"),
        ("CNMD", "1.33", "1.38e9", "46.05"),
        ("PURR", "0.96", "1.77e9", "8.97"),
        ("MTD", "2540.28", "32.57e9", "1625.73"),
        ("TTRX", "465.76", "319.92e6", "10.74"),
        ("VGNT", "36.40", "3.09e9", "43.69"),
        # Amendment 1: PURR's replacement in the count-factor category; the page lists it as CD.
        ("CD.US", "11.87", "768.93e6", "6.99"),
    )
}
#: Slots the page publishes no P/B or market cap for: symbol -> (URL, date).
UNPUBLISHED: Final[dict[str, tuple[str, date]]] = {}

_LATEST_QUARTER_SQL = """
SELECT DISTINCT ON (instrument_id) instrument_id, period_end_date, shareholders_equity
FROM (
    SELECT DISTINCT ON (instrument_id, period_end_date) instrument_id, period_end_date, shareholders_equity
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
    ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
) d
ORDER BY instrument_id, period_end_date DESC
"""

#: Amendment 1: the latest stored normalized quarterly or annual period; a later one than the quarter read ends its
#: equity.
_LAST_PERIOD_SQL = """
SELECT instrument_id, max(period_end_date) AS last_end FROM financial_periods
WHERE superseded_at IS NULL AND normalization_status = 'normalized' AND period_type IN ('Q1','Q2','Q3','Q4','FY')
GROUP BY instrument_id
"""

#: Amendment 1: share-count facts at given (instrument, instant) pairs, for #3911 Amendment 1 rule 2's count check.
_PAIR_FACTS_SQL = """
SELECT f.instrument_id, f.period_end, f.concept, f.val, f.accession_number, f.filed_date
FROM financial_facts_raw f
JOIN unnest(%(ids)s::bigint[], %(ends)s::date[]) AS k(iid, d) ON f.instrument_id = k.iid AND f.period_end = k.d
WHERE f.period_start IS NULL
  AND f.concept IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
"""

_INPUTS_SQL = """
SELECT i.instrument_id, i.symbol, i.currency, fs.as_of_date, fs.book_value, fs.shares_outstanding AS snapshot_shares,
       ar.ratio AS ads_ratio
FROM instruments i
LEFT JOIN LATERAL (
    SELECT * FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id ORDER BY f.as_of_date DESC LIMIT 1
) fs ON TRUE
LEFT JOIN ads_ratio ar ON ar.instrument_id = i.instrument_id
"""

_THESES_SQL = """
SELECT t.prompt_version, count(*) AS n, min(t.created_at) AS first, max(t.created_at) AS last
FROM theses t
WHERE t.created_at >= (SELECT applied_at FROM schema_migrations WHERE filename = %s)
GROUP BY 1 ORDER BY 1
"""


def valid_through(end: date) -> date:
    """The month-end 18 months after ``end``: ``date_trunc('month', end + 18 months) + 1 month - 1 day``."""
    months = end.month - 1 + 18
    y, m = end.year + months // 12, months % 12 + 1
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return date.fromordinal(date(ny, nm, 1).toordinal() - 1)


def expected(
    c: dict[str, Any], q: dict[str, Any] | None, currency: str | None, last_end: date | None
) -> tuple[Decimal | None, str]:
    """The construction over the control's cap and session and the stored latest quarter, with its NULL's cause."""
    if c["market_cap_reason"] is not None:
        return None, f"cap refused: {c['market_cap_reason']}"
    equity = None if q is None else _dec(q["shareholders_equity"])
    if equity is None:
        return None, "no stored equity"
    if not equity.is_finite():
        return None, "non-finite equity"
    if equity <= 0:
        return None, "equity <= 0"
    assert q is not None
    if c["session"] > valid_through(q["period_end_date"]):
        return None, "past the 18-month bound"
    if last_end is not None and last_end > q["period_end_date"]:
        return None, "a later period is stored"
    if currency != "USD":
        return None, "trading currency not USD"
    cap = _dec(c["market_cap_live"])
    assert cap is not None
    return cap / equity, "published"


def control_cause(c: dict[str, Any], inp: dict[str, Any], q: dict[str, Any] | None) -> str:
    """Why the control's legacy predicate (metric_price > 0 AND book_value > 0, no dual-class/FPI) failed."""
    bv = _dec(inp["book_value"])
    if bv is None:
        return f"book_value NULL (quarter count {inp['snapshot_shares']})"
    equity = None if q is None else _dec(q["shareholders_equity"])
    if bv <= 0 and equity is not None and equity > 0:
        scale = q is not None and inp["as_of_date"] == q["period_end_date"]
        return f"book_value {bv} with stored E positive ({'four-place scale' if scale else 'snapshot quarter differs'})"
    price = _dec(c["current_price"])
    if price is None or price <= 0:
        return "metric_price <= 0"
    return ""


def outside(x: Decimal) -> bool:
    return not FACTOR_BAND[0] <= x <= FACTOR_BAND[1]


def outside_ref(x: Decimal) -> bool:
    """#3911 Amendment 1 rule 2's factor band, [0.85, 1.15]."""
    return abs(x - 1) > REFERENCE_TOLERANCE


def _columns(conn: psycopg.Connection[Any], view: str) -> list[tuple[str, str]]:
    return [
        (str(r[0]), str(r[1]))
        for r in conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            (view,),
        ).fetchall()
    ]


def _rows(conn: psycopg.Connection[Any], view: str) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(cast(LiteralString, f"SELECT * FROM {view}")).fetchall()


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, Decimal) and isinstance(b, Decimal) and a.is_nan() and b.is_nan():
        return True
    return a == b


def _median(conn: psycopg.Connection[Any], sql: LiteralString, params: list[dict[str, Any]]) -> float:
    """One warm-up pass, then the median of 5 timed passes over ``params`` (spec acceptance item 6)."""
    for p in params:
        conn.execute(sql, p).fetchall()
    passes = []
    for _ in range(5):
        start = time.perf_counter()
        for p in params:
            conn.execute(sql, p).fetchall()
        passes.append(time.perf_counter() - start)
    return statistics.median(passes)


def main() -> int:
    failures: list[str] = []
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")
        conn.execute("SET LOCAL statement_timeout = '900s'")
        conn.execute(cast(LiteralString, f"CREATE TEMP VIEW val_control AS {view_body(CONTROL)}"))
        conn.execute(cast(LiteralString, f"CREATE TEMP VIEW val_treatment AS {view_body(TREATMENT)}"))
        cols = {arm: _columns(conn, f"pg_temp.val_{arm}") for arm in ("control", "treatment")}
        control_rows = _rows(conn, "val_control")
        treat_rows = _rows(conn, "val_treatment")
        applied = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE filename = %s", (TREATMENT.name,)
        ).fetchone()
        deployed = None if applied is None or applied[0] else _rows(conn, "public.instrument_valuation")
        with conn.cursor(row_factory=dict_row) as cur:
            quarters = {int(r["instrument_id"]): r for r in cur.execute(_LATEST_QUARTER_SQL).fetchall()}
            inputs = {int(r["instrument_id"]): r for r in cur.execute(_INPUTS_SQL).fetchall()}
            ties = cur.execute(_TIES_SQL).fetchall()
            last_end = {int(r["instrument_id"]): r["last_end"] for r in cur.execute(_LAST_PERIOD_SQL).fetchall()}
            theses = cur.execute(_THESES_SQL, (CONTROL.name,)).fetchall()
            traces: dict[int, tuple[list[dict[str, Any]], list[dict[str, Any]]]] = {}
            for c in control_rows:
                q = quarters.get(int(c["instrument_id"]))
                if c["is_complete_ttm"] is None and c["pb_ratio"] is not None and q is not None:
                    iid = int(c["instrument_id"])
                    facts = cur.execute(_COUNT_FACTS_SQL, {"iid": iid, "end": q["period_end_date"]}).fetchall()
                    traces[iid] = (facts, cur.execute(_STORED_COUNTS_SQL, {"iid": iid}).fetchall())
        pairs: list[tuple[int, date]] = []
        for t in treat_rows:
            q = quarters.get(int(t["instrument_id"]))
            if t["pb_ratio"] is not None and q is not None:
                pairs.append((int(t["instrument_id"]), q["period_end_date"]))
                pairs.append((int(t["instrument_id"]), date.fromisoformat(t["market_cap_pit"]["context_date"])))
        count_facts: dict[tuple[int, date], list[tuple[Any, ...]]] = {}
        for f in conn.execute(
            _PAIR_FACTS_SQL, {"ids": [p[0] for p in pairs], "ends": [p[1] for p in pairs]}
        ).fetchall():
            count_facts.setdefault((int(f[0]), f[1]), []).append(f[2:])
        # Latency (spec item 6): 25 lowest instrument_ids with a published cap per arm.
        published = sorted(int(r["instrument_id"]) for r in control_rows if r["market_cap_live"] is not None)
        complete_ids = {int(r["instrument_id"]) for r in control_rows if r["is_complete_ttm"] is True}
        probe = [i for i in published if i in complete_ids][:25] + [i for i in published if i not in complete_ids][:25]
        latency: dict[str, dict[str, float]] = {}
        for arm in ("control", "treatment"):
            latency[arm] = {
                "bulk": _median(conn, cast(LiteralString, f"SELECT * FROM val_{arm}"), [{}]),
                "per-instrument x50": _median(
                    conn,
                    cast(LiteralString, f"SELECT * FROM val_{arm} WHERE instrument_id = %(id)s"),
                    [{"id": i} for i in probe],
                ),
            }
        conn.rollback()

    control = {int(r["instrument_id"]): r for r in control_rows}
    treat = {int(r["instrument_id"]): r for r in treat_rows}
    symbols = {iid: str(inp["symbol"]) for iid, inp in inputs.items()}

    # 1. wiring
    print("\n1. wiring")
    cols_ok = cols["control"] == cols["treatment"]
    print(f"   columns: {len(cols['treatment'])}, names, types and typmods equal {cols_ok}")
    if not cols_ok:
        failures.append("catalog columns")
    multiset_ok = Counter(r["instrument_id"] for r in control_rows) == Counter(r["instrument_id"] for r in treat_rows)
    print(
        f"   rows: control {len(control_rows)}, treatment {len(treat_rows)}; instrument_id multiset equal {multiset_ok}"
    )
    if not multiset_ok or len(control) != len(control_rows):
        failures.append("row multiset")
    bad: Counter[str] = Counter()
    want: dict[int, tuple[Decimal | None, str]] = {}
    for iid, t in treat.items():
        c = control[iid]
        for col, value in c.items():
            if col != "pb_ratio" and not _same(value, t[col]):
                bad[f"{col} differs from control"] += 1
        want[iid] = expected(c, quarters.get(iid), inputs[iid]["currency"], last_end.get(iid))
        if not _close(_dec(t["pb_ratio"]), want[iid][0]):
            bad["pb_ratio off the independent evaluation"] += 1
            print("   mismatch", symbols[iid], t["pb_ratio"], want[iid])
        if t["pb_ratio"] is not None and inputs[iid]["currency"] != "USD":
            bad["published on a non-USD row"] += 1
    if deployed is None:
        print("   sql/509 already applied: the control is the file baseline")
    else:
        dep = {int(r["instrument_id"]): r for r in deployed}
        same = set(dep) == set(control) and all(_same(v, control[i][k]) for i, r in dep.items() for k, v in r.items())
        print(f"   deployed view equals the file control, every row and column: {same}")
        if not same:
            bad["deployed differs from the file control"] += 1
    winning = [t for t in ties if t["at_latest"] and (t["n_eq"] > 1 or 0 < t["n_nonnull"] < t["n"])]
    losing = [t for t in ties if not t["at_latest"]]
    print(f"   ties: {len(ties)} groups; disagreeing at a latest quarter's winning rank {len(winning)}")
    for t in losing:
        print(f"     losing-rank tie {symbols.get(int(t['instrument_id']))} {t['period_end_date']} n={t['n']}")
    if winning:
        bad["disagreeing tie at the winning rank"] += len(winning)
    print(f"   failures {dict(bad) or 'none'}")
    failures.extend(bad)

    # 2. A/B
    print("\n2. A/B, publication state (control -> treatment):")
    cells: Counter[tuple[str, str, str]] = Counter()
    moved: list[tuple[int, str, str]] = []
    for iid, t in treat.items():
        c = control[iid]
        arm = "complete" if c["is_complete_ttm"] is True else "legacy"
        before, after = _dec(c["pb_ratio"]), _dec(t["pb_ratio"])
        cells[(arm, "pub" if before is not None else "NULL", "pub" if after is not None else "NULL")] += 1
        if before is not None and after is None:
            moved.append((iid, "lost", want[iid][1]))
        elif before is None and after is not None:
            cause = control_cause(c, inputs[iid], quarters.get(iid)) if arm == "legacy" else ""
            moved.append((iid, "gained", cause))
        elif before is not None and after is not None and before != after:
            moved.append((iid, "changed", "decomposition" if arm == "legacy" else ""))
    for k, n in sorted(cells.items()):
        print(f"   {k[0]:8} {k[1]:4} -> {k[2]:4} {n}")
    print(
        "   expected (spec, Amendment 1): legacy kept 100, gained 16, lost 47 (46 to a refused cap, PURR to a later"
        " period); complete arm: 8 lost to a later period, no value changes"
    )
    by_kind = Counter((kind, cause.split(" (")[0]) for _, kind, cause in moved)
    for k, n in sorted(by_kind.items()):
        print(f"   {k[0]:8} {k[1] or 'UNATTRIBUTED'}: {n}")
    for iid, kind, cause in sorted(moved, key=lambda m: (m[1], m[2], symbols[m[0]])):
        if kind != "changed":
            print(f"     {kind:7} {symbols[iid]:8} {control[iid]['pb_ratio']} -> {treat[iid]['pb_ratio']}  {cause}")
        if not cause:
            failures.append(f"unattributed {kind} {symbols[iid]}")

    # 3. factor decomposition
    print("\n3. legacy rows publishing both ways whose value changed: today / treatment = price x count x equity")
    print("   symbol today treatment ratio | price count equity | provenance of each factor outside [0.99, 1.01]")
    count_factor: dict[int, Decimal] = {}
    for iid, kind, _ in sorted(moved, key=lambda m: symbols[m[0]]):
        if kind != "changed" or control[iid]["is_complete_ttm"] is True:
            continue
        c, t, inp, q = control[iid], treat[iid], inputs[iid], quarters[iid]
        pit = c["market_cap_pit"]
        close, cap = Decimal(str(pit["close"])), Decimal(str(pit["market_cap"]))
        pit_shares = cap / close
        metric_price = Decimal(str(c["current_price"])) / (_dec(inp["ads_ratio"]) or 1)
        ss, bv, e = _dec(inp["snapshot_shares"]), _dec(inp["book_value"]), _dec(q["shareholders_equity"])
        assert ss is not None and bv is not None and e is not None
        price_f, count_f, equity_f = metric_price / close, ss / pit_shares, e / (bv * ss)
        count_factor[iid] = count_f
        ratio = Decimal(str(c["pb_ratio"])) / Decimal(str(t["pb_ratio"]))
        if abs(ratio / (price_f * count_f * equity_f) - 1) > IDENTITY_TOL:
            failures.append(f"decomposition identity {symbols[iid]}")
        notes = []
        if outside(price_f):
            src = c["price_as_of"]
            later = src is not None and src.date() > c["session"]
            notes.append(f"price: {c['current_price']} as of {src} vs session {c['session']} close {close}")
            if not later:
                failures.append(f"price factor untraced {symbols[iid]}")
        if outside(equity_f):
            scale = abs(bv - e / ss) <= Decimal("0.00005")
            other_quarter = inp["as_of_date"] != q["period_end_date"]
            notes.append(
                f"equity: book {bv} x {ss} vs E {e}; snapshot {inp['as_of_date']} quarter {q['period_end_date']}"
                f" ({'four-place scale' if scale else 'snapshot quarter differs' if other_quarter else 'UNTRACED'})"
            )
            if not (scale or other_quarter):
                failures.append(f"equity factor untraced {symbols[iid]}")
        if outside(count_f):
            facts, stored = traces.get(iid, ([], []))
            notes.append(
                f"count: stored {ss:.0f} at {q['period_end_date']}; PIT {pit_shares:.0f} {pit['shares_concept']}"
                f" {pit['accession']} context {pit['context_date']}; instants"
                f" {[(f['concept'], f'{f["val"]:.0f}', f['accession_number']) for f in facts]};"
                f" stored {[(str(s['period_end_date']), s['period_type'], s['shares_outstanding']) for s in stored]}"
            )
            if not pit.get("accession") or not pit.get("context_date"):
                failures.append(f"count factor untraced {symbols[iid]}")
        print(
            f"   {symbols[iid]:8} {Decimal(str(c['pb_ratio'])):.4f} {Decimal(str(t['pb_ratio'])):.4f} {ratio:.4f}"
            f" | {price_f:.4f} {count_f:.4f} {equity_f:.4f}"
        )
        for n in notes:
            print(f"       {n}")

    # 4. external values
    print(f"\n4. external values ({REFERENCE_SOURCE}); ours / external = (cap / cap_x) x (E_x / E)")
    by_symbol = {symbols[i]: i for i in treat}
    slots = {k: list(v) for k, v in SLOTS.items()}
    kept_legacy = sorted(
        (
            i
            for i, t in treat.items()
            if t["is_complete_ttm"] is None and t["pb_ratio"] is not None and control[i]["pb_ratio"] is not None
        ),
        key=lambda i: -Decimal(str(treat[i]["pb_ratio"])),
    )
    taken = {s for names in SLOTS.values() for s in names}
    slots["largest kept legacy"] = [symbols[i] for i in kept_legacy if symbols[i] not in taken][:LARGEST_KEPT]
    gained_pool = sorted(
        (i for i, kind, _ in moved if kind == "gained"), key=lambda i: -Decimal(str(treat[i]["pb_ratio"]))
    )
    # Amendment 1: a slot whose treatment is NULL (a refused cap or a construction refusal) is reported with its cause
    # and replaced by its category's next name; PURR and MTD are mandatory while their P/B publishes.
    pools = {
        "gained": gained_pool,
        "count factor": sorted(count_factor, key=lambda i: -abs(count_factor[i] - 1)),
        "largest kept legacy": kept_legacy,
    }
    unmeasured = 0
    for category, names in slots.items():
        measured = 0
        k = 0
        while k < len(names):
            sym = names[k]
            k += 1
            iid = by_symbol.get(sym)
            row = treat.get(iid) if iid is not None else None
            if row is None or row["pb_ratio"] is None:
                cause = "no row" if iid is None or row is None else want[iid][1]
                print(f"   {category:20} {sym:6} treatment NULL ({cause}); replaced")
                used = {n for v in slots.values() for n in v}
                pool = [i for i in pools.get(category, []) if treat[i]["pb_ratio"] is not None]
                spare = next((symbols[i] for i in pool if symbols[i] not in used), None)
                if spare is not None:
                    names.append(spare)
                    print(f"     by {spare}")
                continue
            assert iid is not None
            ref = REFERENCE.get(sym)
            if ref is None:
                unmeasured += 1
                where = UNPUBLISHED.get(sym)
                print(f"   {category:20} {sym:6} UNMEASURED {where or 'no reference recorded'}")
                if where is None or sym in MANDATORY:
                    failures.append(f"no reference {sym}")
                continue
            page_date, pb_x, cap_x, price_x = ref
            ours, cap = _dec(row["pb_ratio"]), _dec(row["market_cap_live"])
            assert ours is not None and cap is not None
            e = cap / ours
            e_x = cap_x / pb_x
            q = ours / pb_x
            if abs(q / ((cap / cap_x) * (e_x / e)) - 1) > IDENTITY_TOL:
                failures.append(f"reference identity {sym}")
            ok = abs(q - 1) <= REFERENCE_TOLERANCE
            if not ok:
                # Amendment 1, change 2 (#3911 Amendment 1 rule 2's form): ours / ext = (V / P_x) x (N / A) x R, with
                # R = A x P_x / (PB_x x E), the equity remainder under a per-share page. N / A is divided out only when
                # N is the filed fact it claims to be (its accession, at its context date, within 0.5%, split factor
                # 1) and A is the latest-filed balance-sheet count at E's quarter end. The price factor is never
                # accounted for and R is not explained here, so each must sit in [0.85, 1.15], and their product, the
                # residual, must pass ±15%.
                pit = row["market_cap_pit"]
                n = Decimal(str(pit["shares_reported"]))
                v = Decimal(str(row["valuation_price"]))
                at_context = count_facts.get((iid, date.fromisoformat(pit["context_date"])), [])
                n_filed = any(
                    f[2] == pit["accession"] and abs(Decimal(str(f[1])) / n - 1) <= FILED_COUNT_TOL for f in at_context
                )
                q_end = quarters[iid]["period_end_date"]
                bs = [f for f in count_facts.get((iid, q_end), []) if f[0] == "CommonStockSharesOutstanding"]
                a_fact = max(bs, key=lambda f: (f[3], f[2])) if bs else None
                split_one = Decimal(str(pit["split_factor"])) == 1
                price_f = v / price_x
                if a_fact is not None:
                    a = Decimal(str(a_fact[1]))
                    remainder = a * price_x / (pb_x * e)
                    if abs(q / (price_f * (n / a) * remainder) - 1) > IDENTITY_TOL:
                        failures.append(f"rule 2 identity {sym}")
                    residual = q / (n / a)
                    ok = (
                        n_filed
                        and split_one
                        and not outside_ref(price_f)
                        and not outside_ref(remainder)
                        and abs(residual - 1) <= REFERENCE_TOLERANCE
                    )
                    print(
                        f"       rule 2: price {price_f:.4f}; N {n:.0f} ({pit['shares_concept']} {pit['accession']}"
                        f" {pit['context_date']}, filed {n_filed}, split 1 {split_one}); A {a:.0f} (balance sheet at"
                        f" {q_end}, {a_fact[2]} filed {a_fact[3]}); N/A {n / a:.4f}; remainder {remainder:.4f};"
                        f" residual {residual:.4f} {'PASS' if ok else 'FAIL'}"
                    )
                else:
                    print(f"       rule 2: no filed balance-sheet count at {q_end}; FAIL")
            measured += 1
            print(
                f"   {category:20} {sym:6} ours {ours:.4f} ext {pb_x} ({page_date}, price {price_x}) ratio {q:.4f}"
                f" | cap {cap / cap_x:.4f} E_x/E {e_x / e:.4f} {'PASS' if ok else 'FAIL'}"
            )
            if not ok:
                failures.append(f"reference {sym}")
        if measured == 0:
            failures.append(f"category {category} unmeasured")
    if unmeasured > MAX_UNMEASURED:
        failures.append(f"{unmeasured} unmeasured slots")
    gme = by_symbol.get("GME")
    if gme is not None:
        print(f"   GME (state check): {treat[gme]['pb_ratio']} ({want[gme][1]})")

    # 5. latency
    print("\n5. latency, median of 5 passes after one warm-up (seconds)")
    for shape in ("bulk", "per-instrument x50"):
        c_s, t_s = latency["control"][shape], latency["treatment"][shape]
        print(f"   {shape:20} control {c_s:.4f} treatment {t_s:.4f} ratio {t_s / c_s:.2f}")
        if t_s / c_s > LATENCY_BOUND:
            failures.append(f"latency {shape}")

    # 6. corpus evidence
    print("\n6. smoke panel (treatment): symbol, pb_ratio, cap, cause")
    for sym in PANEL:
        iid = by_symbol.get(sym)
        if iid is not None:
            print(f"   {sym:5} {treat[iid]['pb_ratio']} {treat[iid]['market_cap_live']} {want[iid][1]}")
    print(
        f"   theses since {CONTROL.name}'s apply, by prompt version: {[(t['prompt_version'], t['n']) for t in theses]}"
    )

    print(f"\nFAILURES: {failures or 'none'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
