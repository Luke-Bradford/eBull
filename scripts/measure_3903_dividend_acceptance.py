"""#3903 acceptance: ``instrument_valuation.dividend_yield`` as TTM cash dividends paid / market cap (sql/507, sql/508).

Spec: ``docs/specs/fundamentals/2026-10-19-3903-dividend-yield-total-form.md`` ("Acceptance"). Each arm is a pair of
TEMPORARY views, ``ttm_<arm>`` and ``val_<arm>``, the valuation body rewritten to read its own TTM view by name; the
script asserts from ``pg_depend`` that each valuation arm depends on its own TTM arm and on no other. The control is
sql/220's ``financial_periods_ttm`` and sql/506's ``instrument_valuation`` (``sql/rollback/507_…``, byte for byte),
built from the files rather than read as deployed, because the pre-push smoke applies sql/507 to the dev database
before this runs. Amendment 2: the prior arm is sql/507's two bodies, and the treatment is sql/508's TTM body (the
window guard on ``dividends_paid_ttm_observed``) with sql/507's valuation body. Everything is read in one REPEATABLE
READ transaction that is rolled back: nothing is written. Run it after the 08:20Z PIT build and before the evening
candle refresh (spec).

Prints:

1. wiring over the full population: both views' ordered columns and types per arm; the row sets; every treatment
   ``dividend_yield`` against ``proposed_yield`` (the premise script's construction over sql/220's window rebuilt
   from ``financial_periods``, with sql/508's conditions) in ``Decimal``, NULL decisions included; every other column
   of both views against the control; the increment, prior against treatment on every row and column of both views,
   ``dividends_paid_ttm_observed`` against an independent evaluation on every TTM row; and the end-anchored classes;
2. the A/B: section 1's table of the premise script, today (control) against the treatment, with each NULL's cause;
3. the input policy over every publication: the member check (``priority_value`` as of each member's filed date),
   the input defects (newly exposed ones marked), every publication further from the rebuilt common-only yield than
   today and outside [0.95, 1.05] traced to a member defect or to the concept priority, and every publication
   without a resolved comparison;
4. independent values: GME, BJRI and the Amendment 2 names NULL by their causes; every zero publication with its
   members and member check;
   the external yields in ``REFERENCE`` per slot, with the exact identity
   ours ÷ external = (P_x / V) × (N_f / N) × (D / C) × (C / (N_f · E)) and the spec's pass rule and minimum coverage,
   and the mandatory 364-day boundary slot;
5. the ``ranking_pot_exposure.dividend_yield`` reader over the latest v1 and v2 snapshot universes, both arms;
6. latency of per-instrument and bulk reads of both views, both arms.

Exits non-zero when a wiring check, an expected NULL, an untraced publication, the identity, the reference rule, the
minimum coverage or the 2x latency bound fails.

    PYTHONPATH=. uv run python scripts/measure_3903_dividend_acceptance.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, LiteralString, cast

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services import ranking_pot_exposure as exposure
from app.services.ranking_pot_rebalance import decode_snapshot
from scripts.measure_3850_acceptance import REL_TOL, _best, _close, _dec, view_body
from scripts.measure_3903_acceptance import _days, _latest_fact, _units, diluted_shares, filed_count
from scripts.measure_3903_dividend_premise import (
    _WINDOW,
    BAND,
    DESCRIBED,
    GENERIC,
    TIGHT,
    Fact,
    _cf_facts,
    bounds,
    common_quarter,
    common_ttm,
    inventory_cause,
    priority_value,
    proposed_yield,
    quarter_value,
)

_SQL: Final = Path(__file__).resolve().parents[1] / "sql"
TTM_CONTROL: Final = _SQL / "220_ttm_strict_flow_sums.sql"
VAL_CONTROL: Final = _SQL / "rollback" / "507_instrument_valuation_dividend_yield_total_form.sql"
MIGRATION: Final = _SQL / "507_instrument_valuation_dividend_yield_total_form.sql"
TTM_MIGRATION: Final = _SQL / "508_financial_periods_ttm_dividend_window_guard.sql"
#: Amendment 2's causes, in sql/508's order.
GUARD_CAUSES: Final = ("invalid member interval", "overlapping members", "window over one calendar year")
#: Amendment 2: names expected NULL by a guard condition at the 2026-10-20 run.
GUARD_NULLS: Final = (
    ("KO", "window over one calendar year"),
    ("HNI", "window over one calendar year"),
    ("TILE", "window over one calendar year"),
    ("FLXS", "invalid member interval"),
    ("HUN", "overlapping members"),
)
BOUNDARY: Final = "boundary, 364-day window"
BOUNDARY_SLOTS: Final = 2
APPENDED: Final = ("dividends_paid_ttm_observed", "numeric")
LATENCY_BOUND: Final = 2.0
REFERENCE_TOLERANCE: Final = Decimal("0.15")
HALF_PCT: Final = Decimal("0.005")
BASIC: Final = "WeightedAverageNumberOfSharesOutstandingBasic"
MIN_RECONCILED: Final = 10
#: Amendment 1, finding 3: the filed common CASH dividends DECLARED (us-gaap), against which a publication whose stored
#: members come from the generic concept is screened. Declared, not paid, so timing alone can move it; a diagnostic,
#: never a divisor of our numerator. ``DividendsCommonStock`` is not used: it includes stock and other noncash forms.
DECLARED_COMMON: Final = "DividendsCommonStockCash"
#: Spec acceptance item 4's mandatory slots by category; the untagged category is filled at run time.
SLOTS: Final[dict[str, tuple[str, ...]]] = {
    "panel payer": ("MSFT", "JPM", "HD", "AAPL"),
    "gained cash-only payer": ("O", "FAST", "KO"),
    "negative DPS member": ("CEG",),
    "DPS scale outlier": ("EFC",),
}
UNTAGGED_SLOTS: Final = 3

#: Spec acceptance item 4 (Amendment 1, finding 2): per symbol, the external price P_x (the regular-session close the
#: page shows) and every dividend-history row the page lists with an ex-date from 2025-09-01, as (ex-date, cash amount).
#: E is derived by ``external_ttm``: the amounts with an ex-date in the 365 days to the close. The page's "Annual
#: Dividend" is forward (MSFT's $3.92 is 4 x the declared $0.98) and is not used. stockanalysis states amounts on the
#: current share basis; no name below split between 2025-09-01 and the close. A slot without an entry fails.
REFERENCE_SOURCE: Final = (
    "stockanalysis.com/stocks/<ticker>/dividend/, read 2026-10-19 ~22:45Z (close 2026-10-19 16:00 EDT)"
)
REFERENCE_CLOSE: Final = date(2026, 10, 19)
_O_MONTHLY: Final = (
    ("2025-09-02", "0.269"),
    ("2025-10-01", "0.2695"),
    ("2025-10-31", "0.2695"),
    ("2025-11-28", "0.2695"),
    ("2025-12-31", "0.270"),
    ("2026-01-30", "0.270"),
    ("2026-02-27", "0.270"),
    ("2026-03-31", "0.2705"),
    ("2026-04-30", "0.2705"),
    ("2026-05-29", "0.2705"),
    ("2026-06-30", "0.271"),
    ("2026-07-31", "0.271"),
    ("2026-08-31", "0.271"),
    ("2026-09-30", "0.2715"),
    ("2026-10-30", "0.2715"),
)
_EFC_MONTHLY: Final = tuple(
    (d, "0.130")
    for d in (
        "2025-09-30",
        "2025-10-31",
        "2025-11-28",
        "2025-12-31",
        "2026-01-30",
        "2026-02-27",
        "2026-03-31",
        "2026-04-30",
        "2026-05-29",
        "2026-06-30",
        "2026-07-31",
        "2026-08-31",
        "2026-09-30",
        "2026-10-30",
    )
)
REFERENCE_PAGES: Final[dict[str, tuple[str, tuple[tuple[str, str], ...]]]] = {
    "MSFT": (
        "533.52",
        (
            ("2025-11-20", "0.910"),
            ("2026-02-19", "0.910"),
            ("2026-05-21", "0.910"),
            ("2026-08-20", "0.910"),
            ("2026-11-19", "0.980"),
        ),
    ),
    "JPM": (
        "333.53",
        (
            ("2025-10-06", "1.500"),
            ("2026-01-06", "1.500"),
            ("2026-04-06", "1.500"),
            ("2026-07-06", "1.500"),
            ("2026-10-06", "1.650"),
        ),
    ),
    "HD": (
        "309.88",
        (
            ("2025-09-04", "2.300"),
            ("2025-12-04", "2.300"),
            ("2026-03-12", "2.330"),
            ("2026-06-04", "2.330"),
            ("2026-09-03", "2.330"),
        ),
    ),
    "AAPL": (
        "342.94",
        (("2025-11-10", "0.260"), ("2026-02-09", "0.260"), ("2026-05-11", "0.270"), ("2026-08-10", "0.270")),
    ),
    "O": ("55.51", _O_MONTHLY),
    "FAST": (
        "51.02",
        (
            ("2025-10-28", "0.220"),
            ("2026-01-29", "0.240"),
            ("2026-04-28", "0.240"),
            ("2026-07-28", "0.260"),
            ("2026-10-28", "0.260"),
        ),
    ),
    "KO": (
        "93.42",
        (
            ("2025-09-15", "0.510"),
            ("2025-12-01", "0.510"),
            ("2026-03-13", "0.530"),
            ("2026-06-15", "0.530"),
            ("2026-09-15", "0.530"),
            ("2026-12-01", "0.530"),
        ),
    ),
    "CEG": (
        "263.19",
        (("2025-11-17", "0.3878"), ("2026-03-09", "0.4265"), ("2026-05-15", "0.4265"), ("2026-08-18", "0.4265")),
    ),
    "EFC": ("11.35", _EFC_MONTHLY),
    "GM": (
        "82.75",
        (
            ("2025-09-05", "0.150"),
            ("2025-12-05", "0.150"),
            ("2026-03-06", "0.180"),
            ("2026-06-05", "0.180"),
            ("2026-09-04", "0.180"),
        ),
    ),
    # 1.00 on 2026-01-02 is a special distribution; JKP's ret - retx carries specials, so it stays in E.
    "FCBC": (
        "48.74",
        (
            ("2025-11-14", "0.310"),
            ("2026-01-02", "1.000"),
            ("2026-02-13", "0.310"),
            ("2026-05-15", "0.310"),
            ("2026-08-07", "0.330"),
        ),
    ),
    "CRWS": (
        "2.330",
        (
            ("2025-09-12", "0.080"),
            ("2025-12-12", "0.080"),
            ("2026-03-13", "0.080"),
            ("2026-06-11", "0.080"),
            ("2026-09-11", "0.030"),
        ),
    ),
    # Amendment 2: KO's replacement and the boundary category, read 2026-10-20 ~09:20Z (close 2026-10-19).
    "XOM": (
        "178.28",
        (
            ("2025-11-14", "1.030"),
            ("2026-02-12", "1.030"),
            ("2026-05-15", "1.030"),
            ("2026-08-17", "1.030"),
        ),
    ),
    "NVDA": (
        "217.09",
        (
            ("2025-03-12", "0.010"),
            ("2025-06-11", "0.010"),
            ("2025-09-11", "0.010"),
            ("2025-12-04", "0.010"),
            ("2026-03-11", "0.010"),
            ("2026-06-04", "0.250"),
            ("2026-09-10", "0.250"),
        ),
    ),
    "COST": (
        "995.53",
        (
            ("2025-02-07", "1.160"),
            ("2025-05-02", "1.300"),
            ("2025-08-01", "1.300"),
            ("2025-10-31", "1.300"),
            ("2026-01-30", "1.300"),
            ("2026-05-01", "1.470"),
            ("2026-07-24", "1.470"),
            ("2026-10-30", "1.470"),
        ),
    ),
}
#: The first ex-date each stored page list covers; a filing-lag E is computed only over a span the list covers.
REFERENCE_LISTED_FROM: Final[dict[str, date]] = {"NVDA": date(2025, 1, 1), "COST": date(2025, 1, 1)}
DEFAULT_LISTED_FROM: Final = date(2025, 9, 1)


def external_ttm(events: tuple[tuple[str, str], ...], close: date) -> Decimal:
    """E: the cash amounts with an ex-date in the 365 days to ``close`` (close - 365 < ex-date <= close)."""
    return sum(
        (Decimal(a) for d, a in events if close - timedelta(days=365) < date.fromisoformat(d) <= close), Decimal(0)
    )


REFERENCE: Final[dict[str, tuple[Decimal, date, Decimal]]] = {
    symbol: (Decimal(price), REFERENCE_CLOSE, external_ttm(events, REFERENCE_CLOSE))
    for symbol, (price, events) in REFERENCE_PAGES.items()
}

_CIK_SQL: Final = (
    "SELECT instrument_id, lpad(identifier_value, 10, '0') FROM external_identifiers "
    "WHERE provider = 'sec' AND identifier_type = 'cik' AND is_primary"
)
_ELIGIBLE_SQL: Final = """
    SELECT DISTINCT i.instrument_id
    FROM instruments i
    JOIN coverage c ON c.instrument_id = i.instrument_id
    WHERE i.is_tradable = TRUE
      AND c.filings_status = 'analysable'
      AND EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
    ORDER BY i.instrument_id
"""
_SNAPSHOT_SQL: Final = """
    SELECT d.strategy_id, a.attempt_id, a.fired_at, a.snapshot
    FROM ranking_pot_declarations d
    JOIN LATERAL (
        SELECT * FROM ranking_pot_rebalance_attempts a
        WHERE a.declaration_id = d.declaration_id AND a.snapshot IS NOT NULL
        ORDER BY a.fired_at DESC LIMIT 1
    ) a ON TRUE
    ORDER BY d.strategy_id
"""

#: Amendment 2's end-anchored cross-check: per instrument, sql/220's window's last end, first (rn = 4) start and end,
#: the preceding stored quarter's (rn = 5) end, and whether a filed fact's context runs from that start to that end.
_ANCHOR_SQL: Final = """
    WITH d AS (
        SELECT DISTINCT ON (instrument_id, period_end_date) instrument_id, period_start_date, period_end_date
        FROM financial_periods
        WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
        ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
    ), r AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY period_end_date DESC) AS rn FROM d
    ), a AS (
        SELECT instrument_id,
               MAX(period_end_date) FILTER (WHERE rn = 1) AS e,
               MAX(period_start_date) FILTER (WHERE rn = 4) AS s,
               MAX(period_end_date) FILTER (WHERE rn = 4) AS e4,
               MAX(period_end_date) FILTER (WHERE rn = 5) AS e5
        FROM r WHERE rn <= 5 GROUP BY instrument_id
    )
    SELECT a.*, EXISTS (
        SELECT 1 FROM financial_facts_raw f
        WHERE f.instrument_id = a.instrument_id AND f.period_start = a.s AND f.period_end = a.e4
    ) AS start_filed
    FROM a
"""


def ttm_body(path: Path) -> str:
    """The SELECT a migration file creates ``financial_periods_ttm`` as."""
    m = re.search(r"VIEW financial_periods_ttm AS\n(.*?);\n\n", path.read_text(), re.S)
    if m is None:
        raise RuntimeError(f"no financial_periods_ttm body in {path}")
    return m.group(1)


def _same(a: Any, b: Any) -> bool:
    """Equality that reads two NaNs as equal (a NaN member is a legal ``numeric``)."""
    if isinstance(a, Decimal) and isinstance(b, Decimal) and a.is_nan() and b.is_nan():
        return True
    return a == b


def filed_basic_shares(facts: dict[str, Any], end: date) -> tuple[Decimal, str] | None:
    """N_f: ``diluted_shares`` for the basic count, else (Amendment 2 build note) its fiscal-Q4 rule with the
    year-to-date leg widened to 240-290 days: a 12/12/12/16-week filer (COST) files a 36-week (252-day) year-to-date
    context, which the nine-month leg's 260-285 days misses. Duration-weighted, as there."""
    found = diluted_shares(facts, end, BASIC)
    if found is not None:
        return found
    units = [f for f in _units(facts, "us-gaap", BASIC, "shares") if f.get("start")]
    fy = _latest_fact([f for f in units if f["end"] == end.isoformat() and 350 <= _days(f) <= 380])
    if fy is None:
        return None
    ytd = _latest_fact([f for f in units if f["start"] == fy["start"] and 240 <= _days(f) <= 290])
    if ytd is None:
        return None
    d_fy, d_ytd = _days(fy), _days(ytd)
    value = (Decimal(str(fy["val"])) * d_fy - Decimal(str(ytd["val"])) * d_ytd) / (d_fy - d_ytd)
    return value, f"FY-{d_ytd}d duration-weighted"


def _plus_one_year(d: date) -> date:
    """Postgres ``date + INTERVAL '1 year'``: the same month and day a year on, 29 February clamped to the 28th."""
    try:
        return d.replace(year=d.year + 1)
    except ValueError:
        return d.replace(year=d.year + 1, day=28)


def guard_cause(window: list[dict[str, Any]]) -> str | None:
    """sql/508's conditions, in order, over a window that passes sql/507's (four observed, non-negative members); None
    when all pass (spec Amendment 2). A zero total skips condition 3. A NaN total does not equal zero, as in SQL."""
    if any(q["period_start_date"] is None or q["period_start_date"] > q["period_end_date"] for q in window):
        return GUARD_CAUSES[0]
    if any(window[k]["period_start_date"] <= window[k - 1]["period_end_date"] for k in range(1, len(window))):
        return GUARD_CAUSES[1]
    total = sum((q["dividends_paid"] for q in window), Decimal(0))
    if total != 0 and not window[-1]["period_end_date"] < _plus_one_year(window[0]["period_start_date"]):
        return GUARD_CAUSES[2]
    return None


def observed_ttm(window: list[dict[str, Any]]) -> tuple[Decimal | None, str]:
    """``dividends_paid_ttm_observed`` evaluated independently from sql/220's window (``_WINDOW``, which returns only
    windows passing the shape guard), with its NULL's cause. NaN is not negative: it sorts above every number."""
    if len(window) != 4:
        return None, "no window"
    paid = [q["dividends_paid"] for q in window]
    if any(p is None for p in paid):
        return None, "fewer than four observed members" if any(p is not None for p in paid) else "no observed member"
    if any(not p.is_nan() and p < 0 for p in paid):
        return None, "a negative member"
    cause = guard_cause(window)
    if cause is not None:
        return None, cause
    return sum(paid, Decimal(0)), "published"


def amended_yield(r: dict[str, Any], window: list[dict[str, Any]]) -> tuple[Decimal | None, str]:
    """``proposed_yield`` (the construction) with sql/508's conditions applied after it."""
    y, cause = proposed_yield(r, window)
    if y is not None and (g := guard_cause(window)) is not None:
        return None, g
    return y, cause


def anchor_class(a: dict[str, Any] | None) -> str:
    """Amendment 2's end-anchored class of a positive publication's window."""
    if a is None or a["e5"] is None:
        return "no predecessor stored"
    if a["e"] < _plus_one_year(a["e5"] + timedelta(days=1)):
        return "within a year"
    return "predecessor missing, start filed" if a["start_filed"] else "start unverified"


def _columns(conn: psycopg.Connection[Any], view: str) -> list[tuple[str, str]]:
    return [
        (str(r[0]), str(r[1]))
        for r in conn.execute(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            (f"pg_temp.{view}",),
        ).fetchall()
    ]


def _arm(
    conn: psycopg.Connection[Any], arm: str, ttm: str, val: str, eligible: list[int], probe: int
) -> dict[str, Any]:
    """Create one arm's two temporary views and read them."""
    val, n = re.subn(r"\bfinancial_periods_ttm\b", f"pg_temp.ttm_{arm}", val)
    if n != 2:
        raise RuntimeError(f"{arm}: expected 2 financial_periods_ttm references in the valuation body, found {n}")
    conn.execute(cast(LiteralString, f"CREATE TEMP VIEW ttm_{arm} AS {ttm}"))
    conn.execute(cast(LiteralString, f"CREATE TEMP VIEW val_{arm} AS {val}"))
    with conn.cursor(row_factory=dict_row) as cur:
        ttm_rows = {int(r["instrument_id"]): r for r in cur.execute(f"SELECT * FROM ttm_{arm}").fetchall()}  # type: ignore[arg-type]
        val_rows = {int(r["instrument_id"]): r for r in cur.execute(f"SELECT * FROM val_{arm}").fetchall()}  # type: ignore[arg-type]
    one_val = cast(LiteralString, f"SELECT dividend_yield, market_cap_live FROM val_{arm} WHERE instrument_id = %(id)s")
    bulk_val = cast(
        LiteralString,
        f"SELECT instrument_id, dividend_yield, market_cap_live FROM val_{arm} "
        "WHERE instrument_id = ANY(%(ids)s::bigint[])",
    )
    one_ttm = cast(LiteralString, f"SELECT * FROM ttm_{arm} WHERE instrument_id = %(id)s")
    bulk_ttm = cast(LiteralString, f"SELECT * FROM ttm_{arm} WHERE instrument_id = ANY(%(ids)s::bigint[])")
    latency = {
        "valuation per-instrument": _best(conn, one_val, {"id": probe}, 5),
        "valuation bulk": _best(conn, bulk_val, {"ids": eligible}, 3),
        "ttm per-instrument": _best(conn, one_ttm, {"id": probe}, 5),
        "ttm bulk": _best(conn, bulk_ttm, {"ids": eligible}, 3),
    }
    return {
        "ttm": ttm_rows,
        "val": val_rows,
        "ttm_cols": _columns(conn, f"ttm_{arm}"),
        "val_cols": _columns(conn, f"val_{arm}"),
        "latency": latency,
    }


def _depends_on(conn: psycopg.Connection[Any], view: str) -> set[str]:
    """The TTM relations ``view``'s rewrite rule depends on, among both arms and the deployed view."""
    rows = conn.execute(
        """
        SELECT DISTINCT c.relname
        FROM pg_rewrite r
        JOIN pg_depend d ON d.objid = r.oid AND d.classid = 'pg_rewrite'::regclass
        JOIN pg_class c ON c.oid = d.refobjid
        WHERE r.ev_class = %s::regclass
          AND d.refobjid IN ('pg_temp.ttm_control'::regclass, 'pg_temp.ttm_prior'::regclass,
                             'pg_temp.ttm_treatment'::regclass, 'public.financial_periods_ttm'::regclass)
        """,
        (f"pg_temp.{view}",),
    ).fetchall()
    return {str(r[0]) for r in rows}


def window_cause(window: list[dict[str, Any]]) -> str:
    """The member condition alone, independent of the cap."""
    paid = [q["dividends_paid"] for q in window]
    if len(paid) != 4:
        return "no window"
    if all(p is None for p in paid):
        return "no observed member"
    if any(p is None for p in paid):
        return "fewer than four observed members"
    return "four observed members"


def _facts(z: zipfile.ZipFile, names: set[str], cik10: str | None) -> dict[str, Any]:
    if cik10 is None or f"CIK{cik10}.json" not in names:
        return {}
    return json.loads(z.read(f"CIK{cik10}.json")).get("facts", {})


def main() -> int:
    failures: list[str] = []
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    zpath = resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip"
    shafile = zpath.with_name(zpath.name + ".sha256")
    print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
    print(
        f"companyfacts: {zpath} ({zpath.stat().st_size} bytes, sha256 file "
        f"{shafile.read_text().strip()[:12] if shafile.exists() else None})"
    )
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")
        symbols: dict[int, str] = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
        ciks: dict[int, str] = dict(conn.execute(_CIK_SQL).fetchall())
        eligible = [int(r[0]) for r in conn.execute(_ELIGIBLE_SQL).fetchall()]
        probe_row = conn.execute(
            "SELECT instrument_id FROM instruments WHERE symbol = 'AAPL' ORDER BY is_primary_listing DESC, "
            "instrument_id LIMIT 1"
        ).fetchone()
        probe = int(probe_row[0]) if probe_row else 0
        arms = {
            "control": _arm(conn, "control", ttm_body(TTM_CONTROL), view_body(VAL_CONTROL), eligible, probe),
            "prior": _arm(conn, "prior", ttm_body(MIGRATION), view_body(MIGRATION), eligible, probe),
            "treatment": _arm(conn, "treatment", ttm_body(TTM_MIGRATION), view_body(MIGRATION), eligible, probe),
        }
        deps = {arm: _depends_on(conn, f"val_{arm}") for arm in arms}
        # Amendment 1, finding 1: while the deployed views are still sql/220 + sql/506 (no appended column), the file
        # control must reproduce them row for row; after the pre-push smoke applied sql/507 there is nothing to check.
        deployed: dict[str, dict[int, dict[str, Any]]] | None = None
        has_appended = conn.execute(
            "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.financial_periods_ttm'::regclass "
            "AND attname = %s AND NOT attisdropped",
            (APPENDED[0],),
        ).fetchone()
        if has_appended is not None and has_appended[0] == 0:
            with conn.cursor(row_factory=dict_row) as cur:
                deployed = {
                    "ttm": {
                        int(r["instrument_id"]): r for r in cur.execute("SELECT * FROM public.financial_periods_ttm")
                    },
                    "val": {
                        int(r["instrument_id"]): r for r in cur.execute("SELECT * FROM public.instrument_valuation")
                    },
                }
        windows: dict[int, list[dict[str, Any]]] = defaultdict(list)
        with conn.cursor(row_factory=dict_row) as cur:
            for q in cur.execute(_WINDOW).fetchall():
                windows[int(q["instrument_id"])].append(q)
            snapshots = cur.execute(_SNAPSHOT_SQL).fetchall()
            anchors = {int(a["instrument_id"]): a for a in cur.execute(_ANCHOR_SQL).fetchall()}
        conn.rollback()

    control, treat = arms["control"]["val"], arms["treatment"]["val"]
    ttm_c, ttm_t = arms["control"]["ttm"], arms["treatment"]["ttm"]

    # 1. wiring
    print("\n1. wiring")
    print(f"   pg_depend: val_control -> {sorted(deps['control'])}; val_treatment -> {sorted(deps['treatment'])}")
    print(f"   pg_depend: val_prior -> {sorted(deps['prior'])}")
    if deps != {"control": {"ttm_control"}, "prior": {"ttm_prior"}, "treatment": {"ttm_treatment"}}:
        failures.append("pg_depend arms")
    val_same = arms["control"]["val_cols"] == arms["treatment"]["val_cols"]
    ttm_same = arms["treatment"]["ttm_cols"] == [*arms["control"]["ttm_cols"], APPENDED]
    cols_ok = val_same and ttm_same
    print(
        f"   columns: valuation {len(arms['treatment']['val_cols'])} equal {val_same}; "
        f"ttm control {len(arms['control']['ttm_cols'])} + appended {APPENDED} {ttm_same}"
    )
    if not cols_ok:
        failures.append("catalog columns")
    print(f"   rows: valuation {len(control)} / {len(treat)}, ttm {len(ttm_c)} / {len(ttm_t)}")
    if set(control) != set(treat) or set(ttm_c) != set(ttm_t):
        failures.append("row sets differ")
    bad: Counter[str] = Counter()
    expected: dict[int, tuple[Decimal | None, str]] = {}
    for iid, t in treat.items():
        if t["is_complete_ttm"] is not True:
            want: tuple[Decimal | None, str] = (None, "legacy arm")
        else:
            want = amended_yield(t, windows.get(iid, []))
        expected[iid] = want
        if not _close(_dec(t["dividend_yield"]), want[0]):
            bad["dividend_yield off the independent evaluation"] += 1
            if bad["dividend_yield off the independent evaluation"] <= 5:
                print("   mismatch", symbols.get(iid), t["dividend_yield"], want)
        c = control.get(iid, {})
        for col, value in c.items():
            if col != "dividend_yield" and not _same(value, t[col]):
                bad["valuation column differing from control"] += 1
    for iid, t in ttm_t.items():
        for col, value in ttm_c.get(iid, {}).items():
            if not _same(value, t[col]):
                bad["ttm column differing from control"] += 1
    if deployed is None:
        print("   deployed views already carry sql/507: the control is the file baseline (Amendment 1, finding 1)")
    else:
        for kind, file_rows in (("ttm", ttm_c), ("val", control)):
            same = set(deployed[kind]) == set(file_rows) and all(
                _same(v, file_rows[i][k]) for i, row in deployed[kind].items() for k, v in row.items()
            )
            print(f"   deployed {kind} view equals the file control, every row and column: {same}")
            if not same:
                bad[f"deployed {kind} differs from the file control"] += 1
    print(f"   failures {dict(bad) or 'none'}; relative tolerance {REL_TOL}")
    failures.extend(bad)

    # Amendment 2: the increment, prior (sql/507) -> treatment (sql/508), on every row and column of both views.
    ttm_p, val_p = arms["prior"]["ttm"], arms["prior"]["val"]
    inc: Counter[str] = Counter()
    if set(ttm_p) != set(ttm_t) or set(val_p) != set(treat):
        inc["increment key sets differ"] += 1
    if (
        arms["prior"]["ttm_cols"] != arms["treatment"]["ttm_cols"]
        or arms["prior"]["val_cols"] != arms["treatment"]["val_cols"]
    ):
        inc["increment columns differ"] += 1
    guarded: dict[int, str] = {}
    for iid, t in ttm_t.items():
        prior_row = ttm_p.get(iid, {})
        for col, value in prior_row.items():
            if col != APPENDED[0] and not _same(value, t[col]):
                inc["ttm column differing from prior"] += 1
        want_ttm, ttm_cause = observed_ttm(windows.get(iid, []))
        if not _same(t[APPENDED[0]], want_ttm):
            inc["dividends_paid_ttm_observed off the independent evaluation"] += 1
            if inc["dividends_paid_ttm_observed off the independent evaluation"] <= 5:
                print("   increment mismatch", symbols.get(iid), t[APPENDED[0]], want_ttm, ttm_cause)
        if ttm_cause in GUARD_CAUSES:
            guarded[iid] = ttm_cause
            if prior_row.get(APPENDED[0]) is None:
                inc["guard cause on a value sql/507 left NULL"] += 1
        elif not _same(prior_row.get(APPENDED[0]), t[APPENDED[0]]):
            inc["dividends_paid_ttm_observed differing from prior without a guard cause"] += 1
    lost: Counter[str] = Counter()
    for iid, v in treat.items():
        prior_row = val_p.get(iid, {})
        for col, value in prior_row.items():
            if col != "dividend_yield" and not _same(value, v[col]):
                inc["valuation column differing from prior"] += 1
        if iid in guarded:
            if v["dividend_yield"] is not None:
                inc["yield published under a guard cause"] += 1
            if prior_row.get("dividend_yield") is not None:
                lost[guarded[iid]] += 1
        elif not _same(prior_row.get("dividend_yield"), v["dividend_yield"]):
            inc["yield differing from prior without a guard cause"] += 1
    print(
        f"   increment (sql/507 -> sql/508): TTM values refused {dict(Counter(guarded.values()))}; "
        f"publications lost {dict(lost)}; failures {dict(inc) or 'none'}"
    )
    failures.extend(inc)
    classes: Counter[str] = Counter()
    for iid, v in treat.items():
        if (_dec(v["dividend_yield"]) or 0) > 0:
            k = anchor_class(anchors.get(iid))
            classes[k] += 1
            if k != "within a year":
                a = anchors.get(iid) or {}
                print(f"      {k}: {symbols.get(iid)} first start {a.get('s')}, preceding end {a.get('e5')}")
            if k in ("start unverified", "no predecessor stored"):
                failures.append(f"end-anchored {k} {symbols.get(iid)}")
    print(f"   end-anchored classes over positive publications: {dict(classes)}")

    # 2. A/B
    print("\n2. A/B, every complete-TTM row (today = control, proposed = treatment):")
    table: Counter[str] = Counter()
    for iid, t in treat.items():
        if t["is_complete_ttm"] is not True:
            continue
        y, cause = expected[iid]
        today = "published" if control[iid]["dividend_yield"] is not None else "NULL"
        out = ("published zero" if y == 0 else "published") if y is not None else f"NULL: {cause}"
        table[f"today {today}; proposed {out}"] += 1
    for k, v in sorted(table.items()):
        print(f"   {k}: {v}")
    print("   spec 21:55Z: kept 572; gained 262 positive + 9 zero; lost 419 cap, 71 unobserved, 3 negative")
    print("   Amendment 2 (2026-10-20 ~09:00Z): the guard refuses 46 TTM values and 42 publications")

    # 3. input policy (and the per-row material for 4)
    pub = {iid for iid, t in treat.items() if t["dividend_yield"] is not None}
    qb = {iid: bounds(windows[iid], Counter()) for iid in pub}
    member: dict[int, list[tuple[date, str, Decimal | None, Decimal | None, str | None]]] = {}
    rebuilt: dict[int, tuple[Decimal, list[str], set[str], int]] = {}
    filings: dict[int, dict[str, Any]] = {}
    declared: dict[int, Decimal] = {}
    declared_q: dict[int, list[Decimal]] = {}
    declared_prev: dict[int, Decimal] = {}
    rebuild_q: dict[int, list[tuple[str, Decimal, str, set[str]]]] = {}
    with zipfile.ZipFile(zpath) as z:
        names = set(z.namelist())
        for iid in sorted(pub):
            facts = _facts(z, names, ciks.get(iid))
            filings[iid] = facts
            if not facts:
                continue
            g = facts.get("us-gaap", {})
            gaap: dict[str, list[Fact]] = {c: _cf_facts(g, c) for c in (*DESCRIBED, DECLARED_COMMON)}
            values = [quarter_value(gaap[DECLARED_COMMON], q0, q1) for q0, q1 in qb[iid]]
            if all(k == "value" for k, _ in values):
                declared_q[iid] = [v for _, v in values if v is not None]
                declared[iid] = sum(declared_q[iid], Decimal(0))
            # Round 3, finding 1: the quarter before the window, whose declaration a lagged payment brings into it.
            prev_end = qb[iid][0][0] - timedelta(days=1)
            k_prev, v_prev = quarter_value(gaap[DECLARED_COMMON], prev_end - timedelta(days=90), prev_end)
            if k_prev == "value" and v_prev is not None:
                declared_prev[iid] = v_prev
            checks = []
            for q, (q0, q1) in zip(windows[iid], qb[iid], strict=True):
                k, v, c = priority_value(gaap, q0, q1, q["filed_date"])
                st = q["dividends_paid"]
                if k != "value":
                    checks.append((q1, k, st, None, None))
                    continue
                assert v is not None
                ok = abs(v - st) <= HALF_PCT * max(abs(st), Decimal(1))
                checks.append((q1, "agree" if ok else "DIFFER", st, v, c))
            member[iid] = checks
            rebuilt[iid] = common_ttm(gaap, qb[iid])
            rebuild_q[iid] = [common_quarter(gaap, q0, q1) for q0, q1 in qb[iid]]
    print("\n3. input policy over every publication:", len(pub))
    print(
        f"   member check: {dict(Counter(m[1] for ms in member.values() for m in ms))}; "
        f"no companyfacts member {sorted(symbols.get(i, '') for i in pub if i not in member)}"
    )
    defects = sorted(i for i in pub if any(m[1] == "DIFFER" for m in member.get(i, [])))
    print(f"   input defects (a stored member differs from the rule): {len(defects)}")
    for i in defects:
        newly = " NEWLY EXPOSED" if control[i]["dividend_yield"] is None else ""
        diff = [f"{m[0]} stored {m[2]} rule {m[3]}" for m in member[i] if m[1] == "DIFFER"]
        print(f"      {symbols.get(i)}{newly}: {diff}")

    def corrected(i: int) -> Decimal:
        """D with every member that differs from the rule replaced by the rule's value."""
        values = [m[3] if m[1] == "DIFFER" and m[3] is not None else m[2] for m in member[i]]
        return sum((v for v in values if v is not None), Decimal(0))

    def trace(i: int) -> str:
        """Rounds 2-3, finding 10: a trace accounts for the whole gap by its own size. Every member must resolve; each
        differing member is replaced by the rule's value, and each quarter where the rule read the generic concept
        while the rebuild read the common-only one is moved to the rebuild's value. The adjusted total must then lie
        within [0.95, 1.05] of the rebuild (or both be zero)."""
        total = rebuilt[i][0]
        if not all(m[1] in ("agree", "DIFFER") for m in member[i]):
            return "UNTRACED (a member does not resolve)"
        adjusted = corrected(i)
        labels = ["member defect"] if any(m[1] == "DIFFER" for m in member[i]) else []
        for m, (state, value, branch, _) in zip(member[i], rebuild_q[i], strict=True):
            if m[4] == GENERIC and branch == "common" and state == "value" and m[3] is not None:
                adjusted -= m[3] - value
                if "concept priority" not in labels:
                    labels.append("concept priority")
        if labels and ((total > 0 and TIGHT[0] <= adjusted / total <= TIGHT[1]) or (total == 0 and adjusted == 0)):
            return f"{' + '.join(labels)} (adjusted total {adjusted} against rebuilt {total})"
        return f"UNTRACED (adjusted total {adjusted} against rebuilt {total})"

    paid = {i: sum((q["dividends_paid"] for q in windows[i]), Decimal(0)) for i in pub}
    resolved = {i for i in pub if i in rebuilt and not rebuilt[i][3]}
    ratio: dict[int, Decimal] = {i: paid[i] / rebuilt[i][0] for i in resolved if rebuilt[i][0] > 0}
    zero_rebuild = sorted(i for i in resolved if rebuilt[i][0] == 0)
    tight = sum(1 for x in ratio.values() if TIGHT[0] <= x <= TIGHT[1])
    print(
        f"   compared {len(ratio)}; stored / rebuilt in [0.95, 1.05] {tight}; resolved zero rebuild "
        f"{len(zero_rebuild)} (stored also zero {sum(1 for i in zero_rebuild if paid[i] == 0)})"
    )
    for i in sorted((i for i, x in ratio.items() if not TIGHT[0] <= x <= TIGHT[1]), key=lambda i: ratio[i]):
        print(
            f"      outside [0.95, 1.05]: {symbols.get(i)} stored {paid[i]} rebuilt {rebuilt[i][0]} ratio "
            f"{ratio[i]:.3f} quarters {rebuilt[i][1]}"
        )
    # Rows needing a trace: further from the rebuilt yield than today and outside the band; a positive stored total
    # against a resolved zero rebuild; a positive yield today that the treatment publishes as zero against a positive
    # rebuild (round 2, finding 11).
    needs: dict[int, str] = {}
    for i in resolved:
        today, ours, total = _dec(control[i]["dividend_yield"]), _dec(treat[i]["dividend_yield"]), rebuilt[i][0]
        assert ours is not None
        if total == 0:
            if paid[i] != 0:
                needs[i] = f"stored {paid[i]} against a resolved zero rebuild"
            continue
        if ours == 0:
            if today is not None and today > 0:
                needs[i] = f"published zero (today {today:.3f}) against rebuilt {total}"
            continue
        if today is None or today <= 0:
            continue
        ref = total / _dec(treat[i]["market_cap_live"]) * 100  # type: ignore[operator]
        t_x, p_x = today / ref, ours / ref
        if abs(p_x.ln()) > abs(t_x.ln()) and not TIGHT[0] <= p_x <= TIGHT[1]:
            needs[i] = f"today/rebuilt {t_x:.3f} ours/rebuilt {p_x:.3f}"
    untraced: list[str] = []
    for i in sorted(needs, key=lambda i: symbols.get(i, "")):
        tr = trace(i)
        if tr.startswith("UNTRACED"):
            untraced.append(symbols.get(i, str(i)))
        print(f"      needs a trace: {symbols.get(i)} {needs[i]} -> {tr}")
    if untraced:
        failures.append(f"untraced publications {untraced}")
    unresolved = sorted(symbols.get(i, "") for i in pub if i not in resolved)
    print(f"   publications without a resolved comparison: {len(unresolved)} {unresolved}")
    # Amendment 1, finding 3: the generic concept's unfiled deductions, which the rebuild cannot see.
    generic = {i for i in pub if any(m[4] == GENERIC for m in member.get(i, []))}
    screened = {i for i in generic if i in declared}
    proxy = {i: paid[i] / declared[i] for i in screened if declared[i] > 0}
    zero_declared = sorted(i for i in screened if declared[i] == 0)
    candidates = sorted(
        [i for i, x in proxy.items() if not TIGHT[0] <= x <= TIGHT[1]] + [i for i in zero_declared if paid[i] != 0],
        key=lambda i: symbols.get(i, ""),
    )
    print(
        f"   publications with a member on {GENERIC}: {len(generic)}; with a four-quarter filed {DECLARED_COMMON} "
        f"TTM: {len(screened)} ({len(zero_declared)} of them zero); not screenable {len(generic) - len(screened)}; "
        f"numerator proxy investigation candidates (stored / declared outside [0.95, 1.05], or paid against zero "
        f"declared): {len(candidates)}, filed with the normaliser ticket"
    )
    for i in candidates:
        newly = " NEWLY EXPOSED" if control[i]["dividend_yield"] is None else ""
        x = f"{proxy[i]:.3f}" if i in proxy else "zero declared"
        print(f"      {symbols.get(i)}{newly}: stored {paid[i]} declared {declared[i]} ratio {x}")

    # 4. independent values
    print("\n4. independent values")
    by_symbol: dict[str, int] = {}
    for iid, t in treat.items():
        s = symbols.get(iid)
        if s is None or t["is_complete_ttm"] is not True:
            continue
        prev = by_symbol.get(s)
        if prev is None or (_dec(t["market_cap_live"]) or 0) > (_dec(treat[prev]["market_cap_live"]) or 0):
            by_symbol[s] = iid
    for s, cause in (("GME", "no observed member"), ("BJRI", "fewer than four observed members")):
        iid = by_symbol.get(s)
        got = None if iid is None else (treat[iid]["dividend_yield"], window_cause(windows.get(iid, [])))
        ok = got is not None and got == (None, cause)
        print(f"   expected NULL {s} ({cause}): {got} -> {'ok' if ok else 'FAIL'}")
        if not ok:
            failures.append(f"{s} expected NULL")
    for s, cause in GUARD_NULLS:
        iid = by_symbol.get(s)
        got = None if iid is None else (treat[iid]["dividend_yield"], guard_cause(windows.get(iid, [])))
        ok = got is not None and got == (None, cause)
        print(f"   expected NULL {s} ({cause}, Amendment 2): {got} -> {'ok' if ok else 'FAIL'}")
        if not ok:
            failures.append(f"{s} expected NULL")
    zeros = sorted((i for i in pub if treat[i]["dividend_yield"] == 0), key=lambda i: symbols.get(i, ""))
    print(f"   zero publications: {len(zeros)}")
    for i in zeros:
        print(
            f"      {symbols.get(i)}: members {[str(q['dividends_paid']) for q in windows[i]]}; "
            f"check {[m[1] for m in member.get(i, [])] or 'no companyfacts member'}"
        )

    def cap(i: int) -> Decimal:
        return _dec(treat[i]["market_cap_live"]) or Decimal(0)

    def dps(i: int) -> list[Decimal | None]:
        return [q["dps_declared"] for q in windows.get(i, [])]

    def positive(i: int) -> bool:
        return (_dec(treat[i]["dividend_yield"]) or 0) > 0

    category_of = {
        "gained cash-only payer": lambda i: control[i]["dividend_yield"] is None and all(d is None for d in dps(i)),
        "negative DPS member": lambda i: any(d is not None and d < 0 for d in dps(i)),
        "DPS scale outlier": lambda i: (
            len(p := [d for d in dps(i) if d is not None and d.is_finite() and d > 0]) > 0 and max(p) > 50 * min(p)
        ),
    }
    # Rounds 2-3, findings 12, 13, 18: named names are reserved first. A non-panel name is replaced when it no longer
    # meets its category's predicate (whatever its yield) or its cap is refused; any other missing yield on a named
    # name fails. Pools hold one canonical instrument per symbol (``by_symbol``), so a slot's symbol resolves back to
    # the instrument that was assessed.
    canonical = {i for i in pub if by_symbol.get(symbols.get(i, "")) == i}
    kept: dict[str, list[str]] = defaultdict(list)
    to_replace: list[tuple[str, str]] = []

    def refusal(i: int | None) -> str | None:
        """A named slot's refusal: its cap's reason, else an Amendment 2 guard cause."""
        if i is None:
            return None
        cause = expected[i][1]
        return treat[i]["market_cap_reason"] or (cause if cause in GUARD_CAUSES else None)

    for category, names_ in SLOTS.items():
        for s in names_:
            iid = by_symbol.get(s)
            reason = refusal(iid)
            if category != "panel payer" and iid is not None and not category_of[category](iid):
                print(f"   slot {category} {s}: no longer meets the category's predicate")
                to_replace.append((category, s))
            elif iid is not None and positive(iid):
                kept[category].append(s)
            elif reason is not None:
                if category == "panel payer":
                    kept[category].append(s)  # reported below with its reason, not a failure
                else:
                    to_replace.append((category, s))
            else:
                print(f"   slot {category} {s}: not published, and neither its cap nor a guard refused it -> FAIL")
                failures.append(f"named slot {s} unpublished without a cap or guard refusal")
    taken = {s for names_ in kept.values() for s in names_} | {s for _, s in to_replace}
    slots: list[tuple[str, str]] = [(c, s) for c in SLOTS for s in kept[c]]
    for category, s in to_replace:
        pool = sorted(
            (i for i in canonical if positive(i) and category_of[category](i) and symbols.get(i) not in taken),
            key=cap,
            reverse=True,
        )
        sub = symbols.get(pool[0]) if pool else None
        print(f"   slot {category} {s}: original outcome {refusal(by_symbol.get(s))}; replaced by {sub}")
        if sub is None:
            print(f"   slot {category} {s}: no replacement available; counts as unreconciled")
        else:
            slots.append((category, sub))
            taken.add(sub)
    both = {
        i: _dec(control[i]["dividend_yield"]) / _dec(treat[i]["dividend_yield"])  # type: ignore[operator]
        for i in canonical
        if positive(i) and (_dec(control[i]["dividend_yield"]) or 0) > 0
    }
    span = {i: (qb[i][-1][1] - qb[i][0][0]).days + 1 for i in canonical}
    untagged = sorted(
        (
            i
            for i, x in both.items()
            if not BAND[0] <= x <= BAND[1]
            and inventory_cause(windows[i], i in ratio and not TIGHT[0] <= ratio[i] <= TIGHT[1], span[i] > 365)
            == "untagged"
        ),
        key=lambda i: abs(both[i].ln()),
        reverse=True,
    )
    for i in [i for i in untagged if symbols.get(i) not in taken][:UNTAGGED_SLOTS]:
        slots.append(("untagged, largest |log(today / proposed)|", symbols.get(i, str(i))))
        taken.add(symbols.get(i, str(i)))
    # Amendment 2: the mandatory boundary slot, outside the 10 of 12: the largest-cap positive publication whose window
    # spans 364 days (a 52/53-week filer the guard retains). It must reconcile.
    # Build note: two names, at least one reconciled, as every other category is covered; a single name fails on any
    # dividend change after its last filed quarter, which item 4 keeps in the residual (NVDA, 2026-10-20).
    boundary_pool = sorted(
        (i for i in canonical if positive(i) and span[i] == 364 and symbols.get(i) not in taken), key=cap, reverse=True
    )
    boundary = {symbols.get(i, str(i)) for i in boundary_pool[:BOUNDARY_SLOTS]}
    if not boundary:
        print("   boundary slots: no 364-day publication available -> FAIL")
        failures.append("boundary slots unavailable")
    for i in boundary_pool[:BOUNDARY_SLOTS]:
        slots.append((BOUNDARY, symbols.get(i, str(i))))
        taken.add(symbols.get(i, str(i)))
    print(f"   reference: {REFERENCE_SOURCE}; slots {slots}")
    reconciled: Counter[str] = Counter()
    reconciled_names: set[str] = set()
    available: set[str] = set()
    for category, s in slots:
        iid = by_symbol.get(s)
        if iid is None or not positive(iid):
            reason = "no complete-TTM row" if iid is None else refusal(iid)
            print(f"   {category} {s}: refused ({reason}); reported, not a failure")
            continue
        available.add(category)
        if s not in REFERENCE:
            print(f"   {category} {s}: no reference entry")
            failures.append(f"reference {s} missing")
            continue
        p_x, read_on, e = REFERENCE[s]
        t = treat[iid]
        v, n_cap, ours = _dec(t["valuation_price"]), _dec(t["market_cap_live"]), _dec(t["dividend_yield"])
        assert v is not None and n_cap is not None and ours is not None
        n = n_cap / v
        d = sum((q["dividends_paid"] for q in windows[iid]), Decimal(0))
        ext = e / p_x * 100
        r = ours / ext
        facts = filings.get(iid, {})
        nf = filed_basic_shares(facts, windows[iid][-1]["period_end_date"]) if facts else None
        total, branches, _, unres = rebuilt.get(iid, (Decimal(0), [], set(), 1))
        c = total if not unres and total > 0 else None
        rebuild_state = "unresolved" if unres else "resolved zero" if total == 0 else "resolved"
        print(
            f"   {category} {s}: ours {ours:.4f}% (session {t['session']}, V {v}, N {n:.0f}, D {d}) | external "
            f"{ext:.4f}% ({read_on}, P_x {p_x}, E {e}) | ratio {r:.4f}"
        )
        if nf is None:
            print("      N_f unresolved")
            failures.append(f"reference {s}: N_f unresolved")
            continue
        n_f = nf[0]
        price_f, count_f = p_x / v, n_f / n
        if c is not None:
            comp_f, rest = d / c, c / (n_f * e)
            factors = f"(P_x/V) {price_f:.4f} x (N_f/N) {count_f:.4f} x (D/C) {comp_f:.4f} x (C/(N_f E)) {rest:.4f}"
            identity = price_f * count_f * comp_f * rest
        else:
            comp_f, rest = None, d / (n_f * e)
            factors = (
                f"(P_x/V) {price_f:.4f} x (N_f/N) {count_f:.4f} x (D/(N_f E)) {rest:.4f}; C {rebuild_state} {branches}"
            )
            identity = price_f * count_f * rest
        print(f"      {factors}; N_f {n_f:.0f} ({nf[1]})")
        if abs(identity - r) > Decimal("1e-9") * abs(r):
            failures.append(f"identity {s}")
        if abs(r - 1) <= REFERENCE_TOLERANCE:
            reconciled[category] += 1
            reconciled_names.add(s)
            continue
        adjustment = Decimal(1)
        notes: list[str] = []
        pit: dict[str, Any] = t["market_cap_pit"] or {}
        filed, reported, split = (
            filed_count(facts, pit),
            _dec(pit.get("shares_reported")),
            _dec(pit.get("split_factor")),
        )
        # Round 2, finding 9: N must also be the count the PIT row claims, shares_reported x split_factor.
        if (
            filed is not None
            and reported is not None
            and split == 1
            and abs(reported - filed) <= HALF_PCT * filed
            and abs(n - reported * split) <= HALF_PCT * n
        ):
            adjustment *= count_f
            notes.append("count")
        count_valid = "count" in notes
        checks = member.get(iid, [])
        member_defect = any(m[1] == "DIFFER" for m in checks)
        all_agree = len(checks) == 4 and all(m[1] == "agree" for m in checks)
        if comp_f is not None and all_agree:
            adjustment *= comp_f
            notes.append("composition")
        residual = r / adjustment
        # Amendment 1, finding 3, computed afresh: the miss is classified a numerator proxy defect, never reconciled,
        # when every member agrees with the rule, one is on the generic concept, the stored total is outside
        # [0.95, 1.05] of the filed common cash declared C_d, and the filing's C_d per filed share corroborates the
        # external E: (P_x / V) x (N_f / N unless divided out) x C_d / (N_f E) within the tolerance. Our D is then the
        # wrong operand by D / C_d, which is printed; its size is not bounded, because it is not a pass.
        # Rounds 2-3, finding 1: declared is not paid. With a payment lag of up to one quarter, the window pays the
        # preceding quarter's declaration and not its own last one, so timing alone moves D by at most the larger of
        # those; the bound takes the largest declared amount over the preceding quarter and the window's four.
        proxy_residual = None
        if (
            all_agree
            and iid in generic
            and iid in proxy
            and not TIGHT[0] <= proxy[iid] <= TIGHT[1]
            and iid in declared_prev
            and abs(d - declared[iid]) > max([declared_prev[iid], *declared_q[iid]])
        ):
            proxy_residual = price_f * (1 if count_valid else count_f) * declared[iid] / (n_f * e)
        # Round 2, finding 10: a member defect accounts for the miss only by its own size: D is corrected to the rule's
        # values, the eligible factors are re-applied on it, and the residual must be inside the tolerance.
        defect_residual = None
        if member_defect and all(m[1] in ("agree", "DIFFER") for m in checks):
            dc = corrected(iid)
            adj = count_f if count_valid else Decimal(1)
            if c is not None and dc > 0:
                adj *= dc / c
            defect_residual = r * dc / d / adj if d else None
        # Amendment 2 build note: a filing lag. D is the cash of a window that ends at the last filed quarter; when
        # the page's amounts change after that end, E over the 365 days to the window's own end (E_w) is the external
        # counterpart of D. Classified when E_w differs from E and ours x E / E_w (count divided out where valid) lies
        # within the tolerance; never reconciled, never counted. Computed only where the stored list covers the span.
        lag_residual = None
        w_end = windows[iid][-1]["period_end_date"]
        if w_end - timedelta(days=364) >= REFERENCE_LISTED_FROM.get(s, DEFAULT_LISTED_FROM):
            e_w = external_ttm(REFERENCE_PAGES[s][1], w_end)
            if e_w > 0 and e_w != e:
                lag_residual = r * e / e_w / (count_f if count_valid else Decimal(1))
        if abs(residual - 1) <= REFERENCE_TOLERANCE:
            verdict = "accounted"
            reconciled[category] += 1
            reconciled_names.add(s)
        elif defect_residual is not None and abs(defect_residual - 1) <= REFERENCE_TOLERANCE:
            verdict = f"INPUT DEFECT (members corrected to the rule: residual {defect_residual:.4f}; 'Input policy')"
        elif lag_residual is not None and abs(lag_residual - 1) <= REFERENCE_TOLERANCE:
            verdict = (
                f"FILING LAG (Amendment 2 build note): window ends {w_end}; E over the 365 days to it "
                f"{external_ttm(REFERENCE_PAGES[s][1], w_end)} against E {e}; residual {lag_residual:.4f}"
            )
        elif proxy_residual is not None and abs(proxy_residual - 1) <= REFERENCE_TOLERANCE:
            verdict = (
                f"NUMERATOR PROXY DEFECT (Amendment 1): D / C_d {proxy[iid]:.4f} ({GENERIC} member, C_d "
                f"{DECLARED_COMMON} {declared[iid]}); filing corroborates external at {proxy_residual:.4f}"
            )
        else:
            verdict = "FAIL"
            failures.append(f"reference {s} unreconciled")
        print(f"      miss {r - 1:+.1%}; divided out {notes or 'none'}; residual {residual:.4f} -> {verdict}")
    total_reconciled = len(reconciled_names - boundary)
    uncovered = sorted(available - set(reconciled) - {BOUNDARY})
    print(
        f"   reconciled {total_reconciled} of {sum(1 for c, _ in slots if c != BOUNDARY)} slots; by category "
        f"{dict(reconciled)}; categories with an available name not reconciled {uncovered}"
    )
    if total_reconciled < MIN_RECONCILED or uncovered:
        failures.append("minimum coverage not met: rerun after the next 08:20Z build")
    if boundary and not boundary & reconciled_names:
        print(f"   boundary slots {sorted(boundary)}: none reconciled -> FAIL")
        failures.append("boundary category not reconciled")

    # 5. readers
    print("\n5. ranking_pot_exposure.dividend_yield over the latest snapshot universes")
    seen = set()
    for snap in snapshots:
        seen.add(snap["strategy_id"])
        try:
            inputs = decode_snapshot(snap["snapshot"])
        except Exception as exc:  # report a snapshot this decoder cannot read; it does not fail the acceptance
            print(f"   {snap['strategy_id']} attempt {snap['attempt_id']}: undecodable ({exc})")
            continue
        moves: Counter[str] = Counter()
        for f in inputs.facts:
            close = exposure.usable_closes(f.bar_dates, f.bar_rows).get(inputs.last_session)
            got = []
            for rows in (control, treat):
                row = rows.get(f.instrument_id) or {}
                g = replace(
                    f,
                    dps_ttm=_dec(row.get("dps_declared_ttm")),
                    valuation_yield_pct=_dec(row.get("dividend_yield")),
                    valuation_price=_dec(row.get("current_price")),
                )
                got.append(exposure.dividend_yield(g, close))
            a, b = got
            moves["unchanged" if a == b else "gained" if a is None else "lost" if b is None else "changed"] += 1
        print(
            f"   {snap['strategy_id']} attempt {snap['attempt_id']} ({snap['fired_at']:%Y-%m-%d}, D "
            f"{inputs.last_session}): {len(inputs.facts)} names; {dict(moves)}"
        )
    for sid in ("ranking-pot-v1", "ranking-pot-v2"):
        if sid not in seen:
            print(f"   {sid}: no stored snapshot")

    # 6. latency
    print("\n6. latency (best of n)")
    for name, c_s in arms["control"]["latency"].items():
        t_s = arms["treatment"]["latency"][name]
        ratio_l = t_s / c_s if c_s else float("inf")
        print(f"   {name}: control {c_s * 1000:.1f} ms, treatment {t_s * 1000:.1f} ms, ratio {ratio_l:.2f}")
        if ratio_l > LATENCY_BOUND:
            failures.append(f"latency {name}")

    print("\nRESULT:", "PASS" if not failures else "FAIL " + "; ".join(failures))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
