"""#2214 — full-population A/B of migration 419 (retire the 13F-manager ETF overrides).

Population: every instrument holding an ``ownership_institutions_current`` row the migration
re-types (``filer_type = 'ETF'`` on one of its CIKs) — the only rows it can reach. One
REPEATABLE READ transaction: render every instrument (control), apply the migration's SQL,
render every instrument again (treatment), then ROLLBACK, so nothing is written.

The invariant is a split, not a re-count: per instrument, ``institutions + etfs`` shares and
the pie total must be unchanged, and ``etfs`` must reach zero. That zero is global on purpose:
#2214's decision is that NO 13F filer is typed ``ETF``, so any ETF-typed row left after the
migration is a failure whoever it belongs to. Checked on both read paths that
bucket by ``filer_type``: the ownership rollup, and ``/instruments/{symbol}/institutional-holdings``
(``_ENDPOINT_SPLIT_SQL`` mirrors its totals query at ``app/api/instruments.py``, every instrument
at its own latest ``period_of_report``).

    PYTHONPATH=. uv run python -m scripts.ab_2214_retire_etf_seeds
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.ownership_rollup import get_ownership_rollup

_MIGRATION = Path(__file__).resolve().parent.parent / "sql" / "419_retire_13f_manager_etf_seeds.sql"
_CIKS = ["0001214717", "0000102909", "0001086364"]

_ENDPOINT_SPLIT_SQL = """
WITH latest AS (
    SELECT instrument_id, MAX(period_of_report) AS p FROM institutional_holdings GROUP BY 1
)
SELECT h.instrument_id,
       COALESCE(SUM(h.shares) FILTER (WHERE f.filer_type = 'ETF'), 0) AS etfs,
       COALESCE(SUM(h.shares) FILTER (
           WHERE f.filer_type IN ('INV','INS','BD','OTHER') OR f.filer_type IS NULL), 0) AS inst
  FROM institutional_holdings h
  JOIN latest l ON l.instrument_id = h.instrument_id AND l.p = h.period_of_report
  JOIN institutional_filers f USING (filer_id)
 WHERE h.is_put_call IS NULL
 GROUP BY 1
"""


def _view(conn: psycopg.Connection[Any], symbol: str, iid: int) -> tuple[Decimal, Decimal, Decimal, bool]:
    # main() runs at REPEATABLE READ and both arms share one rolled-back transaction, so this
    # render reads one snapshot. snapshot_read() would COMMIT the open transaction first.
    # ownership-rollup-snapshot: caller-owned — see above
    r = get_ownership_rollup(conn, symbol, iid)
    by = {s.category: s.total_shares for s in r.slices}
    pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
    return by.get("institutions", Decimal(0)), by.get("etfs", Decimal(0)), pie, r.residual.oversubscribed


def main() -> int:
    sql = _MIGRATION.read_text(encoding="utf-8")
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SET jit = off")
        rows = conn.execute(
            "SELECT DISTINCT c.instrument_id, i.symbol FROM ownership_institutions_current c"
            " JOIN instruments i USING (instrument_id)"
            " WHERE c.filer_type = 'ETF' AND c.filer_cik = ANY(%(ciks)s) ORDER BY 1",
            {"ciks": _CIKS},
        ).fetchall()
        print(f"instruments carrying a re-typed _current row    {len(rows):>8,}")

        # Population, control, migration and treatment share ONE snapshot: nothing ingested
        # mid-run can enter the migration's reach without entering the population.
        try:
            control = {iid: _view(conn, sym, iid) for iid, sym in rows}
            ep_a = {r[0]: (r[1], r[2]) for r in conn.execute(_ENDPOINT_SPLIT_SQL).fetchall()}
            conn.execute(sql)  # type: ignore[arg-type]  # a trusted, checked-in migration file
            treated = {iid: _view(conn, sym, iid) for iid, sym in rows}
            ep_b = {r[0]: (r[1], r[2]) for r in conn.execute(_ENDPOINT_SPLIT_SQL).fetchall()}
        finally:
            conn.rollback()

        sum_moved = total_moved = etfs_left = oversub_flip = etfs_before = inst_up = 0
        shifted = Decimal(0)
        for iid, sym in rows:
            ia, ea, pa, oa = control[iid]
            ib, eb, pb, ob = treated[iid]
            etfs_before += ea > 0
            inst_up += ib > ia
            shifted += ea - eb
            sum_moved += (ia + ea) != (ib + eb)
            total_moved += pa != pb
            etfs_left += eb != 0
            oversub_flip += oa != ob
            if (ia + ea) != (ib + eb) or pa != pb:
                print(f"  MOVED {sym:<10} inst+etfs {ia + ea:,.0f} -> {ib + eb:,.0f}  pie {pa:,.0f} -> {pb:,.0f}")

        print(f"rendering a non-zero etfs wedge before           {etfs_before:>8,}")
        print(f"institutions wedge grew                          {inst_up:>8,}")
        print(f"shares moved etfs -> institutions                {shifted:>16,.0f}")
        print(f"institutions + etfs changed                      {sum_moved:>8,}   (must be 0)")
        print(f"pie total changed                                {total_moved:>8,}   (must be 0)")
        print(f"etfs wedge non-zero after                        {etfs_left:>8,}   (must be 0)")
        print(f"oversubscribed flag flipped                      {oversub_flip:>8,}   (must be 0)")

        ep_etf_before = sum(1 for e, _ in ep_a.values() if e > 0)
        ep_etf_shares = sum((e - ep_b.get(k, (Decimal(0), Decimal(0)))[0] for k, (e, _) in ep_a.items()), Decimal(0))
        ep_sum_moved = sum(1 for k in ep_a.keys() | ep_b.keys() if sum(ep_a.get(k, (0, 0))) != sum(ep_b.get(k, (0, 0))))
        ep_etf_after = sum(1 for e, _ in ep_b.values() if e > 0)
        print("\n=== /institutional-holdings totals, every instrument at its latest period ===")
        print(f"instruments                                      {len(ep_a):>8,}")
        print(f"non-zero etfs_shares before                      {ep_etf_before:>8,}")
        print(f"etfs_shares moved to institutions_shares         {ep_etf_shares:>16,.0f}")
        print(f"institutions + etfs changed                      {ep_sum_moved:>8,}   (must be 0)")
        print(f"non-zero etfs_shares after                       {ep_etf_after:>8,}   (must be 0)")
        if ep_sum_moved or ep_etf_after:
            return 1
        return 1 if (sum_moved or total_moved or etfs_left or oversub_flip) else 0


if __name__ == "__main__":
    sys.exit(main())
