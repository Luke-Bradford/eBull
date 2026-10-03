"""#3592 premise: does the v2 order pick different entrants from v1's on today's inputs? (read-only)

    PYTHONPATH=. uv run python -m scripts.measure_3592_v2_entrant_overlap [--n 25]

Reads one rebalance's inputs exactly as ranking-pot-v1 would (``ranking_pot_rebalance.read_snapshot_inputs`` inside
``begin_rebalance``'s transaction, which is then rolled back), with S₀ = the latest committed ``v1.5-balanced`` run's
names and that run's scores (no new scoring run, no thesis provenance). It derives R and F with v1's
``universes_of`` and compares the entrants of an empty book (F-rank ≤ N and R-rank ≤ 2N) under:

- v1: ``ranking_pot.real_order`` (the v1.5 score);
- v2: the spec's composite (``docs/proposals/execution/2026-10-03-3592-ranking-pot-v2.md`` §5): the mean of the
  midrank percentiles within R of the v1.5 score, minus FINRA days-to-cover, and the CMP (2012) opportunistic-purchase
  indicator, a missing component taking 1/2.

It prints the counts behind each factor inside R and the two entrant sets' overlap. ⚠ ``quotes`` is a current-row
table, so the F figures are a dated snapshot. Nothing is written: the transaction rolls back.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any

import psycopg

from app.config import settings
from app.services import ranking_pot as pot
from app.services import ranking_pot_rebalance as rb
from app.services.scoring import _DEFAULT_MODEL_VERSION


def midrank_percentiles(values: Mapping[int, Fraction], members: frozenset[int]) -> dict[int, Fraction]:
    """(midrank − 1/2) / n over the members holding a value, ascending (higher value → higher percentile);
    a member with no value takes 1/2."""
    have = sorted((v, iid) for iid, v in values.items() if iid in members)
    n = len(have)
    out: dict[int, Fraction] = dict.fromkeys(members, Fraction(1, 2))
    i = 0
    while i < n:
        j = i
        while j + 1 < n and have[j + 1][0] == have[i][0]:
            j += 1
        mid = Fraction(i + 1 + j + 1, 2)
        for k in range(i, j + 1):
            out[have[k][1]] = (mid - Fraction(1, 2)) / n
        i = j + 1
    return out


def month_back(d: date, months: int) -> date:
    """The first day of the month ``months`` before ``d``'s month."""
    k = d.year * 12 + d.month - 1 - months
    return date(k // 12, k % 12 + 1, 1)


def opportunistic_buyers(
    conn: psycopg.Connection[Any], ids: list[int], target: date, as_of: datetime, months: int
) -> set[int]:
    """Names with ≥ 1 open-market purchase in the ``months`` calendar months before ``target``'s month by an
    (insider, issuer) pair CMP (2012) classifies opportunistic at the start of that purchase's calendar year (the
    three preceding years), the filing known at ``as_of``."""
    window_end = target.replace(day=1)
    first = month_back(target, months)
    rows = conn.execute(
        """
        WITH buys AS (
            SELECT DISTINCT t.instrument_id, f.issuer_cik, t.filer_cik, extract(year FROM t.txn_date)::int AS by
              FROM insider_transactions t
              JOIN insider_filings f ON f.accession_number = t.accession_number
              JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
             WHERE t.instrument_id = ANY(%(ids)s) AND t.filer_cik IS NOT NULL AND f.issuer_cik IS NOT NULL
               AND t.txn_code = 'P' AND NOT t.is_derivative AND t.acquired_disposed_code = 'A'
               AND NOT t.txn_date_invalid AND t.txn_date >= %(first)s AND t.txn_date < %(end)s
               AND m.filed_at <= %(as_of)s
        ), hist AS (
            -- CMP classify at the start of the purchase's calendar year: only history filed before it counts.
            SELECT DISTINCT b.issuer_cik, b.filer_cik, b.by,
                   extract(year FROM h.txn_date)::int AS y, extract(month FROM h.txn_date)::int AS mo
              FROM (SELECT DISTINCT issuer_cik, filer_cik, by FROM buys) b
              JOIN insider_filings hf ON hf.issuer_cik = b.issuer_cik
              JOIN insider_transactions h ON h.accession_number = hf.accession_number AND h.filer_cik = b.filer_cik
              JOIN sec_filing_manifest hm ON hm.accession_number = h.accession_number
             WHERE ((h.txn_code = 'P' AND h.acquired_disposed_code = 'A')
                    OR (h.txn_code = 'S' AND h.acquired_disposed_code = 'D'))
               AND NOT h.is_derivative AND NOT h.txn_date_invalid
               AND extract(year FROM h.txn_date)::int BETWEEN b.by - 3 AND b.by - 1
               AND hm.filed_at < make_timestamptz(b.by, 1, 1, 0, 0, 0, 'UTC')
        ), pairs AS (
            SELECT issuer_cik, filer_cik, by,
                   count(DISTINCT y) = 3 AS classified,
                   EXISTS (SELECT 1 FROM hist h2
                            WHERE h2.issuer_cik = hist.issuer_cik AND h2.filer_cik = hist.filer_cik
                              AND h2.by = hist.by
                            GROUP BY h2.mo HAVING count(DISTINCT h2.y) = 3) AS routine
              FROM hist GROUP BY issuer_cik, filer_cik, by
        )
        SELECT DISTINCT b.instrument_id
          FROM buys b JOIN pairs p USING (issuer_cik, filer_cik, by)
         WHERE p.classified AND NOT p.routine
        """,
        {"ids": ids, "first": first, "end": window_end, "as_of": as_of},
    ).fetchall()
    return {int(r[0]) for r in rows}


def days_to_cover(
    conn: psycopg.Connection[Any], ids: list[int], as_of: datetime
) -> tuple[date | None, dict[int, Fraction]]:
    """S*: the latest settlement date (not after ``as_of``'s date) with a row for any of ``ids`` observed by
    ``as_of`` (``known_from``); each name's latest observed revision at S*, missing when NULL, non-finite or < 0."""
    row = conn.execute(
        """
        SELECT max(settlement_date) FROM finra_short_interest_observations
         WHERE instrument_id = ANY(%s) AND known_from <= %s AND settlement_date <= %s::date
        """,
        (ids, as_of, as_of),
    ).fetchone()
    settle = None if row is None else row[0]
    if settle is None:
        return None, {}
    rows = conn.execute(
        """
        SELECT DISTINCT ON (instrument_id) instrument_id, days_to_cover
          FROM finra_short_interest_observations
         WHERE settlement_date = %s AND instrument_id = ANY(%s) AND known_from <= %s
         ORDER BY instrument_id, known_from DESC, source_document_id DESC
        """,
        (settle, ids, as_of),
    ).fetchall()
    out: dict[int, Fraction] = {}
    for iid, v in rows:
        if v is not None and Decimal(str(v)).is_finite() and Decimal(str(v)) >= 0:
            out[int(iid)] = Fraction(Decimal(str(v)))
    return settle, out


def spearman(a: Mapping[int, Fraction], b: Mapping[int, Fraction], ids: frozenset[int]) -> float:
    """Pearson correlation of two percentile maps over ``ids`` (Spearman when both are midrank percentiles)."""
    xs = [float(a[i]) for i in sorted(ids)]
    ys = [float(b[i]) for i in sorted(ids)]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
    vx = sum((x - mx) ** 2 for x in xs) ** 0.5
    vy = sum((y - my) ** 2 for y in ys) ** 0.5
    return cov / (vx * vy) if vx and vy else float("nan")


def entrants(universes: pot.Universes, order: tuple[int, ...], n: int) -> list[int]:
    r = pot.ranks(universes, order)
    return [iid for iid in order if iid in universes.f_ids and r.f_rank[iid] <= n and r.r_rank[iid] <= 2 * n]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=pot.N)
    parser.add_argument("--insider-months", type=int, default=6)
    args = parser.parse_args()
    as_of = datetime.now(UTC)
    with psycopg.connect(settings.database_url) as conn, conn.transaction(force_rollback=True):
        rb.begin_rebalance(conn)
        scored = conn.execute(
            "SELECT max(scored_at) FROM scores WHERE model_version = %s", (_DEFAULT_MODEL_VERSION,)
        ).fetchone()
        assert scored is not None and scored[0] is not None
        s0 = [
            int(r[0])
            for r in conn.execute(
                "SELECT DISTINCT instrument_id FROM scores WHERE model_version = %s AND scored_at = %s ORDER BY 1",
                (_DEFAULT_MODEL_VERSION, scored[0]),
            ).fetchall()
        ]
        decl = rb.PotDeclaration(0, {"s0": {"instrument_ids": s0}}, "", as_of, None)
        read = rb.read_snapshot_inputs(conn, decl, as_of=as_of, scored_at=scored[0], theses={})
        universes = rb.universes_of(read.inputs)
        if isinstance(universes, str):
            raise SystemExit(f"universes: {universes}")
        target = rb.target_session(as_of)
        r_ids = universes.r_ids
        buyers = opportunistic_buyers(conn, s0, target, as_of, args.insider_months)
        settle, dtc = days_to_cover(conn, s0, as_of)

        u_score = midrank_percentiles({i: Fraction(universes.own_score[i]) for i in r_ids}, r_ids)
        u_dtc = midrank_percentiles({i: -v for i, v in dtc.items()}, r_ids)
        u_ins = midrank_percentiles({i: Fraction(int(i in buyers)) for i in r_ids}, r_ids)
        exact = {i: (u_score[i] + u_dtc[i] + u_ins[i]) / 3 for i in r_ids}
        composite = {i: Decimal(c.numerator) / Decimal(c.denominator) for i, c in exact.items()}

        v1 = entrants(universes, pot.real_order(universes), args.n)
        v2 = entrants(universes, pot.rank_order(r_ids, composite.get), args.n)
        print(f"as_of {as_of:%Y-%m-%d %H:%MZ}; scores run {scored[0]}; target session {target}")
        print(f"S0 {len(s0)} · R {len(r_ids)} · F {len(universes.f_ids)}")
        print(
            f"opportunistic buyers (purchases {month_back(target, args.insider_months)} .. {target.replace(day=1)}): "
            f"S0 {len(buyers)} · R {len(buyers & r_ids)} · F {len(buyers & universes.f_ids)}"
        )
        print(f"days-to-cover at settlement {settle}: S0 {len(dtc)} · R {len(r_ids & dtc.keys())}")
        print(f"entrants (empty book, N={args.n}): v1 {len(v1)} · v2 {len(v2)} · both {len(set(v1) & set(v2))}")
        print(f"  v2 entrants with an opportunistic buy: {len(set(v2) & buyers)}; v1's: {len(set(v1) & buyers)}")
        print(f"rank correlation within R: score vs -DTC {spearman(u_score, u_dtc, r_ids):+.3f} (missing DTC at 1/2)")
        observed = frozenset(r_ids & dtc.keys())
        o_score = midrank_percentiles({i: Fraction(universes.own_score[i]) for i in observed}, observed)
        o_dtc = midrank_percentiles({i: -dtc[i] for i in observed}, observed)
        print(f"  observed-only ({len(observed)} names): {spearman(o_score, o_dtc, observed):+.3f}")
        rb_ids = buyers & r_ids
        if rb_ids:
            mean_u = sum(u_score[i] for i in rb_ids) / len(rb_ids)
            print(f"  mean score percentile of R's opportunistic buyers: {float(mean_u):.3f} (R mean 0.5)")


if __name__ == "__main__":
    main()
