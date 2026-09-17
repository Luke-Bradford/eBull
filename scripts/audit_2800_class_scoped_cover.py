"""#2800 — measure the class of issuers whose ``dei`` cover share count is scoped
to ONE class instead of the entity.

#2800 found `AVAL`'s stored denominator (`dei:EntityCommonStockSharesOutstanding`,
undimensioned) carrying the PREFERENCE class alone — ~3.15x too small — and asked
for the class to be measured before any fix is designed. This script is that
measurement. It proposes nothing and writes nothing.

Discriminator
-------------
The cover tag itself cannot answer it. Measured on the cached quarter
``fsds_2026q1``: ``EntityCommonStockSharesOutstanding`` appears **12 times in
3,690,955 num.txt rows**, so plain FSDS does not carry the cover page at
population scale (it is a companyfacts-sourced value for us — sec-edgar §7.17).

What FSDS *does* carry at scale is ``us-gaap:CommonStockSharesOutstanding``
segmented by ``ClassOfStock`` (15,435 segmented rows in that one quarter). So the
class-scoped cover value is detectable by COMPARISON:

    stored dei cover  vs  Σ FSDS per-class counts for the same CIK and instant

An issuer whose cover value is entity-wide ties out to the sum. `AVAL`'s shape is
a cover value that instead matches ONE member and sits materially below the sum.

⛔ WHAT THIS SHAPE IS NOT (the verdict this script produced)
-----------------------------------------------------------
It is **not** a defect signature, and #2800's inference does not survive it. 26
in-universe instruments carry the shape, and a cover page is *supposed* to report
the **registered** class: `MBLY` (`0001104659-26-086264`) shows 252,419,583
against a 12(b) security of "Class A common stock", its Class B being Intel-held
and unregistered. `AVAL` itself is the same — its FY2025 20-F registers
"American Depositary Shares, each representing 20 **preferred** shares", so the
preference-scoped cover value is the class-MATCHED denominator, and substituting
the entity-wide 23,743,475,754 would understate every percentage by 3.15x.

A denominator is right or wrong only relative to its NUMERATOR. The surviving
`AVAL` defect is a **unit** mismatch, not a class one: the 13F numerators are
ADSs while the denominator is preferred shares at 20 per ADS, and `ads_ratio`
(5 rows) has no `AVAL` row — #2117/#2136's family, not this ticket's.

So the output below is a **census**, not a defect list. To turn a row into a
finding you must show the cover's class differs from the instrument's own 12(b)
registered security; `dei:Security12bTitle` is the field, and we do not store it
(the only `dei` concepts in `financial_facts_raw` are
`EntityCommonStockSharesOutstanding`, `EntityPublicFloat`, `EntityNumberOfEmployees`).

Bands are **I19's**, not invented here: agrees ≤2% / minor_skew 2-5% /
diverges >5% (`_classify_cross_check`, #1647 pt5).

Reuse
-----
``fsds_class_shares.parse_class_member`` / ``read_fsds_sub`` / ``iter_fsds_num``
are the settled readers for this archive and are used verbatim. In particular
``parse_class_member`` REJECTS a multi-axis cell
(``ClassOfStock=X;EquityComponents=Y;``) — a real trap here, because the same
member appears at two scales in one filing (CIK 0000008063, ddate 20251231:
``ClassOfStock=CommonClassB;EquityComponents=CommonStock;`` = 3,966,000 against
``ClassOfStock=CommonClassB;`` = 39,658). Summing both would be meaningless.

The stored-denominator read mirrors ``ownership_rollup._read_shares_outstanding_near``
(NEAREST period, not latest) so the two sides are compared at one instant.

Usage
-----
    PYTHONPATH=. uv run python -m scripts.audit_2800_class_scoped_cover
    PYTHONPATH=. uv run python -m scripts.audit_2800_class_scoped_cover --limit 50
"""

from __future__ import annotations

import argparse
import collections
import zipfile
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.fsds_class_shares import iter_fsds_num, parse_class_member, read_fsds_sub
from app.services.sec_bulk_orchestrator_jobs import _bulk_dir

COVER_CONCEPT = "EntityCommonStockSharesOutstanding"
CLASS_TAG = "CommonStockSharesOutstanding"

# I19 bands (#1647 pt5, ownership_rollup._classify_cross_check). Not chosen here.
AGREE_PCT = Decimal("2")
MINOR_PCT = Decimal("5")


@dataclass(frozen=True)
class ClassGroup:
    """Per-class FSDS counts for one (cik, instant) with >= 2 distinct members."""

    cik: str
    ddate: date
    members: dict[str, Decimal]

    @property
    def total(self) -> Decimal:
        return sum(self.members.values(), Decimal(0))


def _read_class_groups(zip_paths: list[Path]) -> dict[tuple[str, date], ClassGroup]:
    """``{(cik, ddate): ClassGroup}`` across every cached FSDS archive.

    Only rows whose ``ddate`` IS the filing's own reporting period are kept —
    the same current-period filter the production ingest applies, so a
    comparative prior-year column cannot enter the sum.

    ⚠⚠ Members are accumulated **per accession first**, and ONE filing is then
    chosen per (cik, instant). Merging members across filings produces a total
    **no filing reports**: CIK ``0002007825`` at 2025-09-30 appears in both
    cached quarters, and a per-member merge kept a ``CommonClassB`` value from
    ``fsds_2025q4`` alongside the replacement members from ``fsds_2026q1``,
    adding 10,350,000 phantom shares to the sum — which then drives both the
    verdict and the ratio. Caught at Codex checkpoint 2, not by any gate.
    The winner is the latest ``filed`` date, ``adsh`` breaking a tie.
    """
    per_filing: dict[tuple[str, date, str], dict[str, Decimal]] = collections.defaultdict(dict)
    filed_at: dict[str, date | None] = {}
    for zip_path in zip_paths:
        with zipfile.ZipFile(zip_path) as zf:
            subs = read_fsds_sub(zf)
            for row in iter_fsds_num(zf):
                if row.get("tag") != CLASS_TAG or row.get("uom") != "shares":
                    continue
                member = parse_class_member(row.get("segments") or "")
                if member is None:
                    continue
                adsh = row.get("adsh") or ""
                sub = subs.get(adsh)
                if sub is None or sub.period is None:
                    continue
                ddate_raw = row.get("ddate") or ""
                if len(ddate_raw) != 8 or not ddate_raw.isdigit():
                    continue
                ddate = date(int(ddate_raw[0:4]), int(ddate_raw[4:6]), int(ddate_raw[6:8]))
                if ddate != sub.period:
                    continue
                try:
                    value = Decimal(row.get("value") or "")
                except ArithmeticError:
                    continue
                if value <= 0:
                    continue
                per_filing[(sub.cik, ddate, adsh)][member] = value
                filed_at[adsh] = sub.filed

    winners: dict[tuple[str, date], tuple[tuple[date, str], dict[str, Decimal]]] = {}
    for (cik, ddate, adsh), members in per_filing.items():
        rank = (filed_at.get(adsh) or date.min, adsh)
        key = (cik, ddate)
        incumbent = winners.get(key)
        if incumbent is None or rank > incumbent[0]:
            winners[key] = (rank, members)
    return {k: ClassGroup(cik=k[0], ddate=k[1], members=v) for k, (_rank, v) in winners.items() if len(v) >= 2}


def _instruments_by_cik(conn: psycopg.Connection[Any], ciks: set[str]) -> dict[str, list[tuple[int, str]]]:
    """``{cik: [(instrument_id, symbol), ...]}`` — one-to-MANY by design.

    A CIK backing several in-universe instruments (share-class siblings, ADR +
    ordinary) must return ALL of them; the data-engineer skill records that a
    ``DISTINCT ON (cik)`` resolver silently drops the siblings.
    """
    out: dict[str, list[tuple[int, str]]] = collections.defaultdict(list)
    if not ciks:
        return out
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.cik, p.instrument_id, i.symbol
              FROM instrument_sec_profile p
              JOIN instruments i USING (instrument_id)
             WHERE p.cik = ANY(%(ciks)s::text[])
            """,
            {"ciks": sorted(ciks)},
        )
        for cik, instrument_id, symbol in cur.fetchall():
            out[str(cik)].append((int(instrument_id), str(symbol)))
    return out


def _stored_cover(conn: psycopg.Connection[Any], instrument_id: int, near: date) -> tuple[Decimal, date] | None:
    """The dei cover value NEAREST ``near`` — mirrors the rollup's own rule."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT val, period_end
              FROM financial_facts_raw
             WHERE instrument_id = %(iid)s
               AND taxonomy = 'dei'
               AND concept = %(concept)s
               AND unit = 'shares'
               AND val IS NOT NULL
             ORDER BY ABS(period_end - %(near)s::date) ASC, filed_date DESC
             LIMIT 1
            """,
            {"iid": instrument_id, "concept": COVER_CONCEPT, "near": near},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return Decimal(row[0]), row[1]


def _stored_usgaap_total(conn: psycopg.Connection[Any], instrument_id: int, near: date) -> tuple[Decimal, date] | None:
    """The UNDIMENSIONED ``us-gaap:CommonStockSharesOutstanding`` nearest ``near``.

    A second witness to the FSDS class sum that does not come from FSDS. It is
    the balance-sheet-parenthetical count companyfacts DOES return (it has a
    non-dimensional default), so where it exists it corroborates — or refutes —
    the sum independently of how the archive segments its rows.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT val, period_end
              FROM financial_facts_raw
             WHERE instrument_id = %(iid)s
               AND taxonomy = 'us-gaap'
               AND concept = 'CommonStockSharesOutstanding'
               AND unit = 'shares'
               AND val IS NOT NULL
             ORDER BY ABS(period_end - %(near)s::date) ASC, filed_date DESC
             LIMIT 1
            """,
            {"iid": instrument_id, "near": near},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return Decimal(row[0]), row[1]


def _classify(stored: Decimal, total: Decimal) -> str:
    """I19's bands, applied to (stored - total) / total."""
    if total == 0:
        return "unavailable"
    skew = abs(stored - total) / total * 100
    if skew <= AGREE_PCT:
        return "agrees"
    if skew <= MINOR_PCT:
        return "minor_skew"
    return "diverges"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="cap the instruments compared (debug only)")
    parser.add_argument("--max-days", type=int, default=400, help="reject a comparison this far apart")
    args = parser.parse_args()

    zips = sorted(_bulk_dir().glob("fsds_*.zip"))
    if not zips:
        raise SystemExit(f"no cached FSDS archives under {_bulk_dir()}")
    print(f"FSDS archives: {[z.name for z in zips]}")

    groups = _read_class_groups(zips)
    print(f"(cik, instant) groups with >=2 ClassOfStock members: {len(groups)}")
    print(f"distinct CIKs: {len({g.cik for g in groups.values()})}")

    with psycopg.connect(settings.database_url) as conn:
        by_cik = _instruments_by_cik(conn, {g.cik for g in groups.values()})
        in_universe = {k: v for k, v in groups.items() if v.cik in by_cik}
        print(f"…of which map to an in-universe instrument: {len({g.cik for g in in_universe.values()})} CIKs")

        verdicts: collections.Counter[str] = collections.Counter()
        matches_one_member: list[tuple[str, str, Decimal, Decimal, str, int, str]] = []
        compared = 0
        # #3150 review (WARNING): the cap has to leave the OUTER loop too — a
        # `break` in the inner one only ends that group's instruments and the
        # next group resumes, so `--limit` capped nothing.
        for group in sorted(in_universe.values(), key=lambda g: (g.cik, g.ddate)):
            if args.limit is not None and compared >= args.limit:
                break
            for instrument_id, symbol in by_cik[group.cik]:
                if args.limit is not None and compared >= args.limit:
                    break
                stored = _stored_cover(conn, instrument_id, group.ddate)
                if stored is None:
                    verdicts["no_stored_cover"] += 1
                    continue
                value, period_end = stored
                if abs((period_end - group.ddate).days) > args.max_days:
                    verdicts["instants_too_far_apart"] += 1
                    continue
                compared += 1
                verdict = _classify(value, group.total)
                verdicts[verdict] += 1
                if verdict != "diverges" or value >= group.total:
                    continue
                # AVAL's shape: the cover value IS one member, not the sum.
                for member, member_value in group.members.items():
                    if member_value == 0:
                        continue
                    if abs(value - member_value) / member_value * 100 <= AGREE_PCT:
                        witness = _stored_usgaap_total(conn, instrument_id, group.ddate)
                        if witness is None:
                            corroboration = "absent"
                        else:
                            corroboration = f"{_classify(witness[0], group.total)}:{witness[0]:,.0f}"
                        matches_one_member.append(
                            (symbol, group.cik, value, group.total, member, len(group.members), corroboration)
                        )
                        break

    print("\n--- verdicts (instrument x instant comparisons) ---")
    for key, count in verdicts.most_common():
        print(f"{key:26s} {count:7d}")
    print(f"\ncompared: {compared}")

    print("\n--- the #2800 shape: stored cover BELOW the class sum AND equal to one member ---")
    print("second witness = our own UNDIMENSIONED us-gaap:CommonStockSharesOutstanding vs the class sum")
    print(f"{'symbol':8s} {'stored':>16s} {'class sum':>16s} {'ratio':>6s} {'member':20s} {'n':>2s}  us-gaap witness")
    # #3150 review (NITPICK): key the dedup on (symbol, cik), not symbol alone.
    # Collapsing a symbol's several INSTANTS into one line is the intent; silently
    # dropping a second issuer that shares the ticker is not.
    seen: set[tuple[str, str]] = set()
    for symbol, _cik, value, total, member, n_members, corroboration in sorted(matches_one_member):
        if (symbol, _cik) in seen:
            continue
        seen.add((symbol, _cik))
        ratio = float(total / value) if value else float("inf")
        print(
            f"{symbol:8s} {value:16,.0f} {total:16,.0f} {ratio:6.2f} {member[:20]:20s} {n_members:2d}  {corroboration}"
        )
    print(f"\ndistinct instruments carrying the #2800 shape: {len(seen)}")


if __name__ == "__main__":
    main()
