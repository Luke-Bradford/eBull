"""#3227 — full-population A/B and backfill of a Form 4 unit whose titles name one class.

Spec: docs/specs/ownership/2026-10-18-3227-form4-multi-title.md.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_multi_title            # A/B, writes nothing
    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_multi_title --apply    # backfill (commits)
    PYTHONPATH=. uv run python -m scripts.ab_3227_form4_multi_title --verify   # after --apply

The cohort is every ``(instrument, accession, filer)`` of a live Form 4/4A whose non-derivative
transaction and holding lines carry more than one distinct ``security_title``. Only these units can
move: ``form4_title_match`` is equality for a single-title unit, which is the rule before it.

A/B (control = what is stored now):

* stage 0, one REPEATABLE READ snapshot — the weekly writer (``derive_form4_observations``, every
  Form 4/4A observation it reaches) against the stored observations: observations up / unchanged /
  down / added, inside and outside the cohort (outside must not move; down must be 0), every field
  other than ``shares`` unchanged, the cohort's verdicts, and the largest rises for reading;
* stage 1 — ``scripts.ab_3227_form4_mixed_groups``'s rolled-back live-writer replay over the
  cohort's accessions (siblings, both ``_current`` refreshers, rollup, history, smoke panel).

``--apply`` and ``--verify`` are the mixed-groups script's, over this cohort: replay through
``upsert_filing`` with the per-accession refresh off, a full ``sync_insiders``, a batch refresh of
``_current``; then stored == recompute, siblings == entity, ``_current`` == a fresh refresh.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.services.insider_transactions import form4_title_class, form4_title_match, form4_within_retention
from app.services.ownership_observations_sync import derive_form4_observations
from scripts import ab_3227_form4_mixed_groups as mixed

_COHORT: Final[LiteralString] = """
WITH lines AS (
  SELECT t.instrument_id, t.accession_number, t.filer_cik, t.security_title
    FROM insider_transactions t JOIN insider_filings f USING (accession_number)
   WHERE NOT t.is_derivative AND f.document_type IN ('4', '4/A') AND NOT f.is_tombstone
  UNION ALL
  SELECT h.instrument_id, h.accession_number, h.filer_cik, h.security_title
    FROM insider_form4_holdings h JOIN insider_filings f USING (accession_number)
   WHERE NOT h.is_derivative AND f.document_type IN ('4', '4/A') AND NOT f.is_tombstone
)
SELECT l.instrument_id, l.accession_number, l.filer_cik, array_agg(DISTINCT l.security_title) AS titles,
       COALESCE((min(m.filed_at) AT TIME ZONE 'UTC')::date,
                (SELECT max(fe.filing_date) FROM filing_events fe
                  WHERE fe.provider = 'sec' AND fe.provider_filing_id = l.accession_number
                    AND fe.instrument_id = l.instrument_id)) AS filing_date
  FROM lines l
  LEFT JOIN sec_filing_manifest m ON m.accession_number = l.accession_number
 GROUP BY 1, 2, 3
HAVING count(DISTINCT l.security_title) > 1
"""


@dataclass(frozen=True)
class Unit:
    instrument_id: int
    accession: str
    filer_cik: str
    titles: tuple[str | None, ...]
    # The weekly writer's Form 4 retention gate on the manifest-first filing date (#1233).
    in_retention: bool

    @property
    def one_class(self) -> bool:
        return form4_title_match(self.titles) is not None


def _units(conn: psycopg.Connection[Any]) -> tuple[list[Unit], list[str]]:
    units = [
        Unit(int(iid), acc, (cik or "").strip(), tuple(titles), fd is not None and form4_within_retention(fd))
        for iid, acc, cik, titles, fd in conn.execute(_COHORT).fetchall()
    ]
    return units, sorted({u.accession for u in units})


def _stage0(conn: psycopg.Connection[Any]) -> list[Unit]:  # noqa: C901, PLR0912
    units, accs = _units(conn)
    by_key = {(u.instrument_id, u.accession, u.filer_cik): u for u in units}
    print("=== stage 0: weekly writer vs stored observations ===")
    print(
        f"cohort: units {len(units):,}, accessions {len(accs):,}, instruments {len({u.instrument_id for u in units}):,}"
    )
    one = [u for u in units if u.one_class]
    print(f"  units whose titles name one class: {len(one):,} over {len({u.instrument_id for u in one}):,} instruments")
    classes = Counter(" ".join(form4_title_class(next(t for t in u.titles if t is not None)) or ()) for u in one)
    for c, n in classes.most_common():
        print(f"    {c:<28} {n:>8,}")
    # Full-population read of the rule: every distinct title set it unifies.
    sets = Counter(tuple(sorted(t for t in u.titles if t is not None)) for u in one)
    print(f"  distinct title sets unified: {len(sets):,}; every one, most frequent first:")
    for titles, n in sets.most_common():
        print(f"    {n:>6,}  {list(titles)}")

    stored = mixed._stored(conn)
    weekly = list(derive_form4_observations(conn))
    print(f"weekly-writer observations {len(weekly):,}")
    moves: Counter[str] = Counter()
    field_diffs: list[str] = []
    outside: list[str] = []
    verdicts: Counter[str] = Counter()
    rises: list[tuple[Decimal, str]] = []
    produced: set[tuple[int, str, str | None, str]] = set()
    symbols: dict[int, str] = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
    for o in weekly:
        k = (o.instrument_id, o.accession_number, o.holder_cik, o.ownership_nature)
        produced.add(k)
        unit = by_key.get((o.instrument_id, o.accession_number, o.holder_cik or ""))
        where = "cohort" if unit is not None else "outside"
        prior = stored.get(k)
        if prior is None:
            moves[f"{where} {o.kind}: added"] += 1
            if unit is None:
                outside.append(f"added {k} {o.shares}")
            else:
                rises.append((o.shares, f"{symbols.get(o.instrument_id)} {k} added {o.shares:,} {list(unit.titles)}"))
            continue
        filed = o.sec_filed_at or datetime.combine(o.period_end, datetime.min.time(), tzinfo=UTC)
        fields = (o.period_end, filed, o.source_url, o.holder_name or (o.holder_cik or "UNKNOWN"))
        if prior[1:] != fields:
            field_diffs.append(f"{k}: stored {prior[1:]}, recompute {fields}")
        move = "up" if o.shares > prior[0] else "down" if o.shares < prior[0] else "unchanged"
        moves[f"{where} {o.kind}: {move}"] += 1
        if unit is None:
            if move != "unchanged":
                outside.append(f"{move} {k} stored {prior[0]} recompute {o.shares}")
            continue
        verdicts[f"{o.ownership_nature} {o.kind} vehicle={o.verdict} fold={o.fold_verdict} -> {move}"] += 1
        if move == "up":
            rises.append(
                (
                    o.shares - prior[0],
                    f"{symbols.get(o.instrument_id)} {k} {prior[0]:,} -> {o.shares:,} {list(unit.titles)}",
                )
            )
    entity = {(u.instrument_id, u.accession) for u in units}
    in_retention = {u.accession for u in units if u.in_retention}
    removed = [k for k in stored if (k[0], k[1]) in entity and k not in produced and k[1] in in_retention]
    unreached = sum(1 for k in stored if (k[0], k[1]) in entity and k not in produced and k[1] not in in_retention)

    for m, n in sorted(moves.items()):
        print(f"  {m:<44} {n:>10,}")
    print(f"outside-cohort moves (must be 0, bar the #3884 mislink): {len(outside):,}")
    for item in outside[:20]:
        print(f"    {item}")
    print(f"non-shares field differences: {len(field_diffs):,}")
    for item in field_diffs[:20]:
        print(f"    {item}")
    print(f"stored cohort entity observations outside retention (the weekly writer never reaches them): {unreached:,}")
    print(f"stored cohort entity observations in retention the writer does not produce: {len(removed):,}")
    for k in removed[:20]:
        print(f"    {k} stored {stored[k][0]}")
    print("cohort verdicts (observations):")
    for v, n in sorted(verdicts.items()):
        print(f"  {v:<100} {n:>8,}")
    rises.sort(key=lambda r: r[0], reverse=True)
    print(f"cohort rises and additions: {len(rises):,}, shares {sum(r[0] for r in rises):,}")
    for _, text in rises[:25]:
        print(f"    {text}")
    conn.rollback()
    return units


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]  # interleave with log lines
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="replay the cohort + full sync_insiders, and commit")
    ap.add_argument("--verify", action="store_true", help="check stored == recompute, siblings, and _current")
    ap.add_argument("--stage0-only", action="store_true", help="skip stage 1 (the rolled-back replay)")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET jit = off")
        conn.commit()
        if args.apply:
            return mixed._apply(conn, _units)
        if args.verify:
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            return mixed._verify(conn, _units)
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ  # one snapshot for stage 0
        units = _stage0(conn)
        conn.rollback()
        if args.stage0_only:
            return 0
        # READ COMMITTED, as the mixed-groups stage 1 explains. One transaction, rolled back.
        conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
        mixed._stage1(conn, units)
    return 0


if __name__ == "__main__":
    sys.exit(main())
