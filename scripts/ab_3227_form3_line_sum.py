"""#3227 item 3 residual — full-population A/B of the Form 3 XML writer's line sum.

The Form 3 observation writers (``insider_form3_ingest._record_form3_observations_for_filing`` and
``ownership_observations_sync.sync_insiders``) now take each ``(filer, D/I)`` observation's shares
from ``form3_observation_shares``. Control = the pre-change rule, the last non-derivative line by
``row_num``. Treatment = the helper. Both are computed from the stored ``insider_initial_holdings``
lines, so the comparison is against a RECOMPUTED baseline, never against whatever the observation
currently holds.

Stage 1 counts the groups whose shares differ and checks their live ``form3`` observations. Stage 2
writes each arm's value onto those observations inside a transaction that is ROLLED BACK, refreshes
``_current`` — control with ``refresh_insiders_current`` as it stands on ``--ref`` (default
``origin/main``), treatment with the working tree, which carries the cross-accession de-collision
clause — and renders the ownership rollup, reporting the operator-visible insider total. The
instruments that clause can reach are added even where no observation changed.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form3_line_sum

``--apply`` is the backfill, run once AFTER the change is deployed: it writes the treatment value
onto every live observation of a changed group, refreshes those instruments' ``_current`` and
COMMITS. Idempotent: a re-run of the A/B afterwards reports every changed observation as already
holding the treatment value.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from decimal import Decimal
from itertools import groupby
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows

from app.config import settings
from app.services.insider_form3_ingest import form3_observation_shares
from app.services.ownership_observations import record_insider_observation, refresh_insiders_current
from app.services.ownership_observations_sync import _StoredForm3Line
from app.services.ownership_rollup import get_ownership_rollup
from scripts.ab_3227_holding_line_sum import _load_control

# Every accession with a (filer, D/I) group of more than one share-bearing non-derivative line —
# the only accessions where the two rules can disagree.
_LINES_SQL = """
WITH multi AS (
    SELECT DISTINCT h.accession_number
      FROM insider_initial_holdings h
     WHERE NOT h.is_derivative AND h.shares IS NOT NULL
     GROUP BY h.accession_number, h.instrument_id, h.filer_cik, h.direct_indirect
    HAVING count(*) > 1
)
SELECT h.instrument_id, h.accession_number, h.row_num, h.filer_cik, h.is_derivative,
       h.security_title, h.nature_of_ownership, h.shares, h.direct_indirect,
       (SELECT count(DISTINCT x.filer_cik) FROM insider_filers x
         WHERE x.accession_number = h.accession_number) AS owner_count
  FROM insider_initial_holdings h
  JOIN multi USING (accession_number)
  JOIN insider_filings f ON f.accession_number = h.accession_number AND NOT f.is_tombstone
 WHERE NOT h.is_derivative AND h.shares IS NOT NULL
 ORDER BY h.instrument_id, h.accession_number, h.row_num
"""

# Instruments where a ``:NDH:`` form3 winner and a plain form3 winner share a holder and a
# ``period_end`` — the only place the new de-collision clause can act.
_DECOLLISION_SQL = """
SELECT DISTINCT n.instrument_id
  FROM ownership_insiders_current n
  JOIN ownership_insiders_current x
    ON x.instrument_id = n.instrument_id AND x.holder_cik IS NOT DISTINCT FROM n.holder_cik
   AND x.source = 'form3' AND x.source_document_id !~ ':' AND x.period_end = n.period_end
 WHERE n.source = 'form3' AND n.source_document_id ~ ':NDH:'
"""

_OBS_SQL = """
SELECT instrument_id, holder_cik, holder_name, ownership_nature, source_document_id,
       source_accession, source_field, source_url, filed_at, period_start, period_end, shares
  FROM ownership_insiders_observations
 WHERE source = 'form3' AND known_to IS NULL
   AND source_document_id = ANY(%(accs)s::text[])
"""


def _key(o: dict[str, Any]) -> tuple[str, str | None, str]:
    return (o["source_document_id"], o["holder_cik"], o["ownership_nature"])


def _last_line_rule(group: list[_StoredForm3Line]) -> dict[tuple[str | None, str | None], Decimal]:
    out: dict[tuple[str | None, str | None], Decimal] = {}
    for line in group:  # ordered by row_num
        if line.shares is not None:
            out[(line.filer_cik, line.direct_indirect)] = line.shares
    return out


def _write(conn: psycopg.Connection[Any], obs: dict[str, Any], shares: Decimal) -> None:
    record_insider_observation(
        conn,
        instrument_id=obs["instrument_id"],
        holder_cik=obs["holder_cik"],
        holder_name=obs["holder_name"],
        ownership_nature=obs["ownership_nature"],
        source="form3",
        source_document_id=obs["source_document_id"],
        source_accession=obs["source_accession"],
        source_field=obs["source_field"],
        source_url=obs["source_url"],
        filed_at=obs["filed_at"],
        period_start=obs["period_start"],
        period_end=obs["period_end"],
        ingest_run_id=uuid4(),
        shares=shares,
    )


def _insider_total(conn: psycopg.Connection[Any], symbol: str, iid: int) -> tuple[Decimal, bool, dict[str, Decimal]]:
    # ownership-rollup-snapshot: caller-owned — each arm is one REPEATABLE READ transaction that is
    # rolled back; snapshot_read() would COMMIT the arm's writes first.
    r = get_ownership_rollup(conn, symbol, iid)
    ins = next((s for s in r.slices if s.category == "insiders"), None)
    holders = {h.filer_name: h.shares for h in ins.holders} if ins else {}
    return (ins.total_shares if ins else Decimal(0)), r.residual.oversubscribed, holders


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the treatment value and commit")
    ap.add_argument("--ref", default="origin/main", help="control refresher ref; EXECUTED — trusted refs only")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SET jit = off")
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_LINES_SQL)
            lines = [_StoredForm3Line.from_row(r) for r in cur.fetchall()]

        groups_total = groups_changed = 0
        # (accession, holder_cik, nature) -> (control, treatment)
        changed: dict[tuple[str, str | None, str], tuple[Decimal, Decimal]] = {}
        for (_, acc), it in groupby(lines, key=lambda line: (line.row["instrument_id"], line.row["accession_number"])):
            group = list(it)
            a = _last_line_rule(group)
            b = form3_observation_shares(group, owner_count=int(group[0].row["owner_count"]))
            assert a.keys() == b.keys()
            for key, va in a.items():
                if sum(1 for line in group if (line.filer_cik, line.direct_indirect) == key) < 2:
                    continue
                groups_total += 1
                if b[key] != va:
                    groups_changed += 1
                    changed[(acc, key[0], "indirect" if key[1] == "I" else "direct")] = (va, b[key])

        print(f"multi-line (filer, D/I) groups                 {groups_total:>10,}")
        print(f"  sum differs from the last line               {groups_changed:>10,}")
        n_d = sum(k[2] == "direct" for k in changed)
        print(f"  of which D / I                               {n_d:>6,} / {len(changed) - n_d:,}")
        print(f"  shares down (sum < last line; must be 0)     {sum(b < a for a, b in changed.values()):>10,}")
        print(f"  distinct accessions                          {len({k[0] for k in changed}):>10,}")

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_OBS_SQL, {"accs": sorted({k[0] for k in changed})})
            obs_rows = [o for o in cur.fetchall() if _key(o) in changed]
        conn.rollback()
        holds = defaultdict(int)
        for o in obs_rows:
            a, b = changed[_key(o)]
            holds["treatment" if o["shares"] == b else "control" if o["shares"] == a else "other"] += 1
        iids = sorted({o["instrument_id"] for o in obs_rows})
        print(f"live form3 observations of changed groups      {len(obs_rows):>10,}   over {len(iids):,} instruments")
        c, t, x = holds["control"], holds["treatment"], holds["other"]
        print(f"  holding control / treatment / other          {c:>6,} / {t:,} / {x:,}")
        for o in obs_rows:
            a, b = changed[_key(o)]
            if o["shares"] not in (a, b):
                acc, iid, held = o["source_document_id"], o["instrument_id"], o["shares"]
                print(f"  other: {acc} iid={iid} holds {held:,.0f}, last line {a:,.0f}, sum {b:,.0f}")

        by_iid: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for o in obs_rows:
            by_iid[o["instrument_id"]].append(o)
        extra = [int(r[0]) for r in conn.execute(_DECOLLISION_SQL).fetchall() if int(r[0]) not in by_iid]
        conn.rollback()
        for iid in extra:
            by_iid[iid] = []
        iids = sorted(by_iid)
        print(f"instruments the de-collision clause can reach  {len(extra):>10,}   (beyond the above)")

        if args.apply:
            for iid, rows in by_iid.items():
                for o in rows:
                    _write(conn, o, changed[_key(o)][1])
                refresh_insiders_current(conn, instrument_id=iid)
                conn.commit()
            print(f"applied: {len(obs_rows):,} observations over {len(by_iid):,} instruments")
            return 0

        control = _load_control(args.ref)
        symbols = dict(
            conn.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%(ids)s::bigint[])",
                {"ids": iids},
            ).fetchall()
        )
        conn.rollback()
        results: list[tuple[str, Decimal, Decimal, bool, bool, str]] = []
        cur_moved = 0
        for iid, rows in by_iid.items():
            sym = symbols.get(iid, "?")
            arms = []
            for idx in (0, 1):
                conn.execute("SELECT 1")
                for o in rows:
                    _write(conn, o, changed[_key(o)][idx])
                (control.refresh_insiders_current if idx == 0 else refresh_insiders_current)(conn, instrument_id=iid)
                cur_sum = conn.execute(
                    "SELECT coalesce(sum(shares), 0) FROM ownership_insiders_current WHERE instrument_id = %s", (iid,)
                ).fetchone()
                arms.append((_insider_total(conn, sym, iid), cur_sum[0] if cur_sum else 0))
                conn.rollback()
            (ta, oa, ha), ca = arms[0]
            (tb, ob, hb), cb = arms[1]
            cur_moved += ca != cb
            top = next(
                (
                    f"{n}: {ha.get(n, Decimal(0)):,.0f} -> {hb.get(n, Decimal(0)):,.0f}"
                    for n in sorted(ha.keys() | hb.keys(), key=lambda n: -abs(hb.get(n, 0) - ha.get(n, 0)))
                    if ha.get(n) != hb.get(n)
                ),
                "",
            )
            results.append((sym, ta, tb, oa, ob, top))

        moved = [r for r in results if r[1] != r[2]]
        print("\n=== _current + rollup, per instrument ===")
        print(f"instruments whose _current shares moved        {cur_moved:>10,}")
        print(f"insider slice total moved                      {len(moved):>10,}")
        up, down = sum(r[2] > r[1] for r in results), sum(r[2] < r[1] for r in results)
        print(f"  up / down                                    {up:>6,} / {down:,}")
        print(f"newly oversubscribed (was not, now is)         {sum((not r[3]) and r[4] for r in results):>10,}")
        for sym, ta, tb, oa, ob, top in results:
            if ob and not oa:
                print(f"  newly: {sym:<10} {ta:>16,.0f} -> {tb:>16,.0f} | {top}")
        for sym, ta, tb, _oa, _ob, top in results:
            if tb < ta:
                print(f"  down: {sym:<10} {ta:>16,.0f} -> {tb:>16,.0f} | {top}")
        print("\n=== largest insider-total moves (25) ===")
        for sym, ta, tb, _oa, ob, top in sorted(results, key=lambda r: -(r[2] - r[1]))[:25]:
            print(f"{sym:<10} {ta:>16,.0f} -> {tb:>16,.0f}{' OVERSUBSCRIBED' if ob else ''}  | {top}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
