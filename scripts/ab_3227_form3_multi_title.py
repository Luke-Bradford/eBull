"""#3227 Form 3 multi-title — full-population A/B, backfill and verify.

Spec: ``docs/specs/ownership/2026-10-19-3227-form3-multi-title.md``. Two line sums now read titles
naming one class as one title (``form4_title_match``): the XML Form 3 writers'
``form3_observation_shares`` and the DERA ``:NDH:`` ``INSIDER_HOLDING_LINE_SUM_LATERAL``. The
rollup's Form 3 component scope (``_summable_scope``) follows them.

Control is the code on ``--ref`` (default ``98b332b0``, the pre-change base; loaded with ``git show`` and EXECUTED —
trusted refs only); treatment is the working tree. Both are recomputed from stored lines, never
read from what an observation happens to hold.

Stage 0 (read-only):
  * XML: every ``(accession, filer, D/I)`` group of more than one share-bearing line, both rules;
  * DERA: every live ``:NDH:`` accession group (any period, not only ``_current`` winners) with
    more than one title — a one-title group takes the exact-title branch, identical in both arms —
    through both fragments;
  * every unified title set, printed, and the changed groups' manifest form (Form 5 / 5A counted).
Stage 1 (one rolled-back transaction per arm per instrument): the cohort is every instrument with a
changed DERA group, every instrument holding a live ``form3`` observation of a changed XML group
(share-class siblings included, matched by accession), and every instrument whose ``_current`` Form
3 XML row has lines under more than one title or a shareless line (the rollup scope, which can move
with no observation moving). ``_current`` and the rollup are
compared per holder; every key removed and every holder total that falls is printed.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form3_multi_title
    PYTHONPATH=. uv run python -m scripts.ab_3227_form3_multi_title --apply    # after deploy
    PYTHONPATH=. uv run python -m scripts.ab_3227_form3_multi_title --verify   # after --apply

``--apply`` and ``--verify`` do not depend on the control: they compare stored state with the
treatment. ``--apply`` writes the treatment value onto every live observation of a multi-line XML
group that does not hold it (siblings included), batch-refreshes ``_current`` over every instrument
either rule can reach, and COMMITS, all in one snapshot. ``--verify`` exits 1 unless every such
observation holds the treatment value and ``_current`` equals a fresh (rolled-back) refresh, every
business column, over the same set. The A/B's control defaults to the pre-change base, and the
script refuses a ref that already carries the change.
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from decimal import Decimal
from itertools import groupby
from pathlib import Path
from types import ModuleType
from typing import Any, LiteralString, cast
from uuid import uuid4

import psycopg
import psycopg.rows

from app.config import settings
from app.services import insider_form3_ingest as new_f3
from app.services import ownership_observations as new_obs
from app.services import ownership_rollup as new_rollup
from app.services.insider_transactions import form4_title_class
from app.services.ownership_observations_sync import _StoredForm3Line

_SMOKE = ("AAPL", "GME", "MSFT", "JPM", "HD")

_XML_LINES_SQL = """
WITH multi AS (
    SELECT DISTINCT h.accession_number
      FROM insider_initial_holdings h
     WHERE NOT h.is_derivative AND h.shares IS NOT NULL
     GROUP BY h.accession_number, h.instrument_id, h.filer_cik, h.direct_indirect
    HAVING count(*) > 1
)
SELECT h.instrument_id, h.accession_number, h.row_num, h.filer_cik, h.is_derivative,
       h.security_title, h.shares, h.direct_indirect,
       (SELECT count(*) FROM (SELECT DISTINCT x.filer_cik FROM insider_filers x
         WHERE x.accession_number = h.accession_number) d) AS owner_count
  FROM insider_initial_holdings h
  JOIN multi USING (accession_number)
 WHERE NOT h.is_derivative AND h.shares IS NOT NULL
 ORDER BY h.instrument_id, h.accession_number, h.row_num
"""

_XML_OBS_SQL = """
SELECT instrument_id, holder_cik, holder_name, ownership_nature, source_document_id,
       source_accession, source_field, source_url, filed_at, period_start, period_end, shares
  FROM ownership_insiders_observations
 WHERE source = 'form3' AND known_to IS NULL
   AND source_document_id = ANY(%(accs)s::text[])
"""

# One pseudo-winner per live ``:NDH:`` accession group with more than one title; the fragment
# reads the group from ``w``'s key, so its first line stands in for the winner.
_NDH_GROUPS_SQL = """
WITH grp AS (
    SELECT instrument_id, holder_identity_key, ownership_nature, source, period_end,
           split_part(source_document_id, ':', 1) AS accession,
           min(source_document_id) AS source_document_id,
           array_agg(DISTINCT security_title) AS titles
      FROM ownership_insiders_observations
     WHERE known_to IS NULL AND source_document_id ~ ':NDH:[0-9]+$'
     GROUP BY 1, 2, 3, 4, 5, 6
    HAVING count(DISTINCT security_title) > 1
)
SELECT w.instrument_id, w.holder_identity_key, w.ownership_nature, w.source, w.period_end,
       w.accession, w.titles, coalesce(m.form, '?') AS form, ls.line_sum
  FROM grp w
  LEFT JOIN sec_filing_manifest m ON m.accession_number = w.accession
"""

# Every business column the refreshers write (``refreshed_at`` aside).
_CURRENT_SQL = """
SELECT holder_identity_key, ownership_nature, source, source_document_id, shares,
       holder_cik, holder_name, source_accession, source_url, filed_at, period_start, period_end
  FROM ownership_insiders_current WHERE instrument_id = %(iid)s
"""

# Instruments whose rollup Form 3 component scope (``_summable_scope``, the ``hold`` branch) can read
# differently with no observation moving: a ``_current`` Form 3 XML row whose accession carries the
# holder's non-derivative lines under more than one title, or a shareless one.
_SCOPE_SQL = """
SELECT DISTINCT oc.instrument_id
  FROM ownership_insiders_current oc
  JOIN insider_initial_holdings h ON h.accession_number = oc.source_accession AND h.filer_cik = oc.holder_cik
 WHERE oc.source = 'form3' AND oc.source_document_id !~ ':' AND NOT h.is_derivative
 GROUP BY oc.instrument_id, oc.source_accession, oc.holder_cik
HAVING count(DISTINCT h.security_title) > 1 OR bool_or(h.shares IS NULL)
"""

# For a removed ``:NDH:`` key: the holder's DERA ``(title, D/I, shares)`` lines on the accession, and
# per plain Form 3 observation accession of the same holder and period, its XML lines (the
# de-collision's match set). The caller compares them as multisets.
_DERA_TUPLES_SQL = """
SELECT security_title, direct_indirect, shares FROM ownership_insiders_observations
 WHERE instrument_id = %(iid)s AND holder_identity_key = %(hik)s AND known_to IS NULL
   AND source_document_id LIKE %(acc)s || ':NDH:%%'
"""
_XML_TUPLES_SQL = """
SELECT h.accession_number, h.security_title, h.direct_indirect, h.shares
  FROM insider_initial_holdings h
 WHERE NOT h.is_derivative AND h.shares IS NOT NULL
   AND (h.accession_number, h.filer_cik) IN (
       SELECT DISTINCT x.source_accession, x.holder_cik FROM ownership_insiders_observations x
        WHERE x.instrument_id = %(iid)s AND x.holder_identity_key = %(hik)s AND x.known_to IS NULL
          AND x.source = 'form3' AND x.source_document_id !~ ':' AND x.period_end = %(pe)s)
"""

# The base this change was built on. ``--apply`` / ``--verify`` do not depend on it (they compare
# stored state with the treatment); the A/B's control does, and ``origin/main`` would be the
# treatment once this merges (Codex checkpoints 1 and 2).
_PRE_CHANGE_REF = "98b332b0"


def _load_at(ref: str, rel_path: str, name: str) -> ModuleType:
    """Import ``rel_path`` as it stands at ``ref``. ⚠ EXECUTES that ref's code: trusted refs only."""
    src = subprocess.run(["git", "show", f"{ref}:{rel_path}"], check=True, capture_output=True, text=True).stdout
    path = Path(tempfile.mkdtemp()) / f"{name}.py"
    path.write_text(src)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(mod)
    return mod


def _class_text(title: str | None) -> str:
    k = form4_title_class(title)
    return " ".join(k) if k is not None else "<none>"


_ObsKey = tuple[str, str | None, str]


def _xml_rules(
    conn: psycopg.Connection[Any], control_f3: ModuleType | None
) -> tuple[dict[_ObsKey, Decimal], dict[_ObsKey, tuple[Decimal, Decimal]], set[_ObsKey], Counter[tuple[str, ...]]]:
    """Over every ``(accession, filer, D/I)`` group of more than one line: the treatment value of
    each, (control, treatment) where they differ, the multi-title keys, and the changed title sets."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_XML_LINES_SQL)
        lines = [_StoredForm3Line.from_row(r) for r in cur.fetchall()]
    treat: dict[_ObsKey, Decimal] = {}
    changed: dict[_ObsKey, tuple[Decimal, Decimal]] = {}
    multi_title: set[_ObsKey] = set()
    sets: Counter[tuple[str, ...]] = Counter()
    for (_, acc), it in groupby(lines, key=lambda ln: (ln.row["instrument_id"], ln.row["accession_number"])):
        group = list(it)
        oc = int(group[0].row["owner_count"])
        b = new_f3.form3_observation_shares(group, owner_count=oc)
        a = control_f3.form3_observation_shares(group, owner_count=oc) if control_f3 else b
        assert a.keys() == b.keys()
        for key, vb in b.items():
            members = [ln for ln in group if (ln.filer_cik, ln.direct_indirect) == key]
            if len(members) < 2:
                continue
            obs_key = (acc, key[0], "indirect" if key[1] == "I" else "direct")
            treat[obs_key] = vb
            titles = {ln.security_title for ln in members}
            if len(titles) > 1:
                multi_title.add(obs_key)
            if a[key] != vb:
                changed[obs_key] = (a[key], vb)
                sets[tuple(sorted(str(t) for t in titles))] += 1
    return treat, changed, multi_title, sets


def _ndh_changed(conn: psycopg.Connection[Any], control_obs: ModuleType | None) -> list[dict[str, Any]]:
    """Every live ``:NDH:`` multi-title accession group, with both arms' line sums."""
    ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT instrument_id FROM ownership_insiders_observations"
            " WHERE known_to IS NULL AND source_document_id ~ ':NDH:'"
        ).fetchall()
    ]
    params = new_obs.insider_ndh_title_class_params(conn, ids)
    out: dict[tuple[Any, ...], dict[str, Any]] = {}
    arms: list[tuple[str, str, dict[str, Any]]] = [("b", new_obs.INSIDER_HOLDING_LINE_SUM_LATERAL, params)]
    if control_obs is not None:
        arms.insert(0, ("a", control_obs.INSIDER_HOLDING_LINE_SUM_LATERAL, {}))
    for arm, frag, p in arms:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(cast(LiteralString, _NDH_GROUPS_SQL + frag), p)
            for r in cur.fetchall():
                k = (
                    r["instrument_id"],
                    r["holder_identity_key"],
                    r["ownership_nature"],
                    r["source"],
                    r["period_end"],
                    r["accession"],
                )
                out.setdefault(k, {**r})[arm] = r["line_sum"]
    return list(out.values())


def _write_xml(conn: psycopg.Connection[Any], obs: dict[str, Any], shares: Decimal) -> None:
    new_obs.record_insider_observation(
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


def _obs_key(o: dict[str, Any]) -> _ObsKey:
    return (o["source_document_id"], o["holder_cik"], o["ownership_nature"])


def _snapshot(conn: psycopg.Connection[Any], iid: int) -> dict[tuple[str, str], tuple[Any, ...]]:
    return {(r[0], r[1]): r[2:] for r in conn.execute(_CURRENT_SQL, {"iid": iid}).fetchall()}


def _rollup(
    mod: ModuleType, conn: psycopg.Connection[Any], sym: str, iid: int
) -> tuple[Decimal, bool, dict[tuple[str, str], Decimal]]:
    # ownership-rollup-snapshot: caller-owned — each arm is one REPEATABLE READ transaction that is
    # rolled back; snapshot_read() would COMMIT the arm's writes first.
    r = mod.get_ownership_rollup(conn, sym, iid)
    ins = next((s for s in r.slices if s.category == "insiders"), None)
    holders: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for h in ins.holders if ins else ():
        # A CIK is the identity (a renamed filer stays one holder); a name only when there is none.
        holders[(h.filer_cik, "") if h.filer_cik else ("", h.filer_name)] += h.shares
    return (ins.total_shares if ins else Decimal(0)), r.residual.oversubscribed, holders


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default=_PRE_CHANGE_REF, help="control ref; its code is EXECUTED — trusted refs only")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="write the treatment values and commit")
    mode.add_argument("--verify", action="store_true", help="exit 1 unless stored state equals the treatment")
    args = ap.parse_args()

    # --apply / --verify compare stored state with the treatment, so they load no control code.
    ab = not (args.apply or args.verify)
    control_obs = control_f3 = control_rollup = None
    if ab:
        control_obs = _load_at(args.ref, "app/services/ownership_observations.py", "control_ownership_observations")
        control_f3 = _load_at(args.ref, "app/services/insider_form3_ingest.py", "control_insider_form3_ingest")
        control_rollup = _load_at(args.ref, "app/services/ownership_rollup.py", "control_ownership_rollup")
        if "ndh_title" in control_obs.INSIDER_HOLDING_LINE_SUM_LATERAL:
            print(f"--ref {args.ref} already carries this change; pass the pre-change base")
            return 2

    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SET jit = off")
        conn.commit()
        # Stage 0, the cohort and --apply's writes all read ONE snapshot: this transaction.

        # ---- stage 0: XML ----
        treat, changed, xml_multi_title, xml_sets = _xml_rules(conn, control_f3)
        print("=== stage 0: XML Form 3 ===")
        print(f"(filer, D/I) groups of >1 line                 {len(treat):>10,}")
        print(f"  of which >1 title                            {len(xml_multi_title):>10,}")
        print(f"  value differs                                {len(changed):>10,}")
        print(f"  down (must be 0)                             {sum(b < a for a, b in changed.values()):>10,}")
        n_d = sum(k[2] == "direct" for k in changed)
        print(f"  D / I                                        {n_d:>6,} / {len(changed) - n_d:,}")
        print(f"  unified title sets ({len(xml_sets)}), every one:")
        for st, n in xml_sets.most_common():
            print(f"    {n:>4}  {st}  -> {sorted({_class_text(t) for t in st})}")

        # ---- stage 0: DERA ----
        ndh = _ndh_changed(conn, control_obs)
        moved = [r for r in ndh if r.get("a") != r.get("b")]
        print("\n=== stage 0: DERA :NDH: accession groups with >1 title (all periods) ===")
        print(f"groups                                         {len(ndh):>10,}")
        print(
            f"  line sum differs                             {len(moved):>10,}   over "
            f"{len({r['instrument_id'] for r in moved}):,} instruments"
        )
        print(
            f"  newly summed / no longer summed (must be 0)  {sum(r.get('a') is None for r in moved):>6,} / "
            f"{sum(r.get('b') is None for r in moved):,}"
        )
        print(f"  by manifest form ('?' = no manifest row)     {dict(Counter(r['form'] for r in moved))}")
        ndh_sets = Counter(tuple(sorted(str(t) for t in r["titles"])) for r in moved)
        print(f"  unified title sets ({len(ndh_sets)}), every one:")
        for st, n in ndh_sets.most_common():
            print(f"    {n:>4}  {st}  -> {sorted({_class_text(t) for t in st})}")

        # ---- observations and cohorts ----
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_XML_OBS_SQL, {"accs": sorted({k[0] for k in treat})})
            xml_obs = [o for o in cur.fetchall() if _obs_key(o) in treat]
        stale_obs = [o for o in xml_obs if o["shares"] != treat[_obs_key(o)]]
        scope = {int(r[0]) for r in conn.execute(_SCOPE_SQL).fetchall()}
        changed_obs: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for o in xml_obs:
            if _obs_key(o) in changed:
                changed_obs[o["instrument_id"]].append(o)
        # Control-free: every instrument either rule can reach, whatever its stored state, so --apply
        # and a later --verify cover the same set. --apply and --verify use it.
        reach = sorted({int(r["instrument_id"]) for r in ndh} | {o["instrument_id"] for o in xml_obs} | scope)
        cohort = sorted(set(changed_obs) | {int(r["instrument_id"]) for r in moved} | scope)
        print(f"\nlive form3 observations of multi-line groups   {len(xml_obs):>10,}")
        print(f"  not at the treatment value                   {len(stale_obs):>10,}")
        print(
            f"  of changed groups                            {sum(map(len, changed_obs.values())):>10,}   over "
            f"{len(changed_obs):,} instruments"
        )
        print(f"instruments whose Form 3 rollup scope can move {len(scope):>10,}")
        print(f"A/B cohort / reach                             {len(cohort):>6,} / {len(reach):,}")

        if args.apply:
            for o in stale_obs:
                _write_xml(conn, o, treat[_obs_key(o)])
            for i in range(0, len(reach), 200):
                new_obs.refresh_insiders_current_batch(conn, instrument_ids=reach[i : i + 200])
            conn.commit()
            print(f"applied: {len(stale_obs):,} observations written, {len(reach):,} instruments refreshed")
            return 0

        if args.verify:
            # Same snapshot as ``stale_obs``: refresh inside this transaction, then roll it all back.
            conn.execute("SELECT 1")  # see the arm loop: keeps each refresh a savepoint
            stale_cur = 0
            for iid in reach:
                before = _snapshot(conn, iid)
                new_obs.refresh_insiders_current(conn, instrument_id=iid)
                if before != _snapshot(conn, iid):
                    stale_cur += 1
                    print(f"  _current stale: instrument {iid}")
            conn.rollback()
            print(f"verify: observations not at treatment {len(stale_obs):,}; _current instruments stale {stale_cur:,}")
            return 1 if stale_obs or stale_cur else 0

        assert control_obs is not None and control_rollup is not None

        # ---- stage 1 ----
        symbols = dict(
            conn.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%(ids)s::bigint[])",
                {"ids": cohort},
            ).fetchall()
        )
        conn.rollback()
        counts: Counter[str] = Counter()
        tot_up = tot_down = newly_over = 0
        out: list[str] = []
        treated: dict[int, dict[tuple[str, str], tuple[Any, ...]]] = {}
        for iid in cohort:
            sym = symbols.get(iid, "?")
            arms = []
            for idx, (obs_mod, roll_mod) in enumerate(((control_obs, control_rollup), (new_obs, new_rollup))):
                # ⚠ Load-bearing: the refreshers open ``conn.transaction()``, which on an IDLE connection
                # is an outer transaction that COMMITS. A statement first makes it a savepoint inside
                # this arm's transaction, so the rollback below undoes it.
                conn.execute("SELECT 1")
                for o in changed_obs.get(iid, []):
                    _write_xml(conn, o, changed[_obs_key(o)][idx])
                obs_mod.refresh_insiders_current(conn, instrument_id=iid)
                arms.append((_snapshot(conn, iid), _rollup(roll_mod, conn, sym, iid)))
                conn.rollback()
            (ca, (ta, oa, ha)), (cb, (tb, ob, hb)) = arms
            treated[iid] = cb
            for k in sorted(ca.keys() | cb.keys()):
                a, b = ca.get(k), cb.get(k)
                if a == b:
                    continue
                out.append(f"  key: {sym} {k} {a[:3] if a else None} -> {b[:3] if b else None}")
                if a is None or b is None:
                    counts["added" if a is None else "removed"] += 1
                    if b is None and a is not None and ":NDH:" in a[1]:
                        q = {"iid": iid, "hik": k[0], "acc": a[1].split(":")[0], "pe": a[9]}
                        dera = Counter(tuple(r) for r in conn.execute(_DERA_TUPLES_SQL, q).fetchall())
                        xml: dict[str, Counter[tuple[Any, ...]]] = defaultdict(Counter)
                        for r in conn.execute(_XML_TUPLES_SQL, q).fetchall():
                            xml[r[0]][tuple(r[1:])] += 1
                        conn.rollback()
                        covered = [acc for acc, c in xml.items() if not (dera - c)]
                        out.append(f"    DERA multiset {dict(dera)} covered by XML accession(s) {covered or 'NONE'}")
                    continue
                counts["winner changed"] += a[1] != b[1]
                counts["shares up"] += b[2] > a[2]
                counts["shares down"] += b[2] < a[2]
                counts["other field"] += a[1] == b[1] and a[2] == b[2]
            tot_up += tb > ta
            tot_down += tb < ta
            newly_over += ob and not oa
            for h in sorted(ha.keys() | hb.keys()):
                va, vb = ha.get(h, Decimal(0)), hb.get(h, Decimal(0))
                if va != vb:
                    out.append(f"  holder {'down' if vb < va else 'up'}: {sym} {h[1]} ({h[0]}): {va:,.0f} -> {vb:,.0f}")
            if ob and not oa:
                out.append(f"  newly oversubscribed: {sym} {ta:,.0f} -> {tb:,.0f}")

        # The batch refresher from the same stored state, over real 200-instrument batches.
        batch_disagree = 0
        for i in range(0, len(cohort), 200):
            chunk = cohort[i : i + 200]
            conn.execute("SELECT 1")  # see the arm loop: keeps the refresh a savepoint
            for iid in chunk:
                for o in changed_obs.get(iid, []):
                    _write_xml(conn, o, changed[_obs_key(o)][1])
            new_obs.refresh_insiders_current_batch(conn, instrument_ids=chunk)
            batch_disagree += sum(_snapshot(conn, iid) != treated[iid] for iid in chunk)
            conn.rollback()

        print("\n=== stage 1: _current + rollup over the A/B cohort ===")
        print(f"_current keys                                  {dict(counts)}")
        print(f"per-instrument vs batch refresher disagreements {batch_disagree:>9,}")
        print(f"insider total up / down                        {tot_up:>6,} / {tot_down:,}")
        print(
            f"holder totals down                             {sum(ln.startswith('  holder down') for ln in out):>10,}"
        )
        print(f"newly oversubscribed                           {newly_over:>10,}")
        print(f"smoke panel in cohort                          {[s for s in _SMOKE if s in symbols.values()]}")
        for line in out:
            print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
