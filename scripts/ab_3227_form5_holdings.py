"""#3227 — full-population A/B and backfill of a Form 5's holding-line observations.

Spec: docs/specs/ownership/2026-10-18-3227-form5-holdings.md, Change 4 and Acceptance 2/5.

    PYTHONPATH=. uv run python -m scripts.ab_3227_form5_holdings            # A/B, writes nothing
    PYTHONPATH=. uv run python -m scripts.ab_3227_form5_holdings --apply    # backfill (commits)
    PYTHONPATH=. uv run python -m scripts.ab_3227_form5_holdings --verify   # after --apply

The cohort is every stored ``form5_xml`` payload containing ``erivativeHolding`` that has an
``insider_filings`` row and a manifest ``filed_at`` inside the Form 5 retention window on its UTC
date. This is the only supported apply path: ``scripts/rewash.py --kind form5_xml`` would also
re-apply the expired payloads (spec, Change 4).

A/B (control = what is stored now; treatment = the live writer's holding observations, computed
from each payload with the production parser and ``form4_holding_observations``):

* stage 0 — the helper's verdict, value and ``period_end`` per group against
  ``scripts.census_3227_form5_holdings``'s, the parser's transactions against the stored
  ``insider_transactions`` rows (a replay must not move a transaction observation), and the ledger
  of the derivative-holdings-only payloads;
* stage 1 — observations added / changed / unchanged against the stored ones, per sibling the live
  writer fans out to;
* stage 2 — ``scripts.ab_3227_form4_holding_rows._stage2``: inside transactions that are ROLLED
  BACK, the cohort's holding rows are inserted, the treatment observations written and ``_current``
  refreshed (per-instrument and batch, which must agree), over every instrument with a treatment
  observation or an inserted holding row's sibling. Reports ``_current`` keys added, removed and
  changed by class, every gated key whose row changes, the insider rollup, newly oversubscribed
  instruments and ``ownership_history`` points. No other instrument's rollup reads a changed row.

``--apply`` replays the cohort through ``parse_form_5_xml`` + ``upsert_filing`` (stored bodies, no
refetch), stamping its manifest row ``parsed`` at ``form5-v2`` and its raw body's version, committing
per accession, then runs ``sync_insiders`` with no ``since`` and no ``LIMIT``,
then batch-refreshes ``_current`` for every instrument with a form4 observation. ``--verify`` checks
every stored Form 4/5 observation the weekly writer reaches equals its recompute, every stored XML
observation of a cohort accession has one (a gated group's stale row fails), stored
``_current`` equals a fresh batch refresh, and prints the derivative-only ledger after the replay.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.services.insider_transactions import (
    _PARSER_VERSION_FORM5,
    FORM4_HOLDING_NATURE,
    FORM4_HOLDING_WRITES,
    ParsedFiling,
    form4_holding_observations,
    form5_within_retention,
    parse_form_5_xml,
    upsert_filing,
)
from app.services.ownership_observations import refresh_insiders_current_batch
from app.services.ownership_observations_sync import sync_insiders
from app.services.rewash_filings import _bump_parser_version
from app.services.sec_identity import resolve_insider_writer_routing, sibling_instruments_for_instrument
from app.services.sec_manifest import transition_status
from scripts import ab_3227_form4_holding_rows as ab4
from scripts import census_3227_form5_holdings as census

_COHORT: Final[LiteralString] = """
SELECT r.accession_number, f.instrument_id, f.primary_document_url, f.is_tombstone, f.document_type,
       f.period_of_report, m.filed_at, r.payload
  FROM filing_raw_documents r
  JOIN insider_filings f ON f.accession_number = r.accession_number
  JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
 WHERE r.document_kind = 'form5_xml'
   AND r.payload LIKE '%%erivativeHolding%%'
"""

_INSERT_HOLDING: Final[LiteralString] = """
INSERT INTO insider_form4_holdings (instrument_id, accession_number, row_num, filer_cik, filer_name,
       security_title, shares, is_derivative, direct_indirect, nature_of_ownership, holding_form_type,
       footnote_refs)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_LEDGER: Final[LiteralString] = """
SELECT f.accession_number, f.is_tombstone,
       (SELECT count(*) FROM insider_form4_holdings h
         WHERE h.accession_number = f.accession_number AND h.is_derivative),
       (SELECT count(*) FROM ownership_insiders_observations o
         WHERE o.instrument_id = f.instrument_id AND o.source_accession = f.accession_number
           AND o.source = 'form4' AND o.known_to IS NULL AND o.source_document_id !~ ':')
  FROM insider_filings f
 WHERE f.accession_number = ANY(%(accs)s::text[])
 ORDER BY 1
"""


def _cohort(conn: psycopg.Connection[Any], now: datetime | None = None) -> list[tuple[Any, ...]]:
    """The cohort rows minus their payloads' failed gates (filed date taken in UTC)."""
    out = []
    with conn.cursor(name="cohort") as cur:
        cur.itersize = 500
        cur.execute(_COHORT)
        for row in cur:
            filed_at = row[6]
            if filed_at is not None and form5_within_retention(filed_at.astimezone(UTC).date(), now=now):
                out.append(row)
    return out


def _derivative_only(parsed: ParsedFiling) -> bool:
    return bool(parsed.holdings) and all(h.is_derivative for h in parsed.holdings)


def _stamp(conn: psycopg.Connection[Any], accession: str) -> None:
    """Record the replay as the manifest worker would (Codex checkpoint 2): the manifest row
    ``parsed`` at ``form5-v2`` and the raw body's ``parser_version`` advanced, in the replay's
    transaction. The state machine admits ``tombstoned``/``parsed`` -> ``pending`` -> ``parsed``
    only, the explicit re-parse gate (``sec_manifest._ALLOWED_TRANSITIONS``)."""
    row = conn.execute(
        "SELECT ingest_status, parser_version FROM sec_filing_manifest WHERE accession_number = %s", (accession,)
    ).fetchone()
    if row is not None and row != ("parsed", _PARSER_VERSION_FORM5):
        if row[0] != "pending":
            transition_status(conn, accession, ingest_status="pending")
        transition_status(conn, accession, ingest_status="parsed", parser_version=_PARSER_VERSION_FORM5)
    _bump_parser_version(conn, accession_number=accession, document_kind="form5_xml", new_version=_PARSER_VERSION_FORM5)


def _apply(conn: psycopg.Connection[Any]) -> int:
    rows = _cohort(conn)
    conn.commit()
    print(f"cohort (gated) {len(rows):,}")
    done = none = failed = 0
    for accession, iid, url, _tomb, _dt, _por, filed_at, payload in rows:
        parsed = parse_form_5_xml(payload)
        if parsed is None:
            none += 1
            continue
        try:
            upsert_filing(
                conn,
                instrument_id=int(iid),
                accession_number=accession,
                primary_document_url=str(url) if url else "",
                parsed=parsed,
                is_rewash=True,
                filed_at=filed_at,
                refresh_current=False,  # one batch refresh below
            )
            _stamp(conn, accession)
            conn.commit()
            done += 1
        except Exception as exc:  # noqa: BLE001 — one accession must not stop the replay; counted
            conn.rollback()
            failed += 1
            print(f"  FAILED {accession}: {exc}")
    print(f"replayed {done:,}, parser None {none:,}, failed {failed:,}")
    summary = sync_insiders(conn)
    conn.commit()
    print(f"sync_insiders: {summary.observations_recorded:,} recorded, {len(summary.orphans):,} orphans")
    ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT instrument_id FROM ownership_insiders_observations WHERE source = 'form4' ORDER BY 1"
        ).fetchall()
    ]
    for i in range(0, len(ids), ab4._BATCH):
        refresh_insiders_current_batch(conn, instrument_ids=ids[i : i + ab4._BATCH])
        conn.commit()
    print(f"batch-refreshed _current for {len(ids):,} instruments")
    return 1 if failed else 0


def _verify(conn: psycopg.Connection[Any]) -> int:
    ledger_accs = [r[0] for r in _cohort(conn) if (p := parse_form_5_xml(r[7])) is not None and _derivative_only(p)]
    print(f"derivative-only ledger after the replay ({len(ledger_accs)}):")
    print("  accession, tombstone, derivative lines, XML observations")
    for acc, tomb, lines, obs in conn.execute(_LEDGER, {"accs": ledger_accs}).fetchall():
        print(f"  {acc} {tomb} {lines} {obs}")
    return ab4._verify(conn, reverse_accessions={r[0] for r in _cohort(conn)})


def _stage01(conn: psycopg.Connection[Any], as_of: datetime) -> dict[str, Any]:  # noqa: C901, PLR0912, PLR0915
    """Stages 0 and 1, printed; returns the inputs stage 2 needs."""
    census_groups, _counts, _inv = census.collect(conn, as_of, set())
    census_keys = {
        (g.instrument_id, g.accession, g.filer_cik, g.di): (g.verdict, g.value, g.period_end) for g in census_groups
    }
    rows = _cohort(conn, now=as_of)
    accs = [r[0] for r in rows]
    stored_txns: dict[str, set[tuple[Any, ...]]] = defaultdict(set)
    stored_obs: dict[tuple[int, str, str | None, str], Decimal] = {}
    for r in conn.execute(ab4._STORED_TXNS, {"accs": accs}).fetchall():
        stored_txns[r[0]].add(ab4._txn_key(ab4._Row(r)))
    for r in conn.execute(ab4._STORED_OBS, {"accs": accs}).fetchall():
        stored_obs[(int(r[0]), r[1], r[2], r[3])] = r[4]

    helper: dict[tuple[int, str, str, str], tuple[str, Decimal | None, Any]] = {}
    treatment: list[dict[str, Any]] = []
    drift: list[str] = []
    ledger: list[str] = []
    parsed_none = 0
    siblings_of: dict[tuple[int, str | None], list[int]] = {}
    entity_of: dict[tuple[int, str | None], int] = {}
    holding_rows: list[tuple[Any, ...]] = []
    evidence_iids: set[int] = set()
    gated_keys: set[tuple[int, str, str]] = set()
    for accession, iid, url, tomb, _dt, _por, filed_at, payload in rows:
        parsed = parse_form_5_xml(payload)
        if parsed is None:
            parsed_none += 1
            continue
        if {ab4._txn_key(t) for t in parsed.transactions} != stored_txns.get(accession, set()):
            drift.append(accession)
        cik = parsed.filers[0].filer_cik
        groups = form4_holding_observations(
            [h for h in parsed.holdings if h.filer_cik == cik],
            [t for t in parsed.transactions if t.filer_cik == cik],
            document_type=parsed.document_type,
            owner_ciks=[f.filer_cik for f in parsed.filers],
            signature_date=parsed.signature_date,
            period_of_report=parsed.period_of_report,
            manifest_filed_at=filed_at,
            not_subject_to_section_16=parsed.not_subject_to_section_16,
        )
        rkey = (int(iid), parsed.issuer_cik)
        if rkey not in siblings_of:
            routing = resolve_insider_writer_routing(
                conn, discovery_instrument_id=int(iid), issuer_cik=parsed.issuer_cik
            )
            siblings_of[rkey] = routing.sibling_instrument_ids or [routing.entity_instrument_id]
            entity_of[rkey] = routing.entity_instrument_id
        for h in parsed.holdings:
            holding_rows.append(
                (
                    entity_of[rkey],
                    accession,
                    h.row_num,
                    h.filer_cik,
                    parsed.filers[0].filer_name,
                    h.security_title,
                    h.shares,
                    h.is_derivative,
                    h.direct_indirect,
                    h.nature_of_ownership,
                    h.holding_form_type,
                    # last: ``ab4._stage2`` wraps the final element in ``Jsonb``
                    [{"footnote_id": r.footnote_id, "field": r.field} for r in h.footnote_refs],
                )
            )
            if not h.is_derivative:
                evidence_iids.update(siblings_of[rkey])
        if _derivative_only(parsed):
            nd_txns = sum(not t.is_derivative for t in parsed.transactions)
            ledger.append(
                f"{accession} tombstoned={bool(tomb)} nonderivative_txns={nd_txns} "
                f"derivative_lines={len(parsed.holdings)} holding_groups={len(groups)}"
            )
        for g in groups:
            helper[(int(iid), accession, (cik or "").strip(), g.direct_indirect)] = (g.verdict, g.shares, g.period_end)
            if g.verdict not in FORM4_HOLDING_WRITES:
                for sib in siblings_of[rkey]:
                    gated_keys.add((sib, f"CIK:{(cik or '').strip()}", FORM4_HOLDING_NATURE[g.direct_indirect]))
            if g.shares is None or g.period_end is None:
                continue
            for sib in siblings_of[rkey]:
                treatment.append(
                    {
                        "instrument_id": sib,
                        "accession": accession,
                        "holder_cik": cik,
                        "holder_name": parsed.filers[0].filer_name.strip() or cik,
                        "nature": FORM4_HOLDING_NATURE[g.direct_indirect],
                        "url": str(url) if url else None,
                        "filed_at": filed_at,
                        "period_end": g.period_end,
                        "shares": g.shares,
                        "doc_type": parsed.document_type,
                        "tombstoned": bool(tomb),
                    }
                )
    symbols = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())

    print("=== stage 0: helper vs census, parser vs stored transactions ===")
    print(f"cohort payloads (gated)                         {len(rows):>10,}")
    print(f"  parser returned None                          {parsed_none:>10,}")
    print(f"  formerly tombstoned                           {sum(bool(r[3]) for r in rows):>10,}")
    print(f"  transaction drift (parse != stored rows)      {len(drift):>10,}")
    for a in drift:
        print(f"    {a}")
    agree = sum(1 for k, v in helper.items() if census_keys.get(k) == v)
    print(f"helper groups                                   {len(helper):>10,}")
    print(f"census groups                                   {len(census_keys):>10,}")
    print(f"  equal (verdict, value, period_end)            {agree:>10,}")
    for k in sorted(helper.keys() ^ census_keys.keys()):
        print(f"    only in {'helper' if k in helper else 'census'}: {k} {helper.get(k) or census_keys.get(k)}")
    for k in sorted(k for k in helper.keys() & census_keys.keys() if helper[k] != census_keys[k]):
        print(f"    differs {k}: helper {helper[k]}, census {census_keys[k]}")
    print("helper verdicts:")
    for v, n in sorted(Counter(v[0] for v in helper.values()).items()):
        print(f"    {v:<40} {n:>8,}")
    print(f"derivative-holdings-only payloads (ledger)      {len(ledger):>10,}")
    for line in ledger:
        print(f"    {line}")

    print("\n=== stage 1: observations ===")
    verdict: Counter[str] = Counter()
    for o in treatment:
        prior = stored_obs.get((o["instrument_id"], o["accession"], o["holder_cik"], o["nature"]))
        verdict["added" if prior is None else "unchanged" if prior == o["shares"] else "changed"] += 1
    print(f"treatment observations (siblings included)      {len(treatment):>10,}")
    print(
        f"  added / unchanged / changed (changed must be 0)  "
        f"{verdict['added']:,} / {verdict['unchanged']:,} / {verdict['changed']:,}"
    )
    print("  removed: 0 by construction (record_insider_observation only upserts)")
    print(f"  from formerly tombstoned filings              {sum(o['tombstoned'] for o in treatment):>10,}")
    by_iid: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for o in treatment:
        by_iid[o["instrument_id"]].append(o)
    print(f"  instruments                                   {len(by_iid):>10,}")
    smoke = [symbols.get(i) for i in by_iid if symbols.get(i) in ab4._SMOKE]
    print(f"  smoke panel touched                           {smoke or 'none'}")
    print(f"gated keys (refused groups, siblings included)  {len(gated_keys):>10,}")
    conn.rollback()
    return {
        "by_iid": dict(by_iid),
        "holding_rows": holding_rows,
        "evidence_iids": evidence_iids,
        "symbols": symbols,
        "gated_keys": gated_keys,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="replay the cohort + full sync_insiders, and commit")
    ap.add_argument("--verify", action="store_true", help="check stored == recompute, for observations and _current")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET jit = off")
        conn.commit()
        if args.apply:
            return _apply(conn)
        if args.verify:
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            return _verify(conn)
        as_of = datetime.now(UTC)
        print(f"as_of {as_of.isoformat()}")
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ  # one snapshot for stages 0-1
        data = _stage01(conn, as_of)
        conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
        ids = sorted(set(data["by_iid"]) | data["evidence_iids"])
        # Every sibling of an instrument that gains a holding row: the rollup reads them by accession.
        ids = sorted({s for i in ids for s in sibling_instruments_for_instrument(conn, i)} | set(ids))
        conn.rollback()
        res = ab4._stage2(conn, data, ids, insert_sql=_INSERT_HOLDING, control_holdings_empty=False)
        ab4._report(res)
    return 0


if __name__ == "__main__":
    sys.exit(main())
