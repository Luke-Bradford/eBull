"""#2351 M2 acceptance — bind every legacy DEF 14A row and diff the exposure with shipped.

Spec: docs/proposals/ownership/2026-09-24-2351-m2-binding.md § Acceptance.

    PYTHONPATH=. uv run python -m scripts.census_2351_binding --lines var/census_2351/m2_lines.pkl
    PYTHONPATH=. uv run python -m scripts.census_2351_binding --bind var/census_2351/m2_lines.pkl \
        --out var/census_2351/m2_decisions.jsonl

``--lines`` runs M1 over every body with stored rows (offline, read-only). ``--bind``
decides every legacy row; its only writes are the shipped ``sec_cover_12b_*`` cover cache
(through ``resolve_cover``, as the shipped job). Exits non-zero on a failed gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pickle
import subprocess
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.def14a_binding import (
    BINDING_RULE_VERSION,
    Decision,
    Identity,
    common_identity,
    decide_row,
    line_evidence,
)
from app.services.def14a_item403_lines import Item403Line, item403_lines
from app.services.def14a_recipients import (
    REASON_NON_COMMON_SIBLING,
    REASON_OTHER_COVER,
    Cover,
    FetchText,
    RunReport,
    Sibling,
    compute_desired,
    resolve_cover,
)

SLICE2: tuple[str, ...] = (REASON_NON_COMMON_SIBLING, REASON_OTHER_COVER)


def _m1(item: tuple[str, str]) -> tuple[str, str, list[Item403Line] | None]:
    accession, payload = item
    digest = hashlib.sha256(payload.encode()).hexdigest()
    try:
        ext = item403_lines(payload)
    except Exception:
        return accession, digest, None
    return accession, digest, None if ext.errors else ext.lines


def run_lines(out: Path, workers: int) -> None:
    result: dict[str, tuple[str, list[Item403Line] | None]] = {}
    with psycopg.connect(settings.database_url) as conn, conn.cursor(name="bodies") as cur:
        cur.itersize = 200
        cur.execute(
            """
            SELECT r.accession_number, r.payload FROM filing_raw_documents r
             WHERE r.document_kind = 'def14a_body'
               AND EXISTS (SELECT 1 FROM def14a_beneficial_holdings h WHERE h.accession_number = r.accession_number)
            """
        )
        with ProcessPoolExecutor(max_workers=workers) as pool:
            while batch := cur.fetchmany(400):
                for accession, digest, lines in pool.map(_m1, batch, chunksize=10):
                    result[accession] = (digest, lines)
                print(f"{len(result)} bodies", flush=True)
    out.write_bytes(pickle.dumps(result))
    print("COMPLETE", flush=True)


@dataclass
class Legacy:
    instrument_id: int
    accession: str
    holder: str
    shares: Decimal | None
    percent: Decimal | None


def _has_class_evidence(lines: list[Item403Line]) -> bool:
    return any(t.startswith(("class:", "series:")) or t == "preferred" for ln in lines for t in line_evidence(ln))


def bind(lines_path: Path, out: Path, fetch_text: FetchText) -> int:
    # The pickle is this script's own ``--lines`` output on the local disk, never external input.
    bodies: dict[str, tuple[str, list[Item403Line] | None]] = pickle.loads(lines_path.read_bytes())
    m1_sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        legacy = [
            Legacy(int(i), str(a), str(h), s, p)
            for i, a, h, s, p in conn.execute(
                "SELECT instrument_id, accession_number, holder_name, shares, percent_of_class"
                " FROM def14a_beneficial_holdings"
            ).fetchall()
        ]
        cik_rows = conn.execute(
            """
            SELECT e.instrument_id, min(e.identifier_value), count(DISTINCT e.identifier_value), i.symbol
              FROM external_identifiers e JOIN instruments i USING (instrument_id)
             WHERE e.provider = 'sec' AND e.identifier_type = 'cik'
             GROUP BY e.instrument_id, i.symbol
            """
        ).fetchall()
        cik_of = {int(i): str(c) for i, c, n, _ in cik_rows if n == 1}
        symbol_of = {int(i): str(s) for i, _, _, s in cik_rows}
        siblings: dict[str, list[Sibling]] = defaultdict(list)
        for iid, cik in sorted(cik_of.items()):
            siblings[cik].append(Sibling(iid, symbol_of[iid]))
        dates = dict(
            conn.execute(
                "SELECT provider_filing_id, min(filing_date) FROM filing_events"
                " WHERE provider = 'sec' AND provider_filing_id = ANY(%s) GROUP BY 1",
                (sorted({r.accession for r in legacy}),),
            ).fetchall()
        )
        acc_ledger = {
            (int(i), str(a)): str(r)
            for i, a, r in conn.execute(
                "SELECT instrument_id, accession_number, reason FROM def14a_recipient_suppressions"
            ).fetchall()
        }
        row_ledger = {
            (int(i), str(a), str(h))
            for i, a, h in conn.execute(
                "SELECT instrument_id, accession_number, holder_name FROM def14a_recipient_row_suppressions"
            ).fetchall()
        }
        ledger_hash = hashlib.sha256(repr(sorted(acc_ledger.items()) + sorted(row_ledger)).encode()).hexdigest()
        print("shipped compute_desired …", flush=True)
        report = RunReport()
        desired = compute_desired(conn, fetch_text, report)
        slice2 = {k for k, row in desired.accessions.items() if row[3] in SLICE2}

        covers: dict[tuple[str, str], Cover | None | str] = {}
        by_group: dict[tuple[str, str], list[Legacy]] = defaultdict(list)
        for r in legacy:
            by_group[(cik_of.get(r.instrument_id, ""), r.accession)].append(r)
        decisions: list[dict[str, Any]] = []
        for n, ((cik, accession), rows) in enumerate(sorted(by_group.items())):
            if n % 500 == 0:
                print(f"{n}/{len(by_group)} groups, fetches {report.fetches}", flush=True)
            digest, body_lines = bodies.get(accession, ("", None))
            sibs = siblings.get(cik, [])
            for r in rows:
                rec: dict[str, Any] = {
                    "instrument_id": r.instrument_id,
                    "symbol": symbol_of.get(r.instrument_id),
                    "accession": accession,
                    "holder": r.holder,
                    "legacy_shares": None if r.shares is None else str(r.shares),
                    "legacy_percent": None if r.percent is None else str(r.percent),
                    "cik": cik or None,
                    "siblings": len(sibs),
                    "proxy_date": str(dates.get(accession)),
                    "body_sha256": digest,
                    "sibling_symbols": sorted(s.symbol for s in sibs),
                }
                key = (r.instrument_id, accession)
                if not cik:
                    d = Decision("keep", "abstained:cik")
                elif r.instrument_id in desired.keep_instruments or key in desired.keep:
                    d = Decision("keep", "shipped:blocked")
                elif key in slice2:
                    d = Decision("withhold", "other_class:non_common_instrument")
                    rec["slice2_row"] = desired.accessions[key]
                elif body_lines is None:
                    d = Decision("keep", "abstained:body")
                else:
                    held = [ln for ln in body_lines if ln.holder_name == r.holder]
                    if not _has_class_evidence(body_lines):
                        d = Decision("keep", "abstained:no_class_evidence")
                    else:
                        if (cik, accession) not in covers:
                            covers[(cik, accession)] = _cover(conn, fetch_text, cik, sibs, dates.get(accession), report)
                        cover = covers[(cik, accession)]
                        if isinstance(cover, str) or cover is None:
                            d = Decision("keep", f"shipped:{cover}" if cover == "blocked" else "abstained:no_cover")
                        else:
                            me = next(s for s in sibs if s.instrument_id == r.instrument_id)
                            ident = common_identity(me, sibs, cover)
                            rec["cover"] = cover.accession
                            rec["cover_pairs"] = sorted(cover.pairs)
                            if isinstance(ident, Identity):
                                rec["identity"] = [ident.designator, ident.title]
                                d = decide_row(legacy_shares=r.shares, identity=ident, lines=held)
                            else:
                                d = Decision("keep", f"abstained:identity:{ident}")
                rec.update(outcome=d.outcome, reason=d.reason, lines=d.lines)
                rec["line_evidence"] = [
                    [
                        ln.table_ordinal,
                        ln.grid_row,
                        sorted(line_evidence(ln)),
                        [c.first_column for c in ln.amount_cells],
                    ]
                    for ln in (body_lines or [])
                    if ln.holder_name == r.holder and (ln.table_ordinal, ln.grid_row) in d.lines
                ]
                rec["new_shares"] = None if d.shares is None else str(d.shares)
                shipped_hidden = key in acc_ledger or (r.instrument_id, accession, r.holder) in row_ledger
                rec["shipped_hidden"] = shipped_hidden
                rec["category"] = _category(d, r.shares, shipped_hidden, blocked=d.reason == "shipped:blocked")
                decisions.append(rec)
    return _report(decisions, legacy, acc_ledger, row_ledger, out, m1_sha, ledger_hash)


def _cover(
    conn: psycopg.Connection[Any],
    fetch_text: FetchText,
    cik: str,
    sibs: list[Sibling],
    proxy_date: Any,
    report: RunReport,
) -> Cover | None | str:
    if proxy_date is None:
        return "blocked"
    res = resolve_cover(
        conn, fetch_text, issuer_cik=cik, sibling_ids=[s.instrument_id for s in sibs], proxy_date=proxy_date
    )
    report.fetches += res.fetches
    return "blocked" if res.unresolved else res.cover


def _category(d: Decision, legacy_shares: Decimal | None, shipped_hidden: bool, *, blocked: bool) -> str:
    """Exposure change against shipped. A rebind to the legacy count is the legacy figure."""
    if blocked:
        return "same"
    m2_hidden = d.outcome == "withhold"
    different = d.outcome == "rebind" and d.shares != legacy_shares
    if shipped_hidden and m2_hidden:
        return "same"
    if shipped_hidden:
        return "recovered" if different else "revived"
    if m2_hidden:
        return "newly_withheld"
    return "rebound" if different else "same"


def _shown(rec: dict[str, Any]) -> str | None:
    """The M2 exposure of a decided row: the shown share count, or None when hidden."""
    if rec["outcome"] == "withhold" or (rec["reason"] == "shipped:blocked" and rec["shipped_hidden"]):
        return None
    shares = rec["new_shares"] if rec["outcome"] == "rebind" else rec["legacy_shares"]
    return None if shares is None else format(Decimal(shares).normalize(), "f")


def _named_cases(decisions: list[dict[str, Any]]) -> list[tuple[str, bool]]:
    def find(symbol: str, accession: str, holder: str) -> list[dict[str, Any]]:
        return [
            r
            for r in decisions
            if r["symbol"] == symbol and r["accession"] == accession and holder.lower() in r["holder"].lower()
        ]

    googl = find("GOOGL", "0001308179-26-000342", "Page")
    meta = find("META", "0001326801-25-000040", "Zuckerberg")
    lennar = find("LEN", "0001193125-26-073504", "Miller")
    return [
        (
            "GOOGL 0001308179-26-000342 Page never shown 389,051,160",
            bool(googl) and all(_shown(r) != "389051160" for r in googl),
        ),
        (
            "META 0001326801-25-000040 Zuckerberg confirmed 141,000",
            bool(meta) and all(r["outcome"] == "confirmed" and _shown(r) == "141000" for r in meta),
        ),
        (
            "LEN 0001193125-26-073504 Miller never shown 21,851,560",
            bool(lennar) and all(_shown(r) != "21851560" for r in lennar),
        ),
    ]


def _report(
    decisions: list[dict[str, Any]],
    legacy: list[Legacy],
    acc_ledger: dict[tuple[int, str], str],
    row_ledger: set[tuple[int, str, str]],
    out: Path,
    m1_sha: str,
    ledger_hash: str,
) -> int:
    with out.open("w") as fh:
        fh.write(
            json.dumps({"rule_version": BINDING_RULE_VERSION, "git_sha": m1_sha, "ledger_sha256": ledger_hash}) + "\n"
        )
        for rec in decisions:
            fh.write(json.dumps(rec, default=str) + "\n")
    failed = False
    keys = [(r["instrument_id"], r["accession"], r["holder"]) for r in decisions]
    legacy_keys = [(r.instrument_id, r.accession, r.holder) for r in legacy]
    ok0 = len(keys) == len(set(keys)) and set(keys) == set(legacy_keys) and len(keys) == len(legacy_keys)
    orphan = [k for k in acc_ledger if not any((r.instrument_id, r.accession) == k for r in legacy)]
    orphan += [k for k in row_ledger if k not in set(legacy_keys)]
    print(f"0 reconciliation: {len(keys)} decisions / {len(legacy_keys)} legacy rows, unique+equal={ok0}")
    print(f"  ledger rows with no legacy row: {len(orphan)} {orphan[:10]}")
    failed |= not ok0 or bool(orphan)
    outcome = Counter((r["outcome"], r["reason"]) for r in decisions)
    print("outcome × reason:")
    for k, v in sorted(outcome.items()):
        print(f"  {k}: {v}")
    cats = Counter(r["category"] for r in decisions)
    print(f"1 exposure diff: {dict(cats)}")
    if cats["revived"]:
        failed = True
        print("  GATE FAIL revived:")
        for r in decisions:
            if r["category"] == "revived":
                print(f"    {r['symbol']} {r['accession']} {r['holder']!r} {r['reason']} {r['legacy_shares']}")
    print("2 changed decisions (issuer × category × reason):")
    changed = Counter(
        ("multi" if r["siblings"] > 1 else "single", r["category"], r["reason"])
        for r in decisions
        if r["category"] != "same"
    )
    for k, v in sorted(changed.items()):
        print(f"  {k}: {v}")
    print("4 named cases:")
    for label, ok in _named_cases(decisions):
        print(f"  {'ok  ' if ok else 'FAIL'} {label}")
        failed |= not ok
    print("6 identity outcomes (per instrument-accession with a cover):")
    per_instrument = {
        (r["instrument_id"], r["accession"]): (
            r["reason"]
            if r["reason"].startswith("abstained:identity")
            else ("lettered" if (r.get("identity") or [None])[0] else "plain")
        )
        for r in decisions
        if "cover" in r
    }
    ident = Counter(per_instrument.values())
    for k, v in sorted(ident.items()):
        print(f"  {k}: {v}")
    print("FAILED" if failed else "PASS")
    return 1 if failed else 0


def reads(lines_path: Path, decisions_path: Path, seed: int) -> None:
    """Acceptance 3: print every row the spec reads, with the holder's M1 lines, for a human read.

    Every recovered / rebound / multi-instrument newly_withheld row; single-instrument
    newly_withheld per reason stratum: all below 10, else a seeded random 10 (a sample).
    """
    import random

    # The pickle is this script's own ``--lines`` output on the local disk, never external input.
    bodies: dict[str, tuple[str, list[Item403Line] | None]] = pickle.loads(lines_path.read_bytes())
    rows = [json.loads(line) for line in decisions_path.read_text().splitlines()[1:]]
    picked = [r for r in rows if r["category"] in ("recovered", "rebound")]
    picked += [r for r in rows if r["category"] == "newly_withheld" and r["siblings"] > 1]
    strata: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        if r["category"] == "newly_withheld" and r["siblings"] <= 1:
            strata[r["reason"]].append(r)
    rng = random.Random(seed)
    every = sum(len(g) for g in strata.values()) <= 150
    for group in strata.values():
        picked += group if every or len(group) < 10 else rng.sample(group, 10)
    for r in sorted(picked, key=lambda x: (x["cik"] or "", x["accession"], x["symbol"] or "", x["holder"])):
        _, body_lines = bodies[r["accession"]]
        print(
            f"\n## {r['symbol']} {r['accession']} {r['holder']!r} {r['category']} {r['reason']}"
            f" legacy={r['legacy_shares']} new={r['new_shares']} identity={r.get('identity')}"
        )
        for ln in body_lines or []:
            if ln.holder_name != r["holder"]:
                continue
            cells = " | ".join(f"{c.raw_text} [{'/'.join(c.captions)}]" for c in ln.amount_cells)
            print(
                f"  t{ln.table_ordinal} r{ln.grid_row} {sorted(line_evidence(ln))} {ln.class_state}"
                f" cls={list(ln.row_class_cells)} sec={ln.section_label!r}: {cells}"
            )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=Path)
    ap.add_argument("--bind", type=Path)
    ap.add_argument("--reads", type=Path, help="decisions jsonl; needs --bind-lines")
    ap.add_argument("--bind-lines", type=Path, default=Path("var/census_2351/m2_lines.pkl"))
    ap.add_argument("--seed", type=int, default=2351)
    ap.add_argument("--out", type=Path, default=Path("var/census_2351/m2_decisions.jsonl"))
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()
    if args.lines:
        run_lines(args.lines, args.workers)
        return 0
    if args.reads:
        reads(args.bind_lines, args.reads, args.seed)
        return 0
    if args.bind:
        from app.providers.implementations.sec_edgar import SecFilingsProvider

        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            return bind(args.bind, args.out, provider.fetch_document_text)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
