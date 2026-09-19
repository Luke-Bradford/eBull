"""#3236 acceptance harness — freeze expectations, run, then diff everything.

Two modes:

  ``--freeze``  read-only. Snapshots the FULL ``blockholder_filings`` keyed
                state, every blockholder observation, ``_current`` for every
                instrument, and the expected repair set derived independently
                of the sweep. Writes JSON to ``--out``.
  ``--verify``  read-only. Re-reads the same state and diffs it against the
                frozen file.

⚠ The expectation is frozen BEFORE the run precisely so "zero resolvable NULLs
remain" cannot be satisfied by the wrong thing happening — linking to the wrong
instrument, writing no observation, or deleting the rows all produce zero NULLs
too. The verify step asserts the differing set EQUALS the frozen set.

Run:
  PYTHONPATH=. uv run python -m scripts.accept_3236_link_sweep --freeze --out /tmp/a.json
  PYTHONPATH=. uv run python -m scripts.accept_3236_link_sweep --verify --out /tmp/a.json
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_13dg import parse_primary_doc
from app.services.blockholders import (
    _resolve_issuer_to_instrument_id,
    resolve_blockholder_reporter_identity,
)

GOLDEN_PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")


def _connect() -> psycopg.Connection[tuple]:
    conn = psycopg.connect(settings.database_url, autocommit=False)
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    level = conn.execute("SHOW transaction_isolation").fetchone()
    assert level is not None and level[0] == "repeatable read", f"isolation is {level}"
    return conn


def _snapshot(conn: psycopg.Connection[tuple]) -> dict[str, Any]:
    """Whole-table keyed state for the three tables the sweep can touch."""
    filings = conn.execute(
        """
        SELECT filing_id, instrument_id, accession_number, submission_type, status,
               issuer_cik, issuer_cusip, securities_class_title, reporter_cik,
               reporter_name, aggregate_amount_owned, percent_of_class,
               date_of_event, filed_at, member_of_group
        FROM blockholder_filings ORDER BY filing_id
        """
    ).fetchall()
    observations = conn.execute(
        """
        SELECT instrument_id, reporter_cik, ownership_nature, source, source_document_id,
               period_end, reporter_name, submission_type, status_flag, source_accession,
               source_url, filed_at, aggregate_amount_owned, percent_of_class, known_to
        FROM ownership_blockholders_observations
        ORDER BY instrument_id, reporter_cik, source_document_id, period_end
        """
    ).fetchall()
    current = conn.execute(
        """
        SELECT instrument_id, reporter_cik, ownership_nature, reporter_name,
               submission_type, status_flag, source, source_document_id,
               filed_at, period_end, aggregate_amount_owned, percent_of_class
        FROM ownership_blockholders_current
        ORDER BY instrument_id, reporter_cik, ownership_nature
        """
    ).fetchall()
    return {
        "filings": {str(r[0]): [str(v) for v in r[1:]] for r in filings},
        "observations": {"|".join(str(v) for v in r[:6]): [str(v) for v in r[6:]] for r in observations},
        "current": {"|".join(str(v) for v in r[:3]): [str(v) for v in r[3:]] for r in current},
        "golden": _golden(conn),
    }


def _golden(conn: psycopg.Connection[tuple]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for symbol in GOLDEN_PANEL:
        rows = conn.execute(
            """
            SELECT count(*), coalesce(sum(c.aggregate_amount_owned), 0)
            FROM ownership_blockholders_current c
            JOIN instruments i USING (instrument_id)
            WHERE i.symbol = %(s)s
            """,
            {"s": symbol},
        ).fetchone()
        out[symbol] = [str(v) for v in (rows or ())]
    return out


def _expected_repairs(conn: psycopg.Connection[tuple]) -> dict[str, dict[str, Any]]:
    """The repair set, derived WITHOUT running the sweep: resolve the issuer,
    then re-parse the stored body and resolve the observation identity the same
    way the chokepoint will. Frozen so the verify step compares against a
    prediction, not against whatever happened."""
    expected: dict[str, dict[str, Any]] = {}
    candidates = conn.execute(
        """
        SELECT accession_number, min(issuer_cik), min(issuer_cusip),
               count(*) FILTER (WHERE instrument_id IS NULL), min(filed_at)
        FROM blockholder_filings
        WHERE instrument_id IS NULL
        GROUP BY accession_number ORDER BY accession_number
        """
    ).fetchall()
    for accession, cik, cusip, nulls, filed_at in candidates:
        iid = _resolve_issuer_to_instrument_id(conn, cusip=cusip, cik=cik)
        if iid is None:
            continue
        seen = conn.execute(
            """
            SELECT 1 FROM ownership_blockholders_observations
            WHERE source_document_id = %(a)s OR source_accession = %(a)s LIMIT 1
            """,
            {"a": accession},
        ).fetchone()
        if seen is not None:
            continue
        body = conn.execute(
            """
            SELECT payload FROM filing_raw_documents
            WHERE accession_number = %(a)s AND document_kind = 'primary_doc_13dg'
            """,
            {"a": accession},
        ).fetchone()
        if body is None:
            continue
        filing = parse_primary_doc(body[0])
        identity = resolve_blockholder_reporter_identity(
            filing.reporting_persons, document_filer_cik=filing.document_filer_cik
        )
        symbol = conn.execute("SELECT symbol FROM instruments WHERE instrument_id = %(i)s", {"i": iid}).fetchone()
        expected[accession] = {
            "instrument_id": iid,
            "symbol": symbol[0] if symbol else None,
            "null_rows": nulls,
            "reporter_cik": identity.reporter_cik if identity else None,
            "aggregate_amount_owned": str(identity.aggregate_amount_owned) if identity else None,
            "percent_of_class": str(identity.percent_of_class) if identity else None,
            "filed_at": str(filed_at),
            "period_end": str(filed_at.date()),
        }
    return expected


def freeze(out: str) -> None:
    with _connect() as conn:
        payload = {"snapshot": _snapshot(conn), "expected": _expected_repairs(conn)}
    with open(out, "w") as fh:
        json.dump(payload, fh)
    exp = payload["expected"]
    rows = sum(int(v["null_rows"]) for v in exp.values())
    iids = sorted({v["instrument_id"] for v in exp.values()})
    print(f"frozen -> {out}")
    print(f"  expected repairs : {len(exp)} accessions / {rows} rows")
    print(f"  instruments      : {len(iids)} {[v['symbol'] for v in exp.values()]}")
    print(f"  filings rows     : {len(payload['snapshot']['filings']):,}")
    print(f"  observations     : {len(payload['snapshot']['observations']):,}")
    print(f"  _current rows    : {len(payload['snapshot']['current']):,}")


def verify(out: str) -> None:
    with open(out) as fh:
        frozen = json.load(fh)
    with _connect() as conn:
        after = _snapshot(conn)
        still = conn.execute(
            """
            SELECT count(*) FROM blockholder_filings
            WHERE instrument_id IS NULL AND accession_number = ANY(%(a)s)
            """,
            {"a": list(frozen["expected"])},
        ).fetchone()

    before = frozen["snapshot"]
    expected = frozen["expected"]
    ok = True

    # -- 1/3. blockholder_filings: the differing set must EQUAL the repair set.
    changed = {k for k in before["filings"] if before["filings"][k] != after["filings"].get(k)}
    added = set(after["filings"]) - set(before["filings"])
    removed = set(before["filings"]) - set(after["filings"])
    expected_accs = set(expected)
    bad_col = []
    wrong_link = []
    for k in changed:
        b, a = before["filings"][k], after["filings"][k]
        # index 0 = instrument_id, 1 = accession_number
        if [x for i, x in enumerate(b) if i != 0] != [x for i, x in enumerate(a) if i != 0]:
            bad_col.append(k)
        if b[1] not in expected_accs or b[0] != "None":
            wrong_link.append(k)
        elif a[0] != str(expected[b[1]]["instrument_id"]):
            wrong_link.append(k)
    exp_rows = sum(int(v["null_rows"]) for v in expected.values())
    print(f"1/3 filings: changed={len(changed)} (expected {exp_rows}) added={len(added)} removed={len(removed)}")
    print(f"    columns other than instrument_id moved : {len(bad_col)}")
    print(f"    rows whose new link != frozen expectation: {len(wrong_link)}")
    print(f"    resolvable NULLs remaining on the cohort: {still[0] if still else '?'} (expected 0)")
    ok &= not added and not removed and not bad_col and not wrong_link
    ok &= len(changed) == exp_rows and (still[0] if still else 1) == 0

    # -- 2. observations: exactly one new LIVE row per repaired accession.
    new_obs = set(after["observations"]) - set(before["observations"])
    mutated = {k for k in before["observations"] if before["observations"][k] != after["observations"].get(k)}
    dropped = set(before["observations"]) - set(after["observations"])
    by_acc: dict[str, int] = {}
    mismatched = []
    for key in new_obs:
        iid, cik, _nature, _src, doc, period = key.split("|")
        by_acc[doc] = by_acc.get(doc, 0) + 1
        want = expected.get(doc)
        if want is None:
            mismatched.append(key)
            continue
        vals = after["observations"][key]
        # ⚠ Compare the amount NUMERICALLY. Postgres returns the stored
        # ``numeric(_, 4)`` form (``81363730.0000``) while the frozen
        # expectation came from the parser (``81363730.00``); a string
        # comparison fails all 19 for a difference that does not exist.
        if (
            iid != str(want["instrument_id"])
            or cik != str(want["reporter_cik"])
            or period != str(want["period_end"])
            or vals[-1] != "None"  # known_to: must be LIVE
            or Decimal(vals[6]) != Decimal(str(want["aggregate_amount_owned"]))
        ):
            mismatched.append(key)
    print(
        f"2.  observations: new={len(new_obs)} (expected {len(expected)}) mutated={len(mutated)} dropped={len(dropped)}"
    )
    print(f"    accessions with != 1 new row : {[a for a, n in by_acc.items() if n != 1]}")
    print(f"    rows not matching the frozen expectation: {len(mismatched)}")
    ok &= len(new_obs) == len(expected) and not mutated and not dropped and not mismatched
    ok &= all(n == 1 for n in by_acc.values()) and set(by_acc) == set(expected)

    # -- 5. _current: bounded to the expected instruments; golden panel frozen.
    exp_iids = {str(v["instrument_id"]) for v in expected.values()}
    cur_changed = {
        k for k in set(before["current"]) | set(after["current"]) if before["current"].get(k) != after["current"].get(k)
    }
    stray = {k for k in cur_changed if k.split("|")[0] not in exp_iids}
    print(f"5.  _current: changed={len(cur_changed)} rows, OUTSIDE expected instruments={len(stray)}")
    print(f"    golden panel unchanged: {before['golden'] == after['golden']}")
    ok &= not stray and before["golden"] == after["golden"]

    print()
    print("ACCEPTANCE:", "PASS" if ok else "FAIL")
    if not ok:
        raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--freeze", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.freeze:
        freeze(args.out)
    elif args.verify:
        verify(args.out)
    else:
        raise SystemExit("pass --freeze or --verify")


if __name__ == "__main__":
    main()
