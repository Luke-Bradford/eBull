"""Full-population A/B for #2358 — ``<br>`` as a line break on the Item 403 path.

Emits ONE JSON line per DEF 14A body, holding every field the change can move:

    {"acc": ..., "holders": {<holder_name_key>: {"shares": str|null,
                                                 "percent": str|null,
                                                 "role": str|null}}}

``holder_name_key`` is ``lower(trim(holder_name))`` — the generated column
``def14a_beneficial_holdings`` keys on — so "lost" here means what it means in
the read path. ``role`` is carried because arms 1 and 2 key on the NAME and are
blind to a column the change moves underneath it (#2164: 40 holders whose
``holder_role`` regressed from ``principal`` to ``None``, invisible to both).

⚠ Deliberately free of any symbol this ticket ADDS, so the SAME file runs
unmodified in a control checkout of ``origin/main``. Never reconstruct what main
would have done — run main (skill: "Harness integrity — a clean result is the
suspicious one").

    PYTHONPATH=. uv run python scripts/ab_2358_def14a_line_structure.py --out /tmp/2358-<arm>.jsonl

Diff the two arms with ``--diff control.jsonl treatment.jsonl``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_def14a import parse_beneficial_ownership_table

# An amount that moves by more than this factor between the arms is the #2358
# corruption clearing (or a new one appearing) rather than an ordinary
# re-extraction — the glue multiplies a count by 10**k for the digit-width k of
# the second amount, so the smallest real instance is three orders of magnitude.
_ORDER_OF_MAGNITUDE = Decimal(1000)


def _emit(out_path: str, limit: int | None) -> None:
    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """
    counters: Counter[str] = Counter()
    with psycopg.connect(settings.database_url) as conn, open(out_path, "w") as fh:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="ab_2358") as cur:
            cur.itersize = 20
            cur.execute(sql, {"limit": limit})
            for accession, body in cur:
                counters["accessions"] += 1
                if counters["accessions"] % 2000 == 0:
                    print(f"... {counters['accessions']} accessions", flush=True)
                try:
                    table = parse_beneficial_ownership_table(body)
                except Exception as exc:  # noqa: BLE001 — a census must not abort mid-corpus
                    counters["parse_errors"] += 1
                    print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                    continue
                holders: dict[str, Any] = {}
                for row in table.rows:
                    holders[row.holder_name.strip().lower()] = {
                        "shares": None if row.shares is None else str(row.shares),
                        "percent": None if row.percent_of_class is None else str(row.percent_of_class),
                        "role": row.holder_role,
                    }
                counters["holders"] += len(holders)
                fh.write(json.dumps({"acc": accession, "holders": holders}) + "\n")
    print("== emitted ==")
    for key, value in sorted(counters.items()):
        print(f"  {key:20s} {value:>10}")


def _load(path: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    with open(path) as fh:
        for line in fh:
            entry = json.loads(line)
            out[entry["acc"]] = entry["holders"]
    return out


def _diff(control_path: str, treatment_path: str) -> None:
    control = _load(control_path)
    treatment = _load(treatment_path)
    if set(control) != set(treatment):
        # Not defaulted away: an arm that scanned a different corpus cannot be
        # diffed, and a silently-truncated run looks exactly like a clean one.
        raise SystemExit(
            f"arms cover different accessions: control {len(control)}, treatment {len(treatment)}, "
            f"symmetric difference {len(set(control) ^ set(treatment))}"
        )

    lost: list[tuple[str, str]] = []
    gained: list[tuple[str, str]] = []
    share_drift: list[tuple[str, str, str | None, str | None]] = []
    # ⚠ `percent` and `role` are NOT optional arms. Both are columns the change
    # can write that arms 1 and 2 key nothing on, so a diff over holder identity
    # and shares alone reports CLEAN while a column moves underneath it. This
    # script shipped without the percent arm and a self-comparison of two
    # treatment runs — not the A/B itself — is what exposed it: one accession
    # (0001213900-26-076369) had gained nine fabricated percents.
    percent_drift: list[tuple[str, str, str | None, str | None]] = []
    role_drift: list[tuple[str, str, str | None, str | None]] = []
    for accession, before in control.items():
        after = treatment[accession]
        lost.extend((accession, name) for name in before.keys() - after.keys())
        gained.extend((accession, name) for name in after.keys() - before.keys())
        for name in before.keys() & after.keys():
            if before[name]["shares"] != after[name]["shares"]:
                share_drift.append((accession, name, before[name]["shares"], after[name]["shares"]))
            if before[name]["percent"] != after[name]["percent"]:
                percent_drift.append((accession, name, before[name]["percent"], after[name]["percent"]))
            if before[name]["role"] != after[name]["role"]:
                role_drift.append((accession, name, before[name]["role"], after[name]["role"]))

    order_of_magnitude = [
        entry
        for entry in share_drift
        if entry[2] is not None
        and entry[3] is not None
        and max(Decimal(entry[2]), Decimal(entry[3])) > _ORDER_OF_MAGNITUDE * min(Decimal(entry[2]), Decimal(entry[3]))
    ]
    dropped_amount = [entry for entry in share_drift if entry[2] is not None and entry[3] is None]
    gained_amount = [entry for entry in share_drift if entry[2] is None and entry[3] is not None]

    print("== arm 1: parse vs parse, keyed on lower(trim(holder_name)) ==")
    print(f"  accessions                {len(control):>8}")
    print(f"  holders control           {sum(len(v) for v in control.values()):>8}")
    print(f"  holders treatment         {sum(len(v) for v in treatment.values()):>8}")
    print(f"  distinct holders LOST     {len(lost):>8}  ({len({a for a, _ in lost})} accessions)")
    print(f"  distinct holders GAINED   {len(gained):>8}  ({len({a for a, _ in gained})} accessions)")
    print(f"  share value drift         {len(share_drift):>8}")
    print(f"    of which >1000x         {len(order_of_magnitude):>8}   <- the #2358 corruption clearing")
    print(f"    amount -> NULL          {len(dropped_amount):>8}")
    print(f"    NULL -> amount          {len(gained_amount):>8}")
    print(f"  percent drift             {len(percent_drift):>8}")
    print(f"  role drift                {len(role_drift):>8}")

    for label, rows in (("LOST", lost), ("GAINED", gained)):
        print(f"\n== {label} — every one, enumerated ==")
        for accession, name in sorted(rows):
            print(f"  {accession}  {name[:90]}")
    for label, rows in (
        ("order-of-magnitude share drift", order_of_magnitude),
        ("amount -> NULL", dropped_amount),
        ("NULL -> amount", gained_amount),
        ("percent drift", percent_drift),
        ("role drift", role_drift),
    ):
        print(f"\n== {label} — every one, enumerated ==")
        for accession, name, before_value, after_value in sorted(rows):
            print(f"  {accession}  {name[:70]}  {before_value} -> {after_value}")


def _stored(control_path: str, treatment_path: str) -> None:
    """Arm 2 — parse vs STORED.

    Arm 1 is blind to holders BOTH arms fail to reproduce: the diff is empty, so
    they never appear. Their rows survive in ``def14a_beneficial_holdings`` only
    because nothing has force-rewashed them (#2158's entire ticket lived here,
    181 accessions). The regression invariant needs both summaries, because the
    question is not "does the branch reproduce every stored holder" — it never
    did — but "is there a stored holder ``main`` reproduces and the branch does
    not".
    """
    control = _load(control_path)
    treatment = _load(treatment_path)
    sql = """
        SELECT accession_number, lower(trim(holder_name))
        FROM def14a_beneficial_holdings
    """
    stored: dict[str, set[str]] = {}
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        for accession, name in conn.execute(sql):
            stored.setdefault(accession, set()).add(name)

    scoped = {acc: names for acc, names in stored.items() if acc in control}
    regressions: list[tuple[str, str]] = []
    both_missing = 0
    for accession, names in scoped.items():
        for name in names:
            in_control = name in control[accession]
            in_treatment = name in treatment[accession]
            if in_control and not in_treatment:
                regressions.append((accession, name))
            elif not in_control and not in_treatment:
                both_missing += 1

    print("== arm 2: parse vs STORED ==")
    print(f"  stored rows in scope                 {sum(len(v) for v in scoped.values()):>8}")
    print(f"  accessions in scope                  {len(scoped):>8}")
    print(f"  stored holders NEITHER arm reproduces{both_missing:>8}   (arm 1 is blind to these)")
    print(f"  REGRESSIONS (control yes, branch no) {len(regressions):>8}")
    for accession, name in sorted(regressions):
        print(f"    {accession}  {name[:90]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--diff", nargs=2, metavar=("CONTROL", "TREATMENT"))
    ap.add_argument("--stored", nargs=2, metavar=("CONTROL", "TREATMENT"))
    args = ap.parse_args()
    if args.diff:
        _diff(*args.diff)
        return 0
    if args.stored:
        _stored(*args.stored)
        return 0
    if not args.out:
        ap.error("one of --out / --diff is required")
    _emit(args.out, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
