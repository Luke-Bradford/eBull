"""Full-population A/B for the #2411 IAR analytics denominator.

Runs the REAL scoring read path — ``scoring._bulk_load_instrument_data`` then
``scoring._analytics_inputs`` then ``instrument_analytics.assemble_instrument_analytics_bulk``,
exactly the three calls ``compute_rankings`` makes — over EVERY instrument, and writes one
record per instrument. Nothing is simulated and nothing is written to the DB.

The control arm is this same script run in a worktree at ``origin/main``. It labels its
own arm from the loader's data shape — ``share_count_row`` is the key #2411 added to
``_empty_instrument_data`` — cross-checked against ``_analytics_inputs``'s return arity,
and refuses to run if the two disagree. A pair of files that carry the same label makes
``--compare`` say so rather than print a vacuous diff.

⚠ The metric is DISTINCT INSTRUMENT, not rows. Both signals under test are one per
instrument, so the two coincide here — ``--compare`` prints both counts rather than
leaving a reader to assume it.

⚠ This change has a real GAIN side and it is the point of the ticket, so the gain side
is enumerated and sampled, not just counted (skill: "a change that only adds looks safe
and isn't"). It also has a LOSS side from two distinct causes that must be kept apart:

  * a share count that exists in ``fundamentals_snapshot`` but not in the settled
    ``share_count_history`` — a source-coverage loss;
  * a share count whose FILING is older than ``share_count_filed`` (183d) — the new
    freshness gate, which can only suppress and only on the short-interest ratio.

Usage:

    # on the branch
    PYTHONPATH=. uv run python -m scripts.ab_2411_share_count_denominator \
        --out /tmp/ab2411_settled.jsonl
    # in a worktree at origin/main (control)
    PYTHONPATH=. uv run python -m scripts.ab_2411_share_count_denominator \
        --out /tmp/ab2411_snapshot.jsonl
    # then, from either tree
    PYTHONPATH=. uv run python -m scripts.ab_2411_share_count_denominator \
        --compare /tmp/ab2411_snapshot.jsonl /tmp/ab2411_settled.jsonl
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.config import settings
from app.services.instrument_analytics import assemble_instrument_analytics_bulk
from app.services.scoring import _analytics_inputs, _bulk_load_instrument_data, _empty_instrument_data

_POPULATION_SQL = "SELECT instrument_id, symbol FROM instruments ORDER BY instrument_id"

# Instruments whose most recent `scores` row is the one an operator sees today —
# so a moved signal can be reported as "moved on a scored instrument" rather than
# "moved somewhere in the corpus".
_SCORED_SQL = """
    SELECT DISTINCT ON (instrument_id) instrument_id, model_version
      FROM scores
     ORDER BY instrument_id, scored_at DESC
"""


def _signal_of(block: dict[str, Any], name: str) -> tuple[Any, Any]:
    sig = block.get("positioning", {}).get(name, {})
    return sig.get("signal"), sig.get("reason")


def _run(out_path: str) -> int:
    # Label the arm from the loader's DATA SHAPE, not from a flag the caller could set
    # wrong: `share_count_row` is the key #2411 added to `_empty_instrument_data`, and
    # `_analytics_inputs` reads the denominator from it. A shape probe survives edits to
    # the function's docstring and log strings, which a code-object probe does not.
    shape = _empty_instrument_data()
    settled_arm = "share_count_row" in shape
    arm = "settled" if settled_arm else "snapshot"

    # Belt and braces, because a mislabelled arm makes `--compare` print a confident
    # diff of the wrong thing: the return arity must agree with the shape. `main`
    # returns (gics, shares); the branch returns (gics, shares, filed).
    probe = _analytics_inputs(shape)
    if (len(probe) == 3) != settled_arm:
        raise SystemExit(f"cannot label arm — shape has share_count_row={settled_arm} but arity is {len(probe)}")

    now = datetime.now(tz=UTC)
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_date")
            today = cur.fetchone()[0]  # type: ignore[index]
            cur.execute(_POPULATION_SQL)
            population = cur.fetchall()
            cur.execute(_SCORED_SQL)
            scored = {int(r[0]): r[1] for r in cur.fetchall()}

        ids = [int(r[0]) for r in population]
        bulk = _bulk_load_instrument_data(conn, ids, now)

        gics_by_id: dict[int, str | None] = {}
        shares_by_id: dict[int, float | None] = {}
        filed_by_id: dict[int, Any] = {}
        for iid in ids:
            got = _analytics_inputs(bulk[iid])
            gics_by_id[iid] = got[0]
            shares_by_id[iid] = got[1]
            filed_by_id[iid] = got[2] if len(got) > 2 else None

        kwargs: dict[str, Any] = {"shares_outstanding_filed_by_id": filed_by_id} if settled_arm else {}
        blocks = assemble_instrument_analytics_bulk(
            conn,
            ids,
            gics_sector_by_id=gics_by_id,
            shares_outstanding_by_id=shares_by_id,
            today=today,
            **kwargs,
        )

    si_reasons: collections.Counter[str] = collections.Counter()
    with open(out_path, "w", encoding="utf-8") as fh:
        for iid, symbol in population:
            iid = int(iid)
            block = blocks[iid]
            si_sig, si_reason = _signal_of(block, "short_interest")
            ins_sig, ins_reason = _signal_of(block, "insider_net_90d")
            si_reasons[str(si_reason or ("signal" if si_sig is not None else "unknown"))] += 1
            filed = filed_by_id.get(iid)
            fh.write(
                json.dumps(
                    {
                        "arm": arm,
                        "today": today.isoformat(),
                        "instrument_id": iid,
                        "symbol": symbol,
                        "shares_outstanding": shares_by_id.get(iid),
                        "shares_filed": filed.isoformat() if filed is not None else None,
                        "scored_model_version": scored.get(iid),
                        "si_signal": si_sig,
                        "si_reason": si_reason,
                        "insider_signal": ins_sig,
                        "insider_reason": ins_reason,
                    }
                )
                + "\n"
            )

    print(f"arm                    {arm}", flush=True)
    print(f"today                  {today}", flush=True)
    print(f"population rows        {len(population)}", flush=True)
    print(f"population ids         {len(set(ids))}", flush=True)
    print(f"with a denominator     {sum(1 for v in shares_by_id.values() if v)}", flush=True)
    print(f"scored ids             {len(scored)}", flush=True)
    for reason, n in si_reasons.most_common():
        print(f"  short_interest {reason:28} {n}", flush=True)
    return 0


def _load(path: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            out[int(rec["instrument_id"])] = rec
    return out


def _diff_one(
    control: dict[int, dict[str, Any]],
    treatment: dict[int, dict[str, Any]],
    shared: list[int],
    key: str,
    label: str,
) -> None:
    gained: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    changed: list[tuple[dict[str, Any], Any, Any]] = []
    for iid in shared:
        c, t = control[iid], treatment[iid]
        cv, tv = c[key], t[key]
        if cv is None and tv is not None:
            gained.append(t)
        elif cv is not None and tv is None:
            suppressed.append(t)
        elif cv != tv:
            changed.append((t, cv, tv))

    print(f"\n--- {label} ---", flush=True)
    print(f"GAINED                 {len(gained)}", flush=True)
    print(f"SUPPRESSED             {len(suppressed)}", flush=True)
    print(f"value changed          {len(changed)}", flush=True)
    scored_moved = sum(1 for r in gained + suppressed + [c[0] for c in changed] if r["scored_model_version"])
    print(f"…of which carry a scores row  {scored_moved}", flush=True)

    if suppressed:
        by_reason = collections.Counter(str(r[f"{key.split('_')[0]}_reason"]) for r in suppressed)
        for reason, n in by_reason.most_common():
            print(f"  suppressed reason {reason:26} {n}", flush=True)
        print("  sample suppressed:", flush=True)
        for r in suppressed[:10]:
            print(f"    {r['symbol']:8} shares={r['shares_outstanding']} filed={r['shares_filed']}", flush=True)
    if gained:
        print("  sample gained (INSPECT — a change that only adds looks safe and isn't):", flush=True)
        for r in gained[:15]:
            print(f"    {r['symbol']:8} shares={r['shares_outstanding']} filed={r['shares_filed']}", flush=True)
    if changed:
        biggest = sorted(changed, key=lambda x: abs((x[2] or 0) - (x[1] or 0)), reverse=True)[:10]
        print("  largest value moves:", flush=True)
        for r, cv, tv in biggest:
            print(f"    {r['symbol']:8} {cv} -> {tv}  shares={r['shares_outstanding']}", flush=True)


def _compare(control_path: str, treatment_path: str) -> int:
    control, treatment = _load(control_path), _load(treatment_path)
    arms = ({r["arm"] for r in control.values()}, {r["arm"] for r in treatment.values()})
    print(f"control arm            {sorted(arms[0])}", flush=True)
    print(f"treatment arm          {sorted(arms[1])}", flush=True)
    if arms[0] == arms[1]:
        print("⚠ BOTH FILES ARE THE SAME ARM — the comparison is vacuous.", flush=True)

    shared = sorted(set(control) & set(treatment))
    print(f"population (shared)    {len(shared)}", flush=True)
    print(f"only in control        {len(set(control) - set(treatment))}", flush=True)
    print(f"only in treatment      {len(set(treatment) - set(control))}", flush=True)

    denom_gained = [
        treatment[i] for i in shared if not control[i]["shares_outstanding"] and treatment[i]["shares_outstanding"]
    ]
    denom_lost = [
        control[i] for i in shared if control[i]["shares_outstanding"] and not treatment[i]["shares_outstanding"]
    ]
    print(f"denominator GAINED     {len(denom_gained)}", flush=True)
    print(f"denominator LOST       {len(denom_lost)} {[r['symbol'] for r in denom_lost][:20]}", flush=True)

    _diff_one(control, treatment, shared, "si_signal", "short_interest")
    _diff_one(control, treatment, shared, "insider_signal", "insider_net_90d")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", help="write one JSONL record per instrument for this arm")
    ap.add_argument("--compare", nargs=2, metavar=("CONTROL", "TREATMENT"), help="diff two arm files")
    args = ap.parse_args()
    if args.compare:
        return _compare(args.compare[0], args.compare[1])
    if not args.out:
        ap.error("one of --out / --compare is required")
    return _run(args.out)


if __name__ == "__main__":
    sys.exit(main())
