"""Full-population A/B for the #2336 short-interest freshness gate.

Runs the REAL IAR reader — ``instrument_analytics._bulk_read_short_interest``,
the same call ``compute_rankings`` makes — over EVERY instrument holding a
``finra_short_interest_current`` row, and writes one record per instrument. The
control arm is the same script run on ``origin/main``: the reader's signature
gains a ``today`` keyword with the fix, so the script probes the signature and
labels its own arm from it. Nothing is simulated and nothing is written to the DB.

⚠ The metric is DISTINCT INSTRUMENT. ``finra_short_interest_current`` is already
one row per instrument, so rows and instruments coincide here — the ``population``
line prints both so a future reader can see that rather than assume it.

⚠ The GAIN side of this change is instruments whose signal appears where none
existed, which must be EMPTY: a freshness gate can only suppress. ``--compare``
prints it as its own line, and a non-zero value is a defect in the gate, not a
win.

Usage:

    # on the branch
    PYTHONPATH=. uv run python -m scripts.ab_2336_short_interest_staleness \
        --out /tmp/ab2336_gated.jsonl
    # on origin/main (control)
    PYTHONPATH=. uv run python -m scripts.ab_2336_short_interest_staleness \
        --out /tmp/ab2336_ungated.jsonl
    # then
    PYTHONPATH=. uv run python -m scripts.ab_2336_short_interest_staleness \
        --compare /tmp/ab2336_ungated.jsonl /tmp/ab2336_gated.jsonl
"""

from __future__ import annotations

import argparse
import collections
import inspect
import json
import sys
from typing import Any

import psycopg

from app.config import settings
from app.services.instrument_analytics import _bulk_read_short_interest

_POPULATION_SQL = """
    SELECT si.instrument_id, i.symbol, si.settlement_date
      FROM finra_short_interest_current si
      JOIN instruments i ON i.instrument_id = si.instrument_id
     ORDER BY si.instrument_id
"""

# Same shares_outstanding the scoring path feeds the analytics block:
# scoring._analytics_inputs takes fund_rows[0], newest fundamentals_snapshot by
# as_of_date. Reproduced here rather than importing the bulk loader, which pulls
# ~12 unrelated sources for a value this A/B needs one column of.
_SHARES_SQL = """
    SELECT DISTINCT ON (instrument_id) instrument_id, shares_outstanding
      FROM fundamentals_snapshot
     WHERE instrument_id = ANY(%(ids)s::bigint[])
     ORDER BY instrument_id, as_of_date DESC
"""

# ⚠ #2411 changed this reader underneath the script in two ways: the scoring denominator
# moved off `fundamentals_snapshot` onto `share_count_history`, and the reader gained a
# SECOND gate on the denominator's filed date. So re-running this script today no longer
# reproduces the `SUPPRESSED 19 / GAINED 0` recorded for #2336 — its gated arm now
# measures both gates. Kept runnable, and kept reading the snapshot above, because that
# is the input #2336's numbers were taken on; the query below exists to satisfy the new
# required argument with a filed date that belongs to the count rather than to whatever
# else was filed in the period. For the current denominator's own A/B see
# `scripts/ab_2411_share_count_denominator.py`.
_SHARE_COUNT_FILED_SQL = """
    SELECT DISTINCT ON (instrument_id) instrument_id, shares_outstanding_filed_date
      FROM share_count_history
     WHERE instrument_id = ANY(%(ids)s::bigint[]) AND shares_outstanding > 0
     ORDER BY instrument_id, period_end DESC
"""

# Instruments whose most recent `scores` row is the one an operator sees today.
_SCORED_SQL = """
    SELECT DISTINCT ON (instrument_id) instrument_id, model_version
      FROM scores
     WHERE instrument_id = ANY(%(ids)s::bigint[])
     ORDER BY instrument_id, scored_at DESC
"""


def _run(out_path: str) -> int:
    gated = "today" in inspect.signature(_bulk_read_short_interest).parameters
    arm = "gated" if gated else "ungated"
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_date")
            today = cur.fetchone()[0]  # type: ignore[index]
            cur.execute(_POPULATION_SQL)
            population = cur.fetchall()
            ids = [int(r[0]) for r in population]
            cur.execute(_SHARES_SQL, {"ids": ids})
            shares = {int(r[0]): (float(r[1]) if r[1] is not None else None) for r in cur.fetchall()}
            cur.execute(_SCORED_SQL, {"ids": ids})
            scored = {int(r[0]): r[1] for r in cur.fetchall()}

        kwargs: dict[str, Any] = {"today": today} if gated else {}
        # #2411 made the denominator's filed date a REQUIRED keyword on this reader, so
        # the call has to supply it or the script raises before producing an arm. Probed
        # the same way `gated` is, for the same reason: this script must keep running on
        # `origin/main` as well as here, and its own A/B is about `settlement_date` only.
        if "shares_outstanding_filed_by_id" in inspect.signature(_bulk_read_short_interest).parameters:
            with conn.cursor() as cur:
                cur.execute(_SHARE_COUNT_FILED_SQL, {"ids": ids})
                kwargs["shares_outstanding_filed_by_id"] = {int(r[0]): r[1] for r in cur.fetchall()}
        signals = _bulk_read_short_interest(conn, ids, shares, **kwargs)

    reasons: collections.Counter[str] = collections.Counter()
    with open(out_path, "w", encoding="utf-8") as fh:
        for iid, symbol, settlement in population:
            sig = signals[int(iid)]
            reasons[str(sig.get("reason") or ("signal" if sig.get("signal") is not None else "unknown"))] += 1
            fh.write(
                json.dumps(
                    {
                        "arm": arm,
                        "today": today.isoformat(),
                        "instrument_id": int(iid),
                        "symbol": symbol,
                        "settlement_date": settlement.isoformat() if settlement is not None else None,
                        "age_days": (today - settlement).days if settlement is not None else None,
                        "shares_outstanding": shares.get(int(iid)),
                        "scored_model_version": scored.get(int(iid)),
                        "signal": sig.get("signal"),
                        "reason": sig.get("reason"),
                        "short_pct": sig.get("short_pct"),
                        "asof": sig.get("asof"),
                    }
                )
                + "\n"
            )

    print(f"arm                {arm}", flush=True)
    print(f"today              {today}", flush=True)
    print(f"population rows    {len(population)}", flush=True)
    print(f"population ids     {len(set(ids))}", flush=True)
    print(f"scored ids         {len(scored)}", flush=True)
    for reason, n in reasons.most_common():
        print(f"  {reason:34} {n}", flush=True)
    return 0


def _load(path: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            out[int(rec["instrument_id"])] = rec
    return out


def _compare(control_path: str, treatment_path: str) -> int:
    control, treatment = _load(control_path), _load(treatment_path)
    arms = ({r["arm"] for r in control.values()}, {r["arm"] for r in treatment.values()})
    print(f"control arm        {sorted(arms[0])}", flush=True)
    print(f"treatment arm      {sorted(arms[1])}", flush=True)
    if arms[0] == arms[1]:
        print("⚠ BOTH FILES ARE THE SAME ARM — the comparison is vacuous.", flush=True)

    only_control = sorted(set(control) - set(treatment))
    only_treatment = sorted(set(treatment) - set(control))
    shared = sorted(set(control) & set(treatment))

    suppressed: list[dict[str, Any]] = []
    gained: list[dict[str, Any]] = []
    changed_value: list[dict[str, Any]] = []
    for iid in shared:
        c, t = control[iid], treatment[iid]
        if c["signal"] is not None and t["signal"] is None:
            suppressed.append(t)
        elif c["signal"] is None and t["signal"] is not None:
            gained.append(t)
        elif c["signal"] != t["signal"]:
            changed_value.append(t)

    print(f"population (shared){len(shared):>7}", flush=True)
    print(f"only in control    {len(only_control)}", flush=True)
    print(f"only in treatment  {len(only_treatment)}", flush=True)
    print(f"SUPPRESSED         {len(suppressed)}", flush=True)
    print(f"GAINED (must be 0) {len(gained)} {[g['symbol'] for g in gained][:20]}", flush=True)
    print(f"value changed      {len(changed_value)} {[g['symbol'] for g in changed_value][:20]}", flush=True)

    by_reason: collections.Counter[str] = collections.Counter(str(s["reason"]) for s in suppressed)
    for reason, n in by_reason.most_common():
        print(f"  reason {reason:28} {n}", flush=True)
    scored_suppressed = [s for s in suppressed if s["scored_model_version"] is not None]
    print(f"suppressed AND carrying a scores row {len(scored_suppressed)}", flush=True)
    if suppressed:
        ages = sorted(s["age_days"] for s in suppressed if s["age_days"] is not None)
        print(f"suppressed age_days min/median/max  {ages[0]}/{ages[len(ages) // 2]}/{ages[-1]}", flush=True)
        print("oldest 10 suppressed:", flush=True)
        for s in sorted(suppressed, key=lambda r: r["age_days"] or 0, reverse=True)[:10]:
            print(f"  {s['symbol']:8} settle={s['settlement_date']} age={s['age_days']}d", flush=True)
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
