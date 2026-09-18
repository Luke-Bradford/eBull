"""#2215 — how often the blockholders wedge is absent, and WHY, on the full population.

    PYTHONPATH=. uv run python -m scripts.census_2215_blockholder_visibility

Read-only. No write, no migration, no job.

#2215 states the defect from the five-name golden panel: every panel instrument
has ``ownership_blockholders_current`` rows and none renders a blockholders
wedge, so an operator cannot distinguish *"no 13D/G filer holds >5%"* from
*"a 13D/G filer does, but their 13F row won the cross-channel dedup"*. Those are
opposite conclusions from an identical rendering.

WHAT THIS MEASURES. Five states, not two — the ticket's own framing has only
two and the extra three are the reason a fix must pick a target rather than the
first cause noticed:

* ``rendered_complete``    — a blockholders wedge renders and no owner's 13D/G
                             channel was folded elsewhere. Honest today.
* ``rendered_partial``     — a wedge renders AND at least one other owner's
                             13D/G channel was folded into insiders /
                             institutions / etfs. The wedge is present and
                             UNDERSTATED, which the ticket does not describe.
* ``absent_folded``        — no wedge, and >=1 owner carries a dropped 13D/G
                             channel. This is the ticket's misleading case.
* ``absent_upstream``      — no wedge and NO dropped 13D/G channel anywhere, so
                             the stored rows never reached cross-channel
                             reconciliation at all (read-path filters, the
                             Rule 13d-5 group collapse, the same-accession
                             channel collapse, a stale-category cutoff). The
                             card is arguably honest here and a "deduped into
                             institutions" note would be a LIE.
* ``no_rollup``            — the rollup could not be built for the instrument.

WHY IT CALLS THE MODULE INSTEAD OF RE-STATING ITS RULES IN SQL. The fold is
``_reconcile_owner_once`` in Python, downstream of ``_dedup_by_priority``,
``_reconcile_institutional_families``, ``_reconcile_13d_groups``,
``_reconcile_insider_control_groups`` and the same-accession collapse. A SQL
identity-overlap query would measure a population the module does not have —
the failure #3104 slice 9 recorded ("my first census was SQL-only and its
'measurable' population was not the module's"). Calling
``get_ownership_rollup`` costs wall-clock and cannot drift from the code.

WHAT IT DOES NOT ESTABLISH.

* **Not a claim that ``absent_folded`` is wrong arithmetic.** The dedup is
  correct and #2215 says so explicitly; ``_PRIORITY_RANK`` is out of scope. What
  is measured is the LEGIBILITY of the absence, not the pie.
* **Not the operator's actual view count.** It measures every instrument that
  has blockholder rows, not the ones anyone opened.
* **Dropped-channel shares are the losing channel's owner subtotal**
  (``DroppedSource.shares``), which is an overlapping restatement of the same
  stake the surviving row already counts. Summing them is NOT an ownership
  figure and is reported only as the magnitude of what is invisible.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.ownership_rollup import OwnershipRollup, get_ownership_rollup

#: The two ``SourceTag`` values that land an owner in the blockholders slice.
BLOCKHOLDER_SOURCES: frozenset[str] = frozenset({"13d", "13g"})

POPULATION_SQL = """
    SELECT b.instrument_id, i.symbol, count(*) AS stored_rows
    FROM ownership_blockholders_current b
    JOIN instruments i ON i.instrument_id = b.instrument_id
    GROUP BY b.instrument_id, i.symbol
    ORDER BY b.instrument_id
"""


def classify(rollup: OwnershipRollup) -> dict[str, Any]:
    """Split one instrument's rollup into the five states above.

    ``folded_owners`` counts SURVIVING holders whose ``dropped_sources`` carry a
    13D/G entry — i.e. owners whose blockholder channel exists, was reconciled,
    and is not rendered as a blockholder.
    """
    wedge = next((s for s in rollup.slices if s.category == "blockholders"), None)
    folded_owners = 0
    overlay_filers = 0
    folded_shares = Decimal(0)
    for slice_ in rollup.slices:
        if slice_.category in {"blockholders", "blockholders_restated"}:
            continue
        for holder in slice_.holders:
            dropped = [d for d in holder.dropped_sources if d.source in BLOCKHOLDER_SOURCES]
            if dropped:
                folded_owners += 1
                # Largest of the owner's dropped blockholder channels: 13d and
                # 13g for one owner are the same stake restated, so summing them
                # would double the magnitude of a single invisible position.
                best = max(d.shares for d in dropped)
                folded_shares += best
                # ⚠ ``folded_owners`` is an UPPER BOUND on what the #2215 overlay
                # renders. ``_build_slice`` drops zero-share holders (#1916
                # Finding A) and a folded channel can legitimately be 0.0000 — GME
                # carries a 0-share Vanguard 13G behind a real Cohen 13D, so its
                # 2 folded owners render as 1 overlay row. Both are reported
                # because they answer different questions: how many owners are
                # invisible, and how many the fix makes visible.
                if best > 0:
                    overlay_filers += 1

    if wedge is not None:
        state = "rendered_partial" if folded_owners else "rendered_complete"
    else:
        state = "absent_folded" if folded_owners else "absent_upstream"

    return {
        "state": state,
        "wedge_filers": wedge.filer_count if wedge else 0,
        "wedge_shares": str(wedge.total_shares) if wedge else "0",
        "folded_owners": folded_owners,
        "overlay_filers": overlay_filers,
        "folded_shares": str(folded_shares),
        # Splits ``absent_upstream``: an instrument with no usable denominator
        # renders NO slices at all, so its empty blockholders wedge is not a
        # dedup artefact and no note of any wording would be true of it.
        "renders_nothing": not rollup.slices,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="stop after N instruments (timing probe only; a limited run is NOT the population)",
    )
    parser.add_argument("--json", type=str, default=None, help="write per-instrument rows to this path")
    args = parser.parse_args(argv)

    started = time.monotonic()
    states: Counter[str] = Counter()
    folded_owner_total = 0
    overlay_filer_total = 0
    instruments_gaining_an_overlay = 0
    renders_nothing_total = 0
    folded_shares_total = Decimal(0)
    rows: list[dict[str, Any]] = []

    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(POPULATION_SQL).fetchall()
        if args.limit is not None:
            population = population[: args.limit]
        total = len(population)
        print(f"population: {total} instruments with >=1 ownership_blockholders_current row", flush=True)

        for index, (instrument_id, symbol, stored_rows) in enumerate(population, start=1):
            try:
                rollup = get_ownership_rollup(conn, symbol, instrument_id)
            except Exception as exc:  # noqa: BLE001 - a census records failures, it does not abort on them
                states["no_rollup"] += 1
                rows.append(
                    {
                        "instrument_id": instrument_id,
                        "symbol": symbol,
                        "stored_rows": stored_rows,
                        "state": "no_rollup",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
            else:
                verdict = classify(rollup)
                states[verdict["state"]] += 1
                folded_owner_total += verdict["folded_owners"]
                overlay_filer_total += verdict["overlay_filers"]
                instruments_gaining_an_overlay += 1 if verdict["overlay_filers"] else 0
                renders_nothing_total += 1 if verdict["renders_nothing"] else 0
                folded_shares_total += Decimal(verdict["folded_shares"])
                rows.append({"instrument_id": instrument_id, "symbol": symbol, "stored_rows": stored_rows, **verdict})
            if index % 100 == 0:
                elapsed = time.monotonic() - started
                print(
                    f"  {index}/{total} in {elapsed:.0f}s ({elapsed / index:.2f}s/instrument)",
                    flush=True,
                )

    print(f"\n=== #2215 blockholder wedge visibility — {total} instruments ===", flush=True)
    for state in ("rendered_complete", "rendered_partial", "absent_folded", "absent_upstream", "no_rollup"):
        count = states[state]
        pct = (count / total * 100) if total else 0.0
        print(f"  {state:20s} {count:6d}  {pct:6.2f}%", flush=True)
    print(f"\n  owners whose 13D/G channel is folded out of the wedge: {folded_owner_total}", flush=True)
    print(f"  ... of those, non-zero and therefore RENDERED by the overlay: {overlay_filer_total}", flush=True)
    print(f"  instruments that gain a blockholders_restated overlay: {instruments_gaining_an_overlay}", flush=True)
    print(f"  instruments rendering NO slices at all (no usable denominator): {renders_nothing_total}", flush=True)
    print(f"  dropped-channel shares (NOT an ownership figure): {folded_shares_total}", flush=True)
    print(f"  elapsed: {time.monotonic() - started:.0f}s", flush=True)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, indent=1)
        print(f"  per-instrument rows: {args.json}", flush=True)

    if args.limit is not None:
        print("\n  ⚠ --limit was set: this is a TIMING PROBE, not the population.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
