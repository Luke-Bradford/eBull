"""#3189 findings 14/15/16 — the three defects in the #2215 restatement overlay, on the full population.

    PYTHONPATH=. uv run python -m scripts.census_3189_restatement_overlay_defects

Read-only. No write, no migration, no job.

:func:`app.services.ownership_rollup._blockholder_restatement_holders` renders the
``blockholders_restated`` memo overlay — the 13D/G channels that
:func:`_reconcile_owner_once` folded into another wedge, so their absence from the
``blockholders`` wedge stops being indistinguishable from *"no 13D/G filer holds >5%"*.
The 2026-09-18 Codex re-review reported three defects in it. This measures all three
against the same population the #2215 census used, because they share one helper, one
scan and one corpus:

* **F14 — the overlay MISSES the case where 13D/G WON.** The helper inspects
  ``dropped_sources`` only. When an owner's 13D/G subtotal beats their 13F,
  :func:`_reconcile_owner_once` keeps ``figure_src = 13d|13g`` while still classifying
  the owner ``institutions`` / ``etfs`` (the branch is ``"13f" in present``, not
  "13f won"). The surviving row's ``winning_source`` is then ``13d``/``13g`` and it
  carries no dropped 13D/G entry, so no overlay row is emitted — and the
  ``blockholders`` wedge is absent for exactly the reason #2215 exists.
  Measured as: pie-wedge holders outside ``blockholders`` whose ``winning_source`` is
  a blockholder source, split by whether a ``blockholders`` wedge renders at all.

* **F15 — an overlay row can be attributed to the WRONG owner.**
  :class:`DroppedSource` carries no CIK or name (stated in
  :func:`_collapse_insider_control_group`'s own docstring as "same limitation as
  #1645"), and three passes append OTHER identities' rows to a representative's
  ``dropped_sources``: the same-accession collapse (#1764), the insider control-group
  collapse (#1652) and the 13D/G group collapse (#1645). The overlay stamps the
  REP's ``filer_cik`` / ``filer_name`` onto the DROPPED entry's accession.
  Measured with a source-grounded discriminator rather than an inference: look the
  entry's accession up in ``ownership_blockholders_current`` and compare the stored
  ``reporter_cik`` / ``reporter_name`` identity key against the overlay row's. A
  mismatch is a false provenance link the operator can click through to.

* **F16 — a tie is broken by iteration order.** ``max(dropped, key=shares)`` returns
  the FIRST maximal entry. The list order comes from :func:`_reconcile_owner_once`'s
  ``losing_sources = [s for s in present if s != figure_src]`` where ``present`` is a
  ``set[SourceTag]``; ``str`` hashing is salted per process, so the displayed
  source/accession for an owner whose 13D and 13G report the SAME figure can differ
  between two renders of identical data.
  Measured as: overlay-producing holders with >=2 dropped blockholder entries tied at
  the maximum share count on distinct ``(source, accession)``.

WHAT THIS DOES NOT ESTABLISH.

* Not a claim about the pie arithmetic. The overlay is non-additive
  (``denominator_basis="cross_channel_restatement"``); every additive path filters on
  the basis. All three defects are provenance/legibility, and F14 is an omission.
* Not the operator's view count — it measures every instrument with stored
  blockholder rows, not the ones anyone opened.
* F15's discriminator is one-directional: an accession absent from
  ``ownership_blockholders_current`` is reported separately (``unresolved``) rather
  than counted as either correct or false, because absence has other causes (a
  retention sweep, a read-path filter) and guessing would fabricate a rate.
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
from app.db.snapshot import snapshot_read
from app.services.ownership_rollup import (
    OwnershipRollup,
    _identity_key,
    get_ownership_rollup,
)

#: The two ``SourceTag`` values that land an owner in the blockholders slice.
BLOCKHOLDER_SOURCES: frozenset[str] = frozenset({"13d", "13g"})

#: Same population as ``scripts/census_2215_blockholder_visibility.py`` — an instrument
#: with no stored blockholder row has no 13D/G channel to fold, win or misattribute.
POPULATION_SQL = """
    SELECT b.instrument_id, i.symbol
    FROM ownership_blockholders_current b
    JOIN instruments i ON i.instrument_id = b.instrument_id
    GROUP BY b.instrument_id, i.symbol
    ORDER BY b.instrument_id
"""

#: The stored identity behind an accession, for F15's attribution check. One accession
#: can carry several reporters (a joint filing), so every one is returned and a match
#: against ANY of them clears the overlay row — the defect is "no stored reporter on
#: this accession is the owner we labelled it with", not "the first one is not".
ACCESSION_OWNERS_SQL = """
    SELECT source_accession, reporter_cik, reporter_name
    FROM ownership_blockholders_current
    WHERE instrument_id = %(instrument_id)s
      AND source_accession = ANY(%(accessions)s)
"""


def scan(rollup: OwnershipRollup) -> dict[str, Any]:
    """Classify one instrument's rollup against F14 / F15 / F16.

    Returns the per-instrument counters plus, for F15, the ``(overlay identity,
    accession)`` pairs the caller must resolve against the stored corpus.
    """
    wedge_renders = any(s.category == "blockholders" for s in rollup.slices)
    pie = [s for s in rollup.slices if s.denominator_basis == "pie_wedge"]

    f14_holders = 0
    f16_ties = 0
    overlay_rows = 0
    attribution_probes: list[dict[str, str]] = []

    for slice_ in pie:
        if slice_.category == "blockholders":
            continue
        for holder in slice_.holders:
            # F14: the owner's 13D/G channel WON the cross-channel MAX, so it is the
            # surviving figure and never became a ``DroppedSource``. The helper's scan
            # cannot see it, and the blockholders wedge is still absent.
            if holder.winning_source in BLOCKHOLDER_SOURCES:
                f14_holders += 1

            dropped = [d for d in holder.dropped_sources if d.source in BLOCKHOLDER_SOURCES]
            if not dropped:
                continue
            best_shares = max(d.shares for d in dropped)
            # ``_build_slice`` drops zero-share holders (#1916 Finding A), so a 0-share
            # folded channel produces no overlay row and cannot exhibit F15 or F16.
            if best_shares <= Decimal(0):
                continue
            overlay_rows += 1

            # F16: >=2 distinct filings tied at the maximum. ``max()`` returns the first,
            # and the list order is derived from ``set`` iteration upstream.
            tied = {(d.source, d.accession_number) for d in dropped if d.shares == best_shares}
            if len(tied) > 1:
                f16_ties += 1

            # F15: the accession the overlay will publish, under the identity it will
            # publish it as. Resolved against the corpus by the caller.
            best = next(d for d in dropped if d.shares == best_shares)
            attribution_probes.append(
                {
                    "accession": best.accession_number,
                    "identity": _identity_key(holder.filer_cik, holder.filer_name),
                    "filer_name": holder.filer_name,
                    "source": best.source,
                }
            )

    return {
        "wedge_renders": wedge_renders,
        "f14_holders": f14_holders,
        "f16_ties": f16_ties,
        "overlay_rows": overlay_rows,
        "attribution_probes": attribution_probes,
    }


def resolve_attribution(
    conn: psycopg.Connection,
    instrument_id: int,
    probes: list[dict[str, str]],
) -> dict[str, int]:
    """Count F15 outcomes for one instrument: ``matched`` / ``misattributed`` / ``unresolved``."""
    if not probes:
        return {"matched": 0, "misattributed": 0, "unresolved": 0}
    accessions = sorted({p["accession"] for p in probes})
    owners: dict[str, set[str]] = {}
    for accession, reporter_cik, reporter_name in conn.execute(
        ACCESSION_OWNERS_SQL, {"instrument_id": instrument_id, "accessions": accessions}
    ).fetchall():
        owners.setdefault(accession, set()).add(_identity_key(reporter_cik, reporter_name))

    out = {"matched": 0, "misattributed": 0, "unresolved": 0}
    for probe in probes:
        stored = owners.get(probe["accession"])
        if not stored:
            out["unresolved"] += 1
        elif probe["identity"] in stored:
            out["matched"] += 1
        else:
            out["misattributed"] += 1
    return out


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
    totals: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []

    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(POPULATION_SQL).fetchall()
        if args.limit is not None:
            population = population[: args.limit]
        total = len(population)
        print(f"population: {total} instruments with >=1 ownership_blockholders_current row", flush=True)

        for index, (instrument_id, symbol) in enumerate(population, start=1):
            try:
                # #2789: ``get_ownership_rollup`` issues many separate reads and opens no
                # transaction of its own, so a concurrent ownership refresh produces a TORN
                # render that raises nothing. Per instrument, not around the loop — one
                # snapshot spanning the whole scan would pin an old xmin on the dev cluster.
                with snapshot_read(conn):
                    rollup = get_ownership_rollup(conn, symbol, instrument_id)
            except Exception as exc:  # noqa: BLE001 - a census records failures, it does not abort
                totals["no_rollup"] += 1
                rows.append({"instrument_id": instrument_id, "symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
                continue

            verdict = scan(rollup)
            attribution = resolve_attribution(conn, instrument_id, verdict["attribution_probes"])

            totals["instruments"] += 1
            totals["overlay_rows"] += verdict["overlay_rows"]
            totals["f14_holders"] += verdict["f14_holders"]
            totals["f16_ties"] += verdict["f16_ties"]
            totals["f15_matched"] += attribution["matched"]
            totals["f15_misattributed"] += attribution["misattributed"]
            totals["f15_unresolved"] += attribution["unresolved"]
            if verdict["f14_holders"]:
                totals["instruments_with_f14"] += 1
                # The subset where F14 costs the operator the whole signal: no wedge at
                # all AND a 13D/G figure that won. "Wedge renders" is the milder case —
                # the wedge is present but understates by this owner.
                if not verdict["wedge_renders"]:
                    totals["instruments_f14_and_no_wedge"] += 1
            if verdict["f16_ties"]:
                totals["instruments_with_f16"] += 1
            if attribution["misattributed"]:
                totals["instruments_with_f15"] += 1
            if verdict["overlay_rows"]:
                totals["instruments_with_overlay"] += 1

            rows.append(
                {
                    "instrument_id": instrument_id,
                    "symbol": symbol,
                    "wedge_renders": verdict["wedge_renders"],
                    "overlay_rows": verdict["overlay_rows"],
                    "f14_holders": verdict["f14_holders"],
                    "f16_ties": verdict["f16_ties"],
                    **{f"f15_{k}": v for k, v in attribution.items()},
                    "probes": verdict["attribution_probes"],
                }
            )
            if index % 100 == 0:
                elapsed = time.monotonic() - started
                print(f"  {index}/{total} in {elapsed:.0f}s ({elapsed / index:.2f}s/instrument)", flush=True)

    print(f"\n=== #3189 findings 14/15/16 — {total} instruments ===", flush=True)
    print(f"  instruments scanned (rollup built):        {totals['instruments']}", flush=True)
    print(f"  instruments the rollup could not build:    {totals['no_rollup']}", flush=True)
    print(f"  instruments rendering >=1 overlay row:     {totals['instruments_with_overlay']}", flush=True)
    print(f"  overlay rows rendered today:               {totals['overlay_rows']}", flush=True)
    print("\n  F14 — 13D/G WON the cross-channel MAX and is rendered outside the wedge:", flush=True)
    print(f"    holders:                                 {totals['f14_holders']}", flush=True)
    print(f"    instruments:                             {totals['instruments_with_f14']}", flush=True)
    print(f"    ... of those, NO blockholders wedge:     {totals['instruments_f14_and_no_wedge']}", flush=True)
    print("\n  F15 — overlay row attributed to the identity that did not file it:", flush=True)
    print(f"    matched (accession names the overlay owner):   {totals['f15_matched']}", flush=True)
    print(f"    MISATTRIBUTED:                                 {totals['f15_misattributed']}", flush=True)
    print(f"    instruments affected:                          {totals['instruments_with_f15']}", flush=True)
    print(f"    unresolved (accession not in _current):        {totals['f15_unresolved']}", flush=True)
    print("\n  F16 — order-sensitive tie between >=2 dropped blockholder filings:", flush=True)
    print(f"    overlay rows with a tie:                 {totals['f16_ties']}", flush=True)
    print(f"    instruments:                             {totals['instruments_with_f16']}", flush=True)
    print(f"\n  elapsed: {time.monotonic() - started:.0f}s", flush=True)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, indent=1, default=str)
        print(f"  per-instrument rows: {args.json}", flush=True)

    if args.limit is not None:
        print("\n  ⚠ --limit was set: this is a TIMING PROBE, not the population.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
