"""#3189 findings 14/15/16 — full-population A/B of the ``blockholders_restated`` overlay.

    PYTHONPATH=. uv run python -m scripts.ab_3189_overlay --out /tmp/arm.json

Read-only. Dumps, per instrument, every row the overlay renders — the identity it
names, the filing it links, and the figure it publishes. Run it once per ARM and diff
the two files with ``--diff control.json treatment.json``.

**The control is the real code, never a simulation** (`full-population-ab.md`). Check
`app/services/ownership_rollup.py` out at `origin/main`, run this, restore, run again:

    git checkout origin/main -- app/services/ownership_rollup.py
    PYTHONPATH=. uv run python -m scripts.ab_3189_overlay --out /tmp/ab_control.json
    git checkout HEAD -- app/services/ownership_rollup.py
    PYTHONPATH=. uv run python -m scripts.ab_3189_overlay --out /tmp/ab_treatment.json
    PYTHONPATH=. uv run python -m scripts.ab_3189_overlay --diff /tmp/ab_control.json /tmp/ab_treatment.json

The diff's headline is a DISTINCT-ENTITY metric — distinct ``(instrument, filer
identity)`` overlay rows — not a row count, because a row whose identity changed is not
a row gained and a row count would report it as one gain and one loss.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

import psycopg

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services.ownership_rollup import get_ownership_rollup

#: Same population as ``scripts/census_3189_restatement_overlay_defects.py`` and the
#: #2215 census: an instrument with no stored blockholder row has no 13D/G channel.
POPULATION_SQL = """
    SELECT b.instrument_id, i.symbol
    FROM ownership_blockholders_current b
    JOIN instruments i ON i.instrument_id = b.instrument_id
    GROUP BY b.instrument_id, i.symbol
    ORDER BY b.instrument_id
"""


def _identity(filer_cik: str | None, filer_name: str) -> str:
    """Local copy of ``_identity_key``'s rule. Deliberately NOT imported: the two arms
    run against different revisions of that module, and a shared key must mean the same
    thing in both files."""
    if filer_cik is not None and filer_cik.strip():
        return f"CIK:{filer_cik.strip()}"
    return f"NAME:{filer_name.strip().lower()}"


def dump(argv_out: str, limit: int | None) -> int:
    started = time.monotonic()
    rows: dict[str, list[dict[str, Any]]] = {}
    with psycopg.connect(settings.database_url) as conn:
        population = conn.execute(POPULATION_SQL).fetchall()
        if limit is not None:
            population = population[:limit]
        total = len(population)
        print(f"population: {total} instruments", flush=True)
        for index, (instrument_id, symbol) in enumerate(population, start=1):
            try:
                with snapshot_read(conn):
                    rollup = get_ownership_rollup(conn, symbol, instrument_id)
            except Exception as exc:  # noqa: BLE001 - an A/B arm records failures
                rows[str(instrument_id)] = [{"error": f"{type(exc).__name__}: {exc}"}]
                continue
            overlay = [s for s in rollup.slices if s.category == "blockholders_restated"]
            rows[str(instrument_id)] = [
                {
                    "symbol": symbol,
                    "identity": _identity(h.filer_cik, h.filer_name),
                    "filer_name": h.filer_name,
                    "source": h.winning_source,
                    "accession": h.winning_accession,
                    "shares": str(h.shares),
                    "as_of": h.as_of_date.isoformat() if h.as_of_date is not None else None,
                }
                for s in overlay
                for h in s.holders
            ]
            if index % 500 == 0:
                elapsed = time.monotonic() - started
                print(f"  {index}/{total} in {elapsed:.0f}s", flush=True)

    with open(argv_out, "w", encoding="utf-8") as handle:
        json.dump(rows, handle)
    emitted = sum(len(v) for v in rows.values())
    print(f"wrote {argv_out}: {len(rows)} instruments, {emitted} overlay rows, {time.monotonic() - started:.0f}s")
    if limit is not None:
        print("  ⚠ --limit was set: this is a TIMING PROBE, not the population.")
    return 0


def diff(control_path: str, treatment_path: str) -> int:
    with open(control_path, encoding="utf-8") as handle:
        control: dict[str, list[dict[str, Any]]] = json.load(handle)
    with open(treatment_path, encoding="utf-8") as handle:
        treatment: dict[str, list[dict[str, Any]]] = json.load(handle)

    if set(control) != set(treatment):
        print(f"⚠ population differs: control {len(control)} vs treatment {len(treatment)} instruments")

    def keyed(arm: dict[str, list[dict[str, Any]]]) -> dict[tuple[str, str], dict[str, Any]]:
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for instrument_id, entries in arm.items():
            for entry in entries:
                if "error" in entry:
                    continue
                out[(instrument_id, entry["identity"])] = entry
        return out

    c, t = keyed(control), keyed(treatment)
    gained = sorted(set(t) - set(c))
    lost = sorted(set(c) - set(t))
    common = set(c) & set(t)
    changed_filing = [k for k in common if (c[k]["accession"], c[k]["source"]) != (t[k]["accession"], t[k]["source"])]
    changed_shares = [k for k in common if c[k]["shares"] != t[k]["shares"]]

    print(f"=== #3189 overlay A/B: {control_path} -> {treatment_path} ===")
    print(f"  distinct (instrument, filer) overlay rows — control:   {len(c)}")
    print(f"  distinct (instrument, filer) overlay rows — treatment: {len(t)}")
    print(f"  GAINED (a filer the control never named):             {len(gained)}")
    print(f"  LOST   (a filer the control named, treatment does not):{len(lost)}")
    print(f"  same filer, DIFFERENT filing published:                {len(changed_filing)}")
    print(f"  same filer, DIFFERENT figure published:                {len(changed_shares)}")
    print(f"  instruments gaining an overlay that had none:          {len({i for i, _ in gained} - {i for i, _ in c})}")
    # Inspect the gain side, per full-population-ab.md: a gain that is not readable is
    # not evidence.
    for label, keys in (("GAINED", gained), ("LOST", lost), ("RE-FILED", changed_filing)):
        print(f"\n  --- {label}: first 8 ---")
        for key in keys[:8]:
            entry = t.get(key) or c[key]
            was = c.get(key)
            suffix = f"  (was {was['filer_name']!r} {was['source']} {was['accession']} {was['shares']})" if was else ""
            print(
                f"    {entry['symbol']:>8}  {entry['filer_name'][:38]:<38} "
                f"{entry['source']:<4} {entry['accession']:<22} {entry['shares']}{suffix}"
            )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=str, default=None, help="dump one arm to this path")
    parser.add_argument("--limit", type=int, default=None, help="timing probe only; NOT the population")
    parser.add_argument("--diff", nargs=2, metavar=("CONTROL", "TREATMENT"), default=None)
    args = parser.parse_args(argv)

    if args.diff:
        return diff(*args.diff)
    if args.out:
        return dump(args.out, args.limit)
    parser.error("one of --out or --diff is required")


if __name__ == "__main__":
    sys.exit(main())
