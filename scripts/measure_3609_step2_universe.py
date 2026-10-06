"""#3609 step 2 spec premise 3: the book universe's size rule, measured on the stage-A panel.

Reads only the published stage-A artefact's admitted rows (ME, ``name_key``, characteristic presence, SIC) and
its frozen JKP ``nyse_cutoffs`` snapshot. It reads no return, holding status or characteristic value, so it
computes no outcome.

The top 1,000 is the step-2 book universe's own rule: ME at s(M) descending, ties by ``name_key`` ascending.
Per formation month, and as min/max over the stage-A grid, it prints:

- admitted names;
- names above JKP's NYSE 50th percentile cutoff (USD millions, normalised to USD);
- the ME of the 1,000th name;
- overlap of the top 1,000 with the above-NYSE-median set: shared names, top-1,000 names below the cutoff,
  above-cutoff names outside the top 1,000, and the top 1,000's share of the above-cutoff ME;
- among the top 1,000: names with all three step-2 families and with at least two (a family is present when any
  member has a value), and names with SIC 6221 (commodity contracts, the SIC commodity pools file under).

Refuses an artefact whose manifest digest is not the pinned stage-A artefact, whose frozen inputs or published
rows fail ``verify_artefact``, whose months are not the 80-month grid, which repeats a (M, name_key) pair or a
cutoff month, whose cutoff grid is incomplete, which carries a non-finite or non-positive ME or cutoff, or which
admits fewer than 1,000 names in a month.

Usage: ``uv run python -m scripts.measure_3609_step2_universe [artefact_dir]``
"""

from __future__ import annotations

import gzip
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

from app.services.factor_panel import formation_months
from scripts.build_3609_factor_panel import verify_artefact

DEFAULT_ARTEFACT = (
    Path.home() / "Library/Application Support/eBull/research/factor_panel_3609/2026-10-06-483ac6ae-stageA"
)
# sha256 of the stage-A artefact's manifest.json, as the step-2 spec's freeze evidence lists it.
STAGE_A_MANIFEST_SHA256 = "ee1e8abc241f26596529509cd38f4692de4de8ff8700c0ac8ca19c2a412f3c1e"
BOOK_UNIVERSE_SIZE = 1000
FAMILIES = (("gp_at",), ("be_me", "ni_me", "ocf_me"), ("at_gr1",))
COMMODITY_SIC = 6221


class MeasureError(RuntimeError):
    pass


def _families(characteristics: dict[str, dict[str, object]]) -> int:
    return sum(any(characteristics.get(k, {}).get("value") is not None for k in members) for members in FAMILIES)


def _finite_positive(value: float | str, what: str) -> float:
    number = float(value)
    if not (math.isfinite(number) and number > 0):
        raise MeasureError(f"{what} is not finite and positive: {value!r}")
    return number


def main(artefact: Path) -> None:
    manifest = verify_artefact(artefact, STAGE_A_MANIFEST_SHA256)
    if manifest.get("stage") != "A":
        raise MeasureError(f"{artefact} is not a stage-A artefact")
    grid = [m.isoformat() for m in formation_months()]
    cutoffs: dict[str, float] = {}
    with gzip.open(artefact / "inputs/reference_snapshot_jkp_nyse_cutoffs.jsonl.gz", "rt") as f:
        for line in f:
            key, month, value = json.loads(line)[:3]
            if key == "nyse_p50":
                if month in cutoffs:
                    raise MeasureError(f"duplicate nyse_p50 cutoff for {month}")
                cutoffs[month] = _finite_positive(value, f"nyse_p50 {month}") * 1e6
    missing = [m for m in grid if m not in cutoffs]
    if missing:
        raise MeasureError(f"nyse_p50 cutoff missing for {len(missing)} grid months, first {missing[0]}")
    by_month: dict[str, list[tuple[float, int, int, int | None]]] = defaultdict(list)
    seen: set[tuple[str, int]] = set()
    with gzip.open(artefact / "rows.jsonl.gz", "rt") as f:
        for line in f:
            row = json.loads(line)
            pair = (row["M"], row["name_key"])
            if pair in seen:
                raise MeasureError(f"duplicate name-month {pair}")
            seen.add(pair)
            if row["exclusion"] is None:
                me = _finite_positive(row["me"]["value"], f"ME {pair}")
                by_month[row["M"]].append((me, row["name_key"], _families(row["characteristics"]), row["sic"]))
    if sorted(by_month) != grid:
        raise MeasureError(f"months {sorted(by_month)[:1]}..{sorted(by_month)[-1:]} are not the stage-A grid")
    columns = (
        "admitted",
        "above_p50",
        "me_rank_1000",
        "shared",
        "top_below_p50",
        "p50_outside_top",
        "p50_me_share_in_top",
        "top_fam3",
        "top_fam2",
        "top_sic6221",
    )
    table: list[dict[str, float]] = []
    print("M", *columns, sep="\t")
    for month in grid:
        rows = sorted(by_month[month], key=lambda r: (-r[0], r[1]))
        if len(rows) < BOOK_UNIVERSE_SIZE:
            raise MeasureError(f"{month}: {len(rows)} admitted names, fewer than {BOOK_UNIVERSE_SIZE}")
        top = rows[:BOOK_UNIVERSE_SIZE]
        top_keys = {r[1] for r in top}
        p50 = cutoffs[month]
        above = [r for r in rows if r[0] > p50]
        above_keys = {r[1] for r in above}
        stats = {
            "admitted": len(rows),
            "above_p50": len(above),
            "me_rank_1000": top[-1][0],
            "shared": len(top_keys & above_keys),
            "top_below_p50": len(top_keys - above_keys),
            "p50_outside_top": len(above_keys - top_keys),
            "p50_me_share_in_top": sum(r[0] for r in above if r[1] in top_keys) / sum(r[0] for r in above),
            "top_fam3": sum(1 for r in top if r[2] == 3),
            "top_fam2": sum(1 for r in top if r[2] >= 2),
            "top_sic6221": sum(1 for r in top if r[3] == COMMODITY_SIC),
        }
        table.append(stats)
        print(month, *(f"{stats[c]:.4g}" for c in columns), sep="\t")
    print(f"months\t{len(table)}")
    for column in columns:
        values = [s[column] for s in table]
        print(f"{column}\tmin {min(values):.4g}\tmax {max(values):.4g}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ARTEFACT)
