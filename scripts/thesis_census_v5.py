"""#2069 — pre-backfill census stratification for the live v5 census (~2026-07-24).

The unstratified census samples held ∪ top-20 (richest-data names); the
backfill population is the T1/T2 tail where fair_value_band is often
absent and fundamentals are thinner — exactly where the v5 prompt's
tier-2 derived-basis and justified-abstention paths are stressed most.
This script segments the live ``prompt_version='v5'`` theses by
band-present × completeness tier and reports the census rates per
segment, then prints the stratified T2 band-absent pre-batch (~20 names)
to ``?force=`` BEFORE the wide backfill.

Read-only. Run at census time (numbers are only meaningful once a week
of hourly v5 rows exists); safe to run earlier — empty segments print
as such, never fabricate.

Usage:
    uv run python -m scripts.thesis_census_v5
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import psycopg
import psycopg.rows
from dotenv import load_dotenv

# Latest v5 thesis per instrument (hourly regen would double-count a
# name's behaviour otherwise), with its segment axes:
#   * band_present — fair_value_band_current base_value at the highest
#     method_version (fvb_v5 live per #2043)
#   * completeness_tier — the instrument's latest scores row (#209)
#   * coverage_tier — T1/T2/T3 (the backfill population is T1+T2)
_CENSUS_SQL = """
WITH v5 AS (
    SELECT DISTINCT ON (t.instrument_id) t.*
    FROM theses t
    WHERE t.prompt_version = 'v5'
    ORDER BY t.instrument_id, t.created_at DESC, t.thesis_version DESC, t.thesis_id DESC
)
SELECT v.thesis_id, v.instrument_id, i.symbol, v.stance,
       v.bear_value, v.base_value, v.bull_value,
       v.buy_zone_low, v.buy_zone_high,
       v.break_conditions_json::text AS break_conditions_text,
       (b.base_value IS NOT NULL) AS band_present,
       s.completeness_tier,
       c.coverage_tier
FROM v5 v
JOIN instruments i ON i.instrument_id = v.instrument_id
LEFT JOIN fair_value_band_current b
       ON b.instrument_id = v.instrument_id
      AND b.method_version = (SELECT max(method_version) FROM fair_value_band_current)
LEFT JOIN LATERAL (
    SELECT s.completeness_tier
    FROM scores s
    WHERE s.instrument_id = v.instrument_id
    ORDER BY s.scored_at DESC
    LIMIT 1
) s ON TRUE
LEFT JOIN coverage c ON c.instrument_id = v.instrument_id
ORDER BY v.instrument_id
"""

# Predicate premise state per thesis (#2012): only 'armed' can fire;
# 'already_true*' = the writer authored a break condition whose premise
# held at mint (the arm/baseline lesson — 35 Altman premise-conds).
_PREDICATES_SQL = """
SELECT thesis_id,
       count(*) AS n_predicates,
       count(*) FILTER (WHERE baseline_state IN ('already_true', 'already_true_after_gap')) AS n_premise_true
FROM thesis_break_predicates
GROUP BY thesis_id
"""

# Stratified pre-batch: ~20 tradable T2 names SKEWED band-absent (every
# band-absent name first — the population the wide backfill stresses
# most — then band-present fill). Ranked by latest score so the batch is
# names the backfill would actually reach.
_PREBATCH_SQL = """
SELECT i.instrument_id, i.symbol, s.score,
       (b.base_value IS NULL) AS band_absent
FROM instruments i
JOIN coverage c ON c.instrument_id = i.instrument_id AND c.coverage_tier = 2
LEFT JOIN fair_value_band_current b
       ON b.instrument_id = i.instrument_id
      AND b.method_version = (SELECT max(method_version) FROM fair_value_band_current)
LEFT JOIN LATERAL (
    SELECT s.total_score AS score
    FROM scores s
    WHERE s.instrument_id = i.instrument_id
    ORDER BY s.scored_at DESC
    LIMIT 1
) s ON TRUE
WHERE i.is_tradable
ORDER BY (b.base_value IS NULL) DESC, s.score DESC NULLS LAST, i.symbol
LIMIT 20
"""


@dataclass
class Segment:
    n: int = 0
    abstention: int = 0
    zoneless_buy: int = 0
    of_float: int = 0
    n_predicates: int = 0
    n_premise_true: int = 0
    symbols: list[str] = field(default_factory=list)


def _pct(num: int, den: int) -> str:
    return f"{num}/{den} ({num / den * 100:.1f}%)" if den else "0/0 (—)"


def run_census(conn: psycopg.Connection[object]) -> None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_CENSUS_SQL)
        theses = cur.fetchall()
        cur.execute(_PREDICATES_SQL)
        predicates = {row["thesis_id"]: row for row in cur.fetchall()}
        cur.execute(_PREBATCH_SQL)
        prebatch = cur.fetchall()

    segments: dict[tuple[bool, str], Segment] = {}
    for row in theses:
        key = (bool(row["band_present"]), str(row["completeness_tier"] or "<none>"))
        seg = segments.setdefault(key, Segment())
        seg.n += 1
        seg.symbols.append(str(row["symbol"]))
        if row["bear_value"] is None and row["base_value"] is None and row["bull_value"] is None:
            seg.abstention += 1
        if row["stance"] == "buy" and row["buy_zone_low"] is None and row["buy_zone_high"] is None:
            seg.zoneless_buy += 1
        if "of float" in (row["break_conditions_text"] or "").lower():
            seg.of_float += 1
        pred = predicates.get(row["thesis_id"])
        if pred:
            seg.n_predicates += int(pred["n_predicates"])
            seg.n_premise_true += int(pred["n_premise_true"])

    print(f"v5 census population (latest v5 thesis per instrument): {len(theses)}")
    if not theses:
        print("No v5 theses yet — run at census time (~2026-07-24).")
    print(
        f"{'band':>5} {'tier':>8} {'n':>4}  {'abstention':>16} {'zoneless_buy':>16} "
        f"{'of_float':>16}  {'premise_true (pred-level)':>26}"
    )
    for (band, tier), seg in sorted(segments.items()):
        print(
            f"{'yes' if band else 'no':>5} {tier:>8} {seg.n:>4}  "
            f"{_pct(seg.abstention, seg.n):>16} {_pct(seg.zoneless_buy, seg.n):>16} "
            f"{_pct(seg.of_float, seg.n):>16}  {_pct(seg.n_premise_true, seg.n_predicates):>26}"
        )

    print(f"\nStratified T2 pre-batch, band-absent skew ({len(prebatch)} names, run BEFORE wide backfill):")
    for row in prebatch:
        band = "band-absent" if row["band_absent"] else "band-present"
        print(f"  {row['symbol']:<10} instrument_id={row['instrument_id']} score={row['score']} {band}")
    print(
        "\nExecute per name (serially — hourly job shares the Ollama queue):\n"
        '  curl -s -X POST -H "Authorization: Bearer $SERVICE_TOKEN" \\\n'
        '    "http://localhost:8000/instruments/<SYMBOL>/thesis?force=true"\n'
        "Then eyeball the memos + run GET /theses/dq-audit over the batch."
    )


def main() -> None:
    load_dotenv("/Users/lukebradford/Dev/eBull/.env")
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        run_census(conn)


if __name__ == "__main__":
    main()
