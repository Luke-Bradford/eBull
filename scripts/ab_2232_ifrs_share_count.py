"""Full-population parse-arm A/B for the #2232 IFRS share-count ingest.

Runs the REAL extractor — :meth:`SecFundamentalsProvider.extract_facts`, the same
call the steady-state ``fundamentals_sync`` path makes — over EVERY instrument in
the affected population and reports what the new ``ifrs-full`` route produces. It
writes nothing to the database: this is the parse-vs-STORED arm, which
``.claude/skills/engineering/full-population-ab.md`` requires be kept separate
from the stored arm because a parse change and a write change fail differently.

Population is the FULL affected set, never a sample: every CIK-mapped instrument
with ZERO ``us-gaap`` rows in ``financial_facts_raw``. That is the operative
definition of "foreign private issuer" available WITHOUT trusting a country or
exchange label — it is measured off our own store, so an issuer that files IFRS
while carrying a US country code still lands in the population instead of being
filtered out by an assumption.

⚠ The metric is DISTINCT INSTRUMENT, never fact-row count. One issuer tagging a
concept across twenty periods would otherwise read as twenty wins.

⚠ Counted per ``(concept, unit)``, not per concept. ``_extract_facts_from_section``
emits a row for EVERY unit in ``_UNIT_PRIORITY`` the tag appears under, so a
concept present only as ``pure`` or ``USD`` is not a share count and must not be
counted as one. The first version of this script had no unit filter while the
comment it fed in ``sec_fundamentals.py`` claimed the census was "in the shares
unit" — the prose and its own reproduction command disagreed. The per-unit table
is what makes that claim checkable instead of asserted.

⚠ THE GAIN SIDE IS THE POINT OF THIS SCRIPT, and on the first run it falsified
the change's original design. The ingest was going to feed
``share_count_history.shares_outstanding``; ``denominator_would_change`` lists
the instruments that would have taken an IFRS denominator, and inspecting those
two showed both would have taken a WRONG one (``AFYA`` 3,855,150 against its own
90,475,878 weighted average). The concepts are ingested as corroboration only,
and this column is the standing evidence for why — it must stay in the output
even though the answer is now "none of them, by construction".

Usage:

    PYTHONPATH=. uv run python -m scripts.ab_2232_ifrs_share_count \
        --out /tmp/ab2232_ifrs.jsonl
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
import time
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_fundamentals import (
    IFRS_TRACKED_CONCEPTS,
    SecFundamentalsProvider,
)

_IFRS_TAGS = frozenset(tag for tags in IFRS_TRACKED_CONCEPTS.values() for tag in tags)

# `gaap = 0` measured off our own store, not inferred from a label — see the
# module docstring. LEFT JOIN on the denominator view so an instrument with no
# share count at all still appears; those are exactly the rows where an IFRS
# value could reach a denominator, so dropping them would hide the gain side.
POPULATION_SQL = """
WITH per AS (
    SELECT instrument_id,
           count(*) FILTER (WHERE taxonomy = 'us-gaap') AS gaap
      FROM financial_facts_raw
     GROUP BY instrument_id
)
SELECT p.instrument_id, i.symbol, s.cik,
       l.latest_shares, l.as_of_date, l.source_taxonomy
  FROM per p
  JOIN instruments i USING (instrument_id)
  JOIN instrument_sec_profile s USING (instrument_id)
  LEFT JOIN instrument_share_count_latest l USING (instrument_id)
 WHERE p.gaap = 0
 ORDER BY p.instrument_id
"""


def _population(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(POPULATION_SQL)
        return cur.fetchall()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = the whole population")
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.12,
        help="seconds between SEC fetches; 0.12 stays under the 10 req/s ceiling",
    )
    args = ap.parse_args()

    provider = SecFundamentalsProvider(user_agent=settings.sec_user_agent)
    with psycopg.connect(settings.database_url) as conn:
        population = _population(conn)
    if args.limit:
        population = population[: args.limit]
    print(f"population (instruments with zero us-gaap facts): {len(population)}", flush=True)

    # Keyed ``(concept, unit)`` — see the module docstring. The ``shares``-unit
    # slice is the only one that could ever corroborate a share count.
    per_concept_unit: collections.Counter[tuple[str, str]] = collections.Counter()
    shares_union = 0
    any_concept = 0
    errors = 0
    denominator_would_change: list[str] = []

    with open(args.out, "w") as fh:
        for n, (iid, symbol, cik, latest, as_of, src) in enumerate(population, start=1):
            rec: dict[str, Any] = {
                "instrument_id": iid,
                "symbol": symbol,
                "cik": cik,
                "stored_shares": str(latest) if latest is not None else None,
                "stored_as_of": as_of.isoformat() if as_of is not None else None,
                "stored_source": src,
            }
            try:
                facts = provider.extract_facts(symbol, cik)
            except Exception as exc:  # noqa: BLE001 — an arm that dies mid-population proves nothing
                rec["error"] = f"{type(exc).__name__}: {exc}"
                errors += 1
                fh.write(json.dumps(rec) + "\n")
                fh.flush()
                continue

            # Keyed ``concept/unit``. Ordering breaks ties on
            # ``(period_end, filed_date, accession_number)``, not ``period_end``
            # alone: an issuer restating the same period end files a SECOND
            # accession for it, and picking between them on period_end only is
            # dict-insertion-order dependent — i.e. non-deterministic across
            # runs of a script whose whole job is to be re-runnable evidence.
            newest_by_key: dict[str, dict[str, Any]] = {}
            newest_rank: dict[str, tuple[str, str, str]] = {}
            for f in facts:
                if f.taxonomy != "ifrs-full" or f.concept not in _IFRS_TAGS or f.val is None:
                    continue
                key = f"{f.concept}/{f.unit}"
                rank = (
                    f.period_end.isoformat(),
                    f.filed_date.isoformat() if f.filed_date is not None else "",
                    f.accession_number or "",
                )
                if key not in newest_rank or rank > newest_rank[key]:
                    newest_rank[key] = rank
                    newest_by_key[key] = {
                        "concept": f.concept,
                        "unit": f.unit,
                        "period_end": rank[0],
                        "filed_date": rank[1] or None,
                        "accession_number": rank[2] or None,
                        "val": str(f.val),
                    }
            rec["ifrs"] = newest_by_key
            for hit in newest_by_key.values():
                per_concept_unit[(hit["concept"], hit["unit"])] += 1
            if any(hit["unit"] == "shares" for hit in newest_by_key.values()):
                shares_union += 1
            if newest_by_key:
                any_concept += 1
                # An instrument with no positive stored count is one where an
                # IFRS value could have reached the denominator. Recorded even
                # though the shipped change never lets it — this is the check
                # that killed the original design, so it stays measurable.
                if latest is None:
                    denominator_would_change.append(symbol)
            fh.write(json.dumps(rec) + "\n")
            fh.flush()
            if n % 40 == 0:
                print(f"  {n}/{len(population)} any_concept={any_concept} errors={errors}", flush=True)
            time.sleep(args.sleep)

    print("", flush=True)
    print(f"population                     {len(population)}", flush=True)
    print(f"errors                         {errors}", flush=True)
    print(f"instruments gaining any concept {any_concept}", flush=True)
    print(f"instruments with >=1 concept in the shares unit {shares_union}", flush=True)
    for (concept, unit), count in per_concept_unit.most_common():
        print(f"  {concept:34} {unit:12} {count}", flush=True)
    print(
        f"denominator_would_change       {len(denominator_would_change)} {sorted(denominator_would_change)}",
        flush=True,
    )
    print(
        "⚠ denominator_would_change is INFORMATIONAL — the shipped change adds no "
        "IFRS concept to share_count_history, so the live denominator cannot move.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
