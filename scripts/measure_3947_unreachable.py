"""#3947 amendment 1 measurements: durable DPS rows beyond the retention-swept raw layer.

Read-only. Reproduces the figures marked [A1]-[A5] in
``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` § Amendment 1:

* ``[A1]`` raw history depth: the most 10-K accessions any instrument holds in
  ``financial_facts_raw`` and when it was fetched. Above 3 only between a companyfacts refresh
  and the next ``financial_facts_retention_sweep`` (02:45 UTC), so the figure is time-dependent.
* ``[A2]`` stored ``dps_declared`` cells with no ``financial_periods_raw`` row now: a current unbacked-row
  count, which depends on when each instrument was last normalised. It is not the amendment's ``unreachable``.
* ``[A3]`` the seed population of Amendment 1 A1: accessions holding the concept in raw, plus
  accessions named in the ``source_ref`` of a ``sec_edgar`` canonical row holding ``dps_declared``.
  An accession's date resolves as A1 states: raw (any concept), else ``sec_filing_manifest.filed_at``,
  else the earliest ``filing_events.filing_date``. Undated accessions are counted by accession year.
* ``[A4]`` per issuer, that seed (dated on or after 2019-06-15) against SEC's companyconcept for the
  concept (filed on or after 2019-06-15). Illustrative, four issuers. One request each, 0.3 s apart.
* ``[A5]`` the scan's pace, from its latest ``job_runs`` note.

    PYTHONPATH=. uv run python -m scripts.measure_3947_unreachable [--skip-fetch]
"""

from __future__ import annotations

import argparse
import time

import httpx
import psycopg

from app.config import settings

_CONCEPT = "CommonStockDividendsPerShareDeclared"
_SCOPE = "2019-06-15"
_ISSUERS = ("WHR", "SCI", "EFC", "JNJ")

# One population definition for [A3] and [A4]: (accession, instrument, resolved date, in raw with the concept).
_SEED = """
WITH seed AS (
    SELECT accession_number AS acc, instrument_id FROM financial_facts_raw WHERE concept = %(concept)s
    UNION
    SELECT trim(a), p.instrument_id
    FROM financial_periods p, unnest(string_to_array(p.source_ref, ',')) a
    WHERE p.source = 'sec_edgar' AND p.dps_declared IS NOT NULL
),
raw AS (
    SELECT accession_number AS acc, max(filed_date) AS filed, bool_or(concept = %(concept)s) AS has_concept
    FROM financial_facts_raw GROUP BY 1
),
events AS (
    SELECT provider_filing_id AS acc, min(filing_date) AS filed FROM filing_events WHERE provider = 'sec' GROUP BY 1
),
dated AS (
    SELECT s.acc, s.instrument_id, COALESCE(r.filed, m.filed_at::date, e.filed) AS filed,
           COALESCE(r.has_concept, false) AS in_raw
    FROM seed s
    LEFT JOIN raw r ON r.acc = s.acc
    LEFT JOIN sec_filing_manifest m ON m.accession_number = s.acc
    LEFT JOIN events e ON e.acc = s.acc
)
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-fetch", action="store_true")
    args = ap.parse_args()
    with psycopg.connect(settings.database_url) as conn:

        def q(sql: str, params: object = None) -> list[tuple]:
            return conn.execute(sql, params).fetchall()  # type: ignore[arg-type]

        print(
            "[A1] max 10-K accessions per instrument in raw:",
            q("""
            SELECT i.symbol, s.n, s.first_filed, s.fetched FROM (
                SELECT instrument_id, count(DISTINCT accession_number) AS n, min(filed_date) AS first_filed,
                       max(fetched_at) AS fetched
                FROM financial_facts_raw WHERE form_type IN ('10-K', '10-K/A') GROUP BY 1 ORDER BY 2 DESC LIMIT 1
            ) s JOIN instruments i USING (instrument_id)"""),
            "now",
            q("SELECT now()")[0][0],
        )
        print(
            "[A2] unbacked dps_declared cells (cells, instruments):",
            q("""
            SELECT count(*), count(DISTINCT p.instrument_id) FROM financial_periods p
            WHERE p.source = 'sec_edgar' AND p.dps_declared IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM financial_periods_raw r
                WHERE r.instrument_id = p.instrument_id AND r.source = 'sec_edgar' AND r.fiscal_year = p.fiscal_year
                  AND r.period_type = p.period_type AND r.fiscal_quarter IS NOT DISTINCT FROM p.fiscal_quarter)"""),
        )
        params = {"concept": _CONCEPT, "scope": _SCOPE}
        print(
            "[A3] seed accessions: all; in raw >= scope; history >= scope; < scope; undated; instruments:",
            q(
                _SEED
                + """
            SELECT count(DISTINCT acc),
                   count(DISTINCT acc) FILTER (WHERE in_raw AND filed >= %(scope)s),
                   count(DISTINCT acc) FILTER (WHERE NOT in_raw AND filed >= %(scope)s),
                   count(DISTINCT acc) FILTER (WHERE filed < %(scope)s),
                   count(DISTINCT acc) FILTER (WHERE filed IS NULL),
                   count(DISTINCT instrument_id)
            FROM dated""",
                params,
            ),
        )
        print(
            "[A3] undated by accession year:",
            q(
                _SEED
                + """
            SELECT substr(acc, 12, 2), count(DISTINCT acc) FROM dated WHERE filed IS NULL GROUP BY 1 ORDER BY 1""",
                params,
            ),
        )
        if not args.skip_fetch:
            for sym in _ISSUERS:
                rows = q(
                    """
                    SELECT i.instrument_id, ei.identifier_value FROM instruments i JOIN external_identifiers ei
                      USING (instrument_id)
                    WHERE i.symbol = %s AND ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary""",
                    (sym,),
                )
                if len(rows) != 1:
                    raise SystemExit(f"[A4] {sym}: expected one primary SEC CIK, found {len(rows)}")
                ((iid, cik),) = rows
                seed = {
                    r[0]
                    for r in q(
                        _SEED + "SELECT acc FROM dated WHERE instrument_id = %(iid)s AND filed >= %(scope)s",
                        {"iid": iid, **params},
                    )
                }
                resp = httpx.get(
                    f"https://data.sec.gov/api/xbrl/companyconcept/CIK{int(cik):010d}/us-gaap/{_CONCEPT}.json",
                    headers={"User-Agent": settings.sec_user_agent},
                    timeout=30,
                )
                resp.raise_for_status()
                concept = {f["accn"] for fs in resp.json()["units"].values() for f in fs if f["filed"] >= _SCOPE}
                print(
                    f"[A4] {sym}: concept {len(concept)}, seed {len(seed)}, both {len(concept & seed)},"
                    f" concept only {sorted(concept - seed)}, seed only {sorted(seed - concept)}"
                )
                time.sleep(0.3)
        print(
            "[A5] latest scan run:",
            q("""
            SELECT started_at, finished_at, row_count, error_msg FROM job_runs
            WHERE job_name = 'sec_dps_inline_scale_scan' ORDER BY started_at DESC LIMIT 1"""),
        )


if __name__ == "__main__":
    main()
