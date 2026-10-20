"""#3908 premise: why the #3841 PIT build refuses ``no_left_anchor``, and what the 13F observation layer recovers.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` (Amendment 2). Read-only; prints its as-of time.
Population: the ``instrument_share_count_pit`` steps covering the run date.

    PYTHONPATH=. uv run python scripts/measure_3908_premise.py
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from app.config import settings

_CURRENT = """
    SELECT s.instrument_id, s.refusal,
           s.evidence->'basis'->'window'->0->>'status' AS left_status,
           (s.evidence->'basis'->'window'->0->>'filers')::int AS left_filers,
           (s.evidence->'basis'->'window'->0->>'q')::date AS left_q,
           EXISTS (SELECT 1 FROM external_identifiers x
                   WHERE x.instrument_id = s.instrument_id AND x.identifier_type = 'cusip') AS has_cusip
    FROM instrument_share_count_pit s
    WHERE s.valid_from <= CURRENT_DATE AND (s.valid_to IS NULL OR s.valid_to > CURRENT_DATE)
"""

QUERIES: dict[str, str] = {
    "steps covering today: refusal x has a CUSIP": f"""
        WITH cur AS ({_CURRENT})
        SELECT coalesce(refusal, 'published'), has_cusip, count(*) FROM cur GROUP BY 1, 2 ORDER BY 1, 2""",
    "no_left_anchor: left-anchor status x filers at q_a": f"""
        WITH cur AS ({_CURRENT})
        SELECT left_status,
               CASE WHEN left_filers = 0 THEN '0' WHEN left_filers < 5 THEN '1-4' WHEN left_filers < 10 THEN '5-9'
                    WHEN left_filers < 20 THEN '10-19' ELSE '20+' END,
               count(*)
        FROM cur WHERE refusal = 'no_left_anchor' GROUP BY 1, 2 ORDER BY 1, 2""",
    "thin left anchors: distinct 13F filers at q_a in the observation layer": f"""
        WITH cur AS ({_CURRENT}),
        thin AS (SELECT * FROM cur WHERE refusal = 'no_left_anchor' AND left_status = 'thin'),
        o AS (SELECT t.instrument_id, count(DISTINCT o.filer_cik) AS n
              FROM thin t JOIN ownership_institutions_observations o
                ON o.instrument_id = t.instrument_id AND o.period_end = t.left_q
              WHERE o.source = '13f' AND o.exposure_kind = 'EQUITY' AND o.shares > 0 AND o.market_value_usd > 0
              GROUP BY 1)
        SELECT CASE WHEN coalesce(o.n, 0) >= 20 THEN 'observation layer >= 20'
                    WHEN coalesce(o.n, 0) > t.left_filers THEN 'more, still < 20'
                    ELSE 'no more' END,
               count(*)
        FROM thin t LEFT JOIN o USING (instrument_id) GROUP BY 1 ORDER BY 1""",
    "legacy (instrument, quarter, filer CIK) EQUITY pairs missing from the observation layer": """
        WITH ih AS (SELECT DISTINCT h.instrument_id, h.period_of_report AS q, f.cik
                    FROM institutional_holdings h JOIN institutional_filers f ON f.filer_id = h.filer_id
                    WHERE h.period_of_report >= '2024-06-30' AND h.is_put_call IS NULL
                      AND h.shares > 0 AND h.market_value_usd > 0),
        ob AS (SELECT DISTINCT instrument_id, period_end AS q, filer_cik AS cik
               FROM ownership_institutions_observations
               WHERE period_end >= '2024-06-30' AND source = '13f' AND exposure_kind = 'EQUITY'
                 AND shares > 0 AND market_value_usd > 0)
        SELECT count(*), count(*) FILTER (WHERE ob.cik IS NULL)
        FROM ih LEFT JOIN ob ON ob.instrument_id = ih.instrument_id AND ob.q = ih.q AND ob.cik = ih.cik""",
    "same-filed_at ties: groups with >1 contributing accession; unit-value spread > 2%, > 10%": """
        WITH base AS (SELECT * FROM ownership_institutions_observations
                      WHERE period_end >= '2024-06-30' AND source = '13f' AND ownership_nature = 'economic'
                        AND known_to IS NULL),
        l AS (SELECT filer_cik, instrument_id, period_end, max(filed_at) AS f FROM base GROUP BY 1, 2, 3),
        acc AS (SELECT b.filer_cik, b.instrument_id, b.period_end, b.source_accession,
                       (b.market_value_usd / b.shares)::float8 AS uv
                FROM base b JOIN l ON l.filer_cik = b.filer_cik AND l.instrument_id = b.instrument_id
                                  AND l.period_end = b.period_end AND l.f = b.filed_at
                WHERE b.exposure_kind = 'EQUITY' AND b.shares > 0 AND b.market_value_usd > 0
                  AND b.shares <> 'NaN'::numeric AND b.market_value_usd <> 'NaN'::numeric),
        g AS (SELECT max(uv) / min(uv) AS r FROM acc GROUP BY filer_cik, instrument_id, period_end
              HAVING count(DISTINCT source_accession) > 1)
        SELECT count(*), count(*) FILTER (WHERE r > 1.02), count(*) FILTER (WHERE r > 1.10) FROM g""",
    "GOOGL distinct filers per quarter: legacy table, observation layer": """
        SELECT q, (SELECT count(DISTINCT filer_id) FROM institutional_holdings h
                   WHERE h.instrument_id = i.instrument_id AND h.period_of_report = q),
               (SELECT count(DISTINCT filer_cik) FROM ownership_institutions_observations o
                WHERE o.instrument_id = i.instrument_id AND o.period_end = q AND o.source = '13f')
        FROM instruments i, unnest(ARRAY['2025-06-30', '2025-12-31', '2026-03-31', '2026-06-30']::date[]) q
        WHERE i.symbol = 'GOOGL' ORDER BY q""",
}


def main() -> None:
    print(f"as of {datetime.now(UTC).isoformat(timespec='seconds')}")
    with psycopg.connect(settings.database_url) as conn:
        for title, sql in QUERIES.items():
            print(f"\n## {title}")
            for row in conn.execute(sql).fetchall():  # type: ignore[arg-type]
                print("  ", *row)


if __name__ == "__main__":
    main()
