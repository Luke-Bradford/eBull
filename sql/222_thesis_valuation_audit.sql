-- 222_thesis_valuation_audit.sql
-- #2009 PR-B / #2007 divergence measurement. Insert-once per thesis; the
-- append-only home for band-vs-LLM divergence signals (keeps `theses` clean).
-- band_base NULL (the ~8,700 no-band path) => divergence_pct/flag NULL, never
-- 0/false (#1632).
CREATE TABLE IF NOT EXISTS thesis_valuation_audit (
    thesis_id            bigint      NOT NULL REFERENCES theses(thesis_id),
    band_method_version  text,
    band_base            numeric(18,6),
    band_quality_status  text,
    price_as_of          date,
    llm_base             numeric(18,6),
    divergence_pct       numeric,          -- NULL when band_base NULL. UNCONSTRAINED numeric
                                           -- (NOT numeric(10,6)): a tiny positive band_base with a
                                           -- large llm_base yields pct >> 9999, and a bounded type
                                           -- would raise numeric_value_out_of_range INSIDE the
                                           -- atomic thesis txn -> abort the whole thesis. Divergence
                                           -- is MEASURE-ONLY and must never gate the insert
                                           -- (Codex ckpt-1 PR-B HIGH).
    divergence_flag      boolean,          -- NULL when band_base NULL
    created_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (thesis_id)
);
