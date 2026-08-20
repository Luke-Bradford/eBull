-- 237_instrument_valuation_fpi_adr_suppress.sql
--
-- #1939 step-1: suppress every price-bearing ratio for foreign-private-
-- issuer ADR/ADS instruments. The SEC DEI share count is the issuer's
-- ORDINARY-share count while the tradable price is PER-ADS (one ADS = N
-- ordinary shares, ratio in the F-6 / depositary agreement — NOT
-- ingested), so any ordinary-shares × ADS-price product overstates by
-- ~the ADS ratio (dev full-pop: Toyota $2.28T, AKTX $1.43T). sql/236
-- (#1857) un-masked this by pricing the view universe-wide.
--
-- Source rule: Exchange Act Rule 3b-4 defines FPI status; the documented
-- consequence is the form set (20-F/40-F annuals + 6-K currents instead
-- of 10-K/10-Q/8-K). Detection REUSES coverage.filings_status = 'fpi'
-- (app/services/coverage.py::_classify — zero US base-or-amend filings
-- AND >=1 20-F/40-F/6-K family filing; full-pop maintained by the
-- coverage machinery; 1,086 tradable rows == an independent
-- filing_events fingerprint scan exactly, verified 2026-07-23).
--
-- Suppressed for FPI (every column carrying a p.price term — the per-ADS
-- price cannot meet a per-ordinary-share denominator): market_cap_live,
-- enterprise_value, pe_ratio, pb_ratio, price_sales, p_fcf_ratio,
-- fcf_yield, ev_revenue, ev_ebitda, dividend_yield. KEPT: current_price
-- / price_as_of (the per-ADS price is itself real), margins, roa, roe,
-- debt_equity_ratio, raw TTM figures. NOTE this is a WIDER list than the
-- #1664 dual-class suppression: dual-class keeps pe_ratio and
-- dividend_yield (combined-diluted EPS / declared DPS are per-share-basis
-- consistent across classes), but for an ADS the price and the per-share
-- figures differ by the ADS ratio, so both are wrong.
--
-- Known residual (step-2, ADS-ratio ingestion child ticket): ADR-class
-- issuers that file DOMESTIC forms (AKTX — UK PLC filing 10-Ks) are not
-- fingerprinted by Rule 3b-4 and remain wrong until the ratio lands.
--
-- Everything else (priced CTE recency rule + perf shape from sql/236,
-- #1664 dual-class suppression) is preserved verbatim.
--
-- View recreate only — no data backfill. Idempotent (DROP VIEW IF EXISTS).

BEGIN;

DROP VIEW IF EXISTS instrument_valuation;

CREATE VIEW instrument_valuation AS
WITH dual_class AS (
    -- Curated dual-class instruments: primary SEC CIK present in the
    -- #1623 per-class FSDS table. Both share-class siblings of a covered
    -- CIK appear (the table is keyed per sibling instrument_id).
    SELECT DISTINCT ei.instrument_id
    FROM external_identifiers ei
    JOIN instrument_class_shares_outstanding c
      ON c.source_cik = lpad(ei.identifier_value, 10, '0')
    WHERE ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
      AND ei.is_primary = TRUE
),
fpi_adr AS (
    -- #1939: Rule 3b-4 FPI fingerprint, reused from the coverage
    -- classifier (single source of truth — do NOT re-derive from
    -- filing_events here) ∪ the eToro ADR/ADS name marker, which catches
    -- the DOMESTIC-form ADR filers the fingerprint cannot (BeiGene/ONC
    -- files 10-Ks yet its ADS = 13 ordinary shares → $467B fake cap).
    -- Full-pop scan 2026-07-23: 182 tradable marker hits, 0 harmful
    -- false positives (the two Ads-* names are already fpi / no-CIK).
    -- Known cost: a 1:1-ratio ADS (TEVA class) is over-suppressed —
    -- fail-closed is correct while the ratio is not ingested (step-2).
    SELECT instrument_id
    FROM coverage
    WHERE filings_status = 'fpi'
    UNION
    SELECT instrument_id
    FROM instruments
    WHERE company_name ~* '\y(ADR|ADS)\y'
),
priced AS NOT MATERIALIZED (
    -- #1857: freshest price wins. The quote is used iff it is at least
    -- as recent (by UTC date — the ::date cast is pinned so a session
    -- timezone cannot flip the winner near midnight) as the latest
    -- strictly-positive daily close — a stale snapshot quote must not
    -- shadow a fresher close. The strictly-positive close filter mirrors
    -- compute_day_change's rule (a non-positive close is a sentinel, not
    -- a price). The as-of stamp pairs with the price actually used.
    --
    -- Shape: driving id-set (both sources' instrument ids, index-only
    -- scans) + LEFT JOIN quotes + LEFT JOIN LATERAL latest-close. NOT a
    -- FULL OUTER JOIN over a global DISTINCT ON — that shape rescanned
    -- and sorted all of price_daily (~5M rows) on EVERY per-instrument
    -- view query (Codex ckpt-2: 1.7s/instrument × ~3,900 = ~2h per
    -- compute_rankings run). The LATERAL is a per-id backward index
    -- scan on the (instrument_id, price_date) PK, and an instrument_id
    -- predicate pushes into both UNION branches. NOT MATERIALIZED is
    -- load-bearing: priced is referenced by BOTH pipelines, so PG would
    -- otherwise materialize the CTE and the per-instrument predicate
    -- could not reach inside it (EXPLAIN showed the full 3M-tuple merge
    -- on every WHERE instrument_id query).
    SELECT
        ids.instrument_id,
        CASE WHEN w.use_quote THEN q.price     ELSE pd.close                    END AS price,
        CASE WHEN w.use_quote THEN q.quoted_at ELSE pd.price_date::timestamptz  END AS quoted_at
    FROM (
        SELECT instrument_id FROM quotes
        UNION
        SELECT instrument_id FROM price_daily
    ) ids
    LEFT JOIN (
        SELECT instrument_id,
               COALESCE(
                   NULLIF(GREATEST(last, 0), 0),
                   CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
               )                    AS price,
               quoted_at
        FROM quotes
    ) q USING (instrument_id)
    LEFT JOIN LATERAL (
        SELECT p.close, p.price_date
        FROM price_daily p
        WHERE p.instrument_id = ids.instrument_id
          AND p.close > 0
        ORDER BY p.price_date DESC
        LIMIT 1
    ) pd ON TRUE
    -- Single source of truth for the recency verdict — referenced by both
    -- CASE columns above so the price and its as-of stamp cannot drift.
    CROSS JOIN LATERAL (
        SELECT q.price IS NOT NULL
               AND (pd.price_date IS NULL
                    OR (q.quoted_at AT TIME ZONE 'UTC')::date >= pd.price_date)
               AS use_quote
    ) w
),
new_pipeline AS (
    SELECT
        p.instrument_id,
        p.price,
        p.quoted_at,
        ttm.revenue_ttm,
        ttm.net_income_ttm,
        ttm.operating_income_ttm,
        ttm.gross_profit_ttm,
        ttm.depreciation_amort_ttm,
        ttm.operating_cf_ttm,
        ttm.capex_ttm,
        ttm.dps_declared_ttm,
        ttm.is_complete_ttm,
        ttm.eps_diluted_ttm,
        ttm.total_assets,
        ttm.total_liabilities,
        ttm.shareholders_equity,
        ttm.cash,
        ttm.long_term_debt,
        ttm.short_term_debt,
        ttm.shares_outstanding,
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
             THEN p.price * ttm.shares_outstanding
        END AS market_cap_live,
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
             THEN p.price * ttm.shares_outstanding
                  + COALESCE(ttm.long_term_debt, 0)
                  + COALESCE(ttm.short_term_debt, 0)
                  - COALESCE(ttm.cash, 0)
        END AS enterprise_value,
        CASE WHEN ttm.eps_diluted_ttm > 0
             THEN p.price / ttm.eps_diluted_ttm
        END AS pe_ratio,
        CASE WHEN ttm.shareholders_equity > 0 AND ttm.shares_outstanding > 0
             THEN p.price / (ttm.shareholders_equity / ttm.shares_outstanding)
        END AS pb_ratio,
        CASE WHEN ttm.revenue_ttm > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding) / ttm.revenue_ttm
        END AS price_sales,
        CASE WHEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) > 0
                  AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding)
                  / (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
        END AS p_fcf_ratio,
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
                  AND (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) IS NOT NULL
             THEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
                  / (p.price * ttm.shares_outstanding)
        END AS fcf_yield,
        CASE WHEN ttm.shareholders_equity > 0
             THEN (COALESCE(ttm.long_term_debt, 0) + COALESCE(ttm.short_term_debt, 0))
                  / ttm.shareholders_equity
        END AS debt_equity_ratio,
        CASE WHEN ttm.revenue_ttm > 0 AND p.price > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding
                   + COALESCE(ttm.long_term_debt, 0)
                   + COALESCE(ttm.short_term_debt, 0)
                   - COALESCE(ttm.cash, 0))
                  / ttm.revenue_ttm
        END AS ev_revenue,
        CASE WHEN (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0)) > 0
                  AND p.price > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding
                   + COALESCE(ttm.long_term_debt, 0)
                   + COALESCE(ttm.short_term_debt, 0)
                   - COALESCE(ttm.cash, 0))
                  / (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0))
        END AS ev_ebitda,
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.net_income_ttm / ttm.revenue_ttm
        END AS net_margin,
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.gross_profit_ttm / ttm.revenue_ttm
        END AS gross_margin,
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.operating_income_ttm / ttm.revenue_ttm
        END AS operating_margin,
        CASE WHEN ttm.total_assets > 0
             THEN ttm.net_income_ttm / ttm.total_assets
        END AS roa,
        CASE WHEN ttm.shareholders_equity > 0
             THEN ttm.net_income_ttm / ttm.shareholders_equity
        END AS roe,
        CASE WHEN p.price > 0 AND ttm.dps_declared_ttm > 0
             THEN ttm.dps_declared_ttm / p.price * 100
        END AS dividend_yield,
        ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0) AS ebitda_ttm,
        ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)) AS fcf_ttm
    FROM priced p
    JOIN financial_periods_ttm ttm USING (instrument_id)
    WHERE ttm.is_complete_ttm = TRUE
),
legacy AS (
    -- LATERAL latest-snapshot rather than DISTINCT ON: a DISTINCT ON
    -- subquery is not qual-pushdown-safe, which forced the per-instrument
    -- view query to evaluate this branch's inlined ``priced`` copy over
    -- the whole table (#1857 ckpt-2 perf finding).
    SELECT
        p.instrument_id,
        p.price,
        p.quoted_at,
        NULL::numeric AS revenue_ttm,
        NULL::numeric AS net_income_ttm,
        NULL::numeric AS operating_income_ttm,
        NULL::numeric AS gross_profit_ttm,
        NULL::numeric AS depreciation_amort_ttm,
        NULL::numeric AS operating_cf_ttm,
        NULL::numeric AS capex_ttm,
        NULL::numeric AS dps_declared_ttm,
        NULL::boolean AS is_complete_ttm,
        NULL::numeric AS eps_diluted_ttm,
        NULL::numeric AS total_assets,
        NULL::numeric AS total_liabilities,
        NULL::numeric AS shareholders_equity,
        fs.cash,
        fs.debt AS long_term_debt,
        NULL::numeric AS short_term_debt,
        fs.shares_outstanding,
        CASE WHEN p.price > 0 AND fs.shares_outstanding > 0
             THEN p.price * fs.shares_outstanding
        END AS market_cap_live,
        NULL::numeric AS enterprise_value,
        CASE WHEN p.price > 0 AND fs.eps > 0
             THEN p.price / fs.eps
        END AS pe_ratio,
        CASE WHEN p.price > 0 AND fs.book_value > 0
             THEN p.price / fs.book_value
        END AS pb_ratio,
        NULL::numeric AS price_sales,
        CASE WHEN p.price > 0 AND fs.shares_outstanding > 0 AND fs.fcf > 0
             THEN (p.price * fs.shares_outstanding) / fs.fcf
        END AS p_fcf_ratio,
        CASE WHEN p.price > 0 AND fs.shares_outstanding > 0
             THEN fs.fcf / (p.price * fs.shares_outstanding)
        END AS fcf_yield,
        CASE WHEN fs.book_value > 0 AND fs.shares_outstanding > 0
             THEN fs.debt / (fs.book_value * fs.shares_outstanding)
        END AS debt_equity_ratio,
        NULL::numeric AS ev_revenue,
        NULL::numeric AS ev_ebitda,
        NULL::numeric AS net_margin,
        fs.gross_margin,
        fs.operating_margin,
        NULL::numeric AS roa,
        NULL::numeric AS roe,
        NULL::numeric AS dividend_yield,
        NULL::numeric AS ebitda_ttm,
        fs.fcf AS fcf_ttm
    FROM priced p
    JOIN LATERAL (
        SELECT *
        FROM fundamentals_snapshot fs0
        WHERE fs0.instrument_id = p.instrument_id
        ORDER BY fs0.as_of_date DESC
        LIMIT 1
    ) fs ON TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM financial_periods_ttm ttm
        WHERE ttm.instrument_id = p.instrument_id
          AND ttm.is_complete_ttm = TRUE
    )
)
SELECT
    v.instrument_id,
    v.price              AS current_price,
    v.quoted_at          AS price_as_of,
    v.revenue_ttm,
    v.net_income_ttm,
    v.operating_income_ttm,
    v.gross_profit_ttm,
    v.depreciation_amort_ttm,
    v.operating_cf_ttm,
    v.capex_ttm,
    v.dps_declared_ttm,
    v.is_complete_ttm,
    v.total_assets,
    v.total_liabilities,
    v.shareholders_equity,
    v.cash,
    v.long_term_debt,
    v.short_term_debt,
    v.shares_outstanding,
    -- #1664: NULL the shares-distorted columns for curated dual-class
    -- issuers (combined-shares × one-class-price is structurally wrong).
    -- The correct total-company figure is sourced in Python by the
    -- decision-grade consumer via resolve_market_cap_basis.
    -- #1939: ALSO NULL every price-bearing column for Rule 3b-4 FPI
    -- ADR/ADS instruments (per-ADS price × per-ordinary-share basis is
    -- wrong by the un-ingested ADS ratio). FPI suppresses a WIDER list
    -- than dual-class: pe_ratio and dividend_yield are per-share-basis
    -- consistent across share classes but NOT across the ADS ratio.
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.market_cap_live END   AS market_cap_live,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.enterprise_value END  AS enterprise_value,
    CASE WHEN fpi.instrument_id IS NULL THEN v.pe_ratio END                                       AS pe_ratio,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.pb_ratio END          AS pb_ratio,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.price_sales END       AS price_sales,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.p_fcf_ratio END       AS p_fcf_ratio,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.fcf_yield END         AS fcf_yield,
    v.debt_equity_ratio,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.ev_revenue END        AS ev_revenue,
    CASE WHEN dc.instrument_id IS NULL AND fpi.instrument_id IS NULL THEN v.ev_ebitda END         AS ev_ebitda,
    v.net_margin,
    v.gross_margin,
    v.operating_margin,
    v.roa,
    v.roe,
    -- forward_pe always NULL: source (analyst_estimates) dropped in
    -- sql/080. Column retained for shape compatibility with any external
    -- (operator-curated SQL, BI tool) reader.
    NULL::numeric AS forward_pe,
    CASE WHEN fpi.instrument_id IS NULL THEN v.dividend_yield END                                 AS dividend_yield,
    v.ebitda_ttm,
    v.fcf_ttm
FROM (
    SELECT * FROM new_pipeline
    UNION ALL
    SELECT * FROM legacy
) v
LEFT JOIN dual_class dc USING (instrument_id)
LEFT JOIN fpi_adr fpi USING (instrument_id);

COMMIT;
