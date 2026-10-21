-- rollback/512_financial_periods_ttm_stock_items_combine.sql
--
-- #3919 rollback: restores sql/508's financial_periods_ttm and sql/509's instrument_valuation (both bodies below are
-- byte for byte from their CREATE, with their COMMENTs). NOT applied by the migration runner, which reads sql/*.sql
-- only. To roll back, one revert PR adds this body as the next sql/NNN migration AND restores app/services/thesis.py's
-- v11 P/B sentence under the next prompt version (spec "Rollback"); theses written under v12 keep their label.
--
-- CREATE OR REPLACE VIEW cannot drop shareholders_equity_end, so both views are dropped and recreated. Plain DROP (no
-- CASCADE) fails, and the transaction rolls back, on any other dependent; the DO block names them first. Both views
-- are owned by the migration role with no grants, so recreating them as that role restores ownership and access.

BEGIN;

DO $$
DECLARE
    deps text;
BEGIN
    SELECT string_agg(DISTINCT format('%s -> %s', r.ev_class::regclass, d.refobjid::regclass), ', ')
    INTO deps
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid
    WHERE d.classid = 'pg_rewrite'::regclass
      AND d.refclassid = 'pg_class'::regclass
      AND d.refobjid IN ('financial_periods_ttm'::regclass, 'instrument_valuation'::regclass)
      AND r.ev_class <> d.refobjid
      AND NOT (d.refobjid = 'financial_periods_ttm'::regclass AND r.ev_class = 'instrument_valuation'::regclass);
    IF deps IS NOT NULL THEN
        RAISE EXCEPTION 'rollback/512: dependents beyond instrument_valuation: %', deps;
    END IF;
END
$$;

DROP VIEW instrument_valuation;
DROP VIEW financial_periods_ttm;

CREATE OR REPLACE VIEW financial_periods_ttm AS
WITH deduped_quarters AS (
    -- Collapse fiscal-year-rekey duplicates (two period_type rows sharing
    -- one period_end_date, #1914 class — 133 instruments on dev) to ONE
    -- row per (instrument_id, period_end_date) BEFORE ranking. Without
    -- this, ROW_NUMBER assigns both dup rows distinct rn, so a dup pair
    -- can land inside rn<=4 and double-count that quarter while
    -- under-spanning the true 4 distinct quarters — a WRONG (non-NULL)
    -- TTM instead of the strict NULL intended. Latest-filed wins (same
    -- tiebreak as the fundamentals_snapshot write-through).
    SELECT DISTINCT ON (instrument_id, period_end_date) *
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
      AND superseded_at IS NULL
      AND normalization_status = 'normalized'
    ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
),
ranked_quarters AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY instrument_id
               ORDER BY period_end_date DESC
           ) AS rn,
           -- #3903 (sql/508): the previous stored quarter's end, for dividends_paid_ttm_observed's overlap test.
           LAG(period_end_date) OVER (
               PARTITION BY instrument_id
               ORDER BY period_end_date
           ) AS prev_end
    FROM deduped_quarters
),
latest_4 AS (
    SELECT * FROM ranked_quarters WHERE rn <= 4
)
SELECT
    instrument_id,
    COUNT(*) AS quarter_count,
    SUM(COALESCE(months_covered, 3)) AS ttm_months_covered,
    COUNT(*) = 4
        AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
        AS is_complete_ttm,
    MIN(period_end_date) AS ttm_start,
    MAX(period_end_date) AS ttm_end,

    -- Flow items, strict: 4 adjacent rows AND the column present in all 4.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(revenue) = 4
         THEN SUM(revenue) END AS revenue_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(cost_of_revenue) = 4
         THEN SUM(cost_of_revenue) END AS cost_of_revenue_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(gross_profit) = 4
         THEN SUM(gross_profit) END AS gross_profit_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(operating_income) = 4
         THEN SUM(operating_income) END AS operating_income_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(net_income) = 4
         THEN SUM(net_income) END AS net_income_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(research_and_dev) = 4
         THEN SUM(research_and_dev) END AS research_and_dev_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(sga_expense) = 4
         THEN SUM(sga_expense) END AS sga_expense_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(depreciation_amort) = 4
         THEN SUM(depreciation_amort) END AS depreciation_amort_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(interest_expense) = 4
         THEN SUM(interest_expense) END AS interest_expense_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(income_tax) = 4
         THEN SUM(income_tax) END AS income_tax_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(sbc_expense) = 4
         THEN SUM(sbc_expense) END AS sbc_expense_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(operating_cf) = 4
         THEN SUM(operating_cf) END AS operating_cf_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(investing_cf) = 4
         THEN SUM(investing_cf) END AS investing_cf_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(financing_cf) = 4
         THEN SUM(financing_cf) END AS financing_cf_ttm,

    -- Sporadic flow items: absence means "did not occur" (spec §Source
    -- rules) — sum over present members, window-shape guard only.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(capex) END AS capex_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(dividends_paid) END AS dividends_paid_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(dps_declared) END AS dps_declared_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(buyback_spend) END AS buyback_spend_ttm,

    -- Stock items: latest quarter only (rn=1) — unchanged.
    MAX(total_assets) FILTER (WHERE rn = 1) AS total_assets,
    MAX(total_liabilities) FILTER (WHERE rn = 1) AS total_liabilities,
    MAX(shareholders_equity) FILTER (WHERE rn = 1) AS shareholders_equity,
    MAX(cash) FILTER (WHERE rn = 1) AS cash,
    MAX(long_term_debt) FILTER (WHERE rn = 1) AS long_term_debt,
    MAX(short_term_debt) FILTER (WHERE rn = 1) AS short_term_debt,
    MAX(shares_outstanding) FILTER (WHERE rn = 1) AS shares_outstanding,
    MAX(inventory) FILTER (WHERE rn = 1) AS inventory,
    MAX(receivables) FILTER (WHERE rn = 1) AS receivables,
    MAX(payables) FILTER (WHERE rn = 1) AS payables,
    MAX(goodwill) FILTER (WHERE rn = 1) AS goodwill,
    MAX(ppe_net) FILTER (WHERE rn = 1) AS ppe_net,

    -- Derived — unchanged shapes.
    MAX(reported_currency) FILTER (WHERE rn = 1) AS reported_currency,
    MAX(eps_basic) FILTER (WHERE rn = 1) AS eps_basic_latest,
    MAX(eps_diluted) FILTER (WHERE rn = 1) AS eps_diluted_latest,
    -- TTM EPS: flow sums, strict like the other statement-core lines.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(eps_basic) = 4
         THEN SUM(eps_basic) END AS eps_basic_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(eps_diluted) = 4
         THEN SUM(eps_diluted) END AS eps_diluted_ttm,
    MAX(shares_basic) FILTER (WHERE rn = 1) AS shares_basic,
    MAX(shares_diluted) FILTER (WHERE rn = 1) AS shares_diluted,

    -- #3903 (sql/507), appended last: the window's cash dividends paid only when all four members are observed and
    -- non-negative. dividends_paid_ttm above keeps its sporadic sum for its other readers.
    -- sql/508 (spec Amendment 2), in order: (1) every member's interval is valid; (2) no member starts on or before the
    -- previous member's end; (3) a positive total's window ends before one calendar year from the first member's
    -- start, so a fixed-date payment schedule cannot land in it twice. A zero total is zero over any span.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(dividends_paid) = 4
              AND MIN(dividends_paid) >= 0
              AND COUNT(*) FILTER (WHERE period_start_date IS NOT NULL
                                     AND period_start_date <= period_end_date) = 4
              AND COUNT(*) FILTER (WHERE rn < 4 AND period_start_date <= prev_end) = 0
              AND (SUM(dividends_paid) = 0
                   OR MAX(period_end_date)
                      < MAX(period_start_date) FILTER (WHERE rn = 4) + INTERVAL '1 year')
         THEN SUM(dividends_paid) END AS dividends_paid_ttm_observed

FROM latest_4
GROUP BY instrument_id;

COMMENT ON VIEW financial_periods_ttm IS
    '#2008/#3903: latest four normalized quarters per instrument. Strict flow sums need all four members; sporadic '
    'ones (capex, dividends_paid, dps_declared, buyback_spend) sum present members. dividends_paid_ttm_observed '
    '(sql/507, sql/508) is the dividends_paid sum only when all four members are observed and non-negative, carry '
    'valid non-overlapping intervals, and, for a positive sum, end before one calendar year from the first start.';

CREATE OR REPLACE VIEW instrument_valuation AS
WITH dual_class AS (
    -- Curated dual-class instruments (sql/201).
    SELECT DISTINCT ei.instrument_id
    FROM external_identifiers ei
    JOIN instrument_class_shares_outstanding c
      ON c.source_cik = lpad(ei.identifier_value, 10, '0')
    WHERE ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
      AND ei.is_primary = TRUE
),
fpi_adr AS (
    -- #1939: Rule 3b-4 FPI fingerprint ∪ the eToro ADR/ADS name marker (sql/237).
    SELECT instrument_id
    FROM coverage
    WHERE filings_status = 'fpi'
    UNION
    SELECT instrument_id
    FROM instruments
    WHERE company_name ~* '\y(ADR|ADS)\y'
),
ratio_known AS (
    -- #2117: curated ADS ratios, except curated multiclass (sql/241).
    SELECT instrument_id, ratio FROM ads_ratio
    WHERE instrument_id NOT IN (SELECT instrument_id FROM dual_class)
),
fpi_adr_suppress AS (
    SELECT instrument_id FROM fpi_adr
    EXCEPT
    SELECT instrument_id FROM ratio_known
),
priced AS NOT MATERIALIZED (
    -- #1857 freshest-price rule and sql/236's per-id shape, unchanged from sql/241 (read its comment there:
    -- NOT MATERIALIZED is load-bearing). #3850 adds the latest positive close's date.
    SELECT
        ids.instrument_id,
        CASE WHEN w.use_quote THEN q.price     ELSE pd.close                    END AS price,
        (CASE WHEN w.use_quote THEN q.price    ELSE pd.close                    END)
            / COALESCE(rk.ratio, 1)                                                 AS metric_price,
        CASE WHEN w.use_quote THEN q.quoted_at ELSE pd.price_date::timestamptz  END AS quoted_at,
        pd.price_date                                                               AS latest_close
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
    LEFT JOIN ratio_known rk ON rk.instrument_id = ids.instrument_id
    CROSS JOIN LATERAL (
        SELECT q.price IS NOT NULL
               AND (pd.price_date IS NULL
                    OR (q.quoted_at AT TIME ZONE 'UTC')::date >= pd.price_date)
               AS use_quote
    ) w
),
new_pipeline AS (
    -- Complete-TTM arm. The count-based columns are computed in the final SELECT over the PIT cap.
    SELECT
        p.instrument_id,
        p.price,
        p.quoted_at,
        p.latest_close,
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
        -- #3903 (sql/509): P/B's equity and its quarter end, both arms; emitted by neither.
        ttm.shareholders_equity AS pb_equity,
        ttm.ttm_end AS pb_equity_end,
        CASE WHEN ttm.shareholders_equity > 0
             THEN (COALESCE(ttm.long_term_debt, 0) + COALESCE(ttm.short_term_debt, 0))
                  / ttm.shareholders_equity
        END AS debt_equity_ratio,
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
        ttm.dividends_paid_ttm_observed,
        ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0) AS ebitda_ttm,
        ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)) AS fcf_ttm
    FROM priced p
    JOIN financial_periods_ttm ttm USING (instrument_id)
    WHERE ttm.is_complete_ttm = TRUE
),
legacy AS (
    -- Legacy arm (sql/241). is_complete_ttm is NULL here, which the final SELECT reads as the arm.
    SELECT
        p.instrument_id,
        p.price,
        p.quoted_at,
        p.latest_close,
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
        -- #3903 (sql/509): the latest quarter's stored equity, the column the complete arm divides by; the snapshot's
        -- per-share book value is no longer read for P/B. An incomplete window still carries its stock items.
        ttm.shareholders_equity AS pb_equity,
        ttm.ttm_end AS pb_equity_end,
        CASE WHEN fs.book_value > 0 AND fs.shares_outstanding > 0
             THEN fs.debt / (fs.book_value * fs.shares_outstanding)
        END AS debt_equity_ratio,
        NULL::numeric AS net_margin,
        fs.gross_margin,
        fs.operating_margin,
        NULL::numeric AS roa,
        NULL::numeric AS roe,
        NULL::numeric AS dividends_paid_ttm_observed,
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
    LEFT JOIN financial_periods_ttm ttm ON ttm.instrument_id = p.instrument_id
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
    mc.market_cap_live,
    CASE WHEN v.is_complete_ttm
         THEN mc.market_cap_live + COALESCE(v.long_term_debt, 0) + COALESCE(v.short_term_debt, 0)
              - COALESCE(v.cash, 0)
    END AS enterprise_value,
    -- #3903: count-based, NULL with market_cap_reason; the legacy arm has no earnings total.
    CASE WHEN v.is_complete_ttm AND v.net_income_ttm > 0 AND v.net_income_ttm < 'Infinity'
         THEN mc.market_cap_live / v.net_income_ttm
    END AS pe_ratio,
    -- #3903 (sql/509): count-based in both arms, NULL with market_cap_reason; the equity usable through the month-end 18
    -- months after its quarter end (JKP max_data_lag), and the cap in USD, the equity's unit.
    CASE WHEN v.pb_equity > 0 AND v.pb_equity < 'Infinity'
              AND ses.session <= (date_trunc('month', v.pb_equity_end + interval '18 months')
                                  + interval '1 month - 1 day')::date
              AND ic.currency = 'USD'
              -- Amendment 1: JKP ends a value's use at the next value's start (accounting_chars.sas:1074); with no
              -- publication lag, a later stored quarterly or annual period ends this quarter's equity, and none is
              -- read (#3919).
              AND NOT EXISTS (
                  SELECT 1 FROM financial_periods nx
                  WHERE nx.instrument_id = v.instrument_id
                    AND nx.period_end_date > v.pb_equity_end
                    AND nx.period_type IN ('Q1','Q2','Q3','Q4','FY')
                    AND nx.superseded_at IS NULL
                    AND nx.normalization_status = 'normalized'
              )
         THEN mc.market_cap_live / v.pb_equity
    END AS pb_ratio,
    CASE WHEN v.is_complete_ttm AND v.revenue_ttm > 0 THEN mc.market_cap_live / v.revenue_ttm END AS price_sales,
    CASE WHEN v.fcf_ttm > 0 THEN mc.market_cap_live / v.fcf_ttm END AS p_fcf_ratio,
    CASE WHEN mc.market_cap_live > 0 THEN v.fcf_ttm / mc.market_cap_live END AS fcf_yield,
    v.debt_equity_ratio,
    CASE WHEN v.is_complete_ttm AND v.revenue_ttm > 0
         THEN (mc.market_cap_live + COALESCE(v.long_term_debt, 0) + COALESCE(v.short_term_debt, 0)
               - COALESCE(v.cash, 0)) / v.revenue_ttm
    END AS ev_revenue,
    CASE WHEN v.is_complete_ttm AND v.ebitda_ttm > 0
         THEN (mc.market_cap_live + COALESCE(v.long_term_debt, 0) + COALESCE(v.short_term_debt, 0)
               - COALESCE(v.cash, 0)) / v.ebitda_ttm
    END AS ev_ebitda,
    v.net_margin,
    v.gross_margin,
    v.operating_margin,
    v.roa,
    v.roe,
    -- forward_pe always NULL: source dropped in sql/080; kept for shape compatibility.
    NULL::numeric AS forward_pe,
    -- #3903: count-based, NULL with market_cap_reason (which subsumes sql/506's FPI suppression here).
    CASE WHEN v.is_complete_ttm AND v.dividends_paid_ttm_observed < 'Infinity' AND mc.market_cap_live > 0
         THEN v.dividends_paid_ttm_observed / mc.market_cap_live * 100
    END AS dividend_yield,
    v.ebitda_ttm,
    v.fcf_ttm,
    ses.session,
    pit.close AS valuation_price,
    fr.market_cap_reason,
    CASE WHEN pit.instrument_id IS NOT NULL THEN to_jsonb(pit) END AS market_cap_pit
FROM (
    SELECT * FROM new_pipeline
    UNION ALL
    SELECT * FROM legacy
) v
LEFT JOIN dual_class dc USING (instrument_id)
LEFT JOIN fpi_adr_suppress fpi USING (instrument_id)
LEFT JOIN ads_ratio ar USING (instrument_id)
LEFT JOIN instruments ic USING (instrument_id)
-- The frontier: one build writes one bars_through per instrument (the last bar in its snapshot), so any step
-- carries it; the latest step is one index probe.
LEFT JOIN LATERAL (
    SELECT s.bars_through AS frontier
    FROM instrument_share_count_pit s
    WHERE s.instrument_id = v.instrument_id
    ORDER BY s.valid_from DESC
    LIMIT 1
) fb ON TRUE
-- The latest positive close on or before the frontier; probed only when the frontier is behind the latest close.
CROSS JOIN LATERAL (
    SELECT CASE
               WHEN fb.frontier IS NULL OR fb.frontier >= v.latest_close THEN v.latest_close
               ELSE COALESCE(
                   (SELECT p.price_date
                    FROM price_daily p
                    WHERE p.instrument_id = v.instrument_id
                      AND p.close > 0
                      AND p.price_date <= fb.frontier
                    ORDER BY p.price_date DESC
                    LIMIT 1),
                   v.latest_close
               )
           END AS session
) ses
-- Steps never overlap within an instrument, so at most one PIT row matches.
LEFT JOIN LATERAL (
    SELECT m.*
    FROM instrument_market_cap_pit m
    WHERE m.instrument_id = v.instrument_id
      AND m.price_date = ses.session
) pit ON TRUE
CROSS JOIN LATERAL (
    SELECT CASE
               WHEN dc.instrument_id IS NOT NULL THEN 'dual_class'
               WHEN ar.instrument_id IS NOT NULL THEN 'ads_ratio'
               WHEN fpi.instrument_id IS NOT NULL THEN 'fpi_adr'
               WHEN pit.instrument_id IS NULL THEN 'no_pit_row'
               ELSE pit.reason
           END AS market_cap_reason
) fr
CROSS JOIN LATERAL (
    SELECT CASE WHEN fr.market_cap_reason IS NULL THEN pit.market_cap::numeric END AS market_cap_live
) mc;

COMMENT ON VIEW instrument_valuation IS
    '#3850/#3903: count-based columns (pe_ratio = market cap / TTM net income; dividend_yield = TTM cash dividends '
    'paid from four observed members / market cap; pb_ratio = market cap / the latest quarter''s equity in both arms, '
    'usable through the month-end 18 months after its quarter end and until a later period is stored, USD only) are '
    'priced on the #3841 PIT market cap '
    'at the session (latest positive close on or before the active PIT build''s frontier), NULL with '
    'market_cap_reason when refused.';

COMMIT;
