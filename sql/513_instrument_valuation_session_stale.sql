-- 513_instrument_valuation_session_stale.sql
--
-- #3923: instrument_valuation refuses a cap whose session is a calendar month or more before the active PIT build's
-- build_date. Spec: docs/specs/fundamentals/2026-10-19-3850-live-cap-share-basis.md, Amendment 2.
--
-- JKP's monthly me is the month's last valid price (bkelly-lab/ReplicationCrisis at 67174c7f170b,
-- GlobalFactors/project_macros.sas:586-615), so no JKP me rests on a price a month old; this is the rolling form of
-- that bound, constructed and frozen by the amendment. A row whose cap would otherwise publish takes
-- market_cap_reason 'session_stale' when session <= build_date - interval '1 month'; market_cap_live, and so every
-- count-based column, is then NULL. A row already refused keeps its reason. build_date is read through the latest
-- step's build_id (one primary-key probe).
--
-- The body is sql/512's instrument_valuation with two edits: the fb lateral also reads its build's build_date, and the
-- session_stale arm in fr. Output columns, order and types are unchanged. View replace only; no backfill. The
-- rollback is sql/rollback/513_instrument_valuation_session_stale.sql (sql/512's body), applied by a revert PR.

BEGIN;

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
        -- #3903 (sql/509): P/B's equity and the end of the period that supplied it (#3919, sql/512), both arms; emitted
        -- by neither.
        ttm.shareholders_equity AS pb_equity,
        ttm.shareholders_equity_end AS pb_equity_end,
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
        -- #3903 (sql/509): the selected stored equity (#3919, sql/512), the column the complete arm divides by; the
        -- snapshot's per-share book value is not read for P/B. An incomplete window still carries its stock items.
        ttm.shareholders_equity AS pb_equity,
        ttm.shareholders_equity_end AS pb_equity_end,
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
    -- months after the end of the period that supplied it (JKP max_data_lag), and the cap in USD, the equity's unit.
    -- #3919 (sql/512) removes sql/509 Amendment 1's later-period refusal: the selected equity is the latest row of its
    -- own series, and a later row of the other series either lacks equity or is an unusable annual row.
    CASE WHEN v.pb_equity > 0 AND v.pb_equity < 'Infinity'
              AND ses.session <= (date_trunc('month', v.pb_equity_end + interval '18 months')
                                  + interval '1 month - 1 day')::date
              AND ic.currency = 'USD'
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
-- carries it; the latest step is one index probe. #3923 (sql/513): its build_id names the active build, because a
-- build replaces every step and writes its build row in one transaction; that build's date is one primary-key probe
-- inside this lateral.
LEFT JOIN LATERAL (
    SELECT s.bars_through AS frontier,
           (SELECT b.build_date FROM market_cap_pit_builds b WHERE b.build_id = s.build_id) AS build_date
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
               WHEN pit.reason IS NOT NULL THEN pit.reason
               -- #3923 (sql/513): a cap that would otherwise publish is refused when its session is a calendar month
               -- or more before the active build's date (spec Amendment 2, the rolling form of JKP's monthly me).
               -- pit.price_date is the session here (pit is matched on it); reading ses.session instead inlines its
               -- CASE and subplan into every count-based column, which lifts the plan's cost over jit_above_cost.
               WHEN pit.price_date <= (fb.build_date - interval '1 month')::date THEN 'session_stale'
           END AS market_cap_reason
) fr
CROSS JOIN LATERAL (
    SELECT CASE WHEN fr.market_cap_reason IS NULL THEN pit.market_cap::numeric END AS market_cap_live
) mc;

COMMENT ON VIEW instrument_valuation IS
    '#3850/#3903: count-based columns (pe_ratio = market cap / TTM net income; dividend_yield = TTM cash dividends '
    'paid from four observed members / market cap; pb_ratio = market cap / the selected equity in both arms, '
    'usable through the month-end 18 months after shareholders_equity_end, USD only) are '
    'priced on the #3841 PIT market cap '
    'at the session (latest positive close on or before the active PIT build''s frontier), NULL with '
    'market_cap_reason when refused; #3923: session_stale when the session is on or before the active build''s '
    'build_date minus one calendar month.';

COMMIT;
