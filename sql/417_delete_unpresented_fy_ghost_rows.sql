-- #2182 item 2 — delete canonical FY rows that no filing ever presented.
--
-- Source rule (Reg S-X): a 10-K presents balance sheets for two fiscal years
-- (Rule 3-01(a)) and income / cash-flow statements for three (Rule 3-02(a)). The
-- equity rollforward (Rule 3-04) and the cash-flow statement also carry the OPENING
-- balance of the earliest year: an instant up to three years before the filing's
-- primary end. Before #3333 (`_fy_period_is_presented`) the normaliser minted a FY
-- row from those opening balances alone. The gate stops new ones; this removes the
-- rows already in canonical, which no re-normalize deletes.
--
-- A row is deleted only when ALL of these hold — the #3333 gate evaluated on the
-- canonical row, fail-closed at every step:
--   1. period_type 'FY', source 'sec_edgar' (the only writer of that path).
--   2. Every column that can only come from an annual DURATION fact is NULL, and so
--      are total_assets and public_float_usd. What is left is instants only.
--   3. EVERY accession in source_ref has a primary end more than 395 days (the
--      `_FLOW_DURATION_DAYS["FY"]` upper bound the gate uses) after period_end.
--      Primary end = the filing's SEC reportDate (filing_events.report_date); when
--      that is missing, the latest canonical FY period_end citing the accession,
--      which is a LOWER bound (a filing reports no period after its own), so an
--      unknown can only keep a row, never delete one.
--   4. No earlier FY row of the instrument carries revenue, net_income or
--      total_assets: the row predates the issuer's first reported year. A thin row
--      in mid-history is a real year whose content an older re-normalize overwrote
--      (#2182 part A); it is not a ghost, and deleting it would drop the year.
--
-- What is lost: one true opening equity / cash figure per pre-first-10-K year. No
-- reader uses it (no average-equity metric exists), and the fact itself stays in
-- financial_facts_raw for as long as its filing is retained.
--
-- Reproduce the count: run the DELETE below in a transaction, then ROLLBACK.

DELETE FROM financial_periods fp
WHERE fp.period_type = 'FY'
  AND fp.source = 'sec_edgar'
  AND fp.revenue IS NULL
  AND fp.cost_of_revenue IS NULL
  AND fp.gross_profit IS NULL
  AND fp.operating_income IS NULL
  AND fp.net_income IS NULL
  AND fp.eps_basic IS NULL
  AND fp.eps_diluted IS NULL
  AND fp.research_and_dev IS NULL
  AND fp.sga_expense IS NULL
  AND fp.depreciation_amort IS NULL
  AND fp.interest_expense IS NULL
  AND fp.income_tax IS NULL
  AND fp.shares_basic IS NULL
  AND fp.shares_diluted IS NULL
  AND fp.sbc_expense IS NULL
  AND fp.operating_cf IS NULL
  AND fp.investing_cf IS NULL
  AND fp.financing_cf IS NULL
  AND fp.capex IS NULL
  AND fp.dividends_paid IS NULL
  AND fp.dps_declared IS NULL
  AND fp.buyback_spend IS NULL
  AND fp.comprehensive_income IS NULL
  AND fp.intangible_amortization IS NULL
  AND fp.deferred_income_tax IS NULL
  AND fp.other_nonoperating_income IS NULL
  AND fp.antidilutive_securities IS NULL
  AND fp.public_float_usd IS NULL
  AND fp.total_assets IS NULL
  AND fp.source_ref IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM unnest(string_to_array(fp.source_ref, ',')) AS a(acc)
      WHERE (COALESCE(
                (SELECT max(fe.report_date)
                 FROM filing_events fe
                 WHERE fe.provider = 'sec' AND fe.provider_filing_id = trim(a.acc)),
                (SELECT max(c.period_end_date)
                 FROM financial_periods c
                 WHERE c.instrument_id = fp.instrument_id
                   AND c.period_type = 'FY'
                   AND c.source = 'sec_edgar'
                   AND trim(a.acc) = ANY (SELECT trim(x) FROM unnest(string_to_array(c.source_ref, ',')) AS x))
            ) - fp.period_end_date <= 395) IS NOT FALSE
  )
  AND NOT EXISTS (
      SELECT 1
      FROM financial_periods e
      WHERE e.instrument_id = fp.instrument_id
        AND e.period_type = 'FY'
        AND e.source = 'sec_edgar'
        AND e.period_end_date < fp.period_end_date
        AND (e.revenue IS NOT NULL OR e.net_income IS NOT NULL OR e.total_assets IS NOT NULL)
  );
