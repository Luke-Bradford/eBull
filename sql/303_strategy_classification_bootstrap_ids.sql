-- #2508 follow-up: migration 302 can run before the optional human-readable
-- eToro lookup rows have been refreshed. The provider IDs themselves are the
-- stable facts (4 Nasdaq, 5 NYSE; 5 Stocks, 6 ETF), so repair only the current
-- same-day imported rows. Future reconciles use the same ID-first rule.

UPDATE instrument_market_classification_history h
   SET primary_listing_market = CASE i.exchange
           WHEN '4' THEN 'nasdaq'
           WHEN '5' THEN 'nyse'
           ELSE h.primary_listing_market
       END,
       security_type = CASE i.instrument_type_id
           WHEN 5 THEN 'common_stock'
           WHEN 6 THEN 'etf'
           ELSE h.security_type
       END
  FROM instruments i
 WHERE i.instrument_id = h.instrument_id
   AND h.effective_to IS NULL
   AND h.effective_from = CURRENT_DATE
   AND h.source_event = 'imported';
