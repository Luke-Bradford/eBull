-- #2523 — extend prospective market classification with provider industry.
--
-- instruments.sector is eToro's stocks-industry ID, despite the legacy column
-- name.  It is current mutable metadata, so it may not be projected backwards.
-- Existing current rows that began before this migration are closed yesterday
-- and reopened today; a row first observed today is corrected in place.  Future
-- universe syncs write a transition only when the provider assignment changes.

ALTER TABLE instrument_market_classification_history
    ADD COLUMN IF NOT EXISTS provider_industry_id INTEGER;

-- Migration 302 originally used the database UTC date while decision lookup
-- has always used America/New_York. If a clean install reaches this migration
-- between 00:00 and 04:00/05:00 UTC, its just-imported row is one market day in
-- the future. Correct only that untouched initial row; no historical transition
-- can satisfy source_event=imported plus effective_from > New York today.
UPDATE instrument_market_classification_history h
   SET effective_from = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date,
       last_confirmed_on = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
 WHERE h.effective_to IS NULL
   AND h.source_event = 'imported'
   AND h.effective_from > (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date;

-- Same-day rows have no finer ordering available and may be corrected safely.
UPDATE instrument_market_classification_history h
   SET provider_industry_id = CASE
           WHEN i.sector ~ '^[0-9]+$' AND i.sector::BIGINT BETWEEN 1 AND 2147483647
               THEN i.sector::INTEGER
           ELSE NULL
       END,
       last_confirmed_on = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
  FROM instruments i
 WHERE i.instrument_id = h.instrument_id
   AND h.effective_to IS NULL
   AND h.effective_from = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date;

-- Older classification rows must not acquire today's sector retrospectively.
WITH closed AS (
    UPDATE instrument_market_classification_history h
       SET effective_to = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - 1,
           last_confirmed_on = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - 1
     WHERE h.effective_to IS NULL
       AND h.effective_from < (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
    RETURNING h.instrument_id, h.provider_exchange_id,
              h.primary_listing_market, h.instrument_type_id,
              h.security_type
)
INSERT INTO instrument_market_classification_history (
    instrument_id, effective_from, effective_to, last_confirmed_on,
    provider_exchange_id, primary_listing_market, instrument_type_id,
    security_type, source_event, provider_industry_id
)
SELECT c.instrument_id,
       (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date,
       NULL,
       (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date,
       c.provider_exchange_id, c.primary_listing_market, c.instrument_type_id,
       c.security_type, 'classification_change',
       CASE
           WHEN i.sector ~ '^[0-9]+$' AND i.sector::BIGINT BETWEEN 1 AND 2147483647
               THEN i.sector::INTEGER
           ELSE NULL
       END
  FROM closed c
  JOIN instruments i USING (instrument_id);

ALTER TABLE strategy_decision_contexts
    ADD COLUMN IF NOT EXISTS provider_industry_id INTEGER;

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_v2_sector_complete
    CHECK (
        candidate_verdict = 'refused'
        OR context_version NOT LIKE 'decision-context-v2:%'
        OR provider_industry_id IS NOT NULL
    );

COMMENT ON COLUMN instrument_market_classification_history.provider_industry_id IS
    'Prospectively observed eToro stocks-industry ID. NULL means unknown/not '
    'applicable and must not be filled from later instruments metadata.';

COMMENT ON COLUMN strategy_decision_contexts.provider_industry_id IS
    'Provider industry copied from the point-in-time classification of this '
    'fired/refused decision; never resolved later from instruments.sector.';
