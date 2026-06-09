-- 188_drop_instruments_instrument_type.sql
--
-- #1464 — drop the dead ``instruments.instrument_type`` TEXT column.
--
-- sql/068 (#503 PR 4) added this column + index for an exchange-vs-instrument
-- integrity cross-check ("a stock-typed instrument on a crypto-classified
-- exchange is a data-integrity flag"). But the eToro ``/market-data/instruments``
-- endpoint returns ``instrumentTypeID`` (-> ``instrument_type_id``), NOT
-- ``instrumentTypeName``, so the universe upsert wrote NULL for every row:
--   SELECT count(*), count(instrument_type) FROM instruments  ->  (12530, 0)
-- The cross-validation it was added for never ran. The human label is fully
-- derivable via ``instrument_type_id -> etoro_instrument_types.description``
-- (the FE already joins for display), so if the integrity check is ever
-- revived it should key on ``instrument_type_id`` directly.
--
-- Idempotent + safe: the column is all-NULL, so DROP COLUMN loses no data.

DROP INDEX IF EXISTS idx_instruments_instrument_type;

ALTER TABLE instruments DROP COLUMN IF EXISTS instrument_type;
