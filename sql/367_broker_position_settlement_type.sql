-- #2602 item 3 -- product identity PER POSITION, from the broker's own field.
--
-- Source rule (live portal 2026-08-23,
-- `api-reference/trading--demo/get-account-pnl-and-portfolio-details`): the
-- open-positions response documents
--
--     settlementTypeID -- "Position investment type. 0 - CFD, 1 - Real Asset,
--                          2 - SWAP, 3 - Crypto MarginTrade, 4 - Future Contract"
--
-- That is the authority for what an EXISTING position IS. It is deliberately
-- not `strategy_core_eligibility_proofs`: that table answers "can this account
-- open the underlying TODAY", which is a different question from "what product
-- is this position I opened months ago". Labelling a held position from today's
-- eligibility would be an inference dressed as evidence.
--
-- ⚠ The two vocabularies are NOT the same set. The eligibility response uses a
-- four-value STRING vocabulary (`real` / `realFutures` / `marginTrade` / `cfd`,
-- see app/services/broker_settlement_arms.py); this is a five-value NUMERIC one
-- and `SWAP` has no counterpart there. They are kept apart on purpose -- a
-- hand-written equivalence between two vendor enums is exactly the kind of
-- invented mapping the source-rule discipline exists to stop.
--
-- ⚠ `isSettled` sits next to it in the same payload and correlates PERFECTLY
-- with this field on all seven positions currently held. It is documented
-- "Obsolete" and must not be used. A 7-row correlation is not corroboration.
--
-- No CHECK on the value set. The vendor can add a sixth type without notice,
-- and a constraint would turn that into a hard failure of position INGEST over
-- an evidence field -- refusing to record the whole position because we cannot
-- label it is strictly worse than recording it with an unlabelled type. The
-- closed vocabulary lives in code (`position_investment_type_label`), which
-- returns NULL for an unrecognised id rather than guessing.
--
-- Backfill is exact and needs no re-sync: `raw_payload` has retained the field
-- verbatim for every position ever written. Measured on dev before this ran --
-- 7 of 7 open positions carry it (4 at 0, 3 at 1).

ALTER TABLE broker_positions
    ADD COLUMN IF NOT EXISTS settlement_type_id SMALLINT;

ALTER TABLE broker_positions_closed
    ADD COLUMN IF NOT EXISTS settlement_type_id SMALLINT;

COMMENT ON COLUMN broker_positions.settlement_type_id IS
    'eToro settlementTypeID -- "Position investment type. 0 - CFD, 1 - Real Asset, '
    '2 - SWAP, 3 - Crypto MarginTrade, 4 - Future Contract" (live portal 2026-08-23, '
    'get-account-pnl-and-portfolio-details). Per-POSITION product identity; not the '
    'same vocabulary as the eligibility response settlementType. NULL = the broker '
    'did not report it.';

COMMENT ON COLUMN broker_positions_closed.settlement_type_id IS
    'Archived copy of broker_positions.settlement_type_id -- see that column comment.';

UPDATE broker_positions
   SET settlement_type_id = (raw_payload->>'settlementTypeID')::smallint
 WHERE settlement_type_id IS NULL
   AND raw_payload ? 'settlementTypeID'
   -- Guard the cast BOTH ways: the column is evidence, so a value we cannot
   -- read must leave the row NULL rather than abort the migration for every
   -- other position. The regex alone is not enough -- it admits '999999', which
   -- then overflows ::smallint and takes the whole migration down (Codex
   -- ckpt-2). ::numeric on a matched digit-string cannot itself throw.
   AND raw_payload->>'settlementTypeID' ~ '^-?[0-9]+$'
   AND (raw_payload->>'settlementTypeID')::numeric BETWEEN -32768 AND 32767;

UPDATE broker_positions_closed
   SET settlement_type_id = (raw_payload->>'settlementTypeID')::smallint
 WHERE settlement_type_id IS NULL
   AND raw_payload ? 'settlementTypeID'
   AND raw_payload->>'settlementTypeID' ~ '^-?[0-9]+$'
   AND (raw_payload->>'settlementTypeID')::numeric BETWEEN -32768 AND 32767;
