-- 181_quotes_last_positive.sql
--
-- #1429: forbid a non-positive `quotes.last`.
--
-- WHY
-- ---
--   eToro returns lastExecution=0.00 for instruments not freshly traded
--   (bid/ask present, no recent trade). The quote writers persisted that 0
--   verbatim. A 0 `last` is not a real trade price; every mark-to-market
--   consumer that read `last` valued such a position at 0 → fake −100% P&L
--   (#1428, VOO/IEP/BBBY). #1428 fixed the read side defensively; this is the
--   canonical data repair: `last` is the actual last-trade price or NULL,
--   never a sentinel 0/negative.
--
-- WHAT IT DOES
-- -----------
--   1. BACKFILL: null out every existing non-positive `last` row in place.
--      (The provider/websocket normalizers now coerce at write time —
--      app/providers/implementations/etoro.py::_normalise_rate and
--      app/services/etoro_websocket.py::_parse_rate_content — so this only
--      repairs rows written before this migration.)
--   2. CONSTRAINT: a hard backstop so any future writer that emits last<=0
--      fails loudly at INSERT/UPDATE instead of silently poisoning marks.
--      `last IS NULL OR last > 0` — NULL stays legal ("no trade price").
--
-- Idempotent: the UPDATE is a no-op once clean; named DROP IF EXISTS + ADD.
-- Safe to re-apply. The backfill MUST precede the ADD CONSTRAINT, else any
-- pre-existing 0 row would make the ADD CONSTRAINT fail.

BEGIN;

UPDATE quotes
SET last = NULL
WHERE last IS NOT NULL
  AND last <= 0;

ALTER TABLE quotes
    DROP CONSTRAINT IF EXISTS quotes_last_positive;

ALTER TABLE quotes
    ADD CONSTRAINT quotes_last_positive
    CHECK (last IS NULL OR last > 0);

COMMENT ON CONSTRAINT quotes_last_positive ON quotes IS
    '#1429: last is the actual last-trade price or NULL, never <=0. eToro '
    'persists lastExecution=0 for un-freshly-traded instruments; a 0 mark '
    'reads as fake -100% P&L. Writers coerce non-positive last to NULL; this '
    'is the hard backstop. Read-side derives a mark from bid/ask when NULL.';

COMMIT;
