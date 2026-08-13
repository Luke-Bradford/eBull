-- Migration 145: canonical_instrument_id for operational duplicates (#819)
--
-- Scope: operational-duplicate ticker variants that point at the same
-- underlying security. The canonical example is the eToro convention of
-- appending ``.RTH`` to a ticker for "regular trading hours" variants
-- (AAPL vs AAPL.RTH, MSFT vs MSFT.RTH, …). The variant is a separate
-- ``instruments`` row but the underlying security is the same, so:
--
--   * Filings (10-K, 13F, insider, dividends) live under the base
--     instrument's CIK. The variant has no CIK row of its own — the
--     SEC partial-unique CIK index (sql/143) blocks a second
--     instrument from claiming the same CIK because cik_discovery
--     correctly resolves to the underlying.
--   * UI surfaces (chart, ownership pie, fundamentals) should render
--     the base instrument's data, so operators don't see an empty
--     ``.RTH`` page.
--
-- The redirect mechanism: instruments.canonical_instrument_id FK to
-- self. NULL = "this instrument IS canonical" (the default for every
-- existing row). When set, the API surfaces the canonical symbol so
-- the frontend can ``<Navigate replace>`` to the underlying.
--
-- Scope clarification (settled decision, #1102):
--   * canonical_instrument_id is for OPERATIONAL DUPLICATES only
--     (.RTH and any future similar suffix variants).
--   * Share-class siblings (GOOG/GOOGL, BRK.A/BRK.B) MUST NOT use
--     this mechanism — they are distinct securities (distinct CUSIPs)
--     that legitimately share an issuer CIK. See sql/143 +
--     docs/settled-decisions.md "CIK = entity, CUSIP = security".
--
-- ON DELETE SET NULL: if the canonical row is ever deleted, the
-- variant becomes self-canonical (NULL) rather than dangling FK.
-- This keeps the variant rendering as itself (empty data) instead of
-- breaking — degraded UX, not a hard error.
--
-- CHECK constraint: canonical_instrument_id != instrument_id. A row
-- pointing at itself is meaningless and would cause the FE redirect
-- loop guard to trip; ban it at the DB layer.
--
-- Idempotency: gated on column shape so partial reruns are safe.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name = 'instruments'
           AND column_name = 'canonical_instrument_id'
    ) THEN
        -- BIGINT to match instruments.instrument_id (sql/001_init.sql).
        -- Codex pre-push round 1.
        ALTER TABLE instruments
            ADD COLUMN canonical_instrument_id BIGINT NULL
                REFERENCES instruments(instrument_id) ON DELETE SET NULL;
    END IF;
END
$$;

-- CHECK: never point at self. Idempotent — the constraint name is
-- pinned and skipped on rerun. Cannot be inlined into ADD COLUMN
-- because the column may already exist on partial reruns.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'instruments_canonical_not_self_chk'
    ) THEN
        ALTER TABLE instruments
            ADD CONSTRAINT instruments_canonical_not_self_chk
            CHECK (canonical_instrument_id IS NULL
                   OR canonical_instrument_id <> instrument_id);
    END IF;
END
$$;

-- Reverse-lookup index. The forward direction is the FK; the reverse
-- ("list every variant pointing at AAPL") is operator-facing for the
-- admin runbook + the populate-script's dry-run report.
CREATE INDEX IF NOT EXISTS idx_instruments_canonical_instrument_id
    ON instruments(canonical_instrument_id)
    WHERE canonical_instrument_id IS NOT NULL;

COMMIT;
