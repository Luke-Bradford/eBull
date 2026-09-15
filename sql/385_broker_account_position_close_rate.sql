-- 385_broker_account_position_close_rate.sql
--
-- #3068 slice 2: retain the broker's PUBLISHED per-position close rate, the conversion
-- rate it applies, the currency it is quoted in, and the instant it was struck.
-- Writer: app/services/account_equity_evidence.py (`_replace_position_marks`), fed by
--         app/providers/implementations/etoro_broker.py (`_parse_account_risk_snapshot`).
-- Reader: app/services/account_equity_evidence.py (`load_account_equity_evidence`).
--
--
-- ⚠⚠ WHY sql/383 WAS NOT ENOUGH, AND WHAT IT GOT WRONG
-- ---------------------------------------------------------------------------
-- sql/383 stored `units`, `amount` and `unrealized_pnl` so a reconciliation could derive
-- a mark as `(amount + unrealized_pnl) / units`. That derivation is WRONG, twice over,
-- and the proposal (`docs/proposals/ops/2026-09-15-3068-reconciliation-mark-instant.md`,
-- revision 4) carries the arithmetic:
--
--   1. It is EQUITY PER UNIT, not price. The portal documents `amount` as "USD amount
--      allocated to the position. This amount includes both the initial investment, and
--      additional margin allocated to the position as collateral" -- so it equals
--      `units * openRate` only at leverage 1 with no added collateral. At leverage 2 the
--      derived mark produces a ~50-unit difference on a perfectly healthy position.
--   2. Where it does NOT produce a false divergence it produces NO SIGNAL. With equal
--      quantities the substituted local value reduces algebraically to
--      `units * open_rate_local - amount_local`, which is identically ZERO for exactly
--      the unleveraged case -- i.e. for every position on the live book (measured
--      2026-09-15: `leverage` 1 on 7/7, and `amount = units * openRate` on 7/7). A value
--      comparison that cannot fail reports green.
--
-- The broker publishes the mark directly, and always did. `unrealizedPnL` -- the object
-- whose `pnL` sql/383 already reads -- documents and populates:
--
--   closeRate            "Current close rate"                    -- 7/7 live
--   closeConversionRate  "Current close conversion rate"         -- 7/7 live (1.0, USD/USD)
--   assetCurrencyId      "Currency ID for the asset"             -- 7/7 live (1 = USD)
--   timestamp            "Timestamp of the PnL calculation"      -- 7/7 live, identical
--
-- (`TradingRealAdminApi_Position.unrealizedPnL`, committed
-- `tests/fixtures/etoro/openapi_v1.375.0.json`; live demo values read read-only
-- 2026-09-15 -- GME `closeRate` 21.54 against our 09-14 close of 21.63, which is the
-- evening mark the whole ticket is about.)
--
-- ⚠ The mistake this migration corrects is the THIRD occurrence of one shape in this
-- ticket: asserting a field is absent from the PAYLOAD because it is absent from OUR
-- PARSED MODEL. It cost a spec revision each time. Our parser is not evidence about the
-- payload; the schema and one read-only GET are.
--
--
-- ⚠ NULLABLE, AND NOT BACKFILLED
-- ---------------------------------------------------------------------------
-- The 7 child rows already stored (2026-09-15, written by the sql/383 writer) predate
-- these columns and read NULL. They are NOT backfilled and cannot be: a close rate is
-- instantaneous and the payload is not retained, so any value put here now would be a
-- reconstruction wearing an observation's clothes. The reader refuses a child row with a
-- NULL operand rather than falling back to the withdrawn derivation. Same forward-only
-- posture as sql/308, sql/350 and sql/384.
--
-- ⚠ The CHECKs are written `IS NULL OR ...` for that reason. They constrain what may be
-- WRITTEN without asserting that every stored row has been.
--
-- ⚠⚠ A CHECK IS NOT A FINITENESS GUARANTEE HERE. PostgreSQL `numeric` admits `NaN`, and
-- orders it ABOVE every non-NaN value -- so `NaN > 0` is TRUE and passes `close_rate > 0`.
-- The reader validates `is_finite()` in Python before dividing or multiplying; this
-- constraint catches sign and zero, not drift.
--
-- ⚠ `pnl_timestamp` carries no CHECK and is not a comparand operand. It is stored because
-- it is the ONLY record of the instant the official marks were struck, and the unsolved
-- half of #3068 -- that the official set and the local book are captured at different
-- times, so any account activity between them is a real difference on a healthy pipeline
-- -- cannot be MEASURED without it. No threshold reads it yet, and inventing one before
-- the window has been measured is the mistake the tolerance already made.
--
-- ⚠ `asset_currency_id` is stored raw, not mapped. The reader maps it through
-- `account_equity_evidence.DOCUMENTED_ACCOUNT_CURRENCIES` and refuses an id it has no
-- documented code for -- the same posture sql/341 takes on the parent's
-- `account_currency_id`. Storing a code we inferred would make the inference
-- indistinguishable from an observation, permanently.

ALTER TABLE broker_account_position_marks
    ADD COLUMN IF NOT EXISTS close_rate            NUMERIC(20,8),
    ADD COLUMN IF NOT EXISTS close_conversion_rate NUMERIC(20,10),
    ADD COLUMN IF NOT EXISTS asset_currency_id     INTEGER,
    ADD COLUMN IF NOT EXISTS pnl_timestamp         TIMESTAMPTZ;

ALTER TABLE broker_account_position_marks
    ADD CONSTRAINT broker_account_position_marks_close_rate_positive
        CHECK (close_rate IS NULL OR close_rate > 0),
    ADD CONSTRAINT broker_account_position_marks_close_conversion_rate_positive
        CHECK (close_conversion_rate IS NULL OR close_conversion_rate > 0),
    ADD CONSTRAINT broker_account_position_marks_asset_currency_id_positive
        CHECK (asset_currency_id IS NULL OR asset_currency_id > 0);

COMMENT ON COLUMN broker_account_position_marks.close_rate IS
    'The broker''s own mark for this position at the snapshot instant, in the ASSET '
    'currency (portal: unrealizedPnL.closeRate, "Current close rate"). This is the '
    'operand the #3068 v2 comparand substitutes for our session close; (amount + '
    'unrealized_pnl) / units is NOT a substitute for it -- that is equity per unit, which '
    'equals the price only at leverage 1 with no added margin. NULL means the row predates '
    'sql/385 and the reader refuses it.';

COMMENT ON COLUMN broker_account_position_marks.close_conversion_rate IS
    'The broker''s asset-currency -> account-currency rate at the same instant (portal: '
    'unrealizedPnL.closeConversionRate). Used instead of our ECB daily rate so the mark '
    'correction and the rate applied to it come from one observation.';

COMMENT ON COLUMN broker_account_position_marks.asset_currency_id IS
    'eToro currency id the close rate is quoted in (portal: unrealizedPnL.assetCurrencyId). '
    'Checked against the local position''s instruments.currency: matching position ids and '
    'directions do not establish that the two sides agree on the currency of the price.';

COMMENT ON COLUMN broker_account_position_marks.pnl_timestamp IS
    'Instant the broker calculated this P&L (portal: unrealizedPnL.timestamp). Evidence, '
    'not an operand: nothing reads it for a verdict. It exists so the gap between the '
    'official capture and the local end-of-day book -- the half of #3068 that is still '
    'open -- can be measured before any rule is written for it.';
