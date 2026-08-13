-- #2602 item 2: record the account base currency the broker REPORTED, not the one we
-- assumed.
--
-- `broker_account_equity_snapshots.currency` shipped as `TEXT NOT NULL CHECK (currency
-- = 'USD')` (sql/324:14) and `record_account_equity_snapshot` binds a `'USD'` SQL
-- literal (account_equity_evidence.py:72).  Nothing on that path ever read a currency
-- from the broker: `BrokerAccountRiskSnapshot` (app/providers/broker.py:372) carried no
-- currency field at all, so the column recorded OUR assumption in the one table whose
-- purpose is official broker evidence.  #2363 recorded the same gap from the cost side
-- -- "Nobody has checked the account currency" -- and #2602 item 2 asks for it verified
-- by measurement.
--
-- Source rule: the eToro live portal's `trading--demo/get-account-pnl-and-portfolio-
-- details` response schema documents `clientPortfolio.accountCurrencyId`, "Currency ID
-- of the account (1 = USD)" (fetched 2026-08-13; see the account-currency section of
-- .claude/skills/data-sources/etoro-api.md).  It is in the payload the provider already
-- parses.  ONE id is documented, so exactly one is mapped -- an unmapped id stores its
-- id with a NULL code rather than a guessed one.
--
-- `account_currency_id` is deliberately NULLable and deliberately NOT backfilled.  A
-- NULL means "written before this migration, currency assumed rather than observed",
-- and those rows can never be repaired: the id exists only in a live payload that was
-- not retained (the table stores no raw response by design, sql/324:4).  Writing 1 into
-- them would manufacture an observation that never happened.  `load_account_equity_
-- evidence` surfaces them as `account_currency_assumed_not_observed`; the writer always
-- binds a non-NULL id, so NULL identifies pre-migration rows permanently.
--
-- Full population at authoring time (dev, the only database):
--   select count(*) from broker_account_equity_snapshots;                    -- 3
--   select currency,count(*) from broker_account_equity_snapshots group by 1;-- [('USD',3)]
-- all three demo rows, 2026-08-11..2026-08-13, all becoming
-- `account_currency_assumed_not_observed`.
--
-- This is NOT a widening of the USD lock.  A non-USD account still cannot be traded:
-- the deployment, pool and core-mandate authorities keep their own `= 'USD'` CHECKs
-- (sql/338, sql/290:96, sql/336:26) and none is touched here.  What changes is that a
-- non-USD account can now be OBSERVED and refused by name instead of being silently
-- recorded as USD -- the evidence table could not previously represent its own
-- falsification.

ALTER TABLE broker_account_equity_snapshots
    ADD COLUMN account_currency_id INTEGER;

ALTER TABLE broker_account_equity_snapshots
    ALTER COLUMN currency DROP NOT NULL;

ALTER TABLE broker_account_equity_snapshots
    DROP CONSTRAINT broker_account_equity_snapshots_currency_check;

-- `IS NOT DISTINCT FROM`, not `=`.  `currency` is now NULLable, and a CHECK is refused
-- only when it evaluates to FALSE -- UNKNOWN passes.  Spelled `currency = 'USD'` this
-- constraint would admit `(account_currency_id = 1, currency = NULL)`, which is the one
-- combination it exists to reject.  Same trap as review-prevention-log.md:4212 (sql/340,
-- one day earlier), reached through a plain nullable-column comparison instead of an
-- array operator.  The two NULL/1 branches are deliberately not merged: they mean
-- different things (assumed vs observed) and only coincide while USD is the sole
-- documented id.
ALTER TABLE broker_account_equity_snapshots
    ADD CONSTRAINT broker_account_equity_snapshots_currency_observed
    CHECK (
        CASE
            WHEN account_currency_id IS NULL THEN currency IS NOT DISTINCT FROM 'USD'
            WHEN account_currency_id = 1 THEN currency IS NOT DISTINCT FROM 'USD'
            ELSE currency IS NULL
        END
    );

COMMENT ON COLUMN broker_account_equity_snapshots.account_currency_id IS
    'eToro clientPortfolio.accountCurrencyId as reported on the observation (1 = USD, '
    'the only id the portal documents). NULL means the row predates #2602 item 2 and '
    'its currency was assumed, not observed -- unrepairable, because the raw payload is '
    'deliberately not retained.';

COMMENT ON COLUMN broker_account_equity_snapshots.currency IS
    'ISO 4217 code for account_currency_id, and NULL when the reported id has no '
    'documented code. A NULL here is a live refusal (account_currency_not_documented), '
    'never a collection gap: the money columns are still true, in a currency we cannot '
    'name and therefore cannot compare.';
