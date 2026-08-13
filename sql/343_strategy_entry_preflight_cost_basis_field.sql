-- #2598 step 3: split `cost_basis` by WHICH FIELD carried the money.
--
-- `sql/342` (merged hours earlier, same day) shipped a single-member vocabulary,
-- `broker_preflight`, on the correct-at-the-time reasoning that `_costs` is the only
-- producer of `stressed_cost_amount`.  Step 3 makes that insufficient rather than
-- wrong: the executor now accepts eToro's UNDOCUMENTED `value` field as the documented
-- monetary `amount`, so "the broker preflight priced it" no longer says which contract
-- was actually read.  One of the two is off-spec, and that is precisely the fact an
-- audit of a trade path needs to carry.
--
--   broker_preflight_amount   the DOCUMENTED field
--   broker_preflight_value    the off-spec field the live demo response actually sends
--
-- Source rule: the live portal, fetched 2026-08-13 per the
-- `.claude/skills/data-sources/etoro-api.md` protocol (llms.txt ->
-- `api-reference/trading--demo/get-what-if-trading-cost-breakdown.md`, WebFetch never
-- curl).  Documented cost-row fields are `costType` + `amount` ("The monetary value of
-- this cost component, expressed in currency") + `currency` ("ISO 4217 currency code in
-- which amount is denominated").  `value` appears NOWHERE in the documentation.  The
-- live demo response carries keys ['costType', 'currency', 'value'] -- `amount` absent
-- as a KEY, not present-and-null -- with `currency` returned as USD, i.e. it ships the
-- denominator of a field it does not ship.
--
-- Both members are REACHABLE: each is returned by its own branch of
-- `strategy_paper_executor._component_amount`, and both branches are tested.  That is
-- the bar `sql/342`'s header set when it declined to mint `static_band_bound` -- a
-- vocabulary member with no producer implies a capability that does not exist.
--
-- Full population at authoring time (dev, the only database):
--   select count(*) from strategy_entry_preflights;                            -- 0
--   select cost_basis, count(*) from strategy_entry_preflights group by 1;     -- []
-- Still empty -- `cost_unit_undocumented` refused every preflight up to this migration,
-- which is the wall this step exists to remove.  The rename is therefore free; it costs
-- a rewrite of exactly zero stored rows.  The UPDATE below is nonetheless included for
-- `sql/342`'s reason: the count is measured at AUTHORING time and the constraint runs at
-- APPLICATION time, and between the two sits a jobs daemon that this very change makes
-- capable of writing an allocated row.

UPDATE strategy_entry_preflights
   SET cost_basis = 'broker_preflight_amount'
 WHERE cost_basis = 'broker_preflight';

ALTER TABLE strategy_entry_preflights
    DROP CONSTRAINT IF EXISTS strategy_entry_preflights_cost_basis_vocabulary;
ALTER TABLE strategy_entry_preflights
    ADD CONSTRAINT strategy_entry_preflights_cost_basis_vocabulary
    CHECK (cost_basis IS NULL OR cost_basis IN ('broker_preflight_amount', 'broker_preflight_value'));

-- ⚠ `strategy_entry_preflights_allocated_cost_basis` (sql/342) is NOT touched: an
-- allocated row must still carry a basis, and that rule is independent of the
-- vocabulary's membership.

COMMENT ON COLUMN strategy_entry_preflights.cost_basis IS
    'Which pricing path produced stressed_cost_amount, AND which response field carried '
    'it: broker_preflight_amount is the documented eToro field, broker_preflight_value '
    'the off-spec one it actually sends (#2598). NOT NULL on allocated rows; NULL on a '
    'rejection that never reached the cost step. Vocabulary is pinned to '
    'app.services.strategy_paper_executor.COST_BASES by '
    'tests/test_2598_preflight_cost_basis.py.';
