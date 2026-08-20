-- 309_strategy_pool_principal_semantics.sql
--
-- F-0 portfolio truth: the configured amount is both the virtual sleeve's
-- contributed principal and its execution authority.  A material change in
-- that amount is therefore an external flow for performance accounting.

COMMENT ON TABLE strategy_paper_pool_events IS
    'Material operator revisions to shared paper strategy principal and its execution authority. Capital-limit deltas are external strategy-pot flows; enabled/mode-only revisions have zero flow. No scheduler heartbeat writes.';
