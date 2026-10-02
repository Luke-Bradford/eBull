-- #3546 slice 3: the alpha arms (paper, AI trial, ranking pot) now write #2961's
-- write-ordering marker too. Documentation only -- the CHECK from sql/392 already
-- admits both values, and no row changes.
--
-- `ensure_strategy_request_id` writes 'authority_committed' when it mints a UUID
-- (the authority transaction); `mark_entry_verb_entered` commits
-- 'broker_verb_entered' before the provider call. `terminalise_unsubmitted_entry`
-- releases an 'authority_committed' entry no other session is submitting, with
-- `last_error_code = 'entry_authority_never_submitted'`. That is a LOCAL rejection,
-- distinct from a broker-reported one ('broker_submission_rejected').
--
-- Rows written before this migration keep NULL and are never terminalisable.
COMMENT ON COLUMN strategy_order_reconciliation_state.submission_phase IS
    'ENTRY write-ordering marker (#2961 core, #3546 alpha arms). authority_committed = '
    'the durable authority exists and the verb-entered marker has NOT committed, so the '
    'broker verb was provably never entered. broker_verb_entered = it MAY have been '
    'entered; nothing client-side can say more. NULL = not an entry row written by a '
    'marked path (manual order, close, or a row predating the marker) and never '
    'terminalisable.';
