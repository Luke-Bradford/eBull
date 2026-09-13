-- #2979 follow-on to 377: an `intent_persisted` close row written BEFORE the marker
-- existed does not mean what the new code reads it to mean.
--
-- 377 gave `intent_persisted` a strictly stronger meaning for closes: "the broker verb
-- was never entered". Rows written by the previous code carry the OLD, weaker meaning —
-- "the crash landed somewhere in the span", which includes the case where the broker
-- accepted and executed the close. Reading such a row with the new interpretation would
-- terminalise it as `close_never_submitted`, return the trade to `open`, and erase a
-- reconciliation warning that was correct.
--
-- Promote every in-flight close intent to `submitting`, which is the conservative
-- reading: the verb MAY have been entered, outcome unknown. That is exactly the state
-- the old code's recovery produced for these rows anyway, so nothing is lost and the
-- new branch is only ever applied to rows the new writer created.
--
-- Separate file rather than an edit to 377 because 377 has already been applied and the
-- migration ledger pins its content hash.
--
-- ⚠ Bounded assumption, stated rather than left implicit: this closes the UPGRADE
-- window, not a rolling-deploy window. Migrations run at app boot before the new code
-- serves, and this system runs one API process and one jobs process — there is no
-- interval in which old code writes a fresh `intent_persisted` close after the backfill.
-- A future multi-process rolling deploy would need the marker written by both versions
-- before this interpretation is safe.
--
-- Measured on the dev DB at authoring time: strategy_position_operations is empty, so
-- this affects 0 rows there. It is correctness for any deployment that is not.
--   select count(*) from strategy_position_operations
--    where operation_type='close' and status='intent_persisted';

UPDATE strategy_position_operations
   SET status = 'submitting', updated_at = now()
 WHERE operation_type = 'close'
   AND status = 'intent_persisted';
