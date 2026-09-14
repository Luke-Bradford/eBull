-- #3017: repair the cost pools `portfolio_sync` left behind on closed positions.
--
-- `portfolio_sync`'s `closed_externally` branch zeroed `current_units` and left
-- `cost_basis` at its full value, so a fully-closed row kept the entire pool. The code
-- fix in this PR stops producing them; this repairs the rows already written.
--
-- ⚠ A zero-unit row is not inert. `_update_position_buy`'s `ON CONFLICT` computes
-- `avg_cost = (positions.cost_basis + EXCLUDED.cost_basis) / sum_units`, so re-opening
-- one of these instruments books an average above every price paid — the #3008 failure
-- with its origin in the other writer.
--
-- Measured on the dev DB at authoring time: 1 row (instrument 1129, WDC —
-- `current_units` 0, `cost_basis` 750.500000, zeroed 2.3 s after its only fill by the
-- post-trade sync that fill queued).
--   select count(*) from positions where current_units <= 0 and cost_basis <> 0;
--
-- `avg_cost` is deliberately left as the historical per-unit cost: on zero units the
-- identity `cost_basis = avg_cost * current_units` holds with an empty pool either way,
-- and a re-open recomputes `avg_cost` from the sums.
--
-- `realized_pnl` is deliberately NOT synthesised. These positions were closed at the
-- broker with no close price ever reaching us; inventing one to book a disposal would be
-- worse than the gap. That belongs to the trade-events / return-attribution layer (#2602).
--
-- Scope is the `current_units <= 0` case only — the one whose cause is established. A row
-- with units still open and a mismatched pool (0 on dev) could come from either writer,
-- and repairing it would be a guess about which column is the right one.

UPDATE positions
   SET cost_basis = 0,
       updated_at = now()
 WHERE current_units <= 0
   AND cost_basis <> 0;
