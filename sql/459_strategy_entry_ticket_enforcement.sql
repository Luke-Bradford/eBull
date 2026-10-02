-- 459_strategy_entry_ticket_enforcement.sql
-- #3542 (gap register P3) slice 2: an engine ENTRY order cannot commit without its trade ticket,
-- and neither the ticket nor the link it vouches for can be rewritten afterwards. Spec:
-- docs/proposals/execution/2026-10-02-3542-entry-trade-ticket.md (design 2).
--
-- Enforcement is a DEFERRED constraint trigger on the link, so a path may write the link and the
-- ticket in either order inside its authority transaction; the transaction cannot commit with an
-- entry link whose ticket is missing or names a different trade. Every engine path submits only
-- after that commit, so an entry without a ticket never reaches the broker. Exit and protective
-- links are untouched (settled: an exit is never blocked).
--
-- Grandfathering: the trigger fires on INSERT only, so entry links committed before sql/458 (dev
-- held 5) are never re-checked, and nothing back-fills a ticket for them -- a ticket is a
-- statement made at the point of action. Reproduce the count with:
--   SELECT count(*) FROM strategy_trade_orders o LEFT JOIN strategy_entry_tickets t USING (order_id)
--   WHERE o.purpose = 'entry' AND t.order_id IS NULL;

CREATE OR REPLACE FUNCTION strategy_entry_link_requires_ticket()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM strategy_entry_tickets
        WHERE order_id = NEW.order_id AND strategy_trade_id = NEW.strategy_trade_id
    ) THEN
        RAISE EXCEPTION 'entry order % of strategy trade % has no trade ticket (#3542)',
            NEW.order_id, NEW.strategy_trade_id;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS trg_strategy_trade_orders_entry_ticket ON strategy_trade_orders;
CREATE CONSTRAINT TRIGGER trg_strategy_trade_orders_entry_ticket
AFTER INSERT ON strategy_trade_orders
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.purpose = 'entry')
EXECUTE FUNCTION strategy_entry_link_requires_ticket();

-- A ticket is stated once. TRUNCATE (test isolation) does not fire row triggers.
CREATE OR REPLACE FUNCTION prevent_strategy_entry_ticket_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% of trade ticket for order % is refused: a ticket is stated once (#3542)',
        TG_OP, OLD.order_id;
END $$;

DROP TRIGGER IF EXISTS trg_strategy_entry_tickets_append_only ON strategy_entry_tickets;
CREATE TRIGGER trg_strategy_entry_tickets_append_only
BEFORE UPDATE OR DELETE ON strategy_entry_tickets
FOR EACH ROW EXECUTE FUNCTION prevent_strategy_entry_ticket_mutation();

-- The link's identity is what the ticket was checked against at commit; re-pointing it afterwards
-- would leave a ticket vouching for a different order, trade or purpose. No app path updates
-- these columns (grep "UPDATE strategy_trade_orders" app: none at sql/459).
CREATE OR REPLACE FUNCTION prevent_strategy_trade_order_relink()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.purpose IS DISTINCT FROM OLD.purpose
       OR NEW.order_id IS DISTINCT FROM OLD.order_id
       OR NEW.strategy_trade_id IS DISTINCT FROM OLD.strategy_trade_id THEN
        RAISE EXCEPTION 'strategy order link % is immutable once written (#3542)', OLD.strategy_trade_order_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_strategy_trade_orders_immutable_link ON strategy_trade_orders;
CREATE TRIGGER trg_strategy_trade_orders_immutable_link
BEFORE UPDATE OF purpose, order_id, strategy_trade_id ON strategy_trade_orders
FOR EACH ROW EXECUTE FUNCTION prevent_strategy_trade_order_relink();
