-- 299_strategy_intraday_universe_immutability.sql
--
-- A #2477 universe version is a research identity. Membership and
-- interpretation may be assembled while draft, but after activation only
-- one-way retirement is legal. Any changed scope receives a new version.

CREATE OR REPLACE FUNCTION enforce_strategy_intraday_universe_immutability()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.status = 'draft' THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'active/retired intraday universe versions are immutable';
    END IF;
    IF OLD.status = 'draft' THEN
        IF NEW.universe_version <> OLD.universe_version
           OR NEW.provider <> OLD.provider
           OR NEW.session_rule <> OLD.session_rule
           OR NEW.rationale <> OLD.rationale
           OR NEW.status NOT IN ('draft', 'active') THEN
            RAISE EXCEPTION 'draft intraday universe identity is immutable; only activation is allowed';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status = 'active'
       AND NEW.status = 'retired'
       AND NEW.universe_version = OLD.universe_version
       AND NEW.provider = OLD.provider
       AND NEW.session_rule = OLD.session_rule
       AND NEW.rationale = OLD.rationale
       AND NEW.activated_at = OLD.activated_at THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'active/retired intraday universe versions are immutable';
END;
$$;

DROP TRIGGER IF EXISTS trg_strategy_intraday_universe_immutable
    ON strategy_intraday_universe_versions;
CREATE TRIGGER trg_strategy_intraday_universe_immutable
BEFORE UPDATE OR DELETE ON strategy_intraday_universe_versions
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_intraday_universe_immutability();

CREATE OR REPLACE FUNCTION enforce_strategy_intraday_members_draft_only()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_version TEXT;
    target_status TEXT;
BEGIN
    target_version := CASE WHEN TG_OP = 'DELETE' THEN OLD.universe_version ELSE NEW.universe_version END;
    SELECT status INTO target_status
    FROM strategy_intraday_universe_versions
    WHERE universe_version = target_version;
    IF target_status IS DISTINCT FROM 'draft' THEN
        RAISE EXCEPTION 'intraday universe members are mutable only while their version is draft';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_strategy_intraday_members_draft_only
    ON strategy_intraday_universe_members;
CREATE TRIGGER trg_strategy_intraday_members_draft_only
BEFORE INSERT OR UPDATE OR DELETE ON strategy_intraday_universe_members
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_intraday_members_draft_only();
