-- 494_tax_fund_disposals.sql
--
-- #3616 slice 3d-3c-3 — non-reporting fund disposals (relevant securities).
-- Spec: docs/specs/metrics/2026-10-15-3616-slice3d3c3-fund-disposals.md (Design 5). Writer:
-- `app.services.tax_matching.run_tax_matching` under `identification_v2` / `tax_character_v2`.
--
-- Character rows gain the fund status columns and the `non_reporting_fund` character; security rows gain
-- `holding_basis`; three tables hold a fund's disposals, identifications and acquisition balances. Nothing is
-- backfilled: rows of runs under any other policy pair (run 7 and earlier) keep every new column NULL, which the
-- policy trigger requires, so no stored run claims a status it never evaluated. CHECKs are row-local; everything that
-- depends on the parent run's policy ids, a sibling row or a determination is a trigger.

BEGIN;

-- --- character rows ---------------------------------------------------------------------------------------------------

ALTER TABLE tax_matching_characters DROP CONSTRAINT IF EXISTS tax_matching_characters_character_check;
ALTER TABLE tax_matching_characters ADD CONSTRAINT tax_matching_characters_character_check
    CHECK (character IN ('share', 'unestablished', 'non_reporting_fund'));

ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS fund_status TEXT CHECK (fund_status IN (
    'no_run', 'run_failed', 'not_observed', 'fund_observation_unavailable', 'not_candidate', 'no_determination',
    'binding_mismatch', 'review_out_of_date', 'documents_unavailable', 'document_changed', 'quote_unverified',
    'coverage_unavailable', 'stale', 'outside_interval', 'not_offshore_fund', 'offshore_fund_unresolved',
    'reporting_status_unestablished', 'exceptions_unreviewed'));
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS fund_status_reason TEXT
    CHECK (fund_status_reason IN ('list_match', 'search_unavailable'));
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS fund_status_date DATE;
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS fund_determination_id TEXT
    REFERENCES tax_fund_determinations (determination_id);
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS fund_determination_run_id BIGINT
    REFERENCES tax_fund_determination_runs (run_id);
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS transparent_fund TEXT
    CHECK (transparent_fund IN ('transparent', 'opaque', 'unresolved'));
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS transparent_basis TEXT
    CHECK (transparent_basis IN ('established', 'assumed', 'unresolved'));
ALTER TABLE tax_matching_characters ADD COLUMN IF NOT EXISTS fund_assumptions TEXT[];

ALTER TABLE tax_matching_characters DROP CONSTRAINT IF EXISTS tax_matching_characters_fund_columns;
ALTER TABLE tax_matching_characters ADD CONSTRAINT tax_matching_characters_fund_columns CHECK ((
    -- `non_reporting_fund` <=> its determination, run, transparency and assumptions, all together.
    (character = 'non_reporting_fund') = (num_nonnulls(fund_determination_id, fund_determination_run_id,
                                                       transparent_fund, transparent_basis, fund_assumptions) = 5)
    AND num_nonnulls(fund_determination_id, fund_determination_run_id, transparent_fund, transparent_basis,
                     fund_assumptions) IN (0, 5)
    -- A status belongs to a fund withheld at the ETF line; its reason and date only to a status.
    AND (fund_status IS NULL OR detail = 'exchange_traded_fund')
    AND (fund_status_reason IS NULL OR fund_status IS NOT NULL)
    AND (fund_status_date IS NOT NULL) = (fund_status IS NOT DISTINCT FROM 'outside_interval')
    AND (transparent_fund IS NULL OR (transparent_basis = 'unresolved') = (transparent_fund = 'unresolved'))
    AND (fund_assumptions IS NULL OR cardinality(fund_assumptions) > 0)
) IS TRUE);

-- --- security rows ----------------------------------------------------------------------------------------------------

ALTER TABLE tax_matching_securities ADD COLUMN IF NOT EXISTS holding_basis TEXT
    CHECK (holding_basis IN ('s104_pool', 'relevant_securities'));
ALTER TABLE tax_matching_securities DROP CONSTRAINT IF EXISTS tax_matching_securities_withheld_basis;
ALTER TABLE tax_matching_securities ADD CONSTRAINT tax_matching_securities_withheld_basis
    CHECK (status <> 'withheld' OR holding_basis IS NULL);

ALTER TABLE tax_matching_dependencies DROP CONSTRAINT IF EXISTS tax_matching_dependencies_reason_check;
ALTER TABLE tax_matching_dependencies ADD CONSTRAINT tax_matching_dependencies_reason_check CHECK (reason IN (
    'unresolved_subject', 'identity_unestablished', 'lot_unavailable', 'subsequent_acquisition_rule', 'unplaceable',
    'character_unestablished', 'time_order_unestablished', 'close_fragments_unestablished', 'pre_2009_rights',
    'identification_incomplete'));

-- --- fund tables ------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tax_fund_disposals (
    matching_run_id           BIGINT NOT NULL,
    tax_security_id           BIGINT NOT NULL,
    lot_key                   TEXT NOT NULL,
    -- Identification order within the security, from 1.
    seq                       INTEGER NOT NULL CHECK (seq > 0),
    executed_at               TIMESTAMPTZ NOT NULL,
    uk_date                   DATE NOT NULL,
    tax_year                  TEXT NOT NULL CHECK (tax_year ~ '^[0-9]{4}/[0-9]{2}$'),
    units                     NUMERIC(24, 8) NOT NULL CHECK (units > 0),
    proceeds_gbp              NUMERIC(44, 14),
    cost_gbp                  NUMERIC(44, 14),
    -- The signed TCGA gain; the basic gain is it floored at nil (reg 42).
    computed_gain_gbp         NUMERIC(44, 14),
    basic_gain_gbp            NUMERIC(44, 14),
    -- NULL for an unresolved character: unresolved, never zero.
    offshore_income_gain_gbp  NUMERIC(44, 14),
    chargeable_gain_gbp       NUMERIC(44, 14),
    status                    TEXT NOT NULL CHECK (status IN ('computed', 'amount_missing')),
    gain_character            TEXT CHECK (gain_character IN (
                                  'capital_loss', 'nil', 'offshore_income_gain', 'oig_or_capital_unresolved')),
    missing_lot_keys          TEXT[] NOT NULL,
    reasons                   TEXT[] NOT NULL,
    PRIMARY KEY (matching_run_id, tax_security_id, lot_key),
    UNIQUE (matching_run_id, tax_security_id, seq),
    FOREIGN KEY (matching_run_id, tax_security_id) REFERENCES tax_matching_securities (matching_run_id, tax_security_id),
    CHECK (((status = 'computed') = (
        num_nonnulls(proceeds_gbp, cost_gbp, computed_gain_gbp, basic_gain_gbp, gain_character) = 5
        AND cardinality(missing_lot_keys) = 0
    )) IS TRUE),
    CHECK ((status <> 'computed' OR computed_gain_gbp = proceeds_gbp - cost_gbp) IS TRUE),
    CHECK ((status <> 'amount_missing' OR (
        (proceeds_gbp IS NULL OR cost_gbp IS NULL)
        AND num_nonnulls(computed_gain_gbp, basic_gain_gbp, gain_character, offshore_income_gain_gbp,
                         chargeable_gain_gbp) = 0
        AND cardinality(missing_lot_keys) > 0
    )) IS TRUE),
    -- Design 3, row by row.
    CHECK ((gain_character IS DISTINCT FROM 'capital_loss' OR (
        computed_gain_gbp < 0 AND basic_gain_gbp = 0 AND offshore_income_gain_gbp = 0
        AND chargeable_gain_gbp = computed_gain_gbp
    )) IS TRUE),
    CHECK ((gain_character IS DISTINCT FROM 'nil' OR (
        computed_gain_gbp = 0 AND basic_gain_gbp = 0 AND offshore_income_gain_gbp = 0 AND chargeable_gain_gbp = 0
    )) IS TRUE),
    CHECK ((gain_character IS DISTINCT FROM 'offshore_income_gain' OR (
        computed_gain_gbp > 0 AND basic_gain_gbp = computed_gain_gbp AND offshore_income_gain_gbp = basic_gain_gbp
        AND chargeable_gain_gbp = 0
    )) IS TRUE),
    CHECK ((gain_character IS DISTINCT FROM 'oig_or_capital_unresolved' OR (
        computed_gain_gbp > 0 AND basic_gain_gbp = computed_gain_gbp AND offshore_income_gain_gbp IS NULL
        AND chargeable_gain_gbp IS NULL
    )) IS TRUE)
);

CREATE TABLE IF NOT EXISTS tax_fund_identifications (
    matching_run_id          BIGINT NOT NULL,
    tax_security_id          BIGINT NOT NULL,
    disposal_lot_key         TEXT NOT NULL,
    seq                      INTEGER NOT NULL CHECK (seq > 0),
    rule                     TEXT NOT NULL CHECK (rule IN ('thirty_day', 'lifo')),
    acquisition_lot_key      TEXT NOT NULL,
    acquisition_executed_at  TIMESTAMPTZ NOT NULL,
    acquisition_uk_date      DATE NOT NULL,
    units                    NUMERIC(24, 8) NOT NULL CHECK (units > 0),
    cost_gbp                 NUMERIC(44, 14) CHECK (cost_gbp >= 0),
    missing_lot_keys         TEXT[] NOT NULL,
    reasons                  TEXT[] NOT NULL,
    PRIMARY KEY (matching_run_id, tax_security_id, disposal_lot_key, seq),
    FOREIGN KEY (matching_run_id, tax_security_id, disposal_lot_key)
        REFERENCES tax_fund_disposals (matching_run_id, tax_security_id, lot_key),
    CHECK ((cost_gbp IS NULL) = (cardinality(missing_lot_keys) > 0)),
    CHECK (cardinality(missing_lot_keys) = 0 OR missing_lot_keys = ARRAY[acquisition_lot_key])
);

CREATE TABLE IF NOT EXISTS tax_fund_acquisition_balances (
    matching_run_id   BIGINT NOT NULL,
    tax_security_id   BIGINT NOT NULL,
    lot_key           TEXT NOT NULL,
    units_left        NUMERIC(24, 8) NOT NULL CHECK (units_left > 0),
    cost_left_gbp     NUMERIC(44, 14) CHECK (cost_left_gbp >= 0),
    missing_lot_keys  TEXT[] NOT NULL,
    reasons           TEXT[] NOT NULL,
    PRIMARY KEY (matching_run_id, tax_security_id, lot_key),
    FOREIGN KEY (matching_run_id, tax_security_id) REFERENCES tax_matching_securities (matching_run_id, tax_security_id),
    CHECK ((cost_left_gbp IS NULL) = (cardinality(missing_lot_keys) > 0)),
    CHECK (cardinality(missing_lot_keys) = 0 OR missing_lot_keys = ARRAY[lot_key])
);

DROP TRIGGER IF EXISTS tax_fund_disposals_guard ON tax_fund_disposals;
CREATE TRIGGER tax_fund_disposals_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_fund_disposals
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_fund_identifications_guard ON tax_fund_identifications;
CREATE TRIGGER tax_fund_identifications_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_fund_identifications
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_fund_acquisition_balances_guard ON tax_fund_acquisition_balances;
CREATE TRIGGER tax_fund_acquisition_balances_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_fund_acquisition_balances
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

-- --- the policy triggers ----------------------------------------------------------------------------------------------
--
-- Each fires after its table's child guard (trigger names order `_guard` before `_policy`), so the run is `building`.

-- True when the run has both v2 policy ids; a mixed pair, either way round, or a NULL is not v2.
CREATE OR REPLACE FUNCTION tax_matching_run_is_v2(run_id BIGINT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT coalesce((SELECT matching_policy_id = 'identification_v2'
                            AND character_policy_id IS NOT DISTINCT FROM 'tax_character_v2'
                     FROM tax_matching_runs WHERE matching_run_id = run_id), FALSE)
$$;

-- Character rows: the legacy branch keeps every new column NULL and never `non_reporting_fund`; the v2 branch takes
-- Design 5's truth table, the determination's `determined` observation and its transparency.
CREATE OR REPLACE FUNCTION tax_matching_characters_policy() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    finding JSONB;
BEGIN
    IF NOT tax_matching_run_is_v2(NEW.matching_run_id) THEN
        IF NEW.character = 'non_reporting_fund'
           OR num_nonnulls(NEW.fund_status, NEW.fund_status_reason, NEW.fund_status_date, NEW.fund_determination_id,
                           NEW.fund_determination_run_id, NEW.transparent_fund, NEW.transparent_basis,
                           NEW.fund_assumptions) > 0 THEN
            RAISE EXCEPTION 'tax_matching_characters: run % is not identification_v2/tax_character_v2: no fund columns',
                NEW.matching_run_id;
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.detail IS NOT DISTINCT FROM 'exchange_traded_fund') <> (NEW.fund_status IS NOT NULL) THEN
        RAISE EXCEPTION 'tax_matching_characters: security % of run %: a fund status exactly at the ETF line',
            NEW.tax_security_id, NEW.matching_run_id;
    END IF;
    IF NEW.character <> 'non_reporting_fund' THEN
        RETURN NEW;
    END IF;
    IF NEW.fund_assumptions <> ARRAY(SELECT DISTINCT a COLLATE "C" FROM unnest(NEW.fund_assumptions) AS a ORDER BY 1)
    THEN
        RAISE EXCEPTION 'tax_matching_characters: fund_assumptions must be sorted and unique';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM tax_fund_determination_observations
        WHERE run_id = NEW.fund_determination_run_id AND tax_security_id = NEW.tax_security_id
          AND determination_id = NEW.fund_determination_id AND outcome = 'determined'
    ) THEN
        RAISE EXCEPTION 'tax_matching_characters: determination run % did not determine % for security %',
            NEW.fund_determination_run_id, NEW.fund_determination_id, NEW.tax_security_id;
    END IF;
    SELECT f INTO finding
    FROM tax_fund_determinations d, jsonb_array_elements(d.body -> 'findings') AS f
    WHERE d.determination_id = NEW.fund_determination_id AND f ->> 'question' = 'transparent_fund';
    IF (finding ->> 'answer', finding ->> 'basis') IS DISTINCT FROM (NEW.transparent_fund, NEW.transparent_basis) THEN
        RAISE EXCEPTION 'tax_matching_characters: transparency of % differs from determination %',
            NEW.tax_security_id, NEW.fund_determination_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_matching_characters_policy ON tax_matching_characters;
CREATE TRIGGER tax_matching_characters_policy
    BEFORE INSERT ON tax_matching_characters
    FOR EACH ROW EXECUTE FUNCTION tax_matching_characters_policy();

-- Security rows: no basis outside v2; under v2 a basis exactly for a computed security.
CREATE OR REPLACE FUNCTION tax_matching_securities_policy() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT tax_matching_run_is_v2(NEW.matching_run_id) THEN
        IF NEW.holding_basis IS NOT NULL THEN
            RAISE EXCEPTION 'tax_matching_securities: run % is not identification_v2/tax_character_v2: no holding_basis',
                NEW.matching_run_id;
        END IF;
    ELSIF (NEW.holding_basis IS NULL) <> (NEW.status = 'withheld') THEN
        RAISE EXCEPTION 'tax_matching_securities: security % of run %: holding_basis is NULL exactly when withheld',
            NEW.tax_security_id, NEW.matching_run_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_matching_securities_policy ON tax_matching_securities;
CREATE TRIGGER tax_matching_securities_policy
    BEFORE INSERT ON tax_matching_securities
    FOR EACH ROW EXECUTE FUNCTION tax_matching_securities_policy();

-- Routing: under v2 a statutory disposal belongs to an `s104_pool` security; a fund row needs a v2 run and a computed
-- `relevant_securities` security. A positive fund gain is an OIG exactly when the security's fund is opaque.
CREATE OR REPLACE FUNCTION tax_matching_basis_policy() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    basis TEXT;
    transparency TEXT;
BEGIN
    SELECT holding_basis INTO basis FROM tax_matching_securities
    WHERE matching_run_id = NEW.matching_run_id AND tax_security_id = NEW.tax_security_id AND status = 'computed';
    IF TG_TABLE_NAME = 'tax_statutory_disposals' THEN
        IF tax_matching_run_is_v2(NEW.matching_run_id) AND basis IS DISTINCT FROM 's104_pool' THEN
            RAISE EXCEPTION 'tax_statutory_disposals: security % of run % is not an s104_pool holding',
                NEW.tax_security_id, NEW.matching_run_id;
        END IF;
        RETURN NEW;
    END IF;
    IF NOT tax_matching_run_is_v2(NEW.matching_run_id) OR basis IS DISTINCT FROM 'relevant_securities' THEN
        RAISE EXCEPTION '%: security % of run % is not a computed relevant_securities holding of a v2 run',
            TG_TABLE_NAME, NEW.tax_security_id, NEW.matching_run_id;
    END IF;
    -- Nested, not `AND`: PL/pgSQL resolves every NEW field an expression names, and only this table has the gain.
    IF TG_TABLE_NAME = 'tax_fund_disposals' THEN
        IF NEW.computed_gain_gbp > 0 THEN
            SELECT transparent_fund INTO transparency FROM tax_matching_characters
            WHERE matching_run_id = NEW.matching_run_id AND tax_security_id = NEW.tax_security_id
              AND character = 'non_reporting_fund';
            IF transparency IS NULL THEN
                RAISE EXCEPTION 'tax_fund_disposals: security % of run % has no non_reporting_fund character row',
                    NEW.tax_security_id, NEW.matching_run_id;
            END IF;
            IF (NEW.gain_character = 'offshore_income_gain') <> (transparency = 'opaque') THEN
                RAISE EXCEPTION 'tax_fund_disposals: positive gain of security % (%) cannot be %',
                    NEW.tax_security_id, transparency, NEW.gain_character;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_statutory_disposals_policy ON tax_statutory_disposals;
CREATE TRIGGER tax_statutory_disposals_policy
    BEFORE INSERT ON tax_statutory_disposals
    FOR EACH ROW EXECUTE FUNCTION tax_matching_basis_policy();

DROP TRIGGER IF EXISTS tax_fund_disposals_policy ON tax_fund_disposals;
CREATE TRIGGER tax_fund_disposals_policy
    BEFORE INSERT ON tax_fund_disposals
    FOR EACH ROW EXECUTE FUNCTION tax_matching_basis_policy();

DROP TRIGGER IF EXISTS tax_fund_identifications_policy ON tax_fund_identifications;
CREATE TRIGGER tax_fund_identifications_policy
    BEFORE INSERT ON tax_fund_identifications
    FOR EACH ROW EXECUTE FUNCTION tax_matching_basis_policy();

DROP TRIGGER IF EXISTS tax_fund_acquisition_balances_policy ON tax_fund_acquisition_balances;
CREATE TRIGGER tax_fund_acquisition_balances_policy
    BEFORE INSERT ON tax_fund_acquisition_balances
    FOR EACH ROW EXECUTE FUNCTION tax_matching_basis_policy();

-- 490's seal, with the three fund tables in the refused-run check.
CREATE OR REPLACE FUNCTION tax_matching_runs_seal() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'building' THEN
            RAISE EXCEPTION 'tax_matching_runs: a run is inserted building, not %', NEW.state;
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'tax_matching_runs: run % cannot be deleted', OLD.matching_run_id;
    END IF;
    IF OLD.state <> 'building' THEN
        RAISE EXCEPTION 'tax_matching_runs: run % is % and cannot change', OLD.matching_run_id, OLD.state;
    END IF;
    IF NEW.state NOT IN ('published', 'refused') THEN
        RAISE EXCEPTION 'tax_matching_runs: run % is sealed published or refused, not %', OLD.matching_run_id, NEW.state;
    END IF;
    -- The identity is in the tuple: an identity column accepts `SET matching_run_id = DEFAULT` on UPDATE.
    IF (NEW.matching_run_id, NEW.evidence_run_id, NEW.matching_policy_id, NEW.class_policy_id, NEW.inputs_digest,
        NEW.inputs_document, NEW.computed_at, NEW.history_fetch_started_at, NEW.character_policy_id, NEW.as_of)
       IS DISTINCT FROM
       (OLD.matching_run_id, OLD.evidence_run_id, OLD.matching_policy_id, OLD.class_policy_id, OLD.inputs_digest,
        OLD.inputs_document, OLD.computed_at, OLD.history_fetch_started_at, OLD.character_policy_id, OLD.as_of) THEN
        RAISE EXCEPTION 'tax_matching_runs: sealing run % changes only state, reason, counts and withheld_all',
            OLD.matching_run_id;
    END IF;
    IF NEW.state = 'refused' AND (
        EXISTS (SELECT 1 FROM tax_matching_instruments WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_lot_classes WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_securities WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_characters WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_dependencies WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_statutory_disposals WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_identifications WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_fund_disposals WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_fund_identifications WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_fund_acquisition_balances WHERE matching_run_id = OLD.matching_run_id)
    ) THEN
        RAISE EXCEPTION 'tax_matching_runs: run % has rows and cannot be refused', OLD.matching_run_id;
    END IF;
    RETURN NEW;
END $$;

COMMIT;
