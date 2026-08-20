-- 264_strategy_holdout_namespace.sql
--
-- Phase 5e-1 — criterion 5's hold-out namespace and its access log.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §5.2 (the frozen
-- split), §8 (stage 5e), acceptance C5. Parent:
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md criterion 5.
-- The row this gates: sql/262 + sql/263. Writer: app/services/result_ledger.py.
-- Literals and the promotion gate: app/services/strategy_result.py.
--
--
-- ⚠⚠ C5 IS "MECHANICALLY INACCESSIBLE", NOT "LOGGED" — AND RLS IS NOT THE
-- MECHANISM HERE. MEASURED, NOT ASSUMED.
-- ---------------------------------------------------------------------------
-- C5: "The hold-out is a separate result namespace that is MECHANICALLY
-- INACCESSIBLE to exploratory queries — logging alone is not the criterion,
-- which says governance fails."
--
-- The obvious Postgres answer is row-level security, so it was tested against
-- this database on 2026-08-07 rather than reasoned about:
--
--     create table _rls_probe(ns text);
--     insert into _rls_probe values ('in_sample'),('hold_out');
--     alter table _rls_probe enable row level security;
--     alter table _rls_probe force row level security;
--     create policy p on _rls_probe for select using (ns = 'in_sample');
--     select count(*) from _rls_probe;   -- 2, as `postgres`
--
-- TWO of two rows came back. `FORCE ROW LEVEL SECURITY` binds the table OWNER
-- but not a superuser, and this app connects as `postgres` with `rolsuper` and
-- `rolbypassrls` both true (`select rolsuper, rolbypassrls from pg_roles where
-- rolname = current_user`). An RLS policy here would be a gate whose only
-- caller walks straight through it — #2288's own warning, one layer down: "a
-- label nobody gates on is worse than no label: it looks like control and
-- provides none."
--
-- ⚠ So RLS is deliberately NOT shipped. Restoring it as a real mechanism needs
-- a non-superuser application role, which is a change to how every connection
-- in the app authenticates and is filed separately, not smuggled in here.
--
--
-- ⚠⚠ WHAT IS SHIPPED INSTEAD: THE OBVIOUS NAME CANNOT EXPRESS A HOLD-OUT ROW
-- ---------------------------------------------------------------------------
-- A VIEW filters for every role, superuser included — there is no bypass bit
-- for "this view has a WHERE clause". So:
--
--   strategy_results         -- a VIEW, in-sample only. The name every doc,
--                            -- every index comment and every exploratory
--                            -- `select * from …` already uses.
--   strategy_results_store   -- the storage, holding BOTH namespaces, under a
--                            -- name you have to decide to type.
--
-- The failure mode C5 is about is not a determined adversary: it is a strategy
-- being iterated against hold-out numbers that arrived in a result set nobody
-- asked for. `select * from strategy_results` can no longer return one. Reading
-- the withheld side means naming the store — a deliberate act — and the
-- sanctioned door (`result_ledger.read_holdout_results`) records an access on
-- the way through.
--
-- ⚠ THE VIEW IS `SELECT *`, WHICH POSTGRES EXPANDS AT CREATION TIME. A later
-- migration adding a column to the store does NOT add it to the view. That is a
-- real drift trap and it is guarded by a TEST, not by a comment:
-- `tests/test_strategy_holdout_namespace.py::test_the_view_exposes_every_stored_column`
-- compares both column lists against `information_schema`. Any migration adding
-- a result column must re-run `CREATE OR REPLACE VIEW` below.
--
-- ⚠ WITH CASCADED CHECK OPTION so the view cannot be used to smuggle a row past
-- its own filter: an INSERT through `strategy_results` carrying
-- `namespace = 'hold_out'` is refused rather than silently landing in the store
-- and vanishing from the view that accepted it.
--
--
-- ⚠⚠ THE WRITE SIDE IS A DATABASE INVARIANT, BECAUSE TRIGGERS DO BIND A
-- SUPERUSER
-- ---------------------------------------------------------------------------
-- C5 also requires that "every access records timestamp and strategy id". A
-- convention in Python is not that; a trigger is. The trigger below makes an
-- unrecorded hold-out evaluation UNREPRESENTABLE: a hold-out result row whose
-- (strategy_id, strategy_version, result_version) has no `evaluate` access
-- record cannot be inserted, by anyone, including the superuser this app
-- connects as.
--
-- ⚠ It fires on UPDATE too. Without that, `insert … namespace = 'in_sample'`
-- followed by `update … set namespace = 'hold_out'` is an unrecorded hold-out
-- row in two statements.
--
-- Measured 2026-08-07 before this migration: `strategy_results` holds 0 rows
-- (spec M10 — no writer existed until this stage), so the rename, the view and
-- the trigger all cost nothing today and every one of them would cost an
-- invented history later.


-- ---------------------------------------------------------------------------
-- 1. The store
-- ---------------------------------------------------------------------------
-- Guarded so a re-run after a partial application is not a hard error. The
-- runner keys on filename and applies each file once, but a rename is the one
-- statement in this file that cannot be expressed with IF NOT EXISTS.
DO $$
BEGIN
    IF to_regclass('strategy_results_store') IS NULL THEN
        EXECUTE 'ALTER TABLE strategy_results RENAME TO strategy_results_store';
    END IF;
END
$$;

COMMENT ON TABLE strategy_results_store IS
    'STORAGE for backtest results, BOTH namespaces. ⚠ Not the name to query: '
    'the view `strategy_results` is in-sample only, and criterion 5 requires '
    'the hold-out to be mechanically inaccessible to exploratory queries. '
    'Reading the withheld side goes through result_ledger.read_holdout_results, '
    'which records an access. Row shape and its rationale: sql/262 + sql/263.';


-- ---------------------------------------------------------------------------
-- 2. The access log (criterion 5)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_holdout_accesses (
    access_id        BIGSERIAL PRIMARY KEY,

    -- C5: "Every access records timestamp and strategy id."
    strategy_id      TEXT NOT NULL,
    strategy_version TEXT NOT NULL,

    -- ⚠ NULLABLE, and the CHECK below makes the null mean exactly one thing: a
    -- READ may span every result version a strategy has, and naming one would
    -- be a fiction. An EVALUATE names the single row it authorises, which is
    -- what the trigger matches on.
    result_version   TEXT,

    -- `evaluate` — a hold-out result is being COMPUTED AND STORED.
    -- `read`     — stored hold-out numbers are being LOOKED AT.
    -- ⚠ Two kinds rather than one because they are different governance
    -- events: the gate compares evaluations against their records, and a read
    -- is what criterion 5's "evaluated more than once" is actually about.
    access_kind      TEXT NOT NULL
        CHECK (access_kind IN ('evaluate', 'read')),

    -- ⚠ WHO and WHY, both NOT NULL and both non-empty. A count of accesses with
    -- no intent attached answers "how many times" and never "should that have
    -- happened", and the second is the only question criterion 5's governance
    -- framing can be audited against.
    accessed_by      TEXT NOT NULL,
    purpose          TEXT NOT NULL,

    accessed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- ⚠ The #2286 shape again: NOT NULL admits a PRESENT-but-empty value, and
    -- an empty `purpose` is an access that logged nothing while counting as a
    -- record. sql/262 and sql/256 make the same check for the same reason.
    CONSTRAINT strategy_holdout_accesses_non_empty
        CHECK (
            strategy_id <> ''
            AND strategy_version <> ''
            AND accessed_by <> ''
            AND purpose <> ''
            AND (result_version IS NULL OR result_version <> '')
        ),

    -- ⚠ An `evaluate` with no result_version would authorise EVERY hold-out row
    -- for that strategy version at once, which is one record standing in for an
    -- unbounded number of evaluations. The trigger matches on the triple, so a
    -- null here would simply never match — this CHECK turns that silent
    -- never-matches into a loud refusal at the point the mistake is made.
    CONSTRAINT strategy_holdout_accesses_evaluate_names_a_result
        CHECK (access_kind <> 'evaluate' OR result_version IS NOT NULL)
);

-- The trigger's lookup, and the gate's two counts.
CREATE INDEX IF NOT EXISTS idx_strategy_holdout_accesses_lookup
    ON strategy_holdout_accesses (strategy_id, strategy_version, access_kind, result_version);

-- The audit read: "who has looked at the hold-out, most recent first".
CREATE INDEX IF NOT EXISTS idx_strategy_holdout_accesses_recent
    ON strategy_holdout_accesses (accessed_at DESC);

COMMENT ON TABLE strategy_holdout_accesses IS
    'Criterion 5''s access records: every hold-out evaluation and every '
    'hold-out read, with timestamp, strategy id, actor and purpose. ⚠ An '
    'evaluate record is REQUIRED BY TRIGGER before a hold_out row may enter '
    'strategy_results_store — the record is an invariant, not a convention.';

COMMENT ON COLUMN strategy_holdout_accesses.purpose IS
    '⚠ NOT decoration. A count of accesses with no intent attached answers "how '
    'many times" and never "should that have happened", and criterion 5 is a '
    'governance criterion.';


-- ---------------------------------------------------------------------------
-- 3. The trigger: no unrecorded hold-out evaluation
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION strategy_results_require_holdout_access()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.namespace = 'hold_out' AND NOT EXISTS (
        SELECT 1
        FROM strategy_holdout_accesses a
        WHERE a.strategy_id = NEW.strategy_id
          AND a.strategy_version = NEW.strategy_version
          AND a.result_version = NEW.result_version
          AND a.access_kind = 'evaluate'
    ) THEN
        RAISE EXCEPTION
            'hold-out result (%, %, %) has no evaluate access record — criterion 5 requires every hold-out '
            'evaluation to be recorded before it is stored (result_ledger.store_holdout_result)',
            NEW.strategy_id, NEW.strategy_version, NEW.result_version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_strategy_results_holdout_access ON strategy_results_store;

-- ⚠ INSERT OR UPDATE, not INSERT. An in-sample row UPDATEd to `hold_out` is an
-- unrecorded hold-out evaluation reached in two statements, and a BEFORE INSERT
-- trigger is blind to it.
CREATE TRIGGER trg_strategy_results_holdout_access
    BEFORE INSERT OR UPDATE ON strategy_results_store
    FOR EACH ROW
    EXECUTE FUNCTION strategy_results_require_holdout_access();


-- ---------------------------------------------------------------------------
-- 4. The safe name
-- ---------------------------------------------------------------------------
-- ⚠ `SELECT *` is expanded at creation. A migration adding a result column MUST
-- re-run this statement; the column-parity test is what catches a forgotten one.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

-- ⚠ Applied separately: CREATE OR REPLACE VIEW cannot add a check option to an
-- existing view definition in one statement on every supported version, and
-- ALTER VIEW … SET is idempotent.
ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMENT ON VIEW strategy_results IS
    'IN-SAMPLE results only. ⚠ This is a VIEW over strategy_results_store, and '
    'the filter is criterion 5''s "mechanically inaccessible to exploratory '
    'queries": a view filters for every role including the superuser this app '
    'connects as, which RLS does not (measured 2026-08-07 — FORCE ROW LEVEL '
    'SECURITY returned both rows). Hold-out results are in the store and are '
    'read through result_ledger.read_holdout_results, which logs the access.';
