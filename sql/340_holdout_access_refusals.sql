-- 340_holdout_access_refusals.sql
--
-- #2611 — the audit trail for an outcome-access attempt that was REFUSED.
-- Spec: docs/proposals/ta/2026-08-13-refused-outcome-access-audit.md.
-- Writer: app/services/result_ledger.py (`_refuse_access`). Refusal
-- vocabulary: app/services/prereg_contract.py. Parent gate: #2599 (sql/333),
-- its supersession chain: #2634 (sql/337).
--
--
-- ⚠⚠ WHY A SECOND TABLE AND NOT A ROW IN `strategy_holdout_accesses`.
-- ---------------------------------------------------------------------------
-- Two independent reasons, either one sufficient:
--
--   1. `result_ledger.holdout_access_counts` counts that relation into
--      `recorded_accesses`, which feeds `check_promotable`'s criterion 5. A
--      refused attempt is not an evaluation and must not move that count.
--   2. `supersede_preregistration` refuses `supersession_trial_already_exposed`
--      on `count(*) > 0` of that relation. A refused attempt is precisely NOT
--      exposure — nothing was returned — so a refusal row there would
--      permanently strand a trial from #2634's repair path over a look that
--      never happened.
--
-- Every other consumer of the access log reads it as "looks that happened"
-- too: `app/api/strategies.py`'s access summary, `trial_register`'s
-- reconstruction evidence, `scripts/sealed_rerun_gate.py`'s spent-entry check,
-- `strategy_promotion_replay`, and sql/264's own hold-out write trigger.
-- Separate storage is what keeps all of them correct without an edit.
--
--
-- ⚠⚠ WHY THIS ROW IS WRITTEN FROM A DIFFERENT TRANSACTION, AND WHY THAT IS
-- SOUND HERE WHEN IT WOULD NOT BE FOR AN ACCESS RECORD.
-- ---------------------------------------------------------------------------
-- An ACCESS record is a claim about DATA: sql/264's trigger must see it in the
-- same transaction as the hold-out row it authorises, and a rolled-back
-- evaluation did not happen. `record_holdout_access` says exactly that and
-- stays correct.
--
-- A REFUSAL record is a claim about an ACT OF THE CALLER. It completes when
-- `PreregDeclarationRefused` is constructed; the caller rolling back does not
-- un-attempt it, and a caller that retries N times really did attempt N times.
-- Postgres has no autonomous transaction, so the writer opens a second
-- connection derived from the caller's own — which is the only reason this row
-- exists at all, since the refusal is an exception and the caller's
-- transaction is very nearly always rolled back.
--
--
-- ⚠ NO FOREIGN KEY ON `declaration_id`, AND NO ADVISORY LOCK IN THE WRITER.
-- ---------------------------------------------------------------------------
-- Measured 2026-08-13: `pg_advisory_xact_lock(hashtext(k))` BLOCKS across
-- connections (a second connection with `statement_timeout=1500ms` raised
-- QueryCanceled). `record_holdout_access` takes that lock on the trial and
-- still holds it at the moment it refuses, so the audit write must take no
-- lock the caller could be holding. An FK is the same hazard in quieter
-- clothing: it takes a KEY SHARE lock on the parent row, and it cannot see a
-- declaration the caller froze in its own still-open transaction. So the id is
-- recorded as a plain BIGINT and may name a declaration that was later rolled
-- back — which is itself accurate, because that is what the refused look saw.

BEGIN;

CREATE TABLE IF NOT EXISTS strategy_holdout_access_refusals (
    refusal_id       BIGSERIAL PRIMARY KEY,

    -- The trial the caller tried to open. Same identity pair as sql/264.
    strategy_id      TEXT NOT NULL,
    strategy_version TEXT NOT NULL,

    -- ⚠ Mirrors sql/264 exactly, including the nullability rule: a `read` may
    -- span every result version and naming one would be a fiction.
    result_version   TEXT,
    access_kind      TEXT NOT NULL
        CHECK (access_kind IN ('evaluate', 'read')),

    -- WHO and WHY, carried through from the attempt. ⚠ Caller-supplied and
    -- unauthenticated, exactly as `strategy_holdout_accesses.accessed_by` is —
    -- this table records what the attempt SAID it was, which is the same
    -- standard the access log has always held.
    accessed_by      TEXT NOT NULL,
    purpose          TEXT NOT NULL,

    -- ⚠ EVERY CODE THAT FIRED, NEVER THE FIRST. `declaration_refusals` returns
    -- the whole tuple for the same reason: an operator acts on the set, and a
    -- refusal reported as one of five codes sends them to fix one of five
    -- things. Same shape as `expected_structural_refusals` on sql/333.
    refusals         TEXT[] NOT NULL,

    -- The declaration the refused look resolved to, or NULL when the trial had
    -- frozen none (`preregistration_not_frozen`). No FK — see the header.
    declaration_id   BIGINT,

    -- ⚠ `clock_timestamp()`, NOT `now()`. `now()` is the transaction's start,
    -- and this transaction begins only after the audit connection is opened —
    -- so `now()` would additionally hide the connect latency. This stamp still
    -- TRAILS the refusal by that latency, bounded by PGCONNECT_TIMEOUT
    -- (app/config.py `DB_CONNECT_TIMEOUT_S` = 10s). It is the instant the
    -- attempt was recorded, not the instant it was refused, and the difference
    -- is stated rather than papered over.
    refused_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- ⚠ The #2286 shape: NOT NULL admits a PRESENT-but-empty value, and an
    -- empty `purpose` is an audit row that logged nothing while counting as a
    -- record. `btrim` rather than `<> ''` because three spaces is non-empty and
    -- says the same nothing (sql/337 makes the same check for the same reason).
    CONSTRAINT strategy_holdout_access_refusals_non_empty
        CHECK (
            btrim(strategy_id) <> ''
            AND btrim(strategy_version) <> ''
            AND btrim(accessed_by) <> ''
            AND btrim(purpose) <> ''
            AND (result_version IS NULL OR btrim(result_version) <> '')
        ),

    -- ⚠ An empty array, or one carrying a NULL or blank code, is a refusal that
    -- names no reason — the audit trail's entire content. Refused at the
    -- constraint rather than trusted from the writer.
    --
    -- ⚠ NO SUBQUERY FORM. `NOT EXISTS (SELECT ... FROM unnest(refusals))` reads
    -- better and Postgres rejects it outright: "cannot use subquery in check
    -- constraint" (FeatureNotSupported, measured 2026-08-13). The two scalar
    -- predicates below were measured to refuse `{}`, `{NULL}`, `{""}` and
    -- `{"a",""}` and to accept `{"a"}` — `array_position` is what catches the
    -- NULL element, because `'' <> ALL(ARRAY[NULL])` is NULL and a NULL CHECK
    -- passes.
    --
    -- ⚠ The closed code vocabulary is deliberately NOT pinned here. It lives in
    -- `prereg_contract.DeclarationRefusal` and grows there; sql/333's
    -- `expected_structural_refusals` makes the same choice for the same reason.
    CONSTRAINT strategy_holdout_access_refusals_names_a_reason
        CHECK (
            cardinality(refusals) > 0
            AND array_position(refusals, NULL) IS NULL
            AND '' <> ALL(refusals)
        ),

    -- The sql/264 rule, mirrored: an `evaluate` names the single result version
    -- it would have authorised. A refused evaluate that named none would be one
    -- record standing in for every hold-out row of that strategy version.
    CONSTRAINT strategy_holdout_access_refusals_evaluate_names_a_result
        CHECK (access_kind <> 'evaluate' OR result_version IS NOT NULL)
);

-- The governance read: "what was refused on this trial, most recent first".
CREATE INDEX IF NOT EXISTS idx_strategy_holdout_access_refusals_trial
    ON strategy_holdout_access_refusals (strategy_id, strategy_version, refusal_id DESC);

COMMENT ON TABLE strategy_holdout_access_refusals IS
    '#2611 — one row per REFUSED outcome-access attempt: the trial, the caller, '
    'the purpose and every refusal code that fired. ⚠ NOT an access log and '
    'deliberately not counted as one — nothing was returned, so a row here is '
    'neither a criterion-5 evaluation nor exposure for #2634''s supersession '
    'check. Written from a SEPARATE connection so it survives the caller''s '
    'rollback, which is the only state in which it can exist: the refusal is an '
    'exception. Writer: result_ledger._refuse_access.';

COMMENT ON COLUMN strategy_holdout_access_refusals.declaration_id IS
    'The declaration the refused look resolved to, or NULL when the trial had '
    'frozen none. No FK by design (see the migration header): the audit '
    'transaction cannot see a declaration the caller froze in its own open '
    'transaction, and an FK check would take a lock on a row that caller may '
    'hold.';

COMMIT;
