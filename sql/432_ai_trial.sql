-- 432_ai_trial.sql
--
-- #3471 slice 1b-i — the AI-discretionary-v1 demo trial's own tables.
-- Spec: docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md §11 (schema), §3
-- (session identity), §6 (refusal vocabulary), §7 (the control draw), §9 (declaration and
-- legal state transitions) and obligation O11 (uncertain legs).
-- Writers: slice 1b-ii (decision job) and slice 2 (executor); nothing writes here yet.
--
-- ⚠ APPEND-ONLY, BY TRIGGER, with ONE exception: `ai_trial_runs` has exactly one legal
-- UPDATE, `claimed → decided | refused`, and only while its lease holds. Triggers bind the
-- superuser this app connects as (sql/264's measured reason for preferring a trigger over RLS).
--
-- ⚠ WHAT THE DATABASE CHECKS, AND WHAT IT CANNOT.
--   * It CAN recompute the §7 draw: the seed material from the declaration digest, the
--     session and `pair_seq`, and `idx = int(sha256(m)) mod len(pool)` over the stored pool.
--     A pair whose control is not the draw's output is refused at insert.
--   * It CAN tie a run's decisions and pairs to the SAME transaction as its `decided`
--     transition (`decided_at = now()`, the transaction timestamp), so a later writer
--     cannot append a decision to a run that has already published (§11 atomicity).
--   * It CANNOT check that the pool is the pack-complete shortlist minus the control leg's
--     holdings, nor recompute `pack_sha256` (Postgres has no canonical-JSON function). The
--     stored pack and pool make both checkable offline.


-- ---------------------------------------------------------------------------
-- 0. Shared: append-only guard
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION prevent_ai_trial_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (#3471): a trial record, once written, stays as written', TG_TABLE_NAME;
END $$;


-- ---------------------------------------------------------------------------
-- 1. Declarations (§9): the frozen document, bound to its #2599 row
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_declarations (
    declaration_id   BIGINT PRIMARY KEY
                     REFERENCES strategy_preregistration_declarations (declaration_id) ON DELETE RESTRICT,
    -- The ARM's strategy id; the control leg's is this plus `-control` (§8).
    strategy_id      TEXT NOT NULL CHECK (strategy_id ~ '^ai-discretionary-v[1-9][0-9]*$'),
    strategy_version TEXT NOT NULL CHECK (strategy_version ~ '^v[1-9][0-9]*$'),
    doc_path         TEXT NOT NULL CHECK (btrim(doc_path) <> ''),
    doc              JSONB NOT NULL,
    doc_sha256       TEXT NOT NULL CHECK (doc_sha256 ~ '^[0-9a-f]{64}$'),
    frozen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ai_trial_declarations_columns_match_doc
        CHECK ((doc ->> 'strategy_id') IS NOT DISTINCT FROM strategy_id
               AND (doc ->> 'strategy_version') IS NOT DISTINCT FROM strategy_version),
    -- O14: each strategy version is its own register entry and its own declaration.
    CONSTRAINT ai_trial_declarations_one_per_version UNIQUE (strategy_id, strategy_version)
);

COMMENT ON TABLE ai_trial_declarations IS
    '#3471 AI-trial declarations, append-only: the frozen document of one #2599 declaration '
    '(contract_version ai-trial-declaration-v1:<doc_sha256>).';

CREATE OR REPLACE FUNCTION ai_trial_declarations_bind_declaration()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    declared_id       TEXT;
    declared_version  TEXT;
    declared_contract TEXT;
BEGIN
    SELECT strategy_id, strategy_version, contract_version
      INTO declared_id, declared_version, declared_contract
    FROM strategy_preregistration_declarations WHERE declaration_id = NEW.declaration_id;
    IF declared_id IS DISTINCT FROM NEW.strategy_id OR declared_version IS DISTINCT FROM NEW.strategy_version THEN
        RAISE EXCEPTION 'declaration % is %/%, not %/%',
            NEW.declaration_id, declared_id, declared_version, NEW.strategy_id, NEW.strategy_version;
    END IF;
    IF declared_contract IS DISTINCT FROM 'ai-trial-declaration-v1:' || NEW.doc_sha256 THEN
        RAISE EXCEPTION 'declaration % contract % does not name document %',
            NEW.declaration_id, declared_contract, NEW.doc_sha256;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_declarations_bind ON ai_trial_declarations;
CREATE TRIGGER trg_ai_trial_declarations_bind
BEFORE INSERT ON ai_trial_declarations
FOR EACH ROW EXECUTE FUNCTION ai_trial_declarations_bind_declaration();

DROP TRIGGER IF EXISTS trg_ai_trial_declarations_append_only ON ai_trial_declarations;
CREATE TRIGGER trg_ai_trial_declarations_append_only
BEFORE UPDATE OR DELETE ON ai_trial_declarations
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();


-- ---------------------------------------------------------------------------
-- 2. State events (§9): the trial's state is its latest event's `to_state`
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_state_events (
    event_id       BIGSERIAL PRIMARY KEY,
    declaration_id BIGINT NOT NULL REFERENCES ai_trial_declarations (declaration_id) ON DELETE RESTRICT,
    -- NULL only on the genesis event. No event at all means NOT active: the loader fails closed.
    from_state     TEXT CHECK (from_state IN
                       ('active', 'halted_harm', 'halted_loss', 'halted_mandate', 'halted_operator')),
    to_state       TEXT NOT NULL CHECK (to_state IN
                       ('active', 'halted_harm', 'halted_loss', 'halted_mandate', 'halted_operator')),
    -- `halted_mandate:<reason>` in the spec is to_state + this column.
    reason         TEXT NOT NULL CHECK (btrim(reason) <> ''),
    actor          TEXT NOT NULL CHECK (actor IN ('engine', 'supervisor', 'operator')),
    at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ai_trial_state_events_declaration
    ON ai_trial_state_events (declaration_id, event_id);

-- §9 legal transitions. `from_state` must equal the CURRENT state (optimistic concurrency):
-- the declaration row lock serialises writers, and under READ COMMITTED each statement below
-- takes a fresh snapshot, so a writer that waited sees the winner's event and is refused.
CREATE OR REPLACE FUNCTION ai_trial_state_events_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    current_state TEXT;
BEGIN
    PERFORM 1 FROM ai_trial_declarations WHERE declaration_id = NEW.declaration_id FOR NO KEY UPDATE;
    SELECT to_state INTO current_state
    FROM ai_trial_state_events WHERE declaration_id = NEW.declaration_id
    ORDER BY event_id DESC LIMIT 1;

    IF NEW.from_state IS DISTINCT FROM current_state THEN
        RAISE EXCEPTION 'stale transition: trial % is in state %, not %',
            NEW.declaration_id, coalesce(current_state, '<none>'), coalesce(NEW.from_state, '<none>');
    END IF;
    IF current_state IS NULL THEN
        IF NEW.to_state <> 'active' THEN
            RAISE EXCEPTION 'the genesis event must be <none> -> active, not -> %', NEW.to_state;
        END IF;
    ELSIF current_state = 'active' THEN
        IF NEW.to_state = 'active' THEN
            RAISE EXCEPTION 'illegal transition active -> %', NEW.to_state;
        END IF;
    ELSIF current_state IN ('halted_mandate', 'halted_operator') THEN
        IF NEW.to_state <> 'active' THEN
            RAISE EXCEPTION 'illegal transition % -> %', current_state, NEW.to_state;
        END IF;
        IF NEW.actor <> 'supervisor' THEN
            RAISE EXCEPTION 'resuming from % is a supervisor action, not a % one', current_state, NEW.actor;
        END IF;
    ELSE
        RAISE EXCEPTION '% is terminal', current_state;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_state_events_transition ON ai_trial_state_events;
CREATE TRIGGER trg_ai_trial_state_events_transition
BEFORE INSERT ON ai_trial_state_events
FOR EACH ROW EXECUTE FUNCTION ai_trial_state_events_transition();

DROP TRIGGER IF EXISTS trg_ai_trial_state_events_append_only ON ai_trial_state_events;
CREATE TRIGGER trg_ai_trial_state_events_append_only
BEFORE UPDATE OR DELETE ON ai_trial_state_events
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();


-- ---------------------------------------------------------------------------
-- 3. Runs (§3, §4, §11): one claim per target session, one leased transition
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_runs (
    run_id                  BIGSERIAL PRIMARY KEY,
    declaration_id          BIGINT NOT NULL REFERENCES ai_trial_declarations (declaration_id) ON DELETE RESTRICT,
    -- §3: the TARGET NYSE session. Keys uniqueness, the §7 draw, execution and expiry.
    session_date            DATE NOT NULL,
    -- §3.2: the claim time IS the pack's knowledge cutoff.
    as_of                   TIMESTAMPTZ NOT NULL,
    lease_until             TIMESTAMPTZ NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed', 'decided', 'refused')),
    -- Set by the transition trigger to the transition's transaction timestamp.
    decided_at              TIMESTAMPTZ,
    -- §3/§4/§6/§8 codes; a `<code>:<detail>` form carries e.g. trial_capacity_unavailable:<reason>.
    refusal_reason          TEXT CHECK (refusal_reason ~ '^[a-z][a-z_]*(:[a-z0-9_.-]+)?$'),
    -- The scores run (§3.1): scores has no run id, a run IS (model_version, scored_at).
    scores_model_version    TEXT,
    scores_scored_at        TIMESTAMPTZ,
    pack                    JSONB,
    pack_sha256             TEXT CHECK (pack_sha256 ~ '^[0-9a-f]{64}$'),
    rendered_prompt         TEXT,
    rendered_prompt_sha256  TEXT CHECK (rendered_prompt_sha256 ~ '^[0-9a-f]{64}$'),
    system_prompt_sha256    TEXT CHECK (system_prompt_sha256 ~ '^[0-9a-f]{64}$'),
    prompt_template_sha256  TEXT CHECK (prompt_template_sha256 ~ '^[0-9a-f]{64}$'),
    model_id                TEXT CHECK (btrim(model_id) <> ''),
    argv_sha256             TEXT CHECK (argv_sha256 ~ '^[0-9a-f]{64}$'),
    executable_path         TEXT CHECK (executable_path LIKE '/%'),
    cli_version             TEXT CHECK (btrim(cli_version) <> ''),
    git_sha                 TEXT CHECK (git_sha ~ '^[0-9a-f]{40}$'),
    policy_hash             TEXT CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    init_event              JSONB,
    exit_code               INTEGER,
    stdout                  TEXT,
    stderr                  TEXT,
    structured_output       JSONB,
    num_turns               INTEGER CHECK (num_turns >= 0),
    cost_usd                NUMERIC CHECK (cost_usd >= 0),
    duration_ms             BIGINT CHECK (duration_ms >= 0),

    CONSTRAINT ai_trial_runs_one_per_session UNIQUE (declaration_id, session_date),
    -- §11: claim time + 15 minutes, by construction.
    CONSTRAINT ai_trial_runs_lease_is_fifteen_minutes
        CHECK (lease_until = as_of + interval '15 minutes'),
    -- O4: stdout and stderr share one 2 MB cap.
    CONSTRAINT ai_trial_runs_output_capped
        CHECK (coalesce(octet_length(stdout), 0) + coalesce(octet_length(stderr), 0) <= 2097152),
    -- ⚠ Every CHECK here is written so a NULL cannot make it UNKNOWN (which Postgres accepts).
    CONSTRAINT ai_trial_runs_prompt_sha_matches
        CHECK (rendered_prompt IS NULL AND rendered_prompt_sha256 IS NULL
               OR rendered_prompt_sha256 IS NOT DISTINCT FROM
                  encode(sha256(convert_to(rendered_prompt, 'UTF8')), 'hex')),
    CONSTRAINT ai_trial_runs_claimed_is_bare
        CHECK (status <> 'claimed' OR (decided_at IS NULL AND refusal_reason IS NULL
                                       AND pack IS NULL AND structured_output IS NULL)),
    CONSTRAINT ai_trial_runs_refused_has_reason
        CHECK (status <> 'refused' OR (refusal_reason IS NOT NULL AND decided_at IS NOT NULL)),
    -- A decided run carries its whole provenance; a refused one carries what it reached.
    CONSTRAINT ai_trial_runs_decided_is_complete
        CHECK (status <> 'decided' OR (
            refusal_reason IS NULL AND decided_at IS NOT NULL
            AND scores_model_version IS NOT NULL AND scores_scored_at IS NOT NULL
            AND pack IS NOT NULL AND pack_sha256 IS NOT NULL
            AND rendered_prompt IS NOT NULL AND rendered_prompt_sha256 IS NOT NULL
            AND system_prompt_sha256 IS NOT NULL AND prompt_template_sha256 IS NOT NULL
            AND model_id IS NOT NULL AND argv_sha256 IS NOT NULL
            AND executable_path IS NOT NULL AND cli_version IS NOT NULL
            AND git_sha IS NOT NULL AND policy_hash IS NOT NULL
            AND init_event IS NOT NULL AND exit_code IS NOT DISTINCT FROM 0
            AND structured_output IS NOT NULL
        ))
);

COMMENT ON TABLE ai_trial_runs IS
    '#3471 AI-trial runs: one claim per (declaration, target session); append-only except the '
    'single leased transition claimed -> decided | refused.';

-- A claim may be backdated (that only shortens its lease) but never post-dated.
CREATE OR REPLACE FUNCTION ai_trial_runs_claim()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status <> 'claimed' THEN
        RAISE EXCEPTION 'a run is inserted as claimed, not %', NEW.status;
    END IF;
    IF NEW.as_of > clock_timestamp() THEN
        RAISE EXCEPTION 'claim time % is in the future', NEW.as_of;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_runs_claim ON ai_trial_runs;
CREATE TRIGGER trg_ai_trial_runs_claim
BEFORE INSERT ON ai_trial_runs
FOR EACH ROW EXECUTE FUNCTION ai_trial_runs_claim();

-- The one legal UPDATE. After the lease only `refused / stale_claim` is allowed, so a worker
-- that finishes late cannot publish decisions. The lease is read on `clock_timestamp()`, not
-- the transaction's start, so a long-open transaction cannot carry an expired lease forward.
CREATE OR REPLACE FUNCTION ai_trial_runs_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'ai_trial_runs is append-only (#3471): a run, once claimed, stays recorded';
    END IF;
    IF OLD.status <> 'claimed' THEN
        RAISE EXCEPTION 'run % is already %; its record is final', OLD.run_id, OLD.status;
    END IF;
    IF NEW.status NOT IN ('decided', 'refused') THEN
        RAISE EXCEPTION 'a claimed run moves to decided or refused, not %', NEW.status;
    END IF;
    IF NEW.run_id <> OLD.run_id OR NEW.declaration_id <> OLD.declaration_id
       OR NEW.session_date <> OLD.session_date OR NEW.as_of <> OLD.as_of
       OR NEW.lease_until <> OLD.lease_until THEN
        RAISE EXCEPTION 'run % identity, session and lease are immutable', OLD.run_id;
    END IF;
    IF clock_timestamp() > OLD.lease_until
       AND (NEW.status <> 'refused' OR NEW.refusal_reason IS DISTINCT FROM 'stale_claim') THEN
        RAISE EXCEPTION 'run % lease expired at %; only refused/stale_claim is allowed',
            OLD.run_id, OLD.lease_until;
    END IF;
    NEW.decided_at := now();
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_runs_transition ON ai_trial_runs;
CREATE TRIGGER trg_ai_trial_runs_transition
BEFORE UPDATE OR DELETE ON ai_trial_runs
FOR EACH ROW EXECUTE FUNCTION ai_trial_runs_transition();


-- ---------------------------------------------------------------------------
-- 4. Decisions (§5, §6): one row per model decision, refused ones included
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_decisions (
    decision_id       BIGSERIAL PRIMARY KEY,
    run_id            BIGINT NOT NULL REFERENCES ai_trial_runs (run_id) ON DELETE RESTRICT,
    -- 0-based index in the model's `decisions` array (§6).
    response_position INTEGER NOT NULL CHECK (response_position >= 0),
    -- §5 fields. A schema failure refuses the WHOLE response (no rows), so every row here
    -- passed the frozen bounds and the CHECKs restate them.
    action            TEXT NOT NULL CHECK (action = 'enter_long'),
    symbol            TEXT NOT NULL CHECK (char_length(symbol) <= 16),
    stop_pct          DOUBLE PRECISION NOT NULL CHECK (stop_pct BETWEEN 2 AND 25),
    target_pct        DOUBLE PRECISION NOT NULL CHECK (target_pct BETWEEN 2 AND 100),
    horizon_days      INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20)),
    size_tier         TEXT NOT NULL CHECK (size_tier IN ('half', 'full')),
    confidence        INTEGER NOT NULL CHECK (confidence BETWEEN 1 AND 5),
    thesis            TEXT NOT NULL CHECK (char_length(thesis) BETWEEN 1 AND 600),
    -- NULL when the symbol is not a shortlist name.
    instrument_id     BIGINT REFERENCES instruments (instrument_id) ON DELETE RESTRICT,
    verdict           TEXT NOT NULL CHECK (verdict IN ('accepted', 'refused')),
    reason_code       TEXT CHECK (reason_code IN (
                          'not_in_shortlist', 'duplicate_symbol', 'already_held',
                          'target_not_above_stop', 'thesis_too_long', 'control_pool_exhausted')),

    CONSTRAINT ai_trial_decisions_position_unique UNIQUE (run_id, response_position),
    CONSTRAINT ai_trial_decisions_verdict_matches_reason
        CHECK ((verdict = 'accepted') = (reason_code IS NULL)),
    CONSTRAINT ai_trial_decisions_accepted_has_instrument
        CHECK (verdict <> 'accepted' OR instrument_id IS NOT NULL)
);

-- §11 atomicity: a decision is written in the SAME transaction as its run's `decided`
-- transition. `decided_at` is that transaction's `now()`, which no later transaction shares.
CREATE OR REPLACE FUNCTION ai_trial_decisions_bind_run()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_status  TEXT;
    run_decided TIMESTAMPTZ;
BEGIN
    SELECT status, decided_at INTO run_status, run_decided FROM ai_trial_runs WHERE run_id = NEW.run_id;
    IF run_status IS DISTINCT FROM 'decided' OR run_decided IS DISTINCT FROM now() THEN
        RAISE EXCEPTION 'run % was not decided in this transaction; its decisions are already published',
            NEW.run_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_decisions_bind_run ON ai_trial_decisions;
CREATE TRIGGER trg_ai_trial_decisions_bind_run
BEFORE INSERT ON ai_trial_decisions
FOR EACH ROW EXECUTE FUNCTION ai_trial_decisions_bind_run();

DROP TRIGGER IF EXISTS trg_ai_trial_decisions_append_only ON ai_trial_decisions;
CREATE TRIGGER trg_ai_trial_decisions_append_only
BEFORE UPDATE OR DELETE ON ai_trial_decisions
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();


-- ---------------------------------------------------------------------------
-- 5. Pairs (§7): one per accepted arm decision; the draw is recomputed here
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_pairs (
    pair_id               BIGSERIAL PRIMARY KEY,
    declaration_id        BIGINT NOT NULL REFERENCES ai_trial_declarations (declaration_id) ON DELETE RESTRICT,
    -- Trial-global, dense, 0-based; assigned only after the draw succeeds (§7).
    pair_seq              INTEGER NOT NULL CHECK (pair_seq >= 0),
    arm_decision_id       BIGINT NOT NULL UNIQUE REFERENCES ai_trial_decisions (decision_id) ON DELETE RESTRICT,
    control_instrument_id BIGINT NOT NULL REFERENCES instruments (instrument_id) ON DELETE RESTRICT,
    seed_material         TEXT NOT NULL,
    -- Ordered by instrument_id ascending (§7).
    pool                  BIGINT[] NOT NULL
                          CHECK (cardinality(pool) >= 1 AND array_position(pool, NULL) IS NULL),
    draw_idx              INTEGER NOT NULL CHECK (draw_idx >= 0),
    -- Copied from the arm decision (§7 shared terms).
    stop_pct              DOUBLE PRECISION NOT NULL,
    target_pct            DOUBLE PRECISION NOT NULL,
    horizon_days          INTEGER NOT NULL,
    size_tier             TEXT NOT NULL,

    CONSTRAINT ai_trial_pairs_seq_unique UNIQUE (declaration_id, pair_seq)
);

-- `int.from_bytes(sha256(m).digest(), "big") mod n`, exactly: Horner's rule over the 32
-- digest bytes, reducing mod n each step (bigint cannot hold 256 bits; the residue can).
CREATE OR REPLACE FUNCTION ai_trial_draw_index(seed_material TEXT, n INTEGER)
RETURNS INTEGER LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    digest BYTEA := sha256(convert_to(seed_material, 'UTF8'));
    acc    BIGINT := 0;
BEGIN
    IF n < 1 THEN
        RAISE EXCEPTION 'draw over an empty pool';
    END IF;
    FOR i IN 0 .. length(digest) - 1 LOOP
        acc := (acc * 256 + get_byte(digest, i)) % n;
    END LOOP;
    RETURN acc::INTEGER;
END $$;

CREATE OR REPLACE FUNCTION ai_trial_pairs_verify()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    d          ai_trial_decisions%ROWTYPE;
    r          ai_trial_runs%ROWTYPE;
    decl_sha   TEXT;
    next_seq   INTEGER;
BEGIN
    SELECT * INTO d FROM ai_trial_decisions WHERE decision_id = NEW.arm_decision_id;
    SELECT * INTO r FROM ai_trial_runs WHERE run_id = d.run_id;
    IF d.verdict IS DISTINCT FROM 'accepted' THEN
        RAISE EXCEPTION 'decision % is not accepted; a refused decision has no pair', NEW.arm_decision_id;
    END IF;
    IF r.status IS DISTINCT FROM 'decided' OR r.decided_at IS DISTINCT FROM now() THEN
        RAISE EXCEPTION 'run % was not decided in this transaction; its pairs are already published', r.run_id;
    END IF;
    IF NEW.declaration_id <> r.declaration_id THEN
        RAISE EXCEPTION 'pair declaration % is not its run''s (%)', NEW.declaration_id, r.declaration_id;
    END IF;

    -- Dense by this check, so max + 1 is the count, and reads the unique index's last entry.
    SELECT coalesce(max(pair_seq) + 1, 0) INTO next_seq FROM ai_trial_pairs WHERE declaration_id = NEW.declaration_id;
    IF NEW.pair_seq <> next_seq THEN
        RAISE EXCEPTION 'pair_seq % is not the next in sequence (%)', NEW.pair_seq, next_seq;
    END IF;

    IF NEW.stop_pct <> d.stop_pct OR NEW.target_pct <> d.target_pct
       OR NEW.horizon_days <> d.horizon_days OR NEW.size_tier <> d.size_tier THEN
        RAISE EXCEPTION 'pair terms differ from arm decision %', d.decision_id;
    END IF;

    -- The pool: strictly ascending (ordered, no duplicates), and without replacement within
    -- the run — no earlier control of this run may be in it.
    IF EXISTS (
        SELECT 1 FROM unnest(NEW.pool) WITH ORDINALITY AS p (id, k)
        WHERE k > 1 AND id <= NEW.pool[k - 1]
    ) THEN
        RAISE EXCEPTION 'pool is not strictly ascending by instrument_id';
    END IF;
    IF EXISTS (
        SELECT 1 FROM ai_trial_pairs p JOIN ai_trial_decisions pd ON pd.decision_id = p.arm_decision_id
        WHERE pd.run_id = d.run_id AND p.control_instrument_id = ANY (NEW.pool)
    ) THEN
        RAISE EXCEPTION 'pool contains a name already drawn this run';
    END IF;

    SELECT doc_sha256 INTO decl_sha FROM ai_trial_declarations WHERE declaration_id = NEW.declaration_id;
    IF NEW.seed_material <> decl_sha || '|' || to_char(r.session_date, 'YYYY-MM-DD') || '|' || NEW.pair_seq THEN
        RAISE EXCEPTION 'seed material % is not the §7 material for this pair', NEW.seed_material;
    END IF;
    IF NEW.draw_idx <> ai_trial_draw_index(NEW.seed_material, cardinality(NEW.pool))
       OR NEW.control_instrument_id IS DISTINCT FROM NEW.pool[NEW.draw_idx + 1] THEN
        RAISE EXCEPTION 'control % is not the draw''s output', NEW.control_instrument_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_pairs_verify ON ai_trial_pairs;
CREATE TRIGGER trg_ai_trial_pairs_verify
BEFORE INSERT ON ai_trial_pairs
FOR EACH ROW EXECUTE FUNCTION ai_trial_pairs_verify();

DROP TRIGGER IF EXISTS trg_ai_trial_pairs_append_only ON ai_trial_pairs;
CREATE TRIGGER trg_ai_trial_pairs_append_only
BEFORE UPDATE OR DELETE ON ai_trial_pairs
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();

-- §7: every accepted arm entry has a control. Checked at COMMIT, because the pair can only
-- be inserted after its decision.
CREATE OR REPLACE FUNCTION ai_trial_decisions_require_pair()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.verdict = 'accepted'
       AND NOT EXISTS (SELECT 1 FROM ai_trial_pairs WHERE arm_decision_id = NEW.decision_id) THEN
        RAISE EXCEPTION 'accepted decision % has no pair; an unpaired entry is refused control_pool_exhausted',
            NEW.decision_id;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_decisions_require_pair ON ai_trial_decisions;
CREATE CONSTRAINT TRIGGER trg_ai_trial_decisions_require_pair
AFTER INSERT ON ai_trial_decisions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION ai_trial_decisions_require_pair();


-- ---------------------------------------------------------------------------
-- 6. Leg links (§11): signal and trade ids, each written once when known
-- ---------------------------------------------------------------------------
-- The instrument a leg trades: the arm decision's, or the drawn control.
CREATE OR REPLACE FUNCTION ai_trial_leg_instrument(p_pair_id BIGINT, p_leg TEXT)
RETURNS BIGINT LANGUAGE sql STABLE AS $$
    SELECT CASE p_leg WHEN 'arm' THEN d.instrument_id ELSE p.control_instrument_id END
    FROM ai_trial_pairs p JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id
    WHERE p.pair_id = p_pair_id
$$;

CREATE TABLE IF NOT EXISTS ai_trial_leg_links (
    pair_id    BIGINT NOT NULL REFERENCES ai_trial_pairs (pair_id) ON DELETE RESTRICT,
    leg        TEXT NOT NULL CHECK (leg IN ('arm', 'control')),
    signal_id  BIGINT NOT NULL UNIQUE REFERENCES strategy_signals (signal_id) ON DELETE RESTRICT,
    linked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, leg)
);

CREATE TABLE IF NOT EXISTS ai_trial_trade_links (
    pair_id           BIGINT NOT NULL REFERENCES ai_trial_pairs (pair_id) ON DELETE RESTRICT,
    leg               TEXT NOT NULL CHECK (leg IN ('arm', 'control')),
    strategy_trade_id BIGINT NOT NULL UNIQUE REFERENCES strategy_trades (strategy_trade_id) ON DELETE RESTRICT,
    linked_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, leg),
    FOREIGN KEY (pair_id, leg) REFERENCES ai_trial_leg_links (pair_id, leg) ON DELETE RESTRICT
);

-- §8 "two legs, two identities": the arm trades as the declaration's strategy id, the control
-- as that id + `-control`, each on its own leg's instrument.
CREATE OR REPLACE FUNCTION ai_trial_leg_links_verify()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    want_strategy TEXT;
    want_version  TEXT;
    want_session  DATE;
    sig           strategy_signals%ROWTYPE;
BEGIN
    SELECT dcl.strategy_id || CASE NEW.leg WHEN 'control' THEN '-control' ELSE '' END,
           dcl.strategy_version, r.session_date
      INTO want_strategy, want_version, want_session
    FROM ai_trial_pairs p
    JOIN ai_trial_declarations dcl ON dcl.declaration_id = p.declaration_id
    JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id
    JOIN ai_trial_runs r ON r.run_id = d.run_id
    WHERE p.pair_id = NEW.pair_id;
    SELECT * INTO sig FROM strategy_signals WHERE signal_id = NEW.signal_id;
    IF sig.strategy_id IS DISTINCT FROM want_strategy OR sig.strategy_version IS DISTINCT FROM want_version THEN
        RAISE EXCEPTION 'signal % is %/%, not the % leg''s %/%',
            NEW.signal_id, sig.strategy_id, sig.strategy_version, NEW.leg, want_strategy, want_version;
    END IF;
    IF sig.instrument_id IS DISTINCT FROM ai_trial_leg_instrument(NEW.pair_id, NEW.leg) THEN
        RAISE EXCEPTION 'signal % instrument % is not the % leg''s', NEW.signal_id, sig.instrument_id, NEW.leg;
    END IF;
    -- A fired entry that fills in the pair's target session (§3 session identity).
    IF sig.signal_kind IS DISTINCT FROM 'entry' OR sig.verdict IS DISTINCT FROM 'fired'
       OR sig.fill_bar_date IS DISTINCT FROM want_session THEN
        RAISE EXCEPTION 'signal % is not a fired entry filling in session %', NEW.signal_id, want_session;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_leg_links_verify ON ai_trial_leg_links;
CREATE TRIGGER trg_ai_trial_leg_links_verify
BEFORE INSERT ON ai_trial_leg_links
FOR EACH ROW EXECUTE FUNCTION ai_trial_leg_links_verify();

-- The trade must be the one funded from THIS leg's signal: an instrument match alone cannot
-- tell the legs apart when the control draws the arm's own name.
CREATE OR REPLACE FUNCTION ai_trial_trade_links_verify()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    trade_signal BIGINT;
    leg_signal   BIGINT;
BEGIN
    SELECT fd.signal_id INTO trade_signal
    FROM strategy_trades t JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id
    WHERE t.strategy_trade_id = NEW.strategy_trade_id;
    SELECT signal_id INTO leg_signal FROM ai_trial_leg_links WHERE pair_id = NEW.pair_id AND leg = NEW.leg;
    IF trade_signal IS DISTINCT FROM leg_signal THEN
        RAISE EXCEPTION 'trade % was funded from signal %, not the % leg''s %',
            NEW.strategy_trade_id, trade_signal, NEW.leg, leg_signal;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_trade_links_verify ON ai_trial_trade_links;
CREATE TRIGGER trg_ai_trial_trade_links_verify
BEFORE INSERT ON ai_trial_trade_links
FOR EACH ROW EXECUTE FUNCTION ai_trial_trade_links_verify();

DROP TRIGGER IF EXISTS trg_ai_trial_leg_links_append_only ON ai_trial_leg_links;
CREATE TRIGGER trg_ai_trial_leg_links_append_only
BEFORE UPDATE OR DELETE ON ai_trial_leg_links
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();

DROP TRIGGER IF EXISTS trg_ai_trial_trade_links_append_only ON ai_trial_trade_links;
CREATE TRIGGER trg_ai_trial_trade_links_append_only
BEFORE UPDATE OR DELETE ON ai_trial_trade_links
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();


-- ---------------------------------------------------------------------------
-- 7. Pair events (§11, O11): per-leg lifecycle plus the pair-level `broken`
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_pair_events (
    event_id  BIGSERIAL PRIMARY KEY,
    pair_id   BIGINT NOT NULL REFERENCES ai_trial_pairs (pair_id) ON DELETE RESTRICT,
    -- NULL exactly for `broken`, which is a property of the pair, not a leg.
    leg       TEXT CHECK (leg IN ('arm', 'control')),
    event     TEXT NOT NULL CHECK (event IN ('submitted', 'uncertain', 'filled', 'closed', 'censored', 'broken')),
    -- `broken:<reasons>`: both legs' reason codes, e.g. {late_fill} or {unresolved}.
    reasons   TEXT[],
    at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ai_trial_pair_events_leg_iff_not_broken CHECK ((event = 'broken') = (leg IS NULL)),
    CONSTRAINT ai_trial_pair_events_reasons_iff_broken CHECK (
        (event = 'broken') = (reasons IS NOT NULL)
        AND (reasons IS NULL OR (cardinality(reasons) >= 1 AND array_position(reasons, NULL) IS NULL))
    )
);

CREATE INDEX IF NOT EXISTS ai_trial_pair_events_pair ON ai_trial_pair_events (pair_id, event_id);

-- Legal order, per leg:  submitted → [uncertain →] filled → [censored →] closed.
-- `broken` is once per pair and does not stop leg events: a leg that fills late is still
-- managed to its exits and reported (O11).
CREATE OR REPLACE FUNCTION ai_trial_pair_events_order()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    last_event TEXT;
    reason     TEXT;
BEGIN
    PERFORM 1 FROM ai_trial_pairs WHERE pair_id = NEW.pair_id FOR NO KEY UPDATE;
    IF NEW.event = 'broken' THEN
        -- Per element: a regex over the joined array cannot tell {'a,b'} from {'a','b'}.
        FOREACH reason IN ARRAY NEW.reasons LOOP
            IF reason !~ '^[a-z][a-z0-9_]*$' THEN
                RAISE EXCEPTION 'broken reason % is not a reason code', reason;
            END IF;
        END LOOP;
        IF EXISTS (SELECT 1 FROM ai_trial_pair_events WHERE pair_id = NEW.pair_id AND event = 'broken') THEN
            RAISE EXCEPTION 'pair % is already broken', NEW.pair_id;
        END IF;
        RETURN NEW;
    END IF;

    SELECT event INTO last_event FROM ai_trial_pair_events
    WHERE pair_id = NEW.pair_id AND leg = NEW.leg
    ORDER BY event_id DESC LIMIT 1;
    -- ⚠ Not NULL: `NULL IN (...)` is NULL, `NOT NULL` is NULL, and IF treats NULL as false —
    -- a first event other than `submitted` would pass the check below silently.
    last_event := coalesce(last_event, '<none>');

    IF NOT (
        (NEW.event = 'submitted' AND last_event = '<none>')
        OR (NEW.event = 'uncertain' AND last_event = 'submitted')
        OR (NEW.event = 'filled' AND last_event IN ('submitted', 'uncertain'))
        OR (NEW.event = 'censored' AND last_event = 'filled')
        OR (NEW.event = 'closed' AND last_event IN ('filled', 'censored'))
    ) THEN
        RAISE EXCEPTION 'illegal % leg event % after %', NEW.leg, NEW.event, last_event;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_pair_events_order ON ai_trial_pair_events;
CREATE TRIGGER trg_ai_trial_pair_events_order
BEFORE INSERT ON ai_trial_pair_events
FOR EACH ROW EXECUTE FUNCTION ai_trial_pair_events_order();

DROP TRIGGER IF EXISTS trg_ai_trial_pair_events_append_only ON ai_trial_pair_events;
CREATE TRIGGER trg_ai_trial_pair_events_append_only
BEFORE UPDATE OR DELETE ON ai_trial_pair_events
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();


-- ---------------------------------------------------------------------------
-- 8. Pair labels (§9): the arm leg's entry-session regime label, written once at its fill
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_trial_pair_labels (
    pair_id            BIGINT PRIMARY KEY REFERENCES ai_trial_pairs (pair_id) ON DELETE RESTRICT,
    entry_session      DATE NOT NULL,
    regime_label       TEXT NOT NULL CHECK (btrim(regime_label) <> ''),
    classifier_version TEXT NOT NULL CHECK (btrim(classifier_version) <> ''),
    labelled_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ai_trial_pair_labels_require_fill()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM ai_trial_pair_events WHERE pair_id = NEW.pair_id AND leg = 'arm' AND event = 'filled'
    ) THEN
        RAISE EXCEPTION 'pair % arm leg has not filled; its label is written at the fill', NEW.pair_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_pair_labels_require_fill ON ai_trial_pair_labels;
CREATE TRIGGER trg_ai_trial_pair_labels_require_fill
BEFORE INSERT ON ai_trial_pair_labels
FOR EACH ROW EXECUTE FUNCTION ai_trial_pair_labels_require_fill();

DROP TRIGGER IF EXISTS trg_ai_trial_pair_labels_append_only ON ai_trial_pair_labels;
CREATE TRIGGER trg_ai_trial_pair_labels_append_only
BEFORE UPDATE OR DELETE ON ai_trial_pair_labels
FOR EACH ROW EXECUTE FUNCTION prevent_ai_trial_mutation();
