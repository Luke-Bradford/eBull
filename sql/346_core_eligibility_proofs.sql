-- 346_core_eligibility_proofs.sql
--
-- #2603 item 2.  The account-specific proof that a candidate core instrument is
-- the UNDERLYING product and not a CFD.
--
-- Source rule: eToro's own documented `settlementType` vocabulary
-- (POST /api/v2/trading/info/{demo|real}/eligibility, live portal 2026-08-13) --
--   real         "the real instrument held in full value"
--   realFutures  "the real future contract, which is a derivative ..."
--   marginTrade  "the real instrument held with only a portion of its value ..."
--   cfd          "contract for difference, which is a derivative ..."
-- Exactly one of those is ownership at full value.  `marginTrade` IS the real
-- instrument but is leveraged, which the standing no-leverage posture bars.
--
-- Why a proof and not an attribute on `instruments`: SPY (3000) and SPY.RTH
-- (3417) are the SAME fund -- identical company_name and maxUnitsPerOrder -- and
-- only SPY.RTH carries a `real`/`long`/x1 arm.  Nothing stored about the
-- instrument separates them, and eligibility is per-account regulatory state
-- that changes without notice, so the answer belongs to an observation.
--
-- APPEND-ONLY.  One row per observation, never updated.  A failing observation
-- is stored exactly as a passing one is: an observation is evidence.
--
-- ⚠ This is a WRITE-TIME gate for `configure_core_mandate` and NOTHING ELSE.  An
-- enabled mandate stays enabled after its proof ages out.  It must never be
-- cited as an execution control; item 3 re-proves at execution time.
--
-- Spec: docs/proposals/ta/2026-08-13-core-eligibility-proof.md

CREATE TABLE IF NOT EXISTS strategy_core_eligibility_proofs (
    core_eligibility_proof_id    BIGSERIAL PRIMARY KEY,
    instrument_id                BIGINT NOT NULL
                                 REFERENCES instruments(instrument_id) ON DELETE RESTRICT,
    -- Account identity.  NOT a single broker_credentials row: an eToro account is
    -- two rows (api_key + user_key), so only the triple names it.
    operator_id                  UUID NOT NULL
                                 REFERENCES operators(operator_id) ON DELETE CASCADE,
    provider                     TEXT NOT NULL CHECK (provider = 'etoro'),
    environment                  TEXT NOT NULL CHECK (environment IN ('demo', 'real')),
    -- The live pair actually used.  The triple above survives a credential swap,
    -- so without these a DIFFERENT eToro account swapped in under the same
    -- operator/environment would silently inherit these proofs.
    api_key_credential_id        UUID NOT NULL
                                 REFERENCES broker_credentials(id) ON DELETE RESTRICT,
    user_key_credential_id       UUID NOT NULL
                                 REFERENCES broker_credentials(id) ON DELETE RESTRICT,
    -- DEFAULT now() and never a parameter: a caller that supplies its own
    -- observation time can extend a proof's validity at will.
    observed_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    verdict                      TEXT NOT NULL
                                 CHECK (verdict IN ('underlying', 'not_underlying', 'unresolved')),
    reason_code                  TEXT CHECK (reason_code IN (
                                     'instrument_not_resolved',
                                     'eligibility_row_ambiguous',
                                     'eligibility_currency_mismatch',
                                     'eligibility_arm_ambiguous',
                                     'instrument_not_open',
                                     'no_underlying_arm'
                                 )),
    requested_currency           TEXT NOT NULL CHECK (requested_currency = 'USD'),
    -- Stored verbatim: the demo response answers 'usd', so comparison is
    -- case-insensitive but the observation is not rewritten.
    response_currency            TEXT NOT NULL CHECK (char_length(response_currency) BETWEEN 1 AND 10),
    -- The matched arm.  NULL unless the verdict is `underlying`, because there is
    -- no matched arm otherwise.
    settlement_type              TEXT CHECK (settlement_type = 'real'),
    direction                    TEXT CHECK (direction = 'long'),
    leverage_values              INTEGER[],
    -- Kept so "exactly one qualifying arm" stays checkable after the fact; a
    -- projection of the selected arm alone cannot show it.
    qualifying_arm_count         SMALLINT NOT NULL CHECK (qualifying_arm_count >= 0),
    allow_open_position          BOOLEAN,
    allow_close_position         BOOLEAN,
    allow_partial_close_position BOOLEAN,
    -- Observed sizing facts.  NO effective minimum is derived here: the
    -- `arm.min_position_amount or row.min_position_exposure` precedence the
    -- executor uses has no citation in the provider's documentation, and a
    -- missing floor is an order-sizing gap rather than evidence about what the
    -- product IS.  Item 3 owns sizing and inherits both numbers.
    min_position_amount          NUMERIC(18,6) CHECK (min_position_amount IS NULL OR min_position_amount > 0),
    min_position_exposure        NUMERIC(18,6) CHECK (min_position_exposure IS NULL OR min_position_exposure > 0),
    max_units_per_order          NUMERIC(18,6) CHECK (max_units_per_order IS NULL OR max_units_per_order > 0),
    -- SHA-256 over the WHOLE canonicalised response, not the instrument row: it
    -- has to cover `currency` and `notFoundInstrumentIds` too.  The recorder
    -- requests exactly one instrument, which is what lets one response digest
    -- stand as the whole evidence.  Raw payloads are not persisted (sql/287).
    response_digest              TEXT NOT NULL CHECK (response_digest ~ '^[0-9a-f]{64}$'),
    policy_version               TEXT NOT NULL CHECK (policy_version = 'core-eligibility-v1'),
    recorded_by                  TEXT NOT NULL CHECK (char_length(recorded_by) BETWEEN 1 AND 100),

    -- Both directions: a pass cannot carry a reason and a failure cannot omit one.
    CONSTRAINT core_eligibility_reason_iff_not_underlying
        CHECK ((verdict = 'underlying') = (reason_code IS NULL)),

    -- Storing the projection is not enough -- without this a row can look like a
    -- pass and not be one.
    --
    -- ⚠ `IS NOT DISTINCT FROM`, not `=`.  A CHECK passes on NULL, and
    -- `NULL = 'real'` is NULL rather than false -- so plain equality would admit
    -- an `underlying` row with a NULL settlement_type, which is precisely the
    -- pass-shaped-but-not-a-pass row this constraint exists to refuse.  Same for
    -- `direction`; `allow_open_position IS TRUE` is already NULL-safe, and the
    -- `leverage_values IS NOT NULL` conjunct guards `= ANY` for the same reason.
    CONSTRAINT core_eligibility_underlying_is_complete
        CHECK (
            verdict <> 'underlying'
            OR (
                settlement_type IS NOT DISTINCT FROM 'real'
                AND direction IS NOT DISTINCT FROM 'long'
                AND allow_open_position IS TRUE
                AND qualifying_arm_count = 1
                AND leverage_values IS NOT NULL
                AND 1 = ANY (leverage_values)
                AND upper(response_currency) = requested_currency
            )
        ),

    -- A failing row must not carry pass-shaped evidence.
    CONSTRAINT core_eligibility_failure_carries_no_arm
        CHECK (
            verdict = 'underlying'
            OR (settlement_type IS NULL AND direction IS NULL AND leverage_values IS NULL)
        )
);

CREATE INDEX IF NOT EXISTS idx_core_eligibility_proofs_latest
    ON strategy_core_eligibility_proofs
       (instrument_id, operator_id, provider, environment, observed_at DESC);

COMMENT ON TABLE strategy_core_eligibility_proofs IS
    'Append-only account-specific observations of whether an instrument is offered as the '
    'UNDERLYING product (#2603 item 2). Evidence of an observation, not an attestation of '
    'one: the credential columns record which keys the recorder used, and nothing can stop '
    'a caller asserting an account it never contacted. Write-time gate only -- an enabled '
    'mandate outlives its proof, so this is never an execution control.';
COMMENT ON COLUMN strategy_core_eligibility_proofs.observed_at IS
    'Our clock, not the broker''s -- the eligibility response documents no lastUpdated/asOf '
    'field (verified on the live portal 2026-08-13). DB-generated so a caller cannot extend '
    'a proof''s validity by declaring its age.';
COMMENT ON COLUMN strategy_core_eligibility_proofs.verdict IS
    '`unresolved` = the response did not answer the question; `not_underlying` = it answered '
    'and the answer is no. Only the second is a fact about the instrument.';
COMMENT ON COLUMN strategy_core_eligibility_proofs.response_digest IS
    'SHA-256 of json.dumps(raw, sort_keys=True, separators=(",",":"), ensure_ascii=False, '
    'allow_nan=False). Sorted keys make it immune to field REORDERING but not to field '
    'ADDITION -- deliberately, because an added field is drift this provider has shipped '
    'before (documented `amount` -> undocumented `value`) and re-proving costs one request. '
    'The canonicalisation is pinned by policy_version; changing it is a new version, never a '
    'redefinition, or digests stop being comparable across the change.';
COMMENT ON COLUMN strategy_core_eligibility_proofs.min_position_amount IS
    'Observed, not decided. No effective minimum is derived here -- see the table body.';
