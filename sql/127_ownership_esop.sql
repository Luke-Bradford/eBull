-- 127_ownership_esop.sql
--
-- Issue #843 — DEF 14A bene-table extension. ESOP / employee benefit
-- plan holdings extracted from DEF 14A Item 12 disclosures.
--
-- Spec: docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md
--
-- ## Why a separate slice
--
-- ESOP plans (Apple Inc 401(k) Plan, Microsoft Profit Sharing Plan,
-- etc.) appear in the DEF 14A bene-ownership table when the plan
-- crosses the 5% disclosure threshold. They land alongside Vanguard
-- Group / BlackRock / officer rows in `def14a_beneficial_holdings`
-- but conceptually they're a distinct ownership category — the plan
-- is a single legal entity holding the issuer's own stock on behalf
-- of employees, NOT an external institutional manager.
--
-- The funds-slice ESOP overlay #961 reads from this table and joins
-- against `ownership_funds_current.fund_filer_cik` on
-- `plan_trustee_cik` to tag fund rows with `esop_plan=true`.
--
-- ## Schema decisions
--
--   * Identity is `(instrument_id, plan_name, period_end,
--     source_document_id)` — multiple plans per issuer (separate
--     401k + ESOP + profit-sharing + foreign-employee plans) each
--     get distinct rows.
--   * `plan_name` is the canonicalised plan name extracted from the
--     bene-table holder_name string by stripping the trustee suffix.
--     Example: holder_name "Apple Inc. 401(k) Plan, c/o Vanguard
--     Fiduciary Trust as Trustee" → plan_name "Apple Inc. 401(k) Plan",
--     plan_trustee_name "Vanguard Fiduciary Trust".
--   * `plan_trustee_cik` is resolved post-extraction via
--     `holder_name_resolver` against `external_identifiers` (best-
--     effort; NULL when the trustee is not a known SEC filer).
--   * `ownership_nature` is locked to 'beneficial' — ESOP plans
--     report beneficial ownership per Rule 13d-3 (the plan trustee
--     has voting + investment power on behalf of beneficiaries).
--   * `source` is locked to 'def14a' — only structured source today.
--     The CHECK widens if/when an alternative ESOP disclosure source
--     lands (10-K Note 14 textual extraction would be a candidate).
--   * `shares NOT NULL CHECK > 0` — every retained row must carry a
--     positive share balance. The ingester's write-side guard
--     enforces; NOT NULL is the schema-level defence.
--   * `percent_of_class` is NUMERIC(8,4) to match the
--     def14a_beneficial_holdings column precision.
--
-- ## Codex round-1 sign-off
--
-- See `.claude/codex-843-r1-review.txt`. ESOP detection regex set is
-- locked in `app/providers/implementations/sec_def14a.py`; this
-- migration does NOT enforce the regex at the SQL layer because the
-- canonical text representation could legitimately omit any of the
-- pattern tokens (e.g. an issuer-specific plan name like "Apple
-- Compensatory Stock Purchase Program") that the parser still tags
-- via section context.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry.

BEGIN;

-- ---------------------------------------------------------------------
-- ownership_esop_observations — append-only fact log
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_esop_observations (
    instrument_id           BIGINT NOT NULL,
    plan_name               TEXT NOT NULL,
    plan_trustee_name       TEXT,
    plan_trustee_cik        TEXT,
    ownership_nature        TEXT NOT NULL CHECK (ownership_nature = 'beneficial'),

    -- Provenance block (uniform across every ownership_*_observations).
    source                  TEXT NOT NULL CHECK (source = 'def14a'),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_field            TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    known_to                TIMESTAMPTZ,
    ingest_run_id           UUID NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Fact payload.
    shares                  NUMERIC(24, 4) NOT NULL CHECK (shares > 0),
    percent_of_class        NUMERIC(8, 4),

    PRIMARY KEY (instrument_id, plan_name, period_end, source_document_id)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_esop_observations IS
    'Immutable per-DEF-14A-filing fact log for ESOP / employee benefit plan holdings (#843). Append-only; rebuild source for ownership_esop_current. Spec: docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md.';

-- Quarterly partitions 2010-2030. Mirrors the partition strategy of
-- sibling ownership_*_observations tables for shape uniformity.
DO $$
DECLARE
    yr INT;
    qtr INT;
    qstart DATE;
    qend DATE;
    pname TEXT;
BEGIN
    FOR yr IN 2010..2030 LOOP
        FOR qtr IN 1..4 LOOP
            qstart := MAKE_DATE(yr, (qtr - 1) * 3 + 1, 1);
            qend := qstart + INTERVAL '3 months';
            pname := FORMAT('ownership_esop_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_esop_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_esop_observations_default
    PARTITION OF ownership_esop_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_period
    ON ownership_esop_observations (instrument_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_esop_obs_trustee_cik
    ON ownership_esop_observations (plan_trustee_cik)
    WHERE plan_trustee_cik IS NOT NULL;


-- ---------------------------------------------------------------------
-- ownership_esop_current — materialised latest-per-plan snapshot
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_esop_current (
    instrument_id           BIGINT NOT NULL,
    plan_name               TEXT NOT NULL,
    plan_trustee_name       TEXT,
    plan_trustee_cik        TEXT,
    ownership_nature        TEXT NOT NULL CHECK (ownership_nature = 'beneficial'),

    source                  TEXT NOT NULL CHECK (source = 'def14a'),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    shares                  NUMERIC(24, 4) NOT NULL CHECK (shares > 0),
    percent_of_class        NUMERIC(8, 4),

    PRIMARY KEY (instrument_id, plan_name)
);

COMMENT ON TABLE ownership_esop_current IS
    'Materialised latest-per-(instrument, plan_name) DEF-14A ESOP snapshot. Rebuilt deterministically by refresh_esop_current() ordering by filed_at DESC, period_end DESC, source_document_id ASC so amendments win over originals.';

CREATE INDEX IF NOT EXISTS idx_esop_current_trustee_cik
    ON ownership_esop_current (plan_trustee_cik)
    WHERE plan_trustee_cik IS NOT NULL;


-- ---------------------------------------------------------------------
-- One-shot expiry of pre-#843 ESOP-shape observations
-- ---------------------------------------------------------------------
-- Pre-#843 the parser tagged ESOP plan rows with the section-context
-- role (typically 'principal' from the 5%-holder block). Those rows
-- live in ``ownership_def14a_observations`` with ``known_to IS NULL``,
-- meaning ``refresh_def14a_current`` would surface them in the
-- def14a slice even after this PR — alongside the new dedicated
-- ESOP slice we ship. Double-count.
--
-- The runtime defence in ``refresh_def14a_current`` filters by name
-- regex (belt + braces), but expiring the legacy observations is the
-- right hygiene step so the next refresh + every audit query sees
-- the same canonical state. Codex pre-push review #843 round 3
-- caught this.
--
-- Pattern mirrors ``_ESOP_NAME_PATTERNS`` in
-- ``app.providers.implementations.sec_def14a`` and the SQL constant
-- ``_ESOP_HOLDER_NAME_SQL_REGEX`` in
-- ``app.services.ownership_observations.refresh_def14a_current``.

UPDATE ownership_def14a_observations
SET known_to = NOW()
WHERE known_to IS NULL
  AND holder_name ~* (
    '\m(?:ESOP'
    '|employee[[:space:]]+stock[[:space:]]+ownership[[:space:]]+plan'
    '|401(?:[[:space:]]*\(?k\)?)?[[:space:]]+plan'
    '|employee[[:space:]]+savings[[:space:]]+plan'
    '|retirement[[:space:]]+savings[[:space:]]+plan'
    '|profit[-[:space:]]sharing[[:space:]]+plan'
    '|employee[[:space:]]+benefit[[:space:]]+plan'
    '|company[[:space:]]+stock[[:space:]]+fund'
    '|(?:savings|retirement|profit[-[:space:]]sharing)[[:space:]]+plan[[:space:]]+trust'
    ')\M'
  );


-- ---------------------------------------------------------------------
-- One-shot purge of stale ESOP-shape rows from the materialised
-- ``ownership_def14a_current`` snapshot.
--
-- Expiring observations alone is not enough: ``_current`` is a
-- materialised view rebuilt only when ``refresh_def14a_current``
-- fires per-instrument. A QUIET instrument (no recent re-ingest)
-- would keep serving the stale ESOP row from ``_current`` until
-- something touches it. Operator audit + Codex round-4 review
-- caught this. Direct DELETE on _current at migration time gets
-- the snapshot to a consistent state immediately; subsequent
-- ingest / refresh continues to apply the runtime regex filter in
-- ``refresh_def14a_current``.

DELETE FROM ownership_def14a_current
WHERE holder_role = 'esop'
   OR holder_name ~* (
    '\m(?:ESOP'
    '|employee[[:space:]]+stock[[:space:]]+ownership[[:space:]]+plan'
    '|401(?:[[:space:]]*\(?k\)?)?[[:space:]]+plan'
    '|employee[[:space:]]+savings[[:space:]]+plan'
    '|retirement[[:space:]]+savings[[:space:]]+plan'
    '|profit[-[:space:]]sharing[[:space:]]+plan'
    '|employee[[:space:]]+benefit[[:space:]]+plan'
    '|company[[:space:]]+stock[[:space:]]+fund'
    '|(?:savings|retirement|profit[-[:space:]]sharing)[[:space:]]+plan[[:space:]]+trust'
    ')\M'
  );

COMMIT;
