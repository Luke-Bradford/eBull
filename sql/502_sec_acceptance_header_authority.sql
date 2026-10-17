-- 502_sec_acceptance_header_authority.sql
--
-- #3714 — `sec_filing_manifest.accepted_at` comes from the EDGAR header `<ACCEPTANCE-DATETIME>` only
-- (spec `docs/proposals/etl/2026-10-17-3714-sec-acceptance-authority.md`, Design 1-3).
--
-- The submissions JSON `acceptanceDateTime` moves by the ET offset between captures of the same filing, so a stored
-- value depended on the capture date (19 of a 300-row header-checked sample were early, i.e. look-ahead). From this
-- migration on:
--   * the trigger holds `accepted_at` at its old value (NULL on INSERT) unless the transaction has run
--     `set_config('ebull.acceptance_writer', 'header', true)`. Only `sec_acceptance_refill` sets it, so an old or
--     forgotten discovery writer keeps running and simply stops moving the column;
--   * `sec_acceptance_legacy` freezes every pre-existing value: the completion denominator, the cohort definition and
--     the rollback source. It is filled in this transaction, after the table lock, so no write can land between the
--     baseline and the trigger;
--   * `sec_acceptance_epoch` is the one-row baseline instant (T1 = priority rows created after it);
--   * `sec_acceptance_checks` is the per-accession header evidence that backs every write.
-- Writer of the three tables and of `accepted_at`: `app.services.sec_acceptance_refill`.
-- Census: `scripts/census_3714_acceptance.py`.
--
-- Rollback (spec disposition 6/21): under the GUC, restore `legacy_accepted_at` for rows whose `accepted_at` still
-- equals their header value, delete their checks, then drop the trigger.

BEGIN;

-- A pending lock queues every later manifest writer behind it, so a busy table fails the apply retryably
-- (`LockNotAvailable`) instead of wedging ingestion (docs/review-prevention-log.md, #2363).
SET LOCAL lock_timeout = '5s';

-- Blocks every manifest write until COMMIT, so the baseline below sees all of them and none can follow it unheld.
LOCK TABLE sec_filing_manifest IN SHARE ROW EXCLUSIVE MODE;

CREATE OR REPLACE FUNCTION sec_filing_manifest_hold_accepted_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('ebull.acceptance_writer', true) IS DISTINCT FROM 'header' THEN
        IF TG_OP = 'INSERT' THEN
            NEW.accepted_at := NULL;
        ELSE
            NEW.accepted_at := OLD.accepted_at;
        END IF;
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_sec_filing_manifest_hold_accepted_at ON sec_filing_manifest;
CREATE TRIGGER trg_sec_filing_manifest_hold_accepted_at
    BEFORE INSERT OR UPDATE ON sec_filing_manifest
    FOR EACH ROW EXECUTE FUNCTION sec_filing_manifest_hold_accepted_at();

CREATE TABLE IF NOT EXISTS sec_acceptance_epoch (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    epoch     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS sec_acceptance_legacy (
    accession_number   TEXT PRIMARY KEY,
    legacy_accepted_at TIMESTAMPTZ NOT NULL,
    source             TEXT NOT NULL,
    baselined_at       TIMESTAMPTZ NOT NULL
);

-- The baseline is written only by the statement that creates the epoch row, so a rerun never moves either.
WITH epoch AS (
    INSERT INTO sec_acceptance_epoch (epoch) VALUES (now())
    ON CONFLICT (singleton) DO NOTHING
    RETURNING epoch
)
INSERT INTO sec_acceptance_legacy (accession_number, legacy_accepted_at, source, baselined_at)
SELECT m.accession_number, m.accepted_at, m.source, epoch.epoch
FROM sec_filing_manifest m CROSS JOIN epoch
WHERE m.accepted_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sec_acceptance_legacy_source
    ON sec_acceptance_legacy (source, accession_number);

-- Outcomes: final = the header itself answered (`found` / `absent` / `invalid_value`); retryable = only our
-- observation failed (`transport_error` / `http_error` / `bad_response`).
CREATE TABLE IF NOT EXISTS sec_acceptance_checks (
    accession_number TEXT PRIMARY KEY,
    outcome          TEXT NOT NULL CHECK (outcome IN (
        'found', 'absent', 'invalid_value', 'transport_error', 'http_error', 'bad_response'
    )),
    raw_value        TEXT,
    acceptance_utc   TIMESTAMPTZ,
    url              TEXT NOT NULL,
    http_status      INT,
    diagnostic       TEXT,
    policy_version   INT NOT NULL,
    attempts         INT NOT NULL CHECK (attempts >= 1),
    checked_at       TIMESTAMPTZ NOT NULL,
    next_retry_at    TIMESTAMPTZ,
    CHECK ((outcome = 'found') = (acceptance_utc IS NOT NULL)),
    CHECK (outcome <> 'found' OR raw_value ~ '^[0-9]{14}$'),
    CHECK ((outcome IN ('found', 'absent', 'invalid_value')) = (next_retry_at IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_sec_acceptance_checks_retry
    ON sec_acceptance_checks (next_retry_at) WHERE next_retry_at IS NOT NULL;

-- T1 selection (priority rows created after the epoch) without a scan of the whole manifest. Not partial on the
-- priority sources: the job binds that list as a parameter, which a partial-index predicate cannot be proved from.
CREATE INDEX IF NOT EXISTS idx_manifest_created_at ON sec_filing_manifest (created_at);

COMMIT;
