-- 484_tax_fx_boe.sql
--
-- #3616 slice 3b — the tax FX source: Bank of England XUDLUSS (US dollars per pound), verified every run against
-- the GOV.UK England-and-Wales bank-holiday calendar. Spec: docs/specs/metrics/2026-10-13-3616-slice3b-tax-fx-boe.md
-- (Design 1). New tables only: `fx_rates` (sql/013), `fx_rates_daily` and every tax figure are untouched; slice 3c is
-- the first consumer.
--
-- ⚠ The repository is public and the Bank's exchange-rate data is excluded from the Open Government Licence: this
-- file holds no rate. The calendar seed below is GOV.UK content (OGL v3.0), copied from the committed file
-- docs/research/3616-slice3b-inputs/bank-holidays_2026-10-13.json, whose sha256 is the seed source tag and
-- `app.services.tax_fx.CALENDAR_SEED_SHA256`.

BEGIN;

-- One row. Changed only by a reviewed migration that first takes pg_advisory_xact_lock(3897302543388114945), the
-- run lock (`app.services.tax_fx.TAX_FX_LOCK_KEY`), so a change cannot race a run or a publication. false makes
-- every date `unavailable`.
CREATE TABLE IF NOT EXISTS tax_fx_source_control (
    singleton   BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    enabled     BOOLEAN NOT NULL,
    reason      TEXT NOT NULL CHECK (reason <> ''),
    changed_at  TIMESTAMPTZ NOT NULL
);

INSERT INTO tax_fx_source_control (singleton, enabled, reason, changed_at)
VALUES (TRUE, TRUE, 'sql/484 (#3616 slice 3b): personal and internal use, pending the Bank data team', now())
ON CONFLICT (singleton) DO NOTHING;

-- One row per job run, created first. run_id is assigned under the run lock, so it totally orders runs.
CREATE TABLE IF NOT EXISTS tax_fx_runs (
    run_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at              TIMESTAMPTZ NOT NULL,
    finished_at             TIMESTAMPTZ,
    -- The run's one effective Europe/London date, used for the request, validation and replay.
    run_date                DATE NOT NULL,
    state                   TEXT NOT NULL
        CHECK (state IN ('in_progress', 'applied', 'refused', 'skipped', 'abandoned')),
    reason                  TEXT,
    -- Every check except confirmation passed (spec Design 2, steps 4.1-4.9).
    eligible                BOOLEAN NOT NULL DEFAULT FALSE,
    policy_id               TEXT NOT NULL CHECK (policy_id <> ''),
    parser_version          TEXT NOT NULL CHECK (parser_version <> ''),
    calendar_used           TEXT CHECK (calendar_used IN ('fetched', 'stored')),
    body_last_date          DATE,
    accepted_through        DATE,
    inserted                INTEGER NOT NULL DEFAULT 0 CHECK (inserted >= 0),
    revised                 INTEGER NOT NULL DEFAULT 0 CHECK (revised >= 0),
    awaiting_confirmation   INTEGER NOT NULL DEFAULT 0 CHECK (awaiting_confirmation >= 0),
    calendar_years_changed  INTEGER NOT NULL DEFAULT 0 CHECK (calendar_years_changed >= 0),
    CHECK ((state = 'in_progress') = (finished_at IS NULL)),
    CHECK (state <> 'applied' OR eligible)
);

-- At most one open run; the lock makes a second impossible, this makes it visible if the lock were bypassed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_tax_fx_runs_one_in_progress
    ON tax_fx_runs ((TRUE)) WHERE state = 'in_progress';

-- Raw evidence: one row per download attempt. Never updated or deleted.
CREATE TABLE IF NOT EXISTS tax_fx_fetches (
    fetch_id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                   BIGINT NOT NULL REFERENCES tax_fx_runs (run_id),
    source                   TEXT NOT NULL CHECK (source IN ('boe_xudluss', 'govuk_bank_holidays')),
    request_url              TEXT NOT NULL CHECK (request_url <> ''),
    user_agent               TEXT NOT NULL CHECK (user_agent <> ''),
    fetched_at               TIMESTAMPTZ NOT NULL,
    outcome                  TEXT NOT NULL CHECK (outcome IN ('received', 'incomplete', 'oversize', 'transport_error')),
    error                    TEXT,
    http_status              SMALLINT CHECK (http_status BETWEEN 100 AND 599),
    content_length_header    TEXT,
    content_encoding_header  TEXT,
    -- The exact entity bytes received (an `oversize` row keeps only the first 64 KiB), gzipped.
    body_gz                  BYTEA,
    body_sha256              TEXT CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    body_length              INTEGER CHECK (body_length >= 0),
    UNIQUE (run_id, source),
    CHECK ((outcome = 'transport_error') = (http_status IS NULL)),
    CHECK ((outcome = 'transport_error') = (body_gz IS NULL)),
    CHECK ((body_gz IS NULL) = (body_sha256 IS NULL) AND (body_gz IS NULL) = (body_length IS NULL)),
    CHECK ((outcome = 'received') = (error IS NULL))
);

-- The current accepted value per date: a complete prefix of working days from 2019-01-01.
CREATE TABLE IF NOT EXISTS tax_fx_observations (
    observation_date  DATE PRIMARY KEY CHECK (observation_date >= DATE '2019-01-01'),
    usd_per_gbp       NUMERIC(12, 6) NOT NULL CHECK (usd_per_gbp > 0),
    set_by_run_id     BIGINT NOT NULL REFERENCES tax_fx_runs (run_id)
);

-- Every value change. Append-only.
CREATE TABLE IF NOT EXISTS tax_fx_revisions (
    revision_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observation_date  DATE NOT NULL REFERENCES tax_fx_observations (observation_date),
    old_usd_per_gbp   NUMERIC(12, 6) NOT NULL CHECK (old_usd_per_gbp > 0),
    new_usd_per_gbp   NUMERIC(12, 6) NOT NULL CHECK (new_usd_per_gbp > 0),
    run_id            BIGINT NOT NULL REFERENCES tax_fx_runs (run_id),
    CHECK (old_usd_per_gbp <> new_usd_per_gbp)
);

-- England-and-Wales holidays per year. source = 'seed:<sha256>' or 'run:<run_id>' (the run that last added or
-- changed the year).
CREATE TABLE IF NOT EXISTS tax_fx_calendar_years (
    year           INTEGER PRIMARY KEY CHECK (year BETWEEN 2019 AND 2200),
    holiday_dates  DATE[] NOT NULL CHECK (cardinality(holiday_dates) >= 8),
    source         TEXT NOT NULL CHECK (source ~ '^(seed:[0-9a-f]{64}|run:[0-9]+)$')
);

-- Every added (old_dates NULL) or changed year. Append-only.
CREATE TABLE IF NOT EXISTS tax_fx_calendar_revisions (
    calendar_revision_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    year                  INTEGER NOT NULL REFERENCES tax_fx_calendar_years (year),
    old_dates             DATE[],
    new_dates             DATE[] NOT NULL,
    run_id                BIGINT NOT NULL REFERENCES tax_fx_runs (run_id)
);

CREATE OR REPLACE FUNCTION tax_fx_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS tax_fx_fetches_append_only ON tax_fx_fetches;
CREATE TRIGGER tax_fx_fetches_append_only
    BEFORE UPDATE OR DELETE ON tax_fx_fetches
    FOR EACH ROW EXECUTE FUNCTION tax_fx_append_only();

DROP TRIGGER IF EXISTS tax_fx_revisions_append_only ON tax_fx_revisions;
CREATE TRIGGER tax_fx_revisions_append_only
    BEFORE UPDATE OR DELETE ON tax_fx_revisions
    FOR EACH ROW EXECUTE FUNCTION tax_fx_append_only();

DROP TRIGGER IF EXISTS tax_fx_calendar_revisions_append_only ON tax_fx_calendar_revisions;
CREATE TRIGGER tax_fx_calendar_revisions_append_only
    BEFORE UPDATE OR DELETE ON tax_fx_calendar_revisions
    FOR EACH ROW EXECUTE FUNCTION tax_fx_append_only();

-- Seed: GOV.UK bank-holidays.json, england-and-wales, 2019-2028, fetched 2026-10-13.
INSERT INTO tax_fx_calendar_years (year, holiday_dates, source) VALUES
    (2019, '{2019-01-01,2019-04-19,2019-04-22,2019-05-06,2019-05-27,2019-08-26,2019-12-25,2019-12-26}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2020, '{2020-01-01,2020-04-10,2020-04-13,2020-05-08,2020-05-25,2020-08-31,2020-12-25,2020-12-28}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2021, '{2021-01-01,2021-04-02,2021-04-05,2021-05-03,2021-05-31,2021-08-30,2021-12-27,2021-12-28}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2022, '{2022-01-03,2022-04-15,2022-04-18,2022-05-02,2022-06-02,2022-06-03,2022-08-29,2022-09-19,2022-12-26,2022-12-27}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2023, '{2023-01-02,2023-04-07,2023-04-10,2023-05-01,2023-05-08,2023-05-29,2023-08-28,2023-12-25,2023-12-26}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2024, '{2024-01-01,2024-03-29,2024-04-01,2024-05-06,2024-05-27,2024-08-26,2024-12-25,2024-12-26}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2025, '{2025-01-01,2025-04-18,2025-04-21,2025-05-05,2025-05-26,2025-08-25,2025-12-25,2025-12-26}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2026, '{2026-01-01,2026-04-03,2026-04-06,2026-05-04,2026-05-25,2026-08-31,2026-12-25,2026-12-28}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2027, '{2027-01-01,2027-03-26,2027-03-29,2027-05-03,2027-05-31,2027-08-30,2027-12-27,2027-12-28}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6'),
    (2028, '{2028-01-03,2028-04-14,2028-04-17,2028-05-01,2028-05-29,2028-08-28,2028-12-25,2028-12-26}'::date[], 'seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6')
ON CONFLICT (year) DO NOTHING;

COMMENT ON TABLE tax_fx_runs IS
    '#3616 slice 3b: one row per tax_fx_refresh run (Bank of England XUDLUSS + GOV.UK calendar).';
COMMENT ON TABLE tax_fx_fetches IS
    '#3616 slice 3b: raw evidence, one row per download attempt, exact entity bytes gzipped. Append-only. '
    'Bank of England bodies: personal and internal use only; never export.';
COMMENT ON TABLE tax_fx_observations IS
    '#3616 slice 3b: accepted XUDLUSS value per working day, each seen in two consecutive eligible responses.';

COMMIT;
