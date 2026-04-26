-- Migration 070 — eToro lookup tables (#515 PR 1).
--
-- The universe ingest stores ``stocksIndustryId`` as a raw
-- integer in ``instruments.sector``. The frontend can't render a
-- meaningful label without joining on eToro's lookup catalogues —
-- see workstream 1 of
-- docs/superpowers/specs/2026-04-26-complete-coverage-spec.md.
--
-- The eToro instruments endpoint also returns ``instrumentTypeID``
-- (int) but NOT ``instrumentTypeName`` — confirmed against the
-- live API in docs/research/etoro-instrument-samples/. Migration
-- 068 added ``instruments.instrument_type`` (TEXT) speculatively
-- on the assumption the name was returned; that column stays
-- NULL across the universe in practice. This migration adds
-- ``instruments.instrument_type_id`` (the field eToro actually
-- returns) so the new ``etoro_instrument_types`` lookup is
-- joinable on a stable int key.
--
-- These tables are operator-curated only via the refresh job —
-- they're a thin reflection of eToro's catalogue with no extra
-- columns. ``description`` / ``name`` are populated from the
-- live API by ``app.services.etoro_lookups.refresh_etoro_lookups``;
-- both columns nullable so the refresh job can insert id-only rows
-- before its first successful body fetch.

BEGIN;

CREATE TABLE IF NOT EXISTS etoro_instrument_types (
    instrument_type_id     INTEGER PRIMARY KEY,
    description            TEXT,
    seeded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE etoro_instrument_types IS
    'Reflection of eToro /api/v1/market-data/instrument-types. '
    'Maps numeric instrumentTypeID (Forex / Commodity / CFD / '
    'Indices / Stocks / ETF / Bonds / …) to the human-readable '
    'description rendered on the instrument page. Refreshed weekly '
    'by app.services.etoro_lookups.refresh_etoro_lookups.';

CREATE TABLE IF NOT EXISTS etoro_stocks_industries (
    industry_id            INTEGER PRIMARY KEY,
    name                   TEXT,
    seeded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE etoro_stocks_industries IS
    'Reflection of eToro /api/v1/market-data/stocks-industries. '
    'Maps numeric industryID (Basic Materials / Healthcare / '
    'Technology / …) to the human-readable industry name '
    'rendered as the instrument-page sector label. Refreshed '
    'weekly with the instrument-types catalogue.';

-- ---------------------------------------------------------------
-- instrument_type_id on instruments — the FK that lets us join.
-- ---------------------------------------------------------------
--
-- Migration 068 added ``instruments.instrument_type`` (TEXT) for
-- the human-readable name. PR 1 of #515 adds the numeric
-- ``instrument_type_id`` so the frontend can join on a stable id
-- rather than a text match (eToro changes labels occasionally;
-- the int id is the durable key).
--
-- No FK constraint to ``etoro_instrument_types`` because the
-- universe sync may run before the lookup refresh on a fresh DB
-- (the universe ingest persists ids it sees; the lookup refresh
-- is a separate weekly job). A FK would tie the two together and
-- block universe sync until the lookup is populated.
--
-- Rollout: pre-070 rows have ``instrument_type_id = NULL`` until
-- the next ``nightly_universe_sync`` run. That job is on-demand
-- (operator triggers via Admin "Run now" or via the daily
-- ``orchestrator_full_sync`` DAG walk; neither catches up on
-- boot). The frontend renders NULL as "label unknown" — the
-- column is additive, no breaking change.

ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS instrument_type_id INTEGER;

COMMENT ON COLUMN instruments.instrument_type_id IS
    'eToro instrumentTypeID. FK-style reference to '
    'etoro_instrument_types(instrument_type_id) — no CONSTRAINT '
    'because the universe ingest can run before the lookup '
    'refresh on a fresh DB.';

COMMIT;
