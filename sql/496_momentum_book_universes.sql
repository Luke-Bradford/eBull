-- 496_momentum_book_universes.sql
--
-- #3822 — each formation's ranking universe: the names the book was ranked in, kept so the live scorecard can draw
-- its random-30 control from the same opportunity set without re-reading 13 months of closes per formation.
-- Writer: `app.services.momentum_book_job.form_book`. Reader: `app.services.momentum_book_scorecard`.
-- Insert only, like the other momentum book tables (`momentum_book_insert_only`, sql/495).

BEGIN;

CREATE TABLE IF NOT EXISTS momentum_book_universes (
    formation_id   BIGINT PRIMARY KEY REFERENCES momentum_book_formations (formation_id),
    -- Instrument ids, ascending.
    universe       JSONB NOT NULL CHECK (jsonb_typeof(universe) = 'array' AND jsonb_array_length(universe) > 0),
    -- 'formation': written with the formation. 'reconstructed': rebuilt later from the same reader, for a formation
    -- that predates this table.
    source         TEXT NOT NULL CHECK (source IN ('formation', 'reconstructed')),
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

DROP TRIGGER IF EXISTS momentum_book_universes_insert_only ON momentum_book_universes;
CREATE TRIGGER momentum_book_universes_insert_only BEFORE UPDATE OR DELETE ON momentum_book_universes
    FOR EACH ROW EXECUTE FUNCTION momentum_book_insert_only();

COMMIT;
