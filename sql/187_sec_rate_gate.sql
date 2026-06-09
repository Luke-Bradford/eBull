-- 187_sec_rate_gate.sql
--
-- #1484 — cross-process SEC 10 req/s rate limiter. The in-process
-- _PROCESS_RATE_LIMIT_CLOCK (app/providers/implementations/sec_edgar.py)
-- paces each PROCESS to <=9 req/s independently; the API + jobs processes
-- together can sum >10 req/s against SEC's single per-IP counter -> UA-ban
-- risk. This table backs a shared GCRA "virtual floor": a single
-- advanceable next_free_at timestamp all processes reserve against, so the
-- global reservation rate stays under the SEC ceiling regardless of
-- process count. Keyed by `budget` so other per-IP limiters (FINRA, etc.)
-- can adopt the same primitive later by inserting another row.
--
-- See docs/specs/ops/2026-06-09-sec-cross-process-rate-limiter.md.

CREATE TABLE IF NOT EXISTS sec_rate_gate (
    budget        TEXT PRIMARY KEY,
    next_free_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO sec_rate_gate (budget) VALUES ('sec') ON CONFLICT (budget) DO NOTHING;
