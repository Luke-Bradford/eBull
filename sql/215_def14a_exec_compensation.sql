-- 215_def14a_exec_compensation.sql
--
-- Issue #1945 (child of #1913 raw-store extraction-completeness audit).
-- Extract-more decision: fund an Item 402 executive-compensation parser
-- over the retained ``def14a_body`` payload rather than sweep it.
--
-- Item 402 of Regulation S-K (17 CFR § 229.402) prescribes the Summary
-- Compensation Table (SCT): one row per named executive officer per
-- fiscal year, up to the last three completed fiscal years
-- (§ 229.402(c)(1)). The column set is fixed and ORDERED by
-- § 229.402(c)(2)(i)–(x); the scaled smaller-reporting-company variant
-- (§ 229.402(n)) simply omits some columns but preserves the order.
--
-- Grain: one row per (accession, executive, fiscal_year) — the SCT's
-- natural grain. Mirrors the ``def14a_beneficial_holdings`` (sql/097)
-- conventions:
--
--   * ``instrument_id`` is nullable in the DDL for audit parity with
--     097, but in practice the manifest parser resolves CIK→instrument
--     before writing (dev: 0/38,607 holdings rows have a NULL
--     instrument_id), so the UNIQUE index below — not a partial index —
--     matches live behaviour and the upsert's ON CONFLICT target.
--   * ``principal_position`` is stored raw free-text (no CHECK), like
--     097's ``holder_role`` — CEO/CFO canonicalisation is deferred to
--     the reader (thesis engine, #1919). A re-parse with better role
--     inference then UPSERTs the same identity row.
--   * Dollar amounts are NUMERIC(18, 2): Item 402 reports whole dollars
--     but 2 dp keeps aggregation arithmetic-clean. Never float.

CREATE TABLE IF NOT EXISTS def14a_exec_compensation (
    comp_id              BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT REFERENCES instruments(instrument_id),  -- nullable, CIK-resolved post-parse (mirror 097)
    accession_number     TEXT NOT NULL,
    issuer_cik           TEXT NOT NULL,
    executive_name       TEXT NOT NULL,
    principal_position   TEXT,                 -- (c)(2)(i) role portion, free-text
    fiscal_year          INTEGER NOT NULL,     -- (c)(2)(ii)
    salary               NUMERIC(18, 2),       -- (c)(2)(iii)
    bonus                NUMERIC(18, 2),       -- (c)(2)(iv)
    stock_awards         NUMERIC(18, 2),       -- (c)(2)(v)   grant-date FV per FASB ASC 718
    option_awards        NUMERIC(18, 2),       -- (c)(2)(vi)  grant-date FV per FASB ASC 718
    non_equity_incentive NUMERIC(18, 2),       -- (c)(2)(vii)
    pension_nqdc         NUMERIC(18, 2),       -- (c)(2)(viii) — NULL for SRC scaled SCT
    other_comp           NUMERIC(18, 2),       -- (c)(2)(ix)
    total_comp           NUMERIC(18, 2),       -- (c)(2)(x)
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Identity: (instrument_id, accession, executive, fiscal_year). Mirrors the
-- LIVE holdings key (instrument_id, accession_number, holder_name):
-- instrument_id is included because a DEF 14A fans out to share-class
-- sibling instruments via _resolve_siblings (manifest_parsers/def14a.py) —
-- omitting it would collapse a dual-class issuer's comp rows into one. The
-- position is EXCLUDED from the key (heuristic free-text, like holder_role
-- in 097) so a re-parse with better title inference UPSERTs in place.
CREATE UNIQUE INDEX IF NOT EXISTS uq_def14a_comp_iid_acc_exec_fy
    ON def14a_exec_compensation (instrument_id, accession_number, executive_name, fiscal_year);

-- Hot path: latest comp for one instrument (read endpoint / thesis input).
CREATE INDEX IF NOT EXISTS idx_def14a_comp_instrument_fy
    ON def14a_exec_compensation (instrument_id, fiscal_year DESC);
