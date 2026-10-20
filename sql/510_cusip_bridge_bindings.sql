-- 510_cusip_bridge_bindings.sql
--
-- #2327: provenance for the Schedule 13D/G issuer-CUSIP bridge. Spec:
-- docs/specs/etl/2026-10-20-2327-13dg-cusip-bridge.md ("Construction" step 1).
--
-- One row per external_identifiers row the bridge inserted (provider = 'sec', identifier_type = 'cusip'), carrying what
-- replays the decision: the issuer CIK, the 13D/G accessions naming the CUSIP, and the window lists (quarter + SHA-256)
-- the gates read. The mapping itself stays in external_identifiers under provider = 'sec', so no reader's provider
-- allow-list changes (prevention log #2213).
--
-- No ON DELETE CASCADE on either FK: rollback (spec "Rollback" step 2) copies these rows to a manifest, deletes them,
-- then deletes the external_identifiers rows they name. A cascade would let the second delete happen without the first.
-- job_run_id has no FK: job_runs is pruned, the provenance is not.

CREATE TABLE IF NOT EXISTS cusip_bridge_bindings (
    external_identifier_id BIGINT PRIMARY KEY REFERENCES external_identifiers (external_identifier_id),
    cusip TEXT NOT NULL,
    instrument_id BIGINT NOT NULL REFERENCES instruments (instrument_id),
    issuer_cik TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('bind', 'confirm')),
    rule_version TEXT NOT NULL,
    accessions TEXT[] NOT NULL,
    list_quarters TEXT[] NOT NULL,
    list_sha256 TEXT[] NOT NULL,
    job_run_id BIGINT NOT NULL,
    bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cusip_bridge_bindings_lists_parallel CHECK (cardinality(list_quarters) = cardinality(list_sha256))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_cusip_bridge_bindings_cusip ON cusip_bridge_bindings (cusip);
