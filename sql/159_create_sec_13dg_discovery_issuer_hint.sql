-- 159: sec_13dg_discovery_issuer_hint — discovery-time hint table for the
--      universe-CIK-driven SC 13D/G discovery layer (#1233 PR11).
--
-- Why this table exists
-- ---------------------
-- PR11 activates the dormant SC 13D/G blockholder pipeline by adding a
-- new discovery layer (``app/services/sec_13dg_discovery.py``) that
-- walks the universe of issuer CIKs, asks EDGAR full-text search for
-- recent SC 13D / SC 13G filings against each CIK, and enqueues every
-- hit into ``sec_filing_manifest`` for the existing manifest worker
-- (``manifest_parsers/sec_13dg.py``) to drain.
--
-- The discovery layer ALSO writes one row per ``(accession_number,
-- instrument_id)`` into this hint table for every universe-member
-- instrument the issuer CIK resolved to. The downstream parser then
-- reads the hint set back out to cross-validate its CUSIP-resolved
-- ``instrument_id`` and to fall back to the hint when CUSIP resolution
-- fails for a single-class issuer.
--
-- Why the PK is (accession_number, instrument_id), not accession alone
-- --------------------------------------------------------------------
-- Per Codex 1b BLOCKING #2 on the PR11 spec: share-class siblings
-- (GOOG/GOOGL on shared CIK 1652044, BRK.A/BRK.B on shared CIK
-- 1067983 — see sql/099 and sql/103 for the documented sibling
-- semantics) are distinct ``instrument_id`` rows hung off a single
-- issuer CIK. A given SC 13D/G accession against that CIK is
-- discovery-time relevant to BOTH siblings; the parser must be able
-- to consult the full hint set per accession and choose the correct
-- sibling via CUSIP. A single-row PK on ``accession_number`` would
-- collapse this and silently route observations to the wrong
-- instrument. ``(accession_number, instrument_id)`` keeps the
-- one-to-many shape and lets the parser see every candidate sibling
-- for the accession.
--
-- 5-case parser branch this hint table feeds
-- ------------------------------------------
-- ``manifest_parsers/sec_13dg.py::_parse_13dg`` (per spec §3.1 step 4)
-- branches on (cusip_resolved_instrument_id, hint_ids) as:
--
--   CASE A (CUSIP-in-hints, happy path):
--     instrument_id_from_cusip IN hint_ids
--     → write observation with instrument_id_from_cusip
--
--   CASE B (CUSIP unresolved + exactly 1 hint, single-class fallback):
--     instrument_id_from_cusip IS NULL AND len(hint_ids) == 1
--     → write observation with the single hint instrument_id
--
--   CASE C (CUSIP unresolved + N>1 hints, ambiguous share-class):
--     instrument_id_from_cusip IS NULL AND len(hint_ids) > 1
--     → write instrument_id=NULL with
--       blockholder_filings_ingest_log.error =
--         "cusip_unresolved_with_ambiguous_hint"
--
--   CASE D (CUSIP resolved but NOT in hints, universe-revalidated
--           per Codex 1c HIGH):
--     instrument_id_from_cusip IS NOT NULL
--     AND instrument_id_from_cusip NOT IN hint_ids
--     → re-check instrument against current tradable universe
--       (country='US' AND is_tradable=TRUE)
--       - CASE D-in:  instrument IS in current universe
--                     → write with instrument_id + discrepancy log
--                       (hint set may simply be stale)
--       - CASE D-out: instrument is NOT in current universe
--                     → write instrument_id=NULL with
--                       blockholder_filings_ingest_log.error =
--                         "cusip_resolved_outside_universe (...)"
--
--   CASE E (no hint at all, legacy daily-index path):
--     hint_ids is empty
--     → CUSIP-only resolution as today; no PR11 regression for the
--       legacy daily-index path. Existing #740 backfill epic
--       continues to own the legacy-path silent-gap.
--
-- UPSERT contract pinned by lint invariant L (Phase 10)
-- -----------------------------------------------------
-- The discovery layer's INSERT MUST use:
--
--     INSERT INTO sec_13dg_discovery_issuer_hint
--         (accession_number, instrument_id, issuer_cik)
--     VALUES (...)
--     ON CONFLICT (accession_number, instrument_id)
--     DO UPDATE SET discovered_at = NOW(),
--                   issuer_cik    = EXCLUDED.issuer_cik
--
-- Re-discovery of an already-known (accession, instrument_id) pair is
-- a no-op semantically EXCEPT for advancing ``discovered_at`` so the
-- freshness operator can observe recent scan activity. Lint invariant
-- L (Phase 10) forbids switching to ``ON CONFLICT DO NOTHING`` (which
-- would mask freshness signal) or any other write shape.
--
-- Lifecycle
-- ---------
-- Rows live as long as the parent ``instruments`` row lives;
-- ``ON DELETE CASCADE`` cleans hints up if an instrument is removed
-- from the universe (e.g. delisting cleanup). The hint table is
-- additive and never trimmed by retention policy — the cutover from a
-- live hint to a permanent observation happens in the manifest parser,
-- not by deleting the hint. (The retention window for SC 13D/G filings
-- themselves is enforced by ``blockholders_retention_cutoff()`` on the
-- discovery query side, not on this hint table.)

CREATE TABLE IF NOT EXISTS sec_13dg_discovery_issuer_hint (
    accession_number  TEXT NOT NULL,
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    issuer_cik        TEXT NOT NULL,
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (accession_number, instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_sec_13dg_discovery_issuer_hint_accession
    ON sec_13dg_discovery_issuer_hint (accession_number);

CREATE INDEX IF NOT EXISTS idx_sec_13dg_discovery_issuer_hint_instrument_id
    ON sec_13dg_discovery_issuer_hint (instrument_id);
