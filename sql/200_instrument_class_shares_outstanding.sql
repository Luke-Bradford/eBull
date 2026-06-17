-- 200_instrument_class_shares_outstanding.sql
--
-- #788 (ownership DQ audit) — per-class shares-outstanding denominator. Spec:
--   docs/specs/etl/2026-06-17-per-class-shares-denominator.md
--
-- A multi-class issuer whose classes share one SEC CIK (GOOG/GOOGL, HEI/HEI.A,
-- METC/METCB) has per-class holdings resolved by CUSIP, but the only
-- shares-outstanding figure in our pipeline is the issuer's COMBINED all-class
-- count (companyfacts strips the dimensional per-class facts). Dividing
-- per-class holdings by the combined count understates every percentage ~2×
-- (GOOGL institutions 20.97% vs true ~43.8%). #1646 shipped an honest caveat;
-- this table supplies the REAL per-class denominator from the SEC DERA Financial
-- Statement Data Sets (FSDS num.txt), whose `segments` column carries the
-- dimensional `ClassOfStock=<member>` axis the JSON APIs drop.
--
-- Sourced by app/services/fsds_class_shares.py from the us-gaap
-- `CommonStockSharesOutstanding` tag (NOT dei — dei per-class is stripped),
-- single-axis `ClassOfStock` rows, current-period instant (ddate == sub.period),
-- mapped (cik, class_member) -> CUSIP -> instrument via a curated, hand-verified
-- map (`_CLASS_MEMBER_TO_CUSIP`). Read by ownership_rollup._read_class_shares_
-- outstanding, which divides by it only when the fail-closed guards pass (period
-- coherence with the combined as_of, 0 < class < combined, no pie holder >
-- class); otherwise the #1646 caveat is preserved. Fail-closed: never fabricate.
--
-- Grain: one row per (instrument_id, period_end); the read selects MAX(period_end).
-- Tiny cardinality (~hundreds of rows over the dual-class dev set).
--
-- RESTATEMENT NO-DEMOTION (Codex ckpt-1 #6): a same-quarter amendment or a later
-- FSDS quarter restating an old period must win deterministically. The upsert
-- orders by (source_filed_at, source_adsh); source_filed_at is NOT NULL (sub.txt
-- always carries `filed`) so the ON CONFLICT predicate never compares against
-- NULL.

CREATE TABLE IF NOT EXISTS instrument_class_shares_outstanding (
    instrument_id      INTEGER       NOT NULL REFERENCES instruments(instrument_id),
    period_end         DATE          NOT NULL,
    shares             NUMERIC(28,4) NOT NULL CHECK (shares > 0),
    class_member       TEXT          NOT NULL,  -- FSDS ClassOfStock localname (audit)
    source_cik         TEXT          NOT NULL CHECK (source_cik ~ '^[0-9]{10}$'),
    source_adsh        TEXT          NOT NULL,  -- FSDS accession -> SharesOutstandingSource.edgar_url
    source_form_type   TEXT          NOT NULL,  -- sub.txt form (10-K/10-Q) -> SharesOutstandingSource
    source_fsds_qtr    TEXT          NOT NULL,  -- e.g. '2025q1' (audit/provenance)
    source_filed_at    DATE          NOT NULL,  -- sub.txt filed; no-demotion tie-break
    resolution_method  TEXT          NOT NULL CHECK (resolution_method = 'curated'),
    parser_version     TEXT          NOT NULL,  -- 'fsds_class_shares_v1'
    ingested_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, period_end)
);
