-- 221_fair_value_band.sql
--
-- #2009 deterministic fair-value valuation-evidence band.
-- Spec: docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md
--
-- TWO-LAYER RATIONALE (mirrors sql/198 instrument_risk_metrics):
--   price_daily is MUTABLE (ingest upserts corrected bars). A past band is
--   NOT reconstructable from current price_daily, so the observation row IS
--   the audit record. _observations is APPEND-ONLY; _current is the
--   write-through row the thesis reads. computed_at is in the observations PK
--   so a vendor correction that does NOT advance as_of_date appends rather
--   than silently overwriting.

-- Pass-1: cohort-member WORKING SET, materialized universe-wide at the single
-- batch as_of_date. One row per (as_of_date, instrument_id, multiple) with the
-- name's as-of multiple + its SIC keys + total_assets + dual-class-suppressed
-- flag + close staleness. Pass-2 reads this per target, walks the SIC ladder to
-- MIN_PEERS, size-refines to nearest PEER_LIMIT by |ln(total_assets)|, and
-- percentiles IN PURE PYTHON (reusing percentiles(), same fn as own-history).
--
-- WHY member-level, not pre-percentiled: peer-median size refinement (§4.3) is
-- PER-TARGET (nearest-8 to THAT name's assets) — a per-(sic,multiple) percentile
-- table cannot carry it (Codex ckpt-1 HIGH #1). Pass-1 still does the expensive
-- price-as-of join once for every name; pass-2's per-target percentile over <=8
-- members is trivial and needs no per-sibling re-price.
CREATE TABLE IF NOT EXISTS fair_value_cohort_members (
    as_of_date            date    NOT NULL,
    instrument_id         bigint  NOT NULL,
    multiple              text    NOT NULL,          -- CHECK IN (pe, ps, pb)
    mult_value            numeric(18,6) NOT NULL,     -- the name's as-of multiple, denominator > 0
    sic                   text,
    sic3                  text,
    sic2                  text,
    total_assets          numeric(20,4),             -- for the log-distance size refinement
    close_date            date    NOT NULL,          -- the price_daily bar used (nearest at/before as_of)
    dual_class_suppressed boolean NOT NULL,          -- curated-oracle member -> excluded from ps/pb medians
    PRIMARY KEY (as_of_date, instrument_id, multiple),
    CONSTRAINT fvcm_multiple_chk CHECK (multiple IN ('pe', 'ps', 'pb'))
);
CREATE INDEX IF NOT EXISTS fair_value_cohort_members_sic_idx
    ON fair_value_cohort_members (as_of_date, multiple, sic);
CREATE INDEX IF NOT EXISTS fair_value_cohort_members_sic3_idx
    ON fair_value_cohort_members (as_of_date, multiple, sic3);
CREATE INDEX IF NOT EXISTS fair_value_cohort_members_sic2_idx
    ON fair_value_cohort_members (as_of_date, multiple, sic2);

-- Append-only audit record.
CREATE TABLE IF NOT EXISTS fair_value_band_observations (
    instrument_id  bigint      NOT NULL,     -- NO FK (survive delist/merge/re-id)
    method_version text        NOT NULL,     -- 'fvb_v1'
    computed_at    timestamptz NOT NULL,
    as_of_date     date        NOT NULL,     -- the single batch as-of
    ttm_end        date,
    price_as_of    date,
    bear_value     numeric(18,6),
    base_value     numeric(18,6),
    bull_value     numeric(18,6),
    quality_status text,                     -- high | medium | low (NULL when no band)
    reason         text        NOT NULL,
    target_basis   text        NOT NULL,     -- resolve_market_cap_basis result
    n_selected     smallint    NOT NULL,
    basis_json     jsonb       NOT NULL,
    PRIMARY KEY (instrument_id, method_version, computed_at),
    CONSTRAINT fvb_obs_reason_chk CHECK (reason IN
        ('ok','no_multiple','currency_mismatch','stale_price','multiclass_unavailable','thin_cohort')),
    CONSTRAINT fvb_obs_quality_chk CHECK (quality_status IS NULL OR quality_status IN ('high','medium','low')),
    CONSTRAINT fvb_obs_order_chk CHECK (
        bear_value IS NULL OR base_value IS NULL OR bull_value IS NULL
        OR (bear_value <= base_value AND base_value <= bull_value))
);

-- Write-through current (the thesis read row).
CREATE TABLE IF NOT EXISTS fair_value_band_current (
    instrument_id  bigint      NOT NULL,
    method_version text        NOT NULL,
    computed_at    timestamptz NOT NULL,
    as_of_date     date        NOT NULL,
    ttm_end        date,
    price_as_of    date,
    bear_value     numeric(18,6),
    base_value     numeric(18,6),
    bull_value     numeric(18,6),
    quality_status text,
    reason         text        NOT NULL,
    target_basis   text        NOT NULL,
    n_selected     smallint    NOT NULL,
    basis_json     jsonb       NOT NULL,
    PRIMARY KEY (instrument_id, method_version),
    CONSTRAINT fvb_cur_reason_chk CHECK (reason IN
        ('ok','no_multiple','currency_mismatch','stale_price','multiclass_unavailable','thin_cohort')),
    CONSTRAINT fvb_cur_quality_chk CHECK (quality_status IS NULL OR quality_status IN ('high','medium','low')),
    CONSTRAINT fvb_cur_order_chk CHECK (
        bear_value IS NULL OR base_value IS NULL OR bull_value IS NULL
        OR (bear_value <= base_value AND base_value <= bull_value))
);

-- Writer's real-band read (skip statused-absent rows).
CREATE INDEX IF NOT EXISTS fair_value_band_current_realband_idx
    ON fair_value_band_current (instrument_id) WHERE base_value IS NOT NULL;

-- SIC prefix ladder support (none today; sql/051 is cik-only).
ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS sic3 text GENERATED ALWAYS AS (left(sic, 3)) STORED,
    ADD COLUMN IF NOT EXISTS sic2 text GENERATED ALWAYS AS (left(sic, 2)) STORED;
CREATE INDEX IF NOT EXISTS instrument_sec_profile_sic_idx  ON instrument_sec_profile (sic);
CREATE INDEX IF NOT EXISTS instrument_sec_profile_sic3_idx ON instrument_sec_profile (sic3);
CREATE INDEX IF NOT EXISTS instrument_sec_profile_sic2_idx ON instrument_sec_profile (sic2);
