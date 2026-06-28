/**
 * Shared constants for the OpenFIGI key nudge banners (#1344 pre-flight,
 * #1791 mid-run drift-heal). Single source of truth — the S13 stage key
 * MUST NOT drift between the two banners.
 */

// S13 — the OpenFIGI CUSIP post-bulk sweep stage (the key only speeds this
// one stage). Mirrors app/services/bootstrap_orchestrator.py
// JOB_CUSIP_RESOLVER_POST_BULK_SWEEP.
export const S13_STAGE_KEY = "cusip_resolver_post_bulk_sweep";

export const OPENFIGI_KEY_URL = "https://www.openfigi.com/api";
