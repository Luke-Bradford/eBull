"""Freeze the resolved sec_rate member set so a future addition is caught and
write-safety-audited before it silently inherits the new concurrency (#1542)."""

import pytest

# Full-boot import resolves a pre-existing cold-import cycle in the registry
# (insider_transactions <-> insider_form3_ingest); unrelated to #1542. Building
# the registry standalone hits it.
import app.main  # noqa: F401
from app.jobs.sources import get_job_name_to_source

# db-tier: builds the full registry (imports scheduler + bootstrap_orchestrator).
# Marked db explicitly so the heavy ``app.main`` import (needed to resolve the
# registry cold-import cycle) stays off the fast push gate.
pytestmark = pytest.mark.db

# Generated 2026-06-08 via: source_for over the full registry (spec §3d).
# Adding/removing a sec_rate job MUST update this set AND re-run the
# write-safety audit for the new member (spec §3a).
#
# Write-safety audit, 2026-08-03 (#2212) — the four members added since the
# 2026-06-08 freeze. `sec_rate` is an N=4 in-process semaphore, so a member runs
# CONCURRENTLY with its lanemates; §3a requires every shared write to be
# ordering-safe by something OTHER than the lane. Traced write-set per job:
#
#   daily_financial_facts            → financial_facts_raw. NOT a sole writer
#     (fundamentals_sync + sec_companyfacts_ingest also write it), but
#     upsert_facts_for_instrument is a last-write-wins idempotent UPSERT on
#     identical source-derived data — rationale already recorded inline at
#     app/jobs/sources.py (docs/etl/sources/sec_xbrl_facts.md). Safe.
#   sec_13f_notice_sync              → institutional_filer_13f_notices, via
#     ON CONFLICT (accession_number) DO UPDATE from an immutable filing. Safe.
#   institutional_13f_notice_backfill → same table, same upsert (shares
#     _process_day with the sync job). The pair can now overlap: _already_captured
#     is a read-then-upsert TOCTOU, so a boundary accession may be fetched twice
#     → one wasted rate-floor-bounded SEC fetch + a convergent write, no
#     corruption. Same acceptance as §3a's "boundary overlap" case. Safe.
#   drs_disclosure_refresh           → ownership_drs_observations only, via
#     ON CONFLICT (instrument_id, source_accession) DO UPDATE from an immutable
#     filing; sole writer of that table. Safe.
#
# None of the four writes a watermark, a *_current table, data_freshness_index or
# sec_filing_manifest, so no ordering-sensitive write is involved. Self-overlap
# stays covered by the per-job-name in-process lock (§4).
EXPECTED_SEC_RATE_MEMBERS = frozenset(
    {
        "cusip_universe_backfill",
        "daily_cik_refresh",
        "daily_financial_facts",
        "daily_research_refresh",
        "drs_disclosure_refresh",
        "filings_history_seed",
        "institutional_13f_notice_backfill",
        "mf_directory_sync",
        "ncen_classifier_yearly",
        "sec_13f_filer_directory_sync",
        "sec_13f_notice_sync",
        "sec_13f_quarterly_sweep",
        "sec_8k_events_ingest",
        "sec_atom_fast_lane",
        "sec_business_summary_bootstrap",
        "sec_daily_index_reconcile",
        "sec_def14a_bootstrap",
        "sec_first_install_drain",
        "sec_form3_ingest",
        "sec_master_idx_gap_close",
        "sec_master_idx_quarterly_sweep",
        "sec_n_csr_bootstrap_drain",
        "sec_n_port_ingest",
        "sec_nport_filer_directory_sync",
        "sec_rebuild",
        "sec_submissions_files_walk",
    }
)


def test_sec_rate_membership_is_frozen():
    resolved = {j for j, s in get_job_name_to_source().items() if s == "sec_rate"}
    assert resolved == EXPECTED_SEC_RATE_MEMBERS, (
        f"sec_rate membership changed.\n"
        f"  added:   {resolved - EXPECTED_SEC_RATE_MEMBERS}\n"
        f"  removed: {EXPECTED_SEC_RATE_MEMBERS - resolved}\n"
        "Update EXPECTED_SEC_RATE_MEMBERS and run the write-safety audit "
        "(spec section 3a) before adding a new sec_rate member."
    )
