"""Freeze the resolved sec_rate member set so a future addition is caught and
write-safety-audited before it silently inherits the new concurrency (#1542)."""

import pytest

# Full-boot import resolves a pre-existing cold-import cycle in the registry
# (insider_transactions <-> insider_form3_ingest); unrelated to #1542. Building
# the registry standalone hits it.
import app.main  # noqa: F401
from app.jobs.sources import get_job_name_to_source

# db-tier: builds the full registry (imports scheduler + bootstrap_orchestrator).
# The auto-marker does NOT mark this module (its source has no psycopg.connect /
# TestClient), so mark it explicitly.
pytestmark = pytest.mark.db

# Generated 2026-06-08 via: source_for over the full registry (spec §3d).
# Adding/removing a sec_rate job MUST update this set AND re-run the
# write-safety audit for the new member (spec §3a).
EXPECTED_SEC_RATE_MEMBERS = frozenset(
    {
        "cusip_universe_backfill",
        "daily_cik_refresh",
        "daily_research_refresh",
        "filings_history_seed",
        "mf_directory_sync",
        "ncen_classifier_yearly",
        "sec_13f_filer_directory_sync",
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
    assert resolved == EXPECTED_SEC_RATE_MEMBERS
