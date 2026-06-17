"""Bootstrap freshness-source coverage map (#1511 / T5 of #1508).

Which ``data_freshness_index`` ``source`` values each bootstrap stage's sink
populates. Used by the Processes-page verdict look-through (part a): a
never-run steady-state poll job whose freshness source is in
``BOOTSTRAP_COVERED_FRESHNESS_SOURCES`` AND is still fresh reads green
**Current** instead of blue "first run pending" — because bootstrap already
seeded that source via the bulk ingest → ``record_manifest_entry`` →
``seed_freshness_for_manifest_row`` path (empirically verified on dev: every
covered source has thousands of ``state='current'`` rows post-bootstrap).

It also defines, by complement, the *uncovered* sources that the
post-bootstrap genuine-gap kick (part b) may target.

**Drift discipline (load-bearing):** the per-stage classification is keyed by
``StageSpec.stage_key`` and is hand-maintained. ``tests/services/processes/
test_bootstrap_coverage.py`` asserts the key set equals ``{s.stage_key for s
in _BOOTSTRAP_STAGE_SPECS}`` exactly, so adding / renaming / removing a stage
forces a deliberate classification here rather than silently mis-marking a
source as covered (false green) or uncovered (spurious kick).
"""

from __future__ import annotations

from typing import Final

from app.services.sec_manifest import ManifestSource

# Issuer filing-metadata forms seeded into ``filing_events`` + the manifest
# (hence ``data_freshness_index``) by the submissions ingest, the first-install
# drain, and the quarterly master-index gap-close. Mirrors
# ``app/jobs/sec_master_idx_quarterly_sweep.py::GAP_CLOSE_FILING_METADATA_SOURCES``
# — kept as a local literal to avoid importing the jobs layer into a
# services/processes module, and pinned EQUAL to it by the drift test so the
# two cannot diverge.
_ISSUER_FILING_METADATA: Final[frozenset[ManifestSource]] = frozenset(
    {"sec_8k", "sec_10k", "sec_10q", "sec_def14a", "sec_13d", "sec_13g"}
)

# stage_key -> data_freshness_index sources the stage's sink populates.
# ``frozenset()`` = the stage writes NO freshness sink (universe / candles /
# directory refreshes / CUSIP + business-summary metadata / pure derivations /
# the read-only validation gate). EVERY ``_BOOTSTRAP_STAGE_SPECS`` stage_key
# MUST appear here (enforced by the drift test).
_BOOTSTRAP_STAGE_FRESHNESS_SOURCES: Final[dict[str, frozenset[ManifestSource]]] = {
    "universe_sync": frozenset(),
    "candle_refresh": frozenset(),
    "cusip_universe_backfill": frozenset(),
    "sec_13f_filer_directory_sync": frozenset(),
    "sec_nport_filer_directory_sync": frozenset(),
    "cik_refresh": frozenset(),
    "sec_bulk_download": frozenset(),
    # Bulk submissions ingest writes filing_events + manifest for the issuer
    # filing-metadata forms.
    "sec_submissions_ingest": _ISSUER_FILING_METADATA,
    "sec_companyfacts_ingest": frozenset({"sec_xbrl_facts"}),
    "sec_13f_ingest_from_dataset": frozenset({"sec_13f_hr"}),
    "sec_insider_ingest_from_dataset": frozenset({"sec_form3", "sec_form4", "sec_form5"}),
    "sec_nport_ingest_from_dataset": frozenset({"sec_n_port"}),
    "cusip_resolver_post_bulk_sweep": frozenset(),
    # #788 — writes the per-class shares table from cached FSDS zips; NOT a
    # manifest source, so no freshness sink (like the resolver sweep above).
    "sec_fsds_class_shares_ingest": frozenset(),
    "sec_master_idx_gap_close": _ISSUER_FILING_METADATA,
    # First-install drain seeds filing_events (issuer filing-metadata) from the
    # local bulk submissions.zip.
    "sec_first_install_drain": _ISSUER_FILING_METADATA,
    "sec_business_summary_bootstrap": frozenset(),
    # Parses 8-K events into structured tables; sec_8k freshness is owned by
    # the submissions / gap-close manifest writes above, not here.
    "sec_8k_events_ingest": frozenset(),
    "ownership_observations_backfill": frozenset(),
    # Normalises XBRL facts into typed tables; sec_xbrl_facts freshness is owned
    # by sec_companyfacts_ingest.
    "fundamentals_sync": frozenset(),
    # Seeds the fund directory + classId map, NOT N-CSR filings — so sec_n_csr
    # is deliberately NOT covered (steady-state manifest-worker discovers it).
    "mf_directory_sync": frozenset(),
    "bootstrap_validation": frozenset(),
}

# Union of every covered source. Excluded by construction (NOT bootstrap-
# covered): ``finra_short_interest`` / ``finra_regsho_daily`` (steady-state
# FINRA lanes) and ``sec_n_csr`` (steady-state discovery) — their freshness
# rows are seeded post-complete, not by a bootstrap stage.
BOOTSTRAP_COVERED_FRESHNESS_SOURCES: Final[frozenset[ManifestSource]] = frozenset(
    src for srcs in _BOOTSTRAP_STAGE_FRESHNESS_SOURCES.values() for src in srcs
)


__all__ = ["BOOTSTRAP_COVERED_FRESHNESS_SOURCES"]
