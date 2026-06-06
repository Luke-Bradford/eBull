"""Drift guard for the bootstrap freshness-source coverage map (#1511 / T5).

The map ``_BOOTSTRAP_STAGE_FRESHNESS_SOURCES`` decides which never-run jobs the
Processes verdict promotes to Current (covered + fresh) and which the
post-bootstrap kick may target (uncovered). If it drifts from the live
``_BOOTSTRAP_STAGE_SPECS`` it silently mis-classifies a source — false green or
spurious kick — so these assertions are load-bearing, mirroring
``tests/test_job_registry.py::test_registry_covers_every_bootstrap_stage``.
"""

from __future__ import annotations

import typing

from app.services.processes.bootstrap_coverage import (
    _BOOTSTRAP_STAGE_FRESHNESS_SOURCES,
    _ISSUER_FILING_METADATA,
    BOOTSTRAP_COVERED_FRESHNESS_SOURCES,
)
from app.services.sec_manifest import ManifestSource

_VALID_SOURCES: frozenset[str] = frozenset(typing.get_args(ManifestSource))


def test_every_bootstrap_stage_has_a_classification() -> None:
    """Key set == live stage_key set exactly — a new / renamed / removed stage
    forces a deliberate classification rather than silent staleness."""
    from app.services.bootstrap_orchestrator import _BOOTSTRAP_STAGE_SPECS

    spec_stage_keys = {stage.stage_key for stage in _BOOTSTRAP_STAGE_SPECS}
    map_keys = set(_BOOTSTRAP_STAGE_FRESHNESS_SOURCES)
    missing = spec_stage_keys - map_keys
    extra = map_keys - spec_stage_keys
    assert not missing, f"stages missing a coverage classification: {sorted(missing)}"
    assert not extra, f"coverage map has stale (non-existent) stage keys: {sorted(extra)}"


def test_all_classified_sources_are_valid_manifest_sources() -> None:
    """Typo guard — every mapped source is a real ``ManifestSource``."""
    for stage_key, sources in _BOOTSTRAP_STAGE_FRESHNESS_SOURCES.items():
        bad = set(sources).difference(_VALID_SOURCES)
        assert not bad, f"{stage_key!r} maps to non-ManifestSource value(s): {sorted(bad)}"


def test_covered_set_is_the_expected_union() -> None:
    """Regression pin on the union — catches an accidental coverage change."""
    expected: frozenset[ManifestSource] = frozenset(
        {
            "sec_8k",
            "sec_10k",
            "sec_10q",
            "sec_def14a",
            "sec_13d",
            "sec_13g",
            "sec_xbrl_facts",
            "sec_13f_hr",
            "sec_form3",
            "sec_form4",
            "sec_form5",
            "sec_n_port",
        }
    )
    assert BOOTSTRAP_COVERED_FRESHNESS_SOURCES == expected


def test_steady_state_sources_are_not_covered() -> None:
    """FINRA lanes + N-CSR are seeded post-complete, NOT by a bootstrap stage —
    they must stay uncovered so the kick can target a genuine gap and the
    look-through never falsely greens them."""
    for src in ("finra_short_interest", "finra_regsho_daily", "sec_n_csr"):
        assert src not in BOOTSTRAP_COVERED_FRESHNESS_SOURCES


def test_issuer_filing_metadata_matches_gap_close_set() -> None:
    """The local issuer filing-metadata literal is pinned EQUAL to the jobs-layer
    ``GAP_CLOSE_FILING_METADATA_SOURCES`` so the two cannot diverge."""
    from app.jobs.sec_master_idx_quarterly_sweep import GAP_CLOSE_FILING_METADATA_SOURCES

    assert _ISSUER_FILING_METADATA == GAP_CLOSE_FILING_METADATA_SOURCES
