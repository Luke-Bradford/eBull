"""Canonical mapping between capability provider tags and manifest sources (#941).

Two parallel string vocabularies exist:

* ``CapabilityProvider`` (``app.services.capabilities``) — operator-
  facing source tags returned by ``resolve_capabilities`` for the
  instrument-summary endpoint. Contains *bundles* like ``sec_13d_13g``
  and *index* tags like ``sec_edgar``.
* ``ManifestSource`` (``app.services.sec_manifest``) — per-form
  manifest enum used by ``sec_filing_manifest``. Splits bundles
  (``sec_13d`` + ``sec_13g``) and uses canonical form-code-derived
  names (``sec_13f_hr`` not ``sec_13f``).

Without an enforced mapping, coverage / evidence checks against
capability tags can disagree with what the manifest actually has.
``sec_13f`` (capability) ≠ ``sec_13f_hr`` (manifest) is not a typo —
they are two different vocabularies that happen to overlap in spelling.

This module locks the mapping. Tests in
``tests/test_capability_manifest_mapping.py`` enforce closure:
adding a new ``ManifestSource`` literal value without a mapping
entry — or without an explicit unmapped-reason — fails CI.

#941 / parent #935.
"""

from __future__ import annotations

from typing import get_args

from app.services.capabilities import CapabilityProvider
from app.services.sec_manifest import ManifestSource

# Capability tag → manifest source(s) that count as upstream evidence.
# Bundles (``sec_13d_13g``) split into multiple manifest sources;
# index tags (``sec_edgar``) span every issuer-scoped SEC source.
#
# Every entry is intentional. Add a new SEC capability tag here when
# extending ``CapabilityProvider``; add a new ``ManifestSource``
# entry below in ``_UNMAPPED_MANIFEST_SOURCES`` when the source has
# no capability tag yet (ETL-only / fund-only / not-SEC).
CAPABILITY_TO_MANIFEST_SOURCES: dict[CapabilityProvider, frozenset[ManifestSource]] = {
    "sec_form4": frozenset({"sec_form4"}),
    # ``sec_13f`` is the capability bundle; manifest splits 13F-HR
    # (the holdings report itself) from 13F-NT (notice-of-non-filing,
    # not currently in ``ManifestSource``).
    "sec_13f": frozenset({"sec_13f_hr"}),
    # ``sec_13d_13g`` is a capability bundle. Manifest splits the two.
    "sec_13d_13g": frozenset({"sec_13d", "sec_13g"}),
    "sec_8k_events": frozenset({"sec_8k"}),
    "sec_10k_item1": frozenset({"sec_10k"}),
    "sec_xbrl": frozenset({"sec_xbrl_facts"}),
    # ``sec_dividend_summary`` is derived from 8-K (Item 8.01); the
    # manifest gate is the upstream 8-K filing.
    "sec_dividend_summary": frozenset({"sec_8k"}),
    # ``sec_edgar`` is the filings INDEX — has no single per-form
    # source. Map to the union of every issuer-scoped SEC source so a
    # caller asking "does this instrument have any SEC filing in the
    # manifest?" gets a useful answer. Fund sources (``sec_n_port`` /
    # ``sec_n_csr``) are filer-scoped, not issuer-scoped — excluded.
    # ``finra_short_interest`` is a different provider family.
    "sec_edgar": frozenset(
        {
            "sec_form3",
            "sec_form4",
            "sec_form5",
            "sec_13d",
            "sec_13g",
            "sec_13f_hr",
            "sec_def14a",
            "sec_10k",
            "sec_10q",
            "sec_8k",
            "sec_xbrl_facts",
        }
    ),
}


# ManifestSource values that intentionally have NO capability mapping.
# Each entry must document the reason. The closure test in
# ``tests/test_capability_manifest_mapping.py`` asserts that every
# ``ManifestSource`` literal value lands either in
# ``CAPABILITY_TO_MANIFEST_SOURCES`` or here — no silent gaps.
_UNMAPPED_MANIFEST_SOURCES: dict[ManifestSource, str] = {
    # ``sec_n_port`` / ``sec_n_csr`` — fund sources. Capability surface
    # is equity-only today; fund coverage is exposed via the fund-
    # specific ownership rollup, not the per-instrument capability
    # cells. Adding ``sec_funds`` to ``CapabilityProvider`` would
    # let these sources participate; deferred to that work.
    "sec_n_port": (
        "Fund holdings (#917). Capability surface is equity-only; fund coverage flows through the fund-series rollup."
    ),
    "sec_n_csr": ("Fund disclosures (#918). Same scope gap as sec_n_port."),
    # FINRA, not SEC. ``CapabilityProvider`` has no FINRA tag; when
    # short-interest goes operator-visible it lands its own tag.
    "finra_short_interest": (
        "FINRA, not SEC — no capability tag yet. Add a `finra_short_interest` "
        "tag to ``CapabilityProvider`` and a mapping entry above when the "
        "panel goes live."
    ),
    "finra_regsho_daily": (
        "FINRA RegSHO daily short volume (#916). Same FINRA-not-SEC scope gap "
        "as finra_short_interest — add a `finra_regsho_daily` tag to "
        "``CapabilityProvider`` and a mapping entry above when the operator-"
        "visible memo overlay sparkline lands."
    ),
    # Late-filing notices (Form 12b-25). #1015 — an issuer red-flag SIGNAL
    # (surfaced via filing_events.red_flag_score + the filings list), not a
    # standing data-coverage capability: a healthy issuer files no NT, so an
    # absent NT is the norm, not missing coverage. No capability cell applies.
    "sec_nt": (
        "Late-filing notice (Form 12b-25, #1015). A red-flag signal, not a "
        "coverage capability — absence is the healthy default, so it has no "
        "per-instrument capability cell."
    ),
}


# Reverse index. Computed at import time so callers can look up which
# capability tags a given manifest source serves. ``frozenset`` so the
# value is hashable + immutable.
def _build_reverse_index() -> dict[ManifestSource, frozenset[CapabilityProvider]]:
    out: dict[ManifestSource, set[CapabilityProvider]] = {}
    for cap, sources in CAPABILITY_TO_MANIFEST_SOURCES.items():
        for src in sources:
            out.setdefault(src, set()).add(cap)
    return {src: frozenset(caps) for src, caps in out.items()}


MANIFEST_SOURCE_TO_CAPABILITIES: dict[ManifestSource, frozenset[CapabilityProvider]] = _build_reverse_index()


def manifest_sources_for_capability(
    capability: CapabilityProvider,
) -> frozenset[ManifestSource]:
    """Return manifest sources that count as evidence for ``capability``.

    Returns empty frozenset for capability tags with no SEC mapping
    (UK / EU / Asia / MENA / crypto / commodity / FX / Canada). Such
    a return is intentional, not an error — those tags map to non-SEC
    providers whose evidence lives outside ``sec_filing_manifest``.
    """
    return CAPABILITY_TO_MANIFEST_SOURCES.get(capability, frozenset())


def capabilities_for_manifest_source(
    source: ManifestSource,
) -> frozenset[CapabilityProvider]:
    """Return capability tags whose evidence includes ``source``.

    Returns empty frozenset for manifest sources listed in
    ``_UNMAPPED_MANIFEST_SOURCES`` — the absence is documented in
    that dict's per-entry reason.
    """
    return MANIFEST_SOURCE_TO_CAPABILITIES.get(source, frozenset())


def all_manifest_sources() -> frozenset[ManifestSource]:
    """Every value of the ``ManifestSource`` Literal as a frozenset."""
    return frozenset(get_args(ManifestSource))


# ---------------------------------------------------------------------------
# Ownership-category → manifest-source mapping (#1233 PR-D)
# ---------------------------------------------------------------------------
#
# Sibling of :data:`CAPABILITY_TO_MANIFEST_SOURCES`. Maps an ownership
# CATEGORY name (per ``app.jobs.ownership_observations_repair._CATEGORIES``)
# to the manifest source family that feeds that category's observations.
#
# Used by the Stream-C correctness gate runbook
# (``app/runbooks/stream_a_stream_c_gate.py``) C6 quiescence fallback:
# when a category has zero NEW observations post-Run-#8 we fall back to
# asking "did the upstream manifest source receive any rows recently?"
# Zero-and-zero → emit ``warning_category_quiescent_<cat>`` (NOT a fail
# — DEF 14A / treasury can legitimately be quiet across a 24h window).
#
# WHY HERE: this is the existing single-source-of-truth module for
# taxonomy mappings keyed on ``ManifestSource``. Co-locating with
# CAPABILITY_TO_MANIFEST_SOURCES keeps one type, one audit point.
#
# TREASURY maps to ``sec_xbrl_facts`` (NOT ``sec_def14a``). Treasury
# observations are derived from XBRL DEI facts via fundamentals_sync
# (``app/services/fundamentals/__init__.py`` ``record_treasury_observation``
# with source='xbrl_dei'), not from DEF 14A proxy parsing. PR-D v3 R3
# fold.
CATEGORY_TO_MANIFEST_SOURCES: dict[str, frozenset[ManifestSource]] = {
    "insiders": frozenset({"sec_form3", "sec_form4", "sec_form5"}),
    "institutions": frozenset({"sec_13f_hr"}),
    "blockholders": frozenset({"sec_13d", "sec_13g"}),
    "treasury": frozenset({"sec_xbrl_facts"}),
    "def14a": frozenset({"sec_def14a"}),
    "funds": frozenset({"sec_n_port", "sec_n_csr"}),
    "esop": frozenset({"sec_def14a"}),
}


def all_capability_providers() -> frozenset[CapabilityProvider]:
    """Every value of the ``CapabilityProvider`` Literal as a frozenset."""
    return frozenset(get_args(CapabilityProvider))
