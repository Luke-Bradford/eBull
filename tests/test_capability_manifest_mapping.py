"""Closure + bidirectional consistency tests for #941.

Locks the canonical mapping between ``CapabilityProvider``
(``app.services.capabilities``) and ``ManifestSource``
(``app.services.sec_manifest``). Adding a new SEC source without a
mapping entry — or without an explicit unmapped-reason — fails CI.
"""

from __future__ import annotations

import pytest

from app.services.capabilities import CapabilityProvider
from app.services.capability_manifest_mapping import (
    _UNMAPPED_MANIFEST_SOURCES,
    CAPABILITY_TO_MANIFEST_SOURCES,
    MANIFEST_SOURCE_TO_CAPABILITIES,
    all_capability_providers,
    all_manifest_sources,
    capabilities_for_manifest_source,
    manifest_sources_for_capability,
)
from app.services.sec_manifest import ManifestSource


class TestCapabilityKeysAreValid:
    def test_every_mapping_key_is_a_capability_provider(self) -> None:
        all_caps = all_capability_providers()
        unknown = set(CAPABILITY_TO_MANIFEST_SOURCES.keys()) - all_caps
        assert not unknown, (
            f"CAPABILITY_TO_MANIFEST_SOURCES has key(s) not declared in CapabilityProvider Literal: {sorted(unknown)}"
        )

    def test_every_mapping_value_contains_only_manifest_sources(self) -> None:
        all_sources = all_manifest_sources()
        for cap, sources in CAPABILITY_TO_MANIFEST_SOURCES.items():
            unknown = sources - all_sources
            assert not unknown, f"capability {cap!r} maps to non-Literal ManifestSource(s): {sorted(unknown)}"

    def test_every_mapping_value_is_non_empty(self) -> None:
        empty = [cap for cap, sources in CAPABILITY_TO_MANIFEST_SOURCES.items() if not sources]
        assert not empty, (
            "Mapped capabilities must list ≥1 manifest source. Capabilities "
            f"with empty mapping: {empty}. If a capability genuinely has no "
            "manifest evidence yet, omit it from the mapping rather than "
            "mapping to an empty frozenset."
        )


class TestUnmappedSourcesAreValid:
    def test_every_unmapped_key_is_a_manifest_source(self) -> None:
        all_sources = all_manifest_sources()
        unknown = set(_UNMAPPED_MANIFEST_SOURCES.keys()) - all_sources
        assert not unknown, (
            f"_UNMAPPED_MANIFEST_SOURCES has key(s) not declared in ManifestSource Literal: {sorted(unknown)}"
        )

    def test_every_unmapped_entry_documents_a_reason(self) -> None:
        empty = [src for src, reason in _UNMAPPED_MANIFEST_SOURCES.items() if not reason.strip()]
        assert not empty, (
            "_UNMAPPED_MANIFEST_SOURCES entries must explain WHY the source "
            f"has no capability tag. Empty reasons for: {empty}"
        )

    def test_unmapped_and_mapped_sets_are_disjoint(self) -> None:
        mapped = set().union(*CAPABILITY_TO_MANIFEST_SOURCES.values())
        unmapped = set(_UNMAPPED_MANIFEST_SOURCES.keys())
        overlap = mapped & unmapped
        assert not overlap, (
            "ManifestSource cannot be both mapped (has capability) and "
            f"unmapped (no capability). Overlap: {sorted(overlap)}"
        )


class TestClosure:
    def test_every_manifest_source_is_classified(self) -> None:
        # The contract: every value of the ManifestSource Literal
        # lands either in the mapping (has a capability) or in
        # _UNMAPPED_MANIFEST_SOURCES (documented absence). New literal
        # values without classification fail this test.
        all_sources = all_manifest_sources()
        mapped = set().union(*CAPABILITY_TO_MANIFEST_SOURCES.values())
        unmapped = set(_UNMAPPED_MANIFEST_SOURCES.keys())
        classified = mapped | unmapped
        missing = all_sources - classified
        assert not missing, (
            "ManifestSource literal value(s) lack BOTH a capability mapping AND "
            f"an unmapped-reason: {sorted(missing)}. Add either a mapping entry "
            "in CAPABILITY_TO_MANIFEST_SOURCES or an explicit reason in "
            "_UNMAPPED_MANIFEST_SOURCES."
        )


class TestBidirectionalConsistency:
    def test_reverse_index_matches_forward_mapping(self) -> None:
        # For every (capability, source) pair in the forward mapping,
        # the reverse must agree: capability ∈ MANIFEST_SOURCE_TO_CAPABILITIES[source].
        for cap, sources in CAPABILITY_TO_MANIFEST_SOURCES.items():
            for src in sources:
                back = MANIFEST_SOURCE_TO_CAPABILITIES.get(src, frozenset())
                assert cap in back, (
                    f"forward maps {cap!r} -> {src!r} but reverse index does not include {cap!r} for {src!r}: {back}"
                )

    def test_reverse_index_has_no_orphan_entries(self) -> None:
        # The reverse index is computed solely from the forward map,
        # so its keys must be a subset of all mapped sources.
        mapped_sources = set().union(*CAPABILITY_TO_MANIFEST_SOURCES.values())
        reverse_keys = set(MANIFEST_SOURCE_TO_CAPABILITIES.keys())
        orphan = reverse_keys - mapped_sources
        assert not orphan, f"reverse index has source(s) not in any forward mapping: {sorted(orphan)}"


class TestHelperContracts:
    def test_manifest_sources_for_known_capability(self) -> None:
        assert manifest_sources_for_capability("sec_form4") == frozenset({"sec_form4"})
        assert manifest_sources_for_capability("sec_13f") == frozenset({"sec_13f_hr"})
        assert manifest_sources_for_capability("sec_13d_13g") == frozenset({"sec_13d", "sec_13g"})

    def test_manifest_sources_for_unmapped_capability_returns_empty(self) -> None:
        # Non-SEC capability tags have no SEC manifest evidence; the
        # helper returns empty rather than raising.
        assert manifest_sources_for_capability("companies_house") == frozenset()
        assert manifest_sources_for_capability("hkex") == frozenset()

    def test_capabilities_for_known_source(self) -> None:
        assert capabilities_for_manifest_source("sec_form4") >= frozenset({"sec_form4"})
        assert capabilities_for_manifest_source("sec_13f_hr") >= frozenset({"sec_13f"})
        assert capabilities_for_manifest_source("sec_13d") >= frozenset({"sec_13d_13g"})
        assert capabilities_for_manifest_source("sec_13g") >= frozenset({"sec_13d_13g"})

    def test_capabilities_for_unmapped_source_returns_empty(self) -> None:
        # ``sec_n_port`` is documented in _UNMAPPED_MANIFEST_SOURCES;
        # has no capability today. Helper returns empty (caller decides
        # what to do — usually "panel hidden until tag wired").
        assert capabilities_for_manifest_source("sec_n_port") == frozenset()
        assert capabilities_for_manifest_source("finra_short_interest") == frozenset()

    def test_8k_source_serves_multiple_capabilities(self) -> None:
        # ``sec_8k`` backs both corporate_events (via sec_8k_events) and
        # the dividend summary (via sec_dividend_summary). The reverse
        # index should surface both.
        caps = capabilities_for_manifest_source("sec_8k")
        assert "sec_8k_events" in caps
        assert "sec_dividend_summary" in caps
        # And ``sec_edgar`` (filings index) covers it too.
        assert "sec_edgar" in caps


class TestReverseClosureForSecTags:
    # Capabilities whose tag begins with ``sec_`` but which intentionally
    # do NOT have a manifest mapping. Each entry must document the
    # reason. Empty today — every sec_* tag in CapabilityProvider has a
    # mapping. Adding a new sec_* tag without a mapping requires an
    # explicit entry here OR a mapping entry; the test below enforces
    # that choice.
    _SEC_TAGS_WITHOUT_MANIFEST_MAPPING: dict[str, str] = {}

    def test_every_sec_capability_tag_is_classified(self) -> None:
        # Codex pre-push: closure was one-way only — adding a new
        # ``sec_*`` capability tag without a mapping silently returned
        # empty from ``manifest_sources_for_capability`` as if the tag
        # were non-SEC. This test forces the choice: every ``sec_*``
        # capability either has a manifest mapping or is explicitly
        # listed (with reason) in
        # ``_SEC_TAGS_WITHOUT_MANIFEST_MAPPING``.
        sec_tags = {cap for cap in all_capability_providers() if cap.startswith("sec_")}
        mapped = set(CAPABILITY_TO_MANIFEST_SOURCES.keys()) & sec_tags
        explicitly_unmapped = set(self._SEC_TAGS_WITHOUT_MANIFEST_MAPPING.keys())
        classified = mapped | explicitly_unmapped
        missing = sec_tags - classified
        assert not missing, (
            "sec_* capability tag(s) lack BOTH a manifest mapping AND an "
            f"explicit reason: {sorted(missing)}. Add either a mapping entry "
            "in CAPABILITY_TO_MANIFEST_SOURCES or document the reason in "
            "_SEC_TAGS_WITHOUT_MANIFEST_MAPPING."
        )


class TestSecEdgarIndexSpan:
    def test_sec_edgar_spans_every_issuer_scoped_source(self) -> None:
        # ``sec_edgar`` is the filings INDEX — should cover every
        # issuer-scoped SEC manifest source, but NOT fund-only or
        # non-SEC sources.
        edgar_sources = manifest_sources_for_capability("sec_edgar")
        assert "sec_n_port" not in edgar_sources, (
            "sec_edgar must NOT include fund-only sources — they're filer-scoped, not issuer-scoped"
        )
        assert "sec_n_csr" not in edgar_sources
        assert "finra_short_interest" not in edgar_sources, (
            "sec_edgar is the SEC filings index — must not include FINRA"
        )
        # Issuer-scoped SEC sources that exist as manifest values
        # MUST be in the index span.
        for src in (
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
        ):
            assert src in edgar_sources, f"sec_edgar (filings index) must include issuer-scoped manifest source {src!r}"


@pytest.mark.parametrize(
    "capability,expected_subset",
    [
        ("sec_form4", {"sec_form4"}),
        ("sec_13f", {"sec_13f_hr"}),
        ("sec_13d_13g", {"sec_13d", "sec_13g"}),
        ("sec_8k_events", {"sec_8k"}),
        ("sec_10k_item1", {"sec_10k"}),
        ("sec_xbrl", {"sec_xbrl_facts"}),
        ("sec_dividend_summary", {"sec_8k"}),
    ],
)
def test_capability_manifest_smoke(
    capability: CapabilityProvider,
    expected_subset: set[ManifestSource],
) -> None:
    sources = manifest_sources_for_capability(capability)
    assert expected_subset <= sources, (
        f"capability {capability!r} expected to include {expected_subset} but got {sources}"
    )
