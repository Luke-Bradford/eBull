"""Unit tests for ``app.services.business_summary`` (#428).

Fixtures model real 10-K shapes: heavy HTML, table-of-contents link
that repeats the "Item 1" heading, iXBRL tags, and the standard
"Item 1A. Risk Factors" boundary marker that terminates the
business section.
"""

from __future__ import annotations

import pytest

from app.services.business_summary import (
    MAX_BODY_BYTES,
    ParsedBusinessSection,
    ParsedCrossReference,
    extract_business_section,
    extract_business_sections,
)


class TestExtractBusinessSection:
    def test_canonical_item_1_between_markers(self) -> None:
        """Happy-path 10-K layout: table of contents lists Item 1, the
        narrative starts later under its own heading, and
        ``Item 1A. Risk Factors`` marks the end."""
        html = """
        <html><body>
        <h2>Table of Contents</h2>
        <p>Item 1. Business .... 3</p>
        <p>Item 1A. Risk Factors ... 10</p>
        <h2>Item 1. Business</h2>
        <p>The Company is a global manufacturer of specialty
           materials used in aerospace and automotive end markets.</p>
        <p>We operate through four segments: Industrial, Safety,
           Transportation, and Consumer.</p>
        <h2>Item 1A. Risk Factors</h2>
        <p>The following factors may affect our results.</p>
        </body></html>
        """
        body = extract_business_section(html)
        assert body is not None
        assert "global manufacturer" in body
        assert "four segments" in body
        # End marker and anything after must be excluded.
        assert "Risk Factors" not in body
        assert "following factors" not in body

    def test_toc_only_returns_short_fragment(self) -> None:
        """A document whose only Item 1 mention is in the TOC (with no
        body heading before Item 1A) yields a short TOC-line fragment.
        The parser intentionally returns it rather than None so the
        ingester can apply the ``_MIN_BODY_LEN`` threshold consistently
        at one layer — short fragments fail that gate and get
        tombstoned as parse misses."""
        html = """
        <html><body>
        <p>Item 1. Business .... 3</p>
        <p>Item 1A. Risk Factors ... 10</p>
        <h2>Item 1A. Risk Factors</h2>
        <p>Risks follow.</p>
        </body></html>
        """
        # The extractor walks to the LAST occurrence of the Item 1
        # marker before Item 1A, so when only the TOC entry exists
        # the extracted body is the TOC line fragment. Accept: the
        # body will be <some tiny string>; callers can enforce a
        # minimum length (done at ingester layer). The unit contract
        # here is "don't crash, return something deterministic".
        body = extract_business_section(html)
        assert body is not None
        # Empty or single-line TOC fragment — service enforces min length.
        assert len(body) < 100

    def test_ixbrl_tags_stripped(self) -> None:
        """Real 10-Ks are iXBRL-inline. The ``<ix:...>`` tags must
        not leak into the stored body."""
        html = """
        <html><body>
        <h2>Item 1. Business</h2>
        <p>We reported <ix:nonfraction name="us-gaap:Revenues"
           contextref="c1" unitref="usd">1000000</ix:nonfraction>
           in revenue last year.</p>
        <h2>Item 1A. Risk Factors</h2>
        </body></html>
        """
        body = extract_business_section(html)
        assert body is not None
        assert "<ix" not in body
        assert "nonfraction" not in body
        assert "contextref" not in body
        # The numeric value is inside the ix tag — it stays as text.
        assert "1000000" in body

    def test_body_truncated_to_cap(self) -> None:
        """Body is capped at MAX_BODY_BYTES so oversized filings
        don't bloat the row."""
        filler = "A specialty chemicals company. " * 10000  # ~300 KB
        html = f"<html><body><h2>Item 1. Business</h2><p>{filler}</p><h2>Item 1A. Risk Factors</h2></body></html>"
        body = extract_business_section(html)
        assert body is not None
        assert len(body.encode("utf-8")) <= MAX_BODY_BYTES

    def test_no_item_1_marker_returns_none(self) -> None:
        """A document without the Item 1 heading returns None rather
        than guessing."""
        html = "<html><body><p>No financial disclosures.</p></body></html>"
        assert extract_business_section(html) is None

    def test_item_1_without_end_marker_takes_bounded_tail(self) -> None:
        """If Item 1A is absent (malformed 10-K), take at most the
        capped byte-count after the Item 1 heading so the extractor
        doesn't swallow the entire remainder of the filing."""
        html = (
            "<html><body>"
            "<h2>Item 1. Business</h2>"
            "<p>We make things. We sell them. Customers buy them.</p>"
            "</body></html>"
        )
        body = extract_business_section(html)
        assert body is not None
        assert "We make things" in body
        assert len(body.encode("utf-8")) <= MAX_BODY_BYTES

    def test_empty_input_returns_none(self) -> None:
        assert extract_business_section("") is None

    def test_whitespace_collapsed_to_single_space(self) -> None:
        """Multi-line + nbsp in the source collapses to a single
        space stream so the stored body is clean to render."""
        html = (
            "<html><body>"
            "<h2>Item&nbsp;1.&nbsp;Business</h2>"
            "<p>Line one.</p>\n\n\n<p>Line&nbsp;two.</p>"
            "<h2>Item 1A. Risk Factors</h2>"
            "</body></html>"
        )
        body = extract_business_section(html)
        assert body is not None
        # No doubled whitespace, no raw &nbsp; sequences.
        assert "  " not in body
        assert "&nbsp;" not in body
        assert "Line one." in body
        assert "Line two." in body

    @pytest.mark.parametrize(
        "heading",
        [
            "Item 1. Business",
            "ITEM 1. BUSINESS",
            "Item 1.    Business",
            "Item  1.  Business",
        ],
    )
    def test_case_and_whitespace_tolerant(self, heading: str) -> None:
        """Real filings vary the exact casing + spacing in the Item 1
        heading. Extractor matches all of them."""
        html = f"<html><body><h2>{heading}</h2><p>We are a company.</p><h2>Item 1A. Risk Factors</h2></body></html>"
        body = extract_business_section(html)
        assert body is not None
        assert "We are a company" in body


class TestExtractBusinessSections:
    """#449 — subsection-level extraction."""

    def test_canonical_subsections_mapped_and_ordered(self) -> None:
        """A 10-K with named subsections: General intro, Products,
        Competition, Human Capital, Regulation. Each heading lands as
        its own section with the canonical section_key and the
        verbatim label preserved."""
        html = """
        <html><body>
        <h2>Item 1. Business</h2>
        <p>The Company is a global manufacturer of industrial
           materials, incorporated in Delaware in 2001.</p>
        <h3>Products</h3>
        <p>Our products span three categories: coatings, films, and
           adhesives. See Item 7 for segment-level revenue breakdown.</p>
        <h3>Competition</h3>
        <p>Competitors include Acme Corp and GlobalChem Inc.</p>
        <h3>Human Capital Resources</h3>
        <p>As of fiscal year end, we employed 12,400 people across 24
           countries.</p>
        <h3>Government Regulation</h3>
        <p>Our operations are subject to environmental regulations
           described in Note 15 of the financial statements.</p>
        <h2>Item 1A. Risk Factors</h2>
        <p>The following risks affect our business.</p>
        </body></html>
        """
        sections = extract_business_sections(html)
        assert len(sections) >= 5
        keys = [s.section_key for s in sections]
        labels = [s.section_label for s in sections]
        # The pre-heading general block is always the first.
        assert keys[0] == "general"
        assert "global manufacturer" in sections[0].body
        # Named subsections map to canonical keys.
        assert "products" in keys
        assert "competition" in keys
        assert "human_capital" in keys
        assert "regulatory" in keys
        # Verbatim label preserved (not collapsed to canonical key).
        assert "Human Capital Resources" in labels

    def test_unmapped_heading_falls_through_as_other(self) -> None:
        """A heading we don't recognise doesn't get silently dropped —
        it lands as section_key='other' with the verbatim label."""
        html = """
        <html><body>
        <h2>Item 1. Business</h2>
        <p>Overview text.</p>
        <h3>Our Approach to Quantum Something</h3>
        <p>Details about an obscure subsection.</p>
        <h2>Item 1A. Risk Factors</h2>
        </body></html>
        """
        sections = extract_business_sections(html)
        others = [s for s in sections if s.section_key == "other"]
        assert len(others) >= 1
        assert any("Quantum" in o.section_label for o in others)
        assert any("obscure subsection" in o.body for o in others)

    def test_cross_references_captured_per_section(self) -> None:
        html = """
        <html><body>
        <h2>Item 1. Business</h2>
        <p>See Item 7 and Item 1A for more.</p>
        <h3>Products</h3>
        <p>Revenue breakdowns in Exhibit 21 and Note 15.</p>
        <h2>Item 1A. Risk Factors</h2>
        </body></html>
        """
        sections = extract_business_sections(html)
        assert sections
        # General block: item refs.
        general = next(s for s in sections if s.section_key == "general")
        ref_types = {r.reference_type for r in general.cross_references}
        ref_targets = {r.target for r in general.cross_references}
        assert "item" in ref_types
        assert "Item 7" in ref_targets
        # Products block: exhibit + note refs.
        products = next((s for s in sections if s.section_key == "products"), None)
        assert products is not None
        prod_refs = {(r.reference_type, r.target) for r in products.cross_references}
        assert ("exhibit", "Exhibit 21") in prod_refs
        assert ("note", "Note 15") in prod_refs

    def test_no_subsections_emits_single_general_block(self) -> None:
        """A 10-K with no recognisable subsection headings falls back
        to a single ``general`` block carrying the full Item 1 body."""
        html = """
        <html><body>
        <h2>Item 1. Business</h2>
        <p>One long paragraph describing the business without internal
           headings. This is a large part of legacy 10-K filings that
           used a single narrative block.</p>
        <h2>Item 1A. Risk Factors</h2>
        </body></html>
        """
        sections = extract_business_sections(html)
        assert len(sections) == 1
        assert sections[0].section_key == "general"
        assert "One long paragraph" in sections[0].body

    def test_empty_input_returns_empty(self) -> None:
        assert extract_business_sections("") == ()
        assert extract_business_sections("<html></html>") == ()

    def test_parsed_shape(self) -> None:
        html = """
        <html><body>
        <h2>Item 1. Business</h2>
        <p>We do things. See Item 7.</p>
        <h2>Item 1A. Risk Factors</h2>
        </body></html>
        """
        sections = extract_business_sections(html)
        assert sections
        assert isinstance(sections[0], ParsedBusinessSection)
        if sections[0].cross_references:
            assert isinstance(sections[0].cross_references[0], ParsedCrossReference)
