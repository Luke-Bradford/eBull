"""Pure-logic tests for dimensional XBRL extraction (#554).

No DB, no HTTP — synthetic instance/linkbase fixtures exercising the
discovery priority, the per-route exact axis-set rule, alias +
duplicate arbitration, the prevention-log §1455 sanity window, label
resolution, and both subtotal detectors.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.api.instruments import SEGMENT_AXIS_PARAM_TO_ENUM, SegmentAxisParam
from app.services.dimensional_facts import (
    DimensionalFact,
    discover_xbrl_files,
    extract_dimensional_facts,
    prettify_member,
)

# ---------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------

_NS = (
    'xmlns="http://www.xbrl.org/2003/instance" '
    'xmlns:us-gaap="http://fasb.org/us-gaap/2025" '
    'xmlns:acme="http://acme.example/20251231" '
    'xmlns:srt="http://fasb.org/srt/2025" '
    'xmlns:xbrldi="http://xbrl.org/2006/xbrldi" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
)


def _context(cid: str, dims: dict[str, str], *, start: str | None, end: str | None, instant: str | None = None) -> str:
    members = "".join(
        f'<xbrldi:explicitMember dimension="{axis}">{member}</xbrldi:explicitMember>' for axis, member in dims.items()
    )
    segment = f"<segment>{members}</segment>" if members else ""
    if instant is not None:
        period = f"<period><instant>{instant}</instant></period>"
    else:
        period = f"<period><startDate>{start}</startDate><endDate>{end}</endDate></period>"
    return (
        f'<context id="{cid}"><entity>'
        f'<identifier scheme="http://www.sec.gov/CIK">0000000001</identifier>'
        f"{segment}</entity>{period}</context>"
    )


def _fact(
    concept: str, cid: str, value: str, *, decimals: str | None = "-6", unit: str = "usd", nil: bool = False
) -> str:
    dec = f' decimals="{decimals}"' if decimals is not None else ""
    nil_attr = ' xsi:nil="true"' if nil else ""
    return f'<{concept} contextRef="{cid}" unitRef="{unit}"{dec}{nil_attr}>{value}</{concept}>'


def _instance(*parts: str) -> bytes:
    units = (
        '<unit id="usd"><measure>iso4217:USD</measure></unit>'
        '<unit id="ratio"><divide><unitNumerator><measure>iso4217:USD</measure></unitNumerator>'
        "<unitDenominator><measure>shares</measure></unitDenominator></divide></unit>"
    )
    return f"<xbrl {_NS}>{units}{''.join(parts)}</xbrl>".encode()


_FY = {"start": "2025-01-01", "end": "2025-12-31"}
_SEG_AXIS = "us-gaap:StatementBusinessSegmentsAxis"
_PROD_AXIS = "srt:ProductOrServiceAxis"
_GEO_AXIS = "srt:StatementGeographicalAxis"
_CONSOL_AXIS = "srt:ConsolidationItemsAxis"
_REV = "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"


def _extract(*parts: str, lab: bytes | None = None, dfn: bytes | None = None) -> list[DimensionalFact]:
    return extract_dimensional_facts(_instance(*parts), lab, dfn, accession="TEST-ACCN")


def _lab(entries: dict[str, str]) -> bytes:
    body = ""
    for i, (fragment, label) in enumerate(entries.items()):
        body += (
            f'<loc xlink:href="acme.xsd#{fragment}" xlink:label="loc{i}" xlink:type="locator"/>'
            f'<labelArc xlink:from="loc{i}" xlink:to="lab{i}" xlink:type="arc"/>'
            f'<label xlink:label="lab{i}" xlink:role="http://www.xbrl.org/2003/role/label"'
            f' xml:lang="en-US" xlink:type="resource">{label}</label>'
        )
    return (
        '<linkbase xmlns="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">'
        f"<labelLink>{body}</labelLink></linkbase>"
    ).encode()


def _def(pairs: list[tuple[str, str]]) -> bytes:
    locs: dict[str, int] = {}
    loc_xml = ""
    arc_xml = ""
    for parent, child in pairs:
        for frag in (parent, child):
            if frag not in locs:
                locs[frag] = len(locs)
                loc_xml += f'<loc xlink:href="acme.xsd#{frag}" xlink:label="loc{locs[frag]}" xlink:type="locator"/>'
        arc_xml += (
            f'<definitionArc xlink:arcrole="http://xbrl.org/int/dim/arcrole/domain-member"'
            f' xlink:from="loc{locs[parent]}" xlink:to="loc{locs[child]}" xlink:type="arc"/>'
        )
    return (
        '<linkbase xmlns="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">'
        f"<definitionLink>{loc_xml}{arc_xml}</definitionLink></linkbase>"
    ).encode()


def _index(names_sizes: list[tuple[str, str]]) -> dict[str, object]:
    return {"directory": {"item": [{"name": n, "size": s} for n, s in names_sizes]}}


# ---------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------


def test_discovery_inline_era_picks_htm_xml_with_linkbases() -> None:
    refs = discover_xbrl_files(
        _index(
            [
                ("acme-20251231.htm", "100"),
                ("acme-20251231_htm.xml", "900"),
                ("acme-20251231_lab.xml", "50"),
                ("acme-20251231_def.xml", "40"),
                ("R1.htm", "10"),
            ]
        ),
        primary_document_name="acme-20251231.htm",
    )
    assert refs is not None
    assert refs.instance_name == "acme-20251231_htm.xml"
    assert refs.label_name == "acme-20251231_lab.xml"
    assert refs.definition_name == "acme-20251231_def.xml"


def test_discovery_multiple_inline_candidates_prefers_primary_stem() -> None:
    refs = discover_xbrl_files(
        _index([("other_htm.xml", "9999"), ("acme-20251231_htm.xml", "5")]),
        primary_document_name="acme-20251231.htm",
    )
    assert refs is not None
    assert refs.instance_name == "acme-20251231_htm.xml"


def test_discovery_standalone_era_excludes_linkbases_and_rendering_artifacts() -> None:
    refs = discover_xbrl_files(
        _index(
            [
                ("acme-20111231.xml", "800"),
                ("acme-20111231_cal.xml", "100"),
                ("acme-20111231_lab.xml", "100"),
                ("acme-20111231_pre.xml", "100"),
                ("R3.xml", "999"),
                ("FilingSummary.xml", "999"),
            ]
        ),
        primary_document_name="acme-20111231.htm",
    )
    assert refs is not None
    assert refs.instance_name == "acme-20111231.xml"


def test_discovery_no_xbrl_returns_none() -> None:
    assert discover_xbrl_files(_index([("acme.htm", "10")]), primary_document_name="acme.htm") is None
    assert discover_xbrl_files({}, primary_document_name=None) is None


def test_discovery_xsd_fallback_when_no_standalone_linkbases() -> None:
    refs = discover_xbrl_files(
        _index([("msft-20250630_htm.xml", "900"), ("msft-20250630.xsd", "200")]),
        primary_document_name="msft-20250630.htm",
    )
    assert refs is not None
    assert refs.label_name == "msft-20250630.xsd"
    assert refs.definition_name == "msft-20250630.xsd"


# ---------------------------------------------------------------------
# Axis routing — per-route EXACT dimension sets
# ---------------------------------------------------------------------


def test_business_segment_route_accepts_bare_and_operating_segments_co_axis() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _context(
            "c2",
            {_SEG_AXIS: "acme:SouthMember", _CONSOL_AXIS: "us-gaap:OperatingSegmentsMember"},
            **_FY,
        ),
        _fact(_REV, "c1", "600"),
        _fact(_REV, "c2", "400"),
    )
    assert [(f.axis, f.member_qname, f.val) for f in facts] == [
        ("business_segment", "acme:NorthMember", Decimal("600")),
        ("business_segment", "acme:SouthMember", Decimal("400")),
    ]


def test_eliminations_member_on_consolidation_axis_is_excluded() -> None:
    facts = _extract(
        _context(
            "c1",
            {_SEG_AXIS: "acme:NorthMember", _CONSOL_AXIS: "us-gaap:IntersegmentEliminationMember"},
            **_FY,
        ),
        _fact(_REV, "c1", "600"),
    )
    assert facts == []


def test_cross_axis_context_is_excluded() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember", _PROD_AXIS: "acme:WidgetMember"}, **_FY),
        _fact(_REV, "c1", "600"),
    )
    assert facts == []


def test_product_and_geographic_routes_accept_revenue_only() -> None:
    facts = _extract(
        _context("c1", {_PROD_AXIS: "acme:WidgetMember"}, **_FY),
        _context("c2", {_GEO_AXIS: "country:US"}, **_FY),
        _fact(_REV, "c1", "100"),
        _fact("us-gaap:OperatingIncomeLoss", "c1", "40"),  # op income on product axis: skipped
        _fact(_REV, "c2", "70"),
    )
    assert [(f.axis, f.metric, f.member_qname) for f in facts] == [
        ("geographic", "revenue", "country:US"),
        ("product_service", "revenue", "acme:WidgetMember"),
    ]


def test_assets_is_instant_and_segment_scoped() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, start=None, end=None, instant="2025-12-31"),
        _fact("us-gaap:Assets", "c1", "5000"),
    )
    assert len(facts) == 1
    assert facts[0].metric == "assets"
    assert facts[0].period_start is None
    assert facts[0].period_end == date(2025, 12, 31)


def test_extension_namespace_concepts_are_skipped() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact("acme:Revenues", "c1", "600"),
    )
    assert facts == []


def test_nil_facts_and_ratio_units_are_skipped() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact(_REV, "c1", "", nil=True),
        _fact(_REV, "c1", "600", unit="ratio"),
    )
    assert facts == []


# ---------------------------------------------------------------------
# Sanity window (prevention log §1455)
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2025-01-01", "6016-12-31"),  # digit-overflow year
        ("0205-01-01", "2025-12-31"),  # digit-truncation year
        ("2025-12-31", "2025-01-01"),  # start after end
    ],
)
def test_sanity_window_rejects_out_of_window_periods(start: str, end: str) -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, start=start, end=end),
        _fact(_REV, "c1", "600"),
    )
    assert facts == []


def test_duration_metric_without_start_date_is_rejected() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, start=None, end=None, instant="2025-12-31"),
        _fact(_REV, "c1", "600"),
    )
    assert facts == []


# ---------------------------------------------------------------------
# Alias + duplicate arbitration
# ---------------------------------------------------------------------


def test_revenue_alias_priority_wins_per_member() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact("us-gaap:Revenues", "c1", "999"),
        _fact(_REV, "c1", "600"),  # higher-priority alias
    )
    assert len(facts) == 1
    assert facts[0].val == Decimal("600")


def test_members_union_across_aliases() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _context("c2", {_SEG_AXIS: "acme:SouthMember"}, **_FY),
        _fact(_REV, "c1", "600"),
        _fact("us-gaap:Revenues", "c2", "400"),  # different member, lower-priority alias
    )
    assert {(f.member_qname, f.val) for f in facts} == {
        ("acme:NorthMember", Decimal("600")),
        ("acme:SouthMember", Decimal("400")),
    }


def test_duplicate_same_value_collapses_silently() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact(_REV, "c1", "600"),
        _fact(_REV, "c1", "600"),
    )
    assert len(facts) == 1


def test_duplicate_differing_values_higher_precision_wins() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact(_REV, "c1", "600", decimals="-6"),
        _fact(_REV, "c1", "601", decimals="INF"),
    )
    assert len(facts) == 1
    assert facts[0].val == Decimal("601")


def test_duplicate_equal_precision_conflict_drops_member() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact(_REV, "c1", "600", decimals="-6"),
        _fact(_REV, "c1", "601", decimals="-6"),
    )
    assert facts == []


# ---------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------


def test_label_from_linkbase_with_member_suffix_stripped() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _fact(_REV, "c1", "600"),
        lab=_lab({"acme_NorthMember": "Northern Operations [Member]"}),
    )
    assert facts[0].member_label == "Northern Operations"


def test_label_fallback_prettifies_localname() -> None:
    facts = _extract(
        _context("c1", {_SEG_AXIS: "acme:NorthAmericaRetailMember"}, **_FY),
        _fact(_REV, "c1", "600"),
    )
    assert facts[0].member_label == "North America Retail"
    assert prettify_member("country:US") == "US"


# ---------------------------------------------------------------------
# Subtotal detection
# ---------------------------------------------------------------------


def test_subtotal_marked_via_definition_linkbase_nesting() -> None:
    facts = _extract(
        _context("c1", {_PROD_AXIS: "us-gaap:ProductMember"}, **_FY),
        _context("c2", {_PROD_AXIS: "acme:WidgetMember"}, **_FY),
        _context("c3", {_PROD_AXIS: "acme:GadgetMember"}, **_FY),
        _fact(_REV, "c1", "100"),
        _fact(_REV, "c2", "60"),
        _fact(_REV, "c3", "40"),
        dfn=_def([("us-gaap_ProductMember", "acme_WidgetMember"), ("us-gaap_ProductMember", "acme_GadgetMember")]),
    )
    by_member = {f.member_qname: f.is_subtotal for f in facts}
    assert by_member == {"us-gaap:ProductMember": True, "acme:WidgetMember": False, "acme:GadgetMember": False}


def test_linkbase_subtotal_scoped_per_period_not_per_axis() -> None:
    # Parent reports FY2025 + FY2024; the child exists only in FY2024.
    # FY2024 parent is a subtotal; FY2025 parent is the finest grain
    # the filer gave us and must stay a leaf (Codex pre-push finding).
    prior = {"start": "2024-01-01", "end": "2024-12-31"}
    facts = _extract(
        _context("c1", {_PROD_AXIS: "us-gaap:ProductMember"}, **_FY),
        _context("c2", {_PROD_AXIS: "us-gaap:ProductMember"}, **prior),
        _context("c3", {_PROD_AXIS: "acme:WidgetMember"}, **prior),
        _fact(_REV, "c1", "120"),
        _fact(_REV, "c2", "100"),
        _fact(_REV, "c3", "100"),
        dfn=_def([("us-gaap_ProductMember", "acme_WidgetMember")]),
    )
    by_key = {(f.member_qname, f.period_end.year): f.is_subtotal for f in facts}
    assert by_key == {
        ("us-gaap:ProductMember", 2025): False,
        ("us-gaap:ProductMember", 2024): True,
        ("acme:WidgetMember", 2024): False,
    }


def test_scenario_container_dimensions_are_routed() -> None:
    # Valid XBRL may carry explicit dimensions under <scenario> rather
    # than <segment>; treating those contexts as dimensionless would
    # both drop the fact and pollute the consolidated anchor.
    scenario_ctx = (
        '<context id="cs"><entity>'
        '<identifier scheme="http://www.sec.gov/CIK">0000000001</identifier>'
        "</entity><period><startDate>2025-01-01</startDate><endDate>2025-12-31</endDate></period>"
        "<scenario><xbrldi:explicitMember "
        'dimension="us-gaap:StatementBusinessSegmentsAxis">acme:NorthMember</xbrldi:explicitMember>'
        "</scenario></context>"
    )
    facts = _extract(scenario_ctx, _fact(_REV, "cs", "600"))
    assert [(f.axis, f.member_qname, f.val) for f in facts] == [
        ("business_segment", "acme:NorthMember", Decimal("600")),
    ]


def test_subtotal_marked_via_value_overage_when_linkbase_flat() -> None:
    # AAPL shape: flat linkbase, Product = Widget + Gadget, consolidated
    # total present as a dimensionless fact in the same instance.
    facts = _extract(
        _context("c0", {}, **_FY),
        _context("c1", {_PROD_AXIS: "us-gaap:ProductMember"}, **_FY),
        _context("c2", {_PROD_AXIS: "acme:WidgetMember"}, **_FY),
        _context("c3", {_PROD_AXIS: "acme:GadgetMember"}, **_FY),
        _context("c4", {_PROD_AXIS: "us-gaap:ServiceMember"}, **_FY),
        _fact(_REV, "c0", "130"),  # consolidated anchor
        _fact(_REV, "c1", "100"),
        _fact(_REV, "c2", "60"),
        _fact(_REV, "c3", "40"),
        _fact(_REV, "c4", "30"),
    )
    by_member = {f.member_qname: f.is_subtotal for f in facts}
    assert by_member == {
        "us-gaap:ProductMember": True,
        "acme:WidgetMember": False,
        "acme:GadgetMember": False,
        "us-gaap:ServiceMember": False,
    }


def test_value_overage_skips_business_segment_axis() -> None:
    # Segment sums legitimately differ from consolidated (unallocated
    # corporate) — no member may be marked from the mismatch.
    facts = _extract(
        _context("c0", {}, **_FY),
        _context("c1", {_SEG_AXIS: "acme:NorthMember"}, **_FY),
        _context("c2", {_SEG_AXIS: "acme:SouthMember"}, **_FY),
        _fact(_REV, "c0", "90"),
        _fact(_REV, "c1", "60"),
        _fact(_REV, "c2", "40"),
    )
    assert all(not f.is_subtotal for f in facts)


def test_value_overage_ambiguous_marks_nothing() -> None:
    # Two members each equal the overage — ambiguous, nothing marked.
    facts = _extract(
        _context("c0", {}, **_FY),
        _context("c1", {_GEO_AXIS: "acme:AMember"}, **_FY),
        _context("c2", {_GEO_AXIS: "acme:BMember"}, **_FY),
        _context("c3", {_GEO_AXIS: "acme:CMember"}, **_FY),
        _fact(_REV, "c0", "50"),
        _fact(_REV, "c1", "25"),
        _fact(_REV, "c2", "25"),
        _fact(_REV, "c3", "25"),
    )
    assert all(not f.is_subtotal for f in facts)


def test_partial_disaggregation_below_total_marks_nothing() -> None:
    facts = _extract(
        _context("c0", {}, **_FY),
        _context("c1", {_GEO_AXIS: "country:US"}, **_FY),
        _fact(_REV, "c0", "100"),
        _fact(_REV, "c1", "60"),
    )
    assert all(not f.is_subtotal for f in facts)


# ---------------------------------------------------------------------
# API axis-param mapping pin (spec §D6, Codex ckpt-1 LOW)
# ---------------------------------------------------------------------


def test_axis_param_to_storage_enum_mapping_is_pinned() -> None:
    assert SEGMENT_AXIS_PARAM_TO_ENUM == {
        "business": "business_segment",
        "product": "product_service",
        "geographic": "geographic",
    }
    from typing import get_args

    assert set(get_args(SegmentAxisParam)) == set(SEGMENT_AXIS_PARAM_TO_ENUM)
