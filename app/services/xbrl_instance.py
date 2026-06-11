"""Shared pure helpers for XBRL instance-document parsing.

Extracted verbatim from ``n_csr_extractor`` (#554) so the dimensional
fact extractor (``dimensional_facts``) and the N-CSR fund-metadata
extractor share one implementation of context/dimension walking.

All helpers use wildcard-namespace matching by ``localname`` — different
filers emit different taxonomy versions and hard-coding namespace URIs
breaks yearly with each SEC taxonomy release (n_csr_extractor design
note, preserved here).
"""

from __future__ import annotations

import lxml.etree as ET

# Hardened parser for externally-fetched XML (SEC EDGAR artifacts).
# SEC XBRL never relies on DTD entity substitution, so disabling
# entity resolution / DTD loading / network access closes the XXE +
# entity-expansion window with no functional cost.
SAFE_XML_PARSER = ET.XMLParser(
    resolve_entities=False,
    load_dtd=False,
    no_network=True,
)


def axis_localname(axis_qname: str | None) -> str | None:
    """Strip namespace prefix from an axis QName (``oef:ClassAxis`` → ``ClassAxis``)."""
    if not axis_qname:
        return None
    val = axis_qname.strip()
    if ":" in val:
        val = val.rsplit(":", 1)[1]
    return val or None


def member_localname(member_ref: str | None) -> str | None:
    """Strip namespace prefix + ``Member`` suffix from a dimension member ref.

    ``fmr:C000203453Member`` → ``C000203453``
    ``oef:IndustrySectorOneMember`` → ``IndustrySectorOne``
    ``OneYearMember`` → ``OneYear``
    """
    if not member_ref:
        return None
    val = member_ref.strip()
    if ":" in val:
        val = val.rsplit(":", 1)[1]
    if val.endswith("Member"):
        val = val[: -len("Member")]
    return val or None


def context_dimensions(ctx_el: ET._Element) -> dict[str, str]:
    """Return ``{axis_qname: member_qname}`` for a context's segment members."""
    dims: dict[str, str] = {}
    seg = None
    for child in ctx_el.iter():
        local = ET.QName(child.tag).localname if isinstance(child.tag, str) else None
        if local == "segment":
            seg = child
            break
    if seg is None:
        return dims
    for m in seg:
        if not isinstance(m.tag, str):
            continue
        if ET.QName(m.tag).localname != "explicitMember":
            continue
        dim = m.get("dimension", "")
        if dim:
            dims[dim] = (m.text or "").strip()
    return dims


def context_period(ctx_el: ET._Element) -> dict[str, str]:
    """Return ``{startDate, endDate, instant}`` for a context's period."""
    period_info: dict[str, str] = {}
    period_el = None
    for child in ctx_el:
        if isinstance(child.tag, str) and ET.QName(child.tag).localname == "period":
            period_el = child
            break
    if period_el is None:
        return period_info
    for child in period_el:
        if not isinstance(child.tag, str):
            continue
        local = ET.QName(child.tag).localname
        if local in {"startDate", "endDate", "instant"}:
            period_info[local] = (child.text or "").strip()
    return period_info
