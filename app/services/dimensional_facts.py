"""Dimensional XBRL fact extraction — segments / product mix / geography (#554).

Pure module: file discovery over a filing's ``index.json`` listing +
fact extraction over the filing's XBRL instance document. No DB, no
HTTP — the DB layer lives in ``dimensional_facts_store`` and the
fetch/orchestration in ``manifest_parsers/sec_10k`` step 2.

Why per-filing instance parsing at all: the SEC companyfacts API
carries NO dimensional facts (verified 2026-06-11, spec §1 —
``docs/proposals/etl/2026-06-11-554-xbrl-dimensional-facts.md``), so
segment/product/geographic breakdowns only exist inside each filing's
own instance document.

Axis routing is per-route EXACT (spec §D3): a context is accepted only
when its dimension set matches one of the three allowed shapes; any
extra axis (segment×product cross-dimensions, elimination members)
is excluded to prevent double-counting.

Concept scope is us-gaap only. Filer-extension revenue concepts are a
conscious v1 undercount (spec §D3) — ``pct_of_total`` is computed over
returned rows downstream so tables stay internally consistent.

Period sanity window per prevention log §1455: ``[1900-01-01,
2100-01-01)`` + ``period_start <= period_end``; violations are
rejected + WARN'd with provenance, never written.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, InvalidOperation
from itertools import combinations
from typing import Literal

import lxml.etree as ET

from app.providers.implementations.sec_fundamentals import TRACKED_CONCEPTS
from app.services.xbrl_instance import (
    SAFE_XML_PARSER,
    axis_localname,
    context_dimensions,
    context_period,
    member_localname,
)

logger = logging.getLogger(__name__)

DimensionalAxis = Literal["business_segment", "product_service", "geographic", "award_type"]
DimensionalMetric = Literal["revenue", "operating_income", "assets", "nonvested_awards"]

# Axes the #554 per-filing extractor (and the #1590 FSDS bulk loader) own.
# The FSNDS notes loader (#844) owns ``award_type`` EXCLUSIVELY — the
# per-filing rewash's delete-then-insert and the FSDS bulk existence
# check are BOTH scoped to this tuple so neither wipes nor blocks the
# other family's rows for the same 10-K accession.
PER_FILING_AXES: tuple[DimensionalAxis, ...] = ("business_segment", "product_service", "geographic")

# Period sanity window (prevention log §1455 — same bounds as the
# sec_fundamentals parser guard).
_SANITY_MIN = date(1900, 1, 1)
_SANITY_MAX = date(2100, 1, 1)  # exclusive

# Concept localname → (metric, alias_priority). Revenue aliases come
# from the single source of truth in the fundamentals provider
# (TRACKED_CONCEPTS — tuple order IS the priority order); operating
# income + assets are single-concept.
_CONCEPT_TO_METRIC: dict[str, tuple[DimensionalMetric, int]] = {
    **{tag: ("revenue", i) for i, tag in enumerate(TRACKED_CONCEPTS["revenue"])},
    **{tag: ("operating_income", i) for i, tag in enumerate(TRACKED_CONCEPTS["operating_income"])},
    **{tag: ("assets", i) for i, tag in enumerate(TRACKED_CONCEPTS["total_assets"])},
}

# Duration metrics carry startDate+endDate contexts; instant metrics
# carry instant contexts. Anything else for that metric is skipped.
_INSTANT_METRICS: frozenset[str] = frozenset({"assets"})

# Metrics allowed per axis route (spec §D3 table; award_type per #844).
_AXIS_METRICS: dict[DimensionalAxis, frozenset[str]] = {
    "business_segment": frozenset({"revenue", "operating_income", "assets"}),
    "product_service": frozenset({"revenue"}),
    "geographic": frozenset({"revenue"}),
    "award_type": frozenset({"nonvested_awards"}),
}

_XSI_NIL = "{http://www.w3.org/2001/XMLSchema-instance}nil"

# index.json names that are never the instance document.
_NON_INSTANCE_XML = re.compile(
    r"(_cal|_def|_lab|_pre|_ref)\.xml$|^r\d+\.xml$|^filingsummary\.xml$|index",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class XbrlFileRefs:
    """File names (within the accession archive) located by discovery."""

    instance_name: str
    label_name: str | None
    definition_name: str | None


@dataclass(frozen=True)
class DimensionalFact:
    """One extracted dimensional fact, ready for the store layer."""

    axis: DimensionalAxis
    member_qname: str
    member_label: str
    metric: DimensionalMetric
    unit: str
    is_subtotal: bool
    period_start: date | None
    period_end: date
    val: Decimal
    decimals: str | None


# ---------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------


def _stem(name: str) -> str:
    base = name.rsplit("/", 1)[-1]
    return base.split(".", 1)[0].removesuffix("_htm")


def discover_xbrl_files(
    raw_index: Mapping[str, object],
    *,
    primary_document_name: str | None,
) -> XbrlFileRefs | None:
    """Locate the XBRL instance (+ optional label linkbase) in a filing's
    ``index.json`` listing.

    The listing has NO SEC document-type labels (``index.json``'s
    ``type`` is a content-type icon — see ``filing_documents`` module
    note), so discovery is by name shape, deterministic priority:

    1. Inline-XBRL era: files ending ``_htm.xml`` (the EDGAR-generated
       extracted instance). Several → prefer the one whose stem matches
       the primary document's stem, else largest size + WARN.
    2. Standalone-instance era: ``.xml`` files that are not linkbases
       (``_cal/_def/_lab/_pre/_ref``), not rendering artifacts
       (``R<n>.xml``, ``FilingSummary.xml``), not index files. Same
       stem-match → largest-size tie-break.

    Returns ``None`` when no candidate exists (pre-XBRL-mandate filings
    — the caller treats that as a clean no-XBRL skip, not an error).
    """
    directory = raw_index.get("directory")
    if not isinstance(directory, dict):
        return None
    items = directory.get("item")
    if not isinstance(items, list):
        return None

    names_sizes: list[tuple[str, int]] = []
    for entry in items:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            continue
        size_raw = entry.get("size")
        try:
            size = int(size_raw) if size_raw not in (None, "") else 0
        except TypeError, ValueError:
            size = 0
        names_sizes.append((name, size))

    primary_stem = _stem(primary_document_name) if primary_document_name else None

    def _pick(candidates: list[tuple[str, int]]) -> str | None:
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0][0]
        if primary_stem is not None:
            stem_matches = [c for c in candidates if _stem(c[0]) == primary_stem]
            if len(stem_matches) == 1:
                return stem_matches[0][0]
            if stem_matches:
                candidates = stem_matches
        # Deterministic tie-break: largest size, then name. The instance
        # is by far the largest XBRL artifact in practice.
        chosen = sorted(candidates, key=lambda c: (-c[1], c[0]))[0][0]
        logger.warning(
            "dimensional facts: ambiguous instance candidates %s; chose %s by size",
            sorted(c[0] for c in candidates),
            chosen,
        )
        return chosen

    inline = [(n, s) for n, s in names_sizes if n.lower().endswith("_htm.xml")]
    instance = _pick(inline)
    if instance is None:
        standalone = [
            (n, s)
            for n, s in names_sizes
            if n.lower().endswith(".xml") and not _NON_INSTANCE_XML.search(n.rsplit("/", 1)[-1].lower())
        ]
        instance = _pick(standalone)
    if instance is None:
        return None

    def _sibling(suffix: str) -> str | None:
        matches = sorted(n for n, _ in names_sizes if n.lower().endswith(suffix))
        if not matches:
            return None
        if primary_stem is not None:
            return next((n for n in matches if _stem(n).startswith(primary_stem)), matches[0])
        return matches[0]

    # Some filer software (e.g. Workiva — MSFT) ships NO standalone
    # linkbase files and embeds labelLink/definitionLink inside the
    # schema's <annotation>. The parsers match by localname, so the
    # .xsd is a drop-in source for both.
    schema = _sibling(".xsd")
    return XbrlFileRefs(
        instance_name=instance,
        label_name=_sibling("_lab.xml") or schema,
        definition_name=_sibling("_def.xml") or schema,
    )


# ---------------------------------------------------------------------
# Label linkbase
# ---------------------------------------------------------------------

_STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
_XLINK = "http://www.w3.org/1999/xlink"


def parse_label_linkbase(label_xml: bytes) -> dict[str, str]:
    """Parse a filing's ``*_lab.xml`` into ``{element_id_fragment: label}``.

    Keys are the schema-fragment ids the linkbase locators point at
    (``aapl_IPhoneMember``). Standard-role labels win; any-role label
    is the fallback. Malformed XML returns ``{}`` — labels are
    best-effort, never fatal (localname prettify covers the gap).
    """
    try:
        doc = ET.fromstring(label_xml, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError:
        logger.warning("dimensional facts: label linkbase parse failed; using localname fallback")
        return {}

    loc_to_fragment: dict[str, str] = {}
    arc_from_to: list[tuple[str, str]] = []
    label_values: dict[str, list[tuple[str, str]]] = {}

    for el in doc.iter():
        if not isinstance(el.tag, str):
            continue
        local = ET.QName(el.tag).localname
        if local == "loc":
            href = el.get(f"{{{_XLINK}}}href", "")
            lab = el.get(f"{{{_XLINK}}}label", "")
            if "#" in href and lab:
                loc_to_fragment[lab] = href.rsplit("#", 1)[1]
        elif local == "labelArc":
            frm = el.get(f"{{{_XLINK}}}from", "")
            to = el.get(f"{{{_XLINK}}}to", "")
            if frm and to:
                arc_from_to.append((frm, to))
        elif local == "label":
            lab = el.get(f"{{{_XLINK}}}label", "")
            role = el.get(f"{{{_XLINK}}}role", "")
            text = (el.text or "").strip()
            if lab and text:
                label_values.setdefault(lab, []).append((role, text))

    out: dict[str, str] = {}
    for frm, to in arc_from_to:
        fragment = loc_to_fragment.get(frm)
        if fragment is None or fragment in out:
            continue
        candidates = label_values.get(to, [])
        if not candidates:
            continue
        standard = next((t for r, t in candidates if r == _STANDARD_LABEL_ROLE), None)
        out[fragment] = standard if standard is not None else candidates[0][1]
    return out


_DOMAIN_MEMBER_ARCROLE = "http://xbrl.org/int/dim/arcrole/domain-member"


def parse_definition_linkbase(definition_xml: bytes) -> set[tuple[str, str]]:
    """Parse a filing's ``*_def.xml`` into ``{(parent_fragment,
    child_fragment)}`` pairs from domain-member arcs.

    Fragments are schema element ids (``aapl_IPhoneMember``) — the same
    keying the label linkbase uses. Subtotal detection downstream marks
    a member as subtotal when it parents another member that also
    carries a fact on the same axis. Malformed XML → empty set
    (best-effort, never fatal — no fact is dropped, only subtotal
    flags are missed)."""
    try:
        doc = ET.fromstring(definition_xml, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError:
        logger.warning("dimensional facts: definition linkbase parse failed; subtotal flags unavailable")
        return set()

    pairs: set[tuple[str, str]] = set()
    # loc labels are scoped per extended link — walk each definitionLink
    # separately so identical xlink:label strings in different links
    # don't cross-wire.
    for link in doc.iter():
        if not isinstance(link.tag, str) or ET.QName(link.tag).localname != "definitionLink":
            continue
        loc_to_fragment: dict[str, str] = {}
        arcs: list[tuple[str, str]] = []
        for el in link:
            if not isinstance(el.tag, str):
                continue
            local = ET.QName(el.tag).localname
            if local == "loc":
                href = el.get(f"{{{_XLINK}}}href", "")
                lab = el.get(f"{{{_XLINK}}}label", "")
                if "#" in href and lab:
                    loc_to_fragment[lab] = href.rsplit("#", 1)[1]
            elif local == "definitionArc":
                if el.get(f"{{{_XLINK}}}arcrole") != _DOMAIN_MEMBER_ARCROLE:
                    continue
                frm = el.get(f"{{{_XLINK}}}from", "")
                to = el.get(f"{{{_XLINK}}}to", "")
                if frm and to:
                    arcs.append((frm, to))
        for frm, to in arcs:
            parent = loc_to_fragment.get(frm)
            child = loc_to_fragment.get(to)
            if parent and child:
                pairs.add((parent, child))
    return pairs


def _member_fragment(member_qname: str) -> str:
    """``aapl:IPhoneMember`` → ``aapl_IPhoneMember`` (the schema element
    id convention used by loc hrefs in SEC linkbases)."""
    return member_qname.replace(":", "_", 1)


_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def prettify_member(member_qname: str) -> str:
    """Fallback label: ``aapl:WearablesHomeandAccessoriesMember`` →
    ``Wearables Homeand Accessories`` (imperfect by design — used only
    when the label linkbase has no entry, e.g. standard-taxonomy
    members like ``country:US`` → ``US``)."""
    local = member_localname(member_qname) or member_qname
    return _CAMEL_BOUNDARY.sub(" ", local).strip()


def _label_for(member_qname: str, labels: Mapping[str, str]) -> str:
    hit = labels.get(_member_fragment(member_qname)) if ":" in member_qname else None
    if hit:
        # SEC standard labels carry a " [Member]" suffix — UI noise.
        return re.sub(r"\s*\[Member\]\s*$", "", hit) or prettify_member(member_qname)
    return prettify_member(member_qname)


# ---------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------


def _to_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text.strip())
    except ValueError:
        return None


def _decimals_rank(decimals: str | None) -> float:
    """Precision ordering for duplicate-fact arbitration: INF > any
    integer > absent."""
    if decimals is None:
        return float("-inf")
    cleaned = decimals.strip()
    if cleaned.upper() == "INF":
        return float("inf")
    try:
        return float(int(cleaned))
    except ValueError:
        return float("-inf")


def _classify_context(dims: Mapping[str, str]) -> tuple[DimensionalAxis, str] | None:
    """Map a context's dimension set to ``(axis, member_qname)`` per the
    spec §D3 exact axis-set rule, or ``None`` when out of scope."""
    by_local: dict[str, str] = {}
    for axis_qname, member_qname in dims.items():
        local = axis_localname(axis_qname)
        if local is None:
            return None
        by_local[local] = member_qname

    keys = frozenset(by_local)
    if keys == frozenset({"StatementBusinessSegmentsAxis"}):
        return ("business_segment", by_local["StatementBusinessSegmentsAxis"])
    if keys == frozenset({"StatementBusinessSegmentsAxis", "ConsolidationItemsAxis"}):
        if member_localname(by_local["ConsolidationItemsAxis"]) == "OperatingSegments":
            return ("business_segment", by_local["StatementBusinessSegmentsAxis"])
        return None
    if keys == frozenset({"ProductOrServiceAxis"}):
        return ("product_service", by_local["ProductOrServiceAxis"])
    if keys == frozenset({"StatementGeographicalAxis"}):
        return ("geographic", by_local["StatementGeographicalAxis"])
    return None


def _instance_units(doc: ET._Element) -> dict[str, str]:
    """``{unit_id: measure}`` for single-measure units (``iso4217:USD``
    → ``USD``). Ratio (divide) units are omitted — the three tracked
    metrics are all plain monetary concepts."""
    units: dict[str, str] = {}
    for el in doc.iter():
        if not isinstance(el.tag, str) or ET.QName(el.tag).localname != "unit":
            continue
        uid = el.get("id")
        if not uid:
            continue
        measures = [
            (child.text or "").strip()
            for child in el.iter()
            if isinstance(child.tag, str) and ET.QName(child.tag).localname == "measure"
        ]
        has_divide = any(isinstance(child.tag, str) and ET.QName(child.tag).localname == "divide" for child in el)
        if has_divide or len(measures) != 1 or not measures[0]:
            continue
        measure = measures[0]
        units[uid] = measure.rsplit(":", 1)[-1]
    return units


def mark_value_overage_subtotals(
    facts: list[DimensionalFact],
    revenue_totals: Mapping[tuple[DimensionalMetric, date | None, date], Decimal],
    *,
    accession: str,
) -> tuple[list[DimensionalFact], dict[str, int]]:
    """Mark product/geographic revenue subtotals by value-overage. Shared by the #554
    per-filing extractor and the #1590 FSDS bulk loader (ONE owner of the rule).

    ASC 606 revenue disaggregation reconciles to consolidated revenue, so when member
    sums OVERSHOOT the dimensionless total the overage is exactly the subtotal mass
    (linkbases are often flat — AAPL nests Product⊃iPhone/Mac/iPad/Wearables in NEITHER
    def nor pre). The smallest member subset summing exactly to the overage is the
    subtotal set; ambiguity or no match marks nothing + WARNs. ``business_segment`` is
    excluded: its sum legitimately differs from consolidated (unallocated corporate items
    are filtered out via the ConsolidationItemsAxis route rule), so overage is signal-free.

    ``revenue_totals`` maps (metric, period_start, period_end) → the dimensionless
    consolidated Decimal value (the per-filing caller passes ``{k: v[2] for k, v in
    totals.items()}``; the bulk caller builds the same shape from num.txt ``segments=''``
    rows). Pure (no I/O): returns the re-marked facts + a per-pass rejection counter the
    caller may surface in its own telemetry."""
    facts = list(facts)
    rejections: dict[str, int] = {}
    groups: dict[tuple[DimensionalAxis, date | None, date], list[int]] = {}
    for i, f in enumerate(facts):
        if f.axis in ("product_service", "geographic") and f.metric == "revenue" and not f.is_subtotal:
            groups.setdefault((f.axis, f.period_start, f.period_end), []).append(i)
    for (axis, period_start, period_end), idxs in sorted(groups.items()):
        if len(idxs) < 2:
            continue
        total = revenue_totals.get(("revenue", period_start, period_end))
        if total is None:
            rejections["no_consolidated_revenue_anchor"] = rejections.get("no_consolidated_revenue_anchor", 0) + 1
            continue
        member_sum = sum(facts[i].val for i in idxs)
        if member_sum <= total:
            continue  # flat or partial disaggregation — nothing overlaps
        target = member_sum - total
        marked: tuple[int, ...] | None = None
        ambiguous = False
        for size in (1, 2, 3):
            matches = [c for c in combinations(idxs, size) if sum(facts[i].val for i in c) == target]
            if len(matches) == 1:
                marked = matches[0]
                break
            if len(matches) > 1:
                ambiguous = True
                break
        if marked is not None:
            for i in marked:
                facts[i] = replace(facts[i], is_subtotal=True)
        else:
            reason = "subtotal_set_ambiguous" if ambiguous else "subtotal_overage_unresolved"
            rejections[reason] = rejections.get(reason, 0) + 1
            logger.warning(
                "dimensional facts: %s accession=%s axis=%s period_end=%s overage=%s",
                reason,
                accession,
                axis,
                period_end,
                target,
            )
    return facts, rejections


def extract_dimensional_facts(
    instance_xml: bytes,
    label_xml: bytes | None,
    definition_xml: bytes | None,
    *,
    accession: str,
) -> list[DimensionalFact]:
    """Extract segment / product / geographic facts from an XBRL
    instance document.

    Raises:
        ValueError: on malformed instance XML (caller decides failure
        semantics — spec §D1: WARN + parsed-without-segments).
    """
    try:
        doc = ET.fromstring(instance_xml, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError as exc:
        raise ValueError(f"XBRL instance parse error: {exc}") from exc

    labels = parse_label_linkbase(label_xml) if label_xml else {}
    member_tree = parse_definition_linkbase(definition_xml) if definition_xml else set()
    units = _instance_units(doc)

    # Context id → (axis, member_qname, period). axis/member are None
    # for DIMENSIONLESS contexts — their consolidated facts are not
    # emitted (financial_facts_raw territory) but anchor the
    # value-overage subtotal detection below.
    contexts: dict[str, tuple[DimensionalAxis | None, str | None, dict[str, str]] | None] = {}
    for ctx in doc.iter():
        if not isinstance(ctx.tag, str) or ET.QName(ctx.tag).localname != "context":
            continue
        cid = ctx.get("id")
        if not cid:
            continue
        dims = context_dimensions(ctx)
        if not dims:
            contexts[cid] = (None, None, context_period(ctx))
            continue
        route = _classify_context(dims)
        contexts[cid] = None if route is None else (route[0], route[1], context_period(ctx))

    rejections: dict[str, int] = {}

    # candidate key → (alias_priority, decimals_rank, fact). Key excludes
    # the concept so revenue-alias arbitration happens in the same pass.
    # ``totals`` holds the DIMENSIONLESS facts for the same metrics —
    # not emitted, only anchoring value-overage subtotal detection.
    candidates: dict[
        tuple[DimensionalAxis, str, DimensionalMetric, date | None, date],
        tuple[int, float, DimensionalFact],
    ] = {}
    conflicted: set[tuple[DimensionalAxis, str, DimensionalMetric, date | None, date]] = set()
    totals: dict[tuple[DimensionalMetric, date | None, date], tuple[int, float, Decimal]] = {}
    totals_conflicted: set[tuple[DimensionalMetric, date | None, date]] = set()

    for el in doc.iter():
        if not isinstance(el.tag, str):
            continue
        cref = el.get("contextRef")
        if not cref:
            continue
        qname = ET.QName(el.tag)
        if not qname.namespace or "fasb.org/us-gaap" not in qname.namespace:
            continue  # extension concepts out of scope v1 (spec §D3)
        mapped = _CONCEPT_TO_METRIC.get(qname.localname)
        if mapped is None:
            continue
        metric, alias_priority = mapped
        route = contexts.get(cref)
        if route is None:
            continue
        axis, member_qname, period = route
        if axis is not None and metric not in _AXIS_METRICS[axis]:
            continue
        if el.get(_XSI_NIL) == "true":
            continue

        if metric in _INSTANT_METRICS:
            period_start = None
            period_end = _to_date(period.get("instant"))
        else:
            period_start = _to_date(period.get("startDate"))
            period_end = _to_date(period.get("endDate"))
            if period_start is None:
                rejections["duration_without_start"] = rejections.get("duration_without_start", 0) + 1
                continue
        if period_end is None:
            rejections["missing_period_end"] = rejections.get("missing_period_end", 0) + 1
            continue
        if not (_SANITY_MIN <= period_end < _SANITY_MAX) or (
            period_start is not None and not (_SANITY_MIN <= period_start < _SANITY_MAX)
        ):
            rejections["period_out_of_sanity_window"] = rejections.get("period_out_of_sanity_window", 0) + 1
            continue
        if period_start is not None and period_start > period_end:
            rejections["period_start_after_end"] = rejections.get("period_start_after_end", 0) + 1
            continue

        unit = units.get(el.get("unitRef") or "")
        if unit is None:
            continue
        raw_val = (el.text or "").strip()
        if not raw_val:
            continue
        try:
            val = Decimal(raw_val)
        except InvalidOperation:
            rejections["non_decimal_value"] = rejections.get("non_decimal_value", 0) + 1
            continue

        rank = _decimals_rank(el.get("decimals"))

        if axis is None or member_qname is None:
            # Consolidated fact — totals anchor, same arbitration rules.
            tkey = (metric, period_start, period_end)
            if tkey in totals_conflicted:
                continue
            t_incumbent = totals.get(tkey)
            if t_incumbent is None:
                totals[tkey] = (alias_priority, rank, val)
                continue
            t_priority, t_rank, t_val = t_incumbent
            if alias_priority != t_priority:
                if alias_priority < t_priority:
                    totals[tkey] = (alias_priority, rank, val)
                continue
            if val == t_val:
                continue
            if rank > t_rank:
                totals[tkey] = (alias_priority, rank, val)
            elif rank == t_rank:
                del totals[tkey]
                totals_conflicted.add(tkey)
            continue

        fact = DimensionalFact(
            axis=axis,
            member_qname=member_qname,
            member_label=_label_for(member_qname, labels),
            metric=metric,
            unit=unit,
            is_subtotal=False,  # marked in the post-pass below
            period_start=period_start,
            period_end=period_end,
            val=val,
            decimals=el.get("decimals"),
        )
        key = (axis, member_qname, metric, period_start, period_end)
        if key in conflicted:
            continue
        incumbent = candidates.get(key)
        if incumbent is None:
            candidates[key] = (alias_priority, rank, fact)
            continue
        inc_priority, inc_rank, inc_fact = incumbent
        # Revenue-alias arbitration: lower priority index wins (spec §D3).
        if alias_priority != inc_priority:
            if alias_priority < inc_priority:
                candidates[key] = (alias_priority, rank, fact)
            continue
        # Same concept duplicated: equal values collapse silently;
        # differing values resolve by precision; equal-precision
        # conflict drops the member+period (spec §D3).
        if fact.val == inc_fact.val:
            continue
        if rank > inc_rank:
            candidates[key] = (alias_priority, rank, fact)
        elif rank == inc_rank:
            del candidates[key]
            conflicted.add(key)
            rejections["duplicate_fact_conflict"] = rejections.get("duplicate_fact_conflict", 0) + 1

    for reason, count in sorted(rejections.items()):
        logger.warning(
            "dimensional facts: rejected %d fact(s) accession=%s reason=%s",
            count,
            accession,
            reason,
        )

    facts = [fact for _, _, fact in candidates.values()]

    # Subtotal post-pass: a member is a subtotal when the definition
    # linkbase says it parents another member that ALSO has a fact in
    # the SAME (axis, metric, period) group (e.g. us-gaap:ProductMember
    # over iPhone/Mac/iPad/Wearables). Scoping per group — not per axis
    # — matters (#554 Codex pre-push finding): a 10-K carries 3 FYs of
    # comparatives, and a parent whose children are reported only for a
    # PRIOR year is the finest grain the filer gave us for the latest
    # year; marking it axis-wide would blank the latest-FY rows. A
    # hierarchy parent with no reported children stays a leaf.
    members_by_group: dict[tuple[DimensionalAxis, DimensionalMetric, date | None, date], set[str]] = {}
    for f in facts:
        gkey = (f.axis, f.metric, f.period_start, f.period_end)
        members_by_group.setdefault(gkey, set()).add(_member_fragment(f.member_qname))
    children_of: dict[str, set[str]] = {}
    for parent, child in member_tree:
        children_of.setdefault(parent, set()).add(child)
    facts = [
        replace(f, is_subtotal=True)
        if children_of.get(_member_fragment(f.member_qname), set())
        & members_by_group[(f.axis, f.metric, f.period_start, f.period_end)]
        else f
        for f in facts
    ]

    # Value-overage subtotal marking (product/geographic revenue) — extracted to the
    # shared ``mark_value_overage_subtotals`` so the #1590 FSDS bulk path applies the
    # SAME rule (DRY: one owner). ``totals`` here is keyed (metric, period_start,
    # period_end) → (priority, rank, Decimal); the helper takes the bare Decimal anchor.
    # The returned per-pass rejection counts are not summary-logged here (they never
    # were — the summary WARN above ran before this pass; the per-overage WARN now
    # lives inside the helper), so behaviour is preserved.
    facts, _ = mark_value_overage_subtotals(facts, {k: v[2] for k, v in totals.items()}, accession=accession)

    return sorted(facts, key=lambda f: (f.axis, f.metric, f.period_end, f.member_qname))
