"""iXBRL fact extractor for N-CSR / N-CSRS fund metadata (spec §5 + §8).

Pure function ``extract_fund_metadata_facts`` parses the iXBRL companion
of an N-CSR / N-CSRS filing + returns one :class:`FundMetadataFacts`
per (series_id, class_id) tuple with strict context-tuple filtering
(spec §8 step 6.c, Codex 1a BLOCKING-2).

Design choices:

- **lxml parsing** via :func:`lxml.etree.fromstring`. iXBRL companion
  sizes are bounded (~5 MB max in spike sample) so in-memory parse is
  fine.
- **Wildcard namespace matching** (``{*}context``) — different filers
  emit different OEF / DEI / SRT taxonomy versions; matching by
  ``localname`` only avoids hard-coding namespace URIs that change
  yearly with each SEC taxonomy release.
- **classId regex** ``C\\d{9}`` — class members come prefixed with
  trust-namespace + suffixed with ``Member`` (e.g.
  ``fmr:C000203453Member``); the regex extracts the canonical
  ``C000NNNNNN`` identifier.
- **SeriesAxis is optional.** Single-series trusts (Fidelity Concord)
  emit no ``oef:SeriesAxis`` member. Multi-series trusts (Vanguard
  Index Funds) do. The extractor produces one observation per class
  regardless, with ``series_id = None`` when SeriesAxis is absent.
- **Hard context filter** for class-level facts: only facts whose
  ``contextRef`` resolves to a context carrying the EXACT class_id
  member are extracted for that class. Series-level facts (HoldingsCount,
  AssetsNet, sector allocation) are extracted from contexts with the
  matching series member AND no ClassAxis member — preventing
  cross-class bleed.
- **Boilerplate blocklist** drops 5 text-block concepts (spec §5)
  that duplicate structured data (HoldingsTableTextBlock etc.) or are
  generic boilerplate (PerformancePastDoesNotIndicateFuture).
- **Tier 2 dimensional routing** is keyed by ``(concept_qname,
  axis_qname)`` — ``oef:PctOfNav`` routes to sector / region / credit
  by inspecting the axis dimension; never by concept alone (Codex 1a
  WARNING-2).
- **Unmapped (concept, axis) tuples** route to ``raw_facts`` with a
  total size cap of 32 KB.
- **AvgAnnlRtrPct period allowlist** keys returns_pct by stripped
  member localname (``OneYear`` → ``"1Y"`` etc.). Unknown period
  members route to raw_facts.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import lxml.etree as ET

logger = logging.getLogger(__name__)

# Hard cap on raw_facts serialized size (spec §5 Tier 3).
RAW_FACTS_SIZE_CAP_BYTES = 32 * 1024
TRUNCATED_SENTINEL = "__truncated__"

# Hard cap on material_chng_notice plain text (spec §5).
MATERIAL_CHNG_NOTICE_CAP_BYTES = 16 * 1024

# Hard cap on raw_facts entries for FactorsAffectingPerfTextBlock (spec §5 routing).
FACTORS_AFFECTING_CAP_BYTES = 8 * 1024


_CLASS_ID_PATTERN = re.compile(r"C\d{9}")
_SERIES_ID_PATTERN = re.compile(r"S\d{9}")


# Boilerplate concepts dropped entirely (spec §5).
_BOILERPLATE_BLOCKLIST: frozenset[str] = frozenset(
    {
        "HoldingsTableTextBlock",  # duplicates oef:PctOfNav × IndustrySectorAxis
        "AvgAnnlRtrTableTextBlock",  # duplicates oef:AvgAnnlRtrPct
        "LineGraphTableTextBlock",  # duplicates oef:AccmVal
        "AddlFundStatisticsTextBlock",  # generic fund-stats narrative
        "AnnlOrSemiAnnlStatementTextBlock",  # TSR intro boilerplate
        "PerformancePastDoesNotIndicateFuture",  # disclaimer
        "NoDeductionOfTaxesTextBlock",  # disclaimer
        "LargestHoldingsTableTextBlock",  # duplicates per-row holdings (out of scope)
        "ExpensesTextBlock",  # boilerplate narrative around ExpensesPaidAmt
        "UpdPerfInfoLocationTextBlock",  # boilerplate
        "AddlInfoTextBlock",  # generic; specific fields captured separately
        "SummaryOfChngLegendTextBlock",  # boilerplate
        "MaterialFundChngTextBlock",  # duplicates MaterialFundChngNoticeTextBlock
        "MaterialFundChngExpensesTextBlock",  # narrative duplicate
    }
)


# Tier 1 single-value concept → column mapping (spec §5).
# Routing here applies when the fact's only dimensional axis is ClassAxis
# (or no axis at all — filing-level concepts like DocumentType).
_TIER1_CONCEPT_TO_COLUMN: dict[str, str] = {
    "EntityRegistrantName": "trust_name",
    "EntityInvCompanyType": "entity_inv_company_type",
    "DocumentType": "document_type",
    "DocumentPeriodEndDate": "period_end",
    "AmendmentFlag": "amendment_flag",
    "SecurityExchangeName": "exchange",
    "TradingSymbol": "trading_symbol",
    "FundName": "series_name",
    "ClassName": "class_name",
    "PerfInceptionDate": "inception_date",
    "ShareholderReportAnnualOrSemiAnnual": "shareholder_report_type",
    "ExpenseRatioPct": "expense_ratio_pct",
    "ExpensesPaidAmt": "expenses_paid_amt",
    "AdvisoryFeesPaidAmt": "advisory_fees_paid_amt",
    "AssetsNet": "net_assets_amt",
    "InvestmentCompanyPortfolioTurnover": "portfolio_turnover_pct",
    "HoldingsCount": "holdings_count",
    "MaterialChngDate": "material_chng_date",
    "MaterialFundChngNoticeTextBlock": "material_chng_notice",
    "AddlInfoPhoneNumber": "contact_phone",
    "AddlInfoWebsite": "contact_website",
    "AddlInfoEmail": "contact_email",
    "UpdProspectusPhoneNumber": "prospectus_phone",
    "UpdProspectusWebAddress": "prospectus_website",
    "UpdProspectusEmailAddress": "prospectus_email",
}


# Period axis allowlist for AvgAnnlRtrPct (spec §5.A).
_PERIOD_MEMBER_ALLOWLIST: dict[str, str] = {
    "OneYear": "1Y",
    "OneYearMember": "1Y",
    "FiveYears": "5Y",
    "FiveYearsMember": "5Y",
    "TenYears": "10Y",
    "TenYearsMember": "10Y",
    "SinceInception": "SinceInception",
    "SinceInceptionMember": "SinceInception",
    "LifeOfFund": "LifeOfFund",
    "LifeOfFundMember": "LifeOfFund",
}


@dataclass
class FundMetadataFacts:
    """Container for facts extracted for one (series_id, class_id) observation."""

    class_id: str
    trust_cik: str
    series_id: str | None = None

    # Tier 1 — filing-level + per-class scalars.
    document_type: str | None = None
    amendment_flag: bool = False
    period_end: date | None = None
    trust_name: str | None = None
    entity_inv_company_type: str | None = None
    series_name: str | None = None
    class_name: str | None = None
    trading_symbol: str | None = None
    exchange: str | None = None
    inception_date: date | None = None
    shareholder_report_type: str | None = None
    expense_ratio_pct: Decimal | None = None
    expenses_paid_amt: Decimal | None = None
    net_assets_amt: Decimal | None = None
    advisory_fees_paid_amt: Decimal | None = None
    portfolio_turnover_pct: Decimal | None = None
    holdings_count: int | None = None
    material_chng_date: date | None = None
    material_chng_notice: str | None = None
    contact_phone: str | None = None
    contact_website: str | None = None
    contact_email: str | None = None
    prospectus_phone: str | None = None
    prospectus_website: str | None = None
    prospectus_email: str | None = None

    # Tier 2 — dimensional JSONB.
    returns_pct: dict[str, Decimal] = field(default_factory=dict)
    benchmark_returns_pct: dict[str, dict[str, Decimal]] = field(default_factory=dict)
    sector_allocation: dict[str, Decimal] = field(default_factory=dict)
    region_allocation: dict[str, Decimal] = field(default_factory=dict)
    credit_quality_allocation: dict[str, Decimal] = field(default_factory=dict)
    growth_curve: list[dict[str, Any]] = field(default_factory=list)

    # Tier 3 — capture-then-decide fallback.
    raw_facts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def _member_localname(member_ref: str | None) -> str | None:
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


def _axis_localname(axis_qname: str | None) -> str | None:
    """Strip namespace prefix from an axis QName (``oef:ClassAxis`` → ``ClassAxis``)."""
    if not axis_qname:
        return None
    val = axis_qname.strip()
    if ":" in val:
        val = val.rsplit(":", 1)[1]
    return val or None


def _to_decimal(text: str | None) -> Decimal | None:
    if text is None:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation, ValueError:
        return None


def _to_int(text: str | None) -> int | None:
    if text is None:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _to_date(text: str | None) -> date | None:
    if text is None:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError:
        return None


def _to_bool(text: str | None) -> bool:
    if text is None:
        return False
    return text.strip().lower() in {"true", "1", "yes"}


def _strip_html(text: str | None) -> str | None:
    """Best-effort HTML strip + collapse whitespace + hard cap."""
    if text is None:
        return None
    # Strip tags via regex (simple — iXBRL text-blocks have inline HTML).
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = re.sub(r"\s+", " ", plain).strip()
    if not plain:
        return None
    if len(plain.encode("utf-8")) > MATERIAL_CHNG_NOTICE_CAP_BYTES:
        encoded = plain.encode("utf-8")[:MATERIAL_CHNG_NOTICE_CAP_BYTES]
        # Decode back, ignoring partial last char.
        plain = encoded.decode("utf-8", errors="ignore") + " [TRUNCATED]"
    return plain


def _context_dimensions(ctx_el: ET._Element) -> dict[str, str]:
    """Return ``{axis_qname: member_qname}`` for a context's segment members."""
    dims: dict[str, str] = {}
    # Find xmlns-wildcard segment.
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


def _context_period(ctx_el: ET._Element) -> dict[str, str]:
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


def _context_entity_cik(ctx_el: ET._Element) -> str | None:
    """Return the 10-digit zero-padded entity CIK."""
    for child in ctx_el.iter():
        if isinstance(child.tag, str) and ET.QName(child.tag).localname == "identifier":
            raw = (child.text or "").strip()
            if raw.isdigit():
                return raw.zfill(10)
    return None


def _route_tier2(
    facts: FundMetadataFacts,
    concept_local: str,
    axis_local: str | None,
    member_local: str | None,
    value: str | None,
    period: dict[str, str],
) -> bool:
    """Return True iff the (concept, axis, member) tuple was bucketed into a Tier 2 column."""
    decimal_value = _to_decimal(value)
    if decimal_value is None:
        return False

    if concept_local == "AvgAnnlRtrPct":
        if axis_local in {"BroadBasedIndexAxis", "AdditionalIndexAxis"}:
            bucket = "broad_based" if axis_local == "BroadBasedIndexAxis" else "additional"
            label = member_local or "unknown"
            facts.benchmark_returns_pct.setdefault(bucket, {})[label] = decimal_value
            return True
        # No axis or non-benchmark axis → return period via period_member if recognised.
        if member_local and member_local in _PERIOD_MEMBER_ALLOWLIST:
            facts.returns_pct[_PERIOD_MEMBER_ALLOWLIST[member_local]] = decimal_value
            return True
        return False

    # Both PctOfNav (Vanguard / Fidelity style) and PctOfTotalInv (iShares
    # style) route to the same allocation columns — they are different
    # denominators (NAV vs total invested capital) but per spec §5 we treat
    # them as the same operator-visible signal. Tracking which denominator
    # was used is preserved via the parser_version (future-iteration: split
    # into separate columns if the distinction becomes operator-relevant).
    if concept_local in {"PctOfNav", "PctOfTotalInv"} and axis_local:
        if axis_local == "IndustrySectorAxis":
            facts.sector_allocation[member_local or "unknown"] = decimal_value
            return True
        if axis_local == "GeographicRegionAxis":
            facts.region_allocation[member_local or "unknown"] = decimal_value
            return True
        if axis_local == "CreditQualityAxis":
            facts.credit_quality_allocation[member_local or "unknown"] = decimal_value
            return True

    if concept_local == "AccmVal":
        instant = period.get("instant") or period.get("endDate")
        if instant:
            facts.growth_curve.append(
                {
                    "period_end": instant,
                    "value": str(decimal_value),
                    "axis_member": member_local,
                }
            )
            return True

    return False


def _route_tier3(
    facts: FundMetadataFacts,
    concept_qname: str,
    concept_local: str,
    axis_qname: str | None,
    member_local: str | None,
    value: str | None,
) -> None:
    """Capture an unmapped fact into raw_facts (Tier 3) under the size cap."""
    if concept_local in _BOILERPLATE_BLOCKLIST:
        return

    entry: dict[str, Any] = {"value": value}
    if axis_qname:
        entry["axis"] = axis_qname
    if member_local:
        entry["member"] = member_local

    # FactorsAffectingPerfTextBlock has its own per-concept cap.
    if concept_local == "FactorsAffectingPerfTextBlock":
        plain = _strip_html(value)
        if plain and len(plain.encode("utf-8")) > FACTORS_AFFECTING_CAP_BYTES:
            plain = (
                plain.encode("utf-8")[:FACTORS_AFFECTING_CAP_BYTES].decode("utf-8", errors="ignore") + " [TRUNCATED]"
            )
        entry = {"value": plain}

    facts.raw_facts.setdefault(concept_qname, []).append(entry)

    # Enforce global size cap (best-effort: estimate serialized length).
    if len(json.dumps(facts.raw_facts, default=str)) > RAW_FACTS_SIZE_CAP_BYTES:
        # Truncate the latest concept's last entry; mark sentinel.
        facts.raw_facts.setdefault(TRUNCATED_SENTINEL, []).append({"reason": "size_cap_exceeded"})
        facts.raw_facts[concept_qname].pop()


def _apply_tier1(
    facts: FundMetadataFacts,
    concept_local: str,
    value: str | None,
) -> bool:
    """Apply a Tier 1 single-value concept. Return True iff applied."""
    column = _TIER1_CONCEPT_TO_COLUMN.get(concept_local)
    if column is None:
        return False

    if column in {"trust_name", "series_name", "class_name", "trading_symbol", "exchange"}:
        setattr(facts, column, (value or "").strip() or None)
    elif column in {
        "contact_phone",
        "contact_website",
        "contact_email",
        "prospectus_phone",
        "prospectus_website",
        "prospectus_email",
    }:
        setattr(facts, column, (value or "").strip() or None)
    elif column == "shareholder_report_type":
        facts.shareholder_report_type = (value or "").strip() or None
    elif column == "document_type":
        facts.document_type = (value or "").strip() or None
    elif column == "entity_inv_company_type":
        facts.entity_inv_company_type = (value or "").strip() or None
    elif column == "amendment_flag":
        facts.amendment_flag = _to_bool(value)
    elif column == "period_end":
        facts.period_end = _to_date(value)
    elif column == "inception_date":
        facts.inception_date = _to_date(value)
    elif column == "material_chng_date":
        facts.material_chng_date = _to_date(value)
    elif column == "material_chng_notice":
        facts.material_chng_notice = _strip_html(value)
    elif column == "holdings_count":
        facts.holdings_count = _to_int(value)
    elif column in {
        "expense_ratio_pct",
        "expenses_paid_amt",
        "net_assets_amt",
        "advisory_fees_paid_amt",
        "portfolio_turnover_pct",
    }:
        setattr(facts, column, _to_decimal(value))
    return True


def extract_fund_metadata_facts(ixbrl_xml: bytes) -> list[FundMetadataFacts]:
    """Parse iXBRL companion + return one :class:`FundMetadataFacts` per
    (series, class) tuple.

    Raises:
        ValueError: on malformed iXBRL (caller maps to ``failed_outcome``).
    """
    try:
        doc = ET.fromstring(ixbrl_xml)
    except ET.XMLSyntaxError as exc:  # pragma: no cover — error-path
        raise ValueError(f"iXBRL parse error: {exc}") from exc

    # 1. Build contexts index.
    contexts: dict[str, dict[str, Any]] = {}
    trust_cik_resolved: str | None = None
    for ctx in doc.iter():
        if not isinstance(ctx.tag, str):
            continue
        if ET.QName(ctx.tag).localname != "context":
            continue
        cid = ctx.get("id")
        if not cid:
            continue
        dims = _context_dimensions(ctx)
        period = _context_period(ctx)
        cik = _context_entity_cik(ctx)
        if trust_cik_resolved is None and cik:
            trust_cik_resolved = cik
        contexts[cid] = {"dims": dims, "period": period, "entity_cik": cik}

    if trust_cik_resolved is None:
        raise ValueError("iXBRL has no entity CIK in any context")

    # 2. Enumerate (series_id, class_id) tuples. SeriesAxis is optional.
    class_to_series: dict[str, str | None] = {}
    for ctx in contexts.values():
        class_member: str | None = None
        series_member: str | None = None
        for axis_qname, member_qname in ctx["dims"].items():
            axis_local = _axis_localname(axis_qname)
            if axis_local == "ClassAxis":
                class_member = member_qname
            elif axis_local in {"SeriesAxis"}:
                series_member = member_qname
        if not class_member:
            continue
        m = _CLASS_ID_PATTERN.search(class_member)
        if not m:
            continue
        class_id = m.group(0)
        if series_member:
            sm = _SERIES_ID_PATTERN.search(series_member)
            series_id = sm.group(0) if sm else None
        else:
            series_id = None
        # First-write-wins; consistent series_id per class_id assumed by spec §8 step 5.
        class_to_series.setdefault(class_id, series_id)

    # 3. Collect all facts grouped by class_id (or filing-level if no ClassAxis).
    filing_level_facts: list[dict[str, Any]] = []
    class_facts: dict[str, list[dict[str, Any]]] = {cid: [] for cid in class_to_series}

    for el in doc.iter():
        if not isinstance(el.tag, str):
            continue
        cref = el.get("contextRef")
        if not cref:
            continue
        qname = ET.QName(el.tag)
        if not qname.namespace:
            continue
        # Skip XBRL infrastructure tags.
        if "xbrl.org/2003/instance" in qname.namespace or qname.namespace.startswith("http://www.w3.org/"):
            continue
        ctx = contexts.get(cref)
        if ctx is None:
            continue

        # Identify the class context-tag (if any).
        dims = ctx["dims"]
        class_member = None
        axis_qname_active: str | None = None
        member_qname_active: str | None = None
        for axis_qname, member_qname in dims.items():
            axis_local = _axis_localname(axis_qname)
            if axis_local == "ClassAxis":
                class_member = member_qname
            else:
                # Track the first non-Class axis for Tier 2 routing decision.
                if axis_qname_active is None:
                    axis_qname_active = axis_qname
                    member_qname_active = member_qname

        record = {
            "concept_qname": f"{qname.namespace}:{qname.localname}",
            "concept_local": qname.localname,
            "value": el.text,
            "context_ref": cref,
            "axis_qname": axis_qname_active,
            "axis_local": _axis_localname(axis_qname_active),
            "member_local": _member_localname(member_qname_active),
            "period": ctx["period"],
        }

        if class_member:
            m = _CLASS_ID_PATTERN.search(class_member)
            if m and m.group(0) in class_facts:
                class_facts[m.group(0)].append(record)
        else:
            filing_level_facts.append(record)

    # 4. Build per-class FundMetadataFacts.
    out: list[FundMetadataFacts] = []
    for class_id, series_id in sorted(class_to_series.items()):
        facts = FundMetadataFacts(class_id=class_id, trust_cik=trust_cik_resolved, series_id=series_id)

        # Apply filing-level facts (DocumentType, AmendmentFlag, EntityRegistrantName,
        # EntityInvCompanyType, DocumentPeriodEndDate, EntityCentralIndexKey, etc.).
        for r in filing_level_facts:
            if r["concept_local"] in _BOILERPLATE_BLOCKLIST:
                continue
            if not _apply_tier1(facts, r["concept_local"], r["value"]):
                # Filing-level dimensional fact (rare). Route to raw_facts.
                if r["concept_local"] not in _BOILERPLATE_BLOCKLIST:
                    _route_tier3(
                        facts, r["concept_qname"], r["concept_local"], r["axis_qname"], r["member_local"], r["value"]
                    )

        # Apply per-class facts.
        for r in class_facts[class_id]:
            if r["concept_local"] in _BOILERPLATE_BLOCKLIST:
                continue
            # Tier 1: single-value class fact (no non-Class axis).
            if r["axis_local"] is None and _apply_tier1(facts, r["concept_local"], r["value"]):
                continue
            # Tier 2: dimensional class fact.
            if _route_tier2(facts, r["concept_local"], r["axis_local"], r["member_local"], r["value"], r["period"]):
                continue
            # Tier 3 fallback.
            _route_tier3(facts, r["concept_qname"], r["concept_local"], r["axis_qname"], r["member_local"], r["value"])

        out.append(facts)

    # 5. Sort growth_curve by period_end ASC.
    for facts in out:
        facts.growth_curve.sort(key=lambda p: p.get("period_end") or "")

    return out
