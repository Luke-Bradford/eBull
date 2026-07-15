"""
SEC EDGAR XBRL fundamentals provider.

Raw-fact extraction against the SEC Company Facts API
(https://data.sec.gov/api/xbrl/companyfacts/).  Completely free, no API key
required, 10 req/s rate limit.

This is the primary fundamentals source for US-listed companies. Non-US
issuers without regulated-source coverage in this repo (yet) return empty
rows from fundamentals queries; per-region integration PRs add their own
free regulated-source providers (Companies House, ESMA, etc.).

This provider extracts RAW XBRL facts + concept catalogue entries only.
Period selection / TTM assembly happens downstream in
``app.services.fundamentals`` (``_derive_periods_from_facts`` + the
canonical merge), and ``fundamentals_snapshot`` is a write-through from
those normalized rows (#2008) — the former JSON-side snapshot builders
(first-tag-wins tag selection, annual-preferred TTM) rotted across issuer
tag migrations and were removed.

Provider contract:
  - Providers are pure HTTP clients — no DB access.
  - The service layer resolves instrument_id → CIK via external_identifiers
    and passes the CIK when calling this provider.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from decimal import Decimal
from types import TracebackType
from typing import Any, Final

import httpx

from app.providers.fundamentals import (
    XbrlConceptCatalogEntry,
    XbrlFact,
)
from app.providers.implementations.sec_edgar import (
    _PROCESS_RATE_LIMIT_CLOCK,
    _PROCESS_RATE_LIMIT_LOCK,
)
from app.providers.resilient_client import ResilientClient

logger = logging.getLogger(__name__)

_BASE_URL = "https://data.sec.gov"

# XBRL API validation patterns shared by the G10 ``companyconcept``
# primitive and the G11 ``frames`` primitive. Both primitives are
# intentionally GENERAL SEC API consumers — NOT bound to
# ``TRACKED_CONCEPTS`` / ``DEI_TRACKED_CONCEPTS``. Those maps govern
# downstream normalisation / projection, not arbitrary probe access.
#
# ``_TAXONOMY_RE`` accepts every published SEC taxonomy namespace
# (``us-gaap``, ``dei``, ``srt``, ``invest``, ``country``,
# ``ifrs-full``, etc.). Trailing-character anchor: leading lowercase
# letter, optional interior alnum-or-dash, MUST end in alnum (rejects
# ``us-gaap-`` and any other trailing-dash form). Single-char
# taxonomies (``a``, ``z``) remain legal via the optional trailing
# group. PR #1198 bot round-1 NITPICK ownership.
#
# ``_CONCEPT_TAG_RE`` is a deliberately tightened subset of legal
# XBRL NCName syntax — XBRL formally permits a leading underscore and
# ``-`` / ``.`` separators, but every SEC-observed concept name uses
# ``[A-Za-z][A-Za-z0-9_]*`` (leading letter; optional underscore for
# custom taxonomies). The tightening rejects URL-injection vectors
# (``foo/bar``, ``foo bar``, ``../etc``) at the validator boundary.
# If a future SEC drift surfaces a legitimate NCName outside this
# subset, the fix is to widen the regex + add a regression test.
#
# ``_UNIT_RE`` (G11) uses an explicit ``token(-per-token)?`` grammar
# — each token is leading-letter alnum, separated by exactly one
# ``-per-`` if a denominator is present. Rejects double-dash
# (``USD--per-shares``), bare ``-per`` (``USD-per``), and trailing
# dash (``USD-per-``). SEC frames URLs use the ``-per-`` syntax;
# slash would become an extra path segment. Lowercase tokens
# admitted (``usd``) — primitive is general, not bound to a
# known-unit allowlist.
#
# ``_PERIOD_RE`` (G11) admits ``CY####`` (annual flow), ``CY####Q#``
# (quarterly flow), and ``CY####Q#I`` (quarterly instantaneous /
# balance-sheet). Rejects ``CY####I`` (no annual-instantaneous frame
# per SEC docs).
#
# ``fullmatch`` discipline is mandatory: ``re.match`` + ``^...$``
# admits a trailing ``\n`` because ``$`` matches before a final
# newline; ``fullmatch`` closes that hole. Specs:
# ``docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md``
# §4.2 +
# ``docs/superpowers/specs/2026-05-18-g11-frames-api-consumer.md``
# §4.2.
_TAXONOMY_RE: Final[re.Pattern[str]] = re.compile(r"[a-z](?:[a-z0-9-]*[a-z0-9])?")
_CONCEPT_TAG_RE: Final[re.Pattern[str]] = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_UNIT_RE: Final[re.Pattern[str]] = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:-per-[A-Za-z][A-Za-z0-9]*)?")
_PERIOD_RE: Final[re.Pattern[str]] = re.compile(r"CY[0-9]{4}(?:Q[1-4]I?)?")

# SEC rate-limit: 10 req/s. Funnelled through the same process-wide
# clock as ``SecFilingsProvider`` so concurrent jobs (e.g. fundamentals
# sync + 8-K events ingest both firing on the hour) share one global
# 10 req/s budget rather than each carrying their own clock and
# collectively bursting past the SEC fair-use limit (#537).
_MIN_REQUEST_INTERVAL_S = 0.11

# XBRL period sanity window (#1218). The DEFAULT partition of
# ``financial_facts_raw`` (sql/156) absorbed parser bugs that emitted
# pre-1900 / year-6016 ``period_end`` values; the 5000-row alarm at
# ``app/services/postgres_health.py::DEFAULT_PARTITION_WARN_ROWS``
# fires once they accumulate. We reject at the parser instead of
# routing junk to the partition.
#
# Bounds: ``[1900-01-01, 2100-01-01)`` — EDGAR began 1993 + XBRL
# became mandatory 2009, so pre-1900 has no filer use-case; 2100
# leaves ~75y headroom beyond the most-distant legitimate
# forward-projected schedule item observed on dev (2041), tight
# enough to catch the year-6016 digit-overflow bug class. Spec
# ``docs/superpowers/specs/2026-05-19-1218-parser-period-end.md``.
_PERIOD_MIN: Final[date] = date(1900, 1, 1)
_PERIOD_MAX: Final[date] = date(2100, 1, 1)

_REJ_END_OUT_OF_WINDOW: Final[str] = "period_end_out_of_window"
_REJ_START_OUT_OF_WINDOW: Final[str] = "period_start_out_of_window"
_REJ_START_AFTER_END: Final[str] = "period_start_after_period_end"
# #1233 — XBRL retention horizon. Pre-cutoff facts are rejected at the
# parser so the bulk ingest never persists them. Independent of
# _PERIOD_MIN (which is a data-shape sanity guard for parser bugs).
_REJ_END_BEFORE_RETENTION_CUTOFF: Final[str] = "period_end_before_retention_cutoff"

# #1233 — Spec `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`
# §4.1. Companyfacts retention cap is 20y of financial history per CIK.
# Operators that need deeper history re-ingest via
# `POST /jobs/sec_rebuild/run` with an explicit since override.
_RETENTION_YEARS: Final[int] = 20


def _default_retention_cutoff(*, today: date | None = None) -> date:
    """Return the default companyfacts retention cutoff (``today - 20y``).

    ``today`` is parametrised so tests can pin a stable reference
    date without monkeypatching ``date.today()``. Production callers
    omit it and the current UTC date is used.
    """
    from datetime import UTC, datetime

    ref = today if today is not None else datetime.now(tz=UTC).date()
    # date.replace handles leap-day source dates (Feb 29) by raising
    # ValueError on non-leap target years; fall back to Mar 1 in that
    # edge case so the cutoff is always well-defined.
    try:
        return ref.replace(year=ref.year - _RETENTION_YEARS)
    except ValueError:
        return date(ref.year - _RETENTION_YEARS, 3, 1)


def _classify_period_rejection(
    period_start: date | None,
    period_end: date,
    *,
    retention_cutoff: date | None = None,
) -> str | None:
    """Return one of the ``_REJ_*`` reason constants if the
    ``(period_start, period_end)`` pair should be rejected, or
    ``None`` if the pair is in-window, well-ordered, and inside the
    retention horizon.

    Pure date predicate — no logging, no XBRL-shape coupling, so the
    helper is trivially testable.

    ``retention_cutoff`` (#1233) — when non-None, rejects facts whose
    ``period_end`` is strictly before the cutoff (typically
    ``_default_retention_cutoff()`` = today - 20y). Independent from
    the ``_PERIOD_MIN`` / ``_PERIOD_MAX`` data-shape sanity guards
    (#1218) which catch parser-bug junk like year-6016 / pre-1900.
    """
    if not (_PERIOD_MIN <= period_end < _PERIOD_MAX):
        return _REJ_END_OUT_OF_WINDOW
    if period_start is not None:
        if not (_PERIOD_MIN <= period_start < _PERIOD_MAX):
            return _REJ_START_OUT_OF_WINDOW
        if period_start > period_end:
            return _REJ_START_AFTER_END
    if retention_cutoff is not None and period_end < retention_cutoff:
        return _REJ_END_BEFORE_RETENTION_CUTOFF
    return None


# ── Expanded XBRL tags for financial_facts_raw pipeline ──────────
TRACKED_CONCEPTS: dict[str, tuple[str, ...]] = {
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
    ),
    "cost_of_revenue": ("CostOfGoodsAndServicesSold", "CostOfRevenue"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss",),
    "eps_basic": ("EarningsPerShareBasic",),
    "eps_diluted": ("EarningsPerShareDiluted",),
    "research_and_dev": ("ResearchAndDevelopmentExpense",),
    "sga_expense": ("SellingGeneralAndAdministrativeExpense",),
    # #2036: ``DepreciationAmortizationAndAccretionNet`` is a TOTAL-semantics
    # concept ("the aggregate net amount of depreciation, amortization, and
    # accretion … added back to net income" — us-gaap element documentation),
    # so it is alias-safe at lowest priority. The COMPONENT concepts
    # (``Depreciation``, ``AmortizationOfIntangibleAssets``) must NOT appear
    # here — a component winning the priority pick would understate D&A.
    # ``Depreciation`` is stored raw-only via RAW_ONLY_CONCEPTS below and
    # summed with intangible_amortization at derive time
    # (docs/proposals/etl/2026-07-15-fundamentals-dna-ytd-decumulation.md §3.3).
    "depreciation_amort": (
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "DepreciationAmortizationAndAccretionNet",
    ),
    "interest_expense": ("InterestExpense", "InterestExpenseDebt"),
    "income_tax": ("IncomeTaxExpenseBenefit",),
    "shares_basic": ("WeightedAverageNumberOfSharesOutstandingBasic",),
    "shares_diluted": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
    "sbc_expense": ("AllocatedShareBasedCompensationExpense", "ShareBasedCompensation"),
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "shareholders_equity": (
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ),
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
    ),
    "long_term_debt": ("LongTermDebt", "LongTermDebtNoncurrent"),
    "short_term_debt": ("ShortTermBorrowings", "CommercialPaper"),
    "shares_outstanding": ("CommonStockSharesOutstanding",),
    "inventory": ("InventoryNet",),
    "receivables": ("AccountsReceivableNetCurrent",),
    "payables": ("AccountsPayableCurrent",),
    "goodwill": ("Goodwill",),
    "ppe_net": ("PropertyPlantAndEquipmentNet",),
    "operating_cf": ("NetCashProvidedByUsedInOperatingActivities",),
    "investing_cf": ("NetCashProvidedByUsedInInvestingActivities",),
    "financing_cf": ("NetCashProvidedByUsedInFinancingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpenditures"),
    # Cash dividends/distributions paid in the period. Corp issuers
    # use ``PaymentsOfDividends*``; pass-through entities (MLPs, LPs,
    # LLCs taxed as partnerships, REIT operating-partnership tiers)
    # report the same flow under the partnership taxonomy concepts
    # ``DistributionMadeTo{LimitedPartner,MemberOrLimitedPartner,
    # LimitedLiabilityCompanyLLCMember}CashDistributionsPaid``.
    # Without these, IEP / ET / EPD / MPLX / pass-through LLCs land at
    # zero recorded dividends paid post-LP-conversion — see #674.
    "dividends_paid": (
        "PaymentsOfDividends",
        "PaymentsOfDividendsCommonStock",
        "DistributionMadeToLimitedPartnerCashDistributionsPaid",
        "DistributionMadeToMemberOrLimitedPartnerCashDistributionsPaid",
        "DistributionMadeToLimitedLiabilityCompanyLLCMemberCashDistributionsPaid",
    ),
    # Per-unit/share declared. SEC tags partnership distributions
    # under three parallel concepts depending on the issuer's legal
    # form (LP / LP+member-aggregate / pure LLC). All three report
    # in ``USD/shares`` units which the existing ``_UNIT_PRIORITY``
    # list already covers, so no unit-priority change is needed.
    #
    # ``DistributionsPerLimitedPartnershipUnitOutstanding`` is an
    # FY-cumulative concept that issuers report on the 10-K alongside
    # prior-year comparatives. SEC's companyfacts re-stamps the SAME
    # XBRL fact under multiple ``(fy, fp)`` contexts (the 10-K's
    # filing year stamps every prior comparative). Pre-#682 the
    # normaliser keyed on ``(fy, fp)`` and picked the EARLIEST
    # period_end's value as canonical (e.g. IEP's 2023 $6.00 row),
    # which then drove a wrong Q4 = FY − YTD = $4.50 in
    # ``_canonical_merge``. #682 (PR #721) fixed this by filtering
    # value attribution to facts whose ``period_end`` matches the
    # canonical max for the group, so this concept is safe to
    # re-include now as a last-priority fallback for FY summary on
    # issuers that don't emit the primary per-quarter concept.
    "dps_declared": (
        "CommonStockDividendsPerShareDeclared",
        "DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit",
        "DistributionMadeToMemberOrLimitedPartnerDistributionsDeclaredPerUnit",
        "DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsDeclaredPerUnit",
        "DistributionsPerLimitedPartnershipUnitOutstanding",
    ),
    # Cash actually distributed per share (often differs from declared
    # by a quarter). Captured separately so dividend-capture strategies
    # can tell declared-but-not-paid cases from paid. Currently lands
    # in ``financial_facts_raw`` only — `PeriodRow` doesn't surface
    # this column yet, so adding more aliases here is purely about
    # raw-store completeness for future analytics work.
    "dps_cash_paid": (
        "CommonStockDividendsPerShareCashPaid",
        "DistributionMadeToLimitedPartnerCashDistributionsPaidPerUnit",
        "DistributionMadeToMemberOrLimitedPartnerCashDistributionsPaidPerUnit",
        "DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsPaidPerUnit",
    ),
    # Per-share declared amount still payable at period end.
    "dividends_payable_per_share": ("DividendsPayableAmountPerShare",),
    "buyback_spend": ("PaymentsForRepurchaseOfCommonStock",),
    # Share-count deltas for the dilution tracker (#435). Captured as
    # flow (per-period) items — combined with the existing
    # shares_outstanding balance-sheet series this answers "how much
    # was issued over time" vs "how much was repurchased".
    "shares_issued_new": ("StockIssuedDuringPeriodSharesNewIssues",),
    "buyback_shares": (
        "StockRepurchasedDuringPeriodShares",
        "TreasuryStockSharesAcquired",
    ),
    # Effective tax rate — useful for cross-filer comparability when
    # net_income alone obscures tax-regime differences.
    "effective_tax_rate": ("EffectiveIncomeTaxRateContinuingOperations",),
    # Ownership / capital-structure concepts (#731). All four are
    # us-gaap balance-sheet items emitted on the same period_end as
    # the rest of the balance sheet, so they project through the
    # existing _derive_periods_from_facts canonical-end filter
    # without special handling. EntityPublicFloat (DEI cover-page
    # fact, period_end = issuer Q2-end ≠ fiscal year-end) is
    # deferred to #735.
    "treasury_shares": (
        "TreasuryStockShares",
        "TreasuryStockCommonShares",
    ),
    "shares_authorized": ("CommonStockSharesAuthorized",),
    "shares_issued": ("CommonStockSharesIssued",),
    "retained_earnings": ("RetainedEarningsAccumulatedDeficit",),
    # Tier 1 + Tier 2 allowlist expansion (#732). Top-30 most-frequent
    # XBRL concepts that previously landed in financial_facts_raw but
    # were dropped during normalisation. Working-capital + liquidity +
    # supplementary P&L items.
    "assets_current": ("AssetsCurrent",),
    "liabilities_current": ("LiabilitiesCurrent",),
    # FASB ASU 2016-18 concept (post-2017 filings include restricted
    # cash by definition). Kept SEPARATE from the legacy `cash` column
    # because the two concepts differ in scope by design.
    "cash_restricted": ("CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",),
    "comprehensive_income": ("ComprehensiveIncomeNetOfTax",),
    "intangible_amortization": ("AmortizationOfIntangibleAssets",),
    "deferred_income_tax": ("DeferredIncomeTaxExpenseBenefit",),
    "other_nonoperating_income": ("OtherNonoperatingIncomeExpense",),
    "additional_paid_in_capital": ("AdditionalPaidInCapital",),
    "accumulated_oci": ("AccumulatedOtherComprehensiveIncomeLossNetOfTax",),
    # Weighted-average count of share-equivalents excluded from EPS;
    # treated as a point-in-time stock (mirrors how shares_basic /
    # shares_diluted are aggregated — TTM uses latest, not sum).
    "antidilutive_securities": ("AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount",),
}

# DEI (document-and-entity-information) cover-page facts. Thin set —
# only the three that carry ongoing value: point-in-time share count
# (fed into market-cap derivation), public float (SEC filer tier
# reference), and period-focus labels for sanity-checking the fiscal
# frame. Added under #430 as the last yfinance-replacement data
# source. Taxonomy is ``dei`` so ``extract_facts`` routes these to
# the same store as us-gaap with taxonomy-aware grouping.
DEI_TRACKED_CONCEPTS: dict[str, tuple[str, ...]] = {
    "dei_shares_outstanding": ("EntityCommonStockSharesOutstanding",),
    "dei_public_float": ("EntityPublicFloat",),
    "dei_employees": ("EntityNumberOfEmployees",),
}

# #2036: concepts captured into ``financial_facts_raw`` WITHOUT a canonical
# column. ``_TAG_TO_COLUMN`` (app/services/fundamentals) is mechanically built
# from ``TRACKED_CONCEPTS``, so a component concept listed there would enter
# the ``depreciation_amort`` priority pick and win alone; the raw-only split
# keeps components out of the pick while still landing their facts. Consumed
# by the derive-time component-sum fallback
# (``_derive_periods_from_facts``: D&A = Depreciation + intangible_amortization
# when no total-semantics concept is tagged).
RAW_ONLY_CONCEPTS: frozenset[str] = frozenset({"Depreciation"})

_ALL_TRACKED_TAGS: frozenset[str] = (
    frozenset(tag for tags in TRACKED_CONCEPTS.values() for tag in tags) | RAW_ONLY_CONCEPTS
)
_ALL_TRACKED_DEI_TAGS: frozenset[str] = frozenset(tag for tags in DEI_TRACKED_CONCEPTS.values() for tag in tags)


def _zero_pad_cik(cik: str | int) -> str:
    return str(int(cik)).zfill(10)


_UNIT_PRIORITY = ("USD", "USD/shares", "shares", "pure")


def _extract_facts_from_section(
    section: dict[str, Any],
    *,
    taxonomy: str,
    allowed_tags: frozenset[str] | None = None,
    retention_cutoff: date | None = None,
) -> list[XbrlFact]:
    """Extract XBRL facts from one ``facts.<taxonomy>`` section.

    ``taxonomy`` names the XBRL namespace (``us-gaap`` or ``dei``) and
    is stamped onto every emitted fact so downstream consumers can
    partition without string-prefix guessing. ``allowed_tags`` is an
    optional allowlist: when ``None`` every concept in the section is
    emitted; when a frozenset is provided only listed tags survive.
    NOTE (#2036): despite the #451 Phase A intent, EVERY production
    caller passes ``_ALL_TRACKED_TAGS`` / ``_ALL_TRACKED_DEI_TAGS``,
    so ``financial_facts_raw`` holds only the tracked subset (~78
    concepts on dev) — raw-store absence of a concept says nothing
    about issuer tagging. Widening coverage means widening
    ``TRACKED_CONCEPTS`` / ``RAW_ONLY_CONCEPTS`` + re-fetching.

    ``retention_cutoff`` (#1233) — when set, rejects facts whose
    ``period_end`` is strictly before the cutoff with a per-(accession,
    reason) WARN. Bulk ingest callers
    (:func:`app.services.sec_companyfacts_ingest.ingest_companyfacts_archive`)
    pass ``_default_retention_cutoff()`` (= today - 20y) to honour
    the spec at
    ``docs/superpowers/specs/2026-05-19-data-retention-rubric.md`` §4.1.
    Per-CIK API callers omit the cutoff to keep ad-hoc deep-dive
    reads unbounded.
    """
    facts: list[XbrlFact] = []
    # Per-call WARN dedup on ``(accession, reason)`` — without this, a
    # single malformed filing fans out across many concept × unit_key
    # rows and drowns the log signal (#1218 spec §3.2).
    warned_rejections: set[tuple[str, str]] = set()
    for tag_name, fact_data in section.items():
        if allowed_tags is not None and tag_name not in allowed_tags:
            continue
        units = fact_data.get("units", {})
        for unit_key in _UNIT_PRIORITY:
            entries = units.get(unit_key)
            if not entries:
                continue
            for entry in entries:
                try:
                    end_str = entry["end"]
                    val = entry["val"]
                    accn = entry["accn"]
                    form = entry["form"]
                    filed_str = entry["filed"]
                except KeyError:
                    logger.debug("Skipping XBRL entry for %s: missing required field", tag_name)
                    continue

                start_str = entry.get("start")
                try:
                    period_end = date.fromisoformat(end_str)
                    period_start = date.fromisoformat(start_str) if start_str else None
                    filed_date = date.fromisoformat(filed_str)
                except ValueError, TypeError:
                    logger.debug("Skipping XBRL entry for %s: bad date format", tag_name)
                    continue

                # #1218 — reject out-of-window period_end / period_start
                # at the parser before the row reaches
                # ``financial_facts_raw``. The DEFAULT partition (sql/156)
                # has a 5000-row alarm; silently routing parser bugs into
                # it is a non-actionable signal. Skip + WARN with reason
                # + full provenance instead.
                # #1233 — additionally rejects facts older than the 20y
                # retention horizon when ``retention_cutoff`` is set.
                rejection_reason = _classify_period_rejection(
                    period_start, period_end, retention_cutoff=retention_cutoff
                )
                if rejection_reason is not None:
                    key = (accn, rejection_reason)
                    if key not in warned_rejections:
                        warned_rejections.add(key)
                        logger.warning(
                            "XBRL parser: rejecting fact for %s/%s "
                            "(taxonomy=%s, accn=%s, form=%s, filed=%s): "
                            "%s — period_start=%s, period_end=%s; "
                            "window [%s, %s)",
                            tag_name,
                            unit_key,
                            taxonomy,
                            accn,
                            form,
                            filed_str,
                            rejection_reason,
                            start_str or "<null>",
                            end_str,
                            _PERIOD_MIN.isoformat(),
                            _PERIOD_MAX.isoformat(),
                        )
                    continue

                # Guard against NaN/Infinity values in SEC data
                try:
                    decimal_val = Decimal(str(val))
                    if not decimal_val.is_finite():
                        logger.debug("Skipping XBRL entry for %s: non-finite val %s", tag_name, val)
                        continue
                except Exception:
                    logger.debug("Skipping XBRL entry for %s: unparseable val %s", tag_name, val)
                    continue

                facts.append(
                    XbrlFact(
                        concept=tag_name,
                        taxonomy=taxonomy,
                        unit=unit_key,
                        period_start=period_start,
                        period_end=period_end,
                        val=decimal_val,
                        frame=entry.get("frame"),
                        accession_number=accn,
                        form_type=form,
                        filed_date=filed_date,
                        fiscal_year=entry.get("fy"),
                        fiscal_period=entry.get("fp"),
                        decimals=str(entry["decimals"]) if "decimals" in entry else None,
                    )
                )
    return facts


def _catalog_from_payload(raw: dict[str, Any]) -> list[XbrlConceptCatalogEntry]:
    """Helper used by :func:`extract_concept_catalog` to avoid
    duplicating the section walk when the caller only wants the
    catalogue side of a companyfacts payload."""
    facts_block: dict[str, Any] = raw.get("facts", {})
    entries: list[XbrlConceptCatalogEntry] = []
    for tax_name, section in facts_block.items():
        if not isinstance(section, dict):
            continue
        entries.extend(_extract_catalog_from_section(section, taxonomy=str(tax_name)))
    return entries


def _extract_catalog_from_section(section: dict[str, Any], *, taxonomy: str) -> list[XbrlConceptCatalogEntry]:
    """Emit one :class:`XbrlConceptCatalogEntry` per concept present
    in a ``facts.<taxonomy>`` section.

    Populated opportunistically by the ingester so
    ``sec_facts_concept_catalog`` accumulates metadata (label,
    description, observed unit types) for every concept seen across
    every issuer — no additional HTTP round-trips, the data is
    already in the companyfacts response we fetched for the
    fact-level extraction.
    """
    entries: list[XbrlConceptCatalogEntry] = []
    for tag_name, fact_data in section.items():
        if not isinstance(fact_data, dict):
            continue
        label = fact_data.get("label")
        description = fact_data.get("description")
        units_raw = fact_data.get("units")
        units_seen: tuple[str, ...]
        if isinstance(units_raw, dict):
            units_seen = tuple(str(k) for k in units_raw if k)
        else:
            units_seen = ()
        entries.append(
            XbrlConceptCatalogEntry(
                taxonomy=taxonomy,
                concept=tag_name,
                label=str(label) if isinstance(label, str) else None,
                description=str(description) if isinstance(description, str) else None,
                units_seen=units_seen,
            )
        )
    return entries


def _extract_facts_from_gaap(gaap: dict[str, Any]) -> list[XbrlFact]:
    """Back-compat shim — existing callers that pass only the
    ``facts.us-gaap`` section. New code should use
    ``_extract_facts_from_section`` directly with an explicit taxonomy.
    """
    return _extract_facts_from_section(
        gaap,
        taxonomy="us-gaap",
        allowed_tags=_ALL_TRACKED_TAGS,
    )


class SecFundamentalsProvider:
    """
    Fetches raw XBRL facts from the SEC EDGAR Company Facts API.

    Use as a context manager:

        with SecFundamentalsProvider(user_agent=...) as provider:
            facts, catalog = provider.extract_facts_and_catalog("AAPL", "0000320193")

    The ``symbol`` parameter is used for logging only; the ``cik`` drives
    the fetch. The service layer resolves symbol → CIK via
    ``external_identifiers`` before calling.
    """

    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._client = httpx.Client(
            base_url=_BASE_URL,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=30.0,
        )
        # Process-wide shared rate-limit clock (#537) — see
        # ``app.providers.implementations.sec_edgar._PROCESS_RATE_LIMIT_CLOCK``.
        from app.providers.sec_rate_gate_holder import get_sec_rate_gate
        from app.providers.sec_throttle_metrics import incr_sec_429

        _gate = get_sec_rate_gate()
        self._http = ResilientClient(
            self._client,
            min_request_interval_s=_MIN_REQUEST_INTERVAL_S,
            shared_last_request=_PROCESS_RATE_LIMIT_CLOCK,
            shared_throttle_lock=_PROCESS_RATE_LIMIT_LOCK,
            gate=_gate,
            on_429=incr_sec_429,
        )

    def __enter__(self) -> SecFundamentalsProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        """Extract XBRL facts from SEC companyfacts.

        Reads both ``facts.us-gaap`` and ``facts.dei`` sections (the
        latter under #430). DEI facts carry cover-page metadata that
        us-gaap omits — point-in-time share count, public float,
        employee count. Stamped with ``taxonomy='dei'`` so downstream
        normalisation routes them independently.

        **#1233**: applies the canonical companyfacts caps —
        ``_ALL_TRACKED_TAGS`` (us-gaap whitelist) +
        ``_ALL_TRACKED_DEI_TAGS`` (DEI whitelist) +
        ``_default_retention_cutoff()`` (today - 20y). This is the
        steady-state refresh path that runs after the bulk bootstrap;
        without the caps here a subsequent refresh of any CIK would
        re-populate pre-cutoff + off-whitelist rows in
        ``financial_facts_raw`` and undo the bootstrap-time discipline
        (Codex 2 P1 catch).
        """
        raw = self._fetch_company_facts(cik)
        if raw is None:
            return []
        all_facts: dict[str, Any] = raw.get("facts", {})
        gaap_section = all_facts.get("us-gaap", {})
        dei_section = all_facts.get("dei", {})
        if not gaap_section and not dei_section:
            logger.info("No us-gaap or dei facts for %s (CIK %s)", symbol, cik)
            return []
        retention_cutoff = _default_retention_cutoff()
        facts: list[XbrlFact] = []
        if gaap_section:
            facts.extend(
                _extract_facts_from_section(
                    gaap_section,
                    taxonomy="us-gaap",
                    allowed_tags=_ALL_TRACKED_TAGS,
                    retention_cutoff=retention_cutoff,
                )
            )
        if dei_section:
            facts.extend(
                _extract_facts_from_section(
                    dei_section,
                    taxonomy="dei",
                    allowed_tags=_ALL_TRACKED_DEI_TAGS,
                    retention_cutoff=retention_cutoff,
                )
            )
        return facts

    def extract_concept_catalog(self, symbol: str, cik: str) -> list[XbrlConceptCatalogEntry]:
        """Extract per-concept metadata (label, description, units)
        from SEC companyfacts.

        Issues its own HTTP fetch; callers that already hold the
        companyfacts payload should use
        :func:`extract_facts_and_catalog` instead to avoid double
        round-trips (#451).
        """
        raw = self._fetch_company_facts(cik)
        if raw is None:
            return []
        return _catalog_from_payload(raw)

    def extract_facts_and_catalog(self, symbol: str, cik: str) -> tuple[list[XbrlFact], list[XbrlConceptCatalogEntry]]:
        """Single-fetch convenience: return both the fact rows AND
        the concept-catalogue entries from one companyfacts payload.

        Preferred call for the ingester path so we don't double
        round-trip SEC for facts + catalogue on every refresh.

        **#1233**: applies the canonical companyfacts caps to the
        fact stream (whitelist + 20y retention cutoff). Catalogue
        entries stay uncapped — the catalogue is a per-concept
        metadata snapshot (label, description, units) keyed on
        concept name; capping the catalogue would orphan downstream
        UI rendering for any concept that exists in catalogue but
        was filtered from facts.
        """
        raw = self._fetch_company_facts(cik)
        if raw is None:
            return [], []
        all_facts: dict[str, Any] = raw.get("facts", {})
        gaap_section = all_facts.get("us-gaap", {})
        dei_section = all_facts.get("dei", {})
        if not gaap_section and not dei_section:
            logger.info("No us-gaap or dei facts for %s (CIK %s)", symbol, cik)
            return [], []
        retention_cutoff = _default_retention_cutoff()
        facts: list[XbrlFact] = []
        entries: list[XbrlConceptCatalogEntry] = []
        if gaap_section:
            facts.extend(
                _extract_facts_from_section(
                    gaap_section,
                    taxonomy="us-gaap",
                    allowed_tags=_ALL_TRACKED_TAGS,
                    retention_cutoff=retention_cutoff,
                )
            )
            entries.extend(_extract_catalog_from_section(gaap_section, taxonomy="us-gaap"))
        if dei_section:
            facts.extend(
                _extract_facts_from_section(
                    dei_section,
                    taxonomy="dei",
                    allowed_tags=_ALL_TRACKED_DEI_TAGS,
                    retention_cutoff=retention_cutoff,
                )
            )
            entries.extend(_extract_catalog_from_section(dei_section, taxonomy="dei"))
        return facts, entries

    # ------------------------------------------------------------------
    # Companyconcept API — single-tag primitive (G10, 2026-05-17)
    # ------------------------------------------------------------------

    def fetch_concept(
        self,
        cik: str,
        taxonomy: str,
        tag: str,
    ) -> dict[str, Any] | None:
        """Fetch one XBRL concept for a CIK from the companyconcept API.

        Returns parsed JSON or None on 404. Other HTTP errors propagate
        via ``raise_for_status()``.

        Args:
          cik: 10-digit zero-padded OR int-able string; normalised via
            ``_zero_pad_cik``.
          taxonomy: SEC namespace identifier — ASCII regex
            ``[a-z][a-z0-9-]*`` matched via ``fullmatch`` (e.g.
            ``us-gaap``, ``dei``, ``srt``, ``invest``, ``ifrs-full``).
            The primitive is intentionally a general SEC
            ``companyconcept`` consumer, NOT bound to
            ``TRACKED_CONCEPTS`` / ``DEI_TRACKED_CONCEPTS`` — those
            maps govern downstream normalisation, not arbitrary probe
            access. See spec
            ``docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md``
            §4.2.
          tag: XBRL concept name — ASCII regex
            ``[A-Za-z][A-Za-z0-9_]*`` matched via ``fullmatch``
            (e.g. ``Revenues``, ``EntityCommonStockSharesOutstanding``,
            ``my_custom_concept``). Leading-letter anchored; rejects
            bare numerics and non-ASCII alnum.

        Raises:
          ValueError: ``taxonomy`` or ``tag`` fails the regex.

        Raw-payload invariant for future consumers (spec §3.3): if a
        subsequent PR wires ``extract_concept_facts`` (or any helper
        that consumes this payload) into a DB-writing job, that PR
        MUST land raw-payload persistence per
        ``docs/review-prevention-log.md`` #1168 IN THE SAME PR —
        either by extending an existing raw table or by introducing
        a sibling ``sec_companyconcept_raw`` table.
        """
        if not _TAXONOMY_RE.fullmatch(taxonomy):
            raise ValueError(
                f"invalid taxonomy {taxonomy!r}: expected lowercase "
                "ASCII + dashes (regex [a-z][a-z0-9-]*) with no "
                "leading dash, whitespace, or trailing newline"
            )
        if not _CONCEPT_TAG_RE.fullmatch(tag):
            raise ValueError(
                f"invalid tag {tag!r}: expected ASCII letters / digits "
                "/ underscores starting with a letter (regex "
                "[A-Za-z][A-Za-z0-9_]*) with no whitespace, slash, "
                "or trailing newline"
            )
        cik_padded = _zero_pad_cik(cik)
        path = f"/api/xbrl/companyconcept/CIK{cik_padded}/{taxonomy}/{tag}.json"
        resp = self._http.get(path)
        if resp.status_code == 404:
            logger.info(
                "SEC companyconcept: no data for CIK %s %s/%s (404)",
                cik,
                taxonomy,
                tag,
            )
            return None
        resp.raise_for_status()
        raw = resp.json()
        return raw  # type: ignore[no-any-return]

    def extract_concept_facts(
        self,
        symbol: str,
        cik: str,
        taxonomy: str,
        tag: str,
    ) -> list[XbrlFact]:
        """Single-concept variant of :meth:`extract_facts`.

        Fetches the companyconcept response and returns the same
        ``XbrlFact`` rows the existing extractor produces for one tag
        inside a companyfacts payload. Empty list on 404 or empty
        units.

        Source-of-truth for the emitted ``XbrlFact.taxonomy`` is the
        REQUEST argument, not the response field — the request was
        validated, the response can shape-drift. A mismatch between
        request and response taxonomy logs a warning before
        proceeding with the request value.
        """
        payload = self.fetch_concept(cik, taxonomy, tag)
        if payload is None:
            return []
        response_taxonomy = payload.get("taxonomy")
        if response_taxonomy is not None and response_taxonomy != taxonomy:
            logger.warning(
                "companyconcept response taxonomy %r differs from request "
                "%r for CIK %s tag %s — using request taxonomy",
                response_taxonomy,
                taxonomy,
                cik,
                tag,
            )
        units = payload.get("units")
        if not isinstance(units, dict):
            logger.warning(
                "companyconcept payload for CIK %s %s/%s missing or non-dict units — returning empty fact list",
                cik,
                taxonomy,
                tag,
            )
            units = {}
        section = {tag: {"units": units}}
        return _extract_facts_from_section(section, taxonomy=taxonomy)

    # ------------------------------------------------------------------
    # Frames API — cross-sectional primitive (G11, 2026-05-18)
    # ------------------------------------------------------------------

    def fetch_frame(
        self,
        taxonomy: str,
        tag: str,
        unit: str,
        period: str,
    ) -> dict[str, Any] | None:
        """Fetch one cross-sectional frame from the SEC frames API.

        Returns parsed JSON or None on 404. Other HTTP errors propagate
        via ``raise_for_status()``.

        Args:
          taxonomy: SEC namespace identifier — ASCII regex
            ``[a-z](?:[a-z0-9-]*[a-z0-9])?`` matched via ``fullmatch``
            (e.g. ``us-gaap``, ``dei``, ``srt``, ``invest``,
            ``ifrs-full``). Reuses ``_TAXONOMY_RE`` from the G10
            primitive.
          tag: XBRL concept name — ASCII regex
            ``[A-Za-z][A-Za-z0-9_]*`` matched via ``fullmatch``.
            Reuses ``_CONCEPT_TAG_RE`` from the G10 primitive.
          unit: XBRL unit identifier — common values ``USD``,
            ``shares``, ``pure``, ``USD-per-shares`` (NOT
            ``USD/shares`` — SEC frames URLs use the ``-per-``
            syntax; a slash becomes an extra path segment in the
            f-string). Validated via ``_UNIT_RE.fullmatch`` against an
            explicit ``token(-per-token)?`` grammar — rejects
            ``USD-per-`` (trailing dash), ``USD-per`` (bare
            ``-per``), ``USD--per-shares`` (double dash), slash, and
            every URL-special character.
          period: Calendar-period frame identifier per SEC frames
            spec — ``CY{year}`` (annual flow), ``CY{year}Q{n}``
            (quarterly flow), or ``CY{year}Q{n}I`` (quarterly
            instantaneous / balance-sheet). Rejects ``CY{year}I``
            (no annual-instantaneous frame per SEC docs). Validated
            via ``_PERIOD_RE.fullmatch``.

        Raises:
          ValueError: any argument fails its regex.

        Raw-payload invariant for future consumers (spec §3.2): if a
        subsequent PR wires ``fetch_frame(...)`` payloads into a
        DB-writing path (``sec_frames_*`` / ``sector_aggregate_*`` /
        ``*_observations``), that PR MUST land raw-payload
        persistence per ``docs/review-prevention-log.md`` #1168 IN
        THE SAME PR — either by extending an existing raw table or
        by introducing a sibling ``sec_frames_raw`` table keyed on
        ``(taxonomy, tag, unit, period, fetched_at)``. Splitting
        persistence into a follow-up is forbidden.
        """
        if not _TAXONOMY_RE.fullmatch(taxonomy):
            raise ValueError(
                f"invalid taxonomy {taxonomy!r}: expected lowercase "
                "ASCII + dashes (regex [a-z](?:[a-z0-9-]*[a-z0-9])?) "
                "with no leading/trailing dash, whitespace, or "
                "trailing newline"
            )
        if not _CONCEPT_TAG_RE.fullmatch(tag):
            raise ValueError(
                f"invalid tag {tag!r}: expected ASCII letters / digits "
                "/ underscores starting with a letter (regex "
                "[A-Za-z][A-Za-z0-9_]*) with no whitespace, slash, "
                "or trailing newline"
            )
        if not _UNIT_RE.fullmatch(unit):
            raise ValueError(
                f"invalid unit {unit!r}: expected ASCII "
                "token(-per-token)? where each token is "
                "[A-Za-z][A-Za-z0-9]* (e.g. 'USD', 'USD-per-shares', "
                "'shares', 'pure'). SEC frames URLs use the `-per-` "
                "syntax, NOT `/`."
            )
        if not _PERIOD_RE.fullmatch(period):
            raise ValueError(
                f"invalid period {period!r}: expected SEC frames "
                "identifier CY{year} / CY{year}Q{n} / CY{year}Q{n}I "
                "(no annual-instantaneous frame; regex "
                "CY[0-9]{4}(?:Q[1-4]I?)?)"
            )
        path = f"/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json"
        resp = self._http.get(path)
        if resp.status_code == 404:
            logger.info(
                "SEC frames: no data for %s/%s/%s/%s (404)",
                taxonomy,
                tag,
                unit,
                period,
            )
            return None
        resp.raise_for_status()
        raw = resp.json()
        return raw  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Private HTTP
    # ------------------------------------------------------------------

    def _fetch_company_facts(self, cik: str) -> dict[str, Any] | None:
        cik_padded = _zero_pad_cik(cik)
        path = f"/api/xbrl/companyfacts/CIK{cik_padded}.json"
        resp = self._http.get(path)
        if resp.status_code == 404:
            logger.info("SEC fundamentals: no company facts for CIK %s", cik)
            return None
        resp.raise_for_status()
        raw = resp.json()
        return raw  # type: ignore[no-any-return]
