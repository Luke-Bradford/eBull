"""Tests for expanded XBRL fact extraction from SEC companyfacts."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_fundamentals import _extract_facts_from_gaap


def _make_xbrl_entry(
    *,
    end: str,
    val: float,
    form: str = "10-Q",
    fp: str = "Q1",
    fy: int = 2024,
    filed: str = "2024-05-01",
    accn: str = "0000320193-24-000042",
    start: str | None = "2024-01-01",
    frame: str | None = "CY2024Q1",
    decimals: int | str = -3,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "end": end,
        "val": val,
        "form": form,
        "fp": fp,
        "fy": fy,
        "filed": filed,
        "accn": accn,
        "frame": frame,
    }
    if start is not None:
        entry["start"] = start
    if decimals is not None:
        entry["decimals"] = decimals
    return entry


class TestXbrlFactDataclass:
    def test_create_duration_fact(self) -> None:
        fact = XbrlFact(
            concept="Revenues",
            taxonomy="us-gaap",
            unit="USD",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 3, 31),
            val=Decimal("1000000.00"),
            frame="CY2024Q1",
            accession_number="0000320193-24-000042",
            form_type="10-Q",
            filed_date=date(2024, 5, 1),
            fiscal_year=2024,
            fiscal_period="Q1",
            decimals="-3",
        )
        assert fact.concept == "Revenues"
        assert fact.period_start == date(2024, 1, 1)
        assert fact.val == Decimal("1000000.00")

    def test_create_instant_fact(self) -> None:
        fact = XbrlFact(
            concept="Assets",
            taxonomy="us-gaap",
            unit="USD",
            period_start=None,
            period_end=date(2024, 3, 31),
            val=Decimal("500000000.00"),
            frame=None,
            accession_number="0000320193-24-000042",
            form_type="10-Q",
            filed_date=date(2024, 5, 1),
            fiscal_year=2024,
            fiscal_period="Q1",
            decimals="-6",
        )
        assert fact.period_start is None
        assert fact.frame is None


class TestExtractFactsFromGaap:
    def test_extracts_revenue_facts(self) -> None:
        gaap = {
            "Revenues": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(end="2024-03-31", val=50_000_000.0),
                        _make_xbrl_entry(
                            end="2024-06-30",
                            val=55_000_000.0,
                            fp="Q2",
                            frame="CY2024Q2",
                            accn="0000320193-24-000050",
                        ),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 2
        assert facts[0].concept == "Revenues"
        assert facts[0].val == Decimal("50000000")
        assert facts[0].period_end == date(2024, 3, 31)
        assert facts[0].frame == "CY2024Q1"

    def test_extracts_both_priority_tags(self) -> None:
        """Both tags are extracted — priority logic is in normalization, not extraction."""
        gaap = {
            "RevenueFromContractWithCustomerExcludingAssessedTax": {
                "units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=50_000_000.0)]}
            },
            "Revenues": {"units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=49_000_000.0)]}},
        }
        facts = _extract_facts_from_gaap(gaap)
        concepts = {f.concept for f in facts}
        assert "RevenueFromContractWithCustomerExcludingAssessedTax" in concepts
        assert "Revenues" in concepts

    def test_handles_instant_items(self) -> None:
        gaap = {
            "Assets": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(
                            end="2024-03-31",
                            val=300_000_000_000.0,
                            start=None,
                            frame=None,
                        ),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].period_start is None
        assert facts[0].concept == "Assets"

    def test_handles_shares_unit(self) -> None:
        gaap = {
            "CommonStockSharesOutstanding": {
                "units": {
                    "shares": [
                        _make_xbrl_entry(
                            end="2024-03-31",
                            val=15_334_000_000.0,
                            start=None,
                            frame=None,
                        ),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].unit == "shares"

    def test_handles_per_share_unit(self) -> None:
        gaap = {
            "EarningsPerShareDiluted": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2024-03-31", val=1.53),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].unit == "USD/shares"

    def test_skips_untracked_tags(self) -> None:
        gaap = {
            "SomeRandomTag": {"units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=999.0)]}},
            "Assets": {"units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=100.0, start=None, frame=None)]}},
        }
        facts = _extract_facts_from_gaap(gaap)
        concepts = {f.concept for f in facts}
        assert "SomeRandomTag" not in concepts
        assert "Assets" in concepts

    def test_empty_gaap_returns_empty(self) -> None:
        facts = _extract_facts_from_gaap({})
        assert facts == []

    def test_handles_inf_decimals(self) -> None:
        """XBRL allows 'INF' as decimals value — should be stored as string."""
        gaap = {
            "Revenues": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(end="2024-03-31", val=50_000_000.0, decimals="INF"),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].decimals == "INF"

    def test_missing_required_fields_skips_entry(self) -> None:
        gaap = {
            "Revenues": {
                "units": {
                    "USD": [
                        {"form": "10-Q", "fp": "Q1"},
                        _make_xbrl_entry(end="2024-03-31", val=50_000_000.0),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1


# ---------------------------------------------------------------------------
# DEI taxonomy extraction (#430)
# ---------------------------------------------------------------------------


class TestDeiExtraction:
    """DEI cover-page facts get their own taxonomy tag + allowlist so
    downstream consumers can partition by ``fact.taxonomy``."""

    def test_entity_common_stock_shares_outstanding_extracted_under_dei(self) -> None:
        from app.providers.implementations.sec_fundamentals import (
            _ALL_TRACKED_DEI_TAGS,
            _extract_facts_from_section,
        )

        dei = {
            "EntityCommonStockSharesOutstanding": {
                "units": {
                    "shares": [
                        _make_xbrl_entry(
                            end="2024-03-31",
                            val=15_000_000_000.0,
                            start=None,  # point-in-time fact
                        )
                    ]
                }
            }
        }
        facts = _extract_facts_from_section(dei, taxonomy="dei", allowed_tags=_ALL_TRACKED_DEI_TAGS)
        assert len(facts) == 1
        assert facts[0].taxonomy == "dei"
        assert facts[0].concept == "EntityCommonStockSharesOutstanding"
        assert facts[0].val == Decimal("15000000000.0")

    def test_non_allowlisted_dei_tag_skipped(self) -> None:
        from app.providers.implementations.sec_fundamentals import (
            _ALL_TRACKED_DEI_TAGS,
            _extract_facts_from_section,
        )

        dei = {
            # Real DEI tag we don't track — should NOT land.
            "DocumentType": {"units": {"pure": [_make_xbrl_entry(end="2024-03-31", val=10.0)]}}
        }
        facts = _extract_facts_from_section(dei, taxonomy="dei", allowed_tags=_ALL_TRACKED_DEI_TAGS)
        assert facts == []

    def test_employees_and_public_float_both_tracked(self) -> None:
        from app.providers.implementations.sec_fundamentals import (
            _ALL_TRACKED_DEI_TAGS,
            _extract_facts_from_section,
        )

        dei = {
            "EntityNumberOfEmployees": {
                "units": {"pure": [_make_xbrl_entry(end="2024-09-30", val=150_000.0, start=None)]}
            },
            "EntityPublicFloat": {
                "units": {"USD": [_make_xbrl_entry(end="2024-06-30", val=2_800_000_000.0, start=None)]}
            },
        }
        facts = _extract_facts_from_section(dei, taxonomy="dei", allowed_tags=_ALL_TRACKED_DEI_TAGS)
        concepts = {f.concept for f in facts}
        assert concepts == {"EntityNumberOfEmployees", "EntityPublicFloat"}
        assert all(f.taxonomy == "dei" for f in facts)


class TestExtendedUsGaapConcepts:
    """#430: expanded us-gaap set (dps_cash_paid, shares_issued_new,
    buyback_shares, effective_tax_rate, dividends_payable_per_share)."""

    def test_dps_cash_paid_extracted(self) -> None:
        gaap = {
            "CommonStockDividendsPerShareCashPaid": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2024-03-31", val=0.24, decimals=4),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "CommonStockDividendsPerShareCashPaid"

    def test_shares_issued_new_extracted(self) -> None:
        gaap = {
            "StockIssuedDuringPeriodSharesNewIssues": {
                "units": {"shares": [_make_xbrl_entry(end="2024-03-31", val=100_000.0)]}
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1

    def test_effective_tax_rate_extracted(self) -> None:
        gaap = {
            "EffectiveIncomeTaxRateContinuingOperations": {
                "units": {"pure": [_make_xbrl_entry(end="2024-03-31", val=0.21, decimals=4)]}
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1

    def test_buyback_shares_alt_tag_extracted(self) -> None:
        # TreasuryStockSharesAcquired is the alternate tag some filers use.
        gaap = {
            "TreasuryStockSharesAcquired": {"units": {"shares": [_make_xbrl_entry(end="2024-03-31", val=5_000_000.0)]}}
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "TreasuryStockSharesAcquired"


class TestPartnershipDistributionConcepts:
    """#674: pass-through entities (MLPs, LPs, LLCs taxed as partnerships)
    file dividend / distribution facts under partnership-specific
    XBRL tags. Without these in the allowlist, IEP / ET / EPD / MPLX
    etc. landed at zero declared DPS in dividend_history despite
    continuing to file quarterly distributions on SEC.

    Live cross-check: SEC ships
    ``DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit``
    on Icahn Enterprises (CIK 0000813762) quarterly under
    ``USD/shares`` units — the same priority slot the corp-style
    ``CommonStockDividendsPerShareDeclared`` already used."""

    def test_lp_distributions_declared_per_unit_extracted(self) -> None:
        gaap = {
            "DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2025-09-30", val=0.50, fp="Q3", fy=2025),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit"
        assert facts[0].val == Decimal("0.5")
        assert facts[0].unit == "USD/shares"

    def test_lp_legacy_distributions_per_unit_outstanding_extracted(self) -> None:
        # #682 (PR #721) fixed the normaliser's prior-year-comparative
        # re-stamping bug, so this FY-cumulative concept is now safely
        # included as a last-priority ``dps_declared`` fallback for
        # issuers that don't emit the primary per-quarter concept.
        gaap = {
            "DistributionsPerLimitedPartnershipUnitOutstanding": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2024-12-31", val=1.25, fp="Q4", fy=2024),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionsPerLimitedPartnershipUnitOutstanding"
        assert facts[0].val == Decimal("1.25")

    def test_lp_cash_distributions_paid_per_unit_extracted(self) -> None:
        gaap = {
            "DistributionMadeToLimitedPartnerCashDistributionsPaidPerUnit": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2025-06-30", val=0.50, fp="Q2", fy=2025),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToLimitedPartnerCashDistributionsPaidPerUnit"

    def test_llc_member_distributions_paid_per_unit_extracted(self) -> None:
        # LLC variant for entities that file LLC member distribution
        # rather than LP unit distribution (e.g. some MLPs structured
        # as LLCs taxed as partnerships).
        gaap = {
            "DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsPaidPerUnit": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2024-09-30", val=0.75, fp="Q3", fy=2024),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsPaidPerUnit"

    def test_lp_aggregate_cash_distributions_paid_extracted(self) -> None:
        gaap = {
            "DistributionMadeToLimitedPartnerCashDistributionsPaid": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(end="2025-12-31", val=200_000_000.0, fp="FY", fy=2025),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToLimitedPartnerCashDistributionsPaid"
        assert facts[0].unit == "USD"

    def test_member_or_lp_aggregate_cash_distributions_paid_extracted(self) -> None:
        gaap = {
            "DistributionMadeToMemberOrLimitedPartnerCashDistributionsPaid": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(end="2025-12-31", val=150_000_000.0, fp="FY", fy=2025),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1

    def test_member_or_lp_declared_per_unit_extracted(self) -> None:
        # LP+member-aggregate variant of the per-unit declared concept.
        gaap = {
            "DistributionMadeToMemberOrLimitedPartnerDistributionsDeclaredPerUnit": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2025-09-30", val=0.30, fp="Q3", fy=2025),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToMemberOrLimitedPartnerDistributionsDeclaredPerUnit"

    def test_llc_member_declared_per_unit_extracted(self) -> None:
        # Pure-LLC variant (e.g. some MLPs structured as LLCs taxed
        # as partnerships emit this rather than the LP-side concept).
        gaap = {
            "DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsDeclaredPerUnit": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2024-12-31", val=2.10, fp="FY", fy=2024),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsDeclaredPerUnit"

    def test_llc_member_aggregate_cash_distributions_paid_extracted(self) -> None:
        # Pure-LLC paid-aggregate counterpart to the LP variant.
        gaap = {
            "DistributionMadeToLimitedLiabilityCompanyLLCMemberCashDistributionsPaid": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(end="2025-12-31", val=180_000_000.0, fp="FY", fy=2025),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].concept == "DistributionMadeToLimitedLiabilityCompanyLLCMemberCashDistributionsPaid"


class TestPartnershipDistributionAliasing:
    """The TRACKED_CONCEPTS allowlist is also the alias map that
    drives the canonical projection in financial_periods. Verify each
    new partnership tag routes to the correct canonical column with
    the expected priority — corp-style stays priority 0 (so legacy
    converters that file BOTH tags don't double-count or flip)."""

    def test_lp_declared_aliases_to_dps_declared_at_priority_after_corp(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, prio = _TAG_TO_COLUMN["DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit"]
        assert col == "dps_declared"
        # Corp-style is priority 0; LP-style must be a higher index so
        # the corp-style tag wins when both exist for the same period.
        assert prio > 0
        corp_col, corp_prio = _TAG_TO_COLUMN["CommonStockDividendsPerShareDeclared"]
        assert corp_col == "dps_declared"
        assert corp_prio == 0
        assert prio > corp_prio

    def test_lp_legacy_per_unit_aliases_to_dps_declared_lowest_priority(self) -> None:
        # #682 (PR #721) fixed the normaliser's prior-year-comparative
        # re-stamping bug, so this concept is now safe to include as a
        # last-priority fallback for FY summary on issuers that don't
        # emit the primary per-quarter concept.
        from app.services.fundamentals import _TAG_TO_COLUMN

        assert "DistributionsPerLimitedPartnershipUnitOutstanding" in _TAG_TO_COLUMN
        col, prio = _TAG_TO_COLUMN["DistributionsPerLimitedPartnershipUnitOutstanding"]
        assert col == "dps_declared"
        # Must be lowest priority (highest index) — primary per-quarter
        # concepts take precedence when present.
        primary_col, primary_prio = _TAG_TO_COLUMN["DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit"]
        assert primary_col == "dps_declared"
        assert prio > primary_prio

    def test_lp_cash_paid_per_unit_aliases_to_dps_cash_paid(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToLimitedPartnerCashDistributionsPaidPerUnit"]
        assert col == "dps_cash_paid"

    def test_llc_member_cash_paid_per_unit_aliases_to_dps_cash_paid(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsPaidPerUnit"]
        assert col == "dps_cash_paid"

    def test_lp_aggregate_cash_paid_aliases_to_dividends_paid(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToLimitedPartnerCashDistributionsPaid"]
        assert col == "dividends_paid"

    def test_member_or_lp_aggregate_cash_paid_aliases_to_dividends_paid(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToMemberOrLimitedPartnerCashDistributionsPaid"]
        assert col == "dividends_paid"

    def test_llc_member_aggregate_cash_paid_aliases_to_dividends_paid(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToLimitedLiabilityCompanyLLCMemberCashDistributionsPaid"]
        assert col == "dividends_paid"

    def test_member_or_lp_declared_per_unit_aliases_to_dps_declared(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToMemberOrLimitedPartnerDistributionsDeclaredPerUnit"]
        assert col == "dps_declared"

    def test_llc_member_declared_per_unit_aliases_to_dps_declared(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToLimitedLiabilityCompanyLLCMemberDistributionsDeclaredPerUnit"]
        assert col == "dps_declared"

    def test_member_or_lp_paid_per_unit_aliases_to_dps_cash_paid(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col, _ = _TAG_TO_COLUMN["DistributionMadeToMemberOrLimitedPartnerCashDistributionsPaidPerUnit"]
        assert col == "dps_cash_paid"


# ---------------------------------------------------------------------------
# Treasury stock shares concept aliasing (#838 / #788 P0c)
# ---------------------------------------------------------------------------


class TestTreasuryStockSharesAliasing:
    """Pin the ``treasury_shares`` concept aliases. Operator audit
    2026-05-03 found ownership rollup banner reporting NULL for every
    instrument — root cause traced to extraction + normalization being
    correct but sparse SEC reporting (AAPL retires buybacks instead of
    holding treasury). Tests guard against accidental removal of the
    canonical concepts so issuers who DO report (JPM, HD, MCD, etc.)
    keep flowing through to the chart."""

    def test_treasury_stock_shares_in_tracked_concepts(self) -> None:
        from app.providers.implementations.sec_fundamentals import TRACKED_CONCEPTS

        assert "treasury_shares" in TRACKED_CONCEPTS
        tags = TRACKED_CONCEPTS["treasury_shares"]
        # Both the legacy ``TreasuryStockShares`` and the modern
        # ``TreasuryStockCommonShares`` must be present. Removing
        # either silently halves coverage (issuers vary by accounting
        # vintage / standard).
        assert "TreasuryStockShares" in tags
        assert "TreasuryStockCommonShares" in tags

    def test_treasury_stock_shares_aliased_to_canonical_column(self) -> None:
        from app.services.fundamentals import _TAG_TO_COLUMN

        col_a, _ = _TAG_TO_COLUMN["TreasuryStockShares"]
        col_b, _ = _TAG_TO_COLUMN["TreasuryStockCommonShares"]
        assert col_a == "treasury_shares"
        assert col_b == "treasury_shares"

    def test_treasury_priority_legacy_outranks_common(self) -> None:
        """``TreasuryStockShares`` (legacy) is priority 0;
        ``TreasuryStockCommonShares`` (modern) is fallback. Pinning
        order guards against an accidental swap that would silently
        flip the value-source for filers that emit both."""
        from app.services.fundamentals import _TAG_TO_COLUMN

        _, prio_legacy = _TAG_TO_COLUMN["TreasuryStockShares"]
        _, prio_modern = _TAG_TO_COLUMN["TreasuryStockCommonShares"]
        assert prio_legacy < prio_modern
