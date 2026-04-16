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
