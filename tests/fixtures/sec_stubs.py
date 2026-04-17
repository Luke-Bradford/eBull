"""Shared stubs for SEC provider tests (planner + executor + integration)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_edgar import MasterIndexFetchResult


@dataclass
class StubFilingsProvider:
    """Stands in for SecFilingsProvider. Tracks call counts so tests
    can assert the planner short-circuits on steady-state days."""

    master_bodies: dict[date, bytes | None] = field(default_factory=dict)
    submissions_by_cik: dict[str, dict[str, object]] = field(default_factory=dict)
    fetch_master_calls: int = 0
    fetch_submissions_calls: int = 0

    def fetch_master_index(
        self,
        target_date: date,
        *,
        if_modified_since: str | None = None,
    ) -> MasterIndexFetchResult | None:
        self.fetch_master_calls += 1
        body = self.master_bodies.get(target_date)
        if body is None:
            return None
        return MasterIndexFetchResult(
            body=body,
            body_hash=f"hash-{target_date.isoformat()}",
            last_modified=f"lm-{target_date.isoformat()}",
        )

    def fetch_submissions(self, cik: str) -> dict[str, object] | None:
        self.fetch_submissions_calls += 1
        return self.submissions_by_cik.get(cik)

    def __enter__(self) -> StubFilingsProvider:
        return self

    def __exit__(self, *_: object) -> None:
        return None


@dataclass
class StubFundamentalsProvider:
    """Stands in for SecFundamentalsProvider."""

    facts_by_cik: dict[str, list[XbrlFact]] = field(default_factory=dict)
    fail_on: set[str] = field(default_factory=set)
    extract_calls: list[str] = field(default_factory=list)

    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        self.extract_calls.append(cik)
        if cik in self.fail_on:
            raise RuntimeError(f"boom for {cik}")
        return self.facts_by_cik.get(cik, [])

    def __enter__(self) -> StubFundamentalsProvider:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def sample_fact(accession: str) -> XbrlFact:
    return XbrlFact(
        concept="Revenues",
        taxonomy="us-gaap",
        unit="USD",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        val=Decimal("90000000000"),
        # Frame must be set for duration facts; normalizer filters out
        # ``frame IS NULL`` duration rows as YTD cumulative.
        frame="CY2026Q1",
        accession_number=accession,
        form_type="10-Q",
        filed_date=date(2026, 4, 15),
        fiscal_year=2026,
        fiscal_period="Q1",
        decimals="-6",
    )


def submissions_json(accession: str, form: str = "10-Q") -> dict[str, object]:
    return {
        "filings": {
            "recent": {
                "accessionNumber": [accession],
                "form": [form],
                "acceptedDate": ["2026-04-15T16:05:00.000Z"],
            }
        }
    }
