"""
Fundamentals provider data shapes.

Raw XBRL fact + concept-catalogue containers shared between the SEC
provider and the fundamentals service layer. Period selection / TTM
assembly is NOT a provider concern: ``fundamentals_snapshot`` is a
write-through from the normalized ``financial_periods`` rows (#2008),
so there is no snapshot dataclass or provider interface here anymore.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class XbrlFact:
    """Single XBRL fact extracted from SEC companyfacts response."""

    concept: str
    taxonomy: str
    unit: str
    period_start: date | None
    period_end: date
    val: Decimal
    frame: str | None
    accession_number: str
    form_type: str
    filed_date: date
    fiscal_year: int | None
    fiscal_period: str | None
    decimals: str | None  # XBRL allows non-integer values like "INF"


@dataclass(frozen=True)
class XbrlConceptCatalogEntry:
    """Per-concept metadata extracted from the SEC companyfacts JSON.

    The companyfacts response carries a ``label`` + ``description``
    alongside each concept's ``units`` block. Capturing these into
    ``sec_facts_concept_catalog`` (#451) lets the UI and analysis
    queries render a human-readable concept name without hard-coding
    a Python alias map for the entire XBRL taxonomy.
    """

    taxonomy: str
    concept: str
    label: str | None
    description: str | None
    # Unit types observed for this concept in the current response.
    # ``sec_facts_concept_catalog.units_seen`` accumulates the union
    # across every ingest pass so a concept reporting in multiple
    # units over time is represented truthfully.
    units_seen: tuple[str, ...]
