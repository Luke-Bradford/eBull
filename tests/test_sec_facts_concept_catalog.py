"""Tests for the SEC facts concept catalogue (#451).

Unit-level: ``_extract_catalog_from_section`` + ``_catalog_from_payload``
over a realistic companyfacts shape.

Integration-level: ``upsert_concept_catalog`` against the live test DB
— ON CONFLICT merges units_seen and refreshes label/description.
"""

from __future__ import annotations

import psycopg
import pytest

from app.providers.fundamentals import XbrlConceptCatalogEntry
from app.providers.implementations.sec_fundamentals import (
    _catalog_from_payload,
    _extract_catalog_from_section,
    _extract_facts_from_section,
)

_SECTION = {
    "Revenues": {
        "label": "Revenues",
        "description": "Amount of revenue recognised from goods sold, services rendered...",
        "units": {
            "USD": [
                {
                    "accn": "0000320193-24-000001",
                    "end": "2024-09-30",
                    "val": 383285000000,
                    "form": "10-K",
                    "fp": "FY",
                    "fy": 2024,
                    "filed": "2024-11-01",
                },
            ]
        },
    },
    # Un-tracked concept — previously dropped by the allowed_tags
    # filter, now captured under #451.
    "OperatingLeaseLiabilityNoncurrent": {
        "label": "Operating Lease Liability, Noncurrent",
        "description": "Present value of lease payments not yet paid...",
        "units": {
            "USD": [
                {
                    "accn": "0000320193-24-000001",
                    "end": "2024-09-30",
                    "val": 11000000000,
                    "form": "10-K",
                    "fp": "FY",
                    "fy": 2024,
                    "filed": "2024-11-01",
                },
            ]
        },
    },
}


class TestExtractCatalog:
    def test_section_emits_entry_per_concept(self) -> None:
        entries = _extract_catalog_from_section(_SECTION, taxonomy="us-gaap")
        codes = {e.concept for e in entries}
        assert "Revenues" in codes
        assert "OperatingLeaseLiabilityNoncurrent" in codes

    def test_label_and_description_preserved_verbatim(self) -> None:
        entries = _extract_catalog_from_section(_SECTION, taxonomy="us-gaap")
        rev = next(e for e in entries if e.concept == "Revenues")
        assert rev.label == "Revenues"
        assert rev.description is not None
        assert "revenue recognised" in rev.description

    def test_units_seen_captured(self) -> None:
        entries = _extract_catalog_from_section(_SECTION, taxonomy="us-gaap")
        rev = next(e for e in entries if e.concept == "Revenues")
        assert "USD" in rev.units_seen

    def test_catalog_from_payload_walks_every_taxonomy(self) -> None:
        payload = {
            "facts": {
                "us-gaap": {"Revenues": {"label": "Revenues", "description": "x", "units": {}}},
                "dei": {
                    "EntityCommonStockSharesOutstanding": {
                        "label": "Shares Outstanding",
                        "description": "Cover-page share count.",
                        "units": {"shares": []},
                    }
                },
            }
        }
        entries = _catalog_from_payload(payload)
        taxes = {e.taxonomy for e in entries}
        assert taxes == {"us-gaap", "dei"}

    def test_empty_payload_returns_empty(self) -> None:
        assert _catalog_from_payload({"facts": {}}) == []
        assert _catalog_from_payload({}) == []


class TestExtractorWideningWithoutFilter:
    """#451 Phase A: extractor emits every concept when no filter."""

    def test_no_filter_captures_untracked_concept(self) -> None:
        facts = _extract_facts_from_section(_SECTION, taxonomy="us-gaap")
        concepts = {f.concept for f in facts}
        assert "OperatingLeaseLiabilityNoncurrent" in concepts

    def test_explicit_filter_still_honoured(self) -> None:
        facts = _extract_facts_from_section(_SECTION, taxonomy="us-gaap", allowed_tags=frozenset({"Revenues"}))
        assert {f.concept for f in facts} == {"Revenues"}


@pytest.mark.integration
class TestUpsertConceptCatalogDB:
    def test_upsert_merges_units_and_refreshes_label(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        from app.services.fundamentals import upsert_concept_catalog

        entries_v1 = [
            XbrlConceptCatalogEntry(
                taxonomy="us-gaap",
                concept="Revenues",
                label="Revenues",
                description="v1 description",
                units_seen=("USD",),
            ),
        ]
        entries_v2 = [
            XbrlConceptCatalogEntry(
                taxonomy="us-gaap",
                concept="Revenues",
                label="Revenues (updated)",
                description="v2 description",
                units_seen=("EUR",),
            ),
        ]
        upsert_concept_catalog(ebull_test_conn, entries=entries_v1)
        upsert_concept_catalog(ebull_test_conn, entries=entries_v2)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT label, description, units_seen
                FROM sec_facts_concept_catalog
                WHERE taxonomy = 'us-gaap' AND concept = 'Revenues'
                """
            )
            row = cur.fetchone()
            assert row is not None
        label, description, units_seen = row
        # Latest label + description win.
        assert label == "Revenues (updated)"
        assert description == "v2 description"
        # Unit-type union preserves both observed units.
        assert set(units_seen) == {"USD", "EUR"}

    def test_upsert_preserves_prior_label_on_null(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A later entry with NULL label must not clobber the prior
        label — COALESCE keeps the earlier non-NULL value."""
        from app.services.fundamentals import upsert_concept_catalog

        upsert_concept_catalog(
            ebull_test_conn,
            entries=[
                XbrlConceptCatalogEntry(
                    taxonomy="us-gaap",
                    concept="GoodConcept",
                    label="Original",
                    description="Original description",
                    units_seen=("USD",),
                )
            ],
        )
        upsert_concept_catalog(
            ebull_test_conn,
            entries=[
                XbrlConceptCatalogEntry(
                    taxonomy="us-gaap",
                    concept="GoodConcept",
                    label=None,
                    description=None,
                    units_seen=("USD",),
                )
            ],
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT label, description
                FROM sec_facts_concept_catalog
                WHERE taxonomy = 'us-gaap' AND concept = 'GoodConcept'
                """
            )
            row = cur.fetchone()
            assert row is not None
        assert row[0] == "Original"
        assert row[1] == "Original description"
