"""#2232 — the ``ifrs-full`` companyfacts route.

Pure-logic, no DB: every assertion here is about the extractor's routing and the
tag namespaces, both of which are decidable from the payload alone.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.providers.implementations.sec_fundamentals import (
    _ALL_TRACKED_DEI_TAGS,
    _ALL_TRACKED_IFRS_TAGS,
    _ALL_TRACKED_TAGS,
    IFRS_TRACKED_CONCEPTS,
    SecFundamentalsProvider,
)
from app.services.sec_companyfacts_ingest import extract_facts_from_companyfacts_payload


def _entry(end: str, val: float, accn: str = "0001104659-26-044493") -> dict[str, object]:
    return {
        "end": end,
        "val": val,
        "accn": accn,
        "form": "20-F",
        "filed": "2026-04-30",
        "fy": 2025,
        "fp": "FY",
    }


def _payload(**sections: object) -> dict[str, object]:
    return {"cik": 1504764, "entityName": "TEST", "facts": dict(sections)}


class TestTrackedTagNamespaces:
    """``uq_facts_raw_identity`` is
    ``(instrument_id, concept, unit, period_start, period_end, accession_number)``
    — taxonomy is NOT a key column. A concept name tracked under two namespaces
    would therefore UPSERT over itself rather than raise a unique violation, and
    whichever row was written second would silently replace the first. That makes
    pairwise disjointness a storage invariant, not a style preference."""

    def test_ifrs_tags_are_disjoint_from_gaap(self) -> None:
        assert _ALL_TRACKED_IFRS_TAGS & _ALL_TRACKED_TAGS == set()

    def test_ifrs_tags_are_disjoint_from_dei(self) -> None:
        assert _ALL_TRACKED_IFRS_TAGS & _ALL_TRACKED_DEI_TAGS == set()

    def test_gaap_and_dei_stay_disjoint(self) -> None:
        assert _ALL_TRACKED_TAGS & _ALL_TRACKED_DEI_TAGS == set()

    def test_the_tag_set_is_non_empty(self) -> None:
        # A disjointness suite passes vacuously against an empty set; without
        # this the three assertions above would survive the route being deleted.
        assert _ALL_TRACKED_IFRS_TAGS
        assert "NumberOfSharesOutstanding" in _ALL_TRACKED_IFRS_TAGS


class TestIfrsSectionIsRouted:
    def test_ifrs_share_count_is_extracted_and_stamped(self) -> None:
        payload = _payload(
            **{
                "ifrs-full": {
                    "NumberOfSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 23_743_475_754)]}}
                }
            }
        )
        facts = extract_facts_from_companyfacts_payload(payload)
        assert len(facts) == 1
        assert facts[0].taxonomy == "ifrs-full"
        assert facts[0].concept == "NumberOfSharesOutstanding"
        assert facts[0].val == Decimal("23743475754")

    def test_untracked_ifrs_concepts_are_dropped(self) -> None:
        """The allowlist is the whole point: ``ifrs-full`` carries 400+ concepts
        for a large filer, and routing the section without a cap would import the
        entire IFRS statement set as a side effect of a share-count fix."""
        payload = _payload(
            **{
                "ifrs-full": {
                    "NumberOfSharesAuthorised": {"units": {"shares": [_entry("2025-12-31", 120_000_000_000)]}},
                    "Revenue": {"units": {"USD": [_entry("2025-12-31", 1_000_000)]}},
                }
            }
        )
        assert extract_facts_from_companyfacts_payload(payload) == []

    def test_all_three_sections_coexist(self) -> None:
        payload = _payload(
            **{
                "us-gaap": {"CommonStockSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 100)]}}},
                "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 7)]}}},
                "ifrs-full": {
                    "NumberOfSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 23_743_475_754)]}}
                },
            }
        )
        facts = extract_facts_from_companyfacts_payload(payload)
        assert {f.taxonomy for f in facts} == {"us-gaap", "dei", "ifrs-full"}

    def test_ifrs_only_filer_is_no_longer_empty(self) -> None:
        """The regression this fixes. A foreign private issuer files under
        Reg S-X 4-01(a)(2) with no us-gaap section at all; before the route
        existed the whole payload extracted to nothing but the cover-page tag."""
        payload = _payload(
            **{
                "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 7)]}}},
                "ifrs-full": {
                    "NumberOfSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 23_743_475_754)]}}
                },
            }
        )
        facts = extract_facts_from_companyfacts_payload(payload)
        ifrs = [f for f in facts if f.taxonomy == "ifrs-full"]
        assert len(ifrs) == 1
        # The point of the ticket: the cover tag and the IFRS reading now
        # coexist, so one can contradict the other.
        dei = [f for f in facts if f.taxonomy == "dei"]
        assert dei[0].val == Decimal("7")
        assert ifrs[0].val > dei[0].val

    @pytest.mark.parametrize("concept", sorted(_ALL_TRACKED_IFRS_TAGS))
    def test_every_declared_concept_round_trips(self, concept: str) -> None:
        """Guards the gap between ``IFRS_TRACKED_CONCEPTS`` and the allowlist
        actually handed to the extractor — adding a concept to the dict without
        it reaching ``_ALL_TRACKED_IFRS_TAGS`` would fail silently."""
        payload = _payload(**{"ifrs-full": {concept: {"units": {"shares": [_entry("2025-12-31", 42)]}}}})
        facts = extract_facts_from_companyfacts_payload(payload)
        assert [f.concept for f in facts] == [concept]


class TestProviderEntryPointsRouteIfrs:
    """The routing branch exists in THREE places — the bulk wrapper
    ``extract_facts_from_companyfacts_payload`` plus
    ``SecFundamentalsProvider.extract_facts`` and
    ``.extract_facts_and_catalog``. The tests above exercise only the wrapper,
    so deleting the ``ifrs_section`` block from either provider method — the two
    the steady-state ``fundamentals_sync`` path actually calls — would leave the
    suite green. Payload is injected, so no HTTP and no DB."""

    @staticmethod
    def _provider(monkeypatch: pytest.MonkeyPatch, payload: dict[str, object]) -> SecFundamentalsProvider:
        provider = SecFundamentalsProvider(user_agent="eBull-test test@example.com")
        monkeypatch.setattr(provider, "_fetch_company_facts", lambda cik: payload)
        return provider

    _PAYLOAD = _payload(
        **{
            "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 7)]}}},
            "ifrs-full": {
                "NumberOfSharesOutstanding": {"units": {"shares": [_entry("2025-12-31", 23_743_475_754)]}},
                "NumberOfSharesAuthorised": {"units": {"shares": [_entry("2025-12-31", 120_000_000_000)]}},
            },
        }
    )

    def test_extract_facts_routes_the_ifrs_section(self, monkeypatch: pytest.MonkeyPatch) -> None:
        facts = self._provider(monkeypatch, self._PAYLOAD).extract_facts("TEST", "0001504764")
        assert [(f.taxonomy, f.concept) for f in facts if f.taxonomy == "ifrs-full"] == [
            ("ifrs-full", "NumberOfSharesOutstanding")
        ]

    def test_extract_facts_and_catalog_routes_the_ifrs_section(self, monkeypatch: pytest.MonkeyPatch) -> None:
        facts, entries = self._provider(monkeypatch, self._PAYLOAD).extract_facts_and_catalog("TEST", "0001504764")
        assert [f.concept for f in facts if f.taxonomy == "ifrs-full"] == ["NumberOfSharesOutstanding"]
        # The catalogue is uncapped BY DESIGN (#1233) and stays that way for
        # ifrs-full: the untracked concept the fact allowlist dropped is still
        # catalogued, which is what lets the UI name a concept it holds no fact
        # for. Safe because sec_facts_concept_catalog is UNIQUE (taxonomy,
        # concept) — sql/063 — so this cannot overwrite a us-gaap row.
        ifrs_entries = {e.concept for e in entries if e.taxonomy == "ifrs-full"}
        assert ifrs_entries == {"NumberOfSharesOutstanding", "NumberOfSharesAuthorised"}

    def test_a_payload_with_only_untracked_ifrs_concepts_yields_no_facts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The bot's NITPICK scenario, pinned. A filer whose ifrs-full section is
        large but wholly untracked takes the NON-empty path (the early return is
        skipped), so it returns no facts and logs no "no facts" line."""
        payload = _payload(**{"ifrs-full": {"Revenue": {"units": {"USD": [_entry("2025-12-31", 1_000_000)]}}}})
        assert self._provider(monkeypatch, payload).extract_facts("TEST", "0001504764") == []


class TestDeclaredConceptsAreCorroborationOnly:
    def test_no_ifrs_concept_is_a_denominator_concept(self) -> None:
        """⚠ Standing guard on the design the gain side falsified. ``AFYA`` tags
        ``NumberOfSharesOutstanding`` at 3,855,150 against its own 90,475,878
        weighted average — a dimension-stripped remnant, not the entity's count
        — so none of these may feed ``share_count_history.shares_outstanding``.
        That view selects by concept NAME with no taxonomy filter, so adding one
        of these names to its ``IN`` list is all it would take.

        ⚠ Scans EVERY file under ``sql/`` — ``rglob``, so a migration moved into
        a subdirectory stays covered — not just the migration that happens to
        define the view today. Pinning one filename makes the guard read a
        file the code no longer uses the moment a later migration redefines
        ``share_count_history`` — it would keep passing while the invariant it
        names had already been broken somewhere else. A deliberate future use
        of an IFRS tag in SQL is expected to fail this and update it.

        The root is resolved from ``__file__`` and the file count asserted
        non-empty, so the guard cannot pass vacuously by scanning nothing."""
        import pathlib

        sql_root = pathlib.Path(__file__).resolve().parent.parent / "sql"
        paths = sorted(sql_root.rglob("*.sql"))
        assert paths, f"no .sql files found under {sql_root} — guard would pass vacuously"

        offenders: list[str] = []
        for path in paths:
            body = path.read_text()
            offenders.extend(f"{path}:{concept}" for concept in _ALL_TRACKED_IFRS_TAGS if concept in body)
        assert offenders == [], (
            f"IFRS share tags reached SQL: {offenders}. They are corroboration "
            "readings, not denominators (#2232) — see IFRS_TRACKED_CONCEPTS for "
            "the gain-side measurement that killed the denominator design."
        )

    def test_concept_dict_keys_are_stable_identifiers(self) -> None:
        assert set(IFRS_TRACKED_CONCEPTS) == {
            "ifrs_shares_outstanding",
            "ifrs_shares_issued",
            "ifrs_weighted_average_shares",
        }
