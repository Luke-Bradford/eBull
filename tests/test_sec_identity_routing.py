"""Pure table-tests for the #828 PR-1 interim entity-row routing policy.

``pick_entity_instrument`` is the extracted-pure policy (no DB): given the
discovery-time instrument, the issuer's production sibling set
(``external_identifiers`` sec/cik), and the ``instrument_cik_history``
instruments for the issuer CIK, decide which instrument the entity-level
rows (``insider_filings`` / ``insider_transactions`` /
``insider_initial_holdings``) bind to, and whether the write is an
owner-stream mislink. Spec:
docs/proposals/etl/2026-07-22-828-insider-cik-routing.md §PR-1.
"""

from __future__ import annotations

import pytest

from app.services.sec_identity import pick_entity_instrument


@pytest.mark.parametrize(
    ("discovery", "siblings", "history", "expected_entity", "expected_mislink"),
    [
        # Empty sibling set (unroutable cohort) — keep discovery linkage.
        (300, [], [], 300, False),
        (300, [], [201], 300, False),
        # Discovery instrument IS a sibling — healthy write, unchanged.
        (201, [201], [], 201, False),
        (202, [201, 202], [201], 202, False),
        # Mislink, no history — min(sibling set).
        (300, [202, 201], [], 201, True),
        (300, [7], [], 7, True),
        # Mislink, unambiguous history inside the sibling set — history wins
        # over min().
        (300, [201, 202], [202], 202, True),
        # Mislink, unambiguous history OUTSIDE the sibling set (stale
        # history) — must NOT escape the sibling set (PR-2 invariant is
        # instrument_id = ANY(siblings)); falls back to min().
        (300, [201, 202], [999], 201, True),
        # Mislink, ambiguous history (>1 instrument) — falls back to min().
        (300, [201, 202], [201, 202], 201, True),
        # Duplicated history ids collapse to one distinct instrument.
        (300, [201, 202], [202, 202], 202, True),
    ],
)
def test_pick_entity_instrument(
    discovery: int,
    siblings: list[int],
    history: list[int],
    expected_entity: int,
    expected_mislink: bool,
) -> None:
    entity, mislink = pick_entity_instrument(
        discovery_instrument_id=discovery,
        sibling_instrument_ids=siblings,
        history_instrument_ids=history,
    )
    assert entity == expected_entity
    assert mislink is expected_mislink
