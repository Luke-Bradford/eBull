"""#3841 slice 3: the acceptance script's per-session coverage outcome (``scripts/measure_3841_acceptance.py``)."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal

from app.services.pit_market_cap import Concept, ShareFact
from app.services.pit_market_cap_build import BasisStamp, InstrumentBuild
from scripts.measure_3841_acceptance import classify_sessions

D1, D2, D3, D4 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 4)


def _result(*, refused: str | None = None, facts: tuple[ShareFact, ...] = ()) -> InstrumentBuild:
    return InstrumentBuild(1, "0000000001", refused, facts, (), Counter(), {}, BasisStamp(0, 0, None))


def _fact(form: str) -> ShareFact:
    return ShareFact(
        concept=Concept.COVER,
        accession="A-1",
        form=form,
        context=date(2026, 6, 30),
        accepted_at=datetime(2026, 8, 1, 20, 30, tzinfo=UTC),
        value=Decimal(1_000_000),
    )


def test_every_session_gets_exactly_one_outcome() -> None:
    # A step covers D2..D3 (valid_to exclusive D4); D2 has a published view row, D3 has no bar.
    outcomes = classify_sessions(_result(), [D1, D2, D3, D4], [(D2, D4)], {D2: None})
    assert outcomes == {D1: "no_eligible_count", D2: "published", D3: "no_close", D4: "no_eligible_count"}


def test_a_view_refusal_is_reported_by_its_reason() -> None:
    assert classify_sessions(_result(), [D2], [(D2, D3)], {D2: "basis_stale"}) == {D2: "basis_stale"}


def test_an_uncovered_session_with_an_out_of_scope_count_is_out_of_scope_form() -> None:
    # A 20-F count would be eligible but for its form; an in-scope form never reads as out of scope.
    assert classify_sessions(_result(facts=(_fact("20-F"),)), [D1], [], {}) == {D1: "out_of_scope_form"}
    assert classify_sessions(_result(facts=(_fact("10-K"),)), [D1], [], {}) == {D1: "no_eligible_count"}


def test_a_refused_cik_is_absent_on_every_session() -> None:
    outcomes = classify_sessions(_result(refused="untimed_accession"), [D1, D2], [], {})
    assert outcomes == {D1: "cik_refused:untimed_accession", D2: "cik_refused:untimed_accession"}
