"""#3714 reader-decision census: the pure decision helpers mirror each reader's own rule."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

from scripts.census_3714_reader_decisions import (
    ChangedInput,
    first_session_of_next_month,
    insider_decision,
    insider_refused,
    pead_decision,
    shock_decision,
)

_FILED = datetime(2026, 9, 30, tzinfo=UTC)


def _row(form: str, before: datetime | None, after: datetime | None, **kwargs: object) -> ChangedInput:
    fields: dict[str, object] = {
        "accession_number": "0000000000-26-000001",
        "form": form,
        "filed_at": _FILED,
        "instrument_id": 1,
        "before": before,
        "after": after,
        "has_material_item": True,
    }
    fields.update(kwargs)
    return ChangedInput(**fields)  # type: ignore[arg-type]


def test_first_session_of_next_month_skips_weekend_and_holiday() -> None:
    assert first_session_of_next_month(date(2026, 9, 30)) == date(2026, 10, 1)
    # 2026-11-01 is a Sunday.
    assert first_session_of_next_month(date(2026, 10, 15)) == date(2026, 11, 2)
    # 2027-01-01 is a Friday holiday.
    assert first_session_of_next_month(date(2026, 12, 31)) == date(2027, 1, 4)


def test_insider_guard_skips_null_and_reads_the_values_own_date() -> None:
    entry = date(2026, 10, 1)
    assert insider_refused(None, entry) is False
    assert insider_refused(datetime(2026, 10, 1, 0, 42, tzinfo=UTC), entry) is True
    assert insider_refused(datetime(2026, 9, 30, 20, 42, tzinfo=UTC), entry) is False
    # The reader's ``.date()`` takes the value's own zone, so the same instant in New York is not refused.
    eastern = timezone(timedelta(hours=-4))
    assert insider_refused(datetime(2026, 10, 1, 0, 42, tzinfo=UTC).astimezone(eastern), entry) is False


def test_insider_decision_flips_when_header_moves_across_entry_date() -> None:
    row = _row("4", datetime(2026, 10, 1, 0, 42, tzinfo=UTC), datetime(2026, 9, 30, 20, 42, tzinfo=UTC))
    decision = insider_decision(row)
    assert decision == {
        "entry_date": date(2026, 10, 1),
        "before_refused": True,
        "after_refused": False,
        "changed": True,
    }
    assert insider_decision(_row("8-K", None, None)) is None


def test_pead_entry_date_falls_back_to_filed_plus_one_and_needs_an_instrument() -> None:
    before = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)  # 08:00 ET, before the open
    decision = pead_decision(_row("10-Q", before, None), date(2026, 9, 30))
    assert decision is not None
    assert decision["before"] == date(2026, 9, 30)
    assert decision["after"] == date(2026, 10, 1)
    assert decision["changed"] is True
    assert decision["filed_date_source"] == "financial_facts_raw"
    no_fact = pead_decision(_row("10-Q", before, None), None)
    assert no_fact is not None and no_fact["filed_date_source"] == "manifest_filed_at"
    assert pead_decision(_row("10-Q", before, None, instrument_id=None), None) is None
    assert pead_decision(_row("10-Q/A", before, None), None) is None


def test_shock_reads_material_8ks_only_and_reports_the_knowledge_delta() -> None:
    before = datetime(2026, 9, 30, 21, 0, tzinfo=UTC)
    after = before - timedelta(hours=4)
    decision = shock_decision(_row("8-K", before, after))
    assert decision is not None
    assert decision["delta_s"] == -14400.0
    assert decision["changed"] is True
    assert shock_decision(_row("8-K", before, after, has_material_item=False)) is None
    assert shock_decision(_row("10-K/A", before, after, has_material_item=False)) is not None
    assert shock_decision(_row("4", before, after)) is None
