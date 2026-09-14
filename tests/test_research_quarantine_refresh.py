"""#3040 — the work condition of the scheduled research-corpus re-quarantine.

Pure. The archives are frozen files, so the steady state of this job is "do
nothing", and the failure that matters is doing the eight-minute full-corpus
rewrite anyway — nightly, for byte-identical rows.

⚠ The reconciliation QUERY is deliberately NOT tested here. It lives in
``test_research_quarantine_refresh_db.py`` because the ``db`` marker is applied
per MODULE (``tests/conftest.py::_module_source_touches_db`` reads the whole
file), so one DB test in this file would evict every pure test above from the
``-m "not db"`` push gate.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS
from app.services.research_corpus_ingest import (
    HF_ARCHIVE,
    INTRADER_ARCHIVE,
    RESEARCH_ARCHIVES,
    refresh_research_quarantine,
)

# ---------------------------------------------------------------------------
# The declared policy
# ---------------------------------------------------------------------------


def test_every_declared_archive_carries_a_quarantine_as_of() -> None:
    """A new archive cannot enter the refresh without declaring its policy.

    ``quarantine_as_of`` is an INPUT to the verdicts — it sets
    ``provisional_from`` — so an archive added to ``RESEARCH_ARCHIVES`` without
    one would be evaluated under whatever default a caller happened to pass.
    """
    assert RESEARCH_ARCHIVES, "the refresh iterates this; an empty tuple is a silent no-op"
    for archive in RESEARCH_ARCHIVES:
        assert isinstance(archive.quarantine_as_of, date)


def test_the_two_archives_declare_different_policies_on_purpose() -> None:
    """Pinned so a future tidy-up cannot harmonise them without reading why.

    Intrader's as_of is its capture date, which makes its trailing bars
    provisional — the basis ``market_regime_provider``'s freeze-time declaration
    was written against. HF's is capture + ``PROVISIONAL_WINDOW_DAYS`` + 1, the
    smallest date that marks nothing provisional. Making both "capture date"
    would mark HF's trailing bars provisional across 7,693 series: a corpus
    change needing its own A/B, not a tidy-up.
    """
    assert INTRADER_ARCHIVE.quarantine_as_of == date(2024, 9, 27)
    # Derived, not chosen — HF's capture date is 2026-07-08.
    assert HF_ARCHIVE.quarantine_as_of == date(2026, 7, 8) + timedelta(days=PROVISIONAL_WINDOW_DAYS + 1)


# ---------------------------------------------------------------------------
# The work condition
# ---------------------------------------------------------------------------


class _StubConn:
    """Stands in for a connection. The refresh never touches it directly.

    Control flow over ``uncovered_series_count``'s answer is the whole of the
    work condition; the query behind that answer is a plan-level invariant and
    is asserted against a real backend in the db-tier twin.
    """


class _Census:
    series_evaluated = 1
    bars_evaluated = 10


@pytest.fixture
def ran(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, date]]:
    """Record every ``run_quarantine`` the refresh actually issues."""
    calls: list[tuple[str, date]] = []

    def _fake_run(conn: Any, *, vendor: str, as_of: date) -> _Census:
        calls.append((vendor, as_of))
        return _Census()

    monkeypatch.setattr("app.services.research_corpus_ingest.run_quarantine", _fake_run)
    return calls


def _uncovered(monkeypatch: pytest.MonkeyPatch, counts: dict[str, int]) -> None:
    monkeypatch.setattr(
        "app.services.research_corpus_ingest.uncovered_series_count",
        lambda conn, archive: counts.get(archive.vendor, 0),
    )


def test_a_vendor_already_at_policy_is_skipped_not_rewritten(
    monkeypatch: pytest.MonkeyPatch, ran: list[tuple[str, date]]
) -> None:
    _uncovered(monkeypatch, {})
    result = refresh_research_quarantine(_StubConn())  # type: ignore[arg-type]

    assert ran == [], "a complete corpus must cost zero series evaluations"
    assert result.series_evaluated == 0
    assert set(result.skipped_vendors) == {a.vendor for a in RESEARCH_ARCHIVES}
    assert result.refreshed_vendors == ()


def test_zero_series_evaluated_is_a_success_not_no_work(
    monkeypatch: pytest.MonkeyPatch, ran: list[tuple[str, date]]
) -> None:
    """The job reports ``series_evaluated`` as its ``row_count``.

    Zero is the healthy steady state for a frozen corpus, so the census must
    name the vendors it skipped — otherwise the only evidence a reader has is a
    zero, which is also what a broken run prints.
    """
    _uncovered(monkeypatch, {})
    result = refresh_research_quarantine(_StubConn())  # type: ignore[arg-type]

    assert result.series_evaluated == 0
    assert result.skipped_vendors


def test_only_the_off_policy_vendor_is_re_evaluated(
    monkeypatch: pytest.MonkeyPatch, ran: list[tuple[str, date]]
) -> None:
    """One archive moving must not drag the other's 22,879 series with it."""
    _uncovered(monkeypatch, {HF_ARCHIVE.vendor: 5})
    result = refresh_research_quarantine(_StubConn())  # type: ignore[arg-type]

    assert [vendor for vendor, _ in ran] == [HF_ARCHIVE.vendor]
    assert result.refreshed_vendors == (HF_ARCHIVE.vendor,)
    assert result.skipped_vendors == (INTRADER_ARCHIVE.vendor,)


def test_the_declared_as_of_is_what_reaches_run_quarantine(
    monkeypatch: pytest.MonkeyPatch, ran: list[tuple[str, date]]
) -> None:
    """The job must never invent an as_of — divergence is what it exists to stop."""
    _uncovered(monkeypatch, {a.vendor: 1 for a in RESEARCH_ARCHIVES})
    refresh_research_quarantine(_StubConn())  # type: ignore[arg-type]

    assert dict(ran) == {
        HF_ARCHIVE.vendor: HF_ARCHIVE.quarantine_as_of,
        INTRADER_ARCHIVE.vendor: INTRADER_ARCHIVE.quarantine_as_of,
    }
