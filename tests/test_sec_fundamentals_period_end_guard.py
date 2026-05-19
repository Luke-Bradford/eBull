"""Tests for ``_extract_facts_from_section`` ``period_start`` /
``period_end`` window guard (#1218).

The DEFAULT partition of ``financial_facts_raw`` (sql/156, #1208
Phase 3) was historically absorbing pre-1900 / year-6016 parser
junk. The guard at
``app/providers/implementations/sec_fundamentals.py``::
``_extract_facts_from_section`` rejects rows outside
``[1900-01-01, 2100-01-01)`` with a per-(accession, reason) WARN.

Spec: ``docs/superpowers/specs/2026-05-19-1218-parser-period-end.md``.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pytest

from app.providers.implementations.sec_fundamentals import (
    _REJ_END_OUT_OF_WINDOW,
    _REJ_START_AFTER_END,
    _REJ_START_OUT_OF_WINDOW,
    _classify_period_rejection,
    _extract_facts_from_section,
)

# ── _classify_period_rejection — pure predicate, no logging ──────


def test_classify_keeps_in_window_pair() -> None:
    assert _classify_period_rejection(date(2024, 1, 1), date(2024, 12, 31)) is None


def test_classify_keeps_in_window_null_start() -> None:
    # Balance-sheet concepts emit period_end only — must be kept.
    assert _classify_period_rejection(None, date(2024, 12, 31)) is None


@pytest.mark.parametrize(
    "end_iso,reason",
    [
        ("1899-12-31", _REJ_END_OUT_OF_WINDOW),
        ("1900-01-01", None),
        ("2099-12-31", None),
        ("2100-01-01", _REJ_END_OUT_OF_WINDOW),
        ("6016-06-30", _REJ_END_OUT_OF_WINDOW),
        ("9999-12-31", _REJ_END_OUT_OF_WINDOW),
    ],
)
def test_classify_period_end_boundaries(end_iso: str, reason: str | None) -> None:
    assert _classify_period_rejection(None, date.fromisoformat(end_iso)) == reason


@pytest.mark.parametrize(
    "start_iso,reason",
    [
        ("1899-12-31", _REJ_START_OUT_OF_WINDOW),
        ("1900-01-01", None),
        ("2099-12-31", None),
        ("2100-01-01", _REJ_START_OUT_OF_WINDOW),
        ("1850-06-30", _REJ_START_OUT_OF_WINDOW),
    ],
)
def test_classify_period_start_boundaries(start_iso: str, reason: str | None) -> None:
    # period_end fixed at 2099-12-31 so the end-side is in-window even
    # when start is far in the future (else end-out-of-window would
    # mask start-out-of-window).
    assert _classify_period_rejection(date.fromisoformat(start_iso), date(2099, 12, 31)) == reason


def test_classify_negative_duration() -> None:
    # period_start > period_end — the parser-bug class that flips
    # start/end on a fp-context misread.
    assert _classify_period_rejection(date(2025, 6, 30), date(2024, 12, 31)) == _REJ_START_AFTER_END


def test_classify_equal_start_end_ok() -> None:
    # Single-day period (e.g. 8-K item with start==end) is well-ordered.
    assert _classify_period_rejection(date(2024, 12, 31), date(2024, 12, 31)) is None


# ── _extract_facts_from_section — integration with the rejection path ──


def _entry(
    *,
    end: str,
    start: str | None = None,
    val: str = "1000",
    accn: str = "0001234567-25-000001",
    form: str = "10-K",
    filed: str = "2025-01-15",
) -> dict[str, Any]:
    e: dict[str, Any] = {"end": end, "val": val, "accn": accn, "form": form, "filed": filed}
    if start is not None:
        e["start"] = start
    return e


def test_extractor_rejects_out_of_window_period_end(caplog: pytest.LogCaptureFixture) -> None:
    section = {
        "OperatingLossCarryforwards": {
            "units": {
                "USD": [
                    _entry(end="6016-06-30", val="1000"),
                    _entry(end="2024-12-31", val="2000"),
                ]
            }
        }
    }
    with caplog.at_level(logging.WARNING):
        facts = _extract_facts_from_section(section, taxonomy="us-gaap")
    assert len(facts) == 1
    assert facts[0].period_end == date(2024, 12, 31)
    # Provenance: bad date + accession + reason all in the log line.
    messages = [r.getMessage() for r in caplog.records]
    assert any("6016-06-30" in m for m in messages)
    assert any("0001234567-25-000001" in m for m in messages)
    assert any(_REJ_END_OUT_OF_WINDOW in m for m in messages)


def test_extractor_rejects_out_of_window_period_start(caplog: pytest.LogCaptureFixture) -> None:
    section = {
        "IncomeTaxExpenseBenefit": {
            "units": {
                "USD": [
                    _entry(start="1850-01-01", end="2024-12-31"),
                ]
            }
        }
    }
    with caplog.at_level(logging.WARNING):
        facts = _extract_facts_from_section(section, taxonomy="us-gaap")
    assert facts == []
    messages = [r.getMessage() for r in caplog.records]
    assert any(_REJ_START_OUT_OF_WINDOW in m for m in messages)
    assert any("1850-01-01" in m for m in messages)


def test_extractor_rejects_start_after_end(caplog: pytest.LogCaptureFixture) -> None:
    section = {
        "Revenues": {
            "units": {
                "USD": [
                    _entry(start="2025-06-30", end="2024-12-31"),
                ]
            }
        }
    }
    with caplog.at_level(logging.WARNING):
        facts = _extract_facts_from_section(section, taxonomy="us-gaap")
    assert facts == []
    messages = [r.getMessage() for r in caplog.records]
    assert any(_REJ_START_AFTER_END in m for m in messages)


def test_extractor_keeps_balance_sheet_with_null_start() -> None:
    # period_start absent → balance-sheet concept; must survive the guard.
    section = {
        "Assets": {
            "units": {
                "USD": [
                    _entry(end="2024-12-31"),
                ]
            }
        }
    }
    facts = _extract_facts_from_section(section, taxonomy="us-gaap")
    assert len(facts) == 1
    assert facts[0].period_end == date(2024, 12, 31)
    assert facts[0].period_start is None


def test_extractor_dei_taxonomy_path_validates_the_same(caplog: pytest.LogCaptureFixture) -> None:
    # Routing through dei vs us-gaap must not bypass the window.
    section = {
        "EntityCommonStockSharesOutstanding": {
            "units": {
                "shares": [
                    _entry(end="6016-12-31", val="100"),
                    _entry(end="2024-12-31", val="200"),
                ]
            }
        }
    }
    with caplog.at_level(logging.WARNING):
        facts = _extract_facts_from_section(section, taxonomy="dei")
    assert len(facts) == 1
    assert facts[0].taxonomy == "dei"
    assert facts[0].period_end == date(2024, 12, 31)
    assert any(_REJ_END_OUT_OF_WINDOW in r.getMessage() for r in caplog.records)


def test_extractor_dedups_warnings_per_accession_reason(caplog: pytest.LogCaptureFixture) -> None:
    # Same accession + same reason across many rows → 1 WARN only.
    section = {
        "ConceptA": {
            "units": {
                "USD": [
                    _entry(end="6016-01-01", accn="0001111111-25-000001"),
                    _entry(end="6016-04-01", accn="0001111111-25-000001"),
                    _entry(end="6016-07-01", accn="0001111111-25-000001"),
                ]
            }
        },
        "ConceptB": {
            "units": {
                "USD": [
                    _entry(end="6016-10-01", accn="0001111111-25-000001"),
                ]
            }
        },
    }
    with caplog.at_level(logging.WARNING):
        _ = _extract_facts_from_section(section, taxonomy="us-gaap")
    # All four rows have same (accn, reason) → exactly one WARN.
    end_oow_warnings = [r for r in caplog.records if _REJ_END_OUT_OF_WINDOW in r.getMessage()]
    assert len(end_oow_warnings) == 1


def test_extractor_logs_distinct_reasons_for_same_accession(caplog: pytest.LogCaptureFixture) -> None:
    # Same accession but two different reasons → 2 WARNs.
    section = {
        "ConceptA": {
            "units": {
                "USD": [
                    _entry(end="6016-01-01", accn="0002222222-25-000001"),
                    _entry(start="1850-01-01", end="2024-12-31", accn="0002222222-25-000001"),
                ]
            }
        }
    }
    with caplog.at_level(logging.WARNING):
        _ = _extract_facts_from_section(section, taxonomy="us-gaap")
    relevant = [
        r for r in caplog.records if "0002222222-25-000001" in r.getMessage() and ("XBRL parser" in r.getMessage())
    ]
    assert len(relevant) == 2
    messages = " ".join(r.getMessage() for r in relevant)
    assert _REJ_END_OUT_OF_WINDOW in messages
    assert _REJ_START_OUT_OF_WINDOW in messages
