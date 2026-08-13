"""Unit tests for the Schedule 13D/13G retention helpers.

Pins the four invariants from spec
``docs/superpowers/specs/2026-05-21-pr11-blockholders-activation-design.md``
§3.2:

  1. ``INSIDER_BLOCKHOLDERS_RETENTION_YEARS = 3``.
  2. ``SEC_SCHEDULE_13_XML_MANDATE_DATE = date(2024, 12, 18)`` (SEC EDGAR
     Release 23.4 effective date).
  3. ``blockholders_retention_cutoff()`` returns a ``date`` (NOT a
     ``datetime``) — calendar-day granularity, see helper docstring.
  4. Cutoff clamps to the XML mandate while ``today − 3y`` is still
     earlier than 2024-12-18; degrades to the plain 3-year rolling floor
     once the rolling boundary catches up.
  5. ``blockholders_within_retention`` is inclusive at the cutoff midnight
     UTC boundary and strict before it; ``None`` resolves to ``False``.

``datetime.now`` is monkeypatched at the module-symbol layer (i.e. the
``app.services.blockholders.datetime`` rebinding) rather than via
``datetime.datetime.now`` (impossible — built-in immutability) or via
``freezegun`` (not a dependency in this repo). A ``MagicMock`` with
``side_effect`` wires the ``.now(...)`` call to a fixed return while the
constructor (``datetime(...)``) keeps the real built-in semantics so the
helper's ``datetime(...).date()`` chain still works.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest

from app.services import blockholders
from app.services.blockholders import (
    INSIDER_BLOCKHOLDERS_RETENTION_YEARS,
    SEC_SCHEDULE_13_XML_MANDATE_DATE,
    blockholders_retention_cutoff,
    blockholders_within_retention,
)

# ---------------------------------------------------------------------------
# Pinning constants
# ---------------------------------------------------------------------------


def test_retention_years_pinned_to_3() -> None:
    """Spec §3.2 constant pin — bump alongside the spec, not in code review."""
    assert INSIDER_BLOCKHOLDERS_RETENTION_YEARS == 3


def test_xml_mandate_date_pinned_to_2024_12_18() -> None:
    """SEC EDGAR Release 23.4 effective date — Schedule 13 XBRL mandate."""
    assert SEC_SCHEDULE_13_XML_MANDATE_DATE == date(2024, 12, 18)


# ---------------------------------------------------------------------------
# Cutoff function — return type + clamp semantics
# ---------------------------------------------------------------------------


def _install_fake_now(monkeypatch: pytest.MonkeyPatch, fake_now: datetime) -> None:
    """Patch ``app.services.blockholders.datetime`` so ``.now(...)`` returns
    ``fake_now`` while ``datetime(year, month, day, ...)`` constructor calls
    still pass through to the real built-in. The helper only calls
    ``datetime.now(tz=UTC)`` — overriding ``.now`` via ``side_effect`` is
    sufficient.
    """
    mock_datetime = MagicMock(wraps=datetime)
    mock_datetime.now = MagicMock(return_value=fake_now)
    monkeypatch.setattr(blockholders, "datetime", mock_datetime)


def test_cutoff_returns_date_not_datetime() -> None:
    """Helper MUST return ``date`` (Codex 1d HIGH lesson; spec §3.2)."""
    result = blockholders_retention_cutoff()
    assert isinstance(result, date)
    # ``datetime`` is a subclass of ``date`` — pin the stricter check too.
    assert not isinstance(result, datetime)


def test_cutoff_clamps_to_mandate_when_3y_floor_is_earlier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """today = 2026-05-21 → today − 3y = 2023-05-21 < 2024-12-18 mandate.

    Cutoff MUST clamp to the mandate floor so pre-mandate HTML-only
    filings are excluded by construction.
    """
    _install_fake_now(monkeypatch, datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC))
    assert blockholders_retention_cutoff() == date(2024, 12, 18)


def test_cutoff_uses_3y_floor_once_rolling_boundary_catches_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """today = 2028-01-15 → today − 3y = 2025-01-15 > 2024-12-18 mandate.

    Cutoff MUST degrade to the rolling 3-year floor once it overtakes
    the mandate. (The mandate floor stops binding on / after 2027-12-18.)
    """
    _install_fake_now(monkeypatch, datetime(2028, 1, 15, 12, 0, 0, tzinfo=UTC))
    # Calendar-exact 3-year subtraction (today.replace(year=today.year-3)):
    # 2028-01-15 minus 3 calendar years = 2025-01-15. Leap-year-stable.
    assert blockholders_retention_cutoff() == date(2025, 1, 15)


# ---------------------------------------------------------------------------
# within_retention predicate — None + inclusive boundary + strict-before
# ---------------------------------------------------------------------------


def test_within_retention_treats_none_as_outside(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row missing ``filed_at`` cannot be safely placed inside the
    retention window — defensive ``False`` per spec §3.2 helper block."""
    _install_fake_now(monkeypatch, datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC))
    assert blockholders_within_retention(None) is False


def test_within_retention_inclusive_at_cutoff_midnight_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``filed_at == cutoff 00:00 UTC`` retains — inclusive boundary."""
    _install_fake_now(monkeypatch, datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC))
    # Cutoff clamps to 2024-12-18 at this fake-now.
    at_cutoff_midnight = datetime(2024, 12, 18, 0, 0, 0, tzinfo=UTC)
    assert blockholders_within_retention(at_cutoff_midnight) is True


def test_within_retention_rejects_one_second_before_cutoff_midnight_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``filed_at`` strictly before cutoff midnight UTC drops — strict
    on the wrong side of the calendar-day boundary.
    """
    _install_fake_now(monkeypatch, datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC))
    # One second before 2024-12-18 00:00 UTC ⇒ 2024-12-17 23:59:59 UTC,
    # whose ``.date()`` is 2024-12-17 < 2024-12-18 cutoff.
    one_second_before = datetime(2024, 12, 17, 23, 59, 59, tzinfo=UTC)
    assert blockholders_within_retention(one_second_before) is False
