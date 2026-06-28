"""Pure-logic tests for the Form 12b-25 extractor (#1015).

No DB. Two real fixtures (DOMO NT 10-Q, Quantum NT 10-K) ground the happy path;
synthetic snippets cover checkbox-encoding variants the population sample
surfaced (glyph before/after the label, ASCII ``x``, ambiguous → None) and the
transition-report case.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.services.nt_notices import NtNotice, parse_nt_notice

_FIXTURES = Path(__file__).parent / "fixtures" / "nt"


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8", errors="replace")


# --- Real fixtures -------------------------------------------------------


def test_domo_nt10q_happy_path() -> None:
    notice = parse_nt_notice(_fixture("domo_nt10q.htm"), "10-Q")
    assert notice is not None
    assert notice.late_form == "10-Q"
    assert notice.grace_period_days == 5
    assert notice.period_of_report == date(2026, 4, 30)
    assert notice.is_transition_report is False
    # DOMO checks "No" on item (3).
    assert notice.results_change_anticipated is False
    assert notice.reason_text is not None
    assert "additional time" in notice.reason_text.lower()


def test_quantum_nt10k_happy_path() -> None:
    notice = parse_nt_notice(_fixture("quantum_nt10k.htm"), "10-K")
    assert notice is not None
    assert notice.late_form == "10-K"
    assert notice.grace_period_days == 15
    assert notice.period_of_report == date(2026, 3, 31)
    assert notice.results_change_anticipated is False
    assert notice.reason_text is not None
    assert "accelerated filer" in notice.reason_text.lower()


# --- Synthetic checkbox variants ----------------------------------------


def _form_12b25(part3: str, item3_boxes: str, *, period: str = "March 31, 2026") -> str:
    """Minimal Form 12b-25 body with substitutable Part III + item (3) boxes."""
    return f"""
    <html><body>
    UNITED STATES SECURITIES AND EXCHANGE COMMISSION FORM 12b-25
    NOTIFICATION OF LATE FILING For Period Ended: {period}
    PART I REGISTRANT INFORMATION Acme Corp
    PART II RULES 12b-25(b) AND (c)
    PART III NARRATIVE State below in reasonable detail why the report could
    not be filed within the prescribed time period. {part3}
    PART IV OTHER INFORMATION
    (3) Is it anticipated that any significant change in results of operations
    from the corresponding period for the last fiscal year will be reflected?
    {item3_boxes}
    If so, attach an explanation of the anticipated change.
    </body></html>
    """


def test_results_change_yes_glyph_before_label() -> None:
    # "☒ Yes ☐ No" → anticipated True.
    notice = parse_nt_notice(_form_12b25("Reason text here.", "&#9746; Yes &#9744; No"), "10-K")
    assert notice is not None
    assert notice.results_change_anticipated is True


def test_results_change_no_glyph_after_label() -> None:
    # "Yes ☐ No ☒" → anticipated False (glyph follows the label).
    notice = parse_nt_notice(_form_12b25("Reason.", "Yes &#9744; No &#9746;"), "10-Q")
    assert notice is not None
    assert notice.results_change_anticipated is False


def test_results_change_ascii_x_counts_as_checked() -> None:
    # "Yes x No ." → x marks Yes checked.
    notice = parse_nt_notice(_form_12b25("Reason.", "Yes x No ."), "10-K")
    assert notice is not None
    assert notice.results_change_anticipated is True


def test_results_change_bracketed_boxes() -> None:
    # "[ ] Yes [X] No" → No checked → anticipated False.
    notice = parse_nt_notice(_form_12b25("Reason.", "[ ] Yes [X] No"), "10-Q")
    assert notice is not None
    assert notice.results_change_anticipated is False


def test_results_change_anchor_ignores_part3_mention() -> None:
    # Part III narrative mentions "results of operations"; the real item-(3)
    # box (No) must still win, not yield None.
    part3 = "Delay relates to our results of operations review by the auditor."
    notice = parse_nt_notice(_form_12b25(part3, "&#9744; Yes &#9746; No"), "10-K")
    assert notice is not None
    assert notice.results_change_anticipated is False


def test_results_change_ambiguous_is_none() -> None:
    # Both boxes empty → indeterminate, never a guessed boolean.
    notice = parse_nt_notice(_form_12b25("Reason.", "&#9744; Yes &#9744; No"), "10-K")
    assert notice is not None
    assert notice.results_change_anticipated is None


def test_reason_text_extracted_between_part3_and_part4() -> None:
    notice = parse_nt_notice(
        _form_12b25("The auditor needs more time to complete the audit.", "&#9744; Yes &#9746; No"),
        "10-K",
    )
    assert notice is not None
    assert notice.reason_text == "The auditor needs more time to complete the audit"


# --- Transition report ---------------------------------------------------


def test_transition_report_period_and_flag() -> None:
    body = """
    <html><body>
    FORM 12b-25 NOTIFICATION OF LATE FILING
    &#9746; Transition Report on Form 10-K
    For the Transition Period Ended: June 30, 2025
    PART III NARRATIVE State below why it could not be filed within the
    prescribed time period. Transition reason text.
    PART IV (3) results of operations &#9744; Yes &#9746; No
    """
    notice = parse_nt_notice(body, "10-K")
    assert notice is not None
    assert notice.is_transition_report is True
    assert notice.period_of_report == date(2025, 6, 30)


# --- Tombstone / guard cases --------------------------------------------


def test_non_form_12b25_returns_none() -> None:
    assert parse_nt_notice("<html>some unrelated 8-K body</html>", "10-K") is None


def test_empty_body_returns_none() -> None:
    assert parse_nt_notice("", "10-Q") is None


def test_invalid_late_form_raises() -> None:
    with pytest.raises(ValueError, match="late_form must be"):
        parse_nt_notice(_fixture("domo_nt10q.htm"), "8-K")


def test_numeric_period_date_parses() -> None:
    notice = parse_nt_notice(_form_12b25("Reason.", "Yes &#9744; No &#9746;", period="03/31/2026"), "10-K")
    assert notice is not None
    assert notice.period_of_report == date(2026, 3, 31)


def test_returns_frozen_dataclass() -> None:
    notice = parse_nt_notice(_fixture("domo_nt10q.htm"), "10-Q")
    assert isinstance(notice, NtNotice)
    with pytest.raises(AttributeError):
        notice.late_form = "10-K"  # type: ignore[misc]
