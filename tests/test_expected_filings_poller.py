"""Pure-logic tests for the expected-filings seed/poller (#1788).

No DB: the form/window derivation and the filing-match decision are the
risk-bearing logic and extract cleanly to pure functions. End-to-end seed
+ poll semantics (conditional re-seed, manifest write, force-refresh) are
verified live on the dev DB per the PR's DoD clauses 8-12.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from app.jobs.expected_filings_poller import match_filing, next_form_and_window
from app.providers.implementations.sec_submissions import FilingIndexRow


def _row(form: str, *, is_amendment: bool = False, source: str | None = "sec_10q") -> FilingIndexRow:
    return FilingIndexRow(
        accession_number="0000320193-26-000001",
        cik="0000320193",
        form=form,
        source=source,  # type: ignore[arg-type]
        filed_at=datetime(2026, 8, 1, tzinfo=UTC),
        accepted_at=None,
        primary_document_url=None,
        is_amendment=is_amendment,
    )


# --- next_form_and_window -------------------------------------------------


def test_q1_q2_predict_next_10q():
    # latest period-end 2026-03-31 → predicted next period-end +91d = 2026-06-30;
    # 10-Q window = [+30, +55] off that = [2026-07-30, 2026-08-24].
    for ptype in ("Q1", "Q2"):
        form, start, end = next_form_and_window(ptype, date(2026, 3, 31))
        assert form == "10-Q"
        assert start == date(2026, 7, 30)
        assert end == date(2026, 8, 24)


def test_q3_predicts_10k_not_phantom_10q():
    # The Q3→Q1 gap bug: a Q3 issuer's next filing is the FY 10-K, not a
    # 10-Q ~91d after the Q3 10-Q.
    form, start, end = next_form_and_window("Q3", date(2026, 9, 30))
    assert form == "10-K"
    # 10-K window is the wider [+50, +100] band off the predicted FY-end.
    assert (end - start).days == 50


def test_fy_and_q4_predict_next_10q():
    for ptype in ("FY", "Q4"):
        form, _start, _end = next_form_and_window(ptype, date(2025, 12, 31))
        assert form == "10-Q"


def test_unknown_period_type_defaults_to_10q():
    form, _start, _end = next_form_and_window("STUB", date(2026, 3, 31))
    assert form == "10-Q"


def test_10q_window_offsets_are_30_to_55_off_predicted_period_end():
    _form, start, end = next_form_and_window("Q1", date(2026, 3, 31))
    # offsets are measured from the predicted next period-end (latest + 91d),
    # so from the latest period-end they land at +121 and +146.
    assert (start - date(2026, 3, 31)).days == 91 + 30
    assert (end - date(2026, 3, 31)).days == 91 + 55


# --- match_filing ---------------------------------------------------------


def test_exact_form_non_amendment_matches():
    match = match_filing([_row("10-Q")], "10-Q")
    assert match is not None
    assert match.form == "10-Q"


def test_amendment_does_not_match():
    # 10-Q/A maps to the same manifest source but must NOT fulfil an
    # expected original 10-Q.
    assert match_filing([_row("10-Q/A", is_amendment=True)], "10-Q") is None
    assert match_filing([_row("10-Q", is_amendment=True)], "10-Q") is None


def test_wrong_form_does_not_match():
    assert match_filing([_row("8-K", source="sec_8k")], "10-Q") is None
    assert match_filing([_row("10-K", source="sec_10k")], "10-Q") is None


def test_empty_delta_no_match():
    assert match_filing([], "10-Q") is None


def test_returns_first_matching_when_mixed():
    rows = [_row("10-Q/A", is_amendment=True), _row("8-K", source="sec_8k"), _row("10-Q")]
    match = match_filing(rows, "10-Q")
    assert match is not None and match.form == "10-Q" and not match.is_amendment
