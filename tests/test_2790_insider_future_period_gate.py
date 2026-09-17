"""#2790 — the bulk DERA drain must apply #1687's Rule 16a-3(g) invariant.

Pure-logic: the gate is a decision function over (form type, dates, timeliness)
and is table-tested here. The point of the table is the rows that must be
**kept** — an over-broad gate is the failure mode that loses correct Form 3 data
and it passes any test that only checks rejections.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.services.sec_insider_dataset_ingest import _is_future_dated_for_form

FILED = datetime(2026, 6, 10, 14, 30, tzinfo=UTC)


def _call(
    form: str,
    period_end: date,
    *,
    timeliness: str | None = None,
    deemed: str | None = None,
    filed_at: datetime = FILED,
) -> bool:
    return _is_future_dated_for_form(
        form_upper=form,
        period_end=period_end,
        filed_at=filed_at,
        deemed_execution_date=deemed,
        trans_timeliness=timeliness,
    )


class TestRejectsImpossibleForm45Dates:
    """17 CFR 240.16a-3(g)/(f): the filing follows the reported date."""

    @pytest.mark.parametrize("form", ["4", "4/A", "5", "5/A"])
    def test_a_reported_date_after_the_filing_date_is_rejected(self, form: str) -> None:
        assert _call(form, date(2026, 6, 11)) is True

    @pytest.mark.parametrize("form", ["4", "5"])
    def test_same_day_is_kept(self, form: str) -> None:
        # Filing on the day of the transaction is the norm, not an error.
        assert _call(form, date(2026, 6, 10)) is False

    @pytest.mark.parametrize("form", ["4", "5"])
    def test_a_past_date_is_kept(self, form: str) -> None:
        assert _call(form, date(2026, 6, 1)) is False

    def test_the_year_typo_shape_is_rejected(self) -> None:
        # The 300-399 day band on the live corpus: filer typed the wrong year.
        # Rejected, NOT repaired — inferring the intended year is a repair.
        assert _call("4", date(2027, 5, 24)) is True

    def test_a_far_future_date_is_rejected(self) -> None:
        # The row that named this ticket: period_end 2047-05-24.
        assert _call("4", date(2047, 5, 24)) is True


class TestKeepsWhatTheRegAllows:
    """These are the assertions that catch an over-broad gate."""

    @pytest.mark.parametrize("form", ["3", "3/A"])
    def test_form3_is_never_gated(self, form: str) -> None:
        # Exchange Act s16(a)(2) sets only LATEST bounds -- "by the effective
        # date of a registration statement", "within 10 days after". A Form 3
        # filed ahead of a known future event date is compliant.
        assert _call(form, date(2026, 6, 11)) is False

    def test_form3_is_not_gated_even_far_ahead(self) -> None:
        # No earliest bound exists in the reg, so no upper bound is invented.
        # The live corpus carries form3 leads of 7, 14, 29, 84 and 149 days.
        assert _call("3", date(2026, 11, 6)) is False

    @pytest.mark.parametrize("form", ["", "  ", "UNKNOWN"])
    def test_an_unmapped_form_is_not_gated(self, form: str) -> None:
        # Deliberate fail-open. Rejecting a row whose form cannot be
        # established is what loses Form 3 data: 101 breaching rows on the live
        # corpus carry a form4-mapped source but sit on accessions that only
        # ever produced :NDH: holdings rows.
        assert _call(form.strip().upper(), date(2026, 6, 11)) is False

    def test_early_timeliness_keeps_a_future_dated_form4(self) -> None:
        # DERA readme Appendix 6.1: E = Early. Same vocabulary as the ownership
        # XML transactionTimeliness, so #1687's exemption transfers verbatim.
        assert _call("4", date(2026, 6, 11), timeliness="E") is False

    @pytest.mark.parametrize("timeliness", ["e", " E ", "E"])
    def test_the_early_exemption_is_case_and_whitespace_insensitive(self, timeliness: str) -> None:
        # DERA is a hand-assembled TSV; the XML path normalises its own value.
        assert _call("4", date(2026, 6, 11), timeliness=timeliness) is False

    @pytest.mark.parametrize("timeliness", ["L", "", None])
    def test_late_and_on_time_are_still_gated(self, timeliness: str | None) -> None:
        # Only E is exempt. L (late) and empty (on-time) are not.
        assert _call("4", date(2026, 6, 11), timeliness=timeliness) is True

    def test_a_missing_filing_date_cannot_be_judged(self) -> None:
        # evaluate_insider_date_validity's other exemption: with no
        # authoritative anchor there is nothing to compare against. The bulk
        # path skips such submissions upstream, so this is belt-and-braces.
        assert _call("4", date(2026, 6, 11), filed_at=None) is False  # type: ignore[arg-type]


class TestSharesTheDecisionFunctionWithTheXmlPath:
    """The rule must not be able to drift between the two writers."""

    def test_the_gate_delegates_to_evaluate_insider_date_validity(self) -> None:
        from app.services import sec_insider_dataset_ingest as mod
        from app.services.insider_transactions import evaluate_insider_date_validity

        assert mod.evaluate_insider_date_validity is evaluate_insider_date_validity

    def test_a_deemed_execution_date_does_not_change_the_txn_verdict(self) -> None:
        # The helper returns (txn_invalid, deemed_out); the gate consumes only
        # the first. A deemed date that postdates filing quarantines the deemed
        # value in the XML path but must not by itself reject the row here.
        assert _call("4", date(2026, 6, 1), deemed="2026-06-30") is False
