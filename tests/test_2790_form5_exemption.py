"""#2790 — the date-validity exemption keys on (submission type × line form type).

EDGAR Ownership XML Technical Specification v5.1 §3.6.8 gives the timeliness
vocabulary (``E`` Early / ``L`` Late / empty On-time) and §4.3.8.2 says what
"early" attaches to on a Form 4 submission: *"By definition, a '4' transaction is
on time… By definition, a '5' transaction is early. You do not have to provide a
value of 'E,' but you can if you wish."*

Two cases here did not exist before and are the ones the shipped gate gets wrong:

* **form type 5, BLANK timeliness, Form 4 submission, future date** — legitimate
  (the letter is optional, so keying on it rejects correct filings). Worked case
  ``0001127602-24-015987``.
* **form type 5 on a Form 5 submission, future date** — impossible: that filing
  reports transactions during a fiscal year already ended. Worked case
  ``0001415889-23-002362`` (ARCH CAPITAL), which carries the same gift at
  ``15-NOV-2022`` and ``15-NOV-2023``.
"""

from __future__ import annotations

import ast
import inspect
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from app.services import insider_transactions
from app.services.insider_transactions import (
    evaluate_insider_date_validity,
    is_early_form5_line,
)
from app.services.sec_insider_dataset_ingest import _reject_reason_for_form

FILED = datetime(2026, 6, 10, 14, 30, tzinfo=UTC)
FUTURE = date(2026, 6, 11)
PAST = date(2026, 6, 9)


class TestIsEarlyForm5Line:
    @pytest.mark.parametrize("submission", ["4", "4/A", "4/a", " 4 ", None, ""])
    def test_an_explicit_form5_line_on_a_form4_submission_is_exempt(self, submission: str | None) -> None:
        assert is_early_form5_line(submission, "5", "") is True

    @pytest.mark.parametrize("submission", ["5", "5/A", "3"])
    def test_a_form5_line_on_any_other_submission_is_not_exempt(self, submission: str) -> None:
        """16a-3(f) binds the Form 5 itself — the ACGL year-typo shape."""
        assert is_early_form5_line(submission, "5", "") is False

    def test_the_timeliness_letter_alone_exempts_only_an_unknown_line_type(self) -> None:
        assert is_early_form5_line("4", None, "E") is True
        assert is_early_form5_line("4", "", "e") is True

    def test_an_explicit_form4_line_marked_E_is_not_exempt(self) -> None:
        """EDGAR suspends any timeliness but ``E`` on a form-type-5 line, so
        ``4`` + ``E`` is malformed. Exempting it would keep a row #2790's
        correction deletes — the gate and the correction must not disagree."""
        assert is_early_form5_line("4", "4", "E") is False

    def test_a_form4_line_is_never_exempt(self) -> None:
        assert is_early_form5_line("4", "4", "") is False
        assert is_early_form5_line("4", "4", "L") is False

    def test_an_unknown_line_without_the_letter_is_not_exempt(self) -> None:
        assert is_early_form5_line("4", None, None) is False


class TestEvaluateInsiderDateValidity:
    def test_a_future_form5_line_on_a_form4_is_kept(self) -> None:
        invalid, _ = evaluate_insider_date_validity(
            FUTURE, None, FILED, None, submission_form_type="4", txn_form_type="5"
        )
        assert invalid is False

    def test_a_future_form5_line_on_a_form5_is_flagged(self) -> None:
        invalid, _ = evaluate_insider_date_validity(
            FUTURE, None, FILED, None, submission_form_type="5", txn_form_type="5"
        )
        assert invalid is True

    def test_a_future_form4_line_is_flagged(self) -> None:
        invalid, _ = evaluate_insider_date_validity(
            FUTURE, None, FILED, None, submission_form_type="4", txn_form_type="4"
        )
        assert invalid is True

    def test_the_legacy_E_only_call_is_unchanged(self) -> None:
        """Callers that pass neither form type keep #1687's behaviour exactly."""
        assert evaluate_insider_date_validity(FUTURE, None, FILED, "E")[0] is False
        assert evaluate_insider_date_validity(FUTURE, None, FILED, None)[0] is True

    def test_the_statutory_floor_is_not_inherited_by_the_exemption(self) -> None:
        """#2441: no form type makes a pre-1934 date possible."""
        invalid, _ = evaluate_insider_date_validity(
            date(1934, 6, 5), None, FILED, "E", submission_form_type="4", txn_form_type="5"
        )
        assert invalid is True

    def test_a_future_deemed_date_on_an_exempt_line_is_kept(self) -> None:
        _, deemed = evaluate_insider_date_validity(
            FUTURE, FUTURE, FILED, None, submission_form_type="4", txn_form_type="5"
        )
        assert deemed == FUTURE

    def test_a_future_deemed_date_on_a_bound_line_is_quarantined(self) -> None:
        _, deemed = evaluate_insider_date_validity(
            PAST, FUTURE, FILED, None, submission_form_type="4", txn_form_type="4"
        )
        assert deemed is None


class TestDeraGateReadsTheLineFormType:
    def _reason(self, submission: str, line: str | None, *, timeliness: str | None = None) -> str | None:
        return _reject_reason_for_form(
            form_upper=submission,
            period_end=FUTURE,
            filed_at=FILED,
            deemed_execution_date=None,
            trans_timeliness=timeliness,
            trans_form_type=line,
        )

    def test_a_form5_line_with_blank_timeliness_is_kept(self) -> None:
        """The live gate rejects this today — 22 of the 76 exempt rows."""
        assert self._reason("4", "5") is None

    def test_a_form5_line_on_a_form5_submission_is_rejected(self) -> None:
        assert self._reason("5", "5") == "future_dated"

    def test_a_form4_line_is_rejected(self) -> None:
        assert self._reason("4", "4") == "future_dated"

    def test_a_blank_submission_no_longer_hides_an_explicit_form4_line(self) -> None:
        """The fail-open exists to protect Form 3 HOLDINGS, which carry no line
        form type at all. A line explicitly typed ``4`` is not that case."""
        assert self._reason("", "4") == "future_dated"

    def test_a_blank_submission_with_no_line_type_stays_fail_open(self) -> None:
        assert self._reason("", None) is None

    def test_a_form3_holding_is_still_kept(self) -> None:
        """§16(a)(2) sets only LATEST bounds — PR #3145's assertion, retained."""
        assert self._reason("3", None) is None

    def test_the_floor_runs_before_the_form_gate(self) -> None:
        assert (
            _reject_reason_for_form(
                form_upper="",
                period_end=date(1900, 1, 1),
                filed_at=FILED,
                deemed_execution_date=None,
                trans_timeliness=None,
                trans_form_type=None,
            )
            == "pre_section_16"
        )


class TestXmlPathWiring:
    """Parsing a field is inert unless the call site passes it (Codex ckpt-1)."""

    _XML = """<?xml version="1.0"?>
    <ownershipDocument>
      <documentType>4</documentType>
      <periodOfReport>2024-05-16</periodOfReport>
      <issuer><issuerCik>0000315852</issuerCik><issuerTradingSymbol>RRC</issuerTradingSymbol></issuer>
      <reportingOwner><reportingOwnerId><rptOwnerCik>0001234567</rptOwnerCik>
        <rptOwnerName>FUNK JAMES M</rptOwnerName></reportingOwnerId></reportingOwner>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2024-06-03</value></transactionDate>
          <transactionCoding>
            <transactionFormType>5</transactionFormType>
            <transactionCode>J</transactionCode>
            <equitySwapInvolved>0</equitySwapInvolved>
          </transactionCoding>
          <transactionTimeliness><value>E</value></transactionTimeliness>
          <transactionAmounts>
            <transactionShares><value>4782</value></transactionShares>
          </transactionAmounts>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
    </ownershipDocument>"""

    def test_the_parser_extracts_the_line_form_type(self) -> None:
        parsed = insider_transactions.parse_form_4_xml(self._XML)
        assert parsed is not None
        assert [t.txn_form_type for t in parsed.transactions] == ["5"]

    def test_the_upsert_call_site_passes_both_fields(self) -> None:
        """AST over the real call site, not a grep for an import: #2789's lesson
        is that a contract living only in a docstring is not enforced, and an
        import proves nothing about the CALL."""
        source = inspect.getsource(insider_transactions.upsert_filing)
        tree = ast.parse(Path(inspect.getsourcefile(insider_transactions.upsert_filing) or "").read_text())
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "evaluate_insider_date_validity"
            and any(kw.arg == "submission_form_type" for kw in node.keywords)
            and any(kw.arg == "txn_form_type" for kw in node.keywords)
        ]
        assert calls, "no evaluate_insider_date_validity call passes both form-type keywords"
        assert "submission_form_type=parsed.document_type" in " ".join(source.split())
