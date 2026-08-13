"""#2436 — a stored thesis whose memo names a different company is refused.

Pure-logic. Every test here pins a decision the deterministic layer makes about
a stored verdict; none of them needs a database, and the ones that would (the
alerts feed's SQL, the backfill's UPDATE) are covered by the census in the PR
rather than by an integration test per code path.

⚠ The subject-identity RULE itself is #2431's and is tested in
``tests/test_thesis.py``. What is new here is the STORED side: NULL is not
True, absence is not refusal, and the severe-risk exit is not thesis-derived.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from app.services.entry_timing import _compute_take_profit
from app.services.portfolio import (
    EXIT_RED_FLAG_THRESHOLD,
    _evaluate_exit,
    _thesis_absence_reason,
)
from app.services.reporting import _thesis_summary
from app.services.thesis_subject_identity import (
    QUARANTINE_REASON,
    RULE_SET_VERSION,
    ensure_subject_identity_verdicts,
    is_thesis_usable,
    subject_is_checkable,
)
from tests.test_portfolio import _pos, _thesis


class TestIsThesisUsable:
    def test_only_true_is_usable(self) -> None:
        assert is_thesis_usable({"subject_identity_ok": True}) is True

    def test_false_verdict_is_refused(self) -> None:
        assert is_thesis_usable({"subject_identity_ok": False}) is False

    def test_null_verdict_is_refused(self) -> None:
        """⚠⚠ NULL is *not yet checked*, not *passed*.

        A row written before sql/332, or inserted with no subject to check
        against, carries NULL. Reading its valuation band on that basis is the
        exact risk #2436 exists to remove — ``portfolio.py`` turns
        ``base_value`` into an EXIT trigger on a live position, and 14 such
        exits had already fired on wrong-company bands. Fail closed.
        """
        assert is_thesis_usable({"subject_identity_ok": None}) is False

    def test_missing_column_is_refused(self) -> None:
        """A caller that forgot to SELECT the verdict gets the safe answer,
        not the optimistic one."""
        assert is_thesis_usable({"stance": "buy", "base_value": 200.0}) is False

    def test_no_row_is_refused(self) -> None:
        assert is_thesis_usable(None) is False


class TestSubjectIsCheckable:
    def test_symbol_alone_is_enough(self) -> None:
        assert subject_is_checkable({"symbol": "OTEX", "company_name": None}) is True

    def test_company_name_alone_is_enough(self) -> None:
        assert subject_is_checkable({"symbol": "", "company_name": "Open Text Corp"}) is True

    def test_empty_dict_is_not_checkable(self) -> None:
        """⚠ ``_build_context`` yields ``{}`` when the instruments row is
        missing. The rule scores that False, which would be stored as
        "checked and failed" when nothing was checked."""
        assert subject_is_checkable({}) is False
        assert subject_is_checkable({"symbol": "  ", "company_name": None}) is False

    def test_non_dict_is_not_checkable(self) -> None:
        assert subject_is_checkable(None) is False
        assert subject_is_checkable("OTEX") is False


class TestRuleSetVersion:
    def test_version_is_id_plus_code_hash(self) -> None:
        """The stored rule version must identify the CODE, not a hand-bumped
        integer (the ``price_quarantine`` form). A verdict whose rule cannot be
        identified cannot be re-audited."""
        rule_id, _, code_hash = RULE_SET_VERSION.partition("+")
        assert rule_id == "thesis-subject-identity-v1"
        assert len(code_hash) == 12
        assert all(c in "0123456789abcdef" for c in code_hash)


class _FakeConn:
    """Minimal ``conn.execute`` stand-in: first call returns the SELECT rows,
    every later call is an UPDATE and is recorded.

    ⚠⚠ IT RECORDS PARAMETERS AND SENDS NOTHING, so it proves what the probe
    DECIDES and nothing about whether Postgres can plan the write. #2647 was
    invisible here for exactly that reason: ``_WRITE_VERDICT_SQL`` raised
    ``AmbiguousParameter`` on the ``ok=None`` path these tests assert, while
    these tests stayed green. The real-backend twins live in
    ``tests/test_thesis_subject_identity_null_verdict_db.py`` — keep both.
    """

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self._selected = False
        self.updates: list[dict[str, Any]] = []

    def execute(self, _sql: str, params: dict[str, Any] | None = None) -> Any:
        if not self._selected:
            self._selected = True
            return SimpleNamespace(fetchall=lambda: self._rows)
        self.updates.append(params or {})
        return SimpleNamespace(fetchall=list)


class TestEnsureSubjectIdentityVerdicts:
    """The lifespan self-heal (#2436). ⚠ This is what makes sql/332 safe to
    deploy: the migration can only add NULL columns and every consumer reads
    NULL as quarantined, so without this a migrate-and-deploy strips the whole
    historical corpus out of the deterministic layer."""

    def test_verdicts_are_written_for_unchecked_rows(self) -> None:
        conn = _FakeConn(
            [
                (1, "OTEX", "Open Text Corp", "Open Text renewals are sticky.", None),
                (2, "OTEX", "Open Text Corp", "Apple Inc. (AAPL) is executing on AI.", None),
            ]
        )
        assert ensure_subject_identity_verdicts(conn) == 2
        assert [u["ok"] for u in conn.updates] == [True, False]
        assert {u["ver"] for u in conn.updates} == {RULE_SET_VERSION}

    def test_unverdictable_row_already_null_is_not_rewritten(self) -> None:
        """⚠ Without this the row is re-read and re-written on EVERY boot: its
        rule version is NULL by the sql/332 CHECK, so it always looks stale."""
        conn = _FakeConn([(3, None, None, "A memo with no subject to check.", None)])
        assert ensure_subject_identity_verdicts(conn) == 0
        assert conn.updates == []

    def test_a_row_that_became_unverdictable_is_reset_to_null(self) -> None:
        """The other direction: a row previously verdicted whose instrument no
        longer carries a checkable name must lose the stale verdict, not keep
        it. NULL is refused by the consumers, so this fails closed."""
        conn = _FakeConn([(4, None, None, "Memo text.", True)])
        assert ensure_subject_identity_verdicts(conn) == 1
        assert conn.updates[0]["ok"] is None


class TestPortfolioQuarantine:
    def test_quarantined_thesis_does_not_fire_a_valuation_exit(self) -> None:
        """The defect itself: 14 EXIT rows had already fired
        "Valuation target reached" against a base_value written about a
        different company."""
        details: dict[str, Any] = {"thesis_quarantined": True}
        should_exit, reason = _evaluate_exit(_pos(), details, current_price=9999.0)
        assert should_exit is False
        assert reason == ""

    def test_quarantined_thesis_still_fires_the_severe_risk_exit(self) -> None:
        """⚠⚠ THE REGRESSION THIS GUARD EXISTS FOR.

        ``_evaluate_exit`` returns early when no thesis is present, so simply
        withholding the quarantined thesis would have silently removed the
        SEVERE-RISK exit as well — a rule that reads no thesis field at all.
        On the dev corpus that is 5 EXIT recommendations across the quarantined
        set, 4 of whose instruments are currently held. The exit that stops
        firing leaves no row, so nothing would have shown it.

        ``docs/settled-decisions.md:277`` lists severe risk event as its own v1
        EXIT trigger, independent of the thesis.
        """
        details: dict[str, Any] = {
            "thesis_quarantined": True,
            "max_red_flag": EXIT_RED_FLAG_THRESHOLD,
        }
        should_exit, reason = _evaluate_exit(_pos(), details, current_price=1.0)
        assert should_exit is True
        assert "risk" in reason.lower()

    def test_absent_thesis_still_returns_no_exit(self) -> None:
        """The pre-existing no-thesis behaviour is unchanged: an instrument
        with no thesis at all gets no exit, quarantine or not."""
        should_exit, _ = _evaluate_exit(_pos(), {"max_red_flag": EXIT_RED_FLAG_THRESHOLD}, current_price=150.0)
        assert should_exit is False

    def test_usable_thesis_still_fires_a_valuation_exit(self) -> None:
        details: dict[str, Any] = {"thesis": _thesis(base_value=200.0)}
        should_exit, reason = _evaluate_exit(_pos(), details, current_price=200.0)
        assert should_exit is True
        assert "valuation" in reason.lower()

    def test_absence_reason_distinguishes_refusal_from_nothing_written(self) -> None:
        """Absence and "we refuse to use what we have" are different facts
        (docs/review-prevention-log.md:2820). An operator told "No thesis" for
        an instrument whose thesis page shows a memo has been misinformed."""
        assert _thesis_absence_reason({}) == "No thesis"
        assert QUARANTINE_REASON in _thesis_absence_reason({"thesis_quarantined": True})


class TestEntryTimingQuarantine:
    def test_no_base_value_yields_no_take_profit(self) -> None:
        """The quarantine reaches the TP by nulling ``base_value`` before it
        is read, so the level a real position would be closed at is never
        derived from a wrong-company memo."""
        assert _compute_take_profit(Decimal("100"), None) is None

    def test_base_value_still_yields_a_take_profit_when_usable(self) -> None:
        assert _compute_take_profit(Decimal("100"), Decimal("150")) == Decimal("150")


class TestReportingSummary:
    def test_quarantined_is_published_beside_not_evaluable_not_instead_of_it(self) -> None:
        """A refused thesis and an absent one both land in ``not_evaluable``,
        and that is where the conflation does damage: a rising count reads as
        "we lack data" when the truth is "we hold data we refuse to score
        against"."""
        rows: list[dict[str, Any]] = [
            {"target_hit": "base", "stance": "buy"},
            {"target_hit": None, "stance": None, "thesis_quarantined": True},
            {"target_hit": None, "stance": None},
        ]
        summary = _thesis_summary(rows)
        assert summary["not_evaluable"] == 2
        assert summary["quarantined"] == 1
        assert summary["evaluated"] == 1
