"""
Tests for app.services.execution_guard.

Structure:
  - TestCheckKillSwitch        — _check_kill_switch pure logic
  - TestCheckConfigFlags       — _check_auto_trading / _check_live_trading
  - TestCheckCoverage          — _check_coverage
  - TestCheckThesisFreshness   — _check_thesis_freshness
  - TestCheckSpread            — _check_spread
  - TestCheckCash              — _check_cash
  - TestCheckConcentration     — _check_concentration
  - TestBuildExplanation       — _build_explanation
  - TestEvaluateRecommendation — end-to-end via evaluate_recommendation with mock DB

Mock DB approach mirrors test_portfolio.py:
  - _make_cursor(rows) builds a context-manager cursor mock
  - _make_conn(cursors) builds a connection mock whose cursor() calls consume
    a sequence of pre-built cursors in order
  - conn.execute() is a no-op mock (for UPDATE and INSERT without RETURNING)
  - conn.transaction() is a no-op context manager

Cursor call order inside evaluate_recommendation:
  1. _load_recommendation          — fetchone
  2. _load_kill_switch             — fetchone
  3. get_runtime_config            — fetchone (runtime_config singleton)
  4. _load_coverage                — fetchone
  5. _load_latest_thesis           — fetchone
  6. _load_quote                   — fetchone
  7. _load_cash                    — fetchone
  8. _load_sector_exposure         — 4 cursors: instruments, positions,
                                     cash_ledger, mirror_equity
                                     (the mirror_equity cursor is consumed
                                     by _load_mirror_equity, wired into
                                     total_aum by Track 1b / #187).
  9. _write_audit                  — 1 cursor (INSERT RETURNING decision_id)
     + conn.execute (UPDATE status)

Note: for EXIT actions, cursors 4-8 (coverage through sector_exposure) are
skipped, so the sequence is shorter.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.execution_guard import (
    GuardResult,
    RuleResult,
    _build_explanation,
    _check_auto_trading,
    _check_cash,
    _check_concentration,
    _check_coverage,
    _check_kill_switch,
    _check_live_trading,
    _check_spread,
    _check_thesis_freshness,
    evaluate_recommendation,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 9, 0, 0, tzinfo=UTC)


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor_sequence: list[MagicMock]) -> MagicMock:
    """
    Build a fake psycopg connection.
    conn.cursor() calls consume cursor_sequence in order.
    conn.execute() is a no-op mock (UPDATE only).
    conn.transaction() is a no-op context manager.
    """
    conn = MagicMock()
    conn.cursor.side_effect = cursor_sequence
    conn.execute.return_value = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _rec_cursor(
    action: str = "BUY",
    instrument_id: int = 1,
    recommendation_id: int = 42,
    model_version: str | None = "v1-balanced",
) -> MagicMock:
    return _make_cursor(
        [
            {
                "recommendation_id": recommendation_id,
                "instrument_id": instrument_id,
                "action": action,
                "model_version": model_version,
            }
        ]
    )


def _runtime_cursor(
    enable_auto_trading: bool = True,
    enable_live_trading: bool = True,
) -> MagicMock:
    """Cursor returning a runtime_config row.

    Pass an empty list to _make_cursor to simulate a missing singleton row
    (configuration corruption).
    """
    return _make_cursor(
        [
            {
                "enable_auto_trading": enable_auto_trading,
                "enable_live_trading": enable_live_trading,
                "updated_at": _NOW,
                "updated_by": "test",
                "reason": "test",
            }
        ]
    )


def _ks_cursor(is_active: bool = False, reason: str | None = None) -> MagicMock:
    return _make_cursor(
        [
            {
                "is_active": is_active,
                "activated_at": _NOW if is_active else None,
                "reason": reason,
            }
        ]
    )


def _coverage_cursor(tier: int = 1, frequency: str = "weekly") -> MagicMock:
    return _make_cursor([{"coverage_tier": tier, "review_frequency": frequency}])


def _thesis_cursor(age_days: int = 3) -> MagicMock:
    created_at = _NOW - timedelta(days=age_days)
    return _make_cursor([{"created_at": created_at}])


def _quote_cursor(spread_flag: bool | None = False) -> MagicMock:
    return _make_cursor([{"spread_flag": spread_flag}])


def _cash_ledger_cursor(balance: float | None = 10_000.0) -> MagicMock:
    return _make_cursor([{"balance": balance}])


def _sector_cursors(
    sector: str | None = "Technology",
    sector_market_value: float = 0.0,
    total_positions: float = 0.0,
    cash: float = 50_000.0,
    instrument_missing: bool = False,
    mirror_equity: float = 0.0,
) -> list[MagicMock]:
    """Return the cursors consumed by _load_sector_exposure.

    When instrument_missing=True the instruments cursor returns no rows and
    _load_sector_exposure returns early — only 1 cursor is consumed.
    Otherwise 4 cursors are returned (instruments, positions, cash_ledger,
    mirror_equity). The mirror_equity cursor is consumed by
    `_load_mirror_equity`, wired into `total_aum` by Track 1b (#187).
    Existing mock-driven tests default it to 0.0 so the pre-PR behaviour
    is preserved bit-identically.
    """
    if instrument_missing:
        return [_make_cursor([])]
    instrument_cur = _make_cursor([{"sector": sector}])
    if sector is not None and sector_market_value > 0:
        positions_cur = _make_cursor([{"sector": sector, "market_value": sector_market_value}])
    else:
        positions_cur = _make_cursor([])
    cash_cur = _make_cursor([{"balance": cash}])
    mirror_cur = _make_cursor([{"total": mirror_equity}])
    return [instrument_cur, positions_cur, cash_cur, mirror_cur]


def _audit_cursor(decision_id: int = 99) -> MagicMock:
    return _make_cursor([{"decision_id": decision_id}])


def _buy_cursors(
    *,
    ks_active: bool = False,
    runtime_auto: bool = True,
    runtime_live: bool = True,
    runtime_corrupt: bool = False,
    coverage_tier: int = 1,
    coverage_frequency: str = "weekly",
    thesis_age_days: int = 3,
    spread_flag: bool | None = False,
    cash_balance: float | None = 10_000.0,
    sector: str | None = "Technology",
    sector_mv: float = 0.0,
    total_positions: float = 0.0,
    cash_for_sector: float = 50_000.0,
    instrument_missing: bool = False,
    decision_id: int = 99,
) -> list[MagicMock]:
    """Convenience: build the full cursor sequence for a BUY evaluation."""
    runtime = (
        _make_cursor([])
        if runtime_corrupt
        else _runtime_cursor(enable_auto_trading=runtime_auto, enable_live_trading=runtime_live)
    )
    return [
        _rec_cursor(action="BUY"),
        _ks_cursor(is_active=ks_active),
        runtime,
        _coverage_cursor(tier=coverage_tier, frequency=coverage_frequency),
        _thesis_cursor(age_days=thesis_age_days),
        _quote_cursor(spread_flag=spread_flag),
        _cash_ledger_cursor(balance=cash_balance),
        *_sector_cursors(
            sector=sector,
            sector_market_value=sector_mv,
            total_positions=total_positions,
            cash=cash_for_sector,
            instrument_missing=instrument_missing,
        ),
        _audit_cursor(decision_id=decision_id),
    ]


def _exit_cursors(
    *,
    ks_active: bool = False,
    runtime_auto: bool = True,
    runtime_live: bool = True,
    runtime_corrupt: bool = False,
    decision_id: int = 99,
) -> list[MagicMock]:
    """Convenience: build the full cursor sequence for an EXIT evaluation."""
    runtime = (
        _make_cursor([])
        if runtime_corrupt
        else _runtime_cursor(enable_auto_trading=runtime_auto, enable_live_trading=runtime_live)
    )
    return [
        _rec_cursor(action="EXIT"),
        _ks_cursor(is_active=ks_active),
        runtime,
        _audit_cursor(decision_id=decision_id),
    ]


# ---------------------------------------------------------------------------
# TestCheckKillSwitch
# ---------------------------------------------------------------------------


class TestCheckKillSwitch:
    def test_row_missing_fails_closed(self) -> None:
        result = _check_kill_switch(None)
        assert result.passed is False
        assert result.rule == "kill_switch_config_corrupt"
        assert "corrupt" in result.detail

    def test_active_kill_switch_fails(self) -> None:
        row = {"is_active": True, "activated_at": _NOW, "reason": "emergency"}
        result = _check_kill_switch(row)
        assert result.passed is False
        assert result.rule == "kill_switch"
        assert "emergency" in result.detail

    def test_inactive_kill_switch_passes(self) -> None:
        row = {"is_active": False, "activated_at": None, "reason": None}
        result = _check_kill_switch(row)
        assert result.passed is True
        assert result.rule == "kill_switch"


# ---------------------------------------------------------------------------
# TestCheckConfigFlags
# ---------------------------------------------------------------------------


class TestCheckConfigFlags:
    def test_auto_trading_disabled_fails(self) -> None:
        result = _check_auto_trading(False)
        assert result.passed is False
        assert result.rule == "auto_trading"

    def test_auto_trading_enabled_passes(self) -> None:
        result = _check_auto_trading(True)
        assert result.passed is True

    def test_live_trading_disabled_fails(self) -> None:
        result = _check_live_trading(False)
        assert result.passed is False
        assert result.rule == "live_trading"

    def test_live_trading_enabled_passes(self) -> None:
        result = _check_live_trading(True)
        assert result.passed is True


# ---------------------------------------------------------------------------
# TestCheckCoverage
# ---------------------------------------------------------------------------


class TestCheckCoverage:
    def test_no_coverage_row_fails(self) -> None:
        result = _check_coverage(None)
        assert result.passed is False
        assert result.rule == "no_coverage_row"

    def test_tier_2_fails(self) -> None:
        result = _check_coverage({"coverage_tier": 2, "review_frequency": "weekly"})
        assert result.passed is False
        assert result.rule == "coverage_not_tier1"
        assert "2" in result.detail

    def test_tier_1_passes(self) -> None:
        result = _check_coverage({"coverage_tier": 1, "review_frequency": "weekly"})
        assert result.passed is True


# ---------------------------------------------------------------------------
# TestCheckThesisFreshness
# ---------------------------------------------------------------------------


class TestCheckThesisFreshness:
    def test_no_thesis_fails(self) -> None:
        result = _check_thesis_freshness(None, {"review_frequency": "weekly"}, _NOW)
        assert result.passed is False
        assert result.rule == "no_thesis"

    def test_unknown_frequency_fails_conservative(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=1)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "unknown"}, _NOW)
        assert result.passed is False
        assert result.rule == "thesis_stale"

    def test_null_frequency_fails_conservative(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=1)}
        result = _check_thesis_freshness(thesis, {"review_frequency": None}, _NOW)
        assert result.passed is False
        assert result.rule == "thesis_stale"

    def test_fresh_weekly_thesis_passes(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=3)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "weekly"}, _NOW)
        assert result.passed is True

    def test_stale_weekly_thesis_fails(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=8)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "weekly"}, _NOW)
        assert result.passed is False
        assert result.rule == "thesis_stale"

    def test_fresh_daily_passes(self) -> None:
        thesis = {"created_at": _NOW - timedelta(hours=12)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "daily"}, _NOW)
        assert result.passed is True

    def test_stale_daily_fails(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=2)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "daily"}, _NOW)
        assert result.passed is False

    def test_fresh_monthly_passes(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=15)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "monthly"}, _NOW)
        assert result.passed is True

    def test_stale_monthly_fails(self) -> None:
        thesis = {"created_at": _NOW - timedelta(days=31)}
        result = _check_thesis_freshness(thesis, {"review_frequency": "monthly"}, _NOW)
        assert result.passed is False

    def test_naive_datetime_treated_as_utc(self) -> None:
        # Thesis with a naive datetime should not raise; treated as UTC
        naive_created_at = (_NOW - timedelta(days=3)).replace(tzinfo=None)
        thesis = {"created_at": naive_created_at}
        result = _check_thesis_freshness(thesis, {"review_frequency": "weekly"}, _NOW)
        assert result.passed is True


# ---------------------------------------------------------------------------
# TestCheckSpread
# ---------------------------------------------------------------------------


class TestCheckSpread:
    def test_no_quote_row_fails(self) -> None:
        result = _check_spread(None)
        assert result.passed is False
        assert result.rule == "spread_unavailable"

    def test_null_spread_flag_fails(self) -> None:
        result = _check_spread({"spread_flag": None})
        assert result.passed is False
        assert result.rule == "spread_unavailable"

    def test_spread_flag_true_fails(self) -> None:
        result = _check_spread({"spread_flag": True})
        assert result.passed is False
        assert result.rule == "spread_wide"

    def test_spread_flag_false_passes(self) -> None:
        result = _check_spread({"spread_flag": False})
        assert result.passed is True


# ---------------------------------------------------------------------------
# TestCheckCash
# ---------------------------------------------------------------------------


class TestCheckCash:
    def test_none_cash_fails(self) -> None:
        result = _check_cash(None)
        assert result.passed is False
        assert result.rule == "cash_unknown"

    def test_zero_cash_fails(self) -> None:
        # Zero means no buying power — block BUY/ADD
        result = _check_cash(0.0)
        assert result.passed is False
        assert result.rule == "cash_unknown"

    def test_negative_cash_fails(self) -> None:
        result = _check_cash(-1.0)
        assert result.passed is False
        assert result.rule == "cash_unknown"

    def test_positive_cash_passes(self) -> None:
        result = _check_cash(5_000.0)
        assert result.passed is True


# ---------------------------------------------------------------------------
# TestCheckConcentration
# ---------------------------------------------------------------------------


class TestCheckConcentration:
    def test_instrument_missing_fails(self) -> None:
        # Missing instrument row is a data-integrity failure — must not silently pass
        result = _check_concentration(False, None, 0.0, 100_000.0)
        assert result.passed is False
        assert result.rule == "instrument_missing"

    def test_null_sector_fails(self) -> None:
        # Instrument exists but has NULL sector — cannot verify concentration
        result = _check_concentration(True, None, 0.0, 100_000.0)
        assert result.passed is False
        assert result.rule == "sector_missing"

    def test_zero_aum_passes(self) -> None:
        result = _check_concentration(True, "Technology", 0.0, 0.0)
        assert result.passed is True
        assert "total_aum=0" in result.detail

    def test_within_cap_passes(self) -> None:
        # 20% current + 5% alloc = 25% — exactly at cap, not over (> not >=)
        result = _check_concentration(True, "Technology", 0.20, 100_000.0)
        assert result.passed is True

    def test_breach_fails(self) -> None:
        # 21% current + 5% alloc = 26% > 25%
        result = _check_concentration(True, "Technology", 0.21, 100_000.0)
        assert result.passed is False
        assert result.rule == "concentration_breach"
        assert "Technology" in result.detail


# ---------------------------------------------------------------------------
# TestBuildExplanation
# ---------------------------------------------------------------------------


class TestBuildExplanation:
    def test_all_passed_returns_all_pass(self) -> None:
        results = [
            RuleResult(rule="kill_switch", passed=True),
            RuleResult(rule="live_trading", passed=True),
        ]
        assert _build_explanation(results) == "All rules passed"

    def test_single_failure_named(self) -> None:
        results = [
            RuleResult(rule="kill_switch", passed=True),
            RuleResult(rule="cash_unknown", passed=False, detail="ledger empty"),
        ]
        explanation = _build_explanation(results)
        assert "FAIL" in explanation
        assert "cash_unknown" in explanation
        assert "ledger empty" in explanation

    def test_multiple_failures_all_listed(self) -> None:
        results = [
            RuleResult(rule="kill_switch", passed=False, detail="active"),
            RuleResult(rule="live_trading", passed=False, detail="flag=False"),
        ]
        explanation = _build_explanation(results)
        assert "kill_switch" in explanation
        assert "live_trading" in explanation


# ---------------------------------------------------------------------------
# TestEvaluateRecommendation
# ---------------------------------------------------------------------------


class TestEvaluateRecommendation:
    def _eval(
        self,
        cursors: list[MagicMock],
        recommendation_id: int = 42,
    ) -> GuardResult:
        conn = _make_conn(cursors)
        with patch("app.services.execution_guard._utcnow", return_value=_NOW):
            return evaluate_recommendation(conn, recommendation_id)

    # --- Happy path ---

    def test_all_rules_pass_buy_returns_pass(self) -> None:
        result = self._eval(_buy_cursors())
        assert result.verdict == "PASS"
        assert result.failed_rules == []
        assert result.recommendation_id == 42

    def test_all_rules_pass_exit_returns_pass(self) -> None:
        result = self._eval(_exit_cursors())
        assert result.verdict == "PASS"
        assert result.failed_rules == []

    # --- Kill switch ---

    def test_kill_switch_active_fails_buy(self) -> None:
        cursors = _buy_cursors(ks_active=True)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "kill_switch" in result.failed_rules

    def test_kill_switch_active_fails_exit(self) -> None:
        cursors = _exit_cursors(ks_active=True)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "kill_switch" in result.failed_rules

    def test_kill_switch_row_missing_fails_closed(self) -> None:
        # Override: kill_switch cursor returns no rows
        cursors = _buy_cursors()
        cursors[1] = _make_cursor([])  # empty = no kill_switch row
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "kill_switch_config_corrupt" in result.failed_rules

    # --- Runtime config flags ---

    def test_auto_trading_disabled_fails_buy(self) -> None:
        result = self._eval(_buy_cursors(runtime_auto=False))
        assert result.verdict == "FAIL"
        assert "auto_trading" in result.failed_rules

    def test_live_trading_disabled_fails_buy(self) -> None:
        result = self._eval(_buy_cursors(runtime_live=False))
        assert result.verdict == "FAIL"
        assert "live_trading" in result.failed_rules

    def test_both_runtime_flags_off_both_appear_in_failed_rules(self) -> None:
        result = self._eval(_buy_cursors(runtime_auto=False, runtime_live=False))
        assert result.verdict == "FAIL"
        assert "auto_trading" in result.failed_rules
        assert "live_trading" in result.failed_rules

    def test_runtime_config_row_missing_fails_closed_buy(self) -> None:
        # Missing runtime_config singleton -> fail closed; auto/live rules
        # are NOT emitted because the corrupt-config rule supersedes them.
        result = self._eval(_buy_cursors(runtime_corrupt=True))
        assert result.verdict == "FAIL"
        assert "runtime_config_corrupt" in result.failed_rules
        assert "auto_trading" not in result.failed_rules
        assert "live_trading" not in result.failed_rules

    def test_runtime_config_row_missing_fails_closed_exit(self) -> None:
        # Even an EXIT, which skips most checks, must fail when config is corrupt
        result = self._eval(_exit_cursors(runtime_corrupt=True))
        assert result.verdict == "FAIL"
        assert "runtime_config_corrupt" in result.failed_rules

    # --- Coverage ---

    def test_non_tier1_coverage_fails_buy(self) -> None:
        cursors = _buy_cursors(coverage_tier=2)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "coverage_not_tier1" in result.failed_rules

    def test_no_coverage_row_fails_buy(self) -> None:
        cursors = _buy_cursors()
        cursors[3] = _make_cursor([])  # no coverage row
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "no_coverage_row" in result.failed_rules
        # thesis_stale must not appear — review_frequency is unknowable without
        # a coverage row; emitting it alongside no_coverage_row is audit noise
        assert "thesis_stale" not in result.failed_rules

    def test_coverage_not_tier1_does_not_fail_exit(self) -> None:
        # EXIT skips coverage check entirely
        result = self._eval(_exit_cursors())
        assert result.verdict == "PASS"

    # --- Thesis freshness ---

    def test_stale_thesis_fails_buy(self) -> None:
        cursors = _buy_cursors(thesis_age_days=10, coverage_frequency="weekly")
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "thesis_stale" in result.failed_rules

    def test_no_thesis_row_fails_buy(self) -> None:
        cursors = _buy_cursors()
        cursors[4] = _make_cursor([])  # no thesis row
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "no_thesis" in result.failed_rules

    def test_stale_thesis_does_not_fail_exit(self) -> None:
        result = self._eval(_exit_cursors())
        assert result.verdict == "PASS"

    # --- Spread ---

    def test_spread_flag_true_fails_buy(self) -> None:
        cursors = _buy_cursors(spread_flag=True)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "spread_wide" in result.failed_rules

    def test_spread_unavailable_fails_buy(self) -> None:
        cursors = _buy_cursors()
        cursors[5] = _make_cursor([])  # no quotes row
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "spread_unavailable" in result.failed_rules

    def test_spread_does_not_fail_exit(self) -> None:
        result = self._eval(_exit_cursors())
        assert result.verdict == "PASS"

    # --- Cash ---

    def test_cash_unknown_fails_buy(self) -> None:
        cursors = _buy_cursors(cash_balance=None)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "cash_unknown" in result.failed_rules

    def test_zero_cash_fails_buy(self) -> None:
        cursors = _buy_cursors(cash_balance=0.0)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "cash_unknown" in result.failed_rules

    def test_cash_unknown_does_not_fail_exit(self) -> None:
        result = self._eval(_exit_cursors())
        assert result.verdict == "PASS"

    # --- Concentration ---

    def test_concentration_breach_fails_buy(self) -> None:
        # sector already at 21% of AUM; adding 5% → 26% > 25%
        # total_aum = sector_mv + cash = 10_500 + 39_500 = 50_000
        # sector_pct = 10_500 / 50_000 = 21%
        cursors = _buy_cursors(
            sector="Technology",
            sector_mv=10_500.0,
            cash_for_sector=39_500.0,
        )
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "concentration_breach" in result.failed_rules

    def test_concentration_at_cap_passes_buy(self) -> None:
        # Exactly at 20% current + 5% alloc = 25% — not >, so passes
        # total_aum = 10_000 + 40_000 = 50_000; sector_pct = 10_000 / 50_000 = 20%
        cursors = _buy_cursors(
            sector="Technology",
            sector_mv=10_000.0,
            cash_for_sector=40_000.0,
        )
        result = self._eval(cursors)
        assert result.verdict == "PASS"

    def test_instrument_missing_fails_buy(self) -> None:
        # Instrument not in instruments table → hard FAIL, not silent pass
        cursors = _buy_cursors(instrument_missing=True)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "instrument_missing" in result.failed_rules

    def test_null_sector_fails_buy(self) -> None:
        # Instrument exists but sector is NULL → sector_missing FAIL
        cursors = _buy_cursors(sector=None)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "sector_missing" in result.failed_rules

    # --- Audit writing ---

    def test_audit_written_on_pass(self) -> None:
        result = self._eval(_buy_cursors(decision_id=77))
        assert result.decision_id == 77

    def test_audit_written_on_fail(self) -> None:
        cursors = _buy_cursors(ks_active=True, decision_id=55)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert result.decision_id == 55

    def test_status_update_execute_called(self) -> None:
        """conn.execute() must fire the UPDATE — ensures the status write cannot be silently dropped."""
        conn = _make_conn(_buy_cursors(decision_id=99))
        with patch("app.services.execution_guard._utcnow", return_value=_NOW):
            evaluate_recommendation(conn, 42)
        conn.execute.assert_called_once()
        call_sql: str = conn.execute.call_args[0][0]
        assert "UPDATE trade_recommendations" in call_sql
        assert "status" in call_sql

    def test_db_write_and_return_decision_id_match(self) -> None:
        """GuardResult.decision_id reflects the value returned from the INSERT."""
        result = self._eval(_buy_cursors(decision_id=123))
        assert result.decision_id == 123

    def test_audit_recommendation_fk(self) -> None:
        """recommendation_id on the result matches the input."""
        result = self._eval(_buy_cursors(), recommendation_id=42)
        assert result.recommendation_id == 42

    # --- Multiple failures ---

    def test_multiple_failed_rules_all_listed(self) -> None:
        # Kill switch active + both config flags off
        cursors = _buy_cursors(ks_active=True, runtime_auto=False, runtime_live=False)
        result = self._eval(cursors)
        assert result.verdict == "FAIL"
        assert "kill_switch" in result.failed_rules
        assert "auto_trading" in result.failed_rules
        assert "live_trading" in result.failed_rules
        # All three must appear in the explanation string too
        assert "kill_switch" in result.explanation
        assert "auto_trading" in result.explanation
        assert "live_trading" in result.explanation

    # --- Error paths ---

    def test_recommendation_not_found_raises_value_error(self) -> None:
        conn = _make_conn([_make_cursor([])])  # fetchone returns None
        with pytest.raises(ValueError, match="recommendation_id=42 not found"):
            with patch("app.services.execution_guard._utcnow", return_value=_NOW):
                evaluate_recommendation(conn, 42)

    def test_recommendation_not_found_does_not_write_audit(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(ValueError):
            with patch("app.services.execution_guard._utcnow", return_value=_NOW):
                evaluate_recommendation(conn, 42)
        # transaction() should never have been entered
        conn.transaction.assert_not_called()
