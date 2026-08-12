from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest

from app.services.trial_register import TRIAL_REGISTER, TrialExactness
from scripts.evaluate_2582_schedule13d_outcomes import (
    _ALL_13D_PUBLIC_DATES_SQL,
    _CREATE_TEMP_SESSIONS,
    _INITIAL_13G_SOURCE_SQL,
    _SOURCE_EVENTS_SQL,
    ACKNOWLEDGEMENT,
    STRATEGY_ID,
    STRATEGY_VERSION,
    TRIAL_ID,
    Initial13GSourceEvent,
    OutcomeGateRefusal,
    PriceWindow,
    SourceEvent,
    accepted_window_return_pct,
    bucket,
    build_random_time_requests,
    match_tie_break,
    next_regular_session_strictly_after,
    nth_regular_session,
    prepare_price_window_workspace,
    regular_sessions_ending_before,
    require_outcome_gate_preconditions,
    required_event_sessions,
    required_sessions_for_entry,
    total_return_pct,
    treatment_event_outcome,
    window_match_features,
)
from scripts.verify_2582_schedule13d_preregistration import EXPECTED_SHA256, load_and_verify


def test_gate_refuses_without_explicit_acknowledgement() -> None:
    with pytest.raises(OutcomeGateRefusal, match="remain closed"):
        require_outcome_gate_preconditions(
            acknowledgement=None,
            contract_path=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
        )


def test_source_population_uses_public_manifest_clock_and_cannot_read_prices() -> None:
    lowered = _SOURCE_EVENTS_SQL.lower()
    assert "sec_filing_manifest" in lowered
    assert "m.filed_at::date" in lowered
    assert "research_price_daily" not in lowered
    assert "b.filed_at" not in lowered
    assert "between %(first_source_date)s and %(last_complete_filing_date)s" in lowered


def test_13g_challenger_source_is_frozen_outcome_free_and_rule_separated() -> None:
    lowered = _INITIAL_13G_SOURCE_SQL.lower()
    assert "b.submission_type = 'schedule 13g'" in lowered
    assert "sec_filing_manifest" in lowered
    assert "filing_raw_documents" in lowered
    assert "designaterulepursuantthisschedulefiled" in lowered
    assert "[[:space:]]*rule[[:space:]]+13d-1" in lowered
    assert "research_price_daily" not in lowered
    assert "between %(first_source_date)s and %(last_complete_filing_date)s" in lowered


def test_random_placebo_halo_source_is_outcome_free_and_includes_amendments() -> None:
    lowered = _ALL_13D_PUBLIC_DATES_SQL.lower()
    assert "'schedule 13d', 'schedule 13d/a'" in lowered
    assert "sec_filing_manifest" in lowered
    assert "research_price_daily" not in lowered


def test_price_workspace_survives_commit_for_one_read_only_outcome_snapshot() -> None:
    assert "ON COMMIT PRESERVE ROWS" in _CREATE_TEMP_SESSIONS
    assert "ON COMMIT DROP" not in _CREATE_TEMP_SESSIONS


class _WorkspaceInfo:
    transaction_status = psycopg.pq.TransactionStatus.IDLE


class _WorkspaceConnection:
    info = _WorkspaceInfo()

    def __init__(self, *, existing: bool = False) -> None:
        self.executed: list[str] = []
        self.committed = False
        self.existing = existing

    def execute(self, query: str) -> Any:
        self.executed.append(query)
        if "to_regclass" in query:
            return _WorkspaceResult(("schedule13d_trial_sessions" if self.existing else None,))
        return None

    def commit(self) -> None:
        self.committed = True


class _WorkspaceResult:
    def __init__(self, row: tuple[str | None]) -> None:
        self.row = row

    def fetchone(self) -> tuple[str | None]:
        return self.row


def test_price_workspace_preparation_has_an_explicit_pre_outcome_commit() -> None:
    conn = _WorkspaceConnection()
    prepare_price_window_workspace(conn)  # type: ignore[arg-type]
    assert conn.executed == ["SELECT to_regclass('pg_temp.schedule13d_trial_sessions')", _CREATE_TEMP_SESSIONS]
    assert conn.committed


def test_price_workspace_preparation_reuses_an_idle_connection() -> None:
    conn = _WorkspaceConnection(existing=True)
    prepare_price_window_workspace(conn)  # type: ignore[arg-type]
    assert conn.executed == ["SELECT to_regclass('pg_temp.schedule13d_trial_sessions')"]
    assert conn.committed


def test_price_workspace_refuses_to_commit_a_caller_owned_transaction() -> None:
    conn = _WorkspaceConnection()
    conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
    with pytest.raises(OutcomeGateRefusal, match="idle connection"):
        prepare_price_window_workspace(conn)  # type: ignore[arg-type]


def test_trial_is_now_declared_so_the_register_no_longer_refuses() -> None:
    """⚠ THIS TEST'S PREMISE WAS DELIBERATELY DISSOLVED BY #2614.

    It used to assert ``absent from trial-register``, and that assertion was the
    only thing stopping C-4 from running. #2614's scope item 3 requires charging
    C-4's arms to the register, which necessarily removes that refusal — so the
    register entry and the declaration gate had to land in the SAME change, or
    adding the entry alone would have opened the ungated path this ticket exists
    to close.

    What replaces it is the pair below: the preconditions now pass, and the door
    they used to hold shut is held by the declaration check instead.
    """

    digest = require_outcome_gate_preconditions(
        acknowledgement=ACKNOWLEDGEMENT,
        contract_path=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
    )
    assert digest == EXPECTED_SHA256
    assert TRIAL_ID in TRIAL_REGISTER.trial_ids


def test_c4_register_entry_charges_seven_exact_arms() -> None:
    entry = next(trial for trial in TRIAL_REGISTER.trials if trial.trial_id == TRIAL_ID)
    # Three arms that load their own price windows (primary, unfiltered,
    # random-time) plus the four separately-reported 13G rule cells.
    assert entry.searches == 7
    assert entry.exactness is TrialExactness.EXACT


def test_strategy_identity_is_the_frozen_contract_s_own_fields() -> None:
    """A renamed candidate must not silently declare under the old identity."""

    contract, _digest = load_and_verify(Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"))
    assert contract["candidate_id"] == STRATEGY_ID
    assert contract["contract_version"] == STRATEGY_VERSION


def test_next_session_is_strict_and_observes_weekend_and_holiday() -> None:
    assert next_regular_session_strictly_after(date(2026, 7, 2)) == date(2026, 7, 6)
    assert next_regular_session_strictly_after(date(2026, 8, 10)) == date(2026, 8, 11)
    assert nth_regular_session(date(2026, 7, 6), 10) == date(2026, 7, 17)


def test_total_return_uses_adjustment_factor_and_subtracts_adverse_cost() -> None:
    # Raw price halves, but the 2-for-1 adjustment factor doubles: zero gross.
    result = total_return_pct(
        entry_open=Decimal("100"),
        entry_close=Decimal("100"),
        entry_adj_close=Decimal("50"),
        exit_close=Decimal("50"),
        exit_adj_close=Decimal("50"),
    )
    assert result == Decimal("-0.500")


@pytest.mark.parametrize("bad", [Decimal("0"), Decimal("NaN"), Decimal("-1")])
def test_total_return_refuses_bad_adjustment_inputs(bad: Decimal) -> None:
    with pytest.raises(ValueError):
        total_return_pct(
            entry_open=Decimal("10"),
            entry_close=bad,
            entry_adj_close=Decimal("10"),
            exit_close=Decimal("11"),
            exit_adj_close=Decimal("11"),
        )


def test_matching_helpers_are_boundary_stable_and_deterministic() -> None:
    edges = tuple(map(Decimal, ("5", "10", "25")))
    assert bucket(Decimal("9.99"), edges) == 1
    assert bucket(Decimal("10"), edges) == 2
    assert match_tie_break("a", "b") == match_tie_break("a", "b")
    assert match_tie_break("a", "b") != match_tie_break("a", "c")


def _source_event(**changes: object) -> SourceEvent:
    values: dict[str, object] = {
        "accession_number": "0000000001-26-000001",
        "issuer_cik": "0000000001",
        "instrument_id": 42,
        "public_filing_date": date(2026, 7, 2),
        "maximum_percent_of_class": Decimal("7.5"),
        "prior_active": False,
        "prior_passive": False,
        "same_public_date_peer": False,
        "reporter_identity_complete": True,
        "current_security_eligible": True,
        "series_ids": (99,),
        "series_adjustment_bases": ("split_adjusted",),
    }
    values.update(changes)
    return SourceEvent(**values)  # type: ignore[arg-type]


def test_primary_source_refusal_is_explicit_and_fail_closed() -> None:
    assert _source_event().primary_source_refusal is None
    assert _source_event(prior_passive=True).unfiltered_source_refusal is None
    assert _source_event(prior_passive=True).primary_source_refusal == "prior_passive_chain"
    assert _source_event(series_ids=(1, 2)).primary_source_refusal == "research_series_missing_or_ambiguous"
    assert (
        _source_event(series_adjustment_bases=("raw",)).primary_source_refusal
        == "research_series_adjustment_basis_unexpected"
    )
    assert _source_event(reporter_identity_complete=False).primary_source_refusal == "reporter_identity_missing"


def _13g_source(**changes: object) -> Initial13GSourceEvent:
    values: dict[str, object] = {
        "accession_number": "0000000001-26-000002",
        "issuer_cik": "0000000001",
        "instrument_id": 42,
        "public_filing_date": date(2026, 2, 2),
        "rule": "1b",
        "raw_document_count": 1,
        "current_security_eligible": True,
        "series_ids": (99,),
        "series_adjustment_bases": ("split_adjusted",),
    }
    values.update(changes)
    return Initial13GSourceEvent(**values)  # type: ignore[arg-type]


def test_initial_13g_source_refusal_is_explicit_and_unknown_rule_is_retained() -> None:
    assert _13g_source().source_refusal is None
    assert _13g_source(rule="unknown").source_refusal is None
    assert _13g_source(raw_document_count=0).source_refusal == "canonical_raw_document_missing_or_ambiguous"
    assert _13g_source(series_adjustment_bases=("raw",)).source_refusal == (
        "research_series_adjustment_basis_unexpected"
    )


def test_required_sessions_are_exact_and_do_not_skip_market_dates() -> None:
    sessions = required_event_sessions(_source_event())
    assert len(sessions) == 70
    assert sessions[-10] == date(2026, 7, 6)
    assert sessions[-1] == date(2026, 7, 17)
    assert regular_sessions_ending_before(date(2026, 7, 6), 2) == (
        date(2026, 7, 1),
        date(2026, 7, 2),
    )
    assert required_sessions_for_entry(date(2026, 7, 6)) == sessions


def test_random_time_requests_stay_in_entry_month_and_outside_event_halo() -> None:
    treatment = _source_event()
    requests = build_random_time_requests((treatment,), {42: (date(2026, 7, 2),)})
    assert requests
    assert all(request.population == "random" for request in requests)
    assert all(request.sessions[60].month == 7 for request in requests)
    # Filing July 2 enters July 6; +/-10 NYSE sessions excludes through July 20.
    assert min(request.sessions[60] for request in requests) == date(2026, 7, 21)
    assert all(len(request.sessions) == 70 for request in requests)


def _price_window(**changes: object) -> PriceWindow:
    values: dict[str, object] = {
        "event": _source_event(),
        "entry_date": date(2026, 7, 6),
        "exit_date": date(2026, 7, 17),
        "stock_bars_present": 70,
        "market_bars_present": 70,
        "positive_ohlcv_bars": 70,
        "positive_adjustment_bars": 70,
        "quarantine_covered_bars": 70,
        "return_usable": True,
        "entry_open": Decimal("10"),
        "entry_close": Decimal("10.1"),
        "entry_adj_close": Decimal("10.1"),
        "exit_close": Decimal("11"),
        "exit_adj_close": Decimal("11"),
        "trailing_median_dollar_volume": Decimal("20000000"),
        "prior_20_stock_return_pct": Decimal("2"),
        "prior_20_market_return_pct": Decimal("1"),
        "holding_market_return_pct": Decimal("0.5"),
    }
    values.update(changes)
    return PriceWindow(**values)  # type: ignore[arg-type]


def test_price_window_refuses_missing_exact_bar_before_assessing_return() -> None:
    assert _price_window().outcome_refusal is None
    assert _price_window(stock_bars_present=69).outcome_refusal == "exact_stock_session_missing"
    assert _price_window(quarantine_covered_bars=69).outcome_refusal == "quarantine_coverage_incomplete"
    assert (
        _price_window(positive_adjustment_bars=69).outcome_refusal
        == "corporate_action_adjustment_missing_or_nonpositive"
    )


def test_shared_price_window_preserves_population_specific_source_gate() -> None:
    repeated = _source_event(prior_active=True)
    assert _price_window(event=repeated).outcome_refusal == "prior_active_chain"
    assert _price_window(event=repeated, population="unfiltered").outcome_refusal is None
    assert _price_window(event=_13g_source(), population="13g").outcome_refusal is None
    assert (
        _price_window(event=_13g_source(raw_document_count=0), population="13g").outcome_refusal
        == "canonical_raw_document_missing_or_ambiguous"
    )


def test_accepted_window_has_one_return_and_matching_conversion_path() -> None:
    window = _price_window()
    assert accepted_window_return_pct(window) == Decimal("9.500")
    features = window_match_features(window)
    assert features.entry_price == Decimal("10")
    assert features.rule is None
    outcome = treatment_event_outcome(window, sector="technology")
    assert outcome.net_return_pct == pytest.approx(9.5)
    assert outcome.maximum_percent_of_class == 7.5
    assert outcome.sector == "technology"


def test_return_and_primary_conversion_refuse_wrong_population() -> None:
    with pytest.raises(ValueError, match="price window refused"):
        accepted_window_return_pct(_price_window(stock_bars_present=69))
    challenger = _price_window(event=_13g_source(), population="13g")
    assert window_match_features(challenger).rule == "1b"
    with pytest.raises(ValueError, match="clean primary"):
        treatment_event_outcome(challenger)
    with pytest.raises(ValueError, match="clean primary"):
        treatment_event_outcome(_price_window(population="random"))
