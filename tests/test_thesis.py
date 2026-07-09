"""
Unit tests for the thesis engine service.

No network calls, no database, no live LLM API.
All external dependencies are stubbed or mocked.

Coverage:
  - stale detection: no thesis, missing/unknown frequency, in-window, past threshold
  - writer output validation: valid, missing fields, bad thesis_type, bad stance, out-of-range score
  - critic output validation: valid, missing fields, bad verdict
  - writer/critic retry-once + finish_reason propagation (#1919)
  - generate_thesis wiring: correct DB writes, version from DB, critic-fail isolation,
    last_reviewed_at update, thesis_runs recording
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.llm_client import LLMCompletion
from app.services.thesis import (
    ThesisResult,
    _call_critic,
    _call_writer,
    _shape_risk_metrics,
    _to_float,
    _validate_critic_output,
    _validate_writer_output,
    find_stale_instruments,
    generate_thesis,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 4, 12, 0, 0, tzinfo=UTC)

_VALID_WRITER = {
    "thesis_type": "compounder",
    "confidence_score": 0.75,
    "stance": "buy",
    "buy_zone_low": 150.0,
    "buy_zone_high": 170.0,
    "base_value": 200.0,
    "bull_value": 250.0,
    "bear_value": 120.0,
    "break_conditions": ["Revenue growth falls below 10% for two consecutive quarters"],
    "memo_markdown": "## AAPL\n\nStrong compounder with durable moat.\n\n### Valuation\n\nTrading at fair value.",
}

_VALID_CRITIC = {
    "summary": "Valuation leaves no margin of safety given macro headwinds.",
    "key_risks": ["Multiple compression", "China revenue exposure"],
    "hidden_assumptions": ["Services growth continues at 15%+"],
    "evidence_gaps": ["No recent guidance on Vision Pro adoption"],
    "thesis_breakers": ["Apple loses App Store court ruling", "China ban on iPhone"],
    "verdict": "Moderate challenge",
}


def _make_conn(
    *,
    stale_rows: list[tuple] | None = None,
    insert_returns_version: int = 1,
    insert_returns_thesis_id: int = 901,
    inst_row: tuple | None = None,
) -> MagicMock:
    """
    Build a minimal psycopg connection mock.

    stale_rows:               rows returned by find_stale_instruments query
    insert_returns_version:   the thesis_version returned by the INSERT ... RETURNING
    insert_returns_thesis_id: the thesis_id returned by the INSERT ... RETURNING
    inst_row:                 row returned by the instruments SELECT

    Every executed statement (whitespace-normalised, lowercase) is
    appended to ``conn.sql_log`` so tests can assert on the write
    sequence (thesis_runs recording, coverage update).
    """
    conn = MagicMock()
    sql_log: list[str] = []
    conn.sql_log = sql_log

    def execute_side_effect(sql, params: dict | None = None):  # type: ignore[no-untyped-def]
        # sql may be a str OR a psycopg.sql.SQL/Composed object.
        # Normalise via str() so substring-based branch matching still
        # works regardless of which shape the service uses.
        cursor = MagicMock()
        sql_str = sql if isinstance(sql, str) else str(sql)
        sql_strip = " ".join(sql_str.split()).lower()
        sql_log.append(sql_strip)

        if "insert into thesis_runs" in sql_strip:
            cursor.fetchone.return_value = (77,)
        elif sql_strip.startswith("update thesis_runs"):
            cursor.rowcount = 1
        elif "insert into theses" in sql_strip:
            # Branch priority note: this check must come before any branch that
            # matches "from theses", because the atomic INSERT contains a scalar
            # subquery "FROM theses WHERE instrument_id = ..." — that substring
            # would also match the prior-thesis SELECT branch below if checked
            # first. Correct today; document the assumption explicitly.
            #
            # The mock bypasses real SQL entirely: it always returns the
            # configured (thesis_id, version) regardless of prior thesis state.
            # The correctness of the scalar subquery on the first-thesis
            # (no-prior-rows) path requires integration tests against a real DB.
            cursor.fetchone.return_value = (insert_returns_thesis_id, insert_returns_version)
        elif "max(t.created_at)" in sql_strip:
            cursor.fetchall.return_value = stale_rows or []
        elif sql_strip.startswith("update coverage"):
            cursor.fetchone.return_value = None
        elif "from instruments" in sql_strip and "symbol" in sql_strip:
            default = ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics", "US", "USD")
            cursor.fetchone.return_value = inst_row or default
        elif "from fundamentals_snapshot" in sql_strip:
            cursor.fetchall.return_value = []
        elif "from filing_events" in sql_strip:
            cursor.fetchall.return_value = []
        elif "from news_events" in sql_strip:
            cursor.fetchall.return_value = []
        elif "from theses" in sql_strip and "order by thesis_version desc" in sql_strip:
            cursor.fetchone.return_value = None
        else:
            cursor.fetchall.return_value = []
            cursor.fetchone.return_value = None
        return cursor

    conn.execute.side_effect = execute_side_effect
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


# ---------------------------------------------------------------------------
# Fake LLM client (#1919 — replaces the pre-provider Anthropic mock)
# ---------------------------------------------------------------------------


class _FakeLLMClient:
    """Scripted LLMClient: pops one completion (or raises one exception)
    per complete() call, recording every call's kwargs."""

    provider_name = "openai_compatible"
    model = "test-model"

    def __init__(self, completions: list[LLMCompletion | Exception]) -> None:
        self._completions = list(completions)
        self.calls: list[dict[str, object]] = []

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion:
        self.calls.append({"system": system, "user": user, "max_tokens": max_tokens})
        if not self._completions:
            raise AssertionError("FakeLLMClient exhausted — test scripted too few completions")
        item = self._completions.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _completion(payload: dict | str, finish_reason: str = "stop") -> LLMCompletion:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return LLMCompletion(text=text, finish_reason=finish_reason, model="test-model-resolved")


# ---------------------------------------------------------------------------
# _to_float
# ---------------------------------------------------------------------------


class TestToFloat:
    def test_none_returns_none(self) -> None:
        assert _to_float(None) is None

    def test_int_converts(self) -> None:
        assert _to_float(42) == 42.0

    def test_string_float_converts(self) -> None:
        assert _to_float("3.14") == pytest.approx(3.14)

    def test_invalid_string_returns_none(self) -> None:
        assert _to_float("not-a-number") is None

    def test_zero_converts(self) -> None:
        assert _to_float(0) == 0.0


# ---------------------------------------------------------------------------
# Stale detection
# ---------------------------------------------------------------------------


class TestFindStaleInstruments:
    def test_no_thesis_is_stale(self) -> None:
        # (instrument_id, symbol, review_frequency, latest_thesis_at,
        #  latest_event_filing_date, latest_event_filing_type)
        rows = [(1, "AAPL", "weekly", None, None, None)]
        conn = _make_conn(stale_rows=rows)
        result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "no_thesis"
        assert result[0].symbol == "AAPL"

    def test_unknown_frequency_is_stale(self) -> None:
        rows = [(1, "AAPL", "biannual", _NOW - timedelta(days=1), None, None)]
        conn = _make_conn(stale_rows=rows)
        result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "missing_frequency"

    def test_null_frequency_is_stale(self) -> None:
        rows = [(1, "AAPL", None, _NOW - timedelta(days=1), None, None)]
        conn = _make_conn(stale_rows=rows)
        result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "missing_frequency"

    def test_past_weekly_threshold_is_stale(self) -> None:
        # thesis created 8 days ago, weekly frequency → stale
        rows = [(1, "AAPL", "weekly", _NOW - timedelta(days=8), None, None)]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis._utcnow", return_value=_NOW):
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "stale"

    def test_within_weekly_threshold_is_fresh(self) -> None:
        # thesis created 3 days ago, weekly frequency → fresh
        rows = [(1, "AAPL", "weekly", _NOW - timedelta(days=3), None, None)]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis._utcnow", return_value=_NOW):
            result = find_stale_instruments(conn, tier=1)
        assert result == []

    def test_daily_threshold(self) -> None:
        # thesis created 25 hours ago, daily frequency → stale
        rows = [(1, "MSFT", "daily", _NOW - timedelta(hours=25), None, None)]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis._utcnow", return_value=_NOW):
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].symbol == "MSFT"

    def test_monthly_threshold(self) -> None:
        # thesis created 31 days ago, monthly frequency → stale
        rows = [(1, "TSLA", "monthly", _NOW - timedelta(days=31), None, None)]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis._utcnow", return_value=_NOW):
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1

    def test_multiple_instruments_mixed(self) -> None:
        rows = [
            (1, "AAPL", "weekly", _NOW - timedelta(days=3), None, None),  # fresh
            (2, "MSFT", "weekly", _NOW - timedelta(days=8), None, None),  # stale
            (3, "GOOG", "weekly", None, None, None),  # no thesis
        ]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis._utcnow", return_value=_NOW):
            result = find_stale_instruments(conn, tier=1)
        symbols = {r.symbol for r in result}
        assert "AAPL" not in symbols
        assert "MSFT" in symbols
        assert "GOOG" in symbols

    def test_exactly_at_threshold_is_stale(self) -> None:
        # now == created_at + 7 days exactly → stale (>= boundary)
        rows = [(1, "AAPL", "weekly", _NOW - timedelta(days=7), None, None)]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis._utcnow", return_value=_NOW):
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "stale"


# ---------------------------------------------------------------------------
# Writer output validation
# ---------------------------------------------------------------------------


class TestValidateWriterOutput:
    def test_valid_passes(self) -> None:
        _validate_writer_output(_VALID_WRITER)  # should not raise

    def test_missing_field_raises(self) -> None:
        bad = {k: v for k, v in _VALID_WRITER.items() if k != "stance"}
        with pytest.raises(ValueError, match="missing fields"):
            _validate_writer_output(bad)

    def test_invalid_thesis_type_raises(self) -> None:
        bad = {**_VALID_WRITER, "thesis_type": "moonshot"}
        with pytest.raises(ValueError, match="invalid thesis_type"):
            _validate_writer_output(bad)

    def test_invalid_stance_raises(self) -> None:
        bad = {**_VALID_WRITER, "stance": "sell"}
        with pytest.raises(ValueError, match="invalid stance"):
            _validate_writer_output(bad)

    def test_confidence_out_of_range_raises(self) -> None:
        bad = {**_VALID_WRITER, "confidence_score": 1.5}
        with pytest.raises(ValueError, match="out of range"):
            _validate_writer_output(bad)

    def test_break_conditions_not_list_raises(self) -> None:
        bad = {**_VALID_WRITER, "break_conditions": "just a string"}
        with pytest.raises(ValueError, match="list"):
            _validate_writer_output(bad)

    def test_empty_memo_raises(self) -> None:
        bad = {**_VALID_WRITER, "memo_markdown": "   "}
        with pytest.raises(ValueError, match="non-empty"):
            _validate_writer_output(bad)

    @pytest.mark.parametrize("tt", ["compounder", "value", "turnaround", "speculative"])
    def test_all_valid_thesis_types_pass(self, tt: str) -> None:
        _validate_writer_output({**_VALID_WRITER, "thesis_type": tt})

    @pytest.mark.parametrize("st", ["buy", "hold", "watch", "avoid"])
    def test_all_valid_stances_pass(self, st: str) -> None:
        _validate_writer_output({**_VALID_WRITER, "stance": st})


# ---------------------------------------------------------------------------
# Critic output validation
# ---------------------------------------------------------------------------


class TestValidateCriticOutput:
    def test_valid_passes(self) -> None:
        _validate_critic_output(_VALID_CRITIC)  # should not raise

    def test_missing_field_raises(self) -> None:
        bad = {k: v for k, v in _VALID_CRITIC.items() if k != "verdict"}
        with pytest.raises(ValueError, match="missing fields"):
            _validate_critic_output(bad)

    def test_invalid_verdict_raises(self) -> None:
        bad = {**_VALID_CRITIC, "verdict": "No challenge"}
        with pytest.raises(ValueError, match="invalid verdict"):
            _validate_critic_output(bad)

    @pytest.mark.parametrize("v", ["Strong challenge", "Moderate challenge", "Weak challenge"])
    def test_all_valid_verdicts_pass(self, v: str) -> None:
        _validate_critic_output({**_VALID_CRITIC, "verdict": v})


# ---------------------------------------------------------------------------
# _call_writer / _call_critic with the fake LLM client
# ---------------------------------------------------------------------------


class TestCallWriter:
    def test_returns_parsed_dict_on_valid_response(self) -> None:
        client = _FakeLLMClient([_completion(_VALID_WRITER)])
        result, completion = _call_writer(client, context={})
        assert result["thesis_type"] == "compounder"
        assert result["stance"] == "buy"
        assert completion.model == "test-model-resolved"
        assert len(client.calls) == 1

    def test_raises_on_invalid_json_after_retry(self) -> None:
        # Retry-once (#1919): BOTH attempts must fail before the raise.
        client = _FakeLLMClient(
            [_completion("not json {", finish_reason="stop"), _completion("still not json {", finish_reason="stop")]
        )
        with pytest.raises(ValueError, match="unparseable JSON"):
            _call_writer(client, context={})
        assert len(client.calls) == 2

    def test_retry_recovers_from_first_bad_attempt(self) -> None:
        client = _FakeLLMClient([_completion("garbage"), _completion(_VALID_WRITER)])
        result, _ = _call_writer(client, context={})
        assert result["stance"] == "buy"
        assert len(client.calls) == 2

    def test_raises_on_schema_violation_after_retry(self) -> None:
        bad = {**_VALID_WRITER, "stance": "liquidate"}
        client = _FakeLLMClient([_completion(bad), _completion(bad)])
        with pytest.raises(ValueError, match="invalid stance"):
            _call_writer(client, context={})
        assert len(client.calls) == 2

    def test_error_carries_finish_reason(self) -> None:
        # A truncated response must be distinguishable from a malformed
        # one — the ValueError text carries finish_reason for
        # thesis_runs.error (#1919).
        client = _FakeLLMClient(
            [_completion('{"trunc', finish_reason="length"), _completion('{"trunc', finish_reason="length")]
        )
        with pytest.raises(ValueError, match="finish_reason=length"):
            _call_writer(client, context={})


class TestCallCritic:
    def test_returns_parsed_dict_on_valid_response(self) -> None:
        client = _FakeLLMClient([_completion(_VALID_CRITIC)])
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result["verdict"] == "Moderate challenge"
        assert "key_risks" in result

    def test_returns_empty_dict_on_json_error(self) -> None:
        client = _FakeLLMClient([_completion("not json {"), _completion("not json {")])
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result == {}
        assert len(client.calls) == 2  # retried once before giving up

    def test_retry_recovers_from_first_bad_attempt(self) -> None:
        client = _FakeLLMClient([_completion("garbage"), _completion(_VALID_CRITIC)])
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result["verdict"] == "Moderate challenge"

    def test_returns_empty_dict_on_schema_violation(self) -> None:
        bad = {**_VALID_CRITIC, "verdict": "Unknown"}
        client = _FakeLLMClient([_completion(bad), _completion(bad)])
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result == {}

    def test_returns_empty_dict_on_api_exception(self) -> None:
        client = _FakeLLMClient([RuntimeError("rate limit")])
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result == {}


# ---------------------------------------------------------------------------
# generate_thesis end-to-end (fully mocked)
# ---------------------------------------------------------------------------


def _make_two_call_client(writer_json: dict, critic_json: dict) -> _FakeLLMClient:
    """Build a fake client whose first call returns writer_json and second returns critic_json."""
    return _FakeLLMClient([_completion(writer_json), _completion(critic_json)])


class TestGenerateThesis:
    """
    All tests in this class patch `_utcnow` to a fixed value so that
    `_assemble_context`'s news cutoff calculation is deterministic.
    Without this patch, tests that inject news rows with controlled
    timestamps would fail non-deterministically when wall-clock time
    diverges from _NOW.
    """

    def setup_method(self) -> None:
        self._utcnow_patcher = patch("app.services.thesis._utcnow", return_value=_NOW)
        self._utcnow_patcher.start()

    def teardown_method(self) -> None:
        self._utcnow_patcher.stop()

    def test_returns_thesis_result_with_correct_version(self) -> None:
        # INSERT RETURNING gives version=1
        conn = _make_conn(insert_returns_version=1)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        result = generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        assert isinstance(result, ThesisResult)
        assert result.thesis_version == 1
        assert result.thesis_type == "compounder"
        assert result.stance == "buy"
        assert result.confidence_score == 0.75
        assert result.critic_json is not None
        assert result.critic_json["verdict"] == "Moderate challenge"

    def test_second_generation_increments_version(self) -> None:
        # DB returns version=3 (meaning MAX was 2)
        conn = _make_conn(insert_returns_version=3)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        result = generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        assert result.thesis_version == 3

    def test_commits_read_tx_before_llm_calls(self) -> None:
        """Regression guard for #293: the implicit read tx opened by
        _assemble_context's SELECTs must be committed BEFORE the LLM
        writer/critic calls. Otherwise the connection sits
        'idle in transaction' for the duration of each LLM round-trip
        (minutes on a local 14B), violating the CLAUDE.md 'no HTTP
        inside DB tx' invariant."""
        conn = _make_conn(insert_returns_version=1)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        call_log: list[str] = []
        original_commit = conn.commit
        original_complete = client.complete

        def tracked_commit() -> None:
            call_log.append("commit")
            return original_commit()

        def tracked_complete(**kwargs: Any) -> LLMCompletion:
            call_log.append("llm")
            return original_complete(**kwargs)

        conn.commit = tracked_commit
        client.complete = tracked_complete  # type: ignore[method-assign]

        generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        # First event must be the commit. Every LLM call must come
        # after it — guards both writer AND critic, not just the first
        # LLM call. A pre-commit LLM call would land at call_log[0].
        assert "commit" in call_log, "expected conn.commit() to be called"
        assert "llm" in call_log, "expected client.complete to be called"
        assert call_log[0] == "commit", f"commit must be the first event; got order: {call_log}"
        # Guard every LLM call, not just the first — a regression
        # that inserted _call_critic before the commit would still
        # satisfy an index-based check on the writer call alone.
        last_llm_idx = max(i for i, v in enumerate(call_log) if v == "llm")
        assert last_llm_idx > call_log.index("commit"), f"every LLM call must follow the commit; got order: {call_log}"

    def test_critic_failure_does_not_block_insert(self) -> None:
        conn = _make_conn(insert_returns_version=1)
        # First call (writer) succeeds; second call (critic) raises
        client = _FakeLLMClient([_completion(_VALID_WRITER), RuntimeError("timeout")])

        result = generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        assert result.thesis_version == 1
        assert result.critic_json is None

    def test_last_reviewed_at_updated_on_success(self) -> None:
        conn = _make_conn(insert_returns_version=1)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)
        generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        update_calls = [s for s in conn.sql_log if s.startswith("update coverage")]
        assert len(update_calls) == 1
        assert "last_reviewed_at" in update_calls[0]

    def test_records_thesis_run_ok(self) -> None:
        """#1919: every successful generation inserts a 'running'
        thesis_runs row BEFORE the LLM calls and marks it ok inside the
        thesis-insert transaction."""
        conn = _make_conn(insert_returns_version=1)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        run_inserts = [s for s in conn.sql_log if "insert into thesis_runs" in s]
        assert len(run_inserts) == 1
        ok_updates = [s for s in conn.sql_log if s.startswith("update thesis_runs") and "'ok'" in s]
        assert len(ok_updates) == 1
        # The run row must exist before the thesis row lands.
        assert conn.sql_log.index(run_inserts[0]) < conn.sql_log.index(
            next(s for s in conn.sql_log if "insert into theses" in s)
        )

    def test_writer_failure_records_failed_run_and_reraises(self) -> None:
        """#1919: a writer failure (after its retry) must mark the run
        row failed — with the error text — and re-raise."""
        conn = _make_conn(insert_returns_version=1)
        client = _FakeLLMClient([_completion("bad json {"), _completion("bad json {", finish_reason="length")])

        with pytest.raises(ValueError, match="unparseable JSON"):
            generate_thesis(instrument_id=1, conn=conn, client=client, trigger="scheduled")

        failed_updates = [s for s in conn.sql_log if s.startswith("update thesis_runs") and "'failed'" in s]
        assert len(failed_updates) == 1
        # No thesis row was inserted on the failure path.
        assert not [s for s in conn.sql_log if "insert into theses (" in s]

    def test_float_fields_consistent_between_db_and_result(self) -> None:
        """
        Verifies that _to_float is used consistently: the values inserted into
        the DB and the values in ThesisResult are derived from the same function.
        Catches any divergence if the two sites had different conversion logic.
        """
        conn = _make_conn(insert_returns_version=1)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        result = generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        assert result.buy_zone_low == _to_float(_VALID_WRITER["buy_zone_low"])
        assert result.buy_zone_high == _to_float(_VALID_WRITER["buy_zone_high"])
        assert result.base_value == _to_float(_VALID_WRITER["base_value"])
        assert result.bull_value == _to_float(_VALID_WRITER["bull_value"])
        assert result.bear_value == _to_float(_VALID_WRITER["bear_value"])

    def test_null_optional_fields_returned_as_none(self) -> None:
        writer_no_targets = {
            **_VALID_WRITER,
            "stance": "watch",
            "buy_zone_low": None,
            "buy_zone_high": None,
            "base_value": None,
            "bull_value": None,
            "bear_value": None,
        }
        conn = _make_conn(insert_returns_version=1)
        client = _make_two_call_client(writer_no_targets, _VALID_CRITIC)

        result = generate_thesis(instrument_id=1, conn=conn, client=client, trigger="manual")

        assert result.buy_zone_low is None
        assert result.buy_zone_high is None
        assert result.base_value is None
        assert result.bull_value is None
        assert result.bear_value is None


# ---------------------------------------------------------------------------
# Risk-metrics context block (#1632) — pure row-shaping
# ---------------------------------------------------------------------------

# Column order mirrors the SELECT in _assemble_context (20 cols).
#   0 window_key, 1 as_of_date, 2 benchmark_symbol,
#   3 cagr, 4 excess_cagr_vs_spy, 5 vol_annualized, 6 beta, 7 beta_r2, 8 calmar,
#   9 max_drawdown, 10 current_drawdown, 11 var_5, 12 worst_day,
#   13 cagr_status, 14 excess_cagr_status, 15 vol_status, 16 beta_status,
#   17 drawdown_status, 18 distribution_status, 19 calmar_status


def _risk_row(
    window_key: str = "1y",
    as_of: date | None = date(2026, 6, 12),
    *,
    cagr: object = 0.46,
    beta: object = 0.81,
    max_dd: object = -0.14,
    var_5: object = -0.02,
    cagr_status: str | None = "ok",
    beta_status: str | None = "ok",
) -> tuple[object, ...]:
    return (
        window_key,
        as_of,
        "SPY",
        cagr,
        0.23,
        0.24,
        beta,
        0.19,
        3.36,
        max_dd,
        -0.075,
        var_5,
        -0.05,
        cagr_status,
        "ok",
        "ok",
        beta_status,
        "ok",
        "partial_window",
        "ok",
    )


def _windows(out: dict[str, object] | None) -> list[Any]:
    """Narrow the object-typed `windows` list for indexable test access."""
    assert out is not None
    windows = out["windows"]
    assert isinstance(windows, list)
    return windows


class TestShapeRiskMetrics:
    def test_no_rows_returns_none(self) -> None:
        # Never-computed instrument — None, not an empty block, no fabricated data.
        assert _shape_risk_metrics([], "risk_v1") is None

    def test_shapes_windows_with_version_and_per_window_as_of(self) -> None:
        out = _shape_risk_metrics(
            [_risk_row("1y", date(2026, 6, 12)), _risk_row("3y", date(2026, 6, 11))],
            "risk_v1",
        )
        assert out is not None
        assert out["metric_version"] == "risk_v1"
        assert "fractions" in str(out["basis_note"])
        windows = _windows(out)
        assert len(windows) == 2
        # as_of_date rides each window (no shared-date assumption).
        assert windows[0]["as_of_date"] == "2026-06-12"
        assert windows[1]["as_of_date"] == "2026-06-11"
        assert windows[0]["benchmark_symbol"] == "SPY"
        assert windows[0]["beta"] == pytest.approx(0.81)

    def test_signed_losses_preserved(self) -> None:
        out = _shape_risk_metrics([_risk_row(max_dd=-0.52, var_5=-0.07)], "risk_v1")
        w = _windows(out)[0]
        # Losses stay negative — never abs()'d or flipped.
        assert w["max_drawdown"] == pytest.approx(-0.52)
        assert w["var_5"] == pytest.approx(-0.07)

    def test_null_scalar_stays_none_not_zero(self) -> None:
        # Thin-history: cagr NULL + flagged status. NULL must NOT become 0.
        out = _shape_risk_metrics([_risk_row(cagr=None, cagr_status="partial_window")], "risk_v1")
        w = _windows(out)[0]
        assert w["cagr"] is None
        assert w["cagr_status"] == "partial_window"

    def test_flagged_status_passthrough(self) -> None:
        # benchmark_missing beta passes through verbatim (absent, not zero).
        out = _shape_risk_metrics([_risk_row(beta=None, beta_status="benchmark_missing")], "risk_v1")
        w = _windows(out)[0]
        assert w["beta"] is None
        assert w["beta_status"] == "benchmark_missing"

    def test_as_of_none_tolerated(self) -> None:
        out = _shape_risk_metrics([_risk_row(as_of=None)], "risk_v1")
        assert _windows(out)[0]["as_of_date"] is None
