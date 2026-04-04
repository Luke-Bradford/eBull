"""
Unit tests for the thesis engine service.

No network calls, no database, no live Claude API.
All external dependencies are stubbed or mocked.

Coverage:
  - stale detection: no thesis, missing/unknown frequency, in-window, past threshold
  - thesis_version increment: first thesis → 1, second → 2
  - writer output validation: valid, missing fields, bad thesis_type, bad stance, out-of-range score
  - critic output validation: valid, missing fields, bad verdict
  - generate_thesis wiring: correct DB writes, version increment, last_reviewed_at update
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from app.services.thesis import (
    ThesisResult,
    _call_critic,
    _call_writer,
    _next_thesis_version,
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
    max_version: int = 0,
    inst_row: tuple | None = None,
) -> MagicMock:
    """
    Build a minimal psycopg connection mock.

    stale_rows: rows returned by the find_stale_instruments query
    max_version: value returned by the MAX(thesis_version) query
    inst_row: row returned by the instruments SELECT
    """
    conn = MagicMock()

    def execute_side_effect(sql: str, params: dict | None = None):
        cursor = MagicMock()
        sql_strip = " ".join(sql.split()).lower()

        if "coalesce(max(thesis_version)" in sql_strip:
            cursor.fetchone.return_value = (max_version,)
        elif "max(t.created_at)" in sql_strip:
            cursor.fetchall.return_value = stale_rows or []
        elif "from instruments" in sql_strip and "instrument_id" in sql_strip and "symbol" in sql_strip:
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
# Stale detection
# ---------------------------------------------------------------------------


class TestFindStaleInstruments:
    def test_no_thesis_is_stale(self) -> None:
        rows = [(1, "AAPL", "weekly", None)]
        conn = _make_conn(stale_rows=rows)
        result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "no_thesis"
        assert result[0].symbol == "AAPL"

    def test_unknown_frequency_is_stale(self) -> None:
        rows = [(1, "AAPL", "biannual", _NOW - timedelta(days=1))]
        conn = _make_conn(stale_rows=rows)
        result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "missing_frequency"

    def test_null_frequency_is_stale(self) -> None:
        rows = [(1, "AAPL", None, _NOW - timedelta(days=1))]
        conn = _make_conn(stale_rows=rows)
        result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "missing_frequency"

    def test_past_weekly_threshold_is_stale(self) -> None:
        # thesis created 8 days ago, weekly frequency → stale
        rows = [(1, "AAPL", "weekly", _NOW - timedelta(days=8))]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis.datetime") as mock_dt:
            mock_dt.now.return_value = _NOW
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].reason == "stale"

    def test_within_weekly_threshold_is_fresh(self) -> None:
        # thesis created 3 days ago, weekly frequency → fresh
        rows = [(1, "AAPL", "weekly", _NOW - timedelta(days=3))]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis.datetime") as mock_dt:
            mock_dt.now.return_value = _NOW
            result = find_stale_instruments(conn, tier=1)
        assert result == []

    def test_daily_threshold(self) -> None:
        # thesis created 25 hours ago, daily frequency → stale
        rows = [(1, "MSFT", "daily", _NOW - timedelta(hours=25))]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis.datetime") as mock_dt:
            mock_dt.now.return_value = _NOW
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1
        assert result[0].symbol == "MSFT"

    def test_monthly_threshold(self) -> None:
        # thesis created 31 days ago, monthly frequency → stale
        rows = [(1, "TSLA", "monthly", _NOW - timedelta(days=31))]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis.datetime") as mock_dt:
            mock_dt.now.return_value = _NOW
            result = find_stale_instruments(conn, tier=1)
        assert len(result) == 1

    def test_multiple_instruments_mixed(self) -> None:
        rows = [
            (1, "AAPL", "weekly", _NOW - timedelta(days=3)),  # fresh
            (2, "MSFT", "weekly", _NOW - timedelta(days=8)),  # stale
            (3, "GOOG", "weekly", None),  # no thesis
        ]
        conn = _make_conn(stale_rows=rows)
        with patch("app.services.thesis.datetime") as mock_dt:
            mock_dt.now.return_value = _NOW
            result = find_stale_instruments(conn, tier=1)
        symbols = {r.symbol for r in result}
        assert "AAPL" not in symbols
        assert "MSFT" in symbols
        assert "GOOG" in symbols


# ---------------------------------------------------------------------------
# Version increment
# ---------------------------------------------------------------------------


class TestNextThesisVersion:
    def test_first_thesis_is_version_1(self) -> None:
        conn = _make_conn(max_version=0)
        assert _next_thesis_version(conn, instrument_id=1) == 1

    def test_increments_existing_version(self) -> None:
        conn = _make_conn(max_version=3)
        assert _next_thesis_version(conn, instrument_id=1) == 4


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
# _call_writer / _call_critic with mocked Anthropic client
# ---------------------------------------------------------------------------


def _make_anthropic_client(response_json: dict) -> MagicMock:
    """Build a minimal anthropic.Anthropic mock that returns response_json as text."""
    import anthropic

    client = MagicMock(spec=anthropic.Anthropic)
    block = MagicMock()
    block.text = json.dumps(response_json)
    msg = MagicMock()
    msg.content = [block]
    client.messages.create.return_value = msg
    return client


class TestCallWriter:
    def test_returns_parsed_dict_on_valid_response(self) -> None:
        client = _make_anthropic_client(_VALID_WRITER)
        result = _call_writer(client, context={})
        assert result["thesis_type"] == "compounder"
        assert result["stance"] == "buy"

    def test_raises_on_invalid_json(self) -> None:
        import anthropic

        client = MagicMock(spec=anthropic.Anthropic)
        block = MagicMock()
        block.text = "not json {"
        msg = MagicMock()
        msg.content = [block]
        client.messages.create.return_value = msg
        with pytest.raises(ValueError, match="unparseable JSON"):
            _call_writer(client, context={})

    def test_raises_on_schema_violation(self) -> None:
        bad = {**_VALID_WRITER, "stance": "liquidate"}
        client = _make_anthropic_client(bad)
        with pytest.raises(ValueError, match="invalid stance"):
            _call_writer(client, context={})


class TestCallCritic:
    def test_returns_parsed_dict_on_valid_response(self) -> None:
        client = _make_anthropic_client(_VALID_CRITIC)
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result["verdict"] == "Moderate challenge"
        assert "key_risks" in result

    def test_returns_empty_dict_on_json_error(self) -> None:
        import anthropic

        client = MagicMock(spec=anthropic.Anthropic)
        block = MagicMock()
        block.text = "not json {"
        msg = MagicMock()
        msg.content = [block]
        client.messages.create.return_value = msg
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result == {}

    def test_returns_empty_dict_on_schema_violation(self) -> None:
        bad = {**_VALID_CRITIC, "verdict": "Unknown"}
        client = _make_anthropic_client(bad)
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result == {}

    def test_returns_empty_dict_on_api_exception(self) -> None:
        import anthropic

        client = MagicMock(spec=anthropic.Anthropic)
        client.messages.create.side_effect = Exception("rate limit")
        result = _call_critic(client, memo_markdown="## memo", context={})
        assert result == {}


# ---------------------------------------------------------------------------
# generate_thesis end-to-end (fully mocked)
# ---------------------------------------------------------------------------


def _make_two_call_client(writer_json: dict, critic_json: dict) -> MagicMock:
    """Build a mock client whose first call returns writer_json and second returns critic_json."""
    import anthropic

    client = MagicMock(spec=anthropic.Anthropic)
    writer_block = MagicMock()
    writer_block.text = json.dumps(writer_json)
    critic_block = MagicMock()
    critic_block.text = json.dumps(critic_json)
    writer_msg = MagicMock()
    writer_msg.content = [writer_block]
    critic_msg = MagicMock()
    critic_msg.content = [critic_block]
    client.messages.create.side_effect = [writer_msg, critic_msg]
    return client


class TestGenerateThesis:
    def _make_full_conn(self, max_version: int = 0) -> MagicMock:
        """
        Connection mock that also handles INSERT and UPDATE statements
        used by generate_thesis.
        """
        conn = _make_conn(max_version=max_version)
        original_side_effect = conn.execute.side_effect

        def extended_side_effect(sql: str, params: dict | None = None):
            sql_strip = " ".join(sql.split()).lower()
            if sql_strip.startswith("insert into theses"):
                return MagicMock()
            if sql_strip.startswith("update coverage"):
                return MagicMock()
            return original_side_effect(sql, params)

        conn.execute.side_effect = extended_side_effect
        return conn

    def test_returns_thesis_result_with_correct_version(self) -> None:
        conn = self._make_full_conn(max_version=0)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        result = generate_thesis(instrument_id=1, conn=conn, client=client)

        assert isinstance(result, ThesisResult)
        assert result.thesis_version == 1
        assert result.thesis_type == "compounder"
        assert result.stance == "buy"
        assert result.confidence_score == 0.75
        assert result.critic_json is not None
        assert result.critic_json["verdict"] == "Moderate challenge"

    def test_second_generation_increments_version(self) -> None:
        conn = self._make_full_conn(max_version=2)
        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)

        result = generate_thesis(instrument_id=1, conn=conn, client=client)

        assert result.thesis_version == 3

    def test_critic_failure_does_not_block_insert(self) -> None:
        import anthropic

        conn = self._make_full_conn(max_version=0)
        client = MagicMock(spec=anthropic.Anthropic)
        writer_block = MagicMock()
        writer_block.text = json.dumps(_VALID_WRITER)
        writer_msg = MagicMock()
        writer_msg.content = [writer_block]
        # First call (writer) succeeds; second call (critic) raises
        client.messages.create.side_effect = [writer_msg, Exception("timeout")]

        result = generate_thesis(instrument_id=1, conn=conn, client=client)

        assert result.thesis_version == 1
        assert result.critic_json is None

    def test_last_reviewed_at_updated_on_success(self) -> None:
        conn = self._make_full_conn(max_version=0)
        update_calls: list[str] = []

        original_side_effect = conn.execute.side_effect

        def tracking_side_effect(sql: str, params: dict | None = None):
            if "update coverage" in " ".join(sql.split()).lower():
                update_calls.append(sql)
                return MagicMock()
            return original_side_effect(sql, params)

        conn.execute.side_effect = tracking_side_effect

        client = _make_two_call_client(_VALID_WRITER, _VALID_CRITIC)
        generate_thesis(instrument_id=1, conn=conn, client=client)

        assert len(update_calls) == 1
        assert "last_reviewed_at" in update_calls[0].lower()
