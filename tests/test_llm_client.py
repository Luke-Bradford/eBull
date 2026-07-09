"""Unit tests for the thesis-scoped LLM provider layer (#1919).

No network, no DB: respx intercepts httpx for the OpenAI-compatible
provider; the Anthropic provider wraps a MagicMock SDK client;
make_llm_client resolves against a mocked runtime_config connection.
"""

from __future__ import annotations

import json
import threading
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from app.services.llm_client import (
    AnthropicProvider,
    LLMProviderNotConfigured,
    OpenAICompatProvider,
    make_llm_client,
    normalize_completion_text,
    strip_code_fence,
    strip_think_block,
)

_BASE_URL = "http://localhost:11434/v1"


def _chat_response(
    content: str,
    *,
    finish_reason: str = "stop",
    model: str = "qwen3:14b",
) -> dict[str, Any]:
    return {
        "model": model,
        "choices": [{"message": {"role": "assistant", "content": content}, "finish_reason": finish_reason}],
    }


# ---------------------------------------------------------------------------
# strip_think_block
# ---------------------------------------------------------------------------


class TestStripThinkBlock:
    def test_plain_text_unchanged(self) -> None:
        assert strip_think_block('{"a": 1}') == '{"a": 1}'

    def test_leading_think_block_removed(self) -> None:
        assert strip_think_block('<think>reasoning\nlines</think>\n{"a": 1}') == '{"a": 1}'

    def test_whitespace_before_think_tolerated(self) -> None:
        assert strip_think_block('  \n<think>x</think> {"a": 1}') == '{"a": 1}'

    def test_unclosed_think_block_not_stripped(self) -> None:
        # Truncated at max_tokens mid-think: no closing tag → text stays
        # as-is; the downstream JSON parse fails and finish_reason='length'
        # tells the operator it was truncation, not malformed output.
        text = "<think>never ends..."
        assert strip_think_block(text) == text

    def test_only_first_leading_block_stripped(self) -> None:
        # A <think> string INSIDE the JSON payload must survive.
        text = '<think>x</think>{"memo": "<think>quoted</think>"}'
        assert strip_think_block(text) == '{"memo": "<think>quoted</think>"}'


class TestStripCodeFence:
    def test_plain_json_unchanged(self) -> None:
        assert strip_code_fence('{"a": 1}') == '{"a": 1}'

    def test_json_fence_unwrapped(self) -> None:
        assert strip_code_fence('```json\n{"a": 1}\n```') == '{"a": 1}'

    def test_bare_fence_unwrapped(self) -> None:
        assert strip_code_fence('```\n{"a": 1}\n```') == '{"a": 1}'

    def test_unclosed_fence_not_stripped(self) -> None:
        # Truncated at max_tokens mid-payload: parse failure +
        # finish_reason='length' must stay honest (same contract as the
        # unclosed <think> case).
        text = '```json\n{"a": 1}'
        assert strip_code_fence(text) == text

    def test_fence_not_wrapping_whole_text_not_stripped(self) -> None:
        text = "prose then ```json\n{}\n```"
        assert strip_code_fence(text) == text

    def test_backticks_inside_json_string_survive(self) -> None:
        # \Z anchor forces the match to the LAST fence, so a fenced code
        # block INSIDE memo_markdown survives the unwrap.
        text = '```json\n{"memo": "use ``` for code"}\n```'
        assert strip_code_fence(text) == '{"memo": "use ``` for code"}'


class TestNormalizeCompletionText:
    def test_think_then_fence_both_stripped(self) -> None:
        # deepseek-r1 shape (#1919 PR-C tilt-check): think block first,
        # then a fenced JSON object.
        text = '<think>reasoning</think>\n```json\n{"a": 1}\n```'
        assert normalize_completion_text(text) == '{"a": 1}'

    def test_plain_json_passthrough(self) -> None:
        assert normalize_completion_text('{"a": 1}') == '{"a": 1}'


# ---------------------------------------------------------------------------
# OpenAICompatProvider
# ---------------------------------------------------------------------------


class TestOpenAICompatProvider:
    @respx.mock
    def test_happy_path_parses_completion(self) -> None:
        route = respx.post(f"{_BASE_URL}/chat/completions").mock(
            return_value=httpx.Response(200, json=_chat_response('{"stance": "buy"}'))
        )
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="qwen3:14b")
        completion = provider.complete(system="sys", user="usr", max_tokens=100)

        assert completion.text == '{"stance": "buy"}'
        assert completion.finish_reason == "stop"
        assert completion.model == "qwen3:14b"

        request = route.calls.last.request
        payload = json.loads(request.content)
        # Empirical qwen3 requirement (#1919): /no_think appended to the
        # system prompt + json_object response_format.
        assert payload["messages"][0]["content"].endswith("/no_think")
        assert payload["response_format"] == {"type": "json_object"}
        assert payload["max_tokens"] == 100
        assert "Authorization" not in request.headers  # no key → no header

    @respx.mock
    def test_bearer_header_sent_when_key_set(self) -> None:
        route = respx.post(f"{_BASE_URL}/chat/completions").mock(
            return_value=httpx.Response(200, json=_chat_response("{}"))
        )
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="m", api_key="sk-test")
        provider.complete(system="s", user="u", max_tokens=10)
        assert route.calls.last.request.headers["Authorization"] == "Bearer sk-test"

    @respx.mock
    def test_length_finish_reason_passes_through(self) -> None:
        respx.post(f"{_BASE_URL}/chat/completions").mock(
            return_value=httpx.Response(200, json=_chat_response('{"trunc', finish_reason="length"))
        )
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="m")
        completion = provider.complete(system="s", user="u", max_tokens=10)
        assert completion.finish_reason == "length"

    @respx.mock
    def test_think_block_stripped_from_text(self) -> None:
        respx.post(f"{_BASE_URL}/chat/completions").mock(
            return_value=httpx.Response(200, json=_chat_response('<think>hmm</think>{"a": 1}'))
        )
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="m")
        assert provider.complete(system="s", user="u", max_tokens=10).text == '{"a": 1}'

    @respx.mock
    def test_http_error_propagates(self) -> None:
        respx.post(f"{_BASE_URL}/chat/completions").mock(return_value=httpx.Response(500, text="boom"))
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="m")
        with pytest.raises(httpx.HTTPStatusError):
            provider.complete(system="s", user="u", max_tokens=10)

    @respx.mock
    def test_no_choices_raises_value_error(self) -> None:
        respx.post(f"{_BASE_URL}/chat/completions").mock(
            return_value=httpx.Response(200, json={"model": "m", "choices": []})
        )
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="m")
        with pytest.raises(ValueError, match="no choices"):
            provider.complete(system="s", user="u", max_tokens=10)

    @respx.mock
    def test_trailing_slash_base_url_normalised(self) -> None:
        route = respx.post(f"{_BASE_URL}/chat/completions").mock(
            return_value=httpx.Response(200, json=_chat_response("{}"))
        )
        provider = OpenAICompatProvider(base_url=_BASE_URL + "/", model="m")
        provider.complete(system="s", user="u", max_tokens=10)
        assert route.called


# ---------------------------------------------------------------------------
# AnthropicProvider
# ---------------------------------------------------------------------------


def _anthropic_message(text: str | None, *, stop_reason: str = "end_turn", model: str = "claude-sonnet-4-6"):
    if text is None:
        block = MagicMock(spec=[])  # no .text attribute
    else:
        block = MagicMock(spec=["text"])
        block.text = text
    msg = MagicMock()
    msg.content = [block]
    msg.stop_reason = stop_reason
    msg.model = model
    return msg


class TestAnthropicProvider:
    def test_happy_path_maps_stop_reason(self) -> None:
        sdk = MagicMock()
        sdk.messages.create.return_value = _anthropic_message('{"a": 1}', stop_reason="end_turn")
        provider = AnthropicProvider(sdk, model="claude-sonnet-4-6")
        completion = provider.complete(system="s", user="u", max_tokens=10)
        assert completion.text == '{"a": 1}'
        assert completion.finish_reason == "stop"
        assert completion.model == "claude-sonnet-4-6"
        assert sdk.messages.create.call_args.kwargs["model"] == "claude-sonnet-4-6"

    def test_max_tokens_maps_to_length(self) -> None:
        sdk = MagicMock()
        sdk.messages.create.return_value = _anthropic_message('{"tr', stop_reason="max_tokens")
        provider = AnthropicProvider(sdk, model="m")
        assert provider.complete(system="s", user="u", max_tokens=10).finish_reason == "length"

    def test_unknown_stop_reason_passes_through(self) -> None:
        sdk = MagicMock()
        sdk.messages.create.return_value = _anthropic_message("{}", stop_reason="tool_use")
        provider = AnthropicProvider(sdk, model="m")
        assert provider.complete(system="s", user="u", max_tokens=10).finish_reason == "tool_use"

    def test_non_text_block_raises(self) -> None:
        sdk = MagicMock()
        sdk.messages.create.return_value = _anthropic_message(None)
        provider = AnthropicProvider(sdk, model="m")
        with pytest.raises(ValueError, match="unexpected content block"):
            provider.complete(system="s", user="u", max_tokens=10)


# ---------------------------------------------------------------------------
# make_llm_client — config-driven provider resolution
# ---------------------------------------------------------------------------


def _config_conn(*, provider: str, base_url: str = _BASE_URL, model: str = "qwen3:14b") -> MagicMock:
    """Mock conn whose runtime_config SELECT returns the given knobs."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = {
        "enable_auto_trading": False,
        "enable_live_trading": False,
        "display_currency": "GBP",
        "llm_provider": provider,
        "llm_base_url": base_url,
        "llm_model": model,
        "updated_at": MagicMock(),
        "updated_by": "test",
        "reason": "test",
    }
    conn.cursor.return_value = cursor
    return conn


class TestMakeLLMClient:
    def test_openai_compatible_default_path(self) -> None:
        client = make_llm_client(_config_conn(provider="openai_compatible"))
        assert isinstance(client, OpenAICompatProvider)
        assert client.provider_name == "openai_compatible"
        assert client.model == "qwen3:14b"

    def test_anthropic_path_requires_key(self) -> None:
        conn = _config_conn(provider="anthropic", model="claude-sonnet-4-6")
        with patch("app.services.llm_client.settings") as settings_mock:
            settings_mock.anthropic_api_key = None
            with pytest.raises(LLMProviderNotConfigured):
                make_llm_client(conn)

    def test_anthropic_path_with_key(self) -> None:
        conn = _config_conn(provider="anthropic", model="claude-sonnet-4-6")
        with patch("app.services.llm_client.settings") as settings_mock:
            settings_mock.anthropic_api_key = "sk-ant-test"
            client = make_llm_client(conn)
        assert isinstance(client, AnthropicProvider)
        assert client.model == "claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# Per-process call serialisation
# ---------------------------------------------------------------------------


class TestSemaphore:
    @respx.mock
    def test_concurrent_completes_serialise(self) -> None:
        """Two threads calling complete() never overlap inside the
        provider — the per-process Semaphore(1) serialises them
        (spec §1 concurrency layer (a))."""
        in_flight = 0
        max_in_flight = 0
        lock = threading.Lock()

        def _handler(request: httpx.Request) -> httpx.Response:
            nonlocal in_flight, max_in_flight
            with lock:
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
            # No sleep needed: with Semaphore(1) the second request can't
            # even start until this returns, so overlap would only show
            # under a broken/absent semaphore with an unlucky schedule —
            # good enough as a smoke guard without slowing the suite.
            with lock:
                in_flight -= 1
            return httpx.Response(200, json=_chat_response("{}"))

        respx.post(f"{_BASE_URL}/chat/completions").mock(side_effect=_handler)
        provider = OpenAICompatProvider(base_url=_BASE_URL, model="m")

        threads = [
            threading.Thread(target=lambda: provider.complete(system="s", user="u", max_tokens=10)) for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert max_in_flight == 1
