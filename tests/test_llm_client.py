"""Unit tests for the thesis-scoped LLM provider layer (#1919, split #1995).

No network, no DB: respx intercepts httpx for the OpenAI-compatible
provider; the Anthropic provider wraps a MagicMock SDK client;
make_llm_clients resolves against a mocked runtime_config connection.
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
    _CHARS_PER_TOKEN,
    LOCAL_CONTEXT_WINDOW,
    AnthropicProvider,
    LLMProviderNotConfigured,
    OllamaNativeProvider,
    OpenAICompatProvider,
    make_llm_clients,
    normalize_completion_text,
    release_local_models,
    strip_code_fence,
    strip_think_block,
)

_BASE_URL = "http://localhost:11434/v1"


def _mock_ollama_probe(*, is_ollama: bool = True) -> None:
    """Pin the #2431 transport probe so tests never depend on a live Ollama.

    ``make_llm_clients`` asks ``/api/version`` whether a LOCAL endpoint is
    actually Ollama (llama.cpp and vLLM serve the OpenAI surface but not that
    route). Left unmocked these tests pass or fail depending on whether the
    operator's Ollama happens to be running, which is not a property of the
    code under test.
    """
    respx.get("http://localhost:11434/api/version").mock(
        return_value=httpx.Response(200 if is_ollama else 404, json={"version": "0.31.1"})
    )


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


def _native_response(
    content: str,
    *,
    done_reason: str = "stop",
    model: str = "qwen3:14b",
    prompt_eval_count: int = 6421,
    eval_count: int = 812,
) -> dict[str, Any]:
    """Ollama's ``/api/chat`` shape — deliberately NOT the OpenAI one."""
    return {
        "model": model,
        "message": {"role": "assistant", "content": content},
        "done": True,
        "done_reason": done_reason,
        "prompt_eval_count": prompt_eval_count,
        "eval_count": eval_count,
    }


class TestOllamaNativeProvider:
    """#2431 — the provider that exists because ``/v1`` ignores ``num_ctx``."""

    @respx.mock
    def test_sends_num_ctx_on_the_native_route(self) -> None:
        route = respx.post("http://localhost:11434/api/chat").mock(
            return_value=httpx.Response(200, json=_native_response('{"stance": "buy"}'))
        )
        provider = OllamaNativeProvider(base_url=_BASE_URL, model="qwen3:14b")
        completion = provider.complete(system="sys", user="usr", max_tokens=100)

        payload = json.loads(route.calls.last.request.content)
        # The whole point: without this the server truncates at its 4096 default.
        assert payload["options"]["num_ctx"] == LOCAL_CONTEXT_WINDOW
        assert payload["options"]["num_predict"] == 100
        # Ollama's JSON mode, not OpenAI's response_format spelling.
        assert payload["format"] == "json"
        assert "response_format" not in payload
        assert payload["messages"][0]["content"].endswith("/no_think")
        # The /v1 suffix is stripped — the native route is off the server root.
        assert str(route.calls.last.request.url).endswith("/api/chat")

        assert completion.text == '{"stance": "buy"}'
        assert completion.finish_reason == "stop"
        assert completion.prompt_tokens == 6421
        assert completion.completion_tokens == 812

    def test_refuses_a_prompt_that_cannot_fit_before_sending(self) -> None:
        """The pre-send guard. A doomed 7-minute generation must never start."""
        provider = OllamaNativeProvider(base_url=_BASE_URL, model="qwen3:14b")
        oversized = "x" * (LOCAL_CONTEXT_WINDOW * 4)  # ~LOCAL_CONTEXT_WINDOW tokens on its own
        with pytest.raises(ValueError, match="does not fit the local context window"):
            provider.complete(system="sys", user=oversized, max_tokens=2048)

    def test_output_reservation_counts_against_the_window(self) -> None:
        """``num_ctx`` covers input AND output, so ``max_tokens`` is not free.

        A prompt that fits on its own must still be refused when the reservation
        pushes it over — the arithmetic that made 8192 too small for a 7,081
        token prompt with a 2,048 token writer budget.
        """
        provider = OllamaNativeProvider(base_url=_BASE_URL, model="qwen3:14b")
        # Comfortably under the window alone; over it once output is reserved.
        near_limit = "x" * ((LOCAL_CONTEXT_WINDOW - 1000) * 4)
        with pytest.raises(ValueError, match="reserved for output"):
            provider.complete(system="", user=near_limit, max_tokens=2048)

    def test_exactly_at_the_ceiling_is_refused(self) -> None:
        """Review NITPICK on #2618: ``>=``, not ``>``.

        Zero headroom is not a fit, and ``_CHARS_PER_TOKEN`` under-estimates
        tokens by design — a prompt that merely REACHES the limit on this
        arithmetic is over it on the server's.
        """
        provider = OllamaNativeProvider(base_url=_BASE_URL, model="qwen3:14b")
        max_tokens = 2048
        exact = "x" * ((LOCAL_CONTEXT_WINDOW - max_tokens) * _CHARS_PER_TOKEN)
        with pytest.raises(ValueError, match="does not fit the local context window"):
            provider.complete(system="", user=exact, max_tokens=max_tokens)

    @respx.mock
    def test_truncation_at_the_ceiling_is_refused_not_returned(self) -> None:
        """Post-send detector: the estimate can be wrong, the server's count is not.

        ⚠ It must RAISE. Returning a completion known to be truncated would
        persist a memo written without part of its instructions — detect,
        report success, carry on, which is this bug's whole signature.
        """
        respx.post("http://localhost:11434/api/chat").mock(
            return_value=httpx.Response(200, json=_native_response("{}", prompt_eval_count=LOCAL_CONTEXT_WINDOW))
        )
        provider = OllamaNativeProvider(base_url=_BASE_URL, model="qwen3:14b")
        with pytest.raises(ValueError, match="truncated by the server"):
            provider.complete(system="sys", user="usr", max_tokens=100)

    @respx.mock
    def test_normal_prompt_count_is_not_flagged(self) -> None:
        """The detector must not cry wolf on a prompt that fitted."""
        respx.post("http://localhost:11434/api/chat").mock(
            return_value=httpx.Response(200, json=_native_response("{}", prompt_eval_count=6421))
        )
        provider = OllamaNativeProvider(base_url=_BASE_URL, model="qwen3:14b")
        completion = provider.complete(system="sys", user="usr", max_tokens=100)
        assert completion.prompt_tokens == 6421


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
# Model release (#2187)
# ---------------------------------------------------------------------------

_OLLAMA_ROOT = "http://localhost:11434"


class TestReleaseModel:
    @respx.mock
    def test_local_model_unloaded_at_server_root(self) -> None:
        # The unload route is Ollama-native and lives at the ROOT, not
        # under /v1 — a request to {base_url}/api/generate would 404.
        route = respx.post(f"{_OLLAMA_ROOT}/api/generate").mock(
            return_value=httpx.Response(200, json={"done_reason": "unload"})
        )
        OpenAICompatProvider(base_url=_BASE_URL, model="qwen3:14b").release_model()

        assert route.called
        assert json.loads(route.calls.last.request.content) == {"model": "qwen3:14b", "keep_alive": 0}

    @respx.mock
    def test_remote_endpoint_is_never_unloaded(self) -> None:
        # Someone else's RAM, someone else's lifecycle: no call at all.
        route = respx.post("https://llm.example.com/api/generate")
        OpenAICompatProvider(base_url="https://llm.example.com/v1", model="qwen3:14b").release_model()
        assert not route.called

    @respx.mock
    def test_failure_is_swallowed(self) -> None:
        # Best-effort by contract: a failed release costs memory, never
        # the batch that just succeeded.
        respx.post(f"{_OLLAMA_ROOT}/api/generate").mock(return_value=httpx.Response(500, text="boom"))
        OpenAICompatProvider(base_url=_BASE_URL, model="qwen3:14b").release_model()

    @respx.mock
    def test_transport_error_is_swallowed(self) -> None:
        respx.post(f"{_OLLAMA_ROOT}/api/generate").mock(side_effect=httpx.ConnectError("down"))
        OpenAICompatProvider(base_url=_BASE_URL, model="qwen3:14b").release_model()

    def test_anthropic_release_is_a_noop(self) -> None:
        sdk = MagicMock()
        AnthropicProvider(sdk, model="claude-sonnet-4-6").release_model()
        assert sdk.mock_calls == []

    @respx.mock
    def test_pair_with_shared_model_releases_once(self) -> None:
        route = respx.post(f"{_OLLAMA_ROOT}/api/generate").mock(
            return_value=httpx.Response(200, json={"done_reason": "unload"})
        )
        _mock_ollama_probe()
        release_local_models(make_llm_clients(_config_conn(provider="openai_compatible")))
        assert route.call_count == 1

    @respx.mock
    def test_pair_with_split_models_releases_each(self) -> None:
        route = respx.post(f"{_OLLAMA_ROOT}/api/generate").mock(
            return_value=httpx.Response(200, json={"done_reason": "unload"})
        )
        _mock_ollama_probe()
        clients = make_llm_clients(
            _config_conn(
                provider="openai_compatible",
                writer_model="deepseek-r1:14b",
                critic_model="qwen3:14b",
            )
        )
        release_local_models(clients)
        assert route.call_count == 2
        released = {json.loads(c.request.content)["model"] for c in route.calls}
        assert released == {"deepseek-r1:14b", "qwen3:14b"}


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
# make_llm_clients — config-driven provider resolution (#1995 split knobs)
# ---------------------------------------------------------------------------


def _config_conn(
    *,
    provider: str,
    base_url: str = _BASE_URL,
    writer_model: str = "qwen3:14b",
    critic_model: str = "qwen3:14b",
) -> MagicMock:
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
        "llm_model_writer": writer_model,
        "llm_model_critic": critic_model,
        "updated_at": MagicMock(),
        "updated_by": "test",
        "reason": "test",
    }
    conn.cursor.return_value = cursor
    return conn


class TestMakeLLMClients:
    def test_local_endpoint_resolves_to_the_native_provider(self) -> None:
        """#2431 — a LOCAL endpoint is Ollama, and only its native route honours
        ``num_ctx``. The default config points at localhost, so the default path
        must be the native provider; picking the OpenAI one here is the bug that
        truncated every thesis for two prompt revisions.
        """
        _mock_ollama_probe()
        clients = make_llm_clients(_config_conn(provider="openai_compatible"))
        assert isinstance(clients.writer, OllamaNativeProvider)
        assert isinstance(clients.critic, OllamaNativeProvider)
        assert clients.writer.provider_name == "ollama_native"
        assert clients.writer.model == "qwen3:14b"
        assert clients.critic.model == "qwen3:14b"

    @respx.mock
    def test_ollama_probe_is_cached_per_process(self) -> None:
        """Review NITPICK on #2618 — ``make_llm_clients`` is documented as doing
        no network I/O (``scheduler.py`` calls it purely to resolve config), so
        the probe must not fire per call and turn a config gate into a
        connectivity gate.
        """
        from app.services import llm_client

        llm_client._OLLAMA_PROBE_CACHE.clear()
        probe = respx.get("http://localhost:11434/api/version").mock(
            return_value=httpx.Response(200, json={"version": "0.31.1"})
        )
        for _ in range(3):
            make_llm_clients(_config_conn(provider="openai_compatible"))
        assert probe.call_count == 1

    @respx.mock
    def test_unreachable_probe_is_not_cached(self) -> None:
        """A restarting Ollama must not pin the OpenAI transport for the process."""
        from app.services import llm_client

        llm_client._OLLAMA_PROBE_CACHE.clear()
        probe = respx.get("http://localhost:11434/api/version").mock(side_effect=httpx.ConnectError("down"))
        for _ in range(2):
            clients = make_llm_clients(_config_conn(provider="openai_compatible"))
            assert not isinstance(clients.writer, OllamaNativeProvider)
        assert probe.call_count == 2  # retried, not remembered

    def test_remote_endpoint_keeps_the_openai_contract(self) -> None:
        """The converse, so the selection is pinned from both sides: a remote
        endpoint speaks OpenAI and must NOT be handed Ollama-native fields.
        """
        clients = make_llm_clients(_config_conn(provider="openai_compatible", base_url="https://api.example.com/v1"))
        assert isinstance(clients.writer, OpenAICompatProvider)
        assert not isinstance(clients.writer, OllamaNativeProvider)
        assert clients.writer.provider_name == "openai_compatible"

    def test_split_models_resolve_per_role(self) -> None:
        clients = make_llm_clients(
            _config_conn(
                provider="openai_compatible",
                writer_model="deepseek-r1:14b",
                critic_model="qwen3:14b",
            )
        )
        assert clients.writer.model == "deepseek-r1:14b"
        assert clients.critic.model == "qwen3:14b"

    def test_local_model_outside_allowlist_rejected(self) -> None:
        # #2187: /config rejects this, but a direct SQL write could still
        # leave it in the row — the load site must fail closed too, as
        # LLMProviderNotConfigured so thesis_refresh records a PREREQ_SKIP.
        conn = _config_conn(provider="openai_compatible", writer_model="mistral-small:latest")
        with pytest.raises(LLMProviderNotConfigured, match="allow-list"):
            make_llm_clients(conn)

    def test_critic_model_outside_allowlist_rejected(self) -> None:
        conn = _config_conn(provider="openai_compatible", critic_model="mistral-small:latest")
        with pytest.raises(LLMProviderNotConfigured, match="llm_model_critic"):
            make_llm_clients(conn)

    def test_remote_endpoint_exempt_from_allowlist(self) -> None:
        # Not our RAM — an arbitrary model name on a remote vLLM/OpenAI
        # endpoint must not be blocked by a local-memory rule.
        clients = make_llm_clients(
            _config_conn(
                provider="openai_compatible",
                base_url="https://llm.example.com/v1",
                writer_model="some-huge-remote-model",
                critic_model="some-huge-remote-model",
            )
        )
        assert clients.writer.model == "some-huge-remote-model"

    def test_anthropic_path_requires_key(self) -> None:
        conn = _config_conn(provider="anthropic", writer_model="claude-sonnet-4-6")
        with patch("app.services.llm_client.settings") as settings_mock:
            settings_mock.anthropic_api_key = None
            with pytest.raises(LLMProviderNotConfigured):
                make_llm_clients(conn)

    def test_anthropic_path_with_key(self) -> None:
        conn = _config_conn(
            provider="anthropic",
            writer_model="claude-sonnet-4-6",
            critic_model="claude-haiku-4-5",
        )
        with patch("app.services.llm_client.settings") as settings_mock:
            settings_mock.anthropic_api_key = "sk-ant-test"
            clients = make_llm_clients(conn)
        assert isinstance(clients.writer, AnthropicProvider)
        assert isinstance(clients.critic, AnthropicProvider)
        assert clients.writer.model == "claude-sonnet-4-6"
        assert clients.critic.model == "claude-haiku-4-5"


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
