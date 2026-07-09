"""Thesis-scoped LLM provider layer (#1919 PR-A).

BYO OpenAI-compatible LLM per the #1888 endpoint contract: eBull plugs
into an operator-configured OpenAI-compatible base URL (local-first
default: Ollama at ``http://localhost:11434/v1``); cloud (Anthropic)
remains available by configuration only. Sentiment stays on Anthropic +
lexicon fallback — out of scope here.

This module is the **single construction site** for every outbound LLM
call under ``app/`` that is not the Anthropic SDK factory
(``app/services/anthropic_client.py``). Constructing an OpenAI-compatible
``/chat/completions`` call anywhere else is forbidden by
``scripts/check_llm_chokepoint.sh`` (pre-push hook + CI), the sibling of
``scripts/check_anthropic_timeout.sh`` — same #1479 hang class: an
unbounded outbound read must never be reintroducible silently.

Timeout shape (non-streaming completions):
  * ``connect=5.0`` — reachable endpoint completes the handshake well
    under this; a dead host fails fast.
  * ``read=600.0`` — a local 14B emitting 2,048 tokens below 11.4 tok/s
    breaks a 180s read window; 600s bounds the #1479 hang class without
    killing slow local decodes, and is sized to survive an Ollama
    server-side queue depth of 2-3 at 14B speeds (spec §1).
  * ``write=30.0`` / ``pool=10.0`` — request bodies are small; pool
    checkout is local.

Concurrency (spec §1, honest about topology #719 — API and jobs daemon
are SEPARATE processes, so no in-process primitive can serialise across
them): a per-process ``threading.Semaphore(1)`` around ``complete()``
stops one process stacking its own concurrent calls; cross-process
contention resolves at the Ollama server-side request queue (serial by
default). No DB advisory lock around LLM calls — holding pool resources
through multi-minute generations is the failure class #293 removed.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass
from typing import Any, Protocol

import anthropic
import httpx
import psycopg

from app.config import settings
from app.services.anthropic_client import make_anthropic_client
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)

# Bounded per-request timeout for every OpenAI-compatible LLM call. See
# the module docstring for the per-phase rationale. Single source of
# truth — do NOT inline these literals at call sites.
LLM_REQUEST_TIMEOUT: httpx.Timeout = httpx.Timeout(
    connect=5.0,
    read=600.0,
    write=30.0,
    pool=10.0,
)

# Model used when the operator flips llm_provider='anthropic' historically
# defaulted here; also the PR-A model for the still-Anthropic-gated cascade
# path (scheduler + refresh_cascade construct AnthropicProvider directly
# until PR-B switches them to make_llm_client).
ANTHROPIC_DEFAULT_MODEL = "claude-sonnet-4-6"

# Empirical (spec "Empirical verification", 2026-07-09): qwen3's default
# thinking mode burned the entire completion budget (`finish: length`,
# EMPTY content, invalid JSON); with `/no_think` in the system prompt the
# output is clean schema-valid JSON. Appended unconditionally on the
# OpenAI-compatible path — models that don't recognise the directive
# treat it as an inert trailing token, which PR-C's eval harness verifies
# per model. Thinking models that ignore it (deepseek-r1) are handled by
# the defensive `<think>` strip below.
_NO_THINK_SUFFIX = "\n/no_think"

# Leading <think>...</think> block emitted by thinking models
# (deepseek-r1 emits one unconditionally). DOTALL so multi-line reasoning
# is covered; a truncated block (no closing tag, finish_reason='length')
# intentionally does NOT match — the downstream JSON parse fails and the
# recorded finish_reason distinguishes truncation from malformed output.
_THINK_BLOCK_RE = re.compile(r"\A\s*<think>.*?</think>\s*", re.DOTALL)

# Per-process serialisation of LLM calls (spec §1 concurrency layer (a)).
_LLM_CALL_SEMAPHORE = threading.Semaphore(1)


class LLMProviderNotConfigured(RuntimeError):
    """Raised when the configured provider cannot be constructed.

    Only reachable on the ``anthropic`` path with no ``ANTHROPIC_API_KEY``
    set — the ``openai_compatible`` path needs no key (Ollama ignores it)
    and its base URL / model columns are NOT NULL with defaults.
    """


@dataclass(frozen=True)
class LLMCompletion:
    """Normalized completion result across providers."""

    text: str  # leading <think>...</think> stripped defensively
    finish_reason: str  # "stop" | "length" | provider-mapped passthrough
    model: str  # as reported by the provider response


class LLMClient(Protocol):
    """Minimal completion interface the thesis engine consumes."""

    provider_name: str
    model: str

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion: ...


def strip_think_block(text: str) -> str:
    """Strip one leading ``<think>...</think>`` block and surrounding whitespace."""
    return _THINK_BLOCK_RE.sub("", text, count=1).strip()


class OpenAICompatProvider:
    """OpenAI-compatible ``/chat/completions`` over httpx (no ``openai`` dep).

    Targets any endpoint speaking the OpenAI chat-completions contract
    (Ollama, llama.cpp server, vLLM, OpenAI itself). The API key is
    optional — sent as ``Authorization: Bearer`` when set; Ollama ignores
    it.
    """

    provider_name = "openai_compatible"

    def __init__(self, *, base_url: str, model: str, api_key: str | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self.model = model
        self._api_key = api_key

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion:
        headers: dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system + _NO_THINK_SUFFIX},
                {"role": "user", "content": user},
            ],
            "max_tokens": max_tokens,
            # Both thesis calls demand ONLY-JSON responses; json_object
            # mode was empirically required for schema-valid qwen3 output.
            "response_format": {"type": "json_object"},
            "stream": False,
        }
        with _LLM_CALL_SEMAPHORE:
            response = httpx.post(
                f"{self._base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=LLM_REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        body = response.json()
        choices = body.get("choices") or []
        if not choices:
            raise ValueError(f"LLM response had no choices (model={self.model})")
        choice = choices[0]
        text = (choice.get("message") or {}).get("content") or ""
        finish_reason = choice.get("finish_reason") or "unknown"
        return LLMCompletion(
            text=strip_think_block(text),
            finish_reason=finish_reason,
            model=body.get("model") or self.model,
        )


class AnthropicProvider:
    """Wraps the existing bounded-timeout Anthropic SDK client (#1479).

    The wrapped client MUST come from ``make_anthropic_client`` (unchanged
    180s read window — cloud models stream fast; the 600s local window is
    an OpenAI-compat concern only).
    """

    provider_name = "anthropic"

    # Anthropic stop_reason → normalized finish_reason. Unknown values
    # pass through verbatim so the failure record stays honest.
    _FINISH_REASON_MAP = {"end_turn": "stop", "max_tokens": "length"}

    def __init__(self, client: anthropic.Anthropic, *, model: str) -> None:
        self._client = client
        self.model = model

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion:
        with _LLM_CALL_SEMAPHORE:
            message = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        block = message.content[0] if message.content else None
        text: str | None = getattr(block, "text", None)
        if text is None:
            raise ValueError(f"Anthropic: unexpected content block type {type(block)!r}")
        stop_reason = message.stop_reason or "unknown"
        return LLMCompletion(
            text=strip_think_block(text),
            finish_reason=self._FINISH_REASON_MAP.get(stop_reason, stop_reason),
            model=message.model,
        )


def make_llm_client(conn: psycopg.Connection[Any]) -> LLMClient:
    """Resolve the configured LLM provider from ``runtime_config``.

    Single construction chokepoint (spec §1): every thesis-path caller
    routes through here so provider resolution, bounded timeouts, and the
    per-process semaphore are applied uniformly.

    Keys stay env-only (``Settings``): ``anthropic_api_key`` for the
    anthropic path (required — raises ``LLMProviderNotConfigured`` when
    unset), ``llm_api_key`` for OpenAI-compatible endpoints that demand
    one (optional; Ollama ignores it).

    Propagates ``RuntimeConfigCorrupt`` from ``get_runtime_config`` —
    callers fail closed, never substitute defaults.
    """
    cfg = get_runtime_config(conn)
    if cfg.llm_provider == "anthropic":
        api_key = settings.anthropic_api_key
        if not api_key:
            raise LLMProviderNotConfigured("llm_provider='anthropic' but ANTHROPIC_API_KEY is not set")
        return AnthropicProvider(make_anthropic_client(api_key), model=cfg.llm_model)
    return OpenAICompatProvider(
        base_url=cfg.llm_base_url,
        model=cfg.llm_model,
        api_key=settings.llm_api_key,
    )
