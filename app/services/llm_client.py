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
from typing import Any, Final, Protocol

import anthropic
import httpx
import psycopg

from app.config import settings
from app.services.anthropic_client import make_anthropic_client
from app.services.runtime_config import (
    get_runtime_config,
    is_local_llm_endpoint,
    local_llm_model_violation,
)

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

# Native Ollama model-lifecycle route (#2187). NOT part of the
# OpenAI-compatible surface — it lives at the server ROOT, not under
# ``/v1``, so the release path strips the ``/v1`` suffix off
# ``llm_base_url``. Verified empirically 2026-08-02 against ollama on
# this box: ``POST {"model": M, "keep_alive": 0}`` answers
# ``{"done_reason": "unload"}`` and the model leaves ``/api/ps`` within
# seconds (ollama RSS 9.28 GB → 0.05 GB). Unload is ASYNCHRONOUS — the
# model can still appear in ``/api/ps`` on the very next call, with
# ``expires_at`` set to now.
#
# Passing ``keep_alive`` in the ``/v1/chat/completions`` BODY does not
# work: it is not an OpenAI field and Ollama ignores it there. That is
# why release is a separate call rather than a request parameter.
_OLLAMA_UNLOAD_PATH = "/api/generate"

# ⚠⚠ #2431 — THE SAME LESSON AS ``keep_alive`` ABOVE, AND IT COST 2,165 MEMOS.
# ``options.num_ctx`` is not an OpenAI field either, so ``/v1/chat/completions``
# accepts it with HTTP 200 and ignores it. Verified on this box 2026-08-12:
# POSTing ``{"options": {"num_ctx": 12288}}`` to ``/v1`` left ``ollama ps`` at
# ``CONTEXT 4096``. A fix written against ``/v1`` would look correct, return
# success, and change nothing — which is exactly how this defect survived two
# prompt revisions. The context window can only be set on the NATIVE route.
_OLLAMA_CHAT_PATH = "/api/chat"

# What the local path asks Ollama to load the model with.
#
# ⚠ Ollama's default is 4,096 and it TRUNCATES SILENTLY above it — no error, no
# warning, HTTP 200. Measured 2026-08-12 across 60 instruments, the thesis
# writer sends system+user of 5,387 / 6,352 / 7,081 tokens (min / median / max),
# so EVERY generation overflowed. The truncated region is the front of the
# prompt, which is where prompt v6 had just moved the subject-identity rule and
# the ``SUBJECT:`` line — so the memo stopped naming its own company and v6
# scored 0 of 127 where v1-v4 scored 487 of 487.
#
# 12,288 = the 7,081 observed maximum + the 2,048 ``max_tokens`` reservation
# (num_ctx covers input AND output) + ~33% headroom. qwen3:14b supports 40,960;
# the rest is left unclaimed deliberately. This box is 24 GB unified with
# ``OLLAMA_KV_CACHE_TYPE=q8_0``, so the KV cache is ~80 KiB/token: ~0.94 GiB
# here against ~0.31 GiB at 4,096. Raising it further costs real memory on a
# machine that pages, and buys nothing measured.
LOCAL_CONTEXT_WINDOW: Final = 12288

# Chars per token, for the PRE-SEND check only. Deliberately crude and
# deliberately an UNDER-estimate of tokens (real English JSON runs ~3.5-4.0
# chars/token), because this guard exists to catch an order-of-magnitude
# mistake, not to bill anyone.
_CHARS_PER_TOKEN = 4

# Release is best-effort housekeeping, not a generation — it must never
# hold a job open the way a 600s decode window legitimately can.
LLM_RELEASE_TIMEOUT: httpx.Timeout = httpx.Timeout(
    connect=5.0,
    read=30.0,
    write=10.0,
    pool=5.0,
)

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

# Markdown code fence wrapping the ENTIRE completion (```json ... ``` or
# bare ```). Empirical (#1919 PR-C tilt-check, 2026-07-09): deepseek-r1
# intermittently fences an otherwise schema-valid JSON object — Ollama
# does not enforce response_format=json_object for it. Stripping is
# lossless for compliant output (no-op unless the fence wraps everything)
# and model-neutral, same class as the <think> strip. A truncated fence
# (no closing ```) intentionally does NOT match — the parse failure +
# finish_reason stay honest.
_CODE_FENCE_RE = re.compile(r"\A```[a-zA-Z]*\s*\n?(.*?)\n?\s*```\s*\Z", re.DOTALL)

# Per-process serialisation of LLM calls (spec §1 concurrency layer (a)).
_LLM_CALL_SEMAPHORE = threading.Semaphore(1)


class LLMProviderNotConfigured(RuntimeError):
    """Raised when the configured provider must not be used as configured.

    Two reachable causes:

    * the ``anthropic`` path with no ``ANTHROPIC_API_KEY`` set (the
      ``openai_compatible`` path needs no key — Ollama ignores it — and
      its base URL / model columns are NOT NULL with defaults);
    * a locally-hosted model outside ``LOCAL_LLM_MODEL_ALLOWLIST``
      (#2187), which /config rejects but a direct SQL write could still
      leave in the row.

    ``thesis_refresh`` catches this and records a PREREQ_SKIP with the
    reason, so a bad config row is a visible skip rather than an hourly
    crash loop.
    """


@dataclass(frozen=True)
class LLMCompletion:
    """Normalized completion result across providers."""

    text: str  # leading <think>...</think> stripped defensively
    finish_reason: str  # "stop" | "length" | provider-mapped passthrough
    model: str  # as reported by the provider response
    # Provider-reported token usage; None when the provider omits it.
    # Consumed by the eval harness (scripts/llm_eval_thesis.py) for tok/s.
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class LLMClient(Protocol):
    """Minimal completion interface the thesis engine consumes."""

    provider_name: str
    model: str

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion: ...

    def release_model(self) -> None:
        """Best-effort: drop this model from the serving process's memory.

        Part of the interface (not an ``OpenAICompatProvider`` detail) so
        every implementation — including test fakes — has to answer the
        question. Implementations that hold nothing locally no-op.
        """
        ...


def strip_think_block(text: str) -> str:
    """Strip one leading ``<think>...</think>`` block and surrounding whitespace."""
    return _THINK_BLOCK_RE.sub("", text, count=1).strip()


def strip_code_fence(text: str) -> str:
    """Unwrap one markdown code fence when it wraps the whole text."""
    match = _CODE_FENCE_RE.match(text.strip())
    return match.group(1).strip() if match else text


def normalize_completion_text(text: str) -> str:
    """Defensive normalization applied to every provider completion.

    Order matters: thinking models emit the ``<think>`` block first, so
    strip it before checking for a whole-text code fence.
    """
    return strip_code_fence(strip_think_block(text))


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
        usage = body.get("usage") or {}
        return LLMCompletion(
            text=normalize_completion_text(text),
            finish_reason=finish_reason,
            model=body.get("model") or self.model,
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
        )

    def release_model(self) -> None:
        """Unload this model from a LOCAL inference server (#2187).

        Skipped for remote endpoints: the weights are not in our RAM and
        the unload route is not ours to call. Holds the same
        per-process semaphore as ``complete`` so a release can never
        land mid-generation in this process.

        Best-effort by contract — a failure here costs memory, never
        correctness, so it is logged and swallowed. Non-Ollama local
        servers (llama.cpp, vLLM) simply 404 this route and take that
        path.
        """
        if not is_local_llm_endpoint(self._base_url):
            return
        root = self._base_url.removesuffix("/v1")
        try:
            with _LLM_CALL_SEMAPHORE:
                response = httpx.post(
                    f"{root}{_OLLAMA_UNLOAD_PATH}",
                    json={"model": self.model, "keep_alive": 0},
                    timeout=LLM_RELEASE_TIMEOUT,
                )
            response.raise_for_status()
        except Exception:
            logger.warning(
                "llm: release of local model %s failed (best-effort, continuing)",
                self.model,
                exc_info=True,
            )
        else:
            logger.info("llm: released local model %s", self.model)


#: Probe results, keyed by base URL. ⚠ CACHED BECAUSE ``make_llm_clients`` IS
#: DOCUMENTED AS DOING NO NETWORK I/O — ``scheduler.py``'s
#: ``_llm_provider_resolvable`` prerequisite check calls it purely to resolve
#: config, and a per-call round trip would turn a config gate into a
#: connectivity gate. One probe per process per URL, so the hourly job pays it
#: at most once. Review NITPICK on #2618.
#:
#: ⚠ Staleness is the accepted trade: swapping Ollama for llama.cpp at the same
#: URL without restarting the process keeps the old answer. That is a
#: deployment change, and the probe fails TO the OpenAI transport anyway, so the
#: stale direction that matters degrades to the pre-#2431 behaviour.
_OLLAMA_PROBE_CACHE: dict[str, bool] = {}


def _endpoint_is_ollama(base_url: str) -> bool:
    """Is this endpoint actually Ollama? Probed once per process, never assumed (#2431).

    ``/api/version`` is Ollama's own route: llama.cpp and vLLM serve the OpenAI
    surface but not this one, so a 200 here is positive evidence rather than a
    guess off the hostname.

    ⚠ FAILS TO THE OPENAI TRANSPORT, deliberately. An unreachable or
    non-Ollama endpoint keeps the contract it had before this change, so the
    worst case of a wrong answer is the status quo rather than a broken one.
    Remote endpoints are never probed — the question only arises locally.
    """
    if not is_local_llm_endpoint(base_url):
        return False
    cached = _OLLAMA_PROBE_CACHE.get(base_url)
    if cached is not None:
        return cached
    try:
        # ⚠ ``rstrip('/')`` HERE and not in ``complete()`` — deliberate, not an
        # oversight (review NITPICK on #2618). This takes the RAW configured
        # ``llm_base_url``, which may carry a trailing slash; the provider works
        # off ``self._base_url``, already normalised by ``__init__``.
        response = httpx.get(
            f"{base_url.rstrip('/').removesuffix('/v1')}/api/version",
            timeout=LLM_RELEASE_TIMEOUT,
        )
    except httpx.HTTPError:
        # ⚠ NOT cached. An unreachable server is a transient condition — caching
        # it would pin the OpenAI transport for the life of the process because
        # Ollama happened to be restarting when the first probe fired.
        return False
    is_ollama = response.status_code == 200
    _OLLAMA_PROBE_CACHE[base_url] = is_ollama
    return is_ollama


class OllamaNativeProvider(OpenAICompatProvider):
    """Ollama's own ``/api/chat`` — the only route that honours ``num_ctx``.

    ⚠⚠ #2431 — THIS CLASS EXISTS FOR ONE REASON: ``/v1/chat/completions``
    silently ignores the context window. It is a SEPARATE TYPE rather than a
    branch inside ``OpenAICompatProvider`` because a class named for the OpenAI
    contract must not quietly stop speaking it depending on the URL — the
    factory chooses the transport, which is where a provider choice belongs.

    Inherits the constructor and ``release_model`` (both already Ollama-aware)
    and overrides only ``complete``. The response shape is Ollama's, so every
    field is mapped explicitly rather than assumed:

    ``message.content`` → text · ``done_reason`` → finish_reason ·
    ``prompt_eval_count`` → prompt_tokens · ``eval_count`` → completion_tokens

    ⚠ ``format: "json"`` is Ollama's own JSON mode. The OpenAI path's
    ``response_format: {"type": "json_object"}`` is a different spelling of the
    same requirement and is NOT accepted here.
    """

    provider_name = "ollama_native"

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion:
        # ⚠ Refuse a prompt that cannot fit rather than letting the server
        # truncate it silently — the defect this whole class exists to end.
        # Deliberately BEFORE the request: a 7-minute generation that was
        # doomed at byte zero should never be started.
        # ⚠ ``>=``, not ``>``. Exactly-at-the-ceiling leaves zero headroom, and
        # ``_CHARS_PER_TOKEN`` is a deliberate UNDER-estimate of tokens — so a
        # prompt that merely reaches the limit on this arithmetic is over it in
        # reality. Review NITPICK on #2618.
        estimated = (len(system) + len(user)) // _CHARS_PER_TOKEN
        if estimated + max_tokens >= LOCAL_CONTEXT_WINDOW:
            raise ValueError(
                f"prompt does not fit the local context window: ~{estimated} estimated prompt tokens "
                f"+ {max_tokens} reserved for output exceeds num_ctx={LOCAL_CONTEXT_WINDOW} "
                f"(model={self.model}). Ollama would truncate this silently and the memo would be "
                f"written without its instructions — see #2431."
            )

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system + _NO_THINK_SUFFIX},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "format": "json",
            "options": {"num_ctx": LOCAL_CONTEXT_WINDOW, "num_predict": max_tokens},
        }
        with _LLM_CALL_SEMAPHORE:
            response = httpx.post(
                f"{self._base_url.removesuffix('/v1')}{_OLLAMA_CHAT_PATH}",
                json=payload,
                timeout=LLM_REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        body = response.json()
        prompt_tokens = body.get("prompt_eval_count")

        # ⚠⚠ POST-SEND confirmation, because the guard above only checks an
        # ESTIMATE and ``_CHARS_PER_TOKEN`` can undercount. ``prompt_eval_count``
        # is what the server actually consumed: at the window ceiling the prompt
        # WAS truncated and this memo was written without part of its
        # instructions.
        #
        # ⚠⚠ RAISES, does not log-and-return. Returning it would persist a memo
        # known to be corrupt — which is this bug's entire signature (detect,
        # report success, carry on). The caller's retry-once machinery gets a
        # second attempt; a genuinely oversized prompt fails twice and lands on
        # a ``thesis_runs`` row instead of in ``theses``.
        if isinstance(prompt_tokens, int) and prompt_tokens >= LOCAL_CONTEXT_WINDOW:
            raise ValueError(
                f"prompt was truncated by the server: prompt_eval_count {prompt_tokens} reached "
                f"num_ctx {LOCAL_CONTEXT_WINDOW} (model={self.model}). The completion was written "
                f"without part of its instructions and must not be stored — see #2431."
            )
        return LLMCompletion(
            text=normalize_completion_text((body.get("message") or {}).get("content") or ""),
            finish_reason=body.get("done_reason") or "unknown",
            model=body.get("model") or self.model,
            prompt_tokens=prompt_tokens,
            completion_tokens=body.get("eval_count"),
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
            text=normalize_completion_text(text),
            finish_reason=self._FINISH_REASON_MAP.get(stop_reason, stop_reason),
            model=message.model,
            prompt_tokens=message.usage.input_tokens,
            completion_tokens=message.usage.output_tokens,
        )

    def release_model(self) -> None:
        """No-op: a cloud model holds no memory on this machine (#2187)."""
        return


@dataclass(frozen=True)
class LLMClientPair:
    """Writer + critic clients resolved from ONE runtime_config read.

    The two roles may run different models (#1995 — e.g. a faster bulk
    writer with a stricter critic) but always share provider / base URL /
    key. Constructing both from a single ``get_runtime_config`` snapshot
    means a concurrent ``/config`` PATCH can never split one generation
    across two half-applied configs (Codex ckpt-1, 2026-07-10).
    """

    writer: LLMClient
    critic: LLMClient


def make_llm_clients(conn: psycopg.Connection[Any]) -> LLMClientPair:
    """Resolve the configured writer + critic clients from ``runtime_config``.

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
        # One SDK client shared by both roles (review #2004 NITPICK) —
        # the per-role split is the model string, not the transport.
        sdk = make_anthropic_client(api_key)
        return LLMClientPair(
            writer=AnthropicProvider(sdk, model=cfg.llm_model_writer),
            critic=AnthropicProvider(sdk, model=cfg.llm_model_critic),
        )
    # #2187 allow-list at the LOAD site, not just at /config: the PATCH
    # guard cannot see a model written straight into the row by SQL, and
    # this is the only place a model is actually pulled into memory.
    for field_name, model in (
        ("llm_model_writer", cfg.llm_model_writer),
        ("llm_model_critic", cfg.llm_model_critic),
    ):
        violation = local_llm_model_violation(
            provider=cfg.llm_provider,
            base_url=cfg.llm_base_url,
            model=model,
            field=field_name,
        )
        if violation is not None:
            raise LLMProviderNotConfigured(violation)
    # ⚠ #2431 — the transport is chosen HERE, explicitly, and ONLY for an
    # endpoint PROVED to be Ollama. Ollama's native route is the only one that
    # honours ``num_ctx`` (``/v1`` accepts the option and ignores it), which is
    # how the writer spent two prompt revisions being silently truncated.
    #
    # ⚠⚠ LOCALITY IS NOT OLLAMA. ``docs/wiki/byo-llm.md`` documents BYO-LLM as
    # any OpenAI-compatible base URL, and llama.cpp / vLLM are both commonly
    # run on loopback. Selecting the native transport off ``is_local_llm_endpoint``
    # alone would send Ollama-shaped requests to those servers and break a
    # supported configuration. The probe answers the question the URL cannot.
    provider = OllamaNativeProvider if _endpoint_is_ollama(cfg.llm_base_url) else OpenAICompatProvider
    return LLMClientPair(
        writer=provider(base_url=cfg.llm_base_url, model=cfg.llm_model_writer, api_key=settings.llm_api_key),
        critic=provider(base_url=cfg.llm_base_url, model=cfg.llm_model_critic, api_key=settings.llm_api_key),
    )


def release_local_models(clients: LLMClientPair) -> None:
    """Release every distinct model a completed batch left resident (#2187).

    Called once per ``thesis_refresh`` batch rather than per generation:
    the model must stay warm across the ≤5 back-to-back generations
    (a reload costs seconds each), but must NOT stay warm for the ~33
    idle minutes that follow — that residency is the OOM.

    Deduplicated by model name because writer and critic share one model
    by default (``DEFAULT_LLM_MODEL_WRITER`` == ``DEFAULT_LLM_MODEL_CRITIC``).
    Each ``release_model`` is already best-effort; this never raises.
    """
    seen: set[str] = set()
    for client in (clients.writer, clients.critic):
        if client.model in seen:
            continue
        seen.add(client.model)
        client.release_model()
