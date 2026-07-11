"""Thesis LLM eval harness (#1919 PR-C, spec §7; judge stage #1995).

Benchmarks the PRODUCTION writer+critic path (prompts, validators, token
budgets imported from ``app.services.thesis`` — never copied, so the
benchmark cannot drift from what ``generate_thesis`` actually sends)
against an OpenAI-compatible endpoint, replaying fixed
``_assemble_context`` fixtures so every model sees identical prompts.

Three subcommands:

* ``capture`` — snapshot ``_assemble_context`` output from the dev DB for
  the house panel (AAPL, GME, MSFT, JPM, HD) into
  ``tests/fixtures/llm_eval/<symbol>.json``. Dev-guarded (#1765): refuses
  to run outside a local dev environment.
* ``run`` — for each fixture x ``--iterations``: writer attempt, one
  retry on failure (mirrors ``_call_with_one_retry``), then a critic
  round against the successful memo. Reports JSON-schema pass rate
  (first-attempt and with-retry), enum validity, finish_reason mix,
  tok/s + wall-clock per call. ``--critic-model`` (#1995) runs the
  critic rounds on a DIFFERENT model, mirroring the production split
  knobs; default is the writer's model (pre-split behaviour).
* ``judge`` (#1995) — content-grading stage. The structural harness above
  gates schema validity only; the judge compares two writers' memos on
  IDENTICAL fixtures for content quality (numeric grounding, anchor
  discipline, reasoning). Pairs writer outputs per (symbol, iteration)
  from ONE ``run --json-out`` file, presents each pair to the judge
  model BLINDED (A/B, no model names) and TWICE with order swapped — a
  win counts only when both orderings agree (position-bias control).
  Every response is checked against ``--ctx-limit`` via the provider-
  reported prompt_tokens: an overflow is a failed comparison, never a
  silently-truncated grade (the Ollama default-4096 trap).

Go-live gate (spec §7): >=9/10 writer passes WITH retry on the chosen
local model. ``--gate-model <model>`` makes the exit code enforce it.

Usage:
    PYTHONPATH=. uv run python scripts/llm_eval_thesis.py capture
    PYTHONPATH=. uv run python scripts/llm_eval_thesis.py run \
        --models qwen3:14b deepseek-r1:14b --critic-model qwen3:14b \
        --gate-model deepseek-r1:14b --json-out /tmp/llm_eval_results.json
    PYTHONPATH=. uv run python scripts/llm_eval_thesis.py judge \
        --results /tmp/llm_eval_results.json \
        --model-a qwen3:14b --model-b deepseek-r1:14b \
        --judge-model qwen3:14b --json-out /tmp/llm_judge_results.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from app.services.llm_client import LLMClient, LLMCompletion, OpenAICompatProvider
from app.services.thesis import (
    _CRITIC_SYSTEM,
    _MAX_TOKENS_CRITIC,
    _MAX_TOKENS_WRITER,
    _VALID_STANCES,
    _VALID_THESIS_TYPES,
    _VALID_VERDICTS,
    _WRITER_SYSTEM,
    _build_critic_prompt,
    _build_writer_prompt,
    _validate_critic_output,
    _validate_writer_output,
)

PANEL_SYMBOLS = ("AAPL", "GME", "MSFT", "JPM", "HD")
FIXTURES_DIR = Path("tests/fixtures/llm_eval")
DEFAULT_BASE_URL = "http://localhost:11434/v1"
# Spec §7 go-live gate: >=9/10 writer passes with retry. The rate alone
# is not enough — 5/5 must NOT pass the gate, so a minimum sample size is
# enforced (Codex ckpt-2 finding, 2026-07-09).
GATE_MIN_PASS_RATE = 0.9
GATE_MIN_ROUNDS = 10


# ---------------------------------------------------------------------------
# Attempt / round records (pure data; aggregation is unit-tested)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AttemptResult:
    """One LLM call, classified."""

    ok: bool
    # "pass" | "transport" | "invalid_json" | "schema_fail"
    category: str
    # Enum membership among PARSED outputs (None when JSON never parsed).
    enum_ok: bool | None
    finish_reason: str | None
    duration_s: float
    completion_tokens: int | None
    error: str | None = None

    @property
    def tok_s(self) -> float | None:
        if self.completion_tokens is None or self.duration_s <= 0:
            return None
        return self.completion_tokens / self.duration_s


@dataclass(frozen=True)
class RoundResult:
    """One fixture round: up to two attempts (prod retry-once shape).

    ``iteration`` disambiguates repeat rounds on the same symbol so the
    judge stage can pair writer outputs deterministically per
    (symbol, iteration) (Codex ckpt-1, 2026-07-10).
    """

    symbol: str
    call: str  # "writer" | "critic"
    attempts: list[AttemptResult]
    parsed: dict[str, object] | None = None
    iteration: int = 1

    @property
    def pass_first(self) -> bool:
        return bool(self.attempts) and self.attempts[0].ok

    @property
    def pass_with_retry(self) -> bool:
        return any(a.ok for a in self.attempts)


@dataclass
class ModelReport:
    """Aggregate for one writer model (+ the critic model its critic
    rounds ran on — may differ under the #1995 split; labelling both
    keeps the report honest about which model produced critic stats)."""

    model: str
    critic_model: str = ""
    writer_rounds: int = 0
    writer_pass_first: int = 0
    writer_pass_retry: int = 0
    critic_rounds: int = 0
    critic_pass_first: int = 0
    critic_pass_retry: int = 0
    # #1987 gate: critic truncations counted per-role — the shared
    # finish_reasons Counter aggregates writer+critic and cannot gate on
    # a critic-only "length" (a truncated critic stores a thesis WITHOUT
    # critic_json in production, silently degrading the audit trail).
    critic_length_failures: int = 0
    finish_reasons: Counter[str] = field(default_factory=Counter)
    enum_checked: int = 0
    enum_ok: int = 0
    durations_s: list[float] = field(default_factory=list)
    tok_s: list[float] = field(default_factory=list)

    @property
    def writer_pass_retry_rate(self) -> float:
        return self.writer_pass_retry / self.writer_rounds if self.writer_rounds else 0.0

    def gate_passes(self) -> bool:
        return (
            self.writer_rounds >= GATE_MIN_ROUNDS
            and self.writer_pass_retry_rate >= GATE_MIN_PASS_RATE
            and self.critic_length_failures == 0
        )


def _writer_enums_ok(data: dict[str, object]) -> bool:
    return data.get("thesis_type") in _VALID_THESIS_TYPES and data.get("stance") in _VALID_STANCES


def _critic_enums_ok(data: dict[str, object]) -> bool:
    return data.get("verdict") in _VALID_VERDICTS


def classify_attempt(
    completion: LLMCompletion,
    duration_s: float,
    *,
    call: str,
) -> tuple[AttemptResult, dict[str, object] | None]:
    """Classify one completion against the production validators.

    Pure — no I/O; unit-tested. Categories are disjoint stages:
    invalid_json (never parsed to a JSON object) then schema_fail
    (production validator rejected) then pass. ``enum_ok`` is computed
    independently on every PARSED output so the spec's "enum validity"
    metric is reportable even when the schema fails on another field.
    """
    validate = _validate_writer_output if call == "writer" else _validate_critic_output
    enums_ok = _writer_enums_ok if call == "writer" else _critic_enums_ok

    try:
        data: object = json.loads(completion.text)
    except json.JSONDecodeError as exc:
        # Keep the head of the raw output: "unparseable" alone cannot
        # distinguish a markdown-fenced payload / tag leak (fixable by
        # provider normalization) from genuinely broken JSON.
        head = completion.text[:200].replace("\n", "\\n")
        return (
            AttemptResult(
                ok=False,
                category="invalid_json",
                enum_ok=None,
                finish_reason=completion.finish_reason,
                duration_s=duration_s,
                completion_tokens=completion.completion_tokens,
                error=f"unparseable JSON: {exc}; head={head!r}",
            ),
            None,
        )
    if not isinstance(data, dict):
        return (
            AttemptResult(
                ok=False,
                category="invalid_json",
                enum_ok=None,
                finish_reason=completion.finish_reason,
                duration_s=duration_s,
                completion_tokens=completion.completion_tokens,
                error=f"JSON is not an object: {type(data).__name__}",
            ),
            None,
        )

    enum_ok = enums_ok(data)
    try:
        validate(data)
    except ValueError as exc:
        return (
            AttemptResult(
                ok=False,
                category="schema_fail",
                enum_ok=enum_ok,
                finish_reason=completion.finish_reason,
                duration_s=duration_s,
                completion_tokens=completion.completion_tokens,
                error=str(exc),
            ),
            None,
        )
    return (
        AttemptResult(
            ok=True,
            category="pass",
            enum_ok=enum_ok,
            finish_reason=completion.finish_reason,
            duration_s=duration_s,
            completion_tokens=completion.completion_tokens,
        ),
        data,
    )


def run_round(
    client: LLMClient,
    *,
    symbol: str,
    call: str,
    system: str,
    user: str,
    max_tokens: int,
    iteration: int = 1,
) -> RoundResult:
    """One production-shaped round: attempt, retry ONCE on parse/schema failure.

    Mirrors ``_call_with_one_retry`` exactly: production retries only the
    ``ValueError`` class (parse/schema); a transport error (connect/read/
    HTTP status) propagates immediately with NO retry. So here a transport
    failure is recorded and ENDS the round — the benchmark must survive a
    flaky model server, but must not credit a recovery production would
    never perform (Codex ckpt-2 finding, 2026-07-09).
    """
    attempts: list[AttemptResult] = []
    parsed: dict[str, object] | None = None
    for _attempt in range(2):
        start = time.monotonic()
        try:
            completion = client.complete(system=system, user=user, max_tokens=max_tokens)
        except Exception as exc:  # httpx transport / HTTP status / provider error
            attempts.append(
                AttemptResult(
                    ok=False,
                    category="transport",
                    enum_ok=None,
                    finish_reason=None,
                    duration_s=time.monotonic() - start,
                    completion_tokens=None,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
            break  # prod does not retry transport errors
        result, data = classify_attempt(completion, time.monotonic() - start, call=call)
        attempts.append(result)
        if result.ok:
            parsed = data
            break
    return RoundResult(symbol=symbol, call=call, attempts=attempts, parsed=parsed, iteration=iteration)


def aggregate(model: str, rounds: list[RoundResult], *, critic_model: str = "") -> ModelReport:
    """Fold round results into the per-model report. Pure; unit-tested."""
    report = ModelReport(model=model, critic_model=critic_model or model)
    for rnd in rounds:
        if rnd.call == "writer":
            report.writer_rounds += 1
            report.writer_pass_first += rnd.pass_first
            report.writer_pass_retry += rnd.pass_with_retry
        else:
            report.critic_rounds += 1
            report.critic_pass_first += rnd.pass_first
            report.critic_pass_retry += rnd.pass_with_retry
            report.critic_length_failures += sum(1 for a in rnd.attempts if a.finish_reason == "length")
        for attempt in rnd.attempts:
            if attempt.finish_reason is not None:
                report.finish_reasons[attempt.finish_reason] += 1
            if attempt.enum_ok is not None:
                report.enum_checked += 1
                report.enum_ok += attempt.enum_ok
            report.durations_s.append(attempt.duration_s)
            if attempt.tok_s is not None:
                report.tok_s.append(attempt.tok_s)
    return report


# ---------------------------------------------------------------------------
# capture
# ---------------------------------------------------------------------------


def capture(symbols: list[str], out_dir: Path) -> int:
    """Snapshot _assemble_context for each symbol into a replayable fixture."""
    # Deferred imports: `run` must work without DB config / psycopg present.
    import psycopg

    from app.config import settings
    from app.services.thesis import _assemble_context
    from scripts._dev_guard import assert_dev_environment

    assert_dev_environment()
    out_dir.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(settings.database_url) as conn:
        for symbol in symbols:
            # symbol is NOT unique (sql/043) — pin the lowest instrument_id so
            # repeat captures always snapshot the same row.
            row = conn.execute(
                "SELECT instrument_id FROM instruments WHERE symbol = %(s)s AND is_tradable"
                " ORDER BY instrument_id LIMIT 1",
                {"s": symbol},
            ).fetchone()
            if row is None:
                print(f"SKIP {symbol}: no tradable instrument row")
                continue
            instrument_id = row[0]
            context = _assemble_context(conn, instrument_id)
            # Same default=str transform _build_writer_prompt applies, so a
            # replayed fixture is byte-equivalent in prompt space.
            fixture = {
                "symbol": symbol,
                "instrument_id": instrument_id,
                "context": json.loads(json.dumps(context, default=str)),
            }
            path = out_dir / f"{symbol}.json"
            path.write_text(json.dumps(fixture, indent=2) + "\n")
            print(f"captured {symbol} -> {path} ({path.stat().st_size:,} bytes)")
    return 0


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def load_fixtures(fixtures_dir: Path) -> list[dict[str, Any]]:
    paths = sorted(fixtures_dir.glob("*.json"))
    if not paths:
        raise SystemExit(f"no fixtures under {fixtures_dir} — run `capture` first (needs dev DB)")
    return [json.loads(p.read_text()) for p in paths]


def run_model(
    model: str,
    fixtures: list[dict[str, Any]],
    *,
    base_url: str,
    iterations: int,
    critic_model: str | None = None,
) -> tuple[ModelReport, list[RoundResult]]:
    """Benchmark one writer model; critic rounds run on ``critic_model``
    when given (#1995 split), else on the writer's model (pre-split)."""
    client = OpenAICompatProvider(base_url=base_url, model=model)
    if critic_model is None or critic_model == model:
        critic_client: LLMClient = client
    else:
        critic_client = OpenAICompatProvider(base_url=base_url, model=critic_model)
    rounds: list[RoundResult] = []
    for iteration in range(1, iterations + 1):
        for fixture in fixtures:
            symbol = fixture["symbol"]
            context = fixture["context"]
            writer = run_round(
                client,
                symbol=symbol,
                call="writer",
                system=_WRITER_SYSTEM,
                user=_build_writer_prompt(context),
                max_tokens=_MAX_TOKENS_WRITER,
                iteration=iteration,
            )
            rounds.append(writer)
            _print_round(model, iteration, writer)
            if writer.parsed is None:
                continue  # prod path: no memo, no critic call
            memo = writer.parsed.get("memo_markdown")
            if not isinstance(memo, str):  # unreachable post-validate; keep pyright honest
                continue
            critic = run_round(
                critic_client,
                symbol=symbol,
                call="critic",
                system=_CRITIC_SYSTEM,
                user=_build_critic_prompt(memo, context),
                max_tokens=_MAX_TOKENS_CRITIC,
                iteration=iteration,
            )
            rounds.append(critic)
            _print_round(critic_client.model, iteration, critic)
    return aggregate(model, rounds, critic_model=critic_client.model), rounds


def _print_round(model: str, iteration: int, rnd: RoundResult) -> None:
    for i, attempt in enumerate(rnd.attempts, start=1):
        tok_s = f"{attempt.tok_s:.1f} tok/s" if attempt.tok_s is not None else "tok/s n/a"
        detail = "" if attempt.ok else f" [{attempt.category}: {attempt.error}]"
        print(
            f"  {model} it{iteration} {rnd.symbol} {rnd.call} a{i}: "
            f"{'PASS' if attempt.ok else 'FAIL'} "
            f"finish={attempt.finish_reason} {attempt.duration_s:.0f}s {tok_s}{detail}",
            flush=True,
        )


def _fmt_rate(numerator: int, denominator: int) -> str:
    if not denominator:
        return "n/a"
    return f"{numerator}/{denominator} ({numerator / denominator:.0%})"


def print_report(report: ModelReport) -> None:
    critic_differs = bool(report.critic_model) and report.critic_model != report.model
    critic_label = f" (critic: {report.critic_model})" if critic_differs else ""
    print(f"\n=== {report.model}{critic_label} ===")
    print(f"writer pass (first attempt): {_fmt_rate(report.writer_pass_first, report.writer_rounds)}")
    print(f"writer pass (with retry):    {_fmt_rate(report.writer_pass_retry, report.writer_rounds)}")
    print(f"critic pass (first attempt): {_fmt_rate(report.critic_pass_first, report.critic_rounds)}")
    print(f"critic pass (with retry):    {_fmt_rate(report.critic_pass_retry, report.critic_rounds)}")
    print(f"enum validity (parsed outputs): {_fmt_rate(report.enum_ok, report.enum_checked)}")
    print(f"finish_reason mix: {dict(report.finish_reasons)}")
    print(f"critic length-failures: {report.critic_length_failures}")
    if report.durations_s:
        print(
            f"wall-clock per call: mean {statistics.mean(report.durations_s):.0f}s, "
            f"median {statistics.median(report.durations_s):.0f}s, "
            f"max {max(report.durations_s):.0f}s"
        )
    if report.tok_s:
        print(f"throughput: mean {statistics.mean(report.tok_s):.1f} tok/s")
    gate = "PASS" if report.gate_passes() else "FAIL"
    if report.writer_rounds < GATE_MIN_ROUNDS:
        gate = f"FAIL (insufficient sample: {report.writer_rounds} < {GATE_MIN_ROUNDS} writer rounds)"
    elif report.critic_length_failures > 0:
        gate = f"FAIL ({report.critic_length_failures} critic length-failure(s))"
    print(
        f"go-live gate (writer with-retry >= {GATE_MIN_PASS_RATE:.0%} over >= {GATE_MIN_ROUNDS} rounds"
        f" AND critic length-failures == 0): {gate}"
    )


def _round_to_json(rnd: RoundResult) -> dict[str, object]:
    return {
        "symbol": rnd.symbol,
        "call": rnd.call,
        "iteration": rnd.iteration,
        # Parsed output rides along so the judge stage can grade writer
        # memos without re-running generation (#1995).
        "parsed": rnd.parsed,
        "attempts": [asdict(a) | {"tok_s": a.tok_s} for a in rnd.attempts],
    }


# ---------------------------------------------------------------------------
# judge — content-grading stage (#1995)
# ---------------------------------------------------------------------------

JUDGE_DIMENSIONS = (
    "numeric_grounding",  # every figure cited traceable to the context (the GME FY25-EBITDA-misread class)
    "anchor_discipline",  # targets/buy zone coherent with the price anchor
    "valuation_reasoning",
    "risk_balance",
    "internal_consistency",  # stance/confidence/zone/targets agree with each other
    "specificity",
)
# 2048, not 1024 — empirical (first 14B judge run, 2026-07-11): 8/10 pairs
# came back unparseable at 1024; the 12-score-plus-rationale object plus any
# leading model chatter needs the same headroom the critic got (#1987).
_MAX_TOKENS_JUDGE = 2048
DEFAULT_CTX_LIMIT = 16384

_JUDGE_SYSTEM = """You are grading two anonymous investment memos written from the SAME research context.
Respond ONLY with a JSON object of this exact shape:
{
  "scores_a": {"numeric_grounding": 1-5, "anchor_discipline": 1-5, "valuation_reasoning": 1-5,
               "risk_balance": 1-5, "internal_consistency": 1-5, "specificity": 1-5},
  "scores_b": {same six keys, 1-5},
  "winner": "A" | "B" | "tie",
  "rationale": "2-3 sentences citing concrete evidence"
}
Grading rules:
- numeric_grounding: penalise ANY figure that does not appear in, or cannot be derived from,
  the CONTEXT. Fabricated or misread financials cap the score at 2.
- anchor_discipline: the buy zone and bear/base/bull values must be coherent with the current
  price anchor in the CONTEXT; a zone wildly disconnected from the live price caps the score at 2.
- internal_consistency: stance, confidence and targets must not contradict each other.
- Judge the MEMOS, not the writing style. Terse but grounded beats fluent but vague.
- If quality is genuinely equivalent, say "tie" — do not force a winner."""


def _build_judge_prompt(context_prompt: str, memo_first: str, memo_second: str) -> str:
    """CONTEXT = the exact writer user-prompt both memos were generated
    from, so the judge can verify every figure against what the writers
    actually saw."""
    return (
        "CONTEXT (identical input both memos were written from):\n"
        "----------------------------------------\n"
        f"{context_prompt}\n"
        "----------------------------------------\n\n"
        f"MEMO A:\n{memo_first}\n\n"
        f"MEMO B:\n{memo_second}\n\n"
        "Grade both memos against the context per your instructions. JSON only."
    )


def _validate_judge_output(data: dict[str, object]) -> None:
    """Raise ValueError unless the judge output matches the contract. Pure; unit-tested."""
    missing = {"scores_a", "scores_b", "winner", "rationale"} - data.keys()
    if missing:
        raise ValueError(f"judge output missing fields: {missing}")
    if data["winner"] not in ("A", "B", "tie"):
        raise ValueError(f"judge output invalid winner: {data['winner']!r}")
    rationale = data["rationale"]
    if not isinstance(rationale, str) or not rationale.strip():
        raise ValueError("judge output rationale must be a non-empty string")
    for side in ("scores_a", "scores_b"):
        scores = data[side]
        if not isinstance(scores, dict):
            raise ValueError(f"judge output {side} must be an object")
        for dim in JUDGE_DIMENSIONS:
            value = scores.get(dim)
            if not isinstance(value, int) or isinstance(value, bool) or not (1 <= value <= 5):
                raise ValueError(f"judge output {side}.{dim} must be an int in [1, 5], got {value!r}")


def pair_writer_outputs(
    results: dict[str, Any],
    model_a: str,
    model_b: str,
) -> list[dict[str, Any]]:
    """Pair writer memos per (symbol, iteration) across two models from one
    ``run --json-out`` payload. Pure; unit-tested. Rounds whose writer
    never passed (parsed=None) drop out — only real memos are judged."""
    pairs: list[dict[str, Any]] = []
    by_key: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}
    for model in (model_a, model_b):
        if model not in results:
            raise SystemExit(f"model {model!r} not present in results file (have: {sorted(results)})")
        by_key[model] = {
            (r["symbol"], int(r.get("iteration", 1))): r["parsed"]
            for r in results[model]["rounds"]
            if r["call"] == "writer" and r.get("parsed") is not None
        }
    for key in sorted(by_key[model_a].keys() & by_key[model_b].keys()):
        pairs.append(
            {
                "symbol": key[0],
                "iteration": key[1],
                "memo_a": by_key[model_a][key],
                "memo_b": by_key[model_b][key],
            }
        )
    return pairs


@dataclass(frozen=True)
class JudgeVerdict:
    """One judged pair: two order-swapped calls, adjudicated."""

    symbol: str
    iteration: int
    # Winner in MODEL terms ("a" | "b" | "tie") — "tie" when the two
    # orderings disagree (position bias) or either call failed.
    winner: str
    agreed: bool  # both orderings produced the same model-level winner
    scores_a: dict[str, float] | None  # mean across both orderings, keyed by dimension
    scores_b: dict[str, float] | None
    rationales: list[str]
    error: str | None = None  # set when either judge call failed (incl. ctx_overflow)


def _model_winner(raw_winner: str, *, a_was_first: bool) -> str:
    """Map the judge's positional A/B verdict back to model terms."""
    if raw_winner == "tie":
        return "tie"
    picked_first = raw_winner == "A"
    return "a" if picked_first == a_was_first else "b"


def adjudicate_pair(
    first_pass: dict[str, object] | None,
    swapped_pass: dict[str, object] | None,
    *,
    symbol: str,
    iteration: int,
    error: str | None = None,
) -> JudgeVerdict:
    """Fold the two order-swapped judge outputs into one verdict. Pure;
    unit-tested. ``first_pass`` saw (a, b); ``swapped_pass`` saw (b, a)."""
    if first_pass is None or swapped_pass is None:
        return JudgeVerdict(
            symbol=symbol,
            iteration=iteration,
            winner="tie",
            agreed=False,
            scores_a=None,
            scores_b=None,
            rationales=[],
            error=error or "judge call failed",
        )
    w1 = _model_winner(str(first_pass["winner"]), a_was_first=True)
    w2 = _model_winner(str(swapped_pass["winner"]), a_was_first=False)
    agreed = w1 == w2
    # Position-mapped per-model scores: pass 1 graded a as A / b as B;
    # pass 2 graded b as A / a as B.
    s1a, s1b = first_pass["scores_a"], first_pass["scores_b"]
    s2b, s2a = swapped_pass["scores_a"], swapped_pass["scores_b"]
    scores_a = {d: (float(s1a[d]) + float(s2a[d])) / 2 for d in JUDGE_DIMENSIONS}  # type: ignore[index]
    scores_b = {d: (float(s1b[d]) + float(s2b[d])) / 2 for d in JUDGE_DIMENSIONS}  # type: ignore[index]
    return JudgeVerdict(
        symbol=symbol,
        iteration=iteration,
        winner=w1 if agreed else "tie",
        agreed=agreed,
        scores_a=scores_a,
        scores_b=scores_b,
        rationales=[str(first_pass["rationale"]), str(swapped_pass["rationale"])],
    )


def aggregate_judgements(verdicts: list[JudgeVerdict]) -> dict[str, Any]:
    """Fold verdicts into the judge report. Pure; unit-tested."""
    scored = [v for v in verdicts if v.scores_a is not None and v.scores_b is not None]
    wins_a = sum(1 for v in verdicts if v.winner == "a")
    wins_b = sum(1 for v in verdicts if v.winner == "b")
    # Real judge ties only — a failed/ctx-overflowed pair is not evidence
    # of equivalence and must not inflate the printed tie rate (review
    # #2004 round 2); it is reported via ``failed`` alone.
    ties = sum(1 for v in verdicts if v.winner == "tie" and v.error is None)
    failed = sum(1 for v in verdicts if v.error is not None)
    mean_a = (
        {
            d: statistics.mean(v.scores_a[d] for v in scored)  # type: ignore[index]
            for d in JUDGE_DIMENSIONS
        }
        if scored
        else {}
    )
    mean_b = (
        {
            d: statistics.mean(v.scores_b[d] for v in scored)  # type: ignore[index]
            for d in JUDGE_DIMENSIONS
        }
        if scored
        else {}
    )
    return {
        "pairs": len(verdicts),
        "judged": len(scored),
        "failed": failed,
        "wins_a": wins_a,
        "wins_b": wins_b,
        "ties": ties,
        "order_agreement_rate": (sum(1 for v in scored if v.agreed) / len(scored)) if scored else None,
        "mean_scores_a": mean_a,
        "mean_scores_b": mean_b,
    }


def _judge_call(
    client: LLMClient,
    *,
    context_prompt: str,
    memo_first: str,
    memo_second: str,
    ctx_limit: int,
) -> tuple[dict[str, object] | None, str | None]:
    """One judge completion: (parsed, error). Retries once on parse/schema
    failure (same shape as production _call_with_one_retry); a transport
    error or ctx overflow ends the call — an overflowed prompt was
    silently truncated server-side and must never be graded."""
    user = _build_judge_prompt(context_prompt, memo_first, memo_second)
    last_error: str | None = None
    for _attempt in range(2):
        try:
            completion = client.complete(system=_JUDGE_SYSTEM, user=user, max_tokens=_MAX_TOKENS_JUDGE)
        except Exception as exc:
            return None, f"transport: {type(exc).__name__}: {exc}"
        if completion.prompt_tokens is not None and completion.prompt_tokens + _MAX_TOKENS_JUDGE > ctx_limit:
            return None, (
                f"ctx_overflow: prompt_tokens={completion.prompt_tokens} + max_tokens={_MAX_TOKENS_JUDGE} "
                f"> ctx_limit={ctx_limit} — response was generated from a truncated prompt"
            )
        try:
            data: object = json.loads(completion.text)
            if not isinstance(data, dict):
                raise ValueError(f"JSON is not an object: {type(data).__name__}")
            _validate_judge_output(data)
            return data, None
        except (json.JSONDecodeError, ValueError) as exc:
            # Head + finish_reason mirror classify_attempt: "unparseable"
            # alone cannot distinguish truncation from chatter (first 14B
            # judge run failed 8/10 with no way to tell which).
            head = completion.text[:160].replace("\n", "\\n")
            last_error = f"invalid judge output (finish_reason={completion.finish_reason}): {exc}; head={head!r}"
    return None, last_error


def run_judge(
    results_path: Path,
    *,
    model_a: str,
    model_b: str,
    judge_model: str,
    base_url: str,
    fixtures_dir: Path,
    ctx_limit: int,
) -> tuple[dict[str, Any], list[JudgeVerdict]]:
    results = json.loads(results_path.read_text())
    pairs = pair_writer_outputs(results, model_a, model_b)
    if not pairs:
        raise SystemExit(f"no judgeable (symbol, iteration) pairs shared by {model_a} and {model_b}")
    contexts = {f["symbol"]: _build_writer_prompt(f["context"]) for f in load_fixtures(fixtures_dir)}

    client = OpenAICompatProvider(base_url=base_url, model=judge_model)
    verdicts: list[JudgeVerdict] = []
    for pair in pairs:
        symbol, iteration = pair["symbol"], pair["iteration"]
        context_prompt = contexts.get(symbol)
        if context_prompt is None:
            verdicts.append(
                adjudicate_pair(None, None, symbol=symbol, iteration=iteration, error=f"no fixture for {symbol}")
            )
            continue
        memo_a = str(pair["memo_a"].get("memo_markdown", ""))
        memo_b = str(pair["memo_b"].get("memo_markdown", ""))
        first_pass, err1 = _judge_call(
            client, context_prompt=context_prompt, memo_first=memo_a, memo_second=memo_b, ctx_limit=ctx_limit
        )
        swapped_pass, err2 = _judge_call(
            client, context_prompt=context_prompt, memo_first=memo_b, memo_second=memo_a, ctx_limit=ctx_limit
        )
        verdict = adjudicate_pair(first_pass, swapped_pass, symbol=symbol, iteration=iteration, error=err1 or err2)
        verdicts.append(verdict)
        print(
            f"  judge {symbol} it{iteration}: winner={verdict.winner} agreed={verdict.agreed}"
            + (f" [{verdict.error}]" if verdict.error else ""),
            flush=True,
        )
    return aggregate_judgements(verdicts), verdicts


def print_judge_report(report: dict[str, Any], *, model_a: str, model_b: str, judge_model: str) -> None:
    print(f"\n=== judge: {model_a} (a) vs {model_b} (b) — judged by {judge_model} ===")
    print(f"pairs: {report['pairs']}  judged: {report['judged']}  failed: {report['failed']}")
    print(f"wins {model_a}: {report['wins_a']}  wins {model_b}: {report['wins_b']}  ties: {report['ties']}")
    agreement = report["order_agreement_rate"]
    print(f"order-swap agreement rate: {agreement:.0%}" if agreement is not None else "order-swap agreement rate: n/a")
    if report["mean_scores_a"]:
        print(f"{'dimension':<22} {model_a:>14} {model_b:>14}")
        for dim in JUDGE_DIMENSIONS:
            print(f"{dim:<22} {report['mean_scores_a'][dim]:>14.2f} {report['mean_scores_b'][dim]:>14.2f}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_capture = sub.add_parser("capture", help="snapshot _assemble_context fixtures from the dev DB")
    p_capture.add_argument("--symbols", nargs="+", default=list(PANEL_SYMBOLS))
    p_capture.add_argument("--out", type=Path, default=FIXTURES_DIR)

    p_run = sub.add_parser("run", help="replay fixtures against one or more models")
    p_run.add_argument("--fixtures-dir", type=Path, default=FIXTURES_DIR)
    p_run.add_argument("--models", nargs="+", required=True)
    p_run.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p_run.add_argument("--iterations", type=int, default=2, help="rounds per fixture (5 fixtures x 2 = 10 gate rounds)")
    p_run.add_argument("--json-out", type=Path, default=None, help="write full per-attempt records + aggregates")
    p_run.add_argument(
        "--critic-model",
        default=None,
        help="run critic rounds on this model instead of each writer model (#1995 split-knob mirror)",
    )
    p_run.add_argument(
        "--gate-model",
        default=None,
        help="model whose spec-§7 go-live gate decides the exit code (others are report-only)",
    )

    p_judge = sub.add_parser("judge", help="content-grade two writers' memos from one run --json-out file (#1995)")
    p_judge.add_argument("--results", type=Path, required=True, help="run --json-out payload holding both models")
    p_judge.add_argument("--model-a", required=True)
    p_judge.add_argument("--model-b", required=True)
    p_judge.add_argument("--judge-model", required=True)
    p_judge.add_argument("--fixtures-dir", type=Path, default=FIXTURES_DIR)
    p_judge.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p_judge.add_argument(
        "--ctx-limit",
        type=int,
        default=DEFAULT_CTX_LIMIT,
        help="server context window; a judge response whose prompt_tokens + max_tokens exceeds this "
        "is a failed comparison (Ollama truncates silently)",
    )
    p_judge.add_argument("--json-out", type=Path, default=None)

    args = parser.parse_args(argv)

    if args.command == "capture":
        return capture(args.symbols, args.out)

    if args.command == "judge":
        report, verdicts = run_judge(
            args.results,
            model_a=args.model_a,
            model_b=args.model_b,
            judge_model=args.judge_model,
            base_url=args.base_url,
            fixtures_dir=args.fixtures_dir,
            ctx_limit=args.ctx_limit,
        )
        print_judge_report(report, model_a=args.model_a, model_b=args.model_b, judge_model=args.judge_model)
        if args.json_out is not None:
            payload = {
                "model_a": args.model_a,
                "model_b": args.model_b,
                "judge_model": args.judge_model,
                "aggregate": report,
                "verdicts": [asdict(v) for v in verdicts],
            }
            args.json_out.write_text(json.dumps(payload, indent=2) + "\n")
            print(f"\nfull judge results -> {args.json_out}")
        # Exit code stays 0 unless nothing could be judged — the verdict is
        # an operator decision input, not a gate.
        return 0 if report["judged"] > 0 else 1

    fixtures = load_fixtures(args.fixtures_dir)
    print(f"{len(fixtures)} fixtures x {args.iterations} iterations x {len(args.models)} models")
    if args.gate_model is not None and args.gate_model not in args.models:
        raise SystemExit(f"--gate-model {args.gate_model!r} not in --models {args.models}")

    reports: dict[str, ModelReport] = {}
    all_rounds: dict[str, list[RoundResult]] = {}
    for model in args.models:
        print(f"\n--- benchmarking {model} ---", flush=True)
        report, rounds = run_model(
            model, fixtures, base_url=args.base_url, iterations=args.iterations, critic_model=args.critic_model
        )
        reports[model] = report
        all_rounds[model] = rounds

    for report in reports.values():
        print_report(report)

    if args.json_out is not None:
        payload = {
            model: {
                "aggregate": {
                    "critic_model": r.critic_model,
                    "writer_rounds": r.writer_rounds,
                    "writer_pass_first": r.writer_pass_first,
                    "writer_pass_retry": r.writer_pass_retry,
                    "critic_rounds": r.critic_rounds,
                    "critic_pass_first": r.critic_pass_first,
                    "critic_pass_retry": r.critic_pass_retry,
                    "critic_length_failures": r.critic_length_failures,
                    "finish_reasons": dict(r.finish_reasons),
                    "enum_ok": r.enum_ok,
                    "enum_checked": r.enum_checked,
                    "gate_passes": r.gate_passes(),
                },
                "rounds": [_round_to_json(rnd) for rnd in all_rounds[model]],
            }
            for model, r in reports.items()
        }
        args.json_out.write_text(json.dumps(payload, indent=2) + "\n")
        print(f"\nfull results -> {args.json_out}")

    if args.gate_model is not None and not reports[args.gate_model].gate_passes():
        gate_report = reports[args.gate_model]
        if gate_report.critic_length_failures > 0:
            reason = f"{gate_report.critic_length_failures} critic length-failure(s)"
        else:
            reason = f"writer with-retry below {GATE_MIN_PASS_RATE:.0%}"
        print(f"\nGATE FAIL: {args.gate_model} — {reason}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
