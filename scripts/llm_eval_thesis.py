"""Thesis LLM eval harness (#1919 PR-C, spec §7).

Benchmarks the PRODUCTION writer+critic path (prompts, validators, token
budgets imported from ``app.services.thesis`` — never copied, so the
benchmark cannot drift from what ``generate_thesis`` actually sends)
against an OpenAI-compatible endpoint, replaying fixed
``_assemble_context`` fixtures so every model sees identical prompts.

Two subcommands:

* ``capture`` — snapshot ``_assemble_context`` output from the dev DB for
  the house panel (AAPL, GME, MSFT, JPM, HD) into
  ``tests/fixtures/llm_eval/<symbol>.json``. Dev-guarded (#1765): refuses
  to run outside a local dev environment.
* ``run`` — for each fixture x ``--iterations``: writer attempt, one
  retry on failure (mirrors ``_call_with_one_retry``), then a critic
  round against the successful memo. Reports JSON-schema pass rate
  (first-attempt and with-retry), enum validity, finish_reason mix,
  tok/s + wall-clock per call.

Go-live gate (spec §7): >=9/10 writer passes WITH retry on the chosen
local model. ``--gate-model <model>`` makes the exit code enforce it.

Usage:
    PYTHONPATH=. uv run python scripts/llm_eval_thesis.py capture
    PYTHONPATH=. uv run python scripts/llm_eval_thesis.py run \
        --models qwen3:14b deepseek-r1:14b --gate-model qwen3:14b \
        --json-out /tmp/llm_eval_results.json
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
    """One fixture round: up to two attempts (prod retry-once shape)."""

    symbol: str
    call: str  # "writer" | "critic"
    attempts: list[AttemptResult]
    parsed: dict[str, object] | None = None

    @property
    def pass_first(self) -> bool:
        return bool(self.attempts) and self.attempts[0].ok

    @property
    def pass_with_retry(self) -> bool:
        return any(a.ok for a in self.attempts)


@dataclass
class ModelReport:
    """Aggregate for one model."""

    model: str
    writer_rounds: int = 0
    writer_pass_first: int = 0
    writer_pass_retry: int = 0
    critic_rounds: int = 0
    critic_pass_first: int = 0
    critic_pass_retry: int = 0
    finish_reasons: Counter[str] = field(default_factory=Counter)
    enum_checked: int = 0
    enum_ok: int = 0
    durations_s: list[float] = field(default_factory=list)
    tok_s: list[float] = field(default_factory=list)

    @property
    def writer_pass_retry_rate(self) -> float:
        return self.writer_pass_retry / self.writer_rounds if self.writer_rounds else 0.0

    def gate_passes(self) -> bool:
        return self.writer_rounds >= GATE_MIN_ROUNDS and self.writer_pass_retry_rate >= GATE_MIN_PASS_RATE


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
    return RoundResult(symbol=symbol, call=call, attempts=attempts, parsed=parsed)


def aggregate(model: str, rounds: list[RoundResult]) -> ModelReport:
    """Fold round results into the per-model report. Pure; unit-tested."""
    report = ModelReport(model=model)
    for rnd in rounds:
        if rnd.call == "writer":
            report.writer_rounds += 1
            report.writer_pass_first += rnd.pass_first
            report.writer_pass_retry += rnd.pass_with_retry
        else:
            report.critic_rounds += 1
            report.critic_pass_first += rnd.pass_first
            report.critic_pass_retry += rnd.pass_with_retry
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
) -> tuple[ModelReport, list[RoundResult]]:
    client = OpenAICompatProvider(base_url=base_url, model=model)
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
            )
            rounds.append(writer)
            _print_round(model, iteration, writer)
            if writer.parsed is None:
                continue  # prod path: no memo, no critic call
            memo = writer.parsed.get("memo_markdown")
            if not isinstance(memo, str):  # unreachable post-validate; keep pyright honest
                continue
            critic = run_round(
                client,
                symbol=symbol,
                call="critic",
                system=_CRITIC_SYSTEM,
                user=_build_critic_prompt(memo, context),
                max_tokens=_MAX_TOKENS_CRITIC,
            )
            rounds.append(critic)
            _print_round(model, iteration, critic)
    return aggregate(model, rounds), rounds


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
    print(f"\n=== {report.model} ===")
    print(f"writer pass (first attempt): {_fmt_rate(report.writer_pass_first, report.writer_rounds)}")
    print(f"writer pass (with retry):    {_fmt_rate(report.writer_pass_retry, report.writer_rounds)}")
    print(f"critic pass (first attempt): {_fmt_rate(report.critic_pass_first, report.critic_rounds)}")
    print(f"critic pass (with retry):    {_fmt_rate(report.critic_pass_retry, report.critic_rounds)}")
    print(f"enum validity (parsed outputs): {_fmt_rate(report.enum_ok, report.enum_checked)}")
    print(f"finish_reason mix: {dict(report.finish_reasons)}")
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
    print(f"go-live gate (writer with-retry >= {GATE_MIN_PASS_RATE:.0%} over >= {GATE_MIN_ROUNDS} rounds): {gate}")


def _round_to_json(rnd: RoundResult) -> dict[str, object]:
    return {
        "symbol": rnd.symbol,
        "call": rnd.call,
        "attempts": [asdict(a) | {"tok_s": a.tok_s} for a in rnd.attempts],
    }


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
        "--gate-model",
        default=None,
        help="model whose spec-§7 go-live gate decides the exit code (others are report-only)",
    )

    args = parser.parse_args(argv)

    if args.command == "capture":
        return capture(args.symbols, args.out)

    fixtures = load_fixtures(args.fixtures_dir)
    print(f"{len(fixtures)} fixtures x {args.iterations} iterations x {len(args.models)} models")
    if args.gate_model is not None and args.gate_model not in args.models:
        raise SystemExit(f"--gate-model {args.gate_model!r} not in --models {args.models}")

    reports: dict[str, ModelReport] = {}
    all_rounds: dict[str, list[RoundResult]] = {}
    for model in args.models:
        print(f"\n--- benchmarking {model} ---", flush=True)
        report, rounds = run_model(model, fixtures, base_url=args.base_url, iterations=args.iterations)
        reports[model] = report
        all_rounds[model] = rounds

    for report in reports.values():
        print_report(report)

    if args.json_out is not None:
        payload = {
            model: {
                "aggregate": {
                    "writer_rounds": r.writer_rounds,
                    "writer_pass_first": r.writer_pass_first,
                    "writer_pass_retry": r.writer_pass_retry,
                    "critic_rounds": r.critic_rounds,
                    "critic_pass_first": r.critic_pass_first,
                    "critic_pass_retry": r.critic_pass_retry,
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
        print(f"\nGATE FAIL: {args.gate_model} writer with-retry below {GATE_MIN_PASS_RATE:.0%}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
