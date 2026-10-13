"""#3620 slice 2a — the condition-4 feasibility look's rule, isolation and preflight, on fixtures only.

Spec: ``docs/research/2026-10-06-3620-cross-asset-tsmom.md`` §"Condition-4 feasibility on W1 (slice 2a)". No
database: nothing here reads a real return.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pytest

from app.services.etf_total_return_reader import EtfVerdict
from app.services.total_return_reader import Month, MonthEnd
from app.services.tsmom_etf import FUNDS, FundCoverage, TsmomRefusal, month_range
from scripts import plan_3620_feasibility as plan

FIRST: Month = (2004, 1)
START: Month = (2005, 1)
END: Month = (2007, 12)


def _universe(drift: Mapping[str, float], last: Month = END) -> dict[str, FundCoverage]:
    """Every fund alternating ±1% around its drift (default 0.2%), so volatilities are positive and equal."""
    out = {}
    for symbol in FUNDS:
        d = drift.get(symbol, 0.002)
        returns = {m: d + (0.01 if i % 2 else -0.01) for i, m in enumerate(month_range(FIRST, last))}
        out[symbol] = FundCoverage(symbol, EtfVerdict.NPORT, FIRST, last, returns)
    return out


RF = {m: 0.0 for m in month_range(FIRST, END)}


def _evaluate(coverage: Mapping[str, FundCoverage], end: Month = END, half: float = 0.001) -> dict[str, object]:
    return plan.evaluate(start=START, w1_end=end, coverage=coverage, rf=RF, entry_half_spread=lambda _s, _m: half)


def test_growth_is_annualised_mean_log_return() -> None:
    assert plan.growth([0.01, -0.02, 0.03]) == pytest.approx(4.0 * (math.log(1.01) + math.log(0.98) + math.log(1.03)))


def test_a_book_at_or_below_b1_is_not_declared() -> None:
    out = _evaluate(_universe({"SPY": 0.02}))
    assert out["verdict"] == plan.NOT_DECLARED
    assert out["g_book"] <= out["g_b1"]  # type: ignore[operator]
    assert out["n"] == len(month_range((2005, 2), END))


def test_a_book_above_b1_continues() -> None:
    # SPY falls (its signal turns off); two other funds rise strongly and carry the book.
    out = _evaluate(_universe({"SPY": -0.02, "GLD": 0.04, "TLT": 0.04}))
    assert out["verdict"] == plan.CONTINUE
    assert out["g_book"] > out["g_b1"]  # type: ignore[operator]


def test_equal_growth_is_not_a_beat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plan, "growth", lambda _returns: 0.05)
    out = _evaluate(_universe({"SPY": -0.02, "GLD": 0.04}))
    assert out["g_book"] == out["g_b1"] == 0.05
    assert out["verdict"] == plan.NOT_DECLARED


def test_fewer_than_twelve_months_refuses_without_a_decision() -> None:
    end: Month = (2005, 12)
    out = plan.evaluate(
        start=START,
        w1_end=end,
        coverage=_universe({}, last=end),
        rf={m: v for m, v in RF.items() if m <= end},
        entry_half_spread=lambda _s, _m: 0.001,
    )
    assert out["verdict"] == plan.REFUSED and "g_book" not in out


def test_a_month_after_w1_end_refuses_before_anything_is_computed() -> None:
    with pytest.raises(TsmomRefusal, match="isolation"):
        _evaluate(_universe({}, last=(2008, 1)))
    with pytest.raises(TsmomRefusal, match="isolation"):
        plan.evaluate(
            start=START,
            w1_end=END,
            coverage=_universe({}),
            rf={**RF, (2008, 1): 0.0},
            entry_half_spread=lambda _s, _m: 0.0,
        )


def test_truncate_drops_every_month_after_the_bound() -> None:
    cut = plan.truncate(_universe({}, last=(2009, 6))["SPY"], END)
    assert max(cut.returns) == END and cut.last_month == END


def test_costs_lower_the_book_relative_to_zero_cost() -> None:
    coverage = _universe({"SPY": -0.02, "GLD": 0.04})
    free = plan.evaluate(start=START, w1_end=END, coverage=coverage, rf=RF, entry_half_spread=lambda _s, _m: 0.0)
    costly = _evaluate(coverage, half=0.01)
    assert costly["g_book"] < free["g_book"]  # type: ignore[operator]


@pytest.mark.parametrize("close", [None, 0.0, -1.0, math.nan])
def test_a_missing_or_unusable_raw_close_refuses(close: float | None) -> None:
    closes = {} if close is None else {"SPY": {(2005, 2): MonthEnd(date(2005, 2, 28), 1.0, close)}}
    with pytest.raises(TsmomRefusal, match="raw_close"):
        plan.band_lookup(closes)("SPY", (2005, 2))


def test_the_band_comes_from_the_fill_month_raw_close() -> None:
    lookup = plan.band_lookup({"SPY": {(2005, 2): MonthEnd(date(2005, 2, 28), 50.0, 120.0)}})
    assert 0.0 < lookup("SPY", (2005, 2)) < 0.01


def test_a_stale_raw_close_refuses() -> None:
    with pytest.raises(TsmomRefusal, match="stale"):
        plan.band_lookup({"SPY": {(2005, 2): MonthEnd(date(2005, 2, 18), 1.0, 120.0)}})("SPY", (2005, 2))


def _closes() -> dict[str, dict[Month, MonthEnd]]:
    """A month-end raw close of 120 for every fund and month (the band is the same everywhere)."""
    out: dict[str, dict[Month, MonthEnd]] = {}
    for symbol in FUNDS:
        out[symbol] = {}
        for m in month_range(FIRST, END):
            last = date(m[0] + m[1] // 12, m[1] % 12 + 1, 1).toordinal() - 1
            out[symbol][m] = MonthEnd(date.fromordinal(last), 120.0, 120.0)
    return out


def test_saved_inputs_reproduce_the_summary_and_a_changed_input_does_not(tmp_path: Path) -> None:
    from app.services.factor_book_declaration import canonical_json

    record = plan.inputs_record(START, END, _universe({"SPY": -0.02, "GLD": 0.04}), RF, _closes())
    summary = plan.evaluate_record(record)
    assert summary["verdict"] == plan.CONTINUE
    output = {"inputs": record, "input_sha256": __import__("hashlib").sha256(canonical_json(record)).hexdigest()}
    path = tmp_path / "out.json"
    path.write_bytes(canonical_json({**output, **summary}))
    assert plan.reproduce(path)
    tampered = json.loads(path.read_text())
    tampered["g_book"] = tampered["g_book"] + 1e-9
    path.write_text(json.dumps(tampered))
    assert not plan.reproduce(path)
    tampered = json.loads(canonical_json({**output, **summary}))
    tampered["inputs"]["rf"][0][1] = 0.5
    path.write_text(json.dumps(tampered))
    assert not plan.reproduce(path)


def _git(status: str = "", head: str = "a", main: str = "a"):  # noqa: ANN202
    def git(*args: str) -> str:
        if args[0] == "status":
            return status
        if args[:2] == ("rev-parse", "HEAD"):
            return head
        if args[:2] == ("rev-parse", "origin/main"):
            return main
        return ""

    return git


@pytest.fixture
def ledgers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(plan, "LEDGER_PATH", tmp_path / "local.jsonl")
    monkeypatch.setattr(plan, "COMMITTED_LEDGER_PATH", tmp_path / "committed.jsonl")
    monkeypatch.setattr(plan, "FEASIBILITY_TRIAL_ID", next(iter(plan.TRIAL_REGISTER.trial_ids)))
    return tmp_path


def test_preflight_returns_head_when_every_condition_holds(ledgers: Path) -> None:
    assert plan.preflight(_git()) == "a"


def test_preflight_refuses_without_the_register_entry(ledgers: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plan, "FEASIBILITY_TRIAL_ID", "no-such-trial")
    with pytest.raises(SystemExit, match="register"):
        plan.preflight(_git())


@pytest.mark.parametrize(("git", "message"), [(_git(status=" M x"), "clean"), (_git(head="b"), "origin/main")])
def test_preflight_refuses_a_dirty_or_moved_checkout(ledgers: Path, git, message: str) -> None:  # noqa: ANN001
    with pytest.raises(SystemExit, match=message):
        plan.preflight(git)


def test_preflight_refuses_a_second_completed_attempt(ledgers: Path) -> None:
    (ledgers / "committed.jsonl").write_text(json.dumps({"event": "completed", "run_id": "x"}) + "\n")
    with pytest.raises(SystemExit, match="already completed"):
        plan.preflight(_git())


def test_preflight_allows_a_retry_after_a_terminal_refusal_but_not_an_unterminated_attempt(ledgers: Path) -> None:
    began = json.dumps({"event": "evaluation_began", "run_id": "x"}) + "\n"
    (ledgers / "local.jsonl").write_text(began + json.dumps({"event": "refused", "run_id": "x"}) + "\n")
    assert plan.preflight(_git()) == "a"
    (ledgers / "local.jsonl").write_text(began)
    with pytest.raises(SystemExit, match="no terminal row"):
        plan.preflight(_git())


def test_a_second_attempt_cannot_take_the_ledger_lock(ledgers: Path) -> None:
    with plan.ledger_lock(), pytest.raises(SystemExit, match="lock"), plan.ledger_lock():
        pass


def test_write_durably_writes_the_bytes(tmp_path: Path) -> None:
    plan.write_durably(tmp_path / "x.json", b"{}\n")
    assert (tmp_path / "x.json").read_bytes() == b"{}\n"


def test_attempt_artefacts_live_under_the_gitignored_var_tree() -> None:
    """Bot PREVENTION (retry-path-untested): an attempt's own artefacts never dirty the checkout."""
    assert plan.LEDGER_PATH.relative_to(plan._REPO_ROOT).parts[0] == "var"
    assert plan.output_path("x").parent == plan.LEDGER_PATH.parent
    assert "/var/*" in (plan._REPO_ROOT / ".gitignore").read_text().splitlines()


def test_a_failure_after_the_output_exists_leaves_the_attempt_unterminated(ledgers: Path) -> None:
    plan.record_failure("x", RuntimeError("before any output"))
    assert [json.loads(line)["event"] for line in (ledgers / "local.jsonl").read_text().splitlines()] == ["failed"]
    plan.output_path("y").write_bytes(b"{")
    plan.record_failure("y", KeyboardInterrupt())
    rows = [json.loads(line) for line in (ledgers / "local.jsonl").read_text().splitlines()]
    assert [row["run_id"] for row in rows] == ["x"]
