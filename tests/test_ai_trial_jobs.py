"""#3471 slice 2c-iv-c — the trial's jobs, without a database or a broker."""

from __future__ import annotations

import signal
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from psycopg.pq import TransactionStatus

from app.jobs.runtime import _INVOKERS
from app.providers.broker import BrokerAccountRiskSnapshot, BrokerProvider
from app.services import ai_trial_jobs
from app.services.ai_trial_deadline import (
    TRIAL_MAX_POSITION_AGE_SECONDS,
    TRIAL_MAX_POSITION_AGE_SESSIONS,
    exit_deadline_session,
)
from app.services.ai_trial_decision import HORIZON_SESSIONS
from app.services.ai_trial_jobs import (
    BarReadiness,
    DecisionJobResult,
    TrialJobError,
    orphan_model_pids,
    prepare_trial_bars,
    resolve_run_environment,
    run_decision_job,
    run_trial_execution,
    sweep_orphan_model_processes,
)
from app.services.ai_trial_pack_reader import INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT, ScoresRun
from app.services.ai_trial_pair_lifecycle import TRIAL_CENSOR_SESSIONS, clock_instant
from app.services.ai_trial_policy import FROZEN_CONSTANTS
from app.services.ai_trial_run import RunEnvironment, RunOutcome, pack_empty_reason
from app.services.ai_trial_version import FUND_V1, V1, TrialVersion
from app.services.market_calendar import us_market_status
from app.workers.scheduler import (
    AI_TRIAL_DECISION_RETRY_MINUTES,
    JOB_AI_TRIAL_DECISION_RUN,
    JOB_AI_TRIAL_EXECUTE,
    JOB_AI_TRIAL_FUND_DECISION_RUN,
    SCHEDULED_JOBS,
    _ai_trial_decision_window_open,
)

EXE = "/opt/homebrew/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe"
SHA = "a" * 40
ENV = RunEnvironment(executable=EXE, cli_version="2.1.280 (Claude Code)", git_sha=SHA, source_env={})
FULL_ENV = {
    "PATH": "/opt/homebrew/bin:/usr/bin",
    "HOME": "/Users/op",
    "USER": "op",
    "LOGNAME": "op",
    "TMPDIR": "/tmp/",
    "DATABASE_URL": "postgresql://secret",
    "ETORO_API_KEY": "secret",
}
# Tuesday 2026-09-29: 23:30 UTC is after the close; 15:00 UTC is in session (11:00 EDT).
AFTER_CLOSE = datetime(2026, 9, 29, 23, 30, tzinfo=UTC)
IN_SESSION = datetime(2026, 9, 29, 15, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# The 40-session age backstop
# ---------------------------------------------------------------------------
def _sessions(start: date, end: date) -> list[date]:
    days = (start + timedelta(days=n) for n in range((end - start).days + 1))
    return [d for d in days if us_market_status(d) != "closed"]


def test_the_age_backstop_outlasts_every_deadline_and_censor_clock_and_never_40_sessions() -> None:
    """r2-53: the declared horizon binds, and so does the §9 censor clock; the age never runs
    past 40 sessions. Checked for every fill session of five calendar years."""
    horizon_max = max(HORIZON_SESSIONS)
    assert horizon_max < TRIAL_MAX_POSITION_AGE_SESSIONS
    age = timedelta(seconds=TRIAL_MAX_POSITION_AGE_SECONDS)
    for fill in _sessions(date(2026, 1, 2), date(2030, 12, 31)):
        opened = datetime.combine(fill, datetime.min.time(), tzinfo=UTC)
        censor = clock_instant(exit_deadline_session(fill, horizon_max), TRIAL_CENSOR_SESSIONS)
        assert censor - opened < age, fill
        forty = exit_deadline_session(fill, TRIAL_MAX_POSITION_AGE_SESSIONS)
        assert opened + age <= datetime.combine(forty, datetime.max.time(), tzinfo=UTC), fill


def test_the_age_backstop_is_hashed_into_the_policy() -> None:
    assert FROZEN_CONSTANTS["ai_trial_deadline.TRIAL_MAX_POSITION_AGE_SECONDS"] == TRIAL_MAX_POSITION_AGE_SECONDS


# ---------------------------------------------------------------------------
# O4: environment, orphans
# ---------------------------------------------------------------------------
def _runner(outputs: dict[str, str], calls: list[dict[str, Any]]) -> Any:
    def run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append({"argv": argv, **kwargs})
        return subprocess.CompletedProcess(argv, 0, stdout=outputs[argv[-1]], stderr="")

    return run


def test_the_environment_resolves_the_real_binary_and_keeps_only_the_allowlist(tmp_path: Path) -> None:
    real = tmp_path / "claude.exe"
    real.write_text("")
    link = tmp_path / "claude"
    link.symlink_to(real)
    calls: list[dict[str, Any]] = []
    env = resolve_run_environment(
        FULL_ENV,
        which=lambda name, path: str(link) if name == "claude" and path == FULL_ENV["PATH"] else None,
        run=_runner({"--version": "2.1.280 (Claude Code)\n"}, calls),
        git_head=lambda: SHA,
    )
    assert env.executable == str(real.resolve())
    assert (env.cli_version, env.git_sha) == ("2.1.280 (Claude Code)", SHA)
    assert set(env.source_env) == {"PATH", "HOME", "USER", "LOGNAME", "TMPDIR"}
    # `claude --version` runs with the allowlisted env too, never the job's.
    assert calls[0]["argv"] == [str(real.resolve()), "--version"] and calls[0]["env"] == env.source_env


@pytest.mark.parametrize(
    ("found", "version", "sha", "message"),
    [
        (None, "v", SHA, "not on PATH"),
        ("/bin/claude", "", SHA, "printed nothing"),
        ("/bin/claude", "v", None, "not a 40-hex sha"),
        ("/bin/claude", "v", "deadbeef", "not a 40-hex sha"),
        ("/bin/claude", "v", SHA.upper(), "not a 40-hex sha"),
    ],
)
def test_the_environment_refuses_what_it_cannot_record(
    found: str | None, version: str, sha: str | None, message: str
) -> None:
    with pytest.raises(TrialJobError, match=message):
        resolve_run_environment(
            FULL_ENV, which=lambda *_a, **_k: found, run=_runner({"--version": version}, []), git_head=lambda: sha
        )


FLAGS = '-p --model claude-opus-5-5 --tools  --strict-mcp-config --mcp-config {"mcpServers":{}}'
PS = "\n".join(
    [
        f"  101     1 {EXE} {FLAGS} --setting-sources  --system-prompt You are ...",
        f"  102   900 {EXE} {FLAGS} --setting-sources  --system-prompt live run",
        f"  103     1 {EXE} --resume abc",
        f"  104     1 {EXE} {FLAGS.replace('--tools ', '--tools Bash')}",
        f"  105     1 /usr/local/bin/claude {FLAGS}",
        f"  106     1 {EXE} {FLAGS.replace('opus-5-5', 'sonnet-5')}",
        " garbage line",
        "",
    ]
)


def test_only_orphaned_trial_model_processes_match() -> None:
    """101 is orphaned; 105 is too, launched through an earlier deployment's binary path; 102 is a
    live run's child; 103, 104 and 106 differ in a pinned flag."""
    assert orphan_model_pids(PS) == [101, 105]


def test_the_sweep_kills_orphans_and_tolerates_one_that_already_exited() -> None:
    killed: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))
        if pid == 107:
            raise ProcessLookupError

    ps = PS + PS.splitlines()[0].replace("  101 ", "  107 ") + "\n"
    calls: list[dict[str, Any]] = []
    assert sweep_orphan_model_processes(run=_runner({"pid=,ppid=,command=": ps}, calls), kill=kill) == 2
    assert killed == [(101, signal.SIGKILL), (105, signal.SIGKILL), (107, signal.SIGKILL)]
    assert calls[0]["argv"] == ["ps", "-A", "-ww", "-o", "pid=,ppid=,command="]


# ---------------------------------------------------------------------------
# The decision job
# ---------------------------------------------------------------------------
class _Conn:
    def commit(self) -> None:
        pass


class _Broker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0

    def get_account_risk_snapshot(self) -> BrokerAccountRiskSnapshot:
        self.calls += 1
        if self.fail:
            raise RuntimeError("503")
        return cast(BrokerAccountRiskSnapshot, "snapshot")


_DECLARATION = SimpleNamespace(declaration_id=16, state="active")
READY = BarReadiness(date(2026, 9, 29), shortlist=50, refreshed=50, current=50)
NOT_READY = BarReadiness(date(2026, 9, 29), shortlist=50, refreshed=50, current=0)


def _decision(
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: datetime,
    declared: bool = True,
    broker: _Broker | None = None,
    state: str = "active",
    claimed: bool = False,
    readiness: BarReadiness | None = READY,
) -> tuple[DecisionJobResult, dict[str, Any]]:
    seen: dict[str, Any] = {"resolved": 0, "swept": [], "fetched": [], "prepared": [], "decided": 0}

    def load(_conn: Any, *, version: TrialVersion) -> object | None:
        seen["loaded_version"] = version
        return SimpleNamespace(declaration_id=16, state=state) if declared else None

    monkeypatch.setattr(ai_trial_jobs, "load_declaration", load)

    def resolve() -> RunEnvironment:
        seen["resolved"] += 1
        return ENV

    def sweep() -> int:
        seen["swept"].append(True)
        return 2

    def decide(conn: Any, *, env: RunEnvironment, risk: Any, fetch_intraday: Any, version: TrialVersion) -> RunOutcome:
        seen["env"], seen["risk"], seen["version"] = env, risk, version
        seen["decided"] += 1
        fetch_intraday(7)
        return RunOutcome("decided", 11, date(2026, 9, 30), None, (5,))

    def candles(instrument_id: int, interval: Any, count: int) -> list[Any]:
        seen["fetched"].append((instrument_id, interval, count))
        return []

    def is_claimed(_conn: Any, declaration_id: int, session_date: date) -> bool:
        seen["claim_checked"] = (declaration_id, session_date)
        return claimed

    def prepare(_conn: Any, *, as_of: datetime, market: Any) -> BarReadiness | None:
        seen["prepared"].append((as_of, market))
        return readiness

    seen["broker"] = broker or _Broker()
    result = run_decision_job(
        cast(Any, _Conn()),
        broker=cast(BrokerProvider, seen["broker"]),
        market=cast(Any, "market"),
        get_intraday_candles=candles,
        now=now,
        resolve=resolve,
        verify=lambda env: seen.setdefault("verified", env),
        sweep=sweep,
        decide=decide,
        claimed=is_claimed,
        prepare=prepare,
    )
    return result, seen


def test_the_decision_job_runs_after_the_close(monkeypatch: pytest.MonkeyPatch) -> None:
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE)
    assert (result.status, result.orphans_killed) == ("decided", 2)
    assert seen["swept"] == [True] and seen["env"] is ENV and seen["verified"] is ENV and seen["risk"] == "snapshot"
    # v1 by default: the declaration and the run are both v1's (fund-v1 spec §7).
    assert seen["loaded_version"] is V1 and seen["version"] is V1
    assert seen["fetched"] == [(7, INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT)]
    # #3529: the bars are prepared for the claim's own target session, before the run.
    assert seen["claim_checked"] == (16, date(2026, 9, 30)) and seen["prepared"] == [(AFTER_CLOSE, "market")]
    assert result.note == (
        "status=decided last_session=2026-09-29 shortlist=50 refreshed=50 current=50 "
        "session=2026-09-30 run_id=11 pairs=1 orphans_killed=2"
    )


def test_the_decision_job_does_nothing_without_a_declaration(monkeypatch: pytest.MonkeyPatch) -> None:
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE, declared=False)
    assert result.status == "declaration_missing"
    assert (seen["resolved"], seen["swept"], seen["broker"].calls, seen["fetched"]) == (0, [], 0, [])


@pytest.mark.parametrize(
    "now",
    [
        IN_SESSION,
        datetime(2026, 9, 30, 5, 0, tzinfo=UTC),  # 01:00 EDT on the target session's own date
        datetime(2026, 9, 29, 19, 59, tzinfo=UTC),  # 15:59 EDT, a minute before the close
    ],
)
def test_the_decision_job_never_runs_on_the_target_session_date(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    result, seen = _decision(monkeypatch, now=now)
    assert result == DecisionJobResult("target_session_date")
    assert (seen["resolved"], seen["broker"].calls) == (0, 0)


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 9, 30, 3, 59, tzinfo=UTC),  # 23:59 EDT on the last completed session's date
        datetime(2026, 10, 3, 12, 0, tzinfo=UTC),  # Saturday: the target is Monday
    ],
)
def test_a_late_catch_up_still_runs_before_the_target_date(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    assert _decision(monkeypatch, now=now)[0].status == "decided"


def test_a_failed_orphan_sweep_is_surfaced_and_never_blocks_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ai_trial_jobs, "load_declaration", lambda _conn, **_kw: _DECLARATION)

    def sweep() -> int:
        raise subprocess.CalledProcessError(1, ["ps"])

    result = run_decision_job(
        cast(Any, _Conn()),
        broker=cast(BrokerProvider, _Broker()),
        market=cast(Any, "market"),
        get_intraday_candles=lambda *_a: [],
        claimed=lambda *_a: False,
        prepare=lambda _conn, **_kw: READY,
        now=AFTER_CLOSE,
        resolve=lambda: ENV,
        verify=lambda _env: None,
        sweep=sweep,
        decide=lambda *_a, **_k: RunOutcome("decided", 1, date(2026, 9, 30)),
    )
    assert (result.status, result.orphans_killed) == ("decided", None)
    assert result.note.endswith("orphans_killed=unknown")


def test_an_unavailable_risk_snapshot_reaches_the_run_as_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """The run records it as `trial_capacity_unavailable:account_risk_unavailable`."""
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE, broker=_Broker(fail=True))
    assert result.status == "decided" and seen["risk"] is None


# ---------------------------------------------------------------------------
# #3529: bar readiness before the claim
# ---------------------------------------------------------------------------
def test_bars_not_ready_returns_before_the_claim_and_a_later_fire_decides(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stale bars at fire time → no claim, no broker call, no model; a named not-ready result.
    A later fire in the window with the bars in claims and decides."""
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE, readiness=NOT_READY)
    assert result.status == "bars_not_ready" and result.outcome is None
    assert (seen["decided"], seen["resolved"], seen["swept"], seen["broker"].calls) == (0, 0, [], 0)
    assert result.note == (
        "status=bars_not_ready last_session=2026-09-29 shortlist=50 refreshed=50 current=0 orphans_killed=0"
    )
    later, seen = _decision(monkeypatch, now=AFTER_CLOSE + timedelta(minutes=30), readiness=READY)
    assert later.status == "decided" and seen["decided"] == 1


@pytest.mark.parametrize(
    ("readiness", "ready"),
    [
        (BarReadiness(date(2026, 9, 29), shortlist=50, refreshed=3, current=49), True),  # one name's own gap
        (BarReadiness(date(2026, 9, 29), shortlist=50, refreshed=50, current=0), False),  # nothing published
        (BarReadiness(date(2026, 9, 29), shortlist=0, refreshed=0, current=0), True),  # the run records it
        (None, True),  # no scores run: step 1 refuses it after the claim, as before
    ],
)
def test_only_a_systemic_gap_holds_the_claim(
    monkeypatch: pytest.MonkeyPatch, readiness: BarReadiness | None, ready: bool
) -> None:
    result, _seen = _decision(monkeypatch, now=AFTER_CLOSE, readiness=readiness)
    assert result.status == ("decided" if ready else "bars_not_ready")


def test_a_claimed_session_returns_before_any_fetch_or_broker_call(monkeypatch: pytest.MonkeyPatch) -> None:
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE, claimed=True)
    assert result.status == "duplicate"
    assert (seen["prepared"], seen["resolved"], seen["broker"].calls, seen["decided"]) == ([], 0, 0, 0)


def test_an_inactive_declaration_skips_the_fetch_and_reaches_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The run refuses ``trial_not_active`` itself; fetching bars for it would be wasted calls."""
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE, state="halted", readiness=NOT_READY)
    assert (result.status, seen["prepared"], seen["decided"]) == ("decided", [], 1)


class _PrepConn:
    def __init__(self) -> None:
        self._autocommit = False
        self.commits = 0
        self.rollbacks = 0
        self.status = TransactionStatus.IDLE

    @property
    def info(self) -> Any:
        return SimpleNamespace(transaction_status=self.status)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        # psycopg refuses the switch mid-transaction.
        if self.status != TransactionStatus.IDLE:
            raise RuntimeError("can't change autocommit inside a transaction")
        self._autocommit = value

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1
        self.status = TransactionStatus.IDLE


def _prepare(
    monkeypatch: pytest.MonkeyPatch,
    *,
    before: set[int],
    after: set[int],
    scored_at: datetime | None = AFTER_CLOSE,
    fetch_raises: bool = False,
) -> tuple[BarReadiness | None, dict[str, Any]]:
    """Shortlist ids 1-3; ``before`` / ``after`` are the names current through 09-29 on the
    pack's reader before and after the refresh."""
    seen: dict[str, Any] = {"reads": 0}
    last = date(2026, 9, 29)
    monkeypatch.setattr(
        ai_trial_jobs,
        "read_scores_run",
        lambda _conn, *, as_of: None if scored_at is None else ScoresRun("v1.5-balanced", scored_at),
    )
    names = [SimpleNamespace(instrument_id=i, symbol=f"S{i}") for i in (1, 2, 3)]
    monkeypatch.setattr(ai_trial_jobs, "read_shortlist", lambda _conn, *, step1: SimpleNamespace(names=names))

    def bars(_conn: Any, ids: list[int], *, last_session: date) -> dict[int, tuple[list[date], list[Any]]]:
        assert last_session == last
        current = before if seen["reads"] == 0 else after
        seen["reads"] += 1
        return {i: ([last if i in current else date(2026, 9, 28)], [{}]) for i in ids}

    monkeypatch.setattr(ai_trial_jobs, "read_bars", bars)
    conn = _PrepConn()

    def candles(market: Any, c: Any, instruments: list[tuple[int, str]], **kwargs: Any) -> None:
        # #2269: per-instrument commits need autocommit for the fetch, and only for it.
        seen["candles"] = (market, c.autocommit, instruments, kwargs)
        if fetch_raises:
            c.status = TransactionStatus.INTRANS
            raise RuntimeError("upstream unreachable")

    def quarantine(c: Any, *, instrument_ids: list[int], as_of: date) -> None:
        seen["quarantine"] = (c.autocommit, instrument_ids, as_of)

    result = prepare_trial_bars(
        cast(Any, conn),
        as_of=AFTER_CLOSE,
        market=cast(Any, "market"),
        refresh_candles=candles,
        refresh_quarantine=quarantine,
    )
    seen["autocommit_after"] = conn.autocommit
    seen["rollbacks"] = conn.rollbacks
    return result, seen


def test_prepare_refreshes_only_the_stale_names_then_rereads() -> None:
    with pytest.MonkeyPatch.context() as mp:
        result, seen = _prepare(mp, before={1}, after={1, 2, 3})
    assert result == BarReadiness(date(2026, 9, 29), shortlist=3, refreshed=2, current=3)
    assert seen["candles"] == (
        "market",
        True,
        [(2, "S2"), (3, "S3")],
        {"skip_quotes": True, "fresh_through": date(2026, 9, 29)},
    )
    # The provisional window follows the decision instant's UTC date, not the host's local date.
    assert seen["quarantine"] == (False, [2, 3], date(2026, 9, 29))
    assert seen["autocommit_after"] is False and seen["reads"] == 2


def test_a_failed_fetch_is_not_ready_and_restores_the_connection() -> None:
    """A systemic fetch failure is retryable (`bars_not_ready`), never a job error, even when the
    names already current would otherwise make the run ready; the open transaction it left is
    rolled back so the connection leaves autocommit cleanly."""
    with pytest.MonkeyPatch.context() as mp:
        result, seen = _prepare(mp, before={1}, after={1}, fetch_raises=True)
    assert result == BarReadiness(date(2026, 9, 29), shortlist=3, refreshed=2, current=1, fetch_failed=True)
    assert result is not None and not result.ready and result.note.endswith("current=1 fetch_failed=1")
    assert (seen["autocommit_after"], seen["rollbacks"]) == (False, 1)
    # Nothing is re-evaluated or re-read on a path that is already not ready.
    assert "quarantine" not in seen and seen["reads"] == 1


def test_prepare_does_nothing_when_every_name_is_current() -> None:
    with pytest.MonkeyPatch.context() as mp:
        result, seen = _prepare(mp, before={1, 2, 3}, after=set())
    assert result == BarReadiness(date(2026, 9, 29), shortlist=3, refreshed=0, current=3)
    assert "candles" not in seen and "quarantine" not in seen and seen["reads"] == 1


def test_prepare_is_not_ready_when_the_refresh_lands_nothing() -> None:
    with pytest.MonkeyPatch.context() as mp:
        result, _seen = _prepare(mp, before=set(), after=set())
    assert result is not None and not result.ready


@pytest.mark.parametrize("scored_at", [None, AFTER_CLOSE - timedelta(days=3, seconds=1)])
def test_prepare_has_no_shortlist_without_a_fresh_scores_run(scored_at: datetime | None) -> None:
    with pytest.MonkeyPatch.context() as mp:
        result, seen = _prepare(mp, before=set(), after=set(), scored_at=scored_at)
    assert result is None and seen["reads"] == 0


@pytest.mark.parametrize(
    ("incomplete", "expected"),
    [
        ({}, "pack_empty"),
        ({"A": "too_few_bars", "B": "stale_last_bar"}, "pack_empty:stale_last_bar"),  # tie → first by name
        ({"A": "too_few_bars", "B": "too_few_bars", "C": "stale_last_bar"}, "pack_empty:too_few_bars"),
    ],
)
def test_the_pack_empty_reason_names_the_dominant_incomplete_reason(incomplete: dict[str, str], expected: str) -> None:
    assert pack_empty_reason(incomplete) == expected


# ---------------------------------------------------------------------------
# The execute job
# ---------------------------------------------------------------------------
def test_the_execute_job_submits_due_legs_in_order_only_in_session(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[date] = []
    executed: list[tuple[int, datetime]] = []
    verdicts = {1: "submitted", 2: "rejected", 3: "submitted"}

    def due(_conn: Any, *, today: date) -> list[int]:
        asked.append(today)
        return [1, 2, 3]

    def execute(_conn: Any, *, broker: Any, signal_id: int, now: datetime) -> Any:
        executed.append((signal_id, now))
        return type("R", (), {"verdict": verdicts[signal_id]})()

    monkeypatch.setattr(ai_trial_jobs, "due_trial_legs", due)
    monkeypatch.setattr(ai_trial_jobs, "execute_trial_signal", execute)

    refreshed: list[int] = []

    def run(clock: Any) -> Any:
        return run_trial_execution(
            cast(Any, _Conn()),
            broker=cast(BrokerProvider, None),
            refresh_halts=lambda: refreshed.append(len(executed)),
            clock=clock,
        )

    # After the close, and in session but before 15:00 UTC (a boot catch-up): nothing is read.
    for instant in (AFTER_CLOSE, IN_SESSION - timedelta(minutes=1)):
        closed = run(lambda instant=instant: instant)
        assert (closed.session_open, closed.note, asked, executed, refreshed) == (False, "session_closed", [], [], [])

    ticks = iter(IN_SESSION + timedelta(seconds=n) for n in range(10))
    result = run(lambda: next(ticks))
    assert asked == [date(2026, 9, 29)]  # the New York date
    # Each leg gets its own instant, never the batch's start (Codex ckpt-2).
    assert executed == [
        (1, IN_SESSION + timedelta(seconds=1)),
        (2, IN_SESSION + timedelta(seconds=2)),
        (3, IN_SESSION + timedelta(seconds=3)),
    ]
    assert (result.legs, result.note) == (3, "legs=3 rejected=1 submitted=2")
    assert refreshed == [0]  # once, before the first leg


@pytest.mark.parametrize(
    ("current", "ok"),
    [
        (ENV, True),
        (RunEnvironment(EXE, ENV.cli_version, "b" * 40, {}), True),  # a docs-only HEAD move
        (RunEnvironment(EXE, "2.1.281 (Claude Code)", SHA, {}), False),
        (RunEnvironment("/other/claude.exe", ENV.cli_version, SHA, {}), False),
    ],
)
def test_a_cli_changed_under_the_worker_fails_closed(current: RunEnvironment, ok: bool) -> None:
    if ok:
        ai_trial_jobs.verify_run_environment(ENV, resolve=lambda _env: current)
    else:
        with pytest.raises(TrialJobError, match="changed since deploy"):
            ai_trial_jobs.verify_run_environment(ENV, resolve=lambda _env: current)


class _TxConn:
    def __init__(self) -> None:
        self.rollbacks = 0
        self.info = type("Info", (), {"transaction_status": TransactionStatus.INERROR})()

    commit_fails = False

    def commit(self) -> None:
        if self.commit_fails:
            raise RuntimeError("commit failed")

    def rollback(self) -> None:
        self.rollbacks += 1


def test_one_leg_raising_does_not_skip_the_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    """#2948: an unmodelled failure on the FIRST leg; the others still execute."""
    executed: list[int] = []

    def execute(_conn: Any, *, broker: Any, signal_id: int, now: datetime) -> Any:
        if signal_id == 1:
            raise RuntimeError("unmodelled transport error")
        executed.append(signal_id)
        return type("R", (), {"verdict": "submitted"})()

    monkeypatch.setattr(ai_trial_jobs, "due_trial_legs", lambda _conn, *, today: [1, 2, 3])
    monkeypatch.setattr(ai_trial_jobs, "execute_trial_signal", execute)
    conn = _TxConn()
    result = run_trial_execution(
        cast(Any, conn), broker=cast(BrokerProvider, None), refresh_halts=lambda: None, clock=lambda: IN_SESSION
    )
    assert executed == [2, 3] and conn.rollbacks == 1
    assert (result.legs, result.errors, result.note) == (2, 1, "legs=2 submitted=2 errors=1")


@pytest.mark.parametrize(("fails", "commit_fails"), [(False, False), (True, False), (False, True)])
def test_the_paper_cycle_lifecycle_step_is_contained(
    monkeypatch: pytest.MonkeyPatch, fails: bool, commit_fails: bool
) -> None:
    from app.services import ai_trial_pair_lifecycle
    from app.workers import scheduler

    def record(_conn: Any) -> int:
        if fails:
            raise RuntimeError("boom")
        return 3

    monkeypatch.setattr(ai_trial_pair_lifecycle, "record_pair_lifecycle", record)
    conn = _TxConn()
    conn.commit_fails = commit_fails
    failed = fails or commit_fails
    assert scheduler._record_trial_pair_lifecycle(cast(Any, conn)) == (None if failed else 3)
    assert conn.rollbacks == (1 if failed else 0)


@pytest.mark.parametrize("outcome", ["ok", "raises", "one_failed"])
def test_the_paper_cycle_halt_step_is_contained(monkeypatch: pytest.MonkeyPatch, outcome: str) -> None:
    from app.services import ai_trial_halts
    from app.workers import scheduler

    def enforce(_conn: Any) -> list[ai_trial_halts.HaltCheck]:
        if outcome == "raises":
            raise RuntimeError("boom")
        checks = [ai_trial_halts.HaltCheck(7, "halted_loss", 1), ai_trial_halts.HaltCheck(8, None, 2)]
        if outcome == "one_failed":
            checks.append(ai_trial_halts.HaltCheck(9, None, 0, failed=True))
        return checks

    monkeypatch.setattr(ai_trial_halts, "enforce_trial_halts", enforce)
    conn = _TxConn()
    note = scheduler._enforce_trial_halts(cast(Any, conn))
    assert note == ("checked=2 halted=7:halted_loss unmeasured=3" if outcome == "ok" else None)
    # A declaration that failed inside its savepoint does not roll back the others' halts.
    assert conn.rollbacks == (1 if outcome == "raises" else 0)


def test_the_deployed_environment_is_resolved_once_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(ai_trial_jobs, "_environment", None)
    monkeypatch.setattr(ai_trial_jobs, "resolve_run_environment", lambda source: calls.append(source) or ENV)
    assert ai_trial_jobs.deployed_run_environment() is ENV
    assert ai_trial_jobs.deployed_run_environment() is ENV
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------
def test_the_trial_jobs_are_scheduled_on_the_trial_lane_and_invocable() -> None:
    jobs = {job.name: job for job in SCHEDULED_JOBS}
    decision = jobs[JOB_AI_TRIAL_DECISION_RUN]
    # #3529: retried through the window; :00 and :30 only, so never on the fund job's 23:45.
    assert decision.cadence.kind == "every_n_minutes" and decision.cadence.interval_minutes == 30
    assert decision.source == "ai_trial" and decision.catch_up_on_boot and JOB_AI_TRIAL_DECISION_RUN in _INVOKERS
    assert decision.misfire_grace_seconds is not None
    assert decision.misfire_grace_seconds < AI_TRIAL_DECISION_RETRY_MINUTES * 60
    for name, (hour, minute) in (
        (JOB_AI_TRIAL_FUND_DECISION_RUN, (23, 45)),
        (JOB_AI_TRIAL_EXECUTE, (15, 0)),
    ):
        job = jobs[name]
        assert job.source == "ai_trial" and job.catch_up_on_boot and name in _INVOKERS
        assert (job.cadence.hour, job.cadence.minute) == (hour, minute)


# ---------------------------------------------------------------------------
# fund-v1's decision job (#3515 spec §0 rule 2)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("declared", "refusal", "expected"),
    [
        (False, None, "declaration_missing"),
        (True, "v1_not_wound_down:trial_active", "v1_not_wound_down:trial_active"),
        (True, None, "decided"),
    ],
)
def test_the_fund_job_runs_only_with_its_declaration_and_v1_wound_down(
    monkeypatch: pytest.MonkeyPatch, declared: bool, refusal: str | None, expected: str
) -> None:
    seen: dict[str, Any] = {}

    def load(_conn: Any, *, version: TrialVersion) -> object | None:
        seen["loaded"] = version
        return object() if declared else None

    def gate(decls: Any) -> str | None:
        seen["gated"] = decls
        return refusal

    def run(_conn: Any, *, version: TrialVersion, **kwargs: Any) -> ai_trial_jobs.DecisionJobResult:
        seen["run"] = (version, kwargs)
        return ai_trial_jobs.DecisionJobResult("decided")

    monkeypatch.setattr(ai_trial_jobs, "load_declaration", load)
    monkeypatch.setattr(ai_trial_jobs, "read_v1_declarations", lambda _conn: ["v1 rows"])
    monkeypatch.setattr(ai_trial_jobs, "wind_down_refusal", gate)
    monkeypatch.setattr(ai_trial_jobs, "run_decision_job", run)
    result = ai_trial_jobs.run_fund_decision_job(cast(Any, _Conn()), broker="b", get_intraday_candles="c")
    assert result.status == expected
    assert seen["loaded"] is FUND_V1
    # The gate reads v1's rows on every fire that has a declaration, before any claim.
    assert seen.get("gated") == (["v1 rows"] if declared else None)
    assert seen.get("run") == (
        (FUND_V1, {"broker": "b", "get_intraday_candles": "c"}) if expected == "decided" else None
    )


@pytest.mark.parametrize(
    ("now", "open_"),
    [
        (datetime(2026, 9, 30, 23, 29, tzinfo=UTC), False),  # before the frozen first fire
        (datetime(2026, 9, 30, 23, 30, tzinfo=UTC), True),
        (datetime(2026, 10, 1, 3, 59, tzinfo=UTC), True),  # 23:59 EDT
        (datetime(2026, 10, 1, 4, 0, tzinfo=UTC), False),  # New York midnight in EDT
        (datetime(2026, 11, 3, 4, 30, tzinfo=UTC), True),  # 23:30 EST: still the evening
        (datetime(2026, 11, 3, 5, 0, tzinfo=UTC), False),  # New York midnight in EST
        (datetime(2026, 10, 1, 15, 0, tzinfo=UTC), False),
    ],
)
def test_the_decision_window_opens_at_the_frozen_fire_and_closes_by_new_york_midnight(
    now: datetime, open_: bool
) -> None:
    assert _ai_trial_decision_window_open(now) is open_
