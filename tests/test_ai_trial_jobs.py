"""#3471 slice 2c-iv-c — the trial's jobs, without a database or a broker."""

from __future__ import annotations

import signal
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
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
    DecisionJobResult,
    TrialJobError,
    orphan_model_pids,
    resolve_run_environment,
    run_decision_job,
    run_trial_execution,
    sweep_orphan_model_processes,
)
from app.services.ai_trial_pack_reader import INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT
from app.services.ai_trial_pair_lifecycle import TRIAL_CENSOR_SESSIONS, clock_instant
from app.services.ai_trial_policy import FROZEN_CONSTANTS
from app.services.ai_trial_run import RunEnvironment, RunOutcome
from app.services.ai_trial_version import V1, TrialVersion
from app.services.market_calendar import us_market_status
from app.workers.scheduler import JOB_AI_TRIAL_DECISION_RUN, JOB_AI_TRIAL_EXECUTE, SCHEDULED_JOBS

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


def _decision(
    monkeypatch: pytest.MonkeyPatch, *, now: datetime, declared: bool = True, broker: _Broker | None = None
) -> tuple[DecisionJobResult, dict[str, Any]]:
    seen: dict[str, Any] = {"resolved": 0, "swept": [], "fetched": []}

    def load(_conn: Any, *, version: TrialVersion) -> object | None:
        seen["loaded_version"] = version
        return object() if declared else None

    monkeypatch.setattr(ai_trial_jobs, "load_declaration", load)

    def resolve() -> RunEnvironment:
        seen["resolved"] += 1
        return ENV

    def sweep() -> int:
        seen["swept"].append(True)
        return 2

    def decide(conn: Any, *, env: RunEnvironment, risk: Any, fetch_intraday: Any, version: TrialVersion) -> RunOutcome:
        seen["env"], seen["risk"], seen["version"] = env, risk, version
        fetch_intraday(7)
        return RunOutcome("decided", 11, date(2026, 9, 30), None, (5,))

    def candles(instrument_id: int, interval: Any, count: int) -> list[Any]:
        seen["fetched"].append((instrument_id, interval, count))
        return []

    seen["broker"] = broker or _Broker()
    result = run_decision_job(
        cast(Any, _Conn()),
        broker=cast(BrokerProvider, seen["broker"]),
        get_intraday_candles=candles,
        now=now,
        resolve=resolve,
        verify=lambda env: seen.setdefault("verified", env),
        sweep=sweep,
        decide=decide,
    )
    return result, seen


def test_the_decision_job_runs_after_the_close(monkeypatch: pytest.MonkeyPatch) -> None:
    result, seen = _decision(monkeypatch, now=AFTER_CLOSE)
    assert (result.status, result.orphans_killed) == ("decided", 2)
    assert seen["swept"] == [True] and seen["env"] is ENV and seen["verified"] is ENV and seen["risk"] == "snapshot"
    # v1 by default: the declaration and the run are both v1's (fund-v1 spec §7).
    assert seen["loaded_version"] is V1 and seen["version"] is V1
    assert seen["fetched"] == [(7, INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT)]
    assert result.note == "status=decided session=2026-09-30 run_id=11 pairs=1 orphans_killed=2"


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
    monkeypatch.setattr(ai_trial_jobs, "load_declaration", lambda _conn, **_kw: object())

    def sweep() -> int:
        raise subprocess.CalledProcessError(1, ["ps"])

    result = run_decision_job(
        cast(Any, _Conn()),
        broker=cast(BrokerProvider, _Broker()),
        get_intraday_candles=lambda *_a: [],
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
def test_both_jobs_are_scheduled_on_the_trial_lane_and_invocable() -> None:
    jobs = {job.name: job for job in SCHEDULED_JOBS}
    for name, hour in ((JOB_AI_TRIAL_DECISION_RUN, 23), (JOB_AI_TRIAL_EXECUTE, 15)):
        job = jobs[name]
        assert job.source == "ai_trial" and job.catch_up_on_boot and name in _INVOKERS
        assert (job.cadence.hour, job.cadence.minute) == (hour, 30 if hour == 23 else 0)
