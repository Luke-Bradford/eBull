"""#3471 slice 2c-iv-c — what the trial's scheduled jobs run (spec §3, §4 / O4, §8).

* **Decision** (``run_decision_job``, 23:30 UTC): uses the run environment resolved once per
  worker process (O4: the ``claude`` binary's absolute path, its ``--version``, the 40-hex git
  sha — the jobs daemon respawns its worker on every ``app/**`` change, so a process is a
  deployment), sweeps orphaned model processes
  left by a crashed run, takes one informational account-risk snapshot and calls
  ``ai_trial_run.run_trial_decision``. Without a frozen declaration it returns before any broker
  call, process sweep or subprocess (§12: scheduling before slice 3 is harmless).
* **Execute** (``run_trial_execution``, 15:00 UTC): every published trial leg whose target session
  is today or earlier and that has no funding decision goes through
  ``ai_trial_executor.execute_trial_signal``, in §7's submission order (``pair_seq`` parity, even =
  arm first); a past-due leg is refused there as ``decision_expired``.
* **Lifecycle**: ``ai_trial_pair_lifecycle.record_pair_lifecycle`` rides the 5-minute paper cycle.
* **Position age**: ``configure_trial_position_managers`` sets the 40-session backstop on both
  legs' paper deployments. Slice 3 calls it when it configures them; nothing configures them yet.

⚠ Both clock jobs refuse to act while the regular session is open or closed respectively, because
each executor refusal is persisted and final (one funding decision per signal):

* the decision job never runs on its target session's own New York date — a boot catch-up at
  05:00 or 14:00 UTC would otherwise decide the CURRENT session from a pack holding its
  pre-market or partial intraday bars. After the close, or on a non-session date, it runs;
* the execute job never runs outside the session — ``market_session_closed`` would otherwise be
  written against a leg that could still have filled that day — nor before 15:00 UTC, where a
  boot catch-up would move the frozen entry time; and it refreshes the halt feed immediately
  before the legs, as the paper cycle does, so a missed feed fire cannot become a final
  ``halt_feed_stale``.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import signal
import subprocess
import threading
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Final

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerProvider
from app.providers.market_data import IntradayBar
from app.services.ai_trial_deadline import TRIAL_ENTRY_TIME_UTC, TRIAL_MAX_POSITION_AGE_SECONDS
from app.services.ai_trial_executor import execute_trial_signal
from app.services.ai_trial_invocation import TRIAL_MODEL_ID, build_argv, build_env
from app.services.ai_trial_pack_reader import INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT, next_us_session
from app.services.ai_trial_run import (
    TRIAL_ARM_STRATEGY_ID,
    TRIAL_STRATEGY_VERSION,
    RunEnvironment,
    RunOutcome,
    load_declaration,
    run_trial_decision,
)
from app.services.market_calendar import latest_completed_us_session
from app.services.strategy_paper_executor import _NY, _session_is_open
from app.services.strategy_position_manager import configure_position_manager
from app.system.git_identity import head_commit

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

_GIT_SHA: Final = re.compile(r"[0-9a-f]{40}")
#: ``ps`` joins argv with single spaces, so the empty ``--tools`` value renders as two spaces
#: (measured 2026-09-28 against CLI 2.1.280).
_ORPHAN_ARGV_PREFIX_LEN: Final = 9
_SUBPROCESS_TIMEOUT_S: Final = 60


class TrialJobError(RuntimeError):
    """The job could not build what the run needs; nothing was claimed."""


# ---------------------------------------------------------------------------
# O4: run environment and orphan sweep
# ---------------------------------------------------------------------------
Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def resolve_run_environment(
    source_env: Mapping[str, str],
    *,
    which: Callable[..., str | None] = shutil.which,
    run: Runner = subprocess.run,
    git_head: Callable[[], str | None] = head_commit,
) -> RunEnvironment:
    """The run's executable (absolute, symlinks resolved), CLI version and git sha (O4).

    Only the §4 allowlisted variables are kept: the model subprocess never sees the job's DB or
    broker environment, and neither does ``claude --version``.
    """
    env = build_env(source_env)
    found = which("claude", path=env["PATH"])
    if not found:
        raise TrialJobError("the claude CLI is not on PATH")
    executable = os.path.realpath(found)
    version = run(
        [executable, "--version"], capture_output=True, text=True, env=env, timeout=_SUBPROCESS_TIMEOUT_S, check=True
    ).stdout.strip()
    if not version:
        raise TrialJobError("claude --version printed nothing")
    # The hook-safe reader (`GIT_*` scrubbed, #2658), never a bare `git` call.
    sha = git_head() or ""
    if not _GIT_SHA.fullmatch(sha):
        raise TrialJobError(f"git rev-parse HEAD returned {sha!r}, not a 40-hex sha")
    return RunEnvironment(executable=executable, cli_version=version, git_sha=sha, source_env=env)


_ENVIRONMENT_LOCK: Final = threading.Lock()
_environment: RunEnvironment | None = None


def verify_run_environment(
    deployed: RunEnvironment, *, resolve: Callable[[Mapping[str, str]], RunEnvironment] = resolve_run_environment
) -> None:
    """Fail closed when the CLI changed under a live worker (a package upgrade in place): the run
    would execute a binary its record does not name. The git sha is not compared — a docs-only
    checkout moves HEAD without respawning the worker, and the loaded code is what ran."""
    current = resolve(deployed.source_env)
    if (current.executable, current.cli_version) != (deployed.executable, deployed.cli_version):
        raise TrialJobError(
            f"the claude CLI changed since deploy ({deployed.executable} {deployed.cli_version!r} → "
            f"{current.executable} {current.cli_version!r}); restart the jobs worker"
        )


def deployed_run_environment() -> RunEnvironment:
    """``resolve_run_environment(os.environ)`` once per worker process (O4: "resolved once at
    deploy time"), so a CLI upgrade or a checkout change mid-life cannot change what a later run
    records or executes. The jobs entrypoint calls it at boot; ``run_decision_job`` re-checks the
    CLI against it before every run (``verify_run_environment``)."""
    global _environment
    with _ENVIRONMENT_LOCK:
        if _environment is None:
            _environment = resolve_run_environment(os.environ)
        return _environment


def orphan_model_pids(ps_output: str) -> list[int]:
    """Trial model processes whose parent has died (re-parented to pid 1).

    Matched on the frozen argv's flags after the executable — the model and the no-tools / no-MCP
    set — so the operator's own ``claude`` sessions never match, and an orphan launched through a
    previous deployment's binary path still does. A live run's child has the worker as its
    parent, so it is never swept.
    """
    argv = build_argv("/claude", model_id=TRIAL_MODEL_ID, system_prompt="", json_schema={})
    flags = " " + " ".join(argv[1:_ORPHAN_ARGV_PREFIX_LEN]) + " "
    pids: list[int] = []
    for line in ps_output.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3 or not parts[0].isdigit() or parts[1] != "1":
            continue
        if flags in parts[2] + " ":
            pids.append(int(parts[0]))
    return pids


def sweep_orphan_model_processes(
    *,
    run: Runner = subprocess.run,
    kill: Callable[[int, int], None] = os.killpg,
) -> int:
    """SIGKILL every orphaned trial model process and its process group (O4: "sweeps orphans at
    the next run"). The CLI was started with ``start_new_session=True``, so its pid is its group
    id and the group kill reaches descendants that have not left it."""
    listing = run(
        ["ps", "-A", "-ww", "-o", "pid=,ppid=,command="],
        capture_output=True,
        text=True,
        timeout=_SUBPROCESS_TIMEOUT_S,
        check=True,
    ).stdout
    killed = 0
    for pid in orphan_model_pids(listing):
        try:
            kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            continue
        logger.warning("ai_trial: killed orphaned model process %s", pid)
        killed += 1
    return killed


# ---------------------------------------------------------------------------
# Decision job
# ---------------------------------------------------------------------------
IntradaySource = Callable[[int, Any, int], Sequence[IntradayBar]]


@dataclass(frozen=True)
class DecisionJobResult:
    #: ``target_session_date`` / ``declaration_missing`` when the job returned before the run,
    #: else the run's own status.
    status: str
    outcome: RunOutcome | None = None
    #: ``None`` when the sweep itself failed; the run still went ahead.
    orphans_killed: int | None = 0

    @property
    def note(self) -> str:
        parts = [f"status={self.status}"]
        if self.outcome is not None:
            if self.outcome.session_date is not None:
                parts.append(f"session={self.outcome.session_date.isoformat()}")
            if self.outcome.run_id is not None:
                parts.append(f"run_id={self.outcome.run_id}")
            if self.outcome.refusal_reason is not None:
                parts.append(f"reason={self.outcome.refusal_reason}")
            parts.append(f"pairs={len(self.outcome.pair_ids)}")
        parts.append(f"orphans_killed={'unknown' if self.orphans_killed is None else self.orphans_killed}")
        return " ".join(parts)


def run_decision_job(
    conn: Conn,
    *,
    broker: BrokerProvider,
    get_intraday_candles: IntradaySource,
    now: datetime | None = None,
    resolve: Callable[[], RunEnvironment] = deployed_run_environment,
    verify: Callable[[RunEnvironment], None] = verify_run_environment,
    sweep: Callable[[], int] = sweep_orphan_model_processes,
    decide: Callable[..., RunOutcome] = run_trial_decision,
) -> DecisionJobResult:
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    # The claim's own target-session rule (`ai_trial_run.claim_run`, §3).
    if next_us_session(latest_completed_us_session(observed)) == observed.astimezone(_NY).date():
        return DecisionJobResult("target_session_date")
    declaration = load_declaration(conn)
    conn.commit()
    if declaration is None:
        return DecisionJobResult("declaration_missing")

    env = resolve()
    # Fail closed BEFORE the claim, whatever raises (a changed CLI, a failed re-resolve): the
    # job run records the failure, no model is called, and the session stays claimable, so a
    # run after the worker restarts can still decide it (up to the target-date guard).
    verify(env)
    # A diagnostics step, not a correctness one: a leftover process cannot publish (the lease
    # refuses a late worker), so a failed sweep is surfaced, never allowed to block the run.
    orphans: int | None
    try:
        orphans = sweep()
    except Exception:
        logger.exception("ai_trial decision job: orphan sweep failed")
        orphans = None
    try:
        risk = broker.get_account_risk_snapshot()
    except Exception:
        # Recorded by the run as `trial_capacity_unavailable:account_risk_unavailable`.
        logger.exception("ai_trial decision job: account-risk snapshot unavailable")
        risk = None

    def fetch_intraday(instrument_id: int) -> Sequence[IntradayBar]:
        return get_intraday_candles(instrument_id, INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT)

    outcome = decide(conn, env=env, risk=risk, fetch_intraday=fetch_intraday)
    return DecisionJobResult(outcome.status, outcome, orphans)


# ---------------------------------------------------------------------------
# Execute job
# ---------------------------------------------------------------------------
_DUE_LEGS_SQL: Final = """
    SELECT l.signal_id
    FROM ai_trial_leg_links l
    JOIN ai_trial_pairs p ON p.pair_id = l.pair_id
    JOIN strategy_signals s ON s.signal_id = l.signal_id
    WHERE s.fill_bar_date <= %s
      AND NOT EXISTS (SELECT 1 FROM strategy_funding_decisions fd WHERE fd.signal_id = l.signal_id)
    ORDER BY s.fill_bar_date, p.declaration_id, p.pair_seq,
             -- §7 submission order: even pair_seq submits the arm first, odd the control.
             CASE WHEN (l.leg = 'arm') = (p.pair_seq %% 2 = 0) THEN 0 ELSE 1 END
"""


def due_trial_legs(conn: Conn, *, today: date) -> list[int]:
    """Published legs with no funding decision whose target session is ``today`` or earlier, in
    §7's submission order. A future session's leg is never returned: the executor would persist
    ``decision_not_yet_due`` against it."""
    return [int(row[0]) for row in conn.execute(_DUE_LEGS_SQL, (today,)).fetchall()]


@dataclass(frozen=True)
class ExecutionJobResult:
    #: ``False`` outside the regular session or before ``TRIAL_ENTRY_TIME_UTC``.
    session_open: bool
    verdicts: Mapping[str, int] = field(default_factory=dict)
    #: Legs whose executor call raised; each was logged and the batch continued.
    errors: int = 0

    @property
    def legs(self) -> int:
        return sum(self.verdicts.values())

    @property
    def note(self) -> str:
        if not self.session_open:
            return "session_closed"
        breakdown = " ".join(f"{k}={v}" for k, v in sorted(self.verdicts.items()))
        errors = f" errors={self.errors}" if self.errors else ""
        return f"legs={self.legs} {breakdown}".rstrip() + errors


def run_trial_execution(
    conn: Conn,
    *,
    broker: BrokerProvider,
    refresh_halts: Callable[[], object],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ExecutionJobResult:
    observed = clock().astimezone(UTC)
    if not _session_is_open(observed) or observed.time() < TRIAL_ENTRY_TIME_UTC:
        return ExecutionJobResult(session_open=False)
    signal_ids = due_trial_legs(conn, today=observed.astimezone(_NY).date())
    conn.commit()
    verdicts: Counter[str] = Counter()
    errors = 0
    if signal_ids:
        refresh_halts()
    for signal_id in signal_ids:
        # A fresh instant per leg: the executor refuses an account snapshot stamped more than 5 s
        # after `now`, so one instant for the batch would make a later leg's fresh snapshot a
        # final `account_risk_stale` (Codex ckpt-2).
        # One leg's unmodelled failure must not skip the rest (#2948): contain it, leave the
        # connection usable, and surface it on the result. The leg has no funding decision,
        # so the next in-session fire retries it or refuses it `decision_expired`.
        try:
            result = execute_trial_signal(conn, broker=broker, signal_id=signal_id, now=clock())
        except Exception:
            logger.exception("ai_trial execute: signal %s raised; continuing with the batch", signal_id)
            if conn.info.transaction_status != TransactionStatus.IDLE:
                conn.rollback()
            errors += 1
            continue
        verdicts[result.verdict] += 1
    return ExecutionJobResult(session_open=True, verdicts=dict(verdicts), errors=errors)


# ---------------------------------------------------------------------------
# Position-age backstop (slice 3 calls this when it configures the deployments)
# ---------------------------------------------------------------------------
def configure_trial_position_managers(conn: Conn, *, updated_by: str) -> dict[str, int]:
    """Set ``max_position_age_seconds`` to the 40-session backstop on both legs' paper deployments
    (§8, r2-53). Refuses unless both exist. Returns each leg's policy revision; the caller commits."""
    rows = conn.execute(
        """
        SELECT strategy_id, deployment_id FROM strategy_deployments
        WHERE strategy_id = ANY(%s) AND strategy_version = %s AND mode = 'paper'
        """,
        ([TRIAL_ARM_STRATEGY_ID, TRIAL_ARM_STRATEGY_ID + "-control"], TRIAL_STRATEGY_VERSION),
    ).fetchall()
    deployments = {str(r[0]): int(r[1]) for r in rows}
    if len(deployments) != 2:
        raise TrialJobError(f"both trial legs need a paper deployment; found {sorted(deployments)}")
    return {
        strategy_id: configure_position_manager(
            conn,
            deployment_id=deployment_id,
            max_position_age_seconds=TRIAL_MAX_POSITION_AGE_SECONDS,
            ratchet_variant_id=None,
            updated_by=updated_by,
            reason="#3471 §8: 40-session position-age backstop above the 20-session horizon",
        )
        for strategy_id, deployment_id in sorted(deployments.items())
    }


__all__ = [
    "DecisionJobResult",
    "ExecutionJobResult",
    "TrialJobError",
    "configure_trial_position_managers",
    "deployed_run_environment",
    "due_trial_legs",
    "orphan_model_pids",
    "resolve_run_environment",
    "run_decision_job",
    "run_trial_execution",
    "sweep_orphan_model_processes",
    "verify_run_environment",
]
