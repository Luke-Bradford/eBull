"""#3471 slice 2c-iv-c — what the trial's scheduled jobs run (spec §3, §4 / O4, §8).

* **Decision** (``run_decision_job``, 23:30 UTC): uses the run environment resolved once per
  worker process (O4: the ``claude`` binary's absolute path, its ``--version``, the 40-hex git
  sha — the jobs daemon respawns its worker on every ``app/**`` change, so a process is a
  deployment), sweeps orphaned model processes
  left by a crashed run, takes one informational account-risk snapshot and calls
  ``ai_trial_run.run_trial_decision``. Without a frozen declaration it returns before any broker
  call, process sweep or subprocess (§12: scheduling before slice 3 is harmless). Before the
  claim it brings the shortlist's daily bars up to the last session (``prepare_trial_bars``,
  #3529) and, when none is there yet, returns ``bars_not_ready`` unclaimed for the next fire.
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
from app.providers.market_data import IntradayBar, MarketDataProvider
from app.services.ai_trial_deadline import TRIAL_ENTRY_TIME_UTC, TRIAL_MAX_POSITION_AGE_SECONDS
from app.services.ai_trial_executor import execute_trial_signal
from app.services.ai_trial_invocation import TRIAL_MODEL_ID, build_argv, build_env
from app.services.ai_trial_pack_reader import (
    INTRADAY_INTERVAL,
    INTRADAY_REQUEST_COUNT,
    SCORES_MAX_AGE,
    Step1,
    next_us_session,
    read_bars,
    read_scores_run,
    read_shortlist,
)
from app.services.ai_trial_run import RunEnvironment, RunOutcome, load_declaration, run_trial_decision
from app.services.ai_trial_version import FUND_V1, V1, TrialVersion
from app.services.ai_trial_wind_down import read_v1_declarations, wind_down_refusal
from app.services.market_calendar import latest_completed_us_session
from app.services.market_data import refresh_market_data
from app.services.price_quarantine_store import refresh_price_quarantine
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
class BarReadiness:
    """The shortlist's daily bars before the claim (#3529).

    ``shortlist`` names, of which ``refreshed`` were behind ``last_session`` on the pack's own
    reader and were re-fetched and re-quarantined, and ``current`` reach ``last_session`` after.
    """

    last_session: date
    shortlist: int
    refreshed: int
    current: int

    @property
    def ready(self) -> bool:
        """Zero current names after a post-close fetch means the session's bars are not there yet
        (the provider has not published, or the fetch failed) — a systemic gap, so the run waits.
        A name still behind once others are current is that name's own gap: the pack drops it as
        ``stale_last_bar`` (O1, nothing replenished), exactly as for any other incomplete name.
        An empty shortlist has nothing to wait for; the run records it."""
        return self.shortlist == 0 or self.current > 0

    @property
    def note(self) -> str:
        return (
            f"last_session={self.last_session.isoformat()} shortlist={self.shortlist} "
            f"refreshed={self.refreshed} current={self.current}"
        )


def _current_names(conn: Conn, instrument_ids: Sequence[int], *, last_session: date) -> set[int]:
    """The names whose pack bars (``read_bars``: the masked reader, cut to the latest segment)
    end on ``last_session`` — the same test ``build_bar_series`` applies as ``stale_last_bar``."""
    bars = read_bars(conn, instrument_ids, last_session=last_session)
    conn.commit()
    return {iid for iid, (dates, _rows) in bars.items() if dates and dates[-1] == last_session}


def prepare_trial_bars(
    conn: Conn,
    *,
    as_of: datetime,
    market: MarketDataProvider,
    refresh_candles: Callable[..., object] = refresh_market_data,
    refresh_quarantine: Callable[..., object] = refresh_price_quarantine,
) -> BarReadiness | None:
    """Bring the shortlist's bars to ``last_session`` before the claim (#3529).

    The nightly full-universe candle sweep runs at 03:00 UTC and the quarantine refresh after
    it, so at the 23:30 fire the pack's masked reader can still end every name on the session
    before. Each stale shortlist name is fetched (``refresh_market_data``, bounded by
    ``fresh_through=last_session`` like the nightly sweep) and re-evaluated
    (``refresh_price_quarantine`` over those ids only). Writes only through those two producers;
    nothing here changes what the pack reads or how (``ai_trial_pack_reader`` is a policy module).

    ``None`` when there is no scores run within ``SCORES_MAX_AGE``: there is no shortlist, and
    step 1 records that refusal itself.
    """
    last_session = latest_completed_us_session(as_of)
    run = read_scores_run(conn, as_of=as_of)
    if run is None or as_of - run.scored_at > SCORES_MAX_AGE:
        conn.commit()
        return None
    step1 = Step1(as_of, last_session, next_us_session(last_session), run, None, None)
    names = [(n.instrument_id, n.symbol) for n in read_shortlist(conn, step1=step1).names]
    conn.commit()
    ids = [iid for iid, _ in names]
    current = _current_names(conn, ids, last_session=last_session)
    stale = [(iid, symbol) for iid, symbol in names if iid not in current]
    if stale:
        # `refresh_market_data` needs an autocommit connection for its per-instrument commits
        # (#2269); the job's connection is idle here, so it is switched rather than a second
        # connection opened on a cluster with no headroom.
        conn.autocommit = True
        try:
            refresh_candles(market, conn, stale, skip_quotes=True, fresh_through=last_session)
        finally:
            conn.autocommit = False
        refresh_quarantine(conn, instrument_ids=[iid for iid, _ in stale])
        conn.commit()
        current = _current_names(conn, ids, last_session=last_session)
    return BarReadiness(last_session, len(names), len(stale), len(current))


@dataclass(frozen=True)
class DecisionJobResult:
    #: ``target_session_date`` / ``declaration_missing`` / ``duplicate`` / ``bars_not_ready`` when
    #: the job returned before the run, else the run's own status.
    status: str
    outcome: RunOutcome | None = None
    #: ``None`` when the sweep itself failed; the run still went ahead.
    orphans_killed: int | None = 0
    readiness: BarReadiness | None = None

    @property
    def note(self) -> str:
        parts = [f"status={self.status}"]
        if self.readiness is not None:
            parts.append(self.readiness.note)
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


def session_claimed(conn: Conn, declaration_id: int, session_date: date) -> bool:
    """Whether the session already has a run. A read ahead of the claim so a retry fire after the
    decision costs no broker call or fetch; ``claim_run``'s unique insert stays the authority."""
    row = conn.execute(
        "SELECT 1 FROM ai_trial_runs WHERE declaration_id = %s AND session_date = %s",
        (declaration_id, session_date),
    ).fetchone()
    conn.commit()
    return row is not None


def run_decision_job(
    conn: Conn,
    *,
    broker: BrokerProvider,
    market: MarketDataProvider,
    get_intraday_candles: IntradaySource,
    now: datetime | None = None,
    resolve: Callable[[], RunEnvironment] = deployed_run_environment,
    verify: Callable[[RunEnvironment], None] = verify_run_environment,
    sweep: Callable[[], int] = sweep_orphan_model_processes,
    decide: Callable[..., RunOutcome] = run_trial_decision,
    claimed: Callable[[Conn, int, date], bool] = session_claimed,
    prepare: Callable[..., BarReadiness | None] = prepare_trial_bars,
    version: TrialVersion = V1,
) -> DecisionJobResult:
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    # The claim's own target-session rule (`ai_trial_run.claim_run`, §3).
    session_date = next_us_session(latest_completed_us_session(observed))
    if session_date == observed.astimezone(_NY).date():
        return DecisionJobResult("target_session_date")
    declaration = load_declaration(conn, version=version)
    conn.commit()
    if declaration is None:
        return DecisionJobResult("declaration_missing")
    if claimed(conn, declaration.declaration_id, session_date):
        return DecisionJobResult("duplicate")

    # #3529: the claim is one per session and final, so the shortlist's bars must be there
    # BEFORE it. Not ready → no claim and no model call; the next fire in the window retries.
    # An inactive declaration skips the fetch: the run refuses it right after the claim anyway.
    readiness: BarReadiness | None = None
    if declaration.state == "active":
        readiness = prepare(conn, as_of=observed, market=market)
        if readiness is not None and not readiness.ready:
            return DecisionJobResult("bars_not_ready", readiness=readiness)

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

    outcome = decide(conn, env=env, risk=risk, fetch_intraday=fetch_intraday, version=version)
    return DecisionJobResult(outcome.status, outcome, orphans, readiness)


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


def run_fund_decision_job(conn: Conn, **kwargs: Any) -> DecisionJobResult:
    """fund-v1's decision job (#3515 spec §0 rule 2, §7): v1's job under ``FUND_V1``, behind the
    start gate. Every fire re-checks that v1 is wound down, so after a v1 resumption fund-v1 opens
    nothing new; the gate refuses before the claim, so a refused fire records no run. Residual
    (spec §0 rule 3): the check and a concurrent supervisor action are not atomic."""
    declaration = load_declaration(conn, version=FUND_V1)
    conn.commit()
    if declaration is None:
        return DecisionJobResult("declaration_missing")
    refusal = wind_down_refusal(read_v1_declarations(conn))
    conn.commit()
    if refusal is not None:
        return DecisionJobResult(refusal)
    return run_decision_job(conn, version=FUND_V1, **kwargs)


# ---------------------------------------------------------------------------
# Position-age backstop (slice 3 calls this when it configures the deployments)
# ---------------------------------------------------------------------------
def configure_trial_position_managers(conn: Conn, *, updated_by: str, version: TrialVersion = V1) -> dict[str, int]:
    """Set ``max_position_age_seconds`` to the 40-session backstop on both legs' paper deployments
    (§8, r2-53). Refuses unless both exist. Returns each leg's policy revision; the caller commits."""
    rows = conn.execute(
        """
        SELECT strategy_id, deployment_id FROM strategy_deployments
        WHERE strategy_id = ANY(%s) AND strategy_version = %s AND mode = 'paper'
        """,
        (list(version.leg_strategy_ids), version.strategy_version),
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
    "run_fund_decision_job",
    "run_trial_execution",
    "sweep_orphan_model_processes",
    "verify_run_environment",
]
