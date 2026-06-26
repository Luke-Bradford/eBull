"""Shared bounded-concurrency driver for universe-wide SEC filer sweeps (#1274).

The 13F (#913) and N-PORT (#917) sweeps walk thousands of filer CIKs. The
legacy ``for cik in ciks:`` loop ran each filer's full fetch→parse→write
pipeline to completion before the next started, using only ~5-10% of the SEC
10 req/s budget. This module fans those pipelines out across a thread pool
that **shares one process-global SEC rate gate** — the gate
(``app/providers/rate_gate.py::InProcessFloorGate``), not the worker count,
bounds the aggregate request rate, so N concurrent pipelines saturate the
budget without exceeding it.

Two pieces, deliberately split so the orchestration is unit-testable without a
database:

* :func:`make_filer_runner` — wraps a per-filer ``work(conn, cik)`` into a
  self-contained, never-raising unit that opens its OWN connection (psycopg
  conns are not thread-safe) and owns its filer's transaction.
* :func:`drain_filers_concurrently` — pure sliding-window dispatch: de-dupe,
  clamp, submit ≤ ``concurrency`` at a time, drain on cancel/deadline, emit
  progress. Fake the ``run_one`` callable to test it with no DB.

The connection / transaction / ContextVar handling in :func:`make_filer_runner`
is exercised by the dev-verify step on the real path (per the #1274 spec).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import errors as pg_errors

from app.jobs.job_connection import connect_job, job_statement_timeout_ms

logger = logging.getLogger(__name__)

# Wake-up cadence for the dispatch loop even when no future has completed.
# Bounds (a) how long a cancel/deadline takes to halt NEW submissions and
# (b) the gap before the orchestrator's wall-clock progress heartbeat fires
# during a long in-flight batch. Well below the 30 s progress cadence.
DEFAULT_HEARTBEAT_SECONDS: float = 5.0

# Transient Postgres errors a per-filer transaction can hit when concurrent
# filer pipelines write overlapping shared rows (e.g. ``unresolved_13f_cusips``).
# Postgres aborts one transaction as the victim; the canonical remedy is to
# retry it — by then the contending transaction has committed (#1274 dev-verify).
# Ordering the hot shared writes (sorted CUSIP upserts) makes these rare; this
# retry is the safety net for any residual cross-table contention.
_RETRYABLE_TX_ERRORS: tuple[type[Exception], ...] = (
    pg_errors.DeadlockDetected,
    pg_errors.SerializationFailure,
)
DEFAULT_MAX_DEADLOCK_RETRIES: int = 3


@dataclass(frozen=True)
class FilerWorkResult[S]:
    """Outcome of one filer pipeline. ``summary`` is ``None`` iff the filer
    crashed (its transaction was rolled back, the batch continues)."""

    cik: str
    summary: S | None
    error: str | None

    @property
    def crashed(self) -> bool:
        return self.summary is None


@dataclass(frozen=True)
class DrainOutcome:
    submitted: int
    completed: int
    deadline_hit: bool
    cancelled: bool


def make_filer_runner[S](
    work: Callable[[psycopg.Connection[Any], str], S],
    *,
    statement_timeout_ms: int | None,
    max_deadlock_retries: int = DEFAULT_MAX_DEADLOCK_RETRIES,
) -> Callable[[str], FilerWorkResult[S]]:
    """Adapt ``work(conn, cik)`` into a ``run_one(cik) -> FilerWorkResult`` that
    opens its own connection and never raises.

    The ``try/except`` wraps ``with connect_job()`` from the **outside** so a
    ``work`` exception reaches ``Connection.__exit__`` (ROLLBACK) before it is
    caught — catching *inside* the ``with`` would let the context manager
    COMMIT a failed transaction. ``job_statement_timeout_ms`` is re-applied on
    the worker thread because ContextVars do not propagate into
    ``ThreadPoolExecutor`` threads (#1690 per-job statement timeout would
    otherwise be lost on worker conns), and reset in ``finally`` so a reused
    pool thread never inherits a stale bound.

    A transient deadlock / serialization failure (concurrent filer pipelines
    writing overlapping shared rows) rolls back and **retries** the whole filer
    on a fresh connection up to ``max_deadlock_retries`` times — the contending
    transaction has committed by then. Exhausting the retries returns a crashed
    result (still isolated, the batch continues).
    """

    def _run(cik: str) -> FilerWorkResult[S]:
        token = job_statement_timeout_ms.set(statement_timeout_ms)
        try:
            for attempt in range(max_deadlock_retries + 1):
                try:
                    with connect_job() as conn:
                        summary = work(conn, cik)
                        conn.commit()
                    return FilerWorkResult(cik=cik, summary=summary, error=None)
                except _RETRYABLE_TX_ERRORS as exc:
                    if attempt >= max_deadlock_retries:
                        logger.warning(
                            "filer ingest: %s exhausted %d deadlock retries (%s)",
                            cik,
                            max_deadlock_retries,
                            type(exc).__name__,
                        )
                        return FilerWorkResult(
                            cik=cik,
                            summary=None,
                            error=f"{cik}: {type(exc).__name__} after {attempt} retries",
                        )
                    # Brief escalating stagger so the retried transaction does
                    # not immediately re-collide with the same contender.
                    logger.info(
                        "filer ingest: %s hit %s (attempt %d); retrying",
                        cik,
                        type(exc).__name__,
                        attempt + 1,
                    )
                    time.sleep(0.05 * (attempt + 1))
            # Unreachable — the loop returns on success or on retry exhaustion.
            return FilerWorkResult(cik=cik, summary=None, error=f"{cik}: retry loop fell through")
        except Exception as exc:  # noqa: BLE001 — per-filer crash must not abort the batch
            logger.exception("filer ingest: %s raised; isolating from batch", cik)
            return FilerWorkResult(cik=cik, summary=None, error=f"{cik}: {exc}")
        finally:
            job_statement_timeout_ms.reset(token)

    return _run


def drain_filers_concurrently[S](
    ciks: Sequence[str],
    *,
    run_one: Callable[[str], FilerWorkResult[S]],
    concurrency: int,
    deadline_ts: float | None,
    on_result: Callable[[FilerWorkResult[S]], None],
    should_cancel: Callable[[], bool] | None = None,
    on_progress: Callable[[int], None] | None = None,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
) -> DrainOutcome:
    """Run ``run_one`` over ``ciks`` with at most ``concurrency`` in flight.

    ``deadline_ts`` is a monotonic soft budget (``None`` = none); ``should_cancel``
    is polled cooperatively (``None`` = no cancel path). On cancel or deadline
    the driver stops submitting NEW filers and drains the in-flight set to
    completion — partial per-filer commits are valid and resumable via each
    sweep's ingest-log tombstones. ``on_result`` runs once per completed filer
    on the orchestrator thread (single-threaded — counter accumulation needs no
    lock). ``on_progress`` is called with the running completed count on every
    wake-up (completion or heartbeat) so the caller can apply its own emit
    cadence.

    Cancel ranks above deadline: at the wake-up that stops the drain, a pending
    cancel is reported in preference to an expired deadline.
    """
    workers = max(1, concurrency)
    # Order-preserving de-dupe — defence in depth so two workers never process
    # one filer concurrently (wasted work; N-PORT lacks a per-accession advisory
    # lock). Production CIK lists are already distinct (``institutional_filers.cik``
    # is UNIQUE), and the orchestrators de-dupe at entry too so ``len(ciks)`` and
    # ``completed`` agree; this is the belt to that braces.
    unique_ciks = list(dict.fromkeys(ciks))
    pending = iter(unique_ciks)

    cancelled = False
    deadline_hit = False
    submitted = 0
    completed = 0

    def _stop_requested() -> bool:
        nonlocal cancelled, deadline_hit
        if should_cancel is not None and should_cancel():
            cancelled = True
            return True
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            deadline_hit = True
            return True
        return False

    in_flight: set[Future[FilerWorkResult[S]]] = set()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="filer-ingest") as executor:

        def _fill_window() -> None:
            # Top the in-flight set up to ``workers``, re-checking stop BEFORE
            # every submit (not just once per refill) so a cancel/deadline that
            # lands mid-refill cannot launch up to ``workers - 1`` extra filers
            # past the signal. Shared by the initial prime and every top-up.
            nonlocal submitted
            while len(in_flight) < workers:
                if _stop_requested():
                    return
                cik = next(pending, None)
                if cik is None:
                    return
                in_flight.add(executor.submit(run_one, cik))
                submitted += 1

        # Prime — an at-entry expired deadline / pre-set cancel submits zero.
        _fill_window()

        while in_flight:
            done, in_flight = wait(in_flight, timeout=heartbeat_seconds, return_when=FIRST_COMPLETED)
            for fut in done:
                try:
                    result = fut.result()
                except Exception as exc:  # noqa: BLE001 — run_one is total; defensive only
                    logger.exception("filer ingest: run_one raised unexpectedly")
                    result = FilerWorkResult(cik="?", summary=None, error=str(exc))
                completed += 1
                on_result(result)
            if on_progress is not None:
                on_progress(completed)
            # Re-check stop on every wake-up (completion OR heartbeat) so a
            # cancel/deadline halts NEW submissions within ~heartbeat_seconds,
            # not "after the next filer completes". Short-circuit once stopped
            # to preserve cancel-over-deadline precedence; _fill_window re-checks
            # again per submit so the refill itself never races past the signal.
            stopped = cancelled or deadline_hit or _stop_requested()
            if not stopped:
                _fill_window()
        # Leaving the `with` joins every worker thread — in-flight drains fully.

    return DrainOutcome(
        submitted=submitted,
        completed=completed,
        deadline_hit=deadline_hit,
        cancelled=cancelled,
    )
