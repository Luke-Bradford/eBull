"""Ranking-pot-v2 monthly rebalance job (#3592 slice 3c-ii; spec ``2026-10-03-3592-ranking-pot-v2.md`` §4).

A copy of v1's orchestration (``ranking_pot_job.py`` at ``85aa0366``), not an import: v1's job imports the executed
book (``ranking_pot_exec``), which §8 bars from v2's hash. Kept exactly: ``window_phase`` and the completed-session
rule, ``WINDOW_OPENS`` / ``BARS_WAIT_UNTIL`` / ``WINDOW_CLOSES``, ``look_pending`` and the bar wait before scoring, the
scoring run on its own connection committed before the snapshot transaction, thesis provenance from
``ScoreResult.thesis_used``, and the snapshot transaction's re-checks. Only ``FIRE_MINUTE`` differs (50, after v1's
:40 on the same lane). A later v1 fix reaches v2 only by an edit here, which drifts v2's hash.

What v2 adds, in the order a fire meets it:

- **The state.** v2 has no executed book (sql/463), so only ``shadow_only`` rebalances; nothing executed is decided.
- **Its own policy check, before the frozen terms are decoded**, so a drifted declaration refuses ``ranking_drift``
  rather than raising on a block its drifted decoder reads differently.
- **Pre-score factor gates** (§4; Appendix A S2-a), read in their own transaction at ``transaction_timestamp()``
  (``ranking_pot_v2_inputs``: FINRA re-stamps ``known_from`` on every refresh, so an earlier fire time would hide the
  rows at S*): ``dtc_unavailable`` / ``dtc_incomplete`` against the frozen baseline, then
  ``insider_history_floor_moved`` against the frozen month counts. They read no scores, so a refusal here spends no
  scoring run. A refusal is a gate verdict, so its detail carries no ``pre_score`` flag (that flag marks the bar wait,
  which ``read_history`` ranks below any verdict).
- **In the snapshot transaction** the same gates re-run at the snapshot's own ``as_of``, and their result governs (a
  FINRA row landing between the two cannot make the stored outcome disagree with the snapshot). Order: v2's policy,
  the factor gates, v1's step-2 gates and universes, then the factor read; a failing read refuses
  ``insider_read_failed``. The DTC rows behind the gate are the rows the ``v2`` block stores.
- **The snapshot** is v1's ``encode_snapshot`` document with v2's policy hash and strategy id, plus the ``v2`` block
  (``ranking_pot_v2.with_v2_block``) inside the one canonical hash. Every v2 attempt row carries
  ``RANKING_POT_V2_POLICY_HASH``.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from fractions import Fraction
from typing import Any, Final, Literal

import psycopg
from psycopg.types.json import Jsonb

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_inputs as reader
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import latest_completed_us_session
from app.services.ranking_pot_v2_declaration import FrozenTerms, frozen_terms, load_declaration
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH, policy_manifest_now
from app.services.scoring import compute_rankings

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

#: UTC, on the completed session's date (v1's, copied).
WINDOW_OPENS: Final = time(23, 30)
#: UTC, the next calendar day: fires before this wait for the session's bars (v1's, copied).
BARS_WAIT_UNTIL: Final = time(9, 0)
#: UTC, the next calendar day: no fire acts at or after this (v1's, copied).
WINDOW_CLOSES: Final = time(12, 0)
#: The scheduler's hourly fire minute, by construction after v1's :40 on the same lane (spec §4). Here, so the policy
#: hash covers it; the scheduler's copy is pinned to this by ``tests/test_ranking_pot_v2_job.py``.
FIRE_MINUTE: Final = 50

Phase = Literal["closed", "waiting", "final"]
V2Refusal = rb.InputRefusal | Literal["look_pending"] | v2.DtcRefusal | v2.InsiderRefusal | Literal["scoring_failed"]


def window_phase(as_of: datetime) -> Phase:
    """Where ``as_of`` falls in the decision window of the last completed NYSE session (v1's rule, copied)."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    d = latest_completed_us_session(as_of)
    nxt = d + timedelta(days=1)
    at = as_of.astimezone(UTC)
    if not datetime.combine(d, WINDOW_OPENS, UTC) <= at < datetime.combine(nxt, WINDOW_CLOSES, UTC):
        return "closed"
    return "waiting" if at < datetime.combine(nxt, BARS_WAIT_UNTIL, UTC) else "final"


@dataclass(frozen=True)
class ScoringRun:
    scored_at: datetime
    #: Every scored name whose score consumed a thesis.
    theses: dict[int, rb.ThesisUsed]


def scoring_step(connect: Callable[[], AbstractContextManager[Conn]]) -> ScoringRun:
    """One ordinary ``scores`` run, committed when its connection closes (v1's step 1, copied)."""
    with connect() as sconn:
        result = compute_rankings(sconn)
    return ScoringRun(
        scored_at=result.run_at,
        theses={
            r.instrument_id: rb.ThesisUsed(
                r.thesis_used.thesis_id, r.thesis_used.created_at, r.thesis_used.model, r.thesis_used.prompt_version
            )
            for r in result.scored
            if r.thesis_used is not None
        },
    )


@dataclass(frozen=True)
class Refused:
    refusal: V2Refusal
    detail: dict[str, Any]


def policy_ok(decl: rb.PotDeclaration) -> bool:
    """§8 drift: the declared hash, v2's import-time hash and the bytes on disk now all agree."""
    return decl.doc.get("policy_hash") == RANKING_POT_V2_POLICY_HASH == policy_manifest_now().digest()


# ---------------------------------------------------------------------------
# Factor gates and the factor read
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FactorGate:
    #: The DTC rows the gate read: the ``v2`` block stores exactly these.
    dtc_rows: tuple[v2.DtcRow, ...]
    dtc: v2.DtcRead
    detail: dict[str, Any]
    refusal: Refused | None


def factor_gates(
    conn: Conn, decl: rb.PotDeclaration, terms: FrozenTerms, *, target_session: date, as_of: datetime
) -> FactorGate:
    """§4 ``dtc_unavailable`` / ``dtc_incomplete``, then Appendix A S2-a ``insider_history_floor_moved``. Reads only,
    in the caller's transaction, at ``as_of`` (that transaction's own time)."""
    rows = reader.read_dtc_rows(conn, s0_ids=decl.s0_ids, as_of=as_of)
    dtc = v2.dtc_read(rows, s0_ids=decl.s0_ids, as_of=as_of)
    detail: dict[str, Any] = {
        "dtc_settlement_date": None if dtc.settlement_date is None else dtc.settlement_date.isoformat(),
        "dtc_usable": len(dtc.values),
        "dtc_baseline": terms.dtc_baseline,
    }
    code = v2.dtc_gate(dtc, as_of=as_of, baseline=terms.dtc_baseline)
    if code is not None:
        return FactorGate(rows, dtc, detail, Refused(code, detail))
    live = reader.usable_history_counts(conn, first_month=terms.history_floor, end_month=terms.freeze_month)
    moved = v2.history_floor_moved(
        terms.month_counts, live, history_floor=terms.history_floor, target_session=target_session
    )
    if moved:
        detail = detail | {"floor_moved": [[m.isoformat(), live.get(m, 0), terms.month_counts[m]] for m in moved]}
        return FactorGate(rows, dtc, detail, Refused("insider_history_floor_moved", detail))
    return FactorGate(rows, dtc, detail, None)


def factor_detail(
    ins: v2.InsiderRead,
    dtc: v2.DtcRead,
    reader_counts: Mapping[str, int],
    r_ids: Iterable[int],
) -> dict[str, Any]:
    """§5 / Appendix A S2-b per-rebalance record: the insider read's counts, DTC coverage and each missing reason, the
    reader's never-gated counts, and R_t's order diagnostics. Derived figures: beside the snapshot, never in it."""
    diag = v2.order_diagnostics(r_ids, dtc.values, ins.buyers)
    return {
        "s0_buyers": len(ins.buyers),
        "purchases_in_window": ins.purchases_in_window,
        "excluded_null_cik": ins.excluded_null_cik,
        "unclassifiable_pairs": ins.unclassifiable_pairs,
        "unclassifiable_names": ins.unclassifiable_names,
        "indeterminate_pairs": ins.indeterminate_pairs,
        "indeterminate_names": ins.indeterminate_names,
        "dtc_missing": dict(sorted(Counter(dtc.missing.values()).items())),
        "reader_counts": dict(sorted(reader_counts.items())),
        "r_buyers": diag.r_buyers,
        "r_dtc_coverage": diag.r_dtc_coverage,
        "dtc_constant": diag.dtc_constant,
        "ins_constant": diag.ins_constant,
    }


def read_factors(
    conn: Conn,
    decl: rb.PotDeclaration,
    terms: FrozenTerms,
    gate: FactorGate,
    *,
    target_session: date,
    as_of: datetime,
) -> tuple[v2.V2Inputs, v2.InsiderRead] | Refused:
    """The ``v2`` block's inputs and the insider read over them, under a savepoint: a failing read refuses
    ``insider_read_failed`` (§4) and leaves the snapshot transaction usable to record it."""
    try:
        with conn.transaction():
            purchases = reader.read_purchases(conn, s0_ids=decl.s0_ids, target_session=target_session, as_of=as_of)
            inputs = v2.V2Inputs(
                purchases=purchases,
                history=reader.read_history(conn, purchases, as_of=as_of),
                dtc=gate.dtc_rows,
                reader_counts=reader.read_manifest_gaps(
                    conn, s0_ids=decl.s0_ids, target_session=target_session, as_of=as_of
                ),
            )
            ins = v2.insider_read(
                inputs.purchases,
                inputs.history,
                s0_ids=decl.s0_ids,
                target_session=target_session,
                as_of=as_of,
                history_floor=terms.history_floor,
            )
    except (psycopg.Error, ValueError, ArithmeticError) as exc:
        logger.exception("ranking pot v2: the factor read failed")
        return Refused("insider_read_failed", gate.detail | {"error": type(exc).__name__})
    return inputs, ins


def snapshot_of(inputs: rb.SnapshotInputs, block: v2.V2Inputs) -> dict[str, Any]:
    """v1's snapshot document under v2's policy hash and strategy id, plus the ``v2`` block (§4)."""
    doc = rb.encode_snapshot(replace(inputs, policy_hash=RANKING_POT_V2_POLICY_HASH))
    return v2.with_v2_block({**doc, "strategy_id": v2.STRATEGY_ID}, block)


def prepare(
    conn: Conn,
    decl: rb.PotDeclaration,
    terms: FrozenTerms,
    history: rb.AttemptHistory,
    *,
    as_of: datetime,
    scored_at: datetime,
    theses: Mapping[int, rb.ThesisUsed],
) -> rb.Prepared | Refused:
    """v1's steps 2–3 with v2's gates and factor read, inside ``begin_rebalance``'s transaction. Writes nothing."""
    read = rb.read_snapshot_inputs(conn, decl, as_of=as_of, scored_at=scored_at, theses=theses)
    inputs, cov = read.inputs, read.coverage
    detail: dict[str, Any] = {
        "s0_tradable": cov.s0_tradable,
        "scored": cov.scored,
        "barred": cov.barred,
        "nyse_exchange_found": read.nyse_found,
        "nyse_caps": len(inputs.nyse_caps),
        "nyse_caps_valid": sum(1 for _, c in inputs.nyse_caps if c is not None),
    }
    if not policy_ok(decl):
        return Refused("ranking_drift", detail)
    gate = factor_gates(conn, decl, terms, target_session=inputs.target_session, as_of=as_of)
    detail |= gate.detail
    if gate.refusal is not None:
        return Refused(gate.refusal.refusal, detail)
    refusal = rb.gate_refusal(
        cov, policy_ok=True, spy_ok=rb.spy_ok(inputs.spy, as_of=as_of, last_session=inputs.last_session)
    )
    if refusal is not None:
        return Refused(refusal, detail)
    universes = rb.universes_of(inputs)
    if isinstance(universes, str):
        return Refused(universes, detail)
    r_count = len(universes.r_ids)
    detail |= {
        "r_count": r_count,
        "f_count": len(universes.f_ids),
        "max_population": universes.max_population,
        "previous_r_count": history.previous_r_count,
    }
    if rb.universe_collapsed(
        r_count, previous_r_count=history.previous_r_count, consecutive_collapses=history.consecutive_collapses
    ):
        return Refused("universe_collapse", detail)
    if history.previous_r_count is not None and Fraction(r_count) < rb.UNIVERSE_FLOOR * history.previous_r_count:
        detail["contraction_recorded"] = True  # the override matured (v1 §4 step 2)
    factors = read_factors(conn, decl, terms, gate, target_session=inputs.target_session, as_of=as_of)
    if isinstance(factors, Refused):
        return Refused(factors.refusal, detail | factors.detail)
    block, ins = factors
    detail["v2"] = factor_detail(ins, gate.dtc, block.reader_counts, universes.r_ids)
    snapshot = snapshot_of(inputs, block)
    return rb.Prepared(
        snapshot=snapshot, snapshot_sha256=canonical_sha256(snapshot), universes=universes, detail=detail
    )


# ---------------------------------------------------------------------------
# Writes: v1's rows under v2's policy hash
# ---------------------------------------------------------------------------
def record_decided(
    conn: Conn,
    decl: rb.PotDeclaration,
    due: rb.DuePlan,
    prepared: rb.Prepared,
    *,
    as_of: datetime,
    scored_at: datetime,
) -> int:
    """The month's ``decided`` row (v1's writer under v2's hash). The stored document is read back and re-hashed."""
    rb._assert_rebalance_transaction(conn)
    if "r_count" not in prepared.detail:
        raise ValueError("a decided row's detail carries r_count (read_history's outage guard)")
    if prepared.snapshot.get("target_session") != due.target_session.isoformat():
        raise ValueError("the snapshot is for another target session")
    if prepared.snapshot.get("strategy_id") != v2.STRATEGY_ID or "v2" not in prepared.snapshot:
        raise ValueError(f"not a {v2.STRATEGY_ID} snapshot")
    row = conn.execute(
        "INSERT INTO ranking_pot_rebalance_attempts "
        "(declaration_id, fired_at, target_session, month, outcome, scored_at, policy_hash, detail, "
        " snapshot, snapshot_sha256) "
        "VALUES (%s, %s, %s, %s, 'decided', %s, %s, %s, %s, %s) RETURNING attempt_id",
        (
            decl.declaration_id,
            as_of,
            due.target_session,
            due.month,
            scored_at,
            RANKING_POT_V2_POLICY_HASH,
            Jsonb(prepared.detail),
            Jsonb(prepared.snapshot),
            prepared.snapshot_sha256,
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("INSERT … RETURNING returned no row")
    attempt_id = int(row[0])
    stored = conn.execute(
        "SELECT snapshot, snapshot_sha256 FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (attempt_id,)
    ).fetchone()
    if stored is None or not (canonical_sha256(stored[0]) == stored[1] == prepared.snapshot_sha256):
        raise rb.SnapshotIntegrityError(f"attempt {attempt_id}: the stored snapshot does not hash to its sha256")
    return attempt_id


def record_refused(
    conn: Conn,
    decl: rb.PotDeclaration,
    due: rb.DuePlan,
    refused: Refused,
    *,
    as_of: datetime,
    scored_at: datetime | None,
) -> int:
    row = conn.execute(
        "INSERT INTO ranking_pot_rebalance_attempts "
        "(declaration_id, fired_at, target_session, month, outcome, refusal, scored_at, policy_hash, detail) "
        "VALUES (%s, %s, %s, %s, 'refused', %s, %s, %s, %s) RETURNING attempt_id",
        (
            decl.declaration_id,
            as_of,
            due.target_session,
            due.month,
            refused.refusal,
            scored_at,
            RANKING_POT_V2_POLICY_HASH,
            Jsonb(refused.detail),
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("INSERT … RETURNING returned no row")
    return int(row[0])


def record_skips(
    conn: Conn, decl: rb.PotDeclaration, due: rb.DuePlan, history: rb.AttemptHistory, *, as_of: datetime
) -> int:
    """Close every month in ``due.skips`` as ``skipped`` with its last refusal (v1 r3-71). Returns the count."""
    for m in due.skips:
        conn.execute(
            "INSERT INTO ranking_pot_rebalance_attempts "
            "(declaration_id, fired_at, target_session, month, outcome, refusal, policy_hash) "
            "VALUES (%s, %s, %s, %s, 'skipped', %s, %s)",
            (
                decl.declaration_id,
                as_of,
                due.target_session,
                m,
                history.last_refusal.get(m, rb.NOT_ATTEMPTED),
                RANKING_POT_V2_POLICY_HASH,
            ),
        )
    return len(due.skips)


# ---------------------------------------------------------------------------
# The fire
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class JobResult:
    note: str
    #: The attempt row this fire wrote (refused or decided), if any.
    attempt_id: int | None = None
    skipped_months: int = 0


def _transaction_time(conn: Conn) -> datetime:
    row = conn.execute("SELECT transaction_timestamp()").fetchone()
    if row is None:
        raise RuntimeError("transaction_timestamp() returned no row")
    return row[0]


def run_rebalance_job(
    conn: Conn,
    *,
    score: Callable[[], ScoringRun],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> JobResult:
    """One fire. ``conn`` must be autocommit: every write below is its own top-level transaction."""
    if not conn.autocommit:
        raise RuntimeError("run_rebalance_job needs an autocommit connection")
    as_of = now()
    decl = load_declaration(conn)
    if decl is None:
        return JobResult(f"no {v2.STRATEGY_ID} declaration")
    if decl.state != "shadow_only":
        return JobResult(f"no rebalance runs in state {decl.state}")

    history = rb.read_history(conn, decl.declaration_id)
    due = rb.plan(as_of, first=rb.first_month(decl.frozen_at), resolved=history.resolved)
    skipped = 0
    if due.skips:  # v1 r3-71: every fire closes what can no longer be decided, in or out of the window
        with conn.transaction():
            skipped = record_skips(conn, decl, due, history, as_of=as_of)
    if not due.due:
        return JobResult(f"target {due.target_session}: not due", skipped_months=skipped)
    phase = window_phase(as_of)
    if phase == "closed":
        return JobResult(f"target {due.target_session}: outside the decision window", skipped_months=skipped)

    def refuse(refused: Refused, scored_at: datetime | None = None) -> JobResult:
        with conn.transaction():
            attempt = record_refused(conn, decl, due, refused, as_of=as_of, scored_at=scored_at)
        return JobResult(f"target {due.target_session}: refused {refused.refusal}", attempt, skipped)

    pending = look.look_pending(conn, decl.declaration_id, due.target_session)
    if pending is not None:
        return refuse(Refused("look_pending", pending))
    if not policy_ok(decl):
        return refuse(Refused("ranking_drift", {}))
    terms = frozen_terms(decl)
    with conn.transaction():
        gate = factor_gates(conn, decl, terms, target_session=due.target_session, as_of=_transaction_time(conn))
    if gate.refusal is not None:
        return refuse(gate.refusal)
    if phase == "waiting":
        barred, tradable = rb.bars_ready(conn, decl, as_of=as_of)
        if not (tradable > 0 and Fraction(barred, tradable) >= rb.BAR_COVERAGE_MIN):
            # v1 r3-88: recorded, not silent; the flag ranks it below any gate verdict in `read_history`.
            refused = refuse(
                Refused("price_daily_stale", {"pre_score": True, "barred": barred, "s0_tradable": tradable})
            )
            return replace(
                refused, note=f"target {due.target_session}: {barred}/{tradable} S0 bars landed; retried next fire"
            )

    try:
        run = score()
    except Exception:
        logger.exception("ranking pot v2: scoring run failed")
        return refuse(Refused("scoring_failed", {}))

    s0 = set(decl.s0_ids)
    # An early exit below rolls the transaction back explicitly (``psycopg.Rollback``) rather than returning
    # through the block, which would COMMIT it: nothing is written before them, and nothing must be.
    early: str | None = None
    with conn.transaction() as tx:
        rb.begin_rebalance(conn)
        # The first query fixes the REPEATABLE READ snapshot; `as_of` is read after it, so every quote and FINRA row
        # the snapshot can see is at or before `as_of`.
        conn.execute("SELECT 1")
        snap_as_of = now()
        if rb.target_session(snap_as_of) != due.target_session or window_phase(snap_as_of) == "closed":
            early = f"target {due.target_session}: the window passed during scoring"
            raise psycopg.Rollback(tx)
        live = load_declaration(conn)
        if live is None or live.declaration_id != decl.declaration_id or live.state != decl.state:
            early = f"declaration changed during the fire ({live and live.state}); retried next fire"
            raise psycopg.Rollback(tx)
        history = rb.read_history(conn, decl.declaration_id)
        if due.month in history.resolved:
            early = f"month {due.month} already resolved"
            raise psycopg.Rollback(tx)
        result = prepare(
            conn,
            live,
            terms,
            history,
            as_of=snap_as_of,
            scored_at=run.scored_at,
            theses={iid: t for iid, t in run.theses.items() if iid in s0},
        )
        if isinstance(result, Refused):
            attempt = record_refused(conn, live, due, result, as_of=snap_as_of, scored_at=run.scored_at)
            return JobResult(f"target {due.target_session}: refused {result.refusal}", attempt, skipped)
        attempt = record_decided(conn, live, due, result, as_of=snap_as_of, scored_at=run.scored_at)
    if early is not None:
        return JobResult(early, None, skipped)
    return JobResult(
        f"target {due.target_session}: decided (R={result.detail['r_count']}, F={result.detail['f_count']}, "
        f"buyers in R={result.detail['v2']['r_buyers']}, snapshot {result.snapshot_sha256[:12]})",
        attempt,
        skipped,
    )


__all__ = [
    "BARS_WAIT_UNTIL",
    "FIRE_MINUTE",
    "WINDOW_CLOSES",
    "WINDOW_OPENS",
    "FactorGate",
    "JobResult",
    "Refused",
    "ScoringRun",
    "factor_detail",
    "factor_gates",
    "policy_ok",
    "prepare",
    "read_factors",
    "record_decided",
    "record_refused",
    "record_skips",
    "run_rebalance_job",
    "scoring_step",
    "snapshot_of",
    "window_phase",
]
