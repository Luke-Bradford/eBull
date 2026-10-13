#!/usr/bin/env python3
"""Refusal surfaces onto the operator push channel (#3614 item 3, slice B).

The engine already records every refusal that matters as a row; nobody reads
rows. This watch reads four of them and pushes a notice through
``app.system.push_channel`` when one appears:

- **Kill-switch changes:** every ``runtime_config_audit`` row with
  ``field = 'kill_switch'``. Each toggle is one notice, so an on-then-off
  between two runs is still seen.
- **Execution blocks:** ``strategy_execution_blocks`` for ``drawdown`` (the
  engine pot's loss limit), ``order_reconciliation`` (orders with unresolved
  broker identity) and ``broker_availability`` (the account-risk probe). A block
  pages once it has stayed active for ``_BLOCK_PERSIST_S``, and sends one notice
  when it clears.
- **Entry refusals:** rejected ``strategy_entry_preflights`` rows whose
  ``reason_code`` is the sandbox boundary (#2844) or a mandate loss limit
  (``_PAGED_ENTRY_REFUSALS``). One run's refusals go out as one notice.
- **Kill drills** (#3614 slice 3; spec
  ``docs/specs/ops/2026-10-06-3614-kill-drill-time-to-flat.md`` § Paging): per
  ``kill_switch_drill_events`` row since ``DRILL_WATCH_FROM``, a failed entry
  verdict, book defects, or a corrupt runtime config; and per monthly slot, an
  occurrence with no drill started inside its grace. Every drill event and slot
  since that date is read, not the last hour, and drill keys never expire from
  the status file (about twelve a year), so a missed pass sends late, never twice.

Deliberately NOT paged: the ``quote_freshness`` and ``scan_freshness`` blocks.
They activate every night after the US close and clear at the open by design
(measured 2026-10-06: ``quote_freshness`` blocked since 2026-10-05 21:30Z, still
active pre-open), so a page for them is a routine check-in, which the 2026-08-22
settled decision rules out. Every other entry refusal code is a per-signal gate
verdict, not an operator decision.

``_BLOCK_PERSIST_S`` is set by construction: ``strategy_paper_cycle`` refreshes
the blocks every 5 min, so 10 min means the block survived two further
refreshes. A one-cycle broker blip does not page; a block that holds does.

Each notice follows the #2843 validity contract: what happened in one
sentence, the row's own evidence, what to do, and the safe default.

Run by the jobs dead-man LaunchAgent on each of its 5-min passes (no separate
install). ``--dry-run`` prints what it would send without sending or saving.
Runbook: ``docs/operator/runbooks/jobs-dead-man.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.services.kill_switch_drill_schedule import DRILL_WATCH_FROM, overdue_occurrences
from app.services.strategy_capital_sandbox import SANDBOX_EXCEEDED
from app.system.push_channel import Priority, config_from_env, local_notify, send_push

_DEFAULT_STATUS_FILE = Path.home() / ".cache" / "ebull" / "operator_alert_watch_status.json"
_BLOCK_PERSIST_S = 600.0
# Event rows are read back this far. The watch runs every 5 min, so a run can
# be missed eleven times before an event falls out of the window unpaged.
_LOOKBACK_S = 3600.0
# A paged key is remembered for twice the read window, so it cannot be re-read
# after it was forgotten. Drill keys are exempt: their read window has no end.
_SEEN_TTL_S = 2 * _LOOKBACK_S
_DRILL_KEY_PREFIX = "kill_drill"

#: C2/C3 refusal codes that mean the runtime config row is unreadable: a
#: configuration-health verdict (``app/services/kill_switch_drill.py`` early codes).
#: ``auto_trading_disabled`` is the other early code and does not page: nothing can enter.
_CONFIG_CORRUPT_CODES: frozenset[str] = frozenset({"runtime_config_corrupt", "core_runtime_config_corrupt"})

#: Block source -> what the operator should check. Insertion order is send order.
_PAGED_BLOCKS: Mapping[str, str] = {
    "drawdown": "The engine pot hit its loss limit or its drawdown is unobservable; check the pot on /strategies.",
    "order_reconciliation": "An engine order's broker identity is unresolved; compare eToro demo with /strategies.",
    "broker_availability": "The eToro account-risk probe is failing or stale; check the eToro API and credentials.",
}

#: Entry refusal codes that are an operator decision: the sandbox boundary and
#: the mandate loss limits. Written by ``app/services/strategy_paper_executor.py``.
_PAGED_ENTRY_REFUSALS: frozenset[str] = frozenset(
    {
        SANDBOX_EXCEEDED,
        "account_drawdown_limit",
        "portfolio_drawdown_limit",
        "portfolio_daily_loss_limit",
        "portfolio_position_loss_limit",
    }
)


@dataclass(frozen=True)
class Block:
    source: str
    active: bool
    blocked_at: float | None
    reason: str


@dataclass(frozen=True)
class KillSwitchChange:
    audit_id: int
    changed_at: float
    changed_by: str
    reason: str
    new_active: bool


@dataclass(frozen=True)
class EntryRefusal:
    signal_id: int
    evaluated_at: float
    reason_code: str
    # The row's own risk evidence, pre-formatted ("deployment 7, account equity ...").
    evidence: str = ""


@dataclass(frozen=True)
class DrillChokepoint:
    chokepoint: str
    outcome: str
    refusal_code: str | None = None


@dataclass(frozen=True)
class DrillEvent:
    event_id: int
    started_at: float
    trigger: str
    actor: str
    entry_verdict: str
    book_verdict: str
    run_failure: str | None = None
    failure_detail: str | None = None
    kill_active_at_verify: bool | None = None
    chokepoints: Sequence[DrillChokepoint] = ()
    unmapped: int | None = None
    multiply_mapped: int | None = None
    without_stop: int | None = None
    without_target: int | None = None
    # Ownerships behind the book counts, from ``kill_switch_drill_positions``.
    defect_ownerships: Sequence[int] = ()
    code_commit: str | None = None
    code_dirty: bool | None = None


@dataclass(frozen=True)
class Observation:
    blocks: Sequence[Block] = ()
    kill_switch: Sequence[KillSwitchChange] = ()
    refusals: Sequence[EntryRefusal] = ()
    # Every drill event started at or after ``DRILL_WATCH_FROM``.
    drills: Sequence[DrillEvent] = ()


@dataclass(frozen=True)
class Notice:
    title: str
    message: str
    priority: Priority
    tag: str
    # What delivering this notice settles in the state.
    seen: Mapping[str, float] = field(default_factory=dict)
    block_on: str | None = None
    block_off: str | None = None


@dataclass(frozen=True)
class WatchState:
    paged_blocks: tuple[str, ...] = ()
    seen: Mapping[str, float] = field(default_factory=dict)  # event key -> event epoch s


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d %H:%MZ")


def plan(prior: WatchState, obs: Observation, *, now: float) -> list[Notice]:
    """Pure policy: the notices this observation calls for, in send order."""
    notices: list[Notice] = []

    for change in sorted(obs.kill_switch, key=lambda c: c.audit_id):
        key = f"kill_switch:{change.audit_id}"
        if key in prior.seen:
            continue
        who = f"by {change.changed_by} at {_iso(change.changed_at)}: {change.reason}"
        if change.new_active:
            notices.append(
                Notice(
                    title="eBull kill switch ACTIVATED",
                    message=(
                        f"The kill switch was turned on {who}. New entries are refused. Broker stop-loss and "
                        "take-profit and exact-owned exits still run; recommendation EXITs and core rebalance "
                        "sells are refused. "
                        "Safe default: leave it on until the cause is understood."
                    ),
                    priority=5,
                    tag="octagonal_sign",
                    seen={key: change.changed_at},
                )
            )
        else:
            notices.append(
                Notice(
                    title="eBull kill switch deactivated",
                    message=(
                        f"The kill switch was turned off {who}. Entries are allowed again. "
                        "If this was not you, turn it back on."
                    ),
                    priority=4,
                    tag="warning",
                    seen={key: change.changed_at},
                )
            )

    by_source = {b.source: b for b in obs.blocks}
    for source, check in _PAGED_BLOCKS.items():
        block = by_source.get(source)
        active = block is not None and block.active
        if source in prior.paged_blocks:
            # A re-block resets ``blocked_at``; still active means still paged.
            if not active:
                notices.append(
                    Notice(
                        title=f"eBull {source} block cleared",
                        message=f"The {source} execution block cleared; entries are no longer refused for it.",
                        priority=3,
                        tag="white_check_mark",
                        block_off=source,
                    )
                )
            continue
        if block is None or not active or block.blocked_at is None or now - block.blocked_at < _BLOCK_PERSIST_S:
            continue
        notices.append(
            Notice(
                title=f"eBull entries blocked: {source}",
                message=(
                    f"New engine entries have been refused since {_iso(block.blocked_at)}: {block.reason}. "
                    f"{check} Safe default: the block holds on its own and exits still run."
                ),
                priority=4,
                tag="no_entry",
                block_on=source,
            )
        )

    # ``signal_id`` alone is the whole key: it is ``strategy_entry_preflights``' PRIMARY KEY
    # (sql/287), so a signal has one preflight row, in one deployment, ever.
    fresh = [
        r
        for r in obs.refusals
        if r.reason_code in _PAGED_ENTRY_REFUSALS and f"entry_refusal:{r.signal_id}" not in prior.seen
    ]
    if fresh:
        counts = Counter(r.reason_code for r in fresh)
        latest = {r.reason_code: r for r in sorted(fresh, key=lambda r: r.evaluated_at)}
        summary = "; ".join(
            f"{code} x{n}" + (f" (latest: {latest[code].evidence})" if latest[code].evidence else "")
            for code, n in sorted(counts.items())
        )
        notices.append(
            Notice(
                title="eBull entries refused at a limit",
                message=(
                    f"{len(fresh)} engine entr{'y was' if len(fresh) == 1 else 'ies were'} refused at the sandbox "
                    f"boundary or a mandate loss limit, last at {_iso(max(r.evaluated_at for r in fresh))}: "
                    f"{summary}. "
                    # The limit is not on the refusal row, and recomputing it here would be a
                    # second copy of the executor's rule that could disagree with it.
                    "The limit is the deployment's mandate; check the pot and its assigned capital on /strategies. "
                    "Safe default: the refusal stands; exits still run."
                ),
                priority=4,
                tag="no_entry",
                seen={f"entry_refusal:{r.signal_id}": r.evaluated_at for r in fresh},
            )
        )

    notices.extend(_drill_notices(prior, obs.drills, now=now))
    return notices


def _drill_label(e: DrillEvent) -> str:
    build = "unknown build" if e.code_commit is None else f"build {e.code_commit[:12]}"
    if e.code_dirty:
        build += " (dirty tree)"
    return f"Kill drill event {e.event_id} ({e.trigger}, {e.actor}, {_iso(e.started_at)}, {build})"


def _entry_failed_notice(e: DrillEvent, key: str) -> Notice:
    outcomes = ", ".join(
        f"{c.chokepoint} {c.outcome}" + (f" ({c.refusal_code})" if c.refusal_code else "") for c in e.chokepoints
    )
    broken = [c.chokepoint for c in e.chokepoints if c.outcome in ("allowed", "error")]
    parts = [f"{_drill_label(e)} did not prove that the kill switch stops entries."]
    if outcomes:
        parts.append(f"Chokepoints: {outcomes}.")
    if broken:
        parts.append(f"{', '.join(broken)} may not stop under kill; fix that path.")
    if e.run_failure == "kill_switch_row_missing":
        parts.append(
            "The kill_switch row is missing: the switch cannot be activated, and every guard reads a missing "
            "row as active, so entries are refused. Restore the row before the next drill."
        )
    elif e.run_failure == "sandbox_committed":
        state = {True: "on", False: "off", None: "unknown"}[e.kill_active_at_verify]
        parts.append(
            f"A probe published the drill's activation; the switch read {state} at verify. "
            "Fix the probe before the next drill."
        )
    elif e.run_failure == "exception":
        parts.append(
            f"The drill could not complete ({e.failure_detail}), so no chokepoint is proven this month. "
            "Re-run it with scripts.run_kill_switch_drill from ~/Dev/eBull after reading that."
        )
    # The drill never changes trading state itself: a self-test that halts trading on its own bug
    # would be an unexplained halt. The strictest applicable default is named instead.
    if e.run_failure in ("kill_switch_row_missing", "sandbox_committed") and not broken:
        parts.append("Safe default: leave the switch as it is now.")
    elif e.run_failure == "exception" and not broken:
        parts.append("Safe default: none needed for trading.")
    else:
        # A broken chokepoint, or a failure this watch does not recognise: the kill path is in doubt.
        parts.append("Safe default: set enable_auto_trading = false until a re-run passes.")
    return Notice(
        title="eBull kill drill FAILED",
        message=" ".join(parts),
        priority=5,
        tag="rotating_light",
        seen={key: e.started_at},
    )


def _drill_notices(prior: WatchState, drills: Sequence[DrillEvent], *, now: float) -> list[Notice]:
    notices: list[Notice] = []
    for e in sorted(drills, key=lambda d: d.event_id):
        entry_key = f"{_DRILL_KEY_PREFIX}:{e.event_id}:entry"
        if (e.entry_verdict == "failed" or e.run_failure is not None) and entry_key not in prior.seen:
            notices.append(_entry_failed_notice(e, entry_key))

        config_key = f"{_DRILL_KEY_PREFIX}:{e.event_id}:config"
        corrupt = sorted({c.refusal_code for c in e.chokepoints if c.refusal_code in _CONFIG_CORRUPT_CODES})
        if e.entry_verdict == "incomplete" and corrupt and config_key not in prior.seen:
            notices.append(
                Notice(
                    title="eBull runtime config unreadable",
                    message=(
                        f"{_drill_label(e)} found the runtime config unreadable ({', '.join(corrupt)}). "
                        "Entries already refuse on that path; restore the runtime_config row. "
                        "Safe default: leave it."
                    ),
                    priority=4,
                    tag="warning",
                    seen={config_key: e.started_at},
                )
            )

        book_key = f"{_DRILL_KEY_PREFIX}:{e.event_id}:book"
        if e.book_verdict == "defects" and book_key not in prior.seen:
            ids = ", ".join(str(i) for i in e.defect_ownerships) or "none recorded"
            notices.append(
                Notice(
                    title="eBull book defects found by kill drill",
                    message=(
                        f"{_drill_label(e)} found {e.unmapped} unmapped and {e.multiply_mapped} multiply mapped "
                        f"ownerships, {e.without_stop} positions without a stop and {e.without_target} without a "
                        f"target (ownerships: {ids}). A missing stop or target breaks the SL/TP rule. "
                        "Safe default: none; the position manager's stop repair runs every 5-minute cycle, and "
                        "a defect that survives two drills is a bug to file."
                    ),
                    priority=4,
                    tag="warning",
                    seen={book_key: e.started_at},
                )
            )

    starts = [datetime.fromtimestamp(e.started_at, UTC) for e in drills]
    for o in overdue_occurrences(starts, now=datetime.fromtimestamp(now, UTC)):
        key = f"{_DRILL_KEY_PREFIX}_overdue:{o.isoformat()}"
        if key in prior.seen:
            continue
        notices.append(
            Notice(
                title="eBull kill drill overdue",
                message=(
                    f"No kill-switch drill started within 24 h of the {_iso(o.timestamp())} slot, so the kill "
                    "switch is unproven this month. Check the kill_switch_drill job's last run on /admin. "
                    "Safe default: none for trading; run the drill by hand with "
                    "scripts.run_kill_switch_drill from ~/Dev/eBull."
                ),
                priority=3,
                tag="hourglass",
                seen={key: o.timestamp()},
            )
        )
    return notices


def settle(prior: WatchState, delivered: Sequence[Notice], *, now: float) -> WatchState:
    """The state after sending: only delivered notices are recorded, so an
    undelivered one is planned again on the next run."""
    seen = {k: at for k, at in prior.seen.items() if k.startswith(_DRILL_KEY_PREFIX) or now - at < _SEEN_TTL_S}
    paged = list(prior.paged_blocks)
    for notice in delivered:
        seen.update(notice.seen)
        if notice.block_on is not None and notice.block_on not in paged:
            paged.append(notice.block_on)
        if notice.block_off is not None and notice.block_off in paged:
            paged.remove(notice.block_off)
    return WatchState(paged_blocks=tuple(paged), seen=seen)


# ──────────────────────────── IO shell ────────────────────────────────


def read_observation() -> Observation:
    """The four surfaces, read in one connection. Raises on any failure: the
    caller logs it, and the dead-man owns the database-down page."""
    import psycopg

    from app.config import Settings

    with psycopg.connect(Settings().database_url, connect_timeout=5) as conn:
        # A one-shot under launchd: a query parked behind a lock would hold every later pass.
        conn.execute("SET statement_timeout = '10s'")
        blocks = [
            Block(str(r[0]), bool(r[1]), r[2].timestamp() if r[2] is not None else None, str(r[3] or ""))
            for r in conn.execute(
                "SELECT source, active, blocked_at, reason FROM strategy_execution_blocks WHERE source = ANY(%s)",
                (list(_PAGED_BLOCKS),),
            ).fetchall()
        ]
        kill_switch = [
            KillSwitchChange(int(r[0]), r[1].timestamp(), str(r[2]), str(r[3] or ""), r[4] == "true")
            for r in conn.execute(
                """
                SELECT audit_id, changed_at, changed_by, reason, new_value
                FROM runtime_config_audit
                WHERE field = 'kill_switch' AND changed_at > now() - make_interval(secs => %s)
                ORDER BY audit_id
                """,
                (_LOOKBACK_S,),
            ).fetchall()
        ]
        refusals = [
            EntryRefusal(int(r[0]), r[1].timestamp(), str(r[2]), _refusal_evidence(*r[3:]))
            for r in conn.execute(
                """
                SELECT signal_id, evaluated_at, reason_code, deployment_id, account_equity,
                       account_invested, broker_available_cash, account_drawdown_pct
                FROM strategy_entry_preflights
                WHERE verdict = 'rejected' AND reason_code = ANY(%s)
                  AND evaluated_at > now() - make_interval(secs => %s)
                """,
                (sorted(_PAGED_ENTRY_REFUSALS), _LOOKBACK_S),
            ).fetchall()
        ]
        drills = _read_drills(conn)
    return Observation(blocks=blocks, kill_switch=kill_switch, refusals=refusals, drills=drills)


def _read_drills(conn: Any) -> list[DrillEvent]:
    rows = conn.execute(
        """
        SELECT kill_switch_drill_event_id, started_at, trigger, actor, entry_verdict, book_verdict,
               run_failure, failure_detail, kill_active_at_verify, unmapped_ownerships,
               multiply_mapped_ownerships, positions_without_stop, positions_without_target,
               code_commit, code_dirty
        FROM kill_switch_drill_events
        WHERE started_at >= %s
        ORDER BY kill_switch_drill_event_id
        """,
        (DRILL_WATCH_FROM,),
    ).fetchall()
    ids = [int(r[0]) for r in rows]
    chokepoints: dict[int, list[DrillChokepoint]] = {}
    for event_id, chokepoint, outcome, code in conn.execute(
        """
        SELECT event_id, chokepoint, outcome, refusal_code FROM kill_switch_drill_chokepoints
        WHERE event_id = ANY(%s) ORDER BY event_id, chokepoint
        """,
        (ids,),
    ).fetchall():
        chokepoints.setdefault(int(event_id), []).append(DrillChokepoint(str(chokepoint), str(outcome), code))
    # The ownerships behind ``mapping_defects`` and the stop/target counts in
    # ``app/services/kill_switch_drill.py``: no broker row, several, or a mapped row lacking either level.
    defects: dict[int, list[int]] = {}
    for event_id, ownership_id in conn.execute(
        """
        SELECT event_id, ownership_id FROM kill_switch_drill_positions
        WHERE event_id = ANY(%s)
        GROUP BY event_id, ownership_id
        HAVING count(broker_position_id) <> 1
            OR bool_or(broker_position_id IS NOT NULL AND (stop_present IS NOT TRUE OR target_present IS NOT TRUE))
        ORDER BY event_id, ownership_id
        """,
        (ids,),
    ).fetchall():
        defects.setdefault(int(event_id), []).append(int(ownership_id))
    return [
        DrillEvent(
            event_id=int(r[0]),
            started_at=r[1].timestamp(),
            trigger=str(r[2]),
            actor=str(r[3]),
            entry_verdict=str(r[4]),
            book_verdict=str(r[5]),
            run_failure=r[6],
            failure_detail=r[7],
            kill_active_at_verify=r[8],
            chokepoints=tuple(chokepoints.get(int(r[0]), ())),
            unmapped=r[9],
            multiply_mapped=r[10],
            without_stop=r[11],
            without_target=r[12],
            defect_ownerships=tuple(defects.get(int(r[0]), ())),
            code_commit=r[13],
            code_dirty=r[14],
        )
        for r in rows
    ]


def _refusal_evidence(*figures: object) -> str:
    """The risk figures the refusal row persisted, in SELECT order; a NULL is left out."""
    deployment_id, equity, invested, cash, drawdown_pct = figures
    parts = [
        f"{label} {value}"
        for label, value in (
            ("deployment", deployment_id),
            ("account equity", equity),
            ("invested", invested),
            ("available cash", cash),
        )
        if value is not None
    ]
    if drawdown_pct is not None:
        parts.append(f"drawdown {drawdown_pct}%")
    return ", ".join(parts)


def load_state(path: Path) -> WatchState:
    try:
        raw = json.loads(path.read_text())
        return WatchState(
            paged_blocks=tuple(str(s) for s in raw.get("paged_blocks", [])),
            seen={str(k): float(v) for k, v in raw.get("seen", {}).items()},
        )
    except OSError, ValueError, TypeError, AttributeError:
        return WatchState()


def save_state(path: Path, state: WatchState) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"paged_blocks": list(state.paged_blocks), "seen": dict(state.seen), "written_at": time.time()})
        )
    except OSError as exc:
        print(f"[operator-alert-watch] status file write failed: {exc!r}", file=sys.stderr, flush=True)
        return False
    return True


def run(*, status_file: Path = _DEFAULT_STATUS_FILE, dry_run: bool = False) -> int:
    """One pass. Returns the number of notices that could not be delivered."""
    obs = read_observation()
    now = time.time()
    prior = load_state(status_file)
    notices = plan(prior, obs, now=now)
    if dry_run:
        for n in notices:
            print(f"[operator-alert-watch] would send: {n.title}: {n.message}", file=sys.stderr, flush=True)
        return 0
    # Probe the status file before sending: unwritable means every pass re-sends, so say so.
    writable = not notices or save_state(status_file, prior)
    note = "" if writable else " [alert watch status file unwritable: not de-duplicated]"
    # As in the dead-man: with push configured, delivery means a 2xx; without it the
    # macOS notification is the only channel, and a notice it failed to show is retried.
    push_on = config_from_env() is not None
    delivered: list[Notice] = []
    for n in notices:
        message = n.message + note
        pushed = send_push(title=n.title, message=message, priority=n.priority, tags=(n.tag,))
        # Both channels on purpose, as ``jobs_dead_man.notify``: the banner is the record on
        # this machine either way. A pass holds at most the last hour's kill-switch toggles,
        # three blocks, one refusal notice and the drill notices not yet delivered (about twelve drills a
        # year), each ``osascript`` bounded at 5 s.
        shown = local_notify(n.title, message)
        ok = pushed or (not push_on and shown)
        print(f"[operator-alert-watch] {n.title}: {message} (push sent: {pushed})", file=sys.stderr, flush=True)
        if ok:
            delivered.append(n)
    save_state(status_file, settle(prior, delivered, now=now))
    return len(notices) - len(delivered)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--status-file", type=Path, default=_DEFAULT_STATUS_FILE)
    p.add_argument("--dry-run", action="store_true", help="print the notices without sending or saving")
    args = p.parse_args(argv)
    return 1 if run(status_file=args.status_file, dry_run=args.dry_run) else 0


if __name__ == "__main__":
    raise SystemExit(main())
