"""Read-only observer for the one attended demo session seven tickets are blocked on.

Refs #2961 #2965 #2979 #2942 #2993 #3007 #2949.
Protocol: ``docs/proposals/execution/2026-09-14-attended-demo-session-protocol.md``.

⚠⚠ THIS SCRIPT NEVER MUTATES BROKER STATE, AND THAT IS STRUCTURAL RATHER THAN POLITE.
``tests/test_unattended_broker_mutation_guard.py`` keeps ``_EXEMPT_SCRIPTS`` empty and
says an entry there *"needs an operator decision, not a commit"*. Every order and close
in the session is performed BY THE OPERATOR through the app's normal path; this module
only reads. The DB reads are read-only queries and the broker calls are informational.

WHAT IT IS FOR
--------------
Each of the seven tickets states a fragment of the same session on its own thread. The
protocol document consolidates them; this is its instrument. One invocation per phase,
each writing its own observation file, tied together by ``--session-id``.

⚠ Repeated polls MUST NOT overwrite each other: the latency arm (P3 arm L) is a
comparison BETWEEN reads, so a run that keeps only the last one has destroyed the
measurement it was taken for.

WHY EVERY CALL IS ISOLATED
--------------------------
A not-found is an EXPECTED outcome here, not a failure — it is half the discriminator.
``lookup_order`` raises ``BrokerOrderNotFound`` for it, so a run that let the first
exception escape would lose the control arm and every later observation, at the cost of
an attended window that cannot be re-run cheaply.

⚠ Outcome, HTTP status and parse result are recorded SEPARATELY. ``lookup_order`` can
reject a genuinely successful 200 during parsing, and ``get_order_status`` folds
transport failures into ``status="failed"`` — so a bare parsed object cannot distinguish
"the broker said no" from "we could not read what it said", and those two license
opposite conclusions about #2961.

PHASES
------
``negative-control``  Arm N. ⚠ UNATTENDED — run it BEFORE scheduling the session. Looks
                      up a freshly minted UUID that was never submitted. If the endpoint
                      does not cleanly answer "no" to garbage, it cannot support #2961's
                      fail-closed discriminator whatever else is observed, and the
                      session should not be booked.
``baseline``          Counter + bounded history + the eligible-exit-lot set (P2/P4).
``open``              Arms P and L for the BUY, plus the ``orderId`` control.
``close``             The EXIT raw body (#3007), the close-order read, arms for the exit
                      UUID, and the counter/history re-read.
``residual``          Post-session exposure and ledger check.

    PYTHONPATH=. uv run python -m scripts.probe_attended_demo_session \
        --phase baseline --session-id 2026-09-20-01 \
        --out tests/fixtures/etoro/attended_2026-09-20-01_baseline_1.json
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.config import settings
from app.providers.broker import BrokerOrderDetail, BrokerOrderLookupError, BrokerOrderNotFound
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.operators import sole_operator_id

_CALLER = "probe_attended_demo_session"

#: ⚠ BOUNDED, not ``HISTORY_EPOCH``. An epoch read is an unbounded paced pagination loop
#: on lane G's 3.33 s floor and can consume the attended window; it also exceeds the
#: operation's documented lookback guidance (#2991). The baseline and close phases MUST
#: use the same window — two differently-scoped reads cannot be diffed.
_DEFAULT_HISTORY_DAYS = 90

_PHASES = ("negative-control", "baseline", "open", "close", "residual")


# --------------------------------------------------------------------------
# Pure classification — table-tested, no DB and no broker
# --------------------------------------------------------------------------

#: The rows of the protocol's P3 table, keyed ``(referenceId outcome, orderId outcome)``.
#: ⚠ Every value states what remains UNCONTROLLED. A reading recorded without its
#: residual ambiguity is how "one lookup 404'd" becomes "v1 is invisible to v2" three
#: sessions later.
_LOOKUP_READINGS: dict[tuple[str, str], tuple[str, str]] = {
    ("found", "found"): (
        "reference_key_present",
        "v1 submissions are keyed by referenceId. Says NOTHING about absence: arms N and "
        "L must both be present before a 404 may be read as non-acceptance (#2961).",
    ),
    ("not_found", "found"): (
        "reference_key_absent_or_lagging",
        "The order IS in the v2 index; the referenceId key is absent OR still lagging — "
        "arm L separates those. An orderId reconciler is viable but only for crashes "
        "AFTER broker_order_ref was persisted, which is not #2942 half 2's window.",
    ),
    ("not_found", "not_found"): (
        "v1_absent_from_v2_index",
        "Consistent with v1 being invisible to v2 lookup. ⚠ Also consistent with a wrong "
        "id, retention, or a lag longer than arm L's last offset. Not a migration mandate.",
    ),
    ("found", "not_found"): (
        "contradictory",
        "Record, do not interpret. A referenceId hit with an orderId miss contradicts the "
        "index model both readings assume.",
    ),
}


def classify_lookup_pair(reference_outcome: str, order_outcome: str) -> dict[str, str]:
    """Map one (referenceId, orderId) observation pair onto a P3 reading.

    ⚠ ``error`` is NOT folded into ``not_found``. An HTTP 500, a 429 or a parse failure
    licenses no conclusion at all, while a clean not-found is half the discriminator —
    collapsing them would manufacture evidence for #2961 out of an outage.
    """
    if reference_outcome == "error" or order_outcome == "error":
        return {
            "reading": "inconclusive",
            "note": (
                f"reference={reference_outcome} order={order_outcome}: an error is not a "
                f"not-found. No P3 row applies; re-run the arm."
            ),
        }
    if "absent" in (reference_outcome, order_outcome):
        # ⚠ An arm DELIBERATELY not run is not the same as an unmodelled result, and the
        # difference is visible in the artefact an operator reads. The negative-control
        # phase runs no orderId arm by design; labelling that "unmodelled outcome pair"
        # makes a correct run look like a defect in the instrument, which is how a clean
        # measurement gets re-taken or discarded.
        return {
            "reading": "single_arm",
            "note": (
                f"reference={reference_outcome} order={order_outcome}: only one arm was run, "
                f"so no P3 row applies. Expected for --phase negative-control, which asks "
                f"only whether the endpoint cleanly answers 'no' to a UUID it has never seen."
            ),
        }
    reading = _LOOKUP_READINGS.get((reference_outcome, order_outcome))
    if reading is None:
        return {
            "reading": "inconclusive",
            "note": f"unmodelled outcome pair reference={reference_outcome} order={order_outcome}",
        }
    return {"reading": reading[0], "note": reading[1]}


def diff_event_counts(
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Per-``(closeYear, assetType)`` deltas for the closed-position-events counter.

    ⚠ PER BUCKET, not on the total. #2993's discriminator is phrased as "the current-year
    figure moves by one", but the counter is bucketed and a bucket can appear, vanish or
    move negative. A total-only diff hides a +1 in one asset type cancelling a −1 in
    another, and reports the one number the reader will quote.

    ⚠ A delta of 0, of more than 1, or negative are all RECORDABLE OUTCOMES, not errors.
    Unchanged is not evidence of non-demo scope (delayed publication, caching, an
    excluded bucket and an incomplete close all produce it) — see the protocol's P4.
    """
    keys = {(row["closeYear"], row["assetType"]) for row in before} | {
        (row["closeYear"], row["assetType"]) for row in after
    }
    index_before = {(r["closeYear"], r["assetType"]): int(r["closedPositionEvents"]) for r in before}
    index_after = {(r["closeYear"], r["assetType"]): int(r["closedPositionEvents"]) for r in after}
    deltas: list[dict[str, Any]] = []
    for close_year, asset_type in sorted(keys, key=lambda k: (str(k[0]), str(k[1]))):
        was = index_before.get((close_year, asset_type))
        now = index_after.get((close_year, asset_type))
        deltas.append(
            {
                "closeYear": close_year,
                "assetType": asset_type,
                "before": was,
                "after": now,
                "delta": None if was is None or now is None else now - was,
                "bucket_appeared": was is None and now is not None,
                "bucket_vanished": was is not None and now is None,
            }
        )
    return deltas


# --------------------------------------------------------------------------
# Isolated observation helpers
# --------------------------------------------------------------------------


def _observe(label: str, call: Any) -> dict[str, Any]:
    """Run one broker read, capturing its outcome instead of propagating it.

    ⚠ ``BrokerOrderNotFound`` is recorded as ``not_found`` and everything else as
    ``error`` WITH its exception type. The distinction is the measurement.

    ⚠⚠ A FAILING READ STILL CARRIES EVIDENCE, and discarding it is the expensive
    mistake here. ``get_demo_close_order`` raises ``BrokerPositionMutationUncertain``
    with the response body attached when, for instance, ``referenceID`` is present but
    not a UUID — and that body is exactly the affected-position and reference evidence
    #2979 and #3007 need. The exception is the only place it exists, so any
    ``raw_payload`` on it is preserved rather than collapsed into a message string.
    """
    observation: dict[str, Any] = {"call": label, "at": datetime.now(UTC).isoformat()}
    try:
        observation["result"] = call()
        observation["outcome"] = "found"
    except BrokerOrderNotFound as exc:
        observation["outcome"] = "not_found"
        observation["error"] = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001 — the outcome IS the datum
        observation["outcome"] = "error"
        observation["error"] = f"{type(exc).__name__}: {exc}"
        payload = getattr(exc, "raw_payload", None)
        if payload is not None:
            observation["error_raw_payload"] = payload
    return observation


def _order_detail_json(detail: BrokerOrderDetail) -> dict[str, Any]:
    """Serialise a lookup result.

    ⚠ ``reference_id`` is ECHOED from the query argument by ``_parse_order_detail`` — it
    is not a broker acknowledgement. It is emitted under ``echoed_reference_id`` so no
    reader can mistake it for one, and ``raw_payload`` is kept so any genuine
    broker-returned reference can be read from the body itself.
    """
    return {
        "broker_order_ref": detail.broker_order_ref,
        "echoed_reference_id": detail.reference_id,
        "status": detail.status,
        "broker_status": detail.broker_status,
        "instrument_id": detail.instrument_id,
        "last_update": detail.last_update.isoformat() if detail.last_update else None,
        # ⚠ Compared on the IMMUTABLE fields #2965 turns on (opening units, average
        # price, execution time, fees) — not on position ids alone. Two polls can agree
        # on ids while disagreeing about the facts the reconciler treats as immutable.
        "position_executions": [
            {
                "position_id": e.position_id,
                "state": e.state,
                "remaining_units": str(e.remaining_units) if e.remaining_units is not None else None,
                "opening_units": str(e.opening_units) if e.opening_units is not None else None,
                "average_price": str(e.average_price) if e.average_price is not None else None,
                "execution_time": e.execution_time.isoformat() if e.execution_time else None,
                "fees": str(e.fees) if e.fees is not None else None,
            }
            for e in detail.position_executions
        ],
        "raw_payload": detail.raw_payload,
    }


def _order_status_read(broker: EtoroBrokerProvider, broker_order_ref: Any) -> dict[str, Any]:
    """Read v1 order info, keeping the transport outcome distinguishable.

    ⚠⚠ ``get_order_status`` NEVER RAISES. It catches HTTP status errors, transport
    failures and non-JSON bodies and returns a ``BrokerOrderResult`` with
    ``status="failed"`` and the error in ``raw_payload``. Handing ``_observe`` only the
    payload therefore labels an HTTP 503 ``outcome="found"`` — a failed read entering the
    artefact as a successful observation.

    That is precisely the confusion the protocol's P3 warns about ("a bare parsed object
    cannot distinguish 'the broker said no' from 'we could not read what it said'"), and
    the two license opposite conclusions about #2961. So the normalised status is carried
    out alongside the payload, and a ``failed`` one is raised here so ``_observe``
    classifies it as ``error`` rather than evidence.
    """
    result = broker.get_order_status(str(broker_order_ref))
    if result.status == "failed":
        raise BrokerOrderLookupError(
            f"get_order_status returned status='failed' (not raised by the provider): {result.raw_payload}"
        )
    return {
        "normalised_status": result.status,
        "filled_price": str(result.filled_price) if result.filled_price is not None else None,
        "filled_units": str(result.filled_units) if result.filled_units is not None else None,
        "raw_payload": result.raw_payload,
    }


def _lookup_arms(
    broker: EtoroBrokerProvider,
    *,
    reference_id: str | None,
    broker_order_ref: str | None,
) -> dict[str, Any]:
    """Arm P (or N) plus the ``orderId`` control, classified."""
    arms: dict[str, Any] = {}
    if reference_id is not None:
        arms["by_reference_id"] = _observe(
            f"lookup_order(reference_id={reference_id})",
            lambda: _order_detail_json(broker.lookup_order(reference_id=reference_id)),
        )
    if broker_order_ref is not None:
        arms["by_order_id"] = _observe(
            f"lookup_order(order_id={broker_order_ref})",
            lambda: _order_detail_json(broker.lookup_order(order_id=broker_order_ref)),
        )
    arms["reading"] = classify_lookup_pair(
        arms.get("by_reference_id", {}).get("outcome", "absent"),
        arms.get("by_order_id", {}).get("outcome", "absent"),
    )
    return arms


# --------------------------------------------------------------------------
# DB reads — all read-only
# --------------------------------------------------------------------------


def _load_demo_credentials(conn: psycopg.Connection[Any]) -> tuple[str, str]:
    ensure_broker_key_loaded(conn)
    operator_id = sole_operator_id(conn)
    keys: list[str] = []
    for label in ("api_key", "user_key"):
        keys.append(
            load_credential_for_provider_use(
                conn,
                operator_id=operator_id,
                provider="etoro",
                label=label,
                environment="demo",
                caller=_CALLER,
            )
        )
        conn.commit()
    return keys[0], keys[1]


def _read_order(conn: psycopg.Connection[Any], order_id: int) -> dict[str, Any] | None:
    """The durable submission identity, and the verbatim broker response.

    ⚠ ``raw_payload_json`` is what decides #3007, at ZERO broker cost: ``_build_result``
    stores the response body unmodified and ``_update_order_with_broker_result``
    persists it that way. Reading the normalised columns instead would read the very
    normaliser whose correctness is the question.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT order_id, instrument_id, action, status, broker_order_ref,
                   recommendation_request_id, requested_amount, requested_units,
                   raw_payload_json, created_at
            FROM orders WHERE order_id = %(oid)s
            """,
            {"oid": order_id},
        )
        return cur.fetchone()


def _read_exit_lots(conn: psycopg.Connection[Any], instrument_id: int | None) -> dict[str, Any]:
    """The lot set ``_load_exit_lot`` chooses from, and the one it would pick.

    ⚠ P2. The selector is FIFO-OLDEST over ``units > 0 AND is_buy AND position_id > 0``,
    so with any pre-existing long lot the EXIT closes THAT one and not the position the
    session just opened. This mirrors the selector rather than re-deriving it, and
    reports the selection explicitly so the close leg is interpreted against the lot the
    broker was actually asked to close.
    """
    params: dict[str, Any] = {"iid": instrument_id}
    scope = "AND instrument_id = %(iid)s::bigint" if instrument_id is not None else ""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT instrument_id, position_id, units, is_buy, open_date_time,
                   position_id > 0 AS broker_closeable
            FROM broker_positions
            WHERE units > 0 {scope}
            ORDER BY instrument_id, open_date_time ASC, position_id ASC
            """,
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]
    eligible = [r for r in rows if r["is_buy"] and r["broker_closeable"]]
    return {
        "all_positive_unit_lots": rows,
        "eligible_for_exit": eligible,
        "would_select": eligible[0] if eligible else None,
        "note": (
            "would_select is the lot _load_exit_lot picks (FIFO-oldest, units>0, is_buy, "
            "position_id>0). None means an EXIT refuses with 'no broker-closeable long "
            "lot found' — expected immediately after an eBull BUY, which writes the "
            "synthetic -order_id id (#3006). Run portfolio_sync first."
        ),
    }


def _read_residual(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """Post-session exposure and ledger state (step 6)."""
    out: dict[str, Any] = {}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT status, count(*) AS n FROM orders "
            "WHERE status IN ('submitted','pending','uncertain') GROUP BY status"
        )
        out["unresolved_orders"] = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT instrument_id, current_units, cost_basis, avg_cost FROM positions WHERE current_units <> 0")
        out["open_positions"] = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT COALESCE(SUM(amount), 0) AS balance FROM cash_ledger")
        row = cur.fetchone()
        out["cash_ledger_balance"] = row["balance"] if row else None
        # ⚠ `trade_recommendations`, not `recommendations` (`sql/001_init.sql:125`). The
        # wrong name raises UndefinedTable, which escapes before the report is written —
        # losing the whole post-session observation at the one moment it cannot be retaken.
        cur.execute(
            "SELECT status, count(*) AS n FROM trade_recommendations "
            "WHERE status IN ('approved','execution_pending') GROUP BY status"
        )
        out["open_recommendations"] = [dict(r) for r in cur.fetchall()]
    return out


# --------------------------------------------------------------------------
# Phases
# --------------------------------------------------------------------------


def resolve_history_min_date(
    *,
    history_min_date: str | None,
    history_days: int,
    now: datetime,
) -> tuple[datetime, bool]:
    """Resolve the history cutoff, and say whether it is comparable across phases.

    ⚠⚠ A RELATIVE WINDOW IS NOT THE SAME WINDOW TWICE. The protocol promises the baseline
    and close phases read an identical range, but ``now - history_days`` is re-evaluated
    per invocation, so the lower boundary walks forward between them — and a trade sitting
    near it can leave the range for that reason alone, contaminating the #2993 comparison
    with a difference the session did not cause.

    So an absolute ``--history-min-date`` is the comparable form, and the relative one is
    kept only for the FIRST phase (which has nothing to compare against yet). The boolean
    travels into the report so a later reader can see which was used rather than assume.
    """
    if history_min_date is not None:
        parsed = datetime.fromisoformat(history_min_date)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed, True
    return now - timedelta(days=history_days), False


def _counter_and_history(
    broker: EtoroBrokerProvider,
    min_date: datetime,
    *,
    comparable: bool,
) -> dict[str, Any]:

    def _counts() -> list[dict[str, Any]]:
        return [
            {
                "closeYear": c.close_year,
                "assetType": c.asset_type,
                "closedPositionEvents": c.closed_position_events,
            }
            for c in broker.get_closed_position_event_counts()
        ]

    def _history() -> dict[str, Any]:
        trades = broker.get_trade_history(min_date)
        return {
            "row_count": len(trades),
            # Per-SLICE identity: eToro reduces the same positionId on a partial close,
            # so ids alone would call two different responses "the same rows".
            "slices": sorted(
                (t.position_id, t.close_timestamp.isoformat(), str(t.units), str(t.net_profit)) for t in trades
            ),
        }

    return {
        "history_min_date": min_date.isoformat(),
        "history_min_date_is_absolute": comparable,
        "comparability_note": (
            "Comparable: pass this exact history_min_date to the close phase via --history-min-date."
            if comparable
            else "⚠ RELATIVE WINDOW — not comparable to another phase. Re-read the "
            "close phase with --history-min-date set to this value, or the #2993 "
            "history diff includes rows that merely aged out of the range."
        ),
        "closed_event_counts": _observe("get_closed_position_event_counts()", _counts),
        "trade_history": _observe(f"get_trade_history(min_date={min_date.isoformat()})", _history),
    }


def _run_phase(
    phase: str,
    *,
    conn: psycopg.Connection[Any],
    broker: EtoroBrokerProvider,
    order_id: int | None,
    instrument_id: int | None,
    history_min_date: datetime,
    history_comparable: bool,
) -> dict[str, Any]:
    if phase == "negative-control":
        never_submitted = str(uuid.uuid4())
        return {
            "arm": "N_negative_control",
            "minted_reference_id": never_submitted,
            "expectation": "not_found — a UUID that was never submitted must resolve to nothing",
            "arms": _lookup_arms(broker, reference_id=never_submitted, broker_order_ref=None),
            "note": (
                "⚠ If this is not a clean not_found, #2961's fail-closed discriminator is "
                "unsupportable whatever the attended session observes, and the session "
                "should not be booked on it."
            ),
        }

    if phase == "baseline":
        return {
            "exit_lots": _read_exit_lots(conn, instrument_id),
            **_counter_and_history(broker, history_min_date, comparable=history_comparable),
        }

    if phase in ("open", "close"):
        if order_id is None:
            raise SystemExit(f"--phase {phase} requires --order-id")
        order = _read_order(conn, order_id)
        if order is None:
            raise SystemExit(f"no orders row for order_id={order_id}")
        reference_id = order["recommendation_request_id"]
        broker_order_ref = order["broker_order_ref"]
        result: dict[str, Any] = {
            "order_row": order,
            # ⚠ The #3007 discriminator, and it needs no broker call.
            "raw_payload_json": order["raw_payload_json"],
            "arms": _lookup_arms(
                broker,
                reference_id=str(reference_id) if reference_id else None,
                broker_order_ref=str(broker_order_ref) if broker_order_ref else None,
            ),
            "order_status_read": (
                _observe(f"get_order_status({broker_order_ref})", lambda: _order_status_read(broker, broker_order_ref))
                if broker_order_ref
                else {"outcome": "absent", "call": "get_order_status", "error": "no broker_order_ref persisted"}
            ),
        }
        if reference_id is None:
            result["warning"] = (
                "recommendation_request_id is NULL — this order did not go through the "
                "#2942 claim path, or enable_live_trading was false and the fill was "
                "SYNTHETIC. Arms P/L measure nothing here. See the protocol's P1."
            )
        if phase == "close":

            def _close_read() -> dict[str, Any]:
                detail = broker.get_demo_close_order(order_id=str(broker_order_ref))
                return {
                    "broker_order_ref": detail.broker_order_ref,
                    "normalised_status": detail.status,
                    "broker_status": detail.broker_status,
                    "position_ids": list(detail.position_ids),
                    # ⚠ Parsed only. `get_demo_close_order` requires a UUID and will
                    # reject an otherwise-informative non-UUID string, so the raw body is
                    # kept alongside and is the thing to compare against our submitted id.
                    "parsed_reference_id": str(detail.reference_id) if detail.reference_id else None,
                    "raw_payload": detail.raw_payload,
                    "note": (
                        "⚠ normalised_status is 'filled' whenever the affected-position "
                        "list is non-empty and no error code is present — match the "
                        "SPECIFIC position id and its units instead of trusting it."
                    ),
                }

            result["close_order_read"] = (
                _observe(f"get_demo_close_order(order_id={broker_order_ref})", _close_read)
                if broker_order_ref
                else {"outcome": "absent", "call": "get_demo_close_order"}
            )
            result.update(_counter_and_history(broker, history_min_date, comparable=history_comparable))
        return result

    if phase == "residual":
        return {"residual": _read_residual(conn), "exit_lots": _read_exit_lots(conn, instrument_id)}

    raise SystemExit(f"unknown phase {phase!r}")


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=_PHASES)
    parser.add_argument("--session-id", required=True, help="ties every file of one session together")
    parser.add_argument("--order-id", type=int, help="orders.order_id for the open/close phases")
    parser.add_argument("--instrument-id", type=int, help="scope the exit-lot read")
    parser.add_argument("--history-days", type=int, default=_DEFAULT_HISTORY_DAYS)
    parser.add_argument(
        "--history-min-date",
        help=(
            "ISO absolute cutoff. ⚠ REQUIRED for the close phase to be comparable with "
            "the baseline — copy the baseline file's history_min_date verbatim."
        ),
    )
    parser.add_argument("--out", type=pathlib.Path, help="write the observation set here")
    args = parser.parse_args(argv)

    # ⚠ Without this the run is INVISIBLE to the #2946 request artefact: the per-request
    # lines `etoro_request_log` emits are INFO, and a bare script has no handler and an
    # effective level of WARNING, so every observed attempt is discarded at process exit.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    started = datetime.now(UTC)
    history_min_date, history_comparable = resolve_history_min_date(
        history_min_date=args.history_min_date,
        history_days=args.history_days,
        now=started,
    )
    with psycopg.connect(settings.database_url) as conn:
        api_key, user_key = _load_demo_credentials(conn)
        with EtoroBrokerProvider(api_key, user_key, env="demo") as broker:
            body = _run_phase(
                args.phase,
                conn=conn,
                broker=broker,
                order_id=args.order_id,
                instrument_id=args.instrument_id,
                history_min_date=history_min_date,
                history_comparable=history_comparable,
            )

    report = {
        "_meta": {
            "refs": "#2961 #2965 #2979 #2942 #2993 #3007 #2949",
            "protocol": "docs/proposals/execution/2026-09-14-attended-demo-session-protocol.md",
            "session_id": args.session_id,
            "phase": args.phase,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "environment": "demo",
            "mutations_performed": "none — this script is read-only by construction",
        },
        "observation": body,
    }
    print(json.dumps(report, indent=2, default=_json_default))
    if args.out:
        if args.out.exists():
            raise SystemExit(
                f"{args.out} already exists — refusing to overwrite. The latency arm is a "
                f"comparison BETWEEN reads, so an overwritten poll destroys the measurement."
            )
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, default=_json_default) + "\n")
        print(f"\nwrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
