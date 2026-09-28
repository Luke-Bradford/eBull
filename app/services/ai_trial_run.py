"""#3471 slice 2c-iv-b — the trial decision run: claim, steps 0-5, one-transaction publish.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §3 (pipeline and
session identity), §4, §6, §7, §9 ("Runtime checks") and §11 (``ai_trial_runs``).

    declaration → sweep stale claims → claim (lease = claim + 15 min)
      → declaration / policy / state checks → step 0 → step 1 → leg books → step 2 (pack)
      → step 3 (model) → step 4 (validate) → publish: step 5 draw + decided + every
        decision, pair, signal and leg link, in ONE transaction

* No frozen declaration → ``declaration_missing`` and nothing is written: a run row needs one.
* One claim per target session (``sql/432`` unique key), so at most one model call per session.
  A second claim for the same session is ``duplicate`` and writes nothing.
* Every refusal after the claim is recorded on the run with what it reached (§11).
* The step-5 draw runs INSIDE the publish transaction, because its seed needs the trial-global
  ``pair_seq``, read under the declaration row lock. ``sql/432``'s triggers re-check the draw,
  the dense sequence and that everything shares the ``decided`` transaction.
* A lease that expires before the publish commits: the trigger refuses it, and the run is
  recorded ``refused`` / ``stale_claim`` — a late worker never publishes (§11).

The trial signals (``strategy_signals``) are the arm and control legs' fired entries for the
target session. ``fill_price`` is the decision-time reference, the leg name's last pack close
(the §6 measurement's ``close``): no backtest fill exists for a forward trial, and the executor
prices from its own pre-submission ask (§8).

Nothing here touches the broker: the intraday fetch and the account-risk snapshot are injected
by the job (slice 2c-iv-c), which also resolves the CLI and records the git sha (O4).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final, Literal

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services.ai_trial_decision import (
    PlannedDecision,
    ValidationResult,
    decision_json_schema,
    pack_atr_measurements,
    plan_pairs,
    validate_response,
)
from app.services.ai_trial_executor import TRIAL_MAX_CONCURRENT_PER_LEG
from app.services.ai_trial_intent import DECLARATION_CONTRACT_PREFIX, declaration_digest
from app.services.ai_trial_invocation import TRIAL_MODEL_ID, InvocationResult, build_argv, invoke_model
from app.services.ai_trial_pack import NonCanonicalValue, canonical_json, canonical_sha256
from app.services.ai_trial_pack_reader import (
    AccountContext,
    IntradayFetch,
    Pack,
    Step1,
    assemble_pack,
    next_us_session,
    read_step1,
)
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH
from app.services.ai_trial_prompt import (
    PROMPT_TEMPLATE_SHA256,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_SHA256,
    RenderedPrompt,
    render_user_prompt,
)
from app.services.ai_trial_start_gate import REFUSAL_PREFIX, preview_trial_capacity
from app.services.market_calendar import latest_completed_us_session
from app.services.strategy_manifest import DEMO_TRIAL_STRATEGY_IDS

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

#: §8 "two legs, two identities": the arm's id; the control's is this plus ``-control``.
TRIAL_ARM_STRATEGY_ID: Final = "ai-discretionary-v1"
TRIAL_STRATEGY_VERSION: Final = "v1"
if {TRIAL_ARM_STRATEGY_ID, TRIAL_ARM_STRATEGY_ID + "-control"} != DEMO_TRIAL_STRATEGY_IDS:
    raise RuntimeError("the trial leg ids must be strategy_manifest.DEMO_TRIAL_STRATEGY_IDS")

#: A live, tradable shortlist is survivor-only by construction (#2288 labelling contract).
SIGNAL_UNIVERSE: Final = "survivor_only"
#: §11 / O4: stdout and stderr share one cap, matching ``ai_trial_runs_output_capped``.
STORED_OUTPUT_CAP_BYTES: Final = 2 * 1024 * 1024

Leg = Literal["arm", "control"]


@dataclass(frozen=True)
class RunEnvironment:
    """What the job resolves once and the run records (§4, O4)."""

    #: Absolute path of the ``claude`` CLI, resolved at deploy time.
    executable: str
    cli_version: str
    #: The 40-hex git sha of the code that runs.
    git_sha: str
    source_env: Mapping[str, str]


@dataclass(frozen=True)
class RunOutcome:
    status: Literal["declaration_missing", "duplicate", "refused", "decided"]
    run_id: int | None = None
    session_date: date | None = None
    refusal_reason: str | None = None
    pair_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class Declaration:
    declaration_id: int
    doc: Any
    doc_sha256: str
    contract_version: str | None
    #: The latest ``ai_trial_state_events.to_state``; ``None`` = no event, which is not active.
    state: str | None


@dataclass(frozen=True)
class LegBook:
    """One leg's slot-holding trades: allocated and not closed or failed (the executor's
    ``_open_leg_trades`` population — pending and uncertain trades hold a slot, O8)."""

    held_instrument_ids: frozenset[int]
    open_positions: tuple[dict[str, Any], ...]

    @property
    def free_slots(self) -> int:
        return max(0, TRIAL_MAX_CONCURRENT_PER_LEG - len(self.open_positions))


# ---------------------------------------------------------------------------
# Declaration, claim
# ---------------------------------------------------------------------------
_DECLARATION_SQL: Final = """
    SELECT dcl.declaration_id, dcl.doc, dcl.doc_sha256, prereg.contract_version,
           (SELECT se.to_state FROM ai_trial_state_events se
             WHERE se.declaration_id = dcl.declaration_id
             ORDER BY se.event_id DESC LIMIT 1) AS state
    FROM ai_trial_declarations dcl
    JOIN strategy_preregistration_declarations prereg ON prereg.declaration_id = dcl.declaration_id
    WHERE dcl.strategy_id = %s AND dcl.strategy_version = %s
"""


def load_declaration(conn: Conn, *, strategy_version: str = TRIAL_STRATEGY_VERSION) -> Declaration | None:
    row = conn.execute(_DECLARATION_SQL, (TRIAL_ARM_STRATEGY_ID, strategy_version)).fetchone()
    if row is None:
        return None
    return Declaration(int(row[0]), row[1], str(row[2]), row[3], row[4])


def declaration_refusal(declaration: Declaration, *, policy_hash: str = AI_TRIAL_POLICY_HASH) -> str | None:
    """§9 runtime checks, before any read that costs: the stored document is digest-intact and
    named by its #2599 contract (the loader's rule), its ``policy_hash`` is this code's
    (``policy_drift``, O6), and the trial is ``active``."""
    try:
        digest = declaration_digest(declaration.doc)
    except NonCanonicalValue:
        return "trial_declaration_not_intact"
    if digest != declaration.doc_sha256 or declaration.contract_version != DECLARATION_CONTRACT_PREFIX + digest:
        return "trial_declaration_not_intact"
    if not isinstance(declaration.doc, dict) or declaration.doc.get("policy_hash") != policy_hash:
        return "policy_drift"
    if declaration.state != "active":
        return "trial_not_active"
    return None


def sweep_stale_claims(conn: Conn, declaration_id: int) -> int:
    """Close every claim whose lease has run out as ``refused`` / ``stale_claim`` — the only
    transition ``sql/432`` allows after the lease. A crashed worker's run is recorded, not left
    ``claimed`` forever."""
    swept = conn.execute(
        """
        UPDATE ai_trial_runs SET status = 'refused', refusal_reason = 'stale_claim'
        WHERE declaration_id = %s AND status = 'claimed' AND lease_until < clock_timestamp()
        """,
        (declaration_id,),
    ).rowcount
    conn.commit()
    return swept


@dataclass(frozen=True)
class Claim:
    run_id: int
    as_of: datetime
    session_date: date


def claim_run(conn: Conn, declaration_id: int) -> Claim | None:
    """Insert the session's ``claimed`` run, or ``None`` when that session is already claimed.

    ``as_of`` is the database's clock at the claim (the insert trigger refuses a future claim),
    and the target session is §3's: the first session after the last completed one at ``as_of``.
    A weekend or holiday fire therefore targets the last weekday run's session and is a
    duplicate, never a second run.
    """
    row = conn.execute("SELECT clock_timestamp()").fetchone()
    assert row is not None
    as_of: datetime = row[0]
    session_date = next_us_session(latest_completed_us_session(as_of))
    inserted = conn.execute(
        """
        INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until)
        VALUES (%s, %s, %s, %s + interval '15 minutes')
        ON CONFLICT (declaration_id, session_date) DO NOTHING
        RETURNING run_id
        """,
        (declaration_id, session_date, as_of, as_of),
    ).fetchone()
    conn.commit()
    return None if inserted is None else Claim(int(inserted[0]), as_of, session_date)


# ---------------------------------------------------------------------------
# Leg books (the pack's account context, the §6 holdings, the §7 control exclusion)
# ---------------------------------------------------------------------------
_LEG_BOOK_SQL: Final = """
    SELECT d.strategy_id, s.instrument_id, i.symbol, t.status, t.exit_deadline_session,
           bp.open_rate, bp.stop_loss_rate, bp.take_profit_rate
    FROM ai_trial_declarations dcl
    JOIN strategy_deployments d
      ON d.strategy_id IN (dcl.strategy_id, dcl.strategy_id || '-control')
     AND d.strategy_version = dcl.strategy_version AND d.mode = 'paper'
    JOIN strategy_funding_decisions fd ON fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
    JOIN strategy_signals s ON s.signal_id = fd.signal_id
    JOIN instruments i ON i.instrument_id = s.instrument_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN strategy_position_ownership ow
      ON ow.strategy_trade_id = t.strategy_trade_id AND ow.status = 'active'
    LEFT JOIN broker_positions bp ON bp.position_id = ow.broker_position_id
    WHERE dcl.declaration_id = %s
      AND (t.strategy_trade_id IS NULL OR t.status NOT IN ('closed', 'failed'))
    ORDER BY s.instrument_id, fd.funding_decision_id
"""


def read_leg_books(conn: Conn, declaration_id: int) -> dict[Leg, LegBook]:
    """Each leg's slot-holding trades. A pending or uncertain trade has no broker position yet,
    so its entry, stop and target are ``null`` — it still holds the slot and the name."""
    positions: dict[Leg, list[dict[str, Any]]] = {"arm": [], "control": []}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_LEG_BOOK_SQL, (declaration_id,))
        for r in cur.fetchall():
            leg: Leg = "control" if str(r["strategy_id"]).endswith("-control") else "arm"
            positions[leg].append(
                {
                    "symbol": r["symbol"],
                    "instrument_id": int(r["instrument_id"]),
                    "status": r["status"] or "planned",
                    "entry": r["open_rate"],
                    "stop": r["stop_loss_rate"],
                    "target": r["take_profit_rate"],
                    "deadline": r["exit_deadline_session"],
                }
            )
    return {leg: LegBook(frozenset(p["instrument_id"] for p in rows), tuple(rows)) for leg, rows in positions.items()}


def max_new_entries(books: Mapping[Leg, LegBook]) -> int:
    """§6: ``min(2, arm free slots, control free slots)``."""
    return min(2, books["arm"].free_slots, books["control"].free_slots)


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------
def _text(raw: bytes, budget: int) -> str:
    """Decoded output within ``budget`` UTF-8 bytes. NUL is not storable in ``text``; a
    replacement character can take more bytes than the byte it replaces, so the budget is
    enforced AFTER decoding, never assumed from the raw length."""
    text = raw.decode("utf-8", errors="replace").replace("\x00", "\ufffd")
    encoded = text.encode("utf-8")
    if len(encoded) <= budget:
        return text
    return encoded[:budget].decode("utf-8", errors="ignore")


def _stored_output(result: InvocationResult) -> tuple[str, str]:
    stdout = _text(result.stdout, STORED_OUTPUT_CAP_BYTES)
    stderr = _text(result.stderr, STORED_OUTPUT_CAP_BYTES - len(stdout.encode("utf-8")))
    return stdout, stderr


def _provenance(
    env: RunEnvironment,
    *,
    step1: Step1 | None = None,
    pack: Pack | None = None,
    prompt: RenderedPrompt | None = None,
    result: InvocationResult | None = None,
) -> dict[str, Any]:
    """The run's columns for whatever the run reached (§11: a refused run carries what it
    reached; a decided one carries all of it)."""
    values: dict[str, Any] = {
        "policy_hash": AI_TRIAL_POLICY_HASH,
        "git_sha": env.git_sha,
        "cli_version": env.cli_version,
        "executable_path": env.executable,
        "model_id": TRIAL_MODEL_ID,
        "system_prompt_sha256": SYSTEM_PROMPT_SHA256,
        "prompt_template_sha256": PROMPT_TEMPLATE_SHA256,
    }
    if step1 is not None and step1.scores_run is not None:
        values["scores_model_version"] = step1.scores_run.model_version
        values["scores_scored_at"] = step1.scores_run.scored_at
    if pack is not None:
        values["pack"] = Jsonb(pack.pack, dumps=canonical_json)
        values["pack_sha256"] = pack.sha256
    if prompt is not None:
        values["rendered_prompt"] = prompt.text
        values["rendered_prompt_sha256"] = prompt.sha256
    if result is not None:
        event = result.result_event or {}
        stdout, stderr = _stored_output(result)
        values.update(
            {
                # The argv `invoke_model` builds from these same inputs (O4: pinned by a unit test).
                "argv_sha256": canonical_sha256(
                    build_argv(
                        env.executable,
                        model_id=TRIAL_MODEL_ID,
                        system_prompt=SYSTEM_PROMPT,
                        json_schema=decision_json_schema(),
                    )
                ),
                "init_event": None if result.init_event is None else Jsonb(result.init_event),
                "exit_code": result.exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "structured_output": None if result.structured_output is None else Jsonb(result.structured_output),
                "num_turns": event.get("num_turns"),
                "cost_usd": event.get("total_cost_usd"),
                "duration_ms": result.duration_ms,
            }
        )
    return values


def _update_run(conn: Conn, run_id: int, status: str, values: Mapping[str, Any]) -> None:
    # Column names come from `_provenance` and the literals in this module, never from input,
    # and are quoted as identifiers regardless.
    columns = ["status", *values]
    assignments = sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder(c)) for c in columns)
    conn.execute(
        sql.SQL("UPDATE ai_trial_runs SET {} WHERE run_id = {}").format(assignments, sql.Placeholder("run_id")),
        {"status": status, "run_id": run_id, **values},
    )


def refuse_run(conn: Conn, claim: Claim, reason: str, values: Mapping[str, Any]) -> RunOutcome:
    """Record ``claimed → refused`` with the reason and what the run reached, in its own
    transaction. After the lease only ``stale_claim`` is legal, so that is what a late refusal
    records."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    row = conn.execute(
        "SELECT status, refusal_reason, clock_timestamp() > lease_until FROM ai_trial_runs WHERE run_id = %s",
        (claim.run_id,),
    ).fetchone()
    assert row is not None
    if row[0] != "claimed":  # already recorded (a refusal inside the run, then its exception)
        conn.commit()
        return RunOutcome(row[0], claim.run_id, claim.session_date, row[1])
    recorded = "stale_claim" if row[2] else reason
    if recorded != reason:
        # sql/432 allows only `stale_claim` after the lease, so the column cannot keep the
        # reason the run actually reached; the log does.
        logger.warning("ai_trial run %s lease expired; refusal %s recorded as stale_claim", claim.run_id, reason)
    _update_run(conn, claim.run_id, "refused", {**values, "refusal_reason": recorded})
    conn.commit()
    logger.info("ai_trial run %s (session %s) refused: %s", claim.run_id, claim.session_date, recorded)
    return RunOutcome("refused", claim.run_id, claim.session_date, recorded)


# ---------------------------------------------------------------------------
# Publish (step 5 + §11 atomicity)
# ---------------------------------------------------------------------------
_DECISION_INSERT: Final = """
    INSERT INTO ai_trial_decisions (
        run_id, response_position, action, symbol, stop_pct, target_pct, horizon_days, size_tier,
        confidence, thesis, instrument_id, verdict, reason_code,
        atr14, close, atr14_pct, stop_atr_multiple, r_multiple
    ) VALUES (
        %(run_id)s, %(response_position)s, %(action)s, %(symbol)s, %(stop_pct)s, %(target_pct)s,
        %(horizon_days)s, %(size_tier)s, %(confidence)s, %(thesis)s, %(instrument_id)s, %(verdict)s,
        %(reason_code)s, %(atr14)s, %(close)s, %(atr14_pct)s, %(stop_atr_multiple)s, %(r_multiple)s
    )
    RETURNING decision_id
"""

_PAIR_INSERT: Final = """
    INSERT INTO ai_trial_pairs (
        declaration_id, pair_seq, arm_decision_id, control_instrument_id, seed_material, pool,
        draw_idx, stop_pct, target_pct, horizon_days, size_tier,
        control_atr14, control_close, control_atr14_pct, control_stop_pct, control_target_pct
    ) VALUES (
        %(declaration_id)s, %(pair_seq)s, %(arm_decision_id)s, %(control_instrument_id)s,
        %(seed_material)s, %(pool)s, %(draw_idx)s, %(stop_pct)s, %(target_pct)s, %(horizon_days)s,
        %(size_tier)s, %(control_atr14)s, %(control_close)s, %(control_atr14_pct)s,
        %(control_stop_pct)s, %(control_target_pct)s
    )
    RETURNING pair_id
"""

_SIGNAL_INSERT: Final = """
    INSERT INTO strategy_signals (
        strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind, verdict,
        fill_bar_date, fill_price, universe, input_rule_set_versions
    ) VALUES (%s, %s, %s, %s, 'entry', 'fired', %s, %s, %s, %s)
    RETURNING signal_id
"""


def _decision_row(run_id: int, plan: PlannedDecision) -> dict[str, Any]:
    v = plan.verdict
    d = v.decision
    atr = v.metrics.atr
    return {
        "run_id": run_id,
        "response_position": v.response_position,
        "action": d.action,
        "symbol": d.symbol,
        "stop_pct": d.stop_pct,
        "target_pct": d.target_pct,
        "horizon_days": d.horizon_days,
        "size_tier": d.size_tier,
        "confidence": d.confidence,
        "thesis": d.thesis,
        "instrument_id": v.instrument_id,
        "verdict": "accepted" if plan.reason_code is None else "refused",
        "reason_code": plan.reason_code,
        "atr14": None if atr is None else atr.atr14,
        "close": None if atr is None else atr.close,
        "atr14_pct": None if atr is None else atr.atr14_pct,
        "stop_atr_multiple": v.metrics.stop_atr_multiple,
        "r_multiple": v.metrics.r_multiple,
    }


def _insert_signal(conn: Conn, *, strategy_id: str, instrument_id: int, step1: Step1, fill_price: object) -> int:
    row = conn.execute(
        _SIGNAL_INSERT,
        (
            strategy_id,
            TRIAL_STRATEGY_VERSION,
            instrument_id,
            step1.last_session,
            step1.session_date,
            fill_price,
            SIGNAL_UNIVERSE,
            Jsonb({"ai_trial_policy": AI_TRIAL_POLICY_HASH}),
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def publish_run(
    conn: Conn,
    *,
    claim: Claim,
    declaration: Declaration,
    step1: Step1,
    pack: Pack,
    validation: ValidationResult,
    control_held_instrument_ids: frozenset[int],
    values: Mapping[str, Any],
) -> tuple[int, ...]:
    """Step 5 and the ``decided`` publish, in ONE transaction (§11). Returns the pair ids."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("the run publish requires an idle connection")
    atr = pack_atr_measurements(pack.pack["names"])
    with conn.transaction():
        # Serialises pair_seq across publishers; the state-event writer takes the same lock.
        conn.execute(
            "SELECT 1 FROM ai_trial_declarations WHERE declaration_id = %s FOR NO KEY UPDATE",
            (declaration.declaration_id,),
        )
        row = conn.execute(
            "SELECT coalesce(max(pair_seq) + 1, 0) FROM ai_trial_pairs WHERE declaration_id = %s",
            (declaration.declaration_id,),
        ).fetchone()
        assert row is not None
        planned = plan_pairs(
            validation.verdicts,
            shortlist_instrument_ids=sorted(pack.complete.values()),
            atr_by_instrument=atr,
            control_held_instrument_ids=control_held_instrument_ids,
            declaration_sha256_hex=declaration.doc_sha256,
            session_date=claim.session_date,
            first_pair_seq=int(row[0]),
        )
        _update_run(conn, claim.run_id, "decided", values)
        pair_ids: list[int] = []
        for plan in planned:
            decision = conn.execute(_DECISION_INSERT, _decision_row(claim.run_id, plan)).fetchone()
            assert decision is not None
            if plan.pair is None:
                continue
            v, pair = plan.verdict, plan.pair
            control = pair.control
            inserted = conn.execute(
                _PAIR_INSERT,
                {
                    "declaration_id": declaration.declaration_id,
                    "pair_seq": pair.pair_seq,
                    "arm_decision_id": int(decision[0]),
                    "control_instrument_id": pair.draw.instrument_id,
                    "seed_material": pair.draw.seed_material,
                    "pool": list(pair.draw.pool),
                    "draw_idx": pair.draw.index,
                    "stop_pct": v.decision.stop_pct,
                    "target_pct": v.decision.target_pct,
                    "horizon_days": v.decision.horizon_days,
                    "size_tier": v.decision.size_tier,
                    "control_atr14": control.atr.atr14,
                    "control_close": control.atr.close,
                    "control_atr14_pct": control.atr.atr14_pct,
                    "control_stop_pct": control.stop_pct,
                    "control_target_pct": control.target_pct,
                },
            ).fetchone()
            assert inserted is not None and v.instrument_id is not None and v.metrics.atr is not None
            pair_id = int(inserted[0])
            legs: tuple[tuple[Leg, str, int, object], ...] = (
                ("arm", TRIAL_ARM_STRATEGY_ID, v.instrument_id, v.metrics.atr.close),
                ("control", TRIAL_ARM_STRATEGY_ID + "-control", pair.draw.instrument_id, control.atr.close),
            )
            for leg, strategy_id, instrument_id, reference_close in legs:
                signal_id = _insert_signal(
                    conn, strategy_id=strategy_id, instrument_id=instrument_id, step1=step1, fill_price=reference_close
                )
                conn.execute(
                    "INSERT INTO ai_trial_leg_links (pair_id, leg, signal_id) VALUES (%s, %s, %s)",
                    (pair_id, leg, signal_id),
                )
            pair_ids.append(pair_id)
    return tuple(pair_ids)


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------
def run_trial_decision(
    conn: Conn,
    *,
    env: RunEnvironment,
    risk: BrokerAccountRiskSnapshot | None,
    fetch_intraday: IntradayFetch,
    invoke: Callable[..., InvocationResult] = invoke_model,
    strategy_version: str = TRIAL_STRATEGY_VERSION,
) -> RunOutcome:
    """One decision run for the current target session (§3 steps 0-5).

    Requires an idle connection. Nothing is held open across the model call: every read commits
    before it, so the 10-minute subprocess never pins a snapshot or a lock.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("the trial run requires an idle connection")
    declaration = load_declaration(conn, strategy_version=strategy_version)
    conn.commit()
    if declaration is None:
        logger.info("ai_trial run: no frozen declaration for %s/%s", TRIAL_ARM_STRATEGY_ID, strategy_version)
        return RunOutcome("declaration_missing")
    sweep_stale_claims(conn, declaration.declaration_id)
    claim = claim_run(conn, declaration.declaration_id)
    if claim is None:
        return RunOutcome("duplicate")

    # What the run has reached so far; `_decide` extends it at each stage, so a failure is
    # recorded with the scores run, pack and prompt it had already built (Codex ckpt-2).
    reached = _provenance(env)
    try:
        return _decide(
            conn,
            claim=claim,
            declaration=declaration,
            env=env,
            risk=risk,
            fetch=fetch_intraday,
            invoke=invoke,
            reached=reached,
        )
    except Exception:
        logger.exception("ai_trial run %s failed; recording it refused", claim.run_id)
        try:
            refuse_run(conn, claim, "run_failed", reached)
        except Exception:
            # Never let the bookkeeping failure replace the failure that caused it.
            logger.exception("ai_trial run %s: recording the failure also failed", claim.run_id)
        raise


def _decide(
    conn: Conn,
    *,
    claim: Claim,
    declaration: Declaration,
    env: RunEnvironment,
    risk: BrokerAccountRiskSnapshot | None,
    fetch: IntradayFetch,
    invoke: Callable[..., InvocationResult],
    reached: dict[str, Any],
) -> RunOutcome:
    reason = declaration_refusal(declaration)
    if reason is not None:
        return refuse_run(conn, claim, reason, reached)

    # Step 0 (§8 start gate): a preview, never a guarantee; no model call when it refuses.
    if risk is None:
        return refuse_run(conn, claim, REFUSAL_PREFIX + "account_risk_unavailable", reached)
    reason = preview_trial_capacity(conn, declaration_id=declaration.declaration_id, risk=risk, now=claim.as_of)
    if reason is not None:
        return refuse_run(conn, claim, reason, reached)

    # Step 1.
    step1 = read_step1(conn, as_of=claim.as_of)
    conn.commit()
    reached.update(_provenance(env, step1=step1))
    if step1.session_date != claim.session_date:  # one session concept everywhere (§3)
        raise RuntimeError(f"step 1 targets {step1.session_date}, the claim {claim.session_date}")
    if step1.refusal is not None:
        return refuse_run(conn, claim, step1.refusal, reached)

    # Step 2.
    # Read once, before the model call, and used unchanged by step 4 and step 5: the validator
    # and the pool judge the book the model was SHOWN. Nothing opens a trial trade between the
    # claim and the publish (the executor runs on the target session's signals); a close only
    # frees a slot, and the executor re-counts slots under the allocator lock anyway.
    books = read_leg_books(conn, declaration.declaration_id)
    entries = max_new_entries(books)
    account = AccountContext(
        open_positions=tuple(
            {k: p[k] for k in ("symbol", "instrument_id", "entry", "stop", "target", "deadline")}
            for p in books["arm"].open_positions
        ),
        free_slots=books["arm"].free_slots,
        max_new_entries=entries,
    )
    pack = assemble_pack(conn, step1=step1, account=account, fetch_intraday=fetch)
    conn.commit()
    reached.update(_provenance(env, step1=step1, pack=pack))

    # Step 3: one call, never retried for the session.
    prompt = render_user_prompt(pack.pack)
    reached.update(_provenance(env, step1=step1, pack=pack, prompt=prompt))
    result = invoke(
        executable=env.executable,
        prompt=prompt.text,
        system_prompt=SYSTEM_PROMPT,
        json_schema=decision_json_schema(),
        source_env=env.source_env,
    )
    values = _provenance(env, step1=step1, pack=pack, prompt=prompt, result=result)
    reached.update(values)
    if result.refusal_reason is not None:
        return refuse_run(conn, claim, result.refusal_reason, values)

    # Step 4: a whole-response refusal refuses the run; there are no decisions (§6).
    validation = validate_response(
        result.structured_output,
        shortlist=pack.complete,
        atr_by_instrument=pack_atr_measurements(pack.pack["names"]),
        arm_held_instrument_ids=books["arm"].held_instrument_ids,
        max_new_entries=entries,
    )
    if validation.whole_refusal is not None:
        return refuse_run(conn, claim, validation.whole_refusal, values)

    # Step 5 + publish.
    try:
        pair_ids = publish_run(
            conn,
            claim=claim,
            declaration=declaration,
            step1=step1,
            pack=pack,
            validation=validation,
            control_held_instrument_ids=books["control"].held_instrument_ids,
            values=values,
        )
    except psycopg.Error:
        # A lease that ran out mid-run is refused by the trigger: record it as the stale claim
        # it is. Anything else is a defect and propagates after the run is recorded.
        conn.rollback()
        outcome = refuse_run(conn, claim, "publish_failed", values)
        if outcome.refusal_reason == "stale_claim":
            return outcome
        raise
    logger.info(
        "ai_trial run %s (session %s) decided: %d decisions, %d pairs",
        claim.run_id,
        claim.session_date,
        len(validation.verdicts),
        len(pair_ids),
    )
    return RunOutcome("decided", claim.run_id, claim.session_date, None, pair_ids)


__all__ = [
    "SIGNAL_UNIVERSE",
    "TRIAL_ARM_STRATEGY_ID",
    "TRIAL_STRATEGY_VERSION",
    "Claim",
    "Declaration",
    "LegBook",
    "RunEnvironment",
    "RunOutcome",
    "claim_run",
    "declaration_refusal",
    "load_declaration",
    "max_new_entries",
    "publish_run",
    "read_leg_books",
    "refuse_run",
    "run_trial_decision",
    "sweep_stale_claims",
]
