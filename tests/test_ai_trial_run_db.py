"""#3471 slice 2c-iv-b — the run publisher against real Postgres.

Steps 1-2 and the step-0 preview are stubbed (each has its own DB test): step 1 returns the
claim's own session, step 2 the synthetic CLI's fictitious-name pack, and the model is a fake
``invoke``. What is exercised for real is everything this slice writes — the claim and its
lease, every refusal record, and the one-transaction publish through ``sql/432``'s triggers.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services import ai_trial_run
from app.services.ai_trial_decision import pack_atr_measurements
from app.services.ai_trial_intent import DECLARATION_CONTRACT_PREFIX, declaration_digest
from app.services.ai_trial_invocation import InvocationResult
from app.services.ai_trial_pack_reader import ScoresRun, Step1, next_us_session
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH
from app.services.ai_trial_run import Claim, RunEnvironment, run_trial_decision
from app.services.market_calendar import latest_completed_us_session
from scripts.ai_trial_synthetic import _NAMES, synthetic_pack

Conn = psycopg.Connection[Any]

ENV = RunEnvironment(executable="/usr/local/bin/claude", cli_version="2.1.280", git_sha="b" * 40, source_env={})
RISK = cast(BrokerAccountRiskSnapshot, object())  # the preview that reads it is stubbed
PACK = synthetic_pack()
ATR = pack_atr_measurements(PACK.pack["names"])
ARM_SYMBOL = "SYN_ALFA"


def _arm_levels() -> tuple[float, float]:
    """Stop at 2 ATRs of the arm name, target at 2R: inside the §6 band and the §5 bounds."""
    atr = ATR[PACK.complete[ARM_SYMBOL]]
    assert atr is not None
    stop = round(float(2 * atr.atr14_pct), 2)
    assert 2 <= stop <= 12.5
    return stop, 2 * stop


def _decisions() -> dict[str, Any]:
    stop, target = _arm_levels()
    common = {"action": "enter_long", "horizon_days": 10, "size_tier": "half", "confidence": 3, "thesis": "Trend."}
    return {
        "no_trade_reason": None,
        "decisions": [
            {**common, "symbol": ARM_SYMBOL, "stop_pct": stop, "target_pct": target},
            {**common, "symbol": "NOPE", "stop_pct": stop, "target_pct": target},
        ],
    }


def _invoke(structured: object = None, refusal: str | None = None) -> Any:
    def fake(**_: object) -> InvocationResult:
        return InvocationResult(
            refusal_reason=refusal,  # type: ignore[arg-type]
            detail="",
            init_event={"type": "system", "subtype": "init", "tools": ["StructuredOutput"]},
            result_event={"type": "result", "num_turns": 1, "total_cost_usd": 0.57},
            structured_output=structured if refusal is None else None,
            exit_code=0 if refusal is None else None,
            stdout=b'{"type":"result"}\n',
            stderr=b"",
            duration_ms=1200,
        )

    return fake


def _seed(conn: Conn, *, policy_hash: str = AI_TRIAL_POLICY_HASH) -> int:
    conn.execute(
        "INSERT INTO exchanges (exchange_id, country, asset_class) VALUES ('2', 'US', 'us_equity') "
        "ON CONFLICT (exchange_id) DO UPDATE SET asset_class='us_equity'"
    )
    for symbol, iid, *_ in _NAMES:
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable) "
            "VALUES (%s, %s, %s, '2', 'USD', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
            (iid, symbol, symbol),
        )
    doc = {"strategy_id": "ai-discretionary-v1", "strategy_version": "v1", "policy_hash": policy_hash}
    digest = declaration_digest(doc)
    row = conn.execute(
        """
        INSERT INTO strategy_preregistration_declarations (
            strategy_id, strategy_version, contract_version, prereg_purpose,
            structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
            declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
            min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256
        ) VALUES (
            'ai-discretionary-v1', 'v1', %(contract)s, 'falsification_only', 'test-policy',
            'survivor_only', true, true, '{}', 40, 8, 'spec §9 cohort', 'test', %(digest)s
        )
        RETURNING declaration_id
        """,
        {"contract": DECLARATION_CONTRACT_PREFIX + digest, "digest": digest},
    ).fetchone()
    assert row is not None
    conn.execute(
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, 'ai-discretionary-v1', 'v1', 'docs/test.json', %s, %s)",
        (row[0], Jsonb(doc), digest),
    )
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'active', 'freeze', 'supervisor')",
        (row[0],),
    )
    conn.commit()
    return int(row[0])


@pytest.fixture
def stubbed(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Steps 0-2 stubbed; the step-1 result follows the claim's own ``as_of``."""
    state: dict[str, Any] = {"step1_refusal": None, "preview": None}

    def step1(_conn: Conn, *, as_of: datetime) -> Step1:
        last = latest_completed_us_session(as_of)
        return Step1(as_of, last, next_us_session(last), ScoresRun("v1.5", as_of), 1, state["step1_refusal"])

    monkeypatch.setattr(ai_trial_run, "read_step1", step1)
    monkeypatch.setattr(ai_trial_run, "assemble_pack", lambda _conn, **_kw: PACK)
    monkeypatch.setattr(ai_trial_run, "preview_trial_capacity", lambda _conn, **_kw: state["preview"])
    return state


def _run(conn: Conn, invoke: Any) -> ai_trial_run.RunOutcome:
    return run_trial_decision(conn, env=ENV, risk=RISK, fetch_intraday=lambda _iid: [], invoke=invoke)


def test_a_run_publishes_decisions_pairs_signals_and_links_in_one_transaction(
    ebull_test_conn: Conn, stubbed: dict[str, Any]
) -> None:
    conn = ebull_test_conn
    assert _run(conn, _invoke(_decisions())).status == "declaration_missing"
    assert conn.execute("SELECT count(*) FROM ai_trial_runs").fetchone() == (0,)

    declaration_id = _seed(conn)
    outcome = _run(conn, _invoke(_decisions()))
    assert outcome.status == "decided" and len(outcome.pair_ids) == 1
    run = conn.execute(
        "SELECT status, pack_sha256, policy_hash, cost_usd, session_date FROM ai_trial_runs WHERE run_id = %s",
        (outcome.run_id,),
    ).fetchone()
    assert run is not None
    assert run[:4] == ("decided", PACK.sha256, AI_TRIAL_POLICY_HASH, Decimal("0.57"))
    assert run[4] == outcome.session_date

    decisions = conn.execute(
        "SELECT response_position, symbol, verdict, reason_code FROM ai_trial_decisions ORDER BY response_position"
    ).fetchall()
    assert decisions == [(0, ARM_SYMBOL, "accepted", None), (1, "NOPE", "refused", "not_in_shortlist")]

    pair = conn.execute(
        "SELECT pair_id, pair_seq, control_instrument_id FROM ai_trial_pairs WHERE declaration_id = %s",
        (declaration_id,),
    ).fetchone()
    assert pair is not None and pair[1] == 0
    links = conn.execute(
        """
        SELECT l.leg, s.strategy_id, s.instrument_id, s.fill_bar_date, s.fill_price
        FROM ai_trial_leg_links l JOIN strategy_signals s ON s.signal_id = l.signal_id
        WHERE l.pair_id = %s ORDER BY l.leg
        """,
        (pair[0],),
    ).fetchall()
    arm_id = PACK.complete[ARM_SYMBOL]
    arm_atr, control_atr = ATR[arm_id], ATR[int(pair[2])]
    assert arm_atr is not None and control_atr is not None
    assert links == [
        ("arm", "ai-discretionary-v1", arm_id, outcome.session_date, arm_atr.close),
        ("control", "ai-discretionary-v1-control", pair[2], outcome.session_date, control_atr.close),
    ]

    # One claim per session: a second fire writes nothing and never calls the model.
    def never(**_: object) -> InvocationResult:
        raise AssertionError("a duplicate claim must not call the model")

    conn.commit()
    assert _run(conn, never).status == "duplicate"


@pytest.mark.parametrize(
    ("scenario", "expected"),
    [
        ("policy_drift", "policy_drift"),
        ("preview", "trial_capacity_unavailable:loss_at_stop"),
        ("step1", "price_daily_stale"),
        ("model", "model_timeout"),
        ("malformed", "malformed_response"),
    ],
)
def test_each_refusal_is_recorded_with_what_the_run_reached(
    ebull_test_conn: Conn, stubbed: dict[str, Any], scenario: str, expected: str
) -> None:
    conn = ebull_test_conn
    _seed(conn, policy_hash="f" * 64 if scenario == "policy_drift" else AI_TRIAL_POLICY_HASH)
    stubbed["preview"] = "trial_capacity_unavailable:loss_at_stop" if scenario == "preview" else None
    stubbed["step1_refusal"] = "price_daily_stale" if scenario == "step1" else None
    invoke = _invoke(refusal="model_timeout") if scenario == "model" else _invoke({"decisions": "x"})

    outcome = _run(conn, invoke)
    assert (outcome.status, outcome.refusal_reason) == ("refused", expected)
    row = conn.execute(
        "SELECT status, refusal_reason, policy_hash, pack_sha256, stdout FROM ai_trial_runs WHERE run_id = %s",
        (outcome.run_id,),
    ).fetchone()
    assert row is not None and row[:3] == ("refused", expected, AI_TRIAL_POLICY_HASH)
    reached_model = scenario in ("model", "malformed")
    assert (row[3] is not None, row[4] is not None) == (reached_model, reached_model)
    assert conn.execute("SELECT count(*) FROM ai_trial_decisions").fetchone() == (0,)


def test_a_failure_after_the_pack_is_recorded_run_failed_with_what_it_reached(
    ebull_test_conn: Conn, stubbed: dict[str, Any]
) -> None:
    conn = ebull_test_conn
    _seed(conn)

    def broken(**_: object) -> InvocationResult:
        raise OSError("claude binary vanished")

    with pytest.raises(OSError):
        _run(conn, broken)
    row = conn.execute(
        "SELECT status, refusal_reason, pack_sha256, rendered_prompt_sha256 FROM ai_trial_runs"
    ).fetchone()
    assert row is not None and row[:3] == ("refused", "run_failed", PACK.sha256) and row[3] is not None


def test_an_expired_lease_is_swept_and_a_late_publish_is_a_stale_claim(
    ebull_test_conn: Conn, stubbed: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    declaration_id = _seed(conn)
    conn.execute(
        "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) "
        "SELECT %s, '2026-01-02', now() - interval '20 minutes', now() + interval '-5 minutes'",
        (declaration_id,),
    )
    conn.commit()

    def late_claim(conn: Conn, declaration_id: int) -> Claim:
        # A claim whose lease runs out while the model call is in flight.
        row = conn.execute("SELECT clock_timestamp() - interval '16 minutes'").fetchone()
        assert row is not None
        session = next_us_session(latest_completed_us_session(row[0]))
        run = conn.execute(
            "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) "
            "VALUES (%s, %s, %s, %s + interval '15 minutes') RETURNING run_id",
            (declaration_id, session, row[0], row[0]),
        ).fetchone()
        assert run is not None
        conn.commit()
        return Claim(int(run[0]), row[0], session)

    monkeypatch.setattr(ai_trial_run, "claim_run", late_claim)
    outcome = _run(conn, _invoke(_decisions()))
    assert (outcome.status, outcome.refusal_reason) == ("refused", "stale_claim")
    rows = conn.execute(
        "SELECT session_date::text, status, refusal_reason FROM ai_trial_runs ORDER BY run_id"
    ).fetchall()
    assert rows[0] == ("2026-01-02", "refused", "stale_claim")
    assert rows[1][1:] == ("refused", "stale_claim")
    assert conn.execute("SELECT count(*) FROM ai_trial_decisions").fetchone() == (0,)
