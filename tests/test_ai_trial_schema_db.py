"""#3471 slice 1b-i — the AI-trial tables' triggers (sql/432_ai_trial.sql).

One test per DB mechanism: the declaration binding, the §9 state machine, the leased run
transition, same-transaction publication, the recomputed §7 draw, the deferred
every-accepted-entry-has-a-pair check, leg identity, and the pair-event order.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any

import psycopg
import pytest

from app.services.ai_trial_decision import AtrMeasurement, draw_control, measure_atr
from app.services.ai_trial_guard import price_decimal
from app.services.ai_trial_levels import Level
from app.services.ai_trial_plan import plan_figures

DOC_SHA = hashlib.sha256(b"ai-trial test declaration").hexdigest()
SESSION = date(2026, 10, 5)
INSTRUMENTS = (11, 12, 13, 14, 15)
# Every name's §6 measurement: ATR 4% (atr14 4, close 100), and its levels for the fixture ids
# are 93 / 116 (§16.3: stop 92 = 2 ATRs and 8%, target 16%, R 2), so every control's plan is the
# arm's.
ATR = measure_atr(4.0, 100.0)
LEVELS = (93, 116)
BASELINE = ("-1/10", "1/20")


def _plan(
    atr: AtrMeasurement | None = ATR, invalidation: object = LEVELS[0], target: object = LEVELS[1]
) -> dict[str, Any]:
    """A §16.3 plan's recorded columns, derived by ``plan_figures`` (the writer's own function)."""
    inv, tgt = (Level(Fraction(str(x)), None) for x in (invalidation, target))
    f = plan_figures(atr, inv, tgt)
    return {
        "atr14": None if atr is None else atr.atr14,
        "close": None if atr is None else atr.close,
        "atr_pct": None if atr is None else atr.atr14_pct,
        "inv": None if atr is None else price_decimal(inv.price),
        "tgt": None if atr is None else price_decimal(tgt.price),
        "stop_price": price_decimal(f.stop_price),
        "mult": f.stop_atr_multiple,
        "stop": f.stop_pct,
        "target": f.target_pct,
        "r": f.r_multiple,
    }


Conn = psycopg.Connection[Any]


def _seed(conn: Conn) -> int:
    """Instruments + a #2599 row + its ai_trial declaration; returns the declaration id."""
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "SELECT i, 'AIT' || i, 'AI trial test ' || i, TRUE FROM unnest(%s::bigint[]) AS i",
        (list(INSTRUMENTS),),
    )
    row = conn.execute(
        """
        INSERT INTO strategy_preregistration_declarations (
            strategy_id, strategy_version, contract_version, prereg_purpose,
            structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
            declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
            min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256
        ) VALUES (
            'ai-discretionary-v1', 'v1', %(contract)s, 'falsification_only',
            'test-policy', 'survivor_only', true, true, '{}', 40, 8,
            'spec §9 cohort, sessions 1-40', 'test', %(digest)s
        )
        RETURNING declaration_id
        """,
        {"contract": f"ai-trial-declaration-v1:{DOC_SHA}", "digest": DOC_SHA},
    ).fetchone()
    assert row is not None
    declaration_id = int(row[0])
    conn.execute(
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, 'ai-discretionary-v1', 'v1', 'docs/test.json', "
        """'{"strategy_id": "ai-discretionary-v1", "strategy_version": "v1"}', %s)""",
        (declaration_id, DOC_SHA),
    )
    conn.commit()
    return declaration_id


def _claim(conn: Conn, declaration_id: int, *, minutes_ago: int = 0) -> int:
    # DB time, not host time: the claim trigger refuses a post-dated `as_of`, and the
    # container's clock is not the host's.
    row = conn.execute(
        "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) "
        "SELECT %(d)s, %(s)s, t, t + interval '15 minutes' FROM (SELECT now() - make_interval(mins => %(m)s) AS t) x "
        "RETURNING run_id",
        {"d": declaration_id, "s": SESSION, "m": minutes_ago},
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


_DECIDED = """
    UPDATE ai_trial_runs SET
        status = 'decided', scores_model_version = 'v1.5', scores_scored_at = now(),
        pack = '{}', pack_sha256 = %(sha)s, rendered_prompt = 'p', rendered_prompt_sha256 = %(psha)s,
        system_prompt_sha256 = %(sha)s, prompt_template_sha256 = %(sha)s, model_id = 'claude-opus-5-5',
        argv_sha256 = %(sha)s, executable_path = '/usr/local/bin/claude', cli_version = '2.1.280',
        git_sha = %(git)s, policy_hash = %(sha)s, init_event = '{}', exit_code = %(exit)s,
        structured_output = '{}'
    WHERE run_id = %(run_id)s
"""


def _decide(conn: Conn, run_id: int, **overrides: object) -> None:
    params: dict[str, object] = {
        "sha": DOC_SHA,
        "psha": hashlib.sha256(b"p").hexdigest(),
        "git": "a" * 40,
        "exit": 0,
        "run_id": run_id,
    }
    conn.execute(_DECIDED, params | overrides)


_RAW_DECISION = """
    INSERT INTO ai_trial_decisions (
        run_id, response_position, action, symbol, setup_type, invalidation_level_id, target_level_id,
        stop_pct, target_pct, horizon_days, size_tier, confidence, thesis, instrument_id, verdict,
        reason_code, atr14, close, atr14_pct, stop_atr_multiple, r_multiple,
        invalidation_price, target_price, stop_price, base_rate_train_mean_net_r, base_rate_holdout_mean_net_r
    ) VALUES (%(run)s, %(pos)s, 'enter_long', 'AIT', %(setup)s, 'sma50', 'range20_projection',
              %(stop)s, %(target)s, 10, 'half', 3, 'Thesis.', %(iid)s, %(verdict)s,
              %(reason)s, %(atr14)s, %(close)s, %(atr_pct)s, %(mult)s, %(r)s,
              %(inv)s, %(tgt)s, %(stop_price)s, %(train)s, %(holdout)s)
    RETURNING decision_id
"""


def _decision_params(run_id: int, position: int, instrument_id: int | None, reason: str | None) -> dict[str, Any]:
    return {
        "run": run_id,
        "pos": position,
        "setup": "pullback_rising_sma20",
        "iid": instrument_id,
        "verdict": "accepted" if reason is None else "refused",
        "reason": reason,
        **_plan(None if instrument_id is None else ATR),
        "train": None if instrument_id is None else BASELINE[0],
        "holdout": None if instrument_id is None else BASELINE[1],
    }


def _decision(conn: Conn, run_id: int, position: int, instrument_id: int | None, reason: str | None = None) -> int:
    row = conn.execute(_RAW_DECISION, _decision_params(run_id, position, instrument_id, reason)).fetchone()
    assert row is not None
    return int(row[0])


def _pair(conn: Conn, declaration_id: int, decision_id: int, pair_seq: int, pool: tuple[int, ...]) -> int:
    draw = draw_control(declaration_sha256_hex=DOC_SHA, session_date=SESSION, pair_seq=pair_seq, pool=pool)
    return _pair_raw(
        conn, declaration_id, decision_id, pair_seq, draw.seed_material, pool, draw.index, draw.instrument_id
    )


def _pair_raw(
    conn: Conn,
    declaration_id: int,
    decision_id: int,
    pair_seq: int,
    seed: str,
    pool: tuple[int, ...],
    idx: int,
    control: int,
) -> int:
    row = conn.execute(
        _RAW_PAIR,
        {"decl": declaration_id, "seq": pair_seq, "decision": decision_id, "control": control, "seed": seed,
         "pool": list(pool), "idx": idx, **_plan()},
    ).fetchone()  # fmt: skip
    assert row is not None
    return int(row[0])


# The arm's terms are the fixture decision's (8 / 16); the control columns are its own plan.
_RAW_PAIR = """
    INSERT INTO ai_trial_pairs (
        declaration_id, pair_seq, arm_decision_id, control_instrument_id, seed_material, pool,
        draw_idx, stop_pct, target_pct, horizon_days, size_tier,
        control_atr14, control_close, control_atr14_pct, control_stop_pct, control_target_pct,
        control_invalidation_price, control_target_price, control_stop_price,
        control_stop_atr_multiple, control_r_multiple
    ) VALUES (%(decl)s, %(seq)s, %(decision)s, %(control)s, %(seed)s, %(pool)s, %(idx)s, 8.0, 16.0, 10, 'half',
              %(atr14)s, %(close)s, %(atr_pct)s, %(stop)s, %(target)s, %(inv)s, %(tgt)s, %(stop_price)s,
              %(mult)s, %(r)s)
    RETURNING pair_id
"""


def _refused(conn: Conn, sql: Any, params: Any, match: str) -> None:
    with pytest.raises(psycopg.Error, match=match):
        conn.execute(sql, params)
        conn.commit()
    conn.rollback()


def test_the_declaration_must_name_its_document(ebull_test_conn: Conn) -> None:
    declaration_id = _seed(ebull_test_conn)
    _refused(
        ebull_test_conn,
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, 'ai-discretionary-v2', 'v1', 'x', "
        """'{"strategy_id": "ai-discretionary-v2", "strategy_version": "v1"}', %s)""",
        (declaration_id, DOC_SHA),
        "is ai-discretionary-v1/v1, not ai-discretionary-v2/v1",
    )
    _refused(ebull_test_conn, "DELETE FROM ai_trial_declarations", None, "append-only")


def test_state_events_follow_the_legal_transitions(ebull_test_conn: Conn) -> None:
    declaration_id = _seed(ebull_test_conn)
    sql = (
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, %s, %s, 'r', %s)"
    )

    _refused(ebull_test_conn, sql, (declaration_id, None, "halted_operator", "engine"), "genesis event must be")
    ebull_test_conn.execute(sql, (declaration_id, None, "active", "supervisor"))
    ebull_test_conn.execute(sql, (declaration_id, "active", "halted_mandate", "engine"))
    ebull_test_conn.commit()
    # Optimistic concurrency: a writer that read `active` is refused once the state moved.
    _refused(ebull_test_conn, sql, (declaration_id, "active", "halted_harm", "engine"), "stale transition")
    _refused(ebull_test_conn, sql, (declaration_id, "halted_mandate", "active", "engine"), "supervisor action")
    ebull_test_conn.execute(sql, (declaration_id, "halted_mandate", "active", "supervisor"))
    ebull_test_conn.execute(sql, (declaration_id, "active", "halted_harm", "engine"))
    ebull_test_conn.commit()
    _refused(ebull_test_conn, sql, (declaration_id, "halted_harm", "active", "supervisor"), "halted_harm is terminal")


def test_only_the_supervisor_completes_a_trial_and_completed_is_terminal(ebull_test_conn: Conn) -> None:
    """#3515 sql/442: a trial that finishes needs a terminal state that is not an engine halt."""
    declaration_id = _seed(ebull_test_conn)
    sql = (
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, %s, %s, 'r', %s)"
    )
    _refused(ebull_test_conn, sql, (declaration_id, None, "completed", "supervisor"), "genesis event must be")
    ebull_test_conn.execute(sql, (declaration_id, None, "active", "supervisor"))
    ebull_test_conn.commit()
    _refused(ebull_test_conn, sql, (declaration_id, "active", "completed", "engine"), "supervisor action")
    # From a resumable halt, the supervisor may resume or complete; nothing else.
    ebull_test_conn.execute(sql, (declaration_id, "active", "halted_operator", "engine"))
    ebull_test_conn.commit()
    _refused(ebull_test_conn, sql, (declaration_id, "halted_operator", "halted_harm", "engine"), "illegal transition")
    _refused(ebull_test_conn, sql, (declaration_id, "halted_operator", "completed", "operator"), "supervisor action")
    ebull_test_conn.execute(sql, (declaration_id, "halted_operator", "completed", "supervisor"))
    ebull_test_conn.commit()
    _refused(ebull_test_conn, sql, (declaration_id, "completed", "active", "supervisor"), "completed is terminal")


def test_a_fund_version_declares_in_the_same_table(ebull_test_conn: Conn) -> None:
    """#3515 §7: ``ai-discretionary-fund-v<N>`` is an admissible trial id; anything else is not."""
    conn = ebull_test_conn
    insert = (
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, %s, 'v1', 'x', jsonb_build_object('strategy_id', %s::text, 'strategy_version', 'v1'), %s)"
    )

    def prereg(strategy_id: str) -> int:
        # A #2599 row naming the same id, so the bind trigger passes and only the CHECK decides.
        row = conn.execute(
            """
            INSERT INTO strategy_preregistration_declarations (
                strategy_id, strategy_version, contract_version, prereg_purpose,
                structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
                declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
                min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256
            ) VALUES (
                %(id)s, 'v1', %(contract)s, 'falsification_only', 'test-policy', 'survivor_only', true, true,
                '{}', 40, 8, 'spec §9 cohort, sessions 1-40', 'test', %(digest)s
            )
            RETURNING declaration_id
            """,
            {"id": strategy_id, "contract": f"ai-trial-declaration-v1:{DOC_SHA}", "digest": DOC_SHA},
        ).fetchone()
        assert row is not None
        conn.commit()
        return int(row[0])

    for strategy_id in ("ai-discretionary-fund-v1", "ai-discretionary-fund-v12"):
        declaration_id = prereg(strategy_id)
        conn.execute(insert, (declaration_id, strategy_id, strategy_id, DOC_SHA))
        conn.commit()
    for strategy_id in ("ai-discretionary-fund-v0", "ai-discretionary-fundv1", "ai-discretionary-x-v1"):
        declaration_id = prereg(strategy_id)
        _refused(
            conn,
            insert,
            (declaration_id, strategy_id, strategy_id, DOC_SHA),
            "ai_trial_declarations_strategy_id_check",
        )


def test_a_run_publishes_once_and_only_in_its_decided_transaction(ebull_test_conn: Conn) -> None:
    declaration_id = _seed(ebull_test_conn)
    _refused(
        ebull_test_conn,
        "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) VALUES (%s, %s, %s, %s)",
        (declaration_id, SESSION, datetime.now(UTC) + timedelta(hours=1), datetime.now(UTC) + timedelta(minutes=75)),
        "in the future",
    )
    run_id = _claim(ebull_test_conn, declaration_id)
    # One claim per target session.
    _refused(
        ebull_test_conn,
        "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) "
        "VALUES (%s, %s, now(), now() + interval '15 minutes')",
        (declaration_id, SESSION),
        "ai_trial_runs_one_per_session",
    )
    # A decision cannot ride on a claimed (unpublished) run.
    with pytest.raises(psycopg.Error, match="not decided in this transaction"):
        _decision(ebull_test_conn, run_id, 0, None, "not_in_shortlist")
    ebull_test_conn.rollback()

    _decide(ebull_test_conn, run_id)
    _decision(ebull_test_conn, run_id, 0, None, "not_in_shortlist")
    ebull_test_conn.commit()

    # A later transaction cannot append to the published run, nor transition it again.
    with pytest.raises(psycopg.Error, match="not decided in this transaction"):
        _decision(ebull_test_conn, run_id, 1, None, "not_in_shortlist")
    ebull_test_conn.rollback()
    _refused(
        ebull_test_conn,
        "UPDATE ai_trial_runs SET status = 'refused', refusal_reason = 'x' WHERE run_id = %s",
        (run_id,),
        "already decided",
    )
    _refused(ebull_test_conn, "DELETE FROM ai_trial_runs WHERE run_id = %s", (run_id,), "append-only")


def test_an_expired_lease_admits_only_stale_claim(ebull_test_conn: Conn) -> None:
    declaration_id = _seed(ebull_test_conn)
    run_id = _claim(ebull_test_conn, declaration_id, minutes_ago=20)
    with pytest.raises(psycopg.Error, match="only refused/stale_claim"):
        _decide(ebull_test_conn, run_id)
    ebull_test_conn.rollback()
    _refused(
        ebull_test_conn,
        "UPDATE ai_trial_runs SET status = 'refused', refusal_reason = 'model_timeout' WHERE run_id = %s",
        (run_id,),
        "only refused/stale_claim",
    )
    ebull_test_conn.execute(
        "UPDATE ai_trial_runs SET status = 'refused', refusal_reason = 'stale_claim' WHERE run_id = %s", (run_id,)
    )
    ebull_test_conn.commit()


def test_the_database_recomputes_the_control_draw(ebull_test_conn: Conn) -> None:
    # The SQL draw and the Python draw (slice 1a) are the same function, across seeds and pool sizes.
    for pair_seq in range(40):
        for n in (1, 2, 7, 50, 1427):
            material = f"{DOC_SHA}|{SESSION.isoformat()}|{pair_seq}"
            row = ebull_test_conn.execute("SELECT ai_trial_draw_index(%s, %s)", (material, n)).fetchone()
            assert row is not None
            assert (
                row[0]
                == draw_control(
                    declaration_sha256_hex=DOC_SHA, session_date=SESSION, pair_seq=pair_seq, pool=tuple(range(n))
                ).index
            )

    declaration_id = _seed(ebull_test_conn)
    run_id = _claim(ebull_test_conn, declaration_id)
    pool = INSTRUMENTS[1:]
    draw = draw_control(declaration_sha256_hex=DOC_SHA, session_date=SESSION, pair_seq=0, pool=pool)
    wrong = next(i for i in pool if i != draw.instrument_id)

    _decide(ebull_test_conn, run_id)
    decision_id = _decision(ebull_test_conn, run_id, 0, INSTRUMENTS[0])
    with pytest.raises(psycopg.Error, match="is not the draw's output"):
        _pair_raw(ebull_test_conn, declaration_id, decision_id, 0, draw.seed_material, pool, draw.index, wrong)
    ebull_test_conn.rollback()

    _decide(ebull_test_conn, run_id)
    decision_id = _decision(ebull_test_conn, run_id, 0, INSTRUMENTS[0])
    with pytest.raises(psycopg.Error, match="not the next in sequence"):
        _pair(ebull_test_conn, declaration_id, decision_id, 1, pool)
    ebull_test_conn.rollback()

    _decide(ebull_test_conn, run_id)
    first = _decision(ebull_test_conn, run_id, 0, INSTRUMENTS[0])
    second = _decision(ebull_test_conn, run_id, 1, INSTRUMENTS[1])
    _pair(ebull_test_conn, declaration_id, first, 0, pool)
    # Without replacement: the second draw's pool may not contain the first control.
    with pytest.raises(psycopg.Error, match="already drawn this run"):
        _pair(ebull_test_conn, declaration_id, second, 1, pool)
    ebull_test_conn.rollback()


def test_an_accepted_decision_without_a_pair_cannot_commit(ebull_test_conn: Conn) -> None:
    declaration_id = _seed(ebull_test_conn)
    run_id = _claim(ebull_test_conn, declaration_id)
    _decide(ebull_test_conn, run_id)
    _decision(ebull_test_conn, run_id, 0, INSTRUMENTS[0])
    with pytest.raises(psycopg.Error, match="has no pair"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()
    row = ebull_test_conn.execute("SELECT status FROM ai_trial_runs WHERE run_id = %s", (run_id,)).fetchone()
    assert row == ("claimed",)  # the whole publication rolled back


def _published_pair(conn: Conn, pool: tuple[int, ...] = INSTRUMENTS[1:]) -> tuple[int, int]:
    declaration_id = _seed(conn)
    run_id = _claim(conn, declaration_id)
    _decide(conn, run_id)
    decision_id = _decision(conn, run_id, 0, INSTRUMENTS[0])
    pair_id = _pair(conn, declaration_id, decision_id, 0, pool)
    conn.commit()
    return declaration_id, pair_id


def test_leg_and_trade_links_bind_the_leg_identity(ebull_test_conn: Conn) -> None:
    # The control draws the arm's own name (a one-name pool), so only identity, not the
    # instrument, tells the legs apart.
    arm_instrument = INSTRUMENTS[0]
    _, pair_id = _published_pair(ebull_test_conn, pool=(arm_instrument,))

    def signal(strategy_id: str, *, version: str = "v1", fill: str = "2026-10-05") -> int:
        row = ebull_test_conn.execute(
            """
            INSERT INTO strategy_signals (
                strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind, verdict,
                fill_bar_date, fill_price, universe, input_rule_set_versions
            ) VALUES (%s, %s, %s, '2026-10-02', 'entry', 'fired', %s, 100, 'survivor_only',
                      '{"indicator_series": "rules-v1"}')
            RETURNING signal_id
            """,
            (strategy_id, version, arm_instrument, fill),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def trade(signal_id: int, strategy_id: str) -> int:
        deployment = ebull_test_conn.execute(
            "INSERT INTO strategy_deployments (strategy_id, strategy_version, mode, capital_limit, enabled, "
            "updated_by, reason) VALUES (%s, 'v1', 'paper', 1000, FALSE, 'test', '#3471 test') RETURNING deployment_id",
            (strategy_id,),
        ).fetchone()
        assert deployment is not None
        funding = ebull_test_conn.execute(
            "INSERT INTO strategy_funding_decisions (signal_id, deployment_id, verdict, amount, reason_code) "
            "VALUES (%s, %s, 'allocated', 125, 'test') RETURNING funding_decision_id",
            (signal_id, deployment[0]),
        ).fetchone()
        assert funding is not None
        row = ebull_test_conn.execute(
            # `planned`: sql/435 links a trade to a trial leg only at allocation, as the executor does.
            "INSERT INTO strategy_trades (funding_decision_id, instrument_id, status) "
            "VALUES (%s, %s, 'planned') RETURNING strategy_trade_id",
            (funding[0], arm_instrument),
        ).fetchone()
        assert row is not None
        return int(row[0])

    link = "INSERT INTO ai_trial_leg_links (pair_id, leg, signal_id) VALUES (%s, %s, %s)"
    _refused(ebull_test_conn, link, (pair_id, "arm", signal("ai-discretionary-v1-control")), "not the arm leg's")
    _refused(ebull_test_conn, link, (pair_id, "arm", signal("ai-discretionary-v1", version="v2")), "not the arm leg's")
    _refused(
        ebull_test_conn, link, (pair_id, "arm", signal("ai-discretionary-v1", fill="2026-10-06")), "not a fired entry"
    )
    arm_signal = signal("ai-discretionary-v1")
    control_signal = signal("ai-discretionary-v1-control")
    ebull_test_conn.execute(link, (pair_id, "arm", arm_signal))
    ebull_test_conn.execute(link, (pair_id, "control", control_signal))
    arm_trade = trade(arm_signal, "ai-discretionary-v1")
    control_trade = trade(control_signal, "ai-discretionary-v1-control")
    ebull_test_conn.commit()

    trade_link = (
        "INSERT INTO ai_trial_trade_links (pair_id, leg, strategy_trade_id, requested_amount) VALUES (%s, %s, %s, %s)"
    )
    _refused(ebull_test_conn, trade_link, (pair_id, "arm", control_trade, 125), "not the arm leg's")
    # sql/434 (§8 "Sizing"): capacity may reduce the funded amount, nothing may raise it above
    # the requested ticket.
    _refused(ebull_test_conn, trade_link, (pair_id, "arm", arm_trade, 124), "above or without the requested")
    ebull_test_conn.execute(trade_link, (pair_id, "arm", arm_trade, 125))
    ebull_test_conn.execute(trade_link, (pair_id, "control", control_trade, 250))
    ebull_test_conn.commit()


def test_a_null_cannot_satisfy_a_check_by_being_unknown(ebull_test_conn: Conn) -> None:
    # Postgres accepts a CHECK that evaluates to UNKNOWN, and PL/pgSQL skips an IF that does.
    declaration_id = _seed(ebull_test_conn)
    _refused(
        ebull_test_conn,
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'active', 'r', 'supervisor'), (%s, 'active', 'halted_operator', 'r', 'engine'), "
        "(%s, 'halted_operator', 'active', 'r', 'operator')",
        (declaration_id, declaration_id, declaration_id),
        "supervisor action, not a operator one",
    )
    _refused(
        ebull_test_conn,
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, 'ai-discretionary-v1', 'v1', 'x', '{}', %s)",
        (declaration_id, DOC_SHA),
        "ai_trial_declarations_columns_match_doc",
    )
    run_id = _claim(ebull_test_conn, declaration_id)
    with pytest.raises(psycopg.errors.CheckViolation, match="ai_trial_runs_decided_is_complete"):
        _decide(ebull_test_conn, run_id, exit=None)
    ebull_test_conn.rollback()
    # A refused run need not be complete, but a prompt it stores still carries its digest.
    _refused(
        ebull_test_conn,
        "UPDATE ai_trial_runs SET status = 'refused', refusal_reason = 'model_timeout', rendered_prompt = 'p' "
        "WHERE run_id = %s",
        (run_id,),
        "ai_trial_runs_prompt_sha_matches",
    )

    _decide(ebull_test_conn, run_id)
    decision_id = _decision(ebull_test_conn, run_id, 0, INSTRUMENTS[0])
    with pytest.raises(psycopg.Error, match="is not the draw's output|ai_trial_pairs_pool_check"):
        _pair_raw(ebull_test_conn, declaration_id, decision_id, 0, f"{DOC_SHA}|{SESSION}|0", (None,), 0, INSTRUMENTS[1])  # type: ignore[arg-type]
    ebull_test_conn.rollback()


def test_pair_events_follow_the_leg_lifecycle(ebull_test_conn: Conn) -> None:
    _, pair_id = _published_pair(ebull_test_conn)
    leg_event = "INSERT INTO ai_trial_pair_events (pair_id, leg, event) VALUES (%s, %s, %s)"
    label = (
        "INSERT INTO ai_trial_pair_labels (pair_id, entry_session, regime_label, classifier_version) "
        "VALUES (%s, '2026-10-05', 'calm', 'r1')"
    )

    _refused(ebull_test_conn, leg_event, (pair_id, "arm", "filled"), "illegal arm leg event filled after <none>")
    _refused(ebull_test_conn, label, (pair_id,), "has not filled")
    for event in ("submitted", "uncertain", "filled", "censored", "closed"):
        ebull_test_conn.execute(leg_event, (pair_id, "arm", event))
    ebull_test_conn.execute(label, (pair_id,))
    ebull_test_conn.commit()
    _refused(ebull_test_conn, leg_event, (pair_id, "arm", "censored"), "illegal arm leg event censored after closed")

    # `broken` is pair-level, once, with reasons — and does not stop a late leg's events (O11).
    broken = "INSERT INTO ai_trial_pair_events (pair_id, event, reasons) VALUES (%s, 'broken', %s)"
    _refused(ebull_test_conn, broken, (pair_id, []), "ai_trial_pair_events_reasons_iff_broken")
    _refused(ebull_test_conn, broken, (pair_id, ["late_fill,unresolved"]), "is not a reason code")
    ebull_test_conn.execute(broken, (pair_id, ["late_fill"]))
    ebull_test_conn.commit()
    _refused(ebull_test_conn, broken, (pair_id, ["unresolved"]), "already broken")
    ebull_test_conn.execute(leg_event, (pair_id, "control", "submitted"))
    ebull_test_conn.execute(leg_event, (pair_id, "control", "filled"))
    ebull_test_conn.commit()


# ---------------------------------------------------------------------------
# sql/439 — §16.3: the recorded plan re-derived without dividing; §16.11 baselines; §16.4 control
# ---------------------------------------------------------------------------
def test_decision_plans_are_verified_without_dividing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id = _seed(conn)
    run_id = _claim(conn, declaration_id)
    good = _decision_params(run_id, 0, INSTRUMENTS[0], "thesis_too_long")
    assert (good["stop"], good["mult"], good["r"]) == (Decimal("8.0000"), Decimal("2.0000"), Decimal("2.0000"))

    def refused(match: str, **overrides: object) -> None:
        _decide(conn, run_id)
        with pytest.raises(psycopg.Error, match=match):
            conn.execute(_RAW_DECISION, good | overrides)
        conn.rollback()

    refused("r_multiple", r=Decimal("2.0001"))
    refused("r_multiple", r=Decimal("2.00001"))  # inside the bracket but off the 4-decimal grid
    refused("r_multiple", r=None)
    refused("stop_atr_multiple", mult=Decimal("1.9999"))
    refused("atr14_pct", atr_pct=Decimal("4.0001"))
    refused("stop_price", stop_price=Decimal("92.5"))
    refused("present exactly when invalidation_price", stop_price=None)
    refused("target_pct", target=16.0001)
    # The double is read by its shortest round-trip form: a 15-digit `::numeric` cast would turn
    # 8.000000000000002 into 8 and let an off-grid stop through.
    refused("stop_pct", stop=8.000000000000002)
    refused("all present or all NULL", close=None)
    refused("unmapped symbol", iid=None, reason="not_in_shortlist")
    refused("without a valid ATR", **(_plan(None) | {"inv": Decimal("93")}))
    refused("names its setup", setup=None)
    refused("ai_trial_decisions_baseline_paired", holdout=None)
    refused("ai_trial_decisions_baseline_shape", train="1/0")
    refused("ai_trial_decisions_reason_code_check", reason="setup_negative_base_rate")  # order 7 is retired
    # Accepted rows: a baseline, a named setup, and a plan passing orders 8-12 (h = 10: floor 1.5).
    accepted = {"verdict": "accepted", "reason": None}
    refused("ai_trial_decisions_accepted_has_baseline", **accepted, train=None, holdout=None)
    refused("accepted decision needs", **accepted, setup="none")
    refused("accepted decision needs", **accepted, **_plan(ATR, "97.5", "116"))  # 0.875 ATR
    refused("accepted decision needs", **accepted, **_plan(ATR, "93", "110"))  # R 1.25

    _decide(conn, run_id)
    conn.execute(_RAW_DECISION, good)
    conn.execute(_RAW_DECISION, _decision_params(run_id, 1, None, "not_in_shortlist"))
    # A refused row may carry a negative stop distance, recorded signed, with no R (r2-11/13).
    negative = good | {"pos": 2, "reason": "level_unavailable"} | _plan(ATR, "102", "116")
    assert (negative["mult"], negative["stop"], negative["r"]) == (Decimal("-0.2500"), Decimal("-1.0000"), None)
    conn.execute(_RAW_DECISION, negative)
    conn.commit()


def test_the_control_plan_must_be_derived_and_admissible(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id = _seed(conn)
    run_id = _claim(conn, declaration_id)
    pool = INSTRUMENTS[1:]
    draw = draw_control(declaration_sha256_hex=DOC_SHA, session_date=SESSION, pair_seq=0, pool=pool)

    def attempt(**plan: object) -> None:
        _decide(conn, run_id)
        decision_id = _decision(conn, run_id, 0, INSTRUMENTS[0])
        conn.execute(
            _RAW_PAIR,
            {"decl": declaration_id, "seq": 0, "decision": decision_id, "control": draw.instrument_id,
             "seed": draw.seed_material, "pool": list(pool), "idx": draw.index, **plan},
        )  # fmt: skip

    ok = _plan(measure_atr(3.0, 100.0), "94.75", "112")  # stop 94: 2 ATRs, 6%; target 12%; R 2
    assert (ok["stop"], ok["target"], ok["r"]) == (Decimal("6.0000"), Decimal("12.0000"), Decimal("2.0000"))
    for overrides, match in (
        ({"mult": Decimal("2.0001")}, "stop_atr_multiple"),
        ({"atr_pct": Decimal("3.0001")}, "atr14_pct"),
        ({"stop_price": Decimal("94.5")}, "stop_price"),
        ({"r": None}, "carries the control"),
        (_plan(measure_atr(3.0, 100.0), "97", "112"), "does not pass"),  # 1.25 ATR < the 10-session 1.5
        (_plan(measure_atr(0.5, 100.0), "98.625", "103"), "does not pass"),  # stop 1.5%: under the 2% bound
    ):
        with pytest.raises(psycopg.Error, match=match):
            attempt(**(ok | overrides))
        conn.rollback()
    attempt(**ok)
    conn.commit()
