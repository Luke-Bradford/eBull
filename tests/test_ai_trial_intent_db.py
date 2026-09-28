"""#3471 slice 2b-i — the trial loader's §8 authorisation query against real Postgres.

One published pair (digest-intact declaration → decided run → accepted decision → drawn
control → both legs' fired signals linked), then each authorisation and kept safety gate
refused in turn. The gate-map half is ``test_ai_trial_intent.py``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.services.ai_trial_decision import decision_metrics, derive_control_levels, draw_control, measure_atr
from app.services.ai_trial_intent import DECLARATION_CONTRACT_PREFIX, declaration_digest, load_trial_intent
from app.services.strategy_control_plane import (
    configure_deployment,
    configure_execution_policy,
    configure_paper_pool,
)

Conn = psycopg.Connection[Any]

SESSION = date(2026, 10, 5)  # a Monday
NOW = datetime(2026, 10, 5, 15, 0, tzinfo=UTC)  # 11:00 New York
ARM_INSTRUMENT = 347101
POOL = (347102, 347103, 347104)
# §6/§7 v5: the arm name's ATR is 4% (stop 8 = 2 ATRs, R 2); every control name's is 2%, so the
# control leg's derived levels are 4 / 8, not the arm's 8 / 16.
ARM_ATR = measure_atr(4.0, 100.0)
CONTROL_ATR = measure_atr(2.0, 100.0)


def _seed_instruments(conn: Conn) -> None:
    conn.execute(
        "INSERT INTO exchanges (exchange_id, country, asset_class) VALUES ('2', 'US', 'us_equity') "
        "ON CONFLICT (exchange_id) DO UPDATE SET asset_class='us_equity'"
    )
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable) "
        "SELECT i, 'AIT' || i, 'AI trial ' || i, '2', 'USD', TRUE FROM unnest(%s::bigint[]) AS i "
        "ON CONFLICT (instrument_id) DO NOTHING",
        ([ARM_INSTRUMENT, *POOL],),
    )
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag) "
        "SELECT i, %s, 99, 100, 99.5, 1, false FROM unnest(%s::bigint[]) AS i "
        "ON CONFLICT (instrument_id) DO NOTHING",
        (NOW, [ARM_INSTRUMENT, *POOL]),
    )
    conn.execute(
        "INSERT INTO strategy_halt_feed_state (source, fetched_at, source_pub_at, item_count, payload_sha256) "
        "VALUES ('nasdaq_trader_rss', %s, %s, 0, %s) ON CONFLICT (source) DO NOTHING",
        (NOW, NOW, "0" * 64),
    )
    if conn.execute("SELECT 1 FROM strategy_paper_pool_events LIMIT 1").fetchone() is None:
        configure_paper_pool(
            conn,
            enabled=True,
            capital_limit=Decimal("2000"),
            risk_profile="balanced",
            approval_mode="manual",
            changed_by="test",
            reason="#3471 trial loader fixture",
        )


def _declare(conn: Conn, version: str, *, tamper: bool) -> tuple[int, str]:
    doc = {"strategy_id": "ai-discretionary-v1", "strategy_version": version, "caps": {"full": 250, "half": 125}}
    digest = declaration_digest(doc)
    stored_sha = declaration_digest({**doc, "tampered": True}) if tamper else digest
    row = conn.execute(
        """
        INSERT INTO strategy_preregistration_declarations (
            strategy_id, strategy_version, contract_version, prereg_purpose,
            structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
            declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
            min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256
        ) VALUES (
            'ai-discretionary-v1', %(version)s, %(contract)s, 'falsification_only',
            'test-policy', 'survivor_only', true, true, '{}', 40, 8,
            'spec §9 cohort, sessions 1-40', 'test', %(digest)s
        )
        RETURNING declaration_id
        """,
        {"version": version, "contract": DECLARATION_CONTRACT_PREFIX + stored_sha, "digest": stored_sha},
    ).fetchone()
    assert row is not None
    declaration_id = int(row[0])
    conn.execute(
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, 'ai-discretionary-v1', %s, 'docs/test.json', %s, %s)",
        (declaration_id, version, Jsonb(doc), stored_sha),
    )
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'active', 'freeze', 'supervisor')",
        (declaration_id,),
    )
    return declaration_id, stored_sha


def _deploy(conn: Conn, strategy_id: str, version: str, *, policy_stop: str) -> None:
    deployment = configure_deployment(
        conn,
        strategy_id=strategy_id,
        strategy_version=version,
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="#3471 trial loader fixture",
    )
    configure_execution_policy(
        conn,
        deployment_id=deployment.deployment_id,
        ticket_sizing_mode="fixed",
        ticket_fraction=None,
        fixed_ticket_amount=Decimal("125"),
        max_ticket_amount=Decimal("250"),
        stop_loss_pct=Decimal(policy_stop),
        take_profit_pct=Decimal("100"),
        max_quote_age_seconds=60,
        max_scan_age_seconds=60,
        max_halt_feed_age_seconds=60,
        max_cost_age_seconds=60,
        max_reconciliation_age_seconds=60,
        max_instrument_exposure_pct=Decimal("30"),
        max_portfolio_exposure_pct=Decimal("80"),
        max_drawdown_pct=Decimal("10"),
        min_net_expectancy_pct=Decimal("0"),
        cost_stress_multiplier=Decimal("2"),
        changed_by="test",
        reason="#3471 trial loader fixture",
    )


def _signal(conn: Conn, strategy_id: str, version: str, instrument_id: int) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind, verdict,
            fill_bar_date, fill_price, universe, input_rule_set_versions
        ) VALUES (%s, %s, %s, '2026-10-02', 'entry', 'fired', %s, 100, 'survivor_only', '{"ai_trial": "v1"}')
        RETURNING signal_id
        """,
        (strategy_id, version, instrument_id, SESSION),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _published_pair(
    conn: Conn, version: str = "v1", *, tamper: bool = False, policy_stop: str = "25"
) -> tuple[int, dict[str, int]]:
    """A decided run with one accepted decision and its pair, both legs linked; returns the
    declaration id and each leg's signal id."""
    _seed_instruments(conn)
    declaration_id, doc_sha = _declare(conn, version, tamper=tamper)
    arm, control = "ai-discretionary-v1", "ai-discretionary-v1-control"
    for strategy_id in (arm, control):
        _deploy(conn, strategy_id, version, policy_stop=policy_stop)
    conn.commit()
    run = conn.execute(
        "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) "
        "SELECT %s, %s, now(), now() + interval '15 minutes' RETURNING run_id",
        (declaration_id, SESSION),
    ).fetchone()
    assert run is not None
    conn.commit()
    sha = "a" * 64
    conn.execute(
        """
        UPDATE ai_trial_runs SET
            status = 'decided', scores_model_version = 'v1.5', scores_scored_at = now(),
            pack = '{}', pack_sha256 = %(sha)s, rendered_prompt = 'p',
            rendered_prompt_sha256 = encode(sha256('p'::bytea), 'hex'),
            system_prompt_sha256 = %(sha)s, prompt_template_sha256 = %(sha)s, model_id = 'claude-opus-5-5',
            argv_sha256 = %(sha)s, executable_path = '/usr/local/bin/claude', cli_version = '2.1.280',
            git_sha = %(git)s, policy_hash = %(sha)s, init_event = '{}', exit_code = 0,
            structured_output = '{}'
        WHERE run_id = %(run_id)s
        """,
        {"sha": sha, "git": "b" * 40, "run_id": run[0]},
    )
    metrics = decision_metrics(8.0, 16.0, ARM_ATR)
    assert ARM_ATR is not None and CONTROL_ATR is not None
    decision = conn.execute(
        """
        INSERT INTO ai_trial_decisions (
            run_id, response_position, action, symbol, stop_pct, target_pct, horizon_days,
            size_tier, confidence, thesis, instrument_id, verdict,
            atr14, close, atr14_pct, stop_atr_multiple, r_multiple
        ) VALUES (%s, 0, 'enter_long', 'AIT347101', 8.0, 16.0, 10, 'half', 3, 'Thesis.', %s, 'accepted',
                  %s, %s, %s, %s, %s)
        RETURNING decision_id
        """,
        (
            run[0],
            ARM_INSTRUMENT,
            ARM_ATR.atr14,
            ARM_ATR.close,
            ARM_ATR.atr14_pct,
            metrics.stop_atr_multiple,
            metrics.r_multiple,
        ),
    ).fetchone()
    assert decision is not None
    draw = draw_control(declaration_sha256_hex=doc_sha, session_date=SESSION, pair_seq=0, pool=POOL)
    levels = derive_control_levels(metrics, CONTROL_ATR)
    assert levels is not None
    pair = conn.execute(
        """
        INSERT INTO ai_trial_pairs (
            declaration_id, pair_seq, arm_decision_id, control_instrument_id, seed_material, pool,
            draw_idx, stop_pct, target_pct, horizon_days, size_tier,
            control_atr14, control_close, control_atr14_pct, control_stop_pct, control_target_pct
        ) VALUES (%s, 0, %s, %s, %s, %s, %s, 8.0, 16.0, 10, 'half', %s, %s, %s, %s, %s)
        RETURNING pair_id
        """,
        (
            declaration_id,
            decision[0],
            draw.instrument_id,
            draw.seed_material,
            list(POOL),
            draw.index,
            CONTROL_ATR.atr14,
            CONTROL_ATR.close,
            CONTROL_ATR.atr14_pct,
            levels.stop_pct,
            levels.target_pct,
        ),
    ).fetchone()
    assert pair is not None
    conn.commit()
    signals = {
        "arm": _signal(conn, arm, version, ARM_INSTRUMENT),
        "control": _signal(conn, control, version, draw.instrument_id),
    }
    for leg, signal_id in signals.items():
        conn.execute(
            "INSERT INTO ai_trial_leg_links (pair_id, leg, signal_id) VALUES (%s, %s, %s)", (pair[0], leg, signal_id)
        )
    conn.commit()
    return declaration_id, signals


def _reason(conn: Conn, signal_id: int, now: datetime = NOW) -> str | None:
    _, reason, _ = load_trial_intent(conn, signal_id=signal_id, now=now)
    conn.commit()
    return reason


def test_both_legs_load_with_the_decision_terms_and_no_evidence(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id, signals = _published_pair(conn)
    for leg, signal_id in signals.items():
        intent, reason, halt_evaluated = load_trial_intent(conn, signal_id=signal_id, now=NOW)
        assert reason is None and intent is not None and halt_evaluated
        assert intent.leg == leg
        assert intent.strategy_id == "ai-discretionary-v1" + ("-control" if leg == "control" else "")
        assert intent.declaration_id == declaration_id and intent.session_date == SESSION
        # §8 table (v5): the arm exits at the decision's levels, the control at its own
        # ATR-derived levels (2 ATRs of a 2% name, R 2).
        expected = (Decimal("8.0"), Decimal("16.0")) if leg == "arm" else (Decimal("4.0000"), Decimal("8.0000"))
        assert (intent.stop_loss_pct, intent.take_profit_pct) == expected
        assert (intent.size_tier, intent.requested_amount, intent.horizon_days) == ("half", Decimal("125"), 10)
        assert intent.ask == Decimal("100")
    conn.commit()


def test_authorisation_and_kept_gates_refuse(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id, signals = _published_pair(conn)
    arm = signals["arm"]

    # The trial loader refuses a non-trial signal (§8 slice-2 test list), before any query.
    non_trial = _signal(conn, "S-NOT-A-TRIAL", "v1", ARM_INSTRUMENT)
    assert _reason(conn, non_trial) == "strategy_not_demo_trial"
    # A trial-strategy signal the trial never decided has no link.
    unlinked = _signal(conn, "ai-discretionary-v1", "v1", POOL[0])
    assert _reason(conn, unlinked) == "trial_link_missing"

    # The target session is the freshness bound, both directions, then the session hours.
    assert _reason(conn, arm, NOW + timedelta(days=1)) == "decision_expired"
    assert _reason(conn, arm, NOW - timedelta(days=1)) == "decision_not_yet_due"
    after_close = NOW.replace(hour=20, minute=30)  # 16:30 New York, same session date
    conn.execute("UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (after_close, ARM_INSTRUMENT))
    conn.execute("UPDATE strategy_halt_feed_state SET fetched_at = %s", (after_close,))
    assert _reason(conn, arm, after_close) == "market_session_closed"
    conn.execute("UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW, ARM_INSTRUMENT))
    conn.execute("UPDATE strategy_halt_feed_state SET fetched_at = %s", (NOW,))
    # Kept safety gates, same codes as the paper path.
    assert _reason(conn, arm, NOW + timedelta(seconds=120)) == "quote_stale"
    conn.execute("UPDATE quotes SET spread_flag = true WHERE instrument_id = %s", (ARM_INSTRUMENT,))
    assert _reason(conn, arm) == "quote_spread_flagged"
    conn.execute("UPDATE quotes SET spread_flag = false WHERE instrument_id = %s", (ARM_INSTRUMENT,))
    conn.execute("UPDATE instruments SET is_tradable = false WHERE instrument_id = %s", (ARM_INSTRUMENT,))
    assert _reason(conn, arm) == "instrument_not_tradable"
    conn.execute("UPDATE instruments SET is_tradable = true WHERE instrument_id = %s", (ARM_INSTRUMENT,))
    conn.commit()
    assert _reason(conn, arm) is None

    # A halt stops even an already-queued decision (§8), and it comes before any market fact.
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'active', 'halted_operator', 'test', 'operator')",
        (declaration_id,),
    )
    conn.commit()
    assert _reason(conn, arm) == "trial_not_active"
    assert _reason(conn, signals["control"]) == "trial_not_active"


def test_a_tampered_declaration_and_an_over_policy_stop_refuse(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    # The #2599 contract names the stored sha, so the database accepts it; only the
    # canonical re-hash sees that the document is not the one the sha names.
    _, tampered = _published_pair(conn, "v2", tamper=True)
    assert _reason(conn, tampered["arm"]) == "trial_declaration_not_intact"
    # The policy stop bounds EACH leg's own stop: the arm's 8% fails a 5% policy stop, the
    # control's derived 4% passes it.
    _, strict = _published_pair(conn, "v3", policy_stop="5")
    assert _reason(conn, strict["arm"]) == "decision_stop_exceeds_policy"
    assert _reason(conn, strict["control"]) is None
