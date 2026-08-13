"""#2454 strategy governance and exact-position ownership integration tests."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, cast

import psycopg
import psycopg.sql
import pytest

from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.result_ledger import (
    store_holdout_result,
    store_in_sample_arm_pair,
    store_in_sample_result,
    stored_result_promotion_refusals,
    stored_result_promotion_refusals_for,
)
from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION
from app.services.strategy_control_plane import (
    StrategyControlError,
    StrategyOwnershipError,
    assert_exact_position_owned,
    claim_exact_position,
    configure_deployment,
    configure_paper_pool,
    create_strategy_trade,
    current_stage,
    decide_funding,
    link_strategy_order,
    promote_strategy,
    record_order_position_execution,
    release_exact_position,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_promotion_evidence import (
    EVIDENCE_VERSION,
    REQUIRED_CHALLENGERS,
    REQUIRED_CONTRASTS,
    REQUIRED_COST_INPUTS,
    ChallengerEvidence,
    ExpectedValueBucket,
    OutcomeContrast,
    PromotionEvidence,
    RecentYearEvidence,
)
from app.services.strategy_promotion_evidence_store import (
    load_promotion_evidences,
    store_promotion_evidence,
)
from app.services.strategy_result import StrategyResult
from app.services.strategy_result_ambiguity import (
    AMBIGUITY_RULE_VERSION,
    AmbiguityRecord,
    load_result_ambiguities,
    store_result_ambiguity,
)
from app.services.strategy_result_universe import (
    ResultUniverseRecord,
    load_result_universes,
    store_result_universe,
)
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION
from tests.test_result_ledger import (
    BOOTSTRAP_BLOCK,
    build_control,
    build_deflated,
    build_metrics,
    build_result,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]


def _passing_promotion_evidence(*, lower_bound: str = "0.1") -> PromotionEvidence:
    return PromotionEvidence(
        evidence_version=EVIDENCE_VERSION,
        causal_observation_rule_version="causal-v1",
        fill_rule_version="fills-v1",
        overlap_rule_version="overlap-v1",
        after_cost_expectancy_ci_low_pct=Decimal(lower_bound),
        max_drawdown_pct=Decimal("-8"),
        expected_shortfall_5_pct=Decimal("-3"),
        worst_gap_pct=Decimal("-5"),
        excluding_best_1_expectancy_pct=Decimal("0.2"),
        recent_year_stable=True,
        recent_years_evaluated=3,
        recent_year_evidence=(
            RecentYearEvidence(2024, 34, Decimal("0.2"), Decimal("-0.1"), True),
            RecentYearEvidence(2025, 33, Decimal("0.3"), Decimal("0.0"), True),
            RecentYearEvidence(2026, 33, Decimal("0.4"), Decimal("0.1"), True),
        ),
        max_date_contribution_pct=Decimal("8"),
        max_name_contribution_pct=Decimal("7"),
        max_sector_contribution_pct=Decimal("20"),
        max_concurrency=12,
        capacity_usd=Decimal("100000"),
        risk_limits_version="test-risk-v1",
        risk_limits_passed=True,
        probability_calibration_passed=True,
        path_diagnostics_complete=True,
        outcome_count=100,
        profitable_outcome_count=60,
        losing_outcome_count=40,
        flat_outcome_count=0,
        target_first_count=40,
        stop_first_count=30,
        timeout_count=30,
        ambiguous_path_count=2,
        observed_cost_inputs=REQUIRED_COST_INPUTS,
        cost_observed_on=date.today(),
        cost_valid_through=date.today(),
        cost_source_version="etoro-quote-v1",
        spread_bps=Decimal("8"),
        slippage_bps=Decimal("5"),
        financing_bps_per_day=Decimal("1"),
        fx_bps=Decimal("2"),
        broker_eligible=True,
        challengers=tuple(
            ChallengerEvidence(
                role,
                100,
                Decimal("0.1"),
                Decimal("0.2"),
                True,
                "causal-v1",
                "fills-v1",
                "overlap-v1",
            )
            for role in sorted(REQUIRED_CHALLENGERS)
        ),
        ev_buckets=(
            ExpectedValueBucket(1, 34, Decimal("-0.2"), Decimal("-0.1")),
            ExpectedValueBucket(2, 33, Decimal("0.1"), Decimal("0.2")),
            ExpectedValueBucket(3, 33, Decimal("0.4"), Decimal("0.5")),
        ),
        outcome_contrasts=tuple(
            OutcomeContrast(role, 60, 40, Decimal("1"), Decimal("0"), Decimal("1"))
            for role in sorted(REQUIRED_CONTRASTS)
        ),
    )


def _universe_record(
    conn: psycopg.Connection[Any],
    result_id: int,
    *,
    evaluated: frozenset[int] = frozenset({1, 2, 3}),
    universe: frozenset[int] = frozenset({1, 2, 3, 4, 5}),
) -> None:
    """The #2621 frozen-universe record a pinned result must carry to promote."""
    store_result_universe(
        conn,
        result_id=result_id,
        record=ResultUniverseRecord(
            universe_rule_version=VALIDATED_UNIVERSE_RULE_VERSION,
            evaluated_instrument_ids=evaluated,
            validated_universe_ids=universe,
        ),
    )


def _ambiguity_record(conn: psycopg.Connection[Any], result_id: int) -> None:
    """The #2625 frozen §3.4 record a pinned result must carry to promote.

    ``shared_measurement`` is the immaterial verdict a non-level strategy
    produces — the only basis that reaches "not material" without a random
    cohort, which this fixture has no way to supply.
    """
    store_result_ambiguity(
        conn,
        result_id=result_id,
        record=AmbiguityRecord(
            ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
            comparison_basis="shared_measurement",
        ),
    )


#: #2625 — the row stamps ``promote_strategy`` now replays through the shared
#: ``structural_promotion_refusals``. ⚠ ``build_result`` defaults to
#: ``survivor_only`` + both costs unmodelled, which is correct for the corpus we
#: actually hold and means a fixture must OPT IN to being promotable. Spelled
#: out here rather than buried per-test so the opt-in is visible.
_PROMOTABLE_STAMPS: dict[str, Any] = {
    "universe_basis": "survivorship_free",
    "carry_unmodelled": False,
    "fx_unmodelled": False,
}


def _promotable_row(**overrides: Any) -> StrategyResult:
    """A row that clears the clauses #2639 taught the transition to replay.

    ⚠ THE OPT-IN GOT WIDER, and every part of it is a real refusal the shared
    helpers now apply at the transition rather than only at result production:

    - the DSR must name TODAY's register — ``build_deflated`` defaults to
      ``declared_trials=11`` under ``trial-register-2026-08-07``, both
      superseded, which is ``trial_register_superseded``;
    - the criterion-3 block must be present, or ``effective_sample_size_not_computed``;
    - §9's control must clear BOTH thresholds — ``build_control`` deliberately
      builds one that fails, because that is the shape today's pipeline
      produces, so a promotable fixture has to move the cohort side. ⚠ Only the
      COHORT side: ``StrategyResult`` binds the two strategy-side figures to
      ``metrics``.
    """
    deflated = build_deflated(
        declared_trials=TRIAL_REGISTER.declared_count,
        trial_register_version=TRIAL_REGISTER_VERSION,
    )
    metrics = build_metrics(profit_factor=1.2, **BOOTSTRAP_BLOCK)
    base: dict[str, Any] = {
        "metrics": metrics,
        "deflated": deflated,
        "trial_count": deflated.declared_trials,
        "deflated_sharpe": Decimal(repr(deflated.deflated_sharpe)),
        "synthetic_control": build_control(
            metrics,
            mean_return_ci_low_pct=-1.0,
            mean_return_ci_high_pct=1.0,
            cohort_sharpe_threshold=-9.0,
        ),
        "evaluated_instrument_count": 3,
        **_PROMOTABLE_STAMPS,
    }
    base.update(overrides)
    return build_result(**base)


def _promotable_pair(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    ambiguity_arm: str = "worst_case",
    **overrides: Any,
) -> int:
    """Store BOTH quarantine arms and return the masked one's ``result_id``.

    ⚠ #2639 — criterion 9 is satisfied by the COMPARISON, and the transition now
    re-derives it from the flipped-arm identity hash. A lone arm refuses
    ``quarantine_arms_not_compared`` however clean it is otherwise, so a fixture
    that means to promote must write the pair.

    ⚠ ``result_scope="portfolio"`` BY DEFAULT, and it is load-bearing rather
    than arbitrary: the tests that call this also write single-armed rows to
    isolate a refusal, and neither the metrics, the DSR nor the control is part
    of ``ResultIdentity`` — so a pair sharing the callers' arms would hash to an
    existing ``result_version`` and hit ``strategy_results_unique``. The scope
    is an identity member and separates them.
    """
    shared: dict[str, Any] = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "namespace": "in_sample",
        "result_scope": "portfolio",
        "ambiguity_arm": ambiguity_arm,
        **overrides,
    }
    masked = _promotable_row(quarantine_arm="masked", **shared)
    admitted = _promotable_row(quarantine_arm="admitted", **shared)
    masked_id, _ = store_in_sample_arm_pair(conn, masked, admitted)
    return masked_id


def _recorded_holdout_evaluation(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
) -> None:
    """One hold-out evaluation WITH its ``evaluate`` access, for criterion 5.

    ⚠ #2639 — the transition now reads ``holdout_access_counts`` live, so a
    strategy version whose hold-out was never evaluated refuses
    ``holdout_never_evaluated``. That is ``check_promotable``'s own rule; the
    transition had simply never applied it. ``store_holdout_result`` writes the
    access first because ``sql/264``'s trigger requires it.
    """
    store_holdout_result(
        conn,
        _promotable_row(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            namespace="hold_out",
            ambiguity_arm="best_case",
            quarantine_arm="admitted",
        ),
        accessed_by="tests/test_strategy_control_plane.py",
        purpose="#2639 criterion 5 fixture",
    )


def _instrument(conn: psycopg.Connection[Any], instrument_id: int = 2454001) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
        VALUES (%s, %s, %s, true)
        """,
        (instrument_id, f"T{instrument_id}", f"Test {instrument_id}"),
    )


def _signal(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int = 2454001,
    strategy_id: str = "S-OWN",
    strategy_version: str = "v1",
) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES (%s, %s, %s, '2026-08-06', 'entry', 'fired',
                  '2026-08-07', 100, 'survivor_only',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """,
        (strategy_id, strategy_version, instrument_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _paper_stage(conn: psycopg.Connection[Any]) -> None:
    """Seed an already-audited chain; transition mechanics are tested separately."""
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES
          ('S-OWN', 'v1', NULL, 'research_candidate', 'test-v1', NULL, 'test', 'registered'),
          ('S-OWN', 'v1', 'research_candidate', 'historical_validated', 'test-v1', 'e:hist', 'test', 'validated'),
          ('S-OWN', 'v1', 'historical_validated', 'forward_observation', 'test-v1', 'e:fwd', 'test', 'observe'),
          ('S-OWN', 'v1', 'forward_observation', 'paper_enabled', 'test-v1', 'e:paper', 'test', 'paper')
        """
    )


def _deployment_and_trade(conn: psycopg.Connection[Any], signal_id: int) -> int:
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="test allocation",
    )
    decision_id = decide_funding(
        conn,
        signal_id=signal_id,
        verdict="allocated",
        deployment_id=deployment.deployment_id,
        amount=Decimal("100"),
        reason_code="within_risk_budget",
    )
    return create_strategy_trade(conn, decision_id)


def _order(conn: psycopg.Connection[Any], *, instrument_id: int, origin: str = "strategy") -> int:
    row = conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status,
            execution_origin
        ) VALUES (%s, 'BUY', 'MARKET', 100, 'filled', %s)
        RETURNING order_id
        """,
        (instrument_id, origin),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _position(conn: psycopg.Connection[Any], position_id: int, instrument_id: int) -> None:
    conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES (%s, %s, true, 1, 100, 100, 100, 1, now(), '{}'::jsonb)
        """,
        (position_id, instrument_id),
    )


def test_promotion_is_ordered_explicit_and_evidenced(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    first = promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register preregistered candidate",
    )
    assert first.from_stage is None
    assert current_stage(conn, "S-GOV", "v1") == "research_candidate"
    with pytest.raises(StrategyControlError, match="strategy_id must be non-empty"):
        promote_strategy(
            conn,
            strategy_id="",
            strategy_version="v1",
            to_stage="live_enabled",
            promoted_by="operator",
            reason="invalid identity must be rejected first",
        )

    with pytest.raises(StrategyControlError, match="invalid promotion transition"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="paper_enabled",
            promoted_by="operator",
            reason="skip evidence",
            evidence_ref="invalid:skip",
        )
    with pytest.raises(StrategyControlError, match="requires an immutable evidence_ref"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="missing evidence",
        )

    # Runtime switches are deliberately irrelevant to governance state.
    conn.execute("UPDATE runtime_config SET enable_auto_trading = true, enable_live_trading = true WHERE id = true")
    assert current_stage(conn, "S-GOV", "v1") == "research_candidate"


def test_historical_validation_requires_passing_edge_evidence(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register candidate",
    )
    result = build_result(
        strategy_id="S-GOV",
        strategy_version="v1",
        namespace="in_sample",
        metrics=build_metrics(profit_factor=1.2),
        evaluated_instrument_count=3,
        **_PROMOTABLE_STAMPS,
    )
    result_id = store_in_sample_result(conn, result)
    _universe_record(conn, result_id)
    _ambiguity_record(conn, result_id)

    with pytest.raises(StrategyControlError, match="promotion_evidence_missing"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="must not validate a bare result",
            evidence_ref="result:test",
            result_ids=(result_id,),
        )

    store_promotion_evidence(
        conn,
        result_id=result_id,
        evidence=_passing_promotion_evidence(lower_bound="0"),
    )
    with pytest.raises(StrategyControlError, match="expectancy_lower_bound_not_positive"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="present but failing evidence is still a refusal",
            evidence_ref="result:test",
            result_ids=(result_id,),
        )

    # ⚠ The passing row also has to clear #2639's replays now — the arm pair,
    # criterion 5's live counts, the DSR against today's register and §9's
    # control. The failing rows above stay single-armed on purpose: they isolate
    # the #2505 evidence refusal this test is about.
    _recorded_holdout_evaluation(conn, strategy_id="S-GOV", strategy_version="v1")
    passing_result_id = _promotable_pair(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        ambiguity_arm="best_case",
    )
    _universe_record(conn, passing_result_id)
    _ambiguity_record(conn, passing_result_id)
    store_promotion_evidence(
        conn,
        result_id=passing_result_id,
        evidence=_passing_promotion_evidence(),
    )
    promotion = promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="historical_validated",
        promoted_by="operator",
        reason="all #2505 evidence passes",
        evidence_ref="result:test-passing",
        result_ids=(passing_result_id,),
    )
    assert promotion.to_stage == "historical_validated"

    with pytest.raises(StrategyControlError, match="expectancy_lower_bound_not_positive"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="forward_observation",
            promoted_by="operator",
            reason="later evidence stages cannot introduce a failing result",
            evidence_ref="result:test-failing-forward",
            result_ids=(result_id,),
        )


def test_promotion_replays_the_frozen_universe_check(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2621 — a result whose evaluated set left the §4.0 validated universe
    cannot reach ``historical_validated`` through ``promote_strategy``, and a
    result stored without its frozen record refuses rather than passing on
    trust in the writer."""
    conn = ebull_test_conn
    promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register candidate",
    )

    def _result_id(ambiguity_arm: str, quarantine_arm: str) -> int:
        result = build_result(
            strategy_id="S-GOV",
            strategy_version="v1",
            namespace="in_sample",
            ambiguity_arm=ambiguity_arm,
            quarantine_arm=quarantine_arm,
            metrics=build_metrics(profit_factor=1.2),
            evaluated_instrument_count=2,
            **_PROMOTABLE_STAMPS,
        )
        result_id = store_in_sample_result(conn, result)
        store_promotion_evidence(conn, result_id=result_id, evidence=_passing_promotion_evidence())
        _ambiguity_record(conn, result_id)
        return result_id

    def _refuses(result_id: int, code: str) -> None:
        with pytest.raises(StrategyControlError, match=code):
            promote_strategy(
                conn,
                strategy_id="S-GOV",
                strategy_version="v1",
                to_stage="historical_validated",
                promoted_by="operator",
                reason="universe re-check must refuse",
                evidence_ref="result:test-universe",
                result_ids=(result_id,),
            )
        assert current_stage(conn, "S-GOV", "v1") == "research_candidate"

    # No frozen record at all — evidence alone must not be enough.
    _refuses(_result_id("worst_case", "masked"), "evaluated_universe_unrecorded")

    # Evaluated set leaves the frozen universe.
    outside_id = _result_id("worst_case", "admitted")
    _universe_record(conn, outside_id, evaluated=frozenset({1, 99}), universe=frozenset({1, 2, 3}))
    _refuses(outside_id, "instrument_outside_validated_universe")

    # Record that does not describe its own row.
    mismatched_id = _result_id("best_case", "masked")
    _universe_record(conn, mismatched_id, evaluated=frozenset({1, 2, 3}), universe=frozenset({1, 2, 3}))
    _refuses(mismatched_id, "evaluated_universe_count_mismatch")

    # A consistent subset record plus passing evidence promotes. ⚠ The passing
    # row must now also clear #2639's replays — the arm pair, criterion 5's
    # counts, the DSR against today's register and §9's control — so it is built
    # through `_promotable_pair` rather than the single-row `_result_id` above,
    # which exists to isolate the universe refusals.
    _recorded_holdout_evaluation(conn, strategy_id="S-GOV", strategy_version="v1")
    passing_id = _promotable_pair(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        ambiguity_arm="best_case",
        evaluated_instrument_count=2,
    )
    store_promotion_evidence(conn, result_id=passing_id, evidence=_passing_promotion_evidence())
    _ambiguity_record(conn, passing_id)
    _universe_record(conn, passing_id, evaluated=frozenset({1, 2}), universe=frozenset({1, 2, 3}))
    promotion = promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="historical_validated",
        promoted_by="operator",
        reason="frozen universe replay passes",
        evidence_ref="result:test-universe-passing",
        result_ids=(passing_id,),
    )
    assert promotion.to_stage == "historical_validated"


def test_promotion_replays_the_ambiguity_record_and_the_structural_stamps(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2625 — the §3.4 comparison and the row's own stamps are re-derived at
    the transition, not trusted from write time.

    ⚠⚠ THE STRUCTURAL HALF IS THE POINT. Before this, `promote_strategy` never
    read `universe_basis`, `carry_unmodelled` or `fx_unmodelled` — a `grep` for
    any of the three in `strategy_control_plane.py` returned nothing — so a
    result stamped `survivor_only` with both costs unmodelled, which is all 324
    rows in dev, could be pinned to a promotion without the transition ever
    looking. Tier 1's refusals could close and promotion would still not consult
    them.
    """
    conn = ebull_test_conn
    promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register candidate",
    )

    def _result_id(ambiguity_arm: str, quarantine_arm: str, **stamps: Any) -> int:
        result = build_result(
            strategy_id="S-GOV",
            strategy_version="v1",
            namespace="in_sample",
            ambiguity_arm=ambiguity_arm,
            quarantine_arm=quarantine_arm,
            metrics=build_metrics(profit_factor=1.2),
            evaluated_instrument_count=3,
            **{**_PROMOTABLE_STAMPS, **stamps},
        )
        result_id = store_in_sample_result(conn, result)
        _universe_record(conn, result_id)
        store_promotion_evidence(conn, result_id=result_id, evidence=_passing_promotion_evidence())
        return result_id

    def _refuses(result_id: int, code: str) -> None:
        with pytest.raises(StrategyControlError, match=code):
            promote_strategy(
                conn,
                strategy_id="S-GOV",
                strategy_version="v1",
                to_stage="historical_validated",
                promoted_by="operator",
                reason="replay must refuse",
                evidence_ref="result:test-2625",
                result_ids=(result_id,),
            )
        assert current_stage(conn, "S-GOV", "v1") == "research_candidate"

    # A clean row with no ambiguity record refuses — evidence and a frozen
    # universe are not enough on their own.
    _refuses(_result_id("worst_case", "masked"), "ambiguity_verdict_unrecorded")

    # The three structural stamps, each refusing on its own.
    survivor = _result_id("worst_case", "admitted", universe_basis="survivor_only")
    _ambiguity_record(conn, survivor)
    _refuses(survivor, "universe_basis_not_survivorship_free")

    carry = _result_id("best_case", "masked", carry_unmodelled=True)
    _ambiguity_record(conn, carry)
    _refuses(carry, "carry_unmodelled")

    # ⚠ Routed through the SHARED `structural_promotion_refusals`, so this also
    # pins that the transition and #2599's preregistration freeze read one copy
    # of the rule rather than two that can drift.
    _recorded_holdout_evaluation(conn, strategy_id="S-GOV", strategy_version="v1")
    passing = _promotable_pair(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        ambiguity_arm="best_case",
    )
    _universe_record(conn, passing)
    store_promotion_evidence(conn, result_id=passing, evidence=_passing_promotion_evidence())
    _ambiguity_record(conn, passing)
    promotion = promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="historical_validated",
        promoted_by="operator",
        reason="ambiguity and structural replays pass",
        evidence_ref="result:test-2625-passing",
        result_ids=(passing,),
    )
    assert promotion.to_stage == "historical_validated"


def test_promotion_replays_the_rows_own_clauses_and_the_holdout_counts(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2639 — the last inputs `promote_strategy` took on trust.

    ⚠⚠ EVERY REFUSAL BELOW WAS ALREADY IN `check_promotable` AND THE TRANSITION
    SIMPLY NEVER APPLIED IT. That is the #2621 defect surviving in the clauses
    #2625 stopped short of: a result stamped `harness_validation`, carrying no
    DSR, deflated against a superseded register, missing its criterion-3 sample
    size, missing §9's control, missing criterion 9's second arm, or belonging
    to a version whose hold-out was never evaluated, could all be pinned to a
    promotion without the transition looking.

    ⚠ The clean row is built ONCE and each case breaks exactly one thing, so a
    refusal is attributable. ⚠ Each case carries its own
    `input_rule_set_version` because neither the metrics, the DSR nor the
    control is part of `ResultIdentity` — two cases differing only in those hash
    to the SAME `result_version` and collide on `strategy_results_unique`.
    """
    conn = ebull_test_conn
    promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register candidate",
    )

    def _records(result_id: int) -> int:
        _universe_record(conn, result_id, evaluated=frozenset({1, 2, 3}))
        _ambiguity_record(conn, result_id)
        store_promotion_evidence(conn, result_id=result_id, evidence=_passing_promotion_evidence())
        return result_id

    def _refuses(result_id: int, code: str) -> None:
        with pytest.raises(StrategyControlError, match=code):
            promote_strategy(
                conn,
                strategy_id="S-GOV",
                strategy_version="v1",
                to_stage="historical_validated",
                promoted_by="operator",
                reason="the row's own clauses must refuse",
                evidence_ref="result:test-2639",
                result_ids=(result_id,),
            )
        assert current_stage(conn, "S-GOV", "v1") == "research_candidate"

    # Criterion 9 — one arm is not a comparison, however clean the row is.
    lone = _records(
        store_in_sample_result(
            conn,
            _promotable_row(
                strategy_id="S-GOV",
                strategy_version="v1",
                namespace="in_sample",
                ambiguity_arm="worst_case",
                quarantine_arm="masked",
                input_rule_set_version="price-quarantine-v1+2639lone000",
            ),
        )
    )
    _refuses(lone, "quarantine_arms_not_compared")

    # The row's OWN purpose, which the transition never read: it refuses on
    # `registered_strategy_purpose` — the MANIFEST's — and S-GOV is a
    # capital_candidate, so a harness-stamped row reached the gate untouched.
    harness = _records(
        _promotable_pair(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            ambiguity_arm="worst_case",
            purpose="harness_validation",
            input_rule_set_version="price-quarantine-v1+2639harness",
        )
    )
    _refuses(harness, "harness_validation_only")

    # Criterion 6 — deflated against a register that has since moved on.
    superseded_dsr = build_deflated(
        declared_trials=TRIAL_REGISTER.declared_count,
        trial_register_version="trial-register-2026-08-07",
    )
    superseded = _records(
        _promotable_pair(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            ambiguity_arm="best_case",
            input_rule_set_version="price-quarantine-v1+2639stale00",
            deflated=superseded_dsr,
            trial_count=superseded_dsr.declared_trials,
            deflated_sharpe=Decimal(repr(superseded_dsr.deflated_sharpe)),
        )
    )
    _refuses(superseded, "trial_register_superseded")

    # Criteria 6, 3 and §9 together — the shape today's pipeline writes.
    bare = _records(
        _promotable_pair(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            ambiguity_arm="best_case",
            input_rule_set_version="price-quarantine-v1+2639bare000",
            deflated=None,
            trial_count=None,
            deflated_sharpe=None,
            metrics=build_metrics(profit_factor=1.2),
            synthetic_control=None,
        )
    )
    _refuses(bare, "deflated_sharpe_not_computed")
    _refuses(bare, "effective_sample_size_not_computed")
    _refuses(bare, "synthetic_control_not_run")

    # Criterion 5 — read LIVE, so a version whose hold-out was never evaluated
    # refuses even with every frozen record in place.
    passing = _records(
        _promotable_pair(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            ambiguity_arm="worst_case",
            input_rule_set_version="price-quarantine-v1+2639pass000",
        )
    )
    _refuses(passing, "holdout_never_evaluated")

    # ⚠⚠ THE SAME PINNED RESULT, UNCHANGED, NOW PROMOTES — because the count it
    # depends on is a property of the STRATEGY VERSION and is read at the
    # transition, not frozen into the row. That is the whole content of the
    # `today` classification: a hold-out evaluation recorded afterwards moves
    # the verdict, and so would an unrecorded one, in the other direction.
    _recorded_holdout_evaluation(conn, strategy_id="S-GOV", strategy_version="v1")
    promotion = promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="historical_validated",
        promoted_by="operator",
        reason="every replayed clause passes",
        evidence_ref="result:test-2639-passing",
        result_ids=(passing,),
    )
    assert promotion.to_stage == "historical_validated"


def test_harness_control_cannot_be_promoted_or_funded(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    entry = next(iter(STRATEGY_MANIFEST.values()))
    version = entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
    promote_strategy(
        conn,
        strategy_id=entry.strategy_id,
        strategy_version=version,
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register control",
    )
    with pytest.raises(StrategyControlError, match="permanent controls"):
        promote_strategy(
            conn,
            strategy_id=entry.strategy_id,
            strategy_version=version,
            to_stage="historical_validated",
            promoted_by="operator",
            reason="must remain a control",
            evidence_ref="result:test",
            result_ids=(1,),
        )
    with pytest.raises(StrategyControlError, match="cannot receive capital"):
        configure_deployment(
            conn,
            strategy_id=entry.strategy_id,
            strategy_version=version,
            mode="paper",
            capital_limit=Decimal("100"),
            enabled=True,
            changed_by="operator",
            reason="must remain a control",
        )


def test_unregistered_strategy_cannot_cross_the_evidence_or_capital_boundary(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    promote_strategy(
        conn,
        strategy_id="S-UNREGISTERED",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="research registration alone is not capital admission",
    )
    with pytest.raises(StrategyControlError, match="unregistered strategies cannot advance"):
        promote_strategy(
            conn,
            strategy_id="S-UNREGISTERED",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="must be refused before evidence lookup",
            evidence_ref="result:test",
            result_ids=(1,),
        )
    with pytest.raises(StrategyControlError, match="unregistered strategies cannot receive capital"):
        configure_deployment(
            conn,
            strategy_id="S-UNREGISTERED",
            strategy_version="v1",
            mode="paper",
            capital_limit=Decimal("100"),
            enabled=True,
            changed_by="operator",
            reason="must not gain authority",
        )


def test_deployment_has_one_current_row_and_complete_history(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _paper_stage(conn)
    first = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="initial paper pot",
    )
    second = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("750"),
        enabled=False,
        changed_by="operator",
        reason="pause allocation",
    )
    assert second.deployment_id == first.deployment_id
    assert second.revision == 2
    assert conn.execute(
        "SELECT count(*) FROM strategy_deployments WHERE strategy_id='S-OWN' AND mode='paper'"
    ).fetchone() == (1,)
    assert conn.execute(
        "SELECT revision, capital_limit, enabled FROM strategy_deployment_events ORDER BY revision"
    ).fetchall() == [(1, Decimal("1000.000000"), True), (2, Decimal("750.000000"), False)]


def test_paper_principal_cannot_be_withdrawn_below_committed_capital(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _instrument(conn)
    signal_id = _signal(conn)
    _paper_stage(conn)
    _deployment_and_trade(conn, signal_id)
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1000"),
        risk_profile="balanced",
        changed_by="operator",
        reason="fund virtual sleeve",
    )

    with pytest.raises(StrategyControlError, match="below committed strategy capital"):
        configure_paper_pool(
            conn,
            enabled=False,
            capital_limit=Decimal("99"),
            risk_profile="balanced",
            changed_by="operator",
            reason="invalid withdrawal",
        )

    assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (1,)


def test_enabled_paper_pool_requires_and_persists_exact_versioned_mandate(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    with pytest.raises(StrategyControlError, match="configured portfolio risk mandate"):
        configure_paper_pool(
            conn,
            enabled=True,
            capital_limit=Decimal("1000"),
            risk_profile="unconfigured",
            changed_by="operator",
            reason="missing mandate",
        )

    pool = configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1000"),
        risk_profile="balanced",
        changed_by="operator",
        reason="balanced mandate",
    )

    assert pool.mandate.policy_version == "portfolio-mandate-v1"
    assert pool.mandate.risk_profile == "balanced"
    assert pool.mandate.target_volatility_pct == Decimal("12")
    assert pool.mandate.max_portfolio_drawdown_pct == Decimal("15")
    assert pool.mandate.max_loss_per_position_pct == Decimal("0.75")
    assert pool.mandate.max_daily_loss_pct == Decimal("1.5")
    assert pool.mandate.active_risk_budget_pct == Decimal("20")
    assert pool.mandate.cash_reserve_pct == Decimal("15")
    assert pool.mandate.max_concurrent_positions == 8
    assert not pool.mandate.shorts_allowed
    assert not pool.mandate.leverage_allowed


def test_same_instrument_manual_position_is_never_inferred_as_owned(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    instrument_id = 2454001
    _instrument(conn, instrument_id)
    signal_id = _signal(conn, instrument_id=instrument_id)
    _paper_stage(conn)
    trade_id = _deployment_and_trade(conn, signal_id)

    manual_position_id = 900001
    strategy_position_id = 900002
    second_strategy_position_id = 900003
    _position(conn, manual_position_id, instrument_id)
    entry_order = _order(conn, instrument_id=instrument_id)
    link_strategy_order(conn, strategy_trade_id=trade_id, order_id=entry_order, purpose="entry")
    duplicate_entry_order = _order(conn, instrument_id=instrument_id)
    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.transaction():
            link_strategy_order(
                conn,
                strategy_trade_id=trade_id,
                order_id=duplicate_entry_order,
                purpose="entry",
            )

    # Instrument equality is not provenance: the pre-existing manual position
    # cannot be claimed because detailed lookup did not return it for this order.
    with pytest.raises(StrategyOwnershipError, match="exact strategy entry order"):
        claim_exact_position(
            conn,
            strategy_trade_id=trade_id,
            entry_order_id=entry_order,
            broker_position_id=manual_position_id,
        )

    record_order_position_execution(conn, order_id=entry_order, broker_position_id=strategy_position_id)
    record_order_position_execution(conn, order_id=entry_order, broker_position_id=second_strategy_position_id)
    # Detailed lookup commonly leads portfolio sync. Exact ownership is
    # claimable before either strategy position enters broker_positions.
    claim_exact_position(
        conn,
        strategy_trade_id=trade_id,
        entry_order_id=entry_order,
        broker_position_id=strategy_position_id,
    )
    # One entry order may produce several positionExecutions. Each exact id is
    # owned independently while the same-instrument manual id remains outside.
    claim_exact_position(
        conn,
        strategy_trade_id=trade_id,
        entry_order_id=entry_order,
        broker_position_id=second_strategy_position_id,
    )

    assert_exact_position_owned(conn, strategy_trade_id=trade_id, broker_position_id=strategy_position_id)
    assert_exact_position_owned(
        conn,
        strategy_trade_id=trade_id,
        broker_position_id=second_strategy_position_id,
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                """
                UPDATE strategy_position_ownership
                SET status = 'released', released_at = now(), release_reason = NULL
                WHERE strategy_trade_id = %s AND broker_position_id = %s
                """,
                (trade_id, second_strategy_position_id),
            )
    with pytest.raises(StrategyOwnershipError, match="not actively owned"):
        assert_exact_position_owned(conn, strategy_trade_id=trade_id, broker_position_id=manual_position_id)

    release_exact_position(
        conn,
        strategy_trade_id=trade_id,
        broker_position_id=strategy_position_id,
        reason="paper position closed",
    )
    with pytest.raises(StrategyOwnershipError, match="not actively owned"):
        assert_exact_position_owned(conn, strategy_trade_id=trade_id, broker_position_id=strategy_position_id)


def test_manual_order_cannot_become_strategy_authority(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    instrument_id = 2454002
    _instrument(conn, instrument_id)
    signal_id = _signal(conn, instrument_id=instrument_id)
    _paper_stage(conn)
    trade_id = _deployment_and_trade(conn, signal_id)
    manual_order = _order(conn, instrument_id=instrument_id, origin="manual")

    with pytest.raises(StrategyControlError, match="manual orders cannot be linked"):
        link_strategy_order(conn, strategy_trade_id=trade_id, order_id=manual_order, purpose="entry")


def test_funding_is_once_only_and_cannot_exceed_operator_cap(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _instrument(conn)
    signal_id = _signal(conn)
    _paper_stage(conn)
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("100"),
        enabled=True,
        changed_by="operator",
        reason="bounded test pot",
    )
    with pytest.raises(StrategyControlError, match="exceeds"):
        decide_funding(
            conn,
            signal_id=signal_id,
            verdict="allocated",
            deployment_id=deployment.deployment_id,
            amount=Decimal("101"),
            reason_code="invalid",
        )
    first = decide_funding(
        conn,
        signal_id=signal_id,
        verdict="rejected",
        reason_code="risk_budget_exhausted",
    )
    assert first > 0
    with pytest.raises(psycopg.errors.UniqueViolation):
        decide_funding(
            conn,
            signal_id=signal_id,
            verdict="rejected",
            reason_code="duplicate",
        )


def test_funding_rechecks_stage_and_aggregate_active_reservations(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    for instrument_id in (2454011, 2454012, 2454013):
        _instrument(conn, instrument_id)
    _paper_stage(conn)
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("100"),
        enabled=True,
        changed_by="operator",
        reason="aggregate reservation test",
    )
    first_signal = _signal(conn, instrument_id=2454011)
    first_decision = decide_funding(
        conn,
        signal_id=first_signal,
        verdict="allocated",
        deployment_id=deployment.deployment_id,
        amount=Decimal("60"),
        reason_code="within_risk_budget",
    )
    first_trade = create_strategy_trade(conn, first_decision)

    second_signal = _signal(conn, instrument_id=2454012)
    with pytest.raises(StrategyControlError, match="exceeds"):
        decide_funding(
            conn,
            signal_id=second_signal,
            verdict="allocated",
            deployment_id=deployment.deployment_id,
            amount=Decimal("41"),
            reason_code="would_exceed_active_reservations",
        )

    # Closed/failed trades release capacity; lifetime allocations do not make
    # an operator's fixed pot unusable forever.
    conn.execute(
        "UPDATE strategy_trades SET status = 'closed' WHERE strategy_trade_id = %s",
        (first_trade,),
    )
    second_decision = decide_funding(
        conn,
        signal_id=second_signal,
        verdict="allocated",
        deployment_id=deployment.deployment_id,
        amount=Decimal("100"),
        reason_code="capacity_released",
    )
    assert second_decision > first_decision

    promote_strategy(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        to_stage="paused",
        promoted_by="operator",
        reason="pause new entries",
    )
    third_signal = _signal(conn, instrument_id=2454013)
    with pytest.raises(StrategyControlError, match="cannot be allocated"):
        decide_funding(
            conn,
            signal_id=third_signal,
            verdict="allocated",
            deployment_id=deployment.deployment_id,
            amount=Decimal("1"),
            reason_code="must_fail_while_paused",
        )


def test_deployment_currency_is_validated_at_the_capital_authority(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2603 item 4: the currency is refused by the service, not by a request model.

    Before this, ``configure_deployment``'s only currency guard was ``_require_text``
    (non-empty), and ``strategy_deployments.currency`` was ``TEXT NOT NULL DEFAULT
    'USD'`` constrained only by ``currency <> ''`` -- the defect
    ``docs/review-prevention-log.md:720`` (#232) already names.  The ``Literal["USD"]``
    fields in ``app/api/strategies.py`` are on RESPONSE views and constrain nothing on
    the write path, so every non-HTTP caller was unguarded.
    """
    conn = ebull_test_conn
    _paper_stage(conn)

    with pytest.raises(StrategyControlError, match="deployment_currency_unsupported") as refusal:
        configure_deployment(
            conn,
            strategy_id="S-OWN",
            strategy_version="v1",
            mode="paper",
            capital_limit=Decimal("1000"),
            enabled=True,
            changed_by="operator",
            reason="FX is unmodelled (#2363)",
            currency=" gbp ",
        )
    # Sloppy input on purpose: the message must name the code the membership test
    # actually saw, so an operator reading it can tell which string was rejected.
    assert "'GBP'" in str(refusal.value)
    assert conn.execute("SELECT count(*) FROM strategy_deployments").fetchone() == (0,)


def test_deployment_currency_is_canonicalised_before_it_is_compared_or_stored(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A supported code in non-canonical form must not read as a currency CHANGE.

    ``is_risk_reducing_deployment_change`` compares the supplied currency to the stored
    one with ``==``.  Without normalising first, an operator-supplied ``"usd"`` against
    a stored ``"USD"`` would make an otherwise risk-reducing edit non-risk-reducing --
    and the returned ``Deployment`` would report a currency the row does not hold.
    """
    conn = ebull_test_conn
    _paper_stage(conn)

    created = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="initial paper pot",
        currency=" usd ",
    )
    assert created.currency == "USD"
    assert conn.execute("SELECT currency FROM strategy_deployments").fetchone() == ("USD",)
    assert conn.execute("SELECT currency FROM strategy_deployment_events").fetchall() == [("USD",)]


@pytest.mark.parametrize(
    ("table", "constraint"),
    [
        ("strategy_deployments", "strategy_deployments_currency_supported"),
        ("strategy_deployment_events", "strategy_deployment_events_currency_supported"),
    ],
)
def test_an_unsupported_currency_is_unrepresentable_at_rest(
    ebull_test_conn: psycopg.Connection[Any],
    table: str,
    constraint: str,
) -> None:
    """sql/338, asserted BY CONSTRAINT NAME.

    Asserting only the exception class passes when some bystander constraint fires --
    the trap recorded on #2634.  Both tables are covered because the event mirror can
    otherwise record a currency its current-state row could never hold.
    """
    conn = ebull_test_conn
    _paper_stage(conn)
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="initial paper pot",
    )
    with pytest.raises(psycopg.errors.CheckViolation) as excinfo:
        with conn.transaction():
            # sql.Identifier, not an f-string: the prevention log's entry on
            # f"...{_METADATA_COLS}..." applies to a parametrized table name too.
            conn.execute(
                psycopg.sql.SQL("UPDATE {} SET currency = 'GBP' WHERE deployment_id = %s").format(
                    psycopg.sql.Identifier(table)
                ),
                (deployment.deployment_id,),
            )
    assert excinfo.value.diag.constraint_name == constraint


class _CountingConn:
    """A connection that records how many statements pass through ``execute``.

    ⚠ Counts the CALLS this code makes, not what the server parses — which is
    exactly the property #2641 is about. A batch that issued one statement per
    result would be invisible to a timing assertion on an idle box and is
    obvious here.
    """

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self._conn = conn
        self.statements = 0

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        self.statements += 1
        return self._conn.execute(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _three_stored_rows(conn: psycopg.Connection[Any]) -> list[int]:
    """Three stored in-sample rows, each its own arm pair so criterion 9 passes.

    Distinct ``input_rule_set_version`` per pair: neither the metrics nor the
    DSR is part of ``ResultIdentity``, so pairs differing only in those hash to
    the same ``result_version`` and collide on ``strategy_results_unique``.
    """
    result_ids: list[int] = []
    for index in range(3):
        version = f"price-quarantine-v1+2641batch{index}"
        masked = _promotable_row(
            strategy_id="S-GOV",
            strategy_version="v1",
            namespace="in_sample",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            input_rule_set_version=version,
        )
        admitted = _promotable_row(
            strategy_id="S-GOV",
            strategy_version="v1",
            namespace="in_sample",
            ambiguity_arm="worst_case",
            quarantine_arm="admitted",
            input_rule_set_version=version,
        )
        masked_id, _ = store_in_sample_arm_pair(conn, masked, admitted)
        _universe_record(conn, masked_id, evaluated=frozenset({1, 2, 3}))
        _ambiguity_record(conn, masked_id)
        store_promotion_evidence(conn, result_id=masked_id, evidence=_passing_promotion_evidence())
        result_ids.append(masked_id)
    return result_ids


def test_pinned_result_reads_do_not_scale_with_the_batch(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2641 — one statement per record type across the WHOLE batch.

    The per-result loop issued five round trips per pinned result, so the read
    cost was N+1 in the batch size. Asserting the count is EQUAL at N=1 and N=3
    pins the property the issue asks for; asserting merely "fewer than before"
    would pass for a batch that still grew.
    """
    conn = ebull_test_conn
    result_ids = _three_stored_rows(conn)

    def _counts(ids: list[int]) -> dict[str, int]:
        counted: dict[str, int] = {}
        for name, call in (
            ("universe", load_result_universes),
            ("ambiguity", load_result_ambiguities),
            ("evidence", load_promotion_evidences),
            ("stored_row", stored_result_promotion_refusals_for),
        ):
            proxy = _CountingConn(conn)
            call(cast(Any, proxy), ids)
            counted[name] = proxy.statements
        return counted

    one = _counts(result_ids[:1])
    three = _counts(result_ids)

    assert one == three, "a read that grows with the batch is the N+1 this ticket removes"
    # The row read is two: the row itself, then criterion 9's arm versions.
    assert three == {"universe": 1, "ambiguity": 1, "evidence": 1, "stored_row": 2}


def test_batched_row_refusals_match_the_per_result_verdict(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Batching changes the statement count and nothing about the verdict.

    Compared against the singular entry point rather than a hard-coded list, so
    the two cannot drift apart without this failing — a fixed expectation would
    keep passing if BOTH sides regressed together.
    """
    conn = ebull_test_conn
    result_ids = _three_stored_rows(conn)

    batched = stored_result_promotion_refusals_for(conn, result_ids)
    for result_id in result_ids:
        assert batched[result_id] == stored_result_promotion_refusals(conn, result_id)


def test_a_missing_row_in_the_batch_raises_rather_than_refusing(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The absent-row contract survives batching.

    ⚠ It raises for the WHOLE batch now, before any result's refusals are
    returned — the reordering named in the function's docstring. `promote_strategy`
    refuses an unknown result_id with its own message long before reaching here,
    so this is the caller-error path, not an operator-visible one.
    """
    conn = ebull_test_conn
    result_ids = _three_stored_rows(conn)

    with pytest.raises(RuntimeError, match="no stored result row for result_id 99999999"):
        stored_result_promotion_refusals_for(conn, [*result_ids, 99999999])
