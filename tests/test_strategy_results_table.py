"""Phase 5c — ``strategy_results_store``'s constraints, exercised against a real database.

⚠ ONE integration test file, per the repo's test-tiering rule: the genuinely-new
SQL mechanism is this table's constraint set, and a mocked cursor asserts the
parameters while passing against a constraint that would reject them.

⚠⚠ THE CASE THIS FILE EXISTS FOR is ``test_a_writer_that_omits_the_basis_is_refused``.
#2288 clause 2 is *"fail closed on absence"*, and the only thing that makes an
omitted label fail is ``NOT NULL`` with **no default** — which is a property of
the DDL and of nothing in Python. ``tests/test_strategy_result.py`` cannot see
it: the dataclass requires the field, so the forgetful writer it guards against
is unrepresentable there.
"""

from __future__ import annotations

import psycopg
import pytest

_BASE: dict[str, object] = {
    "strategy_id": "S-TEST",
    "strategy_version": "strategy-registry-v1+abc123",
    "result_version": "strategy-result-v1+abc123",
    "result_scope": "sleeve",
    # ⚠ `in_sample`, and it was `hold_out` until phase 5e-1 (sql/264). Two
    # reasons it had to move, and the first is the dangerous one:
    #
    # 1. The store's trigger now refuses a hold-out row with no `evaluate`
    #    access record. Every `pytest.raises(psycopg.errors.Error)` case below
    #    would still have PASSED — refused by the trigger rather than by the
    #    constraint it names. A parametrised reject test that passes for the
    #    wrong reason is indistinguishable from one that works.
    # 2. The accepting cases would simply have failed.
    #
    # The hold-out path is exercised in tests/test_strategy_holdout_namespace.py,
    # which is where the mechanism lives.
    "namespace": "in_sample",
    "ambiguity_arm": "worst_case",
    "quarantine_arm": "masked",
    "window_start": "1962-01-02",
    "window_end": "2026-07-08",
    "purpose": "capital_candidate",
    "universe_basis": "survivor_only",
    "corpus_version": "paperswithbacktest/Stocks-Daily-Price@2026-07-08",
    "cost_model_id": "static-p75-insession-v1",
    "carry_unmodelled": True,
    "sizing_rule": "equal_weight_concurrent_v1",
    "benchmark_rule": "equal_weight_buy_and_hold_v1",
    "return_basis": "split-dividend-adjusted-wealth-v1",
    "position_rule_set_version": "position-builder-v1+bbb222",
    "outcome_rule_set_version": "outcome-resolver-v1+ccc333",
    "input_rule_set_version": "price-quarantine-v1+ddd444",
    "evaluated_instrument_count": 5266,
    "trial_count": None,
    "deflated_sharpe": None,
    # --- sql/263, criterion 7's metric set ---------------------------------
    "expectancy_per_trade_pct": "0.51",
    "profit_factor": "1.18",
    "cagr_pct": "3.90",
    "annualised_volatility_pct": "14.20",
    "sharpe": "0.27",
    "sortino": "0.39",
    "max_drawdown_pct": "-31.40",
    "exposure_time_pct": "62.10",
    "turnover_annualised": "3.05",
    "trade_count": 3135355,
    # ⚠ NULL, and at stage 5e-2 that is now the "caller declared no bootstrap
    # seed" case rather than "nothing can compute it". The promotion gate still
    # refuses on it. ⚠⚠ It is one of NINE columns bound together by
    # `strategy_results_bootstrap_all_or_nothing` (sql/265), so it cannot be
    # made non-null on its own — see `_BOOTSTRAP` below.
    "effective_sample_size": None,
    "return_vs_buy_and_hold_pct": "-2.40",
    "losing_trade_count": 1500000,
    "losing_period_count": 7100,
    "open_trade_count": 4200,
    "unpriced_trade_count": 0,
    "periods_per_year": "251.67",
    "total_return_pct": "418.00",
    "buy_and_hold_return_pct": "420.40",
    "metric_set_id": "criterion7-v1",
    # --- sql/265, criterion 3's block bootstrap ----------------------------
    # ⚠ All NULL together: `num_nulls(...) IN (0, 9)` admits the wholly-absent
    # set, which is what a result carrying no criterion-3 measurement looks like.
    "expectancy_ci_low_pct": None,
    "expectancy_ci_high_pct": None,
    "bootstrap_block_length": None,
    "bootstrap_cluster_count": None,
    "bootstrap_resamples": None,
    "bootstrap_seed": None,
    "bootstrap_design_effect": None,
    "bootstrap_model_id": None,
    # --- sql/266, criterion 6's Deflated Sharpe ---------------------------
    # ⚠ All NULL together: `num_nulls(...) IN (0, 11)` admits the wholly-absent
    # set. ⚠⚠ `trial_count` is NOT in that set — it is governed by the one-way
    # dependency `strategy_results_dsr_needs_trial_count`, because a declared
    # count with no DSR yet is a real state while the reverse is what criterion
    # 6 forbids.
    "dsr_trade_sharpe": None,
    "dsr_skewness": None,
    "dsr_kurtosis": None,
    "dsr_expected_max_sharpe": None,
    "dsr_independent_trials": None,
    "dsr_average_trial_correlation": None,
    "dsr_trial_sharpe_variance": None,
    "dsr_measured_trials": None,
    "dsr_model_id": None,
    "trial_register_version": None,
    # --- sql/268, §9's random-entry synthetic control ----------------------
    # ⚠ All NULL together, which is the "§9's control has not been run" state
    # the promotion gate refuses on (`synthetic_control_not_run`).
    "synthetic_control_model_id": None,
    "synthetic_control_size": None,
    "synthetic_control_root_seed": None,
    "synthetic_control_mean_return_pct": None,
    "synthetic_control_mean_return_ci_low_pct": None,
    "synthetic_control_mean_return_ci_high_pct": None,
    "synthetic_control_sharpe_percentile": None,
    "synthetic_control_sharpe_threshold": None,
    "synthetic_control_return_threshold_pct": None,
    "synthetic_control_passed": None,
}

#: A COMPLETE §9 control that this row's own `sharpe` (0.27) CLEARS: the cohort
#: mean straddles zero and the Sharpe threshold sits below 0.27, so
#: `synthetic_control_passed` must be true and the derived-verdict CHECK agrees.
_SYNTH: dict[str, object] = {
    "synthetic_control_model_id": "permuted-entry-uniform-gap-v1",
    "synthetic_control_size": 1000,
    "synthetic_control_root_seed": 20260808,
    "synthetic_control_mean_return_pct": "0.04",
    "synthetic_control_mean_return_ci_low_pct": "-0.31",
    "synthetic_control_mean_return_ci_high_pct": "0.42",
    "synthetic_control_sharpe_percentile": "95.0",
    "synthetic_control_sharpe_threshold": "0.11",
    "synthetic_control_return_threshold_pct": "6.20",
    "synthetic_control_passed": True,
}

#: A COMPLETE criterion-6 block, on a declared trial count. ⚠ Includes
#: `trial_count` because `strategy_results_dsr_needs_trial_count` requires one
#: wherever a `deflated_sharpe` is present.
_DSR: dict[str, object] = {
    "trial_count": 11,
    "deflated_sharpe": "0.7179",
    "dsr_trade_sharpe": "0.017",
    "dsr_skewness": "-0.40",
    "dsr_kurtosis": "8.00",
    "dsr_expected_max_sharpe": "0.015208",
    "dsr_independent_trials": "9.0",
    "dsr_average_trial_correlation": "0.20",
    "dsr_trial_sharpe_variance": "0.0001",
    "dsr_measured_trials": 2,
    "dsr_model_id": "c6-deflated-sharpe-v1",
    "trial_register_version": "trial-register-2026-08-07",
}

#: A COMPLETE criterion-3 block, for the cases that must start from a valid one
#: and break exactly one thing. ⚠ Applied on top of `_BASE`, which is all-null.
_BOOTSTRAP: dict[str, object] = {
    "effective_sample_size": "144750.0",
    "expectancy_ci_low_pct": "-0.4801",
    "expectancy_ci_high_pct": "-0.4031",
    "bootstrap_block_length": 14,
    "bootstrap_cluster_count": 15577,
    "bootstrap_resamples": 2000,
    "bootstrap_seed": 20260807,
    "bootstrap_design_effect": "21.66",
    "bootstrap_model_id": "c3-block-bootstrap-v1",
}

#: ⚠ A FIXED statement, never an f-string built from the override keys. psycopg
#: types `query` as `LiteralString` precisely to stop dynamic SQL, and the
#: pre-push hook catches the f-string form — correctly.
_INSERT = """
    INSERT INTO strategy_results_store (
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, quarantine_arm, window_start, window_end, purpose, universe_basis, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, benchmark_rule, return_basis, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version,
        evaluated_instrument_count, trial_count, deflated_sharpe,
        expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
        max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count, effective_sample_size,
        return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
        unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id,
        expectancy_ci_low_pct, expectancy_ci_high_pct, bootstrap_block_length, bootstrap_cluster_count,
        bootstrap_resamples, bootstrap_seed, bootstrap_design_effect, bootstrap_model_id,
        dsr_trade_sharpe, dsr_skewness, dsr_kurtosis, dsr_expected_max_sharpe,
        dsr_independent_trials, dsr_average_trial_correlation, dsr_trial_sharpe_variance,
        dsr_measured_trials, dsr_model_id, trial_register_version,
        synthetic_control_model_id, synthetic_control_size, synthetic_control_root_seed,
        synthetic_control_mean_return_pct, synthetic_control_mean_return_ci_low_pct,
        synthetic_control_mean_return_ci_high_pct, synthetic_control_sharpe_percentile,
        synthetic_control_sharpe_threshold, synthetic_control_return_threshold_pct,
        synthetic_control_passed
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
        %(ambiguity_arm)s, %(quarantine_arm)s, %(window_start)s, %(window_end)s, %(purpose)s,
        %(universe_basis)s, %(corpus_version)s,
        %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(benchmark_rule)s, %(return_basis)s,
        %(position_rule_set_version)s,
        %(outcome_rule_set_version)s, %(input_rule_set_version)s,
        %(evaluated_instrument_count)s, %(trial_count)s, %(deflated_sharpe)s,
        %(expectancy_per_trade_pct)s, %(profit_factor)s, %(cagr_pct)s, %(annualised_volatility_pct)s,
        %(sharpe)s, %(sortino)s, %(max_drawdown_pct)s, %(exposure_time_pct)s, %(turnover_annualised)s,
        %(trade_count)s, %(effective_sample_size)s, %(return_vs_buy_and_hold_pct)s, %(losing_trade_count)s,
        %(losing_period_count)s, %(open_trade_count)s, %(unpriced_trade_count)s, %(periods_per_year)s,
        %(total_return_pct)s, %(buy_and_hold_return_pct)s, %(metric_set_id)s,
        %(expectancy_ci_low_pct)s, %(expectancy_ci_high_pct)s, %(bootstrap_block_length)s,
        %(bootstrap_cluster_count)s, %(bootstrap_resamples)s, %(bootstrap_seed)s,
        %(bootstrap_design_effect)s, %(bootstrap_model_id)s,
        %(dsr_trade_sharpe)s, %(dsr_skewness)s, %(dsr_kurtosis)s, %(dsr_expected_max_sharpe)s,
        %(dsr_independent_trials)s, %(dsr_average_trial_correlation)s, %(dsr_trial_sharpe_variance)s,
        %(dsr_measured_trials)s, %(dsr_model_id)s, %(trial_register_version)s,
        %(synthetic_control_model_id)s, %(synthetic_control_size)s, %(synthetic_control_root_seed)s,
        %(synthetic_control_mean_return_pct)s, %(synthetic_control_mean_return_ci_low_pct)s,
        %(synthetic_control_mean_return_ci_high_pct)s, %(synthetic_control_sharpe_percentile)s,
        %(synthetic_control_sharpe_threshold)s, %(synthetic_control_return_threshold_pct)s,
        %(synthetic_control_passed)s
    )
"""

#: ⚠ The same statement MINUS `universe_basis`, for the one case that cannot be
#: expressed as an override: a writer that never mentions the column. NOT NULL
#: with no default is what makes that fail, and a default would make it pass
#: silently with the FAVOURABLE value — #2288 clause 2's whole argument.
_INSERT_WITHOUT_BASIS = """
    INSERT INTO strategy_results_store (
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, quarantine_arm, window_start, window_end, purpose, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, benchmark_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version, evaluated_instrument_count,
        expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
        max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count,
        return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
        unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
        %(ambiguity_arm)s, %(quarantine_arm)s, %(window_start)s, %(window_end)s, %(purpose)s, %(corpus_version)s,
        %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(benchmark_rule)s, %(position_rule_set_version)s,
        %(outcome_rule_set_version)s, %(input_rule_set_version)s, %(evaluated_instrument_count)s,
        %(expectancy_per_trade_pct)s, %(profit_factor)s, %(cagr_pct)s, %(annualised_volatility_pct)s,
        %(sharpe)s, %(sortino)s, %(max_drawdown_pct)s, %(exposure_time_pct)s, %(turnover_annualised)s,
        %(trade_count)s, %(return_vs_buy_and_hold_pct)s, %(losing_trade_count)s,
        %(losing_period_count)s, %(open_trade_count)s, %(unpriced_trade_count)s, %(periods_per_year)s,
        %(total_return_pct)s, %(buy_and_hold_return_pct)s, %(metric_set_id)s
    )
"""


def _insert(conn: psycopg.Connection[tuple], **overrides: object) -> None:
    row = {**_BASE}
    row.update(overrides)
    conn.execute(_INSERT, row)


def test_result_purpose_cannot_be_relabelled_after_write(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, purpose="harness_validation")

    with pytest.raises(psycopg.errors.IntegrityConstraintViolation), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "UPDATE strategy_results_store SET purpose='capital_candidate' WHERE strategy_id='S-TEST'"
        )

    assert ebull_test_conn.execute(
        "SELECT purpose FROM strategy_results_store WHERE strategy_id='S-TEST'"
    ).fetchone() == ("harness_validation",)


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        # #2288 clause 2 — the label vocabulary is closed. A free-text basis is
        # a basis nobody can gate on.
        ("unknown universe basis", {"universe_basis": "probably_fine"}),
        ("blank universe basis", {"universe_basis": ""}),
        ("unknown strategy purpose", {"purpose": "probably_profitable"}),
        # ⚠ `purged` is §5.2's verdict for a signal that contributes to NO
        # result. Admitting it as a namespace would give it one.
        ("purged as a namespace", {"namespace": "purged"}),
        ("unknown namespace", {"namespace": "validation"}),
        # §5.4: drawdown and Sharpe do not compose at signal level, so there is
        # no signal-scoped result row.
        ("signal as a scope", {"result_scope": "signal"}),
        ("unknown scope", {"result_scope": "everything"}),
        # §3.4's pair is exactly two arms. "Conservative" is the treatment the
        # spec rejects by name.
        ("unknown ambiguity arm", {"ambiguity_arm": "conservative"}),
        # sql/267 — criterion 9's pair is closed in SQL as well as in Python.
        ("unknown quarantine arm", {"quarantine_arm": "conservative"}),
        ("backwards window", {"window_start": "2026-07-08", "window_end": "1962-01-02"}),
        ("negative instrument count", {"evaluated_instrument_count": -1}),
        # Criterion 6 counts abandoned branches and discarded parameter values,
        # so zero trials is a writer that meant NULL.
        ("zero trial count", {"trial_count": 0}),
        ("negative trial count", {"trial_count": -3}),
        # ⚠ Every one of these is a state NOT NULL alone admits: the column is
        # PRESENT and identifies nothing — the #2286 shape.
        ("blank strategy_id", {"strategy_id": ""}),
        ("blank strategy_version", {"strategy_version": ""}),
        ("blank result_version", {"result_version": ""}),
        ("blank corpus_version", {"corpus_version": ""}),
        ("blank cost_model_id", {"cost_model_id": ""}),
        ("blank sizing_rule", {"sizing_rule": ""}),
        ("blank benchmark_rule", {"benchmark_rule": ""}),
        ("blank return_basis", {"return_basis": ""}),
        ("unknown return_basis", {"return_basis": "adjusted-sometimes"}),
        ("blank position_rule_set_version", {"position_rule_set_version": ""}),
        ("blank outcome_rule_set_version", {"outcome_rule_set_version": ""}),
        ("blank input_rule_set_version", {"input_rule_set_version": ""}),
        ("blank metric_set_id", {"metric_set_id": ""}),
        # --- sql/263 ------------------------------------------------------
        # ⚠ A drawdown is a fall from a running peak. A POSITIVE one is a sign
        # flip, and a sign flip on this column reads as a good result.
        ("positive drawdown", {"max_drawdown_pct": "4.2"}),
        ("exposure above 100%", {"exposure_time_pct": "100.01"}),
        ("negative exposure", {"exposure_time_pct": "-0.01"}),
        ("negative trade count", {"trade_count": -1}),
        ("more losers than trades", {"trade_count": 10, "losing_trade_count": 11}),
        ("negative open count", {"open_trade_count": -1}),
        ("zero annualisation", {"periods_per_year": "0"}),
        # ⚠ The WHOLE bootstrap set, with only the sample size zeroed. Setting it
        # alone would now trip sql/265's all-or-nothing constraint instead of
        # sql/263's positivity CHECK — the case would still be "refused" and the
        # test would still pass, for a reason its label does not name.
        ("zero effective sample size", {**_BOOTSTRAP, "effective_sample_size": "0"}),
        # ⚠⚠ THE TWO THAT MAKE A NULL MEAN SOMETHING. Without these CHECKs a
        # null profit factor could equally be "no losing trade" or "nobody
        # computed it", and the second is the state #2288 clause 2 refuses.
        ("null profit factor with losing trades", {"profit_factor": None}),
        ("profit factor with no losing trades", {"losing_trade_count": 0}),
        ("null sortino with losing periods", {"sortino": None}),
        ("sortino with no losing periods", {"losing_period_count": 0}),
        # --- sql/266, criterion 6 -----------------------------------------
        # ⚠⚠ THE ONE THE CRITERION IS ABOUT: a Deflated Sharpe with no declared
        # trial count. "An undeclared trial count fails; it does not default to
        # the number of shipped strategies."
        ("a DSR with no declared trial count", {**_DSR, "trial_count": None}),
        # ⚠ Each of these starts from a COMPLETE set and removes exactly one
        # input, so the all-or-nothing constraint is what fires — a partial DSR
        # is a correction whose correction cannot be judged.
        ("a DSR with no trial variance", {**_DSR, "dsr_trial_sharpe_variance": None}),
        ("a DSR with no threshold", {**_DSR, "dsr_expected_max_sharpe": None}),
        ("a DSR naming no register", {**_DSR, "trial_register_version": None}),
        # A blank id is PRESENT and meaningless — it satisfies all-or-nothing
        # while naming no construction at all (#2286).
        ("a blank model id", {**_DSR, "dsr_model_id": ""}),
        ("a blank register version", {**_DSR, "trial_register_version": ""}),
        # The DSR is a probability: equation (2) is a Normal CDF.
        ("a DSR above one", {**_DSR, "deflated_sharpe": "1.4"}),
        ("a negative DSR", {**_DSR, "deflated_sharpe": "-0.1"}),
        # Appendix A.3: N_hat interpolates between 1 and M, so it can be neither
        # below 1 nor above the declared count.
        ("more independent trials than declared", {**_DSR, "dsr_independent_trials": "12.0"}),
        ("independent trials at one", {**_DSR, "dsr_independent_trials": "1.0"}),
        # rho is bounded by -1/(M-1) for a positive-definite matrix, which at
        # M=11 is -0.1 — TIGHTER than -1, and the tighter bound is the one that
        # catches a matrix that was never a correlation matrix.
        ("a correlation below the positive-definite bound", {**_DSR, "dsr_average_trial_correlation": "-0.5"}),
        ("a correlation above one", {**_DSR, "dsr_average_trial_correlation": "1.2"}),
        # V[{SR_n}] is a variance and equation (1) takes its square root.
        ("zero trial variance", {**_DSR, "dsr_trial_sharpe_variance": "0"}),
        # A sample variance needs two measured trials, and a measured trial
        # missing from the register is a trial missing from M.
        ("one measured trial", {**_DSR, "dsr_measured_trials": 1}),
        ("more measured trials than declared", {**_DSR, "dsr_measured_trials": 12}),
        # ⚠ RAW kurtosis: a Normal is 3, so 0 means excess kurtosis was stored
        # under a column every reader will take as raw.
        ("excess kurtosis stored as raw", {**_DSR, "dsr_kurtosis": "0"}),
        # ⚠⚠ THE BOUND IS 1, NOT 0. `y4 >= y3^2 + 1` for any real distribution,
        # so (0, 1) is impossible — and a `> 0` CHECK admitted all of it while
        # the column comment said otherwise. A near-Normal series passed as
        # EXCESS kurtosis lands in exactly this range.
        ("a kurtosis between zero and one", {**_DSR, "dsr_kurtosis": "0.4"}),
        # M >= 2 wherever a DSR exists — A.3 needs M > 1 for an average
        # correlation to exist at all. ⚠ Stricter than sql/262's `>= 1`, which
        # still governs a trial count standing on its own.
        ("a DSR on a single declared trial", {**_DSR, "trial_count": 1, "dsr_independent_trials": "1.0"}),
        # --- sql/268, §9's synthetic control -------------------------------
        # ⚠ Each starts from a COMPLETE control and removes exactly one field,
        # so the all-or-nothing constraint fires — a control missing its size or
        # its seed is a null distribution nobody can reproduce.
        ("a control with no declared size", {**_SYNTH, "synthetic_control_size": None}),
        ("a control with no recorded seed", {**_SYNTH, "synthetic_control_root_seed": None}),
        ("a control with no verdict", {**_SYNTH, "synthetic_control_passed": None}),
        # PRESENT and naming no construction — the #2286 shape again.
        ("a blank cohort model id", {**_SYNTH, "synthetic_control_model_id": "  "}),
        ("an empty cohort", {**_SYNTH, "synthetic_control_size": 0}),
        # §9 reads a PERCENTILE off the cohort; 0 and 100 are its endpoints and
        # neither is an order statistic the acceptance describes.
        ("a percentile at one hundred", {**_SYNTH, "synthetic_control_sharpe_percentile": "100"}),
        ("a percentile at zero", {**_SYNTH, "synthetic_control_sharpe_percentile": "0"}),
        (
            "an inverted cohort interval",
            {
                **_SYNTH,
                "synthetic_control_mean_return_ci_low_pct": "0.42",
                "synthetic_control_mean_return_ci_high_pct": "-0.31",
            },
        ),
        # ⚠⚠ THE VERDICT CONTRADICTING ITS OWN INPUTS, which is the state an
        # operator reading the row has no way to detect. Both directions.
        ("a pass whose Sharpe is below the cohort", {**_SYNTH, "synthetic_control_sharpe_threshold": "0.99"}),
        (
            "a pass whose cohort interval excludes zero",
            {
                **_SYNTH,
                "synthetic_control_mean_return_ci_low_pct": "0.11",
            },
        ),
        ("a fail whose inputs both hold", {**_SYNTH, "synthetic_control_passed": False}),
    ],
)
def test_results_table_rejects(ebull_test_conn: psycopg.Connection[tuple], label: str, overrides: dict) -> None:
    with pytest.raises(psycopg.errors.Error), ebull_test_conn.transaction():
        _insert(ebull_test_conn, **overrides)


def test_a_single_declared_trial_is_a_check_violation_not_a_division_by_zero(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ "ANY EXCEPTION" IS WHY THIS WENT UNNOTICED.

    ``strategy_results_dsr_correlation_bounded`` divides by ``trial_count - 1``
    for A.3's positive-definite bound, and Postgres does NOT guarantee the order
    CHECK constraints evaluate in. At ``trial_count = 1`` the division could run
    before ``strategy_results_dsr_trials_above_one`` rejects the row, raising
    ``DivisionByZero`` (SQLSTATE 22012, measured) instead of a check violation —
    the row is refused either way, but the writer sees an arithmetic error
    naming nothing.

    The parametrised reject-case above accepts ``psycopg.errors.Error``, so it
    passes for BOTH outcomes and cannot tell them apart. This pins the class.
    """
    with pytest.raises(psycopg.errors.CheckViolation), ebull_test_conn.transaction():
        _insert(ebull_test_conn, **{**_DSR, "trial_count": 1, "dsr_independent_trials": "1.0"})


def test_a_writer_that_omits_the_basis_is_refused(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """#2288 clause 2, and the reason this migration ships before its writer.

    The column carries no default ON PURPOSE, so the failure mode is a loud
    ``NOT NULL`` rather than a row silently claiming a basis nobody established.
    Not expressible as an override — it needs a statement that never names the
    column, which is exactly what a forgetful 5d writer would emit.
    """
    with pytest.raises(psycopg.errors.NotNullViolation), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            _INSERT_WITHOUT_BASIS,
            {k: v for k, v in _BASE.items() if k not in {"universe_basis", "trial_count", "deflated_sharpe"}},
        )


def test_results_table_accepts_the_valid_shapes(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    with ebull_test_conn.transaction():
        # Today's shape: survivor-only, carry unmodelled, no DSR yet.
        _insert(ebull_test_conn)
        # A declared trial count with NO DSR yet — the register exists, the
        # evaluation has not run. ⚠ Legal on purpose: the one-way dependency
        # forbids the reverse only, and `check_promotable` has a live refusal
        # (`deflated_sharpe_not_computed`) that describes exactly this row.
        _insert(ebull_test_conn, result_version="strategy-result-v1+eee555", trial_count=11)
        # 5e-3's shape: a DSR on a declared trial count, carrying every input.
        _insert(
            ebull_test_conn,
            result_version="strategy-result-v1+def456",
            namespace="in_sample",
            **_DSR,
        )
        # A same-day window is legal — `window_end >= window_start`, not `>`.
        _insert(
            ebull_test_conn,
            result_version="strategy-result-v1+aaa111",
            window_start="2026-07-08",
            window_end="2026-07-08",
        )
        # sql/263's positive branches: an effective sample size once 5e computes
        # one, and the two "denominator was empty" shapes, which are REAL states
        # and must store rather than being refused as missing measurements.
        # ⚠ At stage 5e-2 the sample size can no longer be set ALONE — sql/265
        # binds it to the other eight block-bootstrap columns — so this arm
        # carries the whole set.
        _insert(
            ebull_test_conn,
            result_version="strategy-result-v1+bbb222",
            **{**_BOOTSTRAP, "effective_sample_size": "128.5"},
        )
        _insert(
            ebull_test_conn,
            result_version="strategy-result-v1+ccc333",
            profit_factor=None,
            losing_trade_count=0,
            sortino=None,
            losing_period_count=0,
        )
        # A flat curve has no drawdown at all — 0 is on the allowed side of
        # `<= 0`, and rejecting it would refuse an unstarted sleeve.
        _insert(ebull_test_conn, result_version="strategy-result-v1+ddd444", max_drawdown_pct="0")


def test_the_two_ambiguity_arms_coexist_and_the_same_arm_collides(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The uniqueness key, and §3.4's reason for it.

    The worst-case and best-case arms are two RESULTS of the same strategy
    version, not two views of one — so they must both store. They differ only
    inside ``result_version``, which hashes the arm, and that is what keeps them
    apart on a key that does not mention it.
    """
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, ambiguity_arm="worst_case", result_version="strategy-result-v1+worst1")

    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, ambiguity_arm="best_case", result_version="strategy-result-v1+best01")

    # ⚠ The discriminator: the same result_version twice IS a collision, which
    # is what stops a re-run silently doubling a strategy's evidence.
    with pytest.raises(psycopg.errors.UniqueViolation), ebull_test_conn.transaction():
        _insert(ebull_test_conn, ambiguity_arm="worst_case", result_version="strategy-result-v1+worst1")

    # ...and a different strategy version is a distinct result on the same hash.
    with ebull_test_conn.transaction():
        _insert(
            ebull_test_conn,
            strategy_version="strategy-registry-v1+moved9",
            result_version="strategy-result-v1+worst1",
        )


# ---------------------------------------------------------------------------
# sql/265 — criterion 3's block bootstrap.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        # ⚠⚠ THE ALL-OR-NOTHING SET. Each of these is a row carrying a corrected
        # sample size whose correction cannot be judged, or an interval with no
        # sample size behind it. Criterion 3 asks for both.
        ("sample size with no interval", {"expectancy_ci_low_pct": None, "expectancy_ci_high_pct": None}),
        ("interval with no sample size", {"effective_sample_size": None}),
        ("no block length", {"bootstrap_block_length": None}),
        ("no cluster count", {"bootstrap_cluster_count": None}),
        ("no resample count", {"bootstrap_resamples": None}),
        ("no seed", {"bootstrap_seed": None}),
        ("no design effect", {"bootstrap_design_effect": None}),
        ("no model id", {"bootstrap_model_id": None}),
        # An inverted interval is a swapped write, not a wide interval.
        ("inverted interval", {"expectancy_ci_low_pct": "1.0", "expectancy_ci_high_pct": "-1.0"}),
        # ⚠ A block longer than the axis it was measured on means the length
        # came from somewhere other than that axis.
        ("block longer than the axis", {"bootstrap_block_length": 15578}),
        ("zero block length", {"bootstrap_block_length": 0}),
        # ⚠ Efron & Tibshirani's floor for INTERVAL estimation. Below it the
        # interval's ends are noise.
        ("resamples below the interval floor", {"bootstrap_resamples": 999}),
        ("zero design effect", {"bootstrap_design_effect": "0"}),
        ("negative design effect", {"bootstrap_design_effect": "-1.5"}),
        # ⚠ PRESENT and naming no construction — the #2286 blank-value shape,
        # which would satisfy the all-or-nothing count while meaning nothing.
        ("blank model id", {"bootstrap_model_id": ""}),
    ],
)
def test_the_block_bootstrap_block_rejects(
    ebull_test_conn: psycopg.Connection[tuple], label: str, overrides: dict
) -> None:
    with pytest.raises(psycopg.errors.Error), ebull_test_conn.transaction():
        _insert(ebull_test_conn, **{**_BOOTSTRAP, **overrides})


def test_a_complete_block_bootstrap_set_is_accepted(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ The positive branch. Without it every rejection above could be passing
    because the whole `_BOOTSTRAP` shape is unwritable for some other reason —
    a parametrised reject suite that passes for the wrong reason is
    indistinguishable from one that works (this file's own opening warning)."""
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, **_BOOTSTRAP)
        stored = ebull_test_conn.execute(
            "SELECT bootstrap_model_id, bootstrap_block_length FROM strategy_results_store WHERE strategy_id = %s",
            ("S-TEST",),
        ).fetchone()
    assert stored == ("c3-block-bootstrap-v1", 14)


def test_an_effective_sample_size_above_the_trade_count_is_permitted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ NOT A CONSTRAINT, AND DELIBERATELY SO.

    A design effect below 1 (negatively autocorrelated clusters) puts the
    effective sample size above the nominal trade count. That is a real
    measurement, and clipping it to the nominal count would hide the one case
    where the overlap correction says the opposite of what criterion 3 expects.
    `bootstrap_design_effect` is stored precisely so a reader can see which
    direction it went.
    """
    with ebull_test_conn.transaction():
        _insert(
            ebull_test_conn,
            **{**_BOOTSTRAP, "effective_sample_size": "4000000.0", "bootstrap_design_effect": "0.78"},
        )


def test_a_complete_synthetic_control_is_accepted(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ The positive branch for sql/268, and it is not optional. Every rejection
    above could be passing because the whole ``_SYNTH`` shape is unwritable for
    some other reason — a parametrised reject suite that passes for the wrong
    reason is indistinguishable from one that works."""
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, **_SYNTH)
        stored = ebull_test_conn.execute(
            "SELECT synthetic_control_model_id, synthetic_control_size, synthetic_control_passed "
            "FROM strategy_results_store WHERE strategy_id = %s",
            ("S-TEST",),
        ).fetchone()
    assert stored == ("permuted-entry-uniform-gap-v1", 1000, True)


def test_a_failing_control_is_storable_because_a_failure_is_a_result(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ §10: *"the most likely outcome of stage 5e … is that some or all of
    them fail the random-cohort threshold. That is a result, not a failure of
    the phase."* A table that could only hold passes would make the graveyard
    §9 open question 4 asks for unrepresentable — and would quietly turn the
    acceptance into a filter on what gets recorded."""
    with ebull_test_conn.transaction():
        _insert(
            ebull_test_conn,
            **{**_SYNTH, "synthetic_control_sharpe_threshold": "0.99", "synthetic_control_passed": False},
        )
        stored = ebull_test_conn.execute(
            "SELECT synthetic_control_passed FROM strategy_results_store WHERE strategy_id = %s",
            ("S-TEST",),
        ).fetchone()
    assert stored == (False,)
