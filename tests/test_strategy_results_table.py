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
    "window_start": "1962-01-02",
    "window_end": "2026-07-08",
    "universe_basis": "survivor_only",
    "corpus_version": "paperswithbacktest/Stocks-Daily-Price@2026-07-08",
    "cost_model_id": "static-p75-insession-v1",
    "carry_unmodelled": True,
    "sizing_rule": "equal_weight_concurrent_v1",
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
    # ⚠ NULL is stage 5d's ONLY value — criterion 3's block bootstrap is 5e's —
    # and the promotion gate refuses on it. The accepting test below stores a
    # non-null one too, so the CHECK's positive branch is exercised.
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
}

#: ⚠ A FIXED statement, never an f-string built from the override keys. psycopg
#: types `query` as `LiteralString` precisely to stop dynamic SQL, and the
#: pre-push hook catches the f-string form — correctly.
_INSERT = """
    INSERT INTO strategy_results_store (
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, window_start, window_end, universe_basis, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version,
        evaluated_instrument_count, trial_count, deflated_sharpe,
        expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
        max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count, effective_sample_size,
        return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
        unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
        %(ambiguity_arm)s, %(window_start)s, %(window_end)s, %(universe_basis)s, %(corpus_version)s,
        %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(position_rule_set_version)s,
        %(outcome_rule_set_version)s, %(input_rule_set_version)s,
        %(evaluated_instrument_count)s, %(trial_count)s, %(deflated_sharpe)s,
        %(expectancy_per_trade_pct)s, %(profit_factor)s, %(cagr_pct)s, %(annualised_volatility_pct)s,
        %(sharpe)s, %(sortino)s, %(max_drawdown_pct)s, %(exposure_time_pct)s, %(turnover_annualised)s,
        %(trade_count)s, %(effective_sample_size)s, %(return_vs_buy_and_hold_pct)s, %(losing_trade_count)s,
        %(losing_period_count)s, %(open_trade_count)s, %(unpriced_trade_count)s, %(periods_per_year)s,
        %(total_return_pct)s, %(buy_and_hold_return_pct)s, %(metric_set_id)s
    )
"""

#: ⚠ The same statement MINUS `universe_basis`, for the one case that cannot be
#: expressed as an override: a writer that never mentions the column. NOT NULL
#: with no default is what makes that fail, and a default would make it pass
#: silently with the FAVOURABLE value — #2288 clause 2's whole argument.
_INSERT_WITHOUT_BASIS = """
    INSERT INTO strategy_results_store (
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, window_start, window_end, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version, evaluated_instrument_count,
        expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
        max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count,
        return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
        unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
        %(ambiguity_arm)s, %(window_start)s, %(window_end)s, %(corpus_version)s,
        %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(position_rule_set_version)s,
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


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        # #2288 clause 2 — the label vocabulary is closed. A free-text basis is
        # a basis nobody can gate on.
        ("unknown universe basis", {"universe_basis": "probably_fine"}),
        ("blank universe basis", {"universe_basis": ""}),
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
        ("zero effective sample size", {"effective_sample_size": "0"}),
        # ⚠⚠ THE TWO THAT MAKE A NULL MEAN SOMETHING. Without these CHECKs a
        # null profit factor could equally be "no losing trade" or "nobody
        # computed it", and the second is the state #2288 clause 2 refuses.
        ("null profit factor with losing trades", {"profit_factor": None}),
        ("profit factor with no losing trades", {"losing_trade_count": 0}),
        ("null sortino with losing periods", {"sortino": None}),
        ("sortino with no losing periods", {"losing_period_count": 0}),
    ],
)
def test_results_table_rejects(ebull_test_conn: psycopg.Connection[tuple], label: str, overrides: dict) -> None:
    with pytest.raises(psycopg.errors.Error), ebull_test_conn.transaction():
        _insert(ebull_test_conn, **overrides)


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
        # 5e's shape, once a DSR exists on a declared trial count.
        _insert(
            ebull_test_conn,
            result_version="strategy-result-v1+def456",
            namespace="in_sample",
            trial_count=41,
            deflated_sharpe="0.31",
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
        _insert(
            ebull_test_conn,
            result_version="strategy-result-v1+bbb222",
            effective_sample_size="128.5",
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
