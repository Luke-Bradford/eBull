"""Phase 5c — ``strategy_results``' constraints, exercised against a real database.

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
    "namespace": "hold_out",
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
}

#: ⚠ A FIXED statement, never an f-string built from the override keys. psycopg
#: types `query` as `LiteralString` precisely to stop dynamic SQL, and the
#: pre-push hook catches the f-string form — correctly.
_INSERT = """
    INSERT INTO strategy_results (
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, window_start, window_end, universe_basis, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version,
        evaluated_instrument_count, trial_count, deflated_sharpe
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
        %(ambiguity_arm)s, %(window_start)s, %(window_end)s, %(universe_basis)s, %(corpus_version)s,
        %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(position_rule_set_version)s,
        %(outcome_rule_set_version)s, %(input_rule_set_version)s,
        %(evaluated_instrument_count)s, %(trial_count)s, %(deflated_sharpe)s
    )
"""

#: ⚠ The same statement MINUS `universe_basis`, for the one case that cannot be
#: expressed as an override: a writer that never mentions the column. NOT NULL
#: with no default is what makes that fail, and a default would make it pass
#: silently with the FAVOURABLE value — #2288 clause 2's whole argument.
_INSERT_WITHOUT_BASIS = """
    INSERT INTO strategy_results (
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, window_start, window_end, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version, evaluated_instrument_count
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
        %(ambiguity_arm)s, %(window_start)s, %(window_end)s, %(corpus_version)s,
        %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(position_rule_set_version)s,
        %(outcome_rule_set_version)s, %(input_rule_set_version)s, %(evaluated_instrument_count)s
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
