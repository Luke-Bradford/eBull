"""Phase 5e-1 — criterion 5's namespace, exercised against a real database.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §8 (stage 5e),
acceptance C5. DDL: ``sql/264``. Writer: ``app/services/result_ledger.py``.

⚠⚠ EVERY MECHANISM HERE IS A PROPERTY OF A RELATION, NOT OF PYTHON, which is
why this file is DB-tier and why a mocked cursor cannot stand in for it:

- the VIEW's ``WHERE namespace = 'in_sample'`` — an exploratory ``select *``
  cannot return a withheld row;
- the VIEW's cascaded CHECK OPTION — the in-sample writer cannot smuggle one;
- the STORE's trigger — an unrecorded hold-out evaluation cannot be inserted,
  by anyone, including the superuser this app connects as.

⚠ The last point is the reason RLS is absent. Measured 2026-08-07 on this
database: ``FORCE ROW LEVEL SECURITY`` plus a ``USING (ns = 'in_sample')``
policy returned BOTH rows, because ``current_user`` is ``postgres`` with
``rolsuper`` and ``rolbypassrls`` set. ``test_row_level_security_would_not_have
_bound_this_connection`` re-runs that measurement so the design note cannot go
stale silently — it is the evidence for a decision, not a test of our code.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.result_ledger import (
    HoldoutAccess,
    holdout_access_counts,
    read_holdout_results,
    record_holdout_access,
    store_holdout_result,
    store_in_sample_result,
)
from app.services.strategy_result import PromotionCandidate, check_promotable
from tests.test_result_ledger import BOOTSTRAP_BLOCK, build_metrics, build_result

_ACTOR = "tests/test_strategy_holdout_namespace.py"
_PURPOSE = "stage 5e-1 acceptance"


def _raw_holdout_insert(conn: psycopg.Connection[tuple], table: str) -> None:
    """A hold-out row written by something that is not ``result_ledger``.

    ⚠ Two fixed statements rather than one interpolated, for the reason psycopg
    types ``query`` as ``LiteralString``: the chokepoint lint refuses dynamic
    SQL and it is right to.
    """
    params = {
        "strategy_id": "S-RAW",
        "strategy_version": "strategy-registry-v1+raw000",
        "result_version": "strategy-result-v1+raw000",
    }
    if table == "store":
        conn.execute(_RAW_INSERT_STORE, params)
    else:
        conn.execute(_RAW_INSERT_VIEW, params)


_RAW_TAIL = """
        'sleeve', 'hold_out', 'worst_case', '1962-01-02', '2026-07-08', 'survivor_only',
        'paperswithbacktest/Stocks-Daily-Price@2026-07-08', 'static-p75-insession-v1', true,
        'equal_weight_concurrent_v1', 'p1', 'o1', 'i1', 10,
        0.5, 1.18, 3.9, 14.2, 0.27, 0.39, -31.4, 62.1, 3.05, 100, -2.4, 50, 20, 4, 0, 251.67, 418.0, 420.4,
        'criterion7-v1'
"""

_RAW_COLUMNS = """
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, window_start, window_end, universe_basis, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version, evaluated_instrument_count,
        expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
        max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count,
        return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
        unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id
"""

_RAW_INSERT_STORE = f"""
    INSERT INTO strategy_results_store ({_RAW_COLUMNS})
    VALUES (%(strategy_id)s, %(strategy_version)s, %(result_version)s, {_RAW_TAIL})
"""  # noqa: S608 - both fragments are module-level literals

_RAW_INSERT_VIEW = f"""
    INSERT INTO strategy_results ({_RAW_COLUMNS})
    VALUES (%(strategy_id)s, %(strategy_version)s, %(result_version)s, {_RAW_TAIL})
"""  # noqa: S608 - as above


# ---------------------------------------------------------------------------
# The mechanism
# ---------------------------------------------------------------------------


def test_the_view_exposes_every_stored_column(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠⚠ THE DRIFT GUARD, and the reason it is a test and not a comment.

    ``sql/264`` defines the view as ``SELECT *``, which Postgres EXPANDS at
    creation time. A later migration adding a result column to the store does
    NOT add it to the view, and nothing else in the system would say so — the
    column would simply be missing from every in-sample read, silently.
    """
    store = ebull_test_conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'strategy_results_store' ORDER BY ordinal_position"
    ).fetchall()
    view = ebull_test_conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'strategy_results' ORDER BY ordinal_position"
    ).fetchall()
    assert store, "strategy_results_store is missing — sql/264 renamed it, it was not dropped"
    assert [c for (c,) in view] == [c for (c,) in store]


def test_strategy_results_is_a_view_and_the_store_is_a_table(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The shape C5 rests on. If the obvious name is ever a table again, the
    filter is gone and every test below would still pass on an empty database."""
    kinds = dict(
        ebull_test_conn.execute(
            "SELECT relname, relkind FROM pg_class WHERE relname IN ('strategy_results', 'strategy_results_store')"
        ).fetchall()
    )
    assert kinds == {"strategy_results": "v", "strategy_results_store": "r"}


def test_an_exploratory_select_cannot_see_a_hold_out_result(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ CRITERION 5's SENTENCE, as an assertion.

    *"The hold-out is a separate result namespace that is mechanically
    inaccessible to exploratory queries."* The exploratory query is
    ``select * from strategy_results``, and after a hold-out result is stored it
    still returns nothing.
    """
    with ebull_test_conn.transaction():
        result = build_result(strategy_id="S-HIDDEN")
        store_holdout_result(ebull_test_conn, result, accessed_by=_ACTOR, purpose=_PURPOSE)

        visible = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_results WHERE strategy_id = 'S-HIDDEN'"
        ).fetchone()
        stored = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_results_store WHERE strategy_id = 'S-HIDDEN'"
        ).fetchone()
        assert visible is not None and stored is not None
        assert (visible[0], stored[0]) == (0, 1)


def test_an_in_sample_result_is_visible_under_the_obvious_name(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The complement — the filter must not hide everything, which is the way a
    namespace test passes for the wrong reason."""
    with ebull_test_conn.transaction():
        store_in_sample_result(ebull_test_conn, build_result(strategy_id="S-SHOWN", namespace="in_sample"))
        visible = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_results WHERE strategy_id = 'S-SHOWN'"
        ).fetchone()
        assert visible is not None
        assert visible[0] == 1


def test_a_hold_out_row_with_no_access_record_is_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The trigger, exercised by a writer that bypasses ``result_ledger``.

    ⚠ This is the case that makes the record an INVARIANT rather than a
    convention: it fails for a hand-written INSERT, for a script, and for the
    superuser this app connects as.
    """
    with pytest.raises(psycopg.errors.IntegrityError), ebull_test_conn.transaction():
        _raw_holdout_insert(ebull_test_conn, "store")


def test_an_in_sample_row_cannot_be_updated_into_the_hold_out(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The two-statement route the BEFORE INSERT half is blind to.

    Insert as in-sample, then flip the namespace, and an unrecorded hold-out row
    exists. ``sql/264``'s trigger fires on UPDATE for exactly this.
    """
    with ebull_test_conn.transaction():
        store_in_sample_result(ebull_test_conn, build_result(strategy_id="S-FLIP", namespace="in_sample"))

    with pytest.raises(psycopg.errors.IntegrityError), ebull_test_conn.transaction():
        ebull_test_conn.execute("UPDATE strategy_results_store SET namespace = 'hold_out' WHERE strategy_id = 'S-FLIP'")


def test_the_view_refuses_to_smuggle_a_hold_out_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The cascaded CHECK OPTION, isolated from the trigger.

    ⚠ The access record is written FIRST on purpose. Without it the trigger
    refuses (23000) and this test would pass while asserting nothing about the
    check option; with it the trigger is satisfied and the view is the only
    thing left to refuse (44000).
    """
    with ebull_test_conn.transaction():
        record_holdout_access(
            ebull_test_conn,
            HoldoutAccess(
                strategy_id="S-RAW",
                strategy_version="strategy-registry-v1+raw000",
                access_kind="evaluate",
                accessed_by=_ACTOR,
                purpose=_PURPOSE,
                result_version="strategy-result-v1+raw000",
            ),
        )
        with pytest.raises(psycopg.errors.WithCheckOptionViolation):
            with ebull_test_conn.transaction():
                _raw_holdout_insert(ebull_test_conn, "view")


def test_row_level_security_would_not_have_bound_this_connection(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ EVIDENCE FOR A DESIGN DECISION, not a test of our code.

    ``sql/264`` records that RLS was measured and rejected. If this database ever
    gains a non-superuser application role, this test FAILS — and the failure is
    the signal that C5's read-side half can finally be enforced properly, not a
    regression. The alternative is a comment claiming a measurement nobody
    re-runs.
    """
    with ebull_test_conn.transaction():
        ebull_test_conn.execute("CREATE TEMP TABLE _rls_probe (ns TEXT) ON COMMIT DROP")
        ebull_test_conn.execute("INSERT INTO _rls_probe VALUES ('in_sample'), ('hold_out')")
        ebull_test_conn.execute("ALTER TABLE _rls_probe ENABLE ROW LEVEL SECURITY")
        ebull_test_conn.execute("ALTER TABLE _rls_probe FORCE ROW LEVEL SECURITY")
        ebull_test_conn.execute("CREATE POLICY p ON _rls_probe FOR SELECT USING (ns = 'in_sample')")
        visible = ebull_test_conn.execute("SELECT count(*) FROM _rls_probe").fetchone()
        privileged = ebull_test_conn.execute(
            "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        assert visible is not None and privileged is not None
        assert privileged[0] is True, "a non-superuser role now exists — sql/264's RLS decision can be revisited"
        assert visible[0] == 2, "FORCE RLS bound this connection after all — sql/264's header is wrong"


# ---------------------------------------------------------------------------
# The round trip and the gate's inputs
# ---------------------------------------------------------------------------


def test_the_round_trip_preserves_the_whole_result(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠⚠ THE 39-COLUMN MAPPING, PINNED IN BOTH DIRECTIONS.

    ``_row_params`` and ``_result_from_row`` are two hand-written lists in the
    same order, and a swapped pair of same-typed columns (``sharpe`` for
    ``sortino``, ``losing_trade_count`` for ``losing_period_count``) is invisible
    to every other test in this file. ``build_metrics`` uses deliberately
    distinct awkward floats so a swap cannot coincide.
    """
    with ebull_test_conn.transaction():
        written = build_result(strategy_id="S-TRIP", trial_count=41)
        store_holdout_result(ebull_test_conn, written, accessed_by=_ACTOR, purpose=_PURPOSE)

        read_back = read_holdout_results(
            ebull_test_conn,
            "S-TRIP",
            written.identity.strategy_version,
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
        )
        assert read_back == (written,)


def test_a_result_carrying_the_criterion_3_block_survives_the_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE ROUND TRIP ABOVE CANNOT SEE THE BLOCK-BOOTSTRAP COLUMNS.

    ``build_metrics`` leaves the whole criterion-3 set NULL, so every one of
    `sql/265`'s eight new columns round-trips as ``None`` there — and a writer
    that omitted them entirely, or read them back in the wrong positional order,
    would pass. That was a real gap: the stage-5e-1 ledger was NOT updated when
    `sql/265` landed, and a bootstrap-carrying row was unwritable (one field set,
    eight null, refused by `strategy_results_bootstrap_all_or_nothing`).

    This is the case that exercises them. ``BOOTSTRAP_BLOCK`` uses the same
    awkward-float discipline as the metric set, and its two same-typed integer
    pairs (`bootstrap_cluster_count` / `bootstrap_resamples`,
    `bootstrap_block_length` / `bootstrap_seed`) are deliberately distinct so a
    swapped pair cannot coincide.
    """
    with ebull_test_conn.transaction():
        written = build_result(strategy_id="S-C3TRIP", metrics=build_metrics(**BOOTSTRAP_BLOCK))
        store_holdout_result(ebull_test_conn, written, accessed_by=_ACTOR, purpose=_PURPOSE)

        read_back = read_holdout_results(
            ebull_test_conn,
            "S-C3TRIP",
            written.identity.strategy_version,
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
        )
        assert read_back == (written,)
        assert read_back[0].metrics.bootstrap_model_id == "c3-block-bootstrap-v1"
        assert read_back[0].metrics.bootstrap_cluster_count == 15577


def test_the_holdout_read_returns_only_the_withheld_side(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The read is scoped to ``namespace = 'hold_out'``, and the scope is not
    decoration.

    Drop it and the sanctioned door starts returning in-sample rows too —
    harmless-looking, and it makes every in-sample read the caller does through
    this function log a hold-out access it never made. The count criterion 5
    audits would then be a function of which door was used, not of what was
    looked at.
    """
    with ebull_test_conn.transaction():
        strategy_version = "strategy-registry-v1+bothsides"
        held = build_result(strategy_id="S-BOTH", strategy_version=strategy_version, namespace="hold_out")
        shown = build_result(strategy_id="S-BOTH", strategy_version=strategy_version, namespace="in_sample")
        assert held.identity.version != shown.identity.version

        store_holdout_result(ebull_test_conn, held, accessed_by=_ACTOR, purpose=_PURPOSE)
        store_in_sample_result(ebull_test_conn, shown)

        read_back = read_holdout_results(
            ebull_test_conn, "S-BOTH", strategy_version, accessed_by=_ACTOR, purpose=_PURPOSE
        )
        assert read_back == (held,)


def test_a_stored_row_whose_hash_does_not_describe_it_is_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ A row and its identity hash can only diverge by hand or by a changed
    payload, and either way the row claims an identity that is not its own —
    criterion 11's "a different strategy inherits a track record"."""
    with ebull_test_conn.transaction():
        _raw_holdout_insert_with_access(ebull_test_conn)
        with pytest.raises(ValueError, match="does not match the identity it carries"):
            read_holdout_results(
                ebull_test_conn,
                "S-RAW",
                "strategy-registry-v1+raw000",
                accessed_by=_ACTOR,
                purpose=_PURPOSE,
            )


def _raw_holdout_insert_with_access(conn: psycopg.Connection[tuple]) -> None:
    record_holdout_access(
        conn,
        HoldoutAccess(
            strategy_id="S-RAW",
            strategy_version="strategy-registry-v1+raw000",
            access_kind="evaluate",
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
            result_version="strategy-result-v1+raw000",
        ),
    )
    _raw_holdout_insert(conn, "store")


def test_a_read_is_recorded_even_when_it_returns_nothing(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ Looking is the event criterion 5 governs. Logging only successful looks
    would make the log a function of what happened to be stored."""
    with ebull_test_conn.transaction():
        assert (
            read_holdout_results(
                ebull_test_conn, "S-ABSENT", "strategy-registry-v1+absent", accessed_by=_ACTOR, purpose=_PURPOSE
            )
            == ()
        )
        logged = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = 'S-ABSENT' AND access_kind = 'read'"
        ).fetchone()
        assert logged is not None
        assert logged[0] == 1


def test_the_counts_move_the_promotion_gate_off_holdout_never_evaluated(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The two counts wired into ``check_promotable``, end to end.

    ⚠ The result stays UNPROMOTABLE and that is §6's stated initial state — a
    survivor-only corpus, carry unmodelled, no DSR and no effective sample size.
    What this asserts is narrower and is the whole of 5e-1's contribution: the
    two criterion-5 refusals STOP firing once an evaluation is recorded, so the
    remaining refusals are the ones later stages own.
    """
    with ebull_test_conn.transaction():
        result = build_result(strategy_id="S-GATE")
        universe = frozenset({1, 2, 3})

        before = holdout_access_counts(ebull_test_conn, "S-GATE", result.identity.strategy_version)
        assert before.holdout_evaluations == 0
        assert "holdout_never_evaluated" in check_promotable(
            PromotionCandidate(
                result=result,
                evaluated_instrument_ids=universe,
                validated_universe_ids=universe,
                holdout_evaluations=before.holdout_evaluations,
                recorded_accesses=before.recorded_accesses,
            )
        )

        store_holdout_result(ebull_test_conn, result, accessed_by=_ACTOR, purpose=_PURPOSE)
        after = holdout_access_counts(ebull_test_conn, "S-GATE", result.identity.strategy_version)
        assert (after.holdout_evaluations, after.recorded_accesses) == (1, 1)

        refusals = check_promotable(
            PromotionCandidate(
                result=result,
                evaluated_instrument_ids=universe,
                validated_universe_ids=universe,
                holdout_evaluations=after.holdout_evaluations,
                recorded_accesses=after.recorded_accesses,
                ambiguity_material=False,
            )
        )
        assert "holdout_never_evaluated" not in refusals
        assert "holdout_accesses_unrecorded" not in refusals
        # Still refused, on the reasons stages 5e-2 onward and #2284 own.
        assert set(refusals) == {
            "universe_basis_not_survivorship_free",
            "carry_unmodelled",
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "effective_sample_size_not_computed",
        }


def test_the_two_counts_read_different_relations(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠⚠ THE CHECK THAT WOULD OTHERWISE BE DEAD CODE.

    ``check_promotable`` refuses on ``recorded_accesses < holdout_evaluations``.
    If both numbers came off the same relation that branch could never fire —
    stage 5d's probe lesson, *"a test named after a branch that cannot fire is a
    test that passes for the wrong reason"*. Disabling the trigger produces the
    state the branch names, and the counts diverge.
    """
    with ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "ALTER TABLE strategy_results_store DISABLE TRIGGER trg_strategy_results_holdout_access"
        )
        try:
            _raw_holdout_insert(ebull_test_conn, "store")
        finally:
            ebull_test_conn.execute(
                "ALTER TABLE strategy_results_store ENABLE TRIGGER trg_strategy_results_holdout_access"
            )

        counts = holdout_access_counts(ebull_test_conn, "S-RAW", "strategy-registry-v1+raw000")
        assert (counts.holdout_evaluations, counts.recorded_accesses) == (1, 0)
        assert "holdout_accesses_unrecorded" in check_promotable(
            PromotionCandidate(
                result=build_result(strategy_id="S-RAW"),
                evaluated_instrument_ids=frozenset({1}),
                validated_universe_ids=frozenset({1}),
                holdout_evaluations=counts.holdout_evaluations,
                recorded_accesses=counts.recorded_accesses,
            )
        )


def test_a_second_arm_is_a_second_evaluation_and_a_second_record(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """§3.4's sensitivity pair through the writer.

    Both arms are separate results of the same strategy version, so both must
    store — and each needs its own access record, because the trigger matches on
    ``result_version`` and the two arms hash differently.
    """
    with ebull_test_conn.transaction():
        worst = build_result(strategy_id="S-ARMS", ambiguity_arm="worst_case")
        best = build_result(strategy_id="S-ARMS", ambiguity_arm="best_case")
        assert worst.identity.version != best.identity.version

        store_holdout_result(ebull_test_conn, worst, accessed_by=_ACTOR, purpose=_PURPOSE)
        store_holdout_result(ebull_test_conn, best, accessed_by=_ACTOR, purpose=_PURPOSE)

        counts = holdout_access_counts(ebull_test_conn, "S-ARMS", worst.identity.strategy_version)
        assert (counts.holdout_evaluations, counts.recorded_accesses) == (2, 2)


def test_an_evaluate_record_authorises_only_its_own_result_version(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The trigger matches the TRIPLE, not the strategy version.

    One recorded evaluation must not open the door for every other result of the
    same strategy — otherwise a single access record covers an unbounded number
    of hold-out evaluations, which is the state criterion 5 counts.
    """
    with ebull_test_conn.transaction():
        first = build_result(strategy_id="S-ONE", ambiguity_arm="worst_case")
        store_holdout_result(ebull_test_conn, first, accessed_by=_ACTOR, purpose=_PURPOSE)

    different_arm = build_result(strategy_id="S-ONE", ambiguity_arm="best_case")

    with pytest.raises(psycopg.errors.IntegrityError), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "INSERT INTO strategy_results_store (strategy_id, strategy_version, result_version, result_scope, "
            "namespace, ambiguity_arm, window_start, window_end, universe_basis, corpus_version, cost_model_id, "
            "carry_unmodelled, sizing_rule, position_rule_set_version, outcome_rule_set_version, "
            "input_rule_set_version, evaluated_instrument_count, expectancy_per_trade_pct, profit_factor, cagr_pct, "
            "annualised_volatility_pct, sharpe, sortino, max_drawdown_pct, exposure_time_pct, turnover_annualised, "
            "trade_count, return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count, "
            "unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id) "
            "VALUES (%(sid)s, %(sver)s, %(rver)s, 'sleeve', 'hold_out', 'best_case', '1962-01-02', '2026-07-08', "
            "'survivor_only', 'cv', 'cm', true, 'sr', 'p1', 'o1', 'i1', 10, 0.5, 1.18, 3.9, 14.2, 0.27, 0.39, "
            "-31.4, 62.1, 3.05, 100, -2.4, 50, 20, 4, 0, 251.67, 418.0, 420.4, 'criterion7-v1')",
            {
                "sid": "S-ONE",
                "sver": different_arm.identity.strategy_version,
                "rver": different_arm.identity.version,
            },
        )
