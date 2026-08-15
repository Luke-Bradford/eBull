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

from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    freeze_preregistration,
    holdout_access_counts,
    quarantine_arms_compared,
    read_holdout_results,
    record_holdout_access,
    require_outcome_access,
    store_holdout_arm_pair,
    store_holdout_result,
    store_in_sample_arm_pair,
    store_in_sample_result,
)
from app.services.strategy_result import (
    STRUCTURAL_REFUSAL_POLICY_VERSION,
    PromotionCandidate,
    StrategyResult,
    check_promotable,
)
from tests.test_result_ledger import (
    BOOTSTRAP_BLOCK,
    build_control,
    build_metrics,
    build_result,
    build_result_with_dsr,
)

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
        'sleeve', 'hold_out', 'worst_case', 'masked', '1962-01-02', '2026-07-08', 'capital_candidate', 'survivor_only',
        'paperswithbacktest/Stocks-Daily-Price@2026-07-08', 'static-p75-insession-v1', true,
        'equal_weight_concurrent_v1', 'equal_weight_buy_and_hold_v1', 'raw-close-price-return-v1',
        'ambiguity-verdict-2026-08-13-v1-no-cohort-threshold',
        'p1', 'o1', 'i1', 10,
        0.5, 1.18, 3.9, 14.2, 0.27, 0.39, -31.4, 62.1, 3.05, 100, -2.4, 50, 20, 4, 0, 251.67, 418.0, 420.4,
        'criterion7-v1'
"""

_RAW_COLUMNS = """
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, quarantine_arm, window_start, window_end, purpose, universe_basis, corpus_version,
        cost_model_id, carry_unmodelled, sizing_rule, benchmark_rule, return_basis, ambiguity_rule_version,
        position_rule_set_version,
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


def test_a_result_carrying_the_criterion_6_block_survives_the_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE SAME GAP `sql/265` OPENED, ONE STAGE LATER.

    ``build_result`` leaves the whole criterion-6 set NULL, so every one of
    `sql/266`'s ten columns round-trips as ``None`` in the tests above — and a
    ledger that omitted them, or read them back in the wrong positional order,
    would pass all of them. That is exactly how the stage-5e-1 ledger went stale
    when `sql/265` landed and made every bootstrap-carrying row unwritable.

    ⚠ This row also carries the criterion-3 block, because `sql/266`'s DSR
    consumes `effective_sample_size` — a DSR row with no bootstrap behind it is
    a DSR deflated on a nominal n, which is the state the two stages exist to
    make unreachable.
    """
    with ebull_test_conn.transaction():
        written = build_result_with_dsr(strategy_id="S-C6TRIP")
        store_holdout_result(ebull_test_conn, written, accessed_by=_ACTOR, purpose=_PURPOSE)

        read_back = read_holdout_results(
            ebull_test_conn,
            "S-C6TRIP",
            written.identity.strategy_version,
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
        )
        assert read_back == (written,)
        deflated = read_back[0].deflated
        assert deflated is not None
        assert deflated.model_id == "c6-deflated-sharpe-v1"
        assert deflated.trial_register_version == "trial-register-2026-08-07"
        # ⚠ The two integer fields are distinct so a swapped pair cannot hide,
        # and the sample length must come back as the EFFECTIVE size, not the
        # nominal trade count that sits beside it on the same row.
        assert deflated.declared_trials == 11
        assert deflated.measured_trials == 2
        # ⚠ ONE sample size, and it is criterion 3's — the DSR has no column of
        # its own. It must NOT be the nominal trade count sitting beside it.
        assert deflated.effective_sample_size == read_back[0].metrics.effective_sample_size
        assert read_back[0].metrics.trade_count != deflated.effective_sample_size


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
            "fx_unmodelled",
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "effective_sample_size_not_computed",
            # Stage 5e-5a's, and it is the honest state here: this candidate
            # records no quarantine sensitivity arm, and criterion 9 requires
            # one before a result is promotable.
            "quarantine_arms_not_compared",
            # Stage 5e-5b's, and the same reading: `build_result` carries no
            # synthetic control, so §9's null distribution does not exist for
            # this row and its Sharpe has no scale to be read against.
            "synthetic_control_not_run",
            # #2505: reproducible performance is still not attributable edge.
            "promotion_evidence_missing",
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


def test_the_two_quarantine_arms_do_not_collide(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ WHAT ``sql/267`` EXISTS FOR (criterion 9, stage 5e-5a).

    A masked run and an admitted run are the same strategy, over the same
    corpus, at the same quarantine RULE SET — every other identity field
    identical. Before ``quarantine_arm`` joined the hash they produced the same
    ``result_version`` and the second write hit ``strategy_results_unique``,
    which is a sensitivity arm silently unable to be stored beside the arm it is
    measured against.

    ⚠ Asserted on the DATABASE and not only on the hash: the column carries its
    own CHECK and the store-vs-view parity is what makes an in-sample read see
    it, so a Python-only assertion would pass against a schema that lost it."""
    with ebull_test_conn.transaction():
        masked = build_result(strategy_id="S-QARM", quarantine_arm="masked")
        admitted = build_result(strategy_id="S-QARM", quarantine_arm="admitted")
        assert masked.identity.version != admitted.identity.version

        store_holdout_result(ebull_test_conn, masked, accessed_by=_ACTOR, purpose=_PURPOSE)
        store_holdout_result(ebull_test_conn, admitted, accessed_by=_ACTOR, purpose=_PURPOSE)

        stored = ebull_test_conn.execute(
            "SELECT quarantine_arm FROM strategy_results_store WHERE strategy_id = 'S-QARM' ORDER BY quarantine_arm"
        ).fetchall()
        assert [row[0] for row in stored] == ["admitted", "masked"]

    # ⚠ The vocabulary is closed in SQL as well as in Python — a third arm
    # nobody defined must not be storable by a writer that bypasses the model.
    with pytest.raises(psycopg.errors.IntegrityError), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "UPDATE strategy_results_store SET quarantine_arm = 'conservative' WHERE strategy_id = 'S-QARM'"
        )


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
            "namespace, ambiguity_arm, quarantine_arm, window_start, window_end, purpose, universe_basis, "
            "corpus_version, "
            "cost_model_id, "
            "carry_unmodelled, sizing_rule, benchmark_rule, position_rule_set_version, outcome_rule_set_version, "
            "input_rule_set_version, evaluated_instrument_count, expectancy_per_trade_pct, profit_factor, cagr_pct, "
            "annualised_volatility_pct, sharpe, sortino, max_drawdown_pct, exposure_time_pct, turnover_annualised, "
            "trade_count, return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count, "
            "unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id) "
            "VALUES (%(sid)s, %(sver)s, %(rver)s, 'sleeve', 'hold_out', 'best_case', 'masked', '1962-01-02', "
            "'2026-07-08', "
            "'capital_candidate', 'survivor_only', 'cv', 'cm', true, 'sr', 'br', 'p1', 'o1', 'i1', 10, "
            "0.5, 1.18, 3.9, 14.2, 0.27, 0.39, "
            "-31.4, 62.1, 3.05, 100, -2.4, 50, 20, 4, 0, 251.67, 418.0, 420.4, 'criterion7-v1')",
            {
                "sid": "S-ONE",
                "sver": different_arm.identity.strategy_version,
                "rver": different_arm.identity.version,
            },
        )


def test_a_result_carrying_the_synthetic_control_survives_the_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE SAME GAP sql/265 AND sql/266 EACH OPENED, ONE STAGE LATER AGAIN.

    ``build_result`` leaves §9's whole block NULL, so every one of ``sql/268``'s
    ten columns round-trips as ``None`` in every test above — and a ledger that
    omitted them, or read them back in the wrong positional order, would pass
    all of them. Twice now that is exactly how the ledger went stale when a
    migration landed.

    ⚠ The row also exercises the part ``sql/268`` deliberately does NOT store:
    the strategy's own Sharpe and return are rebuilt FROM ``metrics`` on the way
    back, and ``_result_from_row`` re-derives the stored verdict and refuses a
    disagreement. Equality against ``written`` is therefore an assertion about
    the binding, not only about the columns.
    """
    with ebull_test_conn.transaction():
        metrics = build_metrics()
        written = build_result(strategy_id="S-C9TRIP", metrics=metrics, synthetic_control=build_control(metrics))
        store_holdout_result(ebull_test_conn, written, accessed_by=_ACTOR, purpose=_PURPOSE)

        read_back = read_holdout_results(
            ebull_test_conn,
            "S-C9TRIP",
            written.identity.strategy_version,
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
        )
        assert read_back == (written,)
        control = read_back[0].synthetic_control
        assert control is not None
        assert control.model_id == "permuted-entry-uniform-gap-v1"
        assert control.cohort_size == 1000
        assert control.root_seed == 20260808
        # ⚠ Rebuilt from the metric set, never from a second stored copy.
        assert control.strategy_sharpe == read_back[0].metrics.sharpe
        assert control.strategy_return_pct == read_back[0].metrics.total_return_pct
        # ⚠ A FALSE verdict stored and read back. §10 makes this the expected
        # state, and a fixture that only ever stored a pass would leave the
        # derived-verdict CHECK exercised in one direction.
        assert control.passed is False


# ---------------------------------------------------------------------------
# Criterion 9's arm PAIR (stage 5e-5c)
# ---------------------------------------------------------------------------


def _arms(**overrides: object) -> tuple[StrategyResult, StrategyResult]:
    """One measurement under both arms — identical but for ``quarantine_arm``.

    ⚠ The metric sets are deliberately DIFFERENT. Two arms with identical
    numbers would be a pair that no delta could distinguish, and stage 5e-5a
    measured real movement in every one of criterion 7's twelve.
    """
    masked = build_result(quarantine_arm="masked", **overrides)
    admitted = build_result(
        quarantine_arm="admitted",
        metrics=build_metrics(sharpe=-3.2214778, trade_count=3133792),
        **overrides,
    )
    return masked, admitted


def test_both_arms_land_and_the_pair_reads_as_compared(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The state ``quarantine_arms_not_compared`` refuses, cleared by a write."""
    masked, admitted = _arms(strategy_id="S-PAIR-IN", namespace="in_sample")
    with ebull_test_conn.transaction():
        first, second = store_in_sample_arm_pair(ebull_test_conn, masked, admitted)
        assert first != second
        assert quarantine_arms_compared(ebull_test_conn, masked.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
        # ⚠ Read from EITHER arm's identity — the sibling is computed by
        # flipping the arm, so the answer cannot depend on which one is held.
        assert quarantine_arms_compared(ebull_test_conn, admitted.identity, accessed_by=_ACTOR, purpose=_PURPOSE)


def test_a_lone_arm_does_not_read_as_compared(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ The refusal must survive the arm that IS stored, not just an empty table."""
    masked, _ = _arms(strategy_id="S-PAIR-LONE", namespace="in_sample")
    with ebull_test_conn.transaction():
        store_in_sample_result(ebull_test_conn, masked)
        assert not quarantine_arms_compared(ebull_test_conn, masked.identity, accessed_by=_ACTOR, purpose=_PURPOSE)


def test_mislabelled_arms_are_refused(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ The order is (masked, admitted) — the admitted arm is never the quote."""
    masked, admitted = _arms(strategy_id="S-PAIR-ORDER", namespace="in_sample")
    with pytest.raises(ValueError, match="mislabelled"), ebull_test_conn.transaction():
        store_in_sample_arm_pair(ebull_test_conn, admitted, masked)


def test_arms_differing_in_anything_but_the_arm_are_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ Two results that differ in a second field are a comparison of POPULATIONS.

    ``quarantine_sensitivity.QuarantineCensus`` refuses the same thing one layer
    up, on the bar counts. This is the identity-level twin: a delta between a
    masked ``sleeve`` result and an admitted ``portfolio`` one measures the
    scope, not the handling.
    """
    masked, _ = _arms(strategy_id="S-PAIR-DRIFT", namespace="in_sample")
    other_scope = build_result(
        strategy_id="S-PAIR-DRIFT",
        namespace="in_sample",
        quarantine_arm="admitted",
        result_scope="portfolio",
    )
    with pytest.raises(ValueError, match="do not describe one measurement"), ebull_test_conn.transaction():
        store_in_sample_arm_pair(ebull_test_conn, masked, other_scope)


def test_a_hold_out_pair_records_one_evaluate_per_arm(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ TWO records, not one. sql/264's trigger matches on ``result_version``.

    Two hold-out numbers were produced, and criterion 5 audits evaluations
    rather than sessions — so the gate's ``recorded_accesses`` must keep pace
    with a pair write or the very next check would refuse it.
    """
    masked, admitted = _arms(strategy_id="S-PAIR-HO", namespace="hold_out")
    with ebull_test_conn.transaction():
        store_holdout_arm_pair(ebull_test_conn, masked, admitted, accessed_by=_ACTOR, purpose=_PURPOSE)
        counts = holdout_access_counts(ebull_test_conn, "S-PAIR-HO", masked.identity.strategy_version)
    assert counts.holdout_evaluations == 2
    assert counts.recorded_accesses == 2


def test_reading_the_pair_state_on_the_hold_out_records_the_look(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ Presence is a fact about the withheld side, so looking is an access.

    The record is a ``read`` and not an ``evaluate``: nothing was produced, so
    the gate's evaluation arithmetic must not move. An in-sample identity
    records nothing at all — an audit trail counting automation is not an audit
    trail.
    """
    hold_out, _ = _arms(strategy_id="S-PAIR-LOG", namespace="hold_out")
    in_sample, _ = _arms(strategy_id="S-PAIR-LOG-IS", namespace="in_sample")
    with ebull_test_conn.transaction():
        assert not quarantine_arms_compared(ebull_test_conn, hold_out.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
        assert not quarantine_arms_compared(ebull_test_conn, in_sample.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
        logged = ebull_test_conn.execute(
            "SELECT strategy_id, access_kind, result_version FROM strategy_holdout_accesses ORDER BY access_id"
        ).fetchall()
    assert logged == [("S-PAIR-LOG", "read", None)]


def test_the_gate_clears_the_arm_refusal_once_both_arms_are_stored(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The end-to-end wiring: writer → reader → ``check_promotable``.

    Everything else about this candidate stays unpromotable — that is §6's
    stated initial state — so the assertion is that this ONE refusal moves,
    which is what a per-refusal vocabulary buys.
    """
    masked, admitted = _arms(strategy_id="S-PAIR-GATE", namespace="in_sample")
    with ebull_test_conn.transaction():
        before = quarantine_arms_compared(ebull_test_conn, masked.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
        store_in_sample_arm_pair(ebull_test_conn, masked, admitted)
        after = quarantine_arms_compared(ebull_test_conn, masked.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
    assert "quarantine_arms_not_compared" in check_promotable(
        PromotionCandidate(
            result=masked,
            evaluated_instrument_ids=frozenset({1}),
            validated_universe_ids=frozenset({1}),
            quarantine_arms_compared=before,
        )
    )
    assert "quarantine_arms_not_compared" not in check_promotable(
        PromotionCandidate(
            result=masked,
            evaluated_instrument_ids=frozenset({1}),
            validated_universe_ids=frozenset({1}),
            quarantine_arms_compared=after,
        )
    )


def test_the_pair_writer_is_atomic_on_an_autocommit_connection(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ The lone-arm state must be unreachable however the caller CONNECTED.

    Every other test here runs inside an explicit transaction, where a failed
    second insert aborts the first for free — so none of them can see this. On
    an autocommit connection (this repo opens several: ``app/main.py``'s
    lifespan guards, the runbooks) each statement commits on its own, and a pair
    writer that relied on the caller would leave the masked arm standing after
    the admitted one was refused. That is exactly the state
    ``quarantine_arms_not_compared`` exists to catch and exactly the state this
    API claims to make unreachable.

    The second insert is made to fail by storing the admitted arm FIRST, so the
    pair writer's own admitted insert violates ``strategy_results_unique``.
    """
    masked, admitted = _arms(strategy_id="S-PAIR-ATOMIC", namespace="in_sample")
    ebull_test_conn.rollback()
    ebull_test_conn.autocommit = True
    try:
        store_in_sample_result(ebull_test_conn, admitted)
        with pytest.raises(psycopg.errors.UniqueViolation):
            store_in_sample_arm_pair(ebull_test_conn, masked, admitted)
        landed = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_results_store WHERE strategy_id = 'S-PAIR-ATOMIC'"
        ).fetchone()
        assert landed == (1,), "the masked arm survived a failed pair write"
    finally:
        # ⚠ Autocommit means nothing unwinds itself. Both the row and the
        # connection's mode are restored here rather than left for the fixture.
        ebull_test_conn.execute("DELETE FROM strategy_results_store WHERE strategy_id = 'S-PAIR-ATOMIC'")
        ebull_test_conn.autocommit = False


# ---------------------------------------------------------------------------
# #2599 — the frozen preregistration declaration
# ---------------------------------------------------------------------------


def _declaration(**overrides: object) -> PreregDeclaration:
    """A coherent falsification declaration over a survivor-only trial."""
    base: dict[str, object] = {
        "strategy_id": "S-1",
        "strategy_version": "strategy-registry-v1+aaaaaaaaaaaa",
        "contract_version": "test-contract-v1",
        "prereg_purpose": "falsification_only",
        "structural_refusal_policy_version": STRUCTURAL_REFUSAL_POLICY_VERSION,
        "declared_universe_basis": "survivor_only",
        "declared_carry_unmodelled": True,
        "declared_fx_unmodelled": True,
        "expected_structural_refusals": (
            "universe_basis_not_survivorship_free",
            "carry_unmodelled",
            "fx_unmodelled",
        ),
        "forward_shadow": ForwardShadowFloor(40, 12, "candidate power calculation"),
        "declared_by": _ACTOR,
    }
    base.update(overrides)
    return PreregDeclaration(**base)  # type: ignore[arg-type]


def test_a_frozen_declaration_cannot_be_edited_or_deleted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ IMMUTABILITY IS A PROPERTY OF THE RELATION, not of Python.

    DELETE is barred alongside UPDATE because "unfreeze, look, re-freeze" is the
    same fabrication with an extra step.
    """
    with ebull_test_conn.transaction(force_rollback=True):
        freeze_preregistration(ebull_test_conn, _declaration())
        for statement in (_DECLARATION_UPDATE, _DECLARATION_DELETE):
            with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
                with ebull_test_conn.transaction():
                    ebull_test_conn.execute(statement, {"strategy_id": "S-1"})


def test_one_declaration_per_trial(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Two declarations would let a caller pick whichever the outcome favours."""
    with ebull_test_conn.transaction(force_rollback=True):
        freeze_preregistration(ebull_test_conn, _declaration())
        with pytest.raises(psycopg.errors.UniqueViolation):
            with ebull_test_conn.transaction():
                freeze_preregistration(ebull_test_conn, _declaration(contract_version="test-contract-v2"))


def test_an_incoherent_declaration_is_refused_at_freeze_time(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with ebull_test_conn.transaction(force_rollback=True):
        with pytest.raises(PreregDeclarationRefused) as excinfo:
            freeze_preregistration(ebull_test_conn, _declaration(prereg_purpose="capital_candidate"))
        assert "ineligible_trial_not_declared_falsification" in excinfo.value.refusals


def test_an_undeclared_trial_is_untouched_but_the_named_door_refuses_it(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ NO RETROACTIVE INVALIDATION, and that asymmetry is the whole design.

    ``record_holdout_access`` leaves a trial that predates #2599 alone;
    ``require_outcome_access`` — the door a NEW evaluator uses — refuses it.
    """
    access = HoldoutAccess(
        strategy_id="S-UNDECLARED",
        strategy_version="strategy-registry-v1+undecl",
        access_kind="read",
        accessed_by=_ACTOR,
        purpose=_PURPOSE,
    )
    with ebull_test_conn.transaction(force_rollback=True):
        assert record_holdout_access(ebull_test_conn, access) > 0
        with pytest.raises(PreregDeclarationRefused) as excinfo:
            require_outcome_access(ebull_test_conn, access)
        assert excinfo.value.refusals == ("preregistration_not_frozen",)


def test_a_declared_trial_is_enforced_at_the_shared_chokepoint(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Every paved door funnels through ``record_holdout_access``.

    ⚠ The declaration is frozen coherent and then the POLICY moves underneath
    it — the supersession case. It authorised looks a moment ago and stops now,
    through the old door, with no evaluator change.
    """
    access = HoldoutAccess(
        strategy_id="S-1",
        strategy_version="strategy-registry-v1+aaaaaaaaaaaa",
        access_kind="read",
        accessed_by=_ACTOR,
        purpose=_PURPOSE,
    )
    with ebull_test_conn.transaction(force_rollback=True):
        freeze_preregistration(ebull_test_conn, _declaration())
        assert record_holdout_access(ebull_test_conn, access) > 0

        ebull_test_conn.execute(_DECLARATION_TRIGGER_OFF)
        ebull_test_conn.execute(_DECLARATION_POLICY_DRIFT, {"strategy_id": "S-1"})
        ebull_test_conn.execute(_DECLARATION_TRIGGER_ON)
        with pytest.raises(PreregDeclarationRefused) as excinfo:
            record_holdout_access(ebull_test_conn, access)
        assert "structural_refusal_policy_superseded" in excinfo.value.refusals


def test_storing_a_result_whose_stamps_contradict_the_declaration_is_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Declaring eligible and storing survivor-only is the substitution."""
    with ebull_test_conn.transaction(force_rollback=True):
        freeze_preregistration(
            ebull_test_conn,
            _declaration(
                prereg_purpose="capital_candidate",
                declared_universe_basis="survivorship_free",
                declared_carry_unmodelled=False,
                declared_fx_unmodelled=False,
                expected_structural_refusals=(),
            ),
        )
        with pytest.raises(PreregDeclarationRefused) as excinfo:
            store_holdout_result(
                ebull_test_conn,
                build_result(namespace="hold_out", universe_basis="survivor_only", carry_unmodelled=True),
                accessed_by=_ACTOR,
                purpose=_PURPOSE,
            )
        # ⚠ THREE, not two (#2363): the FX twin is asserted here rather than in
        # a test of its own, because the point is that each stamp is compared
        # SEPARATELY — a single coupled comparison would report one code for
        # what are three distinct substitutions.
        assert set(excinfo.value.refusals) == {
            "declared_universe_basis_substituted",
            "declared_carry_unmodelled_substituted",
            "declared_fx_unmodelled_substituted",
        }


@pytest.mark.parametrize(
    "declared_carry,declared_fx,actual_carry,actual_fx,expected",
    [
        # Declared carry ≠ declared FX in BOTH rows. That is the load-bearing
        # part: it is what makes the two declaration fields distinguishable, so
        # a comparison reading the wrong one changes the outcome.
        (True, False, True, True, "declared_fx_unmodelled_substituted"),
        (False, True, True, True, "declared_carry_unmodelled_substituted"),
    ],
)
def test_each_cost_stamp_is_compared_against_its_own_declaration(
    ebull_test_conn: psycopg.Connection[tuple],
    declared_carry: bool,
    declared_fx: bool,
    actual_carry: bool,
    actual_fx: bool,
    expected: str,
) -> None:
    """⚠⚠ THE TWO DECLARED VALUES MUST DIFFER, OR THIS PROVES NOTHING.

    The test above moves all three stamps together, so every declared value it
    compares against is ``True``. Under that fixture ``declared_fx_unmodelled``
    and ``declared_carry_unmodelled`` are indistinguishable, and a comparison
    reading the wrong one still fires — verified by revert-probe, not assumed:
    re-pointing the FX comparison at ``declared.declared_carry_unmodelled``
    passed the entire module.

    ⚠ A first attempt at this test varied only the ACTUAL stamps and the probe
    STILL passed, because the cross-wire is on the DECLARED side. The operand
    that has to differ is the one being substituted. Each row here therefore
    declares the two halves differently and moves exactly one of them out of
    agreement, so exactly one code may fire.

    #2363's own lesson applied one function over: when two operands coincide, an
    assertion over the RESULT cannot tell the rules apart.
    """
    with ebull_test_conn.transaction(force_rollback=True):
        freeze_preregistration(
            ebull_test_conn,
            _declaration(
                declared_carry_unmodelled=declared_carry,
                declared_fx_unmodelled=declared_fx,
                # Kept coherent with the declared stamps, so the declaration is
                # refused for the SUBSTITUTION and not for an incoherent freeze.
                expected_structural_refusals=(
                    ("universe_basis_not_survivorship_free",)
                    + (("carry_unmodelled",) if declared_carry else ())
                    + (("fx_unmodelled",) if declared_fx else ())
                ),
            ),
        )
        with pytest.raises(PreregDeclarationRefused) as excinfo:
            store_holdout_result(
                ebull_test_conn,
                # ⚠ The basis AGREES with the declaration, so the universe code
                # cannot stand in for the cost code being asserted.
                build_result(
                    namespace="hold_out",
                    universe_basis="survivor_only",
                    carry_unmodelled=actual_carry,
                    fx_unmodelled=actual_fx,
                ),
                accessed_by=_ACTOR,
                purpose=_PURPOSE,
            )
        assert set(excinfo.value.refusals) == {expected}


_DECLARATION_UPDATE = """
    UPDATE strategy_preregistration_declarations
       SET declared_by = 'someone else'
     WHERE strategy_id = %(strategy_id)s
"""

_DECLARATION_DELETE = """
    DELETE FROM strategy_preregistration_declarations WHERE strategy_id = %(strategy_id)s
"""

#: ⚠ Simulates the POLICY moving, not the row being edited — the row is
#: immutable, so the only way this state arises in production is a version bump
#: in `strategy_result`. The trigger is dropped for the one statement because
#: there is no other way to reach the state.
#:
#: ⚠ THREE STATEMENTS AND NOT ONE STRING. psycopg prepares every query, and a
#: prepared statement cannot carry multiple commands — measured here as
#: `SyntaxError: cannot insert multiple commands into a prepared statement`.
_DECLARATION_TRIGGER_OFF = """
    ALTER TABLE strategy_preregistration_declarations
        DISABLE TRIGGER trg_strategy_preregistration_declaration_immutable
"""

_DECLARATION_POLICY_DRIFT = """
    UPDATE strategy_preregistration_declarations
       SET structural_refusal_policy_version = 'structural-refusal-policy-1999-01-01-v0'
     WHERE strategy_id = %(strategy_id)s
"""

_DECLARATION_TRIGGER_ON = """
    ALTER TABLE strategy_preregistration_declarations
        ENABLE TRIGGER trg_strategy_preregistration_declaration_immutable
"""
