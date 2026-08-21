"""#2770 — the one genuinely new SQL mechanism: the authoritative evidence loader.

Every rule ABOUT the matrix is pure and lives in
``test_2770_operator_promotion_path.py``. What can only be shown against Postgres is
that the loader selects the right ROWS: bound to ``current_identity_pins()``, one per
window/arm identity, latest wins, and nothing a caller could influence.

⚠ The seeding goes through ``store_holdout_result`` rather than hand-written SQL,
because the store refuses a hold-out row that has no criterion-5 ``evaluate`` access
record (``sql/264``'s trigger) and refuses an evidence-window id the database does not
itself recognise (``strategy_evidence_window_is_registered``). A hand-rolled INSERT
either fights those or bypasses the shape the producer actually writes.
"""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

from app.services.result_ledger import store_holdout_result
from app.services.strategy_operator_promotion import (
    RecentEvidenceBundle,
    load_authoritative_recent_evidence,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import (
    METRIC_AXIS_RULE_VERSION,
    ResultIdentity,
    metric_axis_invalid_reason,
    metric_axis_sha256,
)
from app.services.strategy_result_identity import current_identity_pins
from app.services.strategy_statistics import periods_per_year
from tests.test_result_ledger import build_metrics, build_result

pytestmark = pytest.mark.integration

_STRATEGY_ID = "S-2770"
_STRATEGY_VERSION = "strategy-registry-v1+2770loader"
_WINDOW_ID = "year-2023"
_ARM = ("best_case", "masked")


def _axis(*days: int) -> tuple[date, ...]:
    """Bar dates inside `year-2023`. A DIFFERENT axis is a different result_version."""
    return tuple(date(2023, 1, day) for day in days)


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    axis: tuple[date, ...],
    window_id: str = _WINDOW_ID,
    ambiguity: str = _ARM[0],
    quarantine: str = _ARM[1],
    **identity_overrides: object,
) -> int:
    window = RECENT_EVIDENCE_WINDOWS[window_id]  # type: ignore[index]
    pins = current_identity_pins()
    identity: dict[str, object] = {
        "strategy_id": _STRATEGY_ID,
        "strategy_version": _STRATEGY_VERSION,
        "namespace": pins["namespace"],
        "ambiguity_arm": ambiguity,
        "quarantine_arm": quarantine,
        "window_start": window.window.start,
        "window_end": window.window.end,
        "evidence_window_id": window_id,
        "corpus_version": pins["corpus_version"],
        "cost_model_id": pins["cost_model_id"],
        "sizing_rule": pins["sizing_rule"],
        "benchmark_rule": pins["benchmark_rule"],
        "return_basis": pins["return_basis"],
        "ambiguity_rule_version": pins["ambiguity_rule_version"],
        "position_rule_set_version": pins["position_rule_set_version"],
        "outcome_rule_set_version": pins["outcome_rule_set_version"],
        "input_rule_set_version": pins["input_rule_set_version"],
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "metric_axis_dates": axis,
        "metric_axis_start": axis[0],
        "metric_axis_end": axis[-1],
        "metric_axis_digest": metric_axis_sha256(axis),
        "opportunity_set_digest": "b" * 64,
    }
    identity.update(identity_overrides)
    # ⚠ `periods_per_year` is DERIVED from the axis, and `store_holdout_result`
    # reconciles the two before writing (`_assert_axis_metric_reconciliation`). A
    # fixture that varies the axis must vary this with it or the store refuses.
    # `build_metrics`'s defaults (-100% total return, -43.3% CAGR) reconcile only over
    # the long evaluation window it was written for; a flat pair reconciles over any.
    identity.setdefault(
        "metrics",
        build_metrics(periods_per_year=periods_per_year(axis), total_return_pct=0.0, cagr_pct=0.0),
    )
    result = build_result(**identity)
    return store_holdout_result(
        conn,
        result,
        accessed_by="tests/test_2770_authoritative_evidence_loader.py",
        purpose="#2770 loader fixture",
    )


def _load(conn: psycopg.Connection[tuple]) -> RecentEvidenceBundle:
    return load_authoritative_recent_evidence(conn, strategy_id=_STRATEGY_ID, strategy_version=_STRATEGY_VERSION)


def test_a_re_run_over_a_moved_axis_is_a_second_row_on_one_identity(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ THIS IS WHY THE LOADER RESOLVES DUPLICATES RATHER THAN REFUSING THEM.

    ``result_version`` hashes ``metric_axis_dates`` (``ResultIdentity.version``), and
    ``strategy_results_unique`` is on ``(strategy_id, strategy_version,
    result_version)`` — NOT on the window/arm identity. So a re-run whose corpus lost
    or gained a single bar writes a SECOND row for the same window and the same arm,
    and both are legitimate. Pinned results are ``ON DELETE RESTRICT``, so the older
    one cannot be tidied away either: "refuse on duplicate" would make any re-run
    strategy permanently unpromotable.
    """
    first = _seed(ebull_test_conn, axis=_axis(3, 4, 5))
    second = _seed(ebull_test_conn, axis=_axis(3, 4, 6))
    assert first != second, "the store accepted two rows on one window/arm identity"

    stored = ebull_test_conn.execute(
        """
        SELECT count(*) FROM strategy_results_store
        WHERE strategy_id = %s AND strategy_version = %s
          AND evidence_window_id = %s AND ambiguity_arm = %s AND quarantine_arm = %s
        """,
        (_STRATEGY_ID, _STRATEGY_VERSION, _WINDOW_ID, *_ARM),
    ).fetchone()
    assert stored is not None and stored[0] == 2


def test_the_loader_returns_the_later_row_and_only_that(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Latest-wins is also the SAFE direction — a worse re-run supersedes a better
    old row, never the reverse, and no caller can influence the choice."""
    older = _seed(ebull_test_conn, axis=_axis(3, 4, 5))
    newer = _seed(ebull_test_conn, axis=_axis(3, 4, 6))

    bundle = _load(ebull_test_conn)
    assert bundle.result_ids == (max(older, newer),)
    assert older not in bundle.result_ids


def test_a_row_off_the_comparability_basis_is_invisible(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The pins ARE the cross-row coherence rule (#2770).

    A different cost model is not the same evidence, and a per-row check cannot see
    that — only the denominator can. Without this the matrix could be label-complete
    while mixing measurement bases.
    """
    on_basis = _seed(ebull_test_conn, axis=_axis(3, 4, 5))
    _seed(ebull_test_conn, axis=_axis(3, 4, 6), cost_model_id="some-other-cost-model")

    bundle = _load(ebull_test_conn)
    assert bundle.result_ids == (on_basis,), "the off-basis row must not supersede the on-basis one"


def test_arms_and_windows_are_not_collapsed(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """`DISTINCT`-style resolution must key on the full identity, not on the window."""
    masked = _seed(ebull_test_conn, axis=_axis(3, 4, 5), quarantine="masked")
    admitted = _seed(ebull_test_conn, axis=_axis(3, 4, 5), quarantine="admitted")
    other_window = _seed(
        ebull_test_conn,
        axis=(date(2022, 1, 3), date(2022, 1, 4)),
        window_id="year-2022",
    )

    bundle = _load(ebull_test_conn)
    assert set(bundle.result_ids) == {masked, admitted, other_window}


def test_an_incomplete_matrix_refuses_and_names_the_gaps(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """One arm of one window is not a denominator, and the refusal must say so."""
    _seed(ebull_test_conn, axis=_axis(3, 4, 5))

    bundle = _load(ebull_test_conn)
    assert not bundle.complete
    refusals = bundle.refusals
    assert f"recent_evidence_arm_missing:{_WINDOW_ID}/worst_case/masked" in refusals
    assert "recent_evidence_window_missing:year-2024" in refusals
    # The seeded window is present, so it must NOT be reported as wholly missing.
    assert f"recent_evidence_window_missing:{_WINDOW_ID}" not in refusals


def test_the_evidence_reference_moves_when_the_selection_moves(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A superseded denominator must be visible in the audit reference, not inferred."""
    _seed(ebull_test_conn, axis=_axis(3, 4, 5))
    before = _load(ebull_test_conn).evidence_ref
    _seed(ebull_test_conn, axis=_axis(3, 4, 6))
    after = _load(ebull_test_conn).evidence_ref
    assert before != after


def test_the_database_itself_refuses_an_undeclared_evidence_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ So ``recent_evidence_window_unknown`` is UNREACHABLE through the store.

    ``strategy_evidence_window_is_registered`` (a CHECK constraint) whitelists the
    same six windows. The refusal code is kept for the case where rows reach the rule
    from somewhere else, and this test records why it never fires here — otherwise a
    later reader would take its absence from the DB tier as an untested path.
    """
    axis = _axis(3, 4, 5)
    undeclared = build_result(
        strategy_id=_STRATEGY_ID,
        strategy_version=_STRATEGY_VERSION,
        namespace="hold_out",
        window_start=date(2023, 1, 1),
        window_end=date(2023, 12, 31),
        evidence_window_id="year-2025",
        metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
        metric_axis_dates=axis,
        metric_axis_start=axis[0],
        metric_axis_end=axis[-1],
        metric_axis_digest=metric_axis_sha256(axis),
        opportunity_set_digest="b" * 64,
        metrics=build_metrics(periods_per_year=periods_per_year(axis), total_return_pct=0.0, cagr_pct=0.0),
    )
    assert metric_axis_invalid_reason(undeclared.identity, undeclared.metrics) == "evidence_window_unregistered"
    with pytest.raises(ValueError, match="metric-axis provenance"):
        store_holdout_result(
            ebull_test_conn,
            undeclared,
            accessed_by="tests/test_2770_authoritative_evidence_loader.py",
            purpose="#2770 undeclared-window fixture",
        )


def test_the_identity_hash_moves_with_the_axis(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The mechanism the duplicate case rests on, asserted rather than assumed.

    Kept in the DB file beside the row it explains, though it needs no connection:
    if this ever stops holding, the two rows above collide on
    ``strategy_results_unique`` and the first test fails for a reason that would be
    hard to read.
    """
    common: dict[str, object] = {
        "strategy_id": _STRATEGY_ID,
        "strategy_version": _STRATEGY_VERSION,
        "result_scope": "sleeve",
        "namespace": "hold_out",
        "ambiguity_arm": _ARM[0],
        "quarantine_arm": _ARM[1],
        "sizing_rule": "s",
        "benchmark_rule": "b",
        "cost_model_id": "c",
        "corpus_version": "cv",
        "window_start": date(2023, 1, 1),
        "window_end": date(2023, 12, 31),
        "position_rule_set_version": "p",
        "outcome_rule_set_version": "o",
        "input_rule_set_version": "i",
        "return_basis": current_identity_pins()["return_basis"],
        "evidence_window_id": _WINDOW_ID,
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "opportunity_set_digest": "b" * 64,
    }

    def _identity(axis: tuple[date, ...]) -> ResultIdentity:
        return ResultIdentity(
            **common,  # type: ignore[arg-type]
            metric_axis_dates=axis,
            metric_axis_start=axis[0],
            metric_axis_end=axis[-1],
            metric_axis_digest=metric_axis_sha256(axis),
        )

    assert _identity(_axis(3, 4, 5)).version != _identity(_axis(3, 4, 6)).version
