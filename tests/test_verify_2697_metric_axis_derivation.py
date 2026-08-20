"""Corpus-free half of #2697's derivation verifier."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION
from app.services.strategy_result import HOLDOUT_BOUNDARY, METRIC_AXIS_RULE_VERSION, metric_axis_sha256
from app.services.strategy_result_universe import ResultUniverseRecord, record_sha256
from scripts.verify_2697_metric_axis_derivation import _integrity_errors, _StoredProvenance


def _child() -> ResultUniverseRecord:
    return ResultUniverseRecord(
        universe_rule_version=VALIDATED_UNIVERSE_RULE_VERSION,
        evaluated_instrument_ids=frozenset({1}),
        evaluated_series_ids=frozenset({101}),
        validated_universe_ids=frozenset({1, 2}),
    )


def _row() -> _StoredProvenance:
    axis = (date(2020, 1, 2), date(2020, 1, 3))
    return _StoredProvenance(
        result_id=1,
        strategy_id="s1",
        namespace="in_sample",
        universe_basis="survivorship_free",
        window_start=date(2020, 1, 1),
        window_end=date(2020, 1, 4),
        axis_rule=METRIC_AXIS_RULE_VERSION,
        axis_dates=axis,
        axis_start=axis[0],
        axis_end=axis[-1],
        axis_digest=metric_axis_sha256(axis),
        opportunity_digest=record_sha256(_child()),
        evidence_window_id=None,
    )


def test_coherent_provenance_and_child_pass_corpus_free_integrity() -> None:
    assert _integrity_errors(_row(), _child()) == []


def test_integrity_replay_names_independent_axis_and_child_failures() -> None:
    row = replace(
        _row(),
        axis_dates=(HOLDOUT_BOUNDARY, date(2021, 6, 30)),
        axis_start=HOLDOUT_BOUNDARY,
        axis_end=date(2021, 6, 30),
        axis_digest="0" * 64,
    )
    errors = _integrity_errors(row, None)
    assert "axis_digest" in errors
    assert "axis_window_containment" in errors
    assert "in_sample_boundary" in errors
    assert "universe_child_missing" in errors


def test_unordered_axis_and_wrong_frozen_child_digest_are_both_visible() -> None:
    reversed_axis = tuple(reversed(_row().axis_dates))
    row = replace(
        _row(),
        axis_dates=reversed_axis,
        axis_start=reversed_axis[0],
        axis_end=reversed_axis[-1],
        axis_digest=metric_axis_sha256(reversed_axis),
    )
    other_child = replace(_child(), evaluated_series_ids=frozenset({102}))
    errors = _integrity_errors(row, other_child)
    assert "axis_shape" in errors
    assert "universe_child_digest" in errors
