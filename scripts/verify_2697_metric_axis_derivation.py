"""Corpus-free integrity and source-dependent derivation replay for #2697.

This verifier deliberately reports the two claims separately:

* integrity replay recomputes the stored tuple digest and endpoints without the
  source corpus;
* derivation replay reloads the frozen corpus/window and requires the stored
  tuple, opportunity digest, and immutable universe child to equal that source.

It reads provenance only, never result performance. Run after a corrected
atomic invocation has completed::

    uv run python -m scripts.verify_2697_metric_axis_derivation
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from app.config import settings
from app.services.backtest_run import load_corpus
from app.services.position_builder import Window
from app.services.strategy_recent_evidence import recent_evidence_window
from app.services.strategy_result import METRIC_AXIS_RULE_VERSION, metric_axis_sha256
from app.services.strategy_result_universe import (
    ResultUniverseRecord,
    load_result_universes,
    record_sha256,
)


@dataclass(frozen=True)
class _StoredProvenance:
    result_id: int
    strategy_id: str
    namespace: str
    universe_basis: str
    window_start: date
    window_end: date
    axis_rule: str
    axis_dates: tuple[date, ...]
    axis_start: date
    axis_end: date
    axis_digest: str
    opportunity_digest: str
    evidence_window_id: str | None


_SELECT = """
    SELECT result_id, strategy_id, namespace, universe_basis, window_start, window_end,
           metric_axis_rule_version, metric_axis_dates, metric_axis_start, metric_axis_end,
           metric_axis_digest, opportunity_set_digest, evidence_window_id
    FROM strategy_results_store
    WHERE metric_axis_rule_version IS NOT NULL
      AND (%(result_ids)s::bigint[] IS NULL OR result_id = ANY(%(result_ids)s::bigint[]))
    ORDER BY result_id
"""


def _stored(rows: list[tuple[Any, ...]]) -> tuple[_StoredProvenance, ...]:
    return tuple(
        _StoredProvenance(
            result_id=int(row[0]),
            strategy_id=str(row[1]),
            namespace=str(row[2]),
            universe_basis=str(row[3]),
            window_start=row[4],  # type: ignore[arg-type]
            window_end=row[5],  # type: ignore[arg-type]
            axis_rule=str(row[6]),
            axis_dates=tuple(row[7]),  # type: ignore[arg-type]
            axis_start=row[8],  # type: ignore[arg-type]
            axis_end=row[9],  # type: ignore[arg-type]
            axis_digest=str(row[10]),
            opportunity_digest=str(row[11]),
            evidence_window_id=None if row[12] is None else str(row[12]),
        )
        for row in rows
    )


def _window(row: _StoredProvenance) -> Window | None:
    if row.namespace == "in_sample":
        if row.evidence_window_id is not None:
            raise RuntimeError(f"result {row.result_id}: in-sample row carries {row.evidence_window_id!r}")
        return None
    if row.namespace != "hold_out" or row.evidence_window_id is None:
        raise RuntimeError(f"result {row.result_id}: current namespace/window identity is incomplete")
    registered = recent_evidence_window(row.evidence_window_id).window
    if (row.window_start, row.window_end) != (registered.start, registered.end):
        raise RuntimeError(f"result {row.result_id}: stored window does not match {row.evidence_window_id!r}")
    return registered


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-id", action="append", type=int, dest="result_ids")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        rows = _stored(conn.execute(_SELECT, {"result_ids": args.result_ids}).fetchall())
        if not rows:
            raise RuntimeError("no current metric-axis result rows found; there is nothing to verify")
        universe_children = load_result_universes(conn, [row.result_id for row in rows])
        corpus_cache: dict[tuple[str, date | None, date | None], object] = {}

        for row in rows:
            integrity_errors: list[str] = []
            if row.axis_rule != METRIC_AXIS_RULE_VERSION:
                integrity_errors.append("axis_rule")
            if not row.axis_dates or (row.axis_start, row.axis_end) != (row.axis_dates[0], row.axis_dates[-1]):
                integrity_errors.append("axis_endpoints")
            if row.axis_digest != metric_axis_sha256(row.axis_dates):
                integrity_errors.append("axis_digest")

            window = _window(row)
            cache_key = (
                row.universe_basis,
                None if window is None else window.start,
                None if window is None else window.end,
            )
            corpus = corpus_cache.get(cache_key)
            if corpus is None:
                corpus = load_corpus(
                    conn,
                    universe_basis=row.universe_basis,  # type: ignore[arg-type]
                    evaluation_window=window,
                )
                corpus_cache[cache_key] = corpus
            source_axis = corpus.in_sample_axis if row.namespace == "in_sample" else corpus.axis  # type: ignore[union-attr]
            source_record: ResultUniverseRecord = corpus.opportunity_records[row.namespace]  # type: ignore[union-attr,index]
            child = universe_children.get(row.result_id)
            derivation_errors: list[str] = []
            if row.axis_dates != source_axis:
                derivation_errors.append("source_axis")
            if row.opportunity_digest != record_sha256(source_record):
                derivation_errors.append("source_opportunity_digest")
            if child is None:
                derivation_errors.append("universe_child_missing")
            elif child != source_record:
                derivation_errors.append("universe_child_source_mismatch")

            print(
                json.dumps(
                    {
                        "result_id": row.result_id,
                        "strategy_id": row.strategy_id,
                        "namespace": row.namespace,
                        "integrity_replay": "pass" if not integrity_errors else "fail",
                        "integrity_errors": integrity_errors,
                        "derivation_replay": "pass" if not derivation_errors else "fail",
                        "derivation_errors": derivation_errors,
                        "axis_dates": len(row.axis_dates),
                        "opportunity_names": len(source_record.evaluated_instrument_ids)
                        + len(source_record.evaluated_series_ids),
                    },
                    sort_keys=True,
                )
            )
            if integrity_errors or derivation_errors:
                raise RuntimeError(f"result {row.result_id} failed #2697 provenance replay")


if __name__ == "__main__":
    main()
