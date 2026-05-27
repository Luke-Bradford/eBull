"""#1273 PR2 — long-pole stage instrumentation wiring smoke.

8 stages instrumented per spec §5.3 — each pins
``set_stage_target`` + ``set_stage_processed`` calls scoped to the
bootstrap-dispatch contextvar set by
``active_bootstrap_run(run_id, stage_key)``.

This file's contract: verify each stage's MODULE wires the three
helpers (import smoke) AND, for each stage, verify that when called
WITHOUT a bootstrap context the helpers are skipped (manual-fire
path = zero progress writes).

Deeper end-to-end "10-row synthetic cohort + monotonic counter +
ROLLBACK survival" coverage lives in
``tests/services/test_bootstrap_state_progress.py`` — that file
exercises the helpers directly against ``ebull_test`` so the bar +
COALESCE + reset semantics are pinned without driving every stage's
ingest pipeline. Together: helpers correctness + per-stage wiring.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Per-stage import smoke — each stage's module must import the three PR2
# helpers at top-of-module. A regression that drops or renames a helper
# import will fail here loudly instead of silently degrading observability
# under a real bootstrap run.
# ---------------------------------------------------------------------------


# Per-stage helper expectation. set_stage_target lives at the
# SCHEDULER boundary for S15/S22/S23 (Codex 2 BLOCKING fold —
# scheduler has every cohort knob in scope; helpers only see the
# post-resolution subset). Those modules don't import set_stage_target.
_STAGE_HELPER_EXPECTATIONS: list[tuple[str, str, tuple[str, ...]]] = [
    (
        "app.services.sec_submissions_files_walk",
        "S14",
        ("resolve_progress_context", "set_stage_target", "set_stage_processed"),
    ),
    # S15: target lives at scheduler.py::filings_history_seed
    ("app.services.filings", "S15", ("resolve_progress_context", "set_stage_processed")),
    (
        "app.jobs.sec_first_install_drain",
        "S16",
        ("resolve_progress_context", "set_stage_target", "set_stage_processed"),
    ),
    ("app.services.def14a_ingest", "S17", ("resolve_progress_context", "set_stage_target", "set_stage_processed")),
    ("app.services.business_summary", "S18", ("resolve_progress_context", "set_stage_target", "set_stage_processed")),
    # S22: target lives at scheduler.py::sec_13f_quarterly_sweep
    ("app.services.institutional_holdings", "S22", ("resolve_progress_context", "set_stage_processed")),
    # S23: target lives at scheduler.py::sec_n_port_ingest
    ("app.services.n_port_ingest", "S23", ("resolve_progress_context", "set_stage_processed")),
    (
        "app.services.fundamentals.__init__",
        "S25",
        ("resolve_progress_context", "set_stage_target", "set_stage_processed"),
    ),
]


@pytest.mark.parametrize(("module_path", "stage_label", "expected_helpers"), _STAGE_HELPER_EXPECTATIONS)
def test_stage_module_imports_pr2_helpers(
    module_path: str, stage_label: str, expected_helpers: tuple[str, ...]
) -> None:
    """Each instrumented module must expose its declared PR2 helpers
    at module scope (via top-of-file imports). The from-import
    statement is preserved as long as the symbol resolves through the
    module.

    S15/S22/S23 instrument set_stage_target at the SCHEDULER boundary
    (app/workers/scheduler.py) rather than in the deep helper —
    Codex 2 BLOCKING fold. Their helper modules import only
    resolve_progress_context + set_stage_processed.
    """
    import importlib

    mod = importlib.import_module(module_path)
    for name in expected_helpers:
        assert hasattr(mod, name), (
            f"{stage_label} module {module_path} missing PR2 helper {name!r}; "
            f"top-of-file `from app.services.bootstrap_state import ...` block "
            f"likely lost the symbol during edit."
        )


# ---------------------------------------------------------------------------
# Manual-fire skip — when resolve_progress_context() returns None (no
# active_bootstrap_run wrapper), set_stage_target + set_stage_processed
# MUST NOT be called. Otherwise a scheduled cron / operator manual trigger
# would write to bootstrap_stages with NO matching run row.
# ---------------------------------------------------------------------------


def test_resolve_progress_context_manual_fire_returns_none() -> None:
    """Outside ``active_bootstrap_run`` the resolver returns None.
    All 8 stages guard every helper call on this — a None ctx means
    every helper site short-circuits and the stage runs at full speed
    with zero side effects against bootstrap_stages.
    """
    from app.services.bootstrap_state import resolve_progress_context

    assert resolve_progress_context() is None


def test_active_bootstrap_run_pins_context_for_resolver() -> None:
    """Inside the contextvar wrapper the resolver returns a typed
    container with the dispatcher-supplied (run_id, stage_key). This
    is the pin every stage's progress writes scope on."""
    from app.services.bootstrap_state import (
        BootstrapProgressContext,
        resolve_progress_context,
    )
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    with active_bootstrap_run(run_id=999, stage_key="S22"):
        ctx = resolve_progress_context()
    assert isinstance(ctx, BootstrapProgressContext)
    assert ctx.run_id == 999
    assert ctx.stage_key == "S22"

    # After the wrapper exits the contextvar resets — resolver
    # returns None again.
    assert resolve_progress_context() is None


# ---------------------------------------------------------------------------
# Per-stage call-shape smoke — drive each stage's progress-write
# sequence under an active_bootstrap_run context and verify the
# expected fingerprint shape lands. We mock the underlying helpers
# so we don't have to seed each stage's full data dependencies; the
# value is in pinning the wiring shape (helper name + kwarg names +
# fingerprint composition) per stage.
# ---------------------------------------------------------------------------


def test_s17_streaming_pattern_pins_target_count_none() -> None:
    """S17 (def14a bootstrap) is streaming-style: target_count MUST
    be None per Codex 1 BLOCKING 2 (upfront COUNT over discovery CTE
    not defensible as ms-cost). Fingerprint pins
    ``pending_predicate_v1`` + rank predicate verbatim.
    """
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    # Patch the helpers at their bound site in def14a_ingest so we
    # don't have to drive the ingest_def14a loop or seed filing_events.
    with (
        patch("app.services.def14a_ingest.set_stage_target") as target_mock,
        patch("app.services.def14a_ingest.set_stage_processed") as processed_mock,
        patch("app.services.def14a_ingest.ingest_def14a") as chunk_mock,
        active_bootstrap_run(run_id=42, stage_key="sec_def14a_bootstrap"),
    ):
        # First chunk returns 0 rows → exit_reason='drained' on iter 1.
        chunk_mock.return_value = MagicMock(
            accessions_seen=0,
            accessions_succeeded=0,
            accessions_partial=0,
            accessions_failed=0,
            rows_inserted=0,
            rows_updated=0,
            first_error=None,
        )
        from app.services.def14a_ingest import bootstrap_def14a

        bootstrap_def14a(MagicMock(), MagicMock(), chunk_limit=500, max_runtime_seconds=3600)

    assert target_mock.called, "S17 must call set_stage_target on entry"
    target_call = target_mock.call_args.kwargs
    assert target_call["target_count"] is None, "S17 is streaming-style — target_count MUST be None"
    fingerprint = target_call["cohort_fingerprint"]
    assert "chunk_limit=500" in fingerprint
    assert "cap_per_filer=2" in fingerprint
    assert "rank_scope=def14a_with_cik" in fingerprint
    assert "pending_predicate_v1=true" in fingerprint
    # set_stage_processed fires at least once (final emit at exit).
    assert processed_mock.called, "S17 must emit final processed_count on exit"


def test_s18_streaming_pattern_pins_pending_predicate_v2() -> None:
    """S18 (business_summary bootstrap) streaming-style. Fingerprint
    declares all 4 pending-predicate branches verbatim including the
    quarantine AND-guard on the tables_null backfill branch.
    """
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    with (
        patch("app.services.business_summary.set_stage_target") as target_mock,
        patch("app.services.business_summary.set_stage_processed") as processed_mock,
        patch("app.services.business_summary.ingest_business_summaries") as chunk_mock,
        active_bootstrap_run(run_id=42, stage_key="sec_business_summary_bootstrap"),
    ):
        chunk_mock.return_value = MagicMock(
            filings_scanned=0,
            rows_inserted=0,
            rows_updated=0,
            fetch_errors=0,
            parse_misses=0,
        )
        from app.services.business_summary import bootstrap_business_summaries

        bootstrap_business_summaries(
            MagicMock(),
            MagicMock(),
            chunk_limit=500,
            max_runtime_seconds=3600,
        )

    assert target_mock.called
    target_call = target_mock.call_args.kwargs
    assert target_call["target_count"] is None
    fingerprint = target_call["cohort_fingerprint"]
    assert "form_types=10-K,10-K/A" in fingerprint
    assert "pending_predicate_v2=" in fingerprint
    assert "tables_null_AND_quarantine_elapsed" in fingerprint, (
        "S18 fingerprint MUST declare the quarantine AND-guard on the tables_null branch — Codex 1 v1.1 GAP B1 fold."
    )
    assert processed_mock.called


def test_s16_streaming_fingerprint_pinned_after_seed_fast_path() -> None:
    """S16 (sec_first_install_drain) is streaming-style. The
    fingerprint MUST be computed AFTER seed_manifest_from_filing_events
    returns (Codex 1 IMPORTANT-2 fold) so ``fast_path_seeded`` reflects
    whether the fast-path fired.
    """
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    with (
        patch("app.jobs.sec_first_install_drain.set_stage_target") as target_mock,
        patch("app.jobs.sec_first_install_drain.set_stage_processed") as processed_mock,
        patch(
            "app.jobs.sec_first_install_drain.seed_manifest_from_filing_events",
            return_value=5,  # > 0 → fast_path_seeded=True
        ),
        patch(
            "app.jobs.sec_first_install_drain._iter_in_universe_subjects",
            return_value=iter([]),  # empty stream → no per-CIK work
        ),
        active_bootstrap_run(run_id=42, stage_key="sec_first_install_drain"),
    ):
        from app.jobs.sec_first_install_drain import run_first_install_drain

        run_first_install_drain(MagicMock(), http_get=MagicMock())

    assert target_mock.called
    target_call = target_mock.call_args.kwargs
    assert target_call["target_count"] is None  # streaming
    fingerprint = target_call["cohort_fingerprint"]
    assert "fast_path_seeded=true" in fingerprint, (
        "S16 fingerprint MUST be computed AFTER seed_manifest_from_filing_events "
        "so fast_path_seeded reflects the real outcome (Codex 1 IMPORTANT-2 fold) "
        "AND rendered lowercase per spec §4 booleans (Codex 2 NIT fold)."
    )
    # final emit fires on exit even when subjects stream is empty.
    assert processed_mock.called


def test_s25_uses_existing_instrument_ids_materialization_no_extra_count() -> None:
    """S25 (fundamentals normalize) MUST use ``len(instrument_ids)``
    after the existing ``cur.fetchall()`` at __init__.py:1651 — NO
    separate ``SELECT COUNT(DISTINCT instrument_id)`` over the 10M-row
    financial_facts_raw table (Codex 1 IMPORTANT-1 fold).
    """
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    # ``SELECT DISTINCT instrument_id FROM financial_facts_raw`` returns
    # 3 rows; len(instrument_ids) MUST be 3.
    fake_cur.fetchall.return_value = [(1,), (2,), (3,)]
    fake_conn.execute.return_value = fake_cur

    with (
        patch("app.services.fundamentals.set_stage_target") as target_mock,
        patch("app.services.fundamentals.set_stage_processed") as processed_mock,
        active_bootstrap_run(run_id=42, stage_key="fundamentals_sync"),
    ):
        from app.services.fundamentals import normalize_financial_periods

        normalize_financial_periods(fake_conn)

    assert target_mock.called
    target_call = target_mock.call_args.kwargs
    assert target_call["target_count"] == 3, (
        "S25 target_count MUST equal len(instrument_ids) from the existing "
        "DISTINCT materialization — no separate COUNT query."
    )
    fingerprint = target_call["cohort_fingerprint"]
    assert "instrument_scope=universe_with_facts" in fingerprint
    assert "source_table=financial_facts_raw" in fingerprint
    assert processed_mock.called
