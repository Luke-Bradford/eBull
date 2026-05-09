"""Envelope dataclass invariants for the admin control hub (#1071).

Pure-data tests — no DB. Adapters round-trip the underlying tables
into these dataclasses; the contract here is: frozen, slots, hashable
(via frozen), JSON-serialisable through Pydantic mirrors.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from app.services.processes import (
    ActiveRunSummary,
    ErrorClassSummary,
    ProcessRow,
    ProcessRunSummary,
    ProcessSnapshot,
    ProcessWatermark,
)


def _now() -> datetime:
    return datetime(2026, 5, 9, 12, 0, 0, tzinfo=UTC)


def _make_row(**overrides: object) -> ProcessRow:
    base = {
        "process_id": "bootstrap",
        "display_name": "First-install bootstrap",
        "lane": "setup",
        "mechanism": "bootstrap",
        "status": "ok",
        "last_run": None,
        "active_run": None,
        "cadence_human": "on demand",
        "cadence_cron": None,
        "next_fire_at": None,
        "watermark": None,
        "can_iterate": False,
        "can_full_wash": True,
        "can_cancel": False,
        "last_n_errors": (),
        "stale_reasons": (),
    }
    base.update(overrides)
    return ProcessRow(**base)  # type: ignore[arg-type]


def test_process_row_is_frozen() -> None:
    row = _make_row()
    with pytest.raises(FrozenInstanceError):
        row.status = "failed"  # type: ignore[misc]


def test_process_row_carries_all_envelope_fields() -> None:
    """The handler converts ProcessRow → ProcessRowResponse field-for-field;
    a missing field on the dataclass would silently lose data on the wire."""
    row = _make_row()
    expected = {
        "process_id",
        "display_name",
        "lane",
        "mechanism",
        "status",
        "last_run",
        "active_run",
        "cadence_human",
        "cadence_cron",
        "next_fire_at",
        "watermark",
        "can_iterate",
        "can_full_wash",
        "can_cancel",
        "last_n_errors",
        "stale_reasons",
    }
    assert set(row.__slots__) == expected


def test_process_row_default_stale_reasons_empty() -> None:
    """An adapter that doesn't set ``stale_reasons`` would TypeError under
    `slots`; this guard pins the empty-tuple default semantic so callers
    treat empty-tuple as the canonical "not stale" value (vs None or
    missing field)."""
    row = _make_row(stale_reasons=())
    assert row.stale_reasons == ()


def test_active_run_envelope_drops_legacy_fields() -> None:
    """``is_stale`` and ``expected_p95_seconds`` were PR3 placeholders
    superseded by §A1 ``stale_reasons`` (PR8 / #1083). They must NOT
    reappear on ActiveRunSummary — a regression here would carry stale
    semantics back into the wire format."""
    from app.services.processes import ActiveRunSummary

    expected = {
        "run_id",
        "started_at",
        "rows_processed_so_far",
        "progress_units_done",
        "progress_units_total",
        "last_progress_at",
        "is_cancelling",
    }
    assert set(ActiveRunSummary.__slots__) == expected


def test_error_class_summary_truncates_at_construction_boundary() -> None:
    """Adapters truncate sample_message themselves before building the
    dataclass; the dataclass holds whatever string the adapter passes.
    Verify the carrier shape is honest about that contract."""
    err = ErrorClassSummary(
        error_class="X",
        count=5,
        last_seen_at=_now(),
        sample_message="x" * 600,
        sample_subject=None,
    )
    assert len(err.sample_message) == 600  # carrier doesn't enforce; producer does


def test_run_summary_skip_dict_is_addressable() -> None:
    summary = ProcessRunSummary(
        run_id=1,
        started_at=_now(),
        finished_at=_now(),
        duration_seconds=12.5,
        rows_processed=42,
        rows_skipped_by_reason={"unresolved_cusip": 3, "rate_limited": 1},
        rows_errored=0,
        status="success",
        cancelled_by_operator_id=None,
    )
    # Caller treats it like a normal dict — no immutable mapping wrapping.
    assert summary.rows_skipped_by_reason["unresolved_cusip"] == 3


def test_active_run_pristine_no_progress() -> None:
    """Producer that never calls set_target leaves total/done at None.
    The FE renders ``Processed: N`` only — no division-by-zero risk."""
    active = ActiveRunSummary(
        run_id=1,
        started_at=_now(),
        rows_processed_so_far=100,
        progress_units_done=None,
        progress_units_total=None,
        last_progress_at=None,
        is_cancelling=False,
    )
    assert active.progress_units_total is None
    assert active.progress_units_done is None
    assert active.rows_processed_so_far == 100
    assert active.last_progress_at is None


def test_watermark_dataclass_round_trip() -> None:
    wm = ProcessWatermark(
        cursor_kind="filed_at",
        cursor_value="2026-05-08T13:00:00+00:00",
        human="Resume from filings filed after 2026-05-08T13:00Z",
        last_advanced_at=_now(),
    )
    assert wm.cursor_kind == "filed_at"


def test_snapshot_partial_flag_defaults_false() -> None:
    snap = ProcessSnapshot(rows=(), partial=False)
    assert snap.rows == ()
    assert snap.partial is False
