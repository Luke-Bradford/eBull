"""#1155 — Layer 1 / 2 / 3 freshness redesign wiring + sec_rebuild
manual triage. Registry tests + sec_rebuild param-validation tests.

Verifies:

* Layers 1/2/3 are in ``VALID_JOB_NAMES`` + ``SCHEDULED_JOBS`` with
  the cadences + prerequisites pinned in
  ``docs/superpowers/specs/2026-05-13-layer-123-wiring.md``.
* sec_rebuild is in ``VALID_JOB_NAMES`` + ``MANUAL_TRIGGER_JOB_METADATA``
  + ``MANUAL_TRIGGER_JOB_SOURCES`` but NOT in ``SCHEDULED_JOBS``.
* ``_lookup_metadata`` falls back to ``MANUAL_TRIGGER_JOB_METADATA``
  for sec_rebuild while preserving the ``SCHEDULED_JOBS`` path for
  Layer 1/2/3.
* ``source_for`` resolves sec_rebuild to ``sec_rate`` via the new
  ``MANUAL_TRIGGER_JOB_SOURCES`` Pass 3.
* sec_rebuild ParamMetadata validation accepts declared keys and
  rejects unknown/typed-bad keys.
"""

from __future__ import annotations

import pytest

from app.jobs.runtime import VALID_JOB_NAMES
from app.jobs.sources import MANUAL_TRIGGER_JOB_SOURCES, source_for
from app.services.processes.param_metadata import (
    MANUAL_TRIGGER_JOB_METADATA,
    ParamValidationError,
    _lookup_metadata,
    validate_job_params,
)
from app.workers.scheduler import (
    JOB_SEC_ATOM_FAST_LANE,
    JOB_SEC_DAILY_INDEX_RECONCILE,
    JOB_SEC_PER_CIK_POLL,
    JOB_SEC_REBUILD,
    SCHEDULED_JOBS,
    Cadence,
)


def _job_by_name(name: str):
    for job in SCHEDULED_JOBS:
        if job.name == name:
            return job
    return None


class TestLayer123Registry:
    """Layer 1/2/3 are scheduled with the cadences + prereqs pinned in spec."""

    def test_layer1_atom_fast_lane_registered(self) -> None:
        assert JOB_SEC_ATOM_FAST_LANE in VALID_JOB_NAMES
        job = _job_by_name(JOB_SEC_ATOM_FAST_LANE)
        assert job is not None
        assert job.cadence == Cadence.every_n_minutes(interval=5)
        assert job.catch_up_on_boot is False
        # _bootstrap_complete prereq present
        assert job.prerequisite is not None
        assert job.source == "sec_rate"

    def test_layer2_daily_index_reconcile_registered(self) -> None:
        assert JOB_SEC_DAILY_INDEX_RECONCILE in VALID_JOB_NAMES
        job = _job_by_name(JOB_SEC_DAILY_INDEX_RECONCILE)
        assert job is not None
        assert job.cadence == Cadence.daily(hour=4, minute=0)
        # CRITICAL: catch_up_on_boot is the entire point of Layer 2;
        # missed-yesterday window must reconcile after a stack restart.
        assert job.catch_up_on_boot is True
        # CRITICAL: NO _bootstrap_complete prereq — JobRuntime evaluates
        # catch_up_on_boot only at boot, so a prereq-blocked catch-up
        # cannot re-fire when bootstrap completes later. Spec §1.4.
        assert job.prerequisite is None
        assert job.source == "sec_rate"

    def test_layer3_per_cik_poll_registered(self) -> None:
        assert JOB_SEC_PER_CIK_POLL in VALID_JOB_NAMES
        job = _job_by_name(JOB_SEC_PER_CIK_POLL)
        assert job is not None
        assert job.cadence == Cadence.hourly(minute=0)
        assert job.catch_up_on_boot is False
        assert job.prerequisite is not None
        assert job.source == "sec_rate"


class TestSecRebuildRegistry:
    """sec_rebuild is manual-trigger-only — registered via the sibling
    side-tables, NOT in SCHEDULED_JOBS."""

    def test_sec_rebuild_in_valid_job_names(self) -> None:
        assert JOB_SEC_REBUILD in VALID_JOB_NAMES

    def test_sec_rebuild_not_in_scheduled_jobs(self) -> None:
        """The whole point of MANUAL_TRIGGER_JOB_METADATA is to keep
        sec_rebuild out of the cron registry. Regression-guard."""
        assert _job_by_name(JOB_SEC_REBUILD) is None

    def test_sec_rebuild_in_manual_trigger_metadata(self) -> None:
        assert JOB_SEC_REBUILD in MANUAL_TRIGGER_JOB_METADATA
        params = MANUAL_TRIGGER_JOB_METADATA[JOB_SEC_REBUILD]
        names = {p.name for p in params}
        assert names == {"instrument_id", "filer_cik", "source", "discover"}

    def test_sec_rebuild_in_manual_trigger_sources(self) -> None:
        assert MANUAL_TRIGGER_JOB_SOURCES[JOB_SEC_REBUILD] == "sec_rate"

    def test_source_for_resolves_sec_rebuild(self) -> None:
        """Round 4 finding — manual-trigger jobs need source-lock coverage
        or JobLock acquisition KeyErrors at dispatch."""
        assert source_for(JOB_SEC_REBUILD) == "sec_rate"


class TestLookupMetadataFallback:
    """``_lookup_metadata`` chain: SCHEDULED_JOBS → MANUAL_TRIGGER → empty."""

    def test_lookup_returns_manual_trigger_metadata_for_sec_rebuild(self) -> None:
        result = _lookup_metadata(JOB_SEC_REBUILD)
        assert len(result) == 4
        assert {p.name for p in result} == {"instrument_id", "filer_cik", "source", "discover"}

    def test_lookup_returns_scheduled_metadata_for_existing_job(self) -> None:
        """Existing SCHEDULED_JOBS path must still work."""
        # sec_13f_quarterly_sweep has min_period_of_report ParamMetadata
        result = _lookup_metadata("sec_13f_quarterly_sweep")
        names = {p.name for p in result}
        assert "min_period_of_report" in names

    def test_lookup_returns_empty_for_unknown_job(self) -> None:
        assert _lookup_metadata("does_not_exist_anywhere") == ()


class TestSecRebuildParamValidation:
    """Validator path through MANUAL_TRIGGER_JOB_METADATA — covers the
    operator API ``POST /jobs/sec_rebuild/run`` contract."""

    def test_empty_body_validates_at_api_layer(self) -> None:
        """Empty body passes validation; ``_resolve_scope`` raises at
        body time. Existing at-least-one-of contract."""
        # No keys to reject; validator returns empty params.
        coerced = validate_job_params(JOB_SEC_REBUILD, {}, allow_internal_keys=False)
        assert coerced == {}

    def test_invalid_source_rejected_by_validator(self) -> None:
        with pytest.raises(ParamValidationError):
            validate_job_params(
                JOB_SEC_REBUILD,
                {"source": "not_a_real_source"},
                allow_internal_keys=False,
            )

    def test_valid_source_passes(self) -> None:
        coerced = validate_job_params(
            JOB_SEC_REBUILD,
            {"source": "sec_form4"},
            allow_internal_keys=False,
        )
        assert coerced == {"source": "sec_form4"}

    def test_instrument_id_int_coerces(self) -> None:
        coerced = validate_job_params(
            JOB_SEC_REBUILD,
            {"instrument_id": 123},
            allow_internal_keys=False,
        )
        assert coerced == {"instrument_id": 123}

    def test_instrument_id_zero_rejected_by_min_value(self) -> None:
        """min_value=1 on instrument_id rejects 0 / negative."""
        with pytest.raises(ParamValidationError):
            validate_job_params(
                JOB_SEC_REBUILD,
                {"instrument_id": 0},
                allow_internal_keys=False,
            )

    def test_unknown_key_rejected(self) -> None:
        with pytest.raises(ParamValidationError):
            validate_job_params(
                JOB_SEC_REBUILD,
                {"this_key_does_not_exist": "anything"},
                allow_internal_keys=False,
            )

    def test_discover_bool_coerces(self) -> None:
        coerced = validate_job_params(
            JOB_SEC_REBUILD,
            {"instrument_id": 123, "discover": False},
            allow_internal_keys=False,
        )
        assert coerced == {"instrument_id": 123, "discover": False}
