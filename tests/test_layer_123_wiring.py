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
from app.services.canonical_instrument_redirects import (
    JOB_POPULATE_CANONICAL_REDIRECTS,
)
from app.services.processes.param_metadata import (
    MANUAL_TRIGGER_JOB_METADATA,
    ParamValidationError,
    _lookup_metadata,
    validate_job_params,
)
from app.workers.scheduler import (
    JOB_FILING_EVENTS_SKIP_TIER_CLEANUP,
    JOB_FINRA_REGSHO_DAILY_REFRESH,
    JOB_FINRA_SHORT_INTEREST_REFRESH,
    JOB_SEC_ATOM_FAST_LANE,
    JOB_SEC_DAILY_INDEX_RECONCILE,
    JOB_SEC_MANIFEST_TOMBSTONE_STALE,
    JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP,
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
        # #1534 — extracted from the over-subscribed sec_rate lane to its own
        # single-job lane (the hourly @ :00 fire lost the non-blocking
        # advisory-lock race to sec_rate siblings and skipped the whole hour).
        assert job.source == "sec_per_cik"

    def test_layer4_master_idx_quarterly_sweep_registered(self) -> None:
        """G12 — cross-quarter discovery walker. Weekly Sun 05:15 UTC.

        Sibling Layer-3 shape (source=sec_rate, prereq=_bootstrap_complete,
        catch_up_on_boot=False). The "Layer 4" label is informal —
        keeps the test file mental model coherent.
        """
        assert JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP in VALID_JOB_NAMES
        job = _job_by_name(JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP)
        assert job is not None
        assert job.cadence == Cadence.weekly(weekday=6, hour=5, minute=15)
        assert job.catch_up_on_boot is False
        assert job.prerequisite is not None
        assert job.exempt_from_universal_bootstrap_gate is False
        assert job.source == "sec_rate"

    def test_finra_short_interest_refresh_registered(self) -> None:
        """G6/#915 — FINRA bimonthly short interest refresh.

        Not a Layer-1/2/3/4 SEC primitive; lives on a new ``finra``
        lane disjoint from ``sec_rate`` (different host). Sibling
        shape (daily 12:00 UTC, prereq=_bootstrap_complete,
        catch_up_on_boot=False, NOT exempt from universal gate).
        """
        assert JOB_FINRA_SHORT_INTEREST_REFRESH in VALID_JOB_NAMES
        job = _job_by_name(JOB_FINRA_SHORT_INTEREST_REFRESH)
        assert job is not None
        assert job.cadence == Cadence.daily(hour=12, minute=0)
        assert job.catch_up_on_boot is False
        assert job.prerequisite is not None
        assert job.exempt_from_universal_bootstrap_gate is False
        assert job.source == "finra"

    def test_finra_regsho_daily_refresh_registered(self) -> None:
        """G6/#916 — FINRA RegSHO daily short volume refresh.

        Second job on the ``finra`` lane (shares the throttle clock +
        the lane with the bimonthly G6/#915 job). Daily 23:00 UTC,
        prereq=_bootstrap_complete, catch_up_on_boot=False, NOT exempt
        from universal gate.
        """
        assert JOB_FINRA_REGSHO_DAILY_REFRESH in VALID_JOB_NAMES
        job = _job_by_name(JOB_FINRA_REGSHO_DAILY_REFRESH)
        assert job is not None
        assert job.cadence == Cadence.daily(hour=23, minute=0)
        assert job.catch_up_on_boot is False
        assert job.prerequisite is not None
        assert job.exempt_from_universal_bootstrap_gate is False
        assert job.source == "finra"


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


class TestFilingEventsSkipTierCleanupRegistry:
    """#1013 — one-shot skip-tier cleanup is manual-trigger-only:
    registered via the sibling side-tables, NOT in SCHEDULED_JOBS (a
    one-shot delete must never auto-fire)."""

    def test_in_valid_job_names(self) -> None:
        assert JOB_FILING_EVENTS_SKIP_TIER_CLEANUP in VALID_JOB_NAMES

    def test_not_in_scheduled_jobs(self) -> None:
        assert _job_by_name(JOB_FILING_EVENTS_SKIP_TIER_CLEANUP) is None

    def test_in_manual_trigger_metadata_with_no_params(self) -> None:
        assert JOB_FILING_EVENTS_SKIP_TIER_CLEANUP in MANUAL_TRIGGER_JOB_METADATA
        assert MANUAL_TRIGGER_JOB_METADATA[JOB_FILING_EVENTS_SKIP_TIER_CLEANUP] == ()

    def test_in_manual_trigger_sources(self) -> None:
        assert MANUAL_TRIGGER_JOB_SOURCES[JOB_FILING_EVENTS_SKIP_TIER_CLEANUP] == "db"

    def test_source_for_resolves(self) -> None:
        assert source_for(JOB_FILING_EVENTS_SKIP_TIER_CLEANUP) == "db"

    def test_zero_param_validation_contract(self) -> None:
        """POST /jobs/filing_events_skip_tier_cleanup/run takes no params:
        an empty body validates; any supplied key is rejected."""
        metadata = _lookup_metadata(JOB_FILING_EVENTS_SKIP_TIER_CLEANUP)
        assert metadata == ()
        # Empty params accepted.
        validate_job_params(JOB_FILING_EVENTS_SKIP_TIER_CLEANUP, {}, allow_internal_keys=False)
        # Unknown key rejected (no operator-tunable surface).
        with pytest.raises(ParamValidationError):
            validate_job_params(
                JOB_FILING_EVENTS_SKIP_TIER_CLEANUP,
                {"batch_size": 100},
                allow_internal_keys=False,
            )


class TestPopulateCanonicalRedirectsRegistry:
    """#819/#813 — the .RTH redirect binder is manual-trigger-only:
    operator fires it after a universe sync introduces new variants.
    The sources.py half of the triangle was missed at #819's merge,
    so every manual trigger was rejected at ``source_for()`` — this
    class pins the full triangle (invoker + metadata + source)."""

    def test_in_valid_job_names(self) -> None:
        assert JOB_POPULATE_CANONICAL_REDIRECTS in VALID_JOB_NAMES

    def test_not_in_scheduled_jobs(self) -> None:
        assert _job_by_name(JOB_POPULATE_CANONICAL_REDIRECTS) is None

    def test_in_manual_trigger_metadata_with_no_params(self) -> None:
        assert JOB_POPULATE_CANONICAL_REDIRECTS in MANUAL_TRIGGER_JOB_METADATA
        assert MANUAL_TRIGGER_JOB_METADATA[JOB_POPULATE_CANONICAL_REDIRECTS] == ()

    def test_in_manual_trigger_sources(self) -> None:
        assert MANUAL_TRIGGER_JOB_SOURCES[JOB_POPULATE_CANONICAL_REDIRECTS] == "db"

    def test_source_for_resolves(self) -> None:
        assert source_for(JOB_POPULATE_CANONICAL_REDIRECTS) == "db"

    def test_zero_param_validation_contract(self) -> None:
        """POST /jobs/populate_canonical_redirects/run takes no params:
        an empty body validates; any supplied key is rejected."""
        metadata = _lookup_metadata(JOB_POPULATE_CANONICAL_REDIRECTS)
        assert metadata == ()
        validate_job_params(JOB_POPULATE_CANONICAL_REDIRECTS, {}, allow_internal_keys=False)
        with pytest.raises(ParamValidationError):
            validate_job_params(
                JOB_POPULATE_CANONICAL_REDIRECTS,
                {"suffix": ".W"},
                allow_internal_keys=False,
            )


class TestSecManifestTombstoneStaleRegistry:
    """#1614 — the #1131 stale-failed-upsert backfill is drained (zero
    candidates; rows_tombstoned=0 every run; the #1131 source fix means the
    candidate shape cannot recur) and self-deactivates by design. Retired
    from SCHEDULED_JOBS to manual-trigger-only because each zero-candidate
    no-op lost the db-lane tick-race and surfaced a false-red "schedule
    missed" verdict on the admin Processes page. Kept in _INVOKERS so an
    operator can still drain a resurfaced pre-#1131 row. Pins the full
    triangle (invoker + metadata + source) so the manual path resolves
    JobLock — the #1413 / populate_canonical_redirects trap. (NOT modelled
    on daily_tax_reconciliation, which is missing its MANUAL_TRIGGER source
    entry and is itself a latent KeyError-on-trigger bug.)"""

    def test_in_valid_job_names(self) -> None:
        assert JOB_SEC_MANIFEST_TOMBSTONE_STALE in VALID_JOB_NAMES

    def test_not_in_scheduled_jobs(self) -> None:
        assert _job_by_name(JOB_SEC_MANIFEST_TOMBSTONE_STALE) is None

    def test_in_manual_trigger_metadata_with_no_params(self) -> None:
        assert JOB_SEC_MANIFEST_TOMBSTONE_STALE in MANUAL_TRIGGER_JOB_METADATA
        assert MANUAL_TRIGGER_JOB_METADATA[JOB_SEC_MANIFEST_TOMBSTONE_STALE] == ()

    def test_in_manual_trigger_sources(self) -> None:
        assert MANUAL_TRIGGER_JOB_SOURCES[JOB_SEC_MANIFEST_TOMBSTONE_STALE] == "db"

    def test_source_for_resolves(self) -> None:
        assert source_for(JOB_SEC_MANIFEST_TOMBSTONE_STALE) == "db"

    def test_zero_param_validation_contract(self) -> None:
        """POST /jobs/sec_manifest_tombstone_stale/run takes no params:
        an empty body validates; any supplied key is rejected."""
        metadata = _lookup_metadata(JOB_SEC_MANIFEST_TOMBSTONE_STALE)
        assert metadata == ()
        validate_job_params(JOB_SEC_MANIFEST_TOMBSTONE_STALE, {}, allow_internal_keys=False)
        with pytest.raises(ParamValidationError):
            validate_job_params(
                JOB_SEC_MANIFEST_TOMBSTONE_STALE,
                {"max_age_hours": 24},
                allow_internal_keys=False,
            )


class TestEveryInvokerJobResolvesASource:
    """Generic registry-parity invariant (Codex review of the
    populate_canonical_redirects fix): every job the API will accept
    (``VALID_JOB_NAMES`` = ``_INVOKERS`` keys) must resolve a lane via
    ``source_for()``, or manual dispatch dies with a rejected queue row
    the operator must reconcile. populate_canonical_redirects shipped
    with only the invoker half and was silently untriggerable for a
    month — this pins the whole class, not just that one job."""

    # Jobs ALREADY in this broken state on main when the invariant
    # landed (S6 audit 2026-06-11). Each needs per-job triage — right
    # lane vs delete-the-invoker-as-dead-code — tracked in #1571.
    # Adding a NEW name here is never the fix; complete the triangle
    # instead.
    _KNOWN_UNRESOLVABLE: frozenset[str] = frozenset(
        {
            "attribution_summary",
            "daily_financial_facts",
            "daily_tax_reconciliation",
            "sec_def14a_ingest",
            "sec_insider_transactions_ingest",
        }
    )

    def test_all_valid_job_names_resolve(self) -> None:
        unresolvable = []
        for name in sorted(VALID_JOB_NAMES):
            try:
                source_for(name)
            except KeyError:
                unresolvable.append(name)
        new_breaks = sorted(set(unresolvable) - self._KNOWN_UNRESOLVABLE)
        assert new_breaks == [], (
            f"jobs wired in _INVOKERS but missing from every source registry (untriggerable): {new_breaks}"
        )

    def test_known_unresolvable_list_shrinks_only(self) -> None:
        """When a follow-up fixes one of the known-broken jobs, its
        name must leave _KNOWN_UNRESOLVABLE — a stale entry would let
        the regression silently reopen."""
        for name in sorted(self._KNOWN_UNRESOLVABLE):
            with pytest.raises(KeyError):
                source_for(name)


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
