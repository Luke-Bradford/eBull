"""Tests for the PR1a job registry foundation.

Covers:

* Every ``ScheduledJob`` declares a ``source`` from the ``Lane`` vocabulary.
* No legacy ``'sec'`` lane (pre-#1020 catch-all) leaks into source keys.
* ``JOB_NAME_TO_SOURCE`` covers every ``_INVOKERS`` name used by bootstrap stages.
* Conflict detection: a source/lane mismatch between SCHEDULED_JOBS and
  _BOOTSTRAP_STAGE_SPECS raises at registry build time.
* ``source_for`` raises ``KeyError`` for unknown job_name (no silent fallback).
* ParamMetadata validation: each field_type, enum membership, bounds.
* JOB_INTERNAL_KEYS gating: bootstrap path allows internal keys; manual rejects.
* materialise_scheduled_params materialises non-None defaults only.
"""

from __future__ import annotations

import pytest

from app.jobs.sources import (
    JobSourceRegistryError,
    Lane,
    get_job_name_to_source,
    reset_job_name_to_source_cache,
    source_for,
)
from app.services.bootstrap_orchestrator import _BOOTSTRAP_STAGE_SPECS
from app.services.processes.param_metadata import (
    JOB_INTERNAL_KEYS,
    ParamMetadata,
    ParamValidationError,
    materialise_scheduled_params,
    validate_job_params,
)
from app.workers.scheduler import SCHEDULED_JOBS

_ALLOWED_SOURCES: frozenset[Lane] = frozenset({"init", "etoro", "sec_rate", "sec_bulk_download", "db"})


class TestScheduledJobSourceField:
    """ScheduledJob.source is required and from the Lane vocabulary."""

    def test_every_scheduled_job_has_source(self) -> None:
        assert len(SCHEDULED_JOBS) >= 27, "audit doc records 27 scheduled jobs"
        missing = [j.name for j in SCHEDULED_JOBS if not j.source]
        assert not missing, f"jobs missing source: {missing}"

    def test_every_source_is_valid_lane(self) -> None:
        bad = [(j.name, j.source) for j in SCHEDULED_JOBS if j.source not in _ALLOWED_SOURCES]
        assert not bad, f"jobs with invalid source: {bad}"

    def test_no_legacy_sec_lane_leak(self) -> None:
        """Regression — pre-#1020 catch-all lane='sec' MUST NOT appear in source keys.

        The PR0 audit explicitly remapped to sec_rate / sec_bulk_download. A
        future edit re-introducing the legacy 'sec' string here would silently
        merge two distinct rate buckets into one lock; this test pins it out.
        """
        leaks = [j.name for j in SCHEDULED_JOBS if j.source == "sec"]  # type: ignore[comparison-overlap]
        assert not leaks, f"jobs with legacy sec lane: {leaks}"


class TestStageSpecParamsField:
    """StageSpec.params field exists and defaults to empty mapping."""

    def test_every_stage_has_params_attribute(self) -> None:
        for stage in _BOOTSTRAP_STAGE_SPECS:
            assert hasattr(stage, "params"), f"stage {stage.stage_key} missing params"

    def test_only_bootstrap_lifted_stages_have_params(self) -> None:
        """PR1c populated stages 14, 15, 21 (the bespoke-wrapper lift targets);
        every other stage stays with the empty default until a future
        ParamMetadata expansion lands."""
        # PR1c #1064 — lifted bespoke wrappers. The job registry audit
        # §4 enumerates these three; any addition must update the audit
        # and this assertion in lockstep.
        lifted_stage_keys = {
            "filings_history_seed",
            "sec_first_install_drain",
            "sec_13f_recent_sweep",
        }
        for stage in _BOOTSTRAP_STAGE_SPECS:
            if stage.stage_key in lifted_stage_keys:
                assert stage.params, (
                    f"stage {stage.stage_key} should carry the bespoke-wrapper params dict; "
                    "PR1c populated this stage to retire the duplicate wrapper body"
                )
            else:
                assert stage.params == {}, (
                    f"stage {stage.stage_key} has unexpected params {stage.params!r}; "
                    "only PR1c-lifted stages should carry non-empty params"
                )


class TestSourceRegistry:
    """JOB_NAME_TO_SOURCE construction + conflict detection."""

    def test_registry_covers_every_scheduled_job(self) -> None:
        registry = get_job_name_to_source()
        for job in SCHEDULED_JOBS:
            assert job.name in registry, f"scheduled job {job.name} missing from registry"
            assert registry[job.name] == job.source

    def test_registry_covers_every_bootstrap_stage(self) -> None:
        registry = get_job_name_to_source()
        for stage in _BOOTSTRAP_STAGE_SPECS:
            assert stage.job_name in registry, (
                f"bootstrap stage {stage.stage_key} dispatches to {stage.job_name!r} "
                f"which is not in the source registry"
            )

    def test_bootstrap_only_invokers_present(self) -> None:
        """Bootstrap-only invokers (not in SCHEDULED_JOBS) must still resolve.

        Pre-PR1c these are: nightly_universe_sync, daily_candle_refresh,
        daily_cik_refresh, sec_bulk_download, sec_*_ingest_from_dataset (4),
        sec_submissions_files_walk, plus the 3 bespoke wrappers.
        """
        registry = get_job_name_to_source()
        scheduled_names = {j.name for j in SCHEDULED_JOBS}
        bootstrap_only = [s.job_name for s in _BOOTSTRAP_STAGE_SPECS if s.job_name not in scheduled_names]
        for name in bootstrap_only:
            assert name in registry, f"bootstrap-only invoker {name} missing from registry"

    def test_source_for_raises_keyerror_on_unknown(self) -> None:
        with pytest.raises(KeyError, match="unknown job_name"):
            source_for("nonexistent_job")

    def test_source_for_returns_correct_lane(self) -> None:
        # Spot-check a few known mappings against the audit.
        assert source_for("orchestrator_full_sync") == "db"
        assert source_for("execute_approved_orders") == "etoro"
        assert source_for("sec_form3_ingest") == "sec_rate"
        assert source_for("nightly_universe_sync") == "init"
        # sec_bulk_download is a bootstrap-only invoker mapped to its
        # own source bucket per _STAGE_LANE_OVERRIDES.
        assert source_for("sec_bulk_download") == "sec_bulk_download"


class TestRegistryConflictDetection:
    """Conflict between scheduled.source and bootstrap.lane raises at build."""

    def test_conflict_raises_at_build(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Inject a fake conflict and assert _build_job_name_to_source raises.

        Resets the lazy cache + monkeypatches a synthetic conflicting stage
        into _BOOTSTRAP_STAGE_SPECS. The registry build pass must surface
        this as a JobSourceRegistryError, not a silent override.
        """
        from app.jobs import sources as sources_mod
        from app.services import bootstrap_orchestrator as boot_mod
        from app.services.bootstrap_state import StageSpec

        # Pick a real scheduled job whose source we'll deliberately conflict with.
        target = SCHEDULED_JOBS[0]
        conflicting_lane: Lane = "etoro" if target.source != "etoro" else "db"

        synthetic = StageSpec(
            stage_key=f"__conflict_test_{target.name}__",
            stage_order=999,
            lane=conflicting_lane,
            job_name=target.name,
        )

        monkeypatch.setattr(
            boot_mod,
            "_BOOTSTRAP_STAGE_SPECS",
            (*_BOOTSTRAP_STAGE_SPECS, synthetic),
        )
        # Wipe the cache so the next get_job_name_to_source rebuilds.
        reset_job_name_to_source_cache()
        try:
            with pytest.raises(JobSourceRegistryError, match=target.name):
                sources_mod.get_job_name_to_source()
        finally:
            # Restore real cache so subsequent tests use the canonical registry.
            reset_job_name_to_source_cache()


class TestParamMetadataValidation:
    """validate_job_params + _coerce_value + _check_bounds coverage."""

    def _meta(self, **overrides: object) -> ParamMetadata:
        defaults: dict[str, object] = {
            "name": "p",
            "label": "Param",
            "help_text": "help",
            "field_type": "string",
        }
        defaults.update(overrides)
        return ParamMetadata.model_validate(defaults)

    def test_int_coercion(self) -> None:
        meta = (self._meta(name="n", field_type="int", min_value=1, max_value=10),)
        out = validate_job_params("anyjob", {"n": "5"}, allow_internal_keys=False, metadata=meta)
        assert out == {"n": 5}

    def test_int_bounds_low(self) -> None:
        meta = (self._meta(name="n", field_type="int", min_value=10),)
        with pytest.raises(ParamValidationError, match="< min_value"):
            validate_job_params("anyjob", {"n": 5}, allow_internal_keys=False, metadata=meta)

    def test_int_bounds_high(self) -> None:
        meta = (self._meta(name="n", field_type="int", max_value=10),)
        with pytest.raises(ParamValidationError, match="> max_value"):
            validate_job_params("anyjob", {"n": 100}, allow_internal_keys=False, metadata=meta)

    def test_bool_coercion(self) -> None:
        meta = (self._meta(name="b", field_type="bool"),)
        for raw, expected in [("true", True), ("false", False), (True, True), ("1", True), ("0", False)]:
            out = validate_job_params("anyjob", {"b": raw}, allow_internal_keys=False, metadata=meta)
            assert out == {"b": expected}, f"raw={raw!r}"

    def test_date_coercion(self) -> None:
        from datetime import date

        meta = (self._meta(name="d", field_type="date"),)
        out = validate_job_params("anyjob", {"d": "2026-05-09"}, allow_internal_keys=False, metadata=meta)
        assert out == {"d": date(2026, 5, 9)}

    def test_quarter_format(self) -> None:
        meta = (self._meta(name="q", field_type="quarter"),)
        out = validate_job_params("anyjob", {"q": "2026q1"}, allow_internal_keys=False, metadata=meta)
        assert out == {"q": "2026Q1"}

        with pytest.raises(ParamValidationError, match="quarter must match"):
            validate_job_params("anyjob", {"q": "2026Q5"}, allow_internal_keys=False, metadata=meta)

    def test_cik_zero_pad(self) -> None:
        meta = (self._meta(name="c", field_type="cik"),)
        out = validate_job_params("anyjob", {"c": "320193"}, allow_internal_keys=False, metadata=meta)
        assert out == {"c": "0000320193"}

    def test_enum_membership(self) -> None:
        meta = (self._meta(name="e", field_type="enum", enum_values=("a", "b", "c")),)
        out = validate_job_params("anyjob", {"e": "a"}, allow_internal_keys=False, metadata=meta)
        assert out == {"e": "a"}

        with pytest.raises(ParamValidationError, match="not in enum_values"):
            validate_job_params("anyjob", {"e": "z"}, allow_internal_keys=False, metadata=meta)

    def test_multi_enum_membership(self) -> None:
        meta = (self._meta(name="m", field_type="multi_enum", enum_values=("10-K", "10-Q")),)
        out = validate_job_params("anyjob", {"m": ["10-K"]}, allow_internal_keys=False, metadata=meta)
        assert out == {"m": ["10-K"]}

        with pytest.raises(ParamValidationError, match="not in enum_values"):
            validate_job_params("anyjob", {"m": ["BAD"]}, allow_internal_keys=False, metadata=meta)

    def test_unknown_key_rejected_manual(self) -> None:
        meta = (self._meta(name="known", field_type="string"),)
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params("anyjob", {"unknown": "v"}, allow_internal_keys=False, metadata=meta)

    def test_internal_keys_path_allows_listed_only(self) -> None:
        """allow_internal_keys=True permits keys in JOB_INTERNAL_KEYS[job_name]."""
        # sec_13f_quarterly_sweep has source_label as internal.
        meta = ()
        out = validate_job_params(
            "sec_13f_quarterly_sweep",
            {"source_label": "sec_edgar_13f_directory_bootstrap"},
            allow_internal_keys=True,
            metadata=meta,
        )
        assert out == {"source_label": "sec_edgar_13f_directory_bootstrap"}

    def test_internal_keys_path_still_rejects_unlisted(self) -> None:
        """Bootstrap path rejects internal-looking keys not in the allow-list."""
        meta = ()
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "sec_13f_quarterly_sweep",
                {"random_internal_key": "x"},
                allow_internal_keys=True,
                metadata=meta,
            )

    def test_manual_path_blocks_internal_keys(self) -> None:
        """Operator API path must NOT permit any internal keys, even if listed."""
        meta = ()
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "sec_13f_quarterly_sweep",
                {"source_label": "operator_attempt"},  # in JOB_INTERNAL_KEYS, but disallowed for manual
                allow_internal_keys=False,
                metadata=meta,
            )


class TestMaterialiseScheduledParams:
    """materialise_scheduled_params returns non-None defaults only."""

    def test_defaults_materialise(self) -> None:
        meta = (
            ParamMetadata.model_validate(
                {
                    "name": "with_default",
                    "label": "x",
                    "help_text": "x",
                    "field_type": "int",
                    "default": 100,
                }
            ),
            ParamMetadata.model_validate(
                {
                    "name": "no_default",
                    "label": "y",
                    "help_text": "y",
                    "field_type": "int",
                    "default": None,
                }
            ),
        )
        out = materialise_scheduled_params("anyjob", metadata=meta)
        assert out == {"with_default": 100}, "None defaults must be omitted"


class TestJobInternalKeysRegistry:
    """JOB_INTERNAL_KEYS is the canonical bootstrap-only allow-list."""

    def test_sec_13f_has_source_label_internal(self) -> None:
        assert "source_label" in JOB_INTERNAL_KEYS["sec_13f_quarterly_sweep"]

    def test_internal_keys_keys_are_known_jobs(self) -> None:
        """Every job in JOB_INTERNAL_KEYS must be in the source registry."""
        registry = get_job_name_to_source()
        for job_name in JOB_INTERNAL_KEYS:
            assert job_name in registry, f"JOB_INTERNAL_KEYS lists {job_name!r} but it's not in the source registry"


class TestBootstrapOnlyMetadataLookup:
    """Review-bot PR1a [PREVENTION] — _lookup_metadata must not raise on
    bootstrap-only job_names (otherwise PR1b dispatch breaks for
    sec_bulk_download / nightly_universe_sync / etc).
    """

    def test_validate_succeeds_for_bootstrap_only_job_with_no_params(self) -> None:
        """PR1b will dispatch bootstrap-only jobs through validate_job_params
        with metadata=None. The lookup must return () (no operator-exposable
        params), not raise."""
        out = validate_job_params(
            "sec_bulk_download",  # bootstrap-only invoker, NOT in SCHEDULED_JOBS
            {},
            allow_internal_keys=True,
            metadata=None,
        )
        assert out == {}

    def test_validate_rejects_unknown_keys_for_bootstrap_only_job_manual_path(self) -> None:
        """Manual API path against a bootstrap-only job rejects everything —
        empty metadata + allow_internal_keys=False = every key unknown."""
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "nightly_universe_sync",
                {"force_full": True},
                allow_internal_keys=False,
                metadata=None,
            )

    def test_validate_rejects_internal_keys_not_in_allow_list(self) -> None:
        """Bootstrap-only job with allow_internal_keys=True still rejects keys
        not in JOB_INTERNAL_KEYS for that job."""
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "sec_bulk_download",
                {"random_key": "x"},  # not in JOB_INTERNAL_KEYS["sec_bulk_download"]
                allow_internal_keys=True,
                metadata=None,
            )


class TestParamMetadataMisconfiguration:
    """Review-bot PR1a [PREVENTION] — misconfigured metadata must raise
    ParamValidationError (mapped to 400), not AssertionError (escapes to
    500 and silently skipped under ``python -O``).
    """

    def test_enum_without_enum_values_raises_param_validation_error(self) -> None:
        """field_type='enum' with enum_values=None raises ParamValidationError."""
        meta = (
            ParamMetadata.model_validate(
                {
                    "name": "broken",
                    "label": "x",
                    "help_text": "x",
                    "field_type": "enum",
                    "enum_values": None,  # misconfigured — enum requires values
                }
            ),
        )
        with pytest.raises(ParamValidationError, match="requires enum_values"):
            validate_job_params("anyjob", {"broken": "x"}, allow_internal_keys=False, metadata=meta)

    def test_multi_enum_without_enum_values_raises_param_validation_error(self) -> None:
        """field_type='multi_enum' with enum_values=None raises ParamValidationError."""
        meta = (
            ParamMetadata.model_validate(
                {
                    "name": "broken",
                    "label": "x",
                    "help_text": "x",
                    "field_type": "multi_enum",
                    "enum_values": None,
                }
            ),
        )
        with pytest.raises(ParamValidationError, match="requires enum_values"):
            validate_job_params("anyjob", {"broken": ["x"]}, allow_internal_keys=False, metadata=meta)
