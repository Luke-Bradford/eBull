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

_ALLOWED_SOURCES: frozenset[Lane] = frozenset(
    {
        "init",
        "etoro",
        "sec_rate",
        # #1478 — sec_manifest_worker extracted from sec_rate into its own
        # lane so the heavy drainer stops starving the SEC producers. A lane
        # is a job-overlap bucket, not a rate gate (the HTTP throttle bounds
        # the 10 req/s budget); see app/jobs/sources.py::Lane.
        "sec_manifest",
        # #1534 — sec_per_cik_poll extracted from sec_rate into its own
        # single-job lane. The hourly @ :00 producer lost the non-blocking
        # advisory-lock race to whichever sec_rate sibling held the lane at
        # :00 and skipped the whole hour (starved 17h+ on dev; the #1510
        # watchdog kick hit the same lock and no-opped). Same shape as the
        # #1478 sec_manifest split. See app/jobs/sources.py::Lane.
        "sec_per_cik",
        "sec_bulk_download",
        "db",
        # #1141 — Phase C bulk-ingest family sources. Bootstrap-only
        # stages today; surfaced here so the Lane-validity assertion
        # stays in lockstep with ``app/jobs/sources.py::Lane``.
        "db_filings",
        "db_fundamentals_raw",
        "db_ownership_inst",
        "db_ownership_insider",
        "db_ownership_funds",
        # #915 / #916 (Phase 6 PRs 11+12, 2026-05-18). FINRA bimonthly
        # short interest + RegSHO daily share a dedicated ``finra``
        # Lane disjoint from sec_rate (CDN serves both endpoints with
        # one shared throttle).
        "finra",
        # #1526 — jobs_liveness_watchdog + jobs_retry_sweeper extracted from the
        # catch-all ``db`` lane into their own single-job lanes. On ``db`` they
        # lost the ``job_source:db`` advisory-lock race to
        # orchestrator_high_frequency_sync every tick and never ran on schedule
        # (the #1508 self-healing infra was itself starved). Separate lanes (not
        # one shared infra lane) so the 15-min watchdog and 5-min sweeper do not
        # re-starve each other. See app/jobs/sources.py::Lane.
        "db_liveness",
        "db_retry",
        # #1527 — daily/hourly continuation of #1526. monitor_positions,
        # cusip_extid_sweep, ownership_observations_sync each fire on a
        # 5-min-aligned slot and lost the ``job_source:db`` race to
        # orchestrator_high_frequency_sync (skipping a full day per collision).
        # Write-target-disjoint from the orchestrator's portfolio/fx ingest, so
        # each owns a single-job lane. See app/jobs/sources.py::Lane.
        "db_positions",
        "db_cusip",
        "db_ownership_obs",
    }
)


class TestScheduledJobSourceField:
    """ScheduledJob.source is required and from the Lane vocabulary."""

    def test_every_scheduled_job_has_source(self) -> None:
        # Floor was 27 at the PR1a audit; #1159-#1167 legacy-cron retirement
        # sweep (2026-05-14) dropped the count to 24. Floor relaxed to 22 to
        # tolerate one more retirement without immediate test drift.
        assert len(SCHEDULED_JOBS) >= 22, (
            f"expected >= 22 scheduled jobs (24 as of 2026-05-14 post-#1159-#1167); got {len(SCHEDULED_JOBS)}"
        )
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
            # PR8 #1233 §4.12 — sec_n_csr_bootstrap_drain previously
            # carried ``horizon_days=730`` params; the param was
            # removed when N_CSR_RETENTION_DAYS became the single
            # source of truth, so the stage now dispatches with
            # ``params={}`` and is intentionally absent from this set.
            # PR7 #1233 §4.6 — sec_n_port_ingest dispatches with
            # ``min_last_seen_filed_at`` resolved at dispatch time to
            # ``today - 380d`` (UTC midnight). Mirror of the #1010
            # cohort bound on stage 21's sec_13f_recent_sweep.
            "sec_n_port_ingest",
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


class TestOrchestratorAdapterSourceCoverage:
    """#1183 — every orchestrator adapter's ``_run_with_lock(job_name, ...)``
    call site must have a source-registry entry. ``JobLock`` resolves the
    source bucket via ``source_for(job_name)`` which KeyErrors on miss.

    #260 (PR #262) moved 8 jobs from standalone ScheduledJob rows into
    orchestrator's FULL / HIGH_FREQUENCY cadences. 2 are covered by
    bootstrap stage entries (nightly_universe_sync, daily_candle_refresh);
    the other 6 were orphaned from the source registry until #1183 added
    them to MANUAL_TRIGGER_JOB_SOURCES. This test pins the contract so a
    new adapter call site without matching registry coverage fails CI.
    """

    @staticmethod
    def _extract_adapter_job_names() -> set[str]:
        """Parse adapters.py via AST to find every ``job_name=<literal>``
        keyword argument. Single source of truth for what the adapter
        actually dispatches — avoids the test going stale relative to
        hand-maintained lists."""
        import ast
        from pathlib import Path

        adapter_path = Path(__file__).resolve().parent.parent / "app" / "services" / "sync_orchestrator" / "adapters.py"
        tree = ast.parse(adapter_path.read_text())
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "job_name" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                        names.add(kw.value.value)
        return names

    def test_every_adapter_job_name_resolves(self) -> None:
        registry = get_job_name_to_source()
        adapter_names = self._extract_adapter_job_names()
        assert adapter_names, "AST extraction returned no adapter job_names — parser likely broken"
        missing = sorted(n for n in adapter_names if n not in registry)
        assert not missing, (
            f"Orchestrator adapter dispatches to {missing!r} but the source registry has no entry. "
            "Add to MANUAL_TRIGGER_JOB_SOURCES (or a bootstrap stage) so JobLock can resolve "
            "the source bucket. #1183."
        )

    def test_run_with_lock_uses_keyword_job_name(self) -> None:
        """Bot WARNING — guard the AST extractor's keyword-only assumption.

        ``_extract_adapter_job_names`` only sees ``job_name=<literal>``
        keyword form. A positional ``_run_with_lock("foo", legacy_fn)``
        call would silently bypass the invariant. This test fails if
        anyone writes a positional ``_run_with_lock`` call so the
        extractor's blind spot stays CI-enforced rather than implicit.
        """
        import ast
        from pathlib import Path

        adapter_path = Path(__file__).resolve().parent.parent / "app" / "services" / "sync_orchestrator" / "adapters.py"
        tree = ast.parse(adapter_path.read_text())
        violations: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            callee_name: str | None
            if isinstance(func, ast.Attribute):
                callee_name = func.attr
            elif isinstance(func, ast.Name):
                callee_name = func.id
            else:
                callee_name = None
            if callee_name != "_run_with_lock":
                continue
            # _run_with_lock(job_name, legacy_fn, progress=...). The
            # first positional arg IS job_name by the function signature
            # at app/services/sync_orchestrator/adapters.py:87, so we
            # require ALL job_name passes to be keyword form for AST
            # discoverability.
            if node.args:
                violations.append(
                    f"line {node.lineno}: positional arg to _run_with_lock — "
                    f"must pass job_name=<literal> as a keyword so the AST "
                    f"invariant test can find it"
                )
            has_job_name_kw = any(kw.arg == "job_name" for kw in node.keywords)
            if not has_job_name_kw:
                violations.append(f"line {node.lineno}: _run_with_lock call missing job_name= keyword")
        assert not violations, "\n".join(violations)

    def test_known_orchestrator_adapter_targets_covered(self) -> None:
        """Pinned-list regression for the 6 jobs #1183 added + the 1
        composite-adapter job #1184 added (morning_candidate_review).

        Mirrors the audit table in the #1183 issue body + the dormant-
        defect found while writing #1184 (composite adapter
        ``refresh_scoring_and_recommendations`` reaches
        ``morning_candidate_review`` via direct ``JobLock(...)``, not
        via ``_run_with_lock``, so the AST sweep below cannot catch a
        missing entry). If any of these regresses, the test catches it
        before CI runs the AST sweep above.
        """
        registry = get_job_name_to_source()
        expected: dict[str, Lane] = {
            "fx_rates_refresh": "db",
            "daily_portfolio_sync": "etoro",
            # daily_research_refresh → sec_rate (Codex BLOCKING 2):
            # body performs per-CIK SEC fetches; Lane docs reserve
            # sec_rate for "every per-CIK + per-accession SEC fetch".
            "daily_research_refresh": "sec_rate",
            "seed_cost_models": "db",
            "weekly_report": "db",
            "monthly_report": "db",
            # #1184 — composite adapter reach; DB-bound read + write.
            "morning_candidate_review": "db",
        }
        for job_name, expected_source in expected.items():
            assert registry.get(job_name) == expected_source, (
                f"{job_name} mapped to {registry.get(job_name)!r}, expected {expected_source!r}"
            )


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

    def test_real_registry_has_no_conflicting_lane_duplicates(self) -> None:
        """PREVENTION (PR #1414 review) — positive invariant on the LIVE
        registries (the sibling test only injects a synthetic conflict).

        A job_name may legitimately appear in more than one source dict —
        the ``finra_*`` jobs are SCHEDULED crons AND manual-triggerable;
        several stages are both SCHEDULED and a bootstrap stage — but every
        path MUST resolve it to the SAME lane. ``_build_job_name_to_source``
        raises ``JobSourceRegistryError`` on a different-lane collision at
        first call (lifespan / first ``JobLock``); this pins it at test
        time so a mis-homed entry — e.g. a per-CIK job dropped from the
        bootstrap catalogue and re-homed to the WRONG ``MANUAL_TRIGGER_JOB_SOURCES``
        lane (#1413) — fails fast in CI rather than at operator boot.
        """
        from app.jobs.sources import MANUAL_TRIGGER_JOB_SOURCES
        from app.services.bootstrap_orchestrator import (
            _BOOTSTRAP_STAGE_SPECS,
            _effective_lane,
        )

        lanes_by_job: dict[str, set[str]] = {}
        for job in SCHEDULED_JOBS:
            lanes_by_job.setdefault(job.name, set()).add(job.source)
        for spec in _BOOTSTRAP_STAGE_SPECS:
            lanes_by_job.setdefault(spec.job_name, set()).add(_effective_lane(spec.stage_key, spec.lane))
        for job_name, manual_lane in MANUAL_TRIGGER_JOB_SOURCES.items():
            lanes_by_job.setdefault(job_name, set()).add(manual_lane)

        conflicts = {name: sorted(lanes) for name, lanes in lanes_by_job.items() if len(lanes) > 1}
        assert not conflicts, f"job_names registered under conflicting lanes: {conflicts}"


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

    def test_sec_13f_min_last_13f_hr_at_is_internal(self) -> None:
        """#1010 — the HR-recency cohort filter is bootstrap-only.
        Exposing it on the manual API would let an operator drop the
        full-cohort safety-net on the non-bootstrap path."""
        assert "min_last_13f_hr_at" in JOB_INTERNAL_KEYS["sec_13f_quarterly_sweep"]

    def test_sec_13f_min_last_13f_hr_at_rejected_on_manual_path(self) -> None:
        """#1010 — operator API path MUST reject ``min_last_13f_hr_at``
        even though it's listed in JOB_INTERNAL_KEYS for the bootstrap
        path."""
        from datetime import UTC, datetime

        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "sec_13f_quarterly_sweep",
                {"min_last_13f_hr_at": datetime(2025, 1, 1, 0, 0, tzinfo=UTC)},
                allow_internal_keys=False,
                metadata=(),
            )

    def test_internal_keys_keys_are_known_jobs(self) -> None:
        """Every job in JOB_INTERNAL_KEYS must be in the source registry."""
        registry = get_job_name_to_source()
        for job_name in JOB_INTERNAL_KEYS:
            assert job_name in registry, f"JOB_INTERNAL_KEYS lists {job_name!r} but it's not in the source registry"

    def test_sec_first_install_drain_use_bulk_zip_is_internal(self) -> None:
        """#1277 T9 — ``use_bulk_zip`` is bootstrap-only. Bootstrap
        dispatch passes it via StageSpec.params; the validator must
        accept it with ``allow_internal_keys=True``.
        """
        assert "use_bulk_zip" in JOB_INTERNAL_KEYS["sec_first_install_drain"]
        out = validate_job_params(
            "sec_first_install_drain",
            {"use_bulk_zip": True, "max_subjects": None},
            allow_internal_keys=True,
            metadata=None,
        )
        # Coerced through; bool round-trips clean.
        assert out["use_bulk_zip"] is True

    def test_sec_first_install_drain_follow_pagination_is_internal(self) -> None:
        """#1413 Step 2.3 — ``follow_pagination`` is bootstrap-only. The
        bootstrap StageSpec passes ``follow_pagination=False`` to collapse
        the secondary-page HTTP walk; the validator must accept it with
        ``allow_internal_keys=True``. The steady-state safety-net keeps
        the invoker default (``True``); the manual API path rejects the
        key (operator must not silently disable deep-history coverage).
        """
        assert "follow_pagination" in JOB_INTERNAL_KEYS["sec_first_install_drain"]
        out = validate_job_params(
            "sec_first_install_drain",
            {"follow_pagination": False, "use_bulk_zip": True, "max_subjects": None},
            allow_internal_keys=True,
            metadata=None,
        )
        assert out["follow_pagination"] is False

    def test_sec_first_install_drain_follow_pagination_rejected_on_manual_path(self) -> None:
        """#1413 Step 2.3 — operator API path MUST reject
        ``follow_pagination`` even though it's bootstrap-internal."""
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "sec_first_install_drain",
                {"follow_pagination": False},
                allow_internal_keys=False,
                metadata=(),
            )

    def test_sec_first_install_drain_use_bulk_zip_rejected_on_manual_path(self) -> None:
        """#1277 T8 — operator / cron path MUST reject ``use_bulk_zip``.
        On-disk archive freshness is only guaranteed inside the
        bootstrap-run window (S7 → S16); operator-trigger / cron has
        no such guarantee. Opt-in for those paths is gated on PR #1286
        daily-refresh freshness telemetry (separate ticket).
        """
        with pytest.raises(ParamValidationError, match="unknown param"):
            validate_job_params(
                "sec_first_install_drain",
                {"use_bulk_zip": True},
                allow_internal_keys=False,
                metadata=None,
            )


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


class TestSecManifestLaneExtraction:
    """#1478 — ``sec_manifest_worker`` lives in its OWN lane, distinct from the
    SEC producers it used to starve. These are DB-free (``source_for`` reads the
    in-memory registry), so they run as the always-on regression gate even when
    dev PG is down / under ``--no-verify`` — the cross-thread JobLock tests in
    ``test_joblock_per_source.py`` need a live DB and skip when it's absent.
    """

    def test_sec_manifest_worker_has_own_lane(self) -> None:
        assert source_for("sec_manifest_worker") == "sec_manifest"

    def test_sec_manifest_worker_lane_differs_from_every_starved_producer(self) -> None:
        """The whole point: the worker no longer shares a lane with the
        producers it starved (7/7 vs 0/7). If a future change re-collapses
        them, this re-breaks."""
        worker_lane = source_for("sec_manifest_worker")
        for producer in (
            "sec_atom_fast_lane",
            "sec_per_cik_poll",
            "sec_filing_documents_ingest",
            "sec_insider_transactions_backfill",
        ):
            assert source_for(producer) != worker_lane, (
                f"{producer} shares lane {worker_lane!r} with sec_manifest_worker — "
                "the #1478 starvation extraction has regressed"
            )


class TestSecPerCikLaneExtraction:
    """#1534 — ``sec_per_cik_poll`` lives in its OWN lane, distinct from the
    ``sec_rate`` siblings that starved its hourly @ :00 fire (non-blocking
    advisory-lock race → skipped the whole hour, 17h+ on dev; the #1510
    watchdog kick hit the same lock and no-opped). DB-free always-on gate,
    same rationale as ``TestSecManifestLaneExtraction``.
    """

    def test_sec_per_cik_poll_has_own_lane(self) -> None:
        assert source_for("sec_per_cik_poll") == "sec_per_cik"

    def test_sec_per_cik_lane_differs_from_every_sec_rate_sibling(self) -> None:
        """The whole point: the hourly poll no longer shares a lane with the
        ``sec_rate`` members that hold the lane at :00. If a future change
        re-collapses it onto ``sec_rate``, this re-breaks."""
        poll_lane = source_for("sec_per_cik_poll")
        assert poll_lane != "sec_rate"
        for sibling in (
            "sec_atom_fast_lane",
            "sec_daily_index_reconcile",
            "sec_filing_documents_ingest",
            "sec_form3_ingest",
            "sec_insider_transactions_backfill",
        ):
            assert source_for(sibling) != poll_lane, (
                f"{sibling} shares lane {poll_lane!r} with sec_per_cik_poll — "
                "the #1534 starvation extraction has regressed"
            )


# ---------------------------------------------------------------------------
# C7 (#1530) — page-scope role: steady_state vs bootstrap vs backfill
# ---------------------------------------------------------------------------
#
# The Processes page shows ONLY steady_state jobs by default. Bootstrap /
# backfill jobs (one-time install drains + historical catch-up) move to a
# collapsed section. Rule: default ``steady_state`` when unsure — an over-
# shown job is safe; a HIDDEN steady-state keeper is a BUG (the page would
# read green while a real keeper is dead). Only the jobs confirmed below
# (by docstring + cadence in app/workers/scheduler.py) are non-steady-state.

# Jobs whose docstrings describe a one-time install drain (auto-firing
# weekly only as a safety net).
_BOOTSTRAP_JOBS = ("sec_business_summary_bootstrap", "sec_def14a_bootstrap")
# Jobs whose docstrings describe a historical-tail catch-up.
_BACKFILL_JOBS = ("sec_insider_transactions_backfill", "ownership_observations_backfill")


def test_bootstrap_backfill_jobs_are_tagged_not_steady_state() -> None:
    by_name = {j.name: j for j in SCHEDULED_JOBS}
    for n in (*_BOOTSTRAP_JOBS, *_BACKFILL_JOBS):
        assert by_name[n].role in ("bootstrap", "backfill"), n


def test_orchestrator_and_sweeps_stay_steady_state() -> None:
    by_name = {j.name: j for j in SCHEDULED_JOBS}
    # Obvious always-keepers: a future mistag of any of these to
    # bootstrap/backfill would silently hide a live keeper from the
    # default page (page reads green while a real keeper is dead).
    # NOT a count assertion — the allowlist is the regression guard.
    # ``cusip_universe_backfill`` is name-suspicious but deliberately
    # steady_state (it keeps the CUSIP↔extid map current); pinning it
    # blocks a future "fix" from wrongly hiding it.
    for n in (
        "orchestrator_full_sync",
        "monitor_positions",
        "sec_manifest_worker",
        "sec_submissions_bulk_refresh",
        "sec_companyfacts_bulk_refresh",
        "sec_quarterly_datasets_bulk_refresh",
        "jobs_liveness_watchdog",
        "cusip_universe_backfill",
    ):
        assert by_name[n].role == "steady_state", n


def test_every_job_role_is_valid() -> None:
    """No job carries an unrecognised role literal (default is steady_state,
    so an untagged keeper stays visible — never silently hidden)."""
    for j in SCHEDULED_JOBS:
        assert j.role in ("steady_state", "bootstrap", "backfill"), j.name
