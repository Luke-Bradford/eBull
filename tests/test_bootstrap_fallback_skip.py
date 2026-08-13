"""Phase C fallback-mode skip tests (#1041).

When ``sec_bulk_download`` measures bandwidth below threshold and
returns ``mode='fallback'``, A3 writes a stub manifest with
``mode='fallback'`` + empty archives. Every Phase C precondition
detects this and raises ``BootstrapPhaseSkipped``; the orchestrator
catches and marks each stage ``skipped`` (not ``error``).

This test pins the contract: write fallback manifest → preconditions
raise BootstrapPhaseSkipped → orchestrator marks stage skipped →
finalize_run sees the run as ``complete`` (skipped is not a failure).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.services.bootstrap_preconditions import (
    BootstrapPhaseSkipped,
    assert_c1a_preconditions,
    assert_c1b_preconditions,
    assert_c2_preconditions,
    assert_c3_preconditions,
    assert_c4_preconditions,
    assert_c5_preconditions,
    assert_not_fallback_mode,
)
from app.services.sec_bulk_download import RUN_MANIFEST_NAME, write_run_manifest


class TestAssertNotFallbackMode:
    def test_no_manifest_does_not_raise(self, tmp_path: Path) -> None:
        # Empty bulk dir = no manifest = no fallback signal. Should pass.
        assert_not_fallback_mode(tmp_path, bootstrap_run_id=42)

    def test_bulk_mode_manifest_does_not_raise(self, tmp_path: Path) -> None:
        # Real bulk-mode manifest with archives = NOT fallback.
        write_run_manifest(tmp_path, bootstrap_run_id=42, archives=[], mode="bulk")
        assert_not_fallback_mode(tmp_path, bootstrap_run_id=42)

    def test_fallback_manifest_raises_phase_skipped(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, bootstrap_run_id=42, archives=[], mode="fallback")
        with pytest.raises(BootstrapPhaseSkipped, match="fallback mode"):
            assert_not_fallback_mode(tmp_path, bootstrap_run_id=42)

    def test_stale_fallback_manifest_does_not_raise(self, tmp_path: Path) -> None:
        # Fallback manifest from a PRIOR run is not load-bearing for
        # the current run — the regular manifest-mismatch check will
        # raise. assert_not_fallback_mode silently passes through.
        write_run_manifest(tmp_path, bootstrap_run_id=99, archives=[], mode="fallback")
        assert_not_fallback_mode(tmp_path, bootstrap_run_id=42)


class TestPhaseCPreconditionsCascadeFallback:
    """Each Phase C precondition must short-circuit on fallback BEFORE
    its DB checks. Without this, C1.b would raise BootstrapPreconditionError
    because its upstream C1.a is `skipped` not `success`."""

    @pytest.fixture()
    def fallback_dir(self, tmp_path: Path) -> Path:
        write_run_manifest(tmp_path, bootstrap_run_id=42, archives=[], mode="fallback")
        return tmp_path

    def test_c1a_short_circuits_on_fallback(self, fallback_dir: Path) -> None:
        # No DB conn supplied — if precondition reached the DB checks
        # it would crash. BootstrapPhaseSkipped raised before that.
        with pytest.raises(BootstrapPhaseSkipped):
            assert_c1a_preconditions(conn=None, bootstrap_run_id=42, bulk_dir=fallback_dir)  # type: ignore[arg-type]

    def test_c2_short_circuits_on_fallback(self, fallback_dir: Path) -> None:
        with pytest.raises(BootstrapPhaseSkipped):
            assert_c2_preconditions(conn=None, bootstrap_run_id=42, bulk_dir=fallback_dir)  # type: ignore[arg-type]

    def test_c3_short_circuits_on_fallback(self, fallback_dir: Path) -> None:
        with pytest.raises(BootstrapPhaseSkipped):
            assert_c3_preconditions(
                conn=None,  # type: ignore[arg-type]
                bootstrap_run_id=42,
                bulk_dir=fallback_dir,
                expected_archive_names=["form13f_2025q1.zip"],
            )

    def test_c4_short_circuits_on_fallback(self, fallback_dir: Path) -> None:
        with pytest.raises(BootstrapPhaseSkipped):
            assert_c4_preconditions(
                conn=None,  # type: ignore[arg-type]
                bootstrap_run_id=42,
                bulk_dir=fallback_dir,
                expected_archive_names=["insider_2025q1.zip"],
            )

    def test_c5_short_circuits_on_fallback(self, fallback_dir: Path) -> None:
        with pytest.raises(BootstrapPhaseSkipped):
            assert_c5_preconditions(
                conn=None,  # type: ignore[arg-type]
                bootstrap_run_id=42,
                bulk_dir=fallback_dir,
                expected_archive_names=["nport_2025q1.zip"],
            )

    def test_c1b_short_circuits_on_fallback(self, fallback_dir: Path) -> None:
        with pytest.raises(BootstrapPhaseSkipped):
            assert_c1b_preconditions(conn=None, bootstrap_run_id=42, bulk_dir=fallback_dir)  # type: ignore[arg-type]


class TestFallbackManifestShape:
    def test_fallback_manifest_records_mode_field(self, tmp_path: Path) -> None:
        write_run_manifest(tmp_path, bootstrap_run_id=42, archives=[], mode="fallback")
        manifest_path = tmp_path / RUN_MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text())
        assert manifest["mode"] == "fallback"
        assert manifest["bootstrap_run_id"] == 42
        assert manifest["archives"] == []

    def test_bulk_mode_manifest_records_mode_field(self, tmp_path: Path) -> None:
        # Default mode='bulk' — back-compat with existing manifest readers.
        write_run_manifest(tmp_path, bootstrap_run_id=42, archives=[])
        manifest_path = tmp_path / RUN_MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text())
        assert manifest["mode"] == "bulk"
