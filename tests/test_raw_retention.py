"""Tests for compaction + age sweep + scheduler (#268 Plan A PR 3).

Unit tests for compact_source / sweep_source / needs_compaction /
state helpers + scheduler drift-guard. Integration tests covering
end-to-end dedup against a real temp filesystem live alongside.

All tests monkeypatch ``raw_persistence._DATA_ROOT`` to tmp_path
and use a real registered source (``fmp``) so the drift guard
never fires on test-only sources.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.services import raw_persistence
from app.services.raw_persistence import (
    COMPACTION_STALENESS,
    RawPersistenceState,
    _parse_tag_prefix,
    compact_source,
    needs_compaction,
    sweep_source,
)

# ---------------------------------------------------------------------
# Helpers — seed files with controlled mtime
# ---------------------------------------------------------------------


def _seed(
    dir_: Path,
    name: str,
    payload: object,
    *,
    age: timedelta = timedelta(days=30),
) -> Path:
    """Seed ``dir_/name`` with ``payload`` serialised as JSON and an
    mtime ``age`` ago. Returns the path.

    Writes with the LEGACY ``indent=2`` format to exercise the
    canonicalisation-transition invariant (r2-B1) — compaction must
    hash the same bytes as a fresh helper write would."""
    dir_.mkdir(parents=True, exist_ok=True)
    path = dir_ / name
    if isinstance(payload, bytes):
        path.write_bytes(payload)
    else:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    old_ts = (datetime.now(UTC) - age).timestamp()
    os.utime(path, (old_ts, old_ts))
    return path


# ---------------------------------------------------------------------
# _parse_tag_prefix
# ---------------------------------------------------------------------


class TestParseTagPrefix:
    def test_hashed_format(self) -> None:
        assert _parse_tag_prefix("sec_facts_0000320193_abcdef0123456789.json") == "sec_facts_0000320193"

    def test_legacy_timestamped_format(self) -> None:
        assert _parse_tag_prefix("sec_facts_0000320193_20260410T190928Z.json") == "sec_facts_0000320193"

    def test_unrecognised_returns_none(self) -> None:
        assert _parse_tag_prefix("random.json") is None
        assert _parse_tag_prefix("not_a_file.txt") is None
        assert _parse_tag_prefix(".hidden_abcdef0123456789.json") == ".hidden"  # still parseable


# ---------------------------------------------------------------------
# compact_source
# ---------------------------------------------------------------------


class TestCompactSource:
    def test_empty_dir_returns_zero_counts(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = compact_source("fmp", dry_run=False)
        assert result.files_scanned == 0
        assert result.files_deleted == 0

    def test_keeps_one_per_hash_deletes_duplicates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """r2-B1 regression — seed 3 files with same logical content
        via legacy indent=2 format; compaction keeps 1 canonical."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"symbol": "AAPL", "currency": "USD"}
        _seed(fmp_dir, "profile_20260101T120000Z.json", payload, age=timedelta(days=30))
        _seed(fmp_dir, "profile_20260102T120000Z.json", payload, age=timedelta(days=29))
        _seed(fmp_dir, "profile_20260103T120000Z.json", payload, age=timedelta(days=28))

        result = compact_source("fmp", dry_run=False)

        assert result.files_scanned == 3
        # Net reduction: 3 duplicates → 1 survivor = 2 deletions.
        assert result.files_deleted == 2
        survivors = list(fmp_dir.iterdir())
        assert len(survivors) == 1
        # Survivor is hashed-filename format + canonical bytes.
        assert survivors[0].name.startswith("profile_")
        assert survivors[0].name.endswith(".json")
        # Canonical bytes match what a fresh helper write would hash.
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        assert survivors[0].read_bytes() == canonical

    def test_different_payloads_both_kept(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        _seed(fmp_dir, "profile_aaa_20260101T120000Z.json", {"x": 1}, age=timedelta(days=30))
        _seed(fmp_dir, "profile_aaa_20260102T120000Z.json", {"x": 2}, age=timedelta(days=29))

        result = compact_source("fmp", dry_run=False)

        assert result.files_deleted == 0
        assert len(list(fmp_dir.iterdir())) == 2

    def test_protected_files_untouched(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Files <24h old are excluded from BOTH keep-and-delete
        decisions entirely (r3-M5)."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"x": 1}
        _seed(fmp_dir, "profile_20260101T120000Z.json", payload, age=timedelta(hours=2))
        _seed(fmp_dir, "profile_20260102T120000Z.json", payload, age=timedelta(hours=3))

        result = compact_source("fmp", dry_run=False)

        # All 2 are protected → no-op group → files_scanned=2, deleted=0.
        assert result.files_scanned == 2
        assert result.files_deleted == 0
        assert len(list(fmp_dir.iterdir())) == 2

    def test_mix_protected_and_mutable(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When both protected and mutable copies exist, compaction
        picks from mutable set and leaves protected alone."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"x": 1}
        _seed(fmp_dir, "profile_a_20260101T120000Z.json", payload, age=timedelta(hours=2))  # protected
        _seed(fmp_dir, "profile_a_20260102T120000Z.json", payload, age=timedelta(days=5))  # mutable
        _seed(fmp_dir, "profile_a_20260103T120000Z.json", payload, age=timedelta(days=10))  # mutable

        result = compact_source("fmp", dry_run=False)

        # Mutable set reduced to 1; protected untouched.
        assert result.files_deleted == 1
        # 1 mutable survivor + 1 protected = 2 (or the survivor rewrite
        # targeting the hashed name overlaps the protected name — possible
        # but unlikely; count actual surviving file set).
        assert len(list(fmp_dir.iterdir())) == 2

    def test_dry_run_does_not_mutate(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"x": 1}
        _seed(fmp_dir, "profile_a_20260101T120000Z.json", payload, age=timedelta(days=30))
        _seed(fmp_dir, "profile_a_20260102T120000Z.json", payload, age=timedelta(days=29))

        result = compact_source("fmp", dry_run=True)

        # Would-delete reported, nothing deleted.
        assert result.files_deleted == 1
        assert result.dry_run is True
        assert len(list(fmp_dir.iterdir())) == 2

    def test_ignores_unparseable_filenames(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Files not matching hashed or legacy patterns are skipped
        entirely — no scanning, no hashing, no deletion."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        fmp_dir.mkdir()
        (fmp_dir / "random.json").write_text("{}")
        (fmp_dir / "README").write_text("")

        result = compact_source("fmp", dry_run=False)

        assert result.files_scanned == 0
        assert (fmp_dir / "random.json").exists()
        assert (fmp_dir / "README").exists()

    def test_survivor_mtime_preserved_after_rewrite(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """P1 regression — compaction's os.replace must NOT refresh
        the survivor's mtime, else age-based sweep would see it as
        'new' and never delete retention-expired files."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"x": 1}
        _seed(fmp_dir, "profile_20260101T120000Z.json", payload, age=timedelta(days=30))
        _seed(fmp_dir, "profile_20260102T120000Z.json", payload, age=timedelta(days=29))
        now = datetime.now(UTC)

        compact_source("fmp", dry_run=False)

        survivors = list(fmp_dir.iterdir())
        assert len(survivors) == 1
        survivor_mtime = datetime.fromtimestamp(survivors[0].stat().st_mtime, tz=UTC)
        age = now - survivor_mtime
        # Newest mutable was 29 days; survivor should carry that age
        # (± 1 minute for scheduling jitter).
        assert abs(age - timedelta(days=29)) < timedelta(minutes=1)

    def test_protected_target_skips_group(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """P2 regression — if the target hashed filename already
        exists AND is protected (<24h old), the entire group is
        skipped. Mutable duplicates survive for the next cycle."""
        import hashlib

        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"x": 1}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        hash16 = hashlib.sha256(canonical).hexdigest()[:16]
        hashed_name = f"profile_{hash16}.json"

        _seed(fmp_dir, hashed_name, payload, age=timedelta(hours=3))  # protected
        _seed(fmp_dir, "profile_20260101T120000Z.json", payload, age=timedelta(days=5))  # mutable

        result = compact_source("fmp", dry_run=False)

        # Group skipped — both files remain on disk.
        assert result.files_deleted == 0
        assert len(list(fmp_dir.iterdir())) == 2

    def test_ignores_hidden_files(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Hidden files (prefix '.') are skipped — they're either
        tmp leftovers or something the user placed intentionally."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        fmp_dir.mkdir()
        (fmp_dir / ".tmp_leftover").write_text("garbage")

        result = compact_source("fmp", dry_run=False)

        assert result.files_scanned == 0
        assert (fmp_dir / ".tmp_leftover").exists()


# ---------------------------------------------------------------------
# sweep_source
# ---------------------------------------------------------------------


class TestSweepSource:
    def test_none_retention_is_noop(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """sec_fundamentals policy has max_age_days=None → sweep never
        deletes regardless of file age."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        sec_dir = tmp_path / "sec_fundamentals"
        _seed(sec_dir, "sec_facts_AAA_20200101T120000Z.json", {"x": 1}, age=timedelta(days=365 * 5))

        result = sweep_source("sec_fundamentals", dry_run=False)

        assert result.files_deleted == 0
        assert len(list(sec_dir.iterdir())) == 1

    def test_deletes_files_older_than_policy(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """etoro policy has max_age_days=7 → files older than 7 days
        are deleted; newer preserved."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        etoro_dir = tmp_path / "etoro"
        _seed(etoro_dir, "old_20260101T120000Z.json", {"x": 1}, age=timedelta(days=10))
        _seed(etoro_dir, "fresh_20260102T120000Z.json", {"y": 2}, age=timedelta(days=3))

        result = sweep_source("etoro", dry_run=False)

        assert result.files_deleted == 1
        remaining = {p.name for p in etoro_dir.iterdir()}
        assert "fresh_20260102T120000Z.json" in remaining
        assert "old_20260101T120000Z.json" not in remaining

    def test_min_age_safeguard_preserves_under_24h(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Even if policy says delete, files <24h old are preserved."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        # fmp policy: 30 days. File is 31 days old AND 2 hours old — the 2h one is protected.
        _seed(fmp_dir, "recent_20260101T120000Z.json", {"x": 1}, age=timedelta(hours=2))
        _seed(fmp_dir, "old_20260102T120000Z.json", {"y": 2}, age=timedelta(days=31))

        result = sweep_source("fmp", dry_run=False)

        assert result.files_deleted == 1
        assert (fmp_dir / "recent_20260101T120000Z.json").exists()
        assert not (fmp_dir / "old_20260102T120000Z.json").exists()

    def test_dry_run_does_not_mutate(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        etoro_dir = tmp_path / "etoro"
        _seed(etoro_dir, "old_20260101T120000Z.json", {"x": 1}, age=timedelta(days=30))

        result = sweep_source("etoro", dry_run=True)

        assert result.files_deleted == 1
        assert result.dry_run is True
        assert (etoro_dir / "old_20260101T120000Z.json").exists()


# ---------------------------------------------------------------------
# needs_compaction
# ---------------------------------------------------------------------


class TestNeedsCompaction:
    def test_null_state_returns_true(self) -> None:
        state = RawPersistenceState(source="fmp")
        assert needs_compaction(state) is True

    def test_within_staleness_returns_false(self) -> None:
        now = datetime.now(UTC)
        state = RawPersistenceState(source="fmp", last_compacted_at=now - timedelta(days=3))
        assert needs_compaction(state, _now=now) is False

    def test_past_staleness_returns_true(self) -> None:
        now = datetime.now(UTC)
        state = RawPersistenceState(source="fmp", last_compacted_at=now - COMPACTION_STALENESS - timedelta(hours=1))
        assert needs_compaction(state, _now=now) is True


# ---------------------------------------------------------------------
# Scheduler drift guards
# ---------------------------------------------------------------------


class TestSchedulerWiring:
    def test_job_in_scheduled_jobs_and_invokers(self) -> None:
        """Registry drift guard — JOB_RAW_DATA_RETENTION_SWEEP appears
        in SCHEDULED_JOBS and is dispatchable via _INVOKERS."""
        from app.jobs.runtime import _INVOKERS
        from app.workers.scheduler import JOB_RAW_DATA_RETENTION_SWEEP, SCHEDULED_JOBS

        scheduled_names = {job.name for job in SCHEDULED_JOBS}
        assert JOB_RAW_DATA_RETENTION_SWEEP in scheduled_names
        assert JOB_RAW_DATA_RETENTION_SWEEP in _INVOKERS

    def test_catch_up_on_boot_is_false(self) -> None:
        """Restart must not trigger a catch-up rehash of 225 GB."""
        from app.workers.scheduler import JOB_RAW_DATA_RETENTION_SWEEP, SCHEDULED_JOBS

        job = next(j for j in SCHEDULED_JOBS if j.name == JOB_RAW_DATA_RETENTION_SWEEP)
        assert job.catch_up_on_boot is False

    def test_dry_run_default_is_false_under_issue_325(self) -> None:
        """Default is no-longer-dry-run — #325 flipped 2026-04-24.

        The retention sweep wrote zero deletions for weeks in dry-run
        mode, growing ``data/raw/`` past 30 GB. Flipped to False so
        age-sweep actually reclaims disk on the next daily run.
        Operators can still force dry-run for one cycle via env var
        ``EBULL_RAW_RETENTION_DRY_RUN=true``.
        """
        # Import Settings class directly — Settings() instantiation is
        # bypassed so environment overrides do not influence this test.
        from app.config import Settings

        defaults = Settings.model_fields["raw_retention_dry_run"]
        assert defaults.default is False


# ---------------------------------------------------------------------
# Integration: end-to-end dedup on real filesystem
# ---------------------------------------------------------------------


class TestEndToEnd:
    def test_15x_duplicate_reclaim(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Models the sec_fundamentals 15×-per-CIK duplication pattern.
        After compaction, exactly 1 file remains with canonical content."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        sec_dir = tmp_path / "sec_fundamentals"
        payload = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [1, 2, 3]}}}}}
        for i in range(15):
            # Filename must match legacy regex:
            # {tag}_YYYYMMDDTHHMMSSZ.json (exactly 8 digits + T + 6 digits + Z).
            _seed(
                sec_dir,
                f"sec_facts_0000320193_202604{(i % 9) + 1:02d}T12{i:02d}00Z.json",
                payload,
                age=timedelta(days=i + 1),
            )

        result = compact_source("sec_fundamentals", dry_run=False)

        assert result.files_scanned == 15
        assert result.files_deleted == 14
        survivors = list(sec_dir.iterdir())
        assert len(survivors) == 1
        # Survivor in hashed format.
        assert len(survivors[0].stem.split("_")[-1]) == 16

    def test_scheduler_dry_run_no_state_changes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dry-run mode must not write to raw_persistence_state or
        mutate the filesystem — operator needs a safe observation pass."""
        from app.workers import scheduler as scheduler_module

        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        fmp_dir = tmp_path / "fmp"
        payload = {"x": 1}
        _seed(fmp_dir, "profile_20260101T120000Z.json", payload, age=timedelta(days=30))
        _seed(fmp_dir, "profile_20260102T120000Z.json", payload, age=timedelta(days=29))

        # Stub settings.database_url + tracked_job + psycopg connection.
        # Focus: the *logic* path — does it call update_* in dry-run?
        fake_settings = MagicMock()
        fake_settings.database_url = "postgresql://test"
        fake_settings.raw_retention_dry_run = True
        monkeypatch.setattr(scheduler_module, "settings", fake_settings)
        tracked = MagicMock()
        monkeypatch.setattr(
            scheduler_module,
            "_tracked_job",
            MagicMock(return_value=MagicMock(__enter__=lambda self: tracked, __exit__=lambda *a: None)),
        )
        # load_state calls conn.execute(...).fetchone() → None means
        # "no state row yet" → needs_compaction returns True →
        # compact_source runs with dry_run=True (no fs changes).
        fake_conn = MagicMock()
        fake_conn.execute.return_value.fetchone.return_value = None
        monkeypatch.setattr(
            scheduler_module.psycopg,
            "connect",
            MagicMock(return_value=MagicMock(__enter__=lambda self: fake_conn, __exit__=lambda *a: None)),
        )

        scheduler_module.raw_data_retention_sweep()

        # Filesystem untouched.
        assert len(list(fmp_dir.iterdir())) == 2
        # No INSERT / UPDATE at all in dry-run mode. Bot pre-merge
        # review noted the previous "raw_persistence_state substring"
        # check was vacuous because SELECT also contains it; asserting
        # on INSERT specifically is the real property we care about.
        for call in fake_conn.execute.call_args_list:
            sql_text = str(call[0][0]) if call[0] else ""
            assert "INSERT" not in sql_text.upper(), f"dry-run must not write: {sql_text!r}"
            assert "UPDATE" not in sql_text.upper(), f"dry-run must not write: {sql_text!r}"

    def test_compact_raise_does_not_skip_sweep(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Bug caught pre-merge: scheduler's compact-phase exception
        handler used to `continue`, which silently suppressed the
        sweep phase for the same source. A recurring compaction
        error on any source would thus defeat retention forever."""
        from app.workers import scheduler as scheduler_module

        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        etoro_dir = tmp_path / "etoro"
        # Age-expired file that sweep should delete (etoro retention=7d).
        _seed(etoro_dir, "old_20260101T120000Z.json", {"x": 1}, age=timedelta(days=30))

        fake_settings = MagicMock()
        fake_settings.database_url = "postgresql://test"
        fake_settings.raw_retention_dry_run = False  # enforce mode
        monkeypatch.setattr(scheduler_module, "settings", fake_settings)
        tracked = MagicMock()
        monkeypatch.setattr(
            scheduler_module,
            "_tracked_job",
            MagicMock(return_value=MagicMock(__enter__=lambda self: tracked, __exit__=lambda *a: None)),
        )
        fake_conn = MagicMock()
        fake_conn.execute.return_value.fetchone.return_value = None
        monkeypatch.setattr(
            scheduler_module.psycopg,
            "connect",
            MagicMock(return_value=MagicMock(__enter__=lambda self: fake_conn, __exit__=lambda *a: None)),
        )

        # Monkeypatch compact_source to raise for every source.
        import app.services.raw_persistence as rp

        def boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("simulated compact failure")

        monkeypatch.setattr(rp, "compact_source", boom)

        scheduler_module.raw_data_retention_sweep()

        # Despite the compact exception, sweep ran for etoro and
        # deleted the age-expired file.
        assert not (etoro_dir / "old_20260101T120000Z.json").exists()
