"""#1328 — runbook log retention (``prune_old_runbook_logs``).

Pure filesystem logic (tmp_path, no DB). Backdates files via ``os.utime`` to
exercise the mtime cutoff, per the issue's acceptance criterion.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from app.runbooks.safety import RUNBOOK_LOG_RETENTION_DAYS, prune_old_runbook_logs

_DAY = 86400


def _touch(path: Path, *, age_days: float) -> None:
    path.write_text("{}\n")
    when = time.time() - age_days * _DAY
    os.utime(path, (when, when))


def test_deletes_only_logs_older_than_retention(tmp_path: Path) -> None:
    old = tmp_path / "run-1.jsonl"
    recent = tmp_path / "run-2.jsonl"
    _touch(old, age_days=RUNBOOK_LOG_RETENTION_DAYS + 1)  # 31d → stale
    _touch(recent, age_days=RUNBOOK_LOG_RETENTION_DAYS - 1)  # 29d → kept

    deleted = prune_old_runbook_logs(tmp_path)

    assert deleted == [old]
    assert not old.exists()
    assert recent.exists()


def test_boundary_exactly_retention_is_kept(tmp_path: Path) -> None:
    # mtime == cutoff is NOT older than cutoff (strict <), so a file aged
    # exactly retention_days survives. Pin a fixed clock so the file's mtime
    # and the cutoff derive from the SAME `now` — otherwise the microseconds
    # between stamping and the call tip strict-equality over the edge.
    now = 1_000_000_000.0
    edge = tmp_path / "edge.jsonl"
    edge.write_text("{}\n")
    mtime = now - RUNBOOK_LOG_RETENTION_DAYS * _DAY  # exactly the cutoff
    os.utime(edge, (mtime, mtime))

    deleted = prune_old_runbook_logs(tmp_path, now=now)

    assert edge.exists()
    assert deleted == []


def test_ignores_non_jsonl_files(tmp_path: Path) -> None:
    keep = tmp_path / "old.log"  # not *.jsonl
    _touch(keep, age_days=999)
    deleted = prune_old_runbook_logs(tmp_path)
    assert deleted == []
    assert keep.exists()


def test_missing_dir_is_a_noop_not_a_raise(tmp_path: Path) -> None:
    absent = tmp_path / "does-not-exist"
    # Fail-open: a rotation over an absent dir must not raise (it must never
    # break the runbook write it is bundled with).
    assert prune_old_runbook_logs(absent) == []


def test_custom_retention_days(tmp_path: Path) -> None:
    f = tmp_path / "x.jsonl"
    _touch(f, age_days=10)
    assert prune_old_runbook_logs(f.parent, retention_days=7) == [f]
    assert not f.exists()
