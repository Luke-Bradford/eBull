"""#1409 P5 Step 5.2 — companyfacts bulk ingester emits stage progress.

Before P5 the companyfacts stage showed ``0 / -`` for its entire (long)
runtime — no target, no tick — so an operator could not distinguish a
healthy multi-minute archive walk from a wedged one. This test pins the
wiring: inside a bootstrap dispatch (``resolve_progress_context`` returns
non-None) the ingester sets ``target_count`` to the CIK-entry count up
front and ticks ``processed_count`` per entry seen.

Spy pattern mirrors tests/test_s14_uses_sidecar.py: the set_stage_*
helpers open their own connection, so we monkeypatch them to record
calls rather than asserting a cross-connection DB write.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import psycopg
import pytest

from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable"),
]


def _bind_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    monkeypatch.setattr(app_settings, "database_url", test_database_url())


def _write_archive(tmp_path: Path, ciks: list[str]) -> Path:
    archive = tmp_path / "companyfacts.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        for cik in ciks:
            # Empty `facts` → extractor yields no facts → entry is SEEN +
            # ticked but raises _SkipEntry before any upsert (no FK row
            # needed). We are exercising the progress plumbing only.
            zf.writestr(f"CIK{cik}.json", json.dumps({"cik": int(cik), "facts": {}}))
        # A non-CIK junk entry must NOT count toward the target.
        zf.writestr("readme.txt", "ignore me")
    return archive


def test_companyfacts_sets_target_and_ticks_processed_in_bootstrap(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_settings(monkeypatch)
    archive = _write_archive(tmp_path, ["0000320193", "0000789019", "0000051143"])

    target_calls: list[dict] = []
    processed_calls: list[dict] = []
    import app.services.sec_companyfacts_ingest as mod

    monkeypatch.setattr(mod, "set_stage_target", lambda **kw: target_calls.append(kw) or 1)
    monkeypatch.setattr(mod, "set_stage_processed", lambda **kw: processed_calls.append(kw) or 1)

    # Drive the dispatch contextvar so resolve_progress_context() resolves.
    with active_bootstrap_run(run_id=4242, stage_key="sec_companyfacts_ingest"):
        mod.ingest_companyfacts_archive(
            conn=ebull_test_conn,
            archive_path=archive,
            cik_to_instrument={},  # all universe-gap; still seen + ticked
        )

    # target_count = number of CIK<10>.json entries (3); junk entry excluded.
    assert len(target_calls) == 1
    assert target_calls[0]["run_id"] == 4242
    assert target_calls[0]["stage_key"] == "sec_companyfacts_ingest"
    assert target_calls[0]["target_count"] == 3

    # processed_count ticked and the final value reached the target.
    assert processed_calls, "no set_stage_processed tick — stage would show 0/N forever"
    assert processed_calls[-1]["processed_count"] == 3
    # Absolute (monotonic non-decreasing), never exceeds target.
    seq = [c["processed_count"] for c in processed_calls]
    assert seq == sorted(seq)
    assert seq[-1] <= target_calls[0]["target_count"]


def test_companyfacts_no_progress_writes_outside_bootstrap(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Manual-fire path (no dispatch context) → zero progress side-effects."""
    _bind_settings(monkeypatch)
    archive = _write_archive(tmp_path, ["0000320193"])

    target_calls: list[dict] = []
    processed_calls: list[dict] = []
    import app.services.sec_companyfacts_ingest as mod

    monkeypatch.setattr(mod, "set_stage_target", lambda **kw: target_calls.append(kw) or 1)
    monkeypatch.setattr(mod, "set_stage_processed", lambda **kw: processed_calls.append(kw) or 1)

    # No active_bootstrap_run wrapper → resolve_progress_context() is None.
    mod.ingest_companyfacts_archive(
        conn=ebull_test_conn,
        archive_path=archive,
        cik_to_instrument={},
    )

    assert target_calls == []
    assert processed_calls == []
