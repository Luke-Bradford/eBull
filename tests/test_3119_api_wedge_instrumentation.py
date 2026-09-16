"""#3119 — served-build identity, the SIGUSR1 dump, and the wedge prober.

Pure-logic tier: no database, no starlette client harness, no real API. The one
behavioural property asserted here (an async handler answering while the AnyIO
threadpool is saturated) is built on a throwaway FastAPI app driven through an
in-process ASGI transport, so it stays off the db marker.

⚠ ``tests/conftest.py::_module_source_touches_db`` matches its marker strings as
plain SUBSTRINGS of this file's source, so naming one in prose — even to say
this module does not use it — evicts every test here from the fast pre-push
tier. The tell is pytest exit 5 with ``0 items`` under ``-m "not db"``.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import sys
import time

import anyio
import anyio.to_thread
import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from app.system import served_build
from app.system.git_identity import WATCHED_SUBTREE, app_tree_hash, head_commit, is_dirty
from scripts.probe_api_wedge import _parse_etime, dump_threads, freshness, probe

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


# ---------------------------------------------------------------------------
# git identity
# ---------------------------------------------------------------------------


def test_app_tree_hash_is_not_head_and_both_resolve() -> None:
    """The staleness comparand is the watched subtree, not HEAD.

    A docs- or test-only commit moves HEAD while correctly producing no reload,
    so a HEAD-based detector would flag most merges in this repo as stale.
    """
    head = head_commit()
    tree = app_tree_hash()
    assert head is not None and len(head) == 40
    assert tree is not None and len(tree) == 40
    assert tree != head
    assert WATCHED_SUBTREE == "app"


def test_git_reads_survive_a_hook_poisoned_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """``GIT_DIR`` from a hook must not redirect the read (#2658).

    Without the scrub these resolve against the hook's repository, which is a
    different checkout answering a question about this one.
    """
    monkeypatch.setenv("GIT_DIR", "/nonexistent/hook.git")
    monkeypatch.setenv("GIT_WORK_TREE", "/nonexistent")
    assert app_tree_hash() is not None
    assert head_commit() is not None


def test_is_dirty_distinguishes_clean_from_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean tree is ``False``; an unreadable one is ``None``, never ``False``.

    For ``status --porcelain``, empty output IS the answer ("clean"), so ``_git``
    must not collapse it into the failure value. A ``git status`` that timed out
    tells you nothing about the working tree, and reporting that as clean would
    hide the one case this field exists to expose.
    """
    assert is_dirty() in (True, False)

    monkeypatch.setattr("app.system.git_identity._git", lambda *args: "")
    assert is_dirty() is False

    monkeypatch.setattr("app.system.git_identity._git", lambda *args: None)
    assert is_dirty() is None


# ---------------------------------------------------------------------------
# served-build sidecar
# ---------------------------------------------------------------------------


def test_activate_writes_a_complete_sidecar(tmp_path) -> None:
    record = served_build.activate(tmp_path)
    assert record is not None

    on_disk = json.loads((tmp_path / served_build.SIDECAR_FILENAME).read_text(encoding="utf-8"))
    assert on_disk == record
    assert on_disk["pid"] == os.getpid()
    assert on_disk["app_tree"] == app_tree_hash()
    assert on_disk["dump_path"] == str(tmp_path / served_build.DUMP_FILENAME)
    assert isinstance(on_disk["faulthandler"], bool)


def test_activate_never_raises_on_an_unwritable_data_dir(tmp_path) -> None:
    """Observability must not be able to fail a boot."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("", encoding="utf-8")
    assert served_build.activate(blocker) is None


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="POSIX only")
def test_sigusr1_appends_a_thread_dump(tmp_path) -> None:
    """The whole point: a stack without root, which ``py-spy`` cannot give on macOS."""
    record = served_build.activate(tmp_path)
    assert record is not None and record["faulthandler"] is True

    dump = tmp_path / served_build.DUMP_FILENAME
    os.kill(os.getpid(), signal.SIGUSR1)

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not dump.read_bytes():
        time.sleep(0.01)

    text = dump.read_text(encoding="utf-8", errors="replace")
    assert "Current thread" in text or "Thread" in text
    assert "test_sigusr1_appends_a_thread_dump" in text


def test_uptime_is_monotonic_and_non_negative() -> None:
    """Wall clock is for the operator-facing timestamp only; an NTP step must
    not be able to produce a negative uptime."""
    first = served_build.uptime_s()
    second = served_build.uptime_s()
    assert 0.0 <= first <= second


# ---------------------------------------------------------------------------
# prober: freshness. Unknown is never fresh.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sidecar", "expected"),
    [
        (None, "unknown"),
        ({}, "unknown"),
        ({"app_tree": None}, "unknown"),
        ({"app_tree": 12345}, "unknown"),
        ({"app_tree": "0" * 40}, "STALE"),
    ],
)
def test_freshness_never_reports_unknown_as_fresh(sidecar, expected) -> None:
    verdict, _served, _ondisk = freshness(sidecar)
    assert verdict == expected


def test_freshness_matches_the_live_tree() -> None:
    verdict, served, ondisk = freshness({"app_tree": app_tree_hash()})
    assert verdict == "fresh"
    assert served == ondisk


def test_freshness_is_unknown_when_the_live_read_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both sides missing is ``unknown``, not a ``None == None`` match."""
    monkeypatch.setattr("scripts.probe_api_wedge.app_tree_hash", lambda: None)
    assert freshness({"app_tree": None})[0] == "unknown"
    assert freshness({"app_tree": "0" * 40})[0] == "unknown"


# ---------------------------------------------------------------------------
# prober: the signal guard. Every refusal below is a case where SIGUSR1 would
# have KILLED the target rather than dumped it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sidecar", "fragment"),
    [
        (None, "no sidecar"),
        ({"faulthandler": True}, "no pid"),
        ({"pid": "not-an-int", "faulthandler": True}, "no pid"),
        ({"pid": os.getpid(), "faulthandler": False}, "it would die"),
        ({"pid": 2**30, "faulthandler": True}, "is gone"),
    ],
)
def test_dump_threads_declines_rather_than_killing(sidecar, fragment) -> None:
    outcome = dump_threads(sidecar)
    assert fragment in outcome
    assert "SIGUSR1 sent" not in outcome


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="POSIX only")
def test_dump_threads_signals_a_registered_live_worker(tmp_path) -> None:
    record = served_build.activate(tmp_path)
    assert record is not None
    assert "SIGUSR1 sent" in dump_threads(record)


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="POSIX only")
def test_dump_threads_refuses_a_recycled_pid(tmp_path) -> None:
    """Liveness is not identity: a sidecar outlives the process it names.

    Both halves of the guard are exercised, because either alone can be
    defeated — a recycled pid may happen to run the same interpreter, and a
    long-lived unrelated process may be older than the record.
    """
    record = served_build.activate(tmp_path)
    assert record is not None

    wrong_executable = {**record, "executable": "/some/other/interpreter"}
    assert "pid reuse" in dump_threads(wrong_executable)

    # Same live pid, but a record claiming to describe a process started long
    # before this one — i.e. the pid was recycled since the record was written.
    ancient = {**record, "started_at": "2020-01-01T00:00:00+00:00"}
    assert "pid reuse" in dump_threads(ancient)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("45:30", 2730.0),
        ("01:02:03", 3723.0),
        ("2-03:04:05", 183845.0),
        ("07", 7.0),
        ("", None),
        ("not-a-time", None),
    ],
)
def test_parse_etime_handles_every_ps_format(raw, expected) -> None:
    """``ps -o etime=`` is a DURATION, so it carries no timezone — which is the
    reason it is preferred here over ``lstart``."""
    assert _parse_etime(raw) == expected


def test_probe_reports_no_response_when_the_peer_never_answers() -> None:
    """The deadline must bound the WHOLE request, not each socket operation.

    ``urlopen(timeout=...)`` bounds individual blocking calls, so a peer that
    accepts and then says nothing can overrun it. A listening socket with no
    accept loop reproduces exactly the 2026-09-16 shape.
    """
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    try:
        started = time.monotonic()
        result = probe(f"http://127.0.0.1:{port}", "/health/live", 0.5)
        elapsed = time.monotonic() - started
    finally:
        listener.close()

    assert result.status is None
    assert not result.answered
    assert elapsed < 5.0


# ---------------------------------------------------------------------------
# The discriminator's one manufacturable row: loop alive, sync path starved.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_async_endpoint_answers_while_the_threadpool_is_saturated() -> None:
    """Row 2 of ``/health/live`` vs ``/health``, asserted rather than claimed.

    FastAPI dispatches ``def`` handlers through the AnyIO worker threadpool and
    ``async def`` handlers on the loop. With every thread token held, the sync
    handler cannot start while the async one still answers — which is why
    ``/health/live`` had to be ``async def`` with no dependency, and why the
    operator's 2026-09-16 probe set (``/health`` and ``/auth/login``, both
    ``def``) could not tell this state from a blocked loop.
    """
    app = FastAPI()

    @app.get("/live")
    async def live() -> dict:  # pragma: no cover - exercised via ASGI
        return {"alive": True}

    @app.get("/sync")
    def sync_endpoint() -> dict:  # pragma: no cover - never gets to run
        return {"ok": True}

    limiter = anyio.to_thread.current_default_thread_limiter()
    original_tokens = limiter.total_tokens
    limiter.total_tokens = 1
    transport = ASGITransport(app=app)

    held = anyio.Event()
    release = anyio.Event()

    async def hold_the_only_token() -> None:
        # A CapacityLimiter token is owned by the BORROWING TASK, so the token
        # has to be held from a different task than the one making the
        # requests — holding it here and then awaiting in the same task raises
        # "this borrower is already holding one of this CapacityLimiter's
        # tokens" instead of blocking, which would assert nothing.
        async with limiter:
            held.set()
            await release.wait()

    try:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(hold_the_only_token)
            await held.wait()

            async with httpx.AsyncClient(transport=transport, base_url="http://wedge.test") as client:
                response = await client.get("/live")
                assert response.status_code == 200
                assert response.json() == {"alive": True}

                with pytest.raises(TimeoutError):
                    with anyio.fail_after(0.5):
                        await client.get("/sync")

                release.set()

                # Token returned: the sync path recovers, so the starvation
                # above was the limiter and not a broken app.
                assert (await client.get("/sync")).status_code == 200
    finally:
        limiter.total_tokens = original_tokens


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_main_module_registers_activation_before_migrations() -> None:
    """Placement is load-bearing: a startup that hangs is one of the shapes the
    2026-09-16 wedge could have been, and registering after the expensive work
    would leave exactly that shape uncovered."""
    source = (
        __import__("pathlib").Path(sys.modules["app.main"].__file__).read_text(encoding="utf-8")
        if "app.main" in sys.modules
        else None
    )
    if source is None:
        import pathlib

        source = (pathlib.Path(__file__).resolve().parents[1] / "app" / "main.py").read_text(encoding="utf-8")

    activate_at = source.index("served_build.activate")
    migrations_at = source.index("Running pending migrations")
    assert activate_at < migrations_at
