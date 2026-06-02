"""Canonical orphan test-DB reaper (#1444).

Single source of truth for the safety rails that drop leaked
``ebull_test_*`` (per-worker pytest DBs) and ``ebull_mig*``
(migration-replay temp DBs) databases. Lives under ``app/`` — NOT under
``tests/`` — so the jobs process can call it at boot + on a daily cadence
without importing test code (the ``app`` → ``tests`` dependency direction
is forbidden). The pytest fixture (``tests/fixtures/ebull_test_db.py``)
imports the constants from here and delegates its sweep to
``sweep_orphan_test_databases`` so the regex + protect-set never drift
between the two callers.

Why this matters (the incident this prevents): a SIGKILL'd xdist worker
or OOM-killed pytest run skips its teardown, orphaning a per-worker DB.
TRUNCATE-only worker-DB reuse means a test that ``CREATE``s relations
without dropping them accumulates relfiles for the whole session — one
such runaway left four ~6-10M-file DBs (~30M files) that turned the next
PG17 crash-recovery fsync pass into a multi-hour stall (2026-06-02). The
test-session-start sweep cannot clear them while PG is down
(chicken-and-egg). Running this from the long-lived jobs process — at
boot and daily — reaps orphans before the next crash.

Safety model (#1208 Phase 2 §4.3, reaffirmed by #1444):

* **Rail 1 — name regex.** Only ``ebull_test_<10-digit-epoch>_<hex>_<suffix>``
  (and ``ebull_mig*`` for the corpse reaper) ever match.
* **Rail 2 — activity.** Any backend in ``pg_stat_activity`` for the
  datname → skip. A live worker / sibling pytest run keeps a
  session-lifetime keepalive connected, so it is always seen and skipped.
* **Rail 3 — age.** ``now - parsed_epoch >= min_age`` (default 1h) is the
  backstop for leaked DBs whose backends have since drained.
* **Rail 0 — literal ``NEVER_DROP``** re-check immediately before the
  DROP; raises ``AssertionError`` on hit (regex-regression backstop).

Two drop mechanisms — and the line between them is load-bearing:

* **Live-capable orphans** (``datconnlimit=-1``) → ``sweep_orphan_test_databases``
  uses **plain ``DROP``**. The activity rail proves no backend only at
  SNAPSHOT time; a sibling pytest run can connect in the snapshot→DROP
  gap → ``ObjectInUse`` → skip + retry next invocation. NEVER ``WITH
  (FORCE)`` here: it would kill that sibling. ``REVOKE CONNECT`` is not a
  guard either — the owner / superusers bypass ``CONNECT`` privilege in a
  ``postgres``-user dev cluster (#1444 Codex review).
* **Corpses** (``datconnlimit=-2``) → ``force_drop_invalid_test_dbs`` uses
  ``DROP ... WITH (FORCE)``. PG refuses ALL connections to a corpse
  (superuser included), so there is no live sibling to evict and no age /
  activity rail is needed — FORCE is both safe and the only way to clear
  the wedged backend that interrupted the original drop.

Best-effort throughout: operational failures are logged + swallowed so a
reap never wedges the caller. ``AssertionError`` (Rail 0) is re-raised.
``CI=true`` short-circuits to a no-op (ephemeral CI containers never
accumulate orphans).
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse, urlunparse

import psycopg
import psycopg.errors
import psycopg.rows
from psycopg import sql

from app.config import settings

logger = logging.getLogger(__name__)

# DB-name layout produced by ``tests/fixtures/ebull_test_db.test_db_name()``:
#   ebull_test_{int(time.time())}_{token_hex(3)}_{worker_id}
# The 10-digit epoch holds until year 2286. Kept byte-identical to the
# fixture's historical pattern; the fixture now imports THIS definition.
ORPHAN_NAME_PATTERN = re.compile(
    r"^ebull_test_(?P<epoch>\d{10})_[0-9a-f]{6}_(?:gw\d+|main|sanity\d*)$",
)

# Final literal guard against a future regex regression. Every entry
# already fails ``ORPHAN_NAME_PATTERN``; belt-on-belt for the operator
# dev DB, the reusable template, and the maintenance DBs.
NEVER_DROP: frozenset[str] = frozenset(
    {
        "ebull",
        "ebull_test_template",
        "postgres",
        "template0",
        "template1",
    },
)


def _swap_database(url: str, new_db: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{new_db}"))


def admin_database_url() -> str:
    """URL for the maintenance ``postgres`` DB (CREATE/DROP/advisory locks)."""
    return _swap_database(settings.database_url, "postgres")


def _drop_database_force(admin: psycopg.Connection[object], db_name: str) -> None:
    """``DROP DATABASE ... WITH (FORCE)`` — evicts any connected backend.

    ONLY safe for ``datconnlimit=-2`` corpses, which PG refuses ALL
    connections to (superuser included) — so there is no live sibling to
    evict. NEVER use this on a normal (``datconnlimit=-1``) orphan: the
    activity rail only proves no backend at SNAPSHOT time, and a sibling
    pytest run can connect in the snapshot→drop gap; FORCE would kill it.
    REVOKE CONNECT is not a guard here either — the DB owner / superusers
    bypass CONNECT privilege in our ``postgres``-user dev cluster.
    """
    query = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name))
    with admin.cursor() as cur:
        cur.execute(query)


def sweep_orphan_test_databases(
    min_age: timedelta = timedelta(hours=1),
    *,
    now: datetime | None = None,
) -> list[str]:
    """Drop stale-named, inactive ``ebull_test_*`` orphan DBs. See module docstring.

    Uses plain ``DROP`` (never ``WITH (FORCE)``): the activity rail proves
    no backend only at snapshot time, so a sibling pytest run connecting
    in the snapshot→drop gap raises ``ObjectInUse`` and is skipped +
    retried next invocation, never evicted (#1208 eviction-without-proof
    invariant; reaffirmed by #1444 Codex review). Returns the dropped
    names; never raises except the Rail-0 ``AssertionError``.
    """
    if os.getenv("CI") == "true":
        return []
    try:
        return _do_sweep(min_age=min_age, now=now)
    except AssertionError:
        raise
    except Exception as exc:  # noqa: BLE001 — best-effort hygiene step
        logger.warning(
            "orphan test-DB sweep failed: %s: %s (cleanup deferred to next run)",
            type(exc).__name__,
            exc,
        )
        return []


def select_orphans_to_drop(
    candidates: Iterable[tuple[str, bool]],
    *,
    min_age: timedelta,
    now: datetime,
) -> list[str]:
    """Pure rail POLICY: from ``(datname, has_active_backend)`` rows, return
    the datnames eligible to drop. No IO — this is the unit-testable core of
    the sweep, separated from the SQL MECHANISM in ``_do_sweep`` so every
    rail combination can be covered in microseconds without creating real
    databases. Rails (see module docstring): activity → name regex → age →
    ``NEVER_DROP``.
    """
    threshold_epoch = int((now - min_age).timestamp())
    out: list[str] = []
    for datname, has_active_backend in candidates:
        if has_active_backend:  # Rail 2
            continue
        match = ORPHAN_NAME_PATTERN.match(datname)  # Rail 1
        if match is None:
            continue
        if int(match.group("epoch")) >= threshold_epoch:  # Rail 3
            continue
        if datname in NEVER_DROP:  # Rail 0
            continue
        out.append(datname)
    return out


def _do_sweep(*, min_age: timedelta, now: datetime | None) -> list[str]:
    resolved_now = now or datetime.now(UTC)
    dropped: list[str] = []

    with psycopg.connect(admin_database_url(), autocommit=True) as admin:
        with admin.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                "SELECT d.datname, "
                "       EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname) "
                "  FROM pg_database d WHERE d.datname LIKE 'ebull_test%'"
            )
            candidates = [(str(row[0]), bool(row[1])) for row in cur.fetchall()]

        for name in select_orphans_to_drop(candidates, min_age=min_age, now=resolved_now):
            # Belt: the pure policy already excludes NEVER_DROP; fail loud
            # if a regex regression ever let one through (#1208 Rail 0).
            assert name not in NEVER_DROP, f"Refusing to DROP protected database {name!r} — regex regression."
            try:
                # Plain DROP — never FORCE on a live-capable orphan.
                with admin.cursor() as cur:
                    cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
                dropped.append(name)
            except psycopg.errors.ObjectInUse:
                # A backend reconnected in the snapshot→DROP gap. Skip +
                # retry next invocation (expected under benign races).
                continue
            except Exception as exc:  # noqa: BLE001 — best-effort, per-DB
                logger.warning("failed to drop orphan %r: %s: %s", name, type(exc).__name__, exc)
                continue

    return dropped


# Dev-like environments where reaping leaked test DBs is appropriate. A
# production jobs process (app_env="prod") must NEVER connect to the admin
# DB to reap test databases — there are none there, and the intent must be
# unambiguous to a reviewer.
_DEV_LIKE_ENVS: frozenset[str] = frozenset({"dev", "test", "local"})


@dataclass(frozen=True)
class ReapResult:
    """Outcome of one ``run_orphan_test_db_reap`` pass."""

    skipped: bool
    invalid: list[str] = field(default_factory=list)
    orphans: list[str] = field(default_factory=list)

    @property
    def total_reaped(self) -> int:
        return len(self.invalid) + len(self.orphans)


def run_orphan_test_db_reap(min_age: timedelta = timedelta(hours=1)) -> ReapResult:
    """Jobs-process entrypoint: reap leaked test DBs (boot + daily cadence).

    Runs ONLY in a dev-like ``app_env`` (``dev`` / ``test`` / ``local``);
    in production it is a hard no-op that never opens a DB connection. In
    dev it force-drops ``datconnlimit=-2`` corpses then plain-DROP sweeps
    stale-named inactive orphans, so a leaked DB never survives to bloat
    the next crash-recovery fsync pass (#1444). Best-effort — the
    underlying reapers swallow operational failures.
    """
    if settings.app_env not in _DEV_LIKE_ENVS:
        logger.info("orphan test-DB reap skipped: app_env=%s", settings.app_env)
        return ReapResult(skipped=True)
    invalid = force_drop_invalid_test_dbs()
    orphans = sweep_orphan_test_databases(min_age)
    if invalid or orphans:
        logger.warning("reaped leaked test DBs (#1444): invalid=%r orphans=%r", invalid, orphans)
    return ReapResult(skipped=False, invalid=invalid, orphans=orphans)


def force_drop_invalid_test_dbs() -> list[str]:
    """Force-drop INVALID (``datconnlimit = -2``) test-owned DB corpses.

    PG marks a DB ``datconnlimit = -2`` when a ``DROP DATABASE`` is
    interrupted (SIGKILL'd worker, wedged ``WITH (FORCE)``). Such a corpse
    refuses ALL new connections, so there is no concurrent-run safety
    concern — no age or activity rail is needed; ``WITH (FORCE)`` is
    required because the wedged backend blocks a plain DROP. Targets
    ``ebull_test_*`` + ``ebull_mig*``; ``NEVER_DROP`` names are skipped.
    """
    if os.getenv("CI") == "true":
        return []
    dropped: list[str] = []
    try:
        with psycopg.connect(admin_database_url(), autocommit=True) as admin:
            with admin.cursor(row_factory=psycopg.rows.tuple_row) as cur:
                cur.execute(
                    "SELECT datname FROM pg_database "
                    "WHERE datconnlimit = -2 "
                    "AND (datname LIKE 'ebull!_test!_%' ESCAPE '!' "
                    "     OR datname LIKE 'ebull!_mig%' ESCAPE '!')"
                )
                names = [row[0] for row in cur.fetchall()]
            for name in names:
                if name in NEVER_DROP:
                    continue
                try:
                    _drop_database_force(admin, name)
                    dropped.append(name)
                except Exception as exc:  # noqa: BLE001 — best-effort, per-DB
                    logger.warning(
                        "failed to force-drop invalid test DB %r: %s: %s",
                        name,
                        type(exc).__name__,
                        exc,
                    )
    except Exception as exc:  # noqa: BLE001 — best-effort hygiene step
        logger.warning("invalid-test-DB reaper failed: %s: %s", type(exc).__name__, exc)
    return dropped
