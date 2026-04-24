"""Shared raw-persistence helper (#268 follow-up, Plan A PR 1).

Centralises the write path for every provider that persists raw
upstream responses under ``data/raw/{source}/``. Replaces per-provider
``_persist_raw`` implementations in PR 2; this PR only ships the
helper + unit tests with no behaviour change.

Design: ``docs/superpowers/specs/2026-04-18-raw-data-housekeeping-design.md``.

Invariants:

- **Best-effort write:** any filesystem error (permission, disk-full,
  transient FS issue) MUST log + return ``None``. The DB is the source
  of truth; raw files are audit trail only. A failing raw write must
  never fail the calling provider's sync flow.
- **Drift guard:** ``source`` must be present in ``_RETENTION_POLICY``.
  Unknown sources raise ``KeyError`` (intentionally propagated, not
  swallowed) so a new provider that forgets to add a policy entry
  fails loudly at test time rather than silently hoarding raw files.
- **Deterministic hash:** ``_canonicalise_for_hash`` produces the same
  bytes for the same logical payload regardless of caller-side
  serialisation format. Used identically by ``persist_raw_if_new``
  (write path) and compaction (PR 3), so both paths compute matching
  hashes and dedup across format differences.
- **Atomic write:** tempfile + ``os.replace`` so a crash mid-write
  never leaves a zero-byte target poisoning future dedup.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

# Module-level constant so tests can monkeypatch to a tmp_path without
# writing to the real ``data/raw/`` tree.
_DATA_ROOT = Path("data/raw")


# ---------------------------------------------------------------------
# Per-source retention policy
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class RetentionPolicy:
    """Per-source raw-file retention rules.

    ``max_age_days``: delete files older than this many days. ``None``
    = retain forever (subject only to exact-duplicate compaction).

    ``max_duplicate_files_per_hash``: how many files per unique content
    hash to keep. ``1`` means "compact exact duplicates only";
    historical snapshots of content that genuinely changed are
    preserved (they hash differently). ``None`` means "never compact
    by hash". v1 uses 1 for every source.
    """

    max_age_days: int | None
    max_duplicate_files_per_hash: int | None


# Per-source rationale:
# - sec_fundamentals / sec: NO new raw writes (#470). All structured
#   fields from companyfacts.json, submissions.json, and filing-index
#   JSON land in SQL.
# - etoro / etoro_broker: NO new raw writes (#471). Instruments,
#   candles, quotes, and broker portfolio all land in SQL via the
#   existing pipeline (instruments / price_daily / quotes /
#   broker_positions / cash_ledger / copy_mirror_positions).
# - All four sources kept in the policy map at max_age_days=0 so
#   ``sweep_source`` still works for the residual cleanup —
#   removing them would KeyError on sweep.
# - companies_house: NO age-based delete. Coverage is thinner than
#   SEC so the raw Companies House payloads are still the parser
#   substrate for some fields.
# - etoro: 7 days covers OHLCV candles (redundant with price_daily),
#   instrument lists, quote batches, and error bodies collectively.
#   If a longer diagnostic trail is ever needed, split etoro into
#   etoro_market + etoro_diagnostics with different policies.
# - etoro_broker: 90 days rolling — broker-state audit for
#   reconciling position / cash discrepancies.
# - fmp: fallback only; 30 days is enough for re-derivation.
_RETENTION_POLICY: dict[str, RetentionPolicy] = {
    # sec_fundamentals / sec: no new writes (providers stopped
    # calling persist_raw_if_new under #470). Retention at 0 so the
    # next sweep reclaims the ~12 GB of prior writes on disk.
    "sec_fundamentals": RetentionPolicy(max_age_days=0, max_duplicate_files_per_hash=1),
    "sec": RetentionPolicy(max_age_days=0, max_duplicate_files_per_hash=1),
    # etoro / etoro_broker: providers stopped writing raw under #471
    # — instruments / candles / quotes / portfolio all land in SQL
    # via the existing pipeline. Policy at 0 so the next sweep
    # reclaims residual files past the 24-hour safeguard.
    "etoro": RetentionPolicy(max_age_days=0, max_duplicate_files_per_hash=1),
    "etoro_broker": RetentionPolicy(max_age_days=0, max_duplicate_files_per_hash=1),
    "fmp": RetentionPolicy(max_age_days=30, max_duplicate_files_per_hash=1),
    "companies_house": RetentionPolicy(max_age_days=None, max_duplicate_files_per_hash=1),
}


# ---------------------------------------------------------------------
# Canonicalisation (used by both write path + compaction)
# ---------------------------------------------------------------------


def _canonicalise_for_hash(payload: object) -> bytes:
    """Deterministic bytes for hashing + atomic write.

    Same function is called by both ``persist_raw_if_new`` (write) and
    ``compact_source`` (compaction, PR 3). Any two payloads that are
    logically equivalent produce the same hash regardless of which
    code path wrote them or what format the caller passed in.

    Accepts ``dict | list | str | bytes``:

    - ``dict`` / ``list``: canonical JSON
      (``sort_keys=True, separators=(",", ":")``).
    - ``str``: try ``json.loads`` — if it parses, canonicalise the
      parsed value; else UTF-8 encode the raw string. Covers
      ``exc.response.text`` paths where the error body may or may
      not be valid JSON.
    - ``bytes``: try ``json.loads`` — if it parses, canonicalise the
      parsed value; else pass through. Covers ``response.content``
      paths (eToro broker) where bytes may or may not be JSON.
    """
    if isinstance(payload, dict | list):
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return payload.encode("utf-8")
        return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")

    if isinstance(payload, bytes):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError, UnicodeDecodeError:
            return payload
        return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")

    # JSON scalars (None, bool, int, float) — match the original
    # ``_persist_raw`` per-provider behaviour which used plain
    # ``json.dumps(payload)``. Upstream APIs sometimes return bare
    # JSON ``null`` with a 200 response; pre-migration that would
    # persist successfully. Scalars are already deterministic so no
    # canonicalisation beyond ``json.dumps`` is needed.
    if payload is None or isinstance(payload, bool | int | float):
        return json.dumps(payload).encode("utf-8")

    raise TypeError(f"persist_raw_if_new: unsupported payload type {type(payload).__name__}")


# ---------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------


def persist_raw_if_new(
    source: str,
    tag: str,
    payload: object,
) -> Path | None:
    """Write ``payload`` under
    ``data/raw/{source}/{tag}_{sha256[:16]}.json`` iff a file with
    the same content does not already exist.

    Returns the path written, or ``None`` on dedup hit or write
    failure. Never raises on filesystem errors (best-effort contract).

    ``KeyError`` for unknown ``source`` IS raised — the drift guard
    must surface loudly so new providers don't silently hoard.
    """
    # Drift guard — propagates. Runs BEFORE any filesystem contact so
    # a new provider fails at test time with a clear error rather
    # than crashing under a permissions mask later.
    if source not in _RETENTION_POLICY:
        raise KeyError(
            f"persist_raw_if_new: unknown source {source!r}. Add a retention "
            f"policy entry to _RETENTION_POLICY before calling."
        )

    # Everything touching the filesystem is inside this try. A
    # disk-full / permission / transient-FS error at any step must
    # log + return None, never escape into provider sync code.
    try:
        body = _canonicalise_for_hash(payload)
        digest = hashlib.sha256(body).hexdigest()[:16]
        dir_ = _DATA_ROOT / source
        dir_.mkdir(parents=True, exist_ok=True)
        target = dir_ / f"{tag}_{digest}.json"

        if target.exists():
            logger.debug("persist_raw_if_new: dedup hit %s", target.name)
            return None

        fd, tmp_path = tempfile.mkstemp(dir=dir_, prefix=f".{tag}_", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(body)
            os.replace(tmp_path, target)
        except OSError:
            # Clean up tmp on inner failure, then re-raise to the
            # outer handler so its best-effort contract still holds.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except OSError:
        logger.warning(
            "persist_raw_if_new: write failed source=%s tag=%s",
            source,
            tag,
            exc_info=True,
        )
        return None
    return target


# ---------------------------------------------------------------------
# Compaction — content-hash dedup across the existing 117k files
# ---------------------------------------------------------------------


# Files younger than this are never touched by compaction or sweep —
# an in-flight sync may still be reading them for retry.
MIN_AGE_FOR_MUTATION = timedelta(hours=24)

# How often the scheduler re-runs compaction per source. Age-based
# sweep still runs daily — it's a cheap mtime glob.
COMPACTION_STALENESS = timedelta(days=7)

# Match ``{tag}_{16_hex_chars}.json`` — the post-compaction layout.
# Survivors with this pattern can skip a second canonicalisation +
# rehash on subsequent runs.
_HASHED_FILENAME_RE = re.compile(r"^(?P<tag>.+)_(?P<hash>[0-9a-f]{16})\.json$")

# Match legacy ``{tag}_{YYYYMMDDTHHMMSSZ}.json`` written by the
# pre-migration per-provider ``_persist_raw`` implementations.
_LEGACY_FILENAME_RE = re.compile(r"^(?P<tag>.+)_\d{8}T\d{6}Z\.json$")


@dataclass(frozen=True)
class CompactionResult:
    """Outcome of one ``compact_source`` call."""

    source: str
    files_scanned: int
    files_deleted: int
    bytes_reclaimed: int
    elapsed_seconds: float
    dry_run: bool


@dataclass(frozen=True)
class SweepResult:
    """Outcome of one ``sweep_source`` call."""

    source: str
    files_deleted: int
    bytes_reclaimed: int
    elapsed_seconds: float
    dry_run: bool


@dataclass(frozen=True)
class RawPersistenceState:
    """Row from ``raw_persistence_state`` table."""

    source: str
    last_compacted_at: datetime | None = None
    last_compaction_files_scanned: int | None = None
    last_compaction_bytes_reclaimed: int | None = None
    last_sweep_at: datetime | None = None


def _parse_tag_prefix(filename: str) -> str | None:
    """Extract the tag prefix from a raw filename.

    Returns the tag portion for both post-migration hashed names
    (``{tag}_{16hex}.json``) and legacy timestamped names
    (``{tag}_{YYYYMMDDTHHMMSSZ}.json``). Returns ``None`` for
    files that match neither pattern (e.g. sidecar files, tmp
    leftovers); callers skip these entirely to avoid accidental
    deletion of unrelated content.
    """
    m = _HASHED_FILENAME_RE.match(filename)
    if m:
        return m.group("tag")
    m = _LEGACY_FILENAME_RE.match(filename)
    if m:
        return m.group("tag")
    return None


def _read_or_none(path: Path) -> bytes | None:
    """Read file bytes, returning None on any OSError so scans
    tolerate race-with-delete + permission transients. Callers treat
    None as "skip this file"."""
    try:
        return path.read_bytes()
    except OSError:
        logger.warning("compact_source: read failed %s", path, exc_info=True)
        return None


def compact_source(
    source: str,
    *,
    dry_run: bool = True,
    _now: datetime | None = None,
) -> CompactionResult:
    """Dedup ``data/raw/{source}/`` by content hash.

    Algorithm:
    1. Acquire a pg advisory lock scoped to the source. On contention
       return ``CompactionResult(skipped=True)`` immediately — no fs
       mutations, scheduler must not advance state.
    2. Walk ``data/raw/{source}/*.json``, parse tag prefix, read bytes,
       canonicalise via ``_canonicalise_for_hash`` (identical to
       write path), hash.
    3. Group by ``(tag_prefix, hash16)``. Files younger than 24h are
       excluded from both keep-and-delete decisions entirely.
    4. For each group: keep newest-mtime mutable file; rewrite it
       under canonical hashed filename; delete siblings.

    ``_now`` is injectable for deterministic age checks in tests.

    ``dry_run=True`` counts would-delete but performs no mutations.

    Best-effort semantics: any OSError on a single file is logged
    and that file skipped, but the overall pass continues.

    Concurrency: serialised at the job level via ``_tracked_job``'s
    APScheduler lock. No per-source advisory locks at this layer
    — scheduler ensures only one ``raw_data_retention_sweep`` runs
    at a time and this function is only called from that path.

    mtime preservation: after ``os.replace`` onto the hashed target,
    the survivor's ORIGINAL mtime is restored via ``os.utime`` so
    age-based sweep in the same scheduler cycle sees the file as
    its pre-compaction age. Without this, compaction would reset
    age to zero and defeat sweep for the full 7-day staleness
    window (Codex pre-push P1).

    Protected-target safeguard: if the hashed-target filename
    already exists AND is <24h old, the entire group is skipped —
    mutable duplicates are NOT deleted for that pass either, to
    uphold the "never touch protected files" invariant. They are
    revisited in the next compaction cycle once the target has
    aged past 24h (Codex pre-push P2).
    """
    started = time.monotonic()
    now = _now or datetime.now(UTC)
    dir_ = _DATA_ROOT / source
    if not dir_.exists():
        return CompactionResult(
            source=source,
            files_scanned=0,
            files_deleted=0,
            bytes_reclaimed=0,
            elapsed_seconds=time.monotonic() - started,
            dry_run=dry_run,
        )

    # Groups keyed by (tag_prefix, hash16). Each value is a list of
    # (path, mtime_datetime, size_bytes).
    groups: dict[tuple[str, str], list[tuple[Path, datetime, int]]] = {}
    # Canonical bytes by (tag_prefix, hash16) so rewrite step doesn't
    # re-canonicalise the survivor.
    canonical_by_group: dict[tuple[str, str], bytes] = {}
    files_scanned = 0

    for entry in dir_.iterdir():
        if not entry.is_file():
            continue
        if entry.name.startswith("."):
            continue  # tmp leftover or hidden
        tag_prefix = _parse_tag_prefix(entry.name)
        if tag_prefix is None:
            continue  # unrecognised filename — leave alone

        files_scanned += 1
        raw = _read_or_none(entry)
        if raw is None:
            continue
        try:
            canonical = _canonicalise_for_hash(raw)
        except TypeError:
            logger.warning("compact_source: uncanonicalisable %s", entry.name)
            continue
        digest = hashlib.sha256(canonical).hexdigest()[:16]

        try:
            st = entry.stat()
        except OSError:
            continue
        mtime = datetime.fromtimestamp(st.st_mtime, tz=UTC)

        key = (tag_prefix, digest)
        groups.setdefault(key, []).append((entry, mtime, st.st_size))
        canonical_by_group[key] = canonical

    files_deleted = 0
    bytes_reclaimed = 0

    for (tag_prefix, hash16), members in groups.items():
        protected = [m for m in members if now - m[1] < MIN_AGE_FOR_MUTATION]
        mutable = [m for m in members if m not in protected]
        if not mutable:
            # Only protected copies exist — no-op this group; retry
            # next cycle when they age past the 24h safeguard.
            continue

        # Newest mtime wins among mutables. Also remember the
        # survivor's mtime so we can restore it after the rewrite
        # (P1 — prevent compaction from refreshing age and defeating
        # downstream sweep).
        survivor_path, survivor_mtime, _ = max(mutable, key=lambda m: m[1])
        target = dir_ / f"{tag_prefix}_{hash16}.json"

        # P2 safeguard: if the target path already exists AND is
        # protected, the entire group is untouchable this cycle —
        # rewriting target would violate the "never touch protected"
        # invariant. Mutable duplicates stay for the next cycle.
        if target.exists():
            try:
                target_mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=UTC)
            except OSError:
                target_mtime = None
            if target_mtime is not None and now - target_mtime < MIN_AGE_FOR_MUTATION:
                continue

        # Count net reduction per group — len(mutable) - 1 files
        # removed, since we always end up with exactly 1 at target.
        # This matches operator intuition: a pure legacy→hashed
        # rename with no duplicates isn't a "deletion". A group of
        # 3 duplicates is 2 deletions.
        net_reduction = max(len(mutable) - 1, 0)
        # Bytes reclaimed ≈ total bytes of pre-existing copies minus
        # target's bytes. We already have all member sizes; target
        # size = len(canonical) post-rewrite.
        canonical = canonical_by_group[(tag_prefix, hash16)]
        total_member_bytes = sum(size for _, _, size in mutable)
        bytes_freed = max(total_member_bytes - len(canonical), 0)

        if not dry_run:
            # Rewrite survivor under canonical hashed filename.
            try:
                fd, tmp_path = tempfile.mkstemp(dir=dir_, prefix=f".{tag_prefix}_", suffix=".tmp")
                try:
                    with os.fdopen(fd, "wb") as f:
                        f.write(canonical)
                    os.replace(tmp_path, target)
                    # Restore survivor's original mtime so age-based
                    # sweep in the same scheduler cycle (or a later
                    # one) sees the file as its true age. Without this,
                    # compaction resets age to zero and defeats sweep
                    # for every file compacted (Codex P1).
                    ts = survivor_mtime.timestamp()
                    try:
                        os.utime(target, (ts, ts))
                    except OSError:
                        logger.warning(
                            "compact_source: utime restore failed for %s",
                            target,
                            exc_info=True,
                        )
                except OSError:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
                    logger.warning("compact_source: rewrite failed %s", target, exc_info=True)
                    continue
            except OSError:
                logger.warning("compact_source: mkstemp failed for %s", target, exc_info=True)
                continue

            # Delete every member whose path differs from the target.
            # If the survivor's OLD path == target (already hashed),
            # it's skipped (but os.replace already wrote canonical
            # bytes to it, which is a no-op if identical).
            for path, _, _size in mutable:
                if path == target:
                    continue
                try:
                    path.unlink()
                except OSError:
                    logger.warning("compact_source: unlink failed %s", path, exc_info=True)

        files_deleted += net_reduction
        bytes_reclaimed += bytes_freed

    return CompactionResult(
        source=source,
        files_scanned=files_scanned,
        files_deleted=files_deleted,
        bytes_reclaimed=bytes_reclaimed,
        elapsed_seconds=time.monotonic() - started,
        dry_run=dry_run,
    )


def sweep_source(
    source: str,
    *,
    dry_run: bool = True,
    _now: datetime | None = None,
) -> SweepResult:
    """Age-based deletion of files older than
    ``_RETENTION_POLICY[source].max_age_days``.

    No-op when the policy is ``max_age_days=None``. Min-age safeguard
    (24h) still applies regardless of policy.
    """
    started = time.monotonic()
    now = _now or datetime.now(UTC)
    policy = _RETENTION_POLICY[source]
    if policy.max_age_days is None:
        return SweepResult(
            source=source,
            files_deleted=0,
            bytes_reclaimed=0,
            elapsed_seconds=time.monotonic() - started,
            dry_run=dry_run,
        )

    dir_ = _DATA_ROOT / source
    if not dir_.exists():
        return SweepResult(
            source=source,
            files_deleted=0,
            bytes_reclaimed=0,
            elapsed_seconds=time.monotonic() - started,
            dry_run=dry_run,
        )

    cutoff = now - timedelta(days=policy.max_age_days)
    min_cutoff = now - MIN_AGE_FOR_MUTATION
    files_deleted = 0
    bytes_reclaimed = 0

    for entry in dir_.iterdir():
        if not entry.is_file():
            continue
        if entry.name.startswith("."):
            continue
        try:
            st = entry.stat()
        except OSError:
            continue
        mtime = datetime.fromtimestamp(st.st_mtime, tz=UTC)
        if mtime >= min_cutoff:
            continue  # protected <24h
        if mtime >= cutoff:
            continue  # still within retention window
        if dry_run:
            files_deleted += 1
            bytes_reclaimed += st.st_size
            continue
        try:
            entry.unlink()
            files_deleted += 1
            bytes_reclaimed += st.st_size
        except OSError:
            logger.warning("sweep_source: unlink failed %s", entry, exc_info=True)

    return SweepResult(
        source=source,
        files_deleted=files_deleted,
        bytes_reclaimed=bytes_reclaimed,
        elapsed_seconds=time.monotonic() - started,
        dry_run=dry_run,
    )


# ---------------------------------------------------------------------
# State table helpers
# ---------------------------------------------------------------------


def load_state(conn: psycopg.Connection[Any], source: str) -> RawPersistenceState:
    """Return the state row for ``source``, or a fresh default when
    no row exists yet. Never raises on missing state — callers
    interpret ``last_compacted_at IS None`` as "never compacted"."""
    row = conn.execute(
        """
        SELECT last_compacted_at,
               last_compaction_files_scanned,
               last_compaction_bytes_reclaimed,
               last_sweep_at
        FROM raw_persistence_state
        WHERE source = %s
        """,
        (source,),
    ).fetchone()
    conn.commit()  # close read-tx per service-wide durability invariant.
    if row is None:
        return RawPersistenceState(source=source)
    return RawPersistenceState(
        source=source,
        last_compacted_at=row[0],
        last_compaction_files_scanned=int(row[1]) if row[1] is not None else None,
        last_compaction_bytes_reclaimed=int(row[2]) if row[2] is not None else None,
        last_sweep_at=row[3],
    )


def update_compaction_state(conn: psycopg.Connection[Any], source: str, result: CompactionResult) -> None:
    """Record compaction outcome in ``raw_persistence_state``."""
    conn.commit()  # M1 invariant.
    conn.execute(
        """
        INSERT INTO raw_persistence_state
            (source, last_compacted_at, last_compaction_files_scanned,
             last_compaction_bytes_reclaimed)
        VALUES (%s, NOW(), %s, %s)
        ON CONFLICT (source) DO UPDATE SET
            last_compacted_at              = EXCLUDED.last_compacted_at,
            last_compaction_files_scanned  = EXCLUDED.last_compaction_files_scanned,
            last_compaction_bytes_reclaimed = EXCLUDED.last_compaction_bytes_reclaimed
        """,
        (source, result.files_scanned, result.bytes_reclaimed),
    )
    conn.commit()


def update_sweep_state(conn: psycopg.Connection[Any], source: str) -> None:
    """Record that a sweep pass completed for ``source``."""
    conn.commit()
    conn.execute(
        """
        INSERT INTO raw_persistence_state (source, last_sweep_at)
        VALUES (%s, NOW())
        ON CONFLICT (source) DO UPDATE SET
            last_sweep_at = EXCLUDED.last_sweep_at
        """,
        (source,),
    )
    conn.commit()


def needs_compaction(state: RawPersistenceState, *, _now: datetime | None = None) -> bool:
    """True when ``state`` indicates compaction is due — never run
    before, or last run older than ``COMPACTION_STALENESS``."""
    if state.last_compacted_at is None:
        return True
    now = _now or datetime.now(UTC)
    return now - state.last_compacted_at > COMPACTION_STALENESS
