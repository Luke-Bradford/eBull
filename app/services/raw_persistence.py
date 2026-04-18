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
import tempfile
from dataclasses import dataclass
from pathlib import Path

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
# - sec_fundamentals / sec / companies_house: NO age-based delete.
#   Raw bodies are small post-dedup; re-fetching SEC is rate-limited
#   + slow; reproducible audit trail costs nothing.
# - etoro: 7 days covers OHLCV candles (redundant with price_daily),
#   instrument lists, quote batches, and error bodies collectively.
#   If a longer diagnostic trail is ever needed, split etoro into
#   etoro_market + etoro_diagnostics with different policies.
# - etoro_broker: 90 days rolling — broker-state audit for
#   reconciling position / cash discrepancies.
# - fmp: fallback only; 30 days is enough for re-derivation.
_RETENTION_POLICY: dict[str, RetentionPolicy] = {
    "sec_fundamentals": RetentionPolicy(max_age_days=None, max_duplicate_files_per_hash=1),
    "sec": RetentionPolicy(max_age_days=None, max_duplicate_files_per_hash=1),
    "etoro": RetentionPolicy(max_age_days=7, max_duplicate_files_per_hash=1),
    "etoro_broker": RetentionPolicy(max_age_days=90, max_duplicate_files_per_hash=1),
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
