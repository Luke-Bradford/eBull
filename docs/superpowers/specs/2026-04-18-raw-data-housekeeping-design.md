# Raw data housekeeping — design v4

**Goal:** reclaim ~180 GB of duplicate raw-file waste in `data/raw/**`, bound future growth to a per-source policy, and prevent silent re-hoarding when new providers land.

**Scope:** Plan A from `docs/superpowers/plans/2026-04-18-data-housekeeping-richness.md`. Plans B + C deferred to separate specs.

**Revision history:**
- v1 — initial. Codex r1 (2 B, 3 H, 2 M, 1 L) + 8 stress-test items.
- v4 — Codex r4 (1 B, 1 H, 2 M, 2 L):
  - (r4-B1) Compaction grouped by content hash only; two different endpoints returning identical payloads (e.g. two FMP endpoints both returning `[]`) would collapse into one file, destroying audit evidence for one of them. Fix: parse `{tag_prefix}_{anything}.json` from existing filenames, group by `(tag_prefix, hash16)`, target filename uses the parsed `tag_prefix`. Groups never cross tag boundaries.
  - (r4-H2) Dry-run mode defeats staleness control — state only updates when `dry_run=False`, so cautious operator running dry-run for several days rehashes 225 GB daily. Fix: track `last_dry_run_compacted_at` separately; scheduler skips compaction in dry-run mode when within staleness window just like enforce mode. Dry-run still makes zero filesystem writes; only the state table (used purely for throttling) advances.
  - (r4-M3) Canonicalisation test contract drifted — tests 3+4 say "bytes passes through" and "str UTF-8 encodes", but v3 canonicalisation tries `json.loads` first. Fix: split into four tests per type — (bytes non-JSON passes through), (bytes JSON canonicalised + matches dict equivalent), (str non-JSON UTF-8), (str JSON canonicalised + matches dict).
  - (r4-M4) Test patch target ambiguous. If provider does `from app.services.raw_persistence import persist_raw_if_new`, patching the source module doesn't replace the already-bound name. Fix: **provider migration mandates `from app.services import raw_persistence` + `raw_persistence.persist_raw_if_new(...)` call pattern** so tests patching `app.services.raw_persistence.persist_raw_if_new` work correctly. Spec call-site examples in PR 2 updated to match.
  - (r4-L5) Test numbering still inconsistent — revision history says 26, list has 24, shipping order references old numbers. Fix: unified final count **9 helper + 1 provider + 14 compact/sweep + 2 integration = 26 total**; shipping order references by PR not by number.
  - (r4-L6) Advisory-lock integration test underspecified — sequential calls don't contend. Fix: test monkeypatches `pg_try_advisory_lock` to return False on second call, asserts `CompactionResult.skipped=True`, no filesystem mutations.
- v3 — Codex r3 (2 B, 2 H, 3 M, 2 L):
  - (r3-B1) `dir_.mkdir` and `tempfile.mkstemp` happen before the `try:` block, so permission / disk-full errors escape and fail the calling provider sync. Fix: wrap everything (mkdir → mkstemp → write → replace) in one `try:` with the `except OSError: log + return None` contract.
  - (r3-B2) Canonicalisation mismatch between helper and compactor for pass-through payloads. `etoro_broker.py:425` persists `response.content` bytes; the helper hashes raw bytes while compactor would re-canonicalise if bytes happen to look like JSON → second write rehashes, reintroduces duplicate. Fix: single `_canonicalise_for_hash(raw)` function used identically by helper and compactor. Always tries `json.loads(raw)` first; on success returns canonical JSON bytes; on failure passes raw through. Both code paths compute the same hash from the same bytes for any given payload.
  - (r3-H3) Scheduler job's `ScheduledJob.catch_up_on_boot` defaults True. On restart triggers unnecessary 225 GB rehash. Fix: declare `catch_up_on_boot=False` explicitly for `JOB_RAW_DATA_RETENTION_SWEEP`.
  - (r3-H4) `etoro` source lumps OHLCV candles + instrument lists + quote batches + error bodies together; 7-day blanket delete kills diagnostic audit trail. Fix: split etoro into two sources via a `tag`-to-source map in the helper OR explicitly accept 7-day loss for all etoro raw and document. v3 picks the second — 7 days is enough for any reasonable debugging window; if a longer trail is ever needed, revisit.
  - (r3-M5) "Keep newest mtime" vs "never touch <24h" conflict. Fix: among non-protected copies, keep newest; protected copies (<24h) excluded from both keep-and-delete decisions entirely. If only protected copies exist for a given hash, compaction no-ops that group and retries next week.
  - (r3-M6) `CompactionResult.skipped` field missing. Add `skipped: bool = False` + specify scheduler MUST NOT update `last_compacted_at` when result.skipped is True (advisory-lock skip path).
  - (r3-M7) PR 2 deletes per-provider `_persist_raw` symbols; existing tests at `tests/test_market_data.py:325` and `tests/test_broker_provider.py:648` patch those symbols and will break. Fix: PR 2 scope explicitly includes migrating every `patch("app.providers.*.\\_persist_raw")` callsite to patch `app.services.raw_persistence.persist_raw_if_new`.
  - (r3-L8) PR test numbering inconsistent across revision history / test-section / shipping-order. Fix: unified numbering below (9 helper tests + 1 provider test + 14 compact/sweep tests + 2 integration tests = 26 total across 3 PRs).
  - (r3-L9) Settings snippet uses `Field` but `app/config.py:1` only imports `field_validator`. Fix: either add `Field` to the existing `from pydantic import ...` line OR drop `Field(...)` wrapper and use a bare `bool = True` assignment with a trailing `# docstring` comment. PR 3 (which adds this setting) picks the first — single-line import change.
- v2 — Codex r2 (2 B, 3 H, 3 M):
  - (r2-B1) Canonicalisation mismatch: existing writers use `indent=2`; helper uses compact+sorted. Compaction must parse + re-canonicalise survivor bytes before writing the hashed file, else next provider write computes a different hash and rehoards. Addressed in compact_source spec + test 9.
  - (r2-B2) Helper rejected `str` payloads; `etoro.py:174` persists `exc.response.text` for failed-chunk audit trail. Helper now accepts `str | bytes | dict | list`; str → UTF-8 encode. Regression test added (7b).
  - (r2-H3) `max_distinct_copies=1` semantics ambiguous (could mean "keep 1 version ever" = delete historical snapshots). Renamed to `max_duplicate_files_per_hash`; doc clarifies it is exact-duplicate compaction only.
  - (r2-H4) Observability wrongly cited `data_ingestion_runs.job_name`; that table has `source`/`endpoint`. Scheduler already writes `job_runs` via `_tracked_job`. Solution now uses `job_runs` + structured log lines; no misuse of `data_ingestion_runs`.
  - (r2-H5) Integration test used `source='tmp_test'` which violates the unknown-source drift guard. Fix: tests use `monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)` + a real registered source.
  - (r2-M6) PR 1 test list mixed helper + compaction/sweep tests. Split: PR 1 ships tests 1–8 only (helper). PR 2 adds test 20 (providers). PR 3 adds tests 9–19 (compact/sweep/scheduler).
  - (r2-M7) Sec retention rationale falsely claimed `filings.py:281` relies on disk for full filing text. Actual filings pipeline stores metadata to DB; raw is audit trail. Rewrote rationale: conservative retention because SEC raw is <12 GB and re-fetch is cheap but history is priceless for forensic replay.
  - (r2-M8) Scheduler re-compacted every source every day. Added `raw_persistence_state` table tracking `last_compacted_at` per source. `raw_data_retention_sweep` skips compaction for sources where `NOW() - last_compacted_at < COMPACTION_STALENESS` (default 7 days). Sweep (age-based) still runs daily per source.

---

## Problem

`du -sh data/` reports **237 GB across 117,273 files**, zero retention logic anywhere in `app/`. Concrete drivers:

| Path | Size | Files | Pattern | Duplication |
|---|---|---|---|---|
| `data/raw/sec_fundamentals/` | 225 GB | 59,789 | `sec_facts_<cik>_<ts>.json` (7.5 MB each) | 13–15 byte-identical copies per CIK |
| `data/raw/sec/` | 12 GB | 55,082 | `sec_submissions_<cik>_<ts>.json`, `sec_filing_*.json` | Unverified; likely similar |
| `data/raw/etoro/` | 184 MB | 1,388 | `candles_<iid>_<ts>.json` | Daily OHLCV, already persisted to `price_daily` — raw is redundant |
| `data/raw/etoro_broker/` | 71 MB | 252 | `etoro_portfolio_<ts>.json` | Broker snapshots, audit-relevant |
| `data/raw/fmp/` | 1 MB | 762 | `fmp_*_<symbol>_<ts>.json` | Fallback only |
| `data/raw/companies_house/` | TBD | TBD | `ch_*_<number>_<ts>.json` | Active writer, surveyed in implementation |

Spot-check: CIK `0000320193` (AAPL) has 15 copies in `sec_fundamentals/`, md5 `2ee9730e...` on every one. Every CIK exhibits the pattern. Root cause: `_persist_raw(tag, payload)` at 6 provider sites always writes a fresh timestamped file; no content-hash check, no sweep anywhere.

Retention-time-window alone won't fix this — most duplicates are <30 days old. Content-hash compaction is the actual 180 GB win; retention-by-age is for steady-state boundedness after compaction.

## Solution

### Shared raw-persistence helper — `app/services/raw_persistence.py` (new)

Centralises the write path for every provider. Replaces per-provider `_persist_raw` implementations at:
- `app/providers/implementations/sec_edgar.py:51`
- `app/providers/implementations/sec_fundamentals.py:135`
- `app/providers/implementations/etoro.py:87`
- `app/providers/implementations/etoro_broker.py:43`
- `app/providers/implementations/fmp.py:59`
- `app/providers/implementations/companies_house.py:40`

```python
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_DATA_ROOT = Path("data/raw")


def persist_raw_if_new(
    source: str,
    tag: str,
    payload: object,
) -> Path | None:
    """Write ``payload`` under ``data/raw/{source}/{tag}_{sha256[:16]}.json``
    iff a file with the same content does not already exist.

    Returns the path that was written, or ``None`` on dedup hit.

    - ``source`` is one of the registered sources in ``_RETENTION_POLICY``
      (``sec_fundamentals``, ``sec``, ``etoro``, ``etoro_broker``, ``fmp``,
      ``companies_house``). Unknown sources raise ``KeyError`` so a new
      provider cannot silently start hoarding (config-drift guard item 4).
    - ``payload`` may be a dict (JSON-serialised deterministically) or
      bytes (written as-is). Other types raise ``TypeError``.
    - Write is atomic via tempfile + ``os.replace``; a crash mid-write
      cannot leave a zero-byte file that poisons future dedup (item 1).
    - Hash is in the filename, not in a glob — O(1) lookup, not O(n) scan
      across 60k-file directories (Codex H2).
    """
    # Drift guard runs first — KeyError is intentional, not a disk
    # error, and must propagate so a new provider without a policy
    # entry fails loudly in CI rather than silently hoarding.
    if source not in _RETENTION_POLICY:
        raise KeyError(
            f"persist_raw_if_new: unknown source {source!r}. Add a retention "
            f"policy entry to _RETENTION_POLICY before calling."
        )

    # Everything touching the filesystem is inside the single try. A
    # disk-full / permission / transient-FS error at any step must
    # log + return None, never escape into provider sync code
    # (r3-B1 regression window).
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
            # Clean up tmp on inner failure before re-raising to the
            # outer handler so the outer handler's contract still
            # holds (log + return None).
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except OSError:
        # Mirror existing providers' best-effort behaviour — log, do
        # not raise. A failed raw persist must not fail the sync flow;
        # the DB is the source of truth, raw is audit trail only.
        logger.warning("persist_raw_if_new: write failed source=%s tag=%s", source, tag, exc_info=True)
        return None
    return target


def _canonicalise_for_hash(payload: object) -> bytes:
    """Deterministic bytes for both hashing AND disk write.

    **Same function is called by both ``persist_raw_if_new`` and
    ``compact_source``**, so any two payloads that are logically
    equivalent (same dict, same parseable JSON, same bytes) produce
    the same hash regardless of which code path wrote them (r3-B2).

    Accepts ``dict | list | str | bytes``:

    - ``dict`` / ``list``: canonical JSON (sort_keys=True,
      separators=(",", ":")).
    - ``str``: first try ``json.loads`` — if it parses, canonicalise
      the parsed value; else UTF-8 encode raw. Covers ``exc.response.text``
      paths where the error body IS valid JSON as well as non-JSON
      free-text error bodies.
    - ``bytes``: first try ``json.loads(bytes)`` — if it parses,
      canonicalise the parsed value; else pass through. Covers
      ``response.content`` paths in eToro broker where the bytes
      ARE JSON as well as any non-JSON binary response.

    This unified "try-json-then-pass-through" rule means the hash of
    a payload is stable regardless of whether the caller deserialised
    before or after calling us.
    """
    # dict / list — direct canonical encode.
    if isinstance(payload, dict | list):
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    # str — try parse, else encode.
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return payload.encode("utf-8")
        return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")

    # bytes — try parse, else pass through.
    if isinstance(payload, bytes):
        try:
            parsed = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return payload
        return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")

    raise TypeError(f"persist_raw_if_new: unsupported payload type {type(payload).__name__}")
```

### Per-source retention policy

Policy lives in `app/services/raw_persistence.py` as a module-level dict. Per-source choice reflects audit value vs. churn:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class RetentionPolicy:
    # Delete files older than this many days. ``None`` = retain forever.
    max_age_days: int | None
    # How many files per unique content hash to keep. 1 = compact exact
    # duplicates only (historical snapshots of content that genuinely
    # changed are preserved — they hash differently). None = never
    # compact by hash. v2 rename from ``max_distinct_copies`` which
    # confusingly read as "keep 1 version ever" (r2-H3).
    max_duplicate_files_per_hash: int | None


# Per-source rationale (v2 + r3-H4):
# - sec_fundamentals / sec / companies_house: NO age-based delete.
#   Raw bodies are small (<15 GB post-dedup) and re-fetching SEC is
#   rate-limited + slow; a reproducible audit trail costs nothing.
# - etoro: 7 days. This source covers EVERYTHING etoro.py persists —
#   daily OHLCV candles (redundant, price_daily has history),
#   instrument lists, quote batches, error response bodies. We
#   accept losing diagnostic audit after 7 days because (a) the
#   broker account is authoritative for positions/trades, and (b) a
#   7-day window is enough for any reasonable incident postmortem.
#   If a longer trail is ever needed (e.g. regulator inquiry),
#   revisit by splitting etoro into etoro_market + etoro_diagnostics
#   with different policies.
# - etoro_broker: 90 days rolling — broker-state audit for reconciling
#   position / cash discrepancies.
# - fmp: fallback only; 30 days is enough for any re-derivation need.
_RETENTION_POLICY: dict[str, RetentionPolicy] = {
    "sec_fundamentals": RetentionPolicy(max_age_days=None, max_duplicate_files_per_hash=1),
    "sec":              RetentionPolicy(max_age_days=None, max_duplicate_files_per_hash=1),
    "etoro":            RetentionPolicy(max_age_days=7,    max_duplicate_files_per_hash=1),
    "etoro_broker":     RetentionPolicy(max_age_days=90,   max_duplicate_files_per_hash=1),
    "fmp":              RetentionPolicy(max_age_days=30,   max_duplicate_files_per_hash=1),
    "companies_house":  RetentionPolicy(max_age_days=None, max_duplicate_files_per_hash=1),
}
```

### Compaction (one-time + steady-state)

```python
def compact_source(source: str, *, dry_run: bool = True) -> CompactionResult:
    """Walk ``data/raw/{source}/``, group files by (tag-prefix, canonical
    content hash), keep one representative per group (newest mtime),
    delete the rest. Survivors are rewritten to the canonical bytes +
    hashed filename so subsequent ``persist_raw_if_new`` calls dedup
    via filename-existence alone.

    Canonicalisation: both this function and ``persist_raw_if_new``
    call the SAME ``_canonicalise_for_hash`` helper so the hash of
    any payload is identical across the write and compaction paths
    (r3-B2). After keeping the newest-mtime survivor per hash group,
    REWRITE the survivor under ``{tag}_{hash[:16]}.json`` with
    canonical bytes via the atomic tempfile+replace path. Old
    filenames deleted. Next provider run computing the same hash
    sees the hashed filename and no-ops.

    Algorithm:

        for each file f in source/ (sorted by mtime ASC so protection
        check is deterministic):
            raw_bytes = f.read_bytes()
            canonical = _canonicalise_for_hash(raw_bytes)
            hash16 = sha256(canonical).hexdigest()[:16]
            groups[hash16].append(f)

        for each hash16, files in groups.items():
            # r3-M5: protected files (mtime <24h) are excluded from
            # BOTH keep-and-delete decisions entirely. If only
            # protected copies exist for a hash group, the group is
            # no-op'd for this compaction pass — we retry next week
            # once they age past the safeguard.
            protected = [f for f in files if is_under_24h(f)]
            mutable   = [f for f in files if f not in protected]
            if not mutable:
                continue  # nothing we're allowed to touch
            survivor = max(mutable, key=lambda f: f.mtime)
            # Rewrite survivor as canonical + hashed filename
            # atomically; if target with same hashed name already
            # exists (previous compaction pass), skip rewrite and
            # just delete survivor's old-format filename.
            atomic_rewrite(survivor, dir_/f"{tag}_{hash16}.json", canonical)
            for f in mutable:
                if f != survivor: os.unlink(f)

    Handles the 117k-file legacy layout where filenames carry timestamps
    rather than hashes (mixed state during transition).

    Min-age safeguard (24h, r3-M5 clarified): never delete OR rename a
    file whose mtime is within the past 24 hours. An in-flight sync
    may still be reading raw for retry.

    Concurrency: acquires pg advisory lock
    ``pg_try_advisory_lock(hashtext('raw_compaction_' || source))``
    at function entry. If acquire fails, returns
    ``CompactionResult(skipped=True, ...)`` immediately — overlapping
    runs (manual vs. scheduler) no-op safely. Scheduler then MUST NOT
    update ``last_compacted_at`` for a skipped result (r3-M6).
    """
    ...


@dataclass(frozen=True)
class CompactionResult:
    source: str
    files_scanned: int
    files_deleted: int
    bytes_reclaimed: int
    duplicates_by_hash: int
    elapsed_seconds: float
    dry_run: bool
    # r3-M6: set True when compaction skipped due to advisory lock
    # contention. Scheduler MUST NOT update last_compacted_at when
    # skipped=True, otherwise a wedged lock would wedge the source
    # for a full COMPACTION_STALENESS window.
    skipped: bool = False
```

### Retention sweep (age-based, steady-state)

```python
def sweep_source(source: str, *, dry_run: bool = True) -> SweepResult:
    """Age-based retention pass. Deletes files older than
    ``_RETENTION_POLICY[source].max_age_days``. No-op if policy is
    ``max_age_days=None``. Min-age safeguard applies.

    Complementary to ``compact_source``: compaction kills duplicates
    regardless of age; sweep kills old survivors regardless of duplication.
    Running both keeps `data/raw/` at:
        one-file-per-content-hash × no-file-older-than-policy
    """
    ...
```

### Compaction staleness tracking (r2-M8)

New migration + helper, so daily scheduler doesn't rehash 225 GB every night:

```sql
-- sql/XXX_raw_persistence_state.sql
CREATE TABLE raw_persistence_state (
    source TEXT PRIMARY KEY,
    last_compacted_at TIMESTAMPTZ,
    last_compaction_files_scanned INTEGER,
    last_compaction_bytes_reclaimed BIGINT,
    last_sweep_at TIMESTAMPTZ
);
```

Scheduler calls `compact_source(src)` only when `NOW() - last_compacted_at > COMPACTION_STALENESS` (default 7 days). `sweep_source(src)` still runs daily per source — it's a cheap mtime glob, not a hash scan. Post-compaction, subsequent `persist_raw_if_new` calls dedup via filename-existence (O(1)) so the hashed layout stays clean between compaction windows.

### Scheduler job — `raw_data_retention_sweep`

Wires into `app/workers/scheduler.py::SCHEDULED_JOBS` + `app/jobs/runtime.py::_INVOKERS` + drift-guard tests (r1-M2). Cadence: daily 02:00 UTC (before `orchestrator_full_sync` at 03:00 UTC). **`catch_up_on_boot=False`** (r3-H3) — on a restart this job must NOT fire a catch-up run that rehashes 225 GB unnecessarily; a missed 02:00 window waits for the next natural fire.

```python
COMPACTION_STALENESS = timedelta(days=7)


def raw_data_retention_sweep() -> None:
    """Daily age-based sweep for every source + weekly-ish compaction
    where needed. Scope split:

    - sweep_source: cheap mtime glob, runs daily per source.
    - compact_source: hash scan, runs only when
      ``NOW() - last_compacted_at > COMPACTION_STALENESS``
      (default 7 days) OR last_compacted_at IS NULL (first run).

    Dry-run / enforce split:
    - ``settings.raw_retention_dry_run=True`` logs counts per source
      but does not delete. Default True in v1 of the migration;
      operator flips after inspecting one cycle.

    Observability (r2-H4): job-level row lands in ``job_runs`` via
    ``_tracked_job``. Per-source counts emit structured log lines
    (source, files_scanned, files_deleted, bytes_reclaimed, dry_run).
    No misuse of ``data_ingestion_runs`` — that table's columns
    (source / endpoint) are semantically wrong for this.
    """
    with _tracked_job(JOB_RAW_DATA_RETENTION_SWEEP) as tracker:
        total_deleted = 0
        total_reclaimed = 0
        with psycopg.connect(settings.database_url) as conn:
            for source in _RETENTION_POLICY:
                state = _load_state(conn, source)
                if _needs_compaction(state):
                    compaction = compact_source(source, dry_run=settings.raw_retention_dry_run)
                    # r3-M6: advisory-lock-skipped compactions must not
                    # update state — a stuck lock would wedge the
                    # source for 7 days otherwise.
                    if not settings.raw_retention_dry_run and not compaction.skipped:
                        _update_compaction_state(conn, source, compaction)
                    logger.info(
                        "raw_data_retention_sweep: source=%s phase=compact "
                        "scanned=%d deleted=%d reclaimed=%d dry_run=%s",
                        source, compaction.files_scanned, compaction.files_deleted,
                        compaction.bytes_reclaimed, settings.raw_retention_dry_run,
                    )
                    total_deleted += compaction.files_deleted
                    total_reclaimed += compaction.bytes_reclaimed

                sweep = sweep_source(source, dry_run=settings.raw_retention_dry_run)
                if not settings.raw_retention_dry_run:
                    _update_sweep_state(conn, source)
                logger.info(
                    "raw_data_retention_sweep: source=%s phase=sweep "
                    "deleted=%d reclaimed=%d dry_run=%s",
                    source, sweep.files_deleted, sweep.bytes_reclaimed,
                    settings.raw_retention_dry_run,
                )
                total_deleted += sweep.files_deleted
                total_reclaimed += sweep.bytes_reclaimed
        tracker.row_count = total_deleted


def _needs_compaction(state: RawPersistenceState | None) -> bool:
    if state is None or state.last_compacted_at is None:
        return True
    return datetime.now(UTC) - state.last_compacted_at > COMPACTION_STALENESS
```

### Provider migration

Each of the 6 providers: replace call sites from

```python
_persist_raw("sec_facts_{cik}", raw)
```

to

```python
persist_raw_if_new("sec_fundamentals", f"sec_facts_{cik}", raw)
```

Remove per-provider `_persist_raw` and `_RAW_PAYLOAD_DIR` definitions. One PR per provider for review manageability OR bundled — spec allows either.

### Settings additions — `app/config.py`

```python
raw_retention_dry_run: bool = Field(
    default=True,
    description="If true, raw_data_retention_sweep logs what would be "
                "deleted but does not delete. Flip to false only after "
                "observing one dry-run cycle's output.",
)
```

## Tests

Tests are split by shipping phase (r2-M6) so each PR's scope matches what it ships.

**PR 1 — helper only — `tests/test_raw_persistence.py`:**

1. `_canonicalise(dict)` is deterministic across Python runs (same bytes for same dict).
2. `_canonicalise(list[dict])` deterministic + recurses into nested dicts (sort_keys applies).
3. `_canonicalise(bytes)` passes through.
4. `_canonicalise(str)` UTF-8 encodes (r2-B2 regression — eToro error-body path).
5. `_canonicalise(int)` raises TypeError.
6. `persist_raw_if_new(source, ...)` with unknown source raises KeyError (drift guard).
7. `persist_raw_if_new` first call writes; second call with same payload returns None (dedup hit).
7b. `persist_raw_if_new(source, tag, exc.response.text)` with str payload persists + returns path (regression for r2-B2).
8. `persist_raw_if_new` atomic: simulated `os.replace` failure leaves no orphan `.tmp` files; target path does not exist.
9. `persist_raw_if_new` OSError on write returns None (not raise) — mirrors existing best-effort contract.

All tests use `monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)` + one of the real registered sources (e.g. `"fmp"`) to satisfy the drift guard without polluting the real filesystem (r2-H5).

**PR 2 — provider migration — `tests/test_providers_raw_persistence.py`:**

10. Each of 6 provider call sites exercised with a canned payload; assert `persist_raw_if_new` called with the expected `source` key (spy on the helper). Covers: `sec_fundamentals`, `sec_edgar`, `etoro`, `etoro_broker`, `fmp`, `companies_house`.

**PR 2 also migrates existing tests that patch the deleted symbols** (r3-M7):
- `tests/test_market_data.py:325` patches `app.providers.implementations.etoro._persist_raw` → update to `app.services.raw_persistence.persist_raw_if_new`.
- `tests/test_broker_provider.py:648` same pattern for etoro_broker.
- `grep -rn "_persist_raw" tests/` before merge — every hit must migrate or fail CI.

**PR 3 — compaction / sweep / scheduler — `tests/test_raw_retention.py`:**

11. `compact_source` groups by (tag-prefix, canonical-content-hash), keeps newest mtime per group, deletes rest.
12. `compact_source` canonicalisation transition (r2-B1): seed 3 files with same LOGICAL JSON but different formatting (`indent=2` vs compact). Assert they compact to 1 file with canonical bytes + hashed filename.
13. `compact_source` min-age safeguard: files <24h old untouched regardless of duplication.
14. `compact_source` dry_run=True returns the would-delete count without touching the filesystem.
15. `sweep_source` with `max_age_days=None` is a no-op.
16. `sweep_source` deletes files older than policy; preserves younger.
17. `sweep_source` min-age safeguard: always preserves <24h.
18. `_needs_compaction` returns True on NULL state + on state older than `COMPACTION_STALENESS`; False within staleness window.
19. `raw_data_retention_sweep` with fresh state calls compact_source AND sweep_source for every source.
20. `raw_data_retention_sweep` with state <7 days old calls sweep_source only, skips compaction (r2-M8).
21. `raw_data_retention_sweep` dry-run mode logs expected counts, makes no filesystem or state-table changes.
22. Scheduler registry drift: `JOB_RAW_DATA_RETENTION_SWEEP` appears in both `SCHEDULED_JOBS` and `_INVOKERS`.

**PR 3 integration — `tests/integration/test_raw_retention_real_fs.py`:**

23. End-to-end dedup: use `monkeypatch._DATA_ROOT = tmp_path` + real `fmp` source. Seed `tmp_path/fmp/` with 15 byte-identical files (same mtime-base, all >24h old via `os.utime`). `compact_source("fmp", dry_run=False)` leaves 1 file, reclaims 14×size. Survivor has hashed filename + canonical bytes.
24. Concurrent invocation: two calls to `compact_source` for the same source; second acquires-fail on pg advisory lock and returns `CompactionResult(skipped=True)`.

## Risks

- **Compaction duration on 117k files:** sha256 of 225 GB ≈ 30 min on commodity disk. Scheduler job MUST either (a) run compaction only for sources that haven't been compacted before, or (b) spread work via `max_runtime_seconds`. Approach: track last-compaction-timestamp per source in a new `raw_persistence_state` row; compaction re-runs quickly on a mostly-hashed layout.
- **Mixed-state transition:** legacy timestamped filenames coexist with new hashed filenames until compaction renames survivors. Code must handle both glob patterns.
- **sec/ source compound semantics:** `data/raw/sec/` holds both `sec_submissions_*` (deduplicatable like fundamentals) AND `sec_filing_*` (full filing text, audit-sensitive). Compaction groups by tag-prefix, so `sec_submissions_0000320193` and `sec_filing_0000320193-26-000042` are independently compacted. Policy still applies at the source level.
- **Why `sec` source is retain-forever (v2-M7):** not because `filings.py:281` relies on disk — actual filings pipeline writes metadata to DB, raw is pure audit trail. Rationale is value-vs-cost: post-compaction `sec/` is <12 GB across all CIKs forever; SEC is rate-limited so re-fetching a year of history to investigate a single decision is slow. Cheap to keep, expensive to re-derive. Same logic for `sec_fundamentals` and `companies_house`.
- **Operator disk-full recovery:** if someone runs out of disk before this ships, they can `rm -rf data/raw/sec_fundamentals/*` without consequence (DB has all the normalized facts; raw is audit trail only). Documented in the spec as the fire-break.

## Shipping order

1. Spec Codex review (this doc) — pass before implementing anything.
2. **PR 1:** shared helper + unit tests (1–15). No behavioural change to any provider yet.
3. **PR 2:** migrate 6 providers to `persist_raw_if_new`, delete per-provider `_persist_raw`. Provider regression tests (20).
4. **PR 3:** compaction + sweep + scheduler wiring + integration tests (17–19). Ships in dry-run mode by default.
5. Operator runs scheduler once, inspects dry-run logs, flips `raw_retention_dry_run=false`.
6. **PR 4 (optional):** observability dashboards (admin page card showing per-source file counts + reclaimed-bytes trend).

Sequential PRs (not parallel) because each depends on the previous and the spec optimises for small diffs + independently-reviewable units.

## Codex checkpoints

Per CLAUDE.md:
1. ✅ Pre-spec round 1 — 8 issues → rolled into v1.
2. ✅ Pre-spec round 2 — 8 issues → rolled into v2.
3. ✅ Pre-spec round 3 — 9 issues → rolled into v3.
4. Pre-spec round 4 — before PR 1.
5. Pre-push review per PR.
