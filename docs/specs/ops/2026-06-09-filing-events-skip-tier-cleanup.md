# filing_events skip-tier cleanup (#1013 — #1011 PR2)

Author: claude (autonomous)
Date: 2026-06-09
Status: Draft (pre-Codex ckpt-1)

## Problem

`#1011`/`#1012` (commit `09a8b2f`) introduced the SEC three-tier form
allow-list: `SEC_INGEST_KEEP_FORMS = SEC_PARSE_AND_RAW | SEC_METADATA_ONLY`
([`app/services/filings.py:261`](../../../app/services/filings.py#L261)).
New ingest (bootstrap seed + daily refresh) now filters to that set, but
existing dev DBs still hold pre-allow-list skip-tier rows in `filing_events`.

Measured on dev 2026-06-09 (`bootstrap_state=complete`, 12,530 instruments):

- `filing_events` = **2,712,237 rows / 2,192 MB**. All `provider='sec'`
  (0 non-SEC rows); 0 NULL `filing_type`.
- Literal skip-tier (`filing_type NOT IN SEC_INGEST_KEEP_FORMS`) =
  **320,434 rows**.

## Latent allow-list bug — legacy-named 13D/G (operator-confirmed in scope)

`SEC_PARSE_AND_RAW` lists only the **long-form** `SCHEDULE 13D/G` names.
The DB carries **both** naming conventions (EDGAR renamed `SC 13G` →
`SCHEDULE 13G` on 2024-12-19):

| filing_type | rows |
|---|---|
| `SC 13G/A` | 80,403 |
| `SC 13G` | 29,323 |
| `SC 13D/A` | 17,246 |
| `SC 13D` | 4,573 |
| **total legacy-named 13D/G** | **131,545** |

The blockholder ingest
([`app/services/blockholders.py:166-173`](../../../app/services/blockholders.py#L166-L173))
**actively consumes both** names — its accepted-form set is the
ground truth. A literal `NOT IN keep-set` sweep would delete 131,545
actively-parsed 13D/G filing-metadata rows: data loss, not cleanup.

**Fix (operator-approved):** add the four short-form aliases to
`SEC_PARSE_AND_RAW` so the keep-set matches what the parser accepts.
This is strictly more correct and also fixes future daily-refresh
coverage for legacy-named 13D/G. After the fix:

- **Deletable (scoped `provider='sec'` + `filing_type IS NOT NULL` +
  NOT IN corrected keep-set) = 188,889 rows.**
- Skip-tier accessions with `filing_raw_documents` bodies = **0**
  (skip-tier = no parser → no raw stored). No orphan-raw handling needed.

## Settled decisions / prevention-log

- Settled "Filing event storage": `filing_events` stores metadata; full
  raw text out of scope (separate table). This cleanup only deletes
  metadata rows for forms with no parser/classifier use — preserves the
  decision. `filing_raw_documents` (the separate table) is untouched.
- Prevention-log L367 "`ON DELETE CASCADE` on `*_audit`/`*_log` forbidden":
  does NOT apply — no new FK; we add none. The existing
  `filing_documents → filing_events ON DELETE CASCADE` (sql/062) is a
  live-data child, not an audit/log table; its cascade is correct and
  auto-handles child rows.
- Prevention-log §"psycopg3 service-no-commit" / "orchestrator-of-N
  autocommit": the cleanup is an orchestrator-of-batches that OWNS its
  connection → opens own `autocommit=True` conn, per-batch
  `with conn.transaction()` (top-level tx, committed per batch). Mirrors
  `financial_facts_retention.sweep_retention_all_instruments`.
- New (this PR) prevention-log lesson: an allow-list / filter form-type
  set must cover **every naming convention the ingest path accepts** —
  grep the parser's accepted-form set before freezing a downstream-delete
  predicate. Extracted to prevention-log + data-engineer skill in-PR.

## Design

### Part A — keep-set alias fix (single source of truth)

`app/services/filings.py`: add `"SC 13D"`, `"SC 13D/A"`, `"SC 13G"`,
`"SC 13G/A"` to `SEC_PARSE_AND_RAW` (mirrors `blockholders.py`'s
dual-naming acceptance; comment cross-references the 2024-12-19 rename).
`SEC_INGEST_KEEP_FORMS` (the union) picks them up automatically — single
source of truth, no second list.

### Part B — cleanup service

`app/services/filing_events_cleanup.py`:

```python
@dataclass(frozen=True)
class SkipTierCleanupSummary:
    total_deleted: int
    by_form_type: dict[str, int]   # actually-deleted tally, per filing_type

def cleanup_skip_tier_filing_events(
    *, database_url: str | None = None, batch_size: int = 5000,
) -> SkipTierCleanupSummary:
    ...
```

- Opens own `psycopg.connect(url, autocommit=True)` (orchestrator-of-N
  pattern — per-batch tx isolation, bounded WAL/lock churn).
- Keep-set is imported `SEC_INGEST_KEEP_FORMS` (no duplicate literal).
- Bounded-batch loop until a batch deletes 0 rows:

```sql
DELETE FROM filing_events
WHERE filing_event_id IN (
    SELECT filing_event_id FROM filing_events
    WHERE provider = 'sec'
      AND filing_type IS NOT NULL
      AND filing_type <> ALL(%(keep)s::text[])
    ORDER BY filing_event_id          -- deterministic batching / auditable progress (Codex ckpt-1 M)
    LIMIT %(batch)s
)
RETURNING filing_type;
```

  Each batch runs inside `with conn.transaction()`; `RETURNING filing_type`
  is tallied into a `Counter` → exact per-form deletion counts of what was
  actually removed. `filing_documents` cascades automatically
  (`ON DELETE CASCADE`) — no manual child handling.
- Scope guards (correctness): `provider='sec'` (keep-set is SEC-only —
  never touch other providers, even though dev has none today);
  `filing_type IS NOT NULL` (never delete unclassifiable rows).
- Idempotent: re-run after drain selects 0 → 0 batches → `total_deleted=0`.

### Part C — job wrapper + manual-only registration triangle

Manual-trigger-only, **not** in `SCHEDULED_JOBS` (one-shot; must never
auto-fire). Mirrors `sec_rebuild` (#1155). Bootstrap-gating comes from the
universal `check_bootstrap_state_gate` on the manual-queue dispatch path
(blocks until `bootstrap_state='complete'`, operator-overridable per
#1181 envelope) — no per-job `prerequisite` (that field lives on
`ScheduledJob`).

- `app/workers/scheduler.py`: `JOB_FILING_EVENTS_SKIP_TIER_CLEANUP =
  "filing_events_skip_tier_cleanup"` constant (+ `__all__`); zero-arg
  wrapper `filing_events_skip_tier_cleanup()` wrapping the service in
  `_tracked_job(...)`; logs `total_deleted` + sorted per-form counts.
- `app/jobs/runtime.py`: `_INVOKERS[JOB_FILING_EVENTS_SKIP_TIER_CLEANUP]
  = _adapt_zero_arg(filing_events_skip_tier_cleanup)`.
- `app/jobs/sources.py`: `MANUAL_TRIGGER_JOB_SOURCES[...] = "db"` (pure
  DB op, no SEC HTTP — matches the other retention sweeps' lane).
- `app/services/processes/param_metadata.py`:
  `MANUAL_TRIGGER_JOB_METADATA[...] = ()` (no operator-tunable params;
  `batch_size` is an implementation knob, §6.5.7 item 2 — kept internal).

Terminal-state correctness (§6.5.7 item 8): the job takes no params and
runs to completion in the body. The only prelude-skip is the universal
bootstrap gate — which already handles its own terminal states (manual
*request* row → `rejected`; the `job_runs` prelude → `skipped`, per
[`app/jobs/runtime.py`](../../../app/jobs/runtime.py)). We add no new
`mark_request_completed` call site and no custom terminal handling.

### Orphan-safety (Codex ckpt-1 High)

`filing_events` is per-instrument fan-out (sql/144); parser-owned typed
tables (`blockholder_filings`, `insider_transactions`,
`insider_initial_holdings`, `institutional_holdings`,
`def14a_beneficial_holdings`, `eight_k_events`, …) are accession-keyed
with **no FK** back to `filing_events`. So a wrong keep-set entry could
delete a metadata row whose accession still has parsed typed data —
orphaning the typed side (this is exactly the 13D/G bug class).

Two guards, deliberately NOT a runtime cross-check against ~8 parser
tables (that couples a one-shot to every parser — over-engineering, fails
KISS):

1. **Structural (durable):** pure-logic test asserting
   `SEC_INGEST_KEEP_FORMS` is a superset of every active parser's
   accepted-form set (at minimum the blockholder set in
   `blockholders.py`). If a parser learns a new form name the keep-set
   must too — the test fails until it does. This prevents the keep-set
   from ever dropping below what parsers consume.
2. **Empirical (this run):** before the destructive backfill, run a
   preflight on dev — candidate skip-tier accessions ∩ parser-owned typed
   tables must be ∅ — and record the result in the PR. (Already verified
   for `filing_raw_documents`: 0 overlap.)

## Testing

- **Pure-logic** (`tests/test_filing_allow_list.py` or extend existing):
  assert the four `SC 13D/G` aliases are now in `SEC_PARSE_AND_RAW` and
  thus `SEC_INGEST_KEEP_FORMS`; assert the keep-set is a superset of the
  blockholder accepted-form set's 13D/G members (regression-pin the bug —
  if the parser adds a name the keep-set must too).
- **One DB integration test** (`-m db`,
  `tests/test_filing_events_skip_tier_cleanup.py`): seed `filing_events`
  with (a) keep-tier (`10-K`, `4`), (b) skip-tier (`FWP`, `UPLOAD`),
  (c) legacy 13D/G alias (`SC 13G/A`), (d) a `filing_documents` child on
  a skip-tier row, (e) a `provider != 'sec'` row whose `filing_type` is a
  skip-tier SEC form (must survive — keep-set is SEC-only), (f) a
  `filing_type IS NULL` SEC row (must survive — never delete
  unclassifiable), plus run with a tiny `batch_size` to exercise the
  loop. Assert: only skip-tier-SEC deleted; keep-tier + aliases + (e) +
  (f) survive; cascade removed the child; per-form tally exact; second
  run deletes 0 (idempotent).
- **Registry-shape** (`tests/test_layer_123_wiring.py` /
  `test_job_registry.py`): `source_for("filing_events_skip_tier_cleanup")
  == "db"`; present in `_INVOKERS`; `MANUAL_TRIGGER_JOB_METADATA` entry
  validates (empty); NOT in `SCHEDULED_JOBS`.

## Acceptance (issue #1013) + ETL DoD

- [ ] `POST /jobs/filing_events_skip_tier_cleanup/run` triggers it.
- [ ] `filing_events` drops by the skip-tier count (~188,889; **not** the
      issue's stale ~32%/265k — corrected after the alias fix).
- [ ] KEEP-tier forms (10-K/10-Q/8-K/13F/Form3/4/DEF14A **and** 13D/G
      both naming conventions) untouched.
- [ ] No FK errors (cascade clean).
- [ ] Re-running `bootstrap_filings_history_seed` does not re-introduce
      rows (PR1 allow-list filters them).
- [ ] **ETL clauses 8-12:** smoke panel AAPL/GME/MSFT/JPM/HD — KEEP-tier
      filings survive, `/instruments/<sym>/filings/10-k/history` +
      ownership-rollup render; cross-source one figure; **backfill =
      run the job on dev, record before/after counts**; operator-visible
      endpoint verified post-run; PR records step + commit SHA per clause.

## Out of scope

- `#1014` (raw-payload retention) — **deferred** (operator decision
  2026-06-09): drop-on-success target `def14a_body` is ~96 MB on disk,
  not the 11 GB the issue claims; premise evaporated. Evidence recorded
  on the issue; revisit when a drop-target kind grows past threshold.
- No schema migration (Python-constant + data-cleanup job only).
