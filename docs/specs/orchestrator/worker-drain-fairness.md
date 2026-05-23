# Manifest worker drain fairness — per-source quota + residual top-up

> Status: **DRAFT 2026-05-16** — pending Codex pre-spec 1a + operator signoff.
>
> Issue: **#1179**. Branch: `fix/1179-worker-drain-perf`.

## 1. Problem

`run_manifest_worker` (`app/jobs/sec_manifest_worker.py:153`) calls
`iter_pending(source=None, limit=max_rows)` once per tick. The SQL
(`app/services/sec_manifest.py:481-528`) orders ALL pending rows by
`filed_at ASC` across all 14 `ManifestSource` values:

```sql
SELECT ... FROM sec_filing_manifest
WHERE ingest_status = 'pending'
ORDER BY filed_at ASC
LIMIT %s
```

This globally-oldest-first contract starves sources whose backlog
exists entirely above the global oldest-tail.

### Concrete starvation observed during #1174 / #1176 smoke

Dev DB pending breakdown (2026-05-15 ~01:30 UTC, after #1175 + #1177
drains):

| Source | Pending rows | Oldest `filed_at` | Effective rank by age |
|---|---|---|---|
| `sec_form4` | ~2.5M | 1996 | 1 (always) |
| `sec_form3` | ~0.4M | 1996 | 2 |
| `sec_form5` | ~0.1M | 1996 | 3 |
| `sec_13d` / `sec_13g` | ~0.05M each | early 2000s | 4-5 |
| `sec_def14a` | ~0.1M | ~2010 | 6 |
| `sec_13f_hr` | ~0.1M | ~2013 | 7 |
| `sec_8k` | ~0.05M | ~2018 | 8 |
| `sec_n_port` | ~0.01M | ~2020 | 9 |
| `sec_10k` / `sec_10q` | small / synth no-op | varies | 10 |
| `sec_n_csr` | ~9k (post #1175 + #1177) | 2024-05 (iXBRL mandate) | 11 |
| `sec_xbrl_facts` / `finra_short_interest` | n/a (no parser) | — | skipped |
| Total | ~4.8M | — | — |

Worker tick budget today: `max_rows=100` / 5 min = 20 rows/min. The
1996+ `sec_form4` tail dominates the global `ORDER BY filed_at ASC`
result set on every tick indefinitely. Rows from `sec_n_csr`,
`sec_n_port`, and any source whose historical floor is newer than the
oldest unparsed Form 4 never appear in the worker's batch.

### Why this matters past one smoke run

On a real fresh-install bootstrap (#1174's `bootstrap_n_csr_drain`
fires alongside the equity issuer-scoped `bootstrap_first_install`
drain), every source enqueues rows at the same wall-clock. The worker
still drains globally-oldest-first, so wall-clock to reach a
newer-floor source =
`(rows in older sources) / 20 rows/min ≈ days` for sec_form4-dominated
installs.

The targeted per-source rebuild path
(`POST /jobs/sec_rebuild/run -d '{"source": "sec_n_csr"}'`) works
correctly because it scopes `iter_pending(source='sec_n_csr', ...)`
to one source. Only the scheduled, unscoped tick is broken.

`iter_retryable` has the same shape:
`ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC`
across all sources. Same starvation hazard on the `failed`-retry tail
when one source has dense pre-#1131 retry backlog.

## 2. Goals

1. Scheduled `sec_manifest_worker_tick` (no scope arg) makes drain
   progress on EVERY source with pending rows, not just the
   globally-oldest source.
2. Existing per-source rebuild path remains intact —
   `POST /jobs/sec_rebuild/run -d '{"source": "..."}'` still triggers
   a source-scoped drain that consumes the full `max_rows` budget for
   the requested source.
3. Total per-tick row count is still bounded by `max_rows` (default
   100). No HTTP-budget regression. SEC rate-limit budget at the
   shared 10 r/s token bucket is unaffected — only the *order* of row
   dispatch changes; the row count per tick does not grow.
4. Bursty sources catch up when other sources are quiet — a 9k-row
   backlog in `sec_n_csr` with all other sources at zero drains at
   the full `max_rows` rate, not at a per-source rigid slice.
5. `iter_retryable` adopts the same fairness shape so the failed-row
   retry tail does not re-introduce starvation when one source has
   dense retry backlog.

Non-goals (explicit out-of-scope; file as follow-ups if needed):

- Schema migration (no new column on `sec_filing_manifest`).
- Per-source worker tick splitting (Option 4 below) — design picks
  the in-process fairness shape; ops-surface multiplication is
  rejected.
- Lane B (Layer 1/2/3 firing verification) — separate session.
- Lane C (`data_freshness_index` cadence audit) — separate session.

## 3. Design options

### Option 1 — Per-source quota (rigid)

`per_source_quota = ceil(max_rows / N)` where `N` is the count of
registered sources (or sources with pending rows). Per tick, issue
one `iter_pending(source=s, limit=per_source_quota)` query per source
and union the results.

| Pro | Con |
|---|---|
| Simplest. No SQL surface change. | Bursty sources rigidly capped; a 9k-row backlog in one source drains at `per_source_quota / 5min` regardless of other sources' state. |
| No starvation across sources. | Wastes budget when most sources are empty. |
| One query per source per tick — bounded I/O. | Doesn't scale gracefully if registered-source count grows (per-source quota shrinks). |

### Option 2 — Round-robin within a tick

Iterate `(source, row)` round-robin until `max_rows` rows collected
or every source is empty. Per source, maintain a cursor into its
pending tail.

| Pro | Con |
|---|---|
| Touches every source with rows; bursty sources catch up naturally when others are empty. | Multiple SQL round-trips per tick (one per source, possibly multiple loops). |
| No schema change. | Worker code grows a fairness loop with a per-source cursor — more state. |
| Steady-state behaviour matches Option 1 + residual top-up (see below) but with more per-tick I/O. | |

### Option 3 — Priority hint column

Add `sec_filing_manifest.priority INT NOT NULL DEFAULT 0`. Bootstrap
stages tag rows with `priority=1` (drain ahead of historical
backfill). Worker `ORDER BY priority DESC, filed_at ASC`.

| Pro | Con |
|---|---|
| Most flexible — operator can promote any subset at any time. | Schema migration on a large table. |
| One query per tick (unchanged). | Doesn't solve the underlying starvation if all rows default to `priority=0` — needs every enqueue site to set priority correctly. |
| | Ratchets future complexity: now every parser / drain path picks a priority. |

### Option 4 — Per-source dedicated tick

Schedule one APScheduler job per source. Each job calls
`run_manifest_worker(conn, source=s, max_rows=...)`.

| Pro | Con |
|---|---|
| Most isolated; sources cannot interfere with each other. | Multiplies ops surface (14 scheduled jobs in `/admin/jobs`). |
| Per-source max_rows can be tuned. | Harder to bound the global SEC 10 r/s budget — N independent ticks fire near-simultaneously, all hit the same token bucket. |
| | More job_runs noise. More admin-page clutter. |

### 3.x — Recommendation (Codex 1a round 1 addressed)

**Option 1 with residual top-up.** Pseudocode:

```python
_TICK_COUNTER = itertools.count(0)   # module-global; advances by 1
                                     # per call. Tests inject tick_id
                                     # explicitly; production fallback
                                     # uses this so lead rotates by
                                     # exactly +1 per tick regardless
                                     # of scheduler cadence (avoids
                                     # `gcd(tick_step_seconds, n) > 1`
                                     # regime that would visit only
                                     # a subset of lead offsets).


def run_manifest_worker(conn, *, source=None, max_rows=100, now=None, tick_id=None):
    if source is not None:
        # Existing per-source rebuild path — unchanged.
        rows = list(iter_pending(conn, source=source, limit=max_rows))
        if len(rows) < max_rows:
            rows.extend(iter_retryable(
                conn, source=source, limit=max_rows - len(rows)
            ))
        return _dispatch(conn, rows, now=now)

    sources = sorted(registered_parser_sources())  # deterministic order
    n = len(sources)
    if n == 0:
        # Codex 1a WARNING — explicit early return; no SQL fires.
        return WorkerStats(rows_processed=0, parsed=0, tombstoned=0,
                           failed=0, skipped_no_parser=0)

    # Codex 1a BLOCKING — bounded quota math + tick-rotated lead so
    # `n > max_rows` regime does not permanently starve trailing
    # sources.
    # base = floor(max_rows / n); remainder = max_rows mod n.
    # `lead` rotates the "extra slot" + (when base=0) the "any slot"
    # window across ticks so every source eventually gets Phase A
    # allocation. lead advances by EXACTLY +1 per tick so every
    # source is touched within `n - remainder + 1` ticks regardless
    # of scheduler cadence (avoids `gcd(tick_step_seconds, n) > 1`
    # regime).
    if tick_id is None:
        tick_id = next(_TICK_COUNTER)
    base, remainder = divmod(max_rows, n)
    lead = tick_id % n
    quotas: dict[ManifestSource, int] = {}
    for i, s in enumerate(sources):
        # rotated index — first `remainder` slots from `lead` get +1.
        rot = (i - lead) % n
        quotas[s] = base + (1 if rot < remainder else 0)

    rows: list[ManifestRow] = []

    # Phase A — per-source quota slice. Pending first, retryable
    # within the same per-source budget. Each per-source query is
    # source-scoped, so unregistered sources (`sec_xbrl_facts`,
    # `finra_short_interest`) are excluded by construction.
    for s in sources:
        q = quotas[s]
        if q == 0:
            continue
        per_source_rows = list(iter_pending(conn, source=s, limit=q))
        if len(per_source_rows) < q:
            per_source_rows.extend(iter_retryable(
                conn, source=s, limit=q - len(per_source_rows)
            ))
        rows.extend(per_source_rows)

    # Phase B — residual top-up across the global oldest-tail. Two
    # phases (pending first, retryable second) so the retryable tail
    # is also fair when no pending rows remain.
    seen: set[str] = {r.accession_number for r in rows}
    remaining = max_rows - len(rows)
    if remaining > 0:
        topup_pending = iter_pending_topup(
            conn,
            sources=sources,
            exclude_accessions=seen,
            limit=remaining,
        )
        rows.extend(topup_pending)
        seen.update(r.accession_number for r in topup_pending)
        remaining = max_rows - len(rows)
    if remaining > 0:
        topup_retryable = iter_retryable_topup(
            conn,
            sources=sources,
            exclude_accessions=seen,
            limit=remaining,
        )
        rows.extend(topup_retryable)

    return _dispatch(conn, rows, now=now)
```

Where `iter_pending_topup` issues a single SQL query:

```sql
SELECT accession_number, cik, form, source,
       subject_type, subject_id, instrument_id,
       filed_at, accepted_at, primary_document_url,
       is_amendment, amends_accession,
       ingest_status, parser_version, raw_status,
       last_attempted_at, next_retry_at, error
FROM sec_filing_manifest
WHERE ingest_status = 'pending'
  AND source = ANY(%s::text[])              -- registered sources only
  AND accession_number != ALL(%s::text[])   -- exclude Phase A picks
ORDER BY filed_at ASC, accession_number ASC -- stable across runs
LIMIT %s
```

`iter_retryable_topup` is the sibling shape, matching today's retry
filter predicate:

```sql
SELECT ... FROM sec_filing_manifest
WHERE ingest_status = 'failed'
  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
  AND source = ANY(%s::text[])
  AND accession_number != ALL(%s::text[])
ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC,
         accession_number ASC
LIMIT %s
```

Both top-up calls bind `sources` and `exclude_accessions` as
explicit `%s::text[]` casts (Codex 1a WARNING — psycopg3 needs the
cast for empty-array literals to type-check at plan time).

Same `, accession_number ASC` stable tie-break is added to the
existing `iter_pending` and `iter_retryable` queries (Codex 1a
WARNING — today's `ORDER BY filed_at ASC` alone is unstable when
multiple rows share `filed_at`, which is common inside a daily Atom
batch).

Rationale:

- **No schema change.** Pure service-layer / worker-layer fix.
- **No starvation across registered sources** — Phase A guarantees
  every source with rows gets its quota (`base` or `base+1`).
- **No unregistered-source leakage** — `source = ANY(...)` on both
  top-up queries scopes residual to the registered set.
- **Bursty sources catch up** — Phase B fills unused budget from the
  global oldest tail. When all sources except one are empty, Phase A
  allocates that source `base+1` and Phase B's pending top-up fills
  the residual from the same source.
- **Retryable tail is fair too** — Phase B retryable top-up fires
  after pending top-up so a source whose entire backlog is `failed`
  drains symmetrically with `pending`-only sources.
- **Bounded I/O** — `n` per-source pending queries (≤ 14) + `n`
  per-source retryable queries (most return zero when Phase A
  pending hits quota) + up to 2 top-up queries per tick. At the
  5-min cadence this is negligible.
- **Backwards compatible** — per-source `source=...` path unchanged.

#### 3.x.1 Fairness under `n > max_rows`

When the registered-source count exceeds `max_rows` (not the case
today: 12 sources, default `max_rows=100`; would bite immediately
on operator misconfiguration where `max_rows` is set below
registered count), `base = 0` and only `remainder = max_rows mod n`
sources get a Phase A slot per tick. Phase B's budget is
`max_rows - sum(quotas) = 0`, so Phase B does not fire and trailing
sources would be permanently starved under a fixed sort order.

**Mitigation (Codex 1a round 2 + 5 BLOCKING fix)** — Phase A applies
a tick-rotated lead offset driven by an **opaque +1-per-tick counter**,
not wall-clock:

```
lead = tick_id % n
quotas[sources[i]] = base + (1 if (i - lead) % n < remainder else 0)
```

`tick_id` advances by exactly +1 per tick: it is either passed
explicitly by the caller (tests + future explicit production
plumbing via `job_runs.id`) or pulled from a process-local
`itertools.count(0)` module-global on first call. Wall-clock-derived
rotation (round 2's `int(now.timestamp() // 60) % n`) was REJECTED
in round 5 because tick steps that share a factor with `n`
(`gcd(tick_step, n) > 1`) cycle `lead` through only a subset of
offsets and permanently starve other indices. The +1 counter
sidesteps the gcd hazard entirely.

Coverage bound: every source receives a Phase A slot within
`n - remainder + 1` ticks because `lead` cycles through every
integer 0..n-1 strictly in order. The bound is independent of
scheduler cadence — only the wall-clock time it takes to accumulate
that many ticks varies (60s ticks ⇒ ~5 minutes; 300s ticks ⇒
~25 minutes for n=12 + max_rows=8).

- **Test cadence (any)**: tests inject `tick_id` explicitly. Case 9
  runs `tick_id=0..4` and asserts full source coverage.
- **Scheduled cadence (5-min)**: production fallback counter
  advances +1 per tick. Coverage time = `(n - remainder + 1) ×
  300s`. For `n=12, max_rows=8` ⇒ 25 min. For `n=12, max_rows=2`
  ⇒ 55 min.

**Process restart behaviour**: the module-global counter resets to 0
on every jobs-process restart. After restart, the first `n -
remainder + 1` ticks cover every source — same coverage bound. No
permanent starvation across restarts.

When `base ≥ 1` (the production-shape regime), rotation only
reshuffles which sources get the "+1" slot — every source still
gets at least `base` slots per tick regardless of rotation; the
rotation is operationally invisible.

**Determinism caveat for tests** — `now` no longer drives rotation.
Tests that need deterministic allocation MUST pass `tick_id=0` (or
any fixed integer) explicitly across runs. With explicit `tick_id`,
`lead = tick_id % n` is stable and `quotas` is reproducible.

Why not vanilla Option 2 round-robin: Option 1 + residual top-up
delivers the same fairness in steady state with simpler code (no
per-source cursor state across the loop), and the residual top-up
phase is cheaper than re-issuing per-source SQL in a loop.

Why not Option 3 priority hint: schema migration on
`sec_filing_manifest` (~5M rows on dev today) for a problem that
doesn't need a priority column. If a future use-case actually needs
operator-promoted priority, file that ticket then. YAGNI now.

Why not Option 4 per-source ticks: ops surface multiplication +
harder to bound the global rate budget. Single-tick fairness is
strictly simpler.

## 4. Implementation files

- `app/jobs/sec_manifest_worker.py` — rewrite the body of
  `run_manifest_worker` for the `source is None` branch. Keep the
  per-source rebuild branch (`source is not None`) verbatim. Add a
  new kw-only `tick_id: int | None = None` parameter that defaults
  to a module-global `itertools.count(0)` counter on first call
  (production fallback) and is injected explicitly by tests. The
  `sec_manifest_worker_tick` wrapper at
  `app/workers/scheduler.py:3694` passes `tick_id=None` (lets the
  counter advance). Extend `WorkerStats` with
  `processed_by_source: dict[ManifestSource, int]`
  (Codex 1a WARNING — tests + observability need a per-source
  breakdown). The new field counts rows dispatched to a parser
  (i.e. rows that hit one of `parsed` / `tombstoned` / `failed` /
  `raw_payload_violations` after the dispatch loop); it does NOT
  include `skipped_no_parser` rows (those are already tracked
  separately in `skipped_no_parser_by_source`).
- `app/services/sec_manifest.py`:
  - add `iter_pending_topup(conn, *, sources: Sequence[ManifestSource],
    exclude_accessions: Sequence[str], limit: int)` and
    `iter_retryable_topup(...)` with the SQL shapes shown in §3.x.
  - tighten `iter_pending` and `iter_retryable` ordering to add
    `, accession_number ASC` as a stable tie-break (Codex 1a
    WARNING).
- `tests/test_sec_manifest_worker.py` — extend with the 10 cases
  enumerated in §6.
- `.claude/skills/data-engineer/SKILL.md` — append a note in §11
  manifest worker explaining the per-source quota + top-up fairness
  shape. Cross-reference #1179 + this spec.
- `.claude/skills/data-sources/sec-edgar.md` — append a note in §11
  with the same shape (single source of truth on worker behaviour).
- `docs/review-prevention-log.md` — new entry (extracted post-merge,
  not part of the implementation diff). See §10.

No schema migration. No new tables. No new columns. (If `EXPLAIN`
shows the top-up SQL needs an `(ingest_status, source, filed_at)`
index for sub-second scan, file as a separate ticket — the existing
`idx_manifest_status_retry (ingest_status, next_retry_at) WHERE
ingest_status IN ('pending','failed')` index covers the predicate
prefix and a sort scan is acceptable for `LIMIT ≤ 100`.)

## 5. Source-priority + freshness interactions

- `_PARSERS` registry is the authoritative source list for fairness.
  Sources without a registered parser (`sec_xbrl_facts`,
  `finra_short_interest`) are intentionally excluded — they would be
  debug-skipped anyway under the current contract.
- `record_manifest_entry` inline freshness seed (#956) is untouched.
  Fairness operates on the read-path; the write-path is unchanged.
- Source-priority chain (#1171 `period_end DESC, filed_at DESC,
  source_accession DESC` within `(instrument_id, period_end)`) is
  unaffected — fairness picks WHICH rows the worker fetches per tick;
  the parser still writes observations under the canonical priority
  chain.
- `requires_raw_payload=True` audit gate (#938) is unaffected —
  fairness picks which rows enter the dispatch loop; the per-row
  audit invariant fires inside `run_manifest_worker` as today.

## 6. Test plan

`tests/test_sec_manifest_worker.py` extends with the following cases.
Fixtures seed `sec_filing_manifest` with known rows per source +
register fake parsers via `clear_registered_parsers()` +
`register_parser(...)`. Each fake parser appends the row's
`accession_number` + `source` to a capture list so tests can assert
dispatch ordering directly (Codex 1a WARNING — don't rely on
`WorkerStats` alone for ordering claims).

Quota notation: `q(s, tick_id)` = `base + (1 if rot < remainder
else 0)` where `base, remainder = divmod(max_rows, N)`,
`N = len(sources)`, `sources = sorted(_PARSERS.keys())`,
`lead = tick_id % N`, `rot = (sources.index(s) - lead) % N`.

All cases below pass an explicit `tick_id` (default `tick_id=0`
unless noted) so quota math is reproducible across runs.

| # | Case | Setup | Assert |
|---|---|---|---|
| 1 | Fairness — every source progresses | Seed 1000 `sec_form4` rows (filed 2010 ASC) + 50 `sec_n_csr` rows (filed 2024-05 ASC). Register fakes for both AND for all other ManifestSource values (so N == registered set in production-shape). `max_rows=100`. | `WorkerStats.processed_by_source['sec_form4'] >= q('sec_form4', 0)`. `processed_by_source['sec_n_csr'] >= q('sec_n_csr', 0)`. `sum(processed_by_source.values()) == 100`. Capture list confirms both sources appear in dispatch order. |
| 2 | Single-source full budget — bursty catch-up | Seed 5000 `sec_n_csr` rows. No other source has rows. Register fakes for all sources. `max_rows=100`. | `processed_by_source['sec_n_csr'] == 100`. All other sources absent or zero. Phase A allocates `q('sec_n_csr', 0)`; Phase B fills residual via top-up with the same source's globally-oldest rows. |
| 3 | Per-source rebuild path unchanged | `run_manifest_worker(conn, source='sec_n_csr', max_rows=100)`. Seed 200 `sec_n_csr` + 200 `sec_form4` rows. | Only `sec_n_csr` rows drained. `processed_by_source == {'sec_n_csr': 100}`. No `sec_form4` row in capture list. |
| 4 | Determinism — same input ⇒ same output | Run case 1 twice on a freshly seeded DB (drop + re-seed between runs to reset `ingest_status`). Pass `tick_id=0` explicitly on BOTH runs so `lead` is identical (otherwise the module-global counter advances and shifts the `+1` slot). | Both runs produce identical capture lists (same order of `(accession_number, source)` tuples). Confirms `ORDER BY filed_at ASC, accession_number ASC` tie-break is stable across runs. |
| 5 | Zero registered sources — explicit early return | `clear_registered_parsers()`. Seed 100 `sec_form4` rows. Worker tick fires. | Returns `WorkerStats(rows_processed=0, processed_by_source={}, ...)`. No row in capture list. Assert NO `transition_status` UPDATE fired by monkeypatching `app.jobs.sec_manifest_worker.transition_status` to a counter sentinel; assert `transition_status` call count is 0 after worker tick. (Before/after-count of rows in `sec_filing_manifest` would not catch a state-flipping UPDATE on existing rows — direct call-count is the correct invariant.) |
| 6 | Retryable tail fairness | Seed 200 `sec_form4` rows + 50 `sec_n_csr` rows ALL in `ingest_status='failed'` with `next_retry_at = NOW() - 1h`. No `pending` rows in either source. `max_rows=100`. Register fakes for both. | Both sources appear in capture list. `processed_by_source['sec_form4'] >= q('sec_form4', 0)`. `processed_by_source['sec_n_csr'] >= q('sec_n_csr', 0)`. Confirms Phase A retryable allocation + Phase B retryable top-up both fire. |
| 7 | Top-up double-dispatch invariant | Seed `q('sec_n_csr', 0) / 2` `sec_n_csr` pending rows (less than the per-source quota). All other sources empty. | All rows dispatched once. `len(set(r.accession_number for r in capture)) == len(capture)`. Phase A picks all `sec_n_csr` rows; Phase B top-up SQL's `accession_number != ALL(...)` excludes them — confirms via instrumented top-up call that no row appears twice. |
| 8 | Unregistered source not dispatched in top-up | Seed 200 `sec_xbrl_facts` rows pending (no parser registered for it). Register fakes for all OTHER sources only. `max_rows=100`. | No `sec_xbrl_facts` row in capture list. `processed_by_source` does not contain `sec_xbrl_facts`. Confirms top-up SQL's `source = ANY(%s::text[])` filter excludes unregistered sources from residual budget. |
| 9 | Tick rotation under `n > max_rows` covers every source | Register 12 fakes. Seed 100 pending rows per source. `max_rows=8` (`base=0`, `remainder=8`). Run worker `n - remainder + 1 = 5` ticks with **explicit `tick_id=0,1,2,3,4`** passed per call. | Tick k picks sources at rotated indices `[k, k + remainder)` mod `n`. Across 5 ticks (lead=0..4), the union of Phase A picks covers ALL 12 source indices (lead=0 covers 0..7, lead=4 covers 4..11 — union = 0..11). Assert: `set(source for tick in 5 ticks for source in capture)` equals the full registered-source set. Confirms `tick_id`-driven rotation resolves the `n > max_rows` starvation regime within `n - remainder + 1` ticks at any scheduler cadence. |
| 10 | Top-up empty-exclude-array | Call `iter_pending_topup(conn, sources=['sec_n_csr'], exclude_accessions=[], limit=5)` directly. Seed 5 `sec_n_csr` pending rows. | Returns all 5 rows without psycopg type-inference error. Confirms `%s::text[]` explicit cast handles empty-array literal. (Sibling test for `iter_retryable_topup`.) |

### Smoke (post-push, dev DB) per CLAUDE.md DoD clauses 8-12

Smoke panel:

| Source | Pre-fix observed drain (per tick) | Post-fix expected drain (per tick) |
|---|---|---|
| `sec_form4` | ~100 (dominates `ORDER BY filed_at ASC`) | ~`per_source_quota` (≥7) + Phase B residual where applicable |
| `sec_form3` | 0 | ≥7 |
| `sec_form5` | 0 | ≥7 |
| `sec_13d` / `sec_13g` | 0 | ≥7 each |
| `sec_13f_hr` | 0 | ≥7 |
| `sec_def14a` | 0 | ≥7 |
| `sec_8k` | 0 | ≥7 |
| `sec_n_port` | 0 | ≥7 |
| `sec_n_csr` | 0 | ≥7 (target: ~9k pending drains in ≤90 min wall-clock) |
| `sec_10k` / `sec_10q` | 0 | ≥7 |

Cross-source verify: trigger `POST /jobs/sec_manifest_worker/run`
twice (back-to-back) on dev DB; query
`sec_filing_manifest GROUP BY source HAVING COUNT(*) FILTER (...) >
0`; confirm pending-row count drops for each source between the two
ticks.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Phase B top-up SQL with `accession_number != ALL(%s::text[])` predicate is slow when Phase A returns ~100 rows. | Phase A rows ≤ `max_rows` (default 100); `!= ALL` over a 100-entry text array against a `LIMIT ≤ 100` scan is bounded. Plan-stage `EXPLAIN ANALYZE` on dev DB pre-push (per CLAUDE.md DoD clause 11) confirms cost; file an `(ingest_status, source, filed_at)` index ticket if `EXPLAIN` shows the existing partial index is insufficient. |
| Source count grows beyond `max_rows` (very long tail OR operator sets `max_rows < registered count`). Per-source quota collapses to 0 for trailing sources in sorted order; Phase B has zero budget because `sum(quotas) == max_rows`. | Tick-rotated `lead` offset (§3.x + §3.x.1) advances Phase A's lead window by exactly +1 per tick via an opaque `tick_id` counter (module-global `itertools.count(0)` fallback; test-injected explicitly). Every source receives a Phase A slot within `n - remainder + 1` ticks at any `(n, max_rows, tick_cadence)` triple — independent of `gcd(tick_step, n)`. §6 case 9 enforces. |
| Test cardinality drift vs registered-source count. | Quota helper exported as `compute_quotas(sources, max_rows, tick_id)` so tests derive expected counts from the same function the worker uses. No hardcoded slot counts in test assertions. |
| Worker tick wall-clock grows with per-source query count (≤14 pending + ≤14 retryable + ≤2 top-up vs 1 today). | Each per-source query is `LIMIT ≤ max(q(s, tick_id)) ≤ ceil(max_rows / N) ≤ 9` against the partial index `idx_manifest_status_retry`. Bound is ~30 short queries per tick at 5-min cadence. `EXPLAIN ANALYZE` rolls into pre-push verification. |
| `WorkerStats` shape change (added `processed_by_source` field). | Field defaults to `{}` via `field(default_factory=dict)` (same shape as existing `skipped_no_parser_by_source`). Existing consumers (`sec_manifest_worker_tick` log line + `tracker.row_count`) ignore extra fields. |
| Phase B top-up SQL with empty `exclude_accessions` array — psycopg3 needs explicit cast for type inference. | All array params are explicitly cast: `%s::text[]`. Empty-list call path covered by §6 case 10 — direct invocation of `iter_pending_topup(conn, sources=['sec_n_csr'], exclude_accessions=[], limit=5)` confirms the cast handles `[]` without a type-inference error. Sibling test on `iter_retryable_topup`. |

## 8. Settled-decisions

- **#719 process topology** — fix lives in jobs process only. The
  rewritten `run_manifest_worker` is still invoked from
  `sec_manifest_worker_tick` inside the jobs process. No FastAPI-side
  change. ✓
- **#1171 fund-metadata source priority** — drain starvation is what
  blocks the priority chain from firing on `sec_n_csr`. Fix unblocks
  the chain by surfacing N-CSR rows in the worker batch; the chain
  itself (parser-side write priority) is untouched. ✓
- No other settled-decisions apply.

## 9. Codex pre-spec 1a checklist (round 1 addressed)

- **BLOCKING (quota math)** — Addressed in §3.x. `divmod(max_rows, n)`
  gives `base` + `remainder`; with tick-rotated `lead` offset,
  rotated indices `(i - lead) % n` get the `+1` slot when
  `rot < remainder`, else `base`. Total Phase A budget == `max_rows`
  exactly. §3.x.1 documents `n > max_rows` regime — rotation
  guarantees every source receives a Phase A slot within
  `n - remainder + 1` ticks; no permanent starvation. §6 case 9
  enforces.
- **BLOCKING (top-up unregistered source leakage)** — Addressed.
  Both `iter_pending_topup` + `iter_retryable_topup` carry
  `AND source = ANY(%s::text[])` bound to `registered_parser_sources()`.
  §3.x SQL + §6 case 8 enforce.
- **BLOCKING (retryable fairness)** — Addressed. Phase B fires TWO
  top-up calls: pending top-up first, then retryable top-up
  consumes remaining budget. §3.x pseudocode + §6 case 6 cover both.
- **WARNING (SQL parameter cast consistency)** — Addressed. All
  array params explicitly cast as `%s::text[]` (both `sources` and
  `exclude_accessions`). Empty-array call path covered by an
  explicit direct-call unit test (§7 risk row: `iter_pending_topup
  (conn, sources=[...], exclude_accessions=[], limit=5)`).
  (Case 5 is the zero-source early-return path; it fires no SQL,
  so it is not the empty-array coverage — reference corrected.)
- **WARNING (determinism in SQL ORDER BY)** — Addressed. Add
  `, accession_number ASC` to `iter_pending`, `iter_retryable`,
  `iter_pending_topup`, `iter_retryable_topup`. §6 case 4 enforces.
- **WARNING (WorkerStats field for tests)** — Addressed. New field
  `processed_by_source: dict[ManifestSource, int]` with explicit
  semantics in §4 (counts dispatched rows; excludes `skipped_no_parser`).
  Tests use this field + parser-capture list for ordering.
- **WARNING (zero registered sources early return)** — Addressed.
  Pseudocode explicit `if n == 0: return WorkerStats(...)` before
  any quota math or SQL. §6 case 5 verifies no SQL fires (sentinel
  count or instrumented cursor).
- **NIT (index claim overstated)** — Addressed. §4 + §7 explicitly
  acknowledge the existing partial index covers the predicate
  prefix; `EXPLAIN ANALYZE` rolls into pre-push verification;
  follow-up ticket if a new index is needed.

## 10. Review-prevention-log applicability

**No existing entry on worker fairness / ordering / starvation.**
Post-merge, extract a new prevention-log entry:

```
### Manifest worker iter_pending must distinguish per-source fairness
from global age

- First seen in: #1174 / #1176 smoke (2026-05-15). The scheduled
  `sec_manifest_worker_tick` drained `sec_form4` 1996+ tail every
  tick; newer-floor sources (`sec_n_csr`, `sec_n_port`) never
  appeared in the result set. T8 + #1176 work invoked scoped, but
  the scheduled tick never picked them up.
- Prevention: when adding a new dispatch loop over a multi-source
  queue, do NOT sort the cross-source result set by a column
  whose distribution is unbalanced across sources (filed_at,
  created_at). The fairness shape is per-source slice + residual
  top-up across the global oldest tail. Self-review prompt: when
  introducing a new `iter_*(source=None)` SQL surface, ask "does
  this starve any source with a newer historical floor?" If yes,
  apply the per-source-quota + residual-top-up shape used by
  `run_manifest_worker` (#1179).
- Enforced in: `app/jobs/sec_manifest_worker.py::run_manifest_worker`;
  `app/services/sec_manifest.py::iter_pending_topup`;
  `tests/test_sec_manifest_worker.py` (cases 1-10).
```

Relevant existing entries (read-only, no impact on this fix):

- **#1247 manifest_parser_audit dict-key relaxation** — not
  triggered (no new audit endpoint).
- **#1259 manifest parser parse-failure branch consistency** —
  unaffected (no parser body changes).
- **#1265 transient-vs-deterministic upsert classification** —
  unaffected (no upsert phase changes).
- **#1296 transaction-aborted-by-CheckViolation** — unaffected (no
  CHECK constraint changes).

## 11. Acceptance criteria

1. `run_manifest_worker(conn, source=None, max_rows=100)` on a seeded
   dev DB drains every source with pending rows in a single tick
   (verified by per-source row-count assertion in WorkerStats).
2. `run_manifest_worker(conn, source='sec_n_csr', max_rows=100)` —
   targeted path — unchanged. 200-row regression test (case 3).
3. Top-up phase does NOT double-dispatch any accession in a single
   tick (case 7).
4. Smoke on dev DB confirms `sec_n_csr` backlog (~9k rows) drains
   naturally on the scheduled tick path, not just under manual
   source-scoped override.
5. Per-source rebuild path
   (`POST /jobs/sec_rebuild/run -d '{"source": "..."}'`) still works
   end-to-end.
6. PR body embeds dev-DB pending-by-source counts before + after a
   single tick fire, plus the per-source WorkerStats breakdown
   confirming fairness.
7. New prevention-log entry extracted post-merge per §10.

## 12. Sign-off

- Codex 1a round 1: 3 BLOCKING + 4 WARNING + 1 NIT. All addressed
  per §9.
- Codex 1a round 2: 1 BLOCKING (`n > max_rows` regime trailing-source
  starvation) + 1 WARNING (empty-array cast coverage reference). Both
  addressed:
  - BLOCKING → tick-rotated `lead` offset added to `quotas` math in
    §3.x; §3.x.1 rewritten to describe the rotation; §6 case 9
    added.
  - WARNING → §9 SQL-cast-coverage row corrected to reference the
    §7 direct-call test, not §6 case 5.
- Codex 1a round 3: 1 BLOCKING (case 9 math off; stale §7 + §9 text)
  + 1 WARNING (§7 contradictory empty-array wording; direct test not
  in §6). All addressed:
  - Case 9 rewritten: `n - remainder + 1 = 5` ticks; assert union
    of Phase A picks across 5 ticks covers all 12 sources.
  - §7 source-count-grows row rewritten to credit rotation, not
    Phase B compensation.
  - §9 BLOCKING-resolution row rewritten to remove "YAGNI" text +
    cite rotation + case 9.
  - §6 case 10 added: direct empty-array invocation of
    `iter_pending_topup` + sibling for `iter_retryable_topup`.
  - §7 empty-array risk row rewritten to point at case 10.
- Codex 1a round 4: 1 BLOCKING (§3.x.1 stale rotation math
  contradicting case 9) + 1 WARNING (stale case counts in §4 + §10).
  Both addressed:
  - §3.x.1 rewritten with explicit `n - remainder + 1` bound and
    cadence-separated test-vs-scheduled examples (aligned with §6
    case 9).
  - §4 "10 cases"; §10 "cases 1-10".
- Codex 1a round 5: 2 BLOCKING (scheduled-cadence rotation math
  wrong; `gcd(tick_step, n) > 1` permanent starvation). Both
  addressed by abandoning wall-clock rotation in favour of an
  opaque `tick_id` counter that advances by +1 per tick:
  - `run_manifest_worker` gains a kw-only `tick_id: int | None`
    parameter (tests inject; production falls back to module-global
    `itertools.count(0)`).
  - §3.x.1 rewritten to use the +1 counter and credit it as the
    gcd-hazard mitigation.
  - §7 source-count-grows row updated to reflect counter-based
    rotation independent of scheduler cadence.
  - §6 case 9 updated to pass `tick_id=0..4` explicitly.
  - §4 implementation-files row notes the new parameter shape +
    counter default.
- Codex 1a round 6: 2 BLOCKING (stale `now`-based determinism
  guidance in §3.x.1; stale `q(s)` notation in §6 ignoring `lead`)
  + 1 WARNING (case 4 needs explicit `tick_id`). All addressed:
  - §3.x.1 determinism caveat rewritten — `now` no longer drives
    rotation; tests pass `tick_id` explicitly.
  - §6 quota notation rewritten as `q(s, tick_id)` with rotation:
    `rot = (i - lead) % N`, `lead = tick_id % N`. All quota
    references in cases 1, 2, 6 updated to `q(..., 0)`.
  - §6 case 4 explicitly injects `tick_id=0` on both runs to pin
    determinism against the module-global counter.
- Codex 1a round 7: 1 WARNING (§7 `compute_quotas` signature stale)
  + 1 NIT (§7 `q(s)` shorthand stale). Both addressed: §7
  `compute_quotas(sources, max_rows, tick_id)`; `q(s)` →
  `q(s, tick_id)` / `max(q(...))`.
- Codex 1a round 8: **CLEAN** (cached at `/tmp/codex_1179_1a_round8.txt`).
- Operator: pending.
- Implementation plan: pending (drafted after spec signoff).
