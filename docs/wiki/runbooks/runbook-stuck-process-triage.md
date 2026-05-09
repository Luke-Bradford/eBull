# Runbook — stuck process triage

When the `/admin` Processes table shows a stale (amber, pulsing) row,
or when an action button hangs without resolving, this runbook walks
the diagnosis chain end-to-end.

## The four-case stale model

A row is stale when at least one of these chips is rendered. Multiple
chips can fire on the same row simultaneously. Source of truth:
`app/services/processes/stale_detection.py`. Chip labels:
`frontend/src/components/admin/processStatus.ts::STALE_REASON_LABEL`.

| Chip label (FE) | `StaleReason` (BE) | What it means | Operator action |
|---|---|---|---|
| `schedule missed` | `schedule_missed` | Cron should have fired by now and didn't (>60s past `expected_fire_at`). Scheduled-job mechanism only. Suppressed while the row is actively `running`. | Check the jobs process is up. If it is, check whether another job is holding a singleton or shared-source lock. |
| `source has fresh data` | `watermark_gap` | `data_freshness_index.expected_next_at` for this process's source is in the past (>60s). We're behind the source. Scheduled-job + ingest-sweep only; bootstrap NEVER watermark-gaps. Suppressed while running. | Click *Iterate*. If Iterate doesn't clear the gap, open the row's Logs tab — usually a structured upstream error (rate limit, 404). |
| `queue stuck` | `queue_stuck` | A `pending_job_requests` row for this process has `status='dispatched'` and worker pickup older than 30 minutes. The dispatcher hasn't observed terminal status from the worker. Applies to all mechanisms. | If the worker is alive (heartbeat fresh), wait — drain may be slow. If the worker is wedged or restarted, see [Jobs-process restart procedure](#jobs-process-restart-procedure). |
| `no progress` | `mid_flight_stuck` | The active run hasn't bumped `last_progress_at` in longer than the per-process threshold. Only fires when the row is `running`. | Decide cooperative cancel vs jobs-process restart based on the per-process threshold (see below). |

The chip is rendered with the elapsed-since-heartbeat appended on the
`mid_flight_stuck` chip (e.g. "no progress 7m"), computed client-side
from `active_run.last_progress_at`.

## Heartbeat — `last_progress_at`

`last_progress_at` IS the heartbeat. There is no separate plumbing.

- The producer calls `record_processed()` (a `JobTelemetryAggregator`
  helper) on each row it writes; that bump sets `last_progress_at =
  now()`.
- A run that has not yet recorded its first tick falls back to
  `started_at` for the heartbeat — see
  `app/services/processes/stale_detection.py:148-154`. This catches
  the "stuck before first tick" case (a worker that died before
  emitting any progress would otherwise never surface as stale).

## Per-process thresholds

The mid_flight_stuck threshold defaults to 5 minutes. Per-process
overrides live in `app/services/processes/stale_thresholds.py`:

```text
DEFAULT_THRESHOLD_S = 300         # 5 min
"bootstrap"                        = 1800   # 30 min
"sec_filing_documents_ingest"      = 1800
"sec_13f_quarterly_sweep"          = 1800
"sec_n_port_ingest"                = 1800
"sec_def14a_bootstrap"             = 1800
"sec_business_summary_bootstrap"   = 1800
"ownership_observations_backfill"  = 1800
"sec_insider_transactions_backfill" = 1800
```

Override is set ONLY when a producer's natural row-write cadence is
slower than the default — SEC bulk archive seeds emit one tick per
archive completion (~1/min), so 30 min keeps the threshold above the
producer's natural emission rate. Adding a new override: edit
`stale_thresholds.py` and confirm `tests/test_stale_thresholds.py`
still passes (the test grep-validates override keys against the live
process registry).

## Action ladder for `mid_flight_stuck`

1. **Wait.** If the row is `running` and the heartbeat is just past
   threshold, the producer might emit on the next tick. Give it
   another threshold's worth of time.
2. **Cooperative cancel.** Click Cancel → leave on the default
   cooperative mode. The worker observes at the next checkpoint and
   completes the in-flight item; the watermark advances to where the
   work landed. See `runbook-cancel-and-resume.md` for the state
   machine.
3. **Wait `2× threshold`.** If cooperative cancel is pending past
   `2× per-process threshold` without `observed_at` set, the worker
   is genuinely wedged — it can't reach a checkpoint.
4. **Jobs-process restart.** See [Jobs-process restart procedure](#jobs-process-restart-procedure).

Do NOT issue a second cancel as an "upgrade" to terminate. The
partial-unique index `process_stop_requests_active_unq` (sql/135)
rejects the second insert with `409 stop_already_pending`. The right
escalation is the jobs-process restart, which lets the boot-recovery
sweep clear the stranded stop row.

## Jobs-process restart procedure

The jobs process owns APScheduler, the manual-trigger executor, the
sync orchestrator, the reaper, the queue dispatcher, the boot-time
freshness sweep, and the heartbeat writer (see
`docs/settled-decisions.md` `## Process topology (#719)`). Restarting
it is a controlled operation:

1. Stop `python -m app.jobs` (Ctrl-C in the dev terminal, or the
   service control whichever applies).
2. **Wait for clean exit.** APScheduler shuts gracefully; any
   in-flight workers either finish their current item or are killed
   when the process exits. Killed workers do NOT leave partial DB
   rows because every write is in a transaction that rolls back on
   process exit.
3. Restart `python -m app.jobs`.
4. Boot recovery sweeps run automatically at startup (see
   `app/services/process_stop.py::boot_recovery_sweep`). They:
   - Close `process_stop_requests` rows abandoned mid-cancel (see
     [Boot-recovery reapers](#boot-recovery-reapers)).
   - Free stuck `pending_job_requests` full-wash fence rows.
5. The wedged process should now show `failed` or `cancelled` on its
   latest run. Click *Iterate* to resume.

## Boot-recovery reapers

Three separate sweeps run at jobs-process startup. They have different
scopes, tables, and thresholds — confusing them is easy.

| Function (in `app/services/process_stop.py`) | Table | Default age | What it does |
|---|---|---|---|
| `reap_orphaned_stop_requests` | `process_stop_requests` | 6h | Closes stop rows that were never observed (`observed_at IS NULL AND completed_at IS NULL AND requested_at < now() - 6h`). Sets `completed_at = now()`; leaves `observed_at` as NULL — the audit-visible "abandoned, never observed" sentinel. Frees the partial-unique active-stop slot. |
| `reap_observed_unfinished_stop_requests` | `process_stop_requests` | 24h | Closes stop rows where the worker saw the signal but crashed before completing (`observed_at IS NOT NULL AND completed_at IS NULL AND requested_at < now() - 24h`). Sets `completed_at = now()`; preserves `observed_at`. The 24h threshold is more generous than the never-observed reaper because observed-but-incomplete is a less-clear-cut abandonment signal. |
| `reap_stuck_full_wash_fences` | `pending_job_requests` | 6h | Transitions `mode='full_wash'` rows stuck in `status='dispatched'` for >6h to `status='rejected'` with an explanatory `error_msg`. Different table and different transition than the stop-request reapers; this is about full-wash fence cleanup, not cancel signals. |

`completed_at IS NOT NULL` on a stop row therefore does NOT prove a
clean cooperative cancel. To verify clean finish, see "How to confirm
a cancel landed" in `runbook-cancel-and-resume.md`.

## "I see a `null primary_sec_cik` in the coverage panel; what now?"

The coverage panel shows `primary_sec_cik` joined from
`external_identifiers`. NULL means: no SEC CIK row is bound to that
instrument as primary.

The lookup chain (deterministic — no fuzzy fallback exists for CIK):

1. **`daily_cik_refresh` ran in the last 24h?**
   `/admin` Processes → row `daily_cik_refresh`. Status should be
   `ok`, not `failed` / `stale`. The job is the live programmatic
   bridge: it fetches SEC's `company_tickers.json` via
   `SecFilingsProvider.build_cik_mapping_conditional` (conditional GET
   with If-Modified-Since + body-hash) and upserts via
   `app/services/filings.py::upsert_cik_mapping`. Scoped to US-listed
   exchanges via the `exchanges` table (asset-class filter — see
   `app/workers/scheduler.py:1605-1618`). If the row is failed, fix
   the job first (typical cause: SEC user-agent rejected, see
   `runbook-job-failures.md`).
2. **Is the symbol in SEC's published map?**
   `https://www.sec.gov/files/company_tickers.json` is the canonical
   source. If your symbol is not listed there, the bridge has nothing
   to bind. The live `daily_cik_refresh` path does NOT do
   suffix-stripping; it upserts the SEC map verbatim against the
   instrument's symbol. Expected long-tail misses:
   - Foreign issuers without ADRs.
   - Defunct / delisted tickers.
   - Bonds / preferreds / warrants (separate ticker from common stock).
   - Operational duplicates with broker-side suffixes (e.g. `.RTH`).
     The unused helper at `app/services/cik_discovery.py` has a
     suffix-stripping fallback, but that helper is not on the live
     production path; if a `.RTH` instrument lacks a CIK today, file
     a ticket rather than relying on suffix-strip.
3. **Share-class siblings (GOOG / GOOGL, BRK.A / BRK.B):**
   `external_identifiers` enforces a unique constraint on
   `(provider, identifier_type, identifier_value)`. Alphabet's CIK
   `0001652044` can therefore bind to ONE instrument at a time.
   `upsert_cik_mapping` runs `ON CONFLICT … DO UPDATE SET
   instrument_id = EXCLUDED.instrument_id`, so whichever sibling is
   processed last in the daily job's `instrument_symbols` loop wins.
   Coverage-panel red on the un-bound sibling is **expected** until
   the canonical-instrument-redirect mechanism (#819) lands. Until
   then, route SEC-derived ownership figures via the bound sibling.
4. **None of the above applies?** File a ticket with the symbol + the
   SEC CIK you've confirmed via direct EDGAR search. There is no
   stable operator-override hook today: `upsert_cik_mapping`
   (`app/services/filings.py:303-363`) is last-writer-wins — it demotes
   any mismatching primary row for the same instrument and then
   upserts the SEC map value. A manually-edited primary CIK that
   diverges from the SEC map will be overwritten on the next
   `daily_cik_refresh` run. The fix path is upstream: get the right
   ticker into `company_tickers.json` (or, for symbols SEC will never
   publish, file the canonical-instrument-redirect work tracked under
   #819).

There is no fuzzy-match step in this chain. The CUSIP fuzzy resolver
at `app/services/cusip_resolver.py` is for a different identifier and
is already 0.92-bounded (#914 closeout); it is unrelated to CIK
lookup.

## Counts (dev DB, 2026-05-09)

For context: 7,284 of 12,417 instruments have NULL `primary_sec_cik`.
The long-tail bucket is large because the panel includes foreign
issuers, delisted tickers, and synthetic listings that the bridge
correctly skips. Confirming that a high-volume US ticker is missing is
unusual — when in doubt, check whether the symbol is listed in
`company_tickers.json` directly.

## Related runbooks

- `runbook-cancel-and-resume.md` — cancel mechanics + watermark resume.
- `runbook-after-parser-change.md` — when to run the rebuild job after
  a parser-version bump.
- `runbook-job-failures.md` — generic job-failure triage chain.
- `runbook-data-freshness.md` — interpreting `data_freshness_index`.
