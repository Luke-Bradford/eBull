"""Job source registry — Lane type + JOB_NAME_TO_SOURCE lookup.

PR1a of #1064 admin-control-hub follow-up sequence.
Plan: docs/internal/plans/pr1-job-registry-refactor.md (uncommitted).
Audit: docs/wiki/job-registry-audit.md.

## Why a dedicated module

Three things needed to coexist without circular imports:

1. The ``Lane`` Literal type (used by ``ScheduledJob.source``,
   ``StageSpec.lane``, and the ``JOB_NAME_TO_SOURCE`` lookup).
2. The ``JobInvoker`` callable alias (used by ``_INVOKERS`` in
   ``app/jobs/runtime.py`` after PR1b widens the contract).
3. The ``JOB_NAME_TO_SOURCE`` registry built from BOTH
   ``SCHEDULED_JOBS`` AND ``_BOOTSTRAP_STAGE_SPECS`` (used by
   ``JobLock`` to resolve a job_name to its source-keyed lock).

If ``Lane`` lived in ``app/workers/scheduler.py``, the bootstrap
orchestrator would import scheduler at module-load — currently
scheduler imports nothing from bootstrap_orchestrator, but the reverse
direction is heavy. Hoisting to a leaf module avoids the cycle.

## JOB_NAME_TO_SOURCE construction

The lookup MUST cover every job_name that ``JobLock`` may receive:

* Every entry in ``SCHEDULED_JOBS`` (~27 entries).
* Every entry in ``_BOOTSTRAP_STAGE_SPECS`` whose ``job_name`` is NOT
  also in ``SCHEDULED_JOBS`` (~10 bootstrap-only entries today —
  ``nightly_universe_sync``, ``daily_candle_refresh``,
  ``daily_cik_refresh``, ``sec_bulk_download``, the four
  ``sec_*_ingest_from_dataset`` entries, ``sec_submissions_files_walk``,
  and the three bespoke wrapper job names that PR1c will collapse into
  the SCHEDULED_JOBS set).

Conflict detection: if a job_name appears in both registries with
different effective sources, raise at module-load. Silent fallback
violates the locked source-lock decision.

Codex round-1 BLOCKING addressed: no per-name fallback in production.
``JobLock`` raises ``KeyError`` on unknown job_name (test fixtures
must register or use the explicit test-only escape hatch).

## Why ``Mapping`` and not ``dict`` for ``JobInvoker`` param

The invoker contract is read-only consumption of the params dict.
``Mapping`` makes the contract explicit and prevents accidental
mutation that would diverge ``params_snapshot`` from what the invoker
actually consumed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Type aliases — used across scheduler, bootstrap_orchestrator, locks, runtime.
# ---------------------------------------------------------------------------

Lane = Literal[
    "init",
    "etoro",
    "sec_rate",
    "sec_manifest",
    "sec_per_cik",
    "sec_expected_filings",
    "sec_filing_docs",
    "sec_insider_backfill",
    "sec_insider_ingest",
    "sec_bulk_download",
    "db",
    "db_filings",
    "db_fundamentals_raw",
    "db_ownership_inst",
    "db_ownership_insider",
    "db_ownership_funds",
    "db_liveness",
    "db_retry",
    "db_positions",
    "db_eod_snapshot",
    "db_cusip",
    "db_ownership_obs",
    "db_raw_sweep",
    "db_fsnds_notes",
    "db_size_sample",
    "db_thesis_dq",
    "db_thesis_break",
    "db_thesis_outcomes",
    "llm_thesis",
    "risk_metrics",
    "fair_value_band",
    "bootstrap",
    "finra",
    "openfigi",
]
"""Source-level concurrency bucket. Operator-locked decision (#1064): same-source
jobs serialise under one ``JobLock``; cross-source jobs run in parallel.

NOTE (#1478): a lane is a **job-overlap** bucket, NOT a request-rate limiter.
The SEC 10 req/s per-IP budget is enforced separately at the HTTP layer
(``app/providers/implementations/sec_edgar.py`` ``_PROCESS_RATE_LIMIT_CLOCK`` +
``_PROCESS_RATE_LIMIT_LOCK`` — a process-wide atomic inter-request floor, safe
under concurrent fetchers). Two SEC jobs on DIFFERENT lanes still cannot exceed
that floor. Do NOT collapse SEC lanes back together believing the lane bounds
the rate — it does not.

* ``init`` — universe-sync only. Pre-everything fence; one job total.
* ``etoro`` — eToro REST budget. ``execute_approved_orders`` +
  ``daily_candle_refresh`` + ``etoro_lookups_refresh`` +
  ``exchanges_metadata_refresh`` serialise.
* ``sec_rate`` — the SEC discovery/producer jobs (per-accession fetchers +
  per-issuer ingest). They serialise under one ``JobLock`` to bound job
  overlap, NOT request rate (the HTTP floor above bounds rate). #1478
  extracted the heavy ``sec_manifest_worker`` drainer; #1534 extracted the
  hourly ``sec_per_cik_poll`` producer (see ``sec_per_cik`` below) — both
  for the same reason: a member that holds the shared lane (a long drainer,
  or any member running at the exact ``:00`` slot the hourly poll fires)
  deterministically starves the others.
* ``sec_manifest`` — ``sec_manifest_worker`` only (#1478). The manifest drainer
  spends most of its run on DB tombstoning, not SEC calls; keeping it in
  ``sec_rate`` made it hold the producers' lane for 20-37s and starve them
  (7/7 vs 0/7). Its own lane lets it run concurrently with the producers while
  its single-instance self-lock + the shared HTTP floor still hold.
* ``sec_per_cik`` — ``sec_per_cik_poll`` only (#1534). The hourly @ :00 Layer-3
  poll shares the ``:00`` slot with every other ``sec_rate`` member that fires
  on the hour (and, during bootstrap, with the near-constant heavy drainers).
  Its fire uses a non-blocking ``pg_try_advisory_lock`` — so on contention it
  does NOT queue, it skips the whole hour. It lost the race for 17h+ on dev
  (last fire 2026-06-07 18:49) and the #1510 watchdog's re-enqueue kick hit the
  same locked lane and no-opped. Its own lane removes the contention. Write
  disjointness (a lane bounds overlap, not rate): its only shared write target
  is ``sec_filing_manifest`` via ``record_manifest_entry`` (idempotent
  ``ON CONFLICT`` upsert keyed by accession) — already written concurrently by
  ``sec_manifest_worker`` from its own lane since #1478, so concurrent manifest
  discovery is a proven-safe pattern; ``record_poll_outcome`` (per-CIK poll
  scheduler rows) and ``set_watermark`` (the per_cik-exclusive
  ``sec.last_modified.per_cik_poll`` namespace) have no other lane writer.
  Scheduled-only, so NOT added to the ``bootstrap_stages.lane`` CHECK (like
  ``sec_manifest`` / ``db_liveness`` / ``db_retry``).
* ``sec_expected_filings`` — ``expected_filings_poller`` only (#1788). The 15-min
  targeted submissions.json poll for instruments in an expected 10-Q/10-K window.
  Own lane so its high cadence can't lose the JobLock race to the hourly
  ``sec_per_cik_poll`` / manifest worker. Its only shared write target is
  ``sec_filing_manifest`` via ``record_manifest_entry`` (idempotent ``ON CONFLICT``
  upsert keyed by accession — already written concurrently from other lanes since
  #1478, proven-safe). Scheduled-only, so NOT added to the ``bootstrap_stages.lane``
  CHECK. (The companion ``expected_filings_seed`` runs on the existing
  ``db_fundamentals_raw`` lane — DB-only, no SEC HTTP.)
* ``sec_filing_docs`` — ``sec_filing_documents_ingest`` only (#1540). The
  hourly @ :35 producer holds the lane ~96s per tick (it expands every filing's
  ``{accession}-index.json`` into ``filing_documents`` rows). On the shared
  ``sec_rate`` lane that 96s hold — far longer than #1538's ~1.75s acquire-retry
  window — deterministically starves whichever member fires at :35
  (``sec_atom_fast_lane`` every 5 min). Own lane removes the contention. Write
  disjointness: it is the SOLE writer of ``filing_documents`` and writes no
  ``data_freshness_index`` / watermark — no shared-row race with any lanemate;
  ``max_instances=1`` blocks self-overlap. Scheduled-only → not in the
  ``bootstrap_stages.lane`` CHECK.
* ``sec_insider_backfill`` — ``sec_insider_transactions_backfill`` only (#1540).
  The hourly @ :45 oldest-first Form-4 tail drainer collides with the @ :45
  ``atom`` tick every hour; when it loses to a slow holder it skips the whole
  hour (#1538 retry can't cover the long holds). Own lane removes the
  contention. Write-ordering-safety (it runs concurrently with
  ``sec_insider_transactions_ingest``, which has its OWN ``sec_insider_ingest``
  lane @:15 since 2026-06-20, and shares its full write set): typed insider tables + ``ownership_insiders_observations``
  + ``filing_raw_documents`` are row-level ``ON CONFLICT`` idempotent from
  immutable filings; ``ownership_insiders_current`` + ``ownership_refresh_state``
  are written only inside ``refresh_insiders_current``, which holds a
  per-instrument ``pg_advisory_xact_lock`` and captures the watermark pre-MERGE —
  so that advisory lock (NOT this JobLock lane) serialises same-instrument
  refreshes and the watermark cannot regress. The lane was not load-bearing for
  correctness here. Scheduled-only → not in the ``bootstrap_stages.lane`` CHECK.
* ``sec_bulk_download`` — fixed-URL SEC archive downloads. Disjoint
  from ``sec_rate`` — large fixed downloads, no per-issuer iteration.
* ``db`` — DB-bound stages NOT owned by a finer family lane — Phase E
  derivations (``fundamentals_sync``, ``ownership_observations_backfill``)
  + scheduler catch-all (``orchestrator_full_sync``,
  ``orchestrator_high_frequency_sync``, ``retry_deferred``). The
  daily/hourly ``monitor_positions`` / ``ownership_observations_sync`` /
  ``cusip_extid_sweep`` were extracted to their own lanes in #1527 (see
  below); ``ownership_observations_backfill`` stays here (S24 bootstrap
  stage — moving it would force a CHECK migration).

The next five are bootstrap Phase C bulk-ingest family lanes
(#1141 / Task E of audit #1136). Each owns a disjoint write
target so the five Phase C stages can dispatch cross-source in
parallel during first-install bootstrap, recovering the
~4-hour wall-clock saving the May 8 design called out before
PR1c #1064 collapsed everything onto a single ``db`` source.

* ``db_filings`` — ``sec_submissions_ingest``; writes
  ``filing_events`` + ``instrument_sec_profile``.
* ``db_fundamentals_raw`` — ``sec_companyfacts_ingest``; writes
  ``company_facts`` via ``upsert_facts_for_instrument``.
* ``db_ownership_inst`` — ``sec_13f_ingest_from_dataset``; writes
  ``ownership_institutions_observations``.
* ``db_ownership_insider`` — ``sec_insider_ingest_from_dataset``;
  writes ``insider_transactions`` + ``form3_holdings_initial``.
* ``db_ownership_funds`` — ``sec_nport_ingest_from_dataset``;
  writes ``n_port_*`` + ``sec_fund_series``.

The next two are scheduled-only infra lanes, each owning exactly ONE
job (#1526 — same "extract the loser out of the contended lane" shape
as the #1478 ``sec_manifest`` split):

* ``db_liveness`` — ``jobs_liveness_watchdog`` (#1507/#1510) only.
* ``db_retry`` — ``jobs_retry_sweeper`` (#1509) only.

  Both were on the catch-all ``db`` lane and fired on the same 5-min
  grid as ``orchestrator_high_frequency_sync`` (every_5min, ``db``),
  which holds ``job_source:db`` re-entrantly through its ingest
  (~0.6s/run). The cross-thread scheduled fire of these light infra
  jobs lost the lane race every tick and starved (proven via
  ``pg_locks`` tick-poll + log correlation, 2026-06-07). They only read
  ``job_runs`` and write ``decision_audit`` / ``pending_job_requests``
  (each already guarded by its own ``pg_advisory_xact_lock``), so they
  are safe to run concurrently with orchestrator ingest. SEPARATE
  lanes, not one shared ``db_infra`` — a shared lane would re-create the
  same starvation between the 15-min watchdog and the 5-min sweeper at
  the :00/:15/:30/:45 ticks they co-fire. Scheduled-only, so NOT added
  to the ``bootstrap_stages.lane`` CHECK (like ``sec_manifest`` /
  ``finra`` / ``bootstrap``).

The next three are steady-state db jobs extracted from the catch-all
``db`` lane (#1527 — the daily/hourly continuation of #1526). Each fires
on a 5-minute-aligned slot and so co-fired ``orchestrator_high_frequency_sync``
(every_5min, ``db``) and lost the ``job_source:db`` cross-thread lane race
every collision — a once-daily job skips a FULL day per collision. Write-target
disjointness was verified before extraction (a lane is a job-overlap bucket,
not a rate limiter): none of the three writes a table the orchestrator's
portfolio_sync / fx_rates ingest writes, so none needs to serialise against
it. Each owns a single-job lane (NOT one shared ``db_steady`` lane — the
#1526 lesson: a shared lane re-creates the starvation between its members
when one overruns). Scheduled-only, so NOT added to the
``bootstrap_stages.lane`` CHECK (matches ``db_liveness`` / ``db_retry``).

* ``db_positions`` — ``monitor_positions`` (hourly @ :15) only. Reads
  ``positions`` (MVCC-safe vs the orchestrator's concurrent portfolio
  write) and writes only ``position_alerts`` via ``persist_position_alerts``.
* ``db_cusip`` — ``cusip_extid_sweep`` (daily @ :50) only. Writes
  ``unresolved_13f_cusips`` (resolve flag) + ``institutional_holdings``
  (13F rewash). The 13F ingest writers already run on ``sec_rate`` /
  ``db_ownership_inst`` (never ``db``), so extraction introduces no NEW
  race — the sweep already ran concurrently with them.
* ``db_raw_sweep`` — ``raw_payload_retention_sweep`` (#1014,
  manual-only) only. A full sweep nulls ~12k multi-MB payloads in
  bounded batches and holds its lane for minutes; on the catch-all
  ``db`` lane it would starve ``orchestrator_high_frequency_sync``
  (every-5-min, same lane) — the #1526/#1527 starvation class.
* ``db_fsnds_notes`` — ``sec_fsnds_notes_ingest`` (#844, manual-only)
  only. Streams up to 12 FSNDS monthly TSV archives (minutes) — same
  starvation rationale as ``db_raw_sweep``; no SEC HTTP (archives
  pre-fetched by ``sec_bulk_download``).
  Write-safety: sole writer of ``payload_sha256``/``payload_swept_at``;
  the raw-row UPDATE re-checks ``payload IS NOT NULL`` under its row
  lock, so concurrent ``store_raw`` upserts (sec_rate / sec_manifest
  lanes) compose to one of the two legal terminal states either way.
* ``db_ownership_obs`` — ``ownership_observations_sync`` (daily @ :30)
  only — the all-7-category ``ownership_*_current`` repair sweep.
  ``ownership_*_current`` has other writers (the live ingesters + bulk
  paths), but they run on ``db_ownership_*`` / ``sec_rate`` lanes —
  already off ``db`` — so the sweep was NEVER lane-serialised against
  them, and extraction introduces no new race. The only writer the
  sweep shared the ``db`` lane with is ``ownership_observations_backfill``
  (S24 bootstrap stage + weekly Sun 03:00), which DELIBERATELY stays on
  ``db``: it is a ``bootstrap_stages.lane`` entry (moving it would force
  a CHECK migration). Both serialise the only shared mutation — the
  ``refresh_*_current`` DELETE-then-INSERT — per-instrument via
  ``pg_advisory_xact_lock`` (the lane is not the guard), and their
  schedules are staggered (backfill 03:00, sweep 03:30) so they never
  co-fire in practice.

* ``db_size_sample`` — ``pg_size_sample`` (#1564, daily @ 02:15) only. A
  trivial one-row ``INSERT … ON CONFLICT`` snapshot of
  ``pg_database_size``, but daily-precision: on the catch-all ``db`` lane it
  would lose the ``job_source:db`` race to a long lanemate
  (``raw_data_retention_sweep`` @ 02:00, whose filesystem rehash can overrun
  past 02:15) and skip a full day's sample — the #1526/#1527 starvation class
  for a once-daily job. Write-disjoint: sole writer of ``pg_size_sample``, no
  other job touches it. Scheduled-only, so NOT added to the
  ``bootstrap_stages.lane`` CHECK (matches ``db_liveness`` / ``db_raw_sweep``).

* ``db_thesis_dq`` / ``db_thesis_break`` — ``thesis_dq_audit`` (daily 05:12,
  #2014) and ``thesis_break_scan`` (daily 05:22, #2012), one single-job lane
  each (#2052). Both sat on the catch-all ``db`` lane, whose 02:30
  ``fundamentals_sync`` held the lock 6-11h+ FOUR consecutive nights
  (07-13→07-16, released only by the next daemon restart) — the #1526/#1527
  starvation class with an unbounded holder: ``thesis_dq_audit`` had ZERO
  scheduled fires ever. SEPARATE lanes, not one shared audit lane (the
  ``db_liveness``/``db_retry`` lesson above): boot catch-up and manual
  triggers co-fire them despite the 05:12/05:22 stagger. Write-disjointness:
  ``thesis_dq_audit`` is read-only (writes only ``job_runs``);
  ``thesis_break_scan`` is the SOLE writer of ``thesis_break_predicates`` +
  ``thesis_break_events`` (full-census 2026-07-16 — 3 write sites, all in
  ``app/services/thesis_break_scan.py``; the ``break_fired`` stale-mark is a
  read-side EXISTS in ``app/services/thesis.py``, not a write). Scheduled-only,
  so NOT added to the ``bootstrap_stages.lane`` CHECK (matches
  ``db_liveness`` / ``db_retry`` / ``db_size_sample``).

* ``llm_thesis`` — ``thesis_refresh`` (#1919 PR-B) only. Hourly LLM
  thesis generation: a batch of ≤5 local-LLM generations holds the lane
  ~20+ min (≈260s/thesis on a local 14B) — on any shared lane that hold
  is the #1526/#1527 starvation class. Write set (``theses`` /
  ``thesis_runs`` / ``coverage.last_reviewed_at`` / the rankings
  retry-queue demote) is shared only with the filing cascade (``db``
  lane, inside ``fundamentals_sync``) and the manual
  ``POST /instruments/{symbol}/thesis`` path — all three serialise
  per-instrument via the K.3 ``instrument_lock`` session advisory lock
  (the lane is not the guard), and the LLM endpoint itself serialises
  cross-process at the Ollama server-side queue (spec §1). Scheduled-only,
  so NOT added to the ``bootstrap_stages.lane`` CHECK.

* ``risk_metrics`` — ``risk_metrics_refresh`` (#591 PR-B) only. The
  orchestrator-driven weekly risk-metric recompute. DB-only producer (no
  external host), so the lane is purely a write-overlap bucket, not a
  rate limiter. Its own lane keeps it write-disjoint: it is the sole
  writer of the risk-metrics store and reads ``price_daily`` MVCC-safe
  against any concurrent candle writer. Reachable via the orchestrator
  adapter inner-JobLock AND the operator manual-trigger path. NOT in
  SCHEDULED_JOBS (the DAG layer's cadence/freshness gate the run; a
  scheduled row would double-fire), so NOT added to the
  ``bootstrap_stages.lane`` CHECK.

* ``fair_value_band`` — ``fair_value_band_refresh`` (#2009) only. The
  orchestrator-driven 24h deterministic fair-value band recompute. DB-only
  producer (no external host), so the lane is purely a write-overlap bucket.
  Own lane keeps it write-disjoint (sole writer of
  ``fair_value_band_observations`` / ``fair_value_band_current`` /
  ``fair_value_cohort_members``; reads ``financial_periods_ttm`` +
  ``price_daily`` MVCC-safe) AND stops the minutes-long full-universe pass
  from starving the catch-all ``db`` lane's every-5-min orchestrator sync
  (#1526/#1527 class) — same rationale as ``risk_metrics``. Reachable via the
  orchestrator adapter inner-JobLock AND the operator manual-trigger path. NOT
  in SCHEDULED_JOBS (the DAG layer's cadence/freshness gate the run), so NOT
  added to the ``bootstrap_stages.lane`` CHECK.

The final lane is bootstrap-only:

* ``bootstrap`` — ``bootstrap_orchestrator`` (G14). Deliberately
  disjoint from every per-stage lane so the outer
  ``JobLock(bootstrap_orchestrator)`` held by the queue listener
  (``_run_manual``) cannot collide with the inner per-stage
  ``JobLock(<stage_job>)`` acquisitions that bootstrap submits to a
  ``ThreadPoolExecutor`` (``app/services/bootstrap_orchestrator.py:1603``).
  Cross-thread ``ContextVar`` propagation is NOT automatic (see
  ``tests/test_job_lock_reentrancy.py::test_threads_do_not_inherit_held_sources``),
  so the #1184 same-context re-entrancy bypass cannot fire from inside
  an executor worker thread. Picking a fresh lane is the surgical fix:
  no stage owns ``bootstrap``, so cross-thread inner acquisitions never
  contend with the outer lock. Multiple bootstrap triggers still
  serialise on the ``bootstrap`` lane's Postgres advisory lock — the
  ``bootstrap_state.status='running'`` fence is the primary serializer
  at trigger-publish time; this is belt-and-braces at dispatch time.

* ``finra`` — FINRA CDN (cdn.finra.org). 1 req/s polite floor
  (FINRA publishes no explicit rate limit on the equity short
  interest catalog page; CDN robots.txt is 403). Disjoint from
  ``sec_rate`` by construction (different host, no shared per-IP
  budget). Module-global throttle clock + lock at
  ``app/providers/implementations/finra_short_interest.py:46-48``;
  the daily RegSHO provider imports the same module-globals so
  bimonthly + daily ingest share one in-process budget.
  v1 jobs: ``finra_short_interest_refresh`` (G6/#915, bimonthly) +
  ``finra_regsho_daily_refresh`` (G6/#916, daily).

* ``openfigi`` — OpenFIGI v3 mapping host (api.openfigi.com).
  Tier-dependent budget (unkeyed 25 req/60s × 10 jobs = 250
  mappings/min; keyed 25 req/6s × 100 jobs = 25,000 mappings/min).
  Disjoint from every SEC lane by construction (different host —
  no shared per-IP budget with sec_rate). Sole consumer is the
  bootstrap S13 ``cusip_resolver_post_bulk_sweep`` stage
  (#1233 PR-1b). SD-1 cross-reference: ``docs/settled-decisions.md``.
  Resolver: ``app/services/openfigi_resolver.py``.
"""


JobInvoker = Callable[[Mapping[str, Any]], None]
"""Invoker callable shape. PR1a keeps the ``_INVOKERS`` dict zero-arg
shape unchanged; PR1b widens to this contract so bodies can read
operator-supplied params via the queue-consumer dispatch path. The
``Mapping`` contract is read-only — invokers must not mutate the
``params`` dict (mutation would diverge ``job_runs.params_snapshot``
from what the invoker consumed)."""


# ---------------------------------------------------------------------------
# JOB_NAME_TO_SOURCE — the canonical source-lookup registry.
# ---------------------------------------------------------------------------
#
# Construction is deferred to ``_build_job_name_to_source()`` (called
# from a single module-load site at the bottom of this module) so the
# imports of ``SCHEDULED_JOBS`` + ``_BOOTSTRAP_STAGE_SPECS`` happen
# AFTER both modules have populated their registries. Any conflict
# (same job_name appearing in both with different effective sources)
# raises ``RuntimeError`` at import time — fail-fast prevents the
# silent source-lock semantic drift Codex round-1 BLOCKING flagged.


class JobSourceRegistryError(RuntimeError):
    """Raised at module-load when JOB_NAME_TO_SOURCE construction fails.

    Two failure modes:

    * Conflict: the same job_name appears in multiple registries
      (SCHEDULED_JOBS / _BOOTSTRAP_STAGE_SPECS / MANUAL_TRIGGER_JOB_SOURCES)
      with different effective sources.
    * Coverage gap: a bootstrap stage references a job_name not in
      either registry (only triggerable if the bootstrap stage table
      is hand-edited inconsistently).
    """


# ---------------------------------------------------------------------------
# MANUAL_TRIGGER_JOB_SOURCES — source-lock coverage for jobs outside
# SCHEDULED_JOBS + _BOOTSTRAP_STAGE_SPECS.
# ---------------------------------------------------------------------------
#
# Every job_name in this map must resolve to a source via ``source_for()``
# so that ``JobLock`` acquisition succeeds. Entries fall into two
# operational patterns, but the source-lookup contract is the same:
#
# 1. Operator manual-trigger-only jobs (e.g. ``sec_rebuild``). Companion
#    param-metadata at ``app/services/processes/param_metadata.py``
#    ``MANUAL_TRIGGER_JOB_METADATA``; covered by
#    ``tests/test_layer_123_wiring.py``.
#
# 2. Jobs registered in ``app/jobs/runtime.py::_INVOKERS`` but not in
#    ``SCHEDULED_JOBS`` (cadence moved into the orchestrator by #260).
#    Reachable via the orchestrator's adapter inner-JobLock, the
#    ``POST /sync`` HTTP direct-call path, the boot sweep, and via
#    manual queue dispatch. The orchestrator scheduled-cron path's
#    inner JobLock is no longer a self-skip hazard since #1184 —
#    ``JobLock`` detects same-source re-entrancy in the same call
#    context and bypasses the redundant Postgres acquire (see
#    ``app/jobs/locks.py::_HELD_SOURCES`` + spec
#    ``docs/superpowers/specs/2026-05-17-orchestrator-inner-lock-removal.md``).

MANUAL_TRIGGER_JOB_SOURCES: dict[str, Lane] = {
    # sec_fsnds_notes_ingest — #844 unvested RSU/PSU counts from the cached
    # FSNDS monthly archives. Streaming TSV parse + per-accession commits
    # over ~12 monthlies (minutes) → own lane, NOT the catch-all ``db``
    # (long-running; the raw_payload_retention_sweep precedent). No SEC
    # HTTP (archives pre-fetched by sec_bulk_download). Invoker in
    # app/jobs/runtime.py::_INVOKERS; not a bootstrap stage in v1.
    "sec_fsnds_notes_ingest": "db_fsnds_notes",
    # filing_events_skip_tier_cleanup — one-shot retroactive delete
    # (#1013). Pure DB operation (no SEC HTTP) → ``db`` lane, matching
    # the other retention sweeps (financial_facts / raw_data). Companion
    # params in MANUAL_TRIGGER_JOB_METADATA (empty); invoker in
    # app/jobs/runtime.py::_INVOKERS.
    "filing_events_skip_tier_cleanup": "db",
    # populate_canonical_redirects — #819 .RTH operational-duplicate
    # redirect binder. Operator triggers after a universe sync
    # introduces new ``.RTH``-style variants. Pure DB operation
    # (instruments self-join + UPDATE, no HTTP), short-running →
    # ``db`` lane. Invoker landed with #819 but this registry entry
    # was missed, so every manual trigger was rejected at
    # ``source_for()`` (found 2026-06-11, S6 audit: 561/561 RTH rows
    # unbound, zero job_runs ever). Companion params in
    # MANUAL_TRIGGER_JOB_METADATA (empty).
    "populate_canonical_redirects": "db",
    # sec_manifest_tombstone_stale — #1614. The #1131 stale-failed-upsert
    # backfill, retired from SCHEDULED_JOBS (was daily 05:30) to
    # manual-only: it is drained (zero candidates; rows_tombstoned=0 on
    # every run; the #1131 source fix means the candidate shape cannot
    # recur), and each zero-candidate no-op lost the db-lane tick-race
    # vs its siblings (#1526/#1527/#1534 class), surfacing a false-red
    # "schedule missed" verdict on the admin Processes page. Kept in
    # _INVOKERS so an operator can still drain a resurfaced pre-#1131 row
    # via POST /jobs/sec_manifest_tombstone_stale/run. Pure DB scan +
    # UPDATE, short-running → catch-all ``db`` lane (its prior
    # ScheduledJob.source). Without this entry ``source_for()`` KeyErrors
    # and every manual trigger is rejected at JobLock acquisition (the
    # #1413 / populate_canonical_redirects trap).
    "sec_manifest_tombstone_stale": "db",
    # raw_payload_retention_sweep — #1014 payload-null sweep. Pure DB
    # operation but LONG-running (minutes over ~12k rows) → own lane,
    # NOT the catch-all ``db`` (would starve the every-5-min
    # orchestrator_high_frequency_sync; #1526/#1527 class). Params
    # (dry_run) in MANUAL_TRIGGER_JOB_METADATA; invoker in
    # app/jobs/runtime.py::_INVOKERS.
    "raw_payload_retention_sweep": "db_raw_sweep",
    # fx_history_backfill — #1594 operator re-run of the full Frankfurter
    # historical FX backfill into fx_rates_daily. Same lane as the
    # portfolio_eod_snapshot scheduled job (db_eod_snapshot) so the two
    # fx_rates_daily writers serialise (spec §8/H2). One batched HTTP call +
    # idempotent upsert; invoker in app/jobs/runtime.py::_INVOKERS, empty
    # params in MANUAL_TRIGGER_JOB_METADATA.
    "fx_history_backfill": "db_eod_snapshot",
    # risk_metrics_refresh — #591 PR-B weekly risk-metric recompute.
    # Own write-disjoint lane (sole writer of the risk-metrics store; reads
    # price_daily MVCC-safe). Orchestrator-driven (DAG layer "risk_metrics")
    # + manual-trigger-only; NOT in SCHEDULED_JOBS (the layer cadence gates
    # the DAG walk — a scheduled row would double-fire). Companion empty
    # params in MANUAL_TRIGGER_JOB_METADATA; invoker in
    # app/jobs/runtime.py::_INVOKERS.
    "risk_metrics_refresh": "risk_metrics",
    # fair_value_band_refresh — #2009 deterministic fair-value band recompute.
    # Own write-disjoint lane (sole writer of fair_value_band_observations /
    # _current + fair_value_cohort_members; reads financial_periods_ttm +
    # price_daily MVCC-safe). Orchestrator-driven (DAG layer "fair_value_band")
    # + manual-trigger-only; NOT in SCHEDULED_JOBS (the layer's 24h
    # cadence/freshness gate the DAG walk — a scheduled row would double-fire).
    # Own lane (NOT catch-all "db") so the minutes-long full-universe recompute
    # cannot starve the db-lane orchestrator sync (#1526/#1527 class), matching
    # the risk_metrics_refresh rationale. Companion empty params in
    # MANUAL_TRIGGER_JOB_METADATA; invoker in app/jobs/runtime.py::_INVOKERS.
    "fair_value_band_refresh": "fair_value_band",
    # sec_rebuild — operator manual triage (#1155). Per-CIK
    # check_freshness probes against SEC submissions.json; shares the
    # 10 req/s SEC fair-use budget with every other sec_rate consumer.
    "sec_rebuild": "sec_rate",
    # institutional_13f_notice_backfill — one-shot 13F-NT backfill (#1639).
    # Per-day daily-index reads + per-Notice primary_doc fetches against SEC;
    # shares the 10 req/s sec_rate budget.
    "institutional_13f_notice_backfill": "sec_rate",
    # bootstrap_orchestrator — first-install + admin retry trigger (G14).
    # POST /system/bootstrap/run + POST /system/bootstrap/retry-failed
    # publish_manual_job_request(JOB_BOOTSTRAP_ORCHESTRATOR); the queue
    # listener routes through ``_run_manual`` which acquires
    # ``JobLock(job_name)``. Without a registry entry the JobLock
    # constructor's ``source_for(...)`` raised ``KeyError`` and the
    # retry handler had to bypass JobLock via direct-Python invocation
    # (PR #1188 T9-POST).
    #
    # Lane = ``bootstrap`` (NOT ``init``). Bootstrap submits its
    # per-stage invokers to a ``ThreadPoolExecutor``
    # (``app/services/bootstrap_orchestrator.py:1603``); Python's
    # ``ContextVar`` is NOT auto-propagated to executor worker threads
    # (regression-pinned by
    # ``tests/test_job_lock_reentrancy.py::test_threads_do_not_inherit_held_sources``),
    # so the #1184 same-context re-entrancy short-circuit CANNOT fire
    # inside an executor worker. Picking any source that an inner stage
    # also uses (``init`` collides with ``nightly_universe_sync``;
    # ``db`` collides with several Phase E stages) would have the worker
    # thread hit ``pg_try_advisory_lock`` on a lock the listener thread
    # already holds, and the inner stage would fail with
    # ``JobAlreadyRunning``. A fresh ``bootstrap`` lane is disjoint from
    # every per-stage lane — no cross-thread contention is possible by
    # construction. Disjointness invariant pinned by
    # ``tests/test_bootstrap_orchestrator_source_registry.py::test_bootstrap_lane_disjoint_from_all_stage_lanes``.
    "bootstrap_orchestrator": "bootstrap",
    # --- Orchestrator-adapter + manual-queue reach (#1183, #1184) ---
    # #260 (PR #262) moved the jobs below from standalone ScheduledJob
    # rows into orchestrator FULL / HIGH_FREQUENCY cadences. PR1a #1064
    # later introduced the source-registry requirement, orphaning the
    # job_names from coverage (fixed in #1183). The orchestrator
    # scheduled-cron path's inner JobLock(<job>) is now safely re-entrant
    # against the outer ``db`` source-lock (#1184), so db-lane bodies
    # execute end-to-end. Lane assignments reflect each body's real
    # resource profile, not the historical "avoid db" workaround.
    "fx_rates_refresh": "db",
    "daily_portfolio_sync": "etoro",
    "daily_research_refresh": "sec_rate",
    "seed_cost_models": "db",
    "weekly_report": "db",
    "monthly_report": "db",
    # morning_candidate_review — heuristic ranking + recommendation
    # build. Reachable via composite orchestrator adapter
    # (refresh_scoring_and_recommendations) AND manual queue dispatch.
    # DB-bound read + write; matches the existing db-lane sibling jobs.
    # Pre-#1184 this was dormant only because composite adapter never
    # reached the inner JobLock (upstream layers PREREQ_SKIPed on
    # partial-bootstrap dev DBs); without the entry, the orchestrator's
    # scoring layer KeyErrored once the deps started running.
    "morning_candidate_review": "db",
    # finra_short_interest_refresh — FINRA bimonthly short interest
    # (G6/#915). Daily 12:00 UTC cron + manual-trigger. Lane=``finra``
    # so it's disjoint from sec_rate (different host).
    "finra_short_interest_refresh": "finra",
    # finra_regsho_daily_refresh — FINRA RegSHO daily short volume
    # (G6/#916). Daily 23:00 UTC cron + manual-trigger. Same ``finra``
    # Lane — module-global throttle clock shared with bimonthly so the
    # in-process FINRA budget never exceeds 1 req/s combined.
    "finra_regsho_daily_refresh": "finra",
    # --- #1571 outside-DAG ops jobs wired for manual trigger. Each is in
    # ``_INVOKERS`` (VALID_JOB_NAMES) but was retired from SCHEDULED_JOBS and
    # is only a DASHBOARD member of the orchestrator (``JOB_TO_LAYERS`` empty
    # tuple = "Background tasks" panel — display, not dispatch). Without a
    # source entry ``source_for`` KeyErrored and every manual trigger landed
    # ``rejected`` (the populate_canonical_redirects / #1413 trap). All three
    # already finalise via ``_tracked_job`` + ``connect_job``; zero-param
    # (empty MANUAL_TRIGGER_JOB_METADATA). Triage #1571 = wire as
    # manual-trigger-only (the steady-state freshness path moved to the
    # manifest worker / fundamentals derivation).
    #
    # attribution_summary — pure-DB; sole writer of the attribution-summary
    # store (``app/services/return_attribution.py``). Catch-all ``db`` lane.
    "attribution_summary": "db",
    # daily_financial_facts — incremental SEC XBRL facts refresh (SEC HTTP via
    # SecFilings/SecFundamentals providers). Lane ``sec_rate`` — a SEC
    # producer that shares the per-IP rate budget, the same lane as its
    # sibling ``daily_research_refresh``; ``db`` would be wrong (it would hold
    # a DB lane across slow SEC HTTP and block db-lane jobs, the #1478 class).
    # NOT a sole writer of ``financial_facts_raw`` — ``fundamentals_sync`` (db)
    # calls it internally (``scheduler.py`` phase 1) and Stage 9
    # ``sec_companyfacts_ingest`` also writes it — but the cross-lane overlap
    # is benign for correctness: the writer ``upsert_facts_for_instrument`` is
    # keyed last-write-wins/idempotent ("data-layer writes are last-write-wins
    # UPSERTs so the race is benign for correctness, only telemetry-confusing"
    # — ``docs/etl/sources/sec_xbrl_facts.md``). So the lane need not serialise
    # the write.
    "daily_financial_facts": "sec_rate",
    # daily_tax_reconciliation — pure-DB tax-lot ingest + disposal matching;
    # sole writer of ``tax_lots`` / ``disposal_matches``
    # (``app/services/tax_ledger.py``). Catch-all ``db`` lane.
    "daily_tax_reconciliation": "db",
    # --- #1413 bulk-only bootstrap — per-CIK SEC jobs dropped from
    # ``_BOOTSTRAP_STAGE_SPECS`` but KEPT in ``_INVOKERS`` as on-demand
    # (steady-state safety-net + sec_rebuild + Admin "Run now"). Their
    # ONLY source-registry path was the bootstrap stage; dropping the
    # stages orphaned them from ``source_for`` → JobLock KeyError on the
    # next non-bootstrap invocation. Re-home here (lane ``sec_rate`` —
    # SEC per-IP rate clock, same lane they carried as bootstrap stages).
    # ``sec_def14a_bootstrap`` + ``sec_insider_transactions_backfill`` are
    # NOT listed because they remain in SCHEDULED_JOBS (Pass 1 covers them).
    "filings_history_seed": "sec_rate",
    "sec_submissions_files_walk": "sec_rate",
    "sec_form3_ingest": "sec_rate",
    "sec_13f_quarterly_sweep": "sec_rate",
    "sec_n_port_ingest": "sec_rate",
    "sec_n_csr_bootstrap_drain": "sec_rate",
}


def _build_job_name_to_source() -> dict[str, Lane]:
    """Build the canonical job_name -> source lookup.

    Imports happen inside the function to defer the dependency on
    ``app/workers/scheduler.py`` and ``app/services/bootstrap_orchestrator.py``
    until both have populated their respective registries.
    """
    # Local imports to avoid module-load cycles.
    from app.services.bootstrap_orchestrator import (
        _BOOTSTRAP_STAGE_SPECS,
        _effective_lane,
    )
    from app.workers.scheduler import SCHEDULED_JOBS

    registry: dict[str, Lane] = {}

    # Pass 1: scheduled jobs.
    for job in SCHEDULED_JOBS:
        registry[job.name] = job.source

    # Pass 2: bootstrap stages. ``_effective_lane`` consults the
    # ``_STAGE_LANE_OVERRIDES`` map then falls back to the StageSpec.lane;
    # the resulting Lane is the source for that job_name when invoked
    # from bootstrap.
    conflicts: list[str] = []
    for stage in _BOOTSTRAP_STAGE_SPECS:
        bootstrap_source: Lane = _effective_lane(stage.stage_key, stage.lane)  # type: ignore[assignment]
        existing = registry.get(stage.job_name)
        if existing is None:
            registry[stage.job_name] = bootstrap_source
        elif existing != bootstrap_source:
            conflicts.append(
                f"job_name={stage.job_name!r}: scheduled.source={existing!r} vs bootstrap.lane={bootstrap_source!r}"
            )

    # Pass 3: manual-trigger-only jobs (#1155). sec_rebuild + future
    # operator-triggered tools without a cadence — they need source-lock
    # coverage because JobLock acquisition resolves through source_for(),
    # which would otherwise KeyError. Companion param-metadata registry
    # lives at app/services/processes/param_metadata.py
    # MANUAL_TRIGGER_JOB_METADATA.
    for job_name, manual_source in MANUAL_TRIGGER_JOB_SOURCES.items():
        existing = registry.get(job_name)
        if existing is None:
            registry[job_name] = manual_source
        elif existing != manual_source:
            conflicts.append(
                f"job_name={job_name!r}: registered.source={existing!r} vs manual-trigger.source={manual_source!r}"
            )

    if conflicts:
        raise JobSourceRegistryError(
            "Source/lane conflict between SCHEDULED_JOBS, _BOOTSTRAP_STAGE_SPECS, and MANUAL_TRIGGER_JOB_SOURCES:\n  - "
            + "\n  - ".join(conflicts)
            + "\nFix the offending entries so a job_name resolves to the same source from every path."
        )

    return registry


_REGISTRY_CACHE: dict[str, Lane] | None = None


def get_job_name_to_source() -> dict[str, Lane]:
    """Return the canonical job_name -> source lookup, building on first call.

    Lazy construction breaks the import cycle: ``app/workers/scheduler.py``
    imports the ``Lane`` type from this module at module-load time, so
    eagerly building the registry here would re-enter scheduler.py mid-load.
    First call materialises + caches; subsequent calls return the cached dict.

    Any source/lane conflict raises ``JobSourceRegistryError`` at the first
    call site — typically the FastAPI lifespan or the first ``JobLock``
    acquisition, both of which are smoke-tested.
    """
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is None:
        _REGISTRY_CACHE = _build_job_name_to_source()
    return _REGISTRY_CACHE


def reset_job_name_to_source_cache() -> None:
    """Test-only reset of the lazy cache. Production code never calls this."""
    global _REGISTRY_CACHE
    _REGISTRY_CACHE = None


def source_for(job_name: str) -> Lane:
    """Return the source-lock bucket for ``job_name``.

    Raises ``KeyError`` for unknown job_name. Production callers MUST
    have the job in ``SCHEDULED_JOBS`` or ``_BOOTSTRAP_STAGE_SPECS``.
    Test fixtures should register their job in the appropriate registry
    (or use ``JobLock.test_only_per_name`` once that escape hatch lands
    in PR1a).
    """
    registry = get_job_name_to_source()
    try:
        return registry[job_name]
    except KeyError as exc:
        raise KeyError(
            f"unknown job_name {job_name!r}: not found in SCHEDULED_JOBS or "
            f"_BOOTSTRAP_STAGE_SPECS. Either register it in the appropriate "
            f"registry (production) or use JobLock.test_only_per_name (tests)."
        ) from exc


__all__ = [
    "MANUAL_TRIGGER_JOB_SOURCES",
    "JobInvoker",
    "JobSourceRegistryError",
    "Lane",
    "get_job_name_to_source",
    "reset_job_name_to_source_cache",
    "source_for",
]
