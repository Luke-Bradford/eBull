# fundamentals_sync LLM cascade removal (#2065)

Status: proposal, Codex ckpt-1 findings folded (3 BLOCKING + 3 MAJOR).
Fix for the upstream disease #2052 deliberately left open: `fundamentals_sync`
(nightly 02:30, db lane) has not completed since 2026-07-12.

## Problem (empirical, dev job_runs)

| night | wall time | end state |
| --- | --- | --- |
| 07-08 → 07-10 | 23–28 min | success |
| 07-11, 07-12 | 23 min | failure (phase-1 XBRL — separate, out of scope) |
| 07-13 → 07-17 | 6.1–11.0 h | `orphaned: reaped at boot` ×5 |

The cliff is the 2026-07-11 16:33 daemon restart onto #1919 PR-B code: the
Phase-3 cascade's `make_llm_clients` went from `LLMProviderNotConfigured` →
skip (no Anthropic key) to the local-first default that always resolves. Since
then the cascade runs 57–96 thesis generations per night (thesis_runs
`trigger='cascade'`, status ok: 57/96/90/74/86) at an observed 300–500 s per
call through the Ollama `Semaphore(1)` — an observed 6–13 h of serial LLM work
inside a data job; the job dies at the next daemon restart, every night.
(Wall-time figures are observed bands, not invariants; the structural claim is
only that cascade wall time is unbounded in stale-count while the data phases
are bounded.)

## Full-population falsification of the issue premise

Issue #2065 option 1 claims the cascade is redundant with #273 event rules +
hourly `thesis_refresh`. Full-pop check (all 403 ok cascade runs, 14 d):

- **368/403 (91%) are `no_thesis` auto-mints** — instruments with NO prior
  thesis, minted because `find_stale_instruments` rule 1 flags any analysable
  changed-CIK instrument. This is universe-wide thesis creation that bypasses
  the operator-gated wide-backfill sequencing (gates #2008/#2007/#2010/#1995/
  #2002 + the ~07-24 census). Not redundant — **unsanctioned**. Removal
  restores the gate.
- **35/403 (9%) are refresh-class** (existing thesis + qualifying event).
  Overlap with `thesis_refresh`'s held ∪ top-20 scope: **0–2 runs/night**
  (417 instruments have theses; 392 sit outside held ∪ top-20). So the
  issue's "drained hourly by thesis_refresh" claim is FALSE as stated —
  the event RULES are the same predicate function; the drain SCOPE is not.

Verdict: option 1 (remove) is right, but it needs a one-leg scope widening of
`thesis_refresh` to make the redundancy claim true for the refresh class.

## Rerank-debt reality check (Codex BLOCKING 1+2, full-pop verified on dev)

The K.2 `RERANK_NEEDED` machinery exists so the cascade's end-of-run
`compute_rankings` drains rerank debt. Live state:

- **Every `scores.scored_at` run since 07-12 (all distinct values, 7 d window)
  is a `morning_candidate_review` fire** (14:08 / 22:50 / 12:43 / 13:49 UTC).
  The cascade orphans before reaching its rerank step, so its rerank path has
  been dead for 5 nights — rankings freshness is already carried entirely by
  the morning review's unconditional scoring phase (7,829 scored per run).
- **`cascade_retry_queue` is empty right now** (0 rows, 0 `RERANK_NEEDED`,
  0 at-cap) — the queue-drop consequence set is the empty set on the live DB.

So removing the queue + `demote_to_rerank_needed` bridge codifies the current
working reality: rankings recompute at the next `morning_candidate_review`
(the accepted-latency path every `thesis_refresh`-minted thesis already
rides today), and thesis-failure retry becomes "still stale next hour →
re-picked by the bounded hourly drain" instead of a parallel outbox.
At-cap retry-exhaustion state: concept is obsolete post-removal (the hourly
drain re-attempts a still-stale instrument at most once per fire, bounded by
batch ≤5 and the advisory lock); the table is retained until the follow-up
drop (below), so any rows that appear during the transition stay inspectable.

## Design

1. **Remove Phase 3 from `daily_financial_facts`** (`scheduler.py`): the
   `make_llm_clients` + `changed_instruments_from_outcome` + `cascade_refresh`
   block and the `cascade_failures` channel of the combined raise. The
   `outcome.failed` / `plan.failed_plan_ciks` channels keep raising.
   **Accepted observability change (Codex MAJOR 1):** LLM thesis failures stop
   turning `fundamentals_sync` red; they surface as `thesis_runs`
   status='failed' rows + the instrument staying stale in
   `find_stale_instruments` (re-picked hourly until success). A data job's
   health signal should not have been coupled to LLM health.
2. **Widen `_thesis_refresh_candidates` with a third leg** (explicit SQL
   invariant, Codex MAJOR 3):

   ```sql
   SELECT i.instrument_id
   FROM instruments i
   WHERE i.is_tradable = TRUE
     AND EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
   ORDER BY i.symbol, i.instrument_id
   ```

   `is_tradable = TRUE` mirrors the cascade's own
   `changed_instruments_from_outcome` filter (behaviour parity); held leg
   keeps including non-tradable held names (unchanged). Python-side dedup
   against held + ranked, appended last. Priority: held → ranked →
   has-thesis. Adds **zero** new `no_thesis` mints by construction (every
   third-leg candidate has a thesis; held/top-N first-mint behaviour
   unchanged). Backlog on a filing burst drains at ≤5/run hourly with the
   existing deferred counter/log; the batch-limit bump decision already
   scheduled at the census point (#2010 close-out) covers scaling.
3. **Delete the K.2 outbox machinery — two-phase (Codex BLOCKING 3)**:
   - **This PR:** delete `refresh_cascade.py` (cascade was sole reader AND
     sole writer of `cascade_retry_queue`), its tests, the scheduler Phase-3
     block, and `thesis_refresh`'s `demote_to_rerank_needed` call. **No
     migration — the table stays.** Old daemon code keeps working against
     the still-present table until the operator restart; new code never
     touches it.
   - **Follow-up ticket (filed at merge):** `DROP TABLE cascade_retry_queue`
     after the daemon restart is verified (first `fundamentals_sync` success
     row). Retired-writer ordering per the #2008 lesson: restart before the
     destructive step, never concurrent with it.
4. **Move `instrument_lock` (K.3) into `app/services/thesis.py`** — its only
   remaining consumer is `thesis_refresh` serialization. No import-cycle
   risk: standalone psycopg advisory-lock context manager; `thesis.py`
   already sits below the scheduler in the import graph.

Out of scope: the 07-11/07-12 phase-1 XBRL failures (independent disease —
ticket if it recurs post-fix); `_warm_fair_value_band` dies with the cascade
(nightly `fair_value_band_refresh` already covers band freshness for
scheduled mints — same freshness class as today's `thesis_refresh` path).

## Failure modes considered

- Filing lands for a held/top-20 name → unchanged (same-scope detection,
  ≤1 h drain vs same-run; consistent with long-horizon posture per issue).
- Filing lands for an outside-scope name WITH thesis → now drained hourly by
  leg 3 (better than today's next-02:30-cascade latency).
- Filing lands for a name WITHOUT thesis → no auto-mint (deliberate: that is
  the wide-backfill operator gate).
- Thesis generation fails → thesis_runs failed row; instrument still stale →
  re-picked next hourly fire (replaces the outbox retry path; bounded).
- compute_rankings unavailable on a given morning → rankings age until the
  next successful morning review — identical to today's live behaviour
  (cascade rerank has not landed since 07-12; verified above).

## Rollout

One PR. Operator: jobs daemon restart required to activate (02:30 job +
hourly job both live in the daemon). No migration in this PR. Verify next
morning: `fundamentals_sync` job_runs row `success` with wall < 1 h; zero
`trigger='cascade'` thesis_runs rows after the restart timestamp; hourly
`thesis_refresh` note shows the widened candidate count. Then the follow-up
drop ticket executes.
