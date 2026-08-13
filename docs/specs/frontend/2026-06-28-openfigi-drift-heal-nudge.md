# OpenFIGI key drift-heal nudge (#1791, follow-up to #1344)

## Problem
The pre-flight `OpenFigiKeyNudgeBanner` (#1790) recommends setting
`OPENFIGI_API_KEY` *before* bootstrap. It explicitly hides once status is
`running` (too late this run). Gap (#1344 acceptance bullet 3, deferred):
the operator who dismissed/missed the pre-flight nudge is now watching
S13 (`cusip_resolver_post_bulk_sweep`) crawl with no key — and gets no
reminder. Re-surface a *drift-heal* nudge mid-run when S13 has been
`running` > 2 min without a key.

## Premise check (falsified on the data path, not assumed)
The issue framed this as "needs live per-stage timing exposed to the
frontend". **False — the data already rides in the existing response.**
`BootstrapStageResponse` (app/api/bootstrap.py:104-118) already carries
`stage_key`, `status`, and `started_at`; the FE type mirrors it
(frontend/src/api/bootstrap.ts:60,65). So this is **frontend-only** — no
backend field, no migration, no daemon restart.

## Source rule / cited numbers
Not a data-treatment decision — no SEC reg. Reuses the same two documented
figures as #1790: rate limit 100× (settled-decision 635); operator-facing
wall-clock ~10× (app/config.py:88-92). Copy cites the **wall-clock** figure
only → no perf-claim-lint trip (no `perf` label, no `## Performance impact`).

## Timing computation — clock-skew posture (prevention-log #822)
`bootstrap_stages.started_at` is `TIMESTAMPTZ` (sql/129_bootstrap_state.sql:88)
→ psycopg yields a tz-aware UTC `datetime` → FastAPI serializes ISO-8601 with
offset → `new Date(started_at)` parses an unambiguous absolute instant. So
there is **no naive-datetime local-time trap** (the #822 / DetailPanel-type
mistake). Elapsed = `Date.now() - new Date(started_at).getTime()`.

The one residual skew is browser wall-clock vs server clock. Accepted, not
fixed with a server-now field, because:
- This is an **advisory** banner, not a correctness predicate (#822 burned
  on freshness *flipping*; here a false +/- just shows/hides a harmless
  "set your key" reminder).
- The poll interval is 60 s, so the threshold is already ±60 s fuzzy;
  normal NTP browser skew (seconds) is well inside that noise floor.
- A multi-minute browser clock skew is pathological and breaks far more
  than this banner.
Documented in the component so a future reviewer sees the tradeoff was
conscious, not missed.

## Design — one component, two modes
Extend `OpenFigiKeyNudgeBanner` (do not add a second polling component —
both would hit `/system/bootstrap/status` every 60 s for the same data).
`nudgeMode(data, nowMs)` returns `"preflight" | "driftheal" | null`:
- `preflight` (unchanged): `pending`, or `partial_error` with S13 not
  terminally `success`/`skipped`.
- `driftheal` (new): `status === "running"` AND the S13 stage is itself
  `running` AND `Date.now() - started_at > 2 min`.
- The two are mutually exclusive by top-level status, so at most one shows.

### Dismiss scope (acceptance bullet: distinct from pre-flight)
- preflight: persistent `localStorage` (`openfigiKeyNudgeDismissed`) —
  unchanged. A deliberate "no key" choice sticks across reloads.
- driftheal: **run-scoped** `sessionStorage`, keyed by `current_run_id`
  (`openfigiKeyDriftHealDismissed:<run_id>`) — Codex ckpt-1 fix. A plain
  session key would stay dismissed across a *next* bootstrap run in the same
  browser session, contradicting "re-reminded on the next run". Keying by
  `current_run_id` means a new run = new key = the nudge returns. The
  operator can't fix the *current* run anyway; the reminder is for the next.
- The two dismiss states are **independent** (separate keys, separate React
  state, mode-specific checks): dismissing the pre-flight nudge must NOT
  pre-dismiss the drift-heal one — catching the operator who dismissed
  pre-flight is the whole point.

### Threshold vs poll latency + stale-data safety (Codex ckpt-1 + ckpt-2)
ckpt-1: crossing 2:00 must render promptly, not wait a full poll. ckpt-2: a
naive standalone "now" tick can surface the nudge from **stale** data — if
S13 finished between fetches, an independent tick still crosses the threshold
against the last-seen `running` snapshot until the next fetch corrects it.

Resolution that satisfies both: **drive the clock from the fetch.** Poll at
30 s and store `{data, fetchedAtMs}` together; `nudgeMode(data, fetchedAtMs)`
computes the threshold against the moment the snapshot was true. Then:
- appears within ≈one 30 s interval of crossing 2 min (ckpt-1), and
- every appearance is backed by a fetch that just confirmed S13 `running`
  (ckpt-2) — a later re-render with a newer `Date.now()` cannot resurrect a
  stale `running` snapshot, because the clock is pinned to fetch-time.
30 s polling is cheap: bootstrap is a rare, bounded operator event.

### Guards
- driftheal hides if the S13 stage is absent, not `running`, has null
  `started_at`, or an unparseable `started_at` (`Number.isNaN(getTime())`).

### Drift-heal dismiss — storage-failure resilience (Codex ckpt-2)
Dismiss writes run-scoped `sessionStorage` AND sets in-memory React state
(`driftHealDismissedRun`). The in-memory state guarantees the banner hides
this mount even if `sessionStorage.setItem` throws; sessionStorage carries
the dismiss across remounts within the session. Render hides on either.

### Copy (honest, mode-specific — conscious deviation from "same copy")
Literal "same copy" would say "before bootstrap" mid-run, which is false:
S13 is already running unkeyed; setting the key now helps **future runs**
(after a restart), not this one. So drift-heal copy is honest about that:
> ⏳ CUSIP resolution (S13) has been running over 2 minutes without
> `OPENFIGI_API_KEY`. Setting a (free) key and restarting the API speeds
> future runs ~10×. — [Get a free key]
Same link (`https://www.openfigi.com/api`), same ~10× wall-clock figure.

## Tests (frontend, vitest, fake timers for the 2-min threshold)
- driftheal shows: running + S13 running + started_at 3 min ago + no key.
- driftheal hidden: S13 running but started_at 1 min ago (under threshold).
- driftheal hidden: running but S13 stage absent / not running.
- driftheal hidden when key present.
- driftheal dismiss → sessionStorage set, hidden; survives remount within
  session; independent of the localStorage preflight key.
- existing preflight tests unchanged and still green.

## Out of scope
- Server-now field for absolute skew-immunity (see posture above — not
  warranted for an advisory banner).
