# OpenFIGI key drift-heal nudge (#1791, follow-up to #1344)

Follow-up to the pre-flight nudge (`2026-06-28-openfigi-key-nudge.md`,
PR #1790). Acceptance bullet 3 of #1344 was deferred here: re-surface the
nudge **mid-run** if bootstrap stage S13 (`cusip_resolver_post_bulk_sweep`)
is crawling with no key — catching the operator who dismissed/missed the
pre-flight nudge and is now watching S13 run slowly.

## Premise verification (full-path, dev)
No data-treatment / SEC rule — UX surface. The two facts the banner relies
on are both verified on the running dev stack, not assumed:
- `bootstrap_stages.started_at` is `TIMESTAMPTZ` (`sql/129`), set to
  `now()` the instant a stage flips to `running`
  (`bootstrap_state.py:534-547`). The API serializes it with a `Z`
  suffix (verified live: `'2026-06-03T18:56:03.770819Z'`), so the client
  `Date.now() - new Date(started_at).getTime()` elapsed calc is correct
  in UTC — NOT a naive-datetime parsed in the browser's local zone
  (prevention-log naive-`datetime` class, line 297). No backend change.
- `openfigi_key_present` already on `BootstrapStatusResponse` (#1344) and
  the FE type. The S13 stage status + `started_at` are already on
  `BootstrapStageResponse`. The FE has everything; this is pure FE.

## Show condition
Mid-run, all must hold:
- `openfigi_key_present === false`, AND
- top-level `data.status === "running"` (defends against a stale/projected
  S13 row leaking from a non-running snapshot — Codex ckpt-1), AND
- the S13 stage (`cusip_resolver_post_bulk_sweep`) is found in `stages[]`
  with `status === "running"` and a non-null `started_at` that parses to a
  finite epoch (NaN → hidden — Codex ckpt-1), AND
- `Date.now() - new Date(started_at).getTime() > 5 min`, AND
- not dismissed for this run (sessionStorage keyed by `current_run_id` — see
  below).

The S13 stage being `running` implies bootstrap is mid-run, so this window
is disjoint from the pre-flight nudge's `pending`/`partial_error` window —
the two banners never show together. Negative / NaN elapsed (clock skew,
future or unparseable `started_at`) → hidden (the `> 5min` test fails).

### Threshold = 5 min, NOT the issue's "2 min" — deliberate (Codex ckpt-1)
The issue suggested ">2 min". Aligned instead to the orchestrator's
`_REAPER_GRACE_SECONDS = 300` (`bootstrap_orchestrator.py:2669`). Rationale:
`reap_orphaned_running_stages` resets a `running` stage ONLY when its
worker is **provably dead** (the `openfigi` lane advisory lock is not held
in any PG session). A genuinely-slow keyless S13 sweep (~48 min,
`config.py`) holds that lock the whole time, so the reaper never resets it.
Therefore a stage still `running` *past the 5-min grace* is provably a
LIVE, slow worker — exactly the "no key" cause. Below 5 min a `running` S13
is ambiguous (could be a just-crashed worker awaiting the reaper); firing
there would mis-attribute a dead stage to "missing key". 5 min eliminates
that false positive at the cost of ~3 min extra latency on an advisory
nudge whose underlying run lasts ~48 min — a good trade.

Detection latency: the 60 s poll (mirrors the sibling banners) drives the
re-render, so the banner appears within ≤60 s of the 5-min mark. No
separate `setInterval` timer for the clock — the poll IS the tick.

## Dismiss semantics — per-run, session-scoped (distinct from pre-flight)
sessionStorage (`openfigiKeyDriftHealDismissedRunId` = the dismissed
`current_run_id`), NOT localStorage. Show only when
`current_run_id !== <dismissed run id>`. The operator can't fix this
mid-run without restarting the API, so a dismiss should silence the
*current run's* nag but genuinely re-arm on the NEXT run (a fresh
`current_run_id`) — plain sessionStorage `"1"` would wrongly suppress
later runs in the same tab session too (Codex ckpt-1). Contrast the
pre-flight nudge's persistent localStorage dismiss (a deliberate "no key"
choice that sticks across reloads).

## Component + shared constants
New `OpenFigiKeyDriftHealBanner.tsx` (sibling in `components/dashboard/`,
rendered in `AppShell` after `OpenFigiKeyNudgeBanner`). Self-contained
poll-effect like the two existing nudge banners (repo convention: one
banner component per concern).

`S13_STAGE_KEY` and `OPENFIGI_KEY_URL` are currently private to
`OpenFigiKeyNudgeBanner.tsx`. Extract to a shared
`components/dashboard/openfigiNudge.ts` and import from both — single
source of truth (the S13 stage key MUST NOT drift between the two banners).

Copy (amber tone to read as "running slow", distinct from the indigo
advisory pre-flight): "⏳ OpenFIGI CUSIP resolution (S13) has been running
several minutes with no `OPENFIGI_API_KEY` — it runs ~10× faster with a
(free) key. Set the key and restart the API to speed up the next run."
Link https://www.openfigi.com/api. Dismissible.

## Tests
Frontend (`OpenFigiKeyDriftHealBanner.test.tsx`). Drive the clock with a
fixed fake timer / explicit `started_at` offset so the threshold is
deterministic:
- shows when S13 `running` + `started_at` > 5 min ago + top-level running +
  no key + not dismissed;
- hidden when key present;
- hidden when top-level `status !== "running"` even with a stale S13
  `running` row;
- hidden when S13 `running` but `started_at` < 5 min ago (threshold);
- hidden at exactly 5 min (condition is strict `>`);
- hidden when S13 `running` but `started_at` is in the future;
- hidden when `started_at` is unparseable (NaN);
- hidden when S13 not `running` (pending/success);
- hidden when S13 stage absent;
- dismiss writes `current_run_id` to sessionStorage and hides;
- pre-seeded dismiss for the SAME `current_run_id` → hidden on mount;
- pre-seeded dismiss for a DIFFERENT run id → shown (re-armed next run).

## Verification
- Unit tests green. Dev currently has the key set (`openfigi_key_present:
  true`) so the banner is correctly invisible on dev — assert the logic via
  unit tests + a manual `openfigi_key_present:false` mock walkthrough.
