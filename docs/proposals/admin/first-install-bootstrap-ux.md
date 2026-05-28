# First-install bootstrap progress UX

**Status**: Proposal · 2026-05-25 · draft 2 (Codex-1 BLOCKERs folded)
**Owner**: TBD (first-install UX epic)
**Source memos**: `.scratch/bootstrap_ux_review_{data_engineer,frontend_design,ui_ux_pro_max,pm}.md` (2026-05-25, 4-lens parallel review)
**Related tickets**: #1225 (bulk-ingester `rows_processed=NULL`); #1271 (per-stage progress columns wiring)

---

## 1. Problem

The first-install bootstrap progress page (`frontend/src/pages/ProcessDetailPage.tsx:1068-1259`) is the first surface a fresh installer of eBull sees. Bootstrap takes **75-180 min wall-clock** across **~27 stages on 6 parallel lanes**. Today the page shows lane-grouped technical cards with SUCCESS / RUNNING / PENDING badges and almost no per-stage progress.

Operator feedback verbatim (2026-05-25):

> *"No one has an idea if anything is progressing, stuck or how long it will be. A loading bar sort of thing — otherwise with the current layout, no one has an idea if anything is progressing, stuck or how long it will be. What would make this page more useful to a user, even if its used once, its there first experience loading the application."*

The structural root cause is split between **(a) missing telemetry** — the columns to drive progress bars exist on `bootstrap_stages` (sql/140) and the FE reads them (`ProcessDetailPage.tsx:1186-1221`), but **no stage writes them** — and **(b) UX framing** — the page is built for power operators, not first-install users.

## 2. Goals

1. From t=0, a first-install user sees visible motion + an honest ETA. No "is anything happening" anxiety.
2. The user can walk away from the tab and come back informed (tab title, browser notification, server-authoritative reload).
3. Failures get an actionable card, not a truncated red strip.
4. A power operator can flip a toggle and see every technical surface they have today — no regression on Priya-the-PM.
5. The telemetry that drives the new UX is cheap, doesn't add lock contention, and surfaces stuck stages.

**Non-goals.** Steady-state job dashboards (post-bootstrap admin); production alerting; multi-environment progress aggregation; new database tables.

## 3. Personas (from PM memo §1)

- **Sam-the-evaluator** — installed on a Friday to see if it's worth a real run. Closes the tab in 90s if it looks hung.
- **Priya-the-PM** — committing capital. Wants every lever visible.
- **Alex-the-distracted** — left the tab, came back 30 min later. Wants to know: did it crash? am I close?

All three load the same URL. The page must serve all three without forcing Sam to be Priya.

## 4. Telemetry & data layer

### 4.1 Per-stage instrumentation classification

From data-engineer memo §1. 22 of 27 stages have a free target source (file-list, cohort count, manifest count). Only 5 stages (S4 / S5 / S26 SEC directory walks + 2 black-box) need timer-only fallback.

| # | Stage | Cheap target source |
|---|---|---|
| 1 | `universe_sync` | `SELECT COUNT(*) FROM instrument_universe` |
| 7 | `sec_bulk_download` | already archive-grid native (`sec_bulk_orchestrator_jobs.py:113-133`) |
| 8 | `sec_submissions_ingest` | `len(zipfile.ZipFile(archive).namelist())` |
| 9 | `sec_companyfacts_ingest` | `len(zf.namelist())` |
| 10 | `sec_13f_ingest_from_dataset` | INFOTABLE.tsv is **streamed** (`_iter_tsv` at `sec_13f_dataset_ingest.py:571`) — no upfront count. **Fall back to indeterminate (variant B).** |
| 11 | `sec_insider_ingest_from_dataset` | `len(holdings) + len(transactions)` — both already loaded into memory (`sec_insider_dataset_ingest.py:493-494`) |
| 12 | `sec_nport_ingest_from_dataset` | FUND_REPORTED_HOLDING.tsv is **streamed** (`_iter_tsv` at `sec_nport_dataset_ingest.py:488`) — no upfront count. **Fall back to indeterminate (variant B).** |
| 16 | `sec_first_install_drain` | `SELECT COUNT(*) FROM sec_filing_manifest WHERE status='pending'` |
| 25 | `fundamentals_sync` | universe CIK count |
| ... | (full table in data-engineer memo §1) | |

For the 5 #1225 bulk ingesters specifically: target sources split into TWO classes (re-pass IMPORTANT fold):

- **S8 + S9 (file-per-CIK ingesters)**: `len(zf.namelist())` walks the ZIP central directory and is instant. Determinate bar.
- **S11 (insider, loads both TSVs into memory)**: `len(holdings) + len(transactions)` from the already-loaded `_open_tsv()` dicts. Determinate bar.
- **S10 + S12 (multi-million-row streamed INFOTABLE / FUND_REPORTED_HOLDING)**: TSV streamed via `_iter_tsv` — by design no `_open_tsv` load (avoids OOM on 30M-row archives, see `sec_13f_dataset_ingest.py:530+`). No upfront row count. `processed_count` ticks per row; `target_count` stays NULL. UI falls back to the indeterminate-bar variant (§6.3 variant B): `"1,234 processed · running 4m"` + shimmer.

### 4.2 #1225 fix sketch — smallest code surface

Codex-1 caught two BLOCKERs in draft 1: (a) `JobTelemetryAggregator.maybe_flush()` *only* flushes to `job_runs` today (`app/services/job_telemetry.py:281`, `:327`) — the bootstrap variant is a **net-new method**, not "reuse"; (b) the inner ingesters accept *no* `agg` argument today and routing the flush through the ingest connection would put heartbeats inside the parse/write transaction — exactly the atomicity bug §4.2 was supposed to prevent. Draft 2 makes both explicit.

Strategy: **NEW** `BootstrapStageProgress` helper (own connection per flush, never reuses the ingest conn) + **NEW** `agg: BootstrapStageProgress | None = None` arg on inner ingesters.

Edits — no new schema migration, no new endpoint:

1. **`app/services/job_telemetry.py`** (+~50 LOC, NET-NEW class):
   ```python
   class BootstrapStageProgress:
       def __init__(self, database_url: str, bootstrap_run_id: int, stage_key: str,
                    *, min_flush_interval_s: float = 5.0): ...
       def set_target(self, target: int) -> None: ...
       def record_processed(self, n: int = 1) -> None: ...
       def maybe_flush(self) -> None:
           """Open OWN psycopg.connect, autocommit=True, single UPDATE, close.
           Never reuses caller's ingest connection — atomicity guarantee."""
   ```
   Pattern mirrors `_record_archive_result` (`app/services/sec_bulk_orchestrator_jobs.py:113-133`) which already opens its own short-lived connection for the same atomicity reason. NOT a method on the existing `JobTelemetryAggregator` (avoid coupling — `job_runs` flush and `bootstrap_stages` flush have different lifecycle owners).

2. **`app/services/sec_bulk_orchestrator_jobs.py`** (~15 LOC × 5 jobs): in the outer job wrapper for S8 `sec_submissions_ingest_job`, S9 `sec_companyfacts_ingest_job`, S10 `sec_13f_ingest_from_dataset_job`, S11 `sec_insider_ingest_from_dataset_job`, S12 `sec_nport_ingest_from_dataset_job` — instantiate `BootstrapStageProgress(database_url, run_id, stage_key)` from the active `_current_running_bootstrap_run_id()` (line 90), pass into the inner `ingest_*_archive` callback, drop on stage exit. Run-id lookup returns `None` outside a bootstrap dispatch — agg is then `None`, ingester runs in legacy non-instrumented path (zero-risk fallback for the manual-trigger and scheduled-cron paths).

3. **Inner ingesters — per-file target source** (Codex-1 IMPORTANT fix: not all stages denominate by `zip namelist`):
   - **`app/services/sec_submissions_ingest.py`** (`ingest_submissions_archive` at line 267): target = `len(zf.namelist())`. Loop = per-entry. `agg.record_processed()` after each entry.
   - **`app/services/sec_companyfacts_ingest.py`** (line 148): same — target = `len(zf.namelist())`. Loop = per-CIK file.
   - **`app/services/sec_13f_dataset_ingest.py`** (line 485, INFOTABLE loop at `:571`): INFOTABLE.tsv is **streamed** via `_iter_tsv` — multi-million rows, can't be loaded with `_open_tsv` (line 530+ comment confirms). No upfront target available. Skip `set_target()`; `record_processed()` still ticks per holding. UI shows variant B (indeterminate bar + "1,234 processed · running Xm").
   - **`app/services/sec_insider_dataset_ingest.py`** (line 442, holdings + transactions loops): `len(holdings) + len(transactions)` — both are `_open_tsv` results already loaded in memory (`sec_insider_dataset_ingest.py:493-494`). Set target up-front; `record_processed()` per row across both loops.
   - **`app/services/sec_nport_dataset_ingest.py`** (line 374, holdings loop at `:488`): FUND_REPORTED_HOLDING.tsv is **streamed** via `_iter_tsv`. Same situation as 13F — no upfront target. Skip `set_target()`; UI shows variant B.

4. **Inner ingester signature delta** (~5 LOC each, all 5 files): add `agg: BootstrapStageProgress | None = None` keyword arg. Pre-loop: `if agg: agg.set_target(...)` (skip on S10/S12 — no upfront target). Inside the entry walk: `if agg: agg.record_processed(); agg.maybe_flush()` — at the **TOP** of each iteration, BEFORE any early-continue branch or fan-out inner loop. See "Progress-tick placement rule" below for the exact invariant + reasoning.

**Critical atomicity rule** (draft-1 wording kept; draft-2 implementation makes it true): `maybe_flush` MUST open its own `with psycopg.connect(self._database_url, autocommit=True) as flush_conn` for every flush. The 5s cadence × ~50ms per psycopg connection acquire = ~1% time overhead per stage. Worth the atomicity guarantee.

**Cadence**: 5s wall-clock floor inside `BootstrapStageProgress.maybe_flush` (`if time.monotonic() - self._last_flush < 5.0: return`). At 5s × N concurrent stages in parallel lanes (peak ~6), system-wide load is ~1.2 UPDATE/sec. Negligible. First flush always fires (`_last_flush == 0`) so motion is visible inside the first 5s.

**Total surface**: ~80-100 LOC across 7 files (`job_telemetry.py` + `sec_bulk_orchestrator_jobs.py` + 5 ingesters). **No** schema migration. **No** new endpoint. **No** API model change in this phase — `app/api/processes.py:692-694` already selects `processed_count` + `target_count`, and `BootstrapTimelineStageResponse` already exposes them (`frontend/src/api/types.ts:1417`). The FE component at `ProcessDetailPage.tsx:1186-1221` already renders the result FOR S8/S9/S11. For S10/S12 the FE component returns `null` today when `target_count` is NULL and `processed_count` is 0; P1 fills `processed_count` but the UI bar stays absent until P3 ships variant B (indeterminate shimmer). Operator gets visible motion on the variant B card via `processed_count` text + elapsed timer — same activity feed, just no bar percentage.

**Progress-tick placement rule** (Codex-1 re-pass IMPORTANT fold): `record_processed()` must fire **once per TSV row / archive entry CONSIDERED**, NOT once per successful write. Concretely:

- Tick BEFORE early-continue branches (orphan accession skip, retention-cutoff skip, unmapped-CIK skip, etc.). Otherwise large skipped cohorts (e.g. S12 retention drops) stall the UI with no perceived progress.
- Tick OUTSIDE any share-class / instrument fan-out inner loop. S11 fans out per holding × per owner — the tick belongs at the TSV row level, NOT inside the fan-out (otherwise the count exceeds the denominator).
- Placement looks like: `for row in _iter_tsv(...): if agg: agg.record_processed(); agg.maybe_flush(); ... per-row work + early continues + fan-out ...`. Tick first, then work. Mirrors the `result.holdings_seen` / `result.transactions_seen` accumulator pattern that already lives at the same scope in each ingester.

### 4.3 Overall progress + ETA algorithm

Primary: **weighted-by-historical-wallclock** (data-engineer memo §2). Per-stage `p50_seconds` and `p90_seconds` from completed `bootstrap_runs` via:

```sql
SELECT stage_key,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY dur) AS p50_seconds,
       percentile_disc(0.9) WITHIN GROUP (ORDER BY dur) AS p90_seconds,
       COUNT(*)                                          AS n_samples
FROM (
  SELECT bs.stage_key,
         EXTRACT(EPOCH FROM (bs.completed_at - bs.started_at)) AS dur
  FROM bootstrap_stages bs
  JOIN bootstrap_runs   br ON br.id = bs.bootstrap_run_id
  WHERE br.status='complete' AND bs.status='success'
    AND bs.started_at IS NOT NULL AND bs.completed_at IS NOT NULL
) t
GROUP BY stage_key;
```

Overall %:
- `SUM(p50_seconds WHERE stage terminalised) / SUM(p50_seconds all stages)`
- Partial credit for currently-running stage: `(running_stage_p50 * stage_progress_fraction)`, where `stage_progress_fraction = processed_count / target_count` if both set, else `min(0.95, elapsed / p50)`

**First-ever-install fallback**: ship `_BOOTSTRAP_STAGE_HISTORICAL_P50_SECONDS` constant next to `_BOOTSTRAP_STAGE_SPECS` at `bootstrap_orchestrator.py:1035`, seeded from `project_1233_run2_measurement.md` numbers. Update once per release. Cold install still gets a credible projection.

**Per-stage ETA** (data-engineer memo §3):

1. `target_count > 0 AND processed_count > 0` → `((now - started_at) / processed_count) * (target - processed)`. Display: `processed/target (PP%) · ~Xm left`.
2. `processed_count > 0` only → no projection. Display: `processed processed · running Xm`.
3. Black-box / never written → render `typically Xm (p90 Ym)` from historical SQL. When `elapsed > p90`, surface amber "slower than usual" chip (NOT stuck — slow-but-progressing is different).
4. Never-run-before + currently running → use shipped baseline; cap "Estimating..." at `max(30s, 0.05 * baseline)`.

### 4.4 Stuck detection

`last_progress_at` is the lever. Add `_BOOTSTRAP_STAGE_STUCK_THRESHOLD_S` map next to stage catalogue:

- Fast stages (S1-S7, S13): 60s
- DB-lane bulk (S8-S12): 5 min
- sec_rate per-CIK walkers (S14-S22): 5 min
- Long phases (S15, S16, S22, S23, S25): 10 min
- Final derivations (S24): 10 min

Escape valve = existing cancel path (`process_stop`, surfaced via `bootstrap_state.py:676`). On stuck: FE renders `may be stuck — Xm since last write · Cancel` chip.

### 4.5 Deprecation flag

`bootstrap_stages.expected_units` and `units_done` (sql/129 lines 91-92) are unused (grep confirms zero writers). sql/140 added the correct replacement pair (`processed_count` / `target_count` / `last_progress_at`). Recommend marking the sql/129 columns deprecated in their COMMENT and dropping in a future migration.

## 5. API layer additions

Codex-1 caught two issues in draft 1: (a) the API surface citation was wrong — `app/api/bootstrap.py:286` is the `/system/bootstrap/status` endpoint, NOT the per-stage timeline; the correct cite is `app/api/processes.py::get_bootstrap_timeline` (`app/api/processes.py:648`); (b) several "additions" listed in draft 1 (`display_name`, `processed_count`, `target_count`, `warning`, `archives`) already exist on `BootstrapTimelineStageResponse`. Draft 2 restates the surface correctly.

### 5.1 Current state — what's already on the timeline payload

- **Endpoint**: `GET /processes/{process_id}/timeline` (only `process_id="bootstrap"` returns data) — `app/api/processes.py:648-700`.
- **SELECT**: `app/api/processes.py:692-694` — selects `stage_key, stage_order, lane, job_name, status, started_at, completed_at, last_error, rows_processed, processed_count, target_count`. Notably **does NOT select `last_progress_at`** despite the column existing on `bootstrap_stages` since sql/140.
- **FE response model**: `BootstrapTimelineStageResponse` at `frontend/src/api/types.ts:1417-1436` — exposes `status, processed_count, target_count, started_at, completed_at, last_error, warning, archives, display_name, stage_key, job_name, lane, stage_order`.

So `processed_count` / `target_count` are ALREADY in the payload — P1 telemetry (§4.2) will populate them with NO API change. Existing FE renders the progress UI (`frontend/src/pages/ProcessDetailPage.tsx:1186-1221`) — determinate bars for S8/S9/S11 (target known); for S10/S12 the existing component still returns `null` until P3 ships variant B, but `processed_count` is in the payload waiting for P3 to consume. **This is the no-API-change pathway for the 3 stages that have a target up-front.**

### 5.2 Additions for P2-P6 (timing + ETA + activity feed + structured failure)

Drive these in the order of the implementation phases (§9) — none of them block P1.

**`bootstrap_stages` SELECT** (`app/api/processes.py:692-694`) — add to the column list:

- `last_progress_at` — exists on schema (sql/140), not currently in the SELECT. Required by §6.3 variant C ("last write 8s ago") and §6.5 stuck-vs-progressing affordances. Bridge to the existing `BootstrapStageProgress.maybe_flush` writer in §4.2.

**`BootstrapTimelineStageResponse` model additions** (`frontend/src/api/types.ts:1417-1436` + server-side Pydantic model in `app/api/processes.py`):

- `last_progress_at: string | null` — from the SELECT above. P2.
- `eta_seconds: number | null` — server-computed per §4.3 case 1. P2.
- `eta_band_p25_p75: [number, number] | null` — historical range, server-side. P2.
- `historical_p50_seconds: number | null` — for cold-start "typically Xm" copy. P2.
- `friendly_display_name: string` — **replaces semantics of existing `display_name`**, not coexists. `display_name` today is humanised stage_key via `app/api/processes.py:645` (`stage_key.replace("_", " ").strip().title()`). P7 overrides with hand-authored plain-English copy.
- `friendly_subtitle: string | null` — supporting copy (e.g. "SEC EDGAR · ~12k filers"). P7.
- `optional: boolean` — declared on `StageSpec` dataclass (see §5.4). Drives the "Skip" button affordance on failure (§7.2). P5.
- `structured_error: { class: string, user_facing_message: string, retryable: boolean } | null` — **augments** the existing `last_error` field (don't remove it; the raw string still belongs in the "Copy error details" clipboard payload). Class is a stable enum (`http_5xx`, `parse_error`, `rate_limit`, `quota_exhausted`, `cap_unmet`, ...). P5.

**`BootstrapTimelineResponse` top-level additions** (`frontend/src/api/types.ts:1438-1449` + server-side):

- `overall_progress_fraction: number` — server-computed per §4.3. P2.
- `overall_eta_seconds: number | null`. P2.
- `overall_eta_band: [number, number] | null`. P2.
- `recent_events: { timestamp, stage_key, friendly_display_name, kind: 'started'|'completed'|'failed', summary: string }[]` — top 5, for the activity feed (§6.3). P4 — cheap server-side compute from existing `bootstrap_stages` rows ordered by `COALESCE(completed_at, started_at) DESC LIMIT 5`.

### 5.3 Field-coexistence rules (Codex-1 IMPORTANT fold)

- `display_name` (existing, computed from stage_key) **stays** for backward-compatibility during P1-P6. P7 ships a new `friendly_display_name` and migrates FE consumers; the legacy `display_name` is then removed in a subsequent cleanup PR — NOT in this epic.
- `last_error` (existing) **stays** even after P5 ships `structured_error`. Raw error still serves the "Copy error details" clipboard button (§7.2). FE renders one OR the other, never both (avoid double-surfacing the same failure).
- `rows_processed` (existing, legacy from sql/129) is **not** the same as `processed_count` (sql/140). The former is set by some stages' final completion summary (`app/api/processes.py:692`); the latter is the live heartbeat counter from §4.2. Keep both, source of truth differs.

### 5.4 StageSpec dataclass migration (Codex-1 NIT fold)

`StageSpec` at `app/services/bootstrap_state.py:136` is the in-process dataclass; `_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:1035-1193` is the catalogue. Today the dataclass has no `optional`, no `friendly_display_name`, no `friendly_subtitle`, no `historical_p50_seconds`.

P5 + P7 add these fields with defaults so existing call sites keep compiling:

```python
@dataclass(frozen=True)
class StageSpec:
    # ... existing fields ...
    optional: bool = False
    friendly_display_name: str | None = None  # NULL → fall back to humanised stage_key
    friendly_subtitle: str | None = None
    historical_p50_seconds: float | None = None  # NULL → use SQL p50 lookup
```

`_BOOTSTRAP_STAGE_SPECS` entries get filled in incrementally — no big-bang migration. The `_BOOTSTRAP_STAGE_HISTORICAL_P50_SECONDS` constant from §4.3 can either live alongside or be inlined onto the per-stage entries; recommend inlining for spec ownership in ONE place.

## 6. UX layer

### 6.1 Page hierarchy

(ui-ux-pro-max memo §1)

```
┌─ Banner (amber) ──────────────────────────────────────────────┐
│ First-install bootstrap · this runs once                       │
└────────────────────────────────────────────────────────────────┘
┌─ HERO ─────────────────────────────────────────────────────────┐
│  Stage 16 of 27 · ~12 min remaining · done around 14:48        │
│  ████████████████░░░░░░░░░░░░░░░░░░░  59%                       │
│  Currently running:  ●▌ Downloading insider trades             │
│  Safe to leave this tab — we'll keep working. [Notify me]      │
└────────────────────────────────────────────────────────────────┘
┌─ Run metadata strip (quiet) ───────────────────────────────────┐
│  Run #3  ·  started 14:02  ·  elapsed 23m  ·  6 lanes · 0 fail │
└────────────────────────────────────────────────────────────────┘
┌─ Activity feed (aria-live=polite, top 5) ──────────────────────┐
│  • 14:24:12  ✓ Loaded 8,712 institutional filers (9m 15s)      │
│  • 14:18:03  ✓ Loaded insider trades for 412 companies (62s)   │
│  ...                                                            │
└────────────────────────────────────────────────────────────────┘
┌─ Stage list (flat, chronological by stage_order) ──────────────┐
│  [compact PENDING / full RUNNING / compact SUCCESS / loud FAIL]│
│  ...                                                            │
└────────────────────────────────────────────────────────────────┘
```

Tailwind: hero rail per `BootstrapProgress.tsx:106-108` (`border-l-2 border-blue-400 bg-blue-50/60 ... dark:border-blue-700 dark:bg-blue-950/40`) for visual continuity with the existing dashboard panel. Bar uses the existing per-stage primitive (`ProcessDetailPage.tsx:1196-1206`) at `h-2` instead of `h-1`.

### 6.2 Default view = simple. Lanes behind a toggle.

(PM memo §3, frontend-design memo §4)

Default view hides:
- Lane group headers (`ProcessDetailPage.tsx:1113`)
- `stage_key` / `job_name` sub-labels (`ProcessDetailPage.tsx:1174`)
- Archive chip grids (`ProcessDetailPage.tsx:1239-1255`)

A `[Show technical detail]` toggle in the page header persists per-browser via **localStorage** (NOT sessionStorage — Priya doesn't want to flip every reload). When ON, the existing lane grid + chips + raw labels render BELOW the simplified view. Additive, not destructive.

### 6.3 Stage card variants (RUNNING / PENDING / SUCCESS / FAILED)

(frontend-design memo §2, ui-ux-pro-max memo §2)

**Variant A — RUNNING with known target**:
```
●▌ RUNNING   Loading insider trades                4m 12s  ⋯
              ████████████░░░░░░░░░░░░░░░  1,234 / 5,678  (21.7%)
```

**Variant B — RUNNING with progress but no target** (rate-based):
```
●▌ RUNNING   Downloading financial filings          4m 12s
              ░▒▓▒░▒▓▒░▒▓▒░  (indeterminate)   1,234 processed
```

**Variant C — RUNNING with no progress data at all** (until #1225 fix lands):
```
●▌ RUNNING   Setting up trust directory             12m 04s
              ░▒▓▒░▒▓▒░▒▓▒░  working · last write 8s ago
```

Indeterminate bar = `relative h-1 w-full overflow-hidden rounded bg-slate-200 dark:bg-slate-800` + inner shimmer `bg-gradient-to-r from-transparent via-sky-400/70 to-transparent animate-[shimmer_1.6s_ease-in-out_infinite]`. Honest pulse — does NOT imply known forward progress. Today's code at `ProcessDetailPage.tsx:1186-1191` returns null in this case; this variant is the critical gap to close.

**Variant D — PENDING with reason** (cheap client-side derivation: lowest-`stage_order` running stage in same lane is the blocker):
```
○  PENDING   Loading mutual-fund holdings
              Waiting on insider trades (~9 min remaining)
```

**Variant E — SUCCESS, collapsed one-liner**:
```
✓ Loaded 1,847 stock symbols   ·   47s
```

**Variant F — FAILED, always-expanded** (PM memo §5):
```
┌──────────────────────────────────────────────────────────────┐
│ ✗  Setup paused — needs a minute                             │
│   We couldn't reach SEC EDGAR (the U.S. government's filings │
│   server). Their server returned a temporary error (HTTP     │
│   503). This usually clears in a few minutes.                │
│   [Retry now]  [Skip for now]  [Copy error details]          │
│   13 of 27 steps already done — those won't re-run.          │
└──────────────────────────────────────────────────────────────┘
```

Promote failure out of the inline red strip (`ProcessDetailPage.tsx:1230-1237`) into a top-of-page card. **Skip button only renders when `optional: true` on the stage** — never offer skip as a generic escape hatch.

### 6.4 Density: collapse-completed wins

(ui-ux-pro-max memo §2) For a one-time experience: SUCCESS rows compress to ~32px, RUNNING expand to full card, PENDING ~28px slim, FAILED always expanded. Active work always sits at consistent vertical position because completed work compresses.

### 6.5 Stuck-vs-progressing affordances

(frontend-design memo §3, ui-ux-pro-max memo §5)

1. Elapsed timer on the card (`started_at` → now). Always present. Turns `text-amber-600` past p75.
2. Pulsing dot in status pill (`animate-pulse` on a 3×3 dot, reuse `BootstrapProgress.tsx:121`).
3. `last write Xs ago` inline. Amber past stage-specific threshold (§4.4). At >10× median, **the shimmer bar stops animating** — a frozen bar is the strongest "lost contact" signal we can give without copy.

### 6.6 Motion + a11y

(ui-ux-pro-max memo §5 + §7)

- `transition-[width] duration-500 ease-out` for known-target bars (already in code).
- Shimmer for indeterminate: `1.6s ease-in-out infinite`, gated `motion-safe:`.
- RUNNING → SUCCESS: 200ms checkmark draw, then compress to one-liner.
- Tab-switch-back: `ring-1 ring-sky-400/50` on recently-updated rows, fades over 2s.
- Progress bar a11y: `role="progressbar"` + `aria-valuenow/min/max/valuetext`; indeterminate omits `aria-valuenow`.
- Activity feed: `<div role="status" aria-live="polite" aria-atomic="false">`.
- Stage cards NOT in tab order (27 stops is a footgun); add skip-link "Jump to currently running stages".

### 6.7 Cold-start (first 30s)

(frontend-design memo §7, ui-ux-pro-max memo §9)

- Render banner + hero immediately with `0 / 27` and `Estimating…`.
- Pre-seed all 27 stages as PENDING rows the moment `run_id` is known — DOM doesn't shrink/grow once first response lands (satisfies `loading-error-empty-states.md:27-39` symmetric-DOM rule).
- First RUNNING flip fires the recently-updated ring as the "it's working" moment.

## 7. Copy + tone

### 7.1 Plain-English stage names (PM memo §4)

Lead with verb in present-continuous, no acronyms in headline, technical name + source in muted parenthetical. 5 worked examples:

| stage_key | friendly_display_name | friendly_subtitle |
|---|---|---|
| `sec_companyfacts_ingest` | Downloading 10 years of financial filings | SEC EDGAR |
| `sec_insider_ingest_from_dataset` | Loading insider trading history | who's buying / selling at each company |
| `cusip_universe_backfill` | Matching every stock to its global ID | needed before we can track holdings |
| `nightly_universe_sync` | Pulling the list of tradable stocks | eToro |
| `sec_13f_filer_directory_sync` | Finding every hedge fund + institutional investor | ~8,700 13F filers |

Full table for all 27 stages: TBD — produce alongside `_BOOTSTRAP_STAGE_HISTORICAL_P50_SECONDS` constant. Live next to `_BOOTSTRAP_STAGE_SPECS` so spec ownership is one place.

### 7.2 Failure card copy template

```
### Setup paused — needs a minute

We couldn't reach {friendly_source_name} ({plain_what_it_is}).
{error_class_description}. {retry_recommendation}.

[Retry now]  [Skip for now]?  [Copy error details]

{N} of 27 steps already done — those won't re-run.
```

`Skip for now` button renders only when `stage.optional === true`. `Copy error details` puts `raw_error + run_id + stage_key + timestamp` on the clipboard.

### 7.3 Completion screen (PM memo §8)

No auto-redirect. Show for 5s, then auto-advance to `/dashboard`:

```
You're all set.

eBull is ready. We loaded:
  • 9,847 tradable stocks
  • 11 years of financial filings (1.2M facts)
  • 8,712 institutional investors tracked

[ Open dashboard → ]
```

Numbers from the run summary already in `BootstrapTimelineResponse`. **No confetti** — wrong register for an investment product.

### 7.4 Tone

Matter-of-fact, quietly competent. Justified by `.claude/CLAUDE.md:9-20` posture (*demo-first, deterministic, auditable*) + existing `BootstrapNudgeBanner.tsx:29` voice. No exclamation marks, no apology, no emoji, no playful one-liners. Direct.

## 8. Walk-away affordances

(PM memo §6)

- **Tab title updates** — `document.title = "eBull — Setup 47%"` while running; `"eBull — Setup paused"` on failure; `"eBull — Ready"` on complete. Cheapest possible win for Alex.
- **Browser Notifications API** — prompt for permission *after* the user has been on the page ~30s and overall progress is <90% (no point asking near the end). Single prompt. Fire one notification on `complete`, one on `partial_error`. Copy: *"eBull is ready — your dashboard is set up."* / *"eBull setup paused — needs your attention."*
- **No email** — overkill for a one-time self-hosted install.
- **Reload-survival** — all state must come from `GET /system/bootstrap/status` + `fetchBootstrapTimeline` (`ProcessDetailPage.tsx:120`). No client-only `useState`. **Verify** during P3 — confirm a cold incognito reload mid-run renders identical content.

## 9. Implementation phases

Smallest-first sequencing. Each phase is shippable independently.

| Phase | Scope | Estimate | Unlocks |
|---|---|---|---|
| **P1 — telemetry instrumentation** | §4.2 #1225 fix sketch (~80-100 LOC, 7 files). Wires `processed_count` + `last_progress_at` writes from 5 bulk-ingest jobs + 5 inner ingesters; `target_count` writes for S8/S9/S11 (3 of 5). | ~1 day | Existing FE progress-bar component (`ProcessDetailPage.tsx:1186-1221`) starts rendering bars for S8/S9/S11 with zero FE changes. S10/S12 get `processed_count` motion but no bar until P3 ships variant B. **Immediate win for 3 of 5 bulk stages.** |
| **P2 — overall progress + ETA algorithm** | §4.3 SQL + `_BOOTSTRAP_STAGE_HISTORICAL_P50_SECONDS` constant + API additions (§5). Hero banner UI (§6.1). | ~2 days | First "X / 27 stages, ~Y min remaining" visible. |
| **P3 — flat stage list + simple/technical toggle** | §6.2 default-flat + localStorage toggle. Stage card variants A-E (§6.3). Cold-start DOM (§6.7). | ~2 days | Sam stops panicking. Priya gets her view via the toggle. |
| **P4 — activity feed + stuck detection** | §4.4 `_BOOTSTRAP_STAGE_STUCK_THRESHOLD_S` + `recent_events` API field + activity feed UI (§6.5). | ~1 day | Trust signal #1 (PM memo §7). |
| **P5 — failure UX redesign** | §6.3 variant F + structured error + Retry / Skip / Copy buttons. `optional: boolean` declared on stage specs. | ~1.5 days | Failures stop confusing first-install users. |
| **P6 — walk-away affordances + completion screen** | §8 + §7.3. Tab title hook, Notifications API permission flow, "You're all set" screen, reload-survival verification. | ~1 day | Alex returns to a useful tab. |
| **P7 — plain-English copy + friendly display names** | §7.1 full table for all 27 stages. Author content alongside historical baselines. | ~0.5 day | Sam sees verbs in English, not stage_keys. |

**~8-9 days end-to-end**, parallelisable across BE + FE. P1 + P2 alone deliver the operator-asked outcome ("loading bar that tells me how long"). P3-P7 are progressively higher-trust enhancements.

## 10. Risks

1. **Historical baselines stale**: shipped `_BOOTSTRAP_STAGE_HISTORICAL_P50_SECONDS` will drift as upstream sources change shape. Mitigation: regenerate constant once per release from the SQL in §4.3 against the prior 30 days of completed runs. Lint that warns on > 30% drift.
2. **Lock contention from progress UPDATEs**: ~1 UPDATE/sec across all stages is negligible — but if the orchestrator ever runs >10 lanes in parallel and stages start emitting at 5s each, we could see ~2 UPDATE/sec. Still negligible, but pin the cadence floor at 5s minimum, never lower.
3. **Indeterminate-bar shimmer fatigue**: if every #1225-class stage shows shimmer for >5 min, the page can feel "stuck in shimmer". Mitigation is partial: P1 lands `target_count` for S8/S9/S11 (3 of 5 bulk ingesters) — those become determinate. S10/S12 cannot have target without doing a counting pre-pass over the multi-million-row INFOTABLE / FUND_REPORTED_HOLDING TSV (per `sec_13f_dataset_ingest.py:530+`), so those two ride the indeterminate variant from P3 onwards. Acceptable: indeterminate is honest, and the rotating `processed_count` + elapsed timer keep motion visible. If post-launch operator feedback shows fatigue, follow-up could add a heuristic target estimator (zip member uncompressed size × ~50 bytes/row).
4. **Wall-clock ETA confidence**: "done around 14:48" is psychologically expensive if it slips. Mitigation: always render the band ("typical: 10-18 min"). Never render bare point estimate.
5. **`processed_count` racy on partial archive parse**: if an archive parses 8k of 12k rows then errors, processed_count=8000 but stage status=failed. The FE must render the failed-but-partial state honestly (display the 8000/12000 frozen + the error card). Frontend lens already plans this in variant F (always-expanded).

## 11. Out of scope

- Steady-state job dashboards (post-bootstrap operator surface — different audience, different surface).
- Multi-environment progress aggregation (prod vs staging vs dev).
- WebSocket push for sub-second progress updates — 60s poll + 5s telemetry flush is the right cadence for this surface. WebSockets are overkill for a one-time wait.
- Restructuring `bootstrap_stages` schema — sql/140 already added the right columns; sql/129 unused columns can be deprecated later.
- Localization — single-locale (en-US) is fine for v1; the copy is short enough to translate later.
- Mobile-specific responsive design — operator tools, desktop-first by convention (`operator-ui-conventions.md`).

## 12. Open questions

1. Should the `_BOOTSTRAP_STAGE_HISTORICAL_P50_SECONDS` constant ship as a Python dict (next to stage specs) or as a JSON snapshot file? Python dict gives lint reach but couples regeneration to a code release.
2. Is `optional: boolean` declared at the stage spec level (`_BOOTSTRAP_STAGE_SPECS`) or computed from cap dependencies (a stage is optional if no downstream stage requires its `provides` cap)? Computed is more correct but harder to surface to FE.
3. Notifications API permission prompt: at 30s into the wait, or immediately on page load? Apple's HIG says "only when the user takes an action that benefits from it" — neither is perfect. Recommend 30s as a compromise.
4. Activity feed: top 5 events, or scroll-back-able log? Memo recommends 5 (PM §7). Anyone disagree?

## 13. References

- Memo sources: `.scratch/bootstrap_ux_review_data_engineer.md`, `bootstrap_ux_review_frontend_design.md`, `bootstrap_ux_review_ui_ux_pro_max.md`, `bootstrap_ux_review_pm.md`
- Existing tickets: #1225, #1271
- Live evidence: Run #8 receipts (in flight 2026-05-25)
- Skills referenced: `frontend/loading-error-empty-states.md`, `frontend/operator-ui-conventions.md`, `data-engineer/SKILL.md`, `metrics-analyst/SKILL.md`
