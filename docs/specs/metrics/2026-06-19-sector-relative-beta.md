# #1674 — Sector-relative beta/excess in `instrument_risk_metrics`

Status: spec (unshipped). Follow-up to #1634 (SIC→GICS→SPDR crosswalk) + #591 (risk drill).

## Goal

Add a **second OLS beta + excess** computed against each instrument's **sector SPDR
ETF**, alongside the existing SPY beta, to the risk-metrics evidence layer. Display
/ evidence only — NOT a scoring input.

## Source rule

- **Beta methodology** = the settled `risk_v1` OLS: regress on the sorted date
  *intersection* of daily simple returns, `beta = cov/var`, `r2 = corr²`, n−1
  denominators. `app/services/risk_metrics.py::ols_beta` (L390) — reused verbatim
  with the sector ETF series as the regressor. No new math.
- **Sector → benchmark** = #1634 crosswalk `resolve_sector_spdr(sic)`
  (`app/services/sector_classification.py:144`), fail-closed: missing / non-numeric /
  unmapped SIC → `None` (never a guessed sector). SIC source =
  `instrument_sec_profile.sic` (4-digit).
- **Settled-decision (`docs/settled-decisions.md` L204):** market-beta-vs-SPY is
  deliberately **excluded from the scoring penalty** (full-pop r² scan). #1674 is the
  **evidence layer** — it does NOT add sector beta to scoring. A future "sector beta
  as a scoring factor" would need its own full-population r² justification + operator
  sign-off (settled-model change) — explicitly out of scope here.
- **Append-only invariant (L216-218):** `instrument_risk_metrics_observations` is
  append-only; a recompute appends a new row (PK includes `computed_at`).

## Full-population verification (dev DB, 2026-06-19)

Population = **5179** instruments with `risk_v1` `3y` current rows (× 3 windows ≈ the
~15.5k rows #591 backfilled).

- **Coverage: 90.3%** (4676) resolve to a SPDR. The 9.7% (503) unresolved are
  *exactly* the SIC-less set (`no SIC at all = 503`) — ETFs / funds / foreign filers
  → honest `benchmark_missing`, never guessed. Distribution sane: XLV 908, XLF 823,
  XLI 762, XLK 696, XLY 494, XLB 251, XLRE 236, XLP 142, XLE 132, XLC 129, XLU 103.
- **All 11 SPDRs exist** as tradable instruments with ~1004 candles / 1511 days
  (~4.1y) → enough for 1y & 3y OLS; the `full` window aligns to the overlap (honest:
  sector `full` beta is effectively ~4y). The SPDRs carry `sic=None` → self-resolve to
  `None`, so **no XLK-vs-XLF artifact and no self-beta = 1.0** edge.
- **Signal — full-population r² scan** (all 4676 resolved, 3y; 4630 = 98.6% have ≥60
  aligned pairs → computable). HONEST distribution, NOT the cherry-picked panel:
  - **Median is a wash**: median sector r² **0.078** vs median SPY r² **0.074**. The
    typical instrument is idiosyncratic to *both* benchmarks.
  - **The tail is the value**: **13.6%** of names clear sector r²≥0.30 vs **3.2%** for
    SPY (≈4×; confirms #1633's ~3.4% SPY figure on the full pop). r²≥0.50: 2.1% vs 0.0%.
  - Paired, sector r² > SPY r² in **55.0%** of names — better, but NOT uniformly.
  - Panel illustration (3y, dev): XOM 0.77 vs SPY 0.01 (XLE); KO 0.51 vs 0.002; JPM
    0.66 vs 0.33; AAPL 0.29 vs 0.36 (mega-cap *is* the market); GME ~0 both.
  - **Verdict:** worth building as **evidence** (display-only, zero scoring/ranking
    consumers; every row carries its honest r²). For the ~14% sector-coherent names
    sector beta is the right risk lens where SPY beta is noise; for idiosyncratic names
    it's a wash and the low r² says so. The feature adds a *2nd benchmark lens with an
    honest per-row fit*, NOT a claim that sector beta is universally tighter.
    *(Modest median surfaced to operator at sign-off — Codex ckpt-1 flagged the
    original sample-only claim.)*

## Decisions

1. **`metric_version`: NO bump — stays `risk_v1`.** Add the sector columns as
   **nullable**; old rows and unresolved instruments carry NULL (honest — not computed
   then / no sector benchmark). Bumping to `risk_v2` would force a full-universe
   recompute of *byte-identical* existing metrics, flip the single
   `RISK_METRICS_VERSION` constant (every reader stops seeing v1 mid-backfill), and
   orphan the append-only v1 history — large risk, zero benefit. Additive-nullable is
   the invariant-consistent + KISS answer to the issue's "metric_version
   consideration." *(Surfaced for sign-off — the issue flagged it.)*
   - **The nullable status column disambiguates the two NULL meanings** (Codex ckpt-1
     MED): `sector_beta_status IS NULL` ⇒ *not computed then* (a pre-#1674 row, never
     recomputed); `sector_beta_status = 'benchmark_missing'` ⇒ *computed, no sector
     benchmark resolved*. A consumer never has to guess from `sector_beta IS NULL`.
   - **Requires a `docs/settled-decisions.md` entry** explicitly blessing
     *additive-nullable evidence columns under a stable `metric_version`* (so a future
     agent does not "fix" the mixed-schema-under-one-version by bumping). Added in this
     PR.

2. **Column scope** (both `_observations` and `_current`):
   - `sector_benchmark_instrument_id BIGINT` — the resolved SPDR's instrument_id (NULL when unresolved).
   - `sector_beta NUMERIC`, `sector_beta_r2 NUMERIC`, `sector_beta_n_obs INT`.
   - `sector_beta_status TEXT` — same CHECK domain as `beta_status`.
   - `sector_excess_cagr NUMERIC`, `sector_excess_cagr_status TEXT` — per-window excess
     CAGR vs the sector ETF (mirrors `excess_cagr_vs_spy`).
   - **DEFER** `sector_excess_trailing_{1m,3m,6m,1y}` — a straightforward mirror of the
     SPY trailing-excess block; add in a follow-up if the operator wants the granular
     view. Keeps #1674 bounded.

3. **Status comes from REUSING the existing helpers verbatim — no re-derivation, NO
   new enum, NO CHECK change to existing columns** (Codex ckpt-1 HIGH: the two status
   rules differ and the source rule must not be inferred):
   - `sector_beta_status = beta_status(sector_aligned_n, sector_present)`
     (`risk_metrics.py:656`): unresolved / no SPDR series → `benchmark_missing`;
     resolved but `aligned_n < MIN_RETURNS_VOL_BETA` (**60**, not 2) →
     `benchmark_insufficient_history`; else `ok`.
   - `sector_excess_cagr_status` = the status **returned by**
     `excess_cagr(inst_w, sector_w, window)` (`risk_metrics.py:588`) — its own
     overlapping-valid-close / `cagr()` rule (`benchmark_missing` when no sector
     series; `benchmark_insufficient_history` when the aligned overlap can't yield a
     CAGR). This is NOT the beta 60-pair rule.
   - New `sector_*_status` columns get a CHECK identical to the existing `*_status`
     domain (the seven values already enumerated).

4. **Frontend: DEFER.** #1674 = backend evidence + API expose. Sector-beta on the
   RiskPage is a pure-render follow-up (the #591 PR-C pattern). *(Surfaced — operator
   may want it folded in.)*

## Schema — `sql/202_instrument_risk_metrics_sector_beta.sql` (new file; 198 frozen)

Idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for each new column on **both**
`instrument_risk_metrics_observations` and `instrument_risk_metrics_current`
(prevention-log #644: new cols on an existing table need `ADD COLUMN IF NOT EXISTS`;
#1333: never edit an applied migration). `sector_beta_status` / `sector_excess_cagr_status`
get the same `CHECK (... IS NULL OR ... IN ('ok','insufficient_history',
'partial_window','benchmark_missing','benchmark_insufficient_history',
'invalid_price_chain','stale'))`. Wrapped in `BEGIN; ... COMMIT;`.

## Service — `app/services/risk_metrics.py`

- `compute_instrument_risk(...)` (L767): add an optional `sector_closes` arg. When
  present, compute `ols_beta(inst_returns, sector_returns)` over the same window slice
  → `sector_beta` / `sector_beta_r2` / `sector_beta_n_obs` / `sector_beta_status`
  (via the existing `beta_status` helper), and `excess_cagr(inst_w, sector_w, window)`
  → `sector_excess_cagr` / `sector_excess_cagr_status`. When absent (unresolved) →
  all sector fields NULL + status `benchmark_missing`. Honest-status passthrough,
  same as SPY.
- `compute_and_store_risk_metrics(...)` (L1100):
  - `_resolve_benchmark_instrument_ids` already returns the 11 SPDR ids
    (`BENCHMARK_SYMBOLS` ⊇ SPDRs, `app/workers/scheduler.py:2068`) — no change there.
  - **Load each SPDR series ONCE** into a `{spdr_symbol: closes}` cache before the
    instrument loop (mirrors the single global `spy_closes` load — must NOT reload 11
    series × 5000 instruments).
  - **SPDR-presence guard** (Codex ckpt-1 LOW): after `_resolve_benchmark_instrument_ids`,
    `log.warning` + count any of the 11 SPDR symbols that failed to resolve to an
    instrument_id (a missing SPDR would silently turn its whole sector into
    `benchmark_missing`). Surfaced in the backfill DoD output.
  - Load a `{instrument_id: sic}` map for the scoped instruments via **LEFT JOIN /
    `dict.get(iid)` defaulting to `None`** (Codex ckpt-1 LOW: an inner join would drop
    no-SIC instruments before they can be written with `benchmark_missing`). EVERY
    scoped instrument still computes a row — sector fields NULL + status
    `benchmark_missing` when unresolved. Resolve each instrument's SPDR, pick its
    series from the cache, pass to `compute_instrument_risk`.
- `_PendingRow` + `_metrics_row_values` + the dynamic column list + both INSERTs
  (`_append_observation_if_changed` L1237, `_rebuild_current_for_batch` L1271): add the
  sector columns. Content-dedup then naturally appends a new observation on the first
  post-deploy recompute (sector values differ from the prior NULLs).

## API — `app/api/instruments.py`

- `RiskWindowMetrics` (L4991): add `sector_beta`, `sector_beta_r2`, `sector_beta_n_obs`,
  `sector_beta_status`, `sector_excess_cagr`, `sector_excess_cagr_status`
  (+ `sector_benchmark_symbol` resolved at top level like `benchmark_symbol`).
- `get_instrument_risk_metrics` (L5207): add the sector columns to the explicit SELECT
  (L5256) and the row construction; resolve `sector_benchmark_symbol` from
  `sector_benchmark_instrument_id`. Direct SELECT (no savepoint), safe under this
  deployment model (Codex ckpt-1 MED): **single uvicorn process**; `app/main.py`
  lifespan runs `run_migrations` to completion **before** the app accepts requests;
  the **both-tables ALTER is one `BEGIN; … COMMIT;`** so there is no reachable
  partial-schema state (one table altered, not the other); a migration failure is
  **fail-fast at lifespan** (app does not serve at all) — never a per-endpoint 500
  against a half-migrated schema. The smoke test boots the real lifespan against the
  dev DB and would catch a bad SELECT. (#1677's `UndefinedColumn` lesson is about
  *savepoint-guarded enrichment* readers degrading to `row=None`; scoring.py does NOT
  read sector cols — untouched.)

## Tests (pure-logic, `-m "not db"`)

- `compute_instrument_risk` with a synthetic sector series of **≥60 aligned pairs** →
  populates `sector_beta` + `sector_beta_status='ok'`; `sector_beta` matches a
  hand-computed `ols_beta`.
- Unresolved (sector_closes=None / empty) → sector fields NULL,
  `sector_beta_status='benchmark_missing'`, `sector_excess_cagr_status='benchmark_missing'`.
- Sector series present but **< MIN_RETURNS_VOL_BETA (60) aligned pairs** →
  `sector_beta_status='benchmark_insufficient_history'` (the real threshold, not 2).
- `resolve_sector_spdr` already unit-tested in #1634; add the no-SIC → `None` case
  (the SPDR-self guard) if not covered.

## Skill / docs upkeep (same PR)

- `.claude/skills/metrics-analyst/SKILL.md` §323-334 + the metrics table (L25-26): add
  sector beta/excess to the `risk_v1` definition.
- `docs/settled-decisions.md`: note sector beta is evidence-only (penalty exclusion
  unchanged).

## ETL Definition-of-Done

1. **Backfill** — re-run `compute_and_store_risk_metrics` over the universe on dev
   (`POST /jobs/risk_metrics_refresh/run` or the `risk_metrics` orchestrator layer).
   Record rows + instruments touched.
2. **Smoke panel** AAPL/GME/MSFT/JPM/HD — record `sector_beta` + status (expect: AAPL
   XLK, JPM XLF, HD XLY resolved; GME XLY low-r²; all honest).
3. **Cross-source** one sector beta vs a public source (e.g. XOM-vs-XLE).
4. **Live** `GET /instruments/AAPL/risk-metrics` renders the sector fields.
5. Jobs proc restart onto the merge SHA for steady-state sector-beta ingest.
