# Fair-value band v2 — reproducibility + tail-robustness

**Status:** proposal, 2026-07-13.
**Extends (does not replace):** [`2026-07-12-deterministic-fair-value-band.md`](./2026-07-12-deterministic-fair-value-band.md) §4.5 synthesis (the "blend + outer envelope" operator decision, 2026-07-12).
**Supersedes:** #2022 ("P/S band-envelope winsorization"). Winsorization is **empirically refuted** as the fix — see §2. #2022 to be re-scoped/closed.
**Method version:** `fvb_v1` → **`fvb_v2`** (wing outputs change; base unchanged — see §5).

---

## 1. Problem

Two coupled requirements on the deterministic comps band (`app/services/fair_value_band.py`):

1. **The P/S bull is too wide for a minority** (AAPL bull ≈ 752 vs base ≈ 310; universe bull/base median ≈ 1.6). #2022 proposed cohort-level P/S winsorization.
2. **The band must be reproducible** — "results we can trust time and time again" across nightly recomputes as prices/peers drift (operator requirement, 2026-07-13). The band feeds the LLM thesis writer as evidence + a **base-to-base** divergence audit (`compute_divergence` compares `band_base`, never `bull`).

## 2. Research + source rule (why #2022 winsorization is the wrong fix)

Four independent reviews (Codex methodology; a cited literature sweep; an unbiased cold-start methodologist; an adversarial red-team) + three empirical tests on the dev full population. Convergent findings:

- **Member-winsorization cannot move `peer.p75`** — *proven on AAPL's real cohort*. Fresh SIC-357 P/S members: `0.8, 1.0, 3.6, 4.6, 7.6, 7.9, 16.3, 24.3, 25.1, 25.1`. The high values are a **genuine cluster of 4 peers, not outliers**. Every principled clip (95th, 90th, Tukey `p75+1.5·IQR`=50, `p50+1.5·IQR`=35) leaves p75 unmoved. The one true outlier (731×) is already stale-excluded. Order-statistic arithmetic: clipping above p75 cannot change p75 (Hyndman & Fan 1996, sample-quantile definitions).
- **A 50/50 mean-blend of the wings is worse, not better.** `own.p80` is a near-max order statistic of ~6 quarters (dev full-pop: 76% of own-legs have ≤6 points, median depth = 6). Under the current `max()`, `own.p80` binds only ~24% of two-sided P/S legs (defers to the stable large-sample `peer.p75` otherwise); a 50/50 blend gives it permanent half-weight → **less** reproducible. Inverse-variance weighting (Gauss–Markov) says the noisier small-sample statistic should be weighted *down*, not equally. Baker & Ruback (1999): arithmetic averaging of multiples is upward-biased (harmonic mean is minimum-variance).
- **`max()`/`min()` outer envelope is not the defect.** The base is a *location* estimator; the wings are a *dispersion/support* estimator — using different operators is correct, not "inconsistent" (adversarial red-team; resolves the Codex "regime-selector" objection). Judge an uncertainty band by **calibration, not width** (Mauboussin) — narrowing 45% of bands with zero calibration evidence manufactures false precision.
- **The root cause is peer comparability.** P/S is a function of **net margin *and growth*** (Damodaran: `P/S = margin · payout · (1+g)/(r−g)`); a SIC+size cohort is neither margin- nor growth-matched. *Proven partial-fix limit:* margin-matching AAPL (27.2% net margin) still retains ANET (38.3%, P/S 24×) and FTNT (27.5%, P/S 16×) — margin-matched but **high-growth**; AAPL is mature, so its own P/S is ~9×. Matching margin **and** growth would starve the cohort below `MIN_PEERS`. **AAPL's wide bull is a genuine comps-comparability limit for a unique mega-cap — not cleanly removable by any single synthesis change** without distorting the majority or starving the cohort. The band already labels this via `quality_status`.

**Sources:** Damodaran — P/S↔margin ([ps.pdf](https://pages.stern.nyu.edu/~adamodar/pdfiles/eqnotes/ps.pdf)), relative-valuation skew→median ([relval](https://pages.stern.nyu.edu/~adamodar/pdfiles/country/relval.pdf)); Baker & Ruback 1999 *Estimating Industry Multiples* (harmonic mean min-variance); Hyndman & Fan 1996 *Sample Quantiles in Statistical Packages*; McKinsey/Koller *The right role for multiples* (match peers on ROIC & growth); Mauboussin (calibration over width); inverse-variance weighting (Gauss–Markov).

## 3. Full-population verification (dev, 567 `ok` bands, 2026-07-13)

- Per-leg envelope ratio (`high_mult/base_mult`): **P/S** (n=291) median 1.63, p90 2.79, p95 **3.08**, max 7.18; **P/E** (n=498) median 1.39, p95 2.62; **P/B** (n=119) median 1.33, p95 **4.08**, max 7.44.
- **Widest names are peer-only** (no own-history): `mean`/`cap`/`winsor` against own-history cannot touch them; only a peer-relative fixed cap bounds them.
- P/S coverage: 153/291 two-sided, 64 peer-only, 74 own-only. Own-binds-under-max: 37/153 (24%).
- Down-side ratio (`base_mult/low_mult`) p95: P/S 4.29, P/E 2.70, P/B 2.41.

## 4. Design — v2 (four parts)

Keep the level/dispersion split and the `min/max` outer-envelope operator (§2). Fix the **inputs and guards**, not the operator.

| # | Change | Fixes | Base-neutral? |
|---|--------|-------|:-:|
| **1** | **Interior quantiles**: own history uses **p25/p75**, never p20/p80 (matches the peer side, which already uses p25/p50/p75) | small-sample wing noise → reproducibility | ✅ base uses p50 only |
| **2** | **Fixed per-leg cap**: `high_mult ≤ base_mult · R_up[m]`, `low_mult ≥ base_mult / R_dn[m]`; `R` frozen, calibrated from full-pop | the **peer-only tail** (max 7×) nothing else bounds | ✅ affects wings only |
| **3** | **Cohort membership hysteresis**: don't re-pick the size-refined 8-nearest each night unless membership changes materially | residual base jitter across recomputes | ⚠ can nudge base |
| **4** | **Companion-variable peer screen**: P/S→net-margin **+ growth** band, P/B→ROE, P/E→growth; widen monotonically only to hold `MIN_PEERS` | the **root cause** of AAPL-class width | ⚠ changes base |

**Parts 1+2 are base-neutral** (they change only bull/bear, never the audited `base_value`), so the base-to-base divergence audit is undisturbed. Parts 3+4 touch the base and are heavier.

## 5. Phasing + gating

- **Phase 1 (this spec's implementation): parts 1 + 2.** Base-neutral wing hardening — interior quantiles + fixed cap + a deterministic-interpolation regression test. `fvb_v1→v2` (wing outputs change; observations audit integrity). Full recompute is orchestrator-driven (`freshness.py` detects "no rows for current method_version" → reschedules the 24 h `fair_value_band_refresh`); operator can force via the recompute trigger. **Not a scoring `model_version`** — the band is not a scoring input.
- **Phase 1b (follow-up ticket): part 3** — cohort hysteresis. Deferred from Phase 1 because it is stateful (needs stored prior membership) and its marginal reproducibility value is lower than parts 1+2 (the audited base already uses medians, robust to 1–2 member swaps). Sequenced by risk/complexity, **not dropped**.
- **Phase 2 (follow-up ticket): part 4** — companion-variable peer screen. The root-cause fix; heaviest, data-gated (needs per-peer margin/growth). Partial for AAPL (growth mismatch persists at `MIN_PEERS`).

## 6. Phase 1 — detailed spec

### 6.1 Interior quantiles (part 1)

`own_range` (`fair_value_band.py:208`) computes `percentiles(pos, (0.20, 0.50, 0.80))` → `OwnPct(p20, p50, p80)`. Change to `(0.25, 0.50, 0.75)`, rename the `OwnPct` fields to `p25/p50/p75`, and update `synth_multiple` to read `own.p25`/`own.p75`. **Base unchanged** (`base_mult = mean(peer.p50, own.p50)`). Only `low_mult = min(peer.p25, own.p25)` and `high_mult = max(peer.p75, own.p75)` shift (own side slightly inward).

### 6.2 Fixed per-leg cap (part 2)

After `synth_multiple` yields `(low_mult, base_mult, high_mult)` for multiple `m`, clamp **in multiple-space, before per-share conversion**:

```text
high_mult = min(high_mult, base_mult * R_UP[m])
low_mult  = max(low_mult,  base_mult / R_DN[m])
```

Preserves `low ≤ base ≤ high` (base·R with R≥1 straddles base). Applies identically to peer-only, own-only, and two-sided names — the **peer-only** tail is the primary target, but **own-only** legs are intentionally bounded too (an undisciplined own-history p75 is no more trustworthy than an undisciplined peer p75). Record `capped_high`/`capped_low` booleans **and the pre-cap wing multiples** (`precap_low_mult`/`precap_high_mult`) into `basis["multiples"][m]` so the clamp is fully reconstructable from the stored row.

**Guards (Codex ckpt-1):** `base_mult ≤ 0` ⇒ no-op (impossible under the §4.1 positive-denominator gate + median-of-positives, but defended so a degenerate row falls through rather than dividing by zero); an unknown multiple (e.g. a future `ev_ebitda`) ⇒ no-op. Inputs are finite by construction (the `_MAX_SANE_MULTIPLE` pass-1 gate + positive-only percentiles bound every multiple), so no `nan/inf` reaches the clamp; `combine_across`'s fail-closed order check is the final backstop regardless.

**Constants — `R_UP` / `R_DN`, frozen, source-ruled from §3 full-pop p95** (the "healthy" upper bound; clips only the top ~5% pathological/peerless tail, leaves the two-sided majority — median ≈1.6 — untouched). Provisional (v1-calibrated; **re-validate on the fvb_v2 distribution post-backfill**, per DoD clause 11):

```text
R_UP = {"pe": 2.6, "ps": 3.1, "pb": 4.1}   # ≈ full-pop p95 of high/base
R_DN = {"pe": 2.7, "ps": 4.3, "pb": 2.4}   # ≈ full-pop p95 of base/low
```

**Honesty note (vs §2):** this cap is a percentile-derived clamp on the envelope *ratio* — the mechanism Codex/the literature warned against *as a standalone AAPL fix*. Its role here is narrow and legitimate: the **only deterministic bound for peer-only names** (which have no own-history to discipline them). It is calibrated to **not** distort the two-sided majority and it does **not** claim to fix AAPL (AAPL p84, ratio 2.35 < R_UP[ps] → uncapped; AAPL is Phase 2). Stated explicitly so the guard is not mistaken for the root fix.

### 6.3 Deterministic interpolation (part of reproducibility core)

`percentiles()` (`fair_value_band.py:92`) is already pure-Python continuous interpolation matching Postgres `percentile_cont` (deterministic, no RNG, stable tie-break by sort). **No change** — add a regression test asserting fixed inputs → fixed outputs (locks the interpolation convention against future drift; Hyndman & Fan document 9 divergent definitions).

### 6.4 Tests (pure tier — `tests/test_fair_value_band_policy.py`)

- `own_range` returns p25/p50/p75 (boundary: n=6 floor; interior quantiles).
- Cap clamps `high_mult` to `base_mult·R_UP[m]` when it exceeds; no-op when within; symmetric low.
- Cap preserves `low ≤ base ≤ high`.
- Peer-only leg (own absent) is cap-bounded (the tail case).
- `capped_high`/`capped_low` audit flags set correctly.
- `percentiles()` fixed-input regression (interpolation lock).

### 6.5 Method version + backfill

- Bump `METHOD_VERSION = "fvb_v2"`. Consumers import the constant (`thesis.py`, `freshness.py`, `fundamentals_admin.py`) → transparent.
- **Operator backfill after merge:** restart the VS Code jobs task, then force the full-universe recompute (`fair_value_band_recompute` trigger) — do not wait for the 24 h cron. Verify per DoD 8–12: AAPL/GME/MSFT/JPM/HD render; cross-check one figure; confirm `/instruments/{symbol}` band.
- **Cap re-validation — explicit acceptance gate (Codex ckpt-1 #4).** The v1-calibrated `R` constants are *provisional*: p25/p75 narrow own-driven ratios, so the v1 caps are likely *looser* post-change — "clips the top ~5%" is **not** guaranteed. Re-run the per-leg envelope-ratio scan on the post-backfill fvb_v2 population and ACCEPT only if, per multiple `m`:
  1. **Tail bounded:** `max(high_mult/base_mult) ≤ R_UP[m] + ε` — the cap actually binds the peer-only tail.
  2. **Majority untouched:** the median envelope ratio shifts `< 2%` vs the v1 distribution — the cap does not distort the bulk.
  3. **Cap-hit rate sane:** the `capped_high` fraction, reviewed per multiple **and** per comparator-side (peer-only vs two-sided), sits in the expected ~2–8% band; a spike flags a mis-calibrated `R_UP[m]`.
  If any leg fails, retune `R_UP[m]`/`R_DN[m]` to the fvb_v2 p95 and re-backfill before declaring done.

## 7. Limitations

- **AAPL is only partially addressable** (Phase 2, and even then bounded by `MIN_PEERS` growth-matching). Phase 1 does **not** narrow AAPL's bull — by design; the cap targets the peerless tail, not p84 names.
- Cap constants are v1-calibrated; must be re-validated on the fvb_v2 distribution (interior quantiles narrow ratios slightly → cap bites marginally less).
