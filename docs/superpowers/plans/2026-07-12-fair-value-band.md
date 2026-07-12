# Deterministic Fair-Value Valuation-Evidence Band (#2009) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute a deterministic bear/base/bull per-share valuation band from regulated fundamentals (peer-median SIC-cohort multiples + own trailing multiple range), store it two-layer, and feed it to the thesis writer as passive evidence with a divergence measurement.

**Architecture:** A pure-policy service (`app/services/fair_value_band.py`, rows-as-args, no DB) does all synthesis; a thin IO wrapper resolves market-cap basis + oracle membership into plain values, runs a two-pass compute (pass-1 materializes cohort percentiles universe-wide, pass-2 per-name synthesis), and write-throughs into a two-layer `observations`/`current` pair. The band is an orchestrator **DAG layer** (`dependencies=("candles","fundamentals")`), plus a per-instrument compute inside `cascade_refresh` immediately before `generate_thesis`. The thesis writer consumes it as a **passive** context block; a NULL-safe divergence row is recorded per thesis.

**Tech Stack:** Python 3.14, psycopg3, Postgres, FastAPI, pytest (fast tier `-m "not db"` + db tier). No new libraries.

## Global Constants

Copied verbatim from spec `docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md`. Every task's requirements implicitly include this section.

- **Source of truth spec:** `docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md` (v2, signed off 2026-07-12).
- **Method version literal:** `fvb_v1`.
- **v1 multiples:** P/E, P/S, P/B only. **EV/EBITDA deferred to v2.**
- **Calibration constants (pinned; validate against full-pop at Task A9 before hard-coding, adjust only on a clearly-wrong distribution):**
  - `MIN_PEERS = 8` (matches `peer_grade._MIN_SECTOR_PEERS`).
  - `PEER_LIMIT = 8` (nearest-by-log-total-assets size refinement).
  - `MIN_OWN_POINTS = 6` (distinct positive-multiple quarters).
  - `PRICE_STALE_DAYS = 7` (target latest close staleness → `stale_price` absence).
  - `PEER_STALE_DAYS = 7` (cohort member close staleness → excluded from median).
  - `DIVERGENCE_THRESHOLD = 0.30` (|llm_base − band_base| / band_base flag; PR-B).
  - Peer percentiles `p25 / p50 / p75`; own percentiles `p20 / p50 / p80`.
- **Eligibility (§4.1, a universal precondition re-applied at selection, cohort, and conversion):** a multiple is computable iff its denominator is strictly positive on the strict-TTM row — P/E `eps_diluted_ttm > 0`, P/S `revenue_ttm > 0`, P/B `shareholders_equity > 0` — AND latest `price_daily.close` exists, `NULLIF(GREATEST(close,0),0)` positive (prevention-log L113), and passes freshness.
- **Strict TTM sole source:** `financial_periods_ttm` VIEW (`sql/220`, #2008). Never add a 4th copy of the strict-TTM/330-day logic.
- **Market-cap basis sole authority:** `resolve_market_cap_basis` (`app/services/xbrl_derived_stats.py:538`). Dual-class handled in Python; never re-implement in SQL (prevention-log #1921).
- **Peer cohort key:** SEC SIC (`instrument_sec_profile.sic`, TEXT 4-digit), walked SIC-4→3→2.
- **Single as-of date:** whole batch computes at ONE market-calendar as-of DATE (newest closed session, data-anchored, NOT `now()::date`). Every price read (target + cohort + own-history) relative to that date.
- **#1632 discipline:** absence is statused (a row with NULL band + a `reason`), never a neutral default, never a missing row. Divergence NULL when `band_base` NULL — never 0/false.
- **Long only v1:** band is downside/upside context, never a short signal; protective EXIT never gated by valuation.
- **`bear ≤ base ≤ bull`** by construction; a final assert fail-closes (no band, statused) otherwise.
- **PR ordering is hard:** PR-A ("band compute+store") merges and its bootstrap/backfill fully drains BEFORE PR-B ("thesis consumer") merges. Do not begin PR-B tasks until PR-A is drained on dev.

---

# PR-A — Band compute + store

Satisfies DoD clauses 8–12 standalone: band renders, `SELECT` over `fair_value_band_current` for the AAPL/GME/MSFT/JPM/HD panel, one figure cross-checked vs an independent source (gurufocus/marketbeat). Branch: `feature/2009-fair-value-band-compute`.

## File Structure (PR-A)

- Create: `sql/221_fair_value_band.sql` — two band tables + pass-1 cohort-member working table + SIC prefix generated columns/index.
- Create: `app/services/fair_value_band.py` — pure policy + IO wrapper.
- Create: `tests/test_fair_value_band_policy.py` — pure-policy tests (fast tier).
- Create: `tests/test_fair_value_band_io.py` — one db-tier integration test (two-pass SQL + dual-class anti-join).
- Modify: `app/services/sync_orchestrator/registry.py` — add `fair_value_band` DataLayer + INIT_CHECKS entry.
- Modify: `app/services/sync_orchestrator/adapters.py` — add `refresh_fair_value_band` adapter.
- Modify: `app/services/refresh_cascade.py` — per-instrument band compute before `generate_thesis`.
- Modify: the bootstrap stage module — add a bulk band stage (locate in Task A8).
- Modify: an admin/ops read router — add a reason-bucket rollup endpoint (locate in Task A9).
- Modify: the operator `sec_rebuild`-analogue job registry — add a full-universe band recompute trigger (Task A9).

---

### Task A1: Migration — two-layer band tables + pass-1 percentiles table + SIC prefix index

**Files:**
- Create: `sql/221_fair_value_band.sql`
- Reference (do not edit): `sql/198_instrument_risk_metrics.sql` (two-layer shape to mirror), `sql/051_instrument_sec_profile.sql:22` (`sic TEXT`).

**Interfaces:**
- Produces tables: `fair_value_band_observations`, `fair_value_band_current`, `fair_value_cohort_members`; generated columns `instrument_sec_profile.sic2`, `.sic3` + indexes.

- [ ] **Step 1: Write the migration**

```sql
-- 221_fair_value_band.sql
--
-- #2009 deterministic fair-value valuation-evidence band.
-- Spec: docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md
--
-- TWO-LAYER RATIONALE (mirrors sql/198 instrument_risk_metrics):
--   price_daily is MUTABLE (ingest upserts corrected bars). A past band is
--   NOT reconstructable from current price_daily, so the observation row IS
--   the audit record. _observations is APPEND-ONLY; _current is the
--   write-through row the thesis reads. computed_at is in the observations PK
--   so a vendor correction that does NOT advance as_of_date appends rather
--   than silently overwriting.

-- Pass-1: cohort-member WORKING SET, materialized universe-wide at the single
-- batch as_of_date. One row per (as_of_date, instrument_id, multiple) with the
-- name's as-of multiple + its SIC keys + total_assets + dual-class-suppressed
-- flag + close staleness. Pass-2 reads this per target, walks the SIC ladder to
-- MIN_PEERS, size-refines to nearest PEER_LIMIT by |ln(total_assets)|, and
-- percentiles IN PURE PYTHON (reusing percentiles(), same fn as own-history).
--
-- WHY member-level, not pre-percentiled: peer-median size refinement (§4.3) is
-- PER-TARGET (nearest-8 to THAT name's assets) — a per-(sic,multiple) percentile
-- table cannot carry it (Codex ckpt-1 HIGH #1). Pass-1 still does the expensive
-- price-as-of join once for every name; pass-2's per-target percentile over <=8
-- members is trivial and needs no per-sibling re-price.
CREATE TABLE IF NOT EXISTS fair_value_cohort_members (
    as_of_date            date    NOT NULL,
    instrument_id         bigint  NOT NULL,
    multiple              text    NOT NULL,          -- CHECK IN (pe, ps, pb)
    mult_value            numeric(18,6) NOT NULL,     -- the name's as-of multiple, denominator > 0
    sic                   text,
    sic3                  text,
    sic2                  text,
    total_assets          numeric(20,4),             -- for the log-distance size refinement
    close_date            date    NOT NULL,          -- the price_daily bar used (nearest at/before as_of)
    dual_class_suppressed boolean NOT NULL,          -- curated-oracle member -> excluded from ps/pb medians
    PRIMARY KEY (as_of_date, instrument_id, multiple),
    CONSTRAINT fvcm_multiple_chk CHECK (multiple IN ('pe', 'ps', 'pb'))
);
CREATE INDEX IF NOT EXISTS fair_value_cohort_members_sic_idx
    ON fair_value_cohort_members (as_of_date, multiple, sic);
CREATE INDEX IF NOT EXISTS fair_value_cohort_members_sic3_idx
    ON fair_value_cohort_members (as_of_date, multiple, sic3);
CREATE INDEX IF NOT EXISTS fair_value_cohort_members_sic2_idx
    ON fair_value_cohort_members (as_of_date, multiple, sic2);

-- Append-only audit record.
CREATE TABLE IF NOT EXISTS fair_value_band_observations (
    instrument_id  bigint      NOT NULL,     -- NO FK (survive delist/merge/re-id)
    method_version text        NOT NULL,     -- 'fvb_v1'
    computed_at    timestamptz NOT NULL,
    as_of_date     date        NOT NULL,     -- the single batch as-of
    ttm_end        date,
    price_as_of    date,
    bear_value     numeric(18,6),
    base_value     numeric(18,6),
    bull_value     numeric(18,6),
    quality_status text,                     -- high | medium | low (NULL when no band)
    reason         text        NOT NULL,
    target_basis   text        NOT NULL,     -- resolve_market_cap_basis result
    n_selected     smallint    NOT NULL,
    basis_json     jsonb       NOT NULL,
    PRIMARY KEY (instrument_id, method_version, computed_at),
    CONSTRAINT fvb_obs_reason_chk CHECK (reason IN
        ('ok','no_multiple','currency_mismatch','stale_price','multiclass_unavailable','thin_cohort')),
    CONSTRAINT fvb_obs_quality_chk CHECK (quality_status IS NULL OR quality_status IN ('high','medium','low')),
    CONSTRAINT fvb_obs_order_chk CHECK (
        bear_value IS NULL OR base_value IS NULL OR bull_value IS NULL
        OR (bear_value <= base_value AND base_value <= bull_value))
);

-- Write-through current (the thesis read row).
CREATE TABLE IF NOT EXISTS fair_value_band_current (
    instrument_id  bigint      NOT NULL,
    method_version text        NOT NULL,
    computed_at    timestamptz NOT NULL,
    as_of_date     date        NOT NULL,
    ttm_end        date,
    price_as_of    date,
    bear_value     numeric(18,6),
    base_value     numeric(18,6),
    bull_value     numeric(18,6),
    quality_status text,
    reason         text        NOT NULL,
    target_basis   text        NOT NULL,
    n_selected     smallint    NOT NULL,
    basis_json     jsonb       NOT NULL,
    PRIMARY KEY (instrument_id, method_version),
    CONSTRAINT fvb_cur_reason_chk CHECK (reason IN
        ('ok','no_multiple','currency_mismatch','stale_price','multiclass_unavailable','thin_cohort')),
    CONSTRAINT fvb_cur_quality_chk CHECK (quality_status IS NULL OR quality_status IN ('high','medium','low')),
    CONSTRAINT fvb_cur_order_chk CHECK (
        bear_value IS NULL OR base_value IS NULL OR bull_value IS NULL
        OR (bear_value <= base_value AND base_value <= bull_value))
);

-- Writer's real-band read (skip statused-absent rows).
CREATE INDEX IF NOT EXISTS fair_value_band_current_realband_idx
    ON fair_value_band_current (instrument_id) WHERE base_value IS NOT NULL;

-- SIC prefix ladder support (none today; sql/051 is cik-only).
ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS sic3 text GENERATED ALWAYS AS (left(sic, 3)) STORED,
    ADD COLUMN IF NOT EXISTS sic2 text GENERATED ALWAYS AS (left(sic, 2)) STORED;
CREATE INDEX IF NOT EXISTS instrument_sec_profile_sic_idx  ON instrument_sec_profile (sic);
CREATE INDEX IF NOT EXISTS instrument_sec_profile_sic3_idx ON instrument_sec_profile (sic3);
CREATE INDEX IF NOT EXISTS instrument_sec_profile_sic2_idx ON instrument_sec_profile (sic2);
```

- [ ] **Step 2: Apply the migration on dev**

Run: `uv run python -m app.db.migrate` (or the repo's migration entrypoint — grep `run_migrations` to confirm the invocation).
Expected: `221_fair_value_band.sql` applied, no error. Verify: connect via psycopg (no `psql` in this env) and `SELECT to_regclass('fair_value_band_current');` returns non-NULL.

- [ ] **Step 3: Commit**

```bash
git add sql/221_fair_value_band.sql
git commit -m "feat(#2009): two-layer fair-value-band tables + SIC prefix ladder index"
```

---

### Task A2: Pure eligibility + multiple selection (§4.1, §4.2)

**Files:**
- Create: `app/services/fair_value_band.py`
- Test: `tests/test_fair_value_band_policy.py`

**Interfaces:**
- Produces:
  - `METHOD_VERSION = "fvb_v1"`, `MIN_PEERS`, `PEER_LIMIT`, `MIN_OWN_POINTS`, `PRICE_STALE_DAYS`, `PEER_STALE_DAYS`, `DIVERGENCE_THRESHOLD` module constants (all defined here in PR-A; `DIVERGENCE_THRESHOLD` is consumed in PR-B).
  - `@dataclass(frozen=True) class TargetInputs` — fields: `eps_diluted_ttm: float | None`, `revenue_ttm: float | None`, `shareholders_equity: float | None`, `net_income_ttm: float | None`, `shares_outstanding: float | None`, `sic: str | None`, `reported_currency: str | None`, `instrument_currency: str | None`, `target_basis: str` (result of `resolve_market_cap_basis`).
  - `select_multiples(t: TargetInputs) -> list[str]` — returns a subset of `["pe","ps","pb"]`, deterministic first-match per §4.2, intersected with `{"pe"}` when `t.target_basis != "not_multiclass"`. Only includes a multiple whose §4.1 denominator is strictly positive.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_fair_value_band_policy.py
from app.services.fair_value_band import TargetInputs, select_multiples


def _t(**kw) -> TargetInputs:
    base = dict(eps_diluted_ttm=None, revenue_ttm=None, shareholders_equity=None,
                net_income_ttm=None, shares_outstanding=1_000.0, sic="3571",
                reported_currency="USD", instrument_currency="USD", target_basis="not_multiclass")
    base.update(kw)
    return TargetInputs(**base)


def test_financial_selects_pb_and_pe():
    # SIC 6021 (national commercial bank) -> financial gate first.
    t = _t(sic="6021", eps_diluted_ttm=2.0, shareholders_equity=5_000.0, revenue_ttm=9_000.0)
    assert select_multiples(t) == ["pb", "pe"]


def test_profitable_nonfinancial_selects_pe_and_ps():
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=2.0, revenue_ttm=9_000.0)
    assert select_multiples(t) == ["pe", "ps"]


def test_revenue_only_selects_ps():
    t = _t(net_income_ttm=-10.0, revenue_ttm=9_000.0, eps_diluted_ttm=-1.0)
    assert select_multiples(t) == ["ps"]


def test_none_computable_empty():
    t = _t(net_income_ttm=None, revenue_ttm=0.0, eps_diluted_ttm=0.0, shareholders_equity=0.0)
    assert select_multiples(t) == []


def test_dual_class_target_intersects_to_pe_only():
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=2.0, revenue_ttm=9_000.0, target_basis="dual_class_combined")
    assert select_multiples(t) == ["pe"]


def test_dual_class_financial_keeps_pe_drops_pb():
    t = _t(sic="6021", eps_diluted_ttm=2.0, shareholders_equity=5_000.0, target_basis="dual_class_combined")
    assert select_multiples(t) == ["pe"]


def test_eligibility_gate_drops_multiple_with_nonpositive_denominator():
    # profitable but eps not positive -> pe dropped, ps kept
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=0.0, revenue_ttm=9_000.0)
    assert select_multiples(t) == ["ps"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fair_value_band_policy.py -v`
Expected: FAIL — `ModuleNotFoundError` / `ImportError: cannot import name 'select_multiples'`.

- [ ] **Step 3: Write the implementation**

```python
# app/services/fair_value_band.py
"""Deterministic fair-value valuation-evidence band (#2009).

Pure policy: cohort ladder, percentile synthesis, blend+envelope, per-share
conversion, dual-class routing, quality scoring — all over rows-as-args, no DB.
The IO wrapper (bottom of file) resolves MarketCapResolution + oracle
membership into plain values BEFORE calling the pure functions.

Spec: docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md
"""
from __future__ import annotations

from dataclasses import dataclass

METHOD_VERSION = "fvb_v1"
MIN_PEERS = 8
PEER_LIMIT = 8
MIN_OWN_POINTS = 6
PRICE_STALE_DAYS = 7
PEER_STALE_DAYS = 7
DIVERGENCE_THRESHOLD = 0.30  # consumed in PR-B (thesis divergence flag)

_FINANCIAL_SIC_LO, _FINANCIAL_SIC_HI = 60, 67


@dataclass(frozen=True)
class TargetInputs:
    eps_diluted_ttm: float | None
    revenue_ttm: float | None
    shareholders_equity: float | None
    net_income_ttm: float | None
    shares_outstanding: float | None
    sic: str | None
    reported_currency: str | None
    instrument_currency: str | None
    target_basis: str  # resolve_market_cap_basis result; "not_multiclass" when single-class


def _pos(x: float | None) -> bool:
    return x is not None and x > 0


def _is_financial(sic: str | None) -> bool:
    if not sic or len(sic) < 2 or not sic[:2].isdigit():
        return False
    return _FINANCIAL_SIC_LO <= int(sic[:2]) <= _FINANCIAL_SIC_HI


def _computable(t: TargetInputs, m: str) -> bool:
    """§4.1 denominator gate for a single multiple."""
    if m == "pe":
        return _pos(t.eps_diluted_ttm)
    if m == "ps":
        return _pos(t.revenue_ttm) and _pos(t.shares_outstanding)
    if m == "pb":
        return _pos(t.shareholders_equity) and _pos(t.shares_outstanding)
    raise ValueError(f"unknown multiple {m!r}")


def select_multiples(t: TargetInputs) -> list[str]:
    """§4.2 deterministic profile selection, first match wins, §4.1-gated.

    Dual-class target (basis != not_multiclass) intersects the set with {pe}
    because sql/201 suppresses cap-/share-based multiples for dual-class.
    """
    if _is_financial(t.sic):
        selected = ["pb", "pe"]
    elif _pos(t.net_income_ttm):
        selected = ["pe", "ps"]
    elif _pos(t.revenue_ttm):
        selected = ["ps"]
    else:
        selected = []

    if t.target_basis != "not_multiclass":
        selected = [m for m in selected if m == "pe"]

    return [m for m in selected if _computable(t, m)]
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_fair_value_band_policy.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add app/services/fair_value_band.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): pure §4.1/§4.2 eligibility + multiple selection"
```

---

### Task A3: Pure percentiles + currency coherence

**Files:**
- Modify: `app/services/fair_value_band.py`
- Test: `tests/test_fair_value_band_policy.py`

**Interfaces:**
- Produces:
  - `percentiles(values: list[float], ps: tuple[float, ...]) -> list[float]` — linear-interpolation percentile over a non-empty list; matches Postgres `percentile_cont` semantics (so the pure own-history computation and the pass-1 SQL agree).
  - `currency_coherent(reported: str | None, instrument: str | None) -> bool`.

- [ ] **Step 1: Write the failing tests**

```python
from app.services.fair_value_band import percentiles, currency_coherent


def test_percentiles_match_postgres_continuous():
    # percentile_cont semantics: linear interpolation between closest ranks.
    vals = [10.0, 20.0, 30.0, 40.0]
    assert percentiles(vals, (0.25, 0.5, 0.75)) == [17.5, 25.0, 32.5]


def test_percentiles_single_value():
    assert percentiles([42.0], (0.2, 0.5, 0.8)) == [42.0, 42.0, 42.0]


def test_percentiles_zero_variance():
    assert percentiles([5.0, 5.0, 5.0], (0.25, 0.5, 0.75)) == [5.0, 5.0, 5.0]


def test_currency_coherent():
    assert currency_coherent("USD", "USD") is True
    assert currency_coherent("EUR", "USD") is False
    assert currency_coherent(None, "USD") is False
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "percentiles or currency" -v`
Expected: FAIL — `ImportError`.

- [ ] **Step 3: Write the implementation**

```python
def percentiles(values: list[float], ps: tuple[float, ...]) -> list[float]:
    """Continuous percentiles matching Postgres percentile_cont.

    For sorted v[0..n-1] and fraction p: rank = p*(n-1); interpolate linearly
    between v[floor(rank)] and v[ceil(rank)].
    """
    if not values:
        raise ValueError("percentiles requires a non-empty list")
    s = sorted(values)
    n = len(s)
    out: list[float] = []
    for p in ps:
        if n == 1:
            out.append(s[0])
            continue
        rank = p * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        out.append(s[lo] + (s[hi] - s[lo]) * frac)
    return out


def currency_coherent(reported: str | None, instrument: str | None) -> bool:
    """Fail-closed: require reported_currency == instrument currency (§4.1)."""
    return reported is not None and instrument is not None and reported == instrument
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "percentiles or currency" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/fair_value_band.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): pure percentile_cont-parity + currency coherence"
```

---

### Task A4: Pure comparator synthesis — blend + outer envelope + per-share conversion (§4.5)

**Files:**
- Modify: `app/services/fair_value_band.py`
- Test: `tests/test_fair_value_band_policy.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class PeerPct` — `p25: float | None`, `p50: float | None`, `p75: float | None` (None when comparator (a) absent for that multiple).
  - `@dataclass(frozen=True) class OwnPct` — `p20: float | None`, `p50: float | None`, `p80: float | None` (None when comparator (b) absent).
  - `synth_multiple(peer: PeerPct, own: OwnPct) -> tuple[float, float, float] | None` — returns `(low_mult, base_mult, high_mult)` via blend+envelope, degrades to the single surviving comparator, None if neither present.
  - `to_per_share(m: str, low_mult, base_mult, high_mult, *, eps, revenue, shareholders_equity, shares) -> tuple[float, float, float]` — converts a `(low, base, high)` multiple triple to per-share values for multiple `m`.
  - `combine_across(triples: list[tuple[float, float, float]]) -> tuple[float, float, float]` — median-of-bases + outer envelope of lows/highs; asserts `bear <= base <= bull`.

- [ ] **Step 1: Write the failing tests**

```python
from app.services.fair_value_band import (
    PeerPct, OwnPct, synth_multiple, to_per_share, combine_across,
)


def test_synth_blend_and_envelope_both_present():
    peer = PeerPct(p25=10.0, p50=20.0, p75=30.0)
    own = OwnPct(p20=12.0, p50=24.0, p80=28.0)
    low, base, high = synth_multiple(peer, own)
    assert base == 22.0            # mean(20, 24)
    assert low == 10.0             # min(peer_p25=10, own_p20=12)
    assert high == 30.0            # max(peer_p75=30, own_p80=28)


def test_synth_degrades_to_peer_only():
    peer = PeerPct(p25=10.0, p50=20.0, p75=30.0)
    own = OwnPct(p20=None, p50=None, p80=None)
    assert synth_multiple(peer, own) == (10.0, 20.0, 30.0)


def test_synth_degrades_to_own_only():
    peer = PeerPct(p25=None, p50=None, p75=None)
    own = OwnPct(p20=12.0, p50=24.0, p80=28.0)
    assert synth_multiple(peer, own) == (12.0, 24.0, 28.0)


def test_synth_none_when_neither():
    assert synth_multiple(PeerPct(None, None, None), OwnPct(None, None, None)) is None


def test_to_per_share_pe():
    assert to_per_share("pe", 30.0, 34.0, 37.0, eps=8.0, revenue=None,
                        shareholders_equity=None, shares=None) == (240.0, 272.0, 296.0)


def test_to_per_share_ps():
    # revenue 9000 / shares 1000 = 9 rev/share
    assert to_per_share("ps", 1.0, 2.0, 3.0, eps=None, revenue=9000.0,
                        shareholders_equity=None, shares=1000.0) == (9.0, 18.0, 27.0)


def test_to_per_share_pb():
    # equity 5000 / shares 1000 = 5 book/share
    assert to_per_share("pb", 1.0, 2.0, 3.0, eps=None, revenue=None,
                        shareholders_equity=5000.0, shares=1000.0) == (5.0, 10.0, 15.0)


def test_combine_across_median_and_envelope():
    # two multiples' per-share triples
    triples = [(240.0, 272.0, 296.0), (250.0, 260.0, 300.0)]
    bear, base, bull = combine_across(triples)
    assert base == 266.0           # median([272, 260]) = mean = 266
    assert bear == 240.0           # min lows
    assert bull == 300.0           # max highs
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "synth or per_share or combine" -v`
Expected: FAIL — `ImportError`.

- [ ] **Step 3: Write the implementation**

```python
from statistics import median


@dataclass(frozen=True)
class PeerPct:
    p25: float | None
    p50: float | None
    p75: float | None


@dataclass(frozen=True)
class OwnPct:
    p20: float | None
    p50: float | None
    p80: float | None


def synth_multiple(peer: PeerPct, own: OwnPct) -> tuple[float, float, float] | None:
    """§4.5 blend base, outer-envelope low/high; degrade to the surviving one."""
    peer_ok = peer.p50 is not None
    own_ok = own.p50 is not None
    if peer_ok and own_ok:
        base = (peer.p50 + own.p50) / 2
        low = min(peer.p25, own.p20)
        high = max(peer.p75, own.p80)
        return (low, base, high)
    if peer_ok:
        return (peer.p25, peer.p50, peer.p75)
    if own_ok:
        return (own.p20, own.p50, own.p80)
    return None


def to_per_share(
    m: str, low_mult: float, base_mult: float, high_mult: float, *,
    eps: float | None, revenue: float | None,
    shareholders_equity: float | None, shares: float | None,
) -> tuple[float, float, float]:
    """Convert a (low, base, high) multiple triple to per-share values."""
    if m == "pe":
        per = eps
    elif m == "ps":
        per = None if not shares else revenue / shares
    elif m == "pb":
        per = None if not shares else shareholders_equity / shares
    else:
        raise ValueError(f"unknown multiple {m!r}")
    if per is None:
        raise ValueError(f"per-share metric unavailable for {m!r}")
    return (low_mult * per, base_mult * per, high_mult * per)


def combine_across(triples: list[tuple[float, float, float]]) -> tuple[float, float, float]:
    """§4.5 median-of-bases + outer envelope. Asserts bear <= base <= bull."""
    if not triples:
        raise ValueError("combine_across requires at least one triple")
    bear = min(t[0] for t in triples)
    base = median([t[1] for t in triples])
    bull = max(t[2] for t in triples)
    assert bear <= base <= bull, f"band order violated: {bear} {base} {bull}"
    return (bear, base, bull)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "synth or per_share or combine" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/fair_value_band.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): pure blend+envelope synthesis + per-share conversion"
```

---

### Task A5: Pure own-history builder + quality status + dual-class filter

**Files:**
- Modify: `app/services/fair_value_band.py`
- Test: `tests/test_fair_value_band_policy.py`

**Interfaces:**
- Consumes: `percentiles`, `MIN_OWN_POINTS`.
- Produces:
  - `own_range(multiple_values: list[float]) -> OwnPct` — filters to positive values, returns `OwnPct(None,None,None)` if fewer than `MIN_OWN_POINTS`, else p20/p50/p80. **Distinctness contract:** the caller (IO, A7 step 4) passes exactly one multiple per distinct snapshot quarter — the own-history SQL groups by `fundamentals_snapshot.as_of_date` (= `period_end`, one row per quarter), so `len(multiple_values)` already equals the distinct-quarter count. `own_range` does not (and cannot) re-enforce distinctness; the SQL owns it.
  - `filter_dual_class(rows: list[tuple[int, float]], dual_class_ids: set[int]) -> list[float]` — pure twin of the SQL anti-join (BLOCKING regression guard); drops any `(instrument_id, mult)` whose id is in `dual_class_ids`, returns surviving mults.
  - `@dataclass(frozen=True) class QualityInputs` — `n_selected: int`, `n_comparator_sides: int` (1 or 2), `own_points: int`, `cohort_n: int`, `excluded_stale_n: int`, `sic_level: int`, `cross_multiple_spread: float` (relative spread of per-multiple bases; 0.0 when n_selected==1).
  - `band_quality_status(q: QualityInputs) -> str` — returns `"high" | "medium" | "low"`.

- [ ] **Step 1: Write the failing tests**

```python
from app.services.fair_value_band import (
    own_range, filter_dual_class, QualityInputs, band_quality_status, OwnPct, MIN_OWN_POINTS,
)


def test_own_range_below_min_points_absent():
    assert own_range([10.0] * (MIN_OWN_POINTS - 1)) == OwnPct(None, None, None)


def test_own_range_drops_nonpositive():
    # 6 positive, 3 non-positive -> uses the 6 positive
    vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, -1.0, 0.0, -5.0]
    r = own_range(vals)
    assert r.p50 == 35.0


def test_filter_dual_class_anti_join():
    rows = [(1, 10.0), (2, 20.0), (3, 30.0)]
    assert filter_dual_class(rows, {2}) == [10.0, 30.0]


def test_filter_dual_class_all_dropped_empty():
    assert filter_dual_class([(1, 10.0), (2, 20.0)], {1, 2}) == []


def test_quality_high():
    q = QualityInputs(n_selected=2, n_comparator_sides=2, own_points=12, cohort_n=20,
                      excluded_stale_n=0, sic_level=4, cross_multiple_spread=0.05)
    assert band_quality_status(q) == "high"


def test_quality_low_thin_and_stale():
    q = QualityInputs(n_selected=1, n_comparator_sides=1, own_points=0, cohort_n=8,
                      excluded_stale_n=6, sic_level=2, cross_multiple_spread=0.0)
    assert band_quality_status(q) == "low"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "own_range or dual_class or quality" -v`
Expected: FAIL — `ImportError`.

- [ ] **Step 3: Write the implementation**

```python
def own_range(multiple_values: list[float]) -> OwnPct:
    """§4.4 own trailing range. Positive values only; MIN_OWN_POINTS floor."""
    pos = [v for v in multiple_values if v > 0]
    if len(pos) < MIN_OWN_POINTS:
        return OwnPct(None, None, None)
    p20, p50, p80 = percentiles(pos, (0.20, 0.50, 0.80))
    return OwnPct(p20=p20, p50=p50, p80=p80)


def filter_dual_class(rows: list[tuple[int, float]], dual_class_ids: set[int]) -> list[float]:
    """Pure twin of the §4.3 curated-oracle anti-join. Drops dual-class members."""
    return [mult for iid, mult in rows if iid not in dual_class_ids]


@dataclass(frozen=True)
class QualityInputs:
    n_selected: int
    n_comparator_sides: int
    own_points: int
    cohort_n: int
    excluded_stale_n: int
    sic_level: int
    cross_multiple_spread: float


def band_quality_status(q: QualityInputs) -> str:
    """§4.7 deterministic quality tier. Points-based; conservative floors."""
    score = 0
    score += 2 if q.n_comparator_sides == 2 else 0
    score += 2 if q.own_points >= 2 * MIN_OWN_POINTS else (1 if q.own_points >= MIN_OWN_POINTS else 0)
    stale_frac = (q.excluded_stale_n / q.cohort_n) if q.cohort_n else 1.0
    score += 2 if stale_frac == 0 else (1 if stale_frac < 0.25 else 0)
    score += {4: 2, 3: 1}.get(q.sic_level, 0)
    score += 1 if q.n_selected >= 2 else 0
    score -= 1 if q.cross_multiple_spread > 0.5 else 0
    if score >= 7:
        return "high"
    if score >= 4:
        return "medium"
    return "low"
```

> **Plan note (Codex ckpt-1 MEDIUM #5):** the `band_quality_status` point weights + 7/4 cutoffs are a **provisional rubric**, not distribution-derived constants. This is acceptable to ship because in v1 quality is **surfaced + measured only, never gating** (spec §7 passive evidence). A9 step 5 validates the resulting `high/medium/low` distribution against the full population and adjusts the cutoffs if degenerate (e.g. everything lands `low`). Keep the rubric here as the single source; tune the numbers, not the shape.

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "own_range or dual_class or quality" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/fair_value_band.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): pure own-history range, dual-class filter twin, quality tiers"
```

---

### Task A6: Pure top-level assembly + golden AAPL fixture

**Files:**
- Modify: `app/services/fair_value_band.py`
- Test: `tests/test_fair_value_band_policy.py`

**Interfaces:**
- Consumes: all of A2–A5.
- Produces:
  - `@dataclass(frozen=True) class BandResult` — `bear: float | None`, `base: float | None`, `bull: float | None`, `quality_status: str | None`, `reason: str`, `target_basis: str`, `n_selected: int`, `basis: dict`. (`target_basis` mirrors the NOT-NULL storage column; `write_band` reads it off the result.)
  - `compute_band(t: TargetInputs, *, peer_by_multiple: dict[str, PeerPct], own_by_multiple: dict[str, OwnPct], own_points_by_multiple: dict[str, int], cohort_meta: dict[str, dict], sic_level: int) -> BandResult` — the pure orchestration: select multiples, per-multiple synth+convert, combine, quality (with the **true** `own_points`/`excluded_stale_n`, no proxy), package `basis`. Returns a statused-absent `BandResult` (reason ∈ enum) rather than raising when no band is derivable. `own_points_by_multiple[m]` = the real distinct-quarter count the IO computed for comparator (b); `cohort_meta[m]` carries `{"cohort_n","excluded_stale_n"}` for quality. Every returned `BandResult` sets `target_basis = t.target_basis`.

- [ ] **Step 1: Write the failing tests**

```python
from app.services.fair_value_band import (
    TargetInputs, PeerPct, OwnPct, compute_band, BandResult,
)


def _aapl() -> TargetInputs:
    return TargetInputs(
        eps_diluted_ttm=8.26, revenue_ttm=None, shareholders_equity=None,
        net_income_ttm=100_000.0, shares_outstanding=15_000.0, sic="3571",
        reported_currency="USD", instrument_currency="USD", target_basis="not_multiclass")


def test_golden_aapl_pe_band():
    # §3 worked fixture: own trailing P/E p20/p50/p80 = 31.2/34.5/36.9, peer absent.
    # Band = 31.2*8.26 / 34.5*8.26 / 36.9*8.26 ~= 257.7 / 285.0 / 304.8.
    res = compute_band(
        _aapl(),
        peer_by_multiple={"pe": PeerPct(None, None, None)},
        own_by_multiple={"pe": OwnPct(p20=31.2, p50=34.5, p80=36.9)},
        own_points_by_multiple={"pe": 7},
        cohort_meta={"pe": {"cohort_n": 0, "excluded_stale_n": 0}},
        sic_level=4,
    )
    assert res.reason == "ok"
    assert res.target_basis == "not_multiclass"
    assert round(res.base, 1) == 285.0
    assert round(res.bear, 1) == 257.7
    assert round(res.bull, 1) == 304.8


def test_compute_band_no_multiple_statused():
    t = TargetInputs(eps_diluted_ttm=0.0, revenue_ttm=0.0, shareholders_equity=0.0,
                     net_income_ttm=None, shares_outstanding=1000.0, sic="3571",
                     reported_currency="USD", instrument_currency="USD", target_basis="not_multiclass")
    res = compute_band(t, peer_by_multiple={}, own_by_multiple={},
                       own_points_by_multiple={}, cohort_meta={}, sic_level=4)
    assert res.reason == "no_multiple"
    assert res.base is None
    assert res.target_basis == "not_multiclass"


def test_compute_band_thin_cohort_when_all_comparators_absent():
    t = _aapl()
    res = compute_band(t, peer_by_multiple={"pe": PeerPct(None, None, None)},
                       own_by_multiple={"pe": OwnPct(None, None, None)},
                       own_points_by_multiple={"pe": 0},
                       cohort_meta={"pe": {"cohort_n": 3, "excluded_stale_n": 0}}, sic_level=4)
    assert res.reason == "thin_cohort"
    assert res.base is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "aapl or compute_band" -v`
Expected: FAIL — `ImportError`.

- [ ] **Step 3: Write the implementation**

```python
@dataclass(frozen=True)
class BandResult:
    bear: float | None
    base: float | None
    bull: float | None
    quality_status: str | None
    reason: str
    target_basis: str
    n_selected: int
    basis: dict


def _absent(t: TargetInputs, reason: str, n_selected: int = 0, basis: dict | None = None) -> BandResult:
    return BandResult(None, None, None, None, reason, t.target_basis, n_selected, basis or {})


def compute_band(
    t: TargetInputs, *,
    peer_by_multiple: dict[str, PeerPct],
    own_by_multiple: dict[str, OwnPct],
    own_points_by_multiple: dict[str, int],
    cohort_meta: dict[str, dict],
    sic_level: int,
) -> BandResult:
    """Pure orchestration. Returns statused-absent BandResult, never raises for
    the normal no-band paths. Currency + basis gates are applied by the caller
    (IO wrapper) — this fn assumes t already passed currency_coherent.

    Quality uses the TRUE own_points (per-multiple distinct-quarter counts the IO
    computed) and the TRUE excluded_stale_n from cohort_meta — no proxy (Codex
    ckpt-1 HIGH #4). band_quality_status is the single source of the tiering rule."""
    selected = select_multiples(t)
    if not selected:
        return _absent(t, "no_multiple")

    per_share_triples: list[tuple[float, float, float]] = []
    basis: dict = {"selected": selected, "multiples": {}}
    max_sides = 0
    max_excluded_stale_n = 0
    max_cohort_n = 0
    contributing_own_points = 0
    for m in selected:
        peer = peer_by_multiple.get(m, PeerPct(None, None, None))
        own = own_by_multiple.get(m, OwnPct(None, None, None))
        synth = synth_multiple(peer, own)
        if synth is None:
            continue
        low_mult, base_mult, high_mult = synth
        triple = to_per_share(
            m, low_mult, base_mult, high_mult,
            eps=t.eps_diluted_ttm, revenue=t.revenue_ttm,
            shareholders_equity=t.shareholders_equity, shares=t.shares_outstanding)
        per_share_triples.append(triple)
        sides = (1 if peer.p50 is not None else 0) + (1 if own.p50 is not None else 0)
        max_sides = max(max_sides, sides)
        meta = cohort_meta.get(m, {})
        cn, esn = meta.get("cohort_n", 0), meta.get("excluded_stale_n", 0)
        max_cohort_n = max(max_cohort_n, cn)
        max_excluded_stale_n = max(max_excluded_stale_n, esn)
        if own.p50 is not None:
            contributing_own_points = max(contributing_own_points, own_points_by_multiple.get(m, 0))
        basis["multiples"][m] = {
            "peer": {"p25": peer.p25, "p50": peer.p50, "p75": peer.p75},
            "own": {"p20": own.p20, "p50": own.p50, "p80": own.p80},
            "base_value": triple[1], "cohort_n": cn, "excluded_stale_n": esn,
            "own_points": own_points_by_multiple.get(m, 0),
        }

    if not per_share_triples:
        return _absent(t, "thin_cohort", n_selected=len(selected), basis=basis)

    bear, base, bull = combine_across(per_share_triples)
    bases = [tr[1] for tr in per_share_triples]
    spread = ((max(bases) - min(bases)) / base) if base and len(bases) > 1 else 0.0
    quality = band_quality_status(QualityInputs(
        n_selected=len(selected), n_comparator_sides=max_sides,
        own_points=contributing_own_points,
        cohort_n=max_cohort_n, excluded_stale_n=max_excluded_stale_n,
        sic_level=sic_level, cross_multiple_spread=spread))
    return BandResult(bear, base, bull, quality, "ok", t.target_basis, len(selected), basis)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_fair_value_band_policy.py -v`
Expected: PASS (all policy tests, ~30).

- [ ] **Step 5: Commit**

```bash
git add app/services/fair_value_band.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): pure compute_band assembly + golden AAPL band"
```

---

### Task A7: IO wrapper — two-pass SQL, basis resolution, write-through

**Files:**
- Modify: `app/services/fair_value_band.py` (add IO section, clearly separated from the pure section)
- Reference: `app/services/xbrl_derived_stats.py:538` (`resolve_market_cap_basis`), `app/services/peer_comparison.py:178` (`_rank_peers`), `app/services/scoring.py:353` (`_apply_market_cap_basis` pattern), `sql/201_instrument_valuation_dual_class_suppress.sql:50-56` (curated-oracle anti-join), `sql/220_ttm_strict_flow_sums.sql` (`financial_periods_ttm`).

**Interfaces:**
- Consumes: all pure functions; `resolve_market_cap_basis`.
- Produces:
  - `resolve_batch_as_of_date(conn) -> datetime.date` — newest closed market session from `price_daily` (data-anchored, NOT `now()`).
  - `materialize_cohort_members(conn, as_of_date) -> None` — **pass-1**: populate `fair_value_cohort_members` with one row per `(instrument_id, multiple)` over the eligible universe at `as_of_date` — the name's as-of multiple (denominator > 0), `sic/sic3/sic2`, `total_assets`, `close_date`, and `dual_class_suppressed` (curated-oracle membership). Deletes prior rows for that `as_of_date` first (idempotent re-run). Does the expensive price-as-of join ONCE for every name.
  - `peer_pct_for(conn, target_id, target_sic, target_total_assets, multiple, as_of_date) -> tuple[PeerPct, dict]` — pass-2 comparator (a): read `fair_value_cohort_members` for `multiple` at `as_of_date`, walk SIC-4→3→2 to the first prefix with `>= MIN_PEERS` members (dual-class members excluded for `ps`/`pb` via `dual_class_suppressed`, stale members excluded by `close_date`), size-refine to nearest `PEER_LIMIT` by `abs(ln(total_assets) - ln(target_total_assets))` (reuse the `peer_comparison._rank_peers` distance), then `percentiles(mults, (0.25,0.5,0.75))` in PURE Python. Returns `(PeerPct, {"cohort_n","excluded_stale_n","sic_level"})`.
  - `compute_band_for_instrument(conn, instrument_id, as_of_date) -> BandResult` — **pass-2** orchestration: gather target inputs, resolve basis, build own-history series → `own_by_multiple` + `own_points_by_multiple`, call `peer_pct_for` per selected multiple, currency gate, call `compute_band`, return result (does NOT write).
  - `write_band(conn, instrument_id, band: BandResult, as_of_date, ttm_end, price_as_of) -> None` — append observation + upsert current, one txn; writes `band.target_basis`, `band.quality_status`, `band.reason`, `band.n_selected`, `band.basis`.
  - `refresh_fair_value_band_batch(conn, instrument_ids: list[int] | None) -> dict` — orchestration: resolve as-of, pass-1 `materialize_cohort_members` (only when `instrument_ids is None`, i.e. full/bootstrap run — a single-name cascade run reads the existing member set), per-instrument pass-2 under a SAVEPOINT (catch `UndefinedTable, UndefinedColumn`), write-through, return `{written, statused, failed}` counts.

- [ ] **Step 1: Write the IO wrapper**

Implement in `app/services/fair_value_band.py` below a `# --- IO wrapper (DB) ---` divider. Key correctness rules to encode (each is a spec §-cited invariant, not optional):

  1. **Single as-of date** (`resolve_batch_as_of_date`): `SELECT max(price_date) FROM price_daily` where the bar is a real close. Every price read below is relative to this one date.
  2. **Pass-1 member set** (`materialize_cohort_members`): a set-based SQL that, for each multiple, computes each eligible name's multiple from its close **as of the batch date** (`price_date <= as_of_date`, nearest at-or-before, NOT each member's own latest close), joins `instrument_sec_profile` for `sic`/`sic3`/`sic2` and its `total_assets` (from the strict-TTM row), and flags `dual_class_suppressed`. **Dual-class oracle membership — copy `sql/201`'s CTE EXACTLY (Codex ckpt-1 HIGH #2):** the `provider='sec' AND identifier_type='cik' AND is_primary=TRUE` predicate is on **`external_identifiers`**, JOINed to `instrument_class_shares_outstanding` on `c.source_cik = lpad(ei.identifier_value, 10, '0')` — NOT on `instrument_class_shares_outstanding` directly:
     ```sql
     -- dual_class oracle, verbatim from sql/201:44-56
     SELECT DISTINCT ei.instrument_id
     FROM external_identifiers ei
     JOIN instrument_class_shares_outstanding c
       ON c.source_cik = lpad(ei.identifier_value, 10, '0')
     WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary = TRUE
     ```
     `dual_class_suppressed = TRUE` for a member in this set — pass-2 drops it from `ps`/`pb` medians but keeps its `pe` (suppression is by oracle membership, not denominator positivity, #1662). Compute rows for ALL of P/E, P/S, P/B where the denominator is positive; pass-2 picks per §4.2. Store `close_date` so pass-2 can apply `PEER_STALE_DAYS`.
  3. **Pass-2 cohort synthesis** (`peer_pct_for`): read the member set for the multiple at `as_of_date`; SIC-4→3→2 ladder to `>= MIN_PEERS` (excluding `dual_class_suppressed` for ps/pb and members staler than `PEER_STALE_DAYS`); size-refine to nearest `PEER_LIMIT` by `abs(ln(total_assets) - ln(target_total_assets))`; `percentiles(mults, (0.25,0.5,0.75))` in pure Python (SAME `percentiles()` as own-history → guaranteed agreement). Return `sic_level` reached + `cohort_n` + `excluded_stale_n` for quality.
  4. **Basis resolution BEFORE pure** (`compute_band_for_instrument`): call `resolve_market_cap_basis` (mirror `_apply_market_cap_basis` at `scoring.py:353`), pass the `target_basis` string into `TargetInputs`. `compute_band` copies it onto `BandResult.target_basis`, which `write_band` persists (schema NOT NULL — Codex ckpt-1 HIGH #3).
  5. **Own-history series** (§4.4): `fundamentals_snapshot` rows, ONE per distinct quarter (`as_of_date = period_end`), × `price_daily.close` **nearest at-or-before each snapshot `as_of_date`** (a post-quarter price is lookahead bias). Build `own_by_multiple[m] = own_range(mults)` AND `own_points_by_multiple[m] = len(positive mults)` — the true distinct-quarter count feeding quality (§4.7). **VERIFY at code time** whether `fundamentals_snapshot.eps` is TTM-diluted and whether `book_value` is per-share or total equity (post-#2008 write-through, `bb03f62e`) — the own P/E and P/B conversions depend on it. If `book_value` is per-share, own P/B = `close / book_value`; if total equity, own P/B = `close / (book_value / shares_outstanding)`. Encode the verified interpretation with a comment citing the #2008 column semantics.
  6. **Currency gate**: return statused `currency_mismatch` when `not currency_coherent(reported, instrument_currency)` — before calling `compute_band`.
  7. **Stale target price**: if target latest close staler than `PRICE_STALE_DAYS` vs as-of → statused `stale_price` (do not write a band computed from a stale price).
  8. **Per-instrument SAVEPOINT** in `refresh_fair_value_band_batch`: `with conn.transaction():` per id, catch `psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn` → status the row, continue the batch (one bad row never aborts the run).
  9. **Write-through** (`write_band`): INSERT into `_observations` (append) with `target_basis`, then `INSERT ... ON CONFLICT (instrument_id, method_version) DO UPDATE` into `_current`. Absence writes a row with NULL band + `reason` + the resolved `target_basis`.

Encode the pass-1 materialize as ONE parameterized SQL statement (no f-string interpolation of identifiers/values — global CLAUDE.md non-negotiable). Pass-2 reads the member set with a parameterized SELECT per multiple and does ladder+size-refine+percentile in Python (reusing `peer_comparison._rank_peers` for the log-distance ranking).

- [ ] **Step 2: Smoke the wrapper against dev**

Write a scratchpad script `scratchpad/fvb_smoke.py` (do not commit) that opens a psycopg connection from `DATABASE_URL` (`.env`), calls `resolve_batch_as_of_date`, `materialize_cohort_members`, then `compute_band_for_instrument` for AAPL's `instrument_id`, and prints the `BandResult`.

Run: `uv run python scratchpad/fvb_smoke.py`
Expected: a `BandResult`. **Dev market-data may be UNREACHABLE in the loop env → AAPL may return `stale_price`** (memory gotcha). That is acceptable here; the correctness rides the pure tier + the operator-window dev-verify (A9). Confirm the wrapper runs without exception and the `reason` is one of the enum values.

- [ ] **Step 3: Run the fast tier + typecheck**

Run: `uv run ruff check app/services/fair_value_band.py && uv run pyright app/services/fair_value_band.py && uv run pytest tests/test_fair_value_band_policy.py -q`
Expected: clean; policy tests PASS.

- [ ] **Step 4: Commit**

```bash
git add app/services/fair_value_band.py
git commit -m "feat(#2009): IO wrapper — two-pass cohort percentiles + write-through"
```

---

### Task A8: DAG layer + cascade hook + bootstrap bulk stage

**Files:**
- Modify: `app/services/sync_orchestrator/registry.py` (add `fair_value_band` DataLayer + INIT_CHECKS entries for `fundamentals`)
- Modify: `app/services/sync_orchestrator/adapters.py` (add `refresh_fair_value_band`)
- Modify: `app/services/refresh_cascade.py` (per-instrument band compute before `generate_thesis`)
- Modify: the bootstrap stage module (locate: `grep -rln "bootstrap" app/services | grep -i stage`; the bulk full-load orchestrator)

**Interfaces:**
- Consumes: `refresh_fair_value_band_batch` (A7).
- Produces: `LAYERS["fair_value_band"]`, `refresh_fair_value_band` adapter, a bootstrap stage entry, a cascade per-instrument call.

- [ ] **Step 1: Register the DAG layer**

In `registry.py`, add to `LAYERS` after `scoring` (import `refresh_fair_value_band` from adapters, plus a `fair_value_band_is_fresh` predicate — model it on `scoring_is_fresh`, an age check over `max(computed_at)` from `fair_value_band_current`). Also add an `INIT_CHECKS["fundamentals"]` entry (`"SELECT EXISTS (SELECT 1 FROM fundamentals_snapshot)"`) since the new layer will declare `requires_layer_initialized=("candles","fundamentals")` and the pre-flight gate raises on a named dep with no INIT_CHECKS entry (#591 precedent, `registry.py:98-101`).

```python
    "fair_value_band": DataLayer(
        name="fair_value_band",
        display_name="Fair-Value Bands",
        tier=3,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=fair_value_band_is_fresh,
        refresh=refresh_fair_value_band,
        dependencies=("candles", "fundamentals"),
        requires_layer_initialized=("candles", "fundamentals"),
        plain_language_sla="Refreshed every morning after fundamentals + candles.",
    ),
```

- [ ] **Step 2: Add the adapter**

In `adapters.py`, mirror `refresh_risk_metrics` (`adapters.py:297`). The legacy_fn wraps `refresh_fair_value_band_batch(conn, instrument_ids=None)` (full-universe pass-1 + pass-2). Follow the `_wrap_single` pattern.

- [ ] **Step 3: Cascade per-instrument compute before generate_thesis**

In `refresh_cascade.py`, inside BOTH the retry loop (`refresh_cascade.py:455`) and the stale loop (`:501`), immediately BEFORE `generate_thesis(iid, conn, clients, trigger="cascade")`, call `refresh_fair_value_band_batch(conn, instrument_ids=[iid])` wrapped in its own try/except (log + continue on failure — a band failure must NEVER block a thesis; the thesis simply reads a stale/absent band). Pass-2 reads stored pass-1 percentiles, so a fresh daily pass-1 must exist; document that a same-day cascade before the first daily layer run reads yesterday's percentiles (acceptable — cohort medians move slowly). Do NOT run pass-1 per-instrument.

> **Plan note:** In PR-A the thesis does not yet read the band, so this hook only warms `fair_value_band_current`. It lands in PR-A so that when PR-B merges the band is already fresh in the row for the very first regenerated thesis.

- [ ] **Step 4: Bootstrap bulk stage**

Add a bootstrap stage that calls `refresh_fair_value_band_batch(conn, instrument_ids=None)` after the fundamentals + candles bulk stages (new-surface rule: bulk first-load, not per-filing drain only — memory `feedback-backfills-belong-in-bootstrap`). Match the existing stage signature.

- [ ] **Step 5: Boot smoke**

Run: `uv run pytest tests/smoke -q`
Expected: PASS — app boots, registry validates (no missing INIT_CHECKS, DAG acyclic).

- [ ] **Step 6: Run orchestrator/registry unit tests**

Run: `uv run pytest tests/test_sync_orchestrator_credential_gate.py -q` (and any `test_*registry*`)
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add app/services/sync_orchestrator/registry.py app/services/sync_orchestrator/adapters.py app/services/refresh_cascade.py <bootstrap-stage-file>
git commit -m "feat(#2009): fair_value_band DAG layer + cascade warm-hook + bootstrap stage"
```

---

### Task A9: Operator recompute trigger + reason-bucket rollup endpoint + integration test

**Files:**
- Modify: the `sec_rebuild`-analogue job/trigger registry (locate: `grep -rn "sec_rebuild" app/ | grep -i job`)
- Modify: an admin/ops read router (locate: `grep -rln "ProcessRow\|/jobs\|admin" app/api` — pick the ops rollup router)
- Create: `tests/test_fair_value_band_io.py` (one db-tier integration test)

**Interfaces:**
- Produces: a `POST /jobs/fair_value_band_recompute/run` (or the repo's analogue) full-universe trigger; a `GET` reason-bucket rollup endpoint; a db-tier test.

- [ ] **Step 1: Operator full-universe recompute trigger**

Add a `fair_value_band_recompute` job analogous to `sec_rebuild` that calls `refresh_fair_value_band_batch(conn, None)`. A `method_version` bump (`fvb_v1→v2`) needs a full recompute; this is the operator handle. Document the invocation in the PR description.

- [ ] **Step 2: Reason-bucket rollup endpoint**

Add a read endpoint returning `SELECT reason, quality_status, count(*) FROM fair_value_band_current GROUP BY 1,2 ORDER BY 1,2` so the operator distinguishes dev-stale from a real bug without reading thousands of rows (DAG layers don't auto-surface as admin ProcessRows). Follow the existing ops-read router shape.

- [ ] **Step 3: Write the db-tier integration test**

```python
# tests/test_fair_value_band_io.py
import pytest

pytestmark = pytest.mark.db  # auto-applied anyway via conftest; explicit for clarity


def test_two_pass_cohort_excludes_dual_class_member(db_conn):
    """Seed a SIC-4 cohort with one curated dual-class member; prove the
    oracle anti-join keeps its P/S and P/B out of the pass-2 medians.
    BLOCKING regression guard (spec §9)."""
    # 1. Seed instruments (>= MIN_PEERS single-class + 1 dual-class), their
    #    instrument_sec_profile.sic (same SIC-4), strict-TTM-backing rows,
    #    price_daily closes at the batch date, and — for the dual-class member —
    #    an external_identifiers row (provider='sec', identifier_type='cik',
    #    is_primary=TRUE) whose identifier_value maps (lpad 10) to a
    #    source_cik present in instrument_class_shares_outstanding. (The oracle
    #    predicate lives on external_identifiers, per sql/201:44-56 — NOT on
    #    instrument_class_shares_outstanding directly.)
    # 2. Run materialize_cohort_members(conn, as_of) -> then peer_pct_for(...)
    #    for the target for 'ps' and 'pb'.
    # 3. Assert the dual-class member row has dual_class_suppressed=TRUE and its
    #    P/S and P/B are NOT reflected in the returned PeerPct.p50 (compute the
    #    expected median from the single-class members only and compare);
    #    assert its P/E IS included (dual_class_suppressed does not drop pe).
    ...
```

Flesh out the seed using the repo's existing db fixtures (grep `tests/` for `instrument_class_shares_outstanding` and `price_daily` seed helpers to reuse — do NOT hand-roll new insert helpers if a fixture exists).

- [ ] **Step 4: Run the db-tier test**

Run: `docker compose --profile test up -d postgres-test && uv run pytest tests/test_fair_value_band_io.py -m db -v`
Expected: PASS.

- [ ] **Step 5: Full-population verification queries (safety, not smoke)**

Against dev `fair_value_band_current` (via psycopg scratchpad), run and record in the PR description:
- dual-class targets → P/E-only (cross-check vs curated roster).
- `currency_mismatch` count (expect 0 today).
- P/B looser-population count vs flow multiples.
- peer stale-exclusion distribution (`excluded_stale_n / cohort_n`).
- `quality_status` distribution — **validate the provisional rubric here** (A5 note): if `high/medium/low` is degenerate (e.g. ~all `low`), adjust the point weights/cutoffs in `band_quality_status` and re-run the pure tests. Record the before/after distribution in the PR.
- band-eligible-but-no-band bucketed by `reason` (must be explainable).

**Pin the §11 constants here:** if any distribution shows a clearly-wrong default (e.g. `PEER_STALE_DAYS=7` excludes ~everything because dev market-data is weeks stale — a dev artifact, note it and keep prod-correct 7), record the decision. Do not tune to dev-stale data.

- [ ] **Step 6: Dev-verify panel (operator-window-gated)**

Trigger the recompute, then `SELECT` `fair_value_band_current` for AAPL/GME/MSFT/JPM/HD; confirm `basis_json` + band render. Cross-check ONE figure (e.g. AAPL base) vs gurufocus/marketbeat. If dev market-data is unreachable, note the `stale_price` absences and defer the live-figure check to the operator window — record this explicitly (DoD clause 11 nuance).

- [ ] **Step 7: Commit**

```bash
git add app/... tests/test_fair_value_band_io.py
git commit -m "feat(#2009): operator recompute trigger + reason rollup + dual-class anti-join integration test"
```

---

### Task A10: PR-A pre-flight, checks, authoring

- [ ] **Step 1: Pre-push gate**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest -m "not db"
uv run pytest tests/smoke
```
Expected: all green.

- [ ] **Step 2: Fresh-agent review**

This touches schema migration + a new metric surface → MANDATORY `.claude/skills/engineering/pre-pr-fresh-agent-review.md` (financial-plumbing + data-engineer + data-scientist + adversarial lenses). Run `codex exec review` on the branch (Codex checkpoint 2, before first push). Fix anything real.

- [ ] **Step 3: Author the PR**

PR description records (DoD clauses 8–12): instruments exercised (AAPL/GME/MSFT/JPM/HD), the cross-source figure + source, the recompute job invocation + outcome, the rollup endpoint output, the commit SHA per clause. State the security model (read-only endpoints; no user-controlled SQL; parameterized throughout). Document the two open follow-ups: `peer_comparison`(eToro-sector)→SIC re-key ticket, and the v2 EV/EBITDA scope.

- [ ] **Step 4: Push, poll, resolve, merge** per the branch/PR workflow. Merge only on APPROVE of the latest commit + CI green.

- [ ] **Step 5: Operator drain**

After merge: operator restarts the VS Code jobs task (#2008 rule — restart BEFORE backfill), then triggers `fair_value_band_recompute` (or lets the bootstrap stage run). Wait for the reason-bucket rollup to stabilize. **PR-B does not start until this drain completes.**

---

# PR-B — Thesis consumer (hard-after PR-A drained)

Branch: `feature/2009-fair-value-band-thesis-consumer`. **Precondition: PR-A merged AND its band backfill fully drained on dev** (else the hourly `thesis_refresh` regenerates theses at the new prompt version against an absent band and won't re-anchor until their next natural refresh).

## File Structure (PR-B)

- Create: `sql/222_thesis_valuation_audit.sql` — insert-once divergence audit table.
- Modify: `app/services/thesis.py` — passive `fair_value_band` context block, `_WRITER_SYSTEM` passive rule, `_PROMPT_VERSION` bump, divergence audit-row insert.
- Modify: `app/services/fair_value_band.py` — add pure `compute_divergence` + `_shape_fair_value_band`.
- Modify: `tests/test_fair_value_band_policy.py` — divergence + context-shape pure tests.

---

### Task B1: Migration — thesis_valuation_audit

**Files:**
- Create: `sql/222_thesis_valuation_audit.sql`

- [ ] **Step 1: Write the migration**

```sql
-- 222_thesis_valuation_audit.sql
-- #2009 PR-B / #2007 divergence measurement. Insert-once per thesis; the
-- append-only home for band-vs-LLM divergence signals (keeps `theses` clean).
-- band_base NULL (the ~8,700 no-band path) => divergence_pct/flag NULL, never
-- 0/false (#1632).
CREATE TABLE IF NOT EXISTS thesis_valuation_audit (
    thesis_id            bigint      NOT NULL REFERENCES theses(thesis_id),
    band_method_version  text,
    band_base            numeric(18,6),
    band_quality_status  text,
    price_as_of          date,
    llm_base             numeric(18,6),
    divergence_pct       numeric(10,6),   -- NULL when band_base NULL
    divergence_flag      boolean,          -- NULL when band_base NULL
    created_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (thesis_id)
);
```

Verify `theses` PK column name before writing the FK (`grep -n "PRIMARY KEY\|thesis_id" sql/*thes*`; adjust `thesis_id` if the real column differs).

- [ ] **Step 2: Apply + commit**

```bash
uv run python -m app.db.migrate
git add sql/222_thesis_valuation_audit.sql
git commit -m "feat(#2009): thesis_valuation_audit divergence table"
```

---

### Task B2: Pure divergence + context-shape functions

**Files:**
- Modify: `app/services/fair_value_band.py`
- Test: `tests/test_fair_value_band_policy.py`

**Interfaces:**
- Produces:
  - `compute_divergence(llm_base: float | None, band_base: float | None, threshold: float) -> tuple[float | None, bool | None]` — returns `(divergence_pct, divergence_flag)`; `(None, None)` when `band_base` is None or non-positive, or `llm_base` is None/NaN (no ZeroDivisionError, #1632).
  - `_shape_fair_value_band(row: tuple | None) -> dict` — the passive context block: `{available, reason, quality_status, bear, base, bull, as_of_date, ttm_end, basis}` or `{available: False, reason}` when absent (context reason enum distinct from storage enum).

- [ ] **Step 1: Write the failing tests**

```python
import math
from app.services.fair_value_band import compute_divergence, _shape_fair_value_band


def test_divergence_normal():
    pct, flag = compute_divergence(120.0, 100.0, 0.30)
    assert pct == pytest.approx(0.20)
    assert flag is False


def test_divergence_flagged():
    pct, flag = compute_divergence(150.0, 100.0, 0.30)
    assert flag is True


def test_divergence_band_base_none_is_null_not_zero():
    assert compute_divergence(120.0, None, 0.30) == (None, None)


def test_divergence_band_base_zero_is_null():
    assert compute_divergence(120.0, 0.0, 0.30) == (None, None)


def test_divergence_llm_nan_is_null():
    assert compute_divergence(float("nan"), 100.0, 0.30) == (None, None)


def test_shape_absent_row():
    out = _shape_fair_value_band(None)
    assert out == {"available": False, "reason": "no_band"}
```

Add `import pytest` at the top of the test module if not present.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_fair_value_band_policy.py -k "divergence or shape" -v`
Expected: FAIL — `ImportError`.

- [ ] **Step 3: Write the implementation**

```python
import math


def compute_divergence(
    llm_base: float | None, band_base: float | None, threshold: float,
) -> tuple[float | None, bool | None]:
    """NULL-safe (#1632). Absent/zero band or NaN llm => (None, None)."""
    if band_base is None or band_base <= 0:
        return (None, None)
    if llm_base is None or math.isnan(llm_base):
        return (None, None)
    pct = abs(llm_base - band_base) / band_base
    return (pct, pct > threshold)


def _shape_fair_value_band(row: tuple[object, ...] | None) -> dict[str, object]:
    """Passive thesis context block. Absent => {available:false, reason}."""
    if row is None:
        return {"available": False, "reason": "no_band"}
    # row cols: (bear, base, bull, quality_status, reason, as_of_date, ttm_end, basis_json)
    bear, base, bull, quality, reason, as_of_date, ttm_end, basis = row
    if base is None:
        return {"available": False, "reason": reason or "no_band"}
    return {
        "available": True, "reason": reason, "quality_status": quality,
        "bear": float(bear), "base": float(base), "bull": float(bull),
        "as_of_date": as_of_date.isoformat() if as_of_date else None,
        "ttm_end": ttm_end.isoformat() if ttm_end else None, "basis": basis,
    }
```

- [ ] **Step 4: Run to verify pass + commit**

```bash
uv run pytest tests/test_fair_value_band_policy.py -k "divergence or shape" -v
git add app/services/fair_value_band.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): NULL-safe divergence + passive context-block shape"
```

---

### Task B3: Wire passive block + writer rule + prompt-version bump

**Files:**
- Modify: `app/services/thesis.py` (`_assemble_context` ~line 634/848, `_WRITER_SYSTEM` ~line 890, `_PROMPT_VERSION` line 106)

**Interfaces:**
- Consumes: `_shape_fair_value_band`.
- Produces: a `fair_value_band` key in the assembled context; a passive `_WRITER_SYSTEM` rule; a bumped `_PROMPT_VERSION`.

- [ ] **Step 1: Read the band in `_assemble_context`**

Add a `SELECT bear_value, base_value, bull_value, quality_status, reason, as_of_date, ttm_end, basis_json FROM fair_value_band_current WHERE instrument_id = %s AND method_version = %s` (params `(instrument_id, "fvb_v1")`), shape it via `_shape_fair_value_band`, and add `context["fair_value_band"] = fair_value_band` alongside `context["price_anchor"]` (~`thesis.py:877`).

- [ ] **Step 2: Add the passive writer rule**

In `_WRITER_SYSTEM`, add a rule (conditioned on `available:true`, mirroring the #1632 availability rule at v3): *"`fair_value_band` is deterministic valuation-band evidence — a mechanical prior. Ground your bear/base/bull targets against it and explain any large gap. When it is absent or `quality_status` is `low`, rely on your own judgement."* State the hierarchy explicitly: band is the primary valuation anchor when present+high; `price_anchor.close`/52-week range remain the fallback — NOT two peer "justify if outside" constraints. Do NOT say "stay within it."

- [ ] **Step 3: Bump `_PROMPT_VERSION`**

Change `_PROMPT_VERSION = "v3"` → the next free version. **#2010 also intends v4** — whichever of {this, #2010} lands second takes the following version. Resolve the literal here (if #2010 unmerged, take `"v4"`; else `"v5"`). Update the version-history comment block (`thesis.py:102-106`).

- [ ] **Step 4: Smoke**

Run: `uv run pytest tests/smoke -q && uv run pyright app/services/thesis.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/thesis.py
git commit -m "feat(#2009): passive fair-value-band context block + prompt-version bump"
```

---

### Task B4: Divergence audit-row insert in the atomic thesis path

**Files:**
- Modify: `app/services/thesis.py` (`generate_thesis`, the atomic insert path ~line 1235-1278)

**Interfaces:**
- Consumes: `compute_divergence`, `DIVERGENCE_THRESHOLD`.

- [ ] **Step 1: Insert the audit row in the same txn**

In `generate_thesis`, after the validated writer output is inserted into `theses` and its `thesis_id` is available (write-once; no post-hoc UPDATE of the append-only row), compute `divergence_pct, divergence_flag = compute_divergence(llm_base, band_base, DIVERGENCE_THRESHOLD)` where `band_base` is the base pulled from `context["fair_value_band"]` (None when absent) and `llm_base` is the writer's `base_value`. INSERT one `thesis_valuation_audit` row snapshotting `band_method_version="fvb_v1"`, `band_base`, `band_quality_status`, `price_as_of`, `llm_base`, `divergence_pct`, `divergence_flag`. The band snapshot makes a past thesis's divergence reconstructable though the live band is mutable. `_validate_writer_output` stays a **coherence-only hard gate** — divergence does NOT raise (soft signal for QA + critic).

- [ ] **Step 2: Verify the block is conditioned on availability**

`band_base` None (the common ~8,700 path) → `compute_divergence` returns `(None, None)` → the row stores NULL divergence, never 0/false. Confirm no branch writes 0/false on absence.

- [ ] **Step 3: Smoke + a targeted test**

Add a pure test asserting the audit values for (a) a present band and (b) an absent band via `compute_divergence` (already covered in B2 — extend if a `generate_thesis`-level pure helper is extracted). Run: `uv run pytest tests/smoke -q`.

- [ ] **Step 4: Commit**

```bash
git add app/services/thesis.py tests/test_fair_value_band_policy.py
git commit -m "feat(#2009): NULL-safe divergence audit row in atomic thesis insert"
```

---

### Task B5: PR-B pre-flight, checks, authoring, rollout

- [ ] **Step 1: Pre-push gate** (same as A10 Step 1) + `codex exec review` (checkpoint 2).

- [ ] **Step 2: Author the PR** — record: prompt-version literal chosen (and the #2010 collision resolution), a sample thesis with a present band + its audit row, a sample with an absent band (NULL divergence), the smoke output. Security model: read-only band consumption; audit insert is in the existing atomic thesis txn.

- [ ] **Step 3: Push, poll, resolve, merge** per workflow.

- [ ] **Step 4: Rollout sequence** — merge B → operator restarts the VS Code jobs task so `thesis_refresh` picks up the new `_PROMPT_VERSION` + band consumption. No band backfill needed (theses are versioned; existing memos are superseded on their next natural refresh). Verify one freshly-regenerated thesis reads the band + wrote an audit row.

---

## Self-Review (against spec v2)

- **§2 source rules** — SIC cohort key (A1 generated cols + A7 pass-1), `resolve_market_cap_basis` sole authority (A7 step 3), strict-TTM `financial_periods_ttm` (A7 step 4), blend+envelope synthesis (A4), two-layer storage (A1), #1632 discipline (A6/B2). ✓
- **§3 populations** — no code, grounding; §11 constants pinned in Global Constants + validated A9. ✓
- **§4.1 universal precondition** — `_computable` gate applied at selection (A2), cohort (A7 pass-1), conversion (A4 `to_per_share` requires positive per-share). ✓
- **§4.2 selection** — A2. **§4.3 peer two-pass + dual-class anti-join** — A7 + A9 integration test. **§4.4 own-history + lookahead-before** — A5 + A7 step 4. **§4.5 synthesis** — A4. **§4.6 freshness/as-of** — A7 steps 1/6. **§4.7 quality** — A5/A6. ✓
- **§5 storage two-layer** — A1. **§6 DAG + pure/IO + savepoint + bootstrap + recompute + rollup** — A7/A8/A9. ✓
- **§7 consumers** — passive block (B3), divergence measure-only (B2/B4), scoring OUT of scope (untouched). ✓
- **§9 testing** — pure module separate (A2–A6, B2), golden AAPL (A6), one db integration w/ dual-class member + pure twin (A9/A5), dev-verify (A9). ✓
- **§10 two hard-ordered PRs** — PR-A / PR-B split with the drain barrier. ✓
- **§11 open items** — constants pinned (Global Constants), §4.2 map ratified verbatim (A2), cross-multiple median+envelope ratified (A4), `peer_comparison`→SIC re-key filed as follow-up (A10 step 3), EV/EBITDA → v2 (out of scope). ✓

**Type-consistency pass:** `TargetInputs`, `PeerPct`/`OwnPct`, `BandResult`, `QualityInputs` names/fields consistent A2→A7; `compute_band`/`compute_divergence`/`_shape_fair_value_band`/`refresh_fair_value_band_batch` signatures stable across tasks. `METHOD_VERSION="fvb_v1"` used in A7 write + B3 read; `target_basis` threads TargetInputs→compute_band→BandResult→write_band (Codex ckpt-1 #3); `own_points_by_multiple` threads A7→compute_band→quality (Codex ckpt-1 #4). No placeholders in code steps (SQL/Python shown in full; the pass-1 `materialize_cohort_members` SQL + the pass-2 member SELECT are specified by invariant + verbatim `sql/201` oracle CTE + reuse anchor — the one place the implementer writes non-trivial SQL against verified anchors).

**Codex ckpt-1 (2026-07-12) — all 7 findings folded:** #1 pass-1 redesigned to member-level `fair_value_cohort_members` (size refinement now possible in pass-2 Python) · #2 dual-class oracle CTE corrected to `external_identifiers` join (verbatim `sql/201`) · #3 `target_basis` added to `BandResult`/`write_band` · #4 true `own_points`/`excluded_stale_n` into quality (proxy removed) · #5 quality rubric flagged provisional + validated in A9 · #6 distinct-quarter enforced in SQL, documented on `own_range` · #7 `DIVERGENCE_THRESHOLD` added to A2 constants.

**One flagged verification the implementer MUST resolve (A7 step 4):** `fundamentals_snapshot.eps` / `book_value` per-share-vs-total semantics post-#2008 — the own-history P/E & P/B conversion depends on it.
