# Thesis context enrichment — price anchor + valuation + IAR evidence + TA state (#1987)

**Status:** spec (pre-implementation). **Issue:** #1987. **Parent:** #1919
(`docs/specs/thesis/2026-07-09-byo-llm-thesis-live.md`, merged `13af1f6d`).
**⚠ Settled-decision change:** this spec AMENDS settled-decisions "Thesis prompt budget"
(docs/settled-decisions.md:177-182). The amendment is stated verbatim in §Amendment below and must
land in the same PR as this spec.

## Source rule

This spec introduces no NEW data treatment — every block forwards an already-persisted,
already-governed surface verbatim. Where a block's underlying treatment IS reg-governed, the rule
was settled upstream and is cited here, not re-derived:

- **Block C positioning signals (#1823):** insider = Form 4 open-market P/S de-duped net
  (`insider_transactions.py::get_insider_summary`, SEC Item 403 lineage per the #1659/#1667
  ownership work); 13F QoQ = `ownership_institutions_observations` aggregate-shares delta;
  short interest = FINRA bi-monthly (`finra_short_interest_current`), % shares outstanding with
  the public-float caveat recorded at source (instrument_analytics.py:16, :281-344). This spec
  passes those blocks through with their source-attached caveats/statuses; it re-derives none of
  them.

The governing contracts for the change itself are internal:

- **settled-decisions "Thesis semantics"** (:143-185): versioning-by-insert, enums, freshness,
  critic-always, and the v1 capped prompt budget. This spec CHANGES the prompt-budget entry only
  (§Amendment); everything else is preserved.
- **#1632 risk-evidence pattern** (docs/specs/thesis/2026-06-18-risk-evidence-ingestion.md +
  settled-decisions:229-242): persisted, versioned, quality-flagged evidence blocks, as-of-stamped,
  statuses passed through verbatim, prompts forbid citing non-`ok` values as precise numbers. All
  four new blocks follow this pattern.
- **Honest missingness** (thesis-writer skill "Failure conditions" + prevention-log): missing data
  stays `None`, never a fabricated zero; absence carries a reason where the source is structurally
  gated.
- **#1845/#1906 currency contract:** `price_daily.close` is native instrument currency; the writer's
  targets are "per-share price targets in the instrument currency" (thesis.py:598). The price anchor
  therefore sources `price_daily` — the anchor and the targets share a currency by construction.
- **#293 / #273 / #1479:** commit-before-LLM-call ordering, filing-event supersede triggers, and the
  bounded-outbound-I/O chokepoint are all untouched — this spec adds read-only DB queries inside
  `_assemble_context` and changes prompt text; no new outbound I/O, no schema changes.

## Current state (verified 2026-07-10, file:line)

`_assemble_context` (app/services/thesis.py:382-555) feeds the writer: instrument metadata,
5 fundamentals snapshots, 3 filing summaries, 10 news events (30d), prior thesis, risk_v1 metrics.
It does NOT include current price, so the writer emits `buy_zone_low/high` + bear/base/bull
per-share targets blind to price (P0 correctness — issue premise CONFIRMED). Also absent:
valuation multiples, the #1823 IAR evidence block, and TA/trend state.

- `_PROMPT_VERSION = "v1"` (thesis.py:89); `_MAX_TOKENS_CRITIC = 1024` (:84) — length-failed live
  on IEP 2026-07-10 (thesis stored without `critic_json`; harness fixtures never hit the limit).
- TA indicators are persisted latest-row-only on `price_daily` (sql/025); `price_vs_sma200` /
  `trend_sma_cross` are derived in `compute_indicators` (technical_analysis.py:279-294) and NOT
  persisted (#1989 tracks persistence; this spec derives them read-side and does not depend on it).
- `scores.analytics_json` (#1823, `iar_v1`) carries piotroski/altman/positioning/peer_grade;
  populated only by `compute_rankings` (GME sample: 1,561 chars).
- `instrument_valuation` (sql/201) is quotes-gated: `priced` CTE reads `FROM quotes` only.

## Full-population verification (dev DB, 2026-07-10)

12,603 tradable instruments:

| surface | coverage |
|---|---|
| `price_daily` any close | 5,198 |
| close within 7d | 705 |
| ≥250 close rows (52w range solid) | 3,372 |
| `quotes` rows | 85 |
| `instrument_valuation` rows | **50** |
| latest `scores` row present / with `analytics_json` | 3,908 / 3,906 |
| latest price row `sma_200` / `return_1y` non-null | 3,412 / 3,377 |

Generation cohort (held ∪ top-20 ∪ 11 existing theses = 26 names): price anchor 26/26 (latest
close ≤2 days old for all); `instrument_valuation` **9/26**; `analytics_json` 24/26 (QQQ/VOO —
unscored ETFs — absent); `sma_200` 23/26 (VMD/WLYB/XOMA <200d history); 52w window thin (<200
rows) for the same 3.

**Premise falsified:** the issue frames the `instrument_valuation` row as a P0 pairing with the
price anchor. Full-population: 50/12,603 rows (quotes-gated, the #1857 class; the COALESCE fix is
operator-gated and NOT taken here). The view is an *optional* statused block; the P0 anchor is
`price_daily` — covered 26/26 on the generation cohort (held ∪ top-20 ∪ existing theses), NOT
universe-wide (5,198/12,603 any close; 705 fresh ≤7d). A manual POST outside the covered names
can still yield `price_anchor = None`; §Block A defines the fail-honest behaviour for that path
(buy zone forbidden without an anchor). Quotes are deliberately excluded from the anchor (85
rows; avoids mixing a second price source/currency into a targets-in-native-currency contract).

**Full-population `analytics_json` shape scan (3,906 rows, 2026-07-10):** 3,906/3,906
`schema='iar_v1'`; all four top-level blocks present; every non-null `signal` ∈ [0,1]; all bands
typed. One structural quirk: 818 rows carry a non-null `insider_net_90d.signal` with NO `asof`
key — upstream attaches `asof` only when `latest_txn_date` exists, while
`open_market_net_shares_90d` is COALESCE'd to 0 (instrument_analytics.py:592-602), so
no-recent-txn names get a neutral signal without a date. The shaper treats `asof` as optional
and the prompt says undated evidence is citable only as approximate.

## Design

All four blocks are added to the `_assemble_context` return dict and described to the writer +
critic. Missing block → `None` (+ machine-readable `reason` where structurally gated). No schema
changes; no new tables; read-only queries on existing surfaces.

### Block A — `price_anchor` (P0)

From the latest `price_daily` row with `close IS NOT NULL`, plus a trailing-365d aggregate:

```text
{ close, price_date, currency (echo of instruments.currency),
  high_52w, low_52w, window_days_52w,          -- MAX(COALESCE(high,close)) / MIN(COALESCE(low,close)) over price_date >= latest-365d; window_days = row count (honest for thin histories)
  return_1w, return_1m, return_3m, return_6m, return_1y }   -- persisted columns, NULLs pass through
```

No rows → `None`. `price_date` staleness passes through verbatim — the writer is told the as-of
date and instructed to treat a stale anchor as approximate (eToro market data is unreachable in
some dev environments; anchors may lag). **No-anchor rule:** when `price_anchor` is `None` the
writer is instructed to leave `buy_zone_low/high` null regardless of stance (an entry band is
meaningless without a market price) and to emit base/bear/bull only where fundamentals give a
defensible per-share basis — the existing "null if insufficient data" rule, made explicit for
the anchorless path.

### Block B — `valuation` (optional evidence; absence statused, availability NOT guaranteed)

`instrument_valuation` row when present: `current_price, price_as_of, market_cap_live,
enterprise_value, pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield, ev_revenue, ev_ebitda,
debt_equity_ratio, net_margin, gross_margin, operating_margin, roa, roe, dividend_yield,
is_complete_ttm`. Absent row → `{"available": false, "reason": "no_live_quote"}` — the view is
quotes-gated, so absence is structural, not an error. #1664 dual-class NULLs pass through
(honest suppression). The writer prompt notes `current_price` here may differ slightly from the
anchor (different source/timestamp) and the ANCHOR is authoritative for target sanity checks.

### Block C — `analytics_evidence` (P1)

Latest `scores` row (`ORDER BY scored_at DESC LIMIT 1`): `analytics_json` shaped compact +
`scored_at` + `model_version` as-of stamps. Shaping (pure function, table-tested, **fail-closed**:
a non-dict block, unexpected type, or out-of-range signal is dropped to `None` with reason
`"malformed"` — never forwarded; grounded by the full-population shape scan above):

- `piotroski`: keep `score, band, components_available, suppressed, reason`; DROP the 9 component
  booleans (token noise).
- `altman_z`: verbatim.
- `positioning`: verbatim (insider_net_90d / inst_13f_qoq / short_interest — already statused,
  as-of-stamped, caveated at source).
- `peer_grade`: keep `peer_key, peer_n, basis`, per-family `{hybrid, percentile}`; DROP `absolute`.

No scores row / NULL `analytics_json` → `None`. ⚠ `analytics_json` refreshes only on
`compute_rankings` — staleness is allowed and stamped (`scored_at`), mirroring risk_v1's
`as_of_date` discipline.

### Block D — `ta_state` (P1)

From the same latest `price_daily` row as Block A: `sma_50, sma_200, rsi_14, macd_histogram,
atr_14, volatility_30d`, plus derived-at-read `price_vs_sma200` ("above"/"below"/null) and
`sma_50_200_regime` ("golden"/"death"/null) — same comparison semantics as
technical_analysis.py:279-294, but named honestly: the internal `trend_sma_cross` value is the
CURRENT 50-vs-200 regime, not a recent crossover event, and the context key must not invite the
writer to infer one (the prompt states this explicitly). Divergence from the internal column
name is deliberate; when #1989 later persists the derived signals, the read-side derivation is
replaced and the context key stays `sma_50_200_regime`. Where the internal fn emits "none"
(either SMA null), the context uses `null` (missing evidence, not a third regime). NULL
indicators stay null (<200d histories keep `sma_200 = None`).

### Prompt changes (writer + critic) — `_PROMPT_VERSION` "v1" → "v2"

Writer system prompt additions:
- Describe the four blocks (statuses, as-of stamps, fractions-vs-percent conventions where needed,
  `signal` fields in `positioning` are 0–1 normalized; a positioning entry without `asof` is
  undated — citable only as approximate; `sma_50_200_regime` is the CURRENT 50-vs-200 relation,
  not a recent crossover event).
- **Target sanity rule:** sanity-check `buy_zone_low/high` and bear/base/bull against
  `price_anchor.close` + the 52w range; state the implied upside/downside to base explicitly in the
  memo; a `buy` stance with a buy zone wholly above the current price, or targets orders of
  magnitude off the 52w range, must be corrected or justified in the memo.
- **No mechanical anchoring:** the anchor grounds the numbers; it does not replace valuation
  judgement — do not emit `base_value ≈ close` as a default.
- Honest-missingness rules extended to the new blocks: absent block = absent evidence; never
  invent multiples or trend states; do not cite a stale `scored_at`/`price_date` figure as current.

Critic system prompt additions:
- Attack target-vs-price inconsistency (buy zone vs market, implausible implied upside, targets
  outside any historical range without justification) and memos that ignore an adverse
  `positioning`/`peer_grade`/trend signal.

### Critic token budget

`_MAX_TOKENS_CRITIC` 1024 → 2048 (= `_MAX_TOKENS_WRITER`). Live length-failure on IEP
(2026-07-10) stored a thesis without `critic_json`; enriched context makes recurrence more likely.
Local-first default makes the cost delta negligible. Constant, not settled-decision material.

## Amendment — settled-decisions "Thesis prompt budget"

Replace the section body (docs/settled-decisions.md:177-182) with:

```text
### Thesis prompt budget
Use capped context in v2 (#1987):
- latest 1 prior thesis
- latest 3 filing events
- latest snapshot + up to 4 prior fundamental snapshots
- latest 10 news items from the last 30 days
- risk-evidence block (#1632): instrument_risk_metrics_current scalars, statused, as-of-stamped
- price anchor (#1987): latest price_daily close (native currency) + 52w range + persisted returns
- valuation block (#1987): instrument_valuation row when present; structurally-absent otherwise
  (quotes-gated view — absence is statused, not an error)
- analytics evidence (#1987): latest scores.analytics_json, shaped compact, scored_at-stamped
- TA state (#1987): latest price_daily indicator columns + derived sma-cross/price-vs-200d signals

All blocks follow the #1632 evidence discipline: statuses verbatim, as-of stamps, missing data
stays missing. Context-shape changes bump `_PROMPT_VERSION`.
```

The four v1 caps are unchanged; the amendment records the evidence blocks that were previously
implicit (#1632) or absent (#1987) so the budget stays the single settled description of writer
input.

## Eval gate (mandatory before impl-PR merge)

1. Re-capture the 5-fixture panel (`scripts/llm_eval_thesis.py capture`) on dev after the context
   change — fixtures are `_assemble_context` snapshots and MUST carry the new blocks.
2. **Harness extension (in scope):** the current gate is writer-only and the `finish_reason` mix
   is aggregated across writer+critic (llm_eval_thesis.py:406) — no gate fires on a critic
   truncation. Add a per-role critic length-failure count to the report and extend the gate:
   **critic `finish_reason == "length"` = 0 across the run** at 2048 tokens.
3. Re-run `run` on qwen3:14b (configured default): **writer ≥9/10 with retry** (unchanged #1919
   gate) + the new critic-length gate.
4. Record on the impl PR: pass rates, tok/s, and input-size growth (prompt chars per fixture,
   before → after). Empirical prior: current writer input ≈4-6k tokens; projected growth ≈+700
   tokens (~15%) — well inside the local model's window (qwen3:14b handles the current 4-6k live).

## Implementation phasing — single PR

Schema: none. Service: 4 context queries + shaping helpers + prompt text + constants
(`_PROMPT_VERSION="v2"`, `_MAX_TOKENS_CRITIC=2048`). Tests: pure table-tests for the shaping
helpers (analytics compaction, sma-cross derivation, 52w aggregate honesty on thin windows) +
extend the existing `_assemble_context` mock test for the new keys. No DB-tier test (no new SQL
mechanism — plain reads on existing tables; the eval-gate recapture exercises the real queries on
dev). Dev-verify: force-regen one held name; memo must reference the current price; targets sane
vs anchor; VerdictTab + /theses render unchanged.

## Out of scope

- #1857 valuation-view COALESCE fix (operator-gated, model_version bump).
- #1988 staleness v2; #1989 TA persistence + ta-analyst skill; #1995 model sweep + content grading.
- Wide T1+T2 backfill (operator plan: after this ships).
- Position/holding state in context (not in #1987).

## Acceptance

- Writer context carries all four blocks on a fully-covered name (GME) and honest absences on a
  gap name (QQQ: no analytics/valuation; price anchor + TA present).
- Eval gate recorded on the impl PR (§Eval gate).
- New theses rows carry `prompt_version='v2'`.
- Settled-decisions amendment merged with this spec; no other settled decision touched.
- `uv run pytest -m "not db"` + smoke green; frontend untouched.
