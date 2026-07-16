# Thesis writer prompt v5 — valuation scaffold + break-condition authoring contract (#2010)

Status: proposal (pre-implementation). Closes the last #2012 sibling.

## Problem — full-population evidence (dev, 2026-07-16, 345 latest theses)

The issue's 2026-07-11 numbers (20/27 no-targets, 2/7 zoneless buys) predate
prompt v3 (#2007) and v4 (#2009 PR-B). Re-scan of the CURRENT population:

1. **Abstention is a band-absence problem, not a stance problem.** v4 latest
   theses joined to `fair_value_band_current` (fvb_v5, base non-null):
   band present → 4/42 no-targets (10%); band absent → 194/205 (95%). The v4
   "band is your primary anchor" rule works; there is NO procedure for the
   band-absent majority, so the writer abstains.
2. **Buys already comply.** v4 buys: 13/13 carry targets AND zones (emergent,
   not enforced). Enforcement is now cheap — zero retro friction.
3. **"of float" fabrication is alive in v4: 58 conditions** (v3: 20, v2: 3 —
   scales with population). The writer is handed the caveat
   "% shares outstanding (public float not ingested)" verbatim
   (`analytics_evidence` positioning block) and still writes "of float".
   Float is not ingested; those conditions are machine-uncheckable forever
   (#2012 extractor Design 4 correctly fails them open).
4. **Premise conditions**: ~11 conditions evaluate true at baseline; 35 match
   distress keywords on names already distressed (#2012 census — e.g. "Altman
   Z crosses into bankruptcy territory (<1.8)" written at z≈−16). The #2012
   arm/baseline machinery quarantines these (`already_true`), but they are
   dead recall — the writer stated its own premise.
5. **Extractor recall is 5.1%** (81/1,585 conditions; 100% precision by
   hand-audit). Recall was explicitly deferred to #2010: durations
   ("for 2+ weeks"), composites ("or days-to-cover >5"), and free phrasing
   all fail open by design.

## Source rules

- Short-interest denominator: shares outstanding, never float. Source
  authority: FINRA's bimonthly equity short interest files publish SHARES
  SHORT (a count), no float (`.claude/skills/data-sources/` FINRA notes,
  #915 ingest); our percent denominator is the EDGAR dei cover-page shares
  outstanding (stated on every 10-K/10-Q cover — the same source the #2012
  freshness bound `share_count_filed ≤ 183d` is built on). Neither source
  publishes float, so float-denominated conditions are uncheckable;
  imputation is banned (`app/services/instrument_analytics.py:325`) and
  #2012 spec Design 4 has the extractor accept EXPLICIT "% of shares
  outstanding" only (shares_out ≥ float ⇒ substituting our denominator
  systematically under-reports the writer's condition).
- Break = TRIGGER not verdict; breaks reach EXIT only via regeneration
  (#2012 spec Design 1, merged). Nothing here touches EXIT.
- Arm/baseline semantics (#2012 spec Design 5): baseline-true = premise,
  never fires. v5's "false at write time" prompt rule is the AUTHORING-side
  complement, not a replacement — the scan-side quarantine stays.
- Prompt budget (settled decisions "Thesis prompt budget", #1987 v2):
  context shape unchanged in v5; system-prompt additions must hold the
  0-length-fail eval gate at the 16k serve window.
- Closed metric vocabulary (#2012, trust-verified per prevention-log):
  `altman_z`, `rsi_14`, `short_interest_pct_shares_out`,
  `short_interest_days_to_cover`, `short_interest_change_pct`,
  `price_vs_sma200`, `sma_50_vs_sma_200`. v5 emits into this vocabulary and
  adds nothing to it.

## Design

### 1. `_WRITER_SYSTEM` v5 — valuation scaffold

Replace the v4 band-grounding rule with a three-tier procedure:

- **Tier 1 (band available + quality high)**: unchanged v4 rule — band is the
  primary anchor; explain any large gap.
- **Tier 2 (band absent / medium / low)**: NEW — derive bear/base/bull from a
  stated basis, restricted to bases whose required inputs the context
  actually supplies (per-basis matrix — Codex ckpt-1 Medium: share count is
  NOT in the context, so any basis needing it is forbidden):
  - `multiple × eps` — needs `fundamentals.eps` (per-share, verified);
  - `multiple × book_value` — needs `fundamentals.book_value` (per-share,
    verified: AAPL 7.26 vs eps 8.26 on dev);
  - re-rating arithmetic `current_price × (target_multiple /
    current_multiple)` — needs `valuation.current_price` + the named ratio
    non-null (`pe_ratio`, `pb_ratio`, `p_fcf_ratio`, `ev_ebitda`, …); the
    algebra cancels share count;
  - FORBIDDEN: bases requiring share count or float (`fcf` and
    `revenue_ttm` are ABSOLUTE dollars in `fundamentals` — a per-share
    target from them cannot be derived from context and would be
    fabricated).
  Justify the multiple chosen (own-history or peer judgement) and SHOW THE
  ARITHMETIC in the memo's valuation section (e.g. "base = 12× EPS 3.10 =
  37.20"). Bear/bull = same basis under stated downside/upside assumptions,
  not copied context landmarks (AMSC #2007 defect).
- **Tier 3 (no defensible basis)**: abstention allowed ONLY with an explicit
  memo line naming the missing input ("No per-share targets: `X` absent").
  Silent null targets are no longer conformant.
- **Buy stance**: targets AND buy zone REQUIRED when `price_anchor` is
  present (validator-enforced, below). `price_anchor` null keeps the v4 rule
  (zones meaningless without a market price).

### 2. `_WRITER_SYSTEM` v5 — break-condition authoring contract

New rules block replacing nothing (additive):

- Every break condition: ONE metric, ONE direction, ONE numeric threshold
  (regime conditions — price vs 200d SMA, 50d vs 200d SMA — carry no
  threshold), checkable on a single scan. No composites ("or"/"and"), no
  duration/persistence qualifiers ("for 2+ weeks", "sustained"), no
  deadlines ("within 6 months", "fails to").
- Conditions must be FALSE at write time — a break is a future transition,
  not a restatement of the current state. Check against the context's
  current values (`analytics_evidence` altman/short interest, `ta_state`
  RSI/SMA regime) before writing one.
- Short interest denominators: "% of shares outstanding" ONLY. Float is not
  in the research context and never will be — a float-denominated condition
  can never be checked.
- Where a condition maps onto the machine vocabulary, ALSO emit it in the
  new `break_predicates` output field (schema below). Prose stays canonical
  for humans; the structured twin is what the nightly scan consumes.

### 3. Writer output schema — `break_predicates` (writer-native recall)

New OPTIONAL output field (operator brief on #2010, item 4):

```json
"break_predicates": [
  {"condition_index": 0, "metric": "altman_z", "op": "<", "threshold": 1.8}
]
```

- `condition_index` references the `break_conditions` array (0-based) and
  must point at a STRING slot — a twin of a malformed non-string element
  has no prose to mirror (Codex ckpt-2), so it is dropped.
- `metric` ∈ closed vocabulary; `op` ∈ {"<", ">"}; `threshold` float, or
  null for the two regime metrics (`price_vs_sma200`, `sma_50_vs_sma_200`).
- **Soft-validated at INSERT, never retry-fails the thesis**: entries with
  unknown metric / bad op / non-numeric threshold (or missing threshold on a
  non-regime metric, or non-null threshold on a regime metric) / out-of-range
  `condition_index` / duplicate `condition_index` are DROPPED with a
  `logger.warning`. A malformed TOP-LEVEL field (non-list, or absent — the
  field is optional) is likewise dropped whole with a warning, never a
  schema retry (Codex ckpt-1 High: the retry-once machinery must not fire
  on this channel). Prose is the canonical record; the structured channel
  is best-effort recall — same posture as the critic.
- Stored in new column `theses.break_predicates_json` (sql/232, additive).
  Only validated survivors are stored.

### 4. Scan merge — writer channel is PURELY ADDITIVE recall

(Codex ckpt-1 High: naive writer-wins would let a hallucinated
`{metric: "rsi_14"}` attached to an Altman prose condition SUPPRESS the
extractor's correct predicate — a false scan channel.)

`run_thesis_break_scan` per latest thesis, per raw `break_conditions` index:

- extractor result for that index non-None → **extractor wins,
  unconditionally** (the 100%-precision channel is never overridden);
- extractor None AND a validated writer predicate exists for that index →
  writer predicate fills the slot (`source_text` = the prose condition);
- neither → no predicate row (unchanged).

The writer channel can therefore only ADD predicates the precision channel
missed — it can never suppress or contradict one. Upsert into
`thesis_break_predicates` unchanged (PK `thesis_id, predicate_index` where
`predicate_index` = raw array index — same alignment contract as PR-A;
prevention-log raw-array-index lesson applies to the writer channel too:
`condition_index` validates against the RAW array bounds, not a filtered
view). New column `thesis_break_predicates.origin`
(`'extractor'`/`'writer'`, default `'extractor'`) records provenance.

Everything downstream is untouched: freshness bounds, arm/baseline,
already_true quarantine, altman sector gate (applies at insert regardless of
origin), event fire path, alerts.

**Re-scan takeover (Codex ckpt-2)**: the upsert's DO NOTHING preserves
baseline state EXCEPT when an extractor-vocabulary improvement now parses a
condition a writer-origin row filled with a DIFFERENT (metric, op,
threshold) — the extractor takes the slot over and the baseline resets to
pending (the old baseline graded a different predicate; gap-uncertainty
machinery then applies). Identical predicates leave the row untouched. A
writer row never shadows the extractor in either direction. Known edge
(accepted, documented): if the stale writer predicate had already FIRED its
event, UNIQUE(thesis_id, predicate_index) suppresses a second fire for the
taken-over predicate on that thesis version — rare (requires a fired writer
twin AND a vocabulary change that reparses it differently), self-heals on
the next thesis version.

### 5. Validator — buy-stance enforcement

`_validate_writer_output` gains an anchor-gated rule, threaded from the call
site (the validator itself stays output-only; the writer call passes
`require_buy_zone=price_anchor is not None`):

- stance == "buy" AND anchor present AND (bear/base/bull null OR zone_low
  null OR zone_high null) → ValueError → rides the existing retry-once.
  Full target set, not base alone (Codex ckpt-2: issue wording is
  "targets/zone", and the tier-2 procedure derives bear/bull from the same
  basis anyway).

Current pop: 13/13 v4 buys already carry all five → expected live failure
rate ≈ 0.

**Harness plumbing (Codex ckpt-1 High)**: `scripts/llm_eval_thesis.py`
`classify_attempt` calls the imported validator directly — it MUST pass
`require_buy_zone` derived from each fixture's `context["price_anchor"]`,
or the re-gate silently under-enforces the exact rule it is gating.

### 6. Version + provenance

`_PROMPT_VERSION` "v4" → "v5" (writer system prompt + output schema change;
context shape unchanged). Version comment documents v5 the same way v3/v4
are documented.

## Re-gate plan (issue requirement + carried #1995 action)

Fixtures are #1987-era (no `fair_value_band` block) — re-capture first.

1. **Re-capture** `tests/fixtures/llm_eval/` on current dev: panel AAPL, GME,
   MSFT, JPM, HD (AAPL/JPM/MSFT carry medium bands, GME low, HD absent) + ONE
   high-band symbol (e.g. FSLR) so Tier 1 is exercised. 6 fixtures.
2. **Structure gate (v5, qwen3:14b writer+critic)**: ≥10 rounds, gate ≥9/10
   with retry, 0 critic length-fails — same bar as #1987.
3. **v4 baseline run (qwen3:14b)** on the SAME new fixtures (old runs are not
   comparable — different fixtures), for the prompt-regression judge.
4. **Judge A — v5 vs v4 (qwen3:14b writers)**: content judge; v5 must not
   lose (win or tie). Checks the scaffold didn't buy targets at the cost of
   numeric grounding / fabrication.
5. **Judge B — phi4:14b vs qwen3:14b, both on v5** (MANDATORY carried action
   from #1995): phi4 content-parity ON THE 6-FIXTURE PANEL — sample
   evidence, not a full-population claim (Codex ckpt-1 flag) — → recommend
   promotion (backfill ~2.8d vs 5.6d). Model default flip stays
   OPERATOR-GATED (PATCH /config) — this PR only posts the recommendation
   with the sample caveat.
6. **New measured metric (informational, not a pass/fail gate)**: predicate
   recall over harness outputs — % of emitted break_conditions covered by a
   valid machine predicate (writer-native or extracted), plus of-float count
   (target: 0) and premise-true count vs fixture context values (hand-audit).
   These are SAMPLE checks — live safety is verified post-merge (below).

All runs on a QUIET Ollama queue (no stale backlog, hourly refresh no-oping,
`pgrep -f llm_eval_thesis` clean).

## Full-population verification

- Scans in "Problem" above: 345 latest theses (247 v4), band-presence join
  against fvb_v5, of-float counts by version — full population, not samples.
- Post-merge: no backfill (prompt change applies to FUTURE generations;
  theses append-only). Existing wide-backfill gate (#1919 item 12) consumes
  this PR's gate results.
- **Post-merge full-population census (Codex ckpt-1 Medium)**: once live v5
  theses accumulate (~1 week of hourly refresh, before the wide backfill),
  re-run the of-float / premise-true / abstention census over
  `prompt_version = 'v5'` rows. The harness numbers are the gate; the live
  census is the safety claim. Recorded as an operator follow-up on #2010's
  close-out comment.

## Non-goals

- No EXIT wiring changes (#2012 Design 1; #2050 settled).
- No extractor pattern changes (precision-gated set stays as PR-A shipped).
- No model default flip (operator-gated; Judge B produces the evidence).
- No `_MAX_TOKENS_WRITER` change unless the gate shows length-fails.
- No live regen wave (issue: re-gate BEFORE any live regen; regen rides the
  existing staleness machinery + the gated wide backfill).

## Files

- sql/232_thesis_break_predicates_writer_native.sql — `theses.break_predicates_json jsonb`,
  `thesis_break_predicates.origin text NOT NULL DEFAULT 'extractor' CHECK (origin IN ('extractor','writer'))`
- app/services/thesis.py — `_WRITER_SYSTEM` v5, `_PROMPT_VERSION`,
  `_validate_writer_output(require_buy_zone=…)`, INSERT stores validated
  `break_predicates_json`, soft-validation helper
- app/services/thesis_break_scan.py — writer-native precedence + origin
- scripts/llm_eval_thesis.py — only if a measured-metric helper is needed
  (prefer a scratchpad script; harness stays benchmark-stable)
- tests: validator table-tests (buy enforcement, predicate soft-validation),
  scan precedence test, migration listed in test manifest
