# Thesis staleness v2 — data-driven regen triggers (#1988)

Status: proposal (design-first per issue; full-population threshold
research below precedes any implementation).

## Problem (issue premise, re-verified 2026-07-16)

`find_stale_instruments` (app/services/thesis.py) triggers on exactly:
no_thesis, missing_frequency, new 10-K/10-Q/8-K (#273), break_fired
(#2012), and age vs review_frequency. The staleness SQL reads
instruments/coverage/theses/filing_events/thesis_break_events only. A
thesis stays "fresh" through a 40% drawdown or a news storm inside its
cadence window — and the portfolio manager keeps consuming its
stance/buy-zones.

## Premise falsification — the issue's predicate 3 is dead weight

The issue proposed three predicates. Full-population verification KILLS
one:

- **Fundamentals delta (issue predicate 3): REDUNDANT — dropped.**
  `fundamentals_snapshot` rows are written through from
  `financial_periods` (#2008 write-through), which derives from
  10-K/10-Q filings. Every new snapshot is therefore preceded by a
  filing event that rule 3 (#273 `event_new_10k`/`event_new_10q`)
  ALREADY fires on — the thesis regenerates with the new fundamentals
  before any delta predicate could observe them. The only snapshot
  changes without a filing event are parser-fix / rebuild backfills,
  which are deliberately NOT regen triggers (a code-side restatement of
  history is not new information about the company). Verified: 342/345
  thesis instruments carry ≥2 snapshots (the capability exists), but
  the trigger path is already covered end-to-end by #273.

Two predicates survive: price-vs-thesis and news spike.

## Source rules

- **Arm/baseline semantics (#2012 spec Design 5, merged):** a condition
  already true at write time is PREMISE, not a transition, and must not
  fire. This applies verbatim to band-exit (below): full-pop check
  2026-07-16 found **15/60 banded latest theses already outside
  [bear, bull] at mint** (2 below bear, 13 above bull — writers price
  bands around, not on, the spot). A naive band-exit predicate would
  thundering-herd 25% of the banded corpus on day one with zero new
  information.
- **Break = TRIGGER not verdict (#2012 Design 1):** staleness v2 reasons
  feed regeneration only; nothing here touches EXIT (#2050 settled).
- **Additive predicates, #273 semantics preserved (issue constraint):**
  new reasons append to the existing ordered rule list; existing
  reasons/ordering unchanged. Every new reason string surfaces in
  `thesis_runs.trigger` detail unchanged (the scheduler already passes
  the stale reason through).
- **Batch/cadence bounds (#1919 PR-B, unchanged):** thesis_refresh
  drains ≤ `_THESIS_REFRESH_BATCH_LIMIT` per hourly run, held-first.
  A market-wide crash marks many stale; the drain stays bounded by
  construction — no new rate machinery needed, and the held ∪ top-20
  scope already prioritises the names that matter.
- **NULL never 0 (#1632):** instruments without bands / without news
  baseline are simply not evaluated by the respective predicate —
  absent inputs, absent trigger.

## Full-population threshold research (dev, 2026-07-16)

**Corpus-age caveat:** all 345 latest theses were minted 2026-07-09→16,
so own-corpus move-since-mint is degenerate (≈0 everywhere). Thresholds
are therefore derived from the UNIVERSE price distribution (5,188
instruments with 30d history) — the corpus will look like the universe
once theses age.

- Universe 30d move: p5 = −31.8%, p10 = −22.7%, median −0.4%.
  Drawdown-exceedance rates: −15% → 18.2%, −20% → 12.8%, −25% → 8.3%,
  **−30% → 5.7%**, −40% → 2.7%.
- News mass (345 thesis instruments): only **84/345 have a nonzero
  trailing-30d news baseline** — the predicate can only ever speak for
  a quarter of the corpus (documented, not a blocker; it exists to
  catch storms on covered names). 7d-rate / 23d-baseline-rate ratio:
  median 0.54, p90 = 3.05, p95 = 5.82. Ratio ≥ 3 fires on 9/345 today.
  A pure ratio explodes on tiny baselines (one story on a
  near-zero-news name), so an absolute-mass floor is required.
- Band coverage: 60/345 latest theses carry both bear+bull (rises with
  #2010's tier-2 derivation shipping targets for the band-absent
  majority).

## Design — two additive predicates

### New reason `price_move` (drawdown/spike since mint)

Fires when `|close_now − close_at_mint| / close_at_mint ≥ 0.30`, where
`close_at_mint` is close at-or-before `latest_thesis.created_at::date`
(`_close_at_or_before` contract, #2014) and `close_now` is the latest
`price_daily` close.

**Price-input guards (source rules, not invented here):** both closes
must be `> 0` (zero closes are non-price sentinels — day-change spec,
docs/specs/ui/2026-07-04-instruments-day-change.md) and `close_now`'s
`price_date` must be ≤ 10 days old (the #2012 break-predicate freshness
bound for price-derived inputs, docs/proposals/thesis/
2026-07-16-thesis-break-predicates.md). A stale or sentinel price means
the predicate is NOT EVALUATED for that name — a stale close firing a
regen would be a false trigger, and the regenerated thesis would anchor
on the same stale price (#2014 stale_anchor class). Both guards apply
identically to rule `band_exit` below.

- Threshold 0.30 ≈ 5.7% of the universe on a rolling 30d basis — a real
  regime change for a long-horizon book, not noise; symmetric (a +30%
  melt-up invalidates a buy-zone as surely as a −30% crash invalidates
  a bear floor).
- **Threshold is PROVISIONAL (flagged):** it rests on the universe
  distribution as a surrogate because the thesis corpus is 7 days old
  and its own move-since-mint distribution is degenerate (research
  above). The honest treatment is surrogate-now + mandatory
  re-verification on the ACTUAL fire rate over the real corpus ~30d
  post-ship (fvb R-retune precedent, #2021/#2022): if trigger rate sits
  outside ~2-8%/month, retune before the wide backfill multiplies the
  corpus. Recorded as an implementation-slice acceptance gate.
- Self-rearming by construction: firing regenerates the thesis → new
  `created_at` → new baseline. No arm/baseline table needed.
- Reason string: `price_move` with detail
  `"close {now} vs {mint} at mint ({pct:+.0%})"`.

### New reason `band_exit` (price crosses outside [bear, bull])

Fires when the latest close crosses OUTSIDE `[bear_value, bull_value]`
AND the thesis was minted with the price INSIDE the band
(arm-at-mint: `close_at_mint` within the band). The 15/60
already-outside-at-mint class is premise and never fires — exactly
#2012 Design 5, implementable here WITHOUT a state table because the
mint-time price is deterministic history (`close_at_or_before(created_at)`),
re-derivable on every scan: armed ⇔ minted-inside, fired ⇔ now-outside.
- Only evaluated when both `bear_value` and `bull_value` are non-null
  (60/345 today, growing under #2010).
- Reason string: `band_exit` with detail
  `"close {now} outside [{bear}, {bull}] (minted inside at {mint})"`.
- Overlap with `price_move` is fine: first matching rule in order wins
  (existing contract); a name 30%-down AND below bear reports
  `price_move`.

### New reason `news_spike`

Fires when trailing-7d importance mass rate ≥ 3× the prior-23d baseline
rate AND 7d mass ≥ 2.0 (absolute floor ≈ two important stories or
several minor ones; kills the tiny-baseline ratio explosion).
`m7 = Σ importance_score (7d)`, `baseline = (m30 − m7) / 23` per day,
fire ⇔ `m7/7 ≥ 3 × baseline AND m7 ≥ 2.0 AND baseline > 0`.

- `importance_score` is the documented ingest-time heuristic
  (category + source tier + recency-at-ingest, `app/services/news.py`
  `_importance_score`) — this predicate treats it as additive event
  mass, the same treatment the scoring sentiment family already applies
  (importance-weighted 30d aggregation). Rows with
  `importance_score IS NULL` are excluded EXPLICITLY (an implicit SQL
  SUM null-skip is still a treatment decision; make it visible in the
  query).
- 9/345 today at ratio ≥ 3 pre-floor — bounded. Self-rearming without
  state: stored scores are static, but WINDOW MEMBERSHIP rolls the
  spike's stories out of the trailing 7d and into the 23d baseline,
  so the ratio subsides ~a week after the storm.
- Baseline-less names (261/345) are not evaluated — documented reach
  limit, not a gap: the predicate exists for covered names' news storms.
- Reason string: `news_spike` with detail
  `"7d news mass {m7:.1f} at {ratio:.1f}x 30d baseline"`.

### Rule ordering (additive; existing reasons + relative order unchanged)

Today's order is 1 no_thesis · 2 missing_frequency · 3 event_new_*
(#273) · 4 break_fired (#2012) · 5 cadence ("stale"). The three new
reasons INSERT between break_fired and cadence:
no_thesis → missing_frequency → event_new_* → break_fired →
**price_move → band_exit → news_spike** → cadence.
Existing reason strings and their relative order are untouched (the
docstring's numeric labels renumber; nothing parses rule numbers).
Data-triggers sit after break_fired (a fired machine predicate is the
sharper signal) and before the cadence catch-all, mirroring how #2012
slotted break_fired.

## Reconciliation with #2012 (issue requirement)

- #2012 break predicates are WRITER-AUTHORED conditions (thesis-specific
  falsifiers, arm/baseline state machine, event rows, alert surface).
  #1988 predicates are STRUCTURAL (identical formula for every thesis,
  no authored condition, no event rows, no alert surface — they are
  scan-time reasons only, recomputed on read).
- No state tables here: rule 6/8 self-rearm via regeneration/decay;
  rule 7 derives its armed state from deterministic history. If a later
  predicate needs true cross-scan state, it joins #2012's tables — not
  a third mechanism.
- A #2012 predicate fire and a #1988 trigger on the same name coalesce
  in rule order (first match wins) — one regeneration either way.

## Cost / thundering-herd check

The two price reasons read `price_daily` (2 indexed lookups per candidate thesis);
the news rule aggregates `news_events` over 30d per candidate. The scan
runs where `find_stale_instruments` already runs: hourly
`thesis_refresh` over held ∪ top-20 candidates (≤ ~45 names/run) and
the filing cascade's scoped checks — there is NO nightly full-tier
staleness scan today and this spec does not add one (query shapes match
the #2014 audit at full population — 282 rows, seconds — so a future
tier-wide scan is cheap if ever wanted). A market-wide −30% day marks the whole banded corpus
stale; the batch limit (≤5/hr) drains held-first, bounded — identical
behaviour to a mass filing-event day under #273 today.

## Non-goals

- No EXIT/portfolio wiring (settled).
- No fundamentals-delta predicate (falsified above).
- No per-instrument threshold tuning, no volatility-scaled thresholds
  (v2 candidates once the calibration ledger (#2002) can grade whether
  regen-on-trigger improved outcomes).
- No new alert surface (regens already surface via #2013 diff alerts).

## Files (implementation slice, after spec approval)

- app/services/thesis.py — three additive rules in the
  `find_stale_instruments` SQL + `StaleReason` literal additions
- tests — pure predicate table-tests (threshold boundaries, premise
  band-exit never fires, floor kills tiny-baseline ratio); ONE db-tier
  test pinning the SQL
- docs/specs move on approval; settled-decisions entry for the
  thresholds (0.30 / 3× + 2.0 floor) at implementation time
