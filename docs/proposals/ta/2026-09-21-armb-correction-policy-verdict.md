# ARM B §7 item 1 — the correction policy for disputed split stamps

Refs #2834, #2437. Evidence: `scripts/measure_2834_armb_correction_policy.py`.
Reproduce with `PYTHONPATH=. uv run python -m scripts.measure_2834_armb_correction_policy`.

Predecessors, both merged: `b65abd9c` (PR #3279) settled the signal basis —
`icyDenev/Intrader` ships a per-bar split ratio in CSV field 6 and the split-only series
is `close / scale`. `8bb1443f` (PR #3280) established that **no non-circular adjudicator
is available**: SEC XBRL reaches 4.52% of stamps and returns a non-direct answer on 12.0%
of a control arm where nothing is in dispute.

So this decision is made without an umpire, and that is measured rather than assumed.

## 1. The question

`b65abd9c`'s §7 item 1: *"Apply, suppress, re-date or quarantine? The four classes in §3
want different answers, and 47.5% of events have no adjudicator at all."*

**Source rule.** The APPLICATION rule is the archive's own reader and is settled
(`IntraderEngine::SplitCheck()` — new shares per old, stamped on the bar that first prints
the post-split level). ⚠ **There is no published rule for what to do with a stamp a second
processing disputes.** None was found and none is invented here: per `.claude/CLAUDE.md`,
the rule is fixed **by construction** and frozen, and §5 states the construction.

## 2. The arms

Every arm is a scale set over the same stored raw closes, so the uncorrected status quo is
not a special case — it is the arm whose scale is 1 everywhere. All four are scored in one
panel pass and compared pairwise, on #3278's window (1994-01-01 .. 2021-06-29), 312
formations, 121,601 decile slots.

| arm | events applied | share of checked |
| --- | ---: | ---: |
| `suppress_all` (today's s2) | 0 | 0.00% |
| `apply_all` | 8,042 | 100.00% |
| `apply_unless_refuted` | 7,659 | 95.24% |
| `apply_only_corroborated` | 3,824 | 47.55% |

Of 8,042 checked events: **383 (4.76%) refuted** by the second processing, **3,835
(47.69%) unadjudicable** — no comparable bar pair exists.

⚠ `quarantine` is deliberately **not** a scored arm. It does not change a score, it removes
SERIES, and a displacement percentage computed against a different cross-section does not
mean the same thing as the other three. It is reported as what it deletes (§3.3) and refused
in §5.

⚠ It is also taken in ONE narrow form — *delete the whole series if any of its stamps is
refuted*. Event-level quarantine, segmenting the series at the disputed bar, excluding only
the affected window, and per-class treatment are all unmeasured alternatives. They are not
refused here; they are out of scope, and a later session wanting one should read §5 item 5 as
"this form of quarantine is refused", not "quarantine is refused".
`re_date` is reported as a **bound** (§3.4), because it can only touch events whose step
appears on a nearby bar and that population is already measurable.

⚠ **The eligibility floor stays on raw `close` in every arm.** `MIN_CLOSE` is applied to the
stored close identically across arms, so the admitted set is the same in all four and the
only thing that moves is the score. Letting the floor follow each arm's basis would confound
membership with signal. This deliberately leaves §7 item 3 — what the floor should read on a
back-adjusted basis — open; it is a strategy-definition question, not a policy one.

## 3. What each policy costs

### 3.1 Decile displacement

| pair | displaced | of 121,601 slots |
| --- | ---: | ---: |
| `suppress_all` → `apply_all` | 17,385 | **14.30%** |
| `suppress_all` → `apply_unless_refuted` | 16,844 | 13.85% |
| `suppress_all` → `apply_only_corroborated` | 6,274 | 5.16% |
| `apply_all` → `apply_unless_refuted` | 603 | **0.50%** |
| `apply_all` → `apply_only_corroborated` | 11,561 | **9.51%** |
| `apply_unless_refuted` → `apply_only_corroborated` | 11,006 | 9.05% |

The first row comes out at `b65abd9c`'s 14.30%, which is the control this table needs: the
panel is the predecessor's, re-expressed with the raw arm as `scale ≡ 1`. ⚠ Agreement to two
decimal places is consistency evidence, not proof that the two runs built identical panels.

⚠ Two properties the pairwise arithmetic inherits from that panel and does not establish.
Ranking breaks ties on `name_key` alone, so equal scores on two series sharing a key would
give unstable membership — `name_key` is the engine's own total ordering key and is assumed
unique here, not verified. And a bar missing from the quarantine coverage range is dropped
before `lag`, which compresses that series' lookback rather than voiding it; the exposure is
identical across arms but is not quantified.

Per decade, the two contrasts that decide the question:

| decade | formations | `apply` → `unless_refuted` | `apply` → `only_corroborated` |
| --- | ---: | ---: | ---: |
| 1990 | 71 | 0.59% | 9.50% |
| 2000 | 107 | 0.14% | 5.26% |
| 2010 | 116 | 0.47% | 11.10% |
| 2020 | 18 | 1.77% | 14.25% |

**Reading it.** Correcting at all is the large move. *Adjudicating* the disputes on top of
that changes **0.50%** of the decile and never exceeds 1.77% in any decade. *Gating* on
corroboration changes **9.51%**, because it suppresses everything the reference does not
positively confirm — **52.45%** of checked events (47.69% unadjudicable plus 4.76% refuted),
leaving the 47.55% it corroborates.

⚠ These are **set distances, not benefits**. They are not additive and none of them is a
measurement of correctness: 9.51% next to 14.30% does not say that gating discards "two
thirds of the correction", only that the two arms' deciles differ by that much. Nothing in
this document measures the return impact of any arm, and nothing should be quoted as if it
did.

⚠ `apply_all` here means **every stamp with a usable own-bar pair — 8,042 of 8,073**. The 31
unusable events (own bar or prior bar missing or non-positive) are outside every arm because
the cross-vendor check cannot classify them, not because the policy would skip them. In
production `apply_all` applies all 8,073.

### 3.2 Not every refutation is about the stamp

Of the 383 refuted events, a subset are ones where the REFERENCE's own close carries the raw
step too, so the disagreement is the reference's basis and not the factor. `b65abd9c` §3's
worked case is CIVB 1996-05-09 on a 4:1: Intrader prints `81 → 20.25` and the reference
prints `20.25 → 5.0625` — both raw, so their one-bar ratios already agree at 0.25 and it is
correcting one of them that opens the gap. Measured on this run: **32 of the 383 refuted
events (8.36%)**, reproducing the predecessor's count at the same tolerance. Any policy that
suppresses on refutation suppresses those too, and they are the subset most likely to be
**correct**.

### 3.3 What `quarantine` deletes — and where its bias actually is

| population | series | carrying delisting evidence | rate |
| --- | ---: | ---: | ---: |
| all admitted | 17,285 | 1,010 | 5.84% |
| stamped, reference-checkable | 1,681 | 28 | **1.67%** |
| stamped, unadjudicable | 1,893 | 328 | **17.33%** |
| refuted → dropped by `quarantine` | 260 | 7 | 2.69% |

⚠ The two middle rows are **not a partition** and their labels are per-SERIES, not per-event:
"checkable" means the series carries at least one comparable event and "unadjudicable" at
least one incomparable event, so a series can be both. They overlap by 5 and do not sum to
the 3,569 series carrying a *checked* stamp (the census counts 3,582 series carrying a
stamp at all — the difference is the 31 unusable events' series).

⚠ `delisting_source IS NOT NULL` measures **recorded evidence of delisting**, not survival.
Its completeness and timing are untested here.

**Quarantine's direct deletion is not the biased part.** It drops 260 series, 1.50% of the
admitted universe — and those 260 are 2.69% delisted-evidence against the universe's 5.84%,
so removing them *raises* the remaining rate slightly rather than stripping delisted names.
Within the population it can actually reach, it is if anything harder on them: from the table
above, **7 of the 28** checkable delisted-evidence series are dropped (25.0%) against **253 of
1,653** without (15.3%).

**The bias is in what it cannot reach.** A rule that fires on refutation can only fire where a
second processing exists, and that population is **1.67%** delisted-evidence against
**17.33%** for the unadjudicable one. ⚠ That is a single aggregate comparison, unadjusted for
era, factor mix, venue, volatility, history length or event frequency, so it supports a
direction and a magnitude — not "the same direction every time". `research-price-corpus.md`
records a consistent measurement on a *different* cohort (the reference vendor is 0/382 on the
2023 Form 25 cohort); this run's 28 checkable delisted-evidence series show that the exclusion
is strong rather than absolute.

So a reference-conditioned policy corrects the reference-served part of the corpus and leaves
the rest to a different rule, and the served part is markedly less likely to carry delisting
evidence. Correction quality would then vary with a property correlated with survival, in a
corpus whose entire purpose is to remove that correlation.

### 3.4 `re_date` is a rounding error

Of 383 refuted events, **39 (10.18%)** have a step within ±3 bars that matches the stamped
factor — 0.48% of checked events.

⚠ **39 is a probe hit count, not a bound in either direction**, and an earlier draft of this
section wrongly called it an upper bound while listing reasons it undercounts. It overcounts
because a real move of the same size registers as a hit. It undercounts because the probe
only runs on events the reference can adjudicate, looks ±3 bars, and requires a 1% match. The
honest statement is that the population re-dating could plausibly repair is **small and
measured only on the served half** — it is not a candidate for the corpus-wide policy. It
remains available as a later refinement on top of whatever policy is chosen, which is a
different question from the one decided here.

⚠ It is also reported against a different partition from `b65abd9c` §3's comparable figure
(32 of 351, which excludes the reference-unadjusted class). The two are not the same
quantity; 39 of 383 includes it.

### 3.5 Direction of harm — which errors get SELECTED IN

The displacement table measures set distance and nothing else. It cannot say which arm's
errors are *worse*, and the arms are not symmetric, because **s2 selects the top decile
only** and the back-adjustment divides the bars *before* an event. For a formation whose
lookback spans the event:

| error | effect on `c_skip / c_back` | consequence |
| --- | --- | --- |
| apply a **forward** factor (>1) that is not really there | inflated | **selected in** — a contaminated holding |
| leave a real **reverse** split (<1) uncorrected | inflated | **selected in** — a contaminated holding |
| leave a real **forward** split uncorrected | depressed | a miss |
| apply a spurious **reverse** factor | depressed | a miss |

On a long top-decile strategy a depressed score is a miss and an inflated one is a position,
so only the first two rows contaminate. Counting them:

| policy | exposed population | events | inside the window's reach |
| --- | --- | ---: | ---: |
| `apply_all` | refuted FORWARD stamps, applied | 228 | **158** |
| `suppress_all` | real REVERSE splits, left uncorrected | 2,728 | **2,122** |

Of 8,042 checked events, 5,314 are forward and 2,728 reverse; **6,561 (81.58%)** fall inside
the window's scoring reach.

⚠ **These are upper bounds on exposure, never counts of contamination.** A spurious factor
only reaches a score if a formation's lookback spans the event, and only enters the decile if
the resulting inflation is large enough. Neither is established here, and the "in reach"
column bounds the calendar span generously (`LOOKBACK_BARS × 7/5` days) rather than per-series.

⚠ This is **not** the *"the error has a sign"* mechanism `b65abd9c` withdrew. That claim was
withdrawn precisely because reverse splits move the error the other way; what survives is the
partition above, which has no global sign and is counted rather than reasoned.

## 4. The damage is NOT contained elsewhere — and rule 10 says it is

`strategy-catalogue-and-backtest-validity.md` §4 rule 10 states the containment corpus-wide:
*"`price_series_break` segments (402 rows) are `not_evaluable`, never spanned."* An
uncorrected split IS a level break, so a reader can reasonably conclude the damage is already
contained and this whole decision is cosmetic. It is not, for two checkable reasons.

1. **`price_series_break` cannot key most of this corpus.** `sql/246:103` declares
   `instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id)`. The research corpus
   is keyed on `series_id` *precisely because* part of it has no `instruments` row — measured
   on the admitted universe, **12,116 of 17,285 series (70.10%) have none**. ⚠ That is a
   missing *stored link*, not proof that no matching instrument exists anywhere; it is enough
   for the claim being made — the break table cannot address those rows today — and not for a
   stronger one. The remaining 29.90% *are* linked, so the clause is unavailable to the
   majority of the universe rather than to all of it.
2. **The research rule set DOES write the verdict, and nothing on the read path consumes it.**
   T3 — *"this level break is not a return"* — is evaluated over this corpus and stored in
   `research_transition_quarantine` (`sql/251:111`). ⚠ This clause is established by **grep,
   not by the script**: `rg research_transition_quarantine app sql` returns the ingest writer
   and the `research_quarantine_census` view and nothing else, and `backtest_run.py:4170-4175`
   names the evaluation phase's three reads without it. On the eToro corpus the same verdicts
   *are* honoured, via `price_series_break` / `price_segments` (settled-decisions,
   2026-09-15, clause 2). Here there is no equivalent layer.

Measured by this run: T3 fires on **1,766** transitions in this universe, of which **810 land
exactly on a stamped split bar** (T3 is keyed on the later bar and a stamp sits on the bar
that first prints the post-split level, so the join is equality). All **17,285** admitted
series carry a coverage row at the current rule-set version, across **38,721,505** evaluated
transitions — so the verdict counts sit against a fully evaluated population rather than
partial coverage. ⚠ A coverage row proves the series was evaluated at that version; it does
not prove every stamp date carries a usable transition, and version equality is not a
freshness check on the underlying bars.

⚠ **T3 firing on only 10.03% of stamped splits is largely the rule working, not a gap.** Its
trigger is on the **observed** ratio — `max(close/prev, prev/close) ≥ magnitude_threshold`
(`price_quarantine.py:478-495`) — and the research corpus is evaluated as `us_equity`
(`research_corpus_ingest.py::ASSET_CLASS`, handed to `evaluate_series` at `:1014`), whose
threshold is **5**, not the strict default of 2. So a 2-for-1, a 3-for-1 and every stock
dividend is well below it: only **2,261 of 8,073** stamped events carry a factor of that
size, and T3 fired on **806 (35.65%)** of them.

⚠⚠ **A large stamped factor is a PROXY for T3's predicate, not the predicate itself**, and
this run shows the proxy leaking in the expected direction: T3 also fired on **4** stamped
events outside that subset, where a real move pushed the observed ratio over the trigger. It
is therefore neither a reachability bound nor a sensitivity measurement — a stamped factor
whose price step is absent stays under the trigger no matter how large the stamp.

⚠ No claim is made about *why* the large-factor subset fires at 35.65% rather than higher.
Turnover admit-back, T1/T2 suppression, provisional deferral and an absent price step are all
candidates and separating them needs a measurement this one does not do. And **35.65% is an
intersection rate** — a stamp date coinciding with a T3 verdict establishes neither that the
verdict was caused by the split nor that the stamp is right.

**Consequence for the decision: there is no level-break layer behind the price basis on this
corpus.** ⚠ Not the same as "nothing else affects the outcome" — the bar-level quarantine
masks still apply, the `MIN_CLOSE` floor still gates eligibility, and a split only reaches a
score if it falls between that formation's two lookback endpoints. What is absent is the one
mechanism whose *purpose* is to stop a return being computed across a level change. So
"suppress and let containment handle it" is not an available option, because the containment
the rule names is not wired here. ⚠ Note also that no transition layer would help with
**unstamped** breaks, which this ticket does not address at all.

## 5. The verdict — `apply_all`, fixed by construction

**Apply every stamped factor uniformly. Do not condition the correction on the second
processing, in either direction.**

The construction, and why each rejected candidate is rejected:

1. **`suppress_all` is refused, and §3.5 is the strongest reason.** It is today's state, and
   it leaves **2,728 real reverse splits** uncorrected — 2,122 of them inside the window's
   reach — every one of which *inflates* a momentum score and therefore pushes the name
   *towards* the top decile. That is contaminating exposure an order of magnitude larger than
   any corrected arm's (158 in reach), and it is not contained downstream (§4). The
   displacement table agrees from the other direction: 14.30% of the decile separates it from
   `apply_all`, 13.85% from `apply_unless_refuted` and 5.16% from `apply_only_corroborated`.
   `b65abd9c` separately measured that applying the stamp moves agreement with a second
   processing from 5.89% to 90.90%.
2. **`apply_only_corroborated` is refused on bias, not on cost.** Its decile differs from
   `apply_all`'s by 9.51%, but the disqualifying fact is §3.3: it can only correct names the
   reference serves, and that population carries delisting evidence at 1.67% against 17.33%
   for the population it cannot reach. It would leave correction quality varying with a
   property correlated with survival, in the one corpus built to remove that correlation.
3. **`apply_unless_refuted` is refused, and this is a trade-off rather than a rout.** State it
   honestly:
   - *For it.* §3.5 bounds exactly what it prevents: at most the **158** in-reach refuted
     FORWARD stamps, out of 6,561 in-reach events — 2.4%. That is a real reduction in
     contaminating exposure and the displacement table's 0.50% cannot see it, because a set
     distance measures neither direction nor harm.
   - *Against it.* The suppression can only fire where the reference serves the name, so it
     removes part of that exposure on the served half and none of it on the other 47.69%.
     It buys an uneven correction rule rather than a uniformly better one.
   - *Against it.* Its discriminator's precision is unmeasured. **32 of the 383 refutations
     (8.36%) are the reference's own unadjusted basis** (§3.2), so the rule is already known
     to suppress correct stamps, and nothing establishes the rate at which it does so —
     #3280's lesson is that an adjudicator without a control arm has no error bar.
   - *Against it.* It makes the corpus depend continuously on a vendor our own ingest records
     as *"ONE observation … circular, never corroborating"*.

   The balance: it prevents at most 2.4% of in-reach events' worth of exposure, using a rule
   whose own error rate is unknown and whose reach is correlated with survival. ⚠ A frozen
   reference snapshot would answer the *dependency* objection but none of the other three, so
   that is not the reason it is refused.
4. **`re_date` is refused as a corpus-wide policy** — it reaches 39 measured events, only on
   the adjudicable half (§3.4). It is not ruled out as a later refinement.
5. **`quarantine` is refused.** It deletes 260 series to fix at most those same 383 disputed
   events, and it can only ever fire on the served half (§3.3). ⚠ It would also need a
   point-in-time rule that this document does not specify: deleting a whole series because of
   a dispute in 2019 removes its 1990s history from formations that predate the evidence.

**Frozen by construction.** The policy is a constant of the follow-on corpus change and must
be carried in its identity hash, not left implicit: no tolerance, no reference vendor and no
adjudication threshold appears anywhere in the applied rule. ⚠ `apply_all` is not *uniquely*
parameter-free — `suppress_all` has no adjudication parameter either — and freezing buys
reproducibility, not correctness. The narrower true claim: **among the candidates that
correct, `apply_all` is the only one with no free parameter**, which is what makes it
honestly freezable in the absence of a published rule.

### The residual, declared

`apply_all` applies 383 stamps that a second processing disputes. **Three different
denominators are in play and they must not be interchanged:**

- **383 of 8,042 = 4.76%** of all CHECKED events;
- **383 of 4,207 = 9.10%** of the COMPARABLE subset — the events the reference can actually
  adjudicate, which is the only population the rate is *about*;
- **351 of 4,207 = 8.34%** of that subset excluding the 32 where the reference is itself the
  unadjusted party. This is `b65abd9c`'s headline figure and the one to quote.

An earlier draft called 4.76% "a discrepancy rate on the overlap subset"; it is not — it is
the same numerator over the whole checked population. Any of the three is a discrepancy rate
at one tolerance against a non-independent processing. **None is a corpus defect rate and
none must be quoted as one.**

`b65abd9c` §3 decomposes the 383 — ~32 reference-unadjusted, ~188 where the factor is absent
from the price series, ~163 of the wrong magnitude — and notes they are evidence-shaped
buckets, not causes. §3.5 adds the direction: 228 of the 383 are forward stamps, 158 of those
inside the window's reach, and those are the ones that can contaminate rather than merely
miss.

⚠ The rate on the unadjudicable 47.69% is **unknown**, not assumed equal. `b65abd9c` §3's B1b
table shows the reference-absent half self-corroborating at a visibly lower rate at every
band (21.83% vs 27.05% at 1%), and that gap is confounded by era, factor mix, venue and
volatility. Nothing here licenses extrapolating any of the three rates onto the other half.

### What would reopen this

Any of the following, and the first is not the only one:

1. **A primary corporate-action source that clears a control arm** — one whose non-direct
   rate on *undisputed* events is low enough to make its disputed-arm rate interpretable
   (#3280's lesson). ⚠ "No non-circular adjudicator exists" overstates the evidence: one
   probe was run and failed. SEC XBRL split-ratio elements through `companyconcept` are
   measured and do not clear it, and ⛔ re-running that probe will not change the answer.
   Untested and NOT excluded: dimensional instance facts in the inline-XBRL documents, 8-K
   Item 5.03 narrative, custom filer tags, and any non-SEC primary record.
2. **A measurement of harm rather than exposure** — §3.5 counts events that *could*
   contaminate. A measurement of how many actually enter a decile, and with what return
   effect, could overturn the balance struck in §5 item 3 in either direction.
3. **A demonstrated implementation defect** in the applied rule, which needs no new source at
   all.

## 6. What the follow-on corpus change must now do

1. Store CSV fields 6 and 7 at ingest and re-load the vendor (`research_corpus_ingest.py:507`
   already names both columns and stores neither).
2. Derive the split-only series as `close / scale`, `scale(d)` = the product of the factors of
   every event strictly after `d`, applying **every** stamp per §5.
3. Decide what `MIN_CLOSE` reads on the corrected basis — §7 item 3, still open, and
   deliberately not pre-empted here. ⚠ Its own comment
   (`s2_cross_sectional_momentum.py:114-121`) carries the third instance of the cross-vendor
   error and must not be edited in place: `_source_hash()` hashes the whole module, so a
   docstring edit there is an identity rotation.
4. Mint a **new strategy id** under §4 rule 11 — identity is `code + config + data contract`
   and this changes the data contract. No inheritance of s2's track record.
5. Corpus rung: full-population A/B (`full-population-ab.md`) and Definition-of-Done clauses
   8–12.
6. ⚠ **Re-run the quarantine for the re-loaded series.** `RULE_SET_VERSION` is a hash of the
   rule module's source, so changing the underlying prices does **not** rotate it: verdicts
   stored against the old bars stay addressable at the same version and nothing makes the
   staleness visible. The ingest path already deletes and rewrites both verdict tables per
   series (`research_corpus_ingest.py:1049-1050`), so a full re-load handles it — but only if
   the re-load actually goes through that path.
7. **Settle the contracts `apply_all` needs and this document does not supply.** Each is a
   place where applying a factor uniformly can double-adjust or feed the rules inconsistent
   inputs, and none is decided here:
   - which basis the quarantine evaluates — raw or corrected. It currently reads raw, while
     the panel scores corrected, so a bar masked for a level break that the correction
     removes stays masked.
   - whether OHLC and `volume` are carried through the same scale as `close`. ⚠ `close ×
     volume` is split-invariant and T3's corroboration depends on that
     (`price_quarantine.py:420`), so scaling one without the other silently breaks the
     admit-back signal.
   - what field 7 (the cash dividend) is for, given §4 wants a price return.
   - **already-adjusted or mixed-basis segments.** `b65abd9c` §3 found 32 events where the
     *reference* is unadjusted; the converse — an Intrader segment already carrying an
     adjustment — is not excluded and would double-adjust under a uniform rule.
8. **Invariants the measurement does not assert and the corpus change must.** The script
   reads a mirror and trusts it; the writer cannot: positive finite factors, no duplicate
   stamps on one bar, non-overlapping scale intervals, and a scale row present wherever one
   is expected. ⚠ `COALESCE(scale, 1)` reads a *missing* scale exactly like "no later split",
   so absent coverage is silently indistinguishable from a correct no-op.

⚠ This does **not** unblock ARM B's prototype. That design remains withdrawn (60 ckpt-1
findings, 7 of which change it, carried in §7 of
`docs/proposals/ta/2026-09-21-armb-signal-basis-blocker.md`).

## 7. Correction to §4 rule 10, second clause

Rule 10's adjustment clause was corrected on 2026-09-21 (it is true of one vendor only). Its
**containment** clause needs the same treatment and for the same reason: *"`price_series_break`
segments (402 rows) are `not_evaluable`, never spanned"* is a statement about `price_daily`.
The break table is instrument-keyed and 70.10% of the admitted `survivorship_free` series have
no instrument row, so on the research corpus the clause covers nothing. Corrected in place.
