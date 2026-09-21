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
mean the same thing as the other three. It is reported as what it deletes (§3.3).
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

The first row reproduces `b65abd9c`'s 14.30% exactly, which is the control this table needs:
the panel is the predecessor's, re-expressed with the raw arm as `scale ≡ 1`.

Per decade, the two contrasts that decide the question:

| decade | formations | `apply` → `unless_refuted` | `apply` → `only_corroborated` |
| --- | ---: | ---: | ---: |
| 1990 | 71 | 0.59% | 9.50% |
| 2000 | 107 | 0.14% | 5.26% |
| 2010 | 116 | 0.47% | 11.10% |
| 2020 | 18 | 1.77% | 14.25% |

**Reading it.** Correcting at all is the large move. *Adjudicating* the disputes on top of
that is worth **0.50%** of the decile and never exceeds 1.77% in any decade. *Gating* on
corroboration is worth **9.51%** — two thirds of the entire correction — because it
suppresses the 47.55% of stamps the reference cannot speak to.

### 3.2 Not every refutation is about the stamp

Of the 383 refuted events, a subset are ones where the REFERENCE's own close carries the raw
step (CIVB 1996-05-09: both vendors print `81 → 20.25` on a 4:1, so the disagreement is the
reference's basis and not the factor). Measured on this run: **32 of the 383 refuted events
(8.36%)** are of that kind, reproducing `b65abd9c` §3's count at the same tolerance. Any
policy that suppresses on refutation suppresses those too, and they are the subset most
likely to be **correct**.

### 3.3 What `quarantine` deletes — and where its bias actually is

| population | series | carrying delisting evidence | rate |
| --- | ---: | ---: | ---: |
| all admitted | 17,285 | 1,010 | 5.84% |
| stamped, reference-checkable | 1,681 | 28 | **1.67%** |
| stamped, unadjudicable | 1,893 | 328 | **17.33%** |
| refuted → dropped by `quarantine` | 260 | 7 | 2.69% |

⚠ The two middle rows are **not a partition** — a series can carry both checkable and
unadjudicable events, so they overlap and do not sum to the 3,569 stamped series.

Quarantine would drop 260 series, 1.50% of the admitted universe. That cost is small. **The
bias is not in what it drops, it is in what it cannot reach.** A rule that fires on
refutation can only fire where a second processing exists, and the reference-checkable
population is **1.67%** delisted against **17.33%** for the unadjudicable population — a
ten-fold difference in the same direction every time. `research-price-corpus.md` already
records the mechanism: the reference vendor is 0/382 on the delisted cohort.

So a reference-conditioned policy treats the surviving half of the corpus differently from
the delisted half, systematically. That is the precise selection a survivorship-free corpus
exists to remove, re-entering through the correction step.

### 3.4 `re_date` is a rounding error

Of 383 refuted events, **39 (10.18%)** have a step within ±3 bars that matches the stamped
factor — 0.48% of checked events. ⚠ An upper bound on both sides: a real move of the same
size registers as a hit, and the probe only reaches events the reference can adjudicate at
all. Re-dating cannot be the policy; at most it is a later refinement of ≤39 events.

## 4. The damage is NOT contained elsewhere — and rule 10 says it is

`strategy-catalogue-and-backtest-validity.md` §4 rule 10 states the containment corpus-wide:
*"`price_series_break` segments (402 rows) are `not_evaluable`, never spanned."* An
uncorrected split IS a level break, so a reader can reasonably conclude the damage is already
contained and this whole decision is cosmetic. It is not, for two checkable reasons.

1. **`price_series_break` cannot key this corpus.** `sql/246:103` declares
   `instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id)`. The research corpus
   is keyed on `series_id` *precisely because* part of it has no `instruments` row — measured
   on the admitted universe, **12,116 of 17,285 series (70.10%) have none**. The clause is
   structurally unavailable to the majority of the universe it is quoted over.
2. **The research rule set DOES write the verdict, and nothing reads it.** T3 — *"this level
   break is not a return"* — is evaluated over this corpus and stored in
   `research_transition_quarantine` (`sql/251:111`). Its only readers anywhere in the
   repository are the ingest writer and the census view; `backtest_run.py:4170-4175` names
   the evaluation phase's three reads and that table is not among them. On the eToro corpus
   the same verdicts *are* honoured, via `price_series_break` / `price_segments`
   (settled-decisions, 2026-09-15, clause 2). Here there is no equivalent layer at all.

Measured: T3 fires on 1,766 transitions in this universe, of which **810 land exactly on a
stamped split bar** — and all 17,285 admitted series carry a coverage row at the current
rule-set version, over 38.7M evaluated transitions, so that is a clean denominator rather
than partial coverage.

⚠ **T3 seeing only 10.03% of stamped splits is mostly the rule working, not a gap.** Its
trigger is `|ratio| ≥ magnitude_threshold`, and the research corpus is evaluated as
`us_equity` (`research_corpus_ingest.py::ASSET_CLASS`, handed to `evaluate_series` at
`:1014`), whose threshold is **5** — not the strict default of 2. So **5,812 of 8,073 stamped
events (72.0%) are below the trigger by construction**: a 2-for-1, a 3-for-1 and every stock
dividend cannot fire T3 at all. Of the **2,261** events it can reach, it fired on **806
(35.65%)**.

⚠ No claim is made here about why the reachable subset fires at 35.65% rather than higher.
Turnover admit-back, T1/T2 suppression and a stamped factor whose price step is not actually
present are all candidates, and separating them needs a measurement this one does not do. The
figure is reported as a bound on how much of the damage the existing verdict even identifies
— which is the only thing the policy decision needs from it.

**Consequence for the decision: on this corpus the correction policy is the only thing
standing between a split and the momentum score.** "Suppress and let containment handle it"
is not an available option, because the containment it names is not wired here.

## 5. The verdict — `apply_all`, fixed by construction

**Apply every stamped factor uniformly. Do not condition the correction on the second
processing, in either direction.**

The construction, and why each rejected candidate is rejected:

1. **`suppress_all` is refused.** It is today's state, it leaves 8,073 raw level breaks in the
   scored series, nothing downstream contains them (§4), and it is 14.30% of the decile away
   from any corrected arm. `b65abd9c` already measured that applying the stamp moves agreement
   with a second processing from 5.89% to 90.90%.
2. **`apply_only_corroborated` is refused on bias, not on cost.** It costs 9.51% of the decile
   against `apply_all`, but the disqualifying fact is §3.3: it can only correct names the
   surviving-half vendor serves (1.67% delisted) and never the unadjudicable population
   (17.33% delisted). It would make correction quality a function of survival.
3. **`apply_unless_refuted` is refused as not worth its dependency.** It is the closest call
   in this document and the honest statement of it is that the difference is *small, not
   zero*: 0.50% of decile slots, 1.77% in its worst decade. Against that it buys a standing
   dependency on a vendor our own ingest records as *"ONE observation … circular, never
   corroborating"*, it inherits the same survivorship asymmetry in weaker form (it can only
   ever suppress on the served half), and it suppresses the §3.2 subset where the reference
   is the unadjusted party and the stamp is probably right.
4. **`re_date` is refused as unreachable** — ≤39 events, 0.48% of checked (§3.4).

**Frozen by construction.** The policy is a constant of the follow-on corpus change and must
be carried in its identity hash, not left implicit: no tolerance, no reference vendor and no
adjudication threshold appears anywhere in the applied rule. That is the property being
bought — `apply_all` is the only candidate with **no free parameter at all**, which is why it
can be frozen honestly in the absence of a published rule.

### The residual, declared

`apply_all` applies 383 stamps (4.76% of checked events) that a second processing disputes.
`b65abd9c` §3 decomposes them — ~32 where the reference is the unadjusted party, ~188 where
the factor is absent from the price series, ~163 of the wrong magnitude — and notes they are
evidence-shaped buckets, not causes. This is a discrepancy rate on the overlap subset at one
tolerance against a non-independent processing. **It is not a corpus defect rate and must not
be quoted as one.**

⚠ The rate on the unadjudicable 47.69% is **unknown**, not assumed equal. `b65abd9c` §3's B1b
table shows the reference-absent half self-corroborating at a visibly lower rate at every
band (21.83% vs 27.05% at 1%), and that gap is confounded by era, factor mix, venue and
volatility. Nothing here licenses extrapolating 4.76% onto the other half.

### What would reopen this

A **primary** corporate-action source that clears a control arm — i.e. one whose non-direct
rate on undisputed events is low enough that its disputed-arm rate is interpretable (#3280's
lesson). SEC XBRL through `companyconcept` is measured and does not clear it; ⛔ do not re-run
that probe. Untested and NOT excluded: dimensional instance facts in the inline-XBRL
documents, 8-K Item 5.03 narrative, custom filer tags.

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

⚠ This does **not** unblock ARM B's prototype. That design remains withdrawn (60 ckpt-1
findings, 7 of which change it, carried in §7 of
`docs/proposals/ta/2026-09-21-armb-signal-basis-blocker.md`).

## 7. Correction to §4 rule 10, second clause

Rule 10's adjustment clause was corrected on 2026-09-21 (it is true of one vendor only). Its
**containment** clause needs the same treatment and for the same reason: *"`price_series_break`
segments (402 rows) are `not_evaluable`, never spanned"* is a statement about `price_daily`.
The break table is instrument-keyed and 70.10% of the admitted `survivorship_free` series have
no instrument row, so on the research corpus the clause covers nothing. Corrected in place.
