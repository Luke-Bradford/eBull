# Full-population A/B verification

## When to use

Before claiming any parser, ETL, scoring or metric change is safe. Mandatory
whenever the change alters what gets extracted, selected, scored or keyed —
i.e. any change whose blast radius is "the corpus", not "this function".

Written from #2140 (14 defects, 8 rounds) and #2158 (4 elements, 3 rounds).
Every rule below is a round somebody actually burned.

## The one rule that matters

**A 3-5 item panel is not evidence.** #2140 ran eight full-population rounds;
the panel was green on all seven that still contained real defects. 8 of its 14
defects were visible only full-population. If you find yourself writing "spot-
checked on AAPL/GME/MSFT and it looks right", you have not verified anything.

**This does not contradict Definition-of-Done clause 8** (`CLAUDE.md`), which requires
smoking the AAPL/GME/MSFT/JPM/HD panel. The two answer different questions and both are
required on a corpus change:

| | question it answers | what a green result proves |
| --- | --- | --- |
| **Golden panel (DoD 8-11)** | does the operator-visible figure still RENDER, end to end, after the backfill? | the read path works on known-good instruments |
| **Full-population A/B** | is the change SAFE across the corpus? | nothing was silently lost or admitted at scale |

A green panel with no A/B means "it renders" and says nothing about safety. An A/B with
no panel means "the numbers moved as intended" and says nothing about whether the chart
still draws. Neither substitutes for the other — and a panel too small to carry a rate
also cannot falsify a documented caveat (see below).

## A documented caveat is a hypothesis about FREQUENCY — measure the rate

The most expensive failure of 2026-08-03 was caused by a skill being right.

`metrics-analyst` documents: *"Treasury IS allowed to push chart 'oversubscribed'
if Σ pie_wedges + treasury > shares_outstanding (stale-13F + fresh Form 4 mix);
residual clamps to zero, banner flags it."* True as written. So when the golden
panel returned `oversubscribed=true` for JPM and HD, I read it as the documented
condition and moved on. **The caveat functioned as a blindfold.**

Sampling 20 large caps instead of 5 returned **9 oversubscribed** — 45%. A
condition that fires on nearly half the population is not an edge case, and that
rate was the tell. The actual cause was arithmetic: `residual` subtracted treasury
from `shares_outstanding`, a base that already excludes it (shares issued = shares
outstanding + treasury). GS carries treasury equal to 199.9% of outstanding, so
`raw` was negative before any holder was counted. Public float rendered as ZERO
for Coca-Cola, Goldman, JPM, P&G and Exxon (#2217).

The rule:

- A caveat tells you a condition CAN occur. It does not tell you how often, and
  the author usually did not measure. **"Expected" at 2% is a caveat; the same
  condition at 45% is a defect wearing the caveat as cover.**
- When you meet a documented-expected condition in live data, spend the one query
  it costs to get its RATE across the population before accepting it.
- The dangerous shape is specifically: a caveat that explains an observation you
  would otherwise have investigated. That is where it earns its cost.
- Corollary to "a 3-5 item panel is not evidence": the panel is not evidence for
  operator-visible METRIC correctness either, not just for parsers. Two of five is
  a shrug; nine of twenty is an investigation. **A panel too small to carry a rate
  cannot falsify a caveat.**

Cheapest form of this check: loop the live endpoint over ~20 names and count. It
took under a minute and it found the largest defect of the session.

## Choosing the metric

**Row count is the wrong metric, and it is wrong in both directions.**

- A row-count *drop* is usually the old code losing **garbage** — an Item 402
  award table, a table-of-contents block, footnote prose. Chasing the count
  back up re-admits it.
- A drop is also **dedup working**, which is often the point of the change.
- A row-count *rise* can be one real table replaced by two copies of a
  breakdown table.

Measure **distinct entities lost and gained** — holders, facts, positions —
keyed exactly as the database keys them, so "lost" means the same thing it
means in the read path. For DEF 14A that is `lower(trim(holder_name))`, matching
the `holder_name_key` generated column.

**Enumerate; never average.** Print every accession that loses an entity and
classify each one by hand. "Net +11,000" hides "and 101 real holders vanished".

**Inspect the GAIN side too.** #2140's last real defect — address fragments
parsed as holders — appeared only among the gains. A change that only adds
looks safe and isn't.

### For a READ-TIME metric, latency is a second metric — measure it

Issue #2229. A metric computed at read time and stored nowhere (the ownership
rollup: `metrics-analyst` says "Storage: not stored") runs its every predicate
on **every operator request**. The correctness arms above ask whether the FIGURE
changed; they are silent on what it now costs, and so is every other gate —
lint, typecheck, the test suite, the review bot.

That change's supersession predicate was correct and made the endpoint **11-20x
slower**: AAPL 214ms → 2,390ms, HD 180ms → 3,675ms.

So time control vs treatment on the real path before pushing. It is the same
`git show origin/main:<path>` control extraction the paired design already uses
— just `perf_counter` around each arm instead of diffing the figure. Doing it
surfaced three things no correctness arm could:

1. A subquery scoped to the **instrument** was being re-evaluated per **row**;
   hoisting it to a CTE was behaviour-preserving (identical counts) and cut
   3,675ms → 1,449ms.
2. The better data source measured **3x worse** until it had a covering index.
3. Therefore: **a missing index can make you reject the correct source.**
   `idx_inst_current_filer (filer_cik)` alone made `MAX(period_end)` heap-fetch
   every row of a filer like Vanguard — 7,651ms, against 25ms with
   `(filer_cik, period_end DESC)`. Measure the source and its index together,
   or the index decision silently becomes a data-model decision.

## Three arms, because one cannot see what the others see

**Arm 1 — parse vs parse.** `main` in a git worktree vs the branch. Catches
what changed.

**Arm 2 — parse vs STORED.** Arm 1 is blind to items where **both** sides
return nothing: the diff is empty, so they never appear. Those are exactly the
items a safety guard is protecting, and their stored rows survive only because
nothing has force-rewashed them. Only a comparison against the persisted table
sees them. #2158's entire ticket lived in this blind spot — 181 accessions.

Pass **both** summaries so the arm can state the regression invariant that
matters: *entities `main` reproduced and the branch does not*. A single-summary
form cannot express it.

**Arm 3 — mechanism audit.** Whatever internal decision the change moves
(a score, a selection, a key), enumerate it across the corpus with before/after
values. Aggregate counters are not enough — see the next section.

## Directionality: count what the change ADMITS, not only what it rejects

When a change **widens** what a selector or scorer can see, its failure mode is
**admitting things it previously could not see**. A guard tuned against the old
input has no coverage of them.

Issue #2158's audit reported "0 shapes where the fold lowers the score, 0 newly
disqualified" and looked clean. Both counters were the wrong direction. The real
hazard was Item 402(g) tables **newly clearing the floor** — 47 shapes in a
300-item smoke, and it emitted vesting data as beneficial ownership.

Before running the audit, write down what the change could newly admit, and
count that.

## A census over a SPARSE table has THREE clauses, not two

The usual check is "does the query implement the rule". When the figure is
counted over a table whose rows are **conditionally written** — quarantine and
exception tables, `*_observations` written only on change, audit rows written
only on failure — a third artefact sits between them: the predicate deciding
what gets stored. The rule can be right, the query can be right, they can agree
with each other, and the number can still be wrong, because the rows were never
written.

#2261 hit this twice in one PR:

- T3-ADMITTED transitions were not marked notable, so they were never stored,
  so the corroboration census read `spike: 1` where the true count was **34**.
  **A narrowing gate's denominator is the set it ADMITTED** — publishing only
  the rejected side makes the gate unmeasurable while every stored number stays
  internally consistent.
- `provisional` alone made a transition notable, storing every ordinary
  transition in the trailing correction window, so a figure named
  `..._provisional_deferred` reported **16,907** against a rule that yields
  **3**.

So state the storage predicate alongside the rule, and for every published
figure with a name, answer in one line: *what is excluded from this count, and
is that exclusion the one the name implies?* Then assert the figure against the
number your write-up claims **before** writing the write-up — both gaps above
were sitting in plain sight in the census output.

## Harness integrity — a clean result is the suspicious one

**Never simulate the control arm.** If the harness reconstructs "what `main`
would have done" instead of running `main`, it inherits every branch of the
condition it is reconstructing. #2158's audit reproduced only *half* the arm
test (keyword condition, not the width condition), so it reported **zero**
Item 402(c) drift where the real two-checkout A/B showed 75 items gaining rows.
Had the change been harmful, the harness would have concealed it just as well.

Corollaries:

- **Prefer two real checkouts.** When a simulated control and a real A/B
  disagree, the real A/B is the authority.
- **Treat a clean result from a simulated control with more suspicion than a
  dirty one** — a harness bug and a genuine no-op are indistinguishable at the
  output.
- **Reconcile aggregate counters against per-item lists.** A totals line
  (`sct_rows 66,445 -> 67,442`) contradicting a per-item drift list of zero is
  the thread that unpicks the harness bug. Read them together, never separately.
- **Never default a missing metric key to empty.** `summary.get("holders", {})`
  makes the section report zero lost *and* zero gained — indistinguishable from
  a clean run. Raise instead.
- **The census must be the rule you wrote down.** Before publishing, re-read the
  query against the rule table clause by clause: same predicate, same threshold,
  same downstream consumer, for every named rule. A rejection census is plausible
  at almost any magnitude, so nothing in the output fires when they diverge.
  Highest risk when the rule is being *refined mid-analysis* — the prose moves
  first and the SQL is left behind. S7 (#2247) published a wick-rule count computed
  by the raw-ratio rule it had just argued was wrong, in the same document.
- **One verdict class, one column.** If the artefact declares more than one kind of
  usability (usable-for-returns vs usable-for-intrabar-touch), a single boolean is
  the tell that the split exists only in the prose. S7 OR-ed range-only defects into
  the return quarantine and over-rejected by 587 windows. Where the rules will ship,
  express them once as a pure function and have both the census and production call
  it, so the divergence cannot reopen.
- **`NOT (col > 0 AND …)` is NULL, not TRUE, when `col IS NULL`.** Used as an
  exclusion in a `CASE`/`WHERE`, every NULL-`col` row falls through to the other
  branch — so a corroboration gate admits precisely the population it cannot verify.
  `coalesce(…, FALSE)` every three-valued predicate. The tell is a downstream count
  disagreeing with the census, not the flag itself.

## Arm 3 must cover fields the harness does NOT key on

Arms 1 and 2 key on the ENTITY identity — for DEF 14A, `holder_name`. Any
other column the change can move is invisible to both, and the diff will look
clean while the column is wrong.

Issue #2164 changed a role-heading regex. Arms 1 and 2 were green; a separate
role audit — re-parsing BOTH trees and diffing `{accession: {name: role}}` —
found **40 holders whose `holder_role` regressed** from `principal` to `None`.
Nothing in the name-keyed arms could have shown it.

Before running, list every column the change can write, and confirm the harness
keys on each one or that an arm-3 audit covers it. Roles, flags, denominator
bases, source tags and `as_of` dates are the usual blind spots.

The audit is a real two-checkout run like the others. Do not reconstruct the
control's roles from the branch's output.

## Normalise identity BEFORE calling anything "lost"

When the change deliberately alters the identity key, the raw diff reports the
whole re-keyed population as loss, and it looks catastrophic.

Issue #2164's arm 1 reported **606 distinct holders lost**. Every one was a re-key:
the fix strips zero-width characters and footnote markers from `holder_name`,
and `holder_name_key` is `lower(trim(...))` — `trim()` removes neither. After
normalising BOTH sides by the same two transforms, genuine loss was **0**.

So: apply the transforms the change intends, to both arms, then diff. Whatever
still disappears is the real regression list. State the normalisation explicitly
in the PR — "0 lost after normalising away zero-width and footnote markers" is a
different and much stronger claim than "606 lost, but they're fine".

The same run had **624** arm-2 non-reproductions, also all re-keys. Verify that
programmatically over the whole set; eyeballing the first 20 is not verification.

## Self-inflicted defects are normal — budget for them

3 of #2140's 14 defects, and 2 of #2158's 4 elements, were defects created by an
earlier fix **in the same PR**. Expect this. Fix them in the same PR rather than
shipping the widening and filing the fallout; each round is cheap compared with
a bad merge reaching stored data.

Budget for multiple rounds on any real parser change: every round that FINDS something,
or that changes the harness, invalidates the previous one and needs a re-run. A single
clean round is sufficient only when the harness demonstrably covers all three arms and
the blind spots listed above — it is the harness coverage that justifies stopping, not
the round count.

Issue #2164 ran three rounds and found a real defect in each of the first two — 35
genuine holders lost in round 1, 40 role regressions in round 2. **Codex, 114
unit tests and the 5-filing panel all passed the round-1 design.** Treat a clean
Codex review as weak evidence about corpus behaviour; it reads the diff, not the
data.

## Before the backfill

A new `WHERE` predicate in a per-row chokepoint needs an index check **before**
you start the corpus run, not after. #2157's instrument-set lookup filtered on
`source_document_id` alone, no index led with it, and the backfill ran at ~28
items/min against a ~145/min baseline — a sequential scan of 111,867 rows per
item. With the index: ~300/min. `EXPLAIN` the new query once; it costs seconds.

## Mechanics

- Offline wherever raw payloads are stored — re-parse from
  `filing_raw_documents`, never re-fetch. No rate-limit drain, and the two sides
  see byte-identical input.
- Run both sides concurrently; a git worktree at `origin/main` gives the control
  side without disturbing the dev checkout.
- ⚠ **If either side reads the LIVE dev DB, two sequential runs are not an A/B.**
  The jobs daemon keeps ingesting underneath you, so the arms see different
  inputs and the diff is measuring ingest, not your change. #2217 ran control
  and treatment as two ~4-minute processes 15 minutes apart; **2,175
  `ownership_institutions_current` rows were rewritten between them**, and the
  diff reported four invariant failures — including zero-treasury control
  instruments moving, and `concentration` changing on 465 instruments, which
  that diff could not touch.

  **The tell is an invariant failing on a quantity your change cannot affect.**
  When you see one, suspect the harness before the fix.

  Fix by **pairing**: one process, and for each item evaluate BOTH arms inside a
  single `snapshot_read` (REPEATABLE READ) from one set of inputs, so nothing
  can land between them. That is not "simulating the control" — extract the
  control function's body verbatim from `git show origin/main:<path>` and `exec`
  it, then invoke it on the same arguments the live path built. Both arms are
  real code; only the contamination is gone. Re-run paired, #2217's four
  invariants all passed and the controls were byte-identical.

  Offline re-parse from stored payloads (above) is immune to this — prefer it
  when the data allows. Pairing is for metrics computed at read time, which have
  no stored artefact to re-parse.
- **Aggregate the arms over the SAME entity set — the intersection, not each
  arm's own.** A per-arm median (or mean, or rate) is computed over whatever
  entities that arm happened to produce a value for, and the arms rarely produce
  values for the same ones: the treatment arm usually covers MORE, and the extra
  entities are systematically the marginal ones. Comparing arm-A-over-its-set
  against arm-B-over-its-set silently compares two different populations.

  **The tell is a result that is physically impossible.** #2252 measured
  inter-update gaps for control / treatment / a ground-truth "wire" arm, each
  over its own instruments: the treatment arm came out *faster than the wire it
  was reading from*, which cannot happen. Nothing was wrong with the fix — the
  medians were over 74 / 84 / 105 instruments respectively. Restricted to the
  121 instruments measurable in all three arms, the numbers were clean and the
  treatment tracked the wire exactly (1.00x).

  So: compute the intersection first, state its size next to the result
  (`121 of 184`), and report what fell out and why. An arm that covers more
  entities is itself a finding — report it as coverage, never let it leak into
  a rate comparison. Same family as "distinct entities, never row count": the
  denominator is the thing most likely to be quietly wrong.
- Long runs need `run_in_background: true` — a `nohup … &` started inside an
  ordinary tool call is killed when that call's process group is cleaned up.
- Reference implementation: `scripts/ab_2140_def14a_parser.py`
  (`--out` / `--diff` / `--stored MAIN BRANCH` / `--audit`).

## A RATIO metric needs a median, not a mean — and the mean fails loudly enough to look like a data defect

Any arm that compares two sources by a ratio (`ours / theirs`, level gap, scale factor) is
on a multiplicative scale, where one mis-levelled entity at 30x moves a mean across
thousands of entities by more than the effect you are measuring.

Precedent (#2282 2b, 2026-08-05). The cross-source guard reported:

```
mean   level gap (research/eToro - 1) : +0.367     <- looks like a failed adjustment basis
median level ratio                    :  1.00203   <- the half-spread the reference predicts
```

The **statistic was wrong, not the data**. 87.5% of 5,174 instruments sat within ±1% of
parity; a long tail of reverse-split epoch mismatches dragged the mean. Half an hour went
into diagnosing a corpus that was correct.

Rules:

- **Report `percentile_cont(0.5)` for any ratio, and per-entity before pooling.** Take the
  median within an entity, then the median across entities — a mean of per-entity means
  reintroduces the same problem one level up.
- **Log-space the thresholds.** `abs(ln(ratio)) <= 0.01` is symmetric; `ratio - 1` is not,
  and a 2x and a 0.5x error should score the same.
- ⚠ **`ln()` raises a domain error on zero and negatives**, so guard with
  `ln(nullif(greatest(ratio, 0), 0))`. Guard it there rather than adding
  `WHERE close > 0` upstream, which silently narrows the population — on a price corpus
  that predicate is a survivorship filter wearing a data-quality hat.
- **Report the tail's SHAPE, not just its size.** Splitting #2282's 686-instrument tail
  into "returns agree, level offset" vs "returns disagree" separated a benign snapshot-epoch
  artefact from a real `price_daily` defect (#2293). A single "686 failures" number would
  have been actioned as one problem.
