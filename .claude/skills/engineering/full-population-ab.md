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

Plan for 3+ rounds on any real parser change. One round means you have not
looked hard enough.

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
- Long runs need `run_in_background: true` — a `nohup … &` started inside an
  ordinary tool call is killed when that call's process group is cleaned up.
- Reference implementation: `scripts/ab_2140_def14a_parser.py`
  (`--out` / `--diff` / `--stored MAIN BRANCH` / `--audit`).
