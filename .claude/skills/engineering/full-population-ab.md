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

#2158's audit reported "0 shapes where the fold lowers the score, 0 newly
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

## Self-inflicted defects are normal — budget for them

3 of #2140's 14 defects, and 2 of #2158's 4 elements, were defects created by an
earlier fix **in the same PR**. Expect this. Fix them in the same PR rather than
shipping the widening and filing the fallout; each round is cheap compared with
a bad merge reaching stored data.

Plan for 3+ rounds on any real parser change. One round means you have not
looked hard enough.

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
