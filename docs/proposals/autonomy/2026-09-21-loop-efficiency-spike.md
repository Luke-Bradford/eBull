# Spike: making the autonomy loop hands-off, long-running and affordable

Written 2026-09-21 against `origin/main`, and **revised after a Codex adversarial pass that
found two arithmetic errors and a self-contradiction in the first draft** (both corrected
below; the pass is summarised in §7).

MEASURED figures come from `~/.codex/sessions/**`, `var/autonomy/iteration-*.log` and
`gh pr list`, all over ONE window: **2026-09-18T18:00Z → 2026-09-21T09:00Z**. DERIVED
figures (byte→token conversions, per-turn re-read estimates) are labelled as such — the
first draft called everything "measured", which was wrong.
The question is the operator's: *can this loop run unattended and efficiently, and what in
our written structure is stopping it?*

## 1. What the weekend actually cost

| measure (window above) | value |
| --- | ---: |
| iterations | 45 |
| PRs merged | 67 |
| loop spend | **$1,190.35** |
| mean cost / iteration | **$26.45** (max $112.15) |
| median tool calls / iteration | **346** (max 1,072) |
| median assistant turns / iteration | **177** |
| cache-read tokens | **1.78 billion** (39.6M / iteration) |
| uncached input tokens | 14,338 |
| output tokens | 6.07M |
| Codex sessions | **278** (~6.2 / iteration), 206M input tokens |
| top 20% of iterations by cost | **32% of total spend** |

⚠ **Corrections to the first draft.** It reported $1,596.84 / 2.43B over "46 iterations":
the dollar and token figures covered 56 iterations (all of 09-18) while the count covered
the post-18:00 window. One window, consistently applied, gives the table above. It also
said "110KB of instructions", which included this project's 17.8KB session-memory index —
a file the LOOP never reads. The loop-relevant total is **92.2KB**.

⚠ **What the traffic split does and does not prove.** Output is 0.3% of tokens, and
cache-reads dominate volume. That establishes *volume*, not *dollar share*: cached reads
are billed at a fraction of uncached input, so a spend breakdown by rate — which this
session cannot obtain from the transcripts — would be needed to prove they dominate cost.
What IS supported: uncached input is 14,338 tokens total, so essentially the entire
context is being served from cache and re-charged at the cache-read rate on every one of
~177 turns. Concentration is real but moderate: the top 20% of iterations carry 32% of
spend, not the overwhelming majority the first draft implied.

## 2. Where the work went, against the priority the operator set

| area | PRs | share |
| --- | ---: | ---: |
| research / backtest (#2840, #2508, #3238) | 32 | 48% |
| job health + #3189 outage findings | 15 | 22% |
| ownership / insider (a PARKED domain) | 9 | 13% |
| **core sleeve / execution (the stated priority)** | **9** | **13%** |
| other | 2 | 3% |

27 of the 67 PRs (40%) changed no `app/`, `sql/` or `frontend/` file at all; **12 were
standalone `docs(...)` prevention-log PRs.**

## 3. Five structural faults, in order of cost

### 3.1 The queue's head is unreachable by an unattended loop — this is the root cause

Iteration 155's own words: *"The whole named queue is now wall-clock or attended-gated.
The binding constraint on the largest block (#2961 window B, #2965 partial fill, #2942
slice C, #2993, #3007) is one attended demo session."*

The loop is forbidden to mutate broker state (`hard_rules.md`, and the prompt's
`loop-ineligible` list). Every remaining execution ticket's acceptance IS a broker
mutation. So the loop cannot finish the work the operator most wants, correctly refuses
to fake it, and falls through to the board — which is exactly the 48% research share
above. **The drift is not indiscipline; it is the queue design meeting a safety rule.**

Until this is fixed, every instruction to "focus on auto-trading" produces more research
PRs, because focus cannot create an eligible ticket.

### 3.2 The loop prompt mandates the full heavy workflow for every ticket, contradicting CLAUDE.md's own ladder

`loop_prompt.md:199-206` states one unconditional chain: *research → falsify → spec →
Codex ckpt-1 → implement → gates → Codex ckpt-2 → PR → poll → resolve → merge*.

`.claude/CLAUDE.md`'s review-intensity ladder says the opposite for small work: a narrow
mechanical diff gets *"self-review + the deterministic pre-push hook + the review bot.
**Nothing else.** No second-opinion pass"*, and checkpoint 2 fires only at the
"behavioural change with data semantics" rung or above.

The loop follows the prompt, so **every ticket buys a spec, a spec review and a diff
review** — 6.0 Codex sessions per iteration, and the spec reviews are the expensive ones
(1.3–2.0M input tokens each, because they say "also read `app/services/…`").

### 3.3 Prevention-log extraction is shipped as its own PR

`.claude/CLAUDE.md` is explicit: *"extract it into the relevant skill AND
`docs/review-prevention-log.md` in the SAME PR — never let the lesson live only in the PR
description."* The loop instead ships `fix(#N)` then `docs(#N)`. 12 of 67 weekend PRs were
that second PR. Each pays a full branch → pre-push gate → CI → bot review → merge cycle
for a documentation edit that the first PR was supposed to carry.

### 3.4 Nothing bounds an iteration

There is no cap on tickets per iteration, tool calls, or turns. The distribution is wide —
$7 to $112, 106 to 1,072 tool calls — and the tail is where the money goes. An iteration
that finishes a ticket and then starts another inherits the whole first context.

### 3.5 110KB of static instructions are re-read on every turn

`.claude/CLAUDE.md` 58.6KB + `loop_prompt.md` 26.4KB + the global `CLAUDE.md` 7.2KB ≈ 92KB
(~23k tokens) before any repo file is opened. At 177 turns that is ~4M tokens per
iteration of pure instruction re-reading, ~10% of the cache-read total. Much of both files
is historical narrative — *why* a rule exists, at paragraph length — which is valuable to
a human reader once and re-bought by the loop every turn.

## 4. Proposed changes

Ordered by expected saving per unit of disruption. None of them weakens a gate.

### P1 — REJECTED as first drafted. Batch the attended window instead.

The first draft proposed a bounded "demo acceptance runner": the loop queues a scenario, a
scheduled runner executes it against the demo account. **A Codex pass killed it, and it was
right.** Recorded here rather than quietly dropped, because the idea is attractive and will
be proposed again.

> *"P1 launders the boundary through delegation. 'The loop never invokes it' is irrelevant
> when the loop controls what gets executed."*

The escalation paths it named, none of which the caps address:

- **queue → privileged deputy** — the request causes a credentialed executor to trade on the loop's behalf;
- **scheduler → invocation** — the loop can write a scheduler entry that calls the runner directly, repeatedly, or through a wrapper;
- **code → bot approval → execution** — the loop can modify the runner, its dependencies or its deployment, get a bot APPROVE, and have the scheduled process run the modified code with broker authority;
- **configuration → broader authority** — `settings.etoro_env == "demo"` proves nothing if the loop can change the setting or the code that checks it;
- **repeated requests → unbounded activity** — "ONE scenario, $50" without independent aggregate limits and replay protection is a per-request cap, not a total;
- **ownership records → unauthorised closing** — if the runner trusts a writable ownership ledger, "only close what I opened" is not a constraint.

The claim in §5 that "no gate is weakened" was **false** for P1, and is withdrawn.

**What replaces it — batched attended windows.** Keep human authorisation of each concrete
execution, and make it cheap instead of frequent:

1. The loop PREPARES each blocked ticket to the point of execution: the exact script, the
   exact command line, the expected evidence, and the acceptance query — then parks with a
   wake condition naming the scenario.
2. Those accumulate. One attended session (operator, or the supervising session with the
   operator's consent) runs the whole batch in a single window and posts the raw evidence
   back to each issue.
3. The loop wakes on that evidence and finishes the tickets.

Five blocked tickets then cost ONE short window rather than five, and every other hour of
the loop's week stays hands-off. This is a scheduling change, not a boundary change.

**If the operator ever wants genuine unattended acceptance**, Codex named the only shape
that could be safe: an independently administered executor — demo-only credentials with no
live authority, immutable code/config/scheduler/ownership ledger, no deployment path
through bot-approved PRs, independent aggregate limits and replay protection, and no loop
authority over any of it. That is a **settled-decision reversal of `hard_rules.md`** and an
explicit operator policy change. It is not this spike's to make, and it should not be
smuggled in as an implementation detail.

### P0 — When the priority queue has no eligible ticket, BACK OFF instead of falling through (Codex's best catch)

The cheapest work is the work not done. Today a blocked queue sends the loop to the board,
which is how 48% of weekend PRs became research nobody asked for — each costing a full
$26 iteration. Instead: when no queue ticket is eligible, the loop should post a one-line
blocked-state note, **sleep to the next scheduled wake, and spend nothing**, rather than
finding something to do.

⚠ This needs a WAKE CONDITION per parked ticket, or the loop simply re-discovers the same
block every cycle and pays the startup cost to learn it: "#2965 — blocked on demo
acceptance scenario B; wake when the scenario result is posted on the issue".

### P2 — Make the loop obey the ladder it already has (fixes 3.2)

⚠ **The first draft got this wrong and Codex caught it:** it proposed "spec + ckpt-2 only"
for behavioural work, which contradicts the cited policy — checkpoint 1 is UNCONDITIONAL
for a judgement artefact, and a spec IS one. Corrected:

- **narrow/mechanical diff** (1–2 files, no data semantics) → **no spec at all**, so no
  ckpt-1 to skip; self-review + pre-push hook + review bot. This is where the saving comes
  from: not writing a spec, rather than writing one and skipping its review.
- **a spec or plan is written** → **ckpt-1 on it, always.** Unchanged.
- **behavioural change with data semantics, or corpus/parser/schema/metric** → ckpt-2
  before first push, plus the domain-skill read, full-population A/B and the
  Definition-of-Done evidence table, all unchanged.
- **rebuttal-only round** → ckpt-3, unchanged.

⚠ File count is a weak risk proxy (a one-line authorisation edit is consequential), so the
rung is decided by WHAT THE DIFF TOUCHES, not by how many files it spans.

And bind the prompt shape: **a ckpt-1 prompt names exact files and line ranges**, never a
directory or an open-ended "also read".

Expected: Codex sessions fall from ~6.0/iteration to ~1.5–2.0, and the remaining ones are
the cheap kind.

### P3 — Bound an iteration by BUDGET with a safe stop, not by ticket count (revised)

The first draft said "one ticket per iteration". Codex's objection is sound: ticket count
bounds nothing (one ticket already reached 1,072 calls), it makes large tickets a loophole
and small ones disproportionately expensive, and a reset converts retained context into
re-discovery — a cost this spike has not measured.

Bound the thing that actually varies instead:

- a **per-iteration budget** (tool calls and/or dollars) with a **safe stop**: at the
  threshold, finish the current PR step, write the handoff note, end the iteration;
- a **daily spend ceiling** for the loop as a whole;
- continue to a SECOND ticket only when the first merged and the context is still small.

⚠ A reset without a durable handoff loses negative findings — "I tried X, it does not
work" — which the next iteration then re-derives. The handoff note is the mitigation and
it is mandatory, not optional.

### P4 — Prevention-log lessons ship inside their PR (fixes 3.3)

Delete the separate `docs(...)` PR. If the bot raises a PREVENTION comment after the code
PR merged, batch those into ONE weekly docs PR rather than one per ticket.

### P5 — Split the instruction files into a hot core and cold precedent (fixes 3.5)

Keep the RULES in `loop_prompt.md` and `.claude/CLAUDE.md`; move the multi-paragraph
precedent narratives (the "⚠ Precedent, 2026-08-08…" blocks) into
`docs/autonomy/precedents.md`, referenced by one line each. Target: loop prompt under
10KB, CLAUDE.md under 25KB. The rules keep their force; the stories stop being re-bought
177 times per iteration.

⚠ Do this one CAREFULLY and last: several of those narratives are the only record of why a
rule exists, and a rule whose reason is lost gets "simplified" away by a later session.
Moving them is fine; deleting them is not.

## 5. What this does NOT propose

- **No weakening of any gate** — with P1 withdrawn, this claim now holds. It did not hold
  for P1 as first drafted, and §4 records why.
- **No weakening of any gate (detail).** The pre-push hook, full-population A/B, the review bot, the
  resolution contract, safe_merge and the kill switch all stay exactly as they are.
- **No cheaper model for judgement work.** Codex checkpoints 1 and 3 stay on `gpt-6-astra`
  at high effort; the saving comes from firing them less often, not from thinking less
  about the things that matter.
- **No change to the sandbox bound or the live-capital position.** This is demo only.

## 6. Expected effect

If P2 + P3 + P4 land, the median iteration should fall from 346 tool calls and $27 toward
a bounded ~150 calls and ~$10–12, with Codex spend down roughly two-thirds — without
removing a single deterministic check. P1 does not save money; it is the one that makes
the remaining spend buy the thing the operator actually asked for.

The risk to weigh is P3: a hard one-ticket stop could fragment work that genuinely needs
two phases (a schema PR then its consumer). The mitigation is that the SECOND phase is a
fresh ticket with a fresh context, which is cheaper than carrying the first one's.


## 7. The Codex pass, and what it changed

Run 2026-09-21 on `gpt-6-astra` at high effort, asked to attack the framing rather than
agree. It produced two arithmetic corrections, one self-contradiction, one killed proposal
and one lever the spike had missed. Recorded because the misses are the useful part.

| finding | disposition |
| --- | --- |
| P1 launders the broker boundary through a deputy | **ACCEPTED — P1 withdrawn and rewritten** (§4) |
| "$1,596.84 / 2.43B over 46 iterations" mixes two windows | **ACCEPTED — recomputed on one window**: $1,190.35 / 1.78B / 45 |
| "110KB of instructions" is 92.2KB; the rest is a memory index the loop never reads | **ACCEPTED — corrected** |
| P2 dropped ckpt-1 for behavioural work, contradicting the policy it cites | **ACCEPTED — corrected**: the saving is not writing a spec for narrow work, never skipping a written spec's review |
| Ticket-count capping bounds nothing; resets can cost more than they save | **ACCEPTED — P3 rebuilt** around a budget + safe stop + mandatory handoff |
| Cheapest missed lever: back off when priority work is blocked, rather than falling through to the board | **ACCEPTED — added as P0**, and it is the best value in the document |
| Traffic share ≠ dollar share; cache reads are billed differently | **ACCEPTED — claim narrowed**; the supportable statement is that uncached input is 14,338 tokens, so the whole context is re-charged at the cache-read rate every turn |
| "The tail is where the money goes" | **PARTLY REBUTTED — measured**: top 20% of iterations = 32% of spend. Real but moderate; wording corrected |
| PR count is not effort, spend or utility; the 48%/13% split proves less than claimed | **ACCEPTED as a caveat.** The split still shows where OUTPUT went, which is what the operator asked about; it is not a cost attribution and no longer claims to be |
| P5 may keep a rule's words and lose the knowledge to apply it | **ACCEPTED — P5 demoted to last, and gated**: extract the operative exceptions INTO the rule first, with retrieval triggers, before any byte target |
| No overall spend ceiling is specified anywhere | **ACCEPTED — folded into P3** as a daily ceiling |
| Success should be measured per accepted priority outcome, not per cheap iteration | **ACCEPTED — see §8** |

## 8. How to tell whether this worked

Not "iterations got cheaper" — that is satisfiable by doing less useful work. The measure
is **cost per merged PR on a queue-priority ticket**, tracked weekly, alongside:

- share of merged PRs that are queue-priority (weekend baseline: **13%**);
- Codex sessions per merged PR (baseline: **4.15**);
- mean cost per iteration (baseline: **$26.45**) and daily loop spend;
- blocked-state iterations that produced no PR (baseline: unmeasured — P0 makes this visible).

If cost per priority PR falls while the priority share rises, the change worked. If cost
per iteration falls while the priority share stays at 13%, it did not.
