---
name: committee-review
description: Multi-agent committee review of a plan, spec, or design document. Dispatches 8 parallel persona-specific lenses (architect, adversarial reviewer, data engineer, API contract, operator/SRE, test engineer, PM, Codex CTO) on a target document, captures verbatim outputs to memory, produces a consolidated findings memo, and writes a handoff prompt. Use when a plan needs adversarial scrutiny before execution, when prior single-reviewer iterations have produced hallucinated APIs / ungrounded numbers / internal contradictions, or when the work crosses multiple expertise boundaries. Skip for trivial reviews where 2-3 lenses suffice — committee is for high-stakes documents where missing a gotcha is expensive.
---

# Committee review

## When to use

Trigger this skill when:
- A plan, spec, or design doc is about to gate code-writing work (high cost of being wrong).
- The doc has already been through 1+ review rounds AND prior reviews produced unverified-claim findings (signal: prior plan cited APIs that don't exist).
- The doc spans multiple expertise boundaries (architecture + data + API + operator + test + PM).
- The user explicitly asks for "committee", "multi-agent review", "rip it apart", "find gotchas before we ship".

Do NOT trigger when:
- Single-page diff that needs a one-lens review (use `feature-dev:code-reviewer` directly).
- The doc is exploratory / WIP (committee is for "ready to execute" docs).
- The user has signed off and wants execution (committee is review, not authorisation).

## What it produces

For target doc at `<path>`, this skill produces:

1. **8 raw review memos** at `~/.claude/projects/<project>/memory/project_<topic>_review_<lens>.md` — one per lens, verbatim agent output, severity-tagged findings with file:line citations.
2. **1 consolidated findings memo** at `~/.claude/projects/<project>/memory/project_<topic>_consolidated_findings.md` — cross-reviewer synthesis: BLOCKING (multiple lenses agree), IMPORTANT (single-lens but solid), OBSERVATION (nice-to-have). Hallucinated APIs surfaced. Strategic reframes if multiple lenses converge.
3. **1 handoff prompt** at `~/.claude/projects/<project>/memory/project_<topic>_next_session_prompt.md` — self-contained prompt for the next session that reads the findings + revises the plan.
4. **MEMORY.md index updates** linking all new memos.

## The 8 lenses

Each lens MUST be briefed with: identity ("You are X"), target doc path, predecessor history if relevant, explicit "do NOT re-litigate prior findings", lens-specific concerns, output format. Briefs should be ~600-1500 words each — terse and self-contained.

| Lens | Agent | Concerns |
|------|-------|----------|
| **Architect** | `feature-dev:code-architect` | Layering, abstractions, dispatcher integration, existing-pattern reuse vs new framework. Find: load-bearing pattern misses, layering violations, "new class when existing dataclass works" mistakes. |
| **Adversarial Reviewer** | `feature-dev:code-reviewer` | Unverified claims, hallucinated APIs, ungrounded numbers, internal contradictions. Code-grounds every numerical/API claim with file:line. Verifies: column names, function names, file paths, projected wall-clock. |
| **Data Engineer** | `general-purpose` + DE persona | Schema discipline, watermarks, conflict keys, indexes, tombstones, partition strategy, smoke matrix coverage, encoding/precision/NULL/timezone. From "I'd ship the 15th source under this" view. |
| **API Contract** | `general-purpose` + API persona | Provider contracts, rate-limit composition, conditional-GET semantics, retry posture per error class, pagination, error response shape, identity drift, schema evolution. |
| **Operator / SRE** | `general-purpose` + SRE persona | Runbook safety, observability, failure modes, recovery procedures, disk/WAL pre-checks, onboarding sequences, rollback plans, concurrent-execution guards. |
| **Test Engineer** | `general-purpose` + test persona | Test gate coverage, CI flakiness, fixture maintainability, test-suite performance, contract test feasibility, regression detection power, mock fixture standardisation. |
| **Project Manager** | `general-purpose` + PM persona | Sequencing, time-boxing, dependency graph, scope creep, risk register, AND/OR acceptance criteria, resume protocol, bus factor. |
| **Codex CTO** | `codex exec` via Bash | Strategic risk-adjusted prioritisation. Highest-ROI sequencing. Biggest residual risk if the plan executes as written. "What this plan does NOT prevent" angle. |

## How to dispatch

1. **Confirm target doc + topic + project slug** with user before dispatch. Topic = short kebab-case name (e.g. `etl-v3`). Project slug used in memory path (e.g. `eBull` → `-Users-lukebradford-Dev-eBull`).

2. **Dispatch 7 agents in parallel** via Agent tool — each in own tool_use block in the same message. Plus 1 Codex via Bash `run_in_background=true`. Each agent brief MUST:
   - State the lens persona explicitly ("You are the X lens on a committee reviewing Y").
   - Cite the target doc path (absolute).
   - Cite predecessor versions if any (with explicit "do NOT re-litigate findings already incorporated").
   - List 8-15 lens-specific concerns to address.
   - State output format ("severity-tagged findings, file:line citations, under N lines").

3. **Wait for all 8 outputs.** Agents return at different rates; Codex via Bash arrives via task-notification.

4. **Save each verbatim** to `~/.claude/projects/<project-slug>/memory/project_<topic>_review_<lens>.md` with frontmatter:
   ```
   ---
   name: <topic>-review-<lens>
   description: <lens> lens on <topic> (date). Found X BLOCKING + Y IMPORTANT + Z OBSERVATION. Key findings — <one sentence>.
   metadata:
     type: project
   ---
   ```

5. **Write consolidated findings memo** at `~/.claude/projects/<project-slug>/memory/project_<topic>_consolidated_findings.md`:
   - Cross-cutting BLOCKING (multiple lenses agree — code-verified).
   - Internal contradictions.
   - Self-violated gates.
   - Numbers ungrounded.
   - Sequencing / strategic findings.
   - IMPORTANT bucket grouped by domain.
   - Strategic reframe if multiple lenses converge on it.
   - Top N must-fix list.
   - File index linking raw reviews.

6. **Write handoff prompt** for next session — self-contained, reads findings + revises the plan or splits into streams. See `references/handoff-template.md` (if needed).

7. **Update MEMORY.md index** with links to all new memos.

## Dispatch checklist

```
[ ] Target doc path absolute + reviewed (read it first to ensure you know what reviewers will see)
[ ] Topic name + project slug confirmed with user
[ ] 7 Agent calls + 1 Bash codex in SAME MESSAGE for parallelism
[ ] Each brief states persona + DO NOT re-litigate + output format
[ ] Saved 8 raw memos with frontmatter
[ ] Consolidated findings memo written
[ ] Handoff prompt written
[ ] MEMORY.md index updated
```

## Anti-patterns

- **Don't synthesize prematurely.** Wait for all 8. Codex may catch what nobody else does.
- **Don't re-litigate prior findings.** Brief explicitly says "do NOT re-litigate". Reviewers must focus on NEW gotchas in the current doc.
- **Don't paraphrase reviewer output.** Save verbatim. Synthesis happens in the consolidated memo, not in the per-lens memo.
- **Don't dispatch lenses serially.** All 8 in parallel = 10 min wall-clock. Serial = 1+ hour.
- **Don't skip Codex.** Strategic / CTO lens via `codex exec` often catches what specialised lenses miss (e.g. "deferring correctness to non-firing scheduled jobs").
- **Don't trust reviewer claims without code grounding.** Reviewer findings about file:line / API names / column existence MUST be cross-checked against actual code before user acts on them. The architect lens often catches when one reviewer's "fact" is itself wrong.

## What this skill is NOT

- Not a code-implementation skill. Output is review + handoff, not code.
- Not a planning skill. Committee reviews a plan; another step writes v(N+1).
- Not a substitute for execution-time verification. Each fix the next session implements still needs its own code-grounding.

## See also

- `superpowers:dispatching-parallel-agents` — general parallel-agent pattern.
- `superpowers:writing-plans` — for writing the plan that committee reviews.
- `feature-dev:code-reviewer` — single-lens review for smaller docs.
