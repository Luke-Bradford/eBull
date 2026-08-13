# Autonomy control-room UI — design

Status: approved by operator (visual mockup session), pending self-review.
Builds on: `2026-07-01-autonomy-engine-pack-seam-design.md` (PR #1882, the Coder-loop engine) and
`2026-07-01-managed-agents-vs-hand-rolled-comparison.md` (the hybrid substrate recommendation this
UI is built to visualize). This is the UI/UX design for #1876 (dashboard) — it also renders #1877's
PM/QA lanes, but does not itself define their role semantics (separate spec).

Produced via the brainstorming skill's visual companion (mockup HTML files are ephemeral, under
gitignored `.superpowers/` — this doc is the durable record of what was shown and approved).

## Scope

UI/UX only — screens, what each shows, what each controls, and which real data source backs it.
Not in scope here: backend implementation (server framework, auth model for the site itself),
#1877's actual PM/QA/Owner role contracts, the registry's repo-add mechanics beyond the wizard's
UI shape. Those are separate specs/plans.

## Screens

### 1. Repos (home)

List of every added repo: status badge (running/paused/needs-setup), active worker summary (e.g.
"Coder ×1"), last-activity timestamp, one-click view/resume/finish-setup. "+ Add repo" entry point.

### 2. Add-repo wizard

Three steps: (1) paste a GitHub URL, (2) live `doctor.sh` results rendered as a checklist (✅/⚠️ per
check from the pack-seam spec's table — CLAUDE.md, skills, review-bot workflow, `gh` scopes, board,
branch protection — plus a stack-detection line suggesting relevant skills, informational only,
never auto-installed), (3) initial config, prefilled from what doctor.sh found (e.g. no review
workflow found → merge gate defaults to `manual`).

### 3. Per-repo config

- **Core agent** — Claude or Codex, model + fallback. Per-repo, not global (different repos may
  want different agents/models).
- **API keys** — per-repo credential, masked, with an explicit "use account default" escape hatch.
  Second key slot appears conditionally if Codex is selected. (Maps onto Managed Agents' vault
  concept for the roles that run there; the Coder loop's own credential resolution stays whatever
  the engine's ambient-auth model ends up being — not re-litigated here.)
- **Merge gate** — radio over the pack-seam spec's four strategies (`manual`/`ci_only`/
  `bot_comment`/`gh_review`), with strategy-specific sub-fields shown conditionally (reviewer login,
  bot marker) and doctor.sh's finding pre-selecting the safe default.
- **Ticket-working rules** — the common levers (priority order, skip-labels) as simple fields, plus
  a direct link to edit the real `.autonomy/loop_prompt.md` for anything deeper — never a shadow
  copy of the pack, the same file the engine reads.
- **Workers** — Coder parallel-loop count, PM sweep on/off + cadence, QA sweep on/off + cadence +
  scope (diff-only / whole affected area / full regression).

### 4. Cross-repo worker-pool assignment

Answers "can we run parallel workers across repos": one bounded Coder pool (sized by the account's
actual rate-limit headroom, not wishful thinking), divided across repos by the operator, rather than
each repo independently assuming it can spawn freely. PM/QA shown separately since they're scheduled
Managed Agents work, not a fixed concurrent pool.

### 5. Live activity — the core visibility surface

- **Now strip** — every worker across every repo, one line each: status (thinking/acting/idle/
  sleeping), current step, model, elapsed.
- **Tree, not drill-in** — every spawned subagent/thread shown *inline*, nested under its parent,
  live, not hidden behind a click. Managed Agents' native sub-threads (e.g. QA's
  `regression-checker` + `visual-diff`) render the same way as the hand-rolled engine's Claude
  Code subagents — same visual shape regardless of which substrate produced the event, per the
  dashboard's activity-source abstraction.
- **Task + git per node** — every row carries what it's working on (ticket # + description, or the
  literal sub-instruction for a subagent) and where that lands in git (branch, commit count,
  PR #, or "read-only" for research/QA subagents that don't commit). Sourced from existing
  `task_started`/result events and git state — nothing new to invent.
- **Flat tally** — the same tree flattened into one table (agent, parent, repo, status, task, git,
  tokens) so nothing requires expanding to audit at a glance.
- **Supervisor voice** — a panel distinct from agent-level activity: the meta-loop's own decisions,
  literally `supervisor.log` lines ("session clean, pace 120s", "USAGE LIMIT, sleeping until
  14:20"). Operators need to see the *loop's* reasoning, not just what the agent inside it is doing.
- **Git in flight** — cross-repo board: ticket → branch → PR → CI → review status → merge gate, one
  table, so "what's actually in motion right now" doesn't require checking each repo separately.

### 6. Chat / control

Target picker (main controller, a repo, or one specific running agent) + a message box. Two-way:
- **Agent → operator:** clarifying questions the agent could only guess at (e.g. a settled-decision
  gate it genuinely needs sign-off on) surface here, not buried in a PR comment.
- **Operator → agent/controller:** status queries ("how's #1857 going") and directives ("pause, I
  want to look first" — ties to the pack-seam spec's graceful-stop sentinel).

### 7. Quotas

**Shared, account-level bars — not per-repo.** Directly visualizes the pack-seam comparison doc's
stampede finding: one Anthropic account's 5h + weekly windows are the real constraint across every
repo, so the UI shows one bar, not N independently-misleading per-repo bars. Separate rows for
OpenAI (Codex) if any repo has it connected, and Managed Agents' own infra rate limits.

### 8. Live model/effort override

Per-row control (model dropdown, effort dropdown) with an explicit scope choice: **this session
only** vs **save as new default for this repo** (writes back to `.autonomy/config.yaml`). Keeps
the override surface honest about whether it's a one-off or a committed pack change — no silent
drift between what the dashboard shows as "current" and what's actually versioned.

### 9. Token throughput widget

Deliberately small — a sidebar sparkline, not a headline chart (operator feedback: visibility, not
a major draw). One thin line per worker, range toggle capped at 1h/6h/24h. Native hover tooltips
(SVG `<title>` per point — no custom JS required) show exact time + tok/min; a persistent
always-visible "current value + trend" readout means you don't have to hover to get the headline
number. A rate-limit hit is visible directly on the line (flatlines to zero, annotated) rather than
only as a separate status badge elsewhere on the page.

## Cross-cutting notes

- **One visual language for both substrates.** Every screen renders hand-rolled-engine events and
  Managed-Agents events identically (same tree shape, same task/git fields, same tally row shape).
  The operator should never need to know or care which substrate produced a given line — this is
  the payoff of the dashboard's activity-source abstraction from the original #1876 issue.
- **Nothing here is a new capability the engine doesn't already produce or plan to produce** — every
  field traces back to an existing event (`task_started`, `rate_limit_event`, `result` with token
  usage), an existing log (`supervisor.log`), an existing git/GH state (branch, PR, CI, review), or
  an existing config file (`.autonomy/config.yaml`). The UI's job is exposing it, not inventing new
  telemetry.

## Open questions (not resolved by this UI pass — implementation spec's job)

- Backend framework/serving model for the site itself (still leaning stdlib/SSE per #1876's
  original "zero new heavy deps" note, but not re-confirmed here).
- Auth model for the control-room site (single-operator local tool today; would need real auth if
  ever exposed beyond localhost).
- Exact mechanism for writing a "session-only" live override back to a *running* engine process
  (signal file? IPC? — same shape as the pack-seam spec's graceful-stop sentinel, not yet designed
  for arbitrary model/effort changes specifically).
