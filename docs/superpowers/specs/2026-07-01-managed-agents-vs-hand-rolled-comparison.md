# Managed Agents vs. hand-rolled engine — architecture comparison

Status: recommendation, pending operator sign-off. Supersedes/revises parts of
`2026-07-01-autonomy-engine-pack-seam-design.md` (PR #1882) — see Recommendation.

## Why this exists

Mid-brainstorm on the hand-rolled `autonomy-engine` spec (PR #1882), operator flagged a recent
Anthropic **Managed Agents** update (GA'd April 2026; dashboard/scheduling/webhooks/vaults now live
in the Claude Console). Confirmed via web search — real, not the MCP-connector-observability
feature that also shipped around the same time (different, unrelated). Managed Agents' feature set
overlaps heavily with what #1876 (dashboard) and #1877 (multi-role org) were going to hand-build.
This needed a real comparison before sinking more effort into the hand-rolled path.

## Candidate A: hand-rolled engine (PR #1882's design)

`claude -p` invoked locally by bash, scheduled via launchd, worktree-isolated, custom dashboard
(tail stream-json logs → SSE → HTML), multi-role via separate sessions handing off through the
GitHub board + ticket comments (per #1877's own guardrails).

## Candidate B: Managed Agents

Anthropic-hosted `agents`/`sessions`/`environments` objects. Relevant primitives:
- **Scheduled deployments** — cron-fired sessions, pause/unpause, per-firing audit trail
  (`deployment_run`), manual-run trigger. Anthropic-hosted scheduler — no launchd.
- **Self-hosted sandboxes** — agent loop runs on Anthropic's side; **tool execution runs on a
  worker you host** (`EnvironmentWorker.run()` / `ant beta:worker poll`, long-polling, outbound-only).
  This is the piece that matters for eBull specifically: it can reach `localhost:5173`/`:8000` and
  a real browser session for FE-QA, which a fully-cloud-hosted container cannot.
- **Native multiagent** — a coordinator agent with a roster of sub-agents, each a persistent thread
  with cross-thread messaging (`agent.thread_message_sent`/`_received`). Closer to #1877's
  PM/Coder/QA/Owner idea than board-comment handoff, with richer in-process coordination.
- **Native dashboard** (Claude Console) — sessions, environments, vaults, cost, parent/child session
  tracking for multi-agent setups. Most of #1876, already built.
- **Webhooks + vaults** — session state-change notifications; credential storage (MCP OAuth,
  env-var secrets substituted at egress) — closer to the "plug in your keys" ask than anything
  hand-rolled here would be.

## Requirement-by-requirement

| Requirement | A (hand-rolled) | B (Managed Agents) |
|---|---|---|
| Reach eBull's local dev stack (FE-QA needs `:5173`/`:8000`/browser) | Yes — runs on the operator's own Mac already | Yes, **but only via self-hosted sandbox** — a fully-cloud container cannot |
| Claude + Codex agent-agnostic | Yes — the agent-adapter boundary designed for exactly this | **No — Claude-only.** Codex loops would stay on path A regardless |
| Dashboard / observability | Must hand-build (#1876) | Native (Console), free |
| Multi-role org (#1877) | Must hand-build (board/comment handoff) | Native (coordinator + roster + threads), largely free |
| Credential/key management | Must hand-build | Native (vaults) |
| **Continuous "drain the board back-to-back for days" pattern** | **Solved, battle-tested** — precise sleep-until-API-reported-reset, no thrash | **Mismatch, unverified.** Scheduled deployments are cron-shaped (minimum granularity: once/minute), designed for periodic work ("nightly scan"), not a tight continuous loop. Whether a long-running session has built-in usage-limit-aware backoff/resume, or just errors out needing external detection + re-trigger (via `deployment_run` error records + webhooks) is **not documented anywhere read for this comparison** — a real open question, not asserted either way. |

## The rate-limit/continuous-loop mismatch (why this isn't a clean swap)

Scheduled deployments are built for periodic, bounded work — the docs' own canonical examples are
"weekly compliance scan," "nightly report." eBull's actual Coder loop is a different shape: fire
back-to-back sessions for hours/days, sleeping *precisely* until an API-reported rate-limit reset,
never thrashing. A naive "cron every 2 minutes" mapping onto scheduled deployments would recreate
exactly the thrash today's `supervisor.sh` was hardened against (#1871: blind exponential backoff →
sleep until the reported reset). Whether a single long-running Managed Agent session handles a
mid-session rate-limit gracefully is unverified from documentation alone and would need an actual
test against real usage before trusting it for the continuous-drain pattern.

## Recommendation: hybrid, not a swap

**Keep PR #1882's hand-rolled continuous engine for the Coder loop.** It already solves the hard,
proven problem (precise rate-limit-aware continuous draining) and keeps Codex-adapter pluggability
intact. Landing it is not wasted effort — this is real, needed substrate regardless of what happens
next.

**Redirect #1876 and #1877 toward Managed Agents, not a from-scratch hand-build:**
- **#1877 (multi-role org):** PM and QA are genuinely periodic/bounded work (triage sweep, scheduled
  regression sweep) — a much better fit for **scheduled deployments** + **native multiagent** than
  a hand-rolled board-comment-handoff protocol. Owner/lead's "check in, clear blockers" role maps
  naturally onto the coordinator. The Coder role stays on Candidate A (continuous, not periodic).
- **#1876 (dashboard):** don't rebuild what the Claude Console dashboard already gives you for
  Managed-Agents-run roles (PM/QA sessions, cost, multi-agent thread view). The dashboard spec's
  own **activity-source abstraction** (already designed to accept "any runner that appends events in
  this shape") is the right unifying layer — add a Managed-Agents source (via webhooks) alongside
  the hand-rolled `claude-autonomy` source for the Coder loop, rather than picking one substrate and
  discarding the other's visibility.
- **Credential/key management, "plug in your keys" ask:** vaults, not a hand-rolled config UI.
- **eBull's own FE-QA-requiring work should run through a self-hosted sandbox worker**, if/when any
  role moves to Managed Agents, specifically because of the local-dev-stack requirement.

**Net effect on PR #1882:** the spec itself doesn't change — it's still the right design for the
Coder loop. Only its "Open follow-ups" section needs revising: the dashboard and multi-role
follow-ups currently read as "build this ourselves later." They should instead point at this
comparison and Managed Agents as the default starting point for those specs, not a hand-rolled
build.

## Open questions before the #1877/#1876 specs can be written for real

- Empirically verify Managed Agents' behavior when a session's account hits a rate limit mid-run —
  does it degrade gracefully (retry/resume) or need external detection? Matters even for
  periodic PM/QA work if a sweep runs long.
- Self-hosted sandbox worker setup cost/reliability on the operator's own Mac (long-poll process,
  same "needs to survive reboots" concern the current supervisor already solves with launchd).
- Whether Managed Agents' per-session/environment cost model has overhead beyond token usage.
