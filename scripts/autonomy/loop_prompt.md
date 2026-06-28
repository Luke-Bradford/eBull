# Autonomy loop — standing task

You are running headless and unattended to **drain the eBull engineering board**.
Work through open tickets back-to-back, clearing as you go. **Do not stop after a
few tickets** — keep going until either (a) there are no actionable open issues
left, or (b) you genuinely cannot make progress without a human decision (see
"When to stop"). Each scheduled run is a fresh session; a later run resumes
whatever is left, so always leave the repo in a clean state (no half-done
branches, no unpushed WIP).

## Each iteration
1. **Triage the board.** `gh issue list --state open --limit 100`. Pick the
   highest-value *actionable* ticket: prefer correctness bugs > operator-visible
   gaps > tech-debt; skip anything blocked, needing a human decision, or already
   in flight (open PR). Decide the order yourself — do not ask.
2. **Execute the full workflow** from `.claude/CLAUDE.md` for that ticket:
   read the issue → `docs/settled-decisions.md` + `docs/review-prevention-log.md`
   → research the source rule + **falsify the premise on the dev DB / full
   population BEFORE speccing** → spec → Codex ckpt-1 → implement (schema →
   service → tests → glue) → local gates → Codex ckpt-2 → branch + PR → poll the
   Claude review bot + CI → resolve EVERY comment (FIXED/EXTRACTED/REBUTTED) →
   merge only on APPROVE-on-latest-commit + green.
3. **Restart the jobs daemon** onto new main after any jobs/ingest/parser/
   scheduler merge (graceful SIGTERM, confirm old PID gone), `sec_rebuild` the
   affected source only if output changed. FE/API/docs/test/script merges need
   no restart.
4. **Feed the board.** Periodically run `uv run python scripts/dq_audit.py` and
   review the live site (`scripts/dev_browser_session.py` + Playwright) for new
   bugs / UX gaps; file verified tickets (confirm the signal full-population +
   cite the source rule first — do not file unverified candidates).
5. Update memory (the index + topic files) as you land work, per the memory rules.
6. Next ticket.

## Hard safety rules — NEVER violate, even unattended
- **NEVER execute, approve, or simulate a trade.** Do not POST to order
  endpoints, do not approve recommendations, do not touch the kill-switch, do
  **not close any position**. Trade execution is human-gated by design. If a
  ticket's only path forward is executing a trade, skip it.
- Never `git push --no-verify` (emergencies only, which this is not).
- Never restart the API (`:8000`) or vite (`:5173`) VS Code tasks.
- Never hard-delete dev data; never run destructive ops on the dev DB beyond a
  ticket's own reviewed migration/backfill.
- Merge ONLY after the Claude review bot APPROVES the latest commit + CI green.
  The bot is the unattended safety gate — never merge around it.

## When to stop (leave a clean state + a note)
- No actionable open issues remain.
- A ticket needs a genuine human decision: a settled-decision reversal, an
  irreversible-loss call, or trade-execution. File/annotate the issue with the
  researched recommendation and move on to the next ticket; only end the run if
  every remaining ticket is blocked that way.
- Local gates or the dev stack are broken in a way you can't fix in-scope —
  stop and leave a clear note (don't paper over).
