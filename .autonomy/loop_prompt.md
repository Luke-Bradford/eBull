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
   **merge ONLY via `"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh" <pr>`** (mechanically
   verifies bot-APPROVE-on-latest-SHA + CI-green; never `gh pr merge` directly).
   If the latest round is **rebuttal-only** (no code change, you think the bot is
   wrong), do NOT merge unattended — that needs Codex ckpt-3 + human judgment;
   leave the PR open with your reasoning and move on. If `safe_merge.sh` reports
   manual-mode (the repo's merge gate is `manual`), leave the PR open and move to
   the next ticket — do not attempt to merge it yourself.

   **Push discipline — run the terminal push/PR step in the FOREGROUND, never
   background it (#1771).** The pre-push gate is slow (full fast tier + smoke +
   chokepoint lints, often >2 min); run `git push` as a normal FOREGROUND Bash
   call with a long timeout (up to 10 min / 600000 ms), and run `gh pr create`
   right after it succeeds — both foreground. Do **NOT** kick off the push or the
   gate as a background task and then yield/end the turn: a headless run that
   completes kills any still-running background tasks, so the push never finishes,
   no branch is pushed and **no PR ever opens** even though the fix is committed
   locally. Only AFTER the branch is pushed AND the PR is confirmed open may you
   background the review-bot/CI **poll** (the PR already exists at that point).
   Never end a turn with an unpushed commit or an un-opened PR for work you
   intended to ship — verify `git push` succeeded and the PR URL exists first.
3. **Restart the jobs daemon** onto new main after any jobs/ingest/parser/
   scheduler merge (graceful SIGTERM, confirm old PID gone), `sec_rebuild` the
   affected source only if output changed. FE/API/docs/test/script merges need
   no restart.
4. **Feed the board (data QA + front-end QA).** Periodically:
   - **Data QA:** `uv run python scripts/dq_audit.py` → confirm any candidate on
     the full population + cite the source rule before filing.
   - **Front-end QA:** mint a dev session (`uv run python
     scripts/dev_browser_session.py`), inject the cookie into a Playwright/chrome
     context (`addCookies`, it's HttpOnly), and actually USE the app as an
     operator would. Walk the key routes — `/` dashboard, `/portfolio`,
     `/calendar`, `/instrument/<symbol>` + its drills (chart, fundamentals,
     dividends, risk, peers, news, filings, ownership, insider), `/rankings`,
     `/recommendations`, `/reports`, `/admin`. For each, screenshot + judge:
     does it look good and intuitive? loading/empty/error states present and
     honest? dark mode clean? numbers match the API (spot-check one figure
     against the endpoint)? any broken layout, dead link, confusing affordance,
     or thin/placeholder content (like the bare calendar #1766)? File verified
     **bug / ux / tech-debt** tickets with the screenshot + the exact route, one
     issue per distinct problem. Site review needs vite (`:5173`) + API
     (`:8000`) up; if down, skip FE-QA this iteration and note it.
     - **Layout integrity, not just function (do NOT skip — this is how
       #1858's dead-space-below-pagination shipped: a functional QA pass
       confirmed pagination/sort/search/numbers but never scrolled).** A
       top-of-viewport screenshot HIDES layout overflow. For every route:
       **scroll the full page top-to-bottom and screenshot the BOTTOM, not
       just the top.** Then assert the page actually bounds to the viewport:
       evaluate `document.scrollingElement.scrollHeight` vs `innerHeight` and
       confirm there is **no dead/empty scroll-space below the content** (a
       page you can scroll well past its last element is a bug). Check this on
       a **tall viewport** (e.g. 1400px) where slack is most visible, and on
       list/paginated/table pages specifically (the footer must sit at the
       bottom of the content, not float above a void). "Looks fine at the top"
       is NOT an FE-QA pass.
5. Update memory (the index + topic files) as you land work, per the memory rules.
6. Next ticket.

## Board discipline — keep the Projects v2 board honest (every ticket)

The board ("eBull engineering board") is the operator's at-a-glance view of live
task state. Keep it truthful by updating it inline via
`"$AUTONOMY_ENGINE_HOME/bin/board.sh"` — it uses your existing `gh` auth (the token already
carries `project` scope; no PAT/Action/secret). It is **best-effort**: a board
hiccup warns and exits 0, so it can NEVER block or fail the real engineering work.
Run it at each lifecycle transition for the issue # you are working:

- Pick a ticket #N (start work)        → `"$AUTONOMY_ENGINE_HOME/bin/board.sh" status N "In Progress"`
- Open its PR                          → `"$AUTONOMY_ENGINE_HOME/bin/board.sh" status N "In Review"`
- After `safe_merge.sh <pr>` succeeds  → `"$AUTONOMY_ENGINE_HOME/bin/board.sh" status N "Done"`
- File a NEW ticket #M                 → `"$AUTONOMY_ENGINE_HOME/bin/board.sh" add M` (lands in the backlog)
- Park a ticket (blocked / operator-hold) → `"$AUTONOMY_ENGINE_HOME/bin/board.sh" status N "Blocked"`

**Future (NOT active yet — do NOT gate merges on this until the operator says the
product is polished):** a "QA" column between In Review and Done, gated by a QA
subagent that exercises the change (FE-QA / behaviour) and must pass before
`safe_merge`. `board.sh status N "QA"` already works the moment that column is
added — no code change. Until activated, the flow is In Review → Done directly.

## Hard safety rules — NEVER violate, even unattended
- **NEVER execute, approve, or simulate a trade.** Do not POST to order
  endpoints (`/portfolio/orders`, `/positions/{id}/close`), do not approve
  recommendations, do not touch the kill-switch, do **not close any position** —
  demo fills are still persisted writes. Trade execution is human-gated by
  design. If a ticket's only path forward is executing a trade, skip it. (The
  loop is also run with NO broker credentials configured, so the order client
  fails closed — see setup.md; this rule is the second layer.)
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
