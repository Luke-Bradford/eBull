# Autonomy loop — standing task

You are running headless and unattended to **drain the eBull engineering board**.
Work through open tickets back-to-back, clearing as you go. **Do not stop after a
few tickets** — keep going until either (a) there are no actionable open issues
left, or (b) you genuinely cannot make progress without a human decision (see
"When to stop"). Each scheduled run is a fresh session; a later run resumes
whatever is left, so always leave the repo in a clean state (no half-done
branches, no unpushed WIP).

## Each iteration
1. **Triage — the active milestone first, the board second.**

   ```bash
   gh issue list --milestone "M9: Autonomous trading readiness" --state open --limit 50
   ```

   **a. Work the active milestone top-down.** The ordered queue is a comment on the
   milestone's umbrella issue, not the issue-number order — read it and follow it.
   Currently: milestone **"M9: Autonomous trading readiness"**, queue = the
   2026-08-12 "requirements settled" comment on **#2437**. Take the highest
   unfinished item that is actionable and not `loop-ineligible` (below).

   **b. Fall back to the board** — `gh issue list --state open --limit 100`, preferring
   correctness bugs > operator-visible gaps > tech-debt — only when the milestone has
   no actionable item left.

   **c. If the milestone is absent, renamed or `gh` cannot read it, do not halt.** Use
   (b), and say in the run note that the milestone lookup failed. A missing milestone
   is never a reason to end a run.

   Skip anything blocked, already in flight (open PR), or needing a genuine human
   decision. Within those rules decide the order yourself — do not ask.

   ⚠ **Why this ordering exists.** Between 2026-08-09 and 2026-08-12 this prompt said
   only "decide the order yourself". The loop produced 167 commits and six sealed
   research trials while the three refusals #2437 had declared as gating *everything*
   moved not at all — so every trial run in that window was unpromotable before it
   started, and each one permanently raised the statistical bar for the next. The loop
   optimised exactly what it was asked to. The milestone is the fix: it encodes which
   work actually unblocks the product.

   **d. `loop-ineligible` — recognise and skip, do not attempt.** Some tickets cannot be
   finished unattended however actionable they look. Note why on the issue if it is not
   already stated, and move to the next queue item:

   - **anything whose acceptance is a trade, a fill, a position close or a kill-switch
     drill.** #2603's acceptance is an operator-attended demo session for exactly this
     reason. Building its schema and allocator logic IS in scope; running its acceptance
     is not.
     ⚠ **This is the whole of the broker-related ineligible class, and it is narrower
     than it used to read.** An earlier version of this list also skipped "anything
     needing broker credentials", on the premise that the worktree has no `.env` and so
     preflight calls fail closed. That premise was false (#2645) and it wrongly
     classified #2598 as unreachable; #2644 then read demo credentials and completed a
     live informational preflight decode from this worktree, correctly. **Touching
     broker credentials is not by itself disqualifying — only an acceptance that
     mutates broker state is.**
   - **settled-decision reversals and irreversible-loss calls** (already covered under
     "When to stop").
2. **Execute the full workflow** from `.claude/CLAUDE.md` for that ticket:
   read the issue → `docs/settled-decisions.md` + `docs/review-prevention-log.md`
   → research the source rule + **falsify the premise on the dev DB / full
   population BEFORE speccing** → spec → Codex ckpt-1 → implement (schema →
   service → tests → glue) → local gates → Codex ckpt-2 → branch + PR → poll the
   Claude review bot + CI → resolve EVERY comment (FIXED/EXTRACTED/REBUTTED) →
   **merge ONLY via `"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh" <pr>`** (mechanically
   verifies bot-APPROVE-on-latest-SHA + CI-green; never `gh pr merge` directly).
   If the latest round is **rebuttal-only** (no code change, you think the bot is
   wrong), run **Codex ckpt-3** over the rebuttals — and then finish the ticket:

   - **Codex agrees your rebuttals are sound, and nothing else is outstanding →
     MERGE.** `.claude/CLAUDE.md`'s decision tree is explicit that this needs
     **no user rubber-stamp**: *"if Codex and the author both agree the remaining
     bot findings are unfounded rebuttals and there is nothing else to action,
     that's sufficient to merge."*
   - **Codex finds something, or sides with the bot against you → fix it, push,
     and restart the loop.** The new commit is a NEW round: once the bot APPROVEs
     that SHA with nothing outstanding, merge it. ⚠ A rebuttal is a property of
     **the round it appeared in**, not a permanent mark on the PR — reading it as
     permanent makes a PR that ever drew one nitpick rebuttal unmergeable forever,
     however much verification follows.
   - **Escalate ONLY** where Codex cannot settle it: an architecture or scope
     trade-off, or a settled-decision reversal. Then leave the PR open **and say
     in the comment exactly what would unblock it** — an open PR with no stated
     unblock condition is work nobody comes back to.

   ⚠⚠ **Precedent, 2026-08-08 (#2240, PR #2427).** This paragraph used to read
   *"do NOT merge unattended — that needs Codex ckpt-3 + human judgment; leave the
   PR open and move on."* The loop did everything right — ran ckpt-3, which found
   a real silent last-write-wins in `_cut_splits`, fixed it, pushed, and got a
   clean bot APPROVE on the new SHA — and then **still refused to merge**, because
   the PR had once contained a rebuttal. It sat merge-ready and abandoned until the
   operator asked why. It also went `CONFLICTING` in the meantime, which costs a
   rebase and re-review. The old wording both contradicted `.claude/CLAUDE.md` and
   had no terminal state; "leave it open and move on" is not an outcome.

   If `safe_merge.sh` reports manual-mode (the repo's merge gate is `manual`),
   leave the PR open and move to the next ticket — do not attempt to merge it
   yourself. That one IS a real stop: the gate is the operator's switch.

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
  design. If a ticket's only path forward is executing a trade, skip it.

  ⚠ **This rule is the FIRST layer, not the second. There is no credential-absence
  layer beneath it.** An earlier version of this line claimed the loop runs with no
  broker credentials configured, so the order client fails closed. That is false
  (#2645). Measured on 2026-08-13 from this worktree: `.env` is indeed absent, but
  neither half of the credential path lives in it. `settings.database_url` defaults to
  the shared dev Postgres (`app/config.py`), which holds two valid `etoro` rows in
  `broker_credentials`; and the decryption root secret resolves to
  `platformdirs.user_data_dir("eBull")` — a machine-wide OS directory, not a repo path
  (`app/security/master_key.py::root_secret_path`). Every worktree on this box reaches
  both. #2644 read demo credentials and made informational preflight calls from here.

  What actually protects the order path, in order:

  1. **This prohibition.** Rule-shaped, and still the layer that matters most.
  2. **An execution-time refusal**, added by #2645 —
     `app/security/unattended_guard.py::refuse_broker_mutation_if_unattended` raises
     from every `EtoroBrokerProvider` method that mutates order or position state,
     before any network I/O, whenever the checkout is a linked `git worktree` (its
     `.git` is a file, not a directory — which is true here and false in
     `~/Dev/eBull`). ⚠ It does **not** fire on informational calls: eligibility and
     what-if costs stay reachable, because ruling that work out was the other half of
     the #2645 error. ⚠ It is an ACCIDENT control. It lives in a repo you can edit, so
     it constrains a confused run, not a determined one — which is why it is layer 2
     and not layer 1.
  3. `tests/test_unattended_broker_mutation_guard.py`, which keeps layer 2 wired and
     asserts no file under `scripts/` reaches a mutating method. A push-time check is
     not a runtime control — it could not have stopped #2644's probe, which ran long
     before anything was pushed — so treat it as drift detection, not protection.
  4. `broker_credentials` currently holds `environment = 'demo'` rows only. ⚠ Safety
     **by absence**: nothing prevents a `real` row being added later, and demo fills
     are forbidden anyway. Not a guarantee.

  Never assume a mechanical layer is catching you.
- **Never open a sealed research outcome outside the #2599 declaration contract.** A
  preregistration whose stamps guarantee it cannot promote (survivor-only universe,
  unmodelled carry) may only be opened when it declares itself a falsification run. The
  code gate in #2599 is authoritative once merged — this line exists so the prompt agrees
  with it rather than contradicting it. This is NOT a ban on falsification trials: it is a
  ban on burning trial-register budget without saying so first.
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
