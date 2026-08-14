# Autonomy loop — standing task

You are running headless and unattended to **build the trading application the
operator can watch working**. Each scheduled run is a fresh session; a later run
resumes whatever is left, so always leave the repo in a clean state (no half-done
branches, no unpushed WIP).

## ⚠⚠ READ THIS FIRST — the standing order changed on 2026-08-14

**Operator, verbatim:** *"I want to see the trading app take shape, its been too
long, feel like I'm having my time wasted on things I'm not seeing, just
falsifications and time wasting exercises."*

The board is **no longer the target**. Draining tickets is what produced months of
audits, falsifications, corrected inventories and instruction-set maintenance while
the operator could not point at a single strategy firing in the demo account. All of
that work was individually correct and collectively off-target.

**THE ONE OBJECTIVE: a handful of genuinely different strategies, firing daily,
visible in the app, running in the demo account.**

Judge every candidate action by one question: *does this move a strategy closer to
firing where the operator can see it?* If it does not, it is not this loop's work,
however correct and however tempting the ticket looks.

### The build queue — work it in this order

Spec: `docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`. It is complete and
signed off. Do not re-open its design decisions; implement it.

⚠⚠ **CHECK `STRATEGY_MANIFEST` BEFORE STARTING ANY STRATEGY IN THIS QUEUE.** An
attended session builds from the same queue, so an item listed below may already
be on `main` by the time you read this. `python -c "from
app.services.strategy_manifest import STRATEGY_MANIFEST; print(sorted(STRATEGY_MANIFEST))"`
settles it in one command. A strategy already in the manifest is DONE — move to
the next item, do not re-derive it.

Precedent, 2026-08-14: this prompt said "build S-6 first"; S-6 was merged
attended (#2714) minutes later, and the loop spent an iteration re-doing the
regime protocol change that was already on main. The queue is a priority order,
not a claim about what is unbuilt.

Shared foundation is **DONE and committed** (`16563dab`): `app/services/market_regime.py`
and `app/services/price_levels.py`, both pure, versioned, validated on real data.

**DONE and on `main` — do NOT rebuild:**
- **S-6** resistance breakout (#2714) — 2.5 entries/name/yr, 37%/yr turnover.
- **S-5** support bounce (#2714) — 10.0 entries/name/yr.
- The regime is already threaded through `PerSeriesSignals`, `segmented_signals`,
  the scan and both backtest arm passes. `MarketRegimeProvider` exists.
- **S-9** squeeze expansion — IN FLIGHT attended (PR #2715). Do not touch.

**Your next items:**
1. **S-7** trend pullback. **S-8** range mean reversion. Follow
   `app/services/strategies/s6_resistance_breakout.py` for the registry/manifest
   contract and the regime-gated shape.
3. **S-7** trend pullback. **S-8** range mean reversion.
4. **S-10** relative-strength leader — ⚠ measure turnover FIRST; if it exceeds
   ~50%/month it is disqualified before any backtest, exactly as S-1 was at 56×/yr.
5. **Wire them to fire daily** on the live universe and render in `/strategies`.
6. **Walk-forward validation on recent regime.** Per-year and per-regime blocks,
   never one pooled number over the whole span — the operator's own point, and it
   is a constraint, not a caveat.

### ⚠ Do NOT do these, however actionable they look

- **Do not work the M9 board top-down.** That instruction is retired. #2603 step 3,
  #2602 and #2525 are all parked — the first is machinery for capital that does not
  exist yet, the second is gated on data accrual, the third has zero inputs
  (`capital_candidate` 0, `strategy_deployments` 0, `strategy_promotions` 0).
- **Do not open new falsification, audit, inventory or prevention-log tickets.** If
  you find a defect while building, fix it inline if it blocks you, or note it in
  one line on the PR. Do not spawn a ticket and do not spawn an investigation.
- **Do not touch the insider/Form 4 line.** The 20-year corpus is recovered
  (`e8daa7e5`) and the measurement is blocked on `insider_transactions` not being
  backfilled. It is parked deliberately. It is not the product path.
- **Do not weaken a promotion gate, set `CARRY_BPS` without charging it, or flip
  the kill switch.** Those are the three shortcuts that would look like progress and
  destroy the trustworthiness the operator asked for first.

### What "visible" means, concretely

At the end of a run the operator should be able to load `/strategies` and see a
strategy that scanned today, how many signals it fired, and on which instruments.
A strategy that is implemented but does not appear there is not finished.

⚠ Measured 2026-08-14 so you do not re-derive it: **93.7%** of instruments with
≥400 bars carry a live level, averaging 3.9 each; **13.4%** sit near support and
**14.1%** near resistance on any given day. The S-5/S-6 funnel is real. If your
implementation fires far less than that, the bug is yours, not the market's.

## Each iteration
1. **Take the next item from the build queue above.**

   Only if the entire build queue is genuinely complete, fall back to the board —
   `gh issue list --state open --limit 100`, preferring correctness bugs >
   operator-visible gaps > tech-debt.

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

   ⚠⚠ **WAIT IN ONE CALL — NEVER RE-POLL `gh pr checks` IN A LOOP OF TURNS.**
   Measured 2026-08-13 on iteration 70: **64 of ~112 Bash calls were
   `gh pr checks`**, i.e. over half the turns in the whole iteration. Every one
   is a full turn that re-reads ~179k tokens of cached context to learn nothing,
   and turns-per-iteration is what actually drives loop cost ($/turn is flat at
   0.11-0.18; turns/iter is what doubled 89 → 200 on the expensive days). Block
   inside a SINGLE Bash call instead, and read the result once:

   ```bash
   for _ in $(seq 60); do
     [ "$(gh pr checks <N> --json bucket --jq '[.[]|select(.bucket=="pending")]|length')" = "0" ] && break
     sleep 20
   done
   gh pr checks <N>; gh pr view <N> --comments
   ```

   ⚠ **BOUND THE WAIT — do not write a bare `until … done`.** An unbounded loop
   in an unattended driver has no stop condition of its own; it relies on the
   tool timeout as its only bound, which is a hang wearing a timeout's clothes.
   The `seq 60` cap (~20 min) exits on its own and then PRINTS THE STATE, so a
   stuck check surfaces as a visible non-empty pending list rather than as a
   killed call that says nothing. Always read the output after the wait — the
   loop's dangerous failures are the silent ones (#2658).

   Same information, same gate, same merge decision — one turn instead of sixty.
   Use `run_in_background` for the wait when you have other work to do meanwhile;
   foreground it when the next thing you would do is the merge anyway. ⚠ This is
   a turn-count fix and nothing else: do NOT weaken the gate itself. The bot must
   still APPROVE the latest SHA with CI green before `safe_merge`.
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
