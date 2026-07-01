# Autonomy engine ↔ pack seam — design

Status: approved by operator, pending spec self-review + write-up into an implementation plan.
Relates to: eBull #1876 (ops dashboard), #1877 (multi-role org) — both explicitly deferred, see Scope.

## Problem

`scripts/autonomy/` in eBull is a working, hardened autonomy loop (supervisor, worktree isolation,
rate-limit handling, mechanical merge gate) but it is entangled with eBull specifically: hardcoded
`.claude/CLAUDE.md` existence check, hardcoded `OWNER`/board title, an inline safety string
duplicated across two scripts, a single fixed merge-gate mechanism assuming one specific
GitHub-Actions review-bot shape, and launchd plist/worktree paths that assume "the" repo is eBull.

The operator wants a standalone engine that can supervise autonomy loops across multiple,
independent target repos/projects, from one Claude account, eventually with a cross-project
dashboard and pluggable local-LLM lanes. Building the dashboard or multi-role org against today's
single-repo-hardcoded scripts means building on a foundation that has to be ripped up later.

## Scope

**This spec: the engine↔pack seam only.** A new standalone repo (`autonomy-engine`) that is
genuinely repo-agnostic, plus eBull's own pack (`.autonomy/`) as the one proof case, with eBull
fully cut over to running through it.

**Explicitly NOT in this spec** (separate specs later, each depends on this one existing first):
- Cross-repo/worktree **registry** — a control unit that knows about and can launch/stop loops
  across *multiple* repos at once. This spec proves the engine against one repo; the CLI contract
  (`supervisor.sh --repo <path>`, nothing hardcoded) is deliberately shaped so a registry can drive
  it later without rework, but the registry itself is not built here.
- **Dashboard** (#1876) — cross-project visibility + control-lever surface (pause, change model,
  query the supervisor interactively from an operator's own Claude/Codex session). Captured
  requirement for that spec, not designed here.
- **Multi-role org** (#1877) — PM/Coder/QA/Owner lanes. Not touched.
- **Local-LLM pluggable lanes.**
- Auto-*provisioning* of GitHub state (creating the review workflow, branch protection, the
  Projects v2 board, secrets) for a cold repo. `doctor.sh` (below) is diagnostic-only.

**Success bar:** eBull's autonomy loop runs identically through the new engine + eBull's
`.autonomy/` pack, with `scripts/autonomy/` deleted from eBull and the launchd plist repointed at
the engine repo. Not validated against a second real repo in this spec — the design is built to be
repo-agnostic, but proven against exactly one.

## Architecture

```text
autonomy-engine/                        (new GitHub repo, bash + python, no packaging —
                                          sibling-cloned and run by path, same convention
                                          eBull's own worktree already uses)
  bin/
    supervisor.sh        # --repo <path> (or $AUTONOMY_TARGET_REPO); no hardcoded target repo
    onboard.sh            # <target-repo> -> scaffolds .autonomy/ with template files
    doctor.sh              # <target-repo> -> diagnostic readiness report (see below)
    setup_worktree.sh       # <target-repo-url-or-path> [worktree-path] -> worktree + plist
    worktree_gc.sh           # --repo <path>
    safe_merge.sh             # generic gate; strategy dispatch, reads .autonomy/config.yaml
    board.sh                   # generic; reads owner/project_title from config.yaml
    unblock_dependents.sh       # already fully repo-agnostic today — moves verbatim
  bin/agents/
    claude.sh                 # only adapter implemented this spec — see Agent adapters
  templates/
    supervisor.plist.tmpl        # __REPO__, __LABEL__ placeholders (label derived from repo slug)
    autonomy-pack/                # what onboard.sh scaffolds: loop_prompt.md, hard_rules.md,
                                   # config.yaml skeletons with commented defaults
  tests/                            # ported + new, see Testing
  README.md                          # the pack contract: required .autonomy/ files, config.yaml
                                      # schema, merge-gate strategy reference

<target-repo>/.autonomy/                 (eBull first; any future target repo same shape)
  loop_prompt.md          # standing task + board discipline — content moved from eBull's
                           # scripts/autonomy/loop_prompt.md, NOT byte-verbatim: every internal
                           # path reference is rewritten (see "Path rewrites" below), plus one
                           # added line: attempt merge via safe_merge.sh; if it reports
                           # manual-mode, leave the PR open and move to the next ticket.
  hard_rules.md           # extracted from the SAFETY string duplicated inline today in
                           # supervisor.sh + run_loop.sh — single source of truth, appended to
                           # the session's system prompt by supervisor.sh, never embedded in bash.
  config.yaml              # see schema below
```

### Path rewrites (Codex finding — real bug if missed)

`loop_prompt.md` today hardcodes `scripts/autonomy/safe_merge.sh` (merge step) and
`scripts/autonomy/board.sh` (every board-discipline line) — both paths are deleted by this spec's
cutover. Every such reference in the moved `loop_prompt.md` becomes
`"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh"` / `"$AUTONOMY_ENGINE_HOME/bin/board.sh"`. The
`hard_rules.md` text that says "follow `.claude/CLAUDE.md` and `scripts/autonomy/loop_prompt.md`
exactly" becomes "...and `.autonomy/loop_prompt.md`" (pack-relative, no engine-repo path needed —
it's shipped alongside `hard_rules.md` in the same directory). This rewrite is a required step in
Cutover, not automatic — call it out explicitly there.

`.autonomy/` existing (with a valid `config.yaml`) is the engine's guard for "this is a valid
target repo" — replaces today's eBull-specific `[ -f .claude/CLAUDE.md ]` check.

### `.autonomy/config.yaml` schema

**Parser (Codex finding — dependency was unspecified):** the engine ships its own small,
stdlib-only parser for a deliberately restricted YAML subset (flat + one level of nesting,
strings/lists, `#` comments) — the exact shape `config.yaml` needs, nothing more. No `PyYAML` or
`yq` dependency to install on whatever machine the engine runs on; if a future pack ever needs real
YAML's full feature set, that's the trigger to reconsider, not now.

```yaml
board:
  owner: Luke-Bradford          # GitHub user/org that owns the Projects v2 board
  project_title: "eBull engineering board"
  # owner_type is NOT a config key — board.sh auto-detects: tries the GraphQL `user(login:...)`
  # shape first, falls back to `organization(login:...)` if that returns null. See board.sh notes.

engine:
  label: ebull                   # optional override for the derived {repo-slug} (see below) —
                                  # set this if two target repos would otherwise collide on
                                  # basename (e.g. two checkouts both named "eBull" on one machine)
  requires_claude_md: true        # hard-fail doctor.sh/preflight if .claude/CLAUDE.md is missing,
                                   # instead of the generic warn-only default. eBull sets this true
                                   # since its whole workflow assumes CLAUDE.md exists.

agent:
  type: claude                   # claude | codex  (only claude has an adapter implemented)
  model:
    primary: claude-sonnet-5       # optional override; the claude adapter has its own sane default
    fallback: claude-sonnet-4-6      # claude-specific: native --fallback-model support
  config: {}                        # opaque, adapter-owned pass-through — the claude adapter
                                     # ignores it; a future local-LLM adapter reads whatever it
                                     # needs from here (endpoint, context size, tool policy,
                                     # timeout, concurrency class) without changing agent_invoke's
                                     # signature. Adapters read config.yaml directly; this map is
                                     # not threaded through as a separate parameter.

merge_gate:
  strategy: bot_comment          # manual | ci_only | bot_comment | gh_review  (see below)
  # bot_comment-specific:
  author_login: github-actions
  marker: "Claude Code Review"
  doc_only_extensions: [".md"]   # doc-only fast path, bot_comment-strategy-specific
  # gh_review-specific:
  # reviewer_login: copilot-pull-request-reviewer[bot]

worktree:
  default_path: "../.{repo-slug}-autonomy"   # sibling dir convention; overridable
```

Every value above is either optional-with-an-engine-default or required-only-for-the-strategy-that-
uses-it. Nothing in the engine hardcodes eBull's actual values — they all come from this file.

**Pack config is per-repo *defaults*, not per-instance state (Codex strategic-fit finding):** this
matters for the deferred registry/multi-role work, which will want to run more than one loop
instance against the same repo (e.g. a PM lane and a Coder lane, different agents/models/labels).
`supervisor.sh` accepts CLI overrides — `--agent-type`, `--model`, `--label` — that take precedence
over `config.yaml`'s values for that one invocation. `config.yaml` stays the single committed
source of truth for *project policy* (merge-gate strategy, board identity, requires_claude_md —
things that don't vary by which instance is running); anything that could legitimately differ
per-running-instance is override-able without editing the pack. Not implementing multi-instance
here — just making sure this spec's CLI contract doesn't foreclose it.

`{repo-slug}` (used in `worktree.default_path` and the launchd label) = `engine.label` if set,
else the target repo's directory basename, lowercased, any non-alphanumeric run collapsed to a
single `-` (e.g. `eBull` → `ebull`). **Collision guard (Codex finding — two different repos with
the same basename, e.g. `~/work-a/eBull` and `~/work-b/eBull`, would silently overwrite each
other's plist/worktree/lock):** `setup_worktree.sh` refuses (doesn't silently overwrite) if the
derived label already has a registered plist/worktree pointing at a *different* source path than
the one it was just asked to set up — the operator must set `engine.label` explicitly to
disambiguate.

## Data flow

```text
launchd (plist, label derived from repo slug, e.g. com.autonomy.ebull.supervisor)
  → bin/supervisor.sh --repo <worktree-path>
      exports AUTONOMY_ENGINE_HOME (derived from own script location, not hardcoded)
      exports AUTONOMY_TARGET_REPO (= --repo arg)
  → preflight: unchanged generic git logic (dirty-tree/rebase guards, detach @ origin/main) PLUS
      one new hard check: $TARGET_REPO/.autonomy/config.yaml must exist and parse; abort loudly if not.
  → source bin/agents/${agent.type}.sh; call agent_invoke(prompt=.autonomy/loop_prompt.md,
      safety=.autonomy/hard_rules.md, model, fallback_model, log_file) — for agent.type=claude this
      is today's exact `claude -p ... --append-system-prompt ... --fallback-model ...
      --output-format stream-json` invocation, moved verbatim, nothing new.
  → call agent_classify_outcome(log_file, exit_code) -> success | usage_limit[+reset_epoch] | error
      — for claude this is today's is_usage_limit_hit + extract_reset_epoch, moved verbatim (already
      100% generic parsing of structured stream-json events, no repo-specific text).
  → session (cwd = $TARGET_REPO worktree) calls, per its pack instructions:
      "$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh" <pr>   (dispatches on config.yaml's merge_gate.strategy)
      "$AUTONOMY_ENGINE_HOME/bin/board.sh" status <n> "<status>"   (reads config.yaml board.*)
```

## Agent adapters

The engine drives *some* CLI agent to actually do the work; today that's always `claude -p`, but
the invocation shape, safety-prompt injection mechanism, output/log format, and rate-limit signal
are all specific to which CLI is doing the work. Confirmed by inspecting `codex exec --help`:
Codex has no `--append-system-prompt`-equivalent (would need the safety text prepended into the
prompt itself), its own JSONL event schema (`--json`), and no native fallback-model support (would
need the *engine* to retry with a different `-m` on failure, not delegate to the CLI). These are
real, structural differences — not something to guess-implement without testing against a live
Codex quota, which this spec doesn't do.

**Same pattern as merge-gate strategies: define the interface now, implement one instance now.**

- `bin/agents/<type>.sh`, dispatched by `.autonomy/config.yaml`'s `agent.type`. Two functions by
  bash naming convention (no plugin loader — matches the engine's existing shell-native style):
  - `agent_invoke(prompt_file, safety_file, model, fallback_model, log_file) -> exit_code` — runs
    the CLI, writes a parseable log. *How* safety text gets in and what the log format is are
    entirely the adapter's own business.
  - `agent_classify_outcome(log_file, exit_code) -> success | usage_limit[+reset_epoch] | error` —
    agent-specific parsing of that adapter's own log format.
- **Only `bin/agents/claude.sh` ships in this spec** — today's exact existing logic (the `claude -p`
  invocation + `is_usage_limit_hit` + `extract_reset_epoch`), relocated, not rewritten.
- A `bin/agents/codex.sh` is a documented future follow-up (see Open follow-ups) — not built,
  not tested, not claimed to work, until someone actually validates it against real Codex usage.

**Split of responsibility for the reset-epoch invariant (Codex finding — today's persisted-reset
mechanism must not get lost in translation):** the adapter's `agent_classify_outcome` only
*extracts* a reset epoch from its own log format and returns it as part of the outcome — it does
NOT write `.last_usage_reset` itself. `supervisor.sh` (generic, unchanged from today) owns
persisting it to `$AUTONOMY_TARGET_REPO/var/autonomy-logs/.last_usage_reset` and all of the existing
`compute_limit_wait`/fallback-backoff logic that reads it back. This split matters because the
no-event-this-time-but-still-blocked fallback (today's `rc != 0 && compute_limit_wait` check) is
supervisor-level policy, not agent-specific — it must keep working the same way regardless of which
adapter is active.

## Merge-gate strategies

**CI-green check, generalized with a fail-safe fix (Codex finding — today's `gh pr checks ... ||
echo '[]'` silently treats a `gh` API failure the same as "zero checks configured," and both look
"green" to the existing fail/pending grep).** The generalized check now distinguishes three
outcomes, every strategy except `manual`:
- Any check failing/pending → refuse (unchanged from today).
- `gh` call itself fails (network/auth error) → **refuse**, logged distinctly as "cannot verify CI
  state — refusing rather than assuming green." Never silently treated as green.
- Zero checks configured at all → **refuse** for `ci_only` specifically (logged: "`ci_only` requires
  at least one configured check; use `manual` for a repo with no CI, or add one"). For
  `bot_comment`/`gh_review`, zero checks is not fatal on its own — the approval-signal check is
  the real gate for those strategies.

On top of the CI check, `safe_merge.sh` dispatches on `merge_gate.strategy`:

| Strategy | Check | Notes |
|---|---|---|
| `manual` (default) | none — never auto-merges | Loop opens/iterates PRs, leaves them for a human. Safe default for a freshly-onboarded repo. |
| `ci_only` | CI green (zero checks = refuse, see above), no approval signal | Repos with no review bot at all |
| `bot_comment` | Match `author_login` + `marker` in an issue **comment**, require it postdates the latest commit, require APPROVE language, no `REQUEST CHANGES`/`[BLOCKING]`. Includes the doc-only fast-path (#1863), scoped to this strategy only. | Today's eBull mechanism. Covers any bot that posts a plain text comment (Claude, a Codex-based reviewer, a custom action) — not GitHub-vendor-specific. |
| `gh_review` | Native `gh pr view --json reviews` — take the **latest** review from `reviewer_login` (by `submittedAt`) that postdates the latest commit; require **that specific review's** `state == APPROVED`. An earlier approval followed by a later `CHANGES_REQUESTED` from the same reviewer must refuse — checking "any APPROVED review exists" would accept a stale approval. | Covers GitHub Copilot's PR reviewer and anything else posting real Review objects — the cleaner signal, not a string-grep. |

A misconfigured strategy (e.g. `gh_review` with no `reviewer_login`) is a **hard refuse with a
clear reason** — `safe_merge.sh` never silently falls back to a weaker strategy.

## Onboarding & readiness (`onboard.sh`, `doctor.sh`)

Split of responsibility, deliberately narrow:

- **Not this engine's job:** generating `.claude/CLAUDE.md` or `.claude/skills/**` content. That's
  each project's own engineering substrate — a generic template would be useless or wrong.
  Claude Code already auto-loads these from cwd; the only obligation on the engine side is running
  `claude -p` from inside the target repo's worktree, which preflight already does.
- **This engine's job:** making the `.autonomy/` pack itself turnkey, and surfacing (not fixing)
  every other gap that would make a cold repo silently fail or hang.

`onboard.sh <target-repo>` scaffolds `.autonomy/` from `templates/autonomy-pack/` (commented
`config.yaml` defaults, skeleton `loop_prompt.md`/`hard_rules.md`). Idempotent — never clobbers an
existing pack.

`doctor.sh <target-repo>` (also run as part of `supervisor.sh`'s preflight, cheaply) reports:

| Check | Severity if missing |
|---|---|
| `.autonomy/` present + `config.yaml` valid | hard fail (supervisor won't start without this) |
| `.claude/CLAUDE.md` present | **warn by default; hard fail if `engine.requires_claude_md: true`** (eBull's pack sets this — see config schema). Points at `/init` or the `claude-md-management:claude-md-improver` skill either way. |
| Review-bot workflow present (`.github/workflows/*` matching a `bot_comment`-shaped Action) | **only runs when `merge_gate.strategy == bot_comment`** (Codex finding — this check is meaningless for `manual`/`ci_only`, and wrong for `gh_review`, where the reviewer is a native GH feature/app, not a repo workflow file). Flags the "PRs open, gate never satisfied, loop silently stalls" trap before it happens. |
| `gh auth status` scopes (repo, project) | warn |
| GitHub Projects v2 board matching `config.yaml`'s `project_title` under `board.owner` | warn — board.sh is silent-best-effort by design; this is for the confused-operator case |
| Branch protection on `main` | warn — `safe_merge.sh` is the *local* gate; the intended real gate is GH branch protection + required checks |

All diagnostic, read-only. No auto-provisioning of GitHub state in this spec.

**`board.sh` owner-type fix (Codex finding — today's GraphQL query hardcodes `user(login:$o)`,
which returns null for an org-owned project board):** the generalized `board.sh` tries
`user(login:...)` first; if that resolves no matching project, it retries with
`organization(login:...)`. No new config key needed — auto-detected, same best-effort/warn-only
failure mode as today if neither matches.

## Error handling

- `.autonomy/` missing/malformed → hard fail, loud, in both `doctor.sh` and `supervisor.sh` preflight.
- `.claude/CLAUDE.md` missing → warn only by default; hard fail if the pack sets
  `engine.requires_claude_md: true` (eBull's pack does).
- Merge-gate misconfigured → refuse with a clear reason, never fall back to a weaker strategy.
- Review bot never posts under `bot_comment`/`gh_review` (workflow/secret missing) → correct
  existing behavior, unchanged: refuse forever. Fail-safe, not fail-open.
- `gh pr checks` API call itself fails → refuse, distinct log message from "checks failing" —
  never silently treated as green.
- `--repo` path invalid/doesn't exist → `supervisor.sh` hard-fails immediately.
- Two target repos resolve to the same `{repo-slug}` → `setup_worktree.sh` refuses rather than
  silently overwriting the other's plist/worktree; operator sets `engine.label` to disambiguate.
- Dirty tree / rebase-in-progress / usage-limit / rate-limit parsing → unchanged, ports verbatim
  (already fully repo-agnostic today).
- `board.sh` → unchanged best-effort/warn-only (never blocks engineering work).

## Testing

- Port verbatim (already repo-agnostic today): `test_preflight_recovery.sh`,
  `test_unblock_dependents.sh`, `test_usage_limit_reset.sh`.
- Update: `test_safe_merge_doc_only.sh` — doc-only logic now lives inside the `bot_comment`
  strategy, not top-level.
- New: `config.yaml` parsing/validation; one test per merge-gate strategy (mocked `gh` calls);
  `doctor.sh` checks (each independently testable against a fixture repo dir); `onboard.sh`
  scaffolding (idempotent — confirm it never clobbers an existing pack); `agent_invoke`/
  `agent_classify_outcome` dispatch resolves to the right adapter file for a given `agent.type`.
- Acceptance step (manual, not automated): run one real session against eBull through the new
  engine post-cutover; confirm lock/log/preflight behavior is unchanged from today.

## Cutover (eBull)

Included in this spec's scope (loop is not currently bootstrapped per operator's own notes — no
live process to disrupt, low-risk moment to do this once):

1. Create `autonomy-engine` GitHub repo, populate `bin/`, `templates/`, `README.md`, `tests/`.
2. Add eBull's `.autonomy/` — loop_prompt.md moved with its path rewrites applied (see Path
   rewrites) plus one added line per Merge-gate section; hard_rules.md extracted from the
   duplicated SAFETY string with its own path reference rewritten; config.yaml with eBull's actual
   values — `merge_gate.strategy: bot_comment` (matching today's real mechanism exactly),
   `engine.requires_claude_md: true`.
3. Delete `scripts/autonomy/*` from eBull.
4. Re-point the launchd plist at the engine repo (`setup_worktree.sh` regenerates it).
5. Run `doctor.sh` against eBull — expect all green (eBull already has CLAUDE.md, skills, the
   review workflow, a `gh` token with the right scopes).
6. Manual acceptance run (see Testing).

## Open follow-ups (not this spec, captured so they aren't lost)

- `bin/agents/codex.sh` — a Codex adapter, once someone validates its rate-limit signal shape and
  safety-prompt-injection approach against real usage. The interface (`agent_invoke` /
  `agent_classify_outcome`) is already shaped to accept it without touching `supervisor.sh`.
- **Shared usage-limit state, once multiple loops share one Claude account (Codex strategic-fit
  finding).** `.last_usage_reset` is persisted per-target-repo today — correct for this spec's
  single-instance scope, but the underlying constraint (one Anthropic account's rate limit) is
  account-global, not repo-specific. With N loops each independently discovering the same wall, each
  would sleep and retry on its own schedule, all piling back in around the same reset time (a
  stampede) instead of one shared wait. The registry spec should replace the per-repo file with a
  shared, engine-level reset marker keyed by account/credential, not by repo.
- Registry/control-unit: multi-repo launch/stop, one control surface.
- Dashboard control-lever API: an operator's own Claude/Codex session talking to the supervisor
  through the dashboard (pause, change model, query "why did you skip X") — needs a control API on
  the supervisor side, not just log-tailing.
- Auto-provisioning of cold-repo GitHub state (review workflow file, branch protection, board,
  secrets) — `doctor.sh` diagnoses today; provisioning is a later, separate decision (mutates
  GitHub state, needs its own security review).
