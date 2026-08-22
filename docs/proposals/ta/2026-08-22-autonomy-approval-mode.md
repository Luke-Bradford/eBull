# `approval_mode` — the autonomy flag and its first headless approver (#2843)

Status: proposed, 2026-08-22. Prerequisites #2844 (`a23c9628`) and #2829 (`103675ac`) are merged.

## What the person-gate actually is

Measured, not recalled:

```
grep -rn 'advance_strategy(' app/ scripts/ | grep -v 'def advance_strategy'
  app/api/strategies.py:3542
```

`advance_strategy` (`app/services/strategy_operator_promotion.py:538`) assembles every
piece of evidence itself — the 24-combination hold-out matrix, the identity pins, the
prospective assessment, the weakening check — and takes from its caller only
`(strategy_id, action, advanced_by, reason, as_of)`. Its one production caller is
`POST /strategies/{id}/advance`, which stamps `advanced_by=session.username` behind
`Depends(require_session)`.

**So the person-gate is: the only path into the stage machine is an authenticated HTTP
request, and the username it stamps is the approval.** Nothing else. `decide_funding`
is already headless; `record_live_promotion_attempt` is separately blocked on the broker
contract (`strategy_live_gate.py:782`) and is out of scope here.

That is what makes this slice small: the flag touches **no gate**. It supplies a
**second caller**.

## Source rule

Not an SEC/market-data decision — no external formulation governs it. The governing rule
is the operator's own settled decision (`docs/settled-decisions.md`, 2026-08-22,
"Live-capital approval is a mandate FLAG, not a person-gate"): *the flag flips WHO
approves, never WHAT qualifies.* Every constant below is fixed by construction and
frozen in `AUTONOMY_POLICY_VERSION`.

## Full-population state (dev, 2026-08-22)

```sql
select count(*) from strategy_paper_pool_events;   -- 0
select count(*) from strategy_promotions;          -- 0
```

Zero rows in both. There are no legacy events to accommodate, no `promoted_by` values to
reconcile, and no promotion this change can retroactively reclassify. The migration can
therefore be strict rather than legacy-tolerant.

## Schema — `sql/365`

`approval_mode` goes on `strategy_paper_pool_events`, beside `capital_mode`,
`capital_limit` and the `risk_profile` mandate columns added by `sql/311`. It is not a
new table and not a new capital surface: #2844's lesson was that minting a parallel
vocabulary for something already stored is the defect, and the mandate is already stored
here.

```sql
ALTER TABLE strategy_paper_pool_events
    ADD COLUMN IF NOT EXISTS approval_mode TEXT NOT NULL DEFAULT 'manual';

-- DROP-then-ADD, matching sql/311's own pattern: a bare ADD CONSTRAINT is not
-- rerunnable and a replayed migration would fail on the existing name.
ALTER TABLE strategy_paper_pool_events
    DROP CONSTRAINT IF EXISTS strategy_paper_pool_approval_mode;
ALTER TABLE strategy_paper_pool_events
    ADD CONSTRAINT strategy_paper_pool_approval_mode CHECK (
        approval_mode IN ('manual', 'autonomous')
        -- An unconfigured mandate authorises nothing, so it cannot authorise a
        -- policy approver either.  `risk_profile <> 'unconfigured'` is the exact
        -- definition of `PortfolioMandate.configured`, and it is only as strong
        -- as sql/311's `strategy_paper_pool_mandate_shape`, which is what forces
        -- a non-`unconfigured` profile to carry complete v1 limits.  Declared
        -- dependency: this constraint is meaningful only while that one stands.
        AND (approval_mode = 'manual' OR risk_profile <> 'unconfigured')
    );
```

Default `manual`, per the ticket ("likely for people testing it first"). Append-only, so
the flag is versioned by the table's own shape — the authority in force at any promotion
is the latest event at or before it.

⚠ The default is what makes this safe on a populated database, and the dev row count is
not the argument. sql/311 deliberately preserves legacy `enabled` + `unconfigured` rows;
every one of them takes `approval_mode = 'manual'` and satisfies the new CHECK unchanged.
Only a NEW row can be `autonomous`, and a new row goes through `configure_paper_pool`.

⚠ "Latest by event id" is the authority, and that is safe here rather than by assumption:
`configure_paper_pool` takes `PAPER_ALLOCATOR_ADVISORY_LOCK` for the whole transaction, so
two revisions cannot commit out of id order.

## Service

### 1. The flag — `strategy_control_plane.py`

- `ApprovalMode = Literal["manual", "autonomous"]`, beside `CapitalMode`.
- `PaperPool.approval_mode: ApprovalMode = "manual"`; read in `load_paper_pool`.
- `configure_paper_pool(..., approval_mode: ApprovalMode)` — **required keyword, no
  default.** ⚠⚠ A default would make every unrelated capital or risk edit silently
  revoke autonomy: the existing `PUT /strategies/paper-pool` does not pass the field, so
  the next capital-limit change would write `manual` over an operator's `autonomous`
  without saying so. Requiring it forces each caller to decide, and the type checker
  finds every one.
- Validated, included in the material-change comparison (otherwise flipping only the flag
  raises "must alter …"), and written in the INSERT. ⚠ Three positional lists to keep
  aligned (column list, placeholders, tuple) per the #2623 prevention entry.
- Refused in the service, not only by the CHECK: `autonomous` with an unconfigured
  mandate raises `StrategyControlError`. `PaperPool` is publicly constructible, so the
  DB constraint is the backstop and not the mechanism.

### 1b. The API — `PUT /strategies/paper-pool`

`StrategyPaperPoolUpdateRequest.approval_mode: ApprovalMode | None = None`, where
**`None` means "carry the current value forward"**, resolved from `load_paper_pool`
inside the same locked transaction. Omission is the common case for every existing
client and for every edit that is not about approval; it must mean *unchanged*, never
*reset to manual*. The resolved value — not the request field — is what the
material-change comparison and the INSERT see.

The rule is `strategy_control_plane.resolve_approval_mode(requested, current)`, a named
pure function rather than an inline ternary, because the wrong spelling is a
one-character difference with no test surface. Table-tested; revert-probed by inverting
it to `"manual" if requested is None`, which fails
`test_resolve_approval_mode[None-autonomous-autonomous]`.

⚠ `require_session` stays. Flipping the flag to `autonomous` is itself an operator
authorisation and is the last one the operator makes; a service token has no identity to
attribute it to.

### 2. The reader — `app/services/strategy_autonomous_promotion.py` (new)

Shipped in the same change, per the prevention entry on declared-but-unwired symbols
(log §"A declared-but-unwired symbol is a claim with no subject"). A flag with no reader
is the defect this repo keeps shipping.

- `AUTONOMY_POLICY_VERSION = "autonomy-v1"`.
- `AUTONOMOUS_APPROVER = f"policy@{AUTONOMY_POLICY_VERSION}"` — persisted as
  `strategy_promotions.promoted_by`, which is the audit the ticket asks for
  (`approved_by: policy@<flag-version>`). The constant has a persister on this branch;
  `grep -rn AUTONOMOUS_APPROVER app tests` returns the writer, not only the definition.
- `run_autonomous_promotion_cycle(conn, *, as_of) -> AutonomousPromotionReport`.

Preconditions, read once per cycle from one `load_paper_pool`:

| condition | outcome |
| --- | --- |
| `approval_mode != 'autonomous'` | whole cycle skips, code `approval_mode_manual` |
| mandate unconfigured | whole cycle skips, code `mandate_unconfigured` |
| `not pool.enabled` | whole cycle skips, code `paper_pool_disabled` |

The third is a decision, not an oversight. The flag is an attribute of a capital
authority; an operator who has disabled the pool has withdrawn the authority the flag
qualifies, and a policy approver advancing strategies toward `paper_enabled` against a
withdrawn authority is the wrong direction to fail in.

Then, per manifest strategy with a current result version, **each in its own
transaction**:

1. `allowed_operator_action(current_stage(...))`. `None` (terminal) → skip.
2. **Only `_EVIDENCE_ACTIONS`.** `register_research_candidate` carries no evidence, so a
   policy that acts on evidence has nothing to act on; registration stays manual. Skip
   code `action_not_evidence_backed`.
3. `advance_strategy(..., advanced_by=AUTONOMOUS_APPROVER, reason=<generated>)`.
   `StrategyControlError` is **recorded, not raised** — a refusal is this job's normal
   output, exactly as `strategy_signal_scan._commit_strategy` treats a per-strategy
   failure.

⚠ **One transaction per strategy, not one per cycle.** A cycle-wide transaction makes an
unrelated failure on the last strategy roll back every promotion before it, and makes the
manifest iteration order decide who advances. Per-strategy commits also mean the report
describes what is durably true.

**At most one step per strategy per cycle** — hygiene, not a safety invariant, and it is
worth being blunt about which. It keeps one report line per strategy and stops a tick
acting on a stage it just created. It does **not** bound elapsed time: manual dispatch,
catch-up and a cadence change all defeat it.

What actually bounds forward observation is already in the code and is not this
caller's to add: `select_prospective_assessment` refuses
`prospective_assessment_predates_forward_observation`, so `paper_enabled` is unreachable
until an assessment is computed *after* forward observation began.

⚠ **No minimum stage-dwell constant is introduced, deliberately.** The first draft of
this spec proposed one. It has no construction: all six `RECENT_EVIDENCE_WINDOWS` end at
or before `INTRADER_CAPTURE_DATE` (2024-09-27), so the historical matrix is frozen and
elapsed time cannot change it. A dwell threshold would have been an invented constant
guarding a quantity that does not move — the exact failure "source-rule before design"
exists to catch.

The cycle drives at most as far as `paper_enabled`. `live_enabled` is refused outright by
`promote_strategy` and belongs to the measured live gate; this change does not touch it.

The generated `reason` is deterministic and bounded:
`f"autonomous advance under approval_mode=autonomous (pool event {event_id}, {AUTONOMY_POLICY_VERSION})"`.
`strategy_promotions.reason` is `TEXT` with only a `<> ''` CHECK (`sql/281:37`), so there
is no length cliff; the bound is for legibility.

### 3. The glue — `strategy_autonomous_promotion` job

`app/workers/scheduler.py`, daily. No broker call and no external lane — DB only, so it
does not go in `strategy_execution`, which `strategy_paper_cycle` holds every five
minutes (a daily job landing in a busy lane is a daily job that skips; the #2603 job
records the same reasoning).

`tracker.note` carries `advanced=<n> skipped=<n>` plus the **distinct refusal codes**, so
a cycle that advanced nothing says *why* rather than reporting a bare zero. A per-strategy
refusal table is deliberately not minted: a refusal is the steady state here, so that
table would be a heartbeat log, and `strategy_promotions` already records every advance
that happened.

An unexpected error (not a `StrategyControlError`) propagates. A job that no-ops and
reports success is invisible to every automated check this repo has.

## What does NOT change

- The execution guard, kill switch, `EXIT` never blocked, `sandbox_exceeded` (#2844).
  None of them is reachable from this diff.
- Every evidence bar inside `advance_strategy` / `promote_strategy`. This change adds a
  caller and reads a flag; it removes no check and relaxes no threshold. The autonomous
  caller supplies no `to_stage`, no `strategy_version` and no `result_ids` — the three
  inputs #2770 removed from the caller's reach — so there is no parameter through which a
  policy could choose its own denominator.
- `POST /strategies/{id}/advance` keeps `require_session`. Manual approval remains
  available under either flag value.

## Tests

Pure-logic first, per the repo's lean-test rule:

- `approval_mode` validation table (`manual`/`autonomous` × configured/unconfigured
  mandate × enabled/disabled).
- The cycle-precondition function as a pure mapping from a `PaperPool` to `None` or a skip
  code, so all three refusals are table-tested without a DB.
- The per-strategy step planner as a pure function over `(stage, purpose)` → action or
  skip code, so the evidence-actions-only rule is table-tested without a DB.
- One DB test: a strategy at `historical_validated` with a complete matrix advances
  exactly one stage under `autonomous` and zero under `manual`, and the real
  `strategy_promotions` row carries `promoted_by = 'policy@autonomy-v1'`. ⚠ Asserted
  against the stored row, not against arguments passed to a mock — an argument-equality
  test proves the call shape and nothing about the evidence actually assembled.
- One DB test that the cycle records a refusal rather than raising when the matrix is
  incomplete, and that no promotion row is written.
- One DB test that omitting `approval_mode` on `PUT /strategies/paper-pool` carries an
  existing `autonomous` forward instead of resetting it.

## Out of scope (named, not forgotten)

- **The alert-validity contract** (§ on #2843) is independently buildable. Splitting it
  keeps this diff to flag + approver, which is the pairing the prevention entry requires
  be shipped together.
- **Per-strategy / per-version autonomy scope.** There is one pool and one mandate, so
  authority is account-wide by construction; a per-strategy allowlist would be a second
  authority surface, which is the #2844 defect. Raised by Codex ckpt-1 and rejected on
  that ground, not overlooked.
- **How long forward observation must last.** `checked_at >= forward_started_at` permits
  an assessment computed one second after the promotion, so the effective floor is one
  assessment-job cadence. Pre-existing, identical on the manual path, and widening this
  ticket to fix it would turn a flag into a governance redesign. Noted on #2843.
- **`current_result_versions()` is code-derived**, so a deploy can change which version
  the cycle has authority over without a mandate revision. Also pre-existing and
  identical on the manual path.
