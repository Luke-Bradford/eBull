# Bind a preregistration declaration to the trial that counts it (#2829)

Status: proposed, 2026-08-22. Codex checkpoint 1 run; it moved the enforcement point, and
the reasons are recorded below rather than silently absorbed.

## The gap, stated precisely

#2829's escalation names it: *"every deflation argument in the sweep cites 'search
#275/#276' against a register that holds 5 rows. The search count that converts a
marginal pass into noise is currently an assertion, not a ledger."*

The issue already warns that two registers are being conflated, and it is right — but the
conflation is not only rhetorical. **The two are not joinable at all**, so "search #275"
cannot be checked mechanically by anything.

| | what it is | where | today |
| --- | --- | --- | --- |
| `TRIAL_REGISTER` | criterion 6's `M`, feeding the DSR's `E[max SR]` | `app/services/trial_register.py`, code-frozen | 30 trials, `declared_count = 274` |
| `strategy_preregistration_declarations` | the per-trial purpose freeze `require_outcome_access` gates on | `sql/333`, DB | 5 rows |

A spike can freeze a declaration, run, and be deflated against an `M` that never counted
it.

## Measured before speccing (dev DB + the shipped register, 2026-08-22)

All five declarations **do** have a corresponding trial — but the key is inconsistent, and
that is the finding:

| declaration `strategy_id` | declaration `strategy_version` | matching `trial_id` | key that matched |
| --- | --- | --- | --- |
| `c4-schedule13d-public-catalyst` | `schedule13d-public-catalyst-v1` | `c4-schedule13d-public-catalyst-v1` | neither exactly — `strategy_id + "-v1"` |
| `form4-code-p-opportunistic-purchase` | `form4-code-p-opportunistic-purchase-v1` | `form4-code-p-opportunistic-purchase-v1` | `strategy_version` |
| `pead-historical-sue-net-income` | `pead-historical-sue-net-income-v1` | `pead-historical-sue-net-income-v1` | `strategy_version` |
| `mt1-capped-volatility-managed-relative-strength-v1` | `strategy-registry-v1+32970feefa00` | `mt1-capped-volatility-managed-relative-strength-v1` | `strategy_id` |
| `mt1-s8-capped-volatility-negative-control-v1` | `strategy-registry-v1+b83c3e4fc997` | `mt1-s8-capped-volatility-negative-control-v1` | `strategy_id` |

Three different join rules across five rows. **There is no key to infer**, which is why
this ships as an explicit declared mapping rather than a naming convention — a convention
already violated three ways cannot be enforced retroactively, and it would break the
moment a `strategy_version` became a registry hash, which two of the five already are.

Reproduce:

```
select strategy_id, strategy_version, prereg_purpose from strategy_preregistration_declarations order by 1;
PYTHONPATH=. uv run python -c "from app.services.trial_register import TRIAL_REGISTER; print(sorted(TRIAL_REGISTER.trial_ids))"
```

Also measured, because it bounds the design:

```
select trial_register_version, count(*) from strategy_results_store group by 1;
 trial-register-2026-08-15-r7 | 220      trial-register-2026-08-12-r5 |  64
 trial-register-2026-08-07    | 108      trial-register-2026-08-11-r3 |  48
 (null)                       |  80      trial-register-2026-08-10    |  48
```

## Scope

1. `DeclaredTrial` gains `declares: tuple[tuple[str, str], ...] = ()` — the
   `(strategy_id, strategy_version)` pairs whose preregistration declaration this trial's
   searches account for. Empty is legitimate and common: many of the thirty trials are
   research *sessions* (`short-horizon-search-session-2026-08-09`,
   `autocorrelation-term-structure-2026-08-09`, …) that no declaration corresponds to.
2. **Cardinality, stated because it was not obvious:** one trial MAY claim MANY pairs (a
   grouped family declared once); a pair is claimed by AT MOST ONE trial. Validated in
   `__post_init__` — a pair on two trials would double-count that declaration's searches
   in `M`, silently and in the flattering direction. A pair repeated *within* one trial's
   tuple is refused too, or the naive cross-trial check would let it through.
3. Each pair's elements are validated non-blank and ≤ 200 characters, mirroring
   `sql/333`'s `strategy_preregistration_declaration_identity` CHECK — a pair the table
   could never hold can never match a row, so it is a typo, not a mapping.
4. `TrialRegister.trial_for_declaration(strategy_id, strategy_version) -> DeclaredTrial |
   None`.
5. **`freeze_preregistration` refuses a declaration no trial claims** — freeze time only.
6. The five existing declarations mapped onto their trials, and a test asserting every
   stored declaration's pair is claimed.

## Source rule

None of these is a formulation with a published source; each is fixed by construction and
the construction is stated. The governing in-repo rule is criterion 6 as
`trial_register.py` states it — `M` must not under-count in the flattering direction — and
every choice below is the one that cannot move `M` down.

### ⚠⚠ The gate is at FREEZE TIME, not in `declaration_refusals`

The first draft put a new `trial_not_in_register` member in `DeclarationRefusal`. Codex
checkpoint 1 killed it, correctly, and the reasons are worth keeping because each is a
path a reader would otherwise assume was considered:

- `declaration_refusals` is **pure** and is called with synthetic identities by
  preregistration, supersession, live-gate, C4, MT1 and pre-cutoff fixtures. Consulting a
  module-global register makes every one of those incoherent unless its identity is added
  to production register data.
- `supersession_refusals` calls `declaration_refusals(successor)`. A declaration stranded
  by a structural-policy bump could then not use the **repair path that exists for exactly
  that situation** — a new wedge, created by the fix.
- `strategy_live_gate` calls it on registration *and on reassessment*, so an already
  registered, immutable policy could become unusable because current code lacks a mapping.
- It would make a code edit **retroactively invalidate** frozen artefacts. `sql/333`'s own
  header is headed "⚠ NO RETROACTIVE INVALIDATION".

Freeze time has none of those properties: it touches nothing already frozen, cannot strand
a supersession, and refuses only the one act the ticket actually wants gated — *"does the
next strategy version freeze a declaration before its first hold-out look?"*

There is an exact precedent at the same seam. `freeze_preregistration` already carries a
freeze-time-only check (#2720's cost-model stamp test) whose comment says why it is not in
`__post_init__`: *"that class is also the read-back of stored rows, and rows frozen under
an earlier cost model legitimately declare stamps today's constants do not produce."* The
same sentence applies here word for word.

It raises `ValueError` with an explanatory message, matching that neighbouring check,
rather than adding a `DeclarationRefusal` member. That keeps the refusal vocabulary — and
every caller, audit payload and exact-tuple assertion that enumerates it — untouched.

### ⚠ What this does and does not prove

Prospectively it is a ledger: from this change on, a declaration cannot be frozen unless
`M` already names the search it represents.

Retrospectively it is **not** a proof, and the spec says so rather than letting the table
above imply it. The five mappings establish that the two registers agree **today**, by
name and by reading each trial's `evidence` field. They cannot establish that the register
counted that search *at the time the look occurred* — a mapping added after exposure looks
identical to one that was always there. Nothing in this change should be cited as evidence
about a past run's deflation.

⚠ The duplicate-claim invariant is likewise one-sided by construction: it forbids an
*inflated* `M`, while the motivating defect is an *understated* one. The understated
direction has no invariant available at the register — only the freeze gate closes it, and
only for declarations frozen after this lands.

### The mapping lives on the REGISTER side, not the declaration

`PreregDeclaration.sha256` hashes `digest_payload`, and `sql/333` stores that digest on
every frozen row. **Adding a field to the declaration would change the digest of all five
frozen rows**, which is the one thing a frozen artefact may not do. The register is code,
is versioned, and already carries a trial's provenance — so the pointer goes there.

Identity is `(strategy_id, strategy_version)` because that is `sql/333`'s own
`strategy_preregistration_declaration_unique`. `contract_version` is deliberately not part
of it, for the same reason.

Consequence, stated rather than discovered later: `declares` names a `strategy_version`,
and a strategy's version moves whenever its module hash moves. A new version is a new
trial and needs its own register entry before its declaration can be frozen. **That is the
discipline #2829 asks for, arriving as a mechanical refusal rather than a convention.**

### `TRIAL_REGISTER_VERSION` is NOT bumped, and that is the load-bearing call

The constant's contract is *"bumped whenever a trial is added or an entry's meaning
changes"*, and it is stored beside every DSR because *"a deflated Sharpe means nothing
without the trial population it was deflated against."*

This diff adds **no trial**, changes **no `searches` value**, and leaves `declared_count`
at **274** and `len(trials)` at **30**. The population `M` is unchanged, so every DSR
computed under `trial-register-2026-08-15-r7` remains exactly correct.

⚠ Bumping anyway is not the safe default here — it is the destructive one. 220 stored
results carry r7, and `strategy_result.py:1311-1314` refuses `trial_register_superseded`
whenever `deflated.trial_register_version != TRIAL_REGISTER_VERSION`. A bump would flip
all 220 to refused for a change that moved nothing they depend on.

⚠ Codex's counter — *"reusing the same register version for a changed binding artefact is
not defensible"* — is noted and rejected on what the field is FOR. It is stored on result
rows to answer "which population was this deflated against", and `strategy_result.py`
compares it against `declared_count`, not against the module's bytes. A binding that
changes neither is outside what the version claims. Treating the constant as a source hash
would make it a different thing than its docstring says, and would do so by mass-refusing
220 rows.

To keep that argument checkable rather than asserted, the tests pin `declared_count == 274`
and `len(trials) == 30` as literals. Any future edit that actually moves `M` fails them and
forces the bump conversation at the right moment.

## Deliberately NOT in scope

- **Backfilling declarations for s1–s10.** #2829 is explicit that this would be governance
  theatre — the hold-out has already been looked at, which is what
  `supersession_trial_already_exposed` refuses. The ten stay undeclared; the next
  `strategy_version` declares.
- **Any change to `searches`, `exactness` or `M`.**
- **Register entries for the six R5 spikes.** Each freezes its own declaration and appends
  its own trial when it runs; this change makes forgetting either half a refusal instead
  of a silent under-count.

## Acceptance

- `freeze_preregistration` raises on a declaration whose pair no trial claims, naming the
  pair and pointing at the register.
- Every one of the five stored declarations is claimed by exactly one trial, checked
  against the real register and the real rows.
- A register declaring one pair on two trials, or the same pair twice within one trial,
  raises at construction.
- `declaration_refusals` is byte-for-byte unchanged, and returns `()` for all five stored
  declarations exactly as before.
- `TRIAL_REGISTER.declared_count == 274` and `len(TRIAL_REGISTER.trials) == 30`.

## Refs

Refs #2829. Refs #2599. Refs #2600. Refs #2832. Refs #2437.
