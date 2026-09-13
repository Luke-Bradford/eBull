# Wiring the feasibility screen: the latest-proof loader and its caller (#2947)

Status: proposal — **post-Codex-ckpt-1 revision. The first draft's scope rule was wrong and
is reversed below.** Refs #2947, #2833 (the sleeve this is applied to), #2603 (the proofs
table and its recorder), #2946 (the write-clock coupling the recorder's timestamps show).

Follows `2026-09-13-portfolio-feasibility-screen.md`, which shipped the pure screen at
`6ce7dcf0` and left this half explicitly open:

> **No caller.** Nothing yet invokes the screen before an outcome run … **No loader.** The
> caller must select the *latest* proof per `(instrument, environment)` and must not prefer
> an older pass over a newer failure.

## Scope

**Ships:** a loader projecting `strategy_core_eligibility_proofs` into the screen's
`LegEligibility`, an account-scope resolver, a runnable caller emitting a dry-run artifact,
pure tests, and **a DB test over deliberately conflicting rows**.

**Does NOT ship enforcement**, and the first draft's justification for that was a false
equivalence — it argued that a screen able to block an outcome run would be "an authority"
in the sense the screen's docstring forbids. It would not: blocking expensive *research*
confers no trading authority. The honest reasons are narrower:

- #2947's acceptance asks for *"a reviewable dry-run artifact"*, not a gate;
- an optional standalone script establishes **no ordering** relative to an acceptance run.
  So after this lands the screen is **runnable and evidenced**, and the original
  *"before an outcome run"* gap is **still open**. The close-out must say that in those
  words. Closing it means either a documented workflow step that is checked, or changing
  the `evaluate_*` scripts — a separate ticket either way.

**Does NOT run a live re-census** (needs a broker call; it is the precondition the prior
close-out identified, not a deliverable here).

## Source rule — selection ordering, and the SQL the first draft got wrong

`load_latest_core_eligibility_proof` (`strategy_core_eligibility.py:316`) already answers
"newest observation for this instrument on this account", ordering
`observed_at DESC, core_eligibility_proof_id DESC`.

⚠ **Copying that ordering literally into a batched query does not compile.** `DISTINCT ON`
requires the leading `ORDER BY` expressions to match the `DISTINCT ON` list. The correct
batched form is:

```sql
SELECT DISTINCT ON (p.instrument_id)
       p.instrument_id, p.core_eligibility_proof_id, p.observed_at,
       p.verdict, p.reason_code, p.response_currency, p.qualifying_arm_count,
       p.allow_open_position, p.min_position_exposure, p.min_position_amount,
       p.operator_id, p.provider, p.environment,
       p.api_key_credential_id, p.user_key_credential_id
  FROM strategy_core_eligibility_proofs p
 WHERE p.instrument_id = ANY(%(instrument_ids)s::bigint[])
   AND p.operator_id = %(operator_id)s
   AND p.provider = %(provider)s
   AND p.environment = %(environment)s
   AND p.api_key_credential_id = %(api_key_credential_id)s
   AND p.user_key_credential_id = %(user_key_credential_id)s
 ORDER BY p.instrument_id, p.observed_at DESC, p.core_eligibility_proof_id DESC
```

`environment` leaves the `DISTINCT ON` key because it is now a fixed equality predicate;
keying on it as well would be redundant. ⚠ `::bigint[]` cast is required — a nullable-shaped
parameter without one raises psycopg3 `AmbiguousParameter`.

### What the tiebreak is and is not for

- `now()` **is** transaction-start time — **verified empirically**, not recalled: inside one
  transaction two `now()` reads are equal while `clock_timestamp()` advances. So a
  single-transaction batch writer *would* stamp every row identically and `observed_at`
  alone could not order them.
- ⚠ **But that is not how these rows were written, and the first draft asserted it as if it
  were.** Measured on the dev DB: **23 rows, 23 distinct `observed_at`**, spaced ~3.5 s
  apart, because `prove_2603_core_eligibility.py` records each instrument in **its own
  transaction**. There are **zero** `(instrument, environment, observed_at)` duplicates
  today. The tiebreak is insurance against a writer shape that is not a contract — keep it,
  but do not claim it is resolving ties that exist.
- ⚠ **Deterministic is not the same as chronologically latest.** `observed_at` is
  transaction *start*, so a transaction that began earlier and committed later can record a
  genuinely later observation that still loses on timestamp. The ID tiebreak cannot repair
  unequal timestamps. This is a property of the recorder's clock choice (#2603's, made so a
  caller cannot extend a proof's validity by supplying its own time), inherited here and
  named rather than fixed.

⚠ Incidental, one line only: the ~3.5 s spacing is the **order-write throttle** — the
eligibility census rode `_http_write` despite eligibility having its own dedicated 20/min
budget, which is exactly #2946's finding 2, visible in stored data. Also, the endpoint
accepts up to 100 ids per request and the census made one request per instrument. Neither
is in scope here.

## The scope rule — REVERSED from the first draft

The first draft loaded the newest row **regardless of credentials** and let the screen's
`_scope_matches` report a mismatch, arguing that filtering would hide the more informative
diagnosis. **Codex refuted this and it was right.**

"Latest wins, never prefer an older pass over a newer failure" is a rule **within one
account**. A row observed under a *revoked* credential pair is not a newer failure for this
account — it is an observation of a **different account**, and letting it shadow a valid
in-scope proof is cross-account interference. `prove_2603_core_eligibility.py` caches
credentials, so a late write carrying old credential ids is plausible, not theoretical.

**So the loader filters on the full scope** (operator, provider, environment, and both live
credential ids) and selects the latest *within* it.

The informative diagnosis is preserved without corrupting the screen's inputs: the loader
**also** returns a separate, non-screen diagnostic — for each requested instrument with no
in-scope proof, whether any **out-of-scope** proof exists and its newest `observed_at`. That
goes in the artifact's evidence envelope, never into the `eligibility` mapping. The operator
then sees "no proof for this account, but N rows exist under other credentials" — which is
"investigate a credential swap", not "run a census".

### Key identity must be enforced by the loader

⚠⚠ **Codex reproduced this: the screen does NOT check that the mapping key equals the
proof's own `instrument_id`.** A mapping `{1: proof(instrument_id=999)}` returns `feasible`.
The screen looks up by key and trusts it. So the loader keys strictly by the row's own
`instrument_id`, and a test asserts key–proof identity. Left to convention this is a silent
wrong-answer path.

### Scope resolution, and why NOT to reuse the locking reader

`operator_id` ← `operators.sole_operator_id` (raises on zero or >1).

The live credential pair: `strategy_core_eligibility._live_credential_ids` resolves it, but
takes **`FOR SHARE`**. The first draft said to reuse it anyway because "a short share lock is
cheap". ⚠ That is wrong in two ways Codex named: `FOR SHARE` cannot run in a read-only
transaction, and it **blocks a concurrent revoke until the transaction ends** — an advisory
research script has no business holding that.

So: extract the shared `SELECT` predicate, and expose **two** readers over it —
`live_credential_ids` (locking, for the write path, behaviour unchanged) and
`live_credential_ids_unlocked` (no `FOR SHARE`, for advisory readers). One definition of
"live", two lock postures, both named. This is the smallest change that avoids a *second*
definition of the contract — the "field wired into one model not its sibling" class.

⚠ **Defined behaviour when scope cannot be resolved** (the first draft had none): zero or
multiple operators, or a missing/revoked key, **raises before the screen runs**. The script
catches it, writes **no artifact**, prints the reason, and exits with the configuration-error
code. A missing credential is not a feasibility verdict and must not be rendered as one.

## Projection

`LegEligibility` needs `verdict`, `reason_code`, `response_currency`,
`qualifying_arm_count`, `allow_open_position`, `min_position_exposure`,
`min_position_amount`, `observed_at`, and the scope.

⚠⚠ **`verdict` and `reason_code` are required and projected, never inferred.**
`evaluate_core_eligibility` leaves `qualifying_arm_count` at its default `0` for an
`unresolved` proof because it never evaluated an arm; a projection carrying only the count
reports a definitive `refused` where the truth is "the response did not answer the question".

⚠ `allow_open_position` stays `bool | None` — `None` is "not recorded", neither false nor
true. ⚠ The two minimum columns stay separate and un-merged;
`broker_settlement_arms.effective_open_minimum` owns that precedence.

## The caller

`scripts/screen_portfolio_feasibility.py`.

- `--leg ID:WEIGHT` (repeatable) **or** `--equal-weight ID,ID,…` — **mutually exclusive**,
  enforced.
- `--assigned-capital`, `--capital-currency`, `--usd-per-capital-unit`,
  `--cash-reserve-fraction`, `--environment` (default `demo`), `--out PATH`.

⚠ **Numeric handling** (first draft left it unspecified): every numeric argument parses
straight to `Decimal` from the literal string — no float hop, which would silently
renormalise. `NaN` and `Infinity` are **rejected at parse**, not passed through: `Decimal`
accepts both, and PG NUMERIC NaN comparisons do not behave like IEEE, so admitting one
anywhere in this path is a known trap. Malformed ids/weights are configuration errors.

⚠ **Never shrink the candidate.** Symbols come from an **outer** join on `instruments`; an
unknown id is an explicit configuration error, never a silently dropped leg. The screen's
own contract is that it returns a verdict per leg of the caller's list and never a subset —
a loader or a join that drops one would defeat it from underneath.

⚠ **Exit codes** and atomic output: `0` feasible · `1` refused · `2` indeterminate · `3`
configuration/resolution error. The artifact is written to a temp file and `os.replace`d, so
a failed run cannot leave a stale artifact looking like its result.

### The artifact needs an evidence envelope

⚠ `FeasibilityReport` alone **cannot support the verification claims** made of it: it omits
the proof id, the raw verdict/reason, the arm count, the raw minima, the proof's own scope
and policy version, and on an early portfolio-level refusal it carries no legs at all. It
also does not echo the assigned capital or the candidate's identity.

So the artifact is `{"report": …, "evidence": …}`, where `evidence` carries, per requested
instrument: the selected `core_eligibility_proof_id`, raw `verdict` / `reason_code` /
`qualifying_arm_count` / both raw minima / `observed_at` / the proof's policy version, and
the out-of-scope diagnostic above. Plus, portfolio-wide: the resolved scope, the assigned
capital and currency, the declared weights as given, and a `schema_version`.

Encoding is declared, not incidental: `Decimal` → string (never float), `datetime` →
RFC-3339 UTC, `timedelta` → integer seconds, `UUID` → string.

⚠ **Credential and operator ids are redacted in PR/stdout output only, never in the
artifact** — the artifact's whole purpose is later re-checkability, and presence/shape
cannot establish scope equality. The artifact is a local dev-DB file, not a published one.

## Applying it to #2833

`candidate_ids [3417, 3434, 3075]` (`SPY.RTH`, `CSPX.L`, `IUSA.L`) from
`docs/proposals/ta/2026-08-24-core-selection-declaration.json`, whose
`eligibility_account_rule` is *"sole operator, provider etoro, environment demo, and the
current non-revoked api_key and user_key credential ids"* — exactly the scope the resolver
builds.

⚠ **The first draft had the equal-weight argument backwards.** The sleeve selects **one**
instrument, so its portfolio is a single leg at weight 1. Screening three legs at ⅓ each is
**stricter**, not weaker: each leg gets a third of the capital against the same $10 floor, so
three legs need ~$30 where one needs ~$10. It answers "can all three be funded
simultaneously", which is not a question the sleeve asks. **Run the three single-leg screens
for the sleeve**; the three-leg form is available and the artifact records the weights used,
but it must not be read as the sleeve's verdict.

⚠ **The expected result, corrected twice.** The first draft said "every stored proof is 22
days old" and predicted `eligibility_proof_stale` as the portfolio code. Both wrong:
the table holds **08-13 and 08-22** observations, so not every proof is the same age; and
`eligibility_proof_stale` is a **per-leg** code — the portfolio-level code is
`leg_indeterminate`. Expected: portfolio `indeterminate` / `leg_indeterminate`, with each
leg `indeterminate` / `eligibility_proof_stale` against the 24-hour bound.

## Tests

**Pure** (fast tier): row → `LegEligibility` projection, table-tested — `allow_open_position`
true/false/null all preserved distinctly; the two minima kept distinct from each other (not
just non-null); an `unresolved` verdict keeps `reason_code` while `qualifying_arm_count` is
0; **key–proof instrument identity**; equal-weight construction sums to 1 within
`DEFAULT_WEIGHT_TOLERANCE`; duplicate ids rejected; unknown id rejected rather than dropped;
`NaN`/`Infinity` rejected at parse; a missing proof reaches the screen **absent from the
mapping**, never as a fabricated `unresolved` projection.

**DB tier** (`-m db`, one file). ⚠ The first draft refused SQL fixtures as "asserting my own
SQL back to me" and leaned on dev-verify instead. **That was rationalisation** — the repo
already has a same-transaction pass→failure DB test (`tests/test_2603_core_eligibility_db.py:256`)
to model. Insert deliberately conflicting rows and **assert the returned
`core_eligibility_proof_id`**, which is what makes it non-circular:

1. newer `not_underlying` beats older `underlying` (same scope);
2. newer `unresolved` beats older `underlying`;
3. tied `observed_at` → higher proof id wins;
4. timestamp order and id order **opposed** → timestamp wins (this is the case a naive
   `ORDER BY id DESC` passes and must not);
5. a newer **out-of-scope** row does not shadow an older in-scope one, and the out-of-scope
   diagnostic reports it;
6. an instrument with no in-scope row is absent from the mapping;
7. environment isolation: a `real` row does not satisfy a `demo` request.

⚠ **Dev-verify cannot substitute for these** and the first draft claimed it could. Three
stale `underlying` candidates cannot exercise tie handling, failure supersession or scope
isolation — picking the *wrong* stale row would produce identical output. Dev-verify shows
the query runs against the real table and the projection is sane; the DB test is what shows
the ordering is right.

## Dev-verify

Run the script against the dev DB for the three #2833 candidates, single-leg, and record on
the PR: the resolved scope (**ids redacted in the PR text**), the per-leg verdicts against
the prior census's latest-per-instrument table (all three `underlying`, 1 qualifying arm,
10 USD floor), the portfolio verdict (expected `indeterminate` / `leg_indeterminate`), the
exit code, and the artifact path.

⚠ A disagreement with the census would mean any of: new data since, a scope change, a
projection error, or wrong row selection — so treat it as a signal to investigate, not as a
single-hypothesis falsification.

## Codex ckpt-1 corrections

| first-draft claim | verdict |
| --- | --- |
| load newest row regardless of credentials, let the screen report mismatch | **reversed** — cross-account interference; filter on scope, carry the diagnosis separately |
| copy the single-row `ORDER BY` into the batched query | **would not compile** — `DISTINCT ON` needs matching leading `ORDER BY` |
| ties exist because a batched census stamps identical timestamps | **hypothetical** — 23 rows, 23 distinct timestamps; recorder commits per instrument |
| reuse `_live_credential_ids` with its `FOR SHARE`, "cheap" | **wrong** — unusable in a read-only txn and blocks revocation; add an unlocked reader |
| enforcement omitted because a blocking screen would be "an authority" | **false equivalence** — blocking research is not trading authority; the real reason is the acceptance's wording, and the gap stays open |
| three legs at ⅓ answers "could any be funded" | **backwards** — it is *stricter*; screen single-leg for the sleeve |
| "every stored proof is 22 days old"; portfolio code `eligibility_proof_stale` | **wrong twice** — 08-13 and 08-22 rows; portfolio code is `leg_indeterminate` |
| SQL fixtures would be circular; dev-verify covers ordering | **rationalisation** — assert proof ids over conflicting rows |
| screen checks the mapping key against the proof | **it does not** (reproduced) — loader must enforce identity |
| CLI numerics, exit codes, atomic write, unknown ids, artifact provenance | **all unspecified** — defined above |
