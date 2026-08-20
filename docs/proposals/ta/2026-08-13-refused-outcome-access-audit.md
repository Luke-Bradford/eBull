# #2611 — recording a refused outcome-access attempt

Parent: `docs/proposals/ta/2026-08-12-preregistration-declaration-gate.md`
("Explicitly NOT in scope" → *Refused-attempt auditing (#2611)*). Refs #2599,
#2634, #2437.

## The gap

`record_holdout_access` and `require_outcome_access` raise
`PreregDeclarationRefused` and write nothing. The declaration, its refusal codes
and the caller are all known at that moment and none of them survive.

Measured, dev, at branch point `466a81fe`:

```
strategy_holdout_accesses                    304  (8 distinct trials)
strategy_preregistration_declarations          0
strategy_results_store                       324  (300 namespace='hold_out')
```

With zero declarations frozen, **every** `require_outcome_access` call today
refuses `preregistration_not_frozen` — so both sealed evaluators
(`scripts/evaluate_2582_schedule13d_outcomes.py`,
`scripts/sealed_rerun_gate.py`) are refused on every invocation and leave no
trace of having tried. That is the state this ticket is about, not a
hypothetical one.

## The question the ticket poses

> Postgres has no autonomous transaction, so a refusal row written in the
> caller's transaction disappears if the caller rolls back — and the caller
> almost certainly will, because the refusal is an exception. A second
> connection would record attempts that never happened.

## Source rule / decision

There is no external standard governing this; it is fixed **by construction**,
and the construction is an asymmetry the framing above does not name:

- An **access** record is a claim about DATA. It must be atomic with the work it
  authorises — `sql/264`'s trigger has to see it in the same transaction as the
  hold-out row, and a rolled-back evaluation did not happen. Hence
  `record_holdout_access`'s existing docstring, which stays correct.
- A **refusal** record is a claim about an ACT OF THE CALLER. It completes at the
  moment `PreregDeclarationRefused` is constructed; the caller rolling back does
  not un-attempt it, and a caller that retries N times really did attempt N
  times.

So the two have different durability requirements and belong in different
transactions. "Records attempts that never happened" is the correct worry about
a second connection writing an *access*; it is not true of a refusal.

### Consequences that follow, each of them load-bearing

1. **A separate table, never `strategy_holdout_accesses`.** Two measured reasons,
   either one sufficient:
   - `holdout_access_counts.recorded_accesses` counts that relation and feeds
     `check_promotable` criterion 5. Refused rows would inflate it.
   - `supersede_preregistration` refuses `supersession_trial_already_exposed` on
     `count(*) > 0` of that relation (`_COUNT_TRIAL_EXPOSURE`). A refused attempt
     is precisely NOT exposure — counting it there would permanently strand a
     trial from #2634's repair path over a look that never happened.
2. **No advisory lock on the audit write, and no FK to the declarations table.**
   Measured 2026-08-13: `pg_advisory_xact_lock(hashtext(k))` **blocks across
   connections** (a second connection with `statement_timeout=1500ms` raised
   `QueryCanceled`). `record_holdout_access` takes exactly that lock and still
   holds it when it refuses, so an audit write that took it would block until the
   caller's transaction ended — a self-deadlock on the exception path. An FK is
   the same failure in quieter clothing: it takes a `KEY SHARE` lock on the
   parent row, and it cannot see a declaration the caller froze in its own
   still-open transaction. `declaration_id` is therefore a plain nullable
   `BIGINT` with a comment saying why.
3. **The audit connection is derived from the CALLER's connection**, via
   `make_conninfo(conn.info.dsn, password=conn.info.password)` — verified to
   round-trip on psycopg 3.3.3. Reading `settings.database_url` instead would
   write a test's refusal into the operator's dev DB and trip
   `tests/conftest.py`'s dev-DB tripwire.
4. **Best-effort, and it never masks the refusal.** If the audit connection or
   its INSERT fails, the failure is logged at ERROR *with the refusal codes
   inline* and `PreregDeclarationRefused` is raised regardless. A gate that can
   be disabled by breaking its logger is not a gate. ⚠ The codes go in the log
   line precisely so a failed audit write is not a silent no-op — the
   "job reports success and writes nothing" shape this repo has been bitten by.
5. **One chokepoint, enforced structurally.** Both refusal sites raise through a
   single `_refuse_access`. An AST-based test asserts that no function on the
   access path constructs `PreregDeclarationRefused` directly — a substring grep
   would be satisfied by the import line (#2631).

## Scope

**In:** the two outcome-access doors the ticket names —
`record_holdout_access`'s incoherent/digest refusal (via
`_refuse_incoherent_declaration`) and `require_outcome_access`'s
`preregistration_not_frozen`.

**Out, deliberately:** `freeze_preregistration` and `supersede_preregistration`
refusals (neither is an attempt to open outcomes — they are attempts to write a
declaration, and both fail before anything is looked at);
`_refuse_declared_stamp_substitution` (a result-WRITE refusal);
`verify_outcome_access_provenance` (#2614's read-only re-check, which already
requires a real `access_id` — so the attempt it re-checks is already recorded).

## Shape

`sql/340_holdout_access_refusals.sql`:

```
strategy_holdout_access_refusals
    refusal_id       BIGSERIAL PRIMARY KEY
    strategy_id      TEXT NOT NULL
    strategy_version TEXT NOT NULL
    result_version   TEXT              -- NULL for a read, as sql/264
    access_kind      TEXT NOT NULL CHECK (access_kind IN ('evaluate','read'))
    accessed_by      TEXT NOT NULL
    purpose          TEXT NOT NULL
    refusals         TEXT[] NOT NULL   -- every code that fired, never the first
    declaration_id   BIGINT            -- no FK; see decision 2
    refused_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    CHECK (non-empty on the four text fields, and cardinality(refusals) > 0)
```

`app/services/result_ledger.py`:

- `_refuse_access(conn, access, refusals, declaration_id)` — writes the audit
  row on its own connection, then raises. Never returns.
- `record_refused_access(conn, ...)` is not public. The only public surface added
  is `read_access_refusals(conn, strategy_id, strategy_version)` for the
  governance read, mirroring `read_holdout_results`' shape.

## Acceptance

Pure-logic:

1. `_refuse_access` raises `PreregDeclarationRefused` carrying every code.
2. An audit-connection failure does NOT suppress the raise, and the codes appear
   in the logged message.
3. AST: no function on the access path constructs `PreregDeclarationRefused`
   directly.

DB-backed (one per mechanism):

4. `require_outcome_access` on an undeclared trial → refusal row with
   `preregistration_not_frozen`, `declaration_id IS NULL`, **and the row survives
   the caller rolling back**.
5. `record_holdout_access` on a trial whose declaration is incoherent → refusal
   row carrying the declaration's id and its codes.
6. The refusal rows do not move `holdout_access_counts` and do not make
   `supersede_preregistration` refuse `supersession_trial_already_exposed`.
