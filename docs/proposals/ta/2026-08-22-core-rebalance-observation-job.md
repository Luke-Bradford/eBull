# The core rebalance observation job (#2603 item 3, step 3b-3)

Status: proposed, 2026-08-22. Codex checkpoint 1 run; the findings it produced are folded
in below and the four rebutted ones are named with their reasons.

## What this is

The **producer**. `#2603` has shipped ten modules for the core/cash arm and the chain they
form is complete:

```
broker.get_account_risk_snapshot()          # informational read, demo-only at the provider
  -> observe_core_sleeve(snapshot, core_instrument_id=...)   -> CoreSleeveState
  -> record_core_rebalance_intent(conn, state=..., recorded_by=...)
       -> load_core_mandate(conn) + evaluate_core_rebalance(...)
       -> one append-only row in strategy_core_rebalance_intents
```

Nothing invokes it. `app/services/strategy_core_sleeve.py` and
`app/services/strategy_core_sizing.py` have **no caller anywhere in `app/` or
`scripts/`**, and `strategy_core_rebalance_intents` holds **0 rows** on dev.

This slice adds one scheduled job that runs that chain and nothing else.

## Measured before speccing (dev DB, 2026-08-22)

| query | result |
| --- | --- |
| `select count(*) from strategy_core_mandate_events` | **0** — no mandate has ever been configured |
| `select count(*) from strategy_core_rebalance_intents` | **0** |
| `strategy_core_eligibility_proofs`, latest per instrument | 3434 / 3418 / 3417 `underlying`; 3000 (SPY) `not_underlying` / `no_underlying_arm` |

So the job **no-ops until a mandate exists**, and that is the correct build order rather
than a gap: declaring the sleeve — which instrument, what target weight, what band — is
`#2833` (R5b phase 1, "the boring sleeve goes live-capable"). Phase 0 owns the machinery;
phase 1 declares what it manages. Stated here so the no-op is read as a sequence and not
as another unwired control.

## ⚠⚠ What an intent row IS, corrected

`sql/348`'s header says *"no table has a foreign key to it and no module reads it, so no
code path can turn a row into an action"*, and `strategy_core_rebalance_intent.py` repeats
it. **The second half is no longer true.** `sql/349` added
`strategy_trades.core_rebalance_intent_id` (an FK), and
`strategy_core_submission_gate.py:142` `SELECT`s from the table to decide whether a stored
verdict may become an order. Both docstrings are stale and this slice corrects them.

What is still true, and is the actual safety statement:

- The submission gate **has no acting caller** — nothing in `app/` or `scripts/` invokes
  it, so no path runs from an intent row to an order.
- This job calls exactly one provider method, `get_account_risk_snapshot`, which is
  informational. `app/security/unattended_guard.py::refuse_broker_mutation_if_unattended`
  is not reached because no mutating method is called, by design (#2645: guarding
  informational reads was the other half of that error).

So the honest claim is **"this job produces gate INPUT, not authority"** — not "authorises
nothing". Writing the weaker-sounding true thing matters here because the stale version is
what a reader would otherwise carry forward.

## Scope

One job, `core_rebalance_observation`:

1. Refuse unless `settings.etoro_env == "demo"` → `_record_prereq_skip`.
2. Load eToro credentials; `_record_prereq_skip` if absent.
3. On a short-lived connection, `load_core_mandate`. `_record_prereq_skip` — **without any
   broker call** — when there is no mandate row, or `core_instrument_id IS NULL`.
4. Open a broker session pinned to `env="demo"`, with **no DB connection held across the
   HTTP call**, and call `get_account_risk_snapshot()`.
5. `observe_core_sleeve(snapshot, core_instrument_id=mandate.core_instrument_id)`.
6. On a fresh connection, `record_core_rebalance_intent(conn, state=..., recorded_by=<job
   name>)` and commit.

Registry entry: `source="etoro"`, `cadence=Cadence.daily(hour=22, minute=45)`,
`catch_up_on_boot=False`, `prerequisite=_bootstrap_complete`.

Explicitly **not** in scope: sizing (`strategy_core_sizing`), the broker half of the
refusal vocabulary (account-risk availability as a stored refusal, what-if cost
assessment, the broker minimum, broker rejection), in-flight suppression, submission, and
any order.

## Source rule

The choices below have no external regulator — but that is a claim about *these* choices,
not a blanket one. The allocator's own boundary-targeting rule already cites a documented
portfolio-rebalancing formulation, and the repo has a market-session rule
(`market_calendar.us_market_status`) which `strategy_core_preflight` applies at submission
time. Neither governs an observation cadence, which is what is fixed by construction here.

### Cadence: daily, 22:45 UTC

**Monitoring frequency is not rebalancing frequency, and only the second is a policy
choice.** The mandate already carries the rebalance rule — `rebalance_band_pct` around
`core_target_pct`, a threshold rule — and that band decides whether a trade happens. This
job decides only *how often the drift is looked at*.

Daily is the coarsest cadence that observes **each calendar day exactly once**. ⚠ Not
"each trading session": a daily cron also fires on weekends and US holidays, and those
ticks record a real (flat) re-observation of the same marks rather than nothing. That is
accepted, not overlooked — an appended `hold` on a Sunday is a true statement about the
sleeve, and gating on `us_market_status` would be wrong anyway, because
`docs/settled-decisions.md` permits a non-US-listed core instrument whose venue this repo
has no calendar for.

22:45 UTC is after the US close (~21:00 UTC), the same anchor `db_eod_snapshot` records
for its 22:30 slot, and 15 minutes after it. ⚠ **The offset is scheduling hygiene, not an
invariant** — overrun, manual dispatch and catch-up can still overlap, and the lane is
what actually serialises.

⚠ It does **not** follow that the marks are "session-final". `snapshot.observed_at` is our
receipt time and the payload carries no broker valuation stamp at all (measured
2026-08-14, `strategy_core_sleeve.py:67-70`). Firing after the close makes it *likely* the
marks reflect that session; nothing in the payload proves it, and no downstream rule here
depends on it.

⚠ The cost is one eToro request per day against the `etoro` lane's budget, not zero.
`daily_portfolio_sync` already calls `get_account_risk_snapshot()` in demo, so this is a
knowing duplication: folding the observation into that job would couple the core arm's
evidence to the portfolio sync's failure modes and cadence anchor, and the second request
is negligible against the lane's budget.

### Lane: `etoro`

The job's only external call is an eToro read, and `etoro` is the lane that owns eToro
request budget. The alternative, `strategy_execution`, is held by `strategy_paper_cycle`
every five minutes — a daily job landing on a busy lane is a daily job that skips. At
22:45 the `etoro` lane's scheduled occupants (`quotes_refresh` hourly at :23,
`daily_portfolio_sync`, `nightly_universe_sync`) do not fire.

### Which refusals this producer can and cannot reach

`CoreRebalanceReasonCode` has eleven members. This job reaches nine. The two it cannot:

- **`core_instrument_unset`** and **`core_mandate_absent`** — both mean there is no
  instrument id, and `observe_core_sleeve` needs one. Reaching them would require
  fabricating a `CoreSleeveState` (an invented instrument id and two invented valuations)
  purely to hang a reason code on, and `strategy_core_rebalance_intents` would then hold a
  fabricated valuation. **It is the fabrication that is refused, not the recording** —
  both facts are fully derivable from `strategy_core_mandate_events` without a broker
  call.

⚠ **`core_mandate_disabled` IS reached**, deliberately, and this is the one place the
cheap choice was not taken. A disabled mandate still has an instrument, so the sleeve is
observable, and the record that accumulates *while* a mandate is disabled is what an
operator needs at the moment they re-enable it. Skipping on `enabled = false` would save
one request a day and make the disabled window a hole indistinguishable from "the job was
down". It would also leave `core_mandate_disabled` producible by the allocator and
reachable from no producer — the shape `#2603` already had to delete once
(`cost_exceeds_available_cash`, step 3b-2 item 1).

⚠ Precisely what a disabled row stores: `core_rebalance_intent_shape_matches_action`
(`sql/348:145`) requires **every derived field NULL on a refusal** — `core_pct`,
`target_pct`, `lower_pct`, `upper_pct`, `effective_floor`, `floor_source`,
`reserve_breached`, `reserve_margin_pct`. What survives is `core_market_value`,
`cash_balance`, `currency` and `state_as_of`. The core weight is therefore not *stored*
while disabled; it is *arithmetic on the two stored components*
(`core_market_value / (core_market_value + cash_balance)`). Said explicitly because
"records the drift" would otherwise read as a column that is not there.

### Demo-only

`strategy_core_mandate_events.mode` is `CHECK (mode = 'paper')` (`sql/349:137`). Observing
a **real** account and stamping the verdict against a paper mandate would attribute a live
book's drift to a paper policy.

Two guards, and the outer one exists only for the audit row: `get_account_risk_snapshot`
already raises `TradingPreflightParseError` when `self._env != "demo"`
(`etoro_broker.py:828`), so a real-environment run fails closed at the provider regardless.
The job checks `settings.etoro_env` first so that case records a clean `PREREQ_SKIP`
instead of an exception, matching `strategy_paper_cycle` verbatim, and constructs the
provider with the literal `env="demo"` rather than re-reading the setting — the same
check/use gap `strategy_paper_cycle` closes.

⚠ `load_core_mandate` does **not** select `mode`, so the job relies on the schema CHECK
rather than on a value it read. That reliance is bound by a test asserting the CHECK still
pins `mode` to `'paper'` — the same device
`test_the_migration_reason_codes_match_the_allocator_vocabulary` already uses for the
reason-code vocabulary. If `mode` ever widens, that test fails and this job's assumption
surfaces before the widening merges.

### A failed observation is a FAILED JOB, not a quiet success

`CoreSleeveObservationError` (naive timestamp, unreported or undocumented account
currency, duplicate instrument row, direct short) and a failed
`get_account_risk_snapshot()` both **propagate**. `_tracked_job` records the run as a
failure with the exception.

Neither is caught-and-noted, and the reason is the repo's own worst monitoring failure
mode: *a job that no-ops and reports success is invisible to every automated check we
have.* There is no primary work here to protect by swallowing — the observation **is** the
work — so the pattern `daily_portfolio_sync` uses (catch, warn, continue) does not apply.

⚠ Consequence, stated so it is not later read as a defect: **an unobservable sleeve is
visible in `job_runs`, not in `strategy_core_rebalance_intents`.** A reader counting rows
in the intents table is counting successful observations, not scheduled ticks. Nothing is
stored for these cases because no `CoreSleeveState` can be constructed at all, and the
table requires an observed sleeve.

The `state_as_of <= evaluated_at` CHECK (`sql/348:205`) is in the same class: `state_as_of`
is the application host's receipt clock and `evaluated_at` is the database's
`clock_timestamp()`, so a large enough skew between the two hosts makes the INSERT fail.
That surfaces as a failed job run, which is the correct outcome — it is a real clock fault
and must not be papered over by relaxing the constraint or by rewriting the observation
time.

## The mandate-revision race

Step 3 loads the mandate to learn the instrument; step 6's
`record_core_rebalance_intent` loads it again inside its own transaction. A revision
landing between them is evaluated against the newer mandate.

⚠ The first draft of this spec claimed that race always yields `sleeve_instrument_mismatch`.
**It does not.** `_state_refusal` runs after the mandate-level checks, so a revision that
is newly disabled, policy-unsupported or invalid returns *its own* code first, and a
revision that changes `core_target_pct`, `rebalance_band_pct`, `liquidity_reserve_pct` or
`min_rebalance_amount` while keeping the instrument is **not detected at all** — the
observation is attributed to the new revision by `core_mandate_event_id`.

⚠ The first draft accepted that as bounded-and-legible. Codex checkpoint 2 raised it as a
P2 and it is **closed instead**, because the close is cheap: the recording connection takes
`CORE_MANDATE_ADVISORY_LOCK` — the same lock `configure_core_mandate` takes
(`strategy_core_mandate.py:365`) — re-reads the mandate under it, and drops the tick with a
logged note if `event_id` moved.

Two things about that shape are load-bearing:

- **Re-reading without the lock is not a fix.** psycopg's default isolation is READ
  COMMITTED, so each statement gets a fresh snapshot and a writer can still commit between
  the re-read and the INSERT. The check-then-write window would just be narrower.
- **The lock is never held across the HTTP call**, which is why the mandate is pre-read on
  a separate short-lived connection rather than under it. It covers two fast reads and one
  INSERT.

Dropping the tick is the right loss: an operator reconfigured the mandate inside a
round-trip, the next tick is correct, and a misattributed row would outlive the confusion
that produced it. It records as a zero-row success with a note naming the moved event —
a REPORTED skip, not the silent no-op the failure paths above refuse to become.

Duplicate intents from repeated manual dispatch are likewise not a new problem:
`strategy_core_submission_gate.py:150` supersedes any intent with a newer one before
acting on it.

## Acceptance

- The job appears in `SCHEDULED_JOBS` and in `app/jobs/runtime.py`'s dispatch map.
- With no mandate configured (dev's current state), one dispatch writes a `PREREQ_SKIP`
  `job_runs` row and makes **no broker call**. ⚠ The skip is recorded from inside the job
  body, not only via the registry `prerequisite`, because manual dispatch bypasses the
  registry path.
- With a mandate carrying an instrument, one dispatch writes exactly one
  `strategy_core_rebalance_intents` row whose `recorded_by` is the job name — *unless* the
  snapshot is unobservable or the broker read fails, in which case the job fails and no
  row is written.
- ⚠ The end-to-end run against a configured mandate is **not** exercised on dev in this
  slice, because no mandate exists to configure without pre-empting `#2833`'s declaration.
  The wiring is covered by tests; the first live row arrives with `#2833`.

## Codex checkpoint-1 findings rebutted, with reasons

- *"No maximum observation age is enforced; a stale snapshot can create intent authority."*
  `observed_at` is assigned by the provider as `datetime.now(UTC)` immediately after the
  response returns (`etoro_broker.py:837`), so within one job run it is now by
  construction. A staleness bound would be checking the clock against itself. The
  freshness that matters is the *intent's* age at submission, which is the owed bound
  named above and belongs to the submission slice.
- *"No market-calendar prerequisite despite `us_market_status` existing."* Applying a US
  calendar would refuse a non-US core instrument that `docs/settled-decisions.md`
  explicitly permits. `strategy_core_preflight` already owns the session question at
  submission time, including `core_unsupported_market_session` for exactly that case.
- *"Pending orders / in-flight rebalances can produce duplicate or reserve-breaching
  trades."* True of the arc, not of this slice: no trade is produced. In-flight
  suppression is a declared owed obligation of `strategy_core_rebalance_intent.py` and is
  owned by the submission gate.
- *"Non-US / other-timezone core instruments are omitted."* Same seam as above.

## Refs

Refs #2603. Refs #2833. Refs #2437.
