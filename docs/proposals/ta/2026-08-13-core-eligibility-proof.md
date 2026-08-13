# Core instrument eligibility proof — #2603 item 2

The account-specific proof that a candidate core instrument is the **underlying product,
not a CFD**, with the freshness rule that follows from eligibility not being immutable.

Item 1 (`2026-08-13-core-cash-mandate.md`) deliberately shipped no eligibility columns:
*"a timestamp plus a product-type label cannot prove 'underlying, not CFD': that needs
account identity, the eligibility arm, the raw response reference and a freshness rule.
Item 2 owns that evidence shape and gets its own table."* This is that shape.

## Source rule

`POST /api/v2/trading/info/{demo|real}/eligibility`, live portal fetched 2026-08-13 via
`https://api-portal.etoro.com/api-reference/trading--demo/check-instrument-trading-eligibility.md`
(WebFetch — `curl` is Cloudflare-blocked; see `.claude/skills/data-sources/etoro-api.md`).
`LeverageConfiguration.settlementType` is a **documented closed vocabulary of four**, each
with the provider's own definition:

| value | eToro's documented definition |
| --- | --- |
| `real` | "the real instrument held in full value" |
| `realFutures` | "the real future contract, which is a derivative of an underlying instrument" |
| `marginTrade` | "the real instrument held with only a portion of its value called margin (leveraged asset)" |
| `cfd` | "contract for difference, which is a derivative following the underlying instrument" |

So "underlying, not a CFD" is **not** inferred here — it is read off the provider's own
definitions. Exactly one value means ownership at full value: `real`.

- `realFutures` and `cfd` are derivatives by the provider's own wording.
- `marginTrade` **is** the real instrument, but held on margin — barred by the standing
  no-leverage posture in `.claude/CLAUDE.md`, not by this vocabulary.

`direction` is documented as `long` | `short`. The qualifying arm is therefore
`settlementType == "real"` **and** `direction == "long"` **and** `1 ∈ leverageValues`.

⚠ **The vocabulary is documented-closed, but it is read case-insensitively and an
unrecognised value fails closed.** A closed vocabulary is the provider's promise, not ours
to rely on: anything that is not `real` is not the underlying, including a value that does
not appear in the table above. Observed values are lowercased before storage so the stored
vocabulary is closed even if the wire one drifts.

**No freshness field is documented on this response** — verified on the same page: no
`lastUpdated`, no `asOf`. The what-if *cost* endpoint has one; eligibility does not. So the
freshness rule cannot be sourced and is fixed by construction, below.

## Full-population measurement (2026-08-13, demo account)

Reproduce — do not quote these figures from this file:

```bash
PYTHONPATH=. uv run python scripts/prove_2603_core_eligibility.py census
```

Whole tradable universe, 12,728 ids, 128 requests inside the documented 20/min dedicated
budget, 0 errors, 12,725 resolved (3 `notFoundInstrumentIds`):

| type | resolved | with a `real`/`long`/x1 arm |
| --- | --- | --- |
| Stocks | 10,847 | 10,771 |
| **ETF** | **1,232** | **703** |
| Crypto | 332 | 223 |
| Commodity / Indices | 234 | 0 (`realFutures` only) |

⚠ **A five-name sample got this exactly wrong and the full population corrected it.**
SPY, VOO, IVV, VTI and QQQ each return `cfd` arms only, which reads as "no ETF is available
as the underlying". The population says 703 of 1,232 ETFs are. Of those 703, **37 are
USD-quoted and 666 are not** (GBP 263 of 264, EUR, AUD 14 of 14, HKD 4 of 4, AED, CAD — the
census prints the split). This is `.claude/CLAUDE.md`'s full-population rule doing its job
on a **descriptive** claim, not on a gate.

⚠⚠ **Scope of that measurement, stated because it bounds every claim above.** It is one
account, one environment (`demo`), one instant. Eligibility is account and regulatory
state, so none of it generalises to the real environment, to another account, or to
tomorrow. That is not a caveat on the finding — it *is* the finding, and it is why this
ticket ships a per-account proof rather than an instrument attribute.

### The finding that decides the shape of this ticket

`SPY` and `SPY.RTH` are **the same fund, listed twice**, and only one of them is ownable:

| | `SPY` | `SPY.RTH` |
| --- | --- | --- |
| `instrument_id` | 3000 | 3417 |
| `exchange` | 5 | 33 |
| `company_name` | State Street SPDR S&P 500 ETF | State Street SPDR S&P 500 ETF |
| `maxUnitsPerOrder` | 134.0 | 134.0 |
| x1 long arm | **`cfd`** | **`real`** |
| other arms | `cfd`/short x1-x20, `cfd`/long x2-x20 | identical |

Naming `SPY` as the core instrument buys a contract for difference. Naming `SPY.RTH` buys
the fund. **For this pair, no column of `instruments` separates them** — same
`company_name`, `instrument_type_id`, `currency` and `country`; they differ on `symbol`
suffix and `exchange`, and neither of those is a rule: exchange 33 holds 4 ETFs of which 2
have a `real` arm, exchange 5 holds 390 of which 26 do, and `AAPL` carries a `real` arm
with no suffix at all. ⚠ That is a statement about this pair plus two venue counts — it is
**not** a full-population claim that no stored attribute could ever discriminate, and it is
not offered as one.

`QQQ.RTH` (3418) and `CSPX.L` (3434, GBP) behave the same way and are recorded as further
candidates, not as a recommendation. **This spec selects no core instrument.** It makes a
selection checkable.

## Schema — `sql/346_core_eligibility_proofs.sql`

`strategy_core_eligibility_proofs`, append-only. One row per observation, never updated. A
failing observation is stored exactly as a passing one is: an observation is evidence.

**Exactly one instrument per proof.** The recorder requests one id and the response is
therefore entirely about that instrument, which is what lets a single `response_digest`
stand as the whole evidence.

### Account identity

The triple `(operator_id, provider, environment)` names the account —
`broker_credentials_unique_active` (sql/019) is unique on
`(operator_id, provider, label, environment) WHERE revoked_at IS NULL`, so that triple has
at most one live `api_key` row and one live `user_key` row. It is deliberately **not**
`broker_credentials.id` alone: an eToro account is *two* credential rows, so no single row
identifies it.

But the triple is stable across a credential swap, and swapping in keys for a **different**
eToro account would let the new account silently inherit the old one's proofs. So the proof
also stores `api_key_credential_id` and `user_key_credential_id`, and the reader requires
them to equal the currently-live pair. Rotating credentials invalidates every prior proof
by construction rather than by a rule someone has to remember.

⚠ **Known limit, stated rather than papered over.** These columns record which credentials
the recorder *used*; nothing can stop a caller writing a row that asserts an account it
never contacted. The table is evidence of an observation, not an attestation of one.

### Columns

| column | why |
| --- | --- |
| `instrument_id` | FK to `instruments`, `ON DELETE RESTRICT` — evidence outlives tidying |
| `operator_id` / `provider` / `environment` | account identity |
| `api_key_credential_id` / `user_key_credential_id` | the live pair used, per above |
| `observed_at` | `DEFAULT now()`, **never a parameter** — a caller that supplies its own observation time can extend a proof's validity at will |
| `verdict` | `underlying` \| `not_underlying` \| `unresolved` |
| `reason_code` | closed six-value vocabulary, below |
| `requested_currency` / `response_currency` | the request asks USD; the demo response answers `"usd"`, so the comparison is case-insensitive and the observed value is stored verbatim |
| `settlement_type` / `direction` / `leverage_values` | the matched arm — NULL unless the verdict is `underlying`, because there is no matched arm otherwise |
| `qualifying_arm_count` | so "exactly one qualifying arm" stays checkable after the fact, which a projection of the selected arm alone cannot show |
| `allow_open_position` / `allow_close_position` / `allow_partial_close_position` | observed permissions |
| `min_position_amount` / `min_position_exposure` / `max_units_per_order` | observed sizing facts |
| `response_digest` | SHA-256 of the whole response |
| `policy_version` / `recorded_by` | which rule set produced the verdict, and which caller observed it |

### `response_digest` — the raw response reference, without the payload

`sql/287`'s comment records the standing rule that *"raw broker/feed payloads are not
persisted"*. The digest is SHA-256 over
`json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`
of the entire parsed response — so it covers response-level facts (`currency`,
`notFoundInstrumentIds`) that an instrument-row digest would miss, and `allow_nan=False`
turns a non-finite number into an error instead of invalid JSON.

⚠ **The tradeoff, chosen deliberately.** Sorting keys makes the digest immune to field
*reordering* but not to field *addition*: a new provider field moves it even though nothing
about eligibility changed. That is the direction to fail in — re-proving is one request, and
a digest that ignored unknown fields would be silent about exactly the drift this skill's
own history (documented `amount` → undocumented `value`) says to expect. The
canonicalisation is pinned by `policy_version`; changing it is a new version, never a
redefinition, or digests stop being comparable across the change.

### Verdicts and the closed reason vocabulary

`unresolved` means the response did not answer the question. `not_underlying` means it
answered and the answer is no. The split matters because only the second is a fact about
the instrument.

| verdict | `reason_code` | when |
| --- | --- | --- |
| `unresolved` | `instrument_not_resolved` | id in `notFoundInstrumentIds`, or no matching row |
| `unresolved` | `eligibility_row_ambiguous` | more than one row for the requested id |
| `unresolved` | `eligibility_currency_mismatch` | `response.currency` ≠ requested |
| `unresolved` | `eligibility_arm_ambiguous` | more than one qualifying arm |
| `not_underlying` | `instrument_not_open` | `allowOpenPosition` false |
| `not_underlying` | `no_underlying_arm` | zero qualifying arms — the SPY case |
| `underlying` | *NULL* | exactly one qualifying arm on an openable row |

⚠ **A transport failure writes no row.** An exception from the provider is the absence of
evidence; persisting it as an observation would turn "we could not ask" into "the broker
said". The recorder propagates and stores nothing.

### Constraints

- `(verdict = 'underlying') = (reason_code IS NULL)` — both directions, so a pass cannot
  carry a reason and a failure cannot omit one.
- `verdict = 'underlying'` implies `settlement_type = 'real'`, `direction = 'long'`,
  `allow_open_position`, `qualifying_arm_count = 1`, `1 = ANY(leverage_values)` and
  `upper(response_currency) = requested_currency`. Storing the projection is not enough:
  without these, a row can look like a pass and not be one.
- `verdict <> 'underlying'` implies `settlement_type`, `direction` and `leverage_values`
  are all NULL — a failing row must not carry pass-shaped evidence.
- Every stored minimum/maximum is `> 0` when present. The provider may omit them; it may
  not assert a non-positive one.

**Sizing is recorded, not decided.** `min_position_amount`, `min_position_exposure` and
`max_units_per_order` are stored as observed facts and **no effective minimum is derived**.
`_eligibility_reason` computes `arm.min_position_amount or row.min_position_exposure`, but
that precedence has no citation in the provider's documentation, and a missing minimum is
an order-sizing gap rather than evidence about what the product *is*. Making it a condition
of `underlying` would conflate the two: a genuine `real`/`long`/x1 arm is the underlying
product whether or not the response quoted a floor. Item 3 owns sizing and inherits both
numbers. The same reasoning keeps `allow_close_position` and `allowStopLossTakeProfit` out
of the verdict — a core sleeve does need to be sellable, but that is an execution property.

Index on `(instrument_id, operator_id, provider, environment, observed_at DESC)`.

## Freshness — fixed by construction, since no source rule exists

`CORE_ELIGIBILITY_MAX_AGE = 24 hours`, frozen in
`CORE_ELIGIBILITY_POLICY_VERSION = "core-eligibility-v1"`. A proof observed exactly
`MAX_AGE` ago is still fresh: the comparison is `now() - observed_at <= MAX_AGE`, stated
here because #2670 is the standing lesson that a boundary nobody wrote down is a boundary
two pieces of code will disagree about.

No published formulation governs this and the provider supplies no freshness field, so per
`.claude/CLAUDE.md` the constant is fixed **by construction**, and the construction is what
is defended rather than the number:

1. Eligibility is account and regulatory state. It can change without notice and the
   response carries no timestamp, so the only age measurable is the age of **our
   observation**.
2. 24 hours is the coarsest window under which a proof cannot outlive more than one
   intervening trading session.
3. Re-proving is cheap and bounded — one instrument is one request against a dedicated
   20/min endpoint — so the constant is a **ceiling on staleness, not a target**. A caller
   that re-proves every time is always correct and never rate-limited.

⚠ It is a window, not an alarm. Nothing polls; a proof simply stops satisfying
`require_core_eligibility` once it ages out.

## The shared arm predicate — `app/services/broker_settlement_arms.py`

`select_underlying_long_arms(row) -> tuple[BrokerLeverageConfig, ...]` returns every arm
that is the underlying product, held long, unleveraged. One definition, two callers: this
proof, and `strategy_paper_executor.py::_eligibility_reason`, which today spells the same
triple out inline. Two copies of "what counts as the underlying" is the drift #2437 keeps
recording.

⚠ **The helper is arm SELECTION only, and is deliberately narrower than
`_eligibility_reason`.** The executor additionally requires exactly one matching row, an
`allowStopLossTakeProfit` arm and a satisfied minimum; those stay in the executor, because
a core sleeve is a stop-less indefinite holding and must not inherit an entry rule.
Calling the helper does not make a caller eligible to trade.

⚠⚠ **`bool` is an `int` in Python, so `1 in leverage_values` is true for
`leverageValues: [true]`** — and the provider parser admits it, because
`isinstance(True, int)`. The helper tests `value == 1 and not isinstance(value, bool)`.
Malformed broker data must not be able to prove x1 eligibility. This hardens the executor's
existing path too, which is one of the reasons the definition moves rather than duplicates.

⚠ **The executor's stored reason vocabulary does not change.** `_eligibility_reason` maps
both "zero qualifying arms" and "more than one" to `eligibility_arm_ambiguous` exactly as
it does today, even though the helper now distinguishes them. `strategy_entry_preflights.reason_code`
is stored data; re-labelling it is a data-semantics change and is filed separately.

## Wiring — one real consumer, chosen so this is not another R4 orphan

`configure_core_mandate` refuses to append an **enabled** mandate unless the named
`core_instrument_id` has a passing, unexpired proof for the account. `strategy_core_mandate`'s
own docstring reserved this: *"No parameter can record an eligibility proof: item 2 owns
that evidence shape and gets its own table, so a mandate cannot claim proof it does not
have."* The mandate cannot claim it, so it must require one that exists independently.

The writer gains required `operator_id` / `provider` / `environment` parameters, because a
gate that cannot name the account cannot select the right proof. `configure_core_mandate`
has no production caller today (tests only), so the signature change costs nothing.

- The gate is the **selection** point: the mandate is where a core instrument is named, so
  it is where naming a CFD has to fail.
- The gate stays offline. It reads a table and makes no broker call, so the writer keeps
  its pure-DB test surface and cannot fail on a network.
- **No TOCTOU.** The check runs inside the writer's existing `pg_advisory_xact_lock`
  transaction and both the check and the row's `observed_at` comparison use `now()`, which
  is transaction-start time and therefore constant across check and insert.
- A **disabled** mandate is not gated. ⚠ `strategy_core_mandate_enabled_has_instrument` is
  `NOT enabled OR core_instrument_id IS NOT NULL`, so a disabled mandate *may* still name an
  instrument — the earlier claim that it "names nothing" would have been wrong. It is
  ungated because it authorises nothing, and no code reads a disabled mandate's instrument;
  re-enabling it goes through the gate like any other enable.

**This is not a `policy_version` bump, and the discriminator is worth stating.** #2670
settled that *a version denotes a rule set, not a row population*, so "0 rows" never
excuses leaving a version alone. It does not apply here on two counts. First,
`validate_core_mandate` — the arithmetic the mandate's version stamps — is untouched, and
every already-constructible `CoreMandate` remains valid with the identical meaning; the
mechanical test is that the change does not edit that function, and it does not. Second,
the rule set that *did* change is versioned: it is `core-eligibility-v1`, stamped on the
proof row that the gate reads. A new rule set with its own version string is not an
unversioned change.

## What this does NOT do

- **It selects no core instrument.** It makes a selection checkable.
- **It does not decide `SPY` vs `SPY.RTH`.** It records that only one of them passes.
- **It runs on no schedule.** No job, no cadence, no re-proof loop.
- ⚠⚠ **It is a WRITE-TIME gate and must never be cited as an execution control.** An
  enabled mandate stays enabled after its proof ages out or is superseded by a failing
  observation — the gate constrains what may be written, not what may be traded. Item 3
  must re-prove at execution time; the module docstring says so, so the next reader cannot
  infer otherwise. Naming this is the whole of #2437's R4 lesson applied to my own slice.
- **It grants no order path.** Nothing here submits, sizes or authorises a trade, and every
  broker call it makes is the informational eligibility endpoint.
- **It does not touch the real environment.** Every measurement is `demo`; the
  `environment` column exists so a demo proof can never be read as a real one.
