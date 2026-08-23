# The broker's open-position minimum: one source rule, one shared helper

#2603 step 3b-2, the broker half of the core submission refusal vocabulary — the minimum
only. Account-risk and cost are already covered elsewhere and are not rebuilt here.

## The gap

Three modules independently record that the effective broker minimum is unsettled, and
each declines to derive one:

- `app/services/strategy_core_eligibility.py:18-23` — *"no effective minimum is derived:
  the executor's `arm.min_position_amount or row.min_position_exposure` precedence has no
  citation in the provider's documentation"*.
- `app/services/strategy_core_allocator.py:249-253` — *"Whether a given broker minimum
  applies to an incremental buy or a partial sell is the caller's determination …
  nothing here establishes it governs a rebalance leg."*
- `app/services/strategy_core_rebalance_intent.py:174-183` — *"whether eToro's
  `min_position_amount` even governs an incremental buy or a partial sell is unsettled …
  The executor holds the eligibility response and can answer it with evidence."*

Meanwhile `app/services/strategy_paper_executor.py:650` applies the uncited precedence:

```python
minimum = arm.min_position_amount or matches[0].min_position_exposure
if minimum is None or amount < minimum:
    return "below_broker_minimum"
```

## Source rule

Live portal, fetched 2026-08-23 via the `.claude/skills/data-sources/etoro-api.md`
protocol (`llms.txt` → per-endpoint `.md`, WebFetch not curl):
`api-reference/trading--demo/check-instrument-trading-eligibility.md`.

| field | object | documented as |
| --- | --- | --- |
| `minPositionExposure` | `InstrumentEligibility` (the top-level row) | *"Minimum exposure value required to open a position on this instrument. The exposure is always calculated in USD as the number of units times the rate times the conversion rate to USD."* |
| `minPositionAmount` | `LeverageConfiguration` (a `leverageConfigs` entry) | *"Minimum margin required to open a position under this leverage configuration."* |

### 1. They are different quantities, so `or` is the wrong combinator

Exposure is units × rate × FX; margin is exposure ÷ leverage. `or` treats them as two
spellings of one number and takes whichever appears first. They are two constraints.

### 2. `max` is a SAFE BOUND, not a reproduction of the broker's rule — and that is the claim being made

Each field says *"required to open a position"*, so an order must clear both. Testing the
order's USD notional against `max` of the quoted figures:

- **can never admit** an order the broker would refuse on either dimension, because the
  notional is tested against the larger of the two thresholds;
- **can refuse** an order the broker would accept, at leverage > x1, where the notional
  exceeds the margin by the leverage multiple and the margin threshold is thereby
  over-applied.

Over-restriction is the safe direction for a floor, and this is the same posture
`strategy_core_sizing.QuotedTradeCost` already takes for cost (*"An upper bound,
deliberately, and every decode rule leans that way"*). It is stated as a bound rather
than sold as exact, because the exact rule needs a margin figure neither caller computes.

⚠ **The over-restriction is unreachable through today's callers and that is enforced, not
assumed.** Both minimum consumers select their arm through
`broker_settlement_arms.select_underlying_long_arms`, which requires `offers_unleveraged`
— x1. At x1 margin equals exposure, so the two thresholds are the same quantity and `max`
is exact. Measured: all 12 qualifying stored proofs carry `leverage_values = [1]` exactly.

### 3. `minPositionAmount` has NO documented currency — so the helper requires USD rather than assuming one

`minPositionExposure` is currency-PINNED to USD by its own wording — *"always calculated
in USD"* — independently of the response's `currency` field. The portal documents no
currency for `minPositionAmount` at all. Rather than infer one (that inference is what
this whole ticket exists to stop), the helper refuses to combine unless the response
currency is USD, under which no non-USD denomination is in play on either field.

This stops being satisfiable at #2603 scope item 4 (the non-USD deployment lift), which is
precisely the change that would otherwise silently `max()` a USD figure against a GBP one.

### 4. Closes are UNDOCUMENTED — which is "unknown", not "no constraint"

The portal states both fields for opening and says nothing about closing or
partial-closing. This spec therefore derives **no close-side floor**, and that is a named
gap rather than a proof of absence: a `sell_core` rebalance leg carries no broker floor
*from this source*, and `allowPartialClosePosition` proves permission, not unrestricted
sizing. If eToro constrains partial-close size, we do not currently know it and would not
refuse on it.

### 5. An entry order IS an open — measured, not asserted

Codex checkpoint 1 raised that a buy into an instrument already held might increment an
existing position rather than open a new one, which would put it outside the documented
scope. It does not: eToro tracks positions individually and does not net.

```sql
select instrument_id, count(*) from broker_positions group by 1 order by 2 desc;
-- 1699|2  1571|2  1181|1  4238|1  3006|1
select count(*), count(distinct instrument_id) from broker_positions;  -- 7 | 5
```

Two instruments hold two positions each on the demo account, so a second order in a held
name opened a second position. Applying an open-side minimum to every entry is therefore
in scope for this broker.

## Full-population verification, and exactly what it does and does not bound

Every stored proof, not a sample — `strategy_core_eligibility_proofs`, 23 rows:

```sql
select count(*) total,
       count(*) filter (where min_position_amount is not null) has_amount,
       count(*) filter (where min_position_exposure is not null) has_exposure,
       count(*) filter (where min_position_amount is not null
                          and min_position_exposure is not null
                          and min_position_amount <> min_position_exposure) differ
  from strategy_core_eligibility_proofs;
-- total=23  has_amount=12  has_exposure=23  differ=0
```

```sql
select leverage_values, count(*), min(min_position_amount), max(min_position_amount),
       min(min_position_exposure), max(min_position_exposure)
  from strategy_core_eligibility_proofs where verdict='underlying' group by 1;
-- [1] | 12 | 10.000000 | 10.000000 | 10.000000 | 10.000000
```

eToro's demo quotes a flat 10.00 for both fields, every qualifying arm is exactly `[1]`,
and the 12 rows carrying both agree exactly. `min_position_amount` is NULL on the 11
`not_underlying` rows because it is read off the qualifying arm, of which they have none.

⚠⚠ **This census bounds the CORE side only, and does not bound the executor's.**
`strategy_paper_executor._eligibility_reason` reads a LIVE `BrokerEligibilityResponse`
per call and never touches `strategy_core_eligibility_proofs`, so no stored row records
what the executor has seen. The honest claim is therefore: **the correction moves no
verdict this repo has stored**, and it is soundness against the documented rule rather
than repair of an observed wrong number. What the census does establish is that the two
fields are populated, agree where both appear, and are x1-only — so a `max` that is exact
today.

⚠ Also measured, and it is why the comparability check normalises: `response_currency` is
**`usd` in lower case** on all 23 rows, against a `requested_currency` of `USD`.

## The change

One helper, in `app/services/broker_settlement_arms.py` — the module that already exists
because two callers shared a definition of "the underlying product" and a second copy is
the drift #2437 keeps recording:

```python
def effective_open_minimum(
    *,
    response_currency: str,
    min_position_exposure: Decimal | None,
    min_position_amount: Decimal | None,
) -> Decimal | None
```

- **Takes the two figures, not the two objects**, so the executor (which holds
  `BrokerInstrumentEligibility` + `BrokerLeverageConfig`) and the core proof (which holds
  two NUMERIC columns) share one rule with no object coupling.
- **Sanitises both inputs itself** — a non-finite or non-positive figure is dropped to
  "not quoted", the `_positive_or_none` rule `strategy_core_eligibility.py:156` already
  applies and `sql/346` already enforces. This closes a real divergence Codex found: the
  stored-proof path sanitises and the executor path does not, so today the two callers
  would reach different verdicts on identical broker data.
- **Returns `max` of the surviving values**; `None` only when neither survives. `None`
  means "the broker quoted no usable minimum", and each caller fails closed on it —
  unchanged from today's `minimum is None or amount < minimum`.
- **Raises `ValueError` when `response_currency` is not USD**, naming scope item 4.

### Why the currency failure raises rather than returning a refusal

Codex checkpoint 1 argued for a returned refusal, on the grounds that a currency mismatch
is external-data state and that raising could abort an execution cycle in the exact state
scope item 4 creates. Rebutted, on the call sites:

- `strategy_paper_executor._eligibility_reason:624` already returns
  `eligibility_unresolved` unless `response.currency.upper() == intent.currency`.
- `strategy_core_eligibility.evaluate_core_eligibility:214` already returns
  `eligibility_currency_mismatch` unless the response currency matches the requested one.

Both callers refuse non-USD **before** reaching the helper. Arriving here with a non-USD
currency means a caller skipped its own check, which is a bug rather than a state of the
world — the distinction `strategy_core_submission_gate.StrategyCoreSubmissionError` draws
for the unheld lock, and `strategy_core_preflight._require_known_action` draws for an
unknown action on a public entry point (*"a guard that only exists on the other caller's
path is not a guard"*). Scope item 4 is a code change that must handle this deliberately;
a loud failure at that moment is the intended outcome, not a regression.

### Boundary and precision — deliberately unchanged

The comparison stays `amount < minimum` → refuse, so the minimum is INCLUSIVE (an amount
exactly equal to the floor passes). No rounding tolerance is introduced: the portal
publishes no precision rule for either field, and inventing one would be the uncited
treatment this ticket exists to remove.

### Call sites

- `strategy_paper_executor._eligibility_reason` — replaces the `or`. Scope matches: an
  entry is an open (§5 above), it is a buy, and the arm is x1.
- The three docstrings quoted at the top. Each defers a question that now has an answer
  for the OPEN side, and each must be corrected to state precisely what is settled
  (open-side, x1, USD, as a safe bound) and what is not (any close-side floor). A
  docstring that still says "unsettled" after it is settled is the stale-claim shape
  `sql/348` was corrected for on this same ticket; a docstring that claims more than §2-§4
  support would be the opposite error.

**Not in scope, stated so it is not read as an omission:**

- no `sell_core` floor — undocumented, see §4;
- no new stored column — the rule is derived on read, so a re-derivation cannot leave a
  stale copy;
- no account-risk preflight — `observe_core_sleeve` already consumes the snapshot and
  refuses on drift, so a second module would be the duplicate this ticket keeps finding
  rather than the missing control;
- no cost fetch — `strategy_core_sizing.decode_quoted_trade_cost` already decodes one, and
  the FETCH belongs with the submitter that holds the sized ticket;
- no broker-rejection code — that is an outcome of submission, not a preflight.

Refs #2603. Refs #2437.
