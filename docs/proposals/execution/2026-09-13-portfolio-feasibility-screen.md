# Prospective portfolio feasibility screen (#2947)

Status: proposal. Refs #2947. Related #2833, #2603, #2508, #2772.

## Question

Can a candidate strategy portfolio be rejected as operationally impossible — against the
assigned pot, the planned weights and the account's recorded broker capabilities — BEFORE
expensive outcome evaluation, without contaminating the historical research universe?

This adds no alpha claim and reopens no strategy verdict. It is a dry-run report.

## What it is NOT

`strategy_core_eligibility.require_core_eligibility` and
`strategy_core_broker_preflight.assess_core_broker_preflight` are the **execution** gates:
they run on the submission path, per leg, and they refuse. Neither re-fetches eligibility —
`require_core_eligibility` reads a recorded proof and the broker preflight is handed
already-proved figures (`strategy_core_broker_preflight.py:283-286`). The difference from
this screen is therefore NOT freshness; it is authority and arity. This screen is
**prospective, portfolio-wide and advisory** — it writes nothing, submits nothing, and its
output authorises no trade. An advisory verdict must never reach the execution path.

It is also not a selection filter. Step 4 of #2947 is binding: today's broker map must not
enter historical sample selection, and a failed screen must not license a post-result
top-N cut. The screen answers "can this be held", never "which names to hold". Concretely it
takes the candidate's OWN leg list and weights and returns a verdict per leg; it never
returns a subset, and a leg with no broker mapping is reported as unmapped rather than
dropped, so the caller cannot silently shrink a portfolio by screening it.

## Source rule

`.claude/skills/data-sources/etoro-api.md` (§ eligibility / what-if endpoints) and the live
portal page `trading--demo/check-instrument-trading-eligibility`, already transcribed and
cited in `app/services/broker_settlement_arms.py:186-217`:

- `minPositionExposure` (eligibility ROW) — *"Minimum exposure value required to open a
  position on this instrument. The exposure is always calculated in **USD** as the number of
  units times the rate times the conversion rate to USD."*
- `minPositionAmount` (a `leverageConfigs` ARM) — *"Minimum margin required to open a
  position under this leverage configuration."*

Two different quantities, hence `effective_open_minimum` takes `max`, not `or`
(`broker_settlement_arms.py:178`). Both are **OPEN-side only**; the portal says nothing about
close or partial-close sizing, so this screen derives no close-side floor.

⚠ `effective_open_minimum` **raises `ValueError` on a non-USD `response_currency`** by
design — `minPositionAmount` carries no documented currency, and its docstring records that
every caller must refuse a mismatch *before* calling it
(`broker_settlement_arms.py:219-234`). This screen therefore refuses
`eligibility_currency_unsupported` ahead of the call; reaching the raise would be a defect in
this module, not a verdict about the world.

"Held as the underlying product, long, unleveraged" is
`broker_settlement_arms.select_underlying_long_arms` — `settlementType == real`,
`direction == long`, `leverage == 1`, unknown values failing closed
(`broker_settlement_arms.py:103-116`). Reused, not re-derived. The stored
`qualifying_arm_count` is that function's own count, so this screen inherits its arm
semantics — including that it does not read the response's `isPotential` flag. That is a
#2603 decision on the recording path; it is named here as an inherited assumption rather
than silently adopted, and changing it belongs to the recorder, not to this screen.

## What the screen measures, and what it cannot

The eligibility row exposes more dimensions than the open minimum: `maxUnitsPerOrder`,
fractional/whole-unit capability, permitted order and quantity types, and close-side
permissions. This screen tests **only** the open-side floor, the underlying-arm question and
open permission. So its passing verdict is `not_refused`, **never** `deployable`:

- order costs and unit rounding are excluded — an allocation sitting exactly on the floor can
  fall under it once either applies. No fee buffer is invented here, because a buffer would be
  a threshold with no source rule.
- `maxUnitsPerOrder`, fractional-unit capability and order-type support are unchecked.
- the close side is **unknown, not unconstrained** — the report carries that explicitly.
- legs are treated as fresh opening orders. An incremental buy or a rebalance into an
  existing position has different sizing needs and is out of scope.
- `assigned_capital` is a caller precondition: the screen cannot tell invested, unsettled or
  reserved funds from free cash, and says so rather than assuming the pot is spendable.

Each of these is a named field on the report, not an omission, so a reader cannot mistake
"not refused" for "checked".

## Full-population verification (measured 2026-09-13, dev DB)

`strategy_core_eligibility_proofs` in full: **23 rows over 20 distinct instruments, 2
observation dates (2026-08-13 and 2026-08-22), 1 environment (`demo`), newest row
`2026-08-22T21:24:01Z`.** Three instruments carry two proofs each; no row has a NULL
`allow_open_position` and no row has `qualifying_arm_count > 1`.

```sql
-- census
SELECT count(*) total, count(DISTINCT instrument_id) instruments,
       count(*) FILTER (WHERE allow_open_position IS NULL) null_open,
       count(*) FILTER (WHERE qualifying_arm_count > 1) multi_arm,
       count(DISTINCT observed_at::date) dates, count(DISTINCT environment) envs,
       max(observed_at) newest
  FROM strategy_core_eligibility_proofs;
-- latest proof per instrument, which is what a loader must select
SELECT DISTINCT ON (p.instrument_id, p.environment)
       i.symbol, p.verdict, p.reason_code, p.qualifying_arm_count, p.allow_open_position,
       p.response_currency, p.min_position_exposure, p.min_position_amount, p.observed_at
  FROM strategy_core_eligibility_proofs p LEFT JOIN instruments i USING (instrument_id)
 WHERE p.environment = 'demo'
 ORDER BY p.instrument_id, p.environment, p.observed_at DESC;
```

Latest-per-instrument verdicts, all `demo`, every `min_position_exposure` = 10 USD:

| instruments | verdict | qualifying arms |
| --- | --- | --- |
| `SPY`, `IVV`, `VOO`, `MTUM`, `SPMO`, `QUAL`, `SPHQ`, `IUSV`, `IWD`, `AVUV` | `not_underlying` (`no_underlying_arm`) | 0 |
| `SPY.RTH`, `QQQ.RTH`, `CSPX.L`, `IUSA.L`, `R1VL.L`, `IUQA.L`, `IUMO.L`, `QDVA.DE`, `QDVB.DE`, `QDVI.DE` | `underlying` | 1 |

Two findings, and the second is the one that matters:

**1. The plain US ETF listings are CFD-only on this account; the `.RTH` / UCITS listings are
not.** `SPY`/`IVV`/`VOO` carry zero qualifying x1 `real` long arms. A CFD is a contract with
the broker, not ownership (project `CLAUDE.md`, risk posture), so a sleeve written against
the plain tickers would not be a held beta sleeve at all. ⚠ **#2833 does not make this
mistake** — its checked-in declaration
(`docs/proposals/ta/2026-08-24-core-selection-declaration.json`) names `candidate_ids`
`[3417, 3434, 3075]` = `SPY.RTH`, `CSPX.L`, `IUSA.L`, and all three are recorded
`underlying`, one arm, open permitted, 10 USD floor. An earlier draft of this spec claimed the
#2833 sleeve was unholdable; that was wrong, caught at Codex checkpoint 1, and is recorded
here because the corrected version is the weaker and more useful claim: the screen's value on
#2833 is confirming a declaration that was already right, not catching an error.

**2. Every stored proof is 22 days old, so the screen applied TODAY refuses the #2833 sleeve
on staleness, not on capital.** This is the operationally useful output: the sleeve's
deployability evidence has expired, and a re-census is a precondition of any allocation. A
recorded verdict is a measurement of an account on a date, never a permanent capability —
which is why proof age is a refusal dimension and not a footnote.

⚠ **This census is coverage of the stored proofs, not of any candidate population.** Twenty
instruments on two dates say nothing about the 4,034-name R6 portfolio or about any name
without a proof; those legs refuse `eligibility_unrecorded`, which is the correct answer and
not a coverage claim.

## The constraint

Let `C` = assigned capital in the pot currency, `q` = USD per unit of pot currency, `r` = cash
reserve fraction, `w_i` = leg `i`'s target weight as a fraction of investable capital, and
`m_i` = leg `i`'s open minimum in **USD**.

```
investable_usd        = C * (1 - r) * q
leg_notional_usd(i)   = investable_usd * w_i
feasible(i)           <=>  leg_notional_usd(i) >= m_i
min_pot_for(i)        =  m_i / (w_i * (1 - r) * q)          [pot currency]
minimum_viable_pot    =  max over capital-curable legs of min_pot_for(i)
```

`q` is load-bearing and was missing from an earlier draft: `m_i` is USD and `C` is not, so
comparing them without it is precisely the #2947 error being prevented (the R6 memo's
`50,000 / 4,034 = 12.39` is in pot currency and was never a USD comparison). A non-USD pot
with no supplied `q` refuses `capital_currency_unconvertible`; a USD pot must be supplied
`q = 1` exactly, and any other value for a USD pot is a caller defect, not a rate.

Worked negative control, corrected: 4,034 equal legs at `m = 10 USD`, `r = 0`,
`q = 1` needs `min_pot = 10 / ((1/4034) * 1 * 1) = 40,340 USD`. So a **USD** 50,000 pot
CLEARS it, and an earlier draft's claim that the 4,034-name portfolio "must refuse" was
wrong. It refuses only once `q`, `r` or a larger `m_i` push the threshold past the pot — e.g.
a £50,000 pot at `q = 1.25` gives `investable_usd = 62,500`, which also clears. The R6
portfolio's failure is its RETURN, not its affordability, and the screen must not be sold as
having caught it. The fixture asserts the derived threshold, not a refusal.

## Refusal vocabulary and precedence

Evaluated in this order per leg, first match wins, so overlapping conditions have one
defined answer rather than an order-dependent one. Fail-closed throughout: unknown is never
feasible.

| # | code | meaning |
| --- | --- | --- |
| 1 | `duplicate_leg` | instrument appears in more than one leg — weights would double-count |
| 2 | `weight_not_positive` | `w_i <= 0`, or not finite |
| 3 | `weight_above_unity` | `w_i > 1` |
| 4 | `eligibility_unrecorded` | no proof for this `(instrument, environment)` |
| 5 | `eligibility_account_mismatch` | proof's provider / environment / operator / credential ids differ from the declared account scope |
| 6 | `eligibility_proof_stale` | proof older than `max_proof_age`, or future-stamped beyond the skew allowance |
| 7 | `eligibility_unresolved` | recorded `verdict == "unresolved"`, or a `reason_code` in `UNRESOLVED_REASONS` |
| 8 | `not_underlying_product` | recorded `verdict == "not_underlying"` — the broker answered, and the answer is no |
| 9 | `eligibility_currency_unsupported` | proof `response_currency` is not USD — refused BEFORE `effective_open_minimum` |
| 10 | `arm_selection_ambiguous` | `qualifying_arm_count > 1` — many is a different answer from one |
| 11 | `eligibility_projection_inconsistent` | pass verdict with `qualifying_arm_count == 0` — the row contradicts itself |
| 12 | `open_permission_unrecorded` | `allow_open_position IS NULL` (the column is nullable) |
| 13 | `open_not_permitted` | `allow_open_position` false |
| 14 | `open_minimum_unrecorded` | `effective_open_minimum` returns `None` |
| 15 | `below_open_minimum` | `leg_notional_usd(i) < m_i` |
| — | `feasible` | none of the above |

⚠⚠ **The recorded VERDICT is read before any field on the row, and an earlier draft of this
spec got that wrong.** It classified the underlying question from `qualifying_arm_count`
alone. `evaluate_core_eligibility` leaves that count at its default `0` for an `unresolved`
proof — `instrument_not_resolved`, `eligibility_row_ambiguous`,
`eligibility_currency_mismatch`, `eligibility_arm_ambiguous` — because it never evaluated an
arm. Reading the default as a measurement converts *"the response did not answer the
question"* into a definitive `refused`, i.e. rejects a candidate portfolio on an incomplete
broker response. Caught at Codex checkpoint 2, which reproduced it by calling the real
recorder with a not-found instrument id and getting `refused / not_underlying_product` out of
the screen. The recorder already draws the line
(`strategy_core_eligibility.py:74-76`: *"`unresolved` means the response did not answer the
question; `not_underlying` means it answered and the answer is no. Only the second is a fact
about the instrument."*) so the screen reuses that vocabulary rather than re-deriving one.

Codes 9-11 are therefore **defence in depth**, reachable only for a projection that did not
come from `evaluate_core_eligibility`: such a row can claim the pass verdict while carrying
field values that contradict it, and a claim is not a measurement.

⚠ **`not_underlying_product` is still narrower than "CFD only".** The recorder reaches it via
`instrument_not_open` or `no_underlying_arm`, and zero qualifying arms can also mean a
leveraged-only or short-only offering. The code names the measurement; only the census above,
where every zero-arm row is an ordinary US-listed ETF, supports the CFD reading for those
specific instruments.

Portfolio-level codes, evaluated before the legs:

| code | meaning |
| --- | --- |
| `capital_not_positive` | `C <= 0` or not finite |
| `reserve_out_of_range` | `r < 0` or `r >= 1` — `r = 1` is excluded because it makes `min_pot_for` a division by zero |
| `capital_currency_unconvertible` | non-USD pot and no `q`, or `q <= 0`, or not finite |
| `weights_do_not_sum` | `abs(sum(w) - 1) > weight_tolerance` |

`weight_tolerance` defaults to `Decimal("1e-6")`, absolute, and is a declared parameter on the
report rather than a constant buried in the comparison. It exists because exact equality is
unreachable in `Decimal`: `1/4034` summed 4,034 times is not 1. It is deliberately tight —
accepting `sum(w) > 1` spends past investable capital and accepting `sum(w) < 1` invents
undeclared cash, so the tolerance covers representation error only, never intent.

⚠ **Every numeric domain check uses `Decimal.is_finite()` before any comparison.** Python
raises `InvalidOperation` on `Decimal("NaN") <= 0` rather than returning False (memory:
Postgres NUMERIC NaN is not IEEE and Python's is not Postgres's either), so a comparison-first
guard would raise instead of refusing.

## Verdict trichotomy

`feasible` / `refused` / `indeterminate`. The third is required: missing or stale evidence
justifies withholding feasibility but does not establish operational impossibility, and a
binary verdict would report "this cannot be held" when the truth is "we have not looked
recently". Codes 4, 5, 6, 7, 9, 10, 11, 12 and 14 are **indeterminate**; 1, 2, 3, 8, 13 and 15 are
**refused**. A portfolio is `feasible` only when every leg is; otherwise it takes the more
severe of the two, with `refused` outranking `indeterminate` — one leg that genuinely cannot
be opened sinks the portfolio regardless of what is unknown elsewhere.

`minimum_viable_capital` is reported only where more capital is the cure — i.e. for
`below_open_minimum` and for `feasible` legs. It is `None` for every other code, because
no pot size fixes a CFD-only listing, a prohibited open, a duplicate leg or an unknown floor.
The portfolio-level figure is the max over legs that have one, and is accompanied by the count
of legs whose floor is unknown, so a reader cannot mistake a max-over-known for a max.

## Shape

New pure module `app/services/portfolio_feasibility.py` — no DB, no HTTP, no clock of its own
(`now` is a parameter, per `strategy_core_broker_preflight.py:239`). Inputs are dataclasses the
caller assembles from `strategy_core_eligibility_proofs`; the loader stays in the caller so the
screen is table-testable.

```python
@dataclass(frozen=True)
class AccountScope:      provider: str; environment: str; operator_id: UUID
                         api_key_credential_id: UUID; user_key_credential_id: UUID
@dataclass(frozen=True)
class FeasibilityLeg:    instrument_id: int; symbol: str; weight: Decimal
@dataclass(frozen=True)
class LegEligibility:    instrument_id: int; scope: AccountScope; observed_at: datetime
                         verdict: CoreEligibilityVerdict; reason_code: str | None
                         response_currency: str; qualifying_arm_count: int
                         allow_open_position: bool | None
                         min_position_exposure: Decimal | None
                         min_position_amount: Decimal | None
@dataclass(frozen=True)
class LegFeasibility:    leg: FeasibilityLeg; code: LegReasonCode
                         disposition: Literal["feasible", "refused", "indeterminate"]
                         open_minimum_usd: Decimal | None
                         allocated_notional_usd: Decimal | None
                         minimum_viable_capital: Decimal | None
                         proof_age: timedelta | None
@dataclass(frozen=True)
class FeasibilityReport: verdict: Literal["feasible", "refused", "indeterminate"]
                         portfolio_code: PortfolioReasonCode | None
                         legs: tuple[LegFeasibility, ...]
                         minimum_viable_capital: Decimal | None
                         legs_with_unknown_minimum: int
                         investable_usd: Decimal | None; weights_sum: Decimal
                         # provenance, so the numbers are auditable without a second read
                         evaluated_at: datetime; scope: AccountScope
                         capital_currency: str; usd_per_capital_unit: Decimal | None
                         cash_reserve_fraction: Decimal; weight_tolerance: Decimal
                         max_proof_age: timedelta; policy_version: str
                         # named unknowns, so "not refused" cannot read as "checked"
                         close_side_floor: Literal["unknown"]
                         costs_and_rounding_excluded: Literal[True]
                         max_units_per_order_checked: Literal[False]

def screen_portfolio_feasibility(*, assigned_capital, capital_currency,
                                 usd_per_capital_unit, cash_reserve_fraction, scope,
                                 legs, eligibility, now,
                                 max_proof_age, weight_tolerance) -> FeasibilityReport
```

`policy_version` is frozen at `portfolio-feasibility-v1` so a later change to the precedence
order or the vocabulary is visible in an artifact rather than silently reinterpreting one.

## Tests (pure, no DB)

Table tests, one per row of both vocabularies, plus:

- the #2947 fixture set: inadequate capital (asserting the derived pot, not just the code),
  unrecorded mapping, unrecorded minimum, non-USD pot with no rate, CFD-only using the
  measured `SPY` shape, and a known-feasible case using the measured `SPY.RTH` shape.
- **`now` is explicit in every fixture.** The measured proofs are dated 2026-08-22; reusing
  their shape with a real clock would assert staleness, not feasibility.
- successful non-USD conversion (`q != 1` with a sufficient pot), reserve arithmetic
  (`r > 0` raising the required pot), unequal minima across legs, and both sides of each
  threshold boundary (`notional == m_i` feasible, one minor unit below refused).
- precedence: a leg that is simultaneously stale and zero-arm returns `eligibility_proof_stale`;
  a duplicate that is also over-weight returns `duplicate_leg`.
- domain guards: `NaN` / `Infinity` capital, weight, `q` and reserve each refuse rather than
  raise; `r = 1` refuses `reserve_out_of_range` rather than dividing by zero.
- the 4,034-leg control, asserting `minimum_viable_capital == 40_340` at `q = 1`, `r = 0` and
  that it does NOT refuse a 50,000 USD pot.
- the #2833 sleeve: its three declared candidates with their measured shapes are `feasible`
  at a stated `now` inside the age bound, and `indeterminate` / `eligibility_proof_stale` at
  a `now` of 2026-09-13.

## Out of scope

Live eligibility census or any broker call; allocation; the close-side floor; `maxUnitsPerOrder`
and fractional-unit capability; cost and rounding haircuts; the `isPotential` arm question
(owned by the recorder). Whether #2833 re-censuses before allocating is #2833's call; this
ticket reports that its evidence has expired.
