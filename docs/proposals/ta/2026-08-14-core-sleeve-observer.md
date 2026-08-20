# Core sleeve observer — one snapshot, one instant, the right question (#2704)

Blocks #2603 item 3 step 3b. Refs #2437.

`evaluate_core_rebalance` needs a `CoreSleeveState`: one instrument's net long market value
plus settled cash, **both valued at one instant from one snapshot** — a warranty its own
docstring places on the supplier and cannot check. Nothing in `app/` can supply it today.

## Source rule

eToro publishes **no per-position market value**, and this spec says so rather than citing a
rule that does not exist. Verified against the live portal
(`https://api-portal.etoro.com/core/guides/calculate-equity.md`, fetched 2026-08-14): the
guide gives one formula,

> Equity = Available Cash + Total Invested + Unrealized PnL

and names exactly four per-position fields — `positions[i].amount`,
`positions[i].unrealizedPnL.pnL`, and the two mirror equivalents. It states no way to
combine them into a single per-position figure.

The aggregate decomposition therefore only *proposes* the treatment: `total_invested` sums
`positions[i].amount` and `unrealized_pnl` sums `positions[i].unrealizedPnL.pnL`
(`calculate-total-invested.md`, same fetch — its formula matches
`_parse_account_risk_snapshot` term for term), so one position's contribution to the equity
identity is `amount + unrealizedPnL.pnL`. ⚠ **Restricting an aggregate to one of its terms
does not establish what that term MEANS.** The semantics are settled below by independent
measurement instead, after `verify_2598_preflight_quote_crosscheck`'s technique.

⚠ The **cash** half needs no work and must not be re-derived. `available_cash = credit -
pending_amount` already matches `calculate-available-cash` exactly, and
`.claude/skills/data-sources/etoro-api.md:492` records the standing rule that portfolio
`credit` is **not** spendable cash.

⚠⚠ **What the cash source does NOT establish.** `CoreSleeveState` warrants cash that is
"settled and unreserved, with pending orders, unsettled proceeds and accrued charges already
deducted". The published formula deducts **pending orders** and nothing else; unsettled
proceeds and accrued charges are not separately identifiable anywhere in this payload. That
part of the warranty is carried as a **known limitation of the source**, not as a verified
property — recorded here and in the observer's docstring because a warranty quietly assumed
is the exact defect class #2704 exists to remove. It is not a reason to prefer a second
call: `get_portfolio()` establishes even less and breaks the one-snapshot warranty outright.

Direction: `isBuy` is documented on `clientPortfolio.positions[]` ("true for long (buy)
positions, false for short (sell) positions", portal `get-account-pnl-and-portfolio-details`,
fetched 2026-08-14).

## Verification — live demo account, 2026-08-14T13:07Z

Informational `GET /api/v1/trading/info/demo/pnl`, no mutation.

⚠ **Scope of the claim.** This is every instrument and every position **this one demo
account reported at one instant** — not a population of accounts, products, directions or
payload states. What it can settle, it settles completely; what it cannot is named at the
end.

### Arm 1 — independent cross-check of the semantics

The P&L payload and our own `quotes` feed have never read each other. If `amount + pnL` is a
position's market value then `(amount + pnL) / units` is an implied per-unit price and must
land on the separately observed quote; if it were a rate, a notional or a fee-adjusted
figure it would match nothing. Symmetrically, if `amount` is cost basis then
`amount / units` must land on `openRate`, which the payload reports independently of it.

| instrument | units | openRate | `amount/units` | cost err | implied px | `quotes.last` | mv err | quote age |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1699 | 1000.0 | 22.59 | 22.5900 | 0.00% | 18.5000 | 18.57 | -0.38% | 47 min |
| 1699 | 500.0 | 22.66 | 22.6600 | 0.00% | 18.5000 | 18.57 | -0.38% | 47 min |
| 1571 | 244.0 | 8.18 | 8.1800 | 0.00% | 7.5000 | 7.50 | **0.00%** | 17 h |
| 1571 | 71.948108 | 9.25 | 9.2499 | -0.00% | 7.4998 | 7.50 | **-0.00%** | 17 h |
| 1181 | 1305.057096 | 6.09 | 6.0900 | 0.00% | 4.4200 | 4.42 | **-0.00%** | 17 h |
| 4238 | 16.929336 | 590.69 | 590.6894 | -0.00% | 714.8195 | 714.82 | **-0.00%** | 17 h |
| 3006 | 17.252438 | 579.62 | 579.6195 | -0.00% | 733.8598 | 733.31 | +0.07% | 21 min |

**Both interpretations are established by measurement, not by decomposition:**

- **`amount` IS cost basis** — `amount/units` reproduces `openRate` to ±0.005% on **7/7**.
- **`amount + pnL` IS market value** — the implied price lands on an independently fed quote
  at **-0.00%** on four positions. The two residuals are the two positions whose quote is
  *freshest relative to nothing* — 1699 at -0.38% and 3006 at +0.07% — i.e. the error tracks
  quote staleness, not the formula.
- The clincher: **1699's two lots opened at different rates (22.59, 22.66) and imply the
  same current price (18.5000 both)**; likewise 1571's 8.18/9.25 → 7.5000/7.4998. Only a
  genuine per-unit mark behaves that way.

### Arm 2 — what `instrument_investments` reports today

Every instrument in the tuple:

| instrument | `amount` (committed) | direct long market value | delta | delta % |
| --- | --- | --- | --- | --- |
| 1181 | 7,947.80 | 5,768.35 | -2,179.45 | **-27.42%** |
| 1571 | 2,661.43 | 2,369.60 | -291.83 | -10.97% |
| 1699 | 33,920.00 | 27,765.00 | -6,155.00 | -18.15% |
| 3006 | 9,999.85 | 12,665.70 | +2,665.85 | **+26.66%** |
| 4238 | 9,999.98 | 12,101.42 | +2,101.44 | +21.01% |
| 33 others | 283.53 … 2,452.75 | **0.00** | -100% | mirror-only |

1. **Drift spans -27.42% to +26.66%** on the five real holdings. On 3006 the committed
   figure understates value by a quarter, so an allocator would compute a **buy** for a
   sleeve already 27% richer than it thinks. **38 of 38** rows disagree with market value.
2. **`unrealizedPnL.pnL` is present per position and then discarded per instrument** — the
   payload already carries every term needed.
3. **33 of 38 reported instruments have no direct position whatsoever.** They are
   copy-trader mirror holdings folded in because the *total invested* formula folds them.
   Sharper than the ticket's "a mirror holding the same ETF would count": here the
   overwhelming majority of rows are *entirely* somebody else's portfolio.
4. **Lots are real.** 1571 = 1,995.92 + 665.51 and 1699 = 22,590.00 + 11,330.00, so the
   "lots netted" clause is exercised, not theoretical.

### Not settled by this run — named, not glossed

- **Shorts are entirely unobserved.** `isBuy` was present on 7/7 and `true` on 7/7. Field
  presence, sign behaviour and the meaning of `amount + pnL` for a short are all specified
  and unit-tested here, and **none of them is measured**. This is why the observer refuses
  on a short rather than valuing one.
- **`ordersForOpen` and `orders` were both empty**, so the pending-order fold — and
  therefore `available_cash`'s deduction term — is confirmed by portal doc and code reading
  only.
- **`isBuy`'s promotion to a required field rests on 7 rows**, all from one account.
- The payload carries **no broker valuation timestamp** (`clientPortfolio` keys observed:
  `accountCurrencyId`, `bonusCredit`, `credit`, `entryOrders`, `exitOrders`, `mirrors`,
  `orders`, `ordersForClose`, `ordersForCloseMultiple`, `ordersForOpen`, `positions`,
  `stockOrders`, `unrealizedPnL`). See "one payload" below.

## Design

### 1. `BrokerInstrumentInvestment` gains three fields

```python
instrument_id: int
amount: Decimal                    # unchanged: committed capital, all ownership
direct_long_market_value: Decimal  # Σ(amount + pnL) over DIRECT positions, isBuy true
direct_long_positions: int         # how many such positions
direct_short_positions: int        # DIRECT positions with isBuy false
```

⚠ **`amount` keeps its meaning exactly.** Three live controls read it as committed capital
and are right to: `_risk_and_amount`'s `instrument_capacity`, and the `portfolio_capacity` /
`drawdown` gates sharing the snapshot. This is an added field, never a changed one.

⚠⚠ **The short arm is a COUNT, not a money sum, and that is the whole point.** The observer
must be able to *tell* a short exists before it can refuse one — and no monetary total can
carry that fact. Two short lots can sum to zero through opposing contributions, and a single
short can sit at `amount + pnL == 0`, so `direct_short_market_value == 0` would be
indistinguishable from "no short". That is the #2623 shape exactly: a control that cannot
express a state the system can reach. A count can only be zero when there is nothing to
count. The short leg's *money* has no consumer here, so it is not carried at all.

The long count exists for the same reason one level down: `direct_long_market_value == 0`
is ambiguous between "no direct holding" and "a holding wiped out to zero". The **33 of 38
mirror-only rows** in Arm 2 are exactly the first case, and only the count distinguishes
them. `direct_long_positions == 0 and direct_short_positions == 0` is the honest way to
say "this row is entirely mirror and pending orders".

⚠ **No defaults on the new fields.** A default would let a future producer omit them and
silently erase short information at the one place the refusal depends on. Every construction
site is updated in this PR instead — there is one producer and a handful of test sites.

### 2. `isBuy` becomes a required field on the direct-position parse

Absent or non-boolean `isBuy` raises `TradingPreflightParseError`. This is consistent with
every other formula input on the same loop (`amount`, `unrealizedPnL.pnL`, `instrumentID`
all already fail closed) and differs deliberately from `accountCurrencyId`, whose leniency
was justified on that field *not* being a formula input. Measured present 7/7 live.

### 3. Bounds: a negative direct market value must NOT fail the parse

The existing guard refuses negative `investments` values. The new fields are **excluded**
from it, and the asymmetry is the point:

- `amount` is a sum of documented non-negative terms, so a negative is a **malformed
  response** — fail closed.
- `amount + pnL` sums a signed term, so a negative is an **extreme but legitimate state** (a
  levered position under water past its committed capital). Failing the parse on it would
  take the paper executor's unrelated cash checks down with it, which is the blast-radius
  trade `_account_currency_id` already reasons about.

The refusal still exists, one layer up and correctly labelled: `_state_refusal` returns
`sleeve_valuation_invalid` on `core_market_value < 0`.

### 4. `observe_core_sleeve` — new, in `app/services/strategy_core_sleeve.py`

```python
def observe_core_sleeve(
    snapshot: BrokerAccountRiskSnapshot, *, core_instrument_id: int
) -> CoreSleeveState
```

Pure; no connection, no clock, no broker call.

⚠⚠ **What "one snapshot" does and does not buy.** Both money components come from ONE HTTP
payload, so they are **mutually consistent** — whatever instant the broker computed them
for, it is the same instant for both. That is the property `CoreSleeveState` actually needs
and it holds by construction, not by discipline. It is **not** a broker valuation timestamp:
`observed_at` is assigned with `datetime.now(UTC)` after the response returns
(`etoro_broker.py:826`), so `as_of` is our **receipt** time, and the payload carries no
valuation stamp of its own (measured — see the observed key list above). The docstring says
receipt time, because calling it a valuation instant would be the same defect one layer up.

Raises `CoreSleeveObservationError` when the sleeve is not observable, in this **declared
precedence** so a snapshot with several defects fails the same way every time:

| # | condition | why it is a refusal and not a value |
| --- | --- | --- |
| 1 | `observed_at` is naive | an instant without a zone cannot be attributed; `_validate_snapshot` sets the precedent |
| 2 | `account_currency_id is None` | #2602 item 2 — an absence to refuse on, never a licence to assume USD |
| 3 | id not in `DOCUMENTED_ACCOUNT_CURRENCIES` | the code would be inferred; same defect |
| 4 | duplicate rows for the core instrument | the parser emits unique rows, but the dataclass is publicly constructible; first-match silently drops a holding, summing silently double-counts. Neither is a number worth having |
| 5 | `direct_short_positions > 0` on the core instrument | the sleeve's value is *long*; folding a short in misstates it, dropping it misstates it the other way, and shorts are unobserved (above) |

Raising rather than returning a verdict: the allocator deliberately never raises, because a
caller that catches to learn the mandate is disabled will catch too broadly. The observer is
a different layer — all five are input drift, not verdicts, and `AccountEquityEvidenceError`
is the settled in-repo precedent for exactly this class.

⚠ Finiteness and magnitude of the money values are **not** re-checked here. `_state_refusal`
already refuses `sleeve_valuation_invalid` on non-finite, negative and out-of-range
components, and duplicating it would be verification that changes no outcome.

`currency` is taken from `DOCUMENTED_ACCOUNT_CURRENCIES[snapshot.account_currency_id]` — the
existing single source, never a literal, because `_state_refusal` compares it to the
mandate's base currency as a **label**.

⚠ The value reported is the **gross** direct long total. It equals net long exactly because
condition 5 refuses whenever a short could make the two differ; the spec avoids the bare
phrase "net long" for the field, since gross and net coincide only under that refusal.

An instrument with no row in `instrument_investments` yields `core_market_value = 0` with
both counts at zero — the same true statement as a row that is entirely mirror. The
allocator's `core_sleeve_empty` handles the resulting state.

## Not in scope

The rest of step 3b: order sizing against quoted cost, the submission, the uncertain-response
resume path, and the refusal vocabulary the step-3a gate deliberately does not carry (kill
switch, `enable_auto_trading`, execution block, market session, quote staleness, broker
minimum). This module gets **no caller** — same posture as every other piece of item 3, and
re-stated here so the next session does not have to infer it.

⚠ No broker mutation anywhere in this change. The one live call made while writing it was an
informational GET.
