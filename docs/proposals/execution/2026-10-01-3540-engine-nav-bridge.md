# #3540 — engine-book NAV/P&L bridge (gap register P1)

Status: revised after Codex ckpt-1 (74 findings; disposition at the end). Source:
`docs/proposals/2026-10-01-gap-register.md` §P1 (PR #3549).

## Goal

For each interval between two consecutive broker snapshots, state the engine book's pot NAV
change as named components, and check each broker figure against an independent
recomputation. Two statuses are reported separately and never conflated:

- **closed** — the statement is arithmetically complete: every owned position is accounted
  for at both endpoints and every lifecycle event in the interval is present.
- **validated** — every check below is within its bound.

There is no independent broker NAV for the engine pot (the account also holds operator,
copy-mirror and supervisor positions — P2, #3541), so "closed" proves completeness, not
correctness. Correctness comes from the checks. An unknown is a named state with amount
`None`, never 0.

## Population

- **Engine book** = every `strategy_position_ownership` row, any status. The bridge also
  reports `trade_without_ownership`: a `strategy_trades` row in `open`/`closing`/`closed` with
  no ownership row (it would otherwise be invisible). It reports the count of owned positions
  NOT linked to a `core_rebalance_intent_id`, so "engine book = core sleeve" (true today, 5/5)
  is checked per run, not assumed.
- **Principal** = `strategy_paper_pool_events.capital_limit` of the latest event with
  `changed_at ≤ observed_at` (ties → highest id). None → state `principal_unobserved`, interval
  not closed. Convention, same as `strategy_wealth` and the TWR spec (#3334): a change in the
  pool limit is the engine's external flow.
- **Snapshots** = `broker_account_equity_snapshots` (environment `demo`) + child
  `broker_account_position_marks` of the same date. Interval t = `(observed_at[t-1],
  observed_at[t]]`. `--sessions N` = the last N intervals, so N+1 snapshots are read. The
  interval length in days is printed; a gap > 1 calendar day is printed, not hidden.
- Everything is computed on read in one `REPEATABLE READ` transaction; a late close, ownership
  fix or same-day snapshot rewrite restates every affected interval on the next run. Nothing is
  persisted, so there is no stale opening value to carry.

## Statement

Lifecycle per owned position, from `trade_events`: one `open` row and zero or more `close` rows
(`realized_pnl_usd` = broker `netProfit`). Membership at instant s: open `executed_at ≤ s` and
cumulative closed units `<` opened units (a position is fully closed when closed units reach
opened units).

```
opening_nav            = closing_nav of interval t-1 (recomputed, not stored)
flows                  = principal[t] − principal[t-1]
realised               = Σ netProfit of owned close rows with executed_at in the interval
unrealised_released    = − Σ pnL[t-1] of positions fully closed in the interval
unrealised_opened      = + Σ pnL[t] of positions opened in the interval
unrealised_continuing  = Σ (pnL[t] − pnL[t-1]) of positions marked at both endpoints
  of which fx_effect   = Σ sign·units[t]·close_rate[t]·(conv[t] − conv[t-1])   (convention: closing price)
  price_effect         = unrealised_continuing − fx_effect
closing_nav            = principal[t] + Σ owned netProfit with executed_at ≤ observed_at[t] + Σ pnL[t]
```

A same-interval open+close contributes only to `realised`. Realised P&L before the window is
read in full (not windowed), so the opening NAV is absolute.

**Fees and distributions are a memo line, not a term.** Source rule:
`TradingDemoApi_Position.totalFees` — *"Total overnight fees and dividends charged/paid on the
position in USD. Negative amount represents refund"* (`tests/fixtures/etoro/openapi_v1.375.0.json`);
one signed number, not separable (`strategy_monitoring.load_owned_pnl`). Whether `pnL` /
`netProfit` already include it is undocumented, and has never been observable: every stored
value is zero (`select count(*), count(total_fees), count(*) filter (where total_fees<>0) from
broker_positions` → 11/11/0; same on `broker_positions_closed` → 4/4/0; close-row `fees_usd`
→ 11/11/0). So the bridge does not infer the treatment. Per interval the memo is one of:
`fees_zero` (every relevant observation present and 0); `fees_unobserved` (any NULL — names the
positions); `fees_nonzero_inclusion_unresolved` (amount = the nonzero values; interval NOT
validated). Relevant observations: `total_fees` on both endpoint marks of every marked
position, plus `broker_positions_closed.total_fees` and close-row `fees_usd` for every close in
the interval. The first nonzero value is the evidence that resolves inclusion; it is resolved
then, by a human-readable comparison, not by this code.

This needs one additive nullable column, `broker_account_position_marks.total_fees`, parsed
from `totalFees` on the same `/pnl` position row. History stays NULL → `fees_unobserved`, so
validation starts at the second snapshot after deploy.

## Checks (each a residual with signed amount = broker − recomputed, and its bound)

Recomputation is defined for **USD instruments only** (`asset_currency_id = 1`, conv = 1),
long or short at leverage 1 — the mandate bars leverage. Anything else → state
`not_recomputable` (named, amount None, interval not validated). Rates come from the broker's
own rows: open rate = the close row's `openRate` / the open event's `price`; mark rate =
`close_rate`. `amount` is NOT used as cost basis (it can include added collateral).

| code | broker | recomputed | bound |
| --- | --- | --- | --- |
| `mark_pnl` | pnL[t] | sign·units·(close_rate − open_rate) | 0.005 + units·(½q(close_rate) + ½q(open_rate)) |
| `close_netprofit` | netProfit | sign·units_closed·(closeRate − openRate) | 0.005 + units_closed·(½q(closeRate) + ½q(openRate)) |
| `close_open_rate` | close row `openRate` | open event `price` | 0 |
| `open_investment` | open event `investment_usd` | units·open_rate | 0.005 + units·½q(open_rate) |
| `units_conserved` | units[t] | units[t-1] − Σ units closed in interval | 0 |
| `mark_incomplete` | any NULL among units/pnL/close_rate/conv/currency on a member mark | — | named, interval not closed |
| `position_altered` | `is_partially_altered` true on a member mark | — | named, interval not validated |

Each snapshot is checked once, as a closing endpoint. The first interval also checks what its
opening NAV rests on: the opening marks, every earlier close and every earlier open (Codex
ckpt-2). Lifecycle breaks that close a statement without a number are named at every endpoint:
`open_event_missing` from the ownership claim time, `units_over_closed`, `close_units_missing`,
and `close_netprofit_missing` for ANY close up to the endpoint, not just the interval's.

`q(x)` = the rate's quantum: 10^−max(2, decimals of x after stripping trailing zeros). By
construction, not published. JSON numbers lose trailing zeros, so an observed 1 dp (`762.8`)
is indistinguishable from 2 dp; finer observed precision is taken as the quantum. If the true
quantum is coarser than 0.01 the bound is too tight and the check fails LOUD (a false residual,
never a hidden one). Units are treated as exact for the same reason. Measured over all 145 demo
marks (2026-10-01, `units·close_rate·conv` vs `market_value`, all positions incl. non-engine):
max 0.0144, 21 rows over 1 cent, all on 16–72-unit positions where `units·0.005` ≥ 0.08 — inside
this bound. An observed maximum does not prove the bound; it only fails to falsify it.

## Surface

Pure function `bridge_intervals(...)` over loaded rows (`app/services/engine_nav_bridge.py`),
one loader, and a read-only script `scripts/engine_nav_bridge.py --sessions N`.

## Acceptance

Over 5 consecutive intervals after deploy, every interval is closed and validated, or every
residual and state is named with its amount (None where unknown). Evidence run on the dev DB,
posted on #3540. Partial closes, shorts, refunds, round trips and non-USD positions are
exercised by table tests, not by the live window (none occur in it).

## Out of scope

Demo→live transfer (#3471 spec :37), account-level provenance (P2, #3541), benchmark
comparison, fee-inclusion inference.

## Codex ckpt-1 disposition (74 findings)

Applied: two statuses, not one (13, 70, 72); unrealised split into released/opened/continuing
(8–10, 12); fee inference removed — memo states only (4, 5, 43–55, 62); cost basis from open
rate, not `amount` (1, 31, 32); USD-only recomputation (2, 9, 59, 73); quantity conservation
replaces drift checks (27–30); principal as-of instant, no zero fallback (21, 22); ownership
gap census + core-link count (17, 18); NULL fields named (26); close/open linkage (6);
compute-on-read in one transaction (37–39); `--sessions` (42); bound terms and fail-loud
direction stated (58, 60, 63–65, 67–69).
Rebutted / out of scope: 11 (convention stated); 14 (pool convention is the existing settled
pot definition); 19, 20, 23 (ownership timing: membership is broker lifecycle of owned ids,
as `strategy_wealth` realised); 35–36 (`observed_at` is the boundary; mark-vs-capture skew is
already evidenced by `pnl_timestamp`); 40–41 (gaps printed, not filled); 61 (second-order,
fails loud); 74 (table tests).
