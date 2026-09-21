# The core sleeve's household currency caveat — what each clause rests on

Reference for `CoreSleeveResponse.household_currency_caveat` in `app/api/strategies.py`.
Extracted from the field's inline comment on review, which had grown to dozens of lines for
one string constant (PRs #3272, #3273).

Shipped text:

> Sterling is not the unit this sleeve is run in. Where the core sleeve holds a USD-quoted
> instrument, a household measuring it in GBP carries GBP/USD exposure on the whole position
> value and not only on its return, and this engine does not hedge it. Where the positions
> table shows £ against that holding, the figure is this engine's own conversion, at a stored
> rate of GBP per USD that this read does not transact at and that can be older than the last
> FX refresh. Converting when you fund or withdraw is a household cost that the preregistered
> ceiling, which prices round-trip spread, does not include.

## Why it is not #2833's verbatim wording

#2833's "On pass" clause asks for the caveat *"unhedged USD exposure on a GBP account"*
verbatim. The words **"on a GBP account"** are dropped, and only those.

```sql
SELECT environment, currency, account_currency_id, count(*)
FROM broker_account_equity_snapshots GROUP BY 1, 2, 3;
```

Every row is `demo`. Rows with a non-NULL `account_currency_id` are broker-observed USD;
rows with NULL are not observations at all, but the pre-migration bound `'USD'` literal that
`sql/341_account_equity_observed_currency.sql` exists to stop us reading as evidence
(`account_currency_assumed_not_observed`).

⚠ That makes the clause **unsupportable, not false**. "On a GBP account" plausibly meant
valuation exposure rather than a per-trade conversion, so nothing here refutes it and both of
its claims are kept. The precedent for deviating at all is the sibling field
`household_tax_caveat`, which #2915's measurement turned from "strictly tax-dominates" into
"mixed".

## Clause by clause

| clause | rests on |
| --- | --- |
| "Where the core sleeve **holds** a USD-quoted instrument" | conditional on purpose: this response is also served in `cash`, `evidence_collecting` and `unavailable`, where no instrument is selected. An unconditional sentence is false in those states. |
| "on the whole position value and not only on its return" | translating a USD holding into GBP moves the whole of it, not just the P&L. Pinned by its own test assertion so the qualifier cannot be dropped. |
| "this engine does not hedge it" | `strategy_core_mandate_events.base_currency` is `CHECK (base_currency = 'USD')` (`sql/336:26`); no hedging instrument or FX overlay exists on the core path. |
| "the figure is **this engine's own conversion**" | `app/api/portfolio.py`, the `if native_ccy != display_currency:` block. `native_ccy` is `instruments.currency` (`USD` for 3417); `display_currency` is `get_runtime_config(conn).display_currency` (`GBP` on dev). `amount`, `open_rate`, `current_price`, `market_value`, `unrealized_pnl`, SL and TP are each passed through `convert`. |
| "at a **stored rate of GBP per USD**" | `load_live_fx_rates_with_metadata` (`app/services/fx.py:59`) selects `rate, quoted_at` from `live_fx_rates`, written by `fx_rates_refresh` (`app/workers/scheduler.py`). Measured 2026-09-21: rate `0.74939`, `quoted_at` `2026-09-18`. ⚠ The direction is stated the long way round deliberately: "a GBP/USD rate" conventionally reads as USD per GBP (≈1.33), and the stored multiplier is its reciprocal. |
| "that **this read does not transact at**" | scoped to this read, and no wider. The rate is applied on read for display only, and the order path is USD end to end — `orderCurrency: "usd"` (`app/providers/implementations/etoro_broker.py:556`, `:612`), `BrokerStrategyOrder.order_currency` refuses non-USD (`app/providers/broker.py:278`), `strategy_core_mandate_events` is `CHECK (base_currency = 'USD')`. ⚠ An earlier draft said "**nothing** was transacted at" — a universal historical negative that nothing here establishes, since a household funding, withdrawal or broker conversion may well have used the same published rate. Cut at Codex ckpt-1. |
| "that **can be older than the last FX refresh**" | `sse_quotes._load_display_context` (`app/api/sse_quotes.py:131`, called once at `:283`) snapshots `load_live_fx_rates` when the stream OPENS, and `StrategyPositions.tsx` overrides `current_price` with that tick. ⚠ The draft said "only as fresh as the last FX refresh", which is wrong in the unsafe direction: a rendered cell can be older than that. |
| "the preregistered ceiling, which prices round-trip spread, does not include" it | the declaration's `all_in_cost_rule` in `docs/proposals/ta/2026-08-24-core-selection-declaration.json`: `p75_full_round_trip_spread_bps` plus a documented 0 bps entry-sizing markup. Household funding/withdrawal conversion is outside it — the half of #2833's spike step 3 the measured verdict never priced. |

## Three claims that were shipped or drafted and are now retracted

Both are pinned negatively in `tests/test_2603_core_mandate_api.py` so they cannot return.

1. **"this engine reports no sterling figure"** — merged in `7b0cd043` and **false**: the
   owned-positions table prints `£` for this sleeve. Verified by grepping
   `StrategyPortfolioLens.tsx` alone; the label is formatted one component down, in
   `StrategyPositions.tsx`, from `position.currency`.
2. **"no amount on this page has been converted by us"** — the first correction, false for the
   same reason one level out: `local_eod_value_in_account_currency` is a conversion we perform
   and the account-evidence panel renders it on this route.

3. **"a £ sign in the positions table is the broker's own label carried through unchanged,
   not a conversion this engine performed"** — the correction for retraction 2, shipped in
   `e5e0dca1`, and **false in both halves** (measured 2026-09-21, #3274).
   - *Not the broker's label.* `_parse_direct_position`
     (`app/providers/implementations/etoro_broker.py:1840`) maps no currency key off the
     eToro position payload. The `currency` on our response is
     `BrokerPositionItem.currency` = `trade_currency` = `display_currency` from
     `get_runtime_config`. ⚠ That is a claim about OUR PARSER. It does not establish
     that eToro sends no currency — `raw_payload=payload` would retain one — and the
     first draft of this retraction said "the broker never sends a currency on a
     position at all", which is the same unbounded negative the entry warns about.
   - *It is a conversion we performed.* On the live dev position, `broker_positions` stores
     `amount 225.04` / `open_rate 759.86` and `quotes.last` is `761.59` — all USD. The route
     returns `168.64` / `569.43` / `570.73`, which are those three times the stored
     `0.74939`. #3274 read `569.43` as SPY's USD price and inferred a mislabel; it is the
     sterling one, and the label was right all along.

A fourth draft cited the selection declaration's `fx_rule` as proof of "neither hedges nor
models". That rule governs instrument-quote-currency → USD conversion, a **different currency
pair** from the household's, and never supported the sentence.

The recurrence is recorded in `docs/review-prevention-log.md` under *"A negative claim is only
as wide as the thing you grepped"*.

## The conversion boundary on this route

⚠⚠ **This section previously claimed `account_equity_evidence._convert_local_total` was the
ONLY converting call reachable from `/strategies`, and that "every other money field on the
route is carried through in the currency its source reported". Both are false (#3274), and
the section is the reason retraction 3 shipped.** The rewrite below is the measured version.

⚠⚠ **This section must not state a COUNT.** The first rewrite of it said "**two** converting
paths", which is the retracted claim with a different integer — and Codex ckpt-1 immediately
produced a third (SSE, below) plus two more multiplication sites. An exhaustive count needs a
defined boundary nobody has drawn. What follows is **the paths established so far**.

1. `account_equity_evidence._convert_local_total` — feeds `local_eod_value_in_account_currency`,
   `local_eod_value_at_official_marks` (that value plus the mark correction), and the
   `difference` / `tolerance` derived from them.
2. **`app/api/portfolio.py::get_portfolio`, imported at `app/api/strategies.py:19` and called
   by the owned-positions route.** Its `if native_ccy != display_currency:` block converts
   `amount`, `open_rate`, `current_price`, `market_value`, `unrealized_pnl`, `stop_loss_rate`
   and `take_profit_rate` from `instruments.currency` to `display_currency`, and sets the row's
   `currency` to whichever one survived. On an `FxRateNotFound` the row degrades to native and
   the label follows it. `total_fees` is passed through and is deliberately outside the
   `currency` contract.
3. **`app/api/sse_quotes.py`, which is not reached through `app/api/strategies.py` at all.**
   `StrategyPositions.tsx` subscribes via `LiveQuoteProvider` / `useLiveTick` and overrides
   `current_price` with `liveTickPriceIn(tick, position.currency)`. `_format_tick` converts
   the tick against `_DisplayContext`, built by `_load_display_context` (`:131`) and
   snapshotted **once per connection** at `:283`. So the CURRENT and GAIN/LOSS cells are not
   the REST response's converted values, and their FX rate can be older than the entry-price
   cell's beside it.

Two further multiplication sites were raised and are **not** verified here, recorded so the
next reader starts from them rather than from zero: `strategy_monitoring.load_owned_pnl`
multiplying native P&L by the broker's `open_conversion_rate`, and the mark substitution in
`account_equity_evidence` using `close_conversion_rate`. Neither is a `convert()` call, which
is why a vocabulary-scoped grep misses both.

⚠⚠ **The command this section used to give could not have found path 2, because it named only
the two files that already agreed with the claim:**

```bash
# WRONG -- scoped to the files the claim was written from.
rg -n 'in_account_currency|_convert_' app/api/strategies.py app/services/account_equity_evidence.py
```

`/strategies` is composed from other routers' service functions, so a claim about the *route*
cannot be checked by grepping the route's *own file*. Follow the imports instead:

```bash
# Every non-stdlib symbol the route pulls in, then the converting calls in each of those files.
rg -n '^from app\.' app/api/strategies.py
rg -n 'def convert|convert\(|_convert' app/api/strategies.py app/api/portfolio.py \
      app/services/valuation.py app/services/account_equity_evidence.py app/services/fx.py
# ⚠ Path 3 is invisible to BOTH of the above, because the component fetches it itself.
rg -n 'useLiveTick|LiveQuoteProvider|liveTickPriceIn' frontend/src/components/strategies/
```

⚠ Even together these are three greps, not a proof. A rendered figure can come from a service
the route never imports (path 3), or from a bare multiplication the vocabulary misses (the two
unverified sites above). The honest end-to-end check is neither grep: render the route and
reconcile one figure against its stored source, as the measurement in retraction 3 did.

⚠ Do **not** widen the caveat on the strength of this page. Re-run the commands — taking a
document's word for a scope claim is precisely the failure the retractions above record, and
this section is now itself an instance of it.

## Related

- #3274 — **REFUSED 2026-09-21, premise falsified.** The `GBP` label is correct: the figures
  are sterling, and `569.43` is `759.86` USD at the stored `0.74939`, not SPY's USD price. The
  ticket's stated unblock (a live-portal check of what eToro's position `currency` means) was
  never needed — we do not read that field. Its real yield is retraction 3 above.
- #2833's other "On pass" deliverable, the net benchmark line, is still unshipped; the page
  renders `BENCHMARK_REFUSALS` (#2602 item 5).
