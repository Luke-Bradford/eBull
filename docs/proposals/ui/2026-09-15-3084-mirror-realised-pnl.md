# #3084 — surface a mirror's closed P&L in the two portfolio list views

## Problem

`/portfolio` and the dashboard positions table both render `INVESTED · VALUE · P&L` for
direct positions and for mirror (COPY) rows in the same table. On a direct position
`VALUE − INVESTED = P&L` exactly. On a mirror row it cannot, and the row carries nothing
to reconcile it with.

Full population of the dev book — every row the table renders: **5 direct instrument rows
(7 underlying trades, grouped) and 2 active mirrors**. One single `GET /portfolio` read,
2026-09-15, GBP display, `fx_incomplete: false`:

| row | INVESTED | VALUE | P&L | VALUE − INVESTED − P&L |
| --- | --- | --- | --- | --- |
| GME | 25,157.11 | 23,640.41 | −1,516.69 | 0.00 |
| QQQ | 7,416.49 | 9,022.32 | 1,605.83 | 0.00 |
| VOO | 7,416.59 | 8,743.87 | 1,327.27 | 0.00 |
| NXH | 5,894.56 | 3,368.32 | −2,526.24 | 0.00 |
| IEP | 1,973.88 | 1,635.60 | −338.29 | 0.00 |
| `thomaspj` | 14,833.20 | 12,964.47 | −353.99 | **−1,514.74** |
| `triangulacapital` | 14,485.36 | 14,876.56 | −224.18 | **+615.38** |

`triangulacapital` is the sharp one: VALUE is **above** INVESTED and the P&L still reads
negative.

⚠ **One read, deliberately.** An earlier draft took the direct rows from a screenshot and
the mirror rows from a later API call; the mirror P&Ls had moved (−226.87 → −224.18) and
the column no longer summed to the header. Mixing two reads of a live book is the defect
this spec is about, committed in its own evidence.

The residual is `copy_mirrors.closed_positions_net_profit` at the display rate, checked
against the stored column rather than inferred from the residual: `+829.73 USD → +615.38
GBP`, `−2,042.37 USD → −1,514.74 GBP`. It is stable across reads because it is a static
broker aggregate, not a marked quantity. Sign: residual `= +closed_pnl`.

Both aggregates reconcile on the same read, and nothing in this change recomputes them:
`Σ VALUE + cash = 75,514.93 = total_aum`.

## Source rule

`docs/etoro-api-reference.md` is the governing document and it settles both halves.

- **The field is realised.** `:457` — `closedPositionsNetProfit` *"Realised P&L from
  closed positions (USD)"*.
- **eToro's account-level decomposition** — `Total Invested` at `:511-517`,
  `Unrealised P&L` at `:520-526`, `Equity` at `:528-532`:

  ```
  Total Invested  = … + SUM(mirrors.availableAmount - mirrors.closedPositionsNetProfit) + …
  Unrealised P&L  = SUM(positions.unrealizedPnL.pnL)
                  + SUM(mirrors.positions.unrealizedPnL.pnL)
                  + SUM(mirrors.closedPositionsNetProfit)
  Equity          = Available Cash + Total Invested + Unrealised P&L
  ```

  eToro subtracts the realised profit from *invested* and books it under a heading it
  labels *Unrealised P&L* — because a closed mirror position returns its proceeds to
  `availableAmount`, so leaving it in invested would double-count it. The **account-level
  label is loose; the field-level definition at `:457` is not.**

- **Our arithmetic is the same algebra, decomposed one term further.** Per mirror, eToro's
  contribution is `(availableAmount − closedPositionsNetProfit + Σ amount)` to invested plus
  `(closedPositionsNetProfit + Σ unrealised)` to P&L, summing to
  `availableAmount + Σ market value` — the shape `load_mirror_breakdowns` computes as
  `mirror_equity`. `unrealized_pnl = mirror_equity − funded − realised` (#226,
  `app/services/portfolio.py:280`) separates the term eToro's account-level label folds in.

  ⚠ **Algebra, not monetary equality.** eToro supplies `unrealizedPnL.pnL`; we substitute
  our own mark (`portfolio.py:230-236`: price delta × units × `open_conversion_rate`, i.e.
  entry FX, no explicit fee term). The identities match in form; the figures are our marks
  and may differ from eToro's in cents. This change neither introduces nor narrows that.

  **Nothing is reversed and no rendered number changes.** The only defect is that the
  separated term is then discarded.

- **The in-tree name is already settled: "Closed P&L".** `/copy-trading/:mirrorId` renders
  exactly this quantity under that label and explains it in prose
  (`frontend/src/pages/CopyTradingPage.tsx:71,79,100`). Reuse it; do not mint "Realised".

## Why the direct rows reconcile and the mirror rows cannot

Not a difference of basis. `valuation.py:199,205` computes a direct position's P&L as
`market_value − cost_basis` — also unrealised-only. An **open position has no realised
term**; a mirror is a *container* that has banked closed trades, so it has one. The two row
kinds are consistent in what they measure and differ in whether the quantity exists.

⚠ An earlier draft called the direct rows "total-basis" and the header "mixed bases". Wrong,
and struck.

## Change

Additive. No existing number changes.

1. `MirrorBreakdown` gains `closed_pnl_usd` — already computed in `load_mirror_breakdowns`
   as `realised` and discarded after line 280.
2. `PortfolioMirrorItem` gains `closed_pnl`, converted by the **same**
   `_convert_value(..., "USD", display_currency, rates)` call as its three siblings, in the
   same loop. `_convert_value` returns USD unchanged when a rate is missing, and the
   response's single `cash_currency` field already labels `funded` / `mirror_equity` /
   `unrealized_pnl` for that reason (`app/api/portfolio.py:155-157`) — the new field joins
   that contract rather than declaring its own.
3. `MirrorRow` in **both** list views renders it beneath the P&L figure as
   `"<money> closed P&L"`, and labels the P&L cell unrealised:
   - `frontend/src/pages/PortfolioPage.tsx`
   - `frontend/src/components/dashboard/PositionsTable.tsx`

   ⚠ Both, deliberately. Fixing one renderer and leaving its twin is the "this rule now
   does X" shape that does not travel (prevention log, 2026-09-15).
4. Sign colouring for the closed figure derives from the closed figure, **not** from
   `positive` (which derives from `unrealized_pnl`). The dev book holds one mirror of each
   sign, so an opposite-sign pair is the live case.
5. `frontend/src/api/types.ts::PortfolioMirrorItem` mirrors the Pydantic field in the same
   commit. Typed factories in `PortfolioPage.test.tsx` and `portfolioRows.test.ts` construct
   this interface and fail typecheck until updated — they are part of this diff.

Two comments predating #226 are replaced with the expression the code evaluates:

- `app/services/portfolio.py:204` — `# mirror_equity - funded`
  → `# mirror_equity - funded - closed_pnl (#226: isolates unrealised)`
- `app/api/portfolio.py:142` — `# mirror_equity - funded (display currency)`
  → `# mirror_equity - funded - closed_pnl (display currency; #226)`

## Explicitly NOT in scope — with the concrete behaviour named

- **The P&L number and the AUM/P&L header.** Nothing is recomputed, so
  `Σ VALUE + cash = total_aum` continues to hold on the same read. ⚠ `/portfolio`'s header
  is labelled plain "P&L" while the dashboard's says "Unrealized P&L"; after this change the
  rows show both components and the portfolio header still excludes the closed one. Label
  only, recorded on #3084.
- **The `%` column.** `unrealized_pnl / funded`: unrealised numerator over total-funded
  denominator. Concretely, withdrawals can drive `funded` to zero (percentage renders `—`) or
  negative (percentage sign reverses while the colour still follows the money). Changing it
  changes a rendered number; recorded on #3084.
- **`valuation.py:300`'s USD-exposure detection** keys on *aggregate* mirror equity. A
  wiped-out mirror (near-zero equity) with a non-zero closed P&L, or two mirrors whose
  equities cancel, can leave `cash_currency = GBP` while the underlying amount stayed USD —
  and `_build_fx_rates_used` (`app/api/portfolio.py:288`) can omit a rate that was used.
  Pre-existing for all three sibling fields; the new field inherits it and does not widen it.
  Recorded on #3084.
- **The mirror MTM hierarchy diverges from the direct one.** `portfolio.py:230` marks mirror
  positions on `GREATEST(q.last,0) → pd.close → open_rate` with no bid/ask fallback, while
  direct rows use `resolve_quote_price(last, bid, ask)` (`valuation.py:190`) and reject
  non-positive closes. So a list mirror figure can disagree with its own drill-down.
  Pre-existing; recorded on #3084.
- **Freshness.** These are static aggregates from the portfolio fetch; the quote subscription
  does not update them, which is already true of `funded` and `mirror_equity`.

## Tests

Pure-logic, no DB.

1. **Non-circular.** Assert `closed_pnl_usd` against the SOURCE column
   (`closed_positions_net_profit`), never against `mirror_equity − funded − unrealized_pnl`
   — the latter is the definition rearranged and passes on any wrong input. Table-test both
   signs, the negative case being a realised **loss**, so a sign flip cannot pass.
   ⚠ This validates the wiring, not the accounting premise: any fee or adjustment outside
   the five source columns still lands silently in `unrealized_pnl`. Pre-existing to #226
   and named on #3084 rather than fixed here.
2. **Real path, not a hand-built model.** Drive `get_portfolio` and assert the serialised
   figure against an independently computed expected value at a non-1.0 FX rate, so a
   uniformly wrong conversion fails. Absolute tolerance `1e-6`, never float equality.
3. **Regression — no existing number moves.** On the same fixture with a non-zero closed
   P&L, pin `funded`, `mirror_equity`, `unrealized_pnl`, the percentage, and `total_aum`
   unchanged. Without this, accidental double-counting passes every new-field test.
4. **Two mirrors at once**, distinct closed amounts, asserted by `mirror_id` — an
   isolated single-mirror case cannot detect cross-row reuse.
5. **Missing-rate degrade:** with no USD→GBP rate, `closed_pnl` returns USD like its
   siblings and the row's currency label agrees.
6. **Zero case:** `closed_positions_net_profit = 0` still renders the closed line — no
   truthiness check may hide it. **Inactive mirrors** stay excluded (`WHERE m.active`).
7. **FE, both renderers:** each renders the closed figure and the unrealised label. Cover
   negative-unrealised/positive-closed, positive-unrealised/negative-closed, both-negative,
   and zero — one opposite-sign case alone lets an always-green closed line pass.

⚠ Reconciliation is asserted on **raw** values with tolerance. Displayed money is rounded
independently per cell, so the rendered pennies may carry a ±0.01 residual; the spec does
not promise exact closure of the printed digits.

## Acceptance

`GET /portfolio` on the dev stack returns `closed_pnl` on both mirrors, reconciling to the
stored USD column at the response's own `fx_rates_used` rate (tolerance `0.01`), and both
list views render a mirror row whose four numbers close. Recorded with the contemporaneous
rate rather than the GBP figures above, which drift with FX.
