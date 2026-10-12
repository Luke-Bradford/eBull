# The comparator in GBP, and what the ISA counterfactual means for the engine (#3616 slice 2)

Source: #3616 (committee review, 2026-10-04). *"The UK investable comparator is a UCITS S&P total-return product in an ISA (US ETFs are CFD-only on this account), in GBP, with 15% dividend withholding and CGT drag."*

Slice 1 (`docs/specs/metrics/2026-10-12-3616-investable-comparator.md`, built in #3780) compares the pot with CSPX.L's market-price return in USD and derives the active hurdle. This slice restates the comparison in the operator's currency and records what is now established about the ISA counterfactual. **It computes no after-tax figure**; the reason, and the slice that will, are below.

## Facts established first

Each was read or measured on 2026-10-12.

1. **The ISA is a separate account that the engine has no API to.** eToro's ISA is "powered by Moneyfarm"; client funds are held "in segregated accounts with MFM Investment Ltd" (FCA 629539), and "your ISA is held in GBP" (etoro.com/investing/isa/stocks-shares). The engine trades the eToro account through the public API (`app/providers/implementations/etoro_broker.py`), which has no ISA endpoint. So if live capital is funded through this engine, it is held outside an ISA, and an ISA holding of the comparator is a counterfactual. Whether that general account is taxable is the operator's own circumstance; the comparator assumes a UK-resident individual. The ISA's instrument list is not public without an account, so **whether Moneyfarm offers this share class is unverified**; the comparator is a hypothetical ISA holding.
2. **The demo account offers CSPX.L as the fund itself.** `POST /api/v2/trading/info/demo/eligibility` returns a `real`/long/x1 arm, with `isPotential` false, for 3434 (CSPX.L), 8020 (CSP1.L), 3075 (IUSA.L) and 3417 (SPY.RTH); SPY (3000) offers CFD arms only. The `real` endpoint returns 404 with the demo keys, so real-account eligibility is not observed. This is a five-instrument demo observation and settles nothing about the real account. The raw response is `docs/research/3616-slice2-eligibility-demo-2026-10-12.json`, from `EtoroBrokerProvider(env="demo").check_instrument_eligibility([3000, 3075, 3417, 3434, 8020])` with the demo credentials (`scripts/verify_2437_trading_preflight._load_demo_credentials`).
3. **Fund tax character differs between the comparator and the core.** CSPX (IE00B5BMR087) is on HMRC's list of reporting funds; SPY, which the core sleeve holds as SPY.RTH, is not found on it. A disposal of a non-reporting fund interest gives an offshore income gain, taxed as income (SI 2009/3001 regs 17–18). The evidence and the recommended core switch are #3781; this slice only reports it.
4. **Rates** (gov.uk, read 2026-10-12): CGT 18% within the basic band and 24% above it, in force since 30 October 2024 (`tax_ledger._CGT_RATE_PERIODS`); dividend tax 10.75% / 35.75% / 39.35% above a £500 allowance from 6 April 2026; ISA subscription limit £20,000 for 2026–27.

## Why this slice computes no after-tax return

The first draft applied a uniform CGT haircut, `R£ · (1 − t)`, to the pot's TWR and called it the SEC's post-liquidation after-tax return (Form N-1A, Release 33-7941). Codex checkpoint 1 round 1 rejected it, correctly, on five grounds:
- N-1A taxes each distribution when paid and each lot on its own basis; a haircut on a TWR performs none of those steps.
- A TWR is a flow-neutral performance chain, not the gains of an investor who bought at S.
- Tax depends on disposal dates and tax years, not only on total proceeds less cost.
- An offshore income gain on a non-reporting fund is income, and cannot be netted against capital losses elsewhere in the pot. SPY.RTH makes that live (fact 3).
- Gains on foreign currency in an individual's bank account are not chargeable (HMRC CG78321). How the pot's USD cash balance is classified is not established.

The right mechanism already exists: `tax_ledger` matches disposals by HMRC's same-day, 30-day and s104 rules per tax year. It ingests only the manual `fills` path and holds no engine trades. A later slice wires engine trades into it, with each instrument's tax character from HMRC's list, and only then publishes an after-tax figure. This slice records that decision rather than publishing an approximation that can be wrong in either direction.

## Scope

Added to the existing `comparator` object, over slice 1's window S to E:
1. the comparator's and the engine's returns in GBP;
2. the engine's excess in GBP;
3. a statement, in the UI, of what the GBP comparison does not include (UK tax on the engine's side, and fact 1's qualifications).

Out of scope: after-tax figures (above), realised sleeve attribution, per-strategy risk-adjusted metrics, the core-instrument switch (#3781), and FX fees on funding either account.

## Source rule

### GBP rate source

- **The series.** ECB euro foreign exchange reference rates, stored as `fx_rates_daily` rows with `base_currency` USD, `quote_currency` GBP, source `frankfurter.timeseries`. The `rate` is GBP per 1 USD.
  - It is the stored dated USD/GBP series. The tax `fx_rates` table belongs to the disposal path and stays separate (`tax-ledger` skill).
  - The ECB sets the rates at a daily concertation around 14:10 CET and publishes them around 16:00 CET, on TARGET business days only (ecb.europa.eu, euro reference exchange rates).
- **Who produces it for these dates.** `portfolio_eod.compute_and_store_eod_snapshot` calls `fx_history.ensure_fx_history(conn, until=snapshot_date)` before writing each EOD snapshot (`app/services/portfolio_eod.py:424-436`). That job also writes the `portfolio_eod_snapshots` rows that date the pot's points. So every S and E is a date up to which that job has already requested rates.
  - A binding test asserts that the call stays in `compute_and_store_eod_snapshot`, analogous to slice 1's `BENCHMARK_SYMBOLS` binding.
  - This guarantees a request up to each snapshot date, not a stored row, and not coverage before the table's floor. `ensure_fx_history` sets its floor from the earliest `trade_events` or `cash_ledger` activity and extends coverage backwards to that floor and forwards to `until`, without repairing internal gaps (`fx_history.py:1-80`). This slice supplies no engine-derived floor. A date with no rate is therefore reported, never estimated: the endpoint rules below return `fx_rate_missing` or `fx_rate_stale`.
  - Measured on the dev DB: the USD/GBP series starts 2025-08-12, the earliest `portfolio_eod_snapshots` date is 2026-06-12, and 0 of the 70 snapshot dates lack a rate within 4 days (query under Worked example). That is an observation, not a guarantee.

### The GBP return (a definition, fixed by construction)

- **Rule.** Each sub-period's GBP wealth relative is its USD wealth relative × `φ_end / φ_start`, where φ is the GBP-per-USD rate at the sub-period's two valuation dates.
- **Why it is exact for this chain.** The pot TWR places every external flow at the start of its sub-period, so `D = V_prev + F` (TWR spec, `docs/specs/metrics/2026-09-23-3334-pot-time-weighted-return.md`, "Rule"). The pot's valuations and flows are all USD (`strategy_wealth`: principal is `capital_limit`, USD). So converting `D` at `φ_start` and `V_t` at `φ_end` is the same as converting the USD relative.
- **It telescopes.** Over the linked chain, every inner rate appears once as an end and once as a start and cancels: `1 + R_e£ = (1 + R_e$) · φ_E / φ_S`. Only the two endpoint rates are read. The same holds for the comparator's single period.
- **Formulas.** `φ = φ_E / φ_S`, a dimensionless multiplier.
  - `R£ = (1 + R$) · φ − 1`.
  - The GBP excess is `R_e£ − R_c£ = φ · (R_e$ − R_c$)`. That is a positive rescaling, so it never changes the sign of slice 1's excess.
  - Slice 1's active hurdle needs no GBP version of its condition. Every sleeve, cash included, is USD, so the pot matches the comparator in GBP exactly when it matches in USD, and the **required active return in USD** is unchanged. Its GBP expression is `(1 + H$) · φ − 1` (6.1712% against 4.9318% USD in the worked example), because USD cash itself moves in GBP. Slice 1's line 2 and its cash assumption therefore gain a USD label.
- **A stated basis.** The rates are read at the **pot's** dates S and E, not at the comparator's resolved close dates. That gives one valuation convention for both returns: each USD value as of S or E, converted at the rate as of S or E. When a comparator close resolves to an earlier date (slice 1, up to 4 days), the comparator's GBP return pairs that close with S's or E's rate. The response returns all four dates so this is visible.

### Rate endpoints

- `φ_S` and `φ_E` are each the latest `fx_rates_daily` USD→GBP rate dated on or before S or E. The rate must be finite and > 0. Invalid rows are skipped, and no row dated after the date is read.
- A pair-specific SQL helper returns the rate together with its own `rate_date`. `fx_history.load_fx_rates_for_date` is not reused, because the date it returns is the newest across all pairs, not this pair's.
- **Coverage tolerance (chosen, not derived).** Each rate's `rate_date` must be within 4 calendar days of its date. The tolerance matches the longest TARGET closure, from Good Friday to Easter Monday; this is the same choice slice 1 made for closes.
  - Like slice 1's, it can also let a missing ordinary fixing through. That is the residual of choosing a tolerance.
  - The dev table's longest gap between consecutive USD/GBP rate dates is 5 days (Good Friday to Easter Monday leaves a date at most 4 days after its latest rate). That is an observation, not a guarantee.
- **No collapse rule for rates.** If S and E resolve to the same fresh row (a window of a few days inside a TARGET closure), then `φ = 1`. That is computable under the convention above, unlike a comparator-price collapse, which leaves no price movement to measure.

### The CSP1.L diagnostic (not a gate)

CSP1.L (8020) is the same fund's GBX line on the LSE. From 2026-09-17 to 2026-10-09 it returned 3.1816%, against 3.4842% for CSPX.L converted at ECB rates. The 30 bp gap is consistent with timing and basis differences: the ECB concertation is earlier in the day than the LSE close, the two lines have separate spreads, and eToro's candle cutoff is undocumented. One interval does not isolate the cause.

The ECB rate is used because one rate must convert the pot and the comparator alike, and the pot has no GBX line.

## Contract change

`comparator` (slice 1) gains one nested object, `gbp`. It is always present, never null:

```
gbp: {
  fx_source: "ecb_reference_fx_rates_daily",
  fx_start_date: date | null,          # rate_date resolved for S
  fx_end_date: date | null,            # rate_date resolved for E
  fx_multiplier: Decimal | null,       # φ_E / φ_S, dimensionless
  comparator_return: Decimal | null,   # R_c£
  engine_return: Decimal | null,       # R_e£
  engine_excess: Decimal | null,       # R_e£ − R_c£
  unavailable_reason: "comparator_unavailable" | "fx_rate_missing" | "fx_rate_stale" | null,
}
```

**Reason precedence (authoritative, first that applies):**
1. `comparator_unavailable`: slice 1's `return` is NULL, so S, E or `R_c$` is missing.
2. `fx_rate_missing`: no valid rate on or before S, or on or before E. S is checked before E.
3. `fx_rate_stale`: a resolved rate is more than 4 days before its date, at either end.

**Field matrix.**

| state | `fx_start_date` / `fx_end_date` | `fx_multiplier` | the three returns |
|---|---|---|---|
| `comparator_unavailable` | NULL | NULL | NULL |
| `fx_rate_missing` | each populated if it resolved | NULL | NULL |
| `fx_rate_stale` | both populated | NULL | NULL |
| available | both | value | all three |

- `engine_return` and `engine_excess` follow slice 1's `engine_excess`. With an available comparator, the pot's cumulative return is never NULL, because `pot_return_unavailable` makes the comparator unavailable in slice 1.
- All values are unrounded cumulative fractions; rounding is UI-only.

## UI

Line 1 of slice 1's `StrategyPortfolioLens` strip gains a GBP clause. There is no new line and no new panel.
- **Line 1:** "vs UCITS comparator iShares Core S&P 500 (USD, Acc), 17 Sept – 9 Oct: +2.28% · engine -1.18 pts · in GBP +3.48% · engine -1.20 pts".
- **Tooltip, added sentences:**
  - the ECB source and both rate dates;
  - "In GBP the gap is the USD gap scaled by the dollar's move; the active return needed, in USD, is unchanged";
  - "Not after UK tax. The engine's account is outside an ISA; the same fund held in an ISA would pay no UK tax on gains. An after-tax comparison needs the engine's trades matched under HMRC rules, which is not built yet."
- **Line 2 (slice 1)** gains a currency label: "Active sleeves would need +4.93% in USD (+2.66 percentage points over the comparator) to match it", and its assumption text says "cash earning 0 in USD".
- **Slice 1's investability sentence** becomes: "The demo account offers this fund as the fund itself, not a CFD (real account not yet observed). eToro's ISA is a separate Moneyfarm GBP account the engine cannot trade, and whether it offers this share class is unverified."
- **Degradation.** An unavailable `gbp` drops only the GBP clause and adds its reason to the tooltip. Slice 1's figures stay. A missing figure shows "—", never 0.

## Tests

- **Pure (GBP):**
  - a three-sub-period chain with four boundary rates (0.8, 0.75, 0.6, 0.96) and a flow at the start of the second sub-period. The per-sub-period GBP product equals `(1 + R$) · φ_E/φ_S`. Wealth and flow inputs are chosen so every intermediate quotient terminates in `Decimal` (V 100 → 110; flow +40, so D = 150 → 120; then 120 → 150: USD relatives 1.1, 0.8, 1.25; GBP relatives 1.03125, 0.64, 2; both products give 1.32 = 1.1 × 1.2), and the test asserts equality, not a tolerance;
  - an unfunded base: the first funding is converted at the rate of `period_start` = S, not of `return_since`; and a funded base;
  - negative and zero USD returns;
  - the excess equals `φ · (R_e$ − R_c$)`, and its sign equals slice 1's.
- **Pure (endpoints):**
  - no rate at S, and none at E;
  - invalid rows (0, negative, NaN, ±Infinity) skipped to the prior valid row;
  - the 4-day boundary at 4 and 5 days;
  - S and E sharing one fresh row, giving `φ = 1`;
  - a comparator close resolved earlier than E while the rate resolves at E (the stated basis).
- **Pure (precedence):**
  - `comparator_unavailable` while rates are also missing;
  - a missing rate at S with a stale one at E, giving `fx_rate_missing`;
  - every row of the field matrix.
- **Binding:**
  - `portfolio_eod.compute_and_store_eod_snapshot` calls `ensure_fx_history` before writing the snapshot;
  - `COMPARATOR_SYMBOL` is unchanged.
- **DB:** one test of the rate helper on a sparse fixture. It returns the latest valid rate on or before a date with its `rate_date`, and reads no later rows.
- **API:**
  - `gbp` is always present;
  - its values are fractions;
  - an unavailable slice 1 comparator gives `comparator_unavailable`, with NULL dates.
- **FE unit:**
  - the GBP clause formats the comparator return in percent (`0.03484237… → +3.48%`) and the excess in points (`−0.01196474… → −1.20 pts`);
  - line 2 carries "in USD";
  - an unavailable `gbp` drops the clause but keeps slice 1's figures;
  - the tax and investability sentences appear in the tooltip.
- **Dev stack:**
  - the endpoint reproduces the worked example;
  - the strip is eyeballed in light and dark.

## Worked example (dev DB, 2026-10-12; S = 2026-09-17, E = 2026-10-09)

The values below are computed from exact inputs: `R_c$ = 841.59 / 822.86 − 1`, and `R_e$` = 0.010937, slice 1's rounded figure.

| quantity | value |
|---|---|
| `R_c$` | 0.02276207… |
| `φ_S`, `φ_E` (rate dates 09-17, 10-09) | 0.74758, 0.75641 |
| `φ` | 1.01181144… |
| `R_c£` | 0.03484237… |
| `R_e£` | 0.02287763… |
| GBP excess | −0.01196474… |
| CSP1.L GBX diagnostic | 0.03181582… |

Reproduce:
```sql
SELECT rate_date, rate FROM fx_rates_daily
WHERE base_currency = 'USD' AND quote_currency = 'GBP' AND rate_date IN ('2026-09-17', '2026-10-09');
-- longest gap between consecutive USD/GBP rate dates (observation, not a guarantee)
SELECT max(gap) FROM (SELECT rate_date - lag(rate_date) OVER (ORDER BY rate_date) gap
  FROM fx_rates_daily WHERE base_currency = 'USD' AND quote_currency = 'GBP') x;
SELECT price_date, close FROM price_daily WHERE instrument_id = 8020 AND price_date IN ('2026-09-17', '2026-10-09');
-- snapshot dates with no VALID USD/GBP rate within 4 days (0 of 70 on 2026-10-12);
-- numeric NaN sorts above Infinity in Postgres, so `< 'Infinity'` excludes both
SELECT count(*) FROM portfolio_eod_snapshots s WHERE NOT EXISTS (SELECT 1 FROM fx_rates_daily f
  WHERE f.base_currency = 'USD' AND f.quote_currency = 'GBP' AND f.rate > 0 AND f.rate < 'Infinity'
    AND f.rate_date BETWEEN s.snapshot_date - 4 AND s.snapshot_date);
```

## Review

Codex checkpoint 1: round 1 raised 39 findings (5 BLOCKING, all against the after-tax haircut), and the tax computation was removed (above). Round 2 left 3 WARNING and 3 NITPICK; round 3 left 2 NITPICK. All were applied. Prompts and outputs are in `var/research/3616/ckpt1_r{1,2,3}*.txt` in the autonomy worktree.

## Security

A read-only addition to an authenticated read endpoint. No authorisation, order or capital path changes. The eligibility observation behind fact 2 came from the read-only preflight endpoint; it places nothing.
