# F-0 acceptance: the reconciled window, 2026-09-15 → 2026-09-22

This is #2602's acceptance page. It records three things for the first window in which the
official/local reconciliation decided: the account's return over that window, the part of the
return that reconciled, and the size of each part that did not.

The rule is `f0-reconcile-v2` (`RECONCILIATION_RULE_VERSION` in
`app/services/account_equity_evidence.py`). The design is in
`2026-08-22-f0-reconciliation-verdict.md`, and the v2 comparand is in
`docs/proposals/ops/2026-09-15-3068-reconciliation-mark-instant.md`.

⚠ **This covers one demo account over six sessions.** It shows that the reconciliation works on
real data. It makes **no performance claim**, because seven of the eight direct positions are the
operator's own holdings. It **enables nothing**: #2844 clause 3's countdown is a separate gate,
counted by `load_reconciliation_streak()`.

## Window

The window runs **from the 2026-09-15 broker observation to the 2026-09-22 broker observation**,
which is 20:25:05 UTC on both days (`broker_account_equity_snapshots.observed_at`). Those are the
first and last NYSE sessions to decide under `f0-reconcile-v2`, so the return below spans five
session-to-session intervals.

The window boundary is set by the rule version, not by outcomes:

- **2026-09-14 is out.** Its v2 verdict is `refused (official_position_marks_not_recorded)`,
  meaning a missing input rather than a failure: the per-position broker marks that v2 reads
  were first stored by #3068. Under v1, that same session was `diverged`, which is why v2
  changed the comparand (the `RECONCILIATION_RULE_VERSION` docstring records the v1 figures).
- **Every earlier broker day (from 2026-08-11) is out.** Each is refused under v2 on a missing
  input: either a forward-only column that predates its migration, or a missing same-day local
  snapshot.
- **The two v1 greens (2026-08-24 and 2026-08-25) are out.** They were issued under the
  superseded comparand.

| session | verdict | official comparand | local EOD (account ccy) | mark bridge | local at official marks | difference | tolerance |
|---|---|---:|---:|---:|---:|---:|---:|
| 2026-09-15 | reconciled | 64,196.85 | 64,195.63 | +1.21 | 64,196.84 | +0.0136 | 31.56 |
| 2026-09-16 | reconciled | 64,837.19 | 64,837.72 | −0.52 | 64,837.20 | −0.0083 | 31.56 |
| 2026-09-17 | reconciled | 66,510.91 | 66,589.21 | −78.29 | 66,510.92 | −0.0117 | 31.56 |
| 2026-09-18 | reconciled | 66,661.55 | 66,661.55 | 0.00 | 66,661.55 | −0.0006 | 31.56 |
| 2026-09-21 | reconciled | 68,486.21 | 67,524.65 | +961.55 | 68,486.21 | +0.0023 | 31.56 |
| 2026-09-22 | reconciled † | 69,590.86 | 69,592.60 | −1.73 | 69,590.87 | −0.0099 | 31.56 |

All figures are in USD. The **mark bridge** is our own EOD valuation re-priced at the broker's
per-position `closeRate`. It is the v2 correction, and it is shown because v2 takes it out of
`difference`. Its two large values are the instants at which the two sides mark: the broker reads
at its observation time, while `price_daily.close` is the session close (#3068). ⚠ Because v2
marks both sides at the broker's price, agreement **does not validate the broker's prices**. It
validates only that the two sides hold the same units and the same cash.

† The loader returns this verdict, but the ledger (`account_reconciliation_days`) has not stored
it yet. The last `account_reconciliation_check` fired at 2026-09-22 22:58 UTC, while 2026-09-22
was still the current UTC day, and `pending_reconciliation_dates` excludes that day because its
broker row is still mutable. The row is expected at the next fire. When this page was written,
the ledger held five `reconciled` v2 rows, 2026-09-15 through 2026-09-21.

No session carried an incomplete reason. On every session:

- the position counts agreed: 7, and 8 from 2026-09-18;
- `official_pending_order_amount` was 0;
- there were 0 direct shorts.

## Account total return, decomposed

Official `equity` went from **101,707.13 to 108,139.98 USD**, a change of
**+6,432.85 (+6.325%)**. It splits into two legs by construction, since
`residual = equity − comparand`. So the split is an identity, not a second reconciliation.

| component | start | end | Δ USD | Δ % | status |
|---|---:|---:|---:|---:|---|
| direct book: `available_cash + Σ direct_long_market_value` | 64,196.85 | 69,590.86 | +5,394.01 | +8.402% | **reconciled at both endpoints and at every session between** |
| `residual_not_in_local_book` = `equity − comparand` | 37,510.28 | 38,549.12 | +1,038.84 | +2.769% | **unreconciled; named and sized below** |

The direct-book change inherits the difference at each endpoint. The local side's change
therefore differs from the official side's by `−0.0099 − 0.0136 = −0.0235 USD`, which is inside
the tolerance at both ends. No return-level tolerance is declared separately. The level
tolerance at each endpoint is the only bound, and this page does not invent another.

**No net external cash flow on the direct cash leg (measured).** `available_cash` went from
1,703.46 to 1,478.40, a change of −225.06. The seven trade events executed between the two
observation instants sum to that figure exactly:

- the SPY.RTH core buy on 2026-09-18: −225.04;
- two $50 attended round trips, on 2026-09-16 and 2026-09-22: −0.01 each (`realized_pnl_usd`);
- one $50 attended round trip on 2026-09-17: 0.00.

All seven events carry `investment_usd`, and all three closes carry `realized_pnl_usd`, so no
NULL drops out of the sum.

⚠ What that identity does **not** establish: a **net** match cannot exclude gross flows that
cancel, such as a deposit exactly offset by a withdrawal or by new copy funding. It also says
nothing about flows inside the residual leg. The direct-book +8.402% is therefore free of net
external cash only. The account-level +6.325% additionally rests on the unreconciled residual.

## Unreconciled components, each named with its size

1. **`residual_not_in_local_book`: 38,549.12 USD at window end (35.6% of equity), +1,038.84 over
   the window.** eToro's formula folds direct positions, mirrors and pending orders into
   `total_invested` (`2026-08-22-f0-reconciliation-verdict.md`). Pending orders and direct shorts
   were 0 at every observation, so the residual is what that formula leaves after the direct
   book: mirror valuation, plus any official-side parse or valuation error. The split between
   those two parts is **not measured**. The residual is a residual, not an attribution, and
   nothing on this page or on the panel calls it "the non-engine holdings". Its gross flows and
   its fees are unobserved.
2. **Direct-position fees and distributions: 0.0000 USD, carried as one combined component.** As
   checked on 2026-09-13 against `openapi_v1.375.0.json` (the #2602 comment of that date), every
   eToro field that carries dividends combines them with overnight fees, so the acceptance names
   them together. Measured at window end, all 8 `broker_positions` rows carry
   `total_fees = 0.0000`. The three closes in the window carry `fees_usd = 0`, and all seven
   raw payloads carry `fees` or `totalFees = 0.0`.
   ⚠ This is a point-in-time field on currently open positions, not a flow over the window. A
   combined zero could in principle be a cancellation. Mirror fees and distributions sit inside
   component 1, unobserved.
3. **Benchmarks.** S&P 500 total return and CPIH real return are **not reconciled components**.
   They render on `/strategies` as named refusal states (#2602 item 5, `43f000b6`).

## Reproduce

Run against the dev DB at a commit carrying `f0-reconcile-v2`. The broker and local tables have no
account key beyond `environment = 'demo'`; the dev DB holds demo credentials only.

```bash
PYTHONPATH=. uv run python - <<'EOF'
import psycopg; from datetime import date
from app.config import settings
from app.services.account_equity_evidence import load_account_equity_evidence
from app.services.account_reconciliation_ledger import load_reconciliation_streak
with psycopg.connect(settings.database_url) as c:
    for d in [date(2026, 9, x) for x in (15, 16, 17, 18, 21, 22)]:
        e = load_account_equity_evidence(c, environment="demo", snapshot_date=d)
        print(d, e.observed_at, e.reconciliation_state, e.official_equity, e.official_available_cash,
              e.official_comparand, e.local_eod_value_in_account_currency,
              e.local_eod_value_at_official_marks, e.residual_not_in_local_book,
              e.difference, e.tolerance, e.incomplete_reasons)
    print(c.execute("""select snapshot_date, official_direct_long_positions,
                              official_direct_short_positions, official_pending_order_amount
                       from broker_account_equity_snapshots
                       where environment='demo' and snapshot_date between '2026-09-15' and '2026-09-22'
                       order by 1""").fetchall())
    # trade events strictly between the two observation instants
    print(c.execute("""with w as (
                         select min(observed_at) lo, max(observed_at) hi
                         from broker_account_equity_snapshots
                         where environment='demo' and snapshot_date in ('2026-09-15','2026-09-22'))
                       select event_kind, executed_at, investment_usd, realized_pnl_usd, fees_usd
                       from trade_events, w
                       where executed_at > w.lo and executed_at <= w.hi
                         and event_kind in ('open','close')
                       order by executed_at""").fetchall())
    print(c.execute("select count(*), sum(total_fees) from broker_positions").fetchone())
    print(c.execute("""select reconciliation_state, count(*) from account_reconciliation_days
                       where environment='demo' and reconciliation_rule_version='f0-reconcile-v2'
                         and snapshot_date between '2026-09-15' and '2026-09-22'
                       group by 1""").fetchall())
    print(load_reconciliation_streak(c, as_of=date(2026, 9, 23)))
EOF
```

The cash identity is Σ(−`investment_usd`) over opens plus Σ(`investment_usd` + `realized_pnl_usd`)
over closes, which comes to −225.06. The `broker_positions` count is live and moves as positions
change; 8 is the value on 2026-09-23. The ledger count grows to 6 once 2026-09-22 is written.

## What this does not claim

- **No tolerance was widened.** Every `difference` is within 0.02 USD against a 31.56 USD bound.
- **Nothing about engine performance.** The engine held one position (SPY.RTH, $225.04) for three
  of the six sessions: 2026-09-18, 09-21 and 09-22.
- **Nothing about broker price accuracy.** See the mark-bridge note above.
- **Nothing about the countdown.** `load_reconciliation_streak(c, as_of=date(2026, 9, 23))` returns
  4/5 with `still_refused_past_due`. It counts only days past the four-day decision lag, and it
  stops at the permanent 2026-09-14 refusal. That is #2844's gate, and this page does not stand in
  for it.
