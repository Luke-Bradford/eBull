# #3068 — the reconciliation comparand marks its two sides at different instants

Status: PROPOSAL. Unshipped. Written 2026-09-15; revision 2 after Codex checkpoint 1,
which killed the first recommendation and surfaced the field that makes a better one
possible. Evidence table also on #3068.

## Problem

`account_reconciliation_check` (#2844 clause 3) decides a day by comparing

- **official** — `broker_account_equity_snapshots.official_direct_long_market_value`
  + `available_cash`, where the market value is `Σ(amount + unrealizedPnL.pnL)` over
  `isBuy` positions, parsed from eToro's `/api/v1/trading/info/demo/pnl` when
  `daily_portfolio_sync` runs — **23:55 UTC**; against
- **local** — `portfolio_eod_snapshots.total_value`, computed at 22:30 UTC as
  `Σ (amount ± units × (close − open_rate))` + cash, converted to the display currency,
  where `close` is `price_daily`'s **regular-session close** (20:00 UTC while the US is on
  EDT). ⚠ That is the real formula (`portfolio_eod.py:214-219`); it collapses to
  `units × close` only for an unleveraged long, which is what the whole current book is.

The tolerance is `RECONCILIATION_RULE_VERSION = "f0-reconcile-v1"`:
`MARK_ROUNDING_PER_UNIT` (one cent) per unit held, plus one cent of cash. Its own
docstring calls it *"the tightest bound defensible without measurement"*. It models
**rounding of one mark against the same mark**, so the rule carries an unstated premise:
*the broker's mark equals our session close*.

That premise is false for any instrument eToro quotes after 20:00 UTC, and the tolerance
has no term for the difference. The day's verdict is therefore a function of a quantity
the rule does not model. ⚠ It is not that every evening move reds a day — a small or
offsetting move still passes. It is that nothing bounds the one that does not.

## Measured

2026-09-14, the first session of the #2844 countdown: `diverged`,
`difference = -204.65` against `tolerance = 31.56`, **no incomplete reasons** — a hard red,
not a refusal. Attributed against eToro's own `get_intraday_candles` (`FifteenMinutes`)
around the snapshot instant:

| instrument | units | our close | broker mark @ ~23:55 UTC | Δ USD |
| --- | --- | --- | --- | --- |
| GME (1699) | 1500 | 21.63 | 21.50 (23:30 bar close) | −195.00 |
| QQQ (3006) | 17.252438 | 710.02 | 709.46 (23:45 bar close) | −9.66 |
| VOO (4238) | 16.929336 | 699.23 | no bar after 20:00 UTC | 0 |
| IEP (1571) | 315.948108 | 7.03 | no bar after 20:00 UTC | 0 |
| NXH (1181) | 1305.057096 | 3.60 | no bar after 20:00 UTC | 0 |
| | | | **total** | **−204.66** |

Observed −204.65, attributed −204.66: agreement **to within one cent**, and the residual is
expected — a 15-minute bar's close is not the tick at `observed_at`. ⚠ Nor does "no bar
after 20:00 UTC" prove those three marks were unchanged; it is consistent with it and is
not a measurement of the broker's mark. The two evening-quoted names carry the whole
signal either way.

Scale: `Σ |units| × 0.01 = 31.55`, so the tolerance is 31.56 including the cash cent. At
1500 GME units a **three-cent** evening move ($45) exceeds it; two cents ($30) does not.

⚠ **This is one day of one five-holding book.** It establishes the mechanism and the
arithmetic; it does not establish a rate across stored days, instruments, leverage,
currencies or corporate actions. No full-population claim is made here and none is needed
— the defect is in the rule's premise, not in a frequency.

Both stored `reconciled` days (08-24, 08-25) are already outside
`MAX_EVIDENCE_AGE_DAYS = 12`, so the streak is **void today**. A
`RECONCILIATION_RULE_VERSION` bump costs zero now and costs a restarted countdown later.

## Source rule

No regulator specifies how a firm reconciles its own book against its broker's, and the
search is not re-run here: it was already done and recorded at `RECONCILIATION_RULE_VERSION`
(an earlier draft's SEC Reg NMS Rule 612 citation was withdrawn as off-point — Rule 612
governs the increments on which NMS stocks may be QUOTED, not how far two valuations of one
holding may differ, and it does not reach CFDs).

⚠ That exemption covers the **tolerance magnitude and the cadence** only. It does not
license inventing field semantics: the operand definitions are governed by eToro's own
published contracts, which must be cited per field. The ones this proposal relies on, from
the committed `tests/fixtures/etoro/openapi_v1.375.0.json`:

- `ClientPortfolio.positions[]` is `TradingRealAdminApi_Position` in **both**
  `PortfolioResponse` and `PortfolioResponseWithPnl` — the same item schema on `/portfolio`
  and on `/pnl`.
- `TradingRealAdminApi_Position.units` — *"Number of units in the position"*.
- `.amount` — *"USD amount allocated to the position"*; `.openRate` — *"Entry price of the
  position in the instrument's currency"*; `.isBuy` — *"true for long (buy) positions"*;
  `.isPartiallyAltered` — *"whether this position was partially closed"*;
  `.initialUnits` — *"does not change"* (so `units` is the current quantity).
- `.unrealizedPnL` — *"only present in PnL endpoint"*, which is what makes the two
  responses differ at all.

Settled decisions consulted: 2026-09-13 "#2844 reconciliation countdown counts DAYS OF
EVIDENCE" (a `RECONCILIATION_RULE_VERSION` bump resets the countdown to zero — anticipated
there, and this proposal takes it) and 2026-08-22 "the allocation boundary is the ONLY
safety net". Neither is reversed.

## ⚠⚠ The field that changes the design — `units` is on the `/pnl` payload

`_parse_account_risk_snapshot` reads `amount`, `unrealizedPnL.pnL`, `instrumentId`,
`positionId`, `isBuy` and `isPartiallyAltered` from each position row and **does not read
`units`**, which the same row publishes. Revision 1 of this document asserted units were
absent from the payload and built a whole candidate around working around that. They are
not absent; they were unread. Checkpoint 1 caught it against the portal schema.

This matters because it makes a **per-position implied mark** computable from the official
payload alone: `mark_official(p) = (amount + pnL) / units`, all three terms from one `/pnl`
row. That removes the mark-instant mismatch **by construction** — the official mark and
the quantity it is applied to come from the same observation — without needing an
independent price at an arbitrary instant.

## ⚠ What the control can actually test — read before choosing

The countdown spec records it in its own limitations: *"`_read_positions` reads **current**
`broker_positions`, so a snapshot for session D is D's closes applied to the book at run
time."* So the local side is not an independent book; it is the broker's own position set
(from `/portfolio`), valued with our marks and our FX.

The comparison therefore tests our parse, our persistence, our valuation arithmetic, our FX
and our cash mapping — across two endpoints — and, nominally, our marks. Only the last is
broken by this defect, and it is the one the control cannot perform anyway: it cannot tell
a wrong mark from an evening move.

⚠ Two claims from revision 1 are withdrawn here, both falsified at checkpoint 1:

- *"a failed `sync_portfolio` leaves a stale book beside a fresh `/pnl` snapshot, and the
  control catches it."* It does not: in `daily_portfolio_sync` the snapshot is recorded
  **after** `sync_portfolio` returns, so an exception there prevents the official write
  too. Equality after a successful sync also demonstrates post-repair consistency, not
  that the run before it was correct.
- *"mark correctness is #3046's control."* Overstated. `portfolio_eod` reads raw
  `price_daily` with no quarantine gate, and quarantine's own admission test is
  *not-known-bad* (`app/services/price_quarantine.py:27`). Giving up the mark check here
  gives it up, full stop — see Accepted losses.

## Candidates

**A — mark the local side at the broker's instant from our own feed** (intraday bars at
`observed_at`). Keeps a mark check. Adds an external fetch inside the reconciliation path,
a bar-alignment rule, and a NEW unmeasured tolerance term (bar close vs tick). ⚠ And
`observed_at` is stamped after the HTTP call completes — it is when we received the
response, not when eToro struck the mark, so "the broker's instant" is itself approximate.

**B — capture the official snapshot just after the regular close**, so the existing premise
becomes true. Smallest diff. But "just after the close" does not guarantee the closing
mark (auction dissemination, bid/ask basis, DST, early closes, feed lag), it holds only for
a US-venue book, and it re-breaks silently with no reason emitted the day a non-US holding
appears. It couples a correctness control to a scheduler constant.

**C — reconcile structurally, mark-free** (position set + units + cash), value comparison
demoted to a recorded figure. Rejected at checkpoint 1 and correctly: as drafted it read
BOTH units operands from `/portfolio`-sourced storage, which compares a value to itself; it
gives up more than marks (a wrong `amount`, direction or currency mapping passes); and the
ledger writer requires a finite `difference` **and** `tolerance` for any decided row
(`account_reconciliation_ledger.py:359`, plus SQL constraints), so a mark-free verdict has
no legal value to store.

**D — value the local book at the official payload's own per-position mark.** ⭐

## Recommendation — D

For each direct long position `p` present in the official payload, with
`mark_official(p) = (amount_official(p) + pnL(p)) / units_official(p)`:

```
local_at_official_marks = Σ_p [ amount_local(p) + units_local(p) × (mark_official(p) − open_rate_local(p)) ]
                          + cash_local        (converted to the account currency)
difference              = official_comparand − local_at_official_marks
```

where `amount_local`, `units_local` and `open_rate_local` come from the stored
`broker_positions` rows written from `/portfolio`, and every official term comes from
`/pnl`.

Why this is the right construction:

- **The mark-instant mismatch cannot occur.** The mark and the quantity it prices are two
  fields of one observation. There is no second instant to align.
- **It stays a VALUE comparison**, so the ledger's finite-`difference`/finite-`tolerance`
  contract, the stored diagnostic figure, and the existing `/strategies` panel labels all
  keep working. C could not say that.
- **The units check is genuinely two-endpoint.** `units_local` is `/portfolio`-sourced and
  `units_official` is `/pnl`-sourced; they are separate requests whose agreement is a real
  assertion. ⚠ They are the same *schema*, so this tests our fetch/parse/persist of two
  responses, not two independent upstream computations. Claim it that narrowly.
- **A structural error shows at its own value.** A missing local position contributes its
  whole market value to `difference`; stale units contribute `Δunits × mark`. Both are
  exactly what the clause wants to catch.
- **The residual tolerance is honestly rounding again** — the division in
  `mark_official`, and FX. Which is what `f0-reconcile-v1` was already written for.

## Design if D is taken

**Parser.** `_parse_account_risk_snapshot` reads `units` per position into
`BrokerDirectPositionInvestment`. Refuse a non-positive or non-finite `units` on a row
carrying a non-zero market value — a zero divisor must not silently become an implied
mark. ⚠ `units` is a JSON number: define the decimal normalisation explicitly
(`Decimal(str(...))`, as every other money field here already does) and the precision the
storage column holds. Do not assert exactness that JSON decoding does not provide.

**Storage.** `broker_account_position_marks`, one row per direct position per snapshot day,
FK to `broker_account_equity_snapshots (environment, snapshot_date)`, keyed
`(environment, snapshot_date, position_id)`, carrying `instrument_id`, `is_buy`, `units`,
`amount`, `unrealized_pnl`, `market_value`, `is_partially_altered`. Written in the same
transaction as the parent, and — because the parent is an `ON CONFLICT DO UPDATE` that
only accepts a NEWER observation for TODAY — the child set must be **replaced wholesale
exactly when the parent accepts the write, and left untouched when it does not**. The
parent's `RETURNING` already reports which happened.

**Verdict inputs.** Position-set symmetric difference recorded with SIDES (missing-local
and extra-local separately, never a count), per-position units deltas, the cash delta, and
the value `difference` above. Existing FX and completeness refusals are unchanged.

**Legacy.** Every stored snapshot predating this has no child rows. That is **missing
evidence → `refused`**, with its own reason, never an empty official book (which would
otherwise read as "the broker holds nothing" and reconcile against an empty local side).

**Version.** `RECONCILIATION_RULE_VERSION` → `f0-reconcile-v2`; resets the countdown by
construction, and both current greens are already void.

**Still unsolved, and D does not claim otherwise.** The official set is captured at 23:55
UTC and the local EOD book at 22:30 UTC, and the countdown spec documents a 0-3 day lag on
the local stamp — so this is not reliably an 85-minute window. Any open, close, partial
close, deposit, withdrawal, fee or dividend in between is a real structural difference on a
healthy pipeline, and it freezes a red. D narrows the exposure from *every mark on every
evening-quoted name* to *account activity between two captures*, which on a static demo
book is currently zero and on a trading engine will not be. Closing it needs the two
captures paired in one observation with a pairing identity, which is its own slice.

## Accepted losses, stated

- A green no longer says anything about our `price_daily` marks, and nothing else currently
  does either (see the withdrawn claim above). This is a real reduction in coverage, taken
  deliberately: the mark check as built cannot distinguish a bad mark from an evening move,
  so what is being given up is the appearance of a control, not a control. If mark
  agreement is wanted it needs its own same-instant comparison, filed separately.
- Aggregate offsetting errors: D reduces this materially (per-position operands) but the
  stored verdict is still one scalar. The per-position deltas are recorded for audit.

## Tests

- Parser→storage→verdict, with `units` varied INDEPENDENTLY between the `/portfolio` and
  `/pnl` fixtures. A test that fabricates both from one constant cannot see the defect this
  whole document is about.
- Same-id corruption: right `position_id`, wrong `instrument_id` / wrong `is_buy`.
- The regression this exists for: an evening mark move of arbitrary size does NOT move the
  verdict, asserted against the measured 2026-09-14 numbers.
- Zero / missing / non-finite `units` on a non-zero market value → refusal, not a divide.
- Legacy snapshot with no child rows → `refused` with its own reason, not `reconciled`.
- Child-set replacement: parent write rejected (older observation) leaves children intact;
  parent accepted replaces them wholesale; a newer snapshot that drops a position removes
  its child row.
- The freeze trigger still refuses to move a decided row.

## Checkpoint-1 findings deferred, not answered

Recorded so the implementing session knows they were seen and are open: precedence between
"incomplete" and "proven mismatch" when both hold; the population contract for unmapped
instruments / zero-unit rows / duplicates / shorts / mirrors; `credit`-vs-documented-
available-cash and the pending-order refusal; per-currency cash scope; `is_partially_altered`
semantics for current-vs-initial units; read consistency when the EOD parent and child rows
are read in separate queries while a replacement runs; and mixed-version deployment/API-label
behaviour across the version bump.

---

# Revision 3 — slice 2's implementation design, and two things revision 2 got wrong

Written 2026-09-15 before any slice-2 code. Revision 2's *"Design if D is taken"* is
amended here, not restated; where the two disagree this section wins.

## ⚠⚠ 1. The local terms cannot come from `broker_positions`

Revision 2 wrote: *"local terms from stored `broker_positions` (`/portfolio`)"*. That is
wrong, and not marginally.

`broker_positions` is CURRENT state — `sync_portfolio` overwrites it on every run, and it
has no snapshot date. `load_account_equity_evidence(snapshot_date=D)` is called by
`account_reconciliation_check` up to `MAX_DECISION_LAG_DAYS = 4` days after D. Reading
`broker_positions` there would make day D's verdict a function of the book as it stands at
decision time — so a position opened after D would red a day it was not part of, and the
same day would verdict differently depending on when the job happened to fire. A
reconciliation whose inputs move after the fact is not evidence.

The AS-OF record already exists: `portfolio_eod_position_snapshots` (sql/196), written in
the same transaction as its `portfolio_eod_snapshots` parent, one row per position, holding
`units`, `close_price`, `native_currency`, `price_status` and `mark_price_date`.

## 2. With as-of local rows, D is reachable as a mark SUBSTITUTION, and needs no new
##    local money columns

For a long position, `portfolio_eod.compute_eod_equity` stores
`value_native = amount + units × (close − open_rate)`. Substituting the official mark:

```
amount + units × (mark_official − open_rate)
  = [amount + units × (close − open_rate)] + units × (mark_official − close)
  =  the value already in positions_value      +  a correction term
```

`close` appears twice with opposite signs at full stored precision and cancels **exactly**
in `Decimal`; `amount` and `open_rate` never have to be re-read. So

```
local_at_official_marks = local_total_value + Σ_p  sign(p) × units_p × (mark_official(p) − close_p)
```

with `sign = +1` long / `−1` short, which is algebraically identical to revision 2's
formula on every position that priced, and requires only `units`, `close` and the direction
from the local side. This is what slice 2 implements.

⚠ It is identical only where the local row is `priced`. A `no_price` or `no_fx` position
contributed nothing to `positions_value`, so there is no value to correct and the
substitution is undefined for it — refused, not skipped (see 4).

## ⚠ 3. Direction is the one local field that is not stored, and it is not borrowable

The sign above needs `is_buy`. `portfolio_eod_position_snapshots` does not carry it. The
official child row does — and taking it from there would assume the two endpoints agree on
the direction of a position, which is precisely the class of unstated premise this ticket
exists to remove, in a slice whose subject is an unstated premise.

`sql/384` adds `is_buy BOOLEAN` (nullable) to `portfolio_eod_position_snapshots`,
forward-only, no backfill — the same posture and precedent as `unrealised_pnl_usd`
(sql/308) and `mark_price_date` (sql/350). NULL means the row predates sql/384 and
**refuses**; it is never read as long. Once stored, local `is_buy` vs official `is_buy` is a
real two-endpoint assertion.

## 4. Refusals added (all of them named, none of them silent)

| slug | condition |
| --- | --- |
| `official_position_marks_not_recorded` | zero child rows while the parent declares `official_direct_long_positions + official_direct_short_positions > 0`, or either count is NULL. Handoff item 3; 11 of 20 stored dev rows. |
| `official_position_marks_unusable` | a child's `units` is non-finite or non-positive. ⚠ Not redundant with the table's `CHECK (units > 0)`: PostgreSQL `numeric` admits `NaN`, and orders it ABOVE every non-NaN value, so `NaN > 0` is TRUE and the CHECK passes it. |
| `official_position_missing_locally` | in the official child set, absent from the local EOD set. |
| `local_position_missing_officially` | the other side. Recorded separately — a count cannot say which side is short, and the two have different causes. |
| `local_position_mark_unusable` | a matched local row that is not `priced`, or has a NULL `close_price` / `native_currency`. |
| `local_eod_position_direction_not_recorded` | matched local row predating sql/384. |
| `position_identity_mismatch` | same `position_id`, different `instrument_id` or different `is_buy`. |

## 5. FX — one rate load, and the correction converts native → ACCOUNT, not native → display

The correction term is in the position's native currency. It is converted **straight to the
account currency** and added after `_convert_local_total` has converted the stored total
from the display currency, rather than being pushed through display first: on the live
configuration (display GBP, account USD, natives USD) the display round-trip would convert
a USD correction into GBP and back for no reason.

⚠ Both conversions use ONE rates dict, loaded once at the local snapshot's own
`fx_rate_date`. Two loads at the same date are not guaranteed to be the same rates (a row
may be revised between them), and a total and its own correction computed at different rate
revisions is a defect with no symptom.

## 6. Deliberately NOT changed

- The tolerance formula and `MARK_ROUNDING_PER_UNIT`. Its magnitude is now wider than the
  residual it guards (division and FX rounding, not a stored mark), which is the safe
  direction; narrowing it is a measurement nobody has made.
- `official_comparand`, and the `official_direct_short_positions_unvalued` refusal — the
  official side still values longs only, so a short book is still undecidable.
- `direct_position_count_mismatch`. It is NOT a duplicate of the set checks above: it
  compares the parent's counts, which are summed from the payload's per-INSTRUMENT
  section (`instrument_investments`), against the local count — while the set checks read
  the per-POSITION section. Different sources, both real.
- The units delta. A units disagreement still shows at its own value rather than becoming a
  refusal, exactly as revision 2 argued, and it is the visible face of the unsolved
  capture-pairing problem.

## ⚠⚠ 7. The handoff's acceptance expectation is falsified — 2026-09-14 cannot reconcile

#3068's slice-1 comment states: *"2026-09-14 should decide `reconciled` on a difference near
zero rather than -204.65."* It cannot, and item 3 of the same comment is why:

```sql
select s.snapshot_date, s.official_direct_long_positions, s.official_direct_short_positions,
       (select count(*) from broker_account_position_marks m
         where m.environment=s.environment and m.snapshot_date=s.snapshot_date) as children,
       (select count(*) from portfolio_eod_position_snapshots p
         where p.snapshot_date=s.snapshot_date) as local_positions
  from broker_account_equity_snapshots s where s.environment='demo'
  order by s.snapshot_date desc limit 2;
-- 2026-09-15 | 7 | 0 | 7 | 0      children, no local book
-- 2026-09-14 | 7 | 0 | 0 | 7      local book, no children
```

09-14 predates the sql/383 writer, so it has no official marks and refuses with
`official_position_marks_not_recorded`. 09-15 has them but no local EOD row yet
(`MAX(price_daily.price_date)` over held instruments is still 2026-09-14). **No stored day
currently carries both sides**, so slice 2 cannot produce a green on merge day and must not
be written as if it will. The first decidable v2 day is the first date carrying a child set
AND a local EOD row — 09-15 at the earliest, judged from 09-16 (`pending_reconciliation_
dates` requires `snapshot_date < as_of`).

⚠ Nothing is frozen in the meantime: the newest stored ledger row is 2026-08-26, so
09-14's `diverged` was evaluated read-only and never written. The countdown has not in fact
been reset by this defect — it has not started.

## 8. What CAN be verified on real data now

The official 09-15 child set and the local 09-14 position set have **identical
`position_id`s and identical `units` on all seven rows**, and the 09-15 official marks are
the Sep-14 evening marks (the row was written by the 03:34 UTC deploy catch-up). So the
substitution can be exercised read-only across the two dates as a simulation of the
comparison 09-14 would have made had children existed — reported as a simulation, never
stored, and never as a verdict.

---

# Revision 4 — candidate D is WITHDRAWN. The broker publishes the mark.

Written 2026-09-15 after Codex checkpoint 1 on revision 3, which killed the recommendation
for the second time in this document, and after a read-only measurement of the live demo
`/pnl` payload. Revisions 2 and 3 are superseded wherever they disagree with this one.

## ⚠⚠ 1. Why D is wrong: `(amount + pnL) / units` is equity per unit, not price

The portal documents `amount` as *"USD amount allocated to the position. This amount
includes both the initial investment, **and additional margin allocated to the position as
collateral**"* (`TradingRealAdminApi_Position.amount`). It is the position's capital, not
`units × openRate`. The two coincide only at leverage 1 with no added collateral, and the
derived "mark" is a price only under that coincidence.

Worked, at leverage 2 — `amount` 50, `units` 1, `openRate` 100, price 110, `pnL` 10:

| | value |
| --- | --- |
| official `amount + pnL` | 60 |
| local `compute_eod_equity` = `amount + units × (close − open_rate)` | 60 ✅ agrees today |
| derived "mark" `60 / 1` | 60 |
| D's substituted local = `50 + 1 × (60 − 100)` | **10** |
| D's `difference` | **50, on a healthy position** |

**And where D does not produce a false divergence, it produces no signal at all.** With
`units_local = units_official = u` the per-position difference reduces, algebraically, to

```
(amount_o + pnL_o) − [amount_l + u × (mark_derived − open_rate_l)]
  = (amount_o + pnL_o) − amount_l − (amount_o + pnL_o) + u × open_rate_l
  = u × open_rate_l − amount_l
```

— which is **identically zero exactly when the position is unleveraged with no added
margin**, i.e. on the entire live book. Measured on the demo account 2026-09-15: all seven
positions report `leverage: 1`, and `amount` equals `units × openRate` on every one
(GME 1000 × 22.59 = 22590.0; IEP 244 × 8.18 = 1995.92; NXH 1305.057096 × 6.09 = 7947.8).
So D would have shipped a value comparison that **cannot fail**, and a comparison that
cannot fail reports green. That is strictly worse than the defect it replaces.

Both points are Codex checkpoint-1 findings 1, 2 and 6, reproduced against
`compute_eod_equity` and then against the live payload.

## ⚠⚠ 2. The mark is a DOCUMENTED, POPULATED field. Third occurrence of one mistake.

`TradingRealAdminApi_Position.unrealizedPnL` — the object whose `pnL` slice-1 already
reads — documents, and the live demo response populates:

| field | portal description | live 2026-09-15 |
| --- | --- | --- |
| `closeRate` | "Current close rate" | populated on 7/7 |
| `closeConversionRate` | "Current close conversion rate" | 1.0 on 7/7 (USD asset, USD account) |
| `assetCurrencyId` | "Currency ID for the asset" | 1 on 7/7 |
| `timestamp` | "Timestamp of the PnL calculation" | `2026-09-15T04:02:27.3169794Z`, identical across positions |
| `pnlAssetCurrency`, `exposureIn*`, `marginIn*` | — | populated |

GME `closeRate` 21.54 against our 2026-09-14 close of 21.63; IEP 7.03 and NXH 3.60 against
our 7.03 and 3.60. That is the evening mark, published, per position, with the instant it
was struck.

⚠ **This is the same error for the third time in one ticket.** Revision 1 asserted `units`
were absent from the payload because they were absent from our parsed model; checkpoint 1
falsified it. Revision 2 built candidate D on the field that discovery exposed — and
asserted, implicitly, that no published mark existed, from the same evidence: our parser
does not read one. Revision 3 repeated it. **Our parser is not evidence about the payload.**
The measurement that settles it is one read-only GET, and it should have been the first
step of the ticket, not the fourth.

## 3. Recommendation — D′: substitute the PUBLISHED close rate

```
correction(p)            = s(p) × units_local(p) × (close_rate_official(p) − close_local(p))   [asset ccy]
                           s = +1 long, −1 short
local_at_official_marks  = convert(local_total_value, display → account)
                           + Σ_p  correction(p) × close_conversion_rate_official(p)
difference               = official_comparand − local_at_official_marks
```

This is the local formula with OUR close replaced by the BROKER's close, and nothing else
touched. Its properties, each of which D lacked:

- **Leverage- and margin-safe.** `amount` and `open_rate` are never re-derived, so the
  identity `amount = units × openRate` is never assumed. Codex findings 1 and 2 do not
  reach it.
- **`close_local` cancels.** `positions_value` carries `+s·u·close_l` and the correction
  carries `−s·u·close_l`; the subtraction is exact in `Decimal`. So the comparand does not
  depend on our marks, which is the whole point and has a consequence in §5.
  ⚠ Exact in the arithmetic, NOT end to end (Codex finding 7, confirmed by test): the
  left operand is read back from `portfolio_eod_snapshots.total_value`, a `NUMERIC(20,4)`,
  so the stored total contributes up to half a unit in its last decimal place. Measured on
  the 2026-09-14 shape that is **2.9e-5 USD against a 31.55 tolerance**. Stated, not
  claimed away — "cancels exactly" is the sentence a later reader would build a tighter
  bound on.
- **It does not degenerate.** With agreement on all fields the difference is zero; with a
  disagreement on `units`, `amount`, `open_rate`, or the broker's P&L rule, each shows at
  its own value. Verified against the live payload: for an unleveraged long the official
  `amount + pnL` = `units × closeRate` (GME: 22590 − 1050 = 21540 = 1000 × 21.54 ✓), and
  the substituted local is `units_local × closeRate`, so the residual is
  `(units_o − units_l) × closeRate` — revision 2's claimed behaviour, which was false for D
  (Codex finding 4) and is true for D′.
- **Dimensionally clean.** `close_rate` is an asset-currency price, `close_local` is an
  asset-currency close, and `close_conversion_rate` is the broker's own asset → account
  rate. No ECB rate enters the correction, so the correction and the stored total cannot be
  computed at different rate revisions (Codex findings 9, 12, 13, 14, 15 dissolve rather
  than being answered).

## 4. Storage and parsing

`sql/385` adds to `broker_account_position_marks`: `close_rate NUMERIC(20,8)`,
`close_conversion_rate NUMERIC(20,10)`, `asset_currency_id INTEGER` and
`pnl_timestamp TIMESTAMPTZ`, with `CHECK (col IS NULL OR col > 0)` on the first three.

⚠ **All four are NULLABLE, and an earlier draft of this section said `NOT NULL`.** They
cannot be `NOT NULL`: seven child rows already exist from the sql/383 writer and there is
nothing to backfill them with — a close rate is instantaneous and the payload is not
retained, so any value written now would be a reconstruction wearing an observation's
clothes. The constraints are therefore written `IS NULL OR ...`: they constrain what may
be WRITTEN without asserting that every stored row has been. The READER supplies the
requirement instead, refusing a row with a NULL operand
(`official_position_marks_unusable`) rather than falling back to the withdrawn derivation.

The first three fail closed AT THE PARSER on absent / non-numeric / non-finite /
non-positive, on the same grounds as `units`: they are comparand operands, so nothing
unusable is ever written in the first place.

⚠ `pnl_timestamp` is stored because it is the ONLY record of the instant the mark was
struck, and the unsolved capture-pairing problem (§7) is unmeasurable without it. It is
deliberately not yet a refusal input — no threshold for it has been measured.

`sql/384` adds `is_buy BOOLEAN` (nullable) to `portfolio_eod_position_snapshots`, because
the correction's SIGN is local and `compute_eod_equity` was discarding it. Rationale and
the forward-only posture are in that migration's header.

⚠⚠ `_parse_direct_position` (the `/portfolio` parser behind `broker_positions`, and hence
behind the local direction) read `is_buy=bool(payload.get("isBuy", True))` — **an absent
direction defaulted to LONG**, and `bool()` coerced any truthy value. A stored default is
not an observation, so the two-endpoint direction check would have been partly vacuous.
Changed to fail closed, matching `_parse_account_risk_snapshot._is_buy`, which already
does. `isBuy` is documented on the shared schema and present on 7/7 live positions, so
this cannot fail on a well-formed payload. Codex finding 23.

## 5. Refusals — the set, and the order they are evaluated in

Precedence is explicit (Codex finding 16): **evidence presence → operand validity →
identity → set → arithmetic**. Nothing divides before its divisor is validated, and legacy
absence can never present as a fabricated missing-position finding.

| slug | condition |
| --- | --- |
| `official_position_marks_not_recorded` | zero child rows while the parent declares `long + short > 0`, or either count is NULL. |
| `official_position_marks_incomplete` | child count ≠ parent `long + short`, or child long/short counts ≠ the parent's. Catches PARTIAL child loss, which zero-child checking cannot (Codex findings 18, 19). |
| `official_position_marks_unusable` | a child operand non-finite or non-positive. ⚠ Not redundant with the table CHECKs: PostgreSQL `numeric` admits `NaN` and orders it ABOVE every value, so `NaN > 0` passes. |
| `local_eod_position_direction_not_recorded` | a matched local row has NULL `is_buy`. |
| `local_position_mark_unusable` | a matched local row is not `priced`, or its `close_price` / `native_currency` / `units` is NULL, non-finite or (units) non-positive. |
| `position_identity_mismatch` | same `position_id`, different `instrument_id`, different `is_buy`, or an `asset_currency_id` that does not map to the local row's `native_currency`. |
| `official_position_missing_locally` | in the official child set, absent from the local EOD set. |
| `local_position_missing_officially` | in the local EOD set, absent from the official child set. |

⚠ **The set check is SYMMETRIC — both sides refuse — and an earlier draft of this section
argued for an asymmetry that would have been a behaviour regression.** The argument was
that a position present OFFICIALLY but not locally leaves the local total fully priced at
broker marks, so the difference is that holding's market value and `diverged` is the
truthful verdict. That is sound in isolation and wrong in context: the existing
`direct_position_count_mismatch` reason ALREADY refuses that exact row today, by comparing
the parent's declared counts against the local `positions_total`. Adopting the asymmetry
would therefore have been a silent widening of an existing gate dressed as a construction
argument. Both sides refuse, which is what happens now.

The sides are still named SEPARATELY rather than counted, because a count cannot say which
side is short and the two have different causes — a missing local row is a sync that has
not run, a missing official row is a position the broker is not pricing.

### Two refusals are RETIRED from this comparand, with the reason

`local_eod_marks_carried_forward` and `local_eod_effective_time_unknown` no longer gate the
v2 comparison. Both describe `close_local`, and §3 shows `close_local` cancels exactly out
of the comparand — so they now name a quantity the verdict does not depend on. This is a
widening and is stated as one: measured on the stored population, **4 of the 10 local
snapshots carrying recorded mark dates have `stale_mark_positions > 0`**

```sql
select count(*) filter (where stale_mark_positions > 0), count(*)
  from portfolio_eod_snapshots where oldest_mark_date is not null;  -- (4, 10)
```

so retaining them would refuse roughly two days in five for an irrelevance, and five
consecutive greens would be materially harder for no gain in safety. A test asserts the
cancellation directly: moving a local close by an arbitrary amount does not move
`difference`.

⚠ `mark_effectiveness_reasons` is DELETED rather than left unused — this was its only
caller, so it was dead, not merely bypassed. The magnitude counters it sat beside
(`local_eod_positions_priced`, `local_eod_stale_mark_positions`) are untouched and still
reported: the operator keeps the information, it stops being a refusal. The FE labels for
both slugs are kept so verdicts STORED under `f0-reconcile-v1` keep rendering.

## 6. API surface — `difference` must keep adding up

`local_eod_value_in_account_currency` keeps its current meaning (the stored local total,
converted). A NEW field `local_eod_value_at_official_marks` carries the substituted
valuation, and `difference = official_comparand − local_eod_value_at_official_marks`.
Redefining the existing field in place would silently change a number the panel already
shows and break its stated relationship to `difference` (Codex finding 31).

## 7. Accepted limitations — recorded, not answered

- **The local EOD rows are as-of at COMPUTE time, and a re-run replaces them.** Revision 3
  called them immutable; they are not (`_write_snapshot` upserts the parent and replaces
  the children for its resolved date, and the 2026-09-12 recovery burst re-stamped an
  18-day-old row). This is still strictly better than `broker_positions`, which has no date
  at all — but a decided verdict's operands can still change underneath it. Codex findings
  27 and 29.
- **The capture-pairing problem is untouched.** The official set is struck at the `/pnl`
  `timestamp` and the local book at the EOD job's own time; any account activity between
  them is a real difference on a healthy pipeline. `pnl_timestamp` is now stored so the
  window can be measured, which is the prerequisite for closing it.
- **Structural mismatches refuse rather than diverge**, and a refusal is retryable inside
  the grace window where a divergence is not (Codex finding 17). A persistent structural
  disagreement therefore never yields a green rather than immediately failing one.
- **The sided diagnostics revision 2 promised are NOT stored.** The side is carried in the
  refusal slug; per-position deltas and units deltas have no storage design and none is
  invented here (Codex finding 30).
- **`compute_eod_equity` treats the broker's USD `amount` as native currency** and uses the
  ECB display rate on the resulting value (Codex finding 10). Pre-existing, unchanged here,
  and harmless while every holding is USD-denominated. Noted on the PR.
- **`portfolio_eod_*` has no `environment` column**, so the loader's `real` branch would
  consume the demo local book; the countdown is demo-only and unaffected (Codex finding 32).
- Deferred and still deferred: partial-close basis, `pnlVersion`, fees/dividends,
  settlement type, per-currency cash scope, `credit`-vs-documented-available-cash.

## 8. Corrections to revision 3's factual claims

- **The parent counts are NOT an independent source.** `_parse_account_risk_snapshot`
  accumulates `direct_long_count` / `direct_short_count` and appends `direct_positions`
  inside the SAME `for item in positions:` loop (`etoro_broker.py:1649`). Revision 3 claimed
  a separate per-instrument payload section; there is none. `direct_position_count_mismatch`
  is retained as a storage-consistency check, not as a second source (Codex finding 33).
- **"11 of 20 rows are missing evidence" understates it.** Eleven lack the parent count
  columns; every snapshot with populated counts but no sql/383 children also refuses, which
  on the stored population is every row before 2026-09-15 (Codex finding 34).
- **"Up to 4 days" is the STREAK bound, not the evaluation bound.** `CALENDAR_LOOKBACK_DAYS`
  = 4 + 12 + 14 = 30, and the replay scan covers it; `MAX_DECISION_LAG_DAYS` limits whether
  a decision counts toward the streak (Codex finding 35).
- Revision 3's §7 conclusion stands but its query does not establish it on its own
  (`LIMIT 2`, and zero local children does not by itself prove no local parent). The claim
  that no stored day carries both sides is re-established over the full stored population,
  not two rows (Codex finding 37).

## 9. Checkpoint 2 — one P2, and it corrected §7's read-consistency reasoning

Codex's pre-push pass returned a single finding, on the multi-query read. §7 had recorded
read consistency as an accepted limitation on the grounds that the race is structurally
excluded: `record_account_equity_snapshot` only ever accepts a write for TODAY, and the
countdown only decides days strictly before today.

That argument covers the OFFICIAL side and **not the local one**, which is the side that
actually moves. `portfolio_eod._write_snapshot` upserts the parent and replaces the
children for whatever date it resolves — and that can be a past date: the 2026-09-12
recovery burst re-stamped a row 18 days old. So a recompute committing between the totals
statement and the position queries hands the loader one snapshot's TOTAL beside another's
MARKS, the correction subtracts closes that never contributed to it, and
`run_reconciliation_check` freezes the result.

Fixed by pairing rather than by isolation: `_reread_write_stamps` re-reads
`broker_account_equity_snapshots.recorded_at` and `portfolio_eod_snapshots.computed_at`
after the position reads and refuses with `reconciliation_inputs_changed_during_read` if
either moved. Both writers bump their stamp in the same transaction as their rows, so an
unchanged pair proves no version boundary was crossed.

⚠ `snapshot_read` was NOT used, and the reason is specific: it COMMITS the caller's
pending transaction before switching isolation, and this loader runs inside
`run_reconciliation_check`'s transaction — buying read consistency that way would commit a
job's in-flight work as a side effect. A refusal is also the recoverable outcome here: the
ledger re-decides an undecided day on the next pass, so refusing costs a day and a wrong
`reconciled` costs the control.
