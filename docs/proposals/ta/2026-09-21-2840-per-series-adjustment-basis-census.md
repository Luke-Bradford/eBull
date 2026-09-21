# #2840 residual — per-series adjustment basis from stored price columns: REFUSED

Status: REFUSED (2026-09-21). Fifth refused construction on #2840's provenance line, and
the first refused for a reason that no amount of tuning reaches.

The construction was measured in full before it was refused. The numbers are kept below
because they are what makes the refusal decisive, not because they support anything.

## What was proposed

`from_archive_basis` (`app/services/strategy_price_basis.py:412`) certifies every bar of a
run as `observed_unadjusted` from an archive-level label whose entire evidence is **two
AAPL bars** (`research_corpus_ingest.py:165-171`), a weakness its own docstring names.
`research_price_daily` stores `close` **and** `adj_close`, so the proposal was to validate
the label per series from the corpus itself. Let

```
f[d] = close[d] / adj_close[d]
```

and call a consecutive-bar step in `f` that is both large (`>= 1.2`, reusing
`_ADJUSTMENT_RATIO_THRESHOLD`, `app/services/market_data.py:1215`) and rational
(`p/q`, `q <= 5`, `p <= 200`, rel-tol `1e-4`) a **re-basing cliff**.

It works, in the narrow sense that it fires where it should. Ground truth, AAPL: the
Intrader `unadjusted` series returns its five real splits at exact ratios — `2.0000`
(1987-06-16), `2.0000` (2000-06-21), `2.0000` (2005-02-28), `7.0000` (2014-06-09),
`4.0000` (2020-08-31) — and the paperswithbacktest `split_adjusted` twin returns zero.

Full population, 30,591 series / 75,972,669 bars, 53 s:

| | series | with ≥1 cliff | events |
| --- | --- | --- | --- |
| `unadjusted` | 22,880 | 4,061 | 8,318 |
| `split_adjusted` | 7,711 | 5 | 5 |

## Refusal 1 — `f` is scale-invariant, and the gate needs a scale

This is the one that ends it.

Let a series' stored `close` be `R/c` for raw `R` and any constant `c`, and its `adj_close`
be `R/S[d]`. Then `f[d] = S[d]/c`, and every consecutive ratio

```
f[d-1]/f[d] = S[d-1]/S[d]
```

is **independent of `c`**. Multiply every close in a series by any constant and not one
detected cliff moves. So the statistic cannot distinguish a raw series from a uniformly
rescaled one — a pence/pounds series, a partially-adjusted series, a series divided by a
historical ADR ratio.

`sql/305`'s first arm requires *"a directly observed unadjusted **level**"*.
`f` is a **relative** quantity by construction and is blind to level. A cliff establishes
that `close` and `adj_close` differ in their adjustment; it does not establish that `close`
is raw. Certifying `observed_unadjusted` from it would be a category error, and no choice
of threshold, rational bound, dividend register or oracle reaches it.

⚠ Ninth safety-shaped claim of mine inverted on #2840. Caught at checkpoint 1, finding 1.

## Refusal 2 — a step in `f` is equally a defect in `adj_close`, and that is not hypothetical

`f`'s numerator and denominator are not independent observations. AACG, one of the five
series the census reported as a falsified `split_adjusted` label:

| vendor | date | close | adj_close | volume |
| --- | --- | --- | --- | --- |
| PWB | 2018-08-24 | 6.72 | 0.7200 | 135,600 |
| PWB | 2018-08-27 | 2.01 | **2.01** | 10,367,600 |
| Intrader | 2018-08-24 | 6.72 | 6.72 | 135,581 |
| Intrader | 2018-08-27 | 2.01 | 2.01 | 10,367,562 |

The `9.333338` cliff is not a price event. `close` falls 6.72 → 2.01 on 10.4M shares
against 135k — a real crash — and `adj_close` simply **stops being adjusted at that row**
and becomes a copy of `close` for the rest of the series. The statistic fired on a
discontinuity in how the vendor built `adj_close`.

⇒ the "5 falsified `split_adjusted` series" is not a falsification list. At least one
member is a vendor adjustment-vintage seam, and the census cannot tell the two apart.

## Refusal 3 — the discriminator was fitted to the evaluation population

`q` and `p` were tightened **twice** against the residue of the run they were being
evaluated on: `q <= 20` admitted `21/17` (CYD), `74/19` (NHC), `28/3` (AACG); the unbounded
form admitted PBR's and ACI's 2022 special dividends. That is model selection on the
evaluation set, and freezing the constants afterwards does not retire it.

The bounds also have measured false negatives in both directions:

- **Excluded real actions** — a `7:6` split (ratio 1.167) fails the `1.2` threshold; a
  `1:250` reverse split fails `p <= 200`; `13:10` fails `q <= 5`.
- **Admitted non-actions** — a cash distribution is a *step*, not a drift: `$5` against
  `$10` gives exactly `2` and passes both clauses.

## Refusal 4 — two premises in the proposal were false

- **"`adj_close` is 100% populated"** was measured on a January-2015 slice of the two Yahoo
  archives only. The eToro comparator loader inserts NULL `adj_close`
  (`app/services/research_comparator_snapshot.py:493`): **0 of 18,198** eToro bars and
  **0 of 1,467** cboe bars carry one. The claimed reach over "all 30,591 series" is false.
- **"No corporate-action register exists"** rested on `table_name ilike '%split%'`, which
  returns 0 rows. `dividend_events` **does** exist (2,799 rows, with `ex_date`,
  `dps_declared`, `currency`), alongside `dividend_history` and
  `instrument_dividend_summary`. Grep-before-cite failure: the grep was for the wrong word.

## The witness exclusion — withdrawn

The census reported the dangerous direction — an `unadjusted` series spanning a known
re-basing with no cliff — as **3 of 4,284**, then argued to **0** on the grounds that all
three (`AACG` 2018-08-27, `KBH` 1989-03-07, `MDLZ` 2012-10-02) had as their only witness a
series the census had itself falsified.

**That argument is circular and is withdrawn.** The same statistic declares the witness bad
and then uses that declaration to delete the evidence against itself; a mislabelled series
can still observe a real event. The honest figure is **3 undetermined in 4,284**, and
AACG's rows show why: its Intrader twin has `adj_close == close` on every bar, so it is in
the blind class and was never capable of witnessing anything.

## ⚠ The coverage bound, which survives and is worth keeping

`adj_close ≡ close` on **every** bar for **12,241 of 22,880** `unadjusted` series (53.50%)
and **4,531 of 7,711** `split_adjusted` ones (58.76%). On that class `f ≡ 1` by
construction and no `f`-based test can ever fire. Any future construction over these two
columns inherits this bound.

## What the next construction should use instead

Not a recommendation to build — a named lead with its own unverified premises, so the next
session does not re-derive the dead end.

`dividend_events.dps_declared` is an **absolute** quantity: dollars per share, with an
`ex_date` and a `currency`. That is the property every construction so far has lacked. A
declared `dps` against a series' own close on the day before the ex-date yields an implied
yield, and a series rescaled by `c` reports that yield `c`× wrong. **Unlike `f`, it is not
scale-invariant** — which is precisely why it is worth testing.

Premises to falsify FIRST, before any spec:

1. `dividend_events` has only 2,799 rows against 30,591 series — measure the join reach
   before anything else. It may be too small to matter.
2. `currency` must agree with the series' own denomination, and neither side has been
   checked.
3. `dps_declared` provenance is unread; if it is itself derived from adjusted prices the
   construction is circular in the same way `f` is.
4. The blind-class bound above does **not** apply here, which is the main reason the lead
   is worth the next session's time.

## Disposition

**Nothing ships from this round but this document.** No census script: a statistic that
cannot answer the question invites exactly the misreading refusal 2 demonstrates, and
`app/services/strategy_price_basis.py` is deliberately untouched — its
`PRICE_BASIS_RULE_VERSION` hashes its own file bytes, so any edit rotates S-12's identity
and cold-starts its watermark for no gain.

⚠⚠ **Five refused constructions on this line is the signal to question the MODEL, not the
case** (CLAUDE.md). The model every one of them shared: *recover provenance from the stored
price columns*. They keep dying the same way — those columns encode **relative** adjustment,
and provenance is an **absolute** property. The `dividend_events` lead is the first
candidate that does not share it.

Refs #2840, #2437.
