# #3619 slice 2 — monthly total-return reader with a splice contract

Status: slice 2 of #3619, revised after Codex checkpoint 1. Slice 1 (#3636, `bdfb5fc3`)
measured the inputs; this fixes how they are combined. Code:
`app/services/total_return_reader.py`. ETFs past 2024-08 (N-PORT Item B.5.a in the DB) are
slice 2b; the PWB refresh to 2026-08 is slice 3.

## Consumer, population and unit

#3609 (monthly factor panel) and anything else needing monthly total return on the
backtest stock population. The reader takes the `UniverseSelection` returned by
`load_universe_selection(conn, universe="survivorship_free", validated_ids=...)` and consumes
each `AdmittedSeries.name_key` verbatim. It never changes membership.

That population is inherited, with its biases: alive `icyDenev/Intrader` series admitted only
when linked to a currently validated US-equity stock instrument, plus every terminating series
whether linked or not (`name_key = -series_id` unless linked to a validated instrument).
Unlinked-alive series are excluded and counted by the selection. "Survivorship-free" below means
the dead names are present, not that the population is a point-in-time investable universe. ETFs
are not in it.

## Source rule

- **Conventional, cited:** chain-linked period-end simple returns, `level(m)/level(m−1) − 1`,
  over consecutive calendar months, no row for a month without a computable return.
- **Vendor bases (documented):** Intrader `close` is raw and `adj_close` carries splits and
  dividends (`research_corpus_ingest.INTRADER_ARCHIVE`); PWB `close` is split-adjusted and
  `adj_close` adds dividends (sql/251; prevention log "A source rule verified on one vendor is not
  a source rule for another").
- **Bar validity (documented):** the #2261 quarantine with `factor_validation`'s semantics. A bar
  outside its series' coverage row at the current rule-set version is unusable (one row per
  series, sql/251 PK). Inside it, a bar with no `research_bar_quarantine` row is clean (the
  verdict table is sparse) and one with a row is usable only if `return_usable`. The reader adds
  no outlier rule.
- **Archive-specific, fixed by construction here:** the switch month, the identity gate, the
  fallback and capture handling. No source rule exists for splicing two vendor archives; these
  are frozen by the module's source hash (the `universe_selection` idiom).

## Contract (`total-return-splice-v1`)

1. **Level.** Per series and calendar month, the last usable bar with finite, positive `close`
   and `adj_close`. Its date is carried: a return runs `(start_bar, end_bar]`, and `end_bar` is
   earlier than the calendar month-end when later bars are missing or quarantined. No trading
   calendar is imposed; the row says which bars it used.
2. **Return.** Between two month-ends of the same series only, so a return never spans vendors.
   Each vendor back-adjusts to its own capture date and the common factor cancels in the ratio.
   A month with no level removes that month's and the next month's return (absent rows).
3. **Intrader supplies months up to 2024-08 for every series.** Its capture month (2024-09, capture
   2024-09-27) is partial for alive series, and from 2024-09 the panel can no longer see names
   that die. A terminating series' final month is kept when it is 2024-08 or earlier, as an
   observed return to its last usable bar. It is not a realised termination: the consumer composes
   `series_termination` once, from the selection's evidence.
4. **PWB supplies months up to 2026-06.** Its capture month (2026-07, capture 2026-07-08) is
   partial. A PWB series ending before capture keeps its final month as an observed return; PWB
   carries no termination evidence, and every month it supplies after 2024-08 is survivor-only
   anyway. Both capture dates are declared constants asserted against `max(last_bar)`. These are
   endpoint checks: prices, links and verdicts can move beneath them, so the panel's version
   string also carries the quarantine and selection rule versions.
5. **Verdict per name, in precedence order:**
   1. `terminating`: the selection marks the Intrader series terminating. It never continues on
      PWB. The archive endpoint does not prove the security died, but a PWB series still trading
      at 2026 under the same instrument has unverified continuity with it, so the conservative
      policy stops where Intrader stops.
   2. `no_pwb_series`: no PWB series for the instrument. `uq_research_price_series_vendor_instrument`
      (sql/249) allows at most one.
   3. `refused_insufficient_overlap`: fewer than 12 paired months in the identity window.
   4. `refused_price_disagreement`: median at or above 5bp.
   5. `spliced`.
6. **Identity gate.** This is a cross-archive consistency check, not corroboration: both archives
   are Yahoo derivatives, so agreement shows they describe the same security, not that either is
   right.
   - Statistic: the median of |Intrader close return − PWB close return|.
   - Window: 2022-01 to 2024-08, the only months where the verdict changes which vendor is used.
     Earlier months come from Intrader whatever the verdict.
   - Close returns, not `adj_close`: Intrader's missing dividends are the defect being fixed.
     An `adj_close` gate over these months would refuse exactly the monthly payers.
   - A month is compared only when both returns run between the same bar dates.
   - A month is skipped when an Intrader split stamp falls inside its return interval. Every
     stamp counts, quarantined bar or not. The skip applies only to this statistic.
   - Pass iff at least 12 paired months and median < 0.0005.
7. **Vendor per month, spliced names.** Intrader before 2022-01. From 2022-01 PWB, and where PWB
   has no return for a month up to 2024-08 the Intrader return is used. After 2024-08, PWB only.
8. **Every other verdict:** Intrader only, through 2024-08.
9. **Row flags.**
   - `dividend_capture_degraded` is true exactly when the row is Intrader-sourced and dated
     2022-01 or later. It marks source risk, not proof that the month missed a payment.
   - `survivor_only` is true exactly when the month is after 2024-08, whatever the source.
10. **Output.**
    - Rows `(name_key, month, total_return, start_bar, end_bar, vendor, series_id,
      dividend_capture_degraded, survivor_only)`, at most one per `(name_key, month)`.
    - Per name: the verdict, the identity check (paired months and median), and the PWB series
      considered.
    - The composite version string.

### Retrospective, not point-in-time

The panel is a retrospective reconstruction. The gate uses 2022–2024 overlap to choose the
vendor for those same months. That choice changes only which archive's measurement of an
already-realised return is used, never membership or any signal input, and the gate reads price
paths, not the dividend treatment it selects. It is not a record of what a researcher in 2022
could have known. Treat it as data cleaning, not as point-in-time availability.

## Why these thresholds

Census on the dev DB, 2026-10-04, over 17,266 admitted names (reproduce:
`PYTHONPATH=. uv run python -m scripts.report_3619_splice_census`):

| verdict | names |
|---|---:|
| terminating | 12,636 |
| no_pwb_series | 206 |
| refused_insufficient_overlap | 211 |
| refused_price_disagreement | 62 |
| spliced | 4,151 |

- **Switch at 2022-01.** Slice 1 compared XBRL `dps_declared` quarters against each vendor's
  implied payments, on Intrader series linked to an instrument and reporting in USD, with PWB on
  the instruments both carry. Intrader's miss rate is 1.2% in 2021 against PWB's 1.6%. In 2022 it
  is 2.7% against 1.5%, and 8.7–10.2% after. Annual rates do not locate the change within 2022.
  The month is an operational boundary at the year the rates cross, not a measured break.
- **5bp is a policy threshold, not a natural gap.** On the 2022–2024 window the tail above 5bp is
  continuous. Passing names at other thresholds: 4,134 (2.5bp), 4,151 (5bp), 4,164 (10bp),
  4,174 (25bp), so the threshold moves fewer than 25 names either way.
- **Refused names.**
  - The five largest medians (`PARA`, `STRR`, `B`, `GOLD`, `BNY`, 3.5%–16% a month) are
    different securities under one instrument link. For example, PWB carries the `PARA`
    instrument under the symbol `BNZI`. The `B` instrument is Barrick Mining Corp, while its
    Intrader series runs under `B` from 1984.
  - The rest are concentrated in sub-dollar prices. On the 57 refused names with a median from
    5bp to 3pp, Intrader month-end closes from 2022-01 to 2024-08 have a median of $0.62, and 76%
    are under $1. On the other alive linked names the figures are $17.30 and 4.7%. That is
    consistent with the two archives quoting low prices at different precision; the mechanism is
    not measured.
  - Refusing a name costs it the dividend repair for 2022-01 to 2024-08 and every PWB month after
    2024-08.
- **12 months** is a floor fixed by construction so that a few coincident months cannot pass. It
  is not tuned. The window holds at most 32 months.

## Known limits (recorded, not fixed here)

- **Measurement quality depends on survival.** Only names alive at Intrader's capture and present
  in PWB get the dividend repair. Of 2022–2024 rows, every row of a terminating name is
  `dividend_capture_degraded`: 79,318 rows, against 143 among spliced names. A 2022–2024 backtest
  holding names that later died understates their return by any dividends Intrader missed.
  Consumers stratify on the flag.
- **A refusal does not repair a wrong link.** Some Intrader series appear linked to an instrument
  that is a different security (for example `B`, above).
  The gate refuses splicing them, but their pre-2022 history still carries the positive
  `name_key`. A consumer joining present-day instrument attributes (fundamentals, sector) to a
  `refused_price_disagreement` name must treat the join as unverified. The link itself is a
  `research_price_series.instrument_id` defect upstream of this reader.
- **After 2024-08**, a PWB series cannot be checked against anything. A reuse after Intrader's
  capture is undetectable here. Yahoo-derived archives key history by security, not ticker, so a
  reuse inside the window shows as whole-window disagreement, as the refused tail does, rather than
  as a segment.
- PWB-only names (validated instruments Intrader lacks) are outside the selection and therefore
  outside this reader.
- ETFs past 2024-08 need slice 2b; commodity pools have no N-PORT return.
