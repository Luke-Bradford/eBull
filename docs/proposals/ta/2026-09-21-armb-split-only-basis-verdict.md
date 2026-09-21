# ARM B signal basis — verdict on the three candidates (#2834)

Status: **verdict, not a spec.** It settles which basis `BACKTEST_UNIVERSE = "survivorship_free"`
should score on and why, and it states plainly what the available evidence cannot establish. The
corpus change that implements it is named in §7 and is not written here.

Reproduce every figure with:

```
PYTHONPATH=. uv run python -m scripts.measure_2834_armb_split_only_basis
```

## 1. What was open

`008da625` (PR #3278) measured that s2's momentum selection is basis-sensitive on this corpus —
**15.00%** of the top decile changes between `close` and `adj_close` — because `close` is the RAW
traded level on `icyDenev/Intrader` while the `sql/251` source rule licensing it was verified on
`paperswithbacktest/Stocks-Daily-Price`. Three candidates were named, with no recommendation:

1. derive a split-only series from corporate-action evidence (⚠ not from `close`/`adj_close`
   endpoint ratios);
2. pin the survivor-only vendor, whose `close` genuinely is split-adjusted, and accept the
   survivorship cost;
3. record that §4's price-return rule cannot be honoured here and change the rule.

## 2. Source rule — the evidence candidate 1 needs already ships with the archive

An Intrader daily CSV is headerless, so the column order is the only contract, and the archive
ships the code that reads it:

- `IntraderFramework/src/DataLoader.cpp:250-255` pushes CSV fields `1..N` in order into one
  `prices` vector per bar, dropping only field 0 (the date).
- `IntraderFramework/include/TickerData.h` indexes that vector:
  `enum class BaseIndicators { OPEN=0, HIGH=1, LOW=2, CLOSE=3, VOLUME=4, SPLIT=5, DIVIDEND=6, ADJ_CLOSE=7 }`.
- `IntraderEngine::SplitCheck()` fixes the semantics: it fires when `GetSplit() != 1.0` and
  multiplies the held share count by that value, so the ratio is new-shares-per-old, stamped on the
  bar where the split takes effect. `DividendCheck()` pays `dividend * shares`, so field 7 is an
  amount per share.

So **CSV field 6 is a split ratio and field 7 a cash dividend**. `research_corpus_ingest.py:507`
(`_INTRADER_COLUMNS`) already names both and stores neither — that naming came from #2398's
shared-bar check, which pinned the date/OHLCV/adjclose positions and never touched the two middle
columns.

⚠ This establishes the fields' **intended semantics**, which is what the reader code can prove. It
does not prove the frozen CSVs were written by the reader version in the mirror.

Census over the settled `survivorship_free` admission (17,285 series; vendor pin + capture bound +
alive-at-capture cut + test-issue exclusion, not a bare vendor filter): **8,073 stamped events on
3,582 series**, 0 missing mirror files, 286,446 bars carrying a dividend. Ratios cluster on 2
(2,505), 1.5 (1,256), 0.1 (570), 0.2 (329), 3 (243) — corporate-action shaped, not noise.

⚠ **Stamp-conditioned.** The census finds bars where the field is not 1. It cannot find a split the
vendor never stamped, so "8,073 split events" means 8,073 *stamped* events and says nothing about
completeness.

## 3. Is the field right? Three checks, and none of them is independent

**B1 — internal.** `adj_close` carries both adjustments, so across a split date the cumulative
adjustment `close / adj_close` should step by exactly the stamped factor. AAPL 2020-08-31:
`(500.04/122.169) / (129.04/126.108) = 4.0006` against a stamped 4. Over 8,042 events with a usable
bar pair: **99.14%**.

⚠⚠ **That figure is near-worthless and the run proves it.** `adj_close` is produced by the same
pipeline as the stamp, so agreeing with it is circular. Of the 383 events the second processing
disagrees with, **364 pass B1**. The worked case is COO 2024-02-20: `372.01 → 95.70` is a 4:1
against a stamped **16**, and `adj_close` was back-adjusted by 16 as well (`23.25 → 95.70`), so B1
sees a perfect match on a factor wrong by 4×. B1 also cannot see a split missing from both columns,
a same-day dividend contaminating the step, or the 31 events with no usable pair.

**B1b — does the vendor's own close step by the factor it stamps?** Reaches every event and is weak
by construction: one series cannot separate the stamped factor from the day's own return.

| band | all 8,042 | reference-served | reference-absent |
| ---: | ---: | ---: | ---: |
| 1% | 24.56% | 27.05% | 21.83% |
| 5% | 68.13% | 74.47% | 61.17% |
| 10% | 82.24% | 87.02% | 77.00% |
| 20% | 91.11% | 93.68% | 88.29% |

The low rates at tight bands are mostly real returns, not bad stamps — but that reading is an
inference, not a measurement, and the served/absent gap is confounded by era, factor mix, venue and
volatility. Nothing downstream should read either column as a defect rate. **What it does settle:
a single-vendor corroboration gate is not available**, because it would reject a third of stamps at
a 5% band with no way to say which third.

**B2 — the second processing.** `paperswithbacktest/Stocks-Daily-Price` stores split-adjusted OHLC,
so the comparison asks whether the stamp reproduces the adjustment another processor applied to the
same prices. Compare the one-bar gross return across the event date, which cancels both the day's
market move and any post-capture split:

| tolerance | corrected agrees | uncorrected agrees | both (non-discriminating) |
| ---: | ---: | ---: | ---: |
| 1% | **90.90%** | 5.89% | 29 |
| 2% | 92.37% | 6.23% | 46 |
| 5% | 94.79% | 9.41% | 224 |
| 10% | 96.08% | 11.03% | 322 |

⚠⚠ **B2 is not independent either, and our own code says so.** `research_corpus_ingest.py`'s
provenance record for this vendor reads: *"A Yahoo redistribution like the HF archive, so the two
are ONE observation and agreement between them is circular, never corroborating."* So B2 tests the
stamp against a **different processing of the same observation** — it can catch a stamp that
disagrees with how Yahoo's own data was adjusted elsewhere, and it cannot confirm that a split
happened. **A split Yahoo never recorded is invisible to every check in this file, and no check
here reaches a primary corporate-action source.**

The 383 disagreements at the 1% band decompose — a raw count would charge the other processing's
defects to the field under test:

| class | n | reading |
| --- | ---: | --- |
| the reference carries the raw step too | 32 | **its close is not split-adjusted there** — CIVB 1996-05-09 prints `81 → 20.25` on Intrader and `20.25 → 5.0625` on the reference, both raw. sql/251 established that vendor's basis on one bar; it does not hold everywhere. |
| stamped factor absent from the price series | 188 | applying it injects a jump that is not in either series |
| stamped factor of the wrong magnitude | 163 | COO's 16-for-4 shape |

⚠ Read these three as **evidence-shaped buckets, not causes**. Each is a ratio test at one
tolerance: "absent" means the implied step is within 1% of 1, not that no split occurred; "wrong
magnitude" is the residual bucket and can contain date offsets, bad reference bars or symbol
collisions. The 188/163 boundary in particular is tolerance-sensitive near an implied step of 1
(BAX 1981-11-04 reads implied 1.0167 against a stamped 2 and lands in "magnitude" when it is
plainly "absent plus a 1.7% market move").

⚠ **The B2 join is `vendor_symbol` equality with no identity guard**, so ticker reuse, successor
entities and share-class differences can enter as apparent factor errors.

**The load-bearing figure is the sum: 351 of 4,207 (8.34%) stamps are uncorroborated at 1%** — and
that is a discrepancy rate on the *overlap subset*, conditioned on that tolerance, against a
non-independent processing. It is not a corpus defect rate and must not be quoted as one. Only 32
of the 351 look like a date-convention offset (the described step appears within ±3 bars), itself
an upper bound because a real move of the same size registers as a match.

## 4. What no check here reaches

**3,835 of 8,073 events (47.5%) have no reference bar pair.** Measured composition rather than
assumed:

- **3,709** — the symbol is not served by the reference vendor at all;
- **126** — served, but no bar on one of the two dates;
- **716 of 3,835 (18.7%)** sit on a series carrying delisting evidence.

⚠ So "the unchecked half" is **not** the same thing as "the delisted half" — an earlier draft of
this document used the two interchangeably and the census refutes it. Coverage is the dominant
reason, delisting a minority one. What survives: a large fraction of stamped events has no second
processing to compare against, and the corpus's survivorship-free names are over-represented there.

## 5. The verdict

**Candidate 1, and the claim is narrower than "the field is correct".** What the evidence supports:

> Field 6 carries real corporate-action information — applying it raises agreement with a second
> processing of the same prices from 5.89% to 90.90% — its semantics are fixed by the archive's own
> reader, and it is the only one of the three candidates that keeps both the survivorship-free
> universe and §4's price-return rule. Its correctness against a primary source is untested, and
> 8.34% of the checkable stamps disagree with the only comparison available.

Rejections, both on grounds independent of that evidence:

- **Candidate 2 — pin the survivor-only vendor.** `research-price-corpus.md` states that
  survivorship is *fatal* for cross-sectional momentum, which is exactly what ARM B is. That cited
  rule is the whole rejection; the supporting observations (that vendor's own 32 unadjusted events,
  the coverage gap) are secondary and weaker.
- **Candidate 3 — rank on total return.** §4: *"changing the ranking input itself would be a new
  strategy trial, not an accounting correction"*. ⚠ Two corrections to how this was argued in the
  first draft. (a) The draft rejected candidate 3 for "understating high-yield names over an
  11-month lookback" — **the direction is inverted**. s2's own docstring says the PRICE return does
  that; a total return does not. The property belongs to the status quo and to candidate 1, and as
  an argument it favours candidate 3. (b) "Change the rule" is broader than "rank on total return";
  only that reading is rejected here, and any other revision of §4 is untouched and unexamined.
- ⚠ **Candidate 1 is NOT exempt from the rule that rejects candidate 3.** The first draft called it
  "an accounting correction, not a new trial" while using that same distinction against candidate 3.
  That is inconsistent: candidate 1 changes the ranking input too (`close` → `close / scale`), and
  §4 rule 11 makes identity `code + config + data contract`, so **it mints a new strategy id with a
  fresh out-of-sample requirement and no inheritance of the prior track record.** The honest
  difference is narrower and still decisive: candidate 1 changes the data so it implements the
  registered hypothesis, candidate 3 changes the hypothesis.

**Sizing, and it is not a clean improvement.** Per stamped event on the checkable subset: 90.90%
move into agreement, ~4.5% (the "absent" class) get a factor applied where neither series steps and
are made worse, ~3.9% carry a different error than before, and 5.89% already agreed uncorrected —
so "the status quo mis-scales all of them" (first draft) is false. ⚠ These are **event counts,
equally weighted**. A stamp's damage depends on its factor, on how much history it rescales and on
how often that name reaches a formation date, none of which this measures.

**Materiality (Phase C).** Scored on the naïvely stamped split-only series instead of raw `close`,
**14.30%** of the top decile changes (17,385 of 121,601 slots, 312 formations, every decade
9.83–20.83%, worst 1998-10-01 at 37.27%). Three caveats, all load-bearing:

- it applies **every** stamp, including the 351 uncorroborated and the 69 B1 failures, so it sizes
  the naïve construction and not the adjudicated one §7 proposes;
- it is not exact s2 — the fidelity limits carried from #3278 (weekday-filtered lags, no segment
  resets, `numeric` arithmetic, unbounded reads, `MIN_CLOSE` on the raw close) all still apply;
- ⚠ **14.30% and 15.00% are separate sensitivities, not components.** One is raw-vs-split-stamped,
  the other raw-vs-total-return, each arm conditioned on its own usable support, and rank
  displacement is non-linear. Their difference says nothing about how much of the 15.00% is splits.
  A three-arm shared panel would be needed for that and was not run.

**Displacement is not correctness.** 14.30% establishes that the choice matters. It cannot show the
corrected series is the right one.

## 6. Prevention

`docs/review-prevention-log.md` — *"a derived column cannot validate the stamp it was derived
from"*, with the COO case and the measured 364-of-383 pass rate.

## 7. What the follow-on corpus change must decide — NOT specced here

1. **The correction policy for disputed stamps is the open question**, and declaring a defect rate
   is not one. Apply, suppress, re-date or quarantine? The four classes in §3 want different
   answers, and 47.5% of events have no adjudicator at all.
2. Store fields 6 and 7 at ingest, re-load the vendor, and derive the split-only series as
   `close / scale` where `scale(d)` is the product of the factors of every event strictly after `d`.
3. Decide what `MIN_CLOSE` reads on a back-adjusted basis. ⚠ Its own comment
   (`s2_cross_sectional_momentum.py:114-121`) carries the **third** instance of the same
   cross-vendor error, and this one inverts the floor's stated behaviour: *"sql/251: the corpus's
   OHLC carry the split adjustment, so a name that traded at $0.20 and later did a 1-for-10 reverse
   split appears at $2.00 in these bars and passes a floor it would have failed at the time"*. On
   Intrader the OHLC are raw, so that floor is **nominal** and the deviation it describes does not
   occur — while a back-adjusted basis would create it. The same comment says *"unadjusting would
   need per-series split factors the corpus does not store"*; §2 above is the finding that the
   vendor ships them and the ingest drops them.
   ⚠ Not corrected in the module, deliberately: `_source_hash()` hashes the whole file, so a
   docstring edit there is an identity rotation (`docs/review-prevention-log.md`, #2840).
4. Test completeness against a source outside the Yahoo lineage — SEC filings are the obvious
   candidate and nothing in this repo has looked.
5. Mint a new strategy id under §4 rule 11; do not carry s2's track record across.
6. Full-population A/B and Definition-of-Done clauses 8–12 apply — it is a corpus change.

⚠ **It does not unblock ARM B's prototype on its own.** That design remains withdrawn (60 ckpt-1
findings, 7 of which change it, carried in §7 of
`docs/proposals/ta/2026-09-21-armb-signal-basis-blocker.md`).

## 8. Correction to §4 rule 10

`strategy-catalogue-and-backtest-validity.md` §4 rule 10 states that *"the research corpus supplies
separate split-adjusted OHLC and split-and-dividend-adjusted `adj_close`"*. That is true of
`paperswithbacktest/Stocks-Daily-Price` — and not uniformly even there, per §3's 32 events — and
**false of `icyDenev/Intrader`**, whose OHLC carry neither adjustment. Same cross-vendor defect
#3278 found in s2's docstring, one level up in the spec that licenses it. Corrected in place.
