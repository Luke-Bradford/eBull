# #2840 — the confirmed re-denomination, and the answer: eToro BACK-ADJUSTS

Refs #2840. Refs #2437. Prior art on this ticket: `91267518` (the question, narrowed and left
open), `627a0736` (`app/services/bar_capture_certificate.py`), `3c6bb73f` (the carrier refusal),
`scripts/probe_2840_intraday_adjustment_basis.py` (the harness whose `is_split_scale` this reuses).

## 1. The question, and what it actually blocked

`627a0736` shipped the capture certificate carrying one precondition it could not satisfy:

> A session open bounds when a corporate action becomes ECONOMICALLY effective. It does not
> bound when the PROVIDER rewrites its own history.

so `PROVIDER_REWRITE_TIMING_VERIFIED = False`, and its comment named the unblock as **"a confirmed
split inside a reachable window"**, via one of `91267518`'s three routes. This discharges that item.

⚠ **Correcting `627a0736`'s own close-out, which overstated which refusal is firing.** That
comment reads as though the provider bucket is what refuses the store today. It is not. Every one
of the 29,234 stored bars predates `sql/402`/`sql/403`, so `capture_certificate` returns
`unverifiable_capture_semantics` at its first branch and never reaches the provider test. The
distinction matters because the two refusals are supplied by different evidence, and reporting the
wrong one points the next session at the wrong work.

**The answer: the provider back-adjusts.** The flag stays `False` — see §7, where the reason
changes and the conclusion does not.

## 2. Source rule

**`ASC 260-10-55-12`** requires retrospective restatement of **per-share** amounts for all periods
presented. **`SEC SAB Topic 4.C`** is the governing interpretation for the **share-count**
disclosure this register actually reads (`CommonStockSharesOutstanding` is a balance-sheet instant,
not an EPS denominator): a change in capital structure effected without additional consideration is
given retroactive effect in the capital-structure presentation. Both are cited because the first
alone does not reach the concept used.

The consequence is the one `docs/specs/ingest/2026-08-03-2231-split-adjustment.md` already turns
on: the same `(instrument, concept, taxonomy, unit, period_start, period_end)` reported at two
`filed_date`s with two different values cannot be issuance — **issuance cannot move a past period**.

⚠⚠ **The converse does NOT hold, and the register does not claim it.** "Splits require
restatement" does not give "a simple-ratio restatement is a split". `ASC 805-40` restates for
reverse acquisitions by the exchange ratio, `SAB Topic 4.D` covers nominal issuances, and #2231
explicitly keeps recapitalisations and filer errors inside the same signal. So this is a register
of **retroactive share-count RE-DENOMINATIONS**, named that way throughout, and §5 states which
direction its imprecision biases.

### Routes not used, and why

⚠ **FINRA's `stockSplitFlag` is not a source rule, because FINRA does not define it.** Our corpus
holds it — `stock_split_flag = 'S'` on **1,697 of 626,885 rows** — but FINRA's published *Equity
Short Interest Data Glossary* (fetched 2026-09-20) has **no entry for the field**. Same narrow form
as `91267518`'s finding about eToro's candle docs: that page specifies no rule, which is weaker
than "no rule exists". It may corroborate; it cannot bound a window.

⚠ **Alpaca corporate actions is unreachable unattended.** `APCA_API_KEY_ID` /
`APCA_API_SECRET_KEY` are absent here and `credential_headers` refuses empty secrets by design
(`app/services/alpaca_delayed_sip_probe.py:51-63`).

⚠ **The skill is wrong and could not be fixed in this PR.**
`.claude/skills/data-sources/finra.md` carries *"`stockSplitFlag`: empty string or `Y`"*. The
stored vocabulary is `''` and `'S'`; **no `'Y'` exists in 626,885 rows**. `.claude/**` is
write-refused from a loop worktree, so the patch text is posted on #2403 per standing procedure.

## 3. ⛔⛔ The register I built FIRST was circular, and the full population killed it

The obvious construction takes a price discontinuity, corroborates it with a share-count
restatement, and calls the pair a confirmed split. I built exactly that. It even carried a
direction test that is correct in isolation — a forward split `k:1` moves price by `1/k` and shares
by `k`, so `price_ratio × share_ratio ≈ 1`. It returned 32 rows over 9 instruments.

**Every one was an artefact.** The price side matched a **single corrupt bar** on 2025-12-09 whose
whole OHLC sits near a tenth of both neighbours:

| symbol | 2025-12-05 close | 2025-12-09 close | 2025-12-10 close |
| --- | ---: | ---: | ---: |
| `PED` | 11.080 | **0.5575** | 11.344 |
| `PW` | 9.901 | **0.9742** | 10.200 |
| `APUS` | 20.600 | **2.0800** | 20.800 |
| `MWG` | 2.500 | **0.2365** | 2.382 |

Not one name: **17 instruments** carry a `>5x` adjacent jump on 2025-12-09, 18 on 2025-12-24 and 18
on 2025-12-26, against 2-4 on an ordinary day. `price_bar_quarantine` already holds 18 rows for
2025-12-09, so the existing guard catches it — **nothing to fix, no ticket**; it is recorded because
it is what falsified the register. ⚠ The tell was in the output and I read past it: **7 of the 9
instruments shared one event date.** A corporate-action register whose members cluster on a single
calendar day is describing the pipeline, not the corporations.

The share side was genuine and **useless as a corroborator**: a restatement brackets its event
between two `filed_date`s, and those brackets run about a **year** wide (one member's is 13 years).
A randomly-placed price date falls inside a year-wide window essentially always, so the conjunction
carried no information.

### ⚠⚠ And the deeper error: the selection excluded the cases it was looking for

Persistence fixes the artefact, and it is in the shipped detector. It does not fix this:

**a price-side filter is circular for this question at any strictness.** If the provider
back-adjusts, a genuine event leaves **no discontinuity in the delivered series at all** — so
requiring one admits only the events the provider did NOT adjust, and the measurement can only ever
return "nominal". Codex named it at checkpoint 1: *"applying that test to potentially adjusted
prices assumes the answer under investigation."* The register must be built from the side that
cannot see the answer, and the price series must be MEASURED, never selected on.

Both halves are in `docs/review-prevention-log.md`.

## 4. The register, as built

From `financial_facts_raw` alone — filings only, blind to price:

- concept `CommonStockSharesOutstanding`, same `(instrument, concept, taxonomy, unit, period_start,
  period_end)` at two `filed_date`s;
- `|ln(new/old)| >= ln(1.2)`. ⚠ **A log distance, because the first cut was `> 1.5 OR < 0.667` —
  which excludes an exact 3:2 (1.5 is not `> 1.5`) while admitting its reciprocal.** `WRB.US`
  survived that asymmetry only because its ratio is 1.5000000019;
- the ratio matches exactly one coprime `p:q`, `p,q <= 30`, within **0.2%**. ⚠ Not
  `is_split_scale`'s 1% default: that is calibrated on price noise, and a share count is a reported
  integer. At 1% and `limit=30` an exact 7:4 has three matches and is rejected as ambiguous;
- restating filing on/after 2025-09-01, instrument tradable;
- collapsed to one entry per `(instrument, ratio)`, keeping the widest bracket.

**1,760 raw restatement pairs → 315 events**, with **673 pairs rejected** as not a simple ratio.
Nothing is sampled. It contains the independently recognisable events as a sanity read: `TPL` 3:1,
`TSCO` 5:1, `WRB.US` 3:2, `TLRY` 1:10, `STEM` 1:20, `VERU` 1:10, and `HON` 1:2.

## 5. The measurement

For an event that multiplied a share count by `s`, a **nominal** price series must step by `1/s` at
the effective date. ⚠ **`1/s`, never "`1/s` or `s`"** — accepting the reciprocal re-admits the
same-direction cases the direction argument exists to exclude, and would score §6's control as a
match when it is precisely not one.

Both levels are **geometric means over 10 sessions**, so a one-bar outlier cannot move the
comparison past the band — that is the 2025-12-09 shape, pinned by a test. The "was there any cliff
at all" bar is **the event's own factor**, not a constant: a flat 1.2x bar put **255 of 285** events
into "a cliff, but not at the expected one", because a sub-dollar small-cap routinely moves its
10-session level 25%. Scaled to the event, `no_cliff` becomes a real negative.

`PYTHONPATH=. uv run python -m scripts.probe_2840_confirmed_split_adjustment`, dev DB, 2026-09-20:

| verdict | count |
| --- | ---: |
| `cliff_at_expected_factor` (nominal delivery) | **0** |
| `cliff_at_other_factor` | 4 |
| `no_cliff` | **129** |
| `insufficient_bars` | 182 |

⚠ **182 of 315 are reported as unmeasurable and are never counted as `no_cliff`** — that is the
direction that would inflate this probe's own conclusion, and most of them come from a
precondition added at Codex checkpoint 2: the series must cover the WHOLE bracket. If history
begins after `old_filed`, the event may sit before the first bar and the flat remainder would read
as a clean negative. A 1000-bar fetch against a 13-year bracket is the concrete case.

Stratified by the factor each event predicts — bands, not a chosen cut, because picking a minimum
ratio would be a threshold with no published basis:

| expected price factor | delivered NOMINALLY |
| --- | ---: |
| `x>=4` or `<=1/4` | **0 of 110** |
| `x2 .. x4` | **0 of 13** |
| `x1.5 .. x2` | **0 of 5** |
| under `x1.5` | **0 of 5** |

**Zero of 133 measurable events left a cliff at their own factor**, at any magnitude.

⇒ **the provider serves a back-adjusted history.**

### The live arm, and why the stored verdicts are the delivered-series verdicts

`--live` refetches `get_daily_candles(count=1000)` per instrument and compares every overlapping
bar. **0 mismatching closes out of 115,073 overlapping bars, across all 315 events, 0 fetch
failures.** So `price_daily` and the delivered series are the same numbers here, and the table
above is a statement about what eToro serves, not about what we happen to have stored.

### ⛔ Two checkpoint-2 defects, both in the direction of my own conclusion

Recorded because the first run reported **3** nominal deliveries and the corrected one reports
**0** — the fix strengthened the result, which is exactly why it had to be found rather than
argued:

1. **Overlapping windows manufacture intermediate factors.** A genuine `x10` step is seen
   partially by the ten candidate windows either side of it, so some window reads `x1.995`.
   Scoring "any window matched" turned a `x10` event into a nominal delivery at expected factor 2.
   All three original matches were this artefact. The verdict now reads the **largest** shift;
   a genuine cliff that is not the largest move in its bracket is withheld from both tallies
   rather than counted as evidence.
2. **Partial bracket coverage**, above.

⚠ **The register's imprecision biases toward this conclusion and the bias is one-directional.** A
member that is an accounting correction rather than a traded re-denomination has no price effect to
adjust, so it lands in `no_cliff` for a reason that has nothing to do with the provider. The
register-wide rate is therefore an **upper bound** on back-adjustment. That is why §6 exists: the
weight rests on an event whose corporate action is documented, with a positive control.

## 6. `HON` — the positive control, and the sharpest single result

`HON` re-denominated **1-for-2 on 2026-06-29**, contingent on the Honeywell Aerospace spin-off the
same day. Form 10-Q for period 2026-06-30, verbatim: *"every two shares of common stock issued and
outstanding … were automatically combined into one share of common stock"*, and *"All share and per
share amounts have been retrospectively adjusted to reflect the Reverse Stock Split for all periods
presented."*

Expected nominal price factor is therefore **x2**. The delivered series steps by **0.4925** — the
detector finds it, on the right date, in the wrong direction. Two events in one session, and the
question is which one survived into the delivered series.

**Aerospace's own value is in the corpus, so this is settled internally rather than by algebra:**

| quantity | value | source |
| --- | ---: | --- |
| `HONA` close, 2026-06-29 | 222.02 | `price_daily` (first bar 2026-06-24, when-issued) |
| `HONA` shares outstanding | 316,952,725 | SEC, filed 2026-08-05 |
| ⇒ Aerospace market value | **~70.4bn** | |
| `HON` close, 2026-06-29 | 228.21 | `price_daily` |
| `HON` shares outstanding, post | 316,900,000 | SEC, period 2026-06-30 |
| ⇒ `HON` post-event value | **~72.3bn** | |
| ⇒ combined pre-event | ~142.7bn over 633,700,000 pre-split shares | SEC, period 2026-03-31 |
| ⇒ **nominal pre-event price** | **~225 / share** | |
| delivered pre-event close, 2026-06-26 | **463.98** | `price_daily`, and live, identical |

463.98 is **~2.06x** the nominal 225. ⇒ the 1-for-2 re-denomination is **already inside the
pre-event bars**, and the surviving step is the spin-off distribution, which the provider did not
adjust.

This is what makes `HON` the control the register needs: **a population of `no_cliff` verdicts with
no demonstrated ability to find a cliff proves nothing.** Here the detector finds one, at the right
session, and correctly refuses to call it the split.

⚠ The spin-off half is `n = 1`. It is **not** generalised into "eToro adjusts splits but not
separations".

## 7. What this means for the certificate

`PROVIDER_REWRITE_TIMING_VERIFIED` **stays `False`**, and the comment above it changes from *"not
yet tested"* to *"tested; back-adjustment is real"*.

⚠⚠ **The measurement does NOT license flipping it, and saying otherwise would be the same
over-reach this ticket keeps catching.** "The provider back-adjusts" is not "the provider re-bases
before the effective session's open" — the second is the only fact that would make the open test
sufficient, and nothing here measures it. Two separate evidence states; one is now filled.

⚠ And there is now a **concrete reason to expect the open not to bound it**: `HON`'s
re-denomination took legal effect at **00:02 ET on 2026-06-29** — after the previous session's
close and before that session's open. Legal effectiveness, first adjusted trading and the
provider's own rewrite are three different clocks; the open test only ever spoke to the second.

**What would move the flag:** a bar CAPTURED between an effective instant and the following open,
compared against the level it traded at. Nothing in the current store can supply that, which is a
forward-collection requirement, not an analysis one.

⚠ Editing that module rotates `CAPTURE_CERTIFICATE_VERSION`, which hashes the file's own bytes.
Free today: `grep -rn "CAPTURE_CERTIFICATE_VERSION\|capture_certificate\b" app/ scripts/ tests/`
returns only `scripts/census_2840_forward_daily_provenance.py`, and no stored row carries it —
checked before the edit, not assumed.

## 8. Not claimed

- **Not** that the provider REWROTE anything. An always-adjusted upstream and a historical rewrite
  are indistinguishable from one delivered snapshot; what is measured is today's representation.
- **Not** that every `no_cliff` member was a traded re-denomination (§5's bias note).
- **Not** anything about INTRADAY. `91267518`'s refusal stands — shared routing is not shared
  adjustment — and `ThirtyMinutes` reaches ~1 month, so no register member is inside its span.
  `FourHours` is the only intraday interval that reaches one.
- **Not** that the 2025-12-09 artefact is unhandled. It is quarantined.
- **Not** a claim about spin-offs (§6, `n = 1`).

## 9. Next

1. The **carrier** — shape settled by `3c6bb73f` §3.1: a per-bar certificate on the uniform
   `PerSeriesSignals` call, a parallel structure, never a mask.
2. The `AS_TRADED_UNIVERSES` / `SCAN_UNIVERSE` overloading, which refuses a composed series before
   any basis is read.
3. The flag's remaining evidence is **forward-collection**: capture a bar between an effective
   instant and the next open. Worth stating as a collection requirement on the intraday harvester
   rather than leaving it as an open analysis question nobody can close.
