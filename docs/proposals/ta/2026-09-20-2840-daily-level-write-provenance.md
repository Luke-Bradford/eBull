# #2840 — the daily-path producer is WITHDRAWN, and the carrier stays next

**Status:** findings. The producer this document originally specced was refused at Codex
checkpoint 1 and the refusal holds. What landed instead is two defects in the **already
merged** certifier, both found by attacking the spec rather than the code.

**Inherited head**, per `308d1e38`'s close-out and §6 item 2 of
`2026-09-20-2840-per-series-price-basis-carrier.md`: *the carrier*. I drafted a case for
displacing it with a daily-path evidence producer. **That case failed.** The head is
unchanged.

## 1. What I proposed, and why it is wrong

`price_daily` has 27 columns and not one is a timestamp (measured,
`information_schema.columns`), so the live scan path —
`price_masked_bars.load_masked_bars`, one of only two `BarSeries` origination sites in
`app/` — has no per-bar provenance evidence. I proposed adding `level_written_at`, stamped
on the upsert, plus a daily arm on `bar_capture_certificate`.

Three findings killed it. Each was verified in source before being accepted.

### 1.1 ⛔ A write timestamp cannot establish COMPLETION

The proposed rule treated "stamp after the session close" as "the bar was complete when
observed". It is not: **fetch at 15:59, write at 16:01**. `clock_timestamp()` at write is an
upper bound on the *observation instant*, which is what makes it sound for the next-open
exclusion — and an upper bound on observation says nothing about whether the payload was
final. The certificate would have admitted a partial bar as a completed one.

⚠ That is the **fail-open** direction, on a rule whose entire purpose is to fail closed —
the same defect class `sql/402` was written to remove, re-entering through a different door.

### 1.2 ⛔ The motivating measurement was a frequency fact dressed as a write fact

I wrote *"a daily bar is typically first written partial, mid-session"* and cited
`daily_candle_refresh` firing 8–30 times a day across 00:00Z–23:45Z. **Job fire counts are a
fact about the scheduler, not about what the writer does per instrument.**

The writer contradicts it (`market_data.py:748`, #2572):

```python
if fresh_through is not None:
    bars = [bar for bar in bars if bar.price_date <= fresh_through]
```

and the scheduler passes `fresh_through=latest_completed_us_session(datetime.now(UTC))`.
**Forming candles are never published to `price_daily`.** So the "last write, not first"
motivation — the whole of the original §3.1 — rests on a claim the code refutes.

### 1.3 ⛔ A moving stamp destroys evidence rather than creating it

`price_daily` keeps no prior value (`sql/387`'s own header). A stamp that moves on revision
therefore describes *today's* version and erases the timing of the version a past decision
consumed — so neither a first-write nor a last-write stamp reconstructs what a stored
verdict read. That needs immutable observation versions, which is a different and much
larger design than one column.

### 1.4 ⚠ And I over-read `sql/387`

The original draft said the daily path has *"no provenance evidence at all"*. Too strong.
`sql/387`'s header kills **one specific join** (`strategy_signals ⋈ price_daily_revision`,
*"neither an upper nor a lower bound"*, five overcounts and three undercounts) for
**locating affected ledger rows** — it does not make revision rows worthless as per-bar
evidence. `sql/388` also added `price_daily_backdated_insert` and the
`price_daily_corpus_mutation` view, which record part of the class 387's header named as
missing.

The defensible claim is narrower: **no *sufficient* certificate exists for a daily bar**,
and the correct statement of the three candidates is:

| candidate | status |
| --- | --- |
| `price_daily_revision` / `_backdated_insert` | real per-bar evidence, but not a sufficient certificate — no old/new payload, transaction-start timestamps, and the classes it records are partial |
| `strategy_signals.fill_price == price_daily.open` | **post-hoc, not admission.** Measured at **221 of 59,069 (0.374%) across 27 instruments**, every disagreement an exact corporate-action ratio — but it needs a verdict to already exist, so it answers "did the bar I decided on move?", never "may I decide on this bar?" |
| `research_price_series.adjustment_basis` | unchanged from §2 of the carrier proposal: pinned archives only, four members, and a label does not validate a payload |

### 1.5 ⚠ The ordering argument did not hold either

I argued the carrier was pointless to build because `PROVIDER_REWRITE_TIMING_VERIFIED =
False` downgrades every certifying verdict to a refusal. The flag behaviour is real and is
already pinned
(`test_a_clean_post_cutover_bar_is_refused_on_provider_grounds`), but it does not establish
the ordering: **universal refusal is not an argument against building the thing that
delivers and enforces refusals.** A carrier still binds evidence to delivered values,
records the reason, and exercises the identity and segmentation contracts. Collection
urgency could have justified reordering; a short-circuit cannot.

⇒ **The carrier remains next**, on the shape §3.1 of the carrier proposal already settled.

## 2. What DID land — two defects in the merged certifier

### 2.1 An inverted sentence, shipped in `308d1e38`

`bar_capture_certificate.py` read:

> "The provider back-adjusts" does not give "the provider re-bases BEFORE the effective
> session's open", **which is the only thing that would make the open test sufficient**.

Backwards. An early re-base *defeats* the open test — it is the failure mode, not the
licence. What makes the test sufficient is the **negative**: that no re-base precedes the
open.

⚠ It survived a full ckpt-1 / ckpt-2 / review-bot round because the paragraph's **verdict**
is correct — the flag stays `False` either way — so nothing downstream ever contradicted the
middle clause. The file already disagreed with itself in two places a reader had to notice
separately: the constant's own name (`..._TIMING_VERIFIED`) and the unblock stated four
paragraphs below.

### 2.2 The NYSE calendar was an undeclared precondition

`next_session_open_utc` reads `us_market_status`; `_SESSION_OPEN` is 09:30
America/New_York. Neither `capture_certificate` nor `nominality_bucket` took any instrument
identity, so the rule applied a NYSE calendar to whatever bar it was handed **and said
nothing about having done so**.

**Not a live defect, and the measurement is why rather than the reassurance.**
`strategy_intraday_bars` holds 8 instruments, all on exchanges 4/5 (`AAPL CENN F IWM JPM KO
QQQ SPY`) — every stored bar really is NYSE-calendar. But `instruments` carries **873 rows
on exchange 7 (`.L`), 520 on 9 (`.PA`), 428 on 31 (`.ASX`)**, and #2834's tilt candidates
are all `.L`. A foreign member joining the harvested panel is a configuration change, not a
rewrite — and the failure would be silent and fail-open: an LSE bar whose 16:30 London close
sits before 09:30 New York gets certified against a session that is not its own.

**Fixed** by a required `session_profile` argument with no default, refused as
`non_nyse_trading_calendar`, checked **before** the semantics gate so a foreign bar's verdict
does not depend on whether `sql/403` happens to be applied.

- The vocabulary is **not invented here**: `app/api/instruments.py::SessionProfile` /
  `_SESSION_PROFILE_SQL` is the repo's source for resolving a listing to its session shape,
  including the non-obvious half (exchange 33 is an RTH-only duplicate whose `asset_class` is
  still `us_equity`, so the exchange test must precede the asset-class test).
- The **derivation stays at its home** and the profile is passed in — a service importing
  from `app.api` inverts the layering, and a second copy of the `CASE` would be the "two
  texts" shape the prevention log warns about. `test_the_accepted_set_has_not_drifted_from_
  its_producer` pins the two together.

### 2.3 Census: both joins are `LEFT`, deliberately

The census's scan feeds counts, so an inner join would silently shrink the denominator — a
bar with no `instruments` or `exchanges` row would vanish from `scanned` rather than be
reported. Measured today: **zero** orphans on both, which is exactly why the inner form would
have looked correct indefinitely. With `LEFT`, a missing row falls through to `continuous`
and is refused — a visible bucket instead of an absence.

## 3. Verification

`PYTHONPATH=. uv run python -m scripts.census_2840_forward_daily_provenance
--capture-certificate`, dev DB, exit 0:

```
over 29,234 stored bars
  capture semantics trustworthy from: 2026-09-20 15:54:30.302942+00:00
  session profiles scanned: us_equity=29,234
```

Every other figure is **unchanged** — 29,234 bars, 1,067 proxy disagreements, 0 in the
reverse direction. The change is verdict-neutral on the current population, which is the
expected result when the precondition it declares already held.

## 4. Not claimed

- That a daily-path certifier is impossible — only that a single write timestamp is not one.
  The route that survives §1.3 is immutable observation versions, unspecced.
- That `PROVIDER_REWRITE_TIMING_VERIFIED` moves. It does not; its evidence is still forward
  collection, and no forward corporate-action calendar exists in the store (`capital_events`
  is a capital-pot ledger, 0 rows).
- That the NYSE precondition ever produced a wrong verdict. Zero bars were affected.

Refs #2840, #2437, #2414.
