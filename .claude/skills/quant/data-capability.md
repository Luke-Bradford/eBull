# quant/data-capability

## When to use

**Before proposing any strategy, indicator or signal — to find out whether we
can compute it at all.** Read it the moment a design mentions volume profile,
order flow, VWAP, footprint, level 2, implied volatility, borrow cost, analyst
estimates, or "intraday".

It exists because a design built on data we do not receive is a design that
cannot be tested, and the discovery usually happens after the spec is written.

> **The rule: state what the data CAN support before designing what to do with
> it.** ⚠ And correct the record when you get it wrong — this file exists partly
> because I repeatedly claimed "we have no intraday data", which is false.

⚠ Companions: `quant/strategy-evidence.md`, `quant/measurement-discipline.md`,
`quant/trade-lifecycle.md`, `data-sources/market-structure.md`.

---

## 1. What we actually hold

```text
research_price_daily        25,920,971 bars   7,709 series, 1962-2026, daily OHLC + adj_close
price_daily                  6,724,254        the eToro-fed operational series
filing_documents             9,243,776   ⚠ a MANIFEST of URLs -- NO document bodies
ownership_institutions_*        ~7M      13F, quarterly partitions
filing_events                2,764,591
sec_filing_manifest          2,613,520
financial_facts_raw_*           ~2M      XBRL
insider_transactions         1,052,947   filer_cik, txn_date, txn_code, filer_role
finra_regsho_daily_*           539,388   ⚠ short VOLUME, not short interest
ownership_insiders_*           ~300K
def14a_beneficial_holdings     110,832
benchmark/sector series        102,027   16 comparators, SPY 1993-01-29 -> 2024-09-27
quotes                           1,557   ⚠ LATEST only, one row per instrument
price_intraday                       0   ⚠ empty = BUILD gap, NOT a data gap (see below)
```

---

## 2. ⚠⚠ The live feed — what it does and does not carry

**We have a live authenticated WebSocket to `wss://ws.etoro.com/ws` covering all
instruments.** `Trading.Instrument.Rate` pushes are parsed and upserted into
`quotes`, and fanned out to SSE consumers.

⚠⚠ **I claimed repeatedly that "we have no intraday data". That is WRONG and the
operator corrected it more than once.** The accurate statement:

| the feed gives us | the feed does NOT give us |
| --- | --- |
| `bid`, `ask`, `last` | **bid size, ask size** |
| tick-level updates | depth beyond L1 |
| all instruments (on subscription) | trade-side / aggressor classification |
| observed `spread_pct` | reliable traded volume |

### ⚠⚠ And the WebSocket is only HALF the answer — we also have REST intraday HISTORY

The table above was still not the full correction, which is why the error kept
regenerating: it describes the LIVE feed, so a reader who wants *history* concludes
we must record it forward from today. **We do not.**

`EtoroMarketDataProvider.get_intraday_candles` serves intraday OHLCV on demand, per
instrument, right now. Measured 2026-08-09 on AAPL at `count=1000`:

| interval | reach back |
| --- | --- |
| `OneMinute` | ~2 days |
| `FiveMinutes` | ~5 days |
| `ThirtyMinutes` | ~1 month |
| `OneHour` | ~2 months |
| `FourHours` | **~8 months** |

⚠ `count` caps at 1000 and **the URL carries no date anchor** — always the *last* N
bars, so reach is a pure function of interval and going deeper does require polling
forward. ⚠ Volume IS populated. ⚠ Extended hours ARE included, so an RTH-only study
must filter sessions itself.

⚠⚠ **This matters for what we can test, not just what we hold.** Brogaard/Han/Kim's
intraday residual reversal — the strongest recent price-only evidence found in the
#2437 sweep, sample through Dec 2022 — is measured on **30-minute midpoints**, which
is exactly what this endpoint returns for the last month.

⚠ Rate limit binds the corpus, not the capability: **60 GET/min shared** means a full
6,700-instrument pass is ~112 minutes per interval and competes with every other eToro
call. A cross-sectional intraday study is a scheduled harvest, not an ad-hoc query.

**Three distinct capabilities. Collapsing them is the error itself:**

| capability | gives | limit |
| --- | --- | --- |
| REST intraday candles | OHLCV history on demand | 1000 bars, no date anchor, 60 GET/min |
| WS rate stream | live bid/ask/last, all instruments | **no depth, no sizes** — the real gap |
| `research_price_daily` | 25.9M daily bars 1962-2026 | daily granularity |

Source of truth: `.claude/skills/data-sources/etoro-api.md` § "WE HAVE INTRADAY
HISTORY". Probe before asserting:
`curl -s "http://localhost:8000/_debug/etoro-candles-probe?instrument_id=1001&count=1000&interval=FourHours"`

⚠ Every occurrence of "size" in `app/services/etoro_websocket.py` refers to
**WebSocket frame bytes**, not order size. Verified by grep, not assumed.

**Two further limits that matter more than they look:**

1. ⚠⚠ **Subscription is VISIBILITY-DRIVEN.** The subscriber holds no opinion
   about which instruments to stream — SSE page views drive it, and it *"boots
   quiet"*. **So an absence of ticks means "nobody was looking", not "nothing
   happened".** Any future tick store needs a `subscription_coverage` table
   recording when each instrument was actually subscribed, or every gap is
   uninterpretable.
2. **`quotes` is latest-only** — one row per instrument, upserted. Nothing
   accumulates history.

---

## 3. What this makes possible and impossible

| ✅ buildable today | ❌ impossible on this feed, at any effort |
| --- | --- |
| tick history + true intraday candles (needs a writer) | **order-flow imbalance (OFI)** — needs L1 sizes |
| **observed** bid/ask spread over time | footprint charts, volume-at-price |
| quote-update frequency as an activity proxy | cumulative delta, absorption, exhaustion |
| realised intraday volatility | trade-side classification, tape reading |
| Amihud illiquidity (`\|return\| / dollar volume`) on 25.9M bars | true session VWAP |

⚠⚠ **The impossible column is a DATA constraint, not a verdict on whether those
methods work.** Cont/Kukanov/Stoikov show OFI predicts short-horizon returns with
a linear, stable relation. **The method is sound; the field is absent from our
payload.** If intraday is ever a priority, the requirement is **L1 depth
minimum** — that is the specific thing a data purchase must buy.

---

## 4. Known gaps, ranked by what they would unlock

1. **Survivorship-free universe** — ⚠⚠ **the highest-value gap.** Only **2 of
   7,709** series carry a delisting date. #2346 measured free archives serving
   **258/259** of a delisted cohort, so the data is obtainable. It closes a
   standing promotion refusal *and* removes the bias inflating every number.
2. **Options / implied volatility** — the largest missing conditioner. Our
   best-understood effect (Nagel) is vol-conditioned, and we can only build the
   trailing-realised proxy.
3. **Borrow cost / short availability** — decides whether a short-side signal is
   real or merely expensive. RegSHO gives volume, not cost.
4. **Analyst estimates + revisions** — without them, PEAD must use time-series
   surprise, which Livnat & Mendenhall show is materially weaker than
   consensus-based surprise.
5. **Index membership history** — `instrument_universe_membership` shipped
   (#2290) and is **empty by design**. Unlocks index-rebalance forced flow.
6. **L1 depth** — see §3.
7. **Filing document bodies** — `filing_documents` is a manifest. Text-change
   strategies need a fetch-and-store pipeline first.

---

## 5. ⚠ Traps in the data we DO have

- **`close` vs `adj_close`** — the backtester reads `close`, so **the whole engine
  is price-return, not total-return** (#2429). `adj_close` is split **and**
  dividend adjusted and is what returns should use. Both legs are affected, so
  the *relative* comparison is less wrong than either absolute figure.
- **Benchmark coverage ends 2024-09-27** while the corpus runs to 2026-07-08 —
  **~21 months with no market leg**. Every regime series must be **fail-closed on
  absence**, never carried forward (#1817 shipped that bug once: stale closes
  rendering as a computed 0%).
- **`instruments.last_seen_at` means "last CHANGED"**, not last observed.
- **RegSHO is short volume**, not short interest, not days-to-cover.
- **The cost model is size-independent** — a flat per-band half-spread. Fine at
  our size; ⚠ **silent about capacity**, which is the dimension Frazzini et al.
  and Novy-Marx/Velikov both rank strategies on. It also under-penalises the
  illiquid microcaps where real impact is worst.
- **20.3% of corpus bars predate decimalisation** (2001-04-09), when a 1/8 tick
  on a \$10 stock was 1.25%. Comparing a modern spread calibration against the
  full history is an era mismatch.

---

## 6. The cheapest capability upgrades

1. **Persist the tick feed.** `price_intraday` exists and is empty. A writer plus
   `subscription_coverage` builds an intraday corpus **from today forward at zero
   data cost**. ⚠ Note the schema mismatch: `price_intraday` has trade-style
   OHLC**V**, but quote ticks give no reliable volume — bid/ask/mid OHLC plus
   spread OHLC and a tick count is the honest shape.
2. **Record observed spreads.** `quotes` already carries `bid`/`ask`/`spread_pct`.
   Storing them over time **replaces a calibration with a measurement** and
   removes the era confound above.
3. **Amihud on the daily corpus.** No new data at all — a conditioner, a
   per-name cost model, and a priced factor in one.
