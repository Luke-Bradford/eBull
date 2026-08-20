# Bounded eToro intraday harvest: implementation and first evidence

Date: 2026-08-10  
Issues: #2477, #2508

## Outcome

The existing partitioned `strategy_intraday_bars` store now has a scheduled and
operator-triggerable producer. The producer reads one immutable active research
universe, fetches at most twelve eToro windows per five-minute run, retains only
completed NYSE regular-session bars, filters overlap at the durable watermark,
and records missing interval ranges without imputation. Gap rows expire on the
same tier horizon as the bars they describe.

Automatic requests are gated to 09:35 ET through ten minutes after the regular
or early close. This prevents an every-five-minute schedule consuming provider
budget when no new RTH bar can exist. The operator-triggered path deliberately
bypasses the scheduling prerequisite so it can repair gaps out of hours.

This is evidence collection, not a promoted strategy. Demo allocation remains
fail-closed.

## Active prospective panel

`ETORO-RTH-V2` declares 18 symbol/timeframe windows across eight instruments:

- ETFs: SPY (NYSE), QQQ (Nasdaq), IWM (NYSE);
- Nasdaq stocks: AAPL (liquid/high nominal price), CENN (sub-$5/low volume);
- NYSE stocks: F (about $14), KO (about $87), JPM (about $356);
- all eight at 30m and 5m; SPY and AAPL only at 1m.

The price descriptions document the selection contrast; candidate evaluation
must derive the contemporaneous as-traded band at each decision. It must not
reuse today's price or venue as a historical fact.

NYSE/Nasdaq above means the current **primary listing market**, not execution
venue. An NMS stock may execute on another exchange, ATS or broker-dealer
internaliser. Listing-market cohorts cannot stand in for broker routing,
spread, slippage or venue-level execution quality.

## First live provider run

The first V1 run completed at about 13:05 UTC and measured:

```text
selected=12
provider bars fetched=11,761
completed RTH bars written=5,760
member failures=0
gap ranges recorded=350
```

The provider's 1,000-bar response includes extended hours. After explicit RTH
filtering, the usable first window was materially smaller than the raw count:

- 1m: 479 rows / two instruments, part of 2026-08-07;
- 5m: 2,463 rows / five instruments, earliest 2026-07-06;
- 30m: 2,818 rows / five instruments, earliest 2026-04-07.

The variation in earliest date is itself evidence: sparse instruments do not
have one candle for every nominal interval. Of the 350 initial gap ranges, 348
belonged to CENN; one each belonged to QQQ 30m and IWM 5m. They remain gaps.

An immediate identical end-to-end scheduled rerun fetched 6,571 provider bars,
wrote zero rows and recorded zero new gaps. Its `job_runs` row was `success`,
proving overlap idempotence through the real provider/job/database path.

The first in-session V2 pass at 09:35 ET wrote 18 newly completed bars across
the bounded 12-member slice with zero failures. This confirms the incremental
path against live-session rather than historical-only responses.

A second in-session tracked pass at 09:40 ET measured 20,184 WAL bytes for the
whole job while writing four new bars (5,046 bytes/new bar at this very small
increment). The total includes fixed `job_runs`, cursor, index and commit WAL,
so bytes-per-row is intentionally conservative and should not be extrapolated
linearly. More importantly for operational load, the complete bounded fire was
about 20 KiB and succeeded with zero gaps or member failures.

After activating V2 and running two bounded passes, dev held:

```text
7,561 completed bars total
  1m:   479 rows / 2 instruments
  5m: 3,413 rows / 8 instruments
 30m: 3,669 rows / 8 instruments

partition leaves: 8
heap + indexes + table overhead: 1,688 KiB
latest two V2 jobs: success, 851 and 950 rows written
```

A representative AAPL 5m August range read returned 234 rows in 1.616 ms using
the inherited compact index, with 39 KiB sort memory. This is an early small-
population measurement, not a full-retention latency claim.

## Storage bound

The active panel has a conservative maximum of approximately 233,064 retained
RTH rows under the already-enforced tier horizons and per-day caps:

```text
30m: 8 * 13 * 504 sessions       =  52,416
 5m: 8 * 78 * 252 sessions       = 157,248
 1m: 2 * 390 * 30 calendar days  =  23,400  (deliberately conservative)
                                      -------
                                      233,064
```

Linearising the first real footprint gives a planning envelope below roughly
60 MiB. Retention still drops whole expired partitions; the collector adds no
tick, quote-history or derived-indicator heap. This projection must be replaced
by measured full-window bytes/latency as the prospective store matures; live
incremental WAL now has the initial bounded-fire measurement above.

## Refusals and limitations

- The provider has no historical date anchor. Missing history cannot be
  reconstructed from this endpoint and no candidate may claim it.
- Only NYSE RTH is retained. Extended-hours hypotheses require a distinct
  version/session contract and storage-budget decision.
- Current quotes are not historical spreads or depth. Intraday OHLCV does not
  prove queue position or stop-market slippage.
- Symbol resolution must produce exactly one currently tradable instrument.
  A missing/ambiguous member fails visibly while healthy peers remain durable.
- Active universe membership is database-enforced immutable. Expansion creates
  a new version; it cannot rewrite the earlier evidence identity.
