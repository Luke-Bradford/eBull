# Recent market and sector comparator frontier

Status: implemented for #2482. Parent #2469.

## Decision

Freeze a **separate** eToro price-return comparator snapshot through the same
`2026-07-08` frontier as the recent equity research corpus. Never append those
bars to the older `icyDenev/Intrader` series.

The recent snapshot contains 18 exact ETFs:

- broad/style: SPY, QQQ, IWM, VTI;
- all eleven current Select Sector SPDRs: XLC, XLY, XLP, XLE, XLF, XLV,
  XLI, XLB, XLRE, XLK, XLU;
- cross-asset regime: TLT, GLD, UUP.

DIA remains available in the legacy snapshot only. eToro has no exact DIA
instrument in the current database, and substituting another Dow product would
be an unregistered comparator change.

## Why this is a new series, not a splice

The old source stops on `2024-09-27` and carries split-adjusted OHLC plus a
separate split-and-dividend-adjusted `adj_close`. eToro supplies OHLCV price
candles and no dividend-adjusted close. A splice would therefore mix two return
bases while retaining one vendor identity.

There is also a later-corporate-action trap. State Street's official notice
records 2:1 splits in XLK, XLY, XLE, XLU and XLB, effective before the open on
2025-12-05. eToro correctly back-adjusts its earlier bars to the post-split
scale. The old archive was frozen before those events and necessarily remains
on the old scale. A raw level comparison therefore reads near 0.5 for those
five symbols; after applying the declared split factor it reads near the same
0.9985 bid-side ratio as every other ETF.

Sources checked 2026-08-10:

- [eToro Builders market data](https://builders.etoro.com/products/market-data-realtime)
  explicitly offers OHLC candles, historical closes and analytics pipelines;
- [eToro Builders FAQ](https://builders.etoro.com/faq) identifies the historical
  candle endpoints for time-series backfills;
- [eToro Builders Economy Terms, 17 February 2026](https://www.etoro.com/wp-content/uploads/2026/03/Master_eToro_Builders_Economy_Terms_17-Feb-2026-clean_R.pdf)
  treats Public API data as Licensed Content: keep it secured, use only what is
  reasonably necessary, do not redistribute it, and retain the ability to
  delete it on request;
- [State Street's official share-split FAQ](https://www.ssga.com/us/en/institutional/library-content/products/fund-docs/etfs/us/information-schedules/select-sector-spdr-fund-share-splits-faq.pdf)
  names the five 2:1 splits and their effective date.

The repo stores no eToro bars or credentials. Licensed bars remain only in the
operator's secured database.

## Measured compatibility

`scripts/verify_2482_comparator_overlap.py` compared each frozen eToro close to
the gitignored independent legacy mirror over 557–592 common sessions:

- daily log-return correlation: **0.994657–0.999779**;
- median normalised level ratio: **0.998400–0.998546**;
- median daily return difference: within **0.0125 bp** of zero;
- p99 absolute daily return difference: **6.046–24.867 bp**.

The level offset is expected: existing venue probes identify eToro's candles as
bid-derived, about 15 bp below the public close. The near-zero return difference
and high correlation establish a compatible price-return shape; they do not
turn the source into official exchange prints or total-return data.

The verifier refuses any symbol with fewer than 500 paired sessions, return
correlation below 0.99, a normalised median level ratio outside 0.995–1.0, or
p99 absolute return disagreement above 70 bp. These are source-compatibility
guards, not strategy parameters.

## Immutable storage contract

Snapshot `etoro-comparators-2026-07-08-v1` contains **18,198 rows** and has
SHA-256:

`f1e551274d4b07d8900c0371bcb38f8d460d78d3d2c822b610063ce6b2127fed`

The loader:

1. resolves exactly one current tradable ETF source row per declared symbol;
2. reads only `price_daily.price_date <= 2026-07-08`;
3. validates ordering, positive finite OHLC, OHLC envelopes and non-negative
   integral volume when volume exists; fractional source volume is refused
   before writing because the established research column is `BIGINT`;
4. hashes canonical numeric facts, source instrument mappings and membership;
5. writes one snapshot metadata row, 18 member rows and one OHLCV row per
   comparator/session in a single transaction;
6. leaves `adj_close` NULL and records dividend basis `none`;
7. forces `research_price_series.instrument_id` and `resolution_method` NULL;
8. refuses a rerun whose source facts differ under the same snapshot ID.

An intentional source revision must mint a new snapshot ID. Existing bars and
evidence remain immutable and visibly stale.

## Database impact

This is bounded raw evidence, not a derived-metric heap:

- 18,198 daily rows; current tuple payload **1,272,997 bytes**;
- 18 series rows (4,320 tuple bytes), 18 member rows (2,880 bytes), one
  snapshot row (488 bytes), plus ordinary primary-index overhead;
- no persisted indicators, rolling returns, beta, regime labels, polling
  history or duplicate intraday data.

That is immaterial beside the 25.9M-row research corpus. Future frontier
updates create a deliberate new bounded snapshot; retention may remove an old
snapshot only after no immutable result identity references it.

## Strategy use and refusals

This snapshot unlocks recent **price-return** market trend, realised
volatility, beta and sector-relative features. At date `t`, a feature may read
only comparator bars dated `<= t`; sessions are intersected, never
forward-filled.

Every candidate/result identity must name the snapshot and symbol it consumed.
No candidate may silently fall back from its sector ETF to SPY, from DIA to a
different Dow proxy, or from price return to total return. A strategy requiring
dividend-adjusted benchmark performance remains unavailable on this snapshot.

The snapshot improves the evidence inputs. It does **not** promote any current
strategy, establish positive expectancy, or justify capital. Candidate
promotion still requires recent after-cost expectancy with a positive lower
confidence bound, acceptable tails/drawdown, causal fills and exits, and
forward demo evidence under the contract in #2469.

## Operator commands

```bash
PYTHONPATH=. uv run python scripts/ingest_2482_etoro_comparators.py --probe
PYTHONPATH=. uv run python scripts/ingest_2482_etoro_comparators.py --load --verify
PYTHONPATH=. uv run python scripts/verify_2482_comparator_overlap.py \
  --mirror-root var/research_corpus/mirrors/icyDenev_Intrader/Data/Day
```
