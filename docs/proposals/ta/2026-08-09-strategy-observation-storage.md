# Bounded strategy observation storage

Date: 2026-08-09
Status: Implemented
Parent: #2437
Ticket: #2448

## Decision

The signal/outcome evidence chain remains complete without retaining every
routine verdict forever:

- fired signals stay durable in `strategy_signals` because outcomes and future
  trade ownership refer to `signal_id`;
- not-fired/not-evaluable detail moves to monthly
  `strategy_signal_observations` partitions and is retained for 90 days;
- `strategy_signal_daily_counts` durably retains every
  strategy/version/date/kind/verdict/reason count;
- the scanner writes all three destinations and its watermark in one
  per-strategy transaction, without conflict suppression;
- a daily 07:05 job drops complete expired leaf partitions, never row-level
  deletes;
- intraday storage holds completed OHLCV bars only—no ticks, forming bars or
  derived indicator series.

The one-time migration from the old mixed signal ledger first proves daily
aggregate equality and that no outcome refers to a non-fired row, moves the
still-retained detail, proves equality again, and only then removes those rows
from the durable fired ledger. The recurring path cannot issue that delete.

Splitting detail does not weaken the old one-verdict-per-logical-key rule. The
writer rejects duplicate keys inside a batch, serialises each strategy/version,
checks the opposite detail table before writing, and rejects dates behind the
scan watermark. The watermark preserves terminality after negative detail has
expired without retaining another per-instrument key relation.

## Fixed caps and backpressure

| tier | instruments | regular-session bars/day | retention | partition |
| --- | ---: | ---: | ---: | --- |
| context | 1,000 | 13 × 30m | 24 months | monthly |
| setup | 250 | 78 × 5m | 12 months | monthly |
| execution | 50 | 390 × 1m | 30 days | daily |

`store_intraday_bars` rejects an unknown tier, an over-wide retained universe,
too many stored-plus-incoming bars for one instrument/day, duplicate keys
inside a batch, unaligned or future/forming bar times, invalid OHLCV, and any
bar outside the tier's retained horizon or at/behind the durable per-tier/
instrument watermark. A transaction lock
serialises each tier's cap/watermark decision, closing the concurrent first-
write race without adding a per-bar btree. It inserts in instrument/time order
so the BRIN ranges remain useful. This is a storage API, not an ingest source;
no recurring intraday fetch is enabled by this ticket.

## Measured design iterations

Reproduce with:

```bash
PYTHONPATH=. uv run python scripts/verify_2437_observation_storage.py --benchmark
```

The script uses rollback-only temporary tables populated from the developer
corpus. On 2026-08-09 it measured:

| shape | bytes/row incl. indexes | capped projection | decision |
| --- | ---: | ---: | --- |
| numeric OHLCV + two btrees | 247.9 | 2.99 GB | rejected |
| double OHLCV + one btree | 189.2 | 2.28 GB | rejected |
| double OHLCV + monotonic watermark + BRIN | 117.5 | 1.422 GB | accepted |

The accepted 200,000-row sample used 23,412,736 heap bytes and 49,152 index
bytes, inserted in 0.25 seconds, and served a representative bounded
instrument/time reads in 8-11 ms across repeated runs. Projection rounds measured bytes-per-row upward;
the code-enforced check fails if a future
shape exceeds the 1.5 GB retained-tier budget.

The current fired population measured 405.6 bytes/row under its fired-only
indexes, projecting 0.528 GB/year at the measured peak 5,171 fired rows per
trading day. Negative detail measured 309.1 bytes/row and is bounded by a
90-day cutoff applied to complete monthly partitions (at most one partial-month
overhang), rather than annual accumulation. The durable daily census has only
46 groups for the current 34,698 logical rows.

The existing all-detail design was 33.4 MB for 34,698 rows and projected 8.42
GB/year. After routing monitoring to the count table, the current-version scan
aggregate fell from 15.6 ms over detail to 0.05 ms over two cached count pages.

## Retention safety

The verifier compares the daily census in both directions against all durable
fired detail and the most recent 90 days of negative detail using `EXCEPT ALL`.
Older routine counts deliberately remain durable after their detail partition
expires and are outside that detail-parity window. The measured mismatch count
is zero. It also enumerates inbound foreign keys; only
`strategy_outcomes.signal_id` protects `strategy_signals`, and the migration
proves none of those rows refers to a routine verdict before moving data.

`retention_plan` discovers only leaf relations whose names match the closed
partition vocabulary and whose entire date bound is expired. `DROP TABLE` uses
an SQL identifier sourced from `pg_class`, never request text. Tests prove a dry
run, actual whole-partition drop, and preservation of current partitions. Once
all retained bars for a tier/instrument have expired, the small watermark row is
deleted too; that permits bounded universe rotation without deleting bar rows.

## What remains disabled

No intraday source, signal evaluator, deployment, allocation or broker writer is
enabled here. The schema makes their future storage bounded; it does not make
their data available or their strategies promotable.
