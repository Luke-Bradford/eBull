# Bounded prospective eToro quote capture

Status: implemented evidence prerequisite for #2484 and #2485; no strategy is
promoted or allocation-enabled by this collector.

## Why this exists

`quotes` owns one mutable latest snapshot per instrument. It is suitable for a
freshness-gated current decision but cannot prove the bid, ask or spread that
was observable at an earlier signal, entry or exit. Retained OHLCV candles are
not a replacement for executable quotes.

The existing five-minute intraday evidence job now makes one additional batch
quote request for the unique symbols in the exact active research universe.
It records the first observation within each five-minute bucket. The row is
immutable, so a later run cannot replace a 15:30 observation with information
from 15:34. The
automatic job runs only from the first completed regular-session bar through
the existing ten-minute close-lag allowance; an operator-triggered run uses the
same collector and may record an out-of-session sample, which downstream trial
session rules must refuse when irrelevant.

## Storage and coverage contract

`strategy_quote_observations` contains:

- active universe version, exact instrument and five-minute sample bucket;
- application observation time and provider quote time separately;
- best bid, ask, optional last and directly calculated spread basis points;
- `observed`, `missing` or `invalid` status and a stable refusal code;
- universe-versioned eToro source provenance.

A provider omission is a `missing` row, never a zero spread. Non-positive,
crossed or timezone-naive quotes are `invalid` rows with price fields removed.
Duplicate or out-of-scope provider IDs refuse the whole batch. Symbol
resolution must produce exactly one currently tradable instrument; healthy
peers remain collectable and the job fails visibly for the unresolved member.

The table does not assert that a quote was fresh enough for a candidate. Quote
age, allowed entry delay, session, halt state and side-aware fill rules belong
to the preregistered trial. Retaining `quote_at` and `observed_at` lets those
rules be applied without rewriting history.

## Bound and retention

The current panel has eight unique instruments. At one sample per five-minute
regular-session bucket and 504 sessions over 24 months, its conservative bound
is:

```text
8 instruments * 78 buckets * 504 sessions = 314,496 rows
```

Repeated/manual runs within one bucket are no-ops rather than appends or
updates.
The daily strategy-observation retention job deletes samples older than 24
calendar months using the indexed bucket column. Completed outcome rows retain
compact cost attribution separately; raw quote samples are not durable
forever. There is no all-instrument subscription, tick tape, order book, depth
history or derived-spread series.

## What this unlocks—and what it does not

Once sufficient prospective coverage accrues, candidate evaluation can use a
long entry at observed ask and exit at observed bid (the reverse for shorts),
measure spread by instrument/time/regime, count collection gaps, and apply
candidate-specific freshness and delayed-entry stress.

It does not provide queue position, fill probability, stop-market gap
slippage, shortability, borrow fees or CFD financing. Those remain independent
fail-closed gates. Historical periods before this collector started still have
no observed eToro quote evidence and must not be backfilled from current rows.

## First live evidence

The first production integration pass exposed two defects before any strategy
consumed a row:

1. capture time had been taken before the network request, making fresh provider
   timestamps appear about 13 seconds in the future;
2. a later run in the same bucket could replace the earlier observation, which
   would let a 15:34 quote overwrite evidence intended for 15:30.

Capture time is now taken after the response and PostgreSQL rejects all row
updates. The eight pre-contract development rows were deleted by exact universe
and bucket after verifying the count, then recollected under the final contract.
They had no signal or outcome consumer.

The 16:05 UTC automatic run then completed through the real provider, scheduler
and database path:

```text
expected=8 observed=8 missing=0 invalid=0 rows_written=8
two immutable buckets / 16 rows total
table + indexes = 64 KiB
```

The observation also proves why `observed` must not mean `fresh`. AAPL, F, IWM
and KO provider times were within one second of capture. SPY, QQQ and JPM were
about 13.7 minutes old, while low-volume CENN was about 94 minutes old. The
collector preserves all of them and their timestamps; a candidate-specific
freshness gate must refuse the latter observations rather than charging their
displayed spreads as executable costs.
