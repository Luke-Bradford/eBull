# Strategy-ledger index cleanup result

Date: 2026-08-11

Issue: #2486

Scope: database efficiency only; no strategy, signal, outcome or trading rule changes

## Before

The dev database was 63 GB with 135 GiB filesystem headroom and no running
strategy, evidence or backtest job. The strategy relations themselves were
small; historical write churn had left disproportionate indexes:

| relation | live rows | heap | indexes | relevant index |
| --- | ---: | ---: | ---: | --- |
| `strategy_outcomes` | 4 | 8 kB | 23 MB | duplicate non-unique 10 MB plus unique 10 MB |
| `strategy_signals` | 5,279 | 8.0 MB | 17 MB | unique identity index 14 MB |

`idx_strategy_outcomes_signal_versions` and the constraint-owned
`strategy_outcomes_unique` both indexed exactly
`(signal_id, rule_set_version, input_rule_set_version)`. Repository-wide read
enumeration found four consumers: pending-outcome selection, strategy
monitoring, the live gate and the Strategies activity query. Every consumer
probes equality on all three columns; none needs different ordering, included
columns or a non-unique semantic.

With sequential scans disabled solely to expose the nested lookup choice, the
exact pending-outcome production join used the duplicate index for 108 probes
and completed in 3.742 ms. That proves the lookup shape, not that the 10 MB
copy is required: the unique index has the same ordered keys and cardinality
guarantee.

## Maintenance contract

Migration 325:

1. runs in autocommit because PostgreSQL forbids concurrent index maintenance
   inside a transaction;
2. drops only the structurally duplicate index with `DROP INDEX CONCURRENTLY`;
3. rebuilds the two historically bloated unique indexes with `REINDEX INDEX
   CONCURRENTLY`;
4. refuses lock acquisition after five seconds and bounds each maintenance
   statement to five minutes;
5. remains replay-safe if interrupted: the drop is `IF EXISTS` and every
   reindex targets the surviving constraint index by its stable name.

No `VACUUM FULL`, table rewrite, row deletion or trading-state mutation is
performed. The migration should be applied outside an active evidence/backtest
run; the measured dev application satisfied that prerequisite.

## After and verification

Migration 325 completed in under one second on dev. Immediately afterwards:

| relation | index bytes after | reduction from measured before |
| --- | ---: | ---: |
| `strategy_outcomes` | 2,441,216 (2.3 MB) | about 20.6 MB |
| `strategy_signals` | 3,989,504 (3.8 MB) | about 13.2 MB |

The four outcome rows and 5,279 signal rows were unchanged. The controlled
pending-outcome plan switched to `strategy_outcomes_unique` for all 108 nested
probes and completed in 0.681 ms versus 3.742 ms before. This is a small-table
plan check, not a general latency claim; its purpose is to prove the surviving
index supplies the production equality lookup.

The real bounded `strategy_outcome_resolution` job then completed successfully,
selecting 106 observations and writing four newly mature outcomes while leaving
both index byte totals unchanged. There were no dead tuples. This confirms a
representative scan/write cycle does not immediately recreate the historical
bloat. Application boot, migration-on-clean-database and the complete repository
gate remain the merge checks.
