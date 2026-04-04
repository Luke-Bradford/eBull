# market-data

## Purpose

Refresh quotes, candles, and price-derived features for covered instruments.

## Inputs

- current quote data\n- historical candle data

## Outputs

- normalized market snapshots\n- rolling return metrics\n- volatility and price trend features

## Rules

- Flag stale quotes\n- Separate raw prices from derived indicators\n- Do not invent missing data

## Failure conditions

- Missing critical source data
- Stale timestamps beyond allowed threshold
- Contradictory evidence without explicit uncertainty handling

## Deliverable format

Return:
- status
- summary
- structured fields
- confidence / uncertainty note where relevant
