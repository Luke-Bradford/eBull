# news-sentiment

## Purpose

Cluster current news and sentiment into signal-bearing events versus noise.

## Inputs

- recent articles\n- feed posts\n- comments\n- prior event history

## Outputs

- event clusters\n- sentiment trend\n- importance scores

## Rules

- Retail hype is not a thesis by itself\n- Cluster duplicates\n- Do not let sentiment dominate fundamentals

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
