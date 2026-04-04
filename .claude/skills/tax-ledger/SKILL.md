# tax-ledger

## Purpose

Maintain a UK-oriented ledger and disposal-matching view from actual events.

## Inputs

- fills\n- dividends\n- fees\n- tax-year boundaries

## Outputs

- realized gains/losses\n- dividend totals\n- disposal matches\n- estimated tax view

## Rules

- Keep raw events immutable\n- Derive tax views from stored events\n- Preserve matching-rule provenance

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
