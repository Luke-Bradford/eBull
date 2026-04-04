# valuation-analyst

## Purpose

Generate bear/base/bull valuation ranges and entry bands.

## Inputs

- price\n- fundamentals\n- thesis type\n- market context

## Outputs

- bear/base/bull values\n- buy zone\n- upside/downside estimates

## Rules

- Match valuation style to company type\n- Deep value and turnarounds need explicit survivability thinking\n- Surface key assumptions

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
