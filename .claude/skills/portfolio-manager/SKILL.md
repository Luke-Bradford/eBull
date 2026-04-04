# portfolio-manager

## Purpose

Turn ranked names into buy/add/hold/trim/exit recommendations.

## Inputs

- ranked candidates\n- current portfolio\n- cash\n- policy limits

## Outputs

- action recommendations\n- suggested sizes\n- allocation notes

## Rules

- Respect concentration and position-size limits\n- Adds require stronger evidence than initial buys\n- Prefer no action to weak action

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
