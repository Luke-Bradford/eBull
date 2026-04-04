# execution-guard

## Purpose

Approve or reject trades using hard portfolio and safety rules before any broker write action.

## Inputs

- proposed order\n- thesis freshness\n- portfolio state\n- market status\n- account mode

## Outputs

- PASS / FAIL\n- exact rejection reasons\n- normalized order payload if PASS

## Rules

- Never bypass a failed hard check\n- Refuse trading on stale research\n- Refuse trading when live mode is disabled

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
