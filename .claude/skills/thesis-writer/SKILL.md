# thesis-writer

## Purpose

Write the structured investment memo for a covered stock.

## Inputs

- company summary\n- filing analysis\n- news and sentiment\n- valuation range\n- prior thesis

## Outputs

- current thesis memo\n- stance\n- key risks\n- break conditions

## Rules

- Be explicit about what must go right\n- Separate facts from judgement\n- Keep the memo structured and comparable over time

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
