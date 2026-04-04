# ranking-engine

## Purpose

Convert score families and penalties into a stable ranked candidate list.

## Inputs

- score family values\n- penalties\n- model weights\n- prior ranks

## Outputs

- ranked list\n- score deltas\n- movement explanations

## Rules

- Version the model\n- Keep weight logic explicit\n- Penalize stale or low-confidence names

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
