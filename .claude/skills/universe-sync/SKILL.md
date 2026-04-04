# universe-sync

## Purpose

Synchronize the actual eToro tradable universe and maintain canonical instrument metadata.

## Inputs

- eToro instrument search / metadata responses

## Outputs

- normalized tradable universe rows\n- new / removed instrument report\n- stale mapping warnings

## Rules

- Cache immutable identifiers locally\n- Do not treat a symbol as tradable unless confirmed by current eToro data\n- Prefer canonical local mapping over ad hoc symbol matching

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
