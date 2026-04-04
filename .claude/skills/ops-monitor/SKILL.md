# ops-monitor

## Purpose

Detect stale data, failed jobs, broken sources, and other operational problems.

## Inputs

- job runs\n- row counts\n- timestamps\n- error logs

## Outputs

- health report\n- alerts\n- stale data flags

## Rules

- Treat silent failure as failure\n- Prefer noisy ops to false confidence\n- Record enough detail for debugging

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
