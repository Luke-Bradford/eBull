# R6 #2908 preregistration correction 1 — halted holding bound

Status: **FROZEN AFTER A FAIL-CLOSED ATTEMPT AND BEFORE ANY ARM RETURN TABLE**

Predecessor declaration SHA-256:
`91ec11351d8851e4b3b89ba51f965b649608916346f2e10d9a7cdede9fd2c62f`, committed at
`222d84386ba00753ec49a7974d694403cc58624e`.

The first outcome command produced zero stdout bytes and no return cell. It stopped while valuing the first
schedule because existing holding `YTEN` had no bar on `2024-07-01`, although its pinned series later resumed on
`2024-09-03`. The predecessor correctly failed loudly, but specified bounds only for a series ending before an
event and did not specify a temporary halt spanning an event. This is an execution-state omission, not a market
finding, and no observed return is available to tune against.

Correction: when an **existing holding** lacks the required rebalance or final session, apply the same two bounds
regardless of whether the series later resumes:

- best case: value and liquidate it at the last adjusted close strictly before the event, charging the sell
  half-spread; and
- governing worst case: zero recovery.

Every such holding is counted in `censored_holdings` on the event. A new target is still admitted only when its
exact frozen first post-formation execution bar exists; this correction cannot create an entry or use a later
resumption price. A holding with no price at or before the event still fails loudly.

This rule is deliberately a bound, not an assertion that a halted stock was executable at its stale mark. It is
adverse in the capital-governing case and prevents an unobservable delisting/halt return from disappearing. All
other population, signal, benchmark, cost, haircut, window and verdict rules remain byte-for-byte as declared.
The already opened factor-only PASS remains valid because this change does not alter factor construction.

Corrected implementation SHA-256:

- `app/services/r6_exclusion_trial.py`: `bfba9460b423359c17a1c17a70ac41a4dbdc52b560fd50271d1b67513ee887e6`
- outcome runner (unchanged): `9baaf5336814e8c631d81f27667ae4be154a1868ec8cf388dc732e2b45c10a0f`

The correction is mutation-tested with an in-series halt that resumes after the valuation date. The next outcome
run must publish all cells even if the bound destroys the arm.
