# Holdout ambiguity support (#2749)

## Decision

A holdout result keeps its own immutable §3.4 ambiguity record. When that
record contains unequal arm Sharpes it correctly has no cohort threshold,
because the 1,000-member random-entry control is never run over withheld data.
Promotion may resolve only that `ambiguity_arms_not_compared` state from the
one exact full-corpus in-sample companion already derived by
`strategy_result_control_support`.

The support id is never caller-selected. It matches the holdout result on the
strategy and result identity, including ambiguity rule, immutable validated
universe and all measurement/rule stamps. Missing or duplicate candidates do
not produce a support id.

## Precedence

The holdout record is loaded and hash-verified first:

1. absent remains `ambiguity_verdict_unrecorded`;
2. corrupt raises as an integrity failure;
3. unrecognised, material, shared-measurement and equal-arm verdicts remain
   authoritative;
4. only the sole `ambiguity_arms_not_compared` outcome may consult support;
5. absent, unrecognised, not-compared or material support remains fail-closed.

The in-sample threshold is never compared with holdout Sharpes and is never
copied onto a holdout row. The companion's own complete verdict is replayed.
This keeps the roles distinct: the holdout row proves both withheld arms were
measured, while the in-sample falsification adjudicates sensitivity to the
daily-OHLC tie rule without spending another withheld look.

## Consumers

- The promotion transition returns only composed refusal codes and batches all
  direct/support reads; it does not expose withheld metrics or record a new
  holdout access.
- The Strategies API applies the same pure precedence function and verifies the
  hashes of both SQL-loaded ambiguity payloads.
- Monitoring and paper execution continue to require a durable promotion and
  therefore inherit this gate; their existing exact synthetic-control support
  checks remain unchanged.

This repair opens no holdout, allocates no capital and changes no live-money
path. Every declared holdout window and every ambiguity/quarantine arm must
still satisfy its own conjunctive performance and promotion-evidence gates.
