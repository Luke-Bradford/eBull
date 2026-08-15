# Long-only PEAD forward-feasibility census

Status: **arrival measured / power verdict refused** for #2493. This is an
outcome-free planning census, not a return result, holdout, promotion record or
authority to create strategy/runtime state.

## Boundary and reproduction

The verifier rebuilds the frozen `pead-historical-sue-net-income-v1` long-signal
rule, expands issuer share-class alternatives only after signal construction,
and applies the declared USD 5 entry-open and prior-20-session USD 10m median
dollar-volume floors. SEC `accepted_at` controls the earliest possible open
where present; a missing timestamp advances to the next filed-date boundary.

It may select the entry open and closes strictly before entry for liquidity. It
has no exit-price, holding-period return, comparator-return or outcome field.
`tests/test_pead_feasibility.py` guards that boundary.

Reproduce with:

```text
PYTHONPATH=. uv run python scripts/verify_2493_pead_feasibility.py
```

The 2026-08-15 capture used Company Facts SHA-256
`0c5b0d0b61257f6856f6c30311806d782d45d6d32118280d75bc859d57ad9c20`.
That is **not** the #2476 result's declared archive
`126056a91f8d0446bd0f9c04f7db84da7e405d171c541fe72c7aae70d5b6c02b`.
The scheduled SEC refresh atomically replaces the mutable cache and no copy of
the older archive was found locally. These counts are therefore a newly hashed
source-rule census, not an exact reproduction of #2476. The verifier prints
this distinction rather than silently claiming source identity.

## Measured arrival ceiling

Through the frozen 2026-07-08 price frontier, the refreshed source produced:

| measure | full 2022+ census | trailing 24 months from 2024-07-08 |
| --- | ---: | ---: |
| eligible long issuer-events | 1,269 | 764 |
| distinct entry dates | 393 | 215 |
| greedily purged 62-NYSE-session dates | 18 | 8 |

Eligible event arrivals by entry year were 164 (2022), 174 (2023), 342
(2024), 351 (2025), and 238 through the frontier in 2026. The entry-known
refusals were 327 below the price floor, 55 with an incomplete prior-liquidity
window, 759 below the liquidity floor, 192 without a usable series/entry, and 6
suppressed alternative share classes, from 2,608 expanded long-signal rows.

The purged count uses the repository's declared NYSE calendar. A retained date
must be at least 62 market-session indices after the prior retained date. It is
a conservative fully non-overlapping dependence ceiling, not a claim that
same-date issuers are economically identical or that a block-bootstrap ESS is
exactly eight.

## Decision

Do not start forward shadow collection from this census alone. The ticket has
not independently frozen either:

- the minimum net effect required to improve the operator's F-0 portfolio
  after its risk penalty; or
- a compatible prospective planning dispersion/block model.

The already observed +2.676% #2476 long-arm mean is forbidden as either input.
Without both inputs there is no honest sample requirement or time-to-power, so
the requested `data_infeasible` versus preregister decision remains refused.
The measured fact available to that decision is stark: a full purge supplies
only eight non-overlapping decision dates inside the 24-month relevance
horizon. Any less-conservative dependence model must be frozen and justified
before it is used to turn the larger nominal event count into effective
evidence.
