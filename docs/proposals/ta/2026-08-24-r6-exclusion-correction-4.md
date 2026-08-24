# R6 #2908 preregistration correction 4 — holdout audit and applicable controls

Status: **FROZEN AFTER FAST-GATE FAILURE, BEFORE THE AUDITED REPRODUCTION**

- Original declaration SHA-256: `91ec11351d8851e4b3b89ba51f965b649608916346f2e10d9a7cdede9fd2c62f`
- Corrections 1-3 SHA-256: `cd694a39f392cf438e4331a29b9fe8613048127ee37770295c00650758f376fa`,
  `becf2537852a85becfc0f444dccc56e9b50a16ebf48a339619bc8187b4e5a858`,
  `f6bbac8492967c881473b30cef3d475252976d9bcea9a75eb6936a5f6c34b425`
- Predecessor corrected-manifest outcome SHA-256:
  `1ed032ca1243301764eb6e9fd9abef562e884be61c476ff50ec377babe2e1426`

The full fast gate found a governance defect: the factor and outcome scripts opened raw price outcomes without
writing the repository's criterion-5 holdout-access record. Earlier accesses are retained evidence but were not
ledger-audited. Both scripts now record a committed `read` access for
`r6-dilution-exclusion@r6-2908-exclusion-v1` **before** loading any price series and emit the resulting access ID.

The gate also required an explicit synthetic-control disposition. A random-entry synthetic control is not
applicable: this arm is a cross-sectional exclusion formed on published annual filings, not an entry chosen from a
set of alternative bars. The declared applicable controls are stronger for this claim: the identical-date,
identical-input annual full-population 1/N portfolio and literal initial-population buy-and-hold. Both remain
mandatory and unchanged.

No population, signal, price transform, portfolio arithmetic, termination, cost, haircut, comparison or verdict
rule changes. Corrected runner SHA-256 values:

- factor gate: `6df5823c6e7b9d258e185b0be0bae506685e875583a1656c089d189ba7128843`
- outcome runner: `d940570bafe16a9293d26e5668ca9db298a148362a12a4fa90b571c82698c16d`

The audited rerun must reproduce every analytical field after removing only `holdout_access_id`. Any other drift
invalidates the predecessor result.
