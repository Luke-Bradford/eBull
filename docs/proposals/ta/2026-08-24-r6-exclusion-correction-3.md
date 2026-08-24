# R6 #2908 preregistration correction 3 — cover fallback fail-closed semantics

Status: **FROZEN AFTER PRE-PUSH REVIEW, BEFORE REPRODUCTION ON THE CORRECTED MANIFEST**

- Original declaration SHA-256: `91ec11351d8851e4b3b89ba51f965b649608916346f2e10d9a7cdede9fd2c62f`
- Correction 1 SHA-256: `cd694a39f392cf438e4331a29b9fe8613048127ee37770295c00650758f376fa`
- Correction 2 SHA-256: `becf2537852a85becfc0f444dccc56e9b50a16ebf48a339619bc8187b4e5a858`
- Predecessor outcome SHA-256: `95ff5c230d9ad55360ab0579a9a73bb97665c91b5c90fb316422e7aec4cda4d9`

The mandatory pre-push semantic review found two latent resolver defects. An unparseable newest same-period filing
could fall through to stale cover facts, while a parsed context with conflicting security values could block a
valid older same-period complete cover. The fixed rule is:

1. an unreadable/unparseable newer accession is unknown and stops resolution for that issuer; and
2. fallback is allowed only after a successfully parsed newer accession proves it has no usable context containing
   singleton `Security12bTitle`, `TradingSymbol`, and `SecurityExchangeName` facts.

Both cases are mutation-tested. The corrected full-population rebuild changed **zero** security records at every
formation: 0 removed and 0 added in 2022, 2023 and 2024. Old/new payload `records` and `formation_census` are exactly
equal, and all three ranking-input hashes remain exactly equal. This means the defect was real but latent in this
pinned corpus; no outcome-sensitive choice follows from the fix.

Corrected identities:

- cover resolver script SHA-256: `b8f7eba8c38a560136c6cd6e44f289f4b4a1834184b5d46f6c72f0d15fcc1abe`
- cover census SHA-256: `f1cddd8fb3ebe4c95b51fc6de1ec408494d969d602c0f3f1116e1aa9fbed9382`
- PIT payload SHA-256: `c2af906737be9dd14229657404b76bf0b0c1865db20ba33c9300374e25b17c81`
- PIT manifest SHA-256: `0b25af8c37b12437963f4681df7513397b83cd0ba7e37b15e1165b52f90d17d2`

The adversarial leak test passes on the new manifest with unchanged ranking-input hashes. The factor-only and full
outcome commands must now be rerun against this manifest and every result compared byte-for-byte after ignoring the
manifest identity. Any numerical drift invalidates the predecessor verdict and must be reported; equality confirms
the prior arm result under the corrected evidence root.
