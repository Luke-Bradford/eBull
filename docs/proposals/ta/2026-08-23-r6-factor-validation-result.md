# R6 factor-construction validation result (#2912)

Verdict: **PASS**. This validates construction identity only; it is not an
investable arm and supplies no return, cost, haircut, or benchmark verdict.

## Frozen identities

- Original declaration SHA-256:
  `f63d0cf6084cf7158a37f9fa904fbe892b66d8df7fd94e0cd17e0c69a02ad68b`
  (commit `7f71c9cf`, before the first result).
- Correction-1 SHA-256:
  `3b111dce37729a63b750c2abb61b8bf53bebb68f0e83e253109148c49e5f0f3c`
  (commit `3fe48d52`, before the corrected result).
- Reproduce: `PYTHONPATH=. uv run python -m scripts.report_2912_factor_validation`.

## Invalid first run and code correction

The frozen failure rule did its job. The first run failed with correlations
`+0.026861` (French) and `+0.023260` (AQR). March 2019 was `-25.113837`, caused
by bottom-leg series `ZBZZT` returning `+14284.713571` from a stored `14` to
`199999.99`. Nasdaq identifies `ZBZZT` as a test security. It is not a company
or an outlier to trim; admitting it was a universe-identity bug.

Correction 1 froze the union of official Nasdaq Symbol Directory rows with
`Test Issue=Y`, then changed the shared, hash-versioned universe rule before a
corrected result was read. Ten test issues were excluded from the pinned
Intrader corpus and a mandatory closed census stratum preserves reconciliation.
No magnitude rule, clipping, winsorisation, or result-selected threshold was
introduced. The invalid run is retained here and discarded as evidence.

## Corrected full-population result

The pinned survivorship-free vendor contains 22,879 series: 17,285 admitted,
5,584 unlinked-alive excluded, 10 exchange test issues excluded, zero
unharvested, and 520 admitted linked early-ending/reuse suspects. The query
read 38,738,790 fail-closed bars and produced 609 months from 299,938 selected
member-legs (285,812 usable endpoints; 14,126 rejected).

| Comparison | Window | Months | Corr | Alpha | Beta | Lag 1 | Lead 1 | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| eBull S-2 vs French MOM | 1971-02–2024-08 | 609 | +0.665267 | +0.00337173 | +1.044071 | +0.016750 | +0.092225 | PASS |
| eBull S-2 vs AQR U.S. MOM | 1972-02–2024-08 | 597 | +0.649738 | +0.00436199 | +0.981939 | -0.029230 | +0.062668 | PASS |
| French MOM vs AQR control | 1972-02–2026-05 | 652 | +0.917444 | +0.00092091 | +0.892036 | +0.030452 | +0.013559 | PASS |

Both eBull comparisons exceed the frozen `+0.20` correlation floor, carry
positive beta, and are contemporaneously stronger than either ±1-month
displacement. The independent reference control exceeds its `+0.50` floor.

## Immutable source evidence

- French five factor: SHA-256 `cbc3724812132654fbbe8daae3c46e0f90e70008434f94a7986fe49f1db6ad3b`,
  parser v2, 4,536 observations, 1963-07–2026-06.
- French momentum: SHA-256 `f405ee2d47a5c75ce05025f789733d0599879361e9836a553504240b89159871`,
  parser v2, 1,194 observations, 1927-01–2026-06. V2 pins each dataset's
  exact header; its full v1↔v2 symmetric observation difference is zero.
- AQR VME: SHA-256 `a2351d0323ab60c715a359c55d70a596560af75da335e7ceaa9326b9737daf49`,
  parser v2, 13,255 observations and 1,111 missing cells, 1972-01–2026-05.
  Parser-v1 rejection of the same bytes is retained; v2 explicitly handles
  blank-string footer rows rather than rewriting history.
- FRED DGS3MO: SHA-256 `b4319130f24c555f5ea8fa887d342d3b66ad2c5976eb615bed37263586d67ec1`,
  11,242 observations and 491 missing values, 1981-09-01–2026-08-20.
- FRED USREC: SHA-256 `50a6d6f351c2a190f0ddc926956afbba791ee69b86c70bda4fdcbbe28bf16feb`,
  2,060 observations, 1854-12–2026-07.

Existing paths were validated rather than duplicated: SEC holds 4,603,674
normalized XBRL facts for 5,252 instruments (57 linked ingest runs), FINRA
short-interest holds 186,983 observations for 6,185 instruments, and FINRA
RegSHO holds 1,518,123 observations for 5,784 instruments.

## Boundary

The finding is exactly that eBull's S-2 rank construction has the expected
published momentum identity, sign, and timing. It does not show that S-2 is
profitable after costs or either required literature haircut. Those questions
remain for a separately preregistered arm after the Tier-0 and Tier-1 gates.
