# R6 #2914 operational-rules result

Verdict: **PASS — OPERATIONAL RULES INSTALLED; FACTOR VALUATION UNAVAILABLE**

Measured at `2026-08-23T22:00:51.191046+00:00` from execution commit `228551704fce152e54ad238e15bfa01c0ec9556b`.
Declaration SHA-256: `1f81ceb1675e6636d52ec0de6d685643a810d56688b9d9c8b4c4786987338c50` at `5fbde41f924c29daafd56b56b2678e9a0d557bfb`.

## Rules

- Turn of month: `r6-2914-turn-of-month-preference-v1`, offsets `[-3, -2, -1, 0, 1, 2, 3]`.
- Factor valuation: `r6-2914-factor-valuation-record-v1`; status `unavailable`.
- Unavailability reason: #2912 accepted corpus contains factor returns and macro context, not valuation-spread levels.
- Deterministic checks: `{'exact_window': True, 'duplicate_calendar_refused': True, 'incomplete_calendar_refused': True, 'missing_anchor_refused': True, 'internally_incomplete_calendar_refused': True, 'return_provenance_relabel_refused': True, 'authority_inputs_absent': True}`.
- The preference creates no order, holding, amount, turnover or execution authority.
- Recent factor returns are explicitly ineligible as a valuation-spread proxy.

## Full accepted #2912 corpus

- Accepted snapshots: 7
- Snapshot-series cells: 38
- Typed observations: 38017
- Observation units: `{'binary_indicator': 2060, 'decimal_return': 24715, 'percent_per_annum': 11242}`
- Genuine valuation-spread series: 0

## Return boundary

- 15% haircut: N/A — operational rule, no return outcome
- 58% haircut: N/A — operational rule, no return outcome
- Buy-and-hold: N/A — no strategy arm or return window

| source | dataset | parser | series | unit | observations | window |
| --- | --- | --- | --- | --- | ---: | --- |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v1 | CMA | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v1 | HML | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v1 | Mkt-RF | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v1 | RF | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v1 | RMW | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v1 | SMB | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_momentum_monthly | kenneth-french-monthly-csv-v1 | Mom | decimal_return | 1194 | 1927-01-31..2026-06-30 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOM | decimal_return | 653 | 1972-01-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOM^AA | decimal_return | 637 | 1972-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_COM | decimal_return | 637 | 1972-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_EQ | decimal_return | 575 | 1977-03-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_FI | decimal_return | 505 | 1983-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_FX | decimal_return | 553 | 1979-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_JP90 | decimal_return | 628 | 1974-02-28..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_ROE90 | decimal_return | 628 | 1974-02-28..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_UK90 | decimal_return | 653 | 1972-01-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOMLS_VME_US90 | decimal_return | 652 | 1972-02-29..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | MOM^SS | decimal_return | 653 | 1972-01-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VAL | decimal_return | 653 | 1972-01-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VAL^AA | decimal_return | 637 | 1972-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_COM | decimal_return | 637 | 1972-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_EQ | decimal_return | 575 | 1977-03-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_FI | decimal_return | 505 | 1983-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_FX | decimal_return | 553 | 1979-01-31..2025-01-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_JP90 | decimal_return | 539 | 1981-07-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_ROE90 | decimal_return | 539 | 1981-07-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_UK90 | decimal_return | 539 | 1981-07-31..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VALLS_VME_US90 | decimal_return | 652 | 1972-02-29..2026-05-31 |
| aqr | aqr_vme_monthly | aqr-vme-monthly-xlsx-v2 | VAL^SS | decimal_return | 652 | 1972-02-29..2026-05-31 |
| fred | fred_dgs3mo | fred-csv-v1 | DGS3MO | percent_per_annum | 11242 | 1981-09-01..2026-08-20 |
| fred | fred_usrec | fred-csv-v1 | USREC | binary_indicator | 2060 | 1854-12-01..2026-07-01 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v2 | CMA | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v2 | HML | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v2 | Mkt-RF | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v2 | RF | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v2 | RMW | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_five_factor_monthly | kenneth-french-monthly-csv-v2 | SMB | decimal_return | 756 | 1963-07-31..2026-06-30 |
| kenneth_french | french_momentum_monthly | kenneth-french-monthly-csv-v2 | Mom | decimal_return | 1194 | 1927-01-31..2026-06-30 |
