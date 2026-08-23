# R6 point-in-time spine result (#2900)

Verdict: **FAIL — NO ADMISSIBLE HISTORICAL FIELD**

Declaration SHA-256: `369b397e17694f2a54b07897b4d68a8728bf0624e35d00ced8a6ce833bb4da20` at commit `51e55d58823a5ebe98e5eea6473895b9d05abc1d`.
Correction-1 SHA-256: `b101123c59183b8204a70b98a9c40b25e350a4fc58a9c114d8ea76735157cff6` at commit `fe67f100e565fd52db8201ac6ad8f1758c2b163f`.
Correction-2 SHA-256: `a330ed170d39da3c201e2bd8e1ce5f80566209a356faa75bb899090e5e2b4f32` at commit `055056a5c3b1aa3bc5971de4a6085b0f7bd72206`.
Execution commit: `9a5e905e3455cbaa426f1cc2a0c38815959b7270`. Registry: `r6-pit-registry-v1+73aa1335b55e`.
Reproduce: `PYTHONPATH=. uv run python -m scripts.verify_2900_point_in_time --format markdown`

## Adversarial leak test

- Decision date: `2020-01-15` (NYSE session).
- Baseline hash: `ec6ad54aba777248a10fad18358cba48eb5bcf2bb8c53cfc1241c51c384c9a41`.
- Post-decision insert hash: `ec6ad54aba777248a10fad18358cba48eb5bcf2bb8c53cfc1241c51c384c9a41` (unchanged).
- Same-key pre-decision overwrite hash: `567bb6682861b4afd8952c70cad353030a2ef49d9cc63616c12b52aebb8e03f5` (changed).
- First unequal field: `ingest_run_id`: `00000000-0000-0000-0000-000000002900` → `00000000-0000-0000-0000-000000002902`.
- Rows recoverable at the old `ingested_at` after overwrite: 0.
- Rollback proved on a new connection: `true`.

## Contract evidence

- Successful non-vacuous probes: D0, D1, F0, F1, F2, H1, H2, H3, L1, N0, N1, N2, O0, O1, O2, O3, P0, P1, P2, P3, P4, P5, R0, R1, X1.
- Refused registry families: derived_fundamentals, dimensional_xbrl, filing_red_flags, finra_short_interest, fundamental_facts, historical_population, live_etoro_state, ownership_observations, research_prices.
- Exact census tables: financial_facts_raw, ownership_insiders_observations, ownership_institutions_observations, ownership_blockholders_observations, ownership_treasury_observations, ownership_def14a_observations, ownership_funds_observations, ownership_esop_observations, finra_short_interest_observations, research_price_series, research_price_daily, research_price_quarantine_coverage, instrument_universe_membership, instrument_symbol_history, external_identifiers.
- Full probe anchors, source hashes, registry matrix and census values are emitted by `--format json`.

## Population census

- `financial_facts_raw`: 4,603,674 rows, 5,252 instruments
- `ownership_insiders_observations`: 5,578,431 rows, 4,995 instruments
- `ownership_institutions_observations`: 10,840,219 rows, 4,561 instruments
- `ownership_blockholders_observations`: 44,767 rows, 4,943 instruments
- `ownership_treasury_observations`: 60,804 rows, 1,859 instruments
- `ownership_def14a_observations`: 125,804 rows, 3,635 instruments
- `ownership_funds_observations`: 3,714,118 rows, 4,101 instruments
- `ownership_esop_observations`: 91 rows, 41 instruments
- `finra_short_interest_observations`: 186,983 rows, 6,185 instruments
- `research_price_series`: 30,591 rows, 5,892 instruments
- `research_price_daily`: 75,972,649 rows
- `research_price_quarantine_coverage`: 30,572 rows
- `instrument_universe_membership`: 12,732 rows, 12,732 instruments
- `instrument_symbol_history`: 12,862 rows, 12,741 instruments
- `external_identifiers`: 25,031 rows, 6,069 instruments

## Consequence

The public-date filter correctly ignores a genuinely later filing, but the production writer overwrites a pre-decision natural key and the prior bytes cannot be recovered. The other source families fail at least one independently probed public-clock, system-version, population/identity or causal-transform condition.

There is no admissible historical ranking field under the current contracts. Tier 2 arms are blocked and remain unmeasured; no return, haircut, cost or benchmark claim was produced.
