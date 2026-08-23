# R6 point-in-time spine result (#2900)

Verdict: **FAIL — NO ADMISSIBLE HISTORICAL FIELD**

Declaration SHA-256: `369b397e17694f2a54b07897b4d68a8728bf0624e35d00ced8a6ce833bb4da20` at commit `51e55d58823a5ebe98e5eea6473895b9d05abc1d`.
Correction-1 SHA-256: `b101123c59183b8204a70b98a9c40b25e350a4fc58a9c114d8ea76735157cff6` at commit `fe67f100e565fd52db8201ac6ad8f1758c2b163f`.
Correction-2 SHA-256: `a330ed170d39da3c201e2bd8e1ce5f80566209a356faa75bb899090e5e2b4f32` at commit `055056a5c3b1aa3bc5971de4a6085b0f7bd72206`.
Execution commit: `0a47d341a410fc9053f83183223d822d5f7efd1f`. Registry: `r6-pit-registry-v1+73aa1335b55e`.
Reproduce: `PYTHONPATH=. uv run python -m scripts.verify_2900_point_in_time --format markdown`

## Adversarial leak test

- Decision date: `2020-01-15` (NYSE session).
- Baseline hash: `b62d107f6af98a224f4f663c8f9ee1ca3ed6cb85ae0150f974e139c7b8b766d1`.
- Post-decision insert hash: `b62d107f6af98a224f4f663c8f9ee1ca3ed6cb85ae0150f974e139c7b8b766d1` (unchanged).
- Same-key pre-decision overwrite hash: `40bd40394974d4b1391bb6d8a6263e963b963da4fc691d7c6369088de9686e5a` (changed).
- First unequal field: `ingest_run_id`: `00000000-0000-0000-0000-000000002900` → `00000000-0000-0000-0000-000000002902`.
- Rows recoverable at the old `ingested_at` after overwrite: 0.
- Rollback proved on a new connection: `true`.

## Contract evidence

- Successful non-vacuous probes: D0, D1, F0, F1, F2, H1, H2, H3, L1, N0, N1, N2, O0, O1, O2, O3, P0, P1, P2, P3, P4, P5, R0, R1, X1.
- Refused registry families: derived_fundamentals, dimensional_xbrl, filing_red_flags, finra_short_interest, fundamental_facts, historical_population, live_etoro_state, ownership_observations, research_prices.
- Exact census tables: financial_facts_raw, ownership_insiders_observations, ownership_institutions_observations, ownership_blockholders_observations, ownership_treasury_observations, ownership_def14a_observations, ownership_funds_observations, ownership_esop_observations, finra_short_interest_observations, research_price_series, research_price_daily, research_price_quarantine_coverage, instrument_universe_membership, instrument_symbol_history, external_identifiers.
- Full probe anchors, source hashes, registry matrix and census values are emitted by `--format json`.

## Population census

- `financial_facts_raw`: `row_count`=4603674, `distinct_instruments`=5252, `min_filed_date`=2009-05-07, `max_filed_date`=2026-08-21, `min_period_end`=2006-06-30, `max_period_end`=2034-06-30
- `ownership_insiders_observations`: `row_count`=5578431, `distinct_instruments`=4995, `min_filed_at`=2006-01-03T00:00:00+00:00, `max_filed_at`=2026-08-22T01:38:40+00:00, `min_period_end`=0001-01-01, `max_period_end`=2047-05-24, `min_ingested_at`=2026-06-03T21:47:34.700519+00:00, `max_ingested_at`=2026-08-23T05:34:31.691900+00:00, `min_known_from`=2026-06-03T19:16:27.134703+00:00, `max_known_from`=2026-08-23T05:20:37.841309+00:00, `min_known_to`=2026-06-24T03:23:52.998553+00:00, `max_known_to`=2026-07-22T16:52:21.983223+00:00
- `ownership_institutions_observations`: `row_count`=10840219, `distinct_instruments`=4561, `min_filed_at`=2006-08-05T00:00:00+00:00, `max_filed_at`=2026-08-21T00:00:00+00:00, `min_period_end`=2024-06-30, `max_period_end`=2026-06-30, `min_ingested_at`=2026-06-03T18:58:08.029620+00:00, `max_ingested_at`=2026-08-23T06:50:16.877605+00:00, `min_known_from`=2026-06-03T18:57:15.047265+00:00, `max_known_from`=2026-08-22T07:06:06.777149+00:00, `min_known_to`=None, `max_known_to`=None
- `ownership_blockholders_observations`: `row_count`=44767, `distinct_instruments`=4943, `min_filed_at`=2024-12-18T00:00:00+00:00, `max_filed_at`=2026-08-22T01:44:20+00:00, `min_period_end`=2024-12-18, `max_period_end`=2026-08-22, `min_ingested_at`=2026-06-14T15:30:10.273379+00:00, `max_ingested_at`=2026-08-23T06:51:44.780049+00:00, `min_known_from`=2026-06-14T14:20:08.744706+00:00, `max_known_from`=2026-08-23T02:35:43.656959+00:00, `min_known_to`=2026-06-15T00:07:39.414654+00:00, `max_known_to`=2026-06-15T00:07:39.440312+00:00
- `ownership_treasury_observations`: `row_count`=60804, `distinct_instruments`=1859, `min_filed_at`=2009-04-23T00:00:00+00:00, `max_filed_at`=2026-08-21T00:00:00+00:00, `min_period_end`=2009-03-31, `max_period_end`=2026-12-31, `min_ingested_at`=2026-06-21T03:04:53.110601+00:00, `max_ingested_at`=2026-08-23T06:52:01.846275+00:00, `min_known_from`=2026-06-03T19:22:16.736325+00:00, `max_known_from`=2026-08-23T02:35:30.268216+00:00, `min_known_to`=None, `max_known_to`=None
- `ownership_def14a_observations`: `row_count`=125804, `distinct_instruments`=3635, `min_filed_at`=2026-06-04T16:10:00.071067+00:00, `max_filed_at`=2026-08-23T02:30:17.087192+00:00, `min_period_end`=1990-10-31, `max_period_end`=2026-12-31, `min_ingested_at`=2026-06-21T03:05:04.453991+00:00, `max_ingested_at`=2026-08-23T06:52:34.939864+00:00, `min_known_from`=2026-06-04T03:45:00.074895+00:00, `max_known_from`=2026-08-23T02:30:17.087192+00:00, `min_known_to`=2026-07-24T22:36:21.016735+00:00, `max_known_to`=2026-08-07T17:20:01.957715+00:00
- `ownership_funds_observations`: `row_count`=3714118, `distinct_instruments`=4101, `min_filed_at`=2024-03-28T20:10:19+00:00, `max_filed_at`=2026-07-28T20:55:43+00:00, `min_period_end`=2024-07-31, `max_period_end`=2026-11-30, `min_ingested_at`=2026-06-03T21:10:00.048023+00:00, `max_ingested_at`=2026-07-28T21:05:04.258198+00:00, `min_known_from`=2026-06-03T18:57:15.046982+00:00, `max_known_from`=2026-07-28T21:05:04.258198+00:00, `min_known_to`=None, `max_known_to`=None
- `ownership_esop_observations`: `row_count`=91, `distinct_instruments`=41, `min_filed_at`=2026-06-14T02:51:59.497312+00:00, `max_filed_at`=2026-07-30T18:25:06.121739+00:00, `min_period_end`=2016-04-20, `max_period_end`=2026-07-30, `min_ingested_at`=2026-06-21T03:05:07.205460+00:00, `max_ingested_at`=2026-08-23T06:52:34.628429+00:00, `min_known_from`=2026-06-14T02:39:03.202685+00:00, `max_known_from`=2026-07-30T18:09:16.199090+00:00, `min_known_to`=2026-07-25T13:58:55.931404+00:00, `max_known_to`=2026-07-30T17:39:10.695219+00:00
- `finra_short_interest_observations`: `row_count`=186983, `distinct_instruments`=6185, `min_filed_at`=2024-01-12T00:00:00+00:00, `max_filed_at`=2026-07-31T00:00:00+00:00, `min_period_end`=2024-01-12, `max_period_end`=2026-07-31, `min_known_from`=2026-06-04T12:00:04.028357+00:00, `max_known_from`=2026-08-23T12:00:01.079207+00:00, `min_settlement_date`=2024-01-12, `max_settlement_date`=2026-07-31
- `research_price_series`: `row_count`=30591, `distinct_instruments`=5892, `min_first_bar`=1962-01-02, `max_first_bar`=2026-07-06, `min_last_bar`=2013-06-21, `max_last_bar`=2026-08-21, `min_created_at`=2026-08-05T08:59:09.414883+00:00, `max_created_at`=2026-08-12T17:23:20.271864+00:00
- `research_price_daily`: `row_count`=75972649, `min_bar_date`=1962-01-02, `max_bar_date`=2026-08-21
- `research_price_quarantine_coverage`: `row_count`=30572, `min_first_bar`=1962-01-02, `max_first_bar`=2026-07-06, `min_last_bar`=2013-06-21, `max_last_bar`=2026-07-08, `min_evaluated_at`=2026-08-05T09:03:36.068897+00:00, `max_evaluated_at`=2026-08-14T11:08:21.119140+00:00
- `instrument_universe_membership`: `row_count`=12732, `distinct_instruments`=12732, `min_effective_from`=2026-08-10, `max_effective_from`=2026-08-21, `min_effective_to`=2026-08-10, `max_effective_to`=2026-08-13
- `instrument_symbol_history`: `row_count`=12862, `distinct_instruments`=12741, `min_effective_from`=2026-06-03, `max_effective_from`=2026-08-21, `min_effective_to`=2026-06-12, `max_effective_to`=2026-08-21
- `external_identifiers`: `row_count`=25031, `distinct_instruments`=6069, `min_created_at`=2026-06-03T18:56:10.213323+00:00, `max_created_at`=2026-08-23T05:12:07.883776+00:00, `min_last_verified_at`=2026-06-03T18:56:56.241482+00:00, `max_last_verified_at`=2026-08-23T02:30:00.249466+00:00

## Consequence

The public-date filter correctly ignores a genuinely later filing, but the production writer overwrites a pre-decision natural key and the prior bytes cannot be recovered. The other source families fail at least one independently probed public-clock, system-version, population/identity or causal-transform condition.

There is no admissible historical ranking field under the current contracts. Tier 2 arms are blocked and remain unmeasured; no return, haircut, cost or benchmark claim was produced.
