# kenneth_french

**Class.** Public factor reference.

**Form / endpoint.** Kenneth French Data Library monthly ZIP archives.

## 1. Origin

Official Dartmouth URLs for `F-F_Research_Data_5_Factors_2x3_CSV.zip` and
`F-F_Momentum_Factor_CSV.zip`; one ZIP containing one CSV. Source definitions
and HTTP calls are in `app/services/reference_data.py`.

## 2. Watermarking model

Latest accepted immutable snapshot at the current parser version supplies
ETag/Last-Modified. Identity includes source, dataset, response SHA-256 and
parser version. A revised payload creates a new snapshot.

## 3. Retry posture

304 is a successful no-op. Other HTTP errors raise to the scheduler. Empty,
invalid ZIP/UTF-8, wrong/ragged headers, duplicate dates and invalid numbers
reject the whole snapshot while retaining its raw bytes and parse error.

## 4. Bootstrap path

No bootstrap stage. First post-bootstrap catch-up invokes the bounded refresh.

## 5. Steady-state path

`french_reference_refresh`, monthly day 10 at 03:05 UTC, on the shared
`reference_data` lane.

## 6. Manifest insert

No SEC manifest. `reference_data_snapshots` is the raw/provenance manifest;
raw bytes commit before parsing and accepted/rejected state is explicit.

## 7. Parser

`parse_french_monthly_zip`, version `kenneth-french-monthly-csv-v2`. It pins
each dataset's exact series header, reads only the monthly section, converts
percent to decimal return, and omits/counts French missing sentinels.

## 8. Observation insert

`reference_data_observations`, PK `(snapshot_id, series_key,
observation_date)`, FK RESTRICT to immutable snapshots. No tombstones or
updates; source revisions are new snapshots.

## 9. Current table refresh

No mutable current table. Readers choose the latest accepted snapshot for the
dataset and parser identity.

## 10. Operator-visible endpoint

No public API endpoint. Operator CLI:
`python -m scripts.refresh_2912_reference_data --source french --verify`.

## 11. Verification queries

```sql
SELECT dataset_key, response_sha256, parser_version, row_count,
       first_observation, last_observation
FROM reference_data_snapshots
WHERE source='kenneth_french' AND parse_status='accepted'
ORDER BY snapshot_id DESC;
```

Cross-source verification is the French/AQR control in
`python -m scripts.report_2912_factor_validation`.

## 12. Smoke test

`tests/test_reference_data.py` exercises source-shaped ZIPs, percentage units,
sentinels, raw retention and idempotence. The source-doc registry checks this
file and all 13 sections.

## 13. Known gotchas

The archive contains prose and annual data after the monthly table. Percent
values are not decimal returns until normalized. A parser change requires a
version bump and unconditional reparse; never rewrite an accepted snapshot.
