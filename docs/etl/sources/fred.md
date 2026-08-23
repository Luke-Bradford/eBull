# fred

**Class.** Public macro reference.

**Form / endpoint.** FRED graph CSV for `DGS3MO` and `USREC`.

## 1. Origin

Official no-key `fredgraph.csv?id={series}` endpoints, fetched directly by
`app/services/reference_data.py`.

## 2. Watermarking model

Per-series latest accepted snapshot supplies ETag/Last-Modified; immutable
identity includes response SHA-256 and parser version. Provider revisions
create new snapshots.

## 3. Retry posture

304 is benign; other HTTP errors raise. Wrong headers, ragged rows, bad dates,
non-finite values or non-binary USREC reject the full snapshot after raw
retention.

## 4. Bootstrap path

No bootstrap stage. A post-bootstrap catch-up performs two bounded GETs.

## 5. Steady-state path

`fred_reference_refresh`, daily at 03:25 UTC, shared `reference_data` lane.

## 6. Manifest insert

No SEC manifest. Each series response is an immutable
`reference_data_snapshots` row with an explicit parse state.

## 7. Parser

`parse_fred_csv`, `fred-csv-v1`, pins `observation_date,<series>` headers.
Blanks and `.` are counted missing; DGS3MO stays percent-per-annum and USREC
must be binary.

## 8. Observation insert

`reference_data_observations`, immutable snapshot/date natural key, NUMERIC
values and closed unit vocabulary. No zero-fill and no tombstone mutation.

## 9. Current table refresh

None; latest accepted parser-compatible snapshot is the read view.

## 10. Operator-visible endpoint

No public route. Use
`python -m scripts.refresh_2912_reference_data --source fred --verify`.

## 11. Verification queries

```sql
SELECT dataset_key, response_sha256, row_count, missing_count,
       first_observation, last_observation
FROM reference_data_snapshots
WHERE source='fred' AND parse_status='accepted'
ORDER BY dataset_key, snapshot_id DESC;
```

Spot-check last non-missing values against the official FRED series pages.

## 12. Smoke test

`tests/test_reference_data.py` covers blank values, binary enforcement,
headers and immutable lifecycle. Source-doc smoke enforces this file.

## 13. Known gotchas

DGS3MO is a daily annualized percentage, not a decimal return. USREC is
monthly. FRED may revise history; a new snapshot is expected and must not
overwrite the prior input.
