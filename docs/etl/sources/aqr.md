# aqr

**Class.** Public factor reference.

**Form / endpoint.** AQR Value and Momentum Everywhere monthly XLSX.

## 1. Origin

Official AQR workbook URL, worksheet `VME Factors`, fetched and parsed in
`app/services/reference_data.py` with `openpyxl`.

## 2. Watermarking model

Latest accepted snapshot at the current parser version supplies conditional
headers. Immutable identity is source + dataset + raw SHA-256 + parser version.

## 3. Retry posture

304 is a successful no-op; other HTTP errors raise. Missing worksheet, exact
header drift, ragged rows, invalid dates/numbers or an empty accepted series
reject the snapshot after raw bytes are durable.

## 4. Bootstrap path

No bootstrap stage; bounded post-bootstrap catch-up only.

## 5. Steady-state path

`aqr_reference_refresh`, monthly day 10 at 03:15 UTC, shared
`reference_data` lane.

## 6. Manifest insert

No SEC manifest. `reference_data_snapshots` stores the exact XLSX, response
headers, SHA-256, parser version and parse outcome.

## 7. Parser

`parse_aqr_vme_monthly`, `aqr-vme-monthly-xlsx-v2`, pins the exact 23-column
header and treats `None` or blank-string cells as missing. Values already use
decimal-return units.

## 8. Observation insert

Typed series/date values write to immutable `reference_data_observations` with
a RESTRICT snapshot FK. Missing cells are counted, never zero-filled.

## 9. Current table refresh

None. Consumers select the newest accepted parser-compatible snapshot.

## 10. Operator-visible endpoint

No public API. Use
`python -m scripts.refresh_2912_reference_data --source aqr --verify`.

## 11. Verification queries

```sql
SELECT response_sha256, parser_version, parse_status, row_count, missing_count,
       first_observation, last_observation, parse_error
FROM reference_data_snapshots
WHERE source='aqr' ORDER BY snapshot_id DESC;
```

The French/AQR control regression is the independent semantic check.

## 12. Smoke test

`tests/test_reference_data.py` builds a source-shaped workbook and proves
header, missing-cell and raw lifecycle behavior; the source-doc smoke covers
this contract.

## 13. Known gotchas

The live workbook has blank-string footer rows. Parser v1 rejected and remains
recorded; v2 classifies fully blank rows correctly. Never hand-convert the XLSX
or silently accept a changed header.
