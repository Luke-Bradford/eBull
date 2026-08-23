# Reference factor and macro data ETL (#2912)

Status: approved implementation contract for the #2912 branch.

## §0 Grep proof

Generated 2026-08-23 on `feature/2912-reference-factor-validation` at
`7f71c9cf`, before implementation. Output is reproduced verbatim.

```text
$ rg -n "reference_data_snapshots|reference_data_observations" app sql tests docs
(no matches)
$ rg -n '^Lane = Literal\\[' app/jobs/sources.py app/services/bootstrap_state.py
app/jobs/sources.py:62:Lane = Literal[
app/services/bootstrap_state.py:52:Lane = Literal[
$ rg -n '"cboe"|JOB_CBOE_VIX_REFRESH' app/jobs/sources.py app/workers/scheduler.py app/jobs/runtime.py | tail -20
app/jobs/runtime.py:93:    JOB_CBOE_VIX_REFRESH,
app/jobs/runtime.py:380:    JOB_CBOE_VIX_REFRESH: _adapt_zero_arg(cboe_vix_refresh),
app/jobs/sources.py:101:    "cboe",
app/workers/scheduler.py:413:JOB_CBOE_VIX_REFRESH = "cboe_vix_refresh"
app/workers/scheduler.py:2221:        name=JOB_CBOE_VIX_REFRESH,
app/workers/scheduler.py:2223:        source="cboe",
app/workers/scheduler.py:5630:    with _tracked_job(JOB_CBOE_VIX_REFRESH) as tracker:
$ rg -n "INSERT INTO reference_data_snapshots|INSERT INTO reference_data_observations" --glob '*.py' --glob '*.sql' .
(no matches)
$ rg -n "FROM reference_data_snapshots|JOIN reference_data_snapshots|FROM reference_data_observations|JOIN reference_data_observations" --glob '*.py' .
(no matches)
```

The new tables therefore have no pre-existing writers, readers, PK, FK or
CHECK vocabulary to preserve. Migration 369 creates their first contract.

## 1 Decisions

eBull retains immutable raw responses and typed normalized observations for
Kenneth French, AQR and FRED. Three separately invokable scheduled refreshes
share one low-volume `reference_data` overlap lane. A rejected parser response
remains retained with its error, while only accepted snapshots feed readers.
No endpoint or UI exposes raw copyrighted responses.

## 2 Identifiers + identity drift

Identity is `(source, dataset_key, response_sha256, parser_version)` for a raw snapshot and
`(snapshot_id, series_key, observation_date)` for an observation. Provider
series names are retained verbatim in `series_key`; aliases are not resolved.
Historical provider revisions create a new immutable snapshot rather than
mutating observations from an older response.

## 3 Endpoint surface

All are HTTP GET, single-response and unpaginated:

- French five factors (source-shaped fixture built in `tests/test_reference_data.py`):
  `https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip`;
  generated ZIP fixture.
- French momentum:
  `https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip`;
  generated ZIP fixture.
- AQR monthly VME factors:
  `https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Value-and-Momentum-Everywhere-Factors-Monthly.xlsx`;
  generated workbook fixture.
- FRED `DGS3MO` and `USREC`:
  `https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}`;
  generated CSV fixtures.

## 4 Schema

`reference_data_snapshots` stores source/dataset/url, fetch headers, exact
`BYTEA` payload, SHA-256, parser version, parse status/error and accepted
coverage/count. It has a BIGSERIAL PK, one unique identity index and one partial
latest-accepted lookup index: three total.
`reference_data_observations` stores snapshot FK, series key, date, NUMERIC
value and a closed unit; its composite PK is its only index. The FK is
`ON DELETE RESTRICT`. UTF-8 applies to text, timestamps are UTC, dates are
provider observation dates, and numbers are Decimal-backed NUMERIC.

## 5 Fetch strategy + rate-limit composition

Each dataset is a bounded `bulk_archive`/static-file GET. The three jobs share
the `reference_data` JobLock lane, conservatively serializing public-host
traffic. Each invocation makes two French, one AQR, or two FRED requests.

## 6 Conditional-GET semantics

The latest accepted snapshot at the current parser version supplies ETag and
Last-Modified headers. HTTP 304 is a successful no-op. A parser-version change
omits conditions so unchanged bytes can be reprocessed under the new contract.

## 7 Retry posture per error-class

HTTP errors raise after the single bounded request and the scheduler records a
failed run; the next due/catch-up invocation retries. 304 is benign. 4xx other
than 304 are failures, including a disappeared source URL. Malformed ZIP/XLSX,
wrong headers, duplicate keys/dates, missing sentinels presented as values,
non-finite numbers or an empty accepted series reject parsing loudly.

## 8 Multi-writer sink registry

N/A — the new sink has one writer module,
`app/services/reference_data.py`. No existing multi-writer sink is touched.

## 9 Watermark + retry-budget

Immutable snapshots are the watermark. The latest accepted snapshot for the
same parser supplies conditional headers. A unique response hash makes retry
idempotent. A raw row is committed before parsing; rejected bytes remain
diagnosable and can be retried after a parser-version bump.

## 10 Encoding / precision / NULL / timezone

French CSV decodes UTF-8 with BOM tolerance; FRED is UTF-8; XLSX is parsed as
OOXML. Provider values use Decimal and persist as NUMERIC. French percent
returns normalize to decimal returns; AQR stays decimal; DGS3MO stays percent
per annum; USREC stays binary. Missing values are omitted and counted, never
stored as zero. Fetch times are timezone-aware UTC.

## 11 Backfill horizon + retention

The complete provider history is retained because the payloads are small and
historical revisions are the audit subject. Each distinct raw payload is kept;
identical responses deduplicate. Expected storage is well below 10 MB per
annual snapshot cycle. A future retention change requires a separate measured
policy and must preserve every snapshot cited by a validation report.

## 12 Partition strategy + extension deadline

N/A — neither small table is partitioned, so there is no extension deadline.

## 13 Bootstrap vs steady-state mode

No bootstrap stage. Like the existing Cboe reference job, first boot catch-up
performs the bounded full-history refresh after bootstrap completes. This
avoids adding HTTP to the derivation-only bootstrap DAG. Steady state is daily
for FRED and monthly for French/AQR; conditional responses make unchanged runs
cheap.

## 14 Tombstones + soft-delete

Nothing is hard-deleted. Upstream revisions are new snapshots. A rejected raw
response is retained with `parse_status='rejected'`; it never becomes current.
Rows removed by an upstream revision remain present in prior snapshots and are
absent only from the new snapshot.

## 15 `rows_skipped` closed-set + other

The parser report counts `missing_value`, `missing_sentinel`,
`outside_monthly_section` and `other`, with optional detail for `other`.
Structural defects reject the whole snapshot rather than becoming skips.

## 16 Schema-evolution migration path

Breaking parser changes bump the per-dataset parser version and force an
unconditional refetch/reparse. Normalized rows remain tied to their immutable
snapshot, allowing old/new outputs to coexist and be compared before readers
adopt the newer accepted snapshot.

## 17 Operator runbooks

`python -m scripts.refresh_2912_reference_data --source <name>` invokes one
refresh through the same service path. `--verify` is read-only and reports
snapshot hashes, coverage and counts. No DELETE/UPDATE operation exists, so a
destructive `--apply` switch is inapplicable.

## 18 Smoke matrix

Source-shaped fixtures cover French archive prose plus monthly section, AQR's
multi-row headers and missing early U.S. cells, FRED blanks, duplicates,
sentinels, bad numerics, exact first/last values, 304, rejected raw retention,
and idempotent same-hash retry. Issuer symbols are inapplicable to global
reference series.

## 19 Cross-source verification

The factor report compares eBull momentum independently with French MOM and
AQR U.S. MOM and also checks French/AQR against each other. FRED latest values
are cross-checked against the official series pages. SEC and FINRA acceptance
is evidenced from their existing source-to-sink contracts and live census.

## 20 Test placement

Pure parsers/statistics are unit tests. DB snapshot lifecycle is an integration
test. Scheduler registration and per-source documentation are smoke/registry
tests. Live HTTP is an explicit runbook/report action, never a default pytest.

## 21 Rationale log

**Decision:** immutable raw snapshots plus normalized child rows.
**Rejected:** overwrite-in-place current values — it destroys the exact input
behind a validation result and hides provider revisions.

**Decision:** one shared reference-data lane with three jobs.
**Rejected:** three new lanes — these are at most five small requests and have
no overlap pressure that justifies tripling the lock vocabulary.

**Decision:** no bootstrap stage; use post-bootstrap catch-up.
**Rejected:** an HTTP bootstrap carve-out — the data is research validation,
not a prerequisite for operating the application.

**Decision:** retain AQR XLSX and parse the named worksheet/headers.
**Rejected:** a hand-converted CSV — it severs the source response hash from
the normalized observations and makes refresh manual.

## 22 Open questions

None. A strategy consumer for FRED or non-momentum factors is intentionally out
of scope; adding one requires its own point-in-time availability contract.
