# sec_bulk_head fixtures

Recorded SEC HEAD response headers for bulk archives.

Used by `tests/test_sec_bulk_etag_reuse.py::TestRecordedHeadFixture` to
pin the ETag-keyed reuse contract against the exact header shape SEC
returns. Headers only — no bodies — because the multi-GB archives are
out of scope for the fixture dir.

## Recording procedure

```
curl -sI -A "eBull dev@example.com" <url> > headers.txt
```

Then extract `etag`, `content-length`, `content-type`, `last-modified`
into a `<name>.headers.json` file (lowercased keys).

## Empirical (2026-05-22)

For `submissions.zip` (https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip):

- `etag: "504b124e9474334e889e9e525db95c14-184"` (stable, S3-backed)
- `If-None-Match` header sent by client → SEC ignores; returns
  `200 + full body` regardless. This is why the reuse path uses
  client-side HEAD ETag comparison and a conditional GET.
