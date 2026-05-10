# Runbook: diagnosing a missing SEC CIK

Use when an instrument's chart / ownership / fundamentals / filings
panes are empty AND the issue is suspected to be a missing SEC CIK
mapping in `external_identifiers`.

## TL;DR

1. Hit `GET /coverage/cik-gap` (#1067) — operator-visible audit.
2. If the instrument is in `unmapped_suffix_variants`: it's an
   operational duplicate (`.RTH`, `.US`, `.CVR`) and SHOULD NOT have
   its own CIK row. Verify the canonical-redirect mechanism (#819)
   is wired and the canonical row has a CIK.
3. If in `unmapped_other`: investigate further (steps below).

## Background

SEC CIK = entity-level identifier. The bridge that maps a ticker to a
CIK is SEC's `company_tickers.json`, fetched daily by
`daily_cik_refresh`. The producer cohort is
`is_tradable=TRUE AND exchanges.asset_class='us_equity'`.

What SHOULDN'T have a CIK row:

- Operational-duplicate variants (`AAPL.RTH`, `AAPL.US`,
  `AAPL.24-7`, `ACLX.CVR`). Per the partial-unique CIK index in
  `sql/143`, only ONE instrument can claim a given CIK; the
  underlying row (e.g. `AAPL`) wins. Variants render via the
  canonical-redirect path (#819) — they should not need their own.
- Crypto / FX / non-US equity instruments — `daily_cik_refresh`
  filters them out of the cohort entirely.
- ETFs and funds — `company_tickers.json` doesn't include most
  registered funds. SEC fund filings live under separate forms
  (N-PORT, N-CSR). Use the funds-side ingest (#917) for those.

What SHOULD have a CIK row:

- Every common-share US equity instrument with an active listing.
- Share-class siblings (`GOOG`/`GOOGL`, `BRK.A`/`BRK.B`). Pre-#1102
  these flapped between siblings; post-#1102 both rows hold the
  shared CIK simultaneously via the partial unique index.

## Diagnostics

### Step 1 — read the audit

```bash
curl -s -H "Cookie: <operator session>" \
  http://localhost:8000/coverage/cik-gap | jq .
```

Returned shape (truncated):

```json
{
  "checked_at": "...",
  "cohort_total": 7151,
  "mapped": 5080,
  "unmapped": 2071,
  "unmapped_suffix_variants": 1900,
  "unmapped_other": 171,
  "sample": [
    {
      "instrument_id": 1023,
      "symbol": "AAXJ",
      "company_name": "iShares MSCI All Country Asia ex Japan ETF",
      "category": "other"
    },
    ...
  ]
}
```

- `cohort_total - mapped == unmapped`. If `unmapped_suffix_variants`
  dominates, the daily refresh is working as expected and
  operational duplicates are the noise floor.
- `unmapped_other` is the actionable count.

### Step 2 — categorise a single instrument

For the instrument in question (let's say `EXAMPLE`):

```sql
SELECT i.symbol, i.company_name, e.asset_class,
       ei.identifier_value AS primary_cik
  FROM instruments i
  JOIN exchanges e ON e.exchange_id = i.exchange
  LEFT JOIN external_identifiers ei
    ON ei.instrument_id = i.instrument_id
   AND ei.provider = 'sec'
   AND ei.identifier_type = 'cik'
   AND ei.is_primary = TRUE
 WHERE UPPER(i.symbol) = 'EXAMPLE';
```

Read the result:

- `asset_class != 'us_equity'` → out of cohort by design. Cannot
  have a SEC CIK from `company_tickers.json`.
- `primary_cik IS NULL` AND `asset_class = 'us_equity'` AND the
  symbol contains a `.` (e.g. `AAPL.RTH`) → operational duplicate.
  Once #819 lands, query
  `SELECT inst_can.symbol FROM instruments i JOIN instruments inst_can ON inst_can.instrument_id = i.canonical_instrument_id WHERE UPPER(i.symbol) = 'AAPL.RTH'`
  to confirm the canonical row carries the CIK.
- `primary_cik IS NULL` AND `asset_class = 'us_equity'` AND symbol
  contains no `.` → genuine gap; proceed to Step 3.

The `is_primary = TRUE` filter is load-bearing — demoted historical
CIK rows still live in `external_identifiers` and would otherwise
falsely report a mapping for an instrument whose current SEC
linkage is broken.

### Step 3 — confirm SEC has the ticker

Hit SEC's `company_tickers.json` directly:

```bash
curl -s -H "User-Agent: <SEC user agent>" \
  https://www.sec.gov/files/company_tickers.json | \
  jq '.[] | select(.ticker == "EXAMPLE")'
```

If SEC has it, `daily_cik_refresh` should pick it up on the next
fire. To force immediately:

```bash
curl -s -X POST -H "Cookie: <operator session>" \
  http://localhost:8000/jobs/daily_cik_refresh/run
```

Poll `/jobs/requests` for the run to finish. Re-check Step 2.

If SEC does NOT have the ticker → the issuer is genuinely off the
SEC bridge. Possible reasons:

- Former ticker (delisted) — the issuer may have changed name /
  ticker; look at the issuer's submissions filing directly.
- ADR / foreign issuer — some ADRs do appear in
  `company_tickers.json` but not all.
- Pre-IPO / private — no SEC filings yet.

For genuine gaps that cannot be resolved via the bridge, manually
insert the CIK after independent verification:

```sql
INSERT INTO external_identifiers
  (instrument_id, provider, identifier_type, identifier_value,
   is_primary, last_verified_at)
VALUES (<instrument_id>, 'sec', 'cik', '<10-digit CIK>',
        TRUE, NOW())
ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
  WHERE provider = 'sec' AND identifier_type = 'cik'
DO UPDATE SET is_primary = TRUE, last_verified_at = NOW();
```

Then trigger `daily_research_refresh` so the new CIK starts driving
fundamentals + filings ingest.

## Cross-references

- Settled decision: `docs/settled-decisions.md` — "CIK = entity,
  CUSIP = security (#1102)" + "Canonical-instrument redirect
  (#819)".
- Data-engineer skill: `.claude/skills/data-engineer/SKILL.md` §11
  (integrity reference matrix for `external_identifiers`).
- SEC EDGAR skill: `.claude/skills/data-sources/sec-edgar.md` §5.1
  (`company_tickers.json` shape + caveats).
- Related runbooks:
  `docs/wiki/runbooks/runbook-after-parser-change.md`,
  `docs/wiki/runbooks/runbook-data-freshness.md`.
