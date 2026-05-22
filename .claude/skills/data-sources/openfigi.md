# OpenFIGI — source-of-truth note

> Status: introduced 2026-05-22 alongside PR-0 of issue #1233 (bootstrap-etl-optimisation v3).
> Approved as the CUSIP-resolver fallback when SEC's 13F Official List name-fuzzy fails.
> Cross-reference: `docs/settled-decisions.md` → "OpenFIGI as approved external CUSIP-resolver fallback".

## When to use

- **CUSIP → ticker reverse resolution** for the bulk-ingest sweep (`cusip_resolver_post_bulk_sweep` stage S13 post PR-1b).
- Bulk-recovery of `unresolved_13f_cusips` rows that the SEC 13F Official List fuzzy-name path could not bridge to an existing `instruments.symbol`.

## When NOT to use

- **ticker → CUSIP** flow. The response payload does NOT contain the CUSIP field that was queried — only ticker / FIGI / exchange / security-type. eBull is permitted to call CUSIP→ticker; the inverse is forbidden.
- Per-filing real-time lookups during ingest (use the bulk sweep stage instead — single calls burn the per-minute budget).
- Inside a SEC-budgeted code path. OpenFIGI lives on its own host (`api.openfigi.com`); use the `openfigi` Lane (added in PR-1b) and never the `sec_rate` Lane.

## 1. Endpoint contract (probed 2026-05-22, unkeyed tier)

| Field | Value |
|---|---|
| Base URL | `https://api.openfigi.com/v3/mapping` |
| Method | `POST` |
| Content-Type | `application/json` |
| Auth header (keyed) | `X-OPENFIGI-APIKEY: <key>` |
| Request body | JSON array of `{"idType": "ID_CUSIP", "idValue": "<9 char CUSIP>"}` |
| Max items per POST | 10 (unkeyed) / 100 (keyed) |

The contract is positional — response is an array parallel to request items, indexed left-to-right.

## 2. Rate limits

| Tier | Per window | Window | Max items / POST | Mappings / min |
|---|---|---|---|---|
| Unkeyed | 25 requests | 60s | 10 | 250 |
| Keyed | 25 requests | 6s | 100 | 25,000 |

**Probed (unkeyed):** 22 successful calls inside one rolling 60s window before tripping 429. Consistent with the documented 25/min ceiling minus startup token-bucket warmup. Empirical headers on every response:

```
ratelimit-limit: 25
ratelimit-policy: 25;w=60
ratelimit-remaining: <decreasing-int>
ratelimit-reset: <seconds-to-window-reset>
```

These headers are the canonical signal — prefer reading `ratelimit-remaining` over counting locally. They are **lower-cased** by httpx (RFC 7230 §3.2 makes header names case-insensitive but most clients normalise to lower).

## 3. 429 behaviour (recorded)

When the bucket empties:

```
status: 429
headers:
  retry-after: 58
  ratelimit-limit: 25
  ratelimit-policy: 25;w=60
  ratelimit-remaining: 0
  ratelimit-reset: 58
body (NOT JSON):
  "Too many requests, please try again later."
```

Important: the 429 body is plain text, NOT JSON. The PR-1b resolver MUST:

1. Branch on `status_code == 429` BEFORE attempting `resp.json()`.
2. Honour `Retry-After` (in seconds). The value matches `ratelimit-reset` in practice but ONLY `Retry-After` is the canonical IETF retry signal.
3. After backoff, retry ONCE; if the next call still 429s, surface the failure to the caller (no infinite retry loop).

## 4. Per-row response shape

### 4.1 Successful lookup

```json
{
  "data": [
    {
      "compositeFIGI": "BBG000B9XRY4",
      "exchCode": "US",
      "figi": "BBG000B9XRY4",
      "marketSector": "Equity",
      "name": "APPLE INC",
      "securityDescription": "AAPL",
      "securityType": "Common Stock",
      "securityType2": "Common Stock",
      "shareClassFIGI": "BBG001S5N8V8",
      "ticker": "AAPL"
    },
    ...254 more entries (every cross-listing, ADR, composite ticker)...
  ]
}
```

**Gotcha:** the `data` array can be ENORMOUS. AAPL `037833100` returns 255 entries — every regional listing, every depositary receipt, every share-class FIGI. The first entry is empirically the US-primary common-stock listing (`exchCode='US'`, `securityType='Common Stock'`). PR-1b's defensive filter:

```python
def _pick_primary(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    # Prefer US common stock by composite FIGI; fall back to first entry.
    for e in entries:
        if e.get("exchCode") == "US" and e.get("securityType") == "Common Stock":
            return e
    return entries[0] if entries else None
```

Do NOT trust `data[0]` blindly without the filter — future API changes may reorder.

### 4.2 Not-found

```json
{"warning": "No identifier found."}
```

Single key `warning`. No `error` key. No `data` key. Probed against `000000000`.

### 4.3 Other observed entry shapes

(None in the probe set. SEC has documented behaviour for malformed `idType` values returning `{"error": "..."}` — PR-1b should defensively check for `error` AND `warning` AND missing `data` and treat any of those three as "no result".)

## 5. eBull integration points (post PR-1b)

| Concern | Location |
|---|---|
| Resolver class | `app/services/openfigi_resolver.py` (PR-1b) |
| API-key env var | `OPENFIGI_API_KEY` (read by resolver `__init__`) |
| Lane | `Lane = Literal[..., "openfigi"]` in `app/jobs/sources.py` (PR-1b) |
| Sweep job | `cusip_resolver_post_bulk_sweep` stage S13 (PR-1b) |
| Persistence | `external_identifiers (provider='openfigi', identifier_type='cusip', is_primary=FALSE)` |
| `_load_cusip_map` filter | `WHERE provider IN ('sec', 'openfigi') AND identifier_type='cusip'` (PR-1b extension to `sec_13f_dataset_ingest.py` + `sec_nport_dataset_ingest.py` + `bootstrap_preconditions.py`) |

OpenFIGI-derived rows go into `external_identifiers` with `provider='openfigi'`, **not** `provider='sec'`. The two-provider union pattern in `_load_cusip_map` is the canonical reader gate.

## 6. Sample payload

Request (1 item):

```json
[{"idType": "ID_CUSIP", "idValue": "037833100"}]
```

Response (`single_aapl.json` truncated; full file in `tests/fixtures/openfigi/`):

```json
[
  {
    "data": [
      {"ticker": "AAPL", "name": "APPLE INC", "exchCode": "US", "securityType": "Common Stock", "figi": "BBG000B9XRY4", "compositeFIGI": "BBG000B9XRY4", "shareClassFIGI": "BBG001S5N8V8", "marketSector": "Equity", "securityDescription": "AAPL", "securityType2": "Common Stock"},
      ...
    ]
  }
]
```

## 7. Gotchas

### 7.1 The probe burns rate-limit budget

`scripts/probe_openfigi.py` issues 4 + 30 = up to 34 POSTs unkeyed. After a full run the unkeyed account is rate-limited for ~60s. CI must NOT run the probe; only operator-driven refreshes are appropriate. Tests under `tests/test_openfigi_fixtures.py` validate stored fixtures with zero HTTP calls.

### 7.2 ToS posture

OpenFIGI free tier permits programmatic use within rate limits. eBull's operator approved the integration in SD-1 (2026-05-22). Do NOT increase polling beyond the documented `Lane` budgets without re-checking ToS.

### 7.3 Response does NOT contain the queried CUSIP

The request body has `idValue=<cusip>`; the response entry does NOT echo that back. Indexing relies on the parallel-array contract:

```python
for cusip, entry in zip(request_cusips, response_array, strict=True):
    ...
```

`strict=True` is non-negotiable — without it, a future API change to inject `null` placeholders would silently re-align cusip→entry pairs.

### 7.4 OpenFIGI sometimes returns multiple `data` entries for SAME composite

A CUSIP can map to several FIGI rows that share `compositeFIGI` (e.g. one row per `exchCode`). For ticker resolution, we want the US-primary row (`exchCode='US'`); for FIGI resolution either composite or share-class FIGI is canonical. eBull stores ticker only (`identifier_type='cusip'` row keyed by `identifier_value=<cusip>` mapping to the US-primary `instrument_id` via `instruments.symbol=ticker`).

### 7.5 Pink-sheet / OTC tickers

OpenFIGI returns OTC tickers under their own `exchCode` (e.g. `'OPRA'`, `'PINX'`). The defensive `_pick_primary` filter above intentionally selects `'US'` (the SEC-registered composite exchange code) to avoid binding ownership rows to OTC mirrors that may not exist in `instruments`. Operator action if no `US`-row exists: the sweep records `name` + `cusip` to `unresolved_13f_cusips` with a follow-up `partial_data_reason='openfigi_no_us_listing'`.

## 8. Operator runbook

### 8.1 Refresh the recorded fixtures

```bash
uv run python scripts/probe_openfigi.py
# Optionally:
OPENFIGI_API_KEY=... uv run python scripts/probe_openfigi.py
```

Probe is idempotent — fixtures overwrite atomically. The summary table prints to stdout; the 429 capture prints "tripped 429 on iteration N/30" to stderr.

### 8.2 Verify a single CUSIP manually

```bash
curl -s -X POST https://api.openfigi.com/v3/mapping \
  -H "Content-Type: application/json" \
  -d '[{"idType":"ID_CUSIP","idValue":"037833100"}]' | jq '.[0].data[0].ticker'
# "AAPL"
```

### 8.3 Obtain an API key

Sign up at <https://www.openfigi.com/api> and provision an API key. Set `OPENFIGI_API_KEY` in eBull's environment to switch the resolver to the keyed tier (250× the unkeyed throughput).

## 9. Cross-references

- `docs/settled-decisions.md` → "OpenFIGI as approved external CUSIP-resolver fallback (2026-05-22)" — the SD-1 entry that gates this integration.
- `docs/superpowers/specs/2026-05-22-bootstrap-etl-optimisation-v3.md` §2 — the PR-0 introduction context; §5 — the PR-1b resolver shape.
- `.claude/skills/data-sources/sec-edgar.md` §5 (CUSIP → CIK bridge) — the upstream bridge OpenFIGI complements when 13F Official List name-fuzzy fails.
- `tests/fixtures/openfigi/README.md` — the recorded fixtures with full request/response payloads.
