# OpenFIGI fixtures

These fixtures are the recorded output of `scripts/probe_openfigi.py`. PR-0 of
issue #1233 (bootstrap-etl-optimisation v3) captured them so PR-1b can build
the production `OpenFigiResolver` against a verified contract instead of
doc-derived guesses.

## What's in each file

| Fixture | Scenario | Status |
|---|---|---|
| `single_aapl.json` | Single CUSIP lookup for AAPL (`037833100`). | 200 |
| `batch_known_5.json` | Batch of 5 known CUSIPs (AAPL, MSFT, JPM, GME, HD). | 200 |
| `batch_with_invalid.json` | Batch of 10 with 1 deliberately invalid CUSIP (`000000000`). Confirms per-row error shape. | 200 |
| `rate_limit_429.json` | Saturated unkeyed quota → trip 429. | 429 |

Each file is the same JSON envelope:

```json
{
  "scenario": "single_aapl",
  "captured_at": "2026-05-22T...Z",
  "request": {"method": "POST", "url": "...", "body": [...]},
  "response": {"status_code": 200, "headers": {...}, "body": [...]},
  "elapsed_ms": 123
}
```

## Empirical findings (probed 2026-05-22, unkeyed tier)

- **API base URL:** `https://api.openfigi.com/v3/mapping`
- **Request shape:** POST array of `{"idType": "ID_CUSIP", "idValue": <9-char>}` objects.
- **Response shape:** parallel array, one entry per request item:
  - Success entry: `{"data": [<mapping>, ...]}` — `data` is itself an array
    of all FIGI mappings for that CUSIP across regions / exchanges / share
    classes. AAPL CUSIP `037833100` returns **255 entries** (US primary +
    every cross-listing + ADRs + composite tickers). The first `data[0]` is
    the US primary (`exchCode: "US"`, `securityType: "Common Stock"`).
  - Not-found entry: `{"warning": "No identifier found."}` — single-key, no
    `error` key for "we just don't know that CUSIP".
- **Rate-limit headers (lower-cased by httpx):**
  - `ratelimit-limit: 25` — requests per window.
  - `ratelimit-policy: 25;w=60` — IETF draft format; 25 requests per 60s.
  - `ratelimit-remaining: <N>` — decrements per call.
  - `ratelimit-reset: <seconds-until-window-reset>`.
  - On 429: `retry-after: <seconds>` (additionally to the four above).
- **429 body:** plain-text `"Too many requests, please try again later."` —
  **not JSON**. Parsers MUST handle 429 separately from the 2xx JSON path.
- **Empirically observed:** with sustained back-to-back batches we tripped
  429 on iteration 23 of 30 (so 22 requests went through the window before
  the bucket emptied — consistent with the documented 25/min ceiling).

## Tier ceilings (from docs, partially probed)

| Tier | Per-window | Window | Max jobs / POST | Mappings / min |
|---|---|---|---|---|
| Unkeyed | 25 | 60s | 10 | 250 |
| Keyed (`X-OPENFIGI-APIKEY`) | 25 | 6s | 100 | ~25,000 |

The unkeyed numbers are PROBED. The keyed numbers are doc-derived — operator
sign-off in SD-1 (`docs/settled-decisions.md`) covers them but a future probe
should verify the keyed rates if the operator obtains a key.

## Refreshing fixtures

```bash
uv run python scripts/probe_openfigi.py
```

The probe is idempotent — re-running it overwrites the fixture files in place
via atomic rename. Pass `--skip-saturation` to skip scenario (d) when you do
not want to burn the rate-limit budget.

Pass `OPENFIGI_API_KEY=...` in the environment to record keyed-tier behaviour.
The fixture filenames are unchanged; consider committing keyed fixtures under
a separate subdirectory if both tiers are needed long-term.
