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

**Gotcha:** the `data` array can be ENORMOUS. AAPL `037833100` returns 255 entries — every regional listing, every depositary receipt, every share-class FIGI. The first entry is empirically the US-primary common-stock listing (`exchCode='US'`, `securityType='Common Stock'`). The resolver's defensive filter (`_pick_us_primary`, `openfigi_resolver.py`):

```python
def _pick_us_primary(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    # US common stock only. NO fallback — if no US-primary row exists the
    # CUSIP stays unresolved (see §7.5), never bound to an OTC/foreign mirror.
    for entry in entries:
        if entry.get("exchCode") == "US" and entry.get("securityType") == "Common Stock":
            return entry
    return None
```

Do NOT trust `data[0]` blindly without the filter — future API changes may reorder. Note the filter returns `None` (unresolved) rather than falling back to `entries[0]`.

### 4.2 Not-found

```json
{"warning": "No identifier found."}
```

Single key `warning`. No `error` key. No `data` key. Probed against `000000000`.

### 4.3 Per-item rejection — `{"error": ...}`

⚠ **This section said "None in the probe set… treated identically to `warning`" until 2026-08-06. That was wrong, and it stood behind a terminal mislabel on 14,477 distinct CUSIPs (#2304).**

```json
{"error": "Invalid idValue format."}
```

Single key `error`. **No structured error code** — the key set is exactly `{"error"}`, so a classifier over the message text is the only option available (checked before writing one).

**OpenFIGI validates the CUSIP mod-10 check digit.** Probed live 2026-08-06 against `idType=ID_CUSIP`:

| `idValue` | check digit | response entry |
|---|---|---|
| `037833100` (AAPL) | valid | `{"data": [...]}` |
| `000000000` | valid | `{"warning": "No identifier found."}` |
| `ZZZZZZZZZ` | **INVALID** | `{"error": "Invalid idValue format."}` |
| `ABC`, `03783310`, `037833100X`, `""`, `03783310@`, `037833abc` | n/a | `{"error": "Invalid idValue format…"}` |

So a **well-formed-looking 9-char uppercase-alphanumeric value is NOT enough** — `ZZZZZZZZZ` passes every shape test we apply locally and is still rejected. The discriminant is the check digit, not the character class.

⚠ **The message comes back in TWO spellings from the same endpoint in a single probe** — `"Invalid idValue format"` and `"Invalid idValue format."`. An exact-literal match is not safe; `_classify_item_error` normalises case and trailing punctuation and compares to one value.

**A rejection is NOT a no-match.** `warning` = "OpenFIGI accepted your identifier and has no mapping" (a coverage fact about the security). `error` = "OpenFIGI would not accept your identifier" (an input fact about filer data). Different owner, different remedy; collapsing them is what #2304 fixed.

**Only the RECOGNISED rejection is terminal.** An unrecognised per-item `error` — provider bug, entitlement, throttling, a shape added after 2026-08-06 — is not proven deterministic and must stay retryable. Widening the classifier recreates #2304 one layer up.

### 4.4 Outcome types (post-#2304)

`resolve_cusips` returns a **TOTAL** `dict[str, OpenFigiOutcome]` — one entry per CUSIP sent, keyed by the caller's own string. There is no "absent means unresolved" convention; absence was itself a lossy fold.

| outcome | shape that produces it | sweep writes |
|---|---|---|
| `OpenFigiMapping` | `data` with a US-primary common-stock row + non-empty `ticker` | `resolved_via_openfigi` / `openfigi_no_instrument` |
| `OpenFigiNoMatch` | `warning`, `data: []`, no US-primary row, blank ticker | `openfigi_unknown` (terminal) |
| `OpenFigiInvalidIdentifier` | recognised `Invalid idValue format` | `openfigi_invalid_identifier` (terminal — `sql/261_unresolved_13f_openfigi_invalid_identifier.sql`) |
| `OpenFigiItemError` | any other `{"error": ...}` | **nothing** — row stays NULL, retries |
| `OpenFigiMalformedEntry` | non-dict entry, non-list `data`, non-dict `data` row, no data/warning/error key | **nothing** — row stays NULL, retries |

**Cross-source check on the check-digit rule (re-measured 2026-08-07 on `13flist2026q2-txt.txt`, 25,333 rows parsed via `parse_13f_list`, 0 unmatched):**

| Official List slice | distinct CUSIPs | fail the mod-10 check digit |
|---|---|---|
| non-option (`COM`, `SHS`, `UNIT`, `NOTE`, `*W EXP`, …) | 13,107 | **0** |
| option (`CALL` / `PUT`) | 12,226 rows | **11,825 rows** |

So the rule holds exactly where it is meant to: a REAL security's CUSIP on SEC's own list never fails, CINS included (CINS inherits the same check digit, so a failing `G`-prefixed value is corrupt, not merely foreign).

⚠ **But a check-digit failure does NOT imply a corrupt identifier**, and the earlier version of this note said it did. The Official List gives each issuer's CALL and PUT class its own CUSIP-shaped identifier: the issuer's first six characters, `9` in position 7 (`95` for the PUT), and the UNDERLYING's check digit copied verbatim into position 9 — which is exactly why they fail mod-10. AAPL is `037833100 COM` / `037833900 CALL` / `037833950 PUT`; a multi-class issuer gets one pair per class (Alphabet `02079K107 CAP STK CL C` → `02079K907`/`02079K957`, `02079K305 CAP STK CL A` → `02079K905`/`02079K955`). They are SEC-published and deliberately not valid CUSIPs.

Measured 2026-08-08 by joining `unresolved_13f_cusips` (110,177 rows / 65,938 distinct CUSIPs) against `13flist2026q2-txt.txt`: **9,561 bulk rows** carrying `openfigi_unknown` and **8,657 legacy rows** still pending are Official-List CALL/PUT classes. Those are not a coverage fact about OpenFIGI at all — see §4.5.

⚠ The prior figure ("12,282 distinct, 0 fail") was arithmetically right and its SUBJECT was narrower than the sentence — it counted the non-option slice while claiming "all lines". That is the defect `.claude/CLAUDE.md` warns about under "state the query and its two numbers, not a percentage whose subject the reader has to infer". Reproduce either half with the query above rather than trusting the table.

⚠ The obvious regex `^[0-9A-Z]{9}\s` parses only 6,300 of those lines — CINS rows use `*` as the delimiter, so it silently drops the entire foreign half, which is exactly the population the rule most needs testing against. Use `^([0-9A-Z]{9})[\s*]` and assert matched + unmatched == total.

Reproduce the corpus split with `uv run python scripts/audit_cusip_check_digit.py --census`.

### 4.5 An option-class CUSIP must never reach this endpoint (#2353)

**Source rule — Form 13F Special Instruction 10** (`https://www.sec.gov/files/form13f.pdf`, p.6-7, quoted):

> "A Manager must report holdings of options only if the options themselves are Section 13(f) securities. ... The Manager must give the entries in **Columns 1 through 5** and in Columns 7 and 8 of the Information Table, however, **in terms of the securities underlying the options, not the options themselves**. ... coupled with a designation "PUT" or "CALL" following such segregated entries in Column 5"

Special Instruction 11.b.iii makes Column 3 the CUSIP, and Column 3 sits inside "Columns 1 through 5". So the identifier an option row is REQUIRED to carry is the **underlying's** CUSIP, with PUT/CALL in Column 5 (`PUTCALL` in the structured INFOTABLE → `institutional_holdings.is_put_call`). Both conventions are live in our corpus:

- **compliant** — AAPL holds 2,230 `institutional_holdings` rows with `is_put_call` set, all bound through `037833100`;
- **deviating** — accession `0001313360-26-000003` (`form13fInfoTable20260806.xml`, fetched from EDGAR 2026-08-08) files `<cusip>78462F953</cusip>` (the Official List's SPY PUT class) **together with** `<putCall>Put</putCall>`.

⚠ **That second example is why `PUTCALL` is NOT the discriminator.** It is set correctly on a row whose Column 3 is wrong. `PUTCALL` describes the POSITION; the defect is in the IDENTIFIER, and the two are independent.

The discriminator is the Official List itself, matched **exactly**: description equal to `CALL` or `PUT` after upper-casing, stripping the `*` added-flag and collapsing whitespace (`_is_put_call` in `app/services/sec_13f_securities_list.py`). Three narrower-than-you-expect traps, each measured on `13flist2026q2-txt.txt` rather than reasoned about:

| test | distinct CUSIPs | fail mod-10 | verdict |
|---|---|---|---|
| description EXACTLY `CALL`/`PUT` | 10,164 | 11,825 of 12,220 rows | the option classes |
| `_is_option` (adds WTS/WARRANT/WT/RIGHT/RIGHTS) | +121 | 0 | genuine securities — do NOT tombstone |
| description CONTAINS `CALL`/`PUT` | +7 | 0 | genuine securities — do NOT tombstone |

Those 7 are 4 BMO structured notes (`CALL NRGU 45`, `CALL NRGD 45`, `CALL BNKU 45`, `CALL LKD 41`) and 3 covered-call ETFs (`ETHE CO CALL ETF`, `KWEB COVERD CALL`, `YIEL S& CALL ETF`). **The containment form was written first and a live probe falsified it**: `063679427` (`CALL NRGU 45`) answers with a populated `data` array where a real option class answers `{"warning": "No identifier found."}`. The check digit could not have caught this — all 7 pass it.

Such a row can never resolve by ANY of our three paths, so it gets its own terminal verdict `option_pseudo_cusip` (`sql/274_unresolved_13f_option_pseudo_cusip.sql`), written by `tombstone_option_pseudo_cusips` inside `cusip_universe_backfill`:

- OpenFIGI **rejects** it on the check digit (§4.3) — 9,561 bulk rows' worth of rate-limited budget;
- the Official-List backfill maps only `_is_common_share` rows, so it never mints one;
- ⚠ the legacy fuzzy-name resolver **would match it** — on the issuer name, which is identical to the underlying's — and write an `external_identifiers (provider='sec')` row binding an option-class identifier to the underlying instrument. Simulated over the full legacy pending partition 2026-08-08: **3,876 of 8,663 would promote**, 703 ambiguous, 4,084 below threshold; the top matches score 1.0 (⚠ that 8,663 is the pre-correction CONTAINMENT set — a superset of the 8,657 the shipped exact-match classifier claims, differing by the 6 compound-description rows that happen to sit in this partition) (`13321L908` → CAMECO, `771049903` → ROBLOX, `29355A907` → ENPHASE ENERGY). That false-mapping risk, not the wasted budget, is the reason this verdict exists.

## 5. eBull integration points (post PR-1b)

| Concern | Location |
|---|---|
| Resolver class | `app/services/openfigi_resolver.py` (PR-1b) |
| API-key env var | `OPENFIGI_API_KEY` — read via `OpenFigiResolver.from_env()` → `settings.openfigi_api_key` (`app/config.py`), not directly in `__init__` |
| Lane | `Lane = Literal[..., "openfigi"]` in `app/jobs/sources.py` (PR-1b) |
| Sweep job | `cusip_resolver_post_bulk_sweep` stage S13 (PR-1b) |
| Persistence | `external_identifiers (provider='openfigi', identifier_type='cusip', is_primary=FALSE)` |
| CUSIP-map reader | `load_bulk_cusip_map` in `app/services/cusip_resolver.py`: `WHERE provider IN ('sec', 'openfigi') AND identifier_type='cusip'`. Called by `sec_13f_dataset_ingest.py` + `sec_nport_dataset_ingest.py`; `bootstrap_preconditions.py` applies the same filter inline. |

OpenFIGI-derived rows go into `external_identifiers` with `provider='openfigi'`, **not** `provider='sec'`. The two-provider union pattern in `load_bulk_cusip_map` is the canonical reader gate (SEC `is_primary=TRUE` wins over an OpenFIGI `is_primary=FALSE` row for the same CUSIP via `ORDER BY is_primary DESC`).

## 6. Sample payload

Request (1 item):

```json
[{"idType": "ID_CUSIP", "idValue": "037833100"}]
```

Response body (the API's `[{"data": [...]}]` shape, truncated). On disk the fixture `tests/fixtures/openfigi/single_aapl.json` nests this under `response.body`, alongside `request` / `response.headers` / `response.status_code` / `scenario`:

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

`scripts/probe_openfigi.py` issues 3 scenario POSTs (single_aapl, batch_known_5, batch_with_invalid) + up to 30 saturation POSTs = up to 33 POSTs unkeyed (plus a preflight GET). After a full run the unkeyed account is rate-limited for ~60s. CI must NOT run the probe; only operator-driven refreshes are appropriate. Tests under `tests/test_openfigi_fixtures.py` validate stored fixtures with zero HTTP calls.

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

OpenFIGI returns OTC tickers under their own `exchCode` (e.g. `'OPRA'`, `'PINX'`). The defensive `_pick_us_primary` filter above intentionally selects `'US'` (the SEC-registered composite exchange code) to avoid binding ownership rows to OTC mirrors that may not exist in `instruments`. When no `US`-row exists the sweep tombstones the `unresolved_13f_cusips` row with `resolution_status='openfigi_unknown'` (sql/192, #740 — terminal in v1; `SET resolution_status=NULL` is the manual retry escape hatch). The sibling `openfigi_no_instrument` status is written when OpenFIGI returns a ticker but it has no unique `is_tradable` `instruments.symbol` match. A fourth verdict, `option_pseudo_cusip` (`sql/274_unresolved_13f_option_pseudo_cusip.sql`, #2353), is NOT written by this sweep at all — `cusip_universe_backfill` writes it off SEC's Official List, and it is deliberately absent from `OPENFIGI_NEGATIVE_STATUSES` so a later mapping cannot un-freeze it (a mapping appearing for an option-class identifier is the defect, not the cure). See §4.5. The third negative, `openfigi_invalid_identifier` (`sql/261_unresolved_13f_openfigi_invalid_identifier.sql`, #2304), is written when OpenFIGI REJECTED the identifier — see §4.3; it is NOT a no-match and must not be read as one.

### 7.6 Per-instance limiter — single-process only

`_RateLimiter` is **per-instance**, NOT module-global ([`openfigi_resolver.py:148-202`](../../../app/services/openfigi_resolver.py#L148-L202); contrast with sec-edgar's `_PROCESS_RATE_LIMIT_CLOCK` module-global pattern in `app/providers/implementations/sec_edgar.py`). Multiple `OpenFigiResolver` instances in the same process do NOT coordinate budget. Two consequences:

- **Single-process safety:** the bootstrap-orchestrator `openfigi` lane is cap=1 (`.claude/skills/data-engineer/SKILL.md` §6.5.1), so only one `cusip_resolver_post_bulk_sweep` runs at a time. Combined with "instantiate once per sweep" ([`openfigi_resolver.py:257`](../../../app/services/openfigi_resolver.py#L257)), the lane cap is the effective budget gate within a process.
- **Cross-process / multi-worker:** N workers = N independent budgets = total budget × N at the OpenFIGI account level. Either (a) keep a single worker for OpenFIGI work, or (b) move the budget gate to Redis / Postgres before scaling out. eBull's current topology is single-worker so the per-instance pattern is correct; document any future scale-out as breaking this invariant.

When ADDING a new caller (e.g. a future on-demand resolver from the API layer), reuse a shared module-global `OpenFigiResolver` instance per process — do NOT instantiate per-request. The token bucket starts empty on construction and would silently burn the unkeyed 25/min budget after ~25 requests.

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

Sign up at <https://www.openfigi.com/api> and provision an API key. Set `OPENFIGI_API_KEY` in eBull's environment to switch the resolver to the keyed tier (25,000 vs 250 mappings/min = 100× the unkeyed throughput).

## 9. Cross-references

- `docs/settled-decisions.md` → "OpenFIGI as approved external CUSIP-resolver fallback (2026-05-22)" — the SD-1 entry that gates this integration.
- `docs/proposals/etl/bootstrap-optimisation.md` §2 — the PR-0 introduction context; §5 — the PR-1b resolver shape.
- `.claude/skills/data-sources/sec-edgar.md` §5 (CUSIP → CIK bridge) — the upstream bridge OpenFIGI complements when 13F Official List name-fuzzy fails.
- `tests/fixtures/openfigi/README.md` — the recorded fixtures with full request/response payloads.
