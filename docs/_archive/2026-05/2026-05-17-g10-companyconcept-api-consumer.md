# G10 — `companyconcept` API consumer

> **Status:** v3 2026-05-17 (post-Codex 1a r1 + r2: matrix-closure
> framing downgraded to PROVIDER PRIMITIVE; raw-payload exemption
> codified as invariant for future consumers; taxonomy widened to
> SEC-syntax + docstring synced; tag regex tightened; rate-limit-clock
> test pinned through identity assertion + transport-level smoke;
> tag-count audit corrected; taxonomy delegation converges on request
> source-of-truth).
> **Phase / PR:** US ETL completion plan §2 Phase 4, PR 7.
> **Gap closed:** §7 G10 — `companyconcept` provider primitive landed
> (no production consumer in v1; see §3.1 + §6 closure framing).

## 1. Goal

Close the `data.sec.gov/api/xbrl/companyconcept/CIK{padded}/{taxonomy}/{tag}.json`
endpoint coverage gap by exposing a thin provider primitive on
`SecFundamentalsProvider` for **single-tag XBRL fetches**.

The endpoint returns the SAME shape as one slice of companyfacts
(`facts.<taxonomy>.<tag>`), keyed by `(cik, taxonomy, tag)`, with the
`units → entries` structure already handled by the existing extractor
`_extract_facts_from_section`. The primitive lands as a callable on the
public provider surface; the existing extractor is reused under a
single-tag synthetic section.

## 2. Non-goals

- **No opt-in wiring of `fundamentals_sync` to companyconcept in v1.**
  See §3.1 — under the live 10 req/s shared SEC budget, any consumer
  that needs ≥2 tags per CIK loses on wall-clock vs the existing
  companyfacts single-fetch path. The primitive enables future
  event-driven single-tag refresh paths (e.g. #435 dilution-tracker
  per-CIK shares-outstanding topup); no production caller is wired in
  this PR.
- **No new persistence table.** `fetch_concept` returns parsed JSON to
  the caller (mirrors `_fetch_company_facts`'s existing shape).
  Persistence to DB is the caller's responsibility — provider stays a
  thin HTTP adapter per settled decisions §"Provider design rule" and
  §"Provider boundary".
- **No `FundamentalsProvider` ABC extension.** `fetch_concept` is
  concrete-class-only (same pattern as `extract_facts`,
  `extract_facts_and_catalog`, `extract_concept_catalog`).
- **No bulk-archive change.** Companyfacts.zip stays the
  bootstrap-drain path (Stage 9 `sec_companyfacts_ingest`). The
  per-CIK companyfacts API path stays the daily top-up writer.
- **No `frames` API consumption.** That is G11, Phase 4 PR 8.

## 3. Design decisions

### 3.1 No opt-in wiring of `fundamentals_sync` to companyconcept — primitive-only landing

The plan brief reads: "fundamentals_sync opts in for the Tier-1 metric
set; full Companyfacts remains the fallback. Performance audit: measure
bandwidth + latency delta vs Companyfacts for a representative cohort."

The audit produces a clear NO-WIRE conclusion under the current shared
rate budget. Empirical analysis:

**Rate-limit cost (10 req/s shared SEC ceiling, `_PROCESS_RATE_LIMIT_CLOCK`
+ `_PROCESS_RATE_LIMIT_LOCK` at `app/providers/implementations/sec_edgar.py:55-80`):**

Every HTTP call against `data.sec.gov` / `www.sec.gov` funnels through
`ResilientClient` with `min_request_interval_s = 0.11` shared
process-wide. Parallelism via `concurrent_iter` is bounded by this
clock — the request budget is request COUNT, not request bytes.

**Candidate consumer path A — `fundamentals_sync` daily snapshot
(`SecFundamentalsProvider.get_latest_snapshot_by_cik`):**

`FundamentalsSnapshot` derives ~10 metric groups from XBRL facts.
Tag-priority lists in `sec_fundamentals.py:67-88` resolve to **18
candidate us-gaap tag fetches per CIK** (4 revenue variants + 1 each
for gross_profit / operating_income / operating_cf + 2 each for capex
/ cash / debt / equity / shares + 1 for EPS). Cost per CIK:

- **Companyfacts (current path):** 1 HTTP call × 0.11 s rate-limit
  gate + ~0.3-0.7 s download + ~50 ms parse ≈ **0.5-1.0 s wall-clock**.
- **Companyconcept (hypothetical replacement):** 18 HTTP calls × 0.11 s
  shared rate-limit gate ≈ **2.0 s wall-clock minimum** before any
  per-call payload time, regardless of payload size.

**Candidate consumer path B — `daily_financial_facts` full extract
(`refresh_financial_facts`):**

Uses `extract_facts_and_catalog` which emits every concept in the
companyfacts payload (post-#451 the extractor no longer filters on
`TRACKED_CONCEPTS` — every concept the issuer reports lands in
`financial_facts_raw`). The tracked-map convention (`TRACKED_CONCEPTS`
= 53 us-gaap concepts / 78 tag variants + 3 dei concepts / 3 dei tag
variants = **81 tag variants total**) defines downstream
normalisation priority, not extractor breadth. Replacing the single
companyfacts fetch with 81+ companyconcept calls per CIK is an order
of magnitude WORSE (~9 s wall-clock minimum per CIK at 81 × 0.11 s),
plus it would silently drop every concept outside the tracked map
that #451 explicitly opens up — a regression in raw-store coverage.

**Per-CIK bytes savings (real, but irrelevant under the current budget):**

Companyfacts payload for large filers (AAPL, MSFT) is ~5-10 MB and
contains hundreds of concepts. Companyconcept per-tag payload is
~5-50 KB. For 18 tag fetches per CIK the byte-count is ~0.1-1 MB vs
~5-10 MB — 5-10× SEC-egress reduction. We pay zero quota for SEC
egress; this is purely good-citizen behaviour and does not buy us
wall-clock or request-budget back.

**Where companyconcept is NET-POSITIVE — the consumer this primitive enables:**

- **Single-tag refresh paths.** Any future workflow that needs to top
  up ONE tag for ONE CIK (e.g. an event-driven hourly
  shares-outstanding refresh after a Form 4 hits, feeding the
  dilution tracker #435) benefits — 1 companyconcept call (~30 KB)
  is cheaper than 1 companyfacts call (~5 MB) at the same 1 × 0.11 s
  rate-limit cost.
- **Operator-driven concept probes.** Admin spot-checks for a single
  concept value across one or two CIKs (debugging an XBRL anomaly)
  no longer require pulling the full companyfacts payload.

**Conclusion:** Land the primitive (provider method + extractor reuse +
tests). DO NOT wire `fundamentals_sync` to it. Document the analysis
above in the PR body. Reopen the wiring question if/when a single-tag
consumer ticket lands (#435 dilution tracker, or any other event-driven
single-tag path).

**Matrix-closure framing (Codex 1a r1 HIGH-1).** Closing G10 as
`✅ WIRED` would overclaim — `WIRED` is reserved for endpoints with a
production caller. The honest closure is `✅ PROVIDER PRIMITIVE` —
the provider method is exposed and callable, no fundamentals_sync /
daily_financial_facts wire-up, no scheduled job, no DB writer. See §6
for the exact matrix row text.

### 3.2 Payload shape — synthetic single-tag section, reuse existing extractor

Companyconcept response shape (per
<https://www.sec.gov/edgar/sec-api-documentation>):

```json
{
  "cik": 320193,
  "taxonomy": "us-gaap",
  "tag": "Revenues",
  "label": "Revenues",
  "description": "...",
  "entityName": "Apple Inc.",
  "units": {
    "USD": [
      {"end": "2024-09-28", "val": 391035000000, "accn": "0000320193-24-000123",
       "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2024-11-01"},
      ...
    ]
  }
}
```

This is identical to ONE entry inside companyfacts'
`facts.<taxonomy>.<tag>` block — the existing `_extract_facts_from_section`
walks the `tag → {units: {unit: [entries]}}` structure. The primitive
synthesises `{tag: {"units": payload["units"]}}` and delegates to the
same extractor with the **request-side `taxonomy`** (Codex 1a r2 MED-2
ownership — single source of truth is the validated request argument,
not the un-validated response field). If `payload["taxonomy"]` differs
from the request, `extract_concept_facts` logs a warning and proceeds
with the request value (SEC shape drift surfaces in logs without
crashing the extractor). Net new parsing code: zero.

`label` / `description` / `cik` / `entityName` are dropped — the
extractor only reads `units`. No catalogue helper is specified in v1
(Codex 1a r1 LOW-8 close-out — earlier wording about an
`extract_concept_catalog` companion is removed).

### 3.3 No raw-payload persistence in v1 — provider primitive matches existing companyfacts grandfather; future-consumer invariant

Review-prevention-log #1168 ("Raw API payload must be persisted before
any parse / normalise step") applies to **jobs / service helpers** that
fetch an external HTTP payload and ingest into our DB. It does NOT
apply to provider methods that return parsed JSON to a caller —
`_fetch_company_facts` already parses without persisting raw, and the
caller `app/services/fundamentals.py::refresh_financial_facts` writes
parsed facts to `financial_facts_raw` (which is a normalised view, not
the wire bytes).

`fetch_concept` lands as a thin HTTP wrapper, returns parsed dict,
no DB writer. The v1 PR does not need a new raw table.

**Future-consumer invariant (Codex 1a r1 HIGH-2 ownership).** The
following invariant is binding on any subsequent PR that introduces a
production caller of `fetch_concept` / `extract_concept_facts`:

> If a future job, service helper, or scheduled writer wires
> `extract_concept_facts(...)` (or any helper that consumes the
> `fetch_concept` payload) into a path that INSERTs derived rows into
> `financial_facts_raw`, `financial_periods`, or any new
> `*_observations` / `*_current` table, that PR MUST also land
> raw-payload persistence per prevention-log #1168 IN THE SAME PR —
> either by extending an existing raw table (e.g. `sec_companyfacts_raw`
> if added) or by introducing a sibling `sec_companyconcept_raw`
> table keyed on `(cik, taxonomy, tag, fetched_at)`. Splitting
> persistence into a follow-up is forbidden.

This spec is the docstring-of-record for the invariant. The PR docstring
on `fetch_concept` cites this spec and the prevention-log entry so a
future consumer-PR's self-review surfaces the obligation.

### 3.4 Rate-limit clock — shared with companyfacts

`SecFundamentalsProvider`'s `_http: ResilientClient` already binds the
process-wide `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK`
imported from `sec_edgar.py`. `fetch_concept` reuses the same
`self._http.get(path)` call shape as `_fetch_company_facts`, so the
new endpoint hits the same 10 req/s shared SEC budget. No new
clock / lock construct.

### 3.5 404 contract — return None, raise on other errors

Mirror `_fetch_company_facts`:

- `404 → None` (concept not present for that CIK; common — e.g.
  `EarningsPerShareDiluted` does not exist for pre-IPO filers).
- `4xx other / 5xx → raise_for_status()` propagation. Caller handles
  via existing fault-handler shape.

## 4. Implementation surface

### 4.1 Provider methods (new, on `SecFundamentalsProvider`)

```python
def fetch_concept(
    self,
    cik: str,
    taxonomy: str,
    tag: str,
) -> dict[str, Any] | None:
    """Fetch one XBRL concept for a CIK from the companyconcept API.

    Returns parsed JSON or None on 404. Other HTTP errors propagate
    via raise_for_status().

    Args:
      cik: 10-digit zero-padded OR int-able string; normalised via _zero_pad_cik.
      taxonomy: SEC namespace identifier — ASCII regex ^[a-z][a-z0-9-]*$
        (e.g. 'us-gaap', 'dei', 'srt', 'invest', 'ifrs-full'). The
        primitive is intentionally a general SEC companyconcept
        consumer, NOT bound to TRACKED_CONCEPTS / DEI_TRACKED_CONCEPTS
        — those maps govern downstream normalisation, not arbitrary
        probe access. See spec
        docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md §4.2.
      tag: XBRL concept name — ASCII regex ^[A-Za-z][A-Za-z0-9_]*$
        (e.g. 'Revenues', 'EntityCommonStockSharesOutstanding',
        'my_custom_concept'). Leading-letter anchored; rejects bare
        numerics and non-ASCII alnum.
    """

def extract_concept_facts(
    self,
    symbol: str,
    cik: str,
    taxonomy: str,
    tag: str,
) -> list[XbrlFact]:
    """Single-concept variant of extract_facts.

    Fetches the companyconcept response and returns the same XbrlFact
    rows the existing extractor produces for one tag inside a
    companyfacts payload. Empty list on 404 or empty units.
    """
```

Both methods live on the concrete `SecFundamentalsProvider` class in
`app/providers/implementations/sec_fundamentals.py`. Neither extends
the `FundamentalsProvider` ABC (per §2 non-goals).

### 4.2 URL construction

```text
GET https://data.sec.gov/api/xbrl/companyconcept/CIK{padded}/{taxonomy}/{tag}.json
```

Built as `f"/api/xbrl/companyconcept/CIK{cik_padded}/{taxonomy}/{tag}.json"`,
relative to the existing `_BASE_URL = "https://data.sec.gov"` on `_http`.

`taxonomy` + `tag` enter the URL path. Validation (Codex 1a r1 MED-3
+ MED-4 + MED-5 ownership — general SEC primitive, not
tracked-map-bound):

- `taxonomy`: ASCII regex `[a-z][a-z0-9-]*` matched via
  `re.compile(...).fullmatch(value)` (matches every published SEC
  taxonomy namespace — `us-gaap`, `dei`, `srt`, `invest`, `country`,
  `ifrs-full`, etc.). The primitive is intentionally a general SEC
  `companyconcept` consumer, NOT restricted to the `(taxonomy, tag)`
  pairs in `TRACKED_CONCEPTS` / `DEI_TRACKED_CONCEPTS` — those maps
  govern downstream normalisation / projection, not arbitrary probe
  access. Empty-string taxonomy rejected. **`fullmatch` is mandatory**
  (Codex 1b r1 MED-1 ownership) — Python's default `re.match` plus
  `^...$` admits trailing `\n` because `$` matches before a final
  newline; `fullmatch` closes that hole.
- `tag`: ASCII regex `[A-Za-z][A-Za-z0-9_]*` matched via
  `re.compile(...).fullmatch(value)`. SEC concept names in practice
  are CamelCase letters + digits (`Revenues`,
  `EntityCommonStockSharesOutstanding`); the `_` allowance covers
  custom-taxonomy concepts that legitimately use underscores (rare
  but documented). The leading-letter anchor rejects bare numerics
  (e.g. `123`) and the ASCII char-class rejects non-ASCII alnum that
  `str.isalnum()` would have admitted. Empty-string tag rejected.
  `fullmatch` discipline applies symmetrically.

Both validations raise `ValueError` with a precise diagnostic
(`"invalid taxonomy {value!r}: expected lowercase ASCII + dashes"`)
before any HTTP call — defence against accidental URL-injection at
the caller boundary.

### 4.3 Extractor synthesis

```python
def extract_concept_facts(self, symbol, cik, taxonomy, tag):
    payload = self.fetch_concept(cik, taxonomy, tag)
    if payload is None:
        return []
    response_taxonomy = payload.get("taxonomy")
    if response_taxonomy is not None and response_taxonomy != taxonomy:
        logger.warning(
            "companyconcept response taxonomy %r differs from request %r "
            "for CIK %s tag %s — using request taxonomy",
            response_taxonomy, taxonomy, cik, tag,
        )
    units = payload.get("units")
    if not isinstance(units, dict):
        logger.warning(
            "companyconcept payload for CIK %s %s/%s missing or non-dict units",
            cik, taxonomy, tag,
        )
    section = {tag: {"units": units if isinstance(units, dict) else {}}}
    return _extract_facts_from_section(section, taxonomy=taxonomy)
```

Per prevention-log #1204 (silent `.get(default)` for financial-semantic
dicts), both the taxonomy-drift branch and the missing-units branch log
warnings before returning empty — defends against future SEC API shape
drift surfacing as silent zero-row results.

## 5. Test plan

`tests/test_sec_fundamentals_companyconcept.py` (new):

1. **`test_fetch_concept_url_zero_pads_cik`** — `fetch_concept("320193",
   "us-gaap", "Revenues")` hits `/api/xbrl/companyconcept/CIK0000320193/us-gaap/Revenues.json`.
   Patches `self._http.get` via a recording stub; asserts on URL path.
2. **`test_fetch_concept_404_returns_none`** — stubbed 404 response
   resolves to None; no exception. Mirrors `_fetch_company_facts`
   404 shape.
3. **`test_fetch_concept_5xx_raises`** — stubbed 500 response raises
   via `raise_for_status()`. Confirms we do NOT swallow non-404
   errors. **Fixture mechanics** (Codex 1b r1 MED-3 ownership):
   `httpx.Response(500, ...)` alone has no `Request` attached and
   `raise_for_status()` skips raising; the test must use
   `httpx.Response(500, request=httpx.Request("GET",
   "https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Revenues.json"))`.
4. **`test_fetch_concept_rejects_malformed_taxonomy`** — parametrised:
   `"Us-Gaap"` (uppercase rejected), `""` (empty rejected),
   `"us gaap"` (space rejected), `"us/gaap"` (slash rejected),
   `"-bad"` (leading dash rejected), `"us-gaap\n"` (trailing newline —
   Codex 1b r1 MED-1 ownership; pins the `fullmatch` discipline since
   `re.match` + `^...$` would have admitted it), `"us-gaap "`
   (trailing space), `"\nus-gaap"` (leading newline) — all raise
   `ValueError`. Legitimate values (`"us-gaap"`, `"dei"`, `"srt"`,
   `"ifrs-full"`, `"invest"`, `"country"`) succeed through the
   stubbed HTTP path.
5. **`test_fetch_concept_rejects_malformed_tag`** — parametrised:
   `"Revenues/Q1"` (slash), `"Revenues "` (trailing space), `""`
   (empty), `"123Revenues"` (leading digit), `"Revenu€s"`
   (non-ASCII), `"Revenues\n"` (trailing newline — `fullmatch`
   discipline) — all raise `ValueError`. Legitimate values
   (`"Revenues"`, `"EntityCommonStockSharesOutstanding"`,
   `"my_custom_concept"`) succeed.
6. **`test_extract_concept_facts_reuses_section_extractor`** — fixture
   payload (`taxonomy=us-gaap`, `tag=Revenues`) with two `USD`
   entries — `(form="10-K", fp="FY", end="2024-09-28", val=391035000000)`
   + `(form="10-Q", fp="Q3", end="2025-06-28", val=85777000000)`
   (real-looking integer USD revenue figures per Codex 1b r1 MED-2
   ownership; `Decimal(str(int))` exercise is trivially safe).
   Yields exactly 2 `XbrlFact` rows with matching `concept` /
   `taxonomy` / `unit` / `period_end` / `val` / `form_type`.
6a. **`test_extract_concept_facts_decimal_str_boundary`** — separate
   test using `taxonomy=us-gaap`, `tag=EarningsPerShareDiluted`
   (`USD/shares`) with `val=3.7` (non-binary-representable float in
   IEEE 754). Asserts `facts[0].val == Decimal("3.7")` exactly,
   pinning prevention-log #1174 (`Decimal(str(<float>))` boundary)
   without muddling the Revenues-integer fixture.
7. **`test_extract_concept_facts_empty_on_404`** — when stubbed
   `fetch_concept` returns None, `extract_concept_facts` returns `[]`.
8. **`test_extract_concept_facts_empty_on_missing_units`** — payload
   with no `units` key returns `[]` AND logs a `logger.warning`
   (caplog assertion).
9. **`test_fetch_concept_shares_rate_limit_clock`** — identity
   assertion (Codex 1a r1 MED-6 + r2 LOW-4 ownership). `ResilientClient`
   captures the `shared_last_request` list in `__init__`, so a
   post-construction patch of `sec_edgar._PROCESS_RATE_LIMIT_CLOCK`
   would NOT affect the constructed provider's `_http._last_request_at`.
   The robust test pins the invariant by identity + behaviour:

   ```python
   from app.providers.implementations import sec_edgar
   provider = SecFundamentalsProvider(user_agent="test")
   assert provider._http._last_request_at is sec_edgar._PROCESS_RATE_LIMIT_CLOCK
   assert provider._http._throttle_lock is sec_edgar._PROCESS_RATE_LIMIT_LOCK
   ```

   Plus a behaviour-level smoke using `httpx.MockTransport`
   (transport-level stub — fires BELOW `ResilientClient`'s throttle so
   the rate-limit gate engages, unlike a `provider._http.get` patch
   which would bypass it). `SecFundamentalsProvider.__init__` has no
   client injection point today (Codex 1a r3 LOW-2 ownership), so the
   test pattern is: construct the provider normally, then
   `provider._client.close()` and reassign
   `provider._client = httpx.Client(transport=httpx.MockTransport(handler),
   base_url=_BASE_URL, headers=...)` followed by reconstructing
   `provider._http` around the new client (re-using
   `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` so the
   throttle continues to engage). Record `time.monotonic()` per stub
   invocation; assert delta ≥ `_MIN_REQUEST_INTERVAL_S` between calls.

   **Test-isolation cleanup** (Codex 1b r1 LOW-4 ownership). The
   shared `_PROCESS_RATE_LIMIT_CLOCK` is mutated by the back-to-back
   calls; a `pytest` fixture must close the swapped client AND reset
   `_PROCESS_RATE_LIMIT_CLOCK[0] = 0.0` in teardown so the next test
   in the full `uv run pytest` doesn't observe stale-clock drift
   from this test's monotonic timestamps.

   Pins the §3.4 invariant end-to-end without depending on
   patchability of the module-level clock.

All HTTP behaviour stubbed — no live SEC calls in CI.

## 6. Matrix + skill updates

Closure framing per Codex 1a r1 HIGH-1 is `✅ PROVIDER PRIMITIVE`,
NOT `✅ WIRED`. `WIRED` claims a production consumer; this PR exposes
the method without one.

`.claude/skills/data-engineer/etl-endpoint-coverage.md` §4 row
`data.sec.gov/api/xbrl/companyconcept/...`:

- Before: `❌ GAP — single-tag smaller payload; would let
  fundamentals_sync avoid full Companyfacts when only N tags needed.
  Tech-debt eligible.`
- After: `✅ PROVIDER PRIMITIVE 2026-05-17 (G10) —
  app/providers/implementations/sec_fundamentals.py::fetch_concept
  + extract_concept_facts. No production consumer in v1 (under the
  10 req/s shared SEC budget, companyconcept is wall-clock net-negative
  for any consumer needing ≥2 tags per CIK). Primitive enables future
  single-tag refresh paths (#435 dilution-tracker per-CIK
  shares-outstanding topup; operator-driven concept probes). See PR
  audit + spec docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md.`

`.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 G10 row:

- Before: `OPEN (low) | — | Smaller-payload alternative to
  Companyfacts for known-tag pulls. Eligible.`
- After: `✅ CLOSED 2026-05-17 — PR #<n> — provider primitive landed
  (sec_fundamentals.py::fetch_concept + extract_concept_facts); no
  fundamentals_sync / daily_financial_facts wire-up by design (audit
  in §3.1 — companyconcept is wall-clock net-negative under the 10
  req/s shared SEC budget for any consumer needing ≥2 tags per CIK).
  Re-open as a wiring ticket when a single-tag consumer lands.`

`.claude/skills/data-sources/sec-edgar.md` §1.6 (Companyconcept row):
append "Provider primitive: `app/providers/implementations/sec_fundamentals.py::fetch_concept`
(G10, 2026-05-17). No production consumer in v1." after the existing
"One XBRL tag (smaller payload)" description.

## 7. Acceptance criteria

1. `SecFundamentalsProvider.fetch_concept(cik, taxonomy, tag)` callable
   from the public surface, returns parsed JSON dict or None.
2. `SecFundamentalsProvider.extract_concept_facts(symbol, cik,
   taxonomy, tag)` callable, returns `list[XbrlFact]` reusing the
   existing `_extract_facts_from_section`.
3. Rate-limit clock shared with companyfacts API path (test 9 above).
4. 404 → None contract pinned by test 2; 5xx → raise pinned by test 3.
5. Taxonomy / tag validation pinned by tests 4 + 5.
6. Matrix §4 + §7 + sec-edgar.md updates landed.
7. `uv run ruff check .`, `uv run ruff format --check .`,
   `uv run pyright`, `uv run pytest` all pass.
8. `tests/smoke/test_app_boots.py` green — app lifespan unchanged
   (provider class import is the only touch point).

## 8. ETL clauses #8-#12 disposition

**NOT APPLICABLE** end-to-end. G10 lands a provider primitive only:

- **#8 smoke against panel:** N/A — no production consumer means no
  per-instrument figure changes. PR body records that no AAPL / GME /
  MSFT / JPM / HD ownership / fundamentals figure is touched.
- **#9 cross-source verify:** N/A — no new figure to compare.
- **#10 backfill:** N/A — no schema / parser / observations change.
- **#11 operator-visible figure verify:** N/A — no rollup endpoint
  affected.
- **#12 verification steps in PR body:** PR body records §8 disposition
  explicitly + cites the architectural analysis in §3.1 as the
  audit-of-record.

The provider primitive IS exercised by unit tests against stubbed
HTTP; that's the relevant verification surface for a thin HTTP
adapter that has no production caller.

## 9. Settled-decisions check

- **Provider boundary / Provider design rule** (settled-decisions
  §"General engineering decisions" + §"Fundamentals provider posture"):
  preserved. `fetch_concept` is thin HTTP, no DB access, no domain
  orchestration.
- **Fundamentals provider posture** (free regulated source only):
  preserved. SEC EDGAR companyconcept is free / regulated.
- **Identifier strategy** (CIK is the SEC filing-lookup key):
  preserved. `fetch_concept` takes CIK; no symbol fuzzy-resolve.
- **Auditability**: preserved. Provider remains thin; if a future
  consumer wires this primitive into a job, that PR also lands the
  raw-payload-persistence guard per prevention-log #1168.

## 10. Codex 1a r1 disposition

| Finding | Severity | Resolution |
|---|---|---|
| Closure framing — `✅ WIRED` overclaims; mark `✅ PROVIDER PRIMITIVE` or leave open | HIGH | FIXED §3.1 closing paragraph + §6 matrix rows now use `✅ PROVIDER PRIMITIVE` |
| Raw-payload carve-out only holds while provider-only; state as invariant for future consumers | HIGH | FIXED §3.3 codified future-consumer invariant (in-this-PR persistence obligation) |
| `{us-gaap, dei}` allow-list conflicts with tracked-map convention / blocks legitimate probes | MED | FIXED §4.2 widened to SEC-syntax `^[a-z][a-z0-9-]*$` taxonomy; primitive is general not tracked-map-bound |
| Renaming concern (general primitive vs tracked-concept-fetch) | MED | FIXED §4.2 docstring + spec now state "general SEC companyconcept primitive, intentionally not bound to TRACKED_CONCEPTS" |
| Tag `isalnum()` too loose AND too strict | MED | FIXED §4.2 ASCII regex `^[A-Za-z][A-Za-z0-9_]*$` (rejects bare numerics + non-ASCII; admits underscore) |
| Test 9 patchability — `_PROCESS_RATE_LIMIT_CLOCK` captured at `__init__` | MED | FIXED test 9 reframed as identity assertion (`is` check) + behaviour-level back-to-back smoke |
| §3.1 says 18 candidate us-gaap tags but `extract_facts` emits all concepts | LOW | FIXED §3.1 split into Path A (18 tags, snapshot) + Path B (81+ tags, full extract) with stale-count corrected |
| §3.2 mentions catalogue helper that isn't specified | LOW | FIXED §3.2 wording dropped catalogue mention; no `extract_concept_catalog` in v1 |

Spec is CLEAN v2 — proceed to plan.

## 11. Codex 1a r2 disposition

| Finding | Severity | Resolution |
|---|---|---|
| §4.1 docstring stale (`'us-gaap' or 'dei'`) vs §4.2 widened SEC syntax | MED | FIXED §4.1 docstring rewritten with the SEC-syntax regex + example list + cross-reference to §4.2 |
| §3.2 / §4.3 taxonomy delegation inconsistency (`payload["taxonomy"]` vs request `taxonomy`) | MED | FIXED both sections converge on request-side `taxonomy` as single source of truth; mismatch logs warning |
| Header "API not consumed" wording contradicts new framing | LOW | FIXED header `Gap closed:` line updated to "provider primitive landed (no production consumer in v1)" |
| Test 9 behaviour smoke must stub below `ResilientClient`, not at `provider._http.get` | LOW | FIXED test 9 now uses `httpx.MockTransport` so the throttle gate engages; the identity assertion is the load-bearing part |

Spec is CLEAN v3 — proceed to plan.

## 12. Codex 1a r3 disposition

| Finding | Severity | Resolution |
|---|---|---|
| Status header v2 vs §11 v3 mismatch | LOW | FIXED header bumped to v3 |
| Test 9 `MockTransport` injection — no provider-side injection point | LOW | FIXED test 9 prose now specifies "construct, then swap `provider._client` + `provider._http` against MockTransport"; pattern is explicit so the implementer doesn't invent an injection point that doesn't exist |

Spec CLEAN through r3 — proceed to plan.

## 13. Codex 2 pre-push r1 disposition

| Finding | Severity | Resolution |
|---|---|---|
| Comment overclaims "every legal XBRL concept name" but tag regex rejects NCName shapes `_foo`, `foo-bar`, `foo.bar` | MED | FIXED `sec_fundamentals.py` validator comment now states explicitly that the regex is a "deliberately tightened subset of legal XBRL NCName syntax — every SEC-observed concept name uses `[A-Za-z][A-Za-z0-9_]*`; widen with regression test if SEC drift surfaces a legitimate NCName outside this subset" |
| Missing test for taxonomy-mismatch warning branch in `extract_concept_facts` | LOW | FIXED added test `test_extract_concept_facts_logs_warning_on_taxonomy_mismatch` (test 9a in spec / plan numbering) that fires a response with `taxonomy='srt'` against a request `taxonomy='us-gaap'`; asserts emitted facts carry the request taxonomy + warning lands in caplog |
| 5xx test takes ~7 s via default `ResilientClient` retry/backoff schedule | LOW | FIXED `_rewire_transport` test helper now takes `max_retries=0` (default) so 5xx fires once and propagates; 5xx test drops from ~7 s to ~0.1 s |

Spec CLEAN through Codex 2 r1 — proceed to push.
