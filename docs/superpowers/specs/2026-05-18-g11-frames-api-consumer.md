# G11 — `frames` API consumer

> **Status:** v4 2026-05-18 (post-Codex 1a r1+r2+r3 + 1b r1: unit
> regex pivoted to explicit token-(per-token)? grammar — rejects
> `--`, bare `-per`, trailing dash, slash; `usd` lowercase moved
> from rejection to admission (primitive is general; no
> known-unit allowlist); fixture realism fixed — `Revenues`
> paired with `CY####` annual not `Q#I`; test 1 URL switched to
> balance-sheet concept `Assets` for `Q#I` correctness).
> **Phase / PR:** US ETL completion plan §2 Phase 4, PR 8.
> **Gap closed:** §7 G11 — `frames` API provider primitive landed
> (no production consumer in v1; see §3.1 + §6 closure framing).

## 1. Goal

Close the `data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json`
endpoint coverage gap by exposing a thin provider primitive on
`SecFundamentalsProvider` for **cross-sectional one-fact-per-filer
frame fetches**.

The endpoint returns every filer's value for one
`(taxonomy, tag, unit, period)` combination — the natural data source
for sector-aggregate / cross-sectional analyses. The primitive lands as
a callable on the public provider surface; no production consumer is
wired in v1 (see §3.1).

## 2. Non-goals

- **No opt-in wiring of any production caller in v1.** See §3.1 — the
  open feature ticket #594 (peer-comparison radar + sector heatmap)
  has plausible downstream demand but does NOT specifically require
  SEC frames as the data source. Wiring a full frames persistence
  pipeline now (raw table + observations table + scheduled job) would
  pre-commit the backend to one data source before #594's data-ingest
  ticket settles. The primitive lands now; persistence is the
  follow-up PR's scope, in lockstep with whichever data ticket #594
  drives.
- **No new persistence table.** `fetch_frame` returns parsed JSON to
  the caller. Persistence is the caller's responsibility — provider
  stays a thin HTTP adapter per settled decisions §"Provider design
  rule" and §"Provider boundary".
- **No `FundamentalsProvider` ABC extension.** `fetch_frame` is
  concrete-class-only (same pattern as `fetch_concept`,
  `extract_concept_facts`, `extract_facts`, etc.).
- **No `extract_frame_facts` typed extractor in v1.** The frames
  payload shape (flat `data[]` with per-row `cik`) does not fit the
  existing `XbrlFact` dataclass (no `cik` field). Adding a typed
  `FrameFact` dataclass + extractor is the future-consumer PR's
  responsibility — until then, callers consume the raw `data[]`
  list directly. Symmetry note: G10 added `extract_concept_facts`
  because companyconcept's payload shape matches one section of
  companyfacts that `_extract_facts_from_section` already handled —
  zero new parsing code. Frames has no such reuse opportunity.
- **No scheduled job, no `ScheduledJob` registration.** No production
  caller exists; nothing to schedule.

## 3. Design decisions

### 3.1 No production consumer wired — primitive-only landing matches G10 pattern

**Decision rule from plan §2 Phase 4 PR 8:**

> If `gh issue list --search "frames OR sector heatmap OR
> cross-sectional"` returns an open feature ticket, wire the
> consumer; otherwise close G11 as BY DESIGN with documentation.

**Decision rule output 2026-05-18:** `gh issue list --search "frames
OR sector heatmap OR cross-sectional" --state open` returns:

- **#594** — "feat(#585): peer-comparison radar + sector heatmap"
- **#585** — "epic(#567): instrument-page chart redesign — quant L2
  drill pages + theme"

#594's "sector heatmap" affordance is the latent consumer. **BUT**
#594 explicitly states (issue body §"Dependencies"):

> Sector aggregates — needs sector median calculations server-side,
> OR client-side aggregation across the peer set.

The frames API is **one option among several** for sector aggregates.
#594 does not commit to it. Wiring a full frames persistence pipeline
(raw table + observations table + scheduled job) ahead of #594's
data-ingest ticket settling would pre-commit the backend to one data
source before the UI/data-ingest shape is locked.

**Closure framing:** match G10 (PR #1198, merge `0ead989`) —
`✅ PROVIDER PRIMITIVE` (callable surface, no production consumer),
NOT `✅ WIRED` (production caller). Rationale (independently
confirmed by Codex G11-scope review):

1. The decision rule's spirit — "downstream demand demands a
   wire-up" — is partially satisfied (latent demand exists via #594)
   but NOT specifically committed to frames as the data source.
2. The G10 provider-primitive pattern is the established
   compromise: callable surface + future-consumer raw-payload
   invariant + matrix closure as `PROVIDER PRIMITIVE`.
3. The data ticket that #594 will drive can wire frames + persistence
   in its own PR with the right schema design.

### 3.2 No raw-payload persistence in v1 — provider primitive matches G10 grandfather; future-consumer invariant

Same shape as G10 spec §3.3. `fetch_frame` is a thin HTTP wrapper
that returns parsed dict; no DB writer. Prevention-log #1168 ("Raw
API payload must be persisted before any parse / normalise step")
applies to **DB-writing callers**, not provider primitives.

**Future-consumer invariant (binding):** If a future job, service
helper, or scheduled writer consumes `fetch_frame(...)` payloads
into a path that INSERTs derived rows into any
`sec_frames_*` / `sector_aggregate_*` / `*_observations` table,
that PR MUST also land raw-payload persistence per
`docs/review-prevention-log.md` #1168 IN THE SAME PR — either by
extending an existing raw table or by introducing a sibling
`sec_frames_raw` table keyed on `(taxonomy, tag, unit, period, fetched_at)`.
Splitting persistence into a follow-up is forbidden.

This spec is the docstring-of-record for the invariant. The
provider docstring cites this spec + prevention-log entry so a
future consumer-PR's self-review surfaces the obligation.

### 3.3 Rate-limit clock — shared with companyfacts / companyconcept

`SecFundamentalsProvider`'s `_http: ResilientClient` already binds the
process-wide `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK`
imported from `sec_edgar.py`. `fetch_frame` reuses the same
`self._http.get(path)` call shape as `_fetch_company_facts` /
`fetch_concept`, so the new endpoint hits the same 10 req/s shared
SEC budget. No new clock / lock construct.

### 3.4 404 contract — return None, raise on other errors

Mirror `_fetch_company_facts` / `fetch_concept`:

- `404 → None` (no data for the given `(taxonomy, tag, unit, period)`
  combination — common when the period is too early for the
  concept, or no filers reported in that frame).
- `4xx other / 5xx → raise_for_status()` propagation. Caller handles
  via existing fault-handler shape.

## 4. Implementation surface

### 4.1 Provider method (new, on `SecFundamentalsProvider`)

```python
def fetch_frame(
    self,
    taxonomy: str,
    tag: str,
    unit: str,
    period: str,
) -> dict[str, Any] | None:
    """Fetch one cross-sectional frame from the frames API.

    Returns parsed JSON or None on 404. Other HTTP errors propagate
    via raise_for_status().

    Args:
      taxonomy: SEC namespace identifier — ASCII regex
        ^[a-z](?:[a-z0-9-]*[a-z0-9])?$ via fullmatch (e.g. 'us-gaap',
        'dei', 'srt', 'invest', 'ifrs-full'). Reuses _TAXONOMY_RE
        from the G10 primitive.
      tag: XBRL concept name — ASCII regex
        ^[A-Za-z][A-Za-z0-9_]*$ via fullmatch. Reuses _CONCEPT_TAG_RE
        from the G10 primitive.
      unit: XBRL unit identifier — common values 'USD', 'shares',
        'pure', 'USD-per-shares' (NOT 'USD/shares' — SEC frames
        URLs use the `-per-` syntax). Validated against an
        explicit token-(per-token)? grammar:
        ^[A-Za-z][A-Za-z0-9]*(?:-per-[A-Za-z][A-Za-z0-9]*)?$ via
        fullmatch. This rejects double-dash ('USD--per-shares'),
        bare 'USD-per', trailing dash ('USD-per-'), and slash —
        only the documented `numerator(-per-denominator)?` SEC
        UOM shape is admitted. Per-segment validation: each token
        is leading-letter alnum (`USD`, `shares`, `pure`, single
        chars 'Y' / 'g').
      period: Calendar-period frame identifier per SEC frames spec —
        'CY{year}', 'CY{year}Q{n}', or 'CY{year}Q{n}I'
        (instantaneous). Validated against
        ^CY[0-9]{4}(Q[1-4]I?)?$ via fullmatch.

    Raises:
      ValueError: any argument fails its regex.

    Raw-payload invariant for future consumers: see spec §3.2.
    """
```

Lives on the concrete `SecFundamentalsProvider` class in
`app/providers/implementations/sec_fundamentals.py`. Does NOT extend
the `FundamentalsProvider` ABC (per §2 non-goals).

No `extract_frame_facts` companion in v1 (per §2 non-goals — XbrlFact
schema mismatch).

### 4.2 URL construction

```text
GET https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json
```

Built as `f"/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json"`,
relative to the existing `_BASE_URL = "https://data.sec.gov"` on
`_http`.

All four path segments are validated against their respective regex
via `fullmatch` (per §4.1) before any HTTP call — defence against
accidental URL-injection at the caller boundary. `fullmatch` (not
`match`) closes the trailing-`\n` hole that `^...$` admits.

### 4.3 Module-level constants

New `_UNIT_RE` + `_PERIOD_RE` patterns added alongside the existing
`_TAXONOMY_RE` + `_CONCEPT_TAG_RE` from G10:

```python
_UNIT_RE: Final[re.Pattern[str]] = re.compile(
    r"[A-Za-z][A-Za-z0-9]*(?:-per-[A-Za-z][A-Za-z0-9]*)?"
)
_PERIOD_RE: Final[re.Pattern[str]] = re.compile(r"CY[0-9]{4}(?:Q[1-4]I?)?")
```

`_UNIT_RE` uses an explicit token-(per-token)? grammar — each
token is leading-letter alnum (`[A-Za-z][A-Za-z0-9]*`), separated
by exactly one `-per-` if a denominator is present. This rejects:

- `USD--per-shares` (double dash — interior token cannot be empty).
- `USD-per` (bare `-per` with no denominator token).
- `USD-per-` (trailing dash — denominator token cannot be empty).
- Any other trailing-dash form.
- Slash + every URL-special character.

Admits: `USD`, `shares`, `pure`, `USD-per-shares`, `Y-per-shares`,
single-char units (`Y`, `g`), and any SEC-published two-token
`numerator-per-denominator` form.

`_UNIT_RE` admits dashes (for `USD-per-shares` and similar composite
denominator forms) but rejects slash + every URL-special character.
SEC frames URLs use `-per-` syntax, NOT `/` — a slash would become
an extra path segment in the f-string (Codex 1a r1 HIGH-1 ownership).
`_PERIOD_RE` admits `CY####`, `CY####Q#`, and `CY####Q#I`
(instantaneous quarterly); rejects `CY####I` (no annual-
instantaneous frame per SEC docs).

`_TAXONOMY_RE` and `_CONCEPT_TAG_RE` are reused from G10 (no
duplication).

## 5. Test plan

`tests/test_sec_fundamentals_frames.py` (new). Tests mirror the G10
template — same `_rewire_transport` helper pattern, same
`httpx.MockTransport` discipline for the rate-limit-clock test.

1. **`test_fetch_frame_url`** — `fetch_frame("us-gaap", "Assets",
   "USD", "CY2024Q1I")` hits
   `/api/xbrl/frames/us-gaap/Assets/USD/CY2024Q1I.json`. Path-only
   pin via recording-stub. `Assets` is a balance-sheet concept that
   pairs correctly with the `Q#I` (instantaneous) period; SEC docs
   reserve `Q#I` for point-in-time facts. `Revenues` is a flow and
   would pair with `CY####` annual or `CY####Q#` quarterly (no `I`).
2. **`test_fetch_frame_404_returns_none`** — stubbed 404 → None.
3. **`test_fetch_frame_5xx_raises`** — stubbed 500 with `request=`
   attached (per G10 test 3 pattern; uses
   `_rewire_transport(max_retries=0, ...)` so the test runs in
   ~0.1 s).
4. **`test_fetch_frame_rejects_malformed_taxonomy`** — parametrise
   over the same `_BAD_TAXONOMY` cases from
   `test_sec_fundamentals_companyconcept.py` (incl. trailing dash
   + trailing newline). Happy path over the same `_GOOD_TAXONOMY`
   list.
5. **`test_fetch_frame_rejects_malformed_tag`** — parametrise over
   `_BAD_TAG` + `_GOOD_TAG`.
6. **`test_fetch_frame_rejects_malformed_unit`** — parametrise over
   bad cases `["", "USD "` (trailing space), `"US D"` (interior
   space), `"USD\n"` (trailing newline), `"123USD"` (leading
   digit), `"USD€"` (non-ASCII), `"USD/shares"` (slash — SEC frames
   URLs require `-per-` syntax, NOT `/`), `"/USD"` (leading slash),
   `"USD-per-"` (trailing dash — denominator token empty),
   `"USD-per"` (bare `-per` with no denominator),
   `"USD--per-shares"` (double dash — interior token empty)`]`.
   Happy path: `["USD", "USD-per-shares", "shares", "pure", "GBP",
   "EUR", "Y", "Y-per-shares", "usd"` (lowercase admitted — primitive
   is general; doesn't second-guess SEC unit naming)`]`. Codex 1b r1
   HIGH-1 ownership: `"usd"` regex-admitted because the primitive
   is intentionally a general SEC `frames` consumer, not bound to
   a known-unit allowlist.
7. **`test_fetch_frame_rejects_malformed_period`** — parametrise over
   bad cases `["cy2024"` (lowercase), `"CY24"` (2-digit year),
   `"CY2024Q5"` (Q5 invalid), `"CY2024Q0"` (Q0 invalid),
   `"CY2024Q1A"` (A not I), `"CY2024Q1I "` (trailing space),
   `"CY2024Q1I\n"` (newline), `""`, `"FY2024"` (FY prefix),
   `"CY2024I"` (annual-instantaneous not a valid frame per SEC
   docs)`]`. Happy path: `["CY2024", "CY2024Q1", "CY2024Q4",
   "CY2024Q1I", "CY2024Q4I"]`.
8. **`test_fetch_frame_returns_parsed_payload`** — stub returns a
   real-shaped frames payload (`taxonomy / tag / ccp / uom / label /
   description / pts / data[]`) for
   `fetch_frame("us-gaap", "Revenues", "USD", "CY2024")` (annual
   flow); assert parsed dict has `data[0]["cik"] == 320193`,
   `data[0]["val"] == 391035000000` (Apple FY2024 Revenues, integer
   USD per Sep-28-2024 10-K). Codex 1b r1 MED-2 ownership —
   `CY2024Q1I` is balance-sheet-only; `Revenues` is a flow and pairs
   with `CY####` annual.
9. **`test_fetch_frame_shares_rate_limit_clock`** — identity
   assertion + behaviour-level back-to-back smoke via
   `httpx.MockTransport`. Same shape as G10 test 10. Includes
   teardown that resets `_PROCESS_RATE_LIMIT_CLOCK[0] = 0.0`.

All HTTP behaviour stubbed — no live SEC calls in CI.

## 6. Matrix + skill updates

`.claude/skills/data-engineer/etl-endpoint-coverage.md` §4 row
`data.sec.gov/api/xbrl/frames/...`:

- Before: `❌ GAP — cross-sectional one-fact-per-filer; useful for
  sector aggregates. Not currently in the v1 metrics surface.
  Tech-debt eligible.`
- After: `✅ PROVIDER PRIMITIVE 2026-05-18 (G11) —
  app/providers/implementations/sec_fundamentals.py::fetch_frame.
  No production consumer in v1 (open downstream ticket #594
  peer-comparison radar + sector heatmap has plausible demand but
  does not specifically commit to frames as the data source —
  per-ticket data-ingest design will decide). Primitive enables
  any future sector-aggregate consumer (#594-driven data ingest,
  or independent cross-sectional metric tickets). See PR audit +
  spec docs/superpowers/specs/2026-05-18-g11-frames-api-consumer.md.`

`.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 G11 row:

- Before: `OPEN (low) | — | Cross-sectional one-fact-per-filer;
  sector aggregates use case. Eligible.`
- After: `✅ CLOSED 2026-05-18 — G11 PR — provider primitive
  landed (sec_fundamentals.py::fetch_frame); no production
  consumer by design (open downstream ticket #594 does not
  specifically commit to frames; data-ingest design TBD). Re-open
  as a wiring ticket when #594's data-ingest scope settles.`

`.claude/skills/data-sources/sec-edgar.md` §1.6 (Frames row): append
"Provider primitive: `app/providers/implementations/sec_fundamentals.py::fetch_frame`
(G11, 2026-05-18). No production consumer in v1." after the existing
"Cross-sectional one-fact-per-filer" description.

## 7. Acceptance criteria

1. `SecFundamentalsProvider.fetch_frame(taxonomy, tag, unit, period)`
   callable from the public surface, returns parsed JSON dict or
   None on 404.
2. Rate-limit clock shared with companyfacts / companyconcept API
   paths (test 9 identity + behaviour).
3. 404 → None contract pinned by test 2; 5xx → raise pinned by
   test 3.
4. Taxonomy / tag / unit / period validation pinned by tests 4-7.
5. Matrix §4 + §7 + sec-edgar.md updates landed.
6. `uv run ruff check .`, `uv run ruff format --check .`,
   `uv run pyright`, `uv run pytest` all pass.

## 8. ETL clauses #8-#12 disposition

**NOT APPLICABLE** end-to-end. G11 lands a provider primitive only:

- **#8 smoke against panel:** N/A — no production consumer, no
  per-instrument figure change.
- **#9 cross-source verify:** N/A — no new figure.
- **#10 backfill:** N/A — no schema / parser / observations change.
- **#11 operator-visible figure verify:** N/A — no rollup endpoint
  affected.
- **#12 verification steps in PR body:** §8 disposition + §3.1
  decision-rule output recorded.

## 9. Settled-decisions check

- **Provider boundary / Provider design rule**: preserved.
  `fetch_frame` is thin HTTP, no DB access, no domain orchestration.
- **Fundamentals provider posture**: preserved. SEC EDGAR frames is
  free / regulated.
- **Identifier strategy**: preserved. Provider takes raw frame
  identifiers; no symbol resolution.
- **Auditability**: preserved. Provider stays thin; future-consumer
  PR carries the raw-payload obligation.

## 10. Codex 1a pre-r1 focus points (historical, resolved in §11)

These prompts were pre-r1 sanity checks. Codex 1a r1 + r2 resolved
each in §11; preserved here for spec audit trail only.

1. Primitive-only decision soundness given #594 open. → §11 LOW
   CONFIRMED.
2. `_UNIT_RE` slash permissiveness. → §11 HIGH FIXED (slash rejected;
   `-per-` syntax adopted).
3. `_PERIOD_RE` completeness. → §11 OK FIXED (CY####I rejected;
   CY####Q#I admitted).
4. Comment generalisation in `sec_fundamentals.py`. → §11 LOW FIXED
   (implementation update planned).
5. Test 9 cleanup pattern. → §11 OK CONFIRMED.

## 11. Codex 1a r1 disposition

| Finding | Severity | Resolution |
|---|---|---|
| `_UNIT_RE` admits slash; SEC frames URLs use `-per-` syntax, slash would become extra path segment | HIGH | FIXED `_UNIT_RE` tightened to `^[A-Za-z][A-Za-z0-9-]*$` (rejects slash). Happy-path fixtures pivoted from `USD/shares` to `USD-per-shares`. Test 6 includes `"USD/shares"` as a rejection case to pin the contract. |
| Primitive-only sound but matrix wording must stay explicit (PROVIDER PRIMITIVE not WIRED) | LOW | CONFIRMED §6 matrix entries already use PROVIDER PRIMITIVE framing |
| `_PERIOD_RE` complete per SEC docs (CY#### / CY####Q# / CY####Q#I); pin `CY####I` rejection | OK | FIXED test 7 now includes `"CY2024I"` as rejection case |
| Comment block in `sec_fundamentals.py` around `_TAXONOMY_RE` + `_CONCEPT_TAG_RE` is G10-specific; generalise | LOW | FIXED implementation will update comment to "G10 + G11 XBRL API validation" + cite both specs |
| Test cleanup — G10 test-10 pattern sufficient | OK | CONFIRMED no additional cleanup needed |

Spec CLEAN v2 — proceed to plan.

## 12. Codex 1a r2 disposition

| Finding | Severity | Resolution |
|---|---|---|
| `_UNIT_RE` trailing-dash admits `USD-per-` while test 6 says reject it | MED | FIXED `_UNIT_RE` now `^[A-Za-z](?:[A-Za-z0-9-]*[A-Za-z0-9])?$` — trailing-alnum anchor (mirrors G10 `_TAXONOMY_RE` fix from PR #1198 bot round-1) |
| §10 contains stale pre-r1 prompts referring to old slash regex + CY####I premise | LOW | FIXED §10 re-titled "pre-r1 focus points (historical, resolved in §11)" with each prompt mapped to its §11 resolution |

Spec CLEAN v3 — proceed to plan.

## 13. Codex 1a r3 disposition

| Finding | Severity | Resolution |
|---|---|---|
| §4.1 docstring still lists pre-r2 unit regex with permissive trailing dash | LOW | FIXED §4.1 docstring updated to `^[A-Za-z](?:[A-Za-z0-9-]*[A-Za-z0-9])?$` |

Spec CLEAN through r3 — proceed to plan.
