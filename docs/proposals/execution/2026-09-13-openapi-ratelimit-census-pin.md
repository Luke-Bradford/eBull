# Pin the quota census against the OpenAPI document — #2946 step 3 item 4

Closes the last substantive item on #2946. Step 2's wording:

> Pin the next census against `/api-reference/openapi.json` with a content hash — the
> current census is a dated transcript, not a pinned artefact.

## Source rule

Step 1 read the **rendered portal pages** with WebFetch, which returns a model's reading of
rendered HTML: no bytes, no hash, and a per-page prose sentence as the only carrier of the
limit.

`https://api-portal.etoro.com/api-reference/openapi.json` is a raw JSON document and
**every one of its 177 operations carries a structured `x-ratelimit`**:

```json
"x-ratelimit": { "limit": 60, "window": 60, "shared": true, "defaultPool": false,
                 "sharedWith": ["GET /api/v1/trading/info/demo/pnl", "…"] }
```

Measured on the fetched bytes (1,787,381 bytes, `info.version` `v1.375.0`): `paths = 147`,
`operations = 177`, `with x-ratelimit = 177`. The limit is a **field**, the pool membership
is an **enumerated list**, the dedicated/shared distinction is a **boolean**.

⚠ **The field names are not the semantic authority — the operation descriptions are, and
they are what this cites.** Each operation's `description` states the rule in prose beside
the extension, e.g. *"Rate limit: 60 requests per 60 seconds. This is the default shared
quota"*. The artefact stores the matching description sentence next to each extract so the
two can be read together.

⚠ **This is a second REPRESENTATION, not an independent upstream witness.** The rendered
pages and this JSON are plausibly generated from one annotation source, which is the same
caveat `etoro_quota_lanes.QuotaLane.witnesses` already records for two pages agreeing. What
it genuinely rules out is *our* transcription error, and it gives us bytes to hash. It does
not double-source eToro.

⚠ It does not retire the general rate-limit page, which classifies by OPERATION and does
not live at the same level. `CallSite.general_tier_per_minute` is untouched.

## Premise, measured before speccing

Cross-check of the live lane map against the spec, all 19 resolvable call sites:

| what | result |
| --- | --- |
| `LANES[site.lane].documented_per_minute` vs `x-ratelimit.limit` | **19 / 19 agree.** A=20, B=20, C=20, D=60, E=60, F=120, G=60. |
| dedicated lanes B and C | `shared: false`, no `sharedWith` — confirms *"dedicated, pooled with nothing"*, which step 1 recorded on a **single witness**. |
| lanes D and E | `peers = 2` each, i.e. 3-member pools counting self. |
| lanes A and F | `peers = 10`, i.e. 11-member pools counting self. |

⚠ **Scope of that check, stated rather than generalised.** It covers the 19 call sites,
which are 17 distinct operations of 177. It says nothing about the real-environment
counterparts we do not call, about peers we never reach, or about any lane's
`env_separation`. The full-population checks below are what extend past it, and they are
about the spec's *internal* consistency, not about our coverage.

### ⚠ Four findings the cross-check surfaced that are NOT "pin the hash"

1. **Three paths our code calls have no operation in the spec** —
   `POST …/market-open-orders/by-amount`, its `by-units` sibling, and
   `GET /api/v1/trading/info/demo/orders/{orderId}`. They are not merely undocumented:
   `by-amount` is **named inside 6 other endpoints' `sharedWith` lists**, `by-units` in 6,
   the order-info path in 2. The spec assigns them to pools while defining no operation for
   them. ⚠ Undocumented is not nonexistent — same posture `KNOWN_PATH_DRIFT` takes.
2. **The spec has 14 distinct orphan peer references** — names appearing only in
   `sharedWith` lists with no operation of their own, of which the three above are ours.
   Stored in the artefact, because an orphan that later acquires an operation is a portal
   change worth seeing.
3. **Lane G's membership is NOT unenumerable in this source.** `defaultPool: true` holds on
   **109 of 177 operations**, so the default pool has a documented membership here even
   though no member carries a peer list. The lane title *"membership UNENUMERATED"* is true
   of the prose census and **false of this document**. ⚠ Recorded, not renamed: what the
   109 means for the lane's effective budget is a quota question, not a census one, and
   renaming the lane without answering it would swap one wrong impression for another.
4. **The structured source puts trade history at 60/min, `defaultPool: true`** — no 20/min
   classification anywhere on the operation. ⚠ **This does NOT establish that the general
   page is wrong about it.** That page describes historical trade information *for a user
   profile*, and the spec separately defines `/user-info/people/{username}/tradeinfo` —
   so the prose may be classifying that endpoint, not ours. **The ambiguity is preserved,
   not resolved**, and item 1's floor (merged `2302624e`) is unchanged. Loosening a floor
   on one newly-read representation is a behavioural change needing its own unit and its
   own evidence; a census must not quietly become a retune.
   ⚠ Do not restate item 1's cost as "+2.2s on a 5-minute job" — that was its *steady
   state*, and the post-fill and WS-reconcile triggers are not five-minute jobs. Its own
   spec states the bounds; and it explicitly rejects treating over-restriction as
   operationally free, because slower pacing delays reconciliation.

## Change

1. **`tests/fixtures/etoro/openapi_v1.375.0.json`** — the fetched response body, committed
   verbatim. ⚠ Committing 1.79 MB is deliberate: without the source, `document_sha256` can
   never be recomputed, the extract cannot be shown to have come from those bytes, and
   "absent from the spec" is unprovable offline — an absence recomputed from an extract of
   the same absence is circular.
2. **`tests/fixtures/etoro/openapi_ratelimit_census.json`** — the extract: provenance
   envelope, both hashes, per-call-site block, `KNOWN_UNTHROTTLED` block, pinned
   unresolved-path list, orphan peer references.
3. **`scripts/refresh_2946_openapi_census.py`** — fetch → validate → extract → **diff
   against the committed artefact** → write both files atomically. Follows
   `scripts/refresh_fixture_pinning.py`'s WHEN TO RUN / WHEN NOT TO RUN shape.
4. **`tests/test_2946_openapi_ratelimit_census.py`** — offline cross-checks.
5. `tests/test_etoro_quota_lanes.py`'s header note and
   `scripts/measure_2946_quota_load.py`'s provenance line, both of which currently tell the
   reader the census is unpinned.

### Provenance envelope

`source_url`, `fetched_at` (UTC, ISO-8601), `byte_length`, `info_version`,
`extractor_version`, and a note naming **exactly what is hashed**: the raw response bytes
as received, before any decode. Without that last part, an extractor change and a source
change are indistinguishable.

### Two hashes, deliberately

- `document_sha256` — over the raw response bytes. Answers *"did the portal change at
  all"*. Noisy by construction.
- `extract_sha256` — over the canonical form of the extract blocks only. Answers *"did
  anything WE depend on change"*.

⚠ Neither is authenticity. A hand-edited extract with a recomputed hash passes both; the
guard is that the **source file is committed**, so the test recomputes the extract *from
the source* and compares. That is what makes editing the artefact fail.

⚠ `extract_sha256` does not cover everything we depend on: the general page's rules, the
real-environment counterparts, uncalled peers' annotations and the 109-member default pool
can all move without touching a called operation's block.

Canonicalisation: `json.dumps(..., sort_keys=True, separators=(",", ":"))`, UTF-8, peer
lists **sorted and de-duplicated with self removed** before hashing — `sharedWith` ordering
carries no meaning and would otherwise make the hash noisy.

### Operation identity

The key is `"{VERB} {path}"` — the spec's own `sharedWith` syntax. Method-only keys would
collide (`place_order` reaches two paths); path-only keys would lose the verb (one path
carries several). Each extract entry carries the list of our call-site methods that reach
it, so the mapping stays many-to-one in the direction it actually is.

### Path resolution

Exact `(verb, path)` match first. `{env}` expands to **both `demo` and `real`** — the raw
pnl probe in `KNOWN_UNTHROTTLED` builds the real path too, and demo-only substitution would
hide half of it. A trailing `...` (the debug candle probe) expands against the spec's path
set and **requires exactly one match**: zero and ambiguous are distinct errors, never a
silent first hit.

### The test must compare the RIGHT number

`LANES[...].documented_per_minute` against `x-ratelimit.limit`. **Not**
`conservative_per_minute`, which folds in `general_tier_per_minute` from the other page and
would fail on history and the TP/SL edit by design.

⚠ Per the lesson this repo just recorded (`docs/review-prevention-log.md`, "a derivation can
silently turn a comparison test into an identity test"): the two sides are separately
authored — the artefact is eToro's, the lane map is ours — so this is a real cross-check.
**Never regenerate the lane map from the artefact.** Doing so makes this test an identity
and leaves the census unguarded.

## What this does NOT do

- **Does not change any floor, lane assignment, lane title or documented number.** The
  cross-check passes 19/19; findings 3 and 4 are explicitly deferred.
- **Does not make the test fetch the network.** A test that reaches the internet fails in CI
  and trains people to skip it.
- **Does not prove our runtime inventory.** The AST guards count expressions, so an
  equal-count URL substitution passes them, and comparing the spec against an unchanged
  hand-maintained table cannot detect that. "Nothing needs to change" means *the numbers in
  the table agree with the document*, not *the code calls what the table says*.
- **Does not claim the spec is a complete inventory** — findings 1 and 2 prove it is not.
- **Does not claim documentation is enforcement.** Still true, and still why item 3's
  request artefact exists.
- **Does not close clause 2** (independent provider instances) or item 2 (lane split).

## Tests

Offline, against the committed source + artefact.

1. **Extract is reproducible from the source** — re-run the extractor over the committed
   JSON and compare to the artefact, field for field. This is the check a hand-edited
   artefact fails, and it subsumes `extract_sha256` as an integrity claim.
2. **Exact coverage, both inventories.** Every `CALL_SITES` entry and every
   `KNOWN_UNTHROTTLED` entry is either resolved in the extract or named in the pinned
   unresolved list — no third state. A new call site fails until it is recorded.
3. **The unresolved list is pinned, not regenerated** — an entry that now resolves fails
   (self-cleaning, the `ACCEPTED_FLOOR_EXCEPTIONS` shape), and a path that stops resolving
   fails rather than being silently blessed.
4. **Limit, window and scope** per resolved site: `limit` == the lane's
   `documented_per_minute`, `window` == `window_s`, `shared: false` ⟺ `scope == "dedicated"`,
   `defaultPool: true` ⟺ `scope == "default"`. Applied to `KNOWN_UNTHROTTLED` too, which the
   first draft checked on limit alone.
5. **Pool IDENTITY, not pool size.** Two sites on one lane *and environment* must report
   the same normalised `{self} ∪ sharedWith` set. ⚠ Counts are not enough: moving a lane-D
   call into lane E passes every limit/window/scope/size check, because both are shared
   60/min 3-member pools.
   ⚠ **Partitioned by environment because the first draft of this test FAILED**, and
   correctly: lane E holds two documented pools, demo and real. The environment is read
   from the operation's `tags` (`"Trading - Demo"` / `"Trading - Real"` / `"Market Data"`)
   — a structured field, not a `/demo/` path heuristic.
5b. **`env_separation="witnessed"` is now checked, not just asserted.** The lane map's
   claim is *"demo and real memberships were enumerated and are disjoint"*; with the pool
   lists in hand, the document can hold us to it. Only where both environments are
   actually reached — an absent side would pass vacuously while looking covered — and the
   test asserts that at least one lane qualified, so it cannot quietly check nothing.
6. **Full-population internal consistency** (all 177 operations, not our 17): `sharedWith`
   is reciprocal — if A names B, B names A — and no operation appears in two different
   non-default pools. These are the spec's own invariants, and a violation means the
   document, not our map, is the thing to re-read.
7. **Extractor validation** — missing / null / malformed `x-ratelimit`, a non-integer or
   non-positive `limit`/`window`, a non-boolean flag, contradictory scope flags
   (`shared: false` with a peer list), and a malformed peer identity each raise. ⚠ A
   malformed annotation must never fall through into the unresolved list: "absent" and
   "present but unreadable" are different facts.
