# #1730 — Prefetch 10-K XBRL linkbase artifacts on re-drain (concurrency)

Status: spec (live). Branch `feature/1730-prefetch-10k-xbrl-linkbase`.
Follow-up to #1591 PR2 (`36fcbc7b`, #1728) which added `_sec10k_fetch_url` so the
10-K PRIMARY doc (business-summary HTML) is prefetched concurrently. The 10-K's
dimensional XBRL fetches dominate the re-drain cost and stay SERIAL.

## Problem

`_fetch_dimensional_facts` (`app/services/manifest_parsers/sec_10k.py`, step 2.5
of `_parse_sec_10k`, #554) fetches, per accession, via a separate
`SecFilingsProvider` after an independent index discovery:
1. `fetch_filing_index(accession)` → `index.json` (parsed dict)
2. `discover_xbrl_files(index, primary_name)` → instance/label/def names
3. `fetch_document_text(base + name)` × {instance, label, def} (~3 fetches)

= ~4 fetches per row, all SERIAL inside the dispatch loop, so they do NOT overlap
across rows. The primary HTML (1 fetch) IS prefetched (PR2); the XBRL (~4) is not.

The existing 2-pass prefetch (`_prefetch_bodies`) cannot reach it: pass-2
(`expand_urls`) keys off the pass-1 BODY (the primary HTML), but the XBRL chain is
discovered from `index.json` — a doc INDEPENDENT of the primary HTML. Making
`index.json` the pass-1 doc would regress primary-HTML prefetch for the (rare)
no-XBRL 10-K, since pass-2 is skipped on a pass-1 miss. So the XBRL chain needs a
genuinely independent third prefetch pass.

## Source rule

This is **pure concurrency, NOT a data-treatment change** — no ownership / metric
resolution, dedup, or denomination decision. Output is byte-identical (parse-and-
drop). The governing rules:

- **XBRL artifacts are parse-and-drop (#470 raw-payload scope narrowing):** the
  instance/label/def are NEVER retained in `filing_raw_documents` (every extracted
  field lands in SQL). So this is concurrency, not reuse — there is no stored body
  and no #1617 retention bucket involved.
- **SEC fair-access throttle (sec-edgar §4; cross-process GCRA #1484 + the
  in-process `_PROCESS_RATE_LIMIT_CLOCK`):** 10 req/s shared per-IP. `concurrent_
  fetch.fetch_document_texts` overlaps wait time against the SAME shared clock —
  it NEVER exceeds the floor regardless of worker count. No multi-process (would
  aggregate beyond fair-access — ticket non-goal).
- **Artifact discovery is fixed by `discover_xbrl_files`** (the existing index
  size-preference rule). The prefetch chain MUST resolve the identical artifact
  set the serial path resolves, or the cache misses — guaranteed by sharing one
  URL/discovery helper between both (see Design).

## Full-population verification (perf-only — parity is the check)

#1730 changes no signal; the verification is (a) the prefetched discovery resolves
the SAME `(index_url, artifact_urls)` the serial path resolves (no drift → cache
hits), and (b) dimensional segments output is byte-identical after a re-drain.
Verified on the dev panel (AAPL/MSFT/GME/JPM/HD 10-Ks) at dev-verify, not a sample
heuristic. Dev-DB scope: 10-K retention horizon is "last 3 annual" → effectively
all in-window 10-Ks post-date the XBRL mandate, so the chain hits on the drain
population.

## Design

Three changes. The cache is keyed by URL→text and the serial path **re-discovers
roles itself** from the (cached) index — the prefetch chain NEVER hands role
information across; it only pre-populates the cache. So there is no structured
"which body is the instance vs label" to lose (Codex ckpt-1 BLOCKING): both the
chain and the serial `_fetch_dimensional_facts` call the SAME `discover_xbrl_files`
on the SAME (index dict, primary_name) → identical refs → identical URLs → cache
hits land on the correct role in the serial path.

### 1. One archive-dir-URL builder (`sec_edgar.py`) — structural parity

The cache hits ONLY if the chain's index URL byte-matches what `fetch_filing_index`
builds — and that builder has issuer-cik-vs-prefix-fallback + agent-rejection rules
(`sec_edgar.py:445-485`). To guarantee parity structurally (not by duplication —
Codex ckpt-1 HIGH), extract the issuer-cik archive-dir construction into a single
module helper `_archive_dir_url(provider_filing_id, *, issuer_cik) -> str` returning
`https://www.sec.gov/Archives/edgar/data/{int(digits)}/{raw_id}/` (the
`len(raw_id)==18` validation stays in `fetch_filing_index`). Route through it:
`fetch_filing_index` (`+ "index.json"`), `_fetch_dimensional_facts`'s `base`, and
the chain. One source of truth → the index/artifact URLs cannot drift.
(The no-issuer_cik legacy fallback stays inside `fetch_filing_index`; the chain
only handles issuer-cik-present rows, so it never uses the fallback.)

### 2. `fetch_filing_index` becomes prefetch-cache-aware (`sec_edgar.py`)

Add a cache consult mirroring `fetch_document_text` (`:613`): if the index URL is
in `_PREFETCH_BODY_CACHE`, `json.loads(cached_text)` → return the dict; guard
non-dict / `JSONDecodeError` → fall through to the live fetch. The cache stores
`resp.text`; the chain's index round fetches via `fetch_document_text`, so
`cache[index_url]` is the index.json text. Behaviour change is scoped: **outside a
worker tick** the cache is `None` → zero change; **inside a tick** any caller of
`fetch_filing_index` for a prefetched URL now gets the cached JSON instead of live
HTTP (Codex ckpt-1 MED — caller-visible in-tick). This is safe (the cached
index.json is the same immutable doc → same dict) and in practice the only in-tick
caller of `fetch_filing_index` for these URLs is the 10-K dimensional path itself.

### 3. New `prefetch_chain` hook + `_sec10k_xbrl_prefetch` (`sec_manifest_worker.py` + `sec_10k.py`)

`PrefetchChainFn = Callable[[list[ManifestRow], SecFilingsProvider], dict[str, str]]`
+ a `prefetch_chain` field on `ParserSpec` + `register_parser` param — a
self-contained per-source prefetch for an INDEPENDENT doc-chain (one the pass-1/
pass-2 body-keyed mechanism can't model). In `_prefetch_bodies`:
- **The early `return {}` when `pass1_url_to_rows` is empty must NOT skip chains**
  (Codex ckpt-1 HIGH): restructure so the provider block opens when EITHER pass-1
  URLs OR any chain-registered rows exist, and chains run regardless of pass-1.
  Return `{}` only when neither exists.
- After pass-1/pass-2, group rows by source; for each source with a
  `prefetch_chain`, invoke `prefetch_chain(rows, provider)` inside the existing
  `with SecFilingsProvider(...)` block and merge `{url: text}` into the cache.
  Best-effort try/except (raise → log + skip; serial parser re-fetches), mirroring
  the pass-1/pass-2 hook handling. No DB / no savepoint needed — the chain is pure
  HTTP keyed on row-local cik/accession/url (unlike the conn-taking pass-1 hooks).

`_sec10k_xbrl_prefetch(rows, provider) -> dict[str, str]` (`sec_10k.py`, beside
`_fetch_dimensional_facts`, sharing `_archive_dir_url` + `discover_xbrl_files`):
1. Filter rows mirroring the parser's pre-XBRL gates: `primary_document_url` +
   `instrument_id` present (`_parse_sec_10k` step-1 tombstone), `filed_at` present
   (step-2.5 guard), `cik` present + numeric (`_fetch_dimensional_facts` skips
   otherwise). Compute each row's index URL via `_archive_dir_url`.
2. Round 1: `fetch_document_texts(provider, {index_urls})` (concurrent). Drop
   `None` (404 / pre-mandate / caught) — those rows have no XBRL.
3. Per fetched index text: `json.loads` → `discover_xbrl_files(index,
   primary_document_name=<same primary-name rule as the serial path>)` →
   `{_archive_dir_url + name}` for instance/label/def (a `set` auto-dedups the
   def==label single-fetch case). Skip on parse/discover miss.
4. Round 2: `fetch_document_texts(provider, {all artifact_urls})` (concurrent).
5. Return `{index_url: text} ∪ {artifact_url: text}` for successful fetches →
   merged into the tick cache. Serial `_fetch_dimensional_facts` then does 0 HTTP
   for the whole XBRL chain (index via change 2, artifacts via `fetch_document_
   text`'s existing cache consult) — re-discovering roles from the cached index.

Register: `register_parser("sec_10k", _parse_sec_10k, requires_raw_payload=True,
fetch_url=_sec10k_fetch_url, prefetch_chain=_sec10k_xbrl_prefetch)`.

### Accepted tradeoff (mirrors PR2's documented batch-window tradeoff)

The chain runs BEFORE the serial primary fetch + `store_raw`, so it prefetches XBRL
for rows the serial parser may then tombstone or fail at step 1/2 — a primary 404
(tombstone), a primary transient fetch failure, or a `store_raw` failure all waste
the row's prefetched XBRL fetches. This is a rate-budget cost only — never data
corruption (parse-and-drop; the serial parser still gates) — and is rate-budget
amplification only during SEC trouble (transient failures cluster). Identical in
kind to PR2's accepted primary-prefetch window tradeoff. The chain mirrors the
cheap row-local gates; the primary-fetch/store outcomes are not row-local-knowable,
so the waste on those is accepted (rare on the last-3-annual horizon).

## Tests

- `_xbrl_chain_urls` (pure): given an index dict + primary url, returns the index
  URL + the artifact URLs `discover_xbrl_files` resolves; index URL byte-matches
  `fetch_filing_index`'s construction; def==label collapses to one artifact;
  no-XBRL index → no artifacts.
- `fetch_filing_index` cache hit: with a bound cache containing the index text,
  returns the parsed dict and issues NO HTTP (assert via a fetch spy / no
  `_http_tickers.get`); malformed cached text → falls through to live fetch.
- `_sec10k_xbrl_prefetch` (DB-tier or pure with a fake provider): a batch of 10-K
  rows → returns index + artifact bodies; rows missing url/instrument_id/filed_at/
  cik are skipped (gate mirror); a 404 index drops that row's artifacts.
- End-to-end via the worker prefetch path (mirror
  `test_two_phase_prefetch_*`): a 10-K row dispatched through
  `_prefetch_then_dispatch` gets index + instance + label + def served from cache
  (0 serial HTTP for the XBRL chain); the primary HTML still served from cache;
  segments written identically.

## Dev-verify (ETL DoD 8-12)

Scoped `POST /jobs/sec_rebuild/run {"source":"sec_10k"}` (or a single-instrument
re-drain) on the panel: confirm the XBRL chain (index + artifacts) is served from
the prefetch cache (0 serial HTTP) and the `dimensional_facts` rows
(segments) are byte-identical to the pre-drain rows (parity). Smoke a
segments-bearing endpoint (e.g. `/instruments/MSFT/financials` segments / the
segments panel) for AAPL/MSFT. No `sec_rebuild` backfill needed for correctness
(parse-and-drop, output identical). Restart the jobs daemon onto new main
post-merge (parser + worker + provider touched).

## Skill ownership

Update `data-engineer SKILL §13.G` (the #1729 reuse/prefetch pattern section) with
the **independent-doc-chain prefetch** note: when a parser fetches a second
doc-chain that the pass-1-body-keyed `expand_urls` can't reach (independent index
discovery), use a self-contained `prefetch_chain` hook + a shared URL/discovery
helper (never duplicate discovery, or the cache silently misses), and make the
discovery-index fetch (`fetch_filing_index`) cache-aware so the whole chain is
0-HTTP on a prefetched re-drain.

## Non-goals

Reuse of XBRL artifacts (parse-and-drop, #470 — nothing stored); multi-process
drains; 8-K XBRL (8-K has no dimensional XBRL step); `primary_doc` reuse (#1617
SWEPT).
