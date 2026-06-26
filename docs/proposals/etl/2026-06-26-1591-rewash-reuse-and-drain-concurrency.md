# #1591 — Rewash/drain efficiency: reuse stored bodies (retained kinds) + drain concurrency

Status: proposal (unshipped). PR1 branch `feature/1591-rewash-reuse-stored-html`; PR2 follows.

## Problem

A `sec_rebuild` re-drain (and the standalone #554 drain) re-runs the manifest-worker
parsers, which **refetch the upstream body unconditionally** even when it is already in
`filing_raw_documents`. Observed in the #554 backfill: ~4.7 s/row, ~1 req/s against a
10 req/s throttle. Two compounding costs → two PRs.

## Source rule

- **SEC filings are immutable once filed.** A given accession's documents never change
  after acceptance; an amendment is a NEW accession with its own number (sec-edgar skill
  §filing identity; settled-decisions "Filing dedupe"). So for a fixed `(accession,
  document_kind)` a stored body is permanently fresh — "present" == "reusable", no decay.
- **Raw-payload retention is partitioned by settled #1617** (settled-decisions.md,
  "Raw-payload retention"): every `DocumentKind` is exactly one of re-read (REWASH) /
  housekept (SWEPT, born-compacted) / kept-negligible. A stored-body READER may be added
  only for a kind whose payload is retained — adding one for a SWEPT kind fails
  `tests/test_raw_payload_retention.py::test_swept_kinds_have_no_rewash_parser` +
  `::test_retention_buckets_are_pairwise_disjoint`.

## Full-population verification (dev DB `filing_raw_documents`, 2026-06-26)

| document_kind | rows | with_payload | swept | avg_bytes | reusable on re-drain? |
|---|---:|---:|---:|---:|---|
| form4_xml | 465,150 | 465,150 | 0 | 6.7K | ✅ |
| **primary_doc** (10-K/8-K/13F-primary) | 111,297 | 38,413 | **72,884** | 73K | ❌ SWEPT #1617 → 100% |
| form3_xml | 59,916 | 59,916 | 0 | 5.8K | ✅ |
| def14a_body | 42,137 | 42,137 | 0 | **725K** | ✅ |
| primary_doc_13dg | 27,743 | 27,743 | 0 | 10K | ✅ |
| infotable_13f | 16,237 | 16,237 | 0 | 198K | ✅ (multi-doc → follow-up) |
| form5_xml | 1,586 | 1,586 | 0 | 7.1K | ✅ |
| nport_xml | 151 | 151 | 0 | 72K | KEPT_NEGLIGIBLE → follow-up |

**Premise correction.** The ticket's flagship `_parse_sec_10k` (`primary_doc`) is the ONE
kind that is born-compacted (`SWEPT_MANIFEST_SOURCES={sec_10k,sec_8k}`, raw_filings.py:89;
65 % already swept, trending 100 %). Reuse there is value-dead AND CI-blocked. The real
reuse win is the retained ownership kinds. Operator decision 2026-06-26: **retarget Part 1
to the retained kinds; keep Part 2 concurrency for 10-K/8-K** (whose only lever is overlap,
since their body must be rehydrated). No manifest parser reuses a stored body today.

## PR1 — reuse stored bodies (retained single-primary kinds)

Scope: `_parse_def14a`, `_parse_form4`, `_parse_form3`, `_parse_13dg` (4 parsers).
Out: `primary_doc` (born-compacted), 13F infotable (multi-doc), n_port + **form5_xml**
(both KEPT_NEGLIGIBLE — a manifest-parser READER would falsify their #1617 "no payload
reader" justification; Form 5 is 1,586 rows / 7 KB so the win is negligible anyway →
follow-up, promote out of `KEPT_NEGLIGIBLE_DOCUMENT_KINDS` if/when reused). Codex ckpt-1.

New helper in `app/services/raw_filings.py` (sibling to `read_raw`, single store home):

```python
def stored_body(conn, *, accession_number, document_kind) -> str | None:
    """Stored payload for reuse, or None when absent OR swept (caller must
    fetch). 'present' == 'fresh' — SEC filings are immutable per accession."""
    doc = read_raw(conn, accession_number=accession_number, document_kind=document_kind)
    return doc.payload if doc is not None else None
```

Each parser, replacing its unconditional fetch+store with a reuse-first branch (all
existing PRE-FETCH gates — instrument_id / url / filed_at / retention / def14a cap — stay
ABOVE this; reuse replaces only the fetch, never a gate):

```python
body = stored_body(conn, accession_number=accession, document_kind="<kind>")
if body is None:                       # first ingest, or swept → rehydrate
    <existing fetch via provider.fetch_document_text(...)>
    if not body: <existing empty-body tombstone>
    <existing store_raw(...)>          # fetch path only
# parse(body) — unchanged
```

Invariants preserved:
- **#938 store-before-parse** holds on reuse — the row already exists with a payload.
- **fetched_at not churned** on reuse (no re-store) — matches `_bump_parser_version`'s
  "preserve operator-visible last-published timestamp" rule.
- **raw_status="stored"** on the reuse path (the row has a payload) — the worker's #938
  audit (`effective_raw_status`) passes without a store_raw call.
- Empty-body guard runs on the fetch path only; stored bodies are non-empty by
  construction (`store_raw` rejects empty).
- def14a hoists its `issuer_cik` resolve + insider/13dg hoist `canonical_url`/`primary_url`
  ABOVE the branch (pure / body-independent; both paths need them downstream).

**Mirror the new pre-fetch gate in the prefetch hooks** (prevention-log #1956 — a hook
that front-runs a gated consumer must replicate EVERY pre-fetch gate, else it burns the
budget it exists to save). `_insider_fetch_url`, `_def14a_fetch_url`, `_blockholder_fetch_url`
return `None` when the body is already stored. Placed AFTER the existing cheap gates so a
gated-out row never pays the DB read. Hooks run savepoint-wrapped in `_prefetch_bodies`
(prevention-log #1963), so the read is safe on the shared conn.

- def14a → `def14a_body`; 13d/g → `primary_doc_13dg` (single kind each).
- `_insider_fetch_url` is shared by Form 3/4/5, so map `row.source` to the EXACT kind via
  an explicit `{"sec_form3": "form3_xml", "sec_form4": "form4_xml"}` dict and **fail
  closed** — a source NOT in the dict (notably `sec_form5`, which PR1 does NOT reuse) skips
  the stored-check and prefetches as today (Codex ckpt-1 #4: a shared `form4_xml` check
  would wrongly skip Form 3/5 rows). The dict's keys == the set of reused insider sources.

Pre-impl invariant audit: confirm EVERY post-body outcome in each of the 4 parsers
(success, parse-failure, tombstone, transient-upsert-fail, deterministic-upsert-tombstone)
already returns `raw_status="stored"`. The reuse branch joins the flow at the parse step,
so those returns must hold for the reuse path identically (Codex ckpt-1 #2 — else a
reuse-hit failure with manifest `row.raw_status='absent'` would mis-trip the worker's #938
audit). They do today (the "8-K Codex round 2 BLOCKING" rule); the audit just verifies none
slipped.

Tests (pure-logic preferred):
- `stored_body`: hit → payload; missing row → None; swept (payload None) → None.
- each parser, stored-hit SUCCESS: reuses (no fetch, no store_raw, parses, outcome
  `raw_status="stored"`). Fake provider (asserts un-called) + store_raw spy.
- each parser, stored-hit FAILURE: a parse-raising body on the reuse path still returns
  `raw_status="stored"` (construct the manifest row with `raw_status="absent"` to prove the
  outcome repairs `effective_raw_status`). Codex ckpt-1 #2.
- each parser, MISS: fetches + stores (existing behaviour unchanged).
- each hook: returns None when stored; returns URL when not stored; `_insider_fetch_url`
  with `sec_form5` prefetches even when a `form5_xml` body exists (fail-closed mapping);
  existing gate tests unchanged.

## PR2 — drain concurrency (serial paths + 10-K/8-K hooks)

The fairness tick is already concurrent (#1686/#1700 prefetch cache + hooks). Two gaps:

1. **Serial re-drain paths.** `run_manifest_worker(source=...)` (per-source rebuild, what
   `sec_rebuild` drains into) and the standalone drain call `_dispatch_rows` directly.
   Swap the per-source rebuild to the existing `_prefetch_then_dispatch` (binds the #1686
   cache) so a re-drain overlaps fetches against the shared throttle.
2. **10-K/8-K have no `fetch_url` hook** → never prefetched. Add `_sec10k_fetch_url` /
   `_eight_k_fetch_url` mirroring their pre-fetch gates (url present, instrument_id
   present; no retention gate), returning `primary_document_url`. XBRL linkbase prefetch
   for 10-K (separate provider + `index.json` discovery) is out of scope, documented.

Throttle: reuses `concurrent_fetch` over the shared `_PROCESS_RATE_LIMIT_*` clock —
overlaps wait, never exceeds 10 req/s. Single per-process budget; no multi-process
(ticket non-goal — aggregate would exceed SEC fair-access).

Accepted tradeoff (Codex ckpt-1 #5): prefetching the whole batch up front widens the
pre-dispatch window, so a row another drainer parses between prefetch and dispatch wastes
its prefetched fetch. This is a rate-budget cost, never data corruption (the serial parser
still re-checks/transitions; the standalone drain already rolls back `parsed→parsed`
races), and is the identical property the fairness tick already ships (#1686). No fresh
status re-read added — the per-source rebuild scope is normally drained by one worker.

Tests: `_sec10k_fetch_url` / `_eight_k_fetch_url` return None on missing url/instrument_id,
URL otherwise; per-source rebuild path prefetches (cache bound).

## Dev-verify (ETL DoD 8-12)

- **PR1:** scoped `POST /jobs/sec_rebuild/run` on a known instrument's def14a + form4;
  assert parse output BYTE-IDENTICAL to a fetch-path run (parity ⇒ no data change) and
  count HTTP saved (= reused rows). Smoke def14a ownership rollup + form4 insider rows for
  AAPL/MSFT.
- **PR2:** scoped sec_10k re-drain; confirm req-rate rises toward the throttle + wall-clock
  drops; AAPL/MSFT 10-K business-summary + segments unchanged.
- Parity proven ⇒ no `sec_rebuild` backfill needed for correctness (output row-identical);
  restart the jobs daemon onto new main post-merge (parser-touching).

## Non-goals

Multi-process drains; un-compacting `primary_doc` (#1617); 13F infotable + n_port reuse
(follow-up); XBRL linkbase prefetch.
