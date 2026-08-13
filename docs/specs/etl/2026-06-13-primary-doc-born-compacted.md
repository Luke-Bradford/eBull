# Stop persisting write-only primary_doc payload at source (born-compacted)

Issue: #1615. Area: filings ETL / data-engineering. Follows A1 (sweep +
VACUUM reclaim, dev 44→21 GB this session). A3 (durable guard) is a
separate follow-up PR.

## Problem

`filing_raw_documents.primary_doc` payload is **write-only** — no rewash
parser reads it (pinned by `tests/test_raw_payload_retention.py`), and
manifest rebuild re-fetches from EDGAR unconditionally (sql/190). It was
~22 GB / 52 % of dev DB. The #1014 sweep nulls it retroactively but it
regrows (742 MB → 23 GB in 3 days) because steady-state ingest re-stores
the payload. Fix it at the **source**: never persist the bytes.

## Write sites (7, not 3)

`document_kind="primary_doc"` with payload:
- Services (lazy / API path): `eight_k_events.py:727`,
  `business_summary.py:1194`, `institutional_holdings.py:1466`.
- **Manifest parsers (steady-state bulk drain — the regrowth engine):**
  `manifest_parsers/sec_10k.py:307` + `:448`, `manifest_parsers/sec_13f_hr.py:294`,
  `manifest_parsers/eight_k.py:293`.

## Design — centralize born-compaction in `store_raw`

All 7 sites call `raw_filings.store_raw(... payload=...)`. Rather than
edit 7 call sites (miss-prone; a future writer forgets), branch **once**
in `store_raw`:

- `document_kind ∈ SWEPT_DOCUMENT_KINDS` (= `{"primary_doc"}`): write the
  row **born-compacted** — `payload = NULL`, `payload_sha256 =
  encode(sha256(convert_to(%(payload)s,'UTF8')),'hex')` (server-side,
  byte-identical to the #1014 sweep + the rehydrate verifier per
  sql/190), `payload_swept_at = NOW()`, plus `source_url`,
  `parser_version`, `fetched_at = NOW()`. The payload param is sent only
  to be hashed server-side; it is never stored in the column. `ON
  CONFLICT` updates the same shape (re-using the `%(payload)s` named
  param in both the INSERT and the UPDATE SET).
- All other kinds: unchanged (store payload, clear sweep state).

This is exactly the post-sweep / rehydratable state, so:
- `chk_swept_rows_carry_hash` (payload NULL ⇒ sha + swept_at) is satisfied.
- `byte_count` (GENERATED from `octet_length(payload)`) is NULL — the
  operator storage chip reports live payload bytes only (correct: 0).
- The #938 raw-before-parsed invariant holds (the row exists; only the
  bytes are absent).
- `rehydrate_raw_document` still works (sha + source_url present): any
  future need re-fetches from EDGAR and hash-verifies.
- The #1014 sweep skips these rows (its batch requires `payload IS NOT
  NULL`) — it becomes a no-op for primary_doc, defense-in-depth only.

The `if not payload: raise` guard stays: callers must still pass the
bytes (we hash them), and an empty payload is still a bug.

### `SWEPT_DOCUMENT_KINDS` single source of truth

Move the canonical `SWEPT_DOCUMENT_KINDS` frozenset from
`raw_payload_retention.py` into `raw_filings.py` (next to `DocumentKind`
+ `store_raw`); `raw_payload_retention` imports it from there. Avoids a
circular import (retention already imports `DocumentKind` from
raw_filings) and gives A3's retention-classification test one set to key
on.

### source_url is load-bearing for swept kinds (Codex ckpt-1 #1)

A born-compacted row's only recovery path is `rehydrate_raw_document`,
which needs `source_url`. All 7 current sites pass a non-empty
`source_url`, but `store_raw`'s signature allows `None` and the existing
`ON CONFLICT` would overwrite a good URL with `NULL` → an unrecoverable
`payload NULL + sha + no URL` row. So:
- For `SWEPT_DOCUMENT_KINDS`, `store_raw` **raises** if `source_url` is
  empty (a born-compacted row MUST be rehydratable — fail loud at write).
  The guard makes `EXCLUDED.source_url` non-NULL on every conflict, so
  `ON CONFLICT … source_url = EXCLUDED.source_url` can never regress a
  stored URL to NULL (no COALESCE needed).

### raw_status semantics — redefined, not "cosmetic" (Codex ckpt-1 #2/#4)

The 7 callers flip manifest `raw_status='stored'` after `store_raw`, and
sql/118 documents `raw_status` as "filing_raw_documents has the body".
Born-compaction makes that literally false for primary_doc, and existing
tests assert body presence (`test_institutional_holdings_ingester.py::
test_raw_payload_persisted_for_primary_doc_and_infotable` via
`require_payload()`; `test_etl_source_to_sink.py` `byte_count > 0`;
`test_manifest_parser_sec_10k.py`). This is a **conscious model change**,
not cosmetic:

- Redefine `raw_status` semantics via a new `sql/195` `COMMENT ON
  COLUMN` migration (sql/118 is an applied migration — the #1333
  content-drift guard forbids editing it): "a raw evidence row exists
  for this accession; whether the **bytes** are present is given by
  `filing_raw_documents.payload_swept_at IS NULL` / `byte_count`".
  `payload_swept_at` (not `raw_status`) is the authoritative "is the body
  actually here" signal — already true for #1014-swept rows.
- Keep `raw_status='stored'` on born-compacted rows (avoids editing 7
  callers; the chokepoint stays in `store_raw`). Consequence: a
  payload-less primary_doc may read `'stored'` (born-compacted) or
  `'compacted'` (#1014-swept). The two are indistinguishable by
  `raw_status` alone — acceptable, because no consumer distinguishes
  them: both are payload-less + carry sha + URL and rehydrate identically.
  Verified no constraint/check enforces `stored ⇒ payload present`;
  rewash keys on `document_kind` + `require_payload()` (raises on NULL),
  rehydrate keys on `payload IS NULL` + sha, the sweep batch keys on
  `payload IS NOT NULL`.
- Update every test asserting primary_doc body/`byte_count` presence to
  the new contract (payload NULL, sha present, source_url present). These
  are listed in the test section.

### Rehydrate of a born-compacted 13F primary_doc (Codex ckpt-1 #3)

The #1014 sweep targets only `sec_10k`/`sec_8k`, so a 13F `primary_doc`
that is *rehydrated* (payload restored) is not re-compacted by the
backstop sweep. In practice nothing in production calls
`rehydrate_raw_document` for `primary_doc` (it is the manual operator
recovery path), so this is theoretical. Documented: rehydrate is allowed
to create a transient live 13F primary_doc payload until the next ingest
overwrites it (which, post-fix, born-compacts it again). A3 may widen
sweep eligibility if this ever matters.

## Tests (pure-logic + one DB integration)

- `store_raw(primary_doc, payload=…)` → row: `payload IS NULL`,
  `payload_sha256` = the server-side hash of the input, `payload_swept_at`
  set, `byte_count IS NULL`, `source_url` kept. (DB integration — one new
  mechanism.)
- `store_raw(form4_xml, payload=…)` → payload **stored**, sha NULL
  (regression guard: re-wash kinds unaffected).
- Idempotent re-call of `store_raw(primary_doc)` stays born-compacted (no
  payload resurrected).
- The recorded `payload_sha256` equals
  `hashlib.sha256(payload.encode("utf-8")).hexdigest()` (rehydrate-verify
  consistency).
- `SWEPT_DOCUMENT_KINDS` import-from-raw_filings round-trips
  (raw_payload_retention still references the same set).

## Dev verification (CLAUDE.md clauses 8-12)

- Panel: trigger a re-ingest of a known 10-K (e.g. AAPL) + an 8-K + a 13F
  primary_doc on dev; confirm each new `filing_raw_documents` primary_doc
  row is born `payload IS NULL` + `payload_sha256` present.
- Confirm `pg_total_relation_size('filing_raw_documents')` stays flat
  across a manifest-drain tick (no payload regrowth) vs the pre-fix
  regrowth.
- Cross-source: rehydrate one born-compacted primary_doc and confirm the
  live EDGAR re-fetch hash-matches the recorded sha (proves the recorded
  hash is the real document's).
- Record commit SHA + figures in the PR.

## Out of scope (→ A3)

Scheduling the sweep as a backstop, the retention-classification CI
test, per-table size alarm, docs/settled-decisions + data-engineer skill
§13.D. The 13F `infotable_13f` second store (institutional_holdings:1531)
is a re-wash kind — untouched.
