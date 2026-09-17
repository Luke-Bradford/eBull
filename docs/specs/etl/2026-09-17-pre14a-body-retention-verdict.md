# `pre14a_body` retention verdict — compact at birth, bound the class

> Issue: **#2774** (child of the #3117 storage umbrella, priority 1).
> Prior art: #1014 (sweep + rehydrate), #1615 (born-compaction), #1617 (the
> three-class partition), `docs/specs/etl/retention-rubric.md` §3.3.
>
> Codex ckpt-1 returned **27 findings**. This spec was rewritten rather than
> patched; every correction is marked ⚠ **CORRECTED** in place, because in each
> case the wrong version was the one that reads naturally.

## 1. Source rule

`docs/settled-decisions.md` → "Raw-payload retention (#1617, settled 2026-06-13)":
a stored `filing_raw_documents.payload` is legitimate only under exactly one of

- **re-read** — a rewash parser reads the stored body. The mechanism is
  `raw_filings.stored_body()`, called on re-drain by the six registered kinds
  (`def14a_body`, `form3_xml`, `form4_xml`, `form5_xml`, `infotable_13f`,
  `primary_doc_13dg`); the registry is `rewash_filings.registered_specs()`.
- **housekept-and-negligible** — born-compacted at source, rehydratable from
  `source_url` (`raw_filings.SWEPT_DOCUMENT_KINDS`).
- **kept-and-negligible** — *"small, write-only, no payload reader"*
  (`raw_filings.KEPT_NEGLIGIBLE_DOCUMENT_KINDS`, kind → justification).

`docs/specs/etl/retention-rubric.md` §3.3 fixes the shape of any retention
change: *"this is an ingest-side discipline, not a row-deletion rule. Caps gate
what new ingests write. Existing rows are NEVER deleted on a per-source basis."*
That is exactly this change: born-compaction gates new writes and deletes nothing.

⚠ **No published or in-repo formulation of "small" / "negligible" exists.**
Grepped 2026-09-17: the rubric caps typed ROWS per source and never payload
bytes; #1617 gives the adjective and no number; `postgres_health` carries
DB-level alarms only. The bar is therefore fixed **by construction** in §4 and
frozen, per the "source-rule before design" rule's instruction for that case.

## 2. Premise, measured on the full population

Dev DB, 2026-09-17. Reproduce every figure below with
`scripts/verify_2774_pre14a_recoverability.py --census`; the per-class table is
`scripts/dq_audit.py`. `byte_count` is generated from `octet_length(payload)`
(`sql/107:68`) — logical bytes, not disk.

| kind | live rows | live logical | class |
| --- | --- | --- | --- |
| `def14a_body` | 43,180 | 32.564 GB | re-read |
| `infotable_13f` | 75,527 | 14.634 GB | re-read |
| **`pre14a_body`** | **557** | **3.803 GB** | **kept-and-negligible** |
| `form4_xml` | 495,222 | 3.405 GB | re-read |
| `primary_doc` | 38,410 (+145,855 compacted) | 2.817 GB | swept |
| `form3_xml` | 82,103 | 1.069 GB | re-read |
| `primary_doc_13dg` | 48,193 | 0.444 GB | re-read |
| `finra_regsho_daily_txt` | 558 | 0.144 GB | kept |
| `nt_body` | 4,329 | 0.130 GB | kept |
| `finra_short_interest_csv` | 36 | 0.074 GB | kept |
| `nport_xml` | 166 | 0.013 GB | kept |
| `form5_xml` | 1,614 | 0.011 GB | re-read |
| `prospectus_body` / `tender_body` | 0 (+51,514 compacted) | 0.000 GB | swept |

`pre14a_body`: **mean 6.83 MB, median 2.40 MB, max 313.6 MB**, stored datum size
3.407 GB (`pg_column_size` — proxies carry inline base64 graphics and barely
compress). Against the enum comment *"Small HTML; retained, not swept"* and the
justification *"reuse deferred by volume"*. It is the **largest per-row kind in
the table** and the third-largest total.

Split by manifest state, with the typed sink reconciled by **anti-join**:

| state | rows | bytes | accessions with a `pre14a_proposal_signals` row |
| --- | --- | --- | --- |
| `parsed` | 285 | 1.457 GB | **285** |
| `tombstoned` | 272 | 2.346 GB | **0** |

⚠ **CORRECTED (ckpt-1 #15).** The draft read "62% produced no typed output" off
manifest state alone. State does not establish that — a re-parse can tombstone
an accession whose earlier signal row still exists. The anti-join above does:
0 tombstoned accessions carry a signal row, 0 parsed accessions lack one. Every
tombstone carries the same error, `no recognizable numbered proposals list`.
Measured, not diagnosed.

⚠ **CORRECTED (ckpt-1 #25).** The draft's reader census missed `stored_body()`,
which is the actual re-drain reuse mechanism and therefore the definition of the
re-read class. Re-run: `stored_body` is called for exactly the six registered
kinds and never for `pre14a_body`; `_parse_pre14a` calls
`provider.fetch_document_text(url)` unconditionally on every parse and re-parse
(`manifest_parsers/sec_pre14a.py:106`). The conclusion is unchanged — no
`pre14a_body` payload reader exists — but it was under-cited.

**Verdict: write-only and not small ⇒ under #1617's own criterion it belongs in
the swept class.** This applies the settled decision; it does not reverse one.

### Alternative considered and rejected

⚠ **ADDED (ckpt-1 #14).** "No current reader" does not establish "no useful
future reader", and the rubric requires weighing plausible future consumers. The
alternative is the opposite move — promote `pre14a_body` to **re-read**, wiring
`stored_body` reuse so a future better parser can re-run without re-fetching
(tempting, given 272 unparsed filings). Rejected on arithmetic: that buys the
avoidance of re-fetching 557 documents, which at the shared 10 req/s ceiling is
**about one minute**, and costs 3.803 GB now plus 0.1–0.35 GB/month. The
re-fetch path already exists and is exercised on every parse.

## 3. Change A — reclassify

Move `"pre14a_body"` into `SWEPT_DOCUMENT_KINDS` (`app/services/raw_filings.py`).
That is the whole behavioural switch:

- `store_raw` born-compacts swept kinds (`:260-297`) — hash server-side, write
  `payload NULL`, `payload_swept_at = NOW()`, require `source_url`. `sec_pre14a`
  already passes `source_url=url`, so the parser is unchanged.
- `raw_status` needs no change: `sql/195` already redefines `'stored'` as *"a raw
  row exists — NOT whether the payload bytes are present"* precisely for
  born-compaction.
- ⚠ **CORRECTED (ckpt-1 #18).** The bytes-present predicate is
  **`payload IS NOT NULL`**, not `payload_swept_at IS NULL`.
  `chk_swept_rows_carry_hash` (`sql/190`) is one-directional — it forbids
  "bytes gone, no proof" but permits a live payload beside a sweep timestamp,
  which is exactly the state a rehydrate leaves. Every query in this change uses
  the payload predicate.
- ⚠ **CORRECTED (ckpt-1 #11).** The hash is over the **UTF-8 encoding of the
  decoded TEXT payload**, per `sql/190`'s stated semantics — not over the
  original HTTP bytes. A charset/decoder change upstream would therefore read as
  a mismatch. That is the contract #1014 already runs under.

### What this does NOT do

- **No retroactive sweep.** `sweep_raw_payloads` is double-gated on
  `document_kind ∈ SWEPT_DOCUMENT_KINDS` **and** `m.source ∈ SWEPT_MANIFEST_SOURCES`
  = `{sec_10k, sec_8k}` (`raw_payload_retention.py:67,113`).
  ⚠ **CORRECTED (ckpt-1 #1).** The draft wrote *"`sec_pre14a` is in neither"*,
  which conflates the kind and source namespaces, and the sweep **joins on
  accession** — so the question is not what the writer intends but what the
  corpus holds. Measured: all **557/557** `pre14a_body` rows hang off a manifest
  row whose `source` is `sec_pre14a`, and that query is now part of `--census`
  so it is re-checked rather than assumed.
- **No receipt contract.** #2774 step 2's durable-receipt machinery exists for
  **re-read** kinds, whose bodies back a rewash. A write-only kind whose re-parse
  re-fetches has nothing to receipt beyond `payload_sha256` + `source_url`.

### ⚠ Consequences that must be stated, not discovered

1. **Re-parse compacts an existing row.** `store_raw`'s swept branch is an UPSERT
   with `payload = NULL`, so a re-parsed `pre14a_body` row loses its bytes.
   ⚠ **CORRECTED (ckpt-1 #5).** The draft said "the only bytes discarded are
   bytes just re-downloaded". Precisely: today's non-swept UPSERT **already**
   destroys the prior body and replaces it with the newly fetched one, so the
   prior bytes are equally gone either way; what changes is that the new body is
   not kept either. If SEC served different content, a wrong URL, or a non-empty
   error page, the old evidence is gone **without** an integrity failure —
   because the UPSERT writes the new hash too.
2. **Re-parse does not check the recorded hash** (ckpt-1 #9). Only
   `rehydrate_raw_document` compares against `payload_sha256`; `_parse_pre14a`
   fetches and stores. So the integrity guard protects recovery, not ingest.
   Pre-existing for every swept kind; recorded here, not fixed here.
3. **Failed and tombstoned filings are compacted too** (ckpt-1 #4). `store_raw`
   runs before extraction, so a body that later tombstones is already
   born-compacted. Given 272 such rows today, that is the majority case.
4. **Drainage of the historical cohort is not gated by the sweep** (ckpt-1 #13).
   A broad `sec_rebuild` over `sec_pre14a` would re-fetch and born-compact all
   557 quickly. The sweep's source list gates the *sweep*; it does not gate a
   rebuild. This is not additional data loss — a rebuild re-fetches every body it
   replaces — but "operator-gated" in §7 means the sweep, and only the sweep.

## 4. Change B — the class gets a measured bar

`KEPT_NEGLIGIBLE_DOCUMENT_KINDS` is CI-enforced for *membership* only
(`test_every_document_kind_is_classified`). Nothing re-measured the adjective,
which is how a kind reached 3.8 GB with CI green throughout. That defect class —
not just its one instance — is what this closes.

```python
KEPT_NEGLIGIBLE_MAX_PAYLOAD_BYTES: int = 1024**3  # 1 GiB
```

Anchors, since no formulation exists to cite: every kind legitimately in the
class is ≤ 0.144 GB (~7× headroom); the largest backlog the repo calls
acceptable-to-leave is the sweep's *"~274 MB eligible"* (~4×); it is ~1.7% of
`DB_SIZE_WARN_BYTES`, i.e. under the resolution at which the DB alarm could
notice one kind growing. It had to fire on 3.803 GB — a requirement the
construction met, not evidence for the value.

⚠ **CORRECTED (ckpt-1 #20, #21).** The draft claimed the anchors showed the bar
"cannot fire on a correct classification". A dev snapshot cannot establish that,
and a per-kind bar does not bound the class: it misses aggregate growth across
kinds and fast growth below 1 GiB. Stated honestly in the code comment — the bar
is a **drift detector**. It can show a classification has stopped being true; it
cannot show one is safe.

### Where it is checked, and what that is worth

`scripts/dq_audit.py` — the board-feeder, whose own docstring says it *"surfaces
candidates, it does not assert bugs"*. The check prints a per-kind census tagged
with retention class and distinguishes three findings that are **not** the same:

| finding | meaning |
| --- | --- |
| kept kind over the bar | a misclassification — the kind needs a verdict |
| **swept kind holding live bytes** | an uncollected #1014 sweep backlog |
| unclassified kind | a kind the corpus holds that no bucket claims |

⚠ **ADDED (ckpt-1 #12).** The draft flagged only the kept class, so the moment
`pre14a_body` was reclassified its 3.803 GB would have vanished from reporting —
the change would have hidden its own subject. The swept-with-live-bytes line
closes that escape, and it immediately surfaced something unrelated: **`primary_doc`
holds 2.817 GB of live payload** (2.458 GB `sec_10k` + 0.274 GB `sec_8k`), both
already on the approved drop-list. That is an uncollected sweep backlog, not a
misclassification — born-compaction binds new writes and the retroactive sweep
is manual. Reported as a measurement, not an alarm: a permanently-red check is
the #1221 failure mode this repo already refuses.

⚠ **CORRECTED (ckpt-1 #19).** No claim is made that the detector runs. `dq_audit`
is dev-only, prints, and returns 0 even on a caught query error. It is a
candidate surfacer on a manual/loop cadence. The durable operator-facing home is
#2774's own unticked acceptance box (*"`/system/postgres-health` exposes
raw-payload size/growth"*), which needs an API-shape change and a frontend cell —
a separate slice, recorded on the ticket rather than tail-ended here.

⚠ Deliberately **not** a `db`-marked test: the db tier is on the pre-push gate
(`.githooks/pre-push:335`), and a push gate keyed on corpus growth fails diffs
that did not cause it.

⚠ **ADDED (ckpt-1 #22).** Census semantics are pinned: it groups **all** rows of
the table (including accessions with no manifest row), reports live and compacted
rows separately, `COALESCE`s the byte sum so an all-compacted kind reads 0 rather
than NULL, and has an explicit `unclassified` arm — the code-side partition test
enumerates the `Literal` and so cannot see a kind the corpus still holds.

## 5. Tests

Pure: `pre14a_body` is swept and not kept; `retention_class_for` across all four
arms including `unclassified`; `kept_negligible_breaches_bar` table-tested below,
**at** (strict `>`, so equality does not flag) and above the bar, plus a swept
kind at 3.8 GB and a re-read kind at 32 GB both returning False; `store_raw`
refuses `pre14a_body` with no `source_url`.

DB tier: the happy path through `run_manifest_worker` asserts born-compaction —
`payload IS NULL`, `byte_count IS NULL`, `payload_swept_at` set, `source_url`
present, and `payload_sha256` **equal to the sha256 of the body the parser
fetched**. ⚠ That equality is the assertion that matters: a row carrying some
other hash satisfies `chk_swept_rows_carry_hash` and is still unrecoverable.

⚠ **CORRECTED (ckpt-1 #24).** The draft proposed proving the switch with a
rolled-back `store_raw` call, which exercises one function rather than the
ingest path. The DB-tier test above drives fetch → store → parse → typed write →
manifest transition through the real worker instead.

Revert-probes, each against a green control (13 tests, exit 0): removing
`pre14a_body` from `SWEPT_DOCUMENT_KINDS` fails 5 tests including the worker
born-compaction assertion; `>` → `>=` fails the at-the-bar case; collapsing the
`unclassified` arm into `kept-and-negligible` fails the classifier test.

## 6. Dev verification

1. **Full-population `source_url` structural validation — 557/557, 0 problems.**
   ⚠ **ADDED (ckpt-1 #8).** `store_raw` only checks the URL is truthy, so
   whitespace, a wrong host, or a URL pointing at another accession would pass
   ingest and fail recovery. Checked offline (scheme, host, accession in path),
   so it costs no SEC budget and covers everything rather than a sample.
2. **Live byte-identity round-trip against EDGAR — 8/8 match**, stratified
   0.22 MB → 17.70 MB, comparing a fresh `fetch_document_text` against the
   server-side hash of the stored payload.
   ⚠ **Reported as a SAMPLE** (ckpt-1 #7). It establishes the stored-hash /
   re-fetch equality on real filings of this kind. It does not establish
   population-wide recoverability, and no check establishes **future** EDGAR
   availability (ckpt-1 #6): a hash detects that a document changed; it cannot
   restore one that was removed.
3. Per-kind class census via `dq_audit` before and after.

## 7. Operator-gated follow-up (presented, not executed)

Reclaiming the existing bytes means adding `sec_pre14a` to
`SWEPT_MANIFEST_SOURCES` and running the sweep with `dry_run=False`. Presented
for explicit approval per #2774 (*"destructive execution requires explicit
operator action"*), with the honest scope:

- **285 rows / 1.457 GB**, not the headline 3.803 GB.
  ⚠ **CORRECTED (ckpt-1 #3).** That figure is now the sweep's **full predicate**
  (`payload IS NOT NULL` ∧ `source = 'sec_pre14a'` ∧ `ingest_status = 'parsed'` ∧
  `raw_status IN ('stored','compacted')`), not a manifest-state count. It
  coincides with the state count today; that is a measurement, not a guarantee,
  and it moves with concurrent ingestion.
- The 272 tombstoned rows / **2.346 GB are out of reach** of the current
  mechanism and need a separate decision: is a body that produced no typed
  output evidence worth keeping, or the clearest waste in the table?
- ⚠ **ADDED (ckpt-1 #2).** Approval is **not** scoped to this cohort. Adding the
  source enables every swept kind attached to it, and invoking the service sweeps
  all eligible rows — including the `sec_10k`/`sec_8k` `primary_doc` backlog
  §4 surfaced. That backlog is separately worth the operator's attention: it is
  ~2.7 GB on sources already approved for sweeping, awaiting a manual run.
- ⚠ **ADDED (ckpt-1 #26).** Nulling a payload does not return disk. The UPDATE
  leaves dead TOAST tuples; the space becomes **reusable** after vacuum once the
  snapshot horizon allows, and returning it to the OS needs a rewrite —
  which #3117 bars as a default fix. Report logical reclaim and physical reclaim
  as different numbers.

## 8. Inherited, flagged, not fixed

⚠ ckpt-1 #16/#17 attack the **#1892 parser**, not this change: it treats a
numbered opening Notice-of-Meeting list as the proposal set, where Rule 14a-4(a)(3)
speaks to identifying separate matters on the proxy form, and its extraction was
validated against ten historical filings plus six fixtures rather than today's
557-row population. That is a live question given 272 tombstones — and it is
orthogonal to retention, because the re-parse path re-fetches either way. Raised
on #2774 as a step-1 extraction gap; deliberately not opened as a new ticket and
not fixed here.
