# `pre14a_body` retention verdict — compact at birth, bound the class

> Issue: **#2774** (child of the #3117 storage umbrella, priority 1).
> Status: spec for the change on `fix/2774-pre14a-retention-verdict`.
> Prior art: #1014 (sweep + rehydrate), #1615 (born-compaction), #1617 (the
> three-class partition), `docs/specs/etl/retention-rubric.md` §3.3.

## 1. Source rule

`docs/settled-decisions.md` → "Raw-payload retention (#1617, settled 2026-06-13)":
a stored `filing_raw_documents.payload` is legitimate only under exactly one of

- **re-read** — a rewash parser reads the stored body (`rewash_filings.registered_specs()`);
- **housekept-and-negligible** — born-compacted at source, rehydratable from
  `source_url` (`raw_filings.SWEPT_DOCUMENT_KINDS`);
- **kept-and-negligible** — *"small, write-only, no payload reader"*
  (`raw_filings.KEPT_NEGLIGIBLE_DOCUMENT_KINDS`, kind → justification).

`docs/specs/etl/retention-rubric.md` §3.3 fixes the shape of any retention change:
*"this is an ingest-side discipline, not a row-deletion rule. Caps gate what new
ingests write. Existing rows are NEVER deleted on a per-source basis."*

⚠ **No published or in-repo formulation of "small" / "negligible" exists** for this
class. Grepped: the rubric caps typed ROWS per source and never bytes; #1617 gives
the adjective and no number; `postgres_health` carries DB-level alarms only. The bar
is therefore fixed **by construction** in §4 and frozen, per the "source-rule before
design" rule's own instruction for the no-published-formulation case.

## 2. Premise, measured on the full population

Dev DB, 2026-09-17, `filing_raw_documents` (27 GB physical). `byte_count` is a
generated column (`octet_length(payload)`, `sql/107:68`) — logical bytes, not disk.

| kind | rows | logical | class |
| --- | --- | --- | --- |
| `def14a_body` | 43,180 | 32.564 GB | re-read |
| `infotable_13f` | 75,527 | 14.634 GB | re-read |
| **`pre14a_body`** | **557** | **3.803 GB** | **kept-and-negligible** |
| `form4_xml` | 495,222 | 3.405 GB | re-read |
| `primary_doc` | 184,265 | 2.817 GB (38,410 unswept) | swept |
| `form3_xml` | 82,103 | 1.069 GB | re-read |
| `primary_doc_13dg` | 48,193 | 0.444 GB | re-read |
| `finra_regsho_daily_txt` | 558 | 0.144 GB | kept |
| `nt_body` | 4,329 | 0.130 GB | kept |
| `finra_short_interest_csv` | 36 | 0.074 GB | kept |
| `nport_xml` | 166 | 0.013 GB | kept |
| `form5_xml` | 1,614 | 0.011 GB | kept → re-read (#1731) |
| `prospectus_body` / `tender_body` | 51,514 | 0.000 GB | swept |

`pre14a_body` detail: **mean 6.83 MB, median 2.40 MB, max 313.6 MB**, stored datum
size (`pg_column_size`) 3.407 GB — proxies barely compress. Against the enum comment
*"Small HTML; retained, not swept"* (`raw_filings.py:79`) and the justification
*"write-only … (reuse deferred by volume #1892)"* (`:151`). Both are falsified by
measurement: it is the **largest per-row kind in the table** and the third-largest
total. Growth, steady state after the July backfill: 48 rows / 0.108 GB in August,
44 rows / 0.347 GB in September-to-date.

Split by manifest state — 285 `parsed` / 1.457 GB, **272 `tombstoned` / 2.346 GB**,
every tombstone carrying the same error, `no recognizable numbered proposals list`.
So 62% of the bytes back filings that produced no typed output. (Measured, not
diagnosed — whether that is a parser gap or genuinely signal-free filings is not
decided here and is recorded as a #2774 step-1 gap candidate.)

Reader census, grep-verified at write time: the only `filing_raw_documents.payload`
readers in `app/` are `rewash_filings` (registry = `def14a_body`, `form3_xml`,
`form4_xml`, `form5_xml`, `infotable_13f`, `primary_doc_13dg` — no `pre14a_body`)
and the retention sweep itself. `_parse_pre14a` calls
`provider.fetch_document_text(url)` **unconditionally** on every parse and re-parse
(`manifest_parsers/sec_pre14a.py:106`). All 557 rows carry an `http` `source_url`.

**Verdict: `pre14a_body` is write-only and not small ⇒ under #1617's own criterion it
belongs in the swept class, not the kept class.** This applies the settled decision;
it does not reverse one.

## 3. Change A — reclassify

Move `"pre14a_body"` from `KEPT_NEGLIGIBLE_DOCUMENT_KINDS` into
`SWEPT_DOCUMENT_KINDS` (`app/services/raw_filings.py`). That is the whole behavioural
switch:

- `store_raw` born-compacts swept kinds (`:260-297`) — hash server-side, write
  `payload NULL`, `payload_swept_at = NOW()`, require `source_url`. `sec_pre14a`
  already passes `source_url=url`, so no parser change.
- `raw_status` needs no change: `sql/195` already redefines `'stored'` as *"a raw
  row exists — NOT whether the payload bytes are present"* precisely for
  born-compaction, and the authoritative bytes-present predicate is
  `payload_swept_at IS NULL`.
- The partition tests (`tests/test_raw_payload_retention.py`) keep holding: the kind
  moves buckets, disjointness and total coverage are unchanged.

### What this does NOT do

- **No retroactive sweep.** `sweep_raw_payloads` is double-gated on
  `document_kind ∈ SWEPT_DOCUMENT_KINDS` **and** `m.source ∈ SWEPT_MANIFEST_SOURCES`
  = `{sec_10k, sec_8k}` (`raw_payload_retention.py:67,113`). `sec_pre14a` is in
  neither, so the 557 existing rows are unreachable by the sweep both before and
  after this change.
- **No receipt contract.** #2774 step 2's durable-receipt machinery exists for
  **re-read** kinds, whose bodies back a rewash. A write-only kind whose re-parse
  path already re-fetches has nothing to receipt beyond `payload_sha256` +
  `source_url`, which #1014/#1615 already store.

### ⚠ Consequence that must be stated, not discovered

After this change an existing `pre14a_body` row that is **re-parsed** (parser-version
bump, `sec_rebuild` scope, or a retry) is re-written born-compacted, because
`store_raw`'s swept branch is an UPSERT with `payload = NULL`. The historical 3.8 GB
therefore drains gradually rather than staying frozen. This is byte-loss without a
sweep, and it is acceptable for exactly one reason: the re-parse path **fetches the
document first**, so the only bytes discarded are bytes just re-downloaded, under a
recorded hash with a verified rehydrate path. Today's re-parse behaviour already
overwrites the payload (`:299-318`); the change is that the overwrite writes NULL.

## 4. Change B — the class gets a measured bar

`KEPT_NEGLIGIBLE_DOCUMENT_KINDS` is CI-enforced for *membership* only
(`test_every_document_kind_is_classified`). Nothing re-measures the adjective, which
is why a kind reached 3.8 GB under a "negligible volume" justification with CI green
throughout. That is the defect class this change closes, not just its one instance.

Add to `raw_filings.py`:

```python
KEPT_NEGLIGIBLE_MAX_PAYLOAD_BYTES: int = 1024**3  # 1 GiB
```

**Fixed by construction — the anchors, since no formulation exists to cite:**

- every kind legitimately in the class today is ≤ 0.144 GB, so the bar sits ~7×
  above the largest of them and cannot fire on a correct classification;
- the largest backlog the repo has explicitly called acceptable-to-leave is the
  sweep's *"only ~274 MB eligible"* (`postgres_health.py:71`) — the bar is ~4× that;
- it is ~1.7% of `DB_SIZE_WARN_BYTES` (60 GiB), i.e. below the resolution at which
  the DB-size alarm could ever notice a kind's growth;
- it must fire on the case that motivated it (3.803 GB) — stated as a requirement
  the construction had to meet, not as evidence for the value.

Changing the value is a code change with a recorded reason. The bar is a **drift
detector**, not a promotion rule: breaching it means the justification's premise no
longer holds and the kind needs a verdict, not that bytes get deleted.

### Where it is checked

`scripts/dq_audit.py` — the board-feeder, whose own docstring says it *"surfaces
candidates, it does not assert bugs"*, and which the autonomy loop already runs
periodically. The check prints the per-kind payload census with each kind's
retention class and flags any kept-and-negligible kind over the bar.

⚠ Deliberately **not** a `db`-marked test. The db tier is on the pre-push gate
(`.githooks/pre-push:335`), and a push gate keyed on corpus growth fails on diffs
that did not cause it — the random-gate failure mode this repo already refuses.

⚠ Not `postgres_health` either, in this slice. #2774's acceptance box *"`/system/postgres-health`
exposes raw-payload size/growth"* wants an operator-facing panel with a breach flag
and a frontend cell — a separate slice with its own API-shape change, recorded on
the ticket rather than tail-ended here.

## 5. Tests

Pure (fast tier):

1. `pre14a_body ∈ SWEPT_DOCUMENT_KINDS` and ∉ `KEPT_NEGLIGIBLE_DOCUMENT_KINDS`
   (the existing disjointness/coverage tests then bind it).
2. The kept-negligible flag predicate is a pure function of `(bytes, bar)` —
   table-test at, below and above the bar, including exact equality (`> bar`, so
   equality does not flag).
3. `store_raw(document_kind="pre14a_body", source_url=None)` raises — the
   born-compacted rehydration precondition.

DB tier:

4. `tests/test_manifest_parser_sec_pre14a.py` happy path asserts born-compaction —
   `payload IS NULL`, `payload_sha256` present and equal to the sha256 of the fetched
   body, `payload_swept_at` set, `source_url` present — replacing today's
   *"pre14a_body is RETAINED (not swept): payload present"* assertion (`:134`).
5. Rehydrate round-trip on a born-compacted `pre14a_body` row restores the exact
   bytes and a mismatched body raises `RawPayloadIntegrityError` (kind-agnostic code,
   so this pins the wiring, not the mechanism).

Revert-probes: each test above must fail when its production line is reverted.

## 6. Dev verification (read-only / rolled back)

1. **Cross-source hash round-trip against live EDGAR.** For two real accessions,
   sha256 the stored payload and compare with a fresh `fetch_document_text` of the
   row's `source_url`. This is the claim born-compaction rests on — that the bytes we
   stop storing are re-obtainable byte-identically.
2. **Born-compaction on the real path**, executed inside an explicit transaction that
   is rolled back: call `store_raw` for an existing accession and print the row state
   before and after, then abort. Proves the switch without mutating dev.
3. Re-run the per-kind census after merge; the figure to watch is that new
   `sec_pre14a` ingests add rows with `byte_count IS NULL`.

## 7. Operator-gated follow-up (presented, not executed)

Reclaiming the existing bytes requires adding `sec_pre14a` to `SWEPT_MANIFEST_SOURCES`
and running the sweep with `dry_run=False`. Presented for explicit approval per #2774
(*"destructive execution requires explicit operator action"*), with the honest scope:

- the sweep's predicate is `ingest_status = 'parsed'`, so it reaches **285 rows /
  1.457 GB**, not the headline 3.803 GB;
- the 272 tombstoned rows / 2.346 GB are **out of reach of the current mechanism**
  and need a separate decision about whether a tombstoned filing's body is evidence
  worth keeping at all;
- logical bytes are not disk bytes: reclaim lands as reusable free space in the TOAST
  relation, and returning it to the OS needs a rewrite (`VACUUM FULL` / rewrite),
  which #3117 bars as a default fix.
