# #3109 — per-CIK poll: batch the due sources of one CIK into ONE fetch

Status: proposal (implementation slice). Scope is the **batching** piece only —
recommendation 1 of the withdrawn slice 1+2 spec ("take the batching as a separate,
earlier piece of work from the selector; it is the lever and its blockers are all
independent of ordering").

**NOT in scope:** the queue-rotation fix (the ticket's headline starvation defect), the
re-poll interval, the recheck/poll budget split. Those are the selector slices.

> **Rewritten after Codex checkpoint 1 returned 44 findings.** Corrections that changed the
> design (not just the prose) are marked ⛔ inline, because the wrong version was in several
> cases the intuitive one.

## 1. Source rule

`https://data.sec.gov/submissions/CIK{padded}.json` is an **entity-wide** response.
`.claude/skills/data-sources/sec-edgar.md` §1 "JSON APIs": *"Per-CIK 1000-most-recent
filings + history pointers"*, carried as the columnar `filings.recent` block whose `form`
array spans every form type the entity filed. There is no per-form or per-source request
variant, and no query parameter narrows it.

⛔ **"Entity-scoped", not "issuer-scoped"** (Codex 22). An earlier draft leaned on §3.6's
fan-out table to call the response issuer-scoped. That is wrong as a general claim — a
reporting person's feed carries every Section-16 form that PERSON filed against ANY issuer
and has no issuer field at all (prevention log, #2788, 2026-08-20). Batching does not need
the stronger claim: it needs only **one CIK ⇒ one response**, which holds for issuers,
reporting persons, institutional managers and fund trusts alike.

So the source's own structure is what makes the current shape redundant: `_probe_subject`
issues one full fetch of that response per due `(subject, source)` triple and then discards
every row whose `map_form_to_source(form)` is not the one source it was called for
(`sec_per_cik_poll.py:158`, `sec_submissions.py:273-275`). N due sources on one CIK cost N
identical responses.

Two further source rules bound the change:

- **§7.4 `recent` window** — SEC serves **at least one year OR 1,000 filings, whichever
  yields more**; older history lives in `filings.files[]`. ⛔ 1,000 is a floor on what is
  served, not a hard ceiling on array length (Codex 28). This job deliberately does not
  follow `files[]` (`run_per_cik_poll` docstring). Batching does not change that: a
  rarely-filed source on a high-volume filer can already fall off `recent`, identically,
  before and after.
- **The URL is built from the ZERO-PADDED CIK** (`_zero_pad_cik`, `sec_submissions.py:140`).
  Two freshness rows whose `cik` differs only by padding therefore address the same
  response, so the batch key must be the padded form, not the stored string (Codex 8).

**Conditional GET:** #3110 measured, live, that `data.sec.gov` returned **no `Last-Modified`
and no `ETag`** on the `submissions/CIK*.json` responses it probed, and 0 rows exist under
`sec.last_modified.*` across 1,949 runs — so the Item-7 (#1233) conditional path is inert in
production. ⛔ That is a probe plus an absence of persisted validators, **not** a
full-population header census (Codex 39); the honest form is "no validator has ever been
stored by this path", which is what the row count actually shows. The skill's §4 table still
says *"`If-Modified-Since` honoured per-CIK"*; that line is wrong and its replacement text is
parked on #3110 (`.claude/**` is write-refused from a loop worktree, #2403). This spec must
still leave the path **correct**, because correctness does not depend on whether it fires.

## 2. Measurements — full population, dev DB, read-only, 2026-09-16 21:5xZ

`scripts/measure_3109_batching.py` reproduces every figure below. ⚠ Each query runs in its
own READ COMMITTED statement against a live DB, so figures are a **snapshot**, not an
invariant (Codex 37).

### M5 — state census (bounds every other query)

`current` 42,174 · `unknown` 13,600 · **nothing else**. No row is `never_filed`, `error` or
`expected_filing_overdue`.

### M0 — CIK shape, full population

```sql
select count(*), count(*) filter (where cik is null),
       count(*) filter (where cik is not null and cik !~ '^[0-9]{1,10}$')
from data_freshness_index;
```

→ **55,774 rows / 0 NULL / 2 non-numeric.**

⛔⛔ **A live defect, and it is the reason the existing guard is dead code.**
`sec_per_cik_poll.py:397` skips a subject when `subject.cik is None`, commented *"FINRA
universe singleton — no submissions.json poll"*. The FINRA singletons do **not** carry a
NULL cik — they carry the literal strings `FINRA_REGSHO` and `FINRA_SI`
(`subject_type='finra_universe'`, sources `finra_regsho_daily` / `finra_short_interest`,
both `state='current'`). The guard has therefore never protected the case it was written
for. If either row becomes due, the job fetches
`https://data.sec.gov/submissions/CIKFINRA_SI.json` (`str.zfill` leaves a 12-character
string unchanged), takes the 404 branch, returns an empty delta, and writes
`outcome='current'` — certifying a FINRA subject from a 404 on a SEC endpoint that does not
serve it.

Neither row is due at the time of measurement (see M6), so this is dormant, not firing.

### M1 — what the selector ACTUALLY returns today

```sql
with p as (select cik from data_freshness_index
 where state in ('unknown','current','expected_filing_overdue')
   and (expected_next_at is null or expected_next_at <= now())
 order by expected_next_at asc nulls first limit 66)
select count(*), count(distinct cik) from p;
```

→ **66 rows / 48 distinct CIKs — in-prefix fan-out 1.375×.**

⛔⛔ **This falsifies the inherited "3.60× / 3.67× fan-out" figure as applied to this job.**
Those are population averages over all candidate triples (M6 re-confirms 3.60×). The
selector takes an ordered *prefix*, and the prefix is far flatter. A population mean does
not describe an ordered prefix, and the redundancy this change removes must be sized on the
prefix.

⚠ The prefix is not fully deterministic: `ORDER BY expected_next_at` has no tie-break, and
196 distinct timestamps span the first 200 due rows, so ties exist at the boundary
(Codex 35). 48 is a snapshot, not a job invariant. The proposed selector fixes this for the
new path by ranking on `(rank key, padded cik)`.

### M2 — what a CIK-denominated budget of the same size reaches

Ranked by `min(coalesce(expected_next_at,'-infinity'))`, tie-broken on the padded CIK — the
**same** expression the proposed selector uses (Codex 36):

→ **66 CIKs / 127 triples / max 8 per CIK / fan-out 1.924×.**

So the realisable claim is precise and modest: **the same 66 logical probes carry 127 triples
instead of 66 — 1.92×** — and they reach 66 distinct CIKs instead of 48.

⛔ "Same request count" is a claim about **logical probes**, not wire requests (Codex 42):
`ResilientClient` retries 429/5xx, so wire count is ≥ probe count both before and after.

### M6 — the due population, and source eligibility

→ candidate **55,774 triples / 15,485 CIKs**; due **27,161 triples / 7,399 CIKs**
(population fan-out 3.60× / 3.67×).

Due-source census: `sec_form3` 5,195 · `sec_10k` 3,890 · `sec_8k` 3,280 · `sec_13g` 2,885 ·
`sec_form4` 2,855 · `sec_13d` 2,711 · `sec_424b` 2,119 · `sec_form5` 1,481 · `sec_13f_hr`
1,193 · `sec_nt` 830 · `sec_10q` 479 · `sec_tender` 122 · `sec_def14a` 121.

⚠ Codex 34 raised that a source with no form mapping (`sec_xbrl_facts`) or a FINRA source
could be marked `current` off a submissions poll that cannot see it. **Measured: neither
appears in the due set**, and the M0 filter below removes the FINRA rows by construction.
`sec_xbrl_facts` remains a latent instance of the same shape and is named in §4.

**Full-replay bound:** 2,984 of the 27,161 due rows carry a NULL `last_known_filing_id`, so
a poll replays their whole filtered `recent` array. Inside the 66-CIK selection that is
**19 of 127 triples** (Codex 20).

### M1b / M2b — the recheck lane is empty at this snapshot

`subjects_due_for_recheck` prefix (34): **0 rows**, and M5 shows 0 rows in `never_filed` /
`error` at all — so the recheck reader cannot currently return anything, and 34 of the 100
budget slots go unused.

⛔ **Not "always idle"** (Codex 40). A single fetch error writes `outcome='error'` and
populates that lane immediately. This is a snapshot of an empty backlog, not a structural
property.

⚠ Deliberately **not fixed here.** Rolling the unused share into the poll lane raises the
probe count (66 → 100) to re-poll a queue head that does not rotate; it is worth doing
*after* the rotation fix, and it changes which subjects are selected — the selector slice
this one is separated from.

### M3 — duplicate `(cik, source)` rows: the manifest-ownership surface

→ **963 colliding pairs / 1,939 rows involved / worst 3.**

⛔ This counts collisions; it does **not** classify them (Codex 23). No identity join or
subject-type breakdown was run, so "share-class siblings" is a hypothesis about these rows,
not a measured fact.

`record_manifest_entry`'s `ON CONFLICT (accession_number) DO UPDATE SET subject_type =
EXCLUDED.subject_type, subject_id = EXCLUDED.subject_id, instrument_id =
EXCLUDED.instrument_id` (`sec_manifest.py:315-322`) makes the last writer own the row.

⛔ **The winner CAN change** (Codex 24 — an earlier draft said "the winner is the same one"
and then immediately said the identity can differ, which is a contradiction). Ordering moves
from `expected_next_at` to `(cik_rank, source, subject_type, subject_id)`, and CIK-first
selection can pull in a sibling that the triple-first prefix would not have reached. So for
these 1,939 rows the persisted `subject_id` / `instrument_id` may differ from what today's
ordering would have written. ⚠ `ingest_status` is preserved by the conflict clause, so an
already-`parsed` row keeps its status under a new owner and is not re-derived (Codex 25).

Not fixed: choosing an owner is a data-treatment decision on the fan-out rule, not a
batching question. ⚠ It is also under-counted — the manifest key is the accession alone, so
cross-CIK accessions (dual-party tender filings, `sec_manifest.py:1092-1096`) are a second
collision surface this `(cik, source)` grouping cannot see (Codex 26).

### M4 — runtime headroom

→ **47 runs / 48 h, avg 16.37 s, max 22.16 s, 47 success, 0 failed.**

⛔ This bounds the **old** path, not the new one (Codex 41), and `status='success'` permits a
non-zero `poll_errors`. Fetch count is unchanged, so the added cost is the parse/write tail
on 127 triples instead of 66 — roughly the 1.92× multiplier on the non-HTTP half of a 16 s
job. `_DEFAULT_JOB_STATEMENT_TIMEOUT_MS` is 30 min and bounds a single **statement**, not
the job; this body is many short statements.

⚠ Codex 21 is right that the real risk is not the timeout but **lock retention**: the job
runs on `connect_job(autocommit=False)` and commits once at the end, so ~1.9× the rows are
held under row locks across the HTTP calls, against `sec_manifest_worker` writing the same
table from its own lane (#1478). Accepted rather than solved — the transaction posture is
pre-existing and changing it is the selector slice's blocker (nested `conn.transaction()`
blocks are savepoints, not commits).

**SEC rate budget:** unchanged, and in any case not established by 66/3,600 (Codex 43).
Rate safety comes from the cross-process `sec_rate_gate` GCRA limiter (#1484), which every
SEC consumer reserves against; this change does not add a consumer or alter its lane.

## 3. Design

### 3.1 The seam

`check_freshness` / `check_freshness_conditional` do four things: **fetch → parse →
source-filter → watermark-truncate**. The first two are per-CIK; the last two are per-triple.

Extract the per-triple tail into ONE shared pure function in
`app/providers/implementations/sec_submissions.py`:

```python
def select_new_filings(
    rows: Sequence[FilingIndexRow],
    *,
    sources: Iterable[ManifestSource] | None,
    last_known_filing_id: str | None,
) -> tuple[list[FilingIndexRow], datetime | None]:
    """Source-filter + watermark-truncate. Returns (new_filings, last_filed_at)."""
```

Both `check_freshness` and `check_freshness_conditional` call it, and so does the batched
job. **One expression of the rule** — the failure mode being avoided is a second copy in the
job drifting from the provider's copy, silently (the #3110 review nitpick).

`last_filed_at` is `max(filed_at)` over the **source-filtered** rows, pre-truncation, which
is what both functions compute today — preserved exactly.

⚠ The truncation semantics are carried over UNCHANGED and their known limits are not
addressed here: stop-at-accession assumes the array is newest-first and that every
undiscovered row precedes the watermark (Codex 30); metadata corrections to an already-known
accession and SEC-side deletions are invisible to prefix truncation (Codex 31); and
`last_known` takes `new_filings[0]` while `last_filed_at` is an independent maximum, so
equal-timestamp filings can leave the freshness UPSERT's strictly-newer guard unmoved
(Codex 32). All pre-existing, all unchanged by batching.

### 3.2 The probe

`_probe_subject(conn, subject)` → `_probe_cik(conn, subjects)` where every element of
`subjects` shares one **padded** cik:

1. One fetch with `sources=None, last_known_filing_id=None` — the unfiltered, untruncated
   parse of the whole response.
2. Per subject: `select_new_filings(rows, sources={subject.source},
   last_known_filing_id=subject.last_known_filing_id)`, then the existing manifest-UPSERT
   loop, then the existing `record_poll_outcome` call, unchanged.

⛔ **A fetch error now fails EVERY subject on that CIK, and that is a real regression in
failure granularity** (Codex 17). Today three subjects on one CIK get three independent
attempts and a transient failure kills one; after, it kills three, and all three land in the
`error` state together. Accepted, not equivalent: `ResilientClient` already retries 429/5xx
beneath the probe, and the alternative (re-fetching per subject on failure) reinstates
exactly the redundancy this change removes.

**Write-failure boundary** (Codex 19): each subject's writes stay inside their own
`with conn.transaction():` savepoint, as today. An exception escaping that block propagates
out of `run_per_cik_poll` and ends the run — identical to today, where subject A's DB
exception also aborts before B and C are probed. No new catch is introduced; doing so would
change failure semantics under cover of a batching change.

**Counters** keep their present meaning: the lanes stay separate (§3.4), so `subjects_polled`
still counts poll-lane subjects, `recheck_subjects_polled` recheck-lane subjects, and
`poll_errors` both (Codex 16).

### 3.3 Budget unit

`run_per_cik_poll(max_subjects=100)` → `run_per_cik_poll(max_ciks=100)`. The parameter is
renamed, not silently redefined: the scheduler does not pass it (`scheduler.py:9477` passes
only `conn` + `http_get_with_meta`) and the manual-dispatch invoker does not read it, so the
only callers are tests. Split stays 2/3 poll + 1/3 recheck, now in CIKs.

### 3.4 Selector

New `ciks_due_for_poll` / `ciks_due_for_recheck` in `app/services/data_freshness.py`,
returning **all due triples of the top-N CIKs**, grouped and contiguous:

```sql
WITH due AS (
  SELECT <cols>, lpad(cik, 10, '0') AS cik_padded,
         MIN(COALESCE(<deadline>, '-infinity'::timestamptz))
           OVER (PARTITION BY lpad(cik, 10, '0')) AS cik_rank_key
  FROM data_freshness_index
  WHERE state IN (<lane states>)
    AND (<deadline> IS NULL OR <deadline> <= %(now)s)
    AND cik ~ '^[0-9]{1,10}$'
    [AND source = %(source)s]
), ranked AS (
  SELECT *, DENSE_RANK() OVER (ORDER BY cik_rank_key, cik_padded) AS cik_rank FROM due
)
SELECT <cols> FROM ranked WHERE cik_rank <= %(limit)s
ORDER BY cik_rank, source, subject_type, subject_id
```

`<deadline>` is `expected_next_at` for the poll lane and `next_recheck_at` for the recheck
lane; `<lane states>` is `('unknown','current','expected_filing_overdue')` and
`('never_filed','error')` respectively (Codex 6).

- ⛔ **`COALESCE(..., '-infinity')` inside the `MIN`, not `NULLS FIRST` outside it**
  (Codex 1). `MIN` ignores NULLs, so a CIK with deadlines `{NULL, yesterday}` would rank on
  `yesterday` and fall behind an all-NULL CIK — inverting the existing NULL-is-most-urgent
  semantics. Folding the NULL into the key makes the group rank match the row rank. (0 NULL
  deadlines exist today; this is correctness by construction, not a live fix.)
- **`cik` inside the `DENSE_RANK` ordering**, so the rank is one per CIK and the tie-break is
  **deterministic** — the ticket's explicit requirement. Ranking on the deadline alone would
  collapse every CIK sharing a timestamp into one rank and blow the budget.
- ⛔ **`cik ~ '^[0-9]{1,10}$'` replaces the dead `cik IS NULL` guard** (M0). This is a
  NARROWING gate, so what it rejects is enumerated rather than described: exactly the two
  rows `('FINRA_REGSHO', 'finra_universe', 'finra_regsho_daily')` and
  `('FINRA_SI', 'finra_universe', 'finra_short_interest')`. The same predicate is what
  `_zero_pad_cik` + the SEC URL require, and what `_MANIFEST_CIK_RE` enforces downstream.
- ⛔ **`source =` filter retained and applied before ranking** (Codex 5) — omitting it would
  make a scoped call process unintended sources and underfill its budget.
- **Group on `lpad(cik,10,'0')`**, the expression the URL is built from, so padding variants
  cannot split one entity into two batches (Codex 8). Measured 0 such variants today; the
  `{1,10}` bound in the filter guarantees `lpad` cannot truncate.

⚠ **Lane overlap is NOT merged** (Codex 3/4). A CIK with both poll-lane and recheck-lane
subjects is selected independently by each and costs **two** fetches — exactly as today,
where they are two separate probes. Merging the lanes would change the budget contract that
#1155 G13's 2/3-1/3 split exists to enforce, and the recheck lane is empty (M1b), so the
saving is currently zero. Named so it is not inherited as an unknown.

**Selection timing** (Codex 7): both lanes are materialised into lists **before** any write,
as `run_per_cik_poll` does today (`sec_per_cik_poll.py:391-394`). Selecting rechecks after
polling would immediately re-select rows the poll lane had just failed, whose
`next_recheck_at` is NULL and therefore instantly due.

**Query plan**, `EXPLAIN (ANALYZE, BUFFERS)` on the dev corpus: seq scan (27,160 rows) →
sort → WindowAgg → sort → WindowAgg with `Run Condition: dense_rank() <= 66` pushed into the
node, **64.1 ms**, 7,074 shared buffer hits, no temp files. Negligible against a 16 s job.

### 3.5 Watermarks

Keys stay per-source (`<cik>:<source>`), unchanged.

⛔⛔ **The unanimity scheme in the first draft was unsound, and so is the invariant it
claimed to preserve** (Codex 9/10). #3110's docstring says a validator stored under
`<cik>:<source>` means *that source was processed at that validator*. With **963 duplicate
`(cik, source)` pairs** (M3), two subjects with different accession watermarks already share
one key: subject A can store the validator after subject B, sharing the key, failed its
manifest writes — and B then receives a 304 and loses its retry. That hole exists **today**,
independent of batching, and no batch-level unanimity check can close it.

So the design does the one thing that provably adds no exposure:

- **`If-Modified-Since` is sent only when the batch contains exactly one subject.** That case
  is byte-identical to today's per-subject probe. Multi-subject batches always fetch
  unconditionally.
- ⛔ **A `not_modified` delta when no `If-Modified-Since` was sent is treated as an error,
  not a success** (Codex 14). `check_freshness_conditional` takes the 304 branch on status
  alone, so an unsolicited 304 would otherwise mark every batched subject `current` without
  any payload being examined.
- ⛔ **The validator is persisted only on a 200** (Codex 13). `check_freshness_conditional`
  returns the 404 branch's `last_modified` too, and `recorded == len(new_filings)` is
  trivially true at `0 == 0`, so today a 404 can persist a validator. The batched path gates
  on an explicit 200.
- On 200, each subject's validator write stays gated on **that subject's own**
  `recorded == len(new_filings)` — the existing rule, per subject, not a new one (Codex 15
  correctly objects to calling this "finer-grained than today"; today's gate is already per
  subject).

⛔ **Docstring correction, in-scope because this change edits those lines** (Codex 12): the
module says a 304 "re-stamps `watermark_at`". It does not. `set_watermark(..., watermark_at=
None)` writes `watermark_at = EXCLUDED.watermark_at` — i.e. **NULL** — and `fetched_at =
NOW()` (`watermarks.py:186-199`). Four docstring sites in `sec_per_cik_poll.py` assert the
false version; all are corrected to name `fetched_at`.

## 4. Explicitly NOT fixed (named so they are not inherited as unknowns)

1. **Rotation / starvation** — the ticket's headline. `record_poll_outcome` derives
   `expected_next_at` from `last_known_filed_at + cadence`, which for an inactive filer stays
   in the past forever, so the queue head does not advance. ⛔ This change **does** alter
   which CIKs are reached (48 → 66 at the same budget, plus the M0 exclusions) — an earlier
   draft claimed otherwise and contradicted its own §2 (Codex 2). What it does not change is
   the *rotation* behaviour: the head set is reached more completely, not replaced.
2. **The swallowed `ValueError`** — `sec_per_cik_poll.py:273` catches a rejected
   `record_manifest_entry`, logs, and `last_known` still advances to the **newest** accession
   (`:296`), so a rejected middle accession is truncated away by the next poll and never
   retried. The `all_recorded` gate protects only the HTTP watermark, which is inert. Real,
   pre-existing, and **not** a one-liner: gating the freshness watermark too would wedge the
   row forever on a permanently-rejectable accession.
3. **Manifest ownership on collision** — §2 M3, both the `(cik, source)` surface and the
   wider accession-only one.
4. **The shared-key watermark hole** — §3.5. Pre-existing; #3110's docstring overstates its
   invariant and should be corrected on that ticket.
5. **`sec_rebuild.py` resets accession watermarks without clearing HTTP validators**
   (Codex 11), so a reset subject could be certified current by a stale validator. Not live
   while no validator is ever stored, but it survives this change.
6. **404 / malformed-payload paths classify as `current`** (Codex 33) — an empty delta is
   indistinguishable from a checked-and-unchanged one. Batching spreads that classification
   across every subject of the CIK instead of one.
7. **`sec_xbrl_facts` has no form mapping** (Codex 34) — a submissions poll cannot discover
   it, so a `current` outcome on such a row means nothing. Measured absent from the due set
   today.
8. **The idle 34% recheck budget** — §2 M1b.

## 5. Acceptance

Tests use a **frozen fake payload** (Codex 18): a live response mutates during the day, so
"identical deltas before and after" is only assertable against identical bytes.

- A CIK with several due sources produces **one** fetch and **N** scheduler outcomes; each
  subject's `new_filings`, `last_filed_at` and `last_known` match the unbatched path for the
  same bytes.
- Source filter and watermark truncation are applied **per subject**, not batch-wide.
- A fetch failure writes `outcome='error'` for **every** subject of that CIK.
- The FINRA singleton rows are never selected; the rejection is asserted by row identity.
- A CIK whose due rows carry different deadlines ranks on the **minimum**, and a NULL
  deadline in the group ranks it first.
- `source=` scoping selects only that source and still fills its CIK budget.
- `If-Modified-Since` is sent for a single-subject batch and withheld for a multi-subject
  one; an unsolicited 304 raises rather than certifying.
- A 404 does not persist a validator.
- `max_ciks` 0 and 1 stay bounded (the degenerate `poll=0, recheck=1` case is preserved).
- Provider behaviour — `files_pages`, `has_more_in_files`, the 404 branch — is unchanged by
  the `select_new_filings` extraction.
- `tests/test_g13_recheck_reader_invariants.py` is updated to the new reader names; its
  invariant (both lanes read, symmetric scope exclusion) is preserved, not dropped.
