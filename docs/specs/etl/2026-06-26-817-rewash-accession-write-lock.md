# #817 — Per-accession write lock for ownership rewash + live ingest

Status: spec (2026-06-26). Branch `feature/817-rewash-accession-write-lock`.
Closes #817. Refs #1542 (the 13F precedent this generalises).

## Problem

`rewash_filings.run_rewash` (manual CLI `scripts/rewash.py`) re-parses every
`filing_raw_documents` row below the current `parser_version` and re-applies
the typed-table write. The cohort scan (`_fetch_cohort`,
`rewash_filings.py:250`) is a plain `SELECT` with **no row claim**, and the
per-kind apply functions do **no per-accession locking** — except 13F, which
got `acquire_13f_accession_write_lock` in #1542.

So two writers of the **same accession's** typed rows can interleave:

- **rewash vs rewash** — two operators each run `scripts/rewash.py --kind X`.
- **rewash vs live** — an operator runs `scripts/rewash.py --kind X` while the
  `sec_manifest_worker` re-drains the same kind (the operator runbook tells
  operators to `POST /jobs/sec_rebuild/run` after a parser change, which
  requeues manifest rows for the live worker; `sec_rebuild` preserves
  `parser_version` — `sec_rebuild.py:131` — so the live drain and a manual
  rewash can both target the same accession).
- **live vs live** — #1274 parallelised filer ingest and #1591 added concurrent
  re-drain prefetch; a re-queue/retry overlap can put two live writers on one
  accession.

### Premise correction (full-population, dev DB)

The ticket claims "both upserts are idempotent (`ON CONFLICT DO UPDATE`), so no
corruption — just duplicate parsing work". **This is only true for two of the
five kinds.** Verified against the actual write code:

| Kind | Per-accession write mechanism | Concurrency hazard |
|---|---|---|
| `form4_xml` | `upsert_filing` — pure `ON CONFLICT DO UPDATE` (insider_filings/filers/footnotes/transactions) | benign (idempotent, deterministic by parser_version) — duplicate work only |
| `form5_xml` | `upsert_filing` (same as form4) | benign |
| `form3_xml` | `upsert_form_3_filing` — `ON CONFLICT` on `insider_filings`, but **DELETE+INSERT** on `insider_filers`, `insider_transaction_footnotes`, `insider_initial_holdings` | **real** — transient-empty child rows + lost-update window |
| `def14a_body` | rewash `_apply_def14a`: **`DELETE FROM def14a_beneficial_holdings`** + `_upsert_holding` loop, gated by `def14a_within_cap`. **Live** path (`manifest_parsers/def14a.py`) is `_upsert_holding`-only, **no DELETE** | **real** (rewash side) — DELETE+INSERT + cap-gated DELETE racing a concurrent `_upsert_holding` |
| `primary_doc_13dg` (blockholders) | `_apply_blockholders`: `blockholders_within_retention` count-gate → **DELETE** → `_upsert_filing_row` (`ON CONFLICT DO NOTHING`) | **real** — matches prevention-log L311 ("SELECT COUNT(*) race when gating a DELETE") |

So the hazard is **non-uniform**: form3 / def14a / blockholders carry a genuine
**lost-data** race (not merely duplicate work); form4 / form5 are benign.

**Live def14a is correctly upsert-only (not a bug):** a live accession is
ingested exactly once, so there is no "new parser version" event at live time to
drop stale holders for — that is precisely what *rewash* exists to do (hence the
rewash-only DELETE). The lock's job here is to serialise the rare rewash-DELETE
against a concurrent live `_upsert_holding`, not to add a DELETE to the live
path.

## Source rule

This is an internal concurrency invariant, not an SEC data-treatment rule, so
the governing "source rule" is our **own settled pattern**, not a reg:

- **#1542 precedent** (`institutional_holdings.acquire_13f_accession_write_lock`,
  `institutional_holdings.py:899`): a transaction-scoped
  `pg_advisory_xact_lock` keyed on the accession serialises the 13F live writer
  (`sec_13f_hr.py:483`) against the 13F rewash DELETE+INSERT
  (`rewash_filings.py:1129`). Acquired **inside the transaction, after all SEC
  fetches**, **first** (before any per-instrument refresh lock) so lock order is
  consistent and deadlock-free.
- **Prevention-log L335-338** ("Advisory lock scope vs concurrent writers"): an
  advisory lock is cooperative — **every** writer of the guarded invariant must
  take the **same** lock. Locking only the rewash side would not protect against
  the live writer.
- **Prevention-log L401-402** ("lock acquisition belongs inside the function,
  not at every call site") and **L407-410** ("a process lock / defensive SELECT
  buys no DB isolation; use a real DB boundary keyed to the natural identity").
- **Prevention-log L311** (count-gated DELETE needs a conflicting lock) — the
  blockholders / def14a count-gate is exactly this shape.

The invariant to enforce: **at most one transaction at a time writes a given
accession's typed ownership rows.** Natural identity = `accession_number`. (Not
globally one-kind-per-accession in general — a 13F filing carries both
`primary_doc` and `infotable_13f` — but among the five kinds this lock covers
there are **0** shared accessions, full-pop verified below, so one namespace is
collision-free for the covered set.)

## Design (smallest change that enforces the invariant)

Generalise the 13F helper to a kind-agnostic, accession-keyed lock and apply it
symmetrically on **both** writers (rewash apply + live manifest drain) for each
of the five ownership kinds — exactly the #1542 shape, replicated.

### 1. Shared helper

Add to `app/services/raw_filings.py` (low-level filing primitive, already
imported by both `rewash_filings` and the manifest parsers — no import cycle):

```python
_FILING_ACCESSION_LOCK_NS = "ingest_filing_accession"

def acquire_filing_accession_write_lock(conn, accession_number: str) -> None:
    """Serialise concurrent writers of ONE accession's typed ownership rows
    (#817; generalises the 13F #1542 lock to all ownership kinds).

    Transaction-scoped (pg_advisory_xact_lock): auto-releases on
    COMMIT/ROLLBACK. MUST be called inside the same non-autocommit
    transaction as the typed-table write, AFTER any SEC fetch (never held
    across network I/O), and BEFORE any per-instrument refresh lock so lock
    order is uniform across writers (per-accession then per-instrument)."""
    conn.execute(
        "SELECT pg_advisory_xact_lock("
        "(hashtextextended(%s, 0) # hashtextextended(%s, 0)))",
        (_FILING_ACCESSION_LOCK_NS, accession_number),
    )
```

Key derivation mirrors `acquire_13f_accession_write_lock` byte-for-byte except
the namespace string. 13F keeps its own helper/namespace — 13F accessions never
collide with the others, so the two namespaces coexist safely; not worth
churning the proven 13F call sites.

### 2. Lock placement — driven by the full writer census (dev, 2026-06-26)

Census (every writer of the 6 guarded typed tables, with txn granularity):

- **Insider (form3/4/5)** — *all* writers (live manifest drain, rewash, legacy
  manual ingest) funnel through exactly **two** once-per-accession chokepoints,
  **zero bypass**: `upsert_filing` (`insider_transactions.py:1183`, form4/5) and
  `upsert_form_3_filing` (`insider_form3_ingest.py:83`, form3). Each is called
  once per accession (loops over child rows internally).
- **def14a** — three per-accession writers, no shared chokepoint (the DELETE is
  rewash-only; `_upsert_holding` is per-*row*): rewash `_apply_def14a`, live
  `manifest_parsers/def14a.py:357` txn, legacy
  `def14a_ingest.py:_ingest_single_accession` (still callable).
- **blockholders** — two active per-accession writers: rewash
  `_apply_blockholders`, live `manifest_parsers/sec_13dg.py:327` txn. **Legacy
  retired** (`blockholders.py` docstring, #1233 PR11 — no active call site).

So:

**Insider → lock inside the two chokepoint functions** (`upsert_filing`,
`upsert_form_3_filing`), as their first statement. This covers manifest +
rewash + legacy automatically and is future-proof against new callers
(prevention-log L401-402: the lock belongs inside the function, not at each
call site). One line each.

**def14a / blockholders → lock at each per-accession entry** (no chokepoint to
hide it in; `_upsert_holding`/`_upsert_filing_row` are per-row so the DELETE
would be unguarded):

| Kind | Sites (lock = first statement, before any gate read / DELETE / upsert) |
|---|---|
| def14a | `_apply_def14a` (top, before the `def14a_within_cap` gate read); `manifest_parsers/def14a.py` (inside the `:357` txn, before the `_upsert_holding` loop); `def14a_ingest._ingest_single_accession` (legacy, before its write) |
| blockholders | `_apply_blockholders` (top, before the `blockholders_within_retention` count-gate); `manifest_parsers/sec_13dg.py` (inside the `:327` txn, before the `_upsert_filer`/`_upsert_filing_row` loop) |

**Count/gate reads must be inside the lock** (prevention-log L311): the
`def14a_within_cap` and `blockholders_within_retention` decisions feed the
DELETE, so the lock is acquired **before** those reads, not merely before the
DELETE.

Lock-order rule (deadlock safety): **per-accession lock FIRST**, then any
per-instrument refresh lock (`refresh_insiders_current` /
`refresh_def14a_current` etc.), matching the 13F comment at
`sec_13f_hr.py:~483`. Every path holds exactly **one** accession lock at a time
(one accession per txn — census-confirmed for all three writer classes), so the
accession locks alone can never form a wait-cycle; layering the per-instrument
locks under a uniform "accession-then-instrument" order keeps the combined graph
acyclic.

### 2a. Manifest-worker batch-hold (Codex ckpt-2 — known, accepted, ticketed)

`sec_manifest_worker_tick` opens `connect_job()` (default `autocommit=False`)
and commits **once** after the whole batch (`scheduler.py:4843`); `_dispatch_rows`
has no per-row commit. So each manifest parser's `with conn.transaction()` is a
**savepoint**, and a `pg_advisory_xact_lock` acquired inside it is held until the
**batch** commits — not per accession.

This is accepted for #817 because:

1. **It is exactly the #1542 precedent.** The 13F lock at `sec_13f_hr.py:~483`
   sits inside the same worker's savepoint with the same batch-hold; so do all
   the per-instrument `refresh_*_current` xact locks. #817 extends an
   already-shipped, reviewed pattern — it does not invent a new hazard.
2. **Correctness is preserved.** The lock still gives mutual exclusion: a
   concurrent rewash and the live drain cannot interleave writes to one
   accession. The only costs are *coarseness* (the live path holds the lock
   batch-long, so a concurrent `scripts/rewash.py` blocks longer) and a
   *self-healing deadlock* surface (rewash-vs-live on shared accession/instrument
   keys → Postgres detects + aborts one side → retry, **no data corruption** —
   the lock converts a silent write-race into a loud, retryable serialisation).
3. **The realistic contention is rare.** The manifest worker is a per-process
   singleton (one tick at a time), and `run_rewash` is a manual CLI; the operator
   runbook drives re-ingest via `sec_rebuild` (the live worker), not concurrent
   `scripts/rewash.py`.

The root-cause fix (per-accession commit in `_dispatch_rows`, which would release
every per-accession lock at its natural boundary and also fix #1542 + the
refresh-lock accumulation) is **out of scope** — it changes the transaction
granularity of a hot, #1591/#1274-tuned path and needs its own dev-verify.
Tracked in **#1735**. Rejected alternative: session-scoped locks with explicit
per-accession release — fragile release on aborted txns, session/xact mixing,
and would force moving the insider lock off its shared chokepoint.

### 3. Bulk-ingest path excluded — by table-surface, not "bootstrap-only"

The bulk / bootstrap orchestrators do **not** write any of the six guarded typed
tables at all (census: `grep 'INSERT INTO insider_*|def14a_*|blockholder_*'`
over `sec_bulk_orchestrator*`, `bootstrap_*` → zero hits). Bulk writes only
`ownership_observations` / `_current` rollups, which are serialised on their own
per-instrument refresh locks. So bulk is not a writer of the guarded surface and
needs no accession lock — and adding one to a **batched** bulk transaction (many
accessions per txn) would have risked the #1274-class multi-lock ordering
deadlock. (The earlier "structurally unable to overlap because clean-bootstrap
gated" rationale is *not* relied upon — bulk jobs are callable outside a running
bootstrap when cached archives exist; the sound reason is the empty table
surface.)

### 4. Cohort scan stays a plain SELECT

`_fetch_cohort` does not need `FOR UPDATE SKIP LOCKED`. It reads only
`(accession, fetched_at)` identifiers (no body, no typed rows), and a row that
vanishes/changes between scan and apply is already handled (driver
`rewash_filings.py:178`, "row vanished between cohort scan and read → skipped").
The serialisation that matters is at the **write**, which the per-accession lock
provides. Adding row claims to the scan would be the misleading-isolation
anti-pattern (L410) — it would not protect the typed-table write the cohort scan
doesn't touch. Run-level single-instance locking (ticket Option C) is likewise
rejected: it would close rewash-vs-rewash only, leaving the named rewash-vs-live
race open and the codebase inconsistent with the 13F per-accession choice.

## Full-population verification (dev DB, 2026-06-26)

Registered rewash kinds (`grep document_kind= rewash_filings.py`): `form4_xml`,
`form3_xml`, `form5_xml`, **`def14a_body`** (def14a), **`primary_doc_13dg`**
(blockholders), `infotable_13f` (already #1542-locked, separate namespace).

1. **Cohort population** (`filing_raw_documents` rows per kind) — all non-empty,
   so the lock guards real accessions:

   | kind | rows |
   |---|---|
   | form4_xml | 465,464 |
   | form3_xml | 62,310 |
   | def14a_body | 42,147 |
   | primary_doc_13dg | 30,206 |
   | infotable_13f | 18,325 |
   | form5_xml | 1,586 |

2. **Single-namespace safety (the assumption that was falsified, then bounded).**
   `SELECT accession_number ... HAVING count(DISTINCT document_kind) > 1`
   returned **18,325 accessions spanning >1 kind** — NOT zero. Drill-down: the
   collisions are **entirely `infotable_13f + primary_doc`** (the 13F cover doc
   + infotable of the *same* 13F filing). Both are outside the new namespace's
   five kinds, and `infotable_13f` is on the *separate* `ingest_13f_accession`
   namespace. Re-run scoped to the five locked kinds
   (`form3_xml/form4_xml/form5_xml/def14a_body/primary_doc_13dg`):
   **0 shared accessions.** ⇒ the single `ingest_filing_accession` namespace has
   **no false-sharing** among the kinds it covers. (Premise note: a naive "one
   accession ⇒ one kind" claim is false in general — 13F filings carry two
   kinds — but it holds for the locked set, which is what the namespace needs.)

## Tests

- **Pure-logic**: table-test `acquire_filing_accession_write_lock`'s key
  derivation is deterministic and (for the sampled accessions) differs from the
  13F namespace's key for the same accession. This is a *negligible-collision*
  check matching #1542's own 64-bit `hashtextextended` XOR scheme — NOT a proof
  of formal disjointness (two distinct 64-bit keyspaces can theoretically
  alias; the risk is the same advisory-key birthday risk the codebase already
  accepts everywhere). Assert the SQL text + params shape (no DB needed).
- **Structural-invariant test (preferred over a flaky interleave)**: a grep/AST
  guard asserting every `_apply_*` in `rewash_filings.py` and every live
  ownership manifest-drain write acquires `acquire_filing_accession_write_lock`
  (or, for 13F, `acquire_13f_accession_write_lock`) before its first typed-table
  mutation. This is the L337-338 "every writer takes the same lock" check made
  executable, and it won't rot silently when a sixth kind lands.
- **One DB-tier interleave** (`-m db`, sparingly): two connections, A holds the
  lock for accession X inside a txn; B's `acquire_filing_accession_write_lock(X)`
  blocks until A commits (use `pg_try_advisory_xact_lock` probe or a short
  `statement_timeout` to assert B is blocked, then released). Proves the lock
  actually serialises — not a defensive SELECT.

## Tradeoffs

- Two lock namespaces (13F legacy + the new general one) rather than unifying —
  chosen to avoid churning the proven #1542 13F call sites and risking a
  key-mismatch regression. The new namespace covers form3/4/5/def14a_body/
  primary_doc_13dg (0 shared accessions among them, full-pop verified); 13F
  keeps its own. Cross-namespace aliasing risk is the ordinary 64-bit
  advisory-key birthday risk #1542 already accepts.
- form4/form5 get the lock despite being benign-idempotent — for uniformity
  across the insider family and cheap future-proofing (one line each, same drain
  function), not because they currently lose data.
- Bulk excluded by structural invariant, not by a lock — documented at the call
  sites so a future maintainer who makes bulk concurrent knows to revisit.

## DoD / dev-verify (ETL clauses 8-12)

Output is **unchanged** (this is pure serialisation — same parse, same rows),
so **no `sec_rebuild` and no parser-version bump**. Dev-verify:

- Run `scripts/rewash.py --kind form3_xml --dry-run` then a real small-scope
  rewash on dev; confirm parity (same typed rows before/after) and that the lock
  acquires/releases (no wedge). Roll back any state touched so dev DB is clean.
- Interleave proof: open two psql sessions, both `pg_advisory_xact_lock` the
  same key — confirm the second blocks until the first commits.
- Smoke the panel (AAPL/GME/MSFT/JPM/HD) `/instruments/<symbol>/ownership-rollup`
  renders unchanged.
