# Follow-up session — #817 cluster residuals (assessed 2026-06-26 via 4 parallel agents)

Worth-doing ranking (best ROI first). Each item carries the best-case approach +
gotchas the assessment surfaced. Already handled this session: #753 closed,
#817 merged (`84529e1b`), #1735 filed, #815/#1620 deferred-with-evidence,
#1324 closed (already-satisfied), #1326 corrected (premise falsified, left open).

---

## 1. #1735 — per-accession commit in the manifest worker  ·  VERDICT: DO-NOW · effort S · **highest value**

Real fix for the coarseness/deadlock surface #817 + #1542 + all `refresh_*_current`
locks inherit. Safe — in-repo precedent already does exactly this
(`tombstone_stale_failed_upserts`, `app/services/sec_manifest.py:924-970`). Nothing
relies on whole-batch atomicity (verified: no cross-row dep; `transition_status` is
its own `with conn.transaction()`; statement_timeout #1690 is a libpq startup param;
#1591 prefetch cache is process-RAM; `tracker.row_count` reads in-memory stats).

**Approach (2 edits, `app/jobs/sec_manifest_worker.py::_dispatch_rows` ~line 617):**
1. `conn.commit()` at the top before `for row in rows:` — closes the implicit read-tx
   so the first `transition_status`'s `with conn.transaction()` opens a TOP-LEVEL tx
   (BEGIN), not a SAVEPOINT. Same `conn.commit()` + rationale as `sec_manifest.py:924`.
2. `conn.commit()` after each per-row terminal `transition_status` (call sites ~:676,
   ~:718, ~:732) — releases that accession's advisory lock at its boundary. Copy the
   sweep's per-row try/except robustness (`sec_manifest.py:926-970`).
   Do **NOT** wrap the loop in `with conn.transaction()` — that re-creates the batch SAVEPOINT.

**Test:** pure-logic — mock conn, assert `commit` call count == rows dispatched; keep the
`parsed->parsed`/`tombstoned->tombstoned` no-op tests + `test_illegal_transition_raises`.
**Dev-verify:** scoped `POST /jobs/sec_rebuild/run {"source":"sec_def14a"}`; while draining,
query `pg_locks WHERE locktype='advisory'` and confirm count stays ~1 (per in-flight
accession), not climbing to batch size; confirm tick parsed/tombstoned/failed counts
output-identical; smoke `/instruments/{AAPL,GME,MSFT,JPM,HD}/ownership-rollup`.
**On landing:** drop the "held until BATCH commit" caveat from the lock docstrings
(`raw_filings.py:186-195`, `manifest_parsers/def14a.py:366-372`, `sec_13dg.py`, and
`institutional_holdings.acquire_13f_accession_write_lock`); close #1735.

---

## 2. #1319 + #1323 — ETL-docs hygiene, ONE docs-only PR  ·  VERDICT: DO · effort M

Branch `docs/1319-1323-etl-source-hygiene`. Claude bot auto-skips doc-only diffs → merge on CI-green.

**#1319** (valid, and BROADER than the ticket): the cited `::test_<name>` functions
don't exist — not just the 7 named (`test_sec_13f_hr_wired` etc.) but also in
`company_tickers.md`, `company_tickers_exchange.md`, `company_tickers_mf.md`,
`etoro_candles.md`, `sec_13f_securities_list.md` (grep-verify each cite is MISSING).
Replace each §12 cite with the REAL parametrized IDs in
`tests/smoke/test_etl_source_to_sink.py` (`test_manifest_source_has_registered_parser[<src>]`,
`test_manifest_source_has_sink_tables[<src>]`, etc.). For asserts the parametrized suite
does NOT cover (ScheduledJob presence, exact bootstrap stage #, table existence), state
"verified by runbook, not the import-time gate". Fix ALL ~12 broken specs (same defect class).
DEFER the ticket's clause-2 "CI test that parses specs and asserts cited IDs exist" — it's
non-docs-only, separate follow-up.

**#1323** (valid, but verify first): add a 21-source retry-posture table to
`docs/etl/sources/README.md`. `1h=_FAILED_RETRY_DELAY` + `24h=_PENDING_CIK_REFRESH_DELAY`
(n_csr resolver-miss only) confirmed in code. ⚠ The "benign-skip on 403/404" FINRA column
is UNVERIFIED — grep of the ingest service found only row-skip counters; trace it through
the PROVIDER/FETCHER layer before tabling it as fact. If that verification stalls, **drop
#1323 and ship #1319 alone.**

---

## 3. #1330 — template-version stamp + lint  ·  VERDICT: DO-LATER · effort M-L · separate non-docs-only PR

⚠ Ticket framing stale: `scripts/check_etl_source_docs.sh` ALREADY EXISTS and is wired
into `.githooks/pre-push` + CI (driven by `scripts/_etl_source_inventory.py`). Real work is
EXTEND it for version-compare, not create. Adds a new pre-push failure mode → needs codex
checkpoint + a full gate run to prove it doesn't wedge + an alignment test (the existing
script has one pinned). Stamp ~21 specs + add canonical version to `_etl_source_inventory.py`.
Lower priority — marginal value (catches template drift), real wedge risk.

---

## 4. #1620 lever-1 (surrogate-PK drop)  ·  VERDICT: DO-WHEN DB>60GB · effort S · clean win, parked

`(fact_id, period_end)` PK = 690MB, 86 lifetime scans. Clean low-risk drop: NO FK refs
`fact_id`; uniqueness already held by `uq_facts_raw_identity`; partition-key-in-PK rule only
blocks CREATING a PK not dropping; 0 publications so losing replica-identity=default is safe.
`ALTER TABLE financial_facts_raw DROP CONSTRAINT financial_facts_raw_pkey;` (cascades to 126
leaves), keep the `fact_id` column + sequence, no app/read change. Smoke: ON CONFLICT upsert
still works; confirm ~690MB reclaimed. **Trigger:** DB crosses 60GB warn threshold AND cheaper
levers exhausted (note: `def14a_body` ~27GB raw is the dominant lever, sequence ahead).
**Lever-2 concept-interning: SKIP** — ticket's 14.6M rows is 3× stale (actual 4.49M → saving
only ~0.4-0.6GB), and it rewrites the upsert identity (`fundamentals/__init__.py:391`), the
Python dedup key (`:528`), and hardcoded concept-string views in `sql/156`. Poor value/risk.

---

## 5. #815 — cik_raw_documents cache race  ·  VERDICT: SKIP-til-trigger · re-scope only

Worst case today = ONE benign duplicate SEC fetch into an already-idempotent
`store_cik_raw` (`ON CONFLICT (cik, document_kind) DO UPDATE`, `cik_raw_filings.py:118`) →
no clobber, no corruption. Only callers are operator CLIs (`scripts/run_reconciliation.py`,
`seed_top_13f_filers.py`, `verify_filer_seeds.py`) — no scheduler/API/job route. The naive
lock-across-30s-`urlopen` fix = the #719/#1129 stranded-idle-conn class for zero gain.
**Action when revisited:** re-scope to an invariant comment at
`reconciliation.py::_fetch_companyfacts_payload` + `filer_seed_verification.py::_fetch_submissions`,
NOT a lock. **Trigger to actually build** (then: `pg_try_advisory_lock` fast-skip — loser
dup-fetches, winner re-checks then fetches, session-lock released in finally, byte-identical
key SQL pinned by tripwire, `%s::text` never int4): (1) a ScheduledJob/API route calls
`run_spot_check` or the seed verifier, OR (2) a 2nd concurrent same-`(cik,document_kind)`
consumer lands.
