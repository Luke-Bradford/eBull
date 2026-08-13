# #1337 §11 — S15 `filings_history_seed` bulk-first optimisation — DEFERRED

Status: **DEFERRED** (2026-05-29). ROI (~6 min wall-clock) does not justify the
correctness risk to DEF 14A + insider-observation data paths. This doc is the
durable record of *why* the two naive framings are wrong and what a sound
version would require, so a future revisit starts from the conclusion, not the
dead ends.

## 0. Decision

S15 (`filings_history_seed`, `app/workers/scheduler.py:4650`) is a **legacy
per-CIK HTTP submissions walk**, now largely redundant with the bulk-first
stages S8 + S13. Both naive bulk-first optimisations for it are unsound or
mis-targeted (§1, §2). The only sound optimisation (§3) is a materially larger,
correctness-sensitive change for ~6 min. Deferred per operator decision.

## 1. Rejected: gap-memo §2 #2 "master.idx 730d backfill" — wrong table

`.scratch/bulk_zip_gap_analysis.md` §2 #2 proposed replacing S15's HTTP walk
with a `master.idx` quarterly backfill emitting `record_manifest_entry`. That
writes `sec_filing_manifest`. S15's consumers read `filing_events`, which is
**not** superseded by the manifest on the bootstrap path:

| consumer | reads | file:line | breakage if `filing_events` empty |
| --- | --- | --- | --- |
| DEF 14A manifest adapter `def14a_within_cap` | `filing_events` | `app/services/def14a_ingest.py:492-503` | tombstones EVERY issuer DEF 14A row |
| business-summary 10-K/A fallback `_find_prior_plain_10k` | `filing_events` | `app/services/business_summary.py:1421` | 10-K/A rows tombstone instead of parse |
| `ownership_observations_sync.sync_insiders` | `filing_events` (LEFT JOIN cap gate, fail-closed) | `app/services/ownership_observations_sync.py:228-231` | zeroes Form 4/5 ownership observations |
| legacy ingest paths (8-K / DEF 14A / business-summary / Form 3/4/5) | `filing_events` | various | no-op (sole discovery source) |

Only a one-way bridge exists (`seed_manifest_from_filing_events`,
`app/jobs/sec_first_install_drain.py:156`, `filing_events → manifest`). No
reverse bridge, and `master.idx` lacks `primary_document_url` + acceptance
precision to seed `filing_events` richly. Honouring the memo would require
building a NEW manifest→filing_events seeder — large + risky. Rejected.

## 2. Rejected: per-issuer `filing_events` row-existence skip-gate — unsound

First redesign: mirror P2 (#1377) `_bulk_already_seeded_13f` against
`filing_events` — skip S15's HTTP walk for any issuer with a `provider='sec'`
row whose `filing_date < today−730d`, gated on bootstrap context.

Codex checkpoint-1 review (3 BLOCKING) — verified correct:

- `filing_events` has **multiple** SEC writers (S8 recent block, S13 `files[]`
  secondary walk, operator triage, prior bootstrap attempts). A single
  pre-window row does **not** prove the in-window range is contiguous/complete.
- **False-positive scenario:** prolific issuer whose S8 recent block reaches
  back only ~200d (gap in `[730d, 200d]`), plus a stray 800d-old row from S13's
  `files[]` walk or a prior triage → `NOT EXISTS` finds the 800d row → S15
  skips → the `[730d, 200d]` gap silently tombstones DEF 14A + zeroes insider
  observations.
- `progress_ctx is not None` proves "running under bootstrap," NOT "clean DB /
  this row came from S8's contiguous recent walk." The contiguity bridge the
  proof depends on is therefore unprovable from a bare row's existence.

The P2 mirror is unsound here precisely because `filing_events` provenance is
many-writer, unlike P2's narrow `sec_filing_manifest source='sec_13f_hr'` scope.

## 3. The only sound version (if ever revisited): gate on S13's completion signal

The bulk-first picture for issuer `filing_events` at bootstrap:

- **S8 `sec_submissions_ingest` (C1.a)** — bulk `submissions.zip` `recent`
  block → `filing_events` + populates `sec_cik_submissions_files_index` sidecar.
- **S13 `sec_submissions_files_walk` (C1.b)** — walks deeper `filings.files[]`
  secondary pages (sidecar-driven) → `filing_events`.

S8 + S13 together cover an issuer's full history. S15 is the legacy backstop.

A sound skip must key on **S13's own per-CIK completion signal** (S13-owned
provenance), not on `filing_events` rows:

- sidecar sentinel `__no_overflow_pages__` (CIK's `recent` fit the 1000-cap →
  S8 alone is complete), OR
- all of the CIK's `sec_cik_submissions_files_index` pages have a fresh
  `external_data_watermarks` row (source-key `sec.last_modified.submissions_files`,
  key `<cik>:<page_name>`) from the current run → S13 finished the CIK.

CIKs with either signal ⇒ S8+S13 jointly complete ⇒ S15 skips. CIKs where S13
failed/incomplete (fail-closed parse-error) ⇒ S15 still walks (backstop intact).

Cost: ~0.5-1 day. Touches DEF 14A + insider-observation correctness →
committee/Codex-level scrutiny warranted. Requires verifying S13's failure
modes don't leave a "looks-complete" signal on an incomplete CIK. Out of scope
for the ~6 min ROI today.

## 4. References

- P2 mirror (sound for 13F, unsound to copy here): `app/jobs/sec_first_install_drain.py:407-456`
- S8 issuer bulk seed: `app/services/sec_submissions_ingest.py:498-544`
- S13 secondary walker: `app/services/sec_submissions_files_walk.py` (docstring §0-§4)
- S8↔S15 both write filing_events: `app/services/bootstrap_orchestrator.py:304-313`
- filing_events retention (10y): `app/services/filings.py:76-118`
- Epic: #1337 §11. Tracking: (filed this session — see epic comment)
