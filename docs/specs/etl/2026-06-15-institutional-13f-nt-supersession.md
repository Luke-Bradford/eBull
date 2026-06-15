# 13F-NT supersession — institutional ownership double-count across filer reorgs (#1639)

Epic #788 (ownership card trustworthy). Bug C of the 2026-06-14 ownership DQ audit.
Sibling fixes: #1638 (13D reporter_cik), #1640 (owner-once dedup).

## Problem

`/instruments/AAPL/ownership-rollup` counts the Vanguard family at
**2,862,733,655 shares = 19.5% of AAPL** — roughly 2×. Vanguard's real
AAPL stake is ~9-10% (cross-source: public Vanguard 13F coverage).

Dev DB (`ownership_institutions_current`, instrument 1001), Vanguard CIKs:

| Filer | CIK | period_end | filed | shares |
|---|---|---|---|---|
| VANGUARD GROUP INC (parent) | 0000102909 | **2025-12-31** | 2026-01-29 | 1,426,283,914 |
| VANGUARD CAPITAL MANAGEMENT LLC | 0002100119 | 2026-03-31 | 2026-05-15 | 953,847,648 |
| VANGUARD PORTFOLIO MANAGEMENT LLC | 0002100121 | 2026-03-31 | 2026-05-08 | 331,437,055 |
| + 8 more sub-entities | … | 2026-03-31 | 2026-05 | … |

The 10 sub-entities (all period 2026-03-31) sum to **1,436,449,741**. The
parent's standalone 2025-12-31 figure is **1,426,283,914**. They are within
0.7% — **the same ~9.3% AAPL book, one quarter apart** — yet the rollup sums
both (parent's stale Q4'25 + children's Q1'26).

Systemic on every large cap (MSFT / JPM / HD / GME all mix a stale parent
quarter with Q1'26 sub-entity quarters).

## Root cause — 13F-NT (Notice) supersession is not modeled

Vanguard restructured its 13F reporting at Q1'26. EDGAR submissions for CIK
0000102909 (verified 2026-06-15):

```text
13F-NT   filed=2026-05-08  period=2026-03-31   acc=0000102909-26-002707   ← NOTICE
13F-HR   filed=2026-01-29  period=2025-12-31   acc=0000102909-26-000031   ← last holdings report
```

A **13F-NT** ("Notice") is the SEC's native anti-double-count primitive: the
filer declares it holds **nothing reportable** this quarter — its holdings are
reported by *other managers* (here, the spun-out sub-entity CIKs, each filing
its own 13F-HR). The parent's prior 13F-HR is thereby **superseded**.

Our pipeline ingests **13F-HR only** — `13F-NT` is intentionally absent from
`_FORM_TO_SOURCE` (`app/services/sec_manifest.py`), so the daily-index /
atom discovery layers see NT filings and drop them (`skipped_unmapped_form`).
We therefore never learn the parent's Q4'25 HR is dead, and the rollup keeps
the stale 1.426B **and** sums the Q1'26 children → 2× double-count.

This is the layer *outside* #1567. #1567 fixed *intra-filing* aggregation
(one manager's discretion-split rows → SUM, giving Vanguard Group Inc's
correct 1.426B single figure). #1639 is *inter-filer* supersession across a
corporate reorg.

### Rejected: the cheap heuristic

`institutional_filers.last_filing_at > last_13f_hr_at` looks like a clean
"latest filing is a Notice" flag. **Empirically it misses Group Inc** (reads
`f`): those columns are maintained by `sec_13f_filer_directory_sync`, which
walks only *closed* quarters' `form.idx`; the 2026-05-08 NT sits in the
still-open 2026-Q2 index. The directory timestamps lag by up to a quarter and
cannot be the supersession signal. Name-prefix ("VANGUARD%") family matching
is also rejected — fragile, and the NT mechanism is the principled SEC-native
signal.

## Design

Read-side correction (operator-confirmed locus 2026-06-15). The **only**
figure-producing reader of `ownership_institutions_current` is the rollup
(`_collect_canonical_holders_from_current`); `capabilities.py` (EXISTS) and
`ownership_observations.py` (COUNT) do not sum. So excluding superseded filers
at the rollup read corrects the only operator-visible figure — pure-read (no
`_current` re-backfill), mirroring PR #1640.

### Supersession rule — by `period_end` (NOT filed_at)

A filer's (latest, per `_current`) 13F-HR for quarter `P_hr` is **superseded**
iff that filer filed a 13F-NT for a *later* quarter:

```text
HR superseded  ⟺  ∃ NT for the same filer_cik with NT.period_end > HR.period_end
```

**Why period, not filed_at** (Codex ckpt-1 HIGH #1, the load-bearing correction):
a `13F-NT/A` amending an *old* quarter can be filed *after* a filer has resumed
holdings reporting — its `filed_at` is later than the live HR's `filed_at`, but
its period is older. A `filed_at` comparison would wrongly suppress the live HR.
Period is the semantically correct axis: "is the filer's *latest quarter's*
word a Notice?". `period_end` ordering is immune to amendment file-time scramble.

This also moots any same-day-filed ambiguity (Codex MEDIUM): the daily index
carries only `Date Filed` → midnight UTC with `accepted_at = None`
(`sec_daily_index.py:118`), so intraday HR-vs-NT ordering is unknowable — but
we never compare filed times. Strict `>` on quarter-ends keeps the HR when a
filer somehow files both HR and NT for the *same* quarter (contradictory; keep
holdings — never drop on non-strictly-later evidence).

Walks every case: resume (HR→NT→HR, newer HR quarter beats old NT quarter →
kept), late `NT/A` for an old quarter (older period → can't kill newer HR),
filer-goes-notice-only (max NT quarter > last HR quarter → dropped), stale
transitional HR (Group Inc Q3'25 = 98,718 shares — not in `_current`; only the
latest HR per filer is, and the NT supersedes it).

### Schema — `institutional_filer_13f_notices`

New table (`sql/199`), keyed on `filer_cik` independent of
`institutional_filers` (some held `filer_cik`s are absent from that directory;
we only ever *exclude* on positive NT evidence, so directory gaps are safe):

```sql
CREATE TABLE IF NOT EXISTS institutional_filer_13f_notices (
    filer_cik         TEXT NOT NULL CHECK (filer_cik ~ '^[0-9]{10}$'),
    accession_number  TEXT NOT NULL,
    period_end        DATE NOT NULL,        -- <periodOfReport> from the NT primary_doc
    form              TEXT NOT NULL CHECK (form IN ('13F-NT', '13F-NT/A')),
    filed_at          TIMESTAMPTZ NOT NULL, -- audit only; NOT used in the supersession predicate
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_number)
);
CREATE INDEX IF NOT EXISTS idx_13f_notices_filer_period
    ON institutional_filer_13f_notices (filer_cik, period_end DESC);
```

PK on `accession_number` keeps capture idempotent. Re-capture uses
`ON CONFLICT (accession_number) DO UPDATE SET period_end = EXCLUDED.period_end,
form = EXCLUDED.form, filed_at = EXCLUDED.filed_at` — **DO UPDATE, not DO
NOTHING** (Codex LOW #1), so a re-fetch can correct/enrich metadata rather than
freeze the first write. The `(filer_cik, period_end DESC)` index serves the
rollup's correlated `NOT EXISTS`.

### Capture — `sec_13f_notice_sync` job

`period_end` is not on any SEC index line — it lives in the NT filing's
`primary_doc.xml` (`<periodOfReport>MM-DD-YYYY</periodOfReport>`, verified on
acc 0000102909-26-002707). So capture must fetch + parse the primary_doc. A
dedicated job (not a branch inside `sec_daily_index_reconcile`) keeps the cheap
fetch-free safety-net reconcile untouched, and lets the backfill reuse the same
code without re-driving the full manifest reconcile over old indexes.

`sec_13f_notice_sync(conn, http_get, *, since=None)`:

1. Read the daily index for each date in the window (default = yesterday;
   reuses `read_daily_index`), filter `row.form ∈ {13F-NT, 13F-NT/A}`.
2. For each NT, fetch `…/edgar/data/{cik}/{accession-nodash}/primary_doc.xml`
   and extract `periodOfReport` via a small dedicated parser
   (`_walk_text(root, "periodOfReport")` + `strptime("%m-%d-%Y")`, reusing
   sec_13f.py's `_walk_text` / `_zero_pad_cik` primitives — NOT the EdgarTools
   holdings parser, which a no-holdings NT would fail).
3. Upsert `(filer_cik, accession, period_end, form, filed_at)`.

Volume is low — most filers file HR, not NT; NT capture is ≈ a few hundred
fetches per quarter clustered on the four 45-day deadline days, near-zero
otherwise. A failed fetch skips that NT (logged) and is retried by the next
run's trailing window — under-capture errs toward the *existing*
(non-suppressing) behaviour, never toward wrongly dropping holdings.

- **Steady-state**: `ScheduledJob` daily (lane `sec_rate`, gated
  `_bootstrap_complete`). Default window = a **trailing `_STEADY_STATE_LOOKBACK_DAYS`
  (5) ending yesterday**, NOT yesterday-only (Codex ckpt-2): a transient
  fetch/parse failure on a deadline day (when NTs cluster) would otherwise be
  committed as "success" and never retried, because the next run scans a
  *different* day. Already-captured Notices in the window are skipped
  (`_already_captured` existence check) so the re-scan re-fetches only the
  new/failed ones — near-zero cost on the ~0-NT days, self-healing on failures.
- **Backfill**: same callable over a date range. Floor =
  `MIN(ownership_institutions_current.period_end)` — **the period axis, NOT
  `filed_at`** (Codex ckpt-2): a `13F-HR/A` amending an old quarter can be filed
  recently, so a `filed_at` floor can sit *after* an NT that supersedes that
  stale amended HR and miss it. Any relevant NT has
  `NT.filed_at > NT.period_end > HR.period_end >= MIN(period_end)`, so flooring
  the daily-index scan at `MIN(period_end)` provably captures it. Capped at the
  8-quarter retention horizon (`THIRTEEN_F_HR_RETENTION_QUARTERS = 8`).
  Registered as a manual-only one-shot (`sec_rebuild` "triangle": `_INVOKERS` +
  `MANUAL_TRIGGER_JOB_SOURCES` (`sec_rate`) + empty `MANUAL_TRIGGER_JOB_METADATA`):
  `POST /jobs/institutional_13f_notice_backfill/run`.

Rejected — **manifest source** (`sec_13f_nt` in `_FORM_TO_SOURCE` + a parser):
more idiomatic and gives atom/per-cik steady-state for free, but reverses the
deliberate "NT intentionally absent from `_FORM_TO_SOURCE`" decision and adds a
CHECK migration. Daily-index discovery is sufficient given quarterly cadence
(daily latency is irrelevant to a quarterly figure).

### Read — rollup exclusion + suppression telemetry

`_collect_canonical_holders_from_current` institutions query gains one clause:

```sql
SELECT filer_cik, filer_name, filer_type, ownership_nature,
       source, source_accession, shares, period_end
FROM ownership_institutions_current c
WHERE c.instrument_id = %s
  AND c.shares IS NOT NULL
  AND c.exposure_kind = 'EQUITY'
  AND NOT EXISTS (
        SELECT 1 FROM institutional_filer_13f_notices n
        WHERE n.filer_cik   = c.filer_cik
          AND n.period_end  > c.period_end
  )
```

That is the entire figure change. Excluded filers vanish from the institutions
slice; residual / concentration / coverage / banner recompute downstream from
the corrected slice total automatically (they read slice outputs, not
`_current`).

**Suppression telemetry** (Codex MEDIUM #2 — make the residual jump
explainable): the filter runs *inside* `_collect_canonical_holders_from_current`,
before `_reconcile_owner_once` (so no double-correction with PR #1640 — Codex
LOW-2 confirmed). Surface the suppressed set — a companion query
(`_read_notice_suppressions`, a lateral join picking the latest superseding NT)
lists the `_current` institution rows that WERE superseded. Three surfaces:

1. **Structured `corrections_applied[]` JSON** (committee 2026-06-15 rec — the
   first concrete down-payment on the #1647 machine-trust contract). The rollup
   carries `corrections_applied: tuple[CorrectionApplied, ...]` and the API
   exposes it as a first-class array of
   `{kind: "suppressed_by_13f_nt", filer_cik, filer_name, shares_removed,
   superseded_period, winning_nt_period, winning_nt_accession}` plus a
   convenience `suppressed_by_notice` count. This is machine-readable — a
   decision agent (or the FE) sees *why* the institutions total changed, not
   just the corrected number. `CorrectionApplied.kind` is a closed vocabulary so
   the contract extends cleanly when #1645 (13D-group) / #1644 (DEF14A) add
   their own correction kinds.
2. **CSV `__suppressed_by_13f_nt:<filer_cik>__` memo rows** (`build_rollup_csv`),
   mirroring PR #1640's `__dropped:` audit rows — for the human/spreadsheet
   audit trail. `as_of_date` carries the superseded HR quarter; `filer_type`
   carries `nt_period=<winning quarter>`.
3. The FE type mirror (`OwnershipCorrectionApplied` in
   `frontend/src/api/ownership.ts`) keeps the contract from drifting.

Without this an operator sees the institutions wedge shrink and the residual
grow with no visible cause.

**Ordering vs PR #1640 / PR #1567** (Codex LOW-2): supersession removes stale
same-filer 13F candidates from `_current` *before* they enter the candidate
list, so it composes cleanly — PR #1567 is intra-filing SUM (upstream, at
ingest); PR #1640 is same-owner cross-channel MAX/SUM (downstream of this
filter).

## What this does NOT do

- **BlackRock AAPL under-count** (~6.2M vs expected ~1.3B — main BlackRock 13F
  CIK 1364742 not resolved for AAPL). Opposite-direction coverage gap, flagged
  in the audit as separate. Deferred; file a follow-up issue.
- **No `_current` / observations change.** `_current` keeps the stale row; only
  the rollup read excludes it. (If a future direct `_current` reader needs the
  correction, promote the filter into `refresh_institutions_current` then.)
- **No new manifest source.** NT stays out of `_FORM_TO_SOURCE`; the notices
  table is a standalone capture.
- **No name/family heuristic.** Supersession is per-`filer_cik` via the SEC NT
  mechanism only.

## Settled decisions / invariants

- **#1102 (CIK = entity, CUSIP = security)** — preserved. Filers stay distinct
  per CIK; we never collapse sub-entity CIKs into the parent. Supersession is a
  per-filer staleness drop, not an identity merge.
- **Coverage banner = 5-state server-driven (#840 / #923), I11** — untouched;
  coverage is telemetry over slice outputs, auto-corrects.
- **I4 `denominator_basis`** — institutions stay `pie_wedge`; only their member
  set shrinks.
- **Prevention-log #1567 "multi-row source position SUM at every locus"** — not
  contradicted; that's intra-filing, this is inter-filer.
- **Prevention-log "MAX overlapping, SUM additive" (#1640, I14)** — orthogonal;
  applies to one owner across channels, runs after this filter.

New prevention-log entry: *"A 13F-NT (Notice) supersedes the filer's prior
13F-HR — model HR/NT supersession by `period_end` (NOT filed_at: an NT/A for an
old quarter can be filed after a resumed HR), never sum a stale parent quarter
alongside post-reorg sub-entity quarters."* + data-engineer skill update
(institutions FAQ Q1 + a new "13F-NT supersession" note).

## Test plan

**As-built note:** the supersession predicate landed in SQL (the `NOT EXISTS`
clause in `_collect_canonical_holders_from_current` + the lateral join in
`_read_notice_suppressions`), not as a standalone pure function. It is therefore
tested where it lives — a DB-backed parametrised test over real
`ownership_institutions_current` + `institutional_filer_13f_notices` rows — which
exercises every case the original pure-function plan enumerated. Two genuinely
pure layers (the NT parser, the capture orchestration) keep pure tests.

Pure (no DB) — `tests/test_sec_13f_notice_parser.py`: NT primary_doc parse over
fixture XML mirroring the real Vanguard NT — `periodOfReport` `MM-DD-YYYY` →
DATE; picks the header filer CIK, never an `otherManagers` CIK; no-holdings NT
body parses without the EdgarTools holdings path; missing `cik` / `periodOfReport`
raise; `13F-NT/A` parses like `13F-NT`.

Pure (no DB, fakes) — `tests/test_sec_13f_notice_sync.py`: `read_daily_index`
monkeypatched + fake `http_get` + fake conn recording upserts. Filters to
`13F-NT` / `13F-NT/A` (non-NT ignored); fetches each Notice's primary_doc; fetch
failure (non-200) and parse failure skip + count without writing; multi-day
window walked; `since > until` raises; primary_doc URL shape pinned.

DB-backed — `tests/test_ownership_13f_nt_supersession.py` (the rollup path):

- Headline: parent (stale Q4 HR) + sub-entity (newer Q1 HR) + parent NT
  (period Q1) → rollup institutions total = sub-entity only (not 2×); parent
  dropped; `corrections_applied` carries the structured record; CSV export emits
  `__suppressed_by_13f_nt:<parent>__`.
- Parametrised predicate cases: `later_nt` (dropped), `older_nt_resume` (kept),
  `nt_a_old_quarter` after resumed HR (kept — the Codex HIGH #1 period-axis
  case), `same_quarter` (kept, strict `>`), `other_filer_nt` (kept — no cross),
  `no_notice` (kept). Each asserts both the kept/dropped outcome AND the
  correction count (0 kept / 1 dropped).
- Registry triangle: `tests/test_layer_123_wiring.py` pins both the scheduled
  `sec_13f_notice_sync` (in SCHEDULED_JOBS, lane sec_rate, bootstrap-gated) and
  the manual-only `institutional_13f_notice_backfill` triangle (invoker +
  metadata + source).

## Verification / DoD (clauses 8-12)

1. Branch backfill run on dev: `POST /jobs/institutional_13f_notice_backfill/run`;
   confirm Group Inc 0000102909 NT (acc 0000102909-26-002707, period 2026-03-31)
   recorded in `institutional_filer_13f_notices`.
2. Smoke panel AAPL / MSFT / JPM / HD / GME — `/instruments/{s}/ownership-rollup`
   institutions total drops to the de-superseded figure. AAPL: 2,862,733,655 →
   **1,436,449,741 (9.78%)**.
3. Cross-source: Vanguard AAPL ~9-10% (✓ against public 13F coverage).
4. PR records the figure + commit SHA per instrument exercised.

Steady-state: `sec_13f_notice_sync` daily ScheduledJob. Jobs-proc restart onto
merge SHA required (new scheduled job + backfill invoker), then run the backfill
once. No `sec_rebuild` (no parser-version bump; pure capture + read).

## Codex ckpt-1 findings — resolution

| # | Sev | Finding | Resolution |
|---|---|---|---|
| 1 | HIGH | `filed_at` predicate wrong for amendments (late NT/A for old quarter kills resumed HR) | **Switched to `period_end` comparison.** Capture the NT's `<periodOfReport>`; predicate `NT.period_end > HR.period_end`. |
| 2 | HIGH | Daily index has no acceptance datetime (`accepted_at=None`, midnight UTC) | Moot — predicate no longer uses filed time. `period_end` fetched from the NT primary_doc. |
| 3 | HIGH | Backfill window "~2 quarters" too short (13F retention = 8 quarters) | Floor = 8-quarter `THIRTEEN_F_HR_RETENTION_QUARTERS`. (Ckpt-2 later refined the floor column from `filed_at` to `MIN(period_end)` — see the Capture section.) |
| 4 | MED | Same-day HR+NT unresolved by date-only capture | Moot under `period_end` axis; strict `>` keeps HR on equal/contradictory period. |
| 5 | MED | Dropping parent can remove instruments sub-entities don't cover — needs telemetry | Added `suppressed_by_notice` count + `__suppressed_by_13f_nt:` CSV memo; classified as ingest coverage-gap in the audit trail. |
| 6 | LOW | `ON CONFLICT DO NOTHING` blocks enrichment | Changed to `DO UPDATE` for `period_end` / `form` / `filed_at`. |
| 7 | LOW | (confirmation) no double-correction with #1640/#1567 if filtered before reconcile | Filter sits in `_collect_canonical_holders_from_current`, before `_reconcile_owner_once`. |
