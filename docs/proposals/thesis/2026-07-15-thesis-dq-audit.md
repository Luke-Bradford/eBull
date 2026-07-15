# Standing thesis DQ audit — nightly full-population scan (#2014)

Status: proposal. Small; self-merge shape. Parent: #2007 (insert-time guards), #2008 (audit slot precedent).
Purpose: stored theses do not re-validate themselves; guards evolve. A nightly scan of the
LATEST thesis per instrument surfaces violations as operator-triage candidates. No auto-regen
(operator / #2010 staleness path decides).

## Source rules / precedents

- Thesis rows are append-only; freshness = latest `created_at` (settled-decisions "thesis" §§).
  Audit scans `DISTINCT ON (instrument_id) … ORDER BY created_at DESC, thesis_version DESC,
  thesis_id DESC` — the API's own tiebreak (`app/api/theses.py:293`), deterministic under
  same-timestamp inserts (Codex ckpt-1).
- Run linkage: `thesis_runs.thesis_id` is a NULLABLE, NON-unique FK (sql/218) — join via
  `LEFT JOIN LATERAL (… WHERE r.thesis_id = t.thesis_id ORDER BY r.run_id DESC LIMIT 1)`
  so bad historical multi-links can't duplicate findings. Two absence states counted
  SEPARATELY: `no_run` (no linked run row) vs `no_context_summary` (run row, NULL audit
  columns — nullable by design, sql/223).
- Ordering/zone invariants = exactly #2007's `_validate_writer_output` semantics
  (`bear<=base<=bull` over non-null pairs, `zone_low<=zone_high`, `_to_float` NaN/±inf→None).
  Predicates extracted into the audit module as pure functions; `_validate_writer_output`
  keeps raising ValueError on the same conditions (no behavior change at insert time).
- Availability truth per run = `thesis_runs.context_summary.blocks` (#2017): per-block
  `available` + as-of stamps, persisted PRE-LLM. The claim-lint class compares the MEMO's
  unavailability claims against the run's OWN summary (fabrication class, #2007 Defect 2) —
  NOT against freshly rebuilt context (rebuilding per block would duplicate
  `_assemble_context`'s queries; drift-vs-now is #2010's staleness concern, not DQ).
- Candidate-not-assertion posture: same contract as `scripts/dq_audit.py` (board-feeder) —
  findings are triage candidates; the operator confirms before filing.

## Violation classes (latest thesis per instrument)

| class | predicate (pure, after `_to_float` coercion) | severity |
| --- | --- | --- |
| `ordering` | NOT bear<=base<=bull over non-null pairs | violation |
| `zone_inverted` | zone_low > zone_high (both non-null) | violation |
| `zoneless_buy` | stance == "buy" AND both zones NULL AND the run's `price_anchor.available` is True — the writer prompt DOCUMENTS null zones when no anchor (`app/services/thesis.py:1012`), so anchor-less buys are exempt (counted `zoneless_buy_no_anchor`, info) | violation |
| `base_far_from_close` | close AT THE ANCHOR DATE (`price_daily.close` at max `price_date <= price_anchor.as_of`) present AND \|base/close − 1\| > 0.60 — write-time sanity vs the price the writer SAW; latest-close drift is #2010's staleness concern, not DQ (Codex ckpt-1 HIGH) | flag |
| `claim_lint` | memo claims a block unavailable (regex per block keyword) while the run's `context_summary.blocks[b].available` is True | candidate |
| `stale_price_anchor` | run summary `price_anchor.as_of` more than 7d older than thesis `created_at` (wrote on a stale anchor) | flag |
| `target_abstention` | bear, base, bull ALL NULL | info count |

`claim_lint` scope: ALL summarized blocks — the writer prompt's availability rule applies to
every block's status fields (`app/services/thesis.py:1024`), so the keyword map covers
{news, filings, earnings_history, analyst_estimates, fundamentals, valuation,
fair_value_band, risk_metrics, ta_state, price_anchor, analytics_evidence} with per-block
keywords (e.g. "fair value band", "risk metrics", "technical"). Regex per block — tuned on
the FULL dev population (82 → 28 → 8 findings across three rounds of snippet audits):
negation-first `(?i)\b(no|missing|unavailable|not available|lack of)\s+(\w+\s+){0,2}<keyword>`
(word-bounded — bare windows matched "no" inside "noted"/"not supported"; ≤2-word gap kills
cross-clause "unavailable due to a stale `<keyword>`"), or keyword-first
`<keyword>[^.,;\n]{0,30}\b(unavailable|not available|missing|non-existent)` (clause-bounded —
never crosses `, ; .`). Candidate severity — residual qualitative FPs ("no strong
fundamentals") tolerated by posture. Theses without a usable summary skip claim_lint +
stale_price_anchor + the anchor-gated predicates (counted via `no_run` / `no_context_summary`).

## Shape

1. `app/services/thesis_dq_audit.py` — pure predicate functions (table-testable, no DB) +
   `compute_thesis_dq_report(conn) -> ThesisDqReport` (frozen dataclasses): one read joining
   latest thesis per instrument ← latest matching `thesis_runs` row (by `thesis_id`) ← latest
   `price_daily` close; per-class counts + bounded per-violation rows (symbol, thesis_id,
   class, detail; cap 200 rows like `compute_cik_gap_report`'s bounded payload).
2. `GET /theses/dq-audit` on the existing authed `theses` router (compute-on-read, mirrors
   `/coverage/manifest-parsers` pattern). Pydantic response mirror in `frontend/src/api/types.ts`
   NOT needed (no FE consumer in v1 — manifest/cik audits are likewise API-only; the admin "DQ
   panel" for these audits does not exist yet — explicitly out of scope, matches precedent).
3. Nightly `ScheduledJob` `thesis_dq_audit` (source `db`, 05:10 UTC, `catch_up_on_boot=False`,
   gated `_bootstrap_complete`): runs the compute, logs one WARN per non-zero class,
   `job_runs.row_count` = total violations (ops health reads job_runs; /system/jobs shows it).
   Standard triangle + registry-shape test entries.

No schema change. No FE change. No model_version/scoring impact (read-only over stored rows).

## Baseline (dev, run at PR time)

2026-07-11 manual scan: 27 theses — 1 ordering, 2 zoneless buys, 20 target-abstentions.
Stored rows have since evolved (#2007 shipped 07-12; regens). PR records the day-one numbers
from `compute_thesis_dq_report` on dev and reconciles against those classes. Baseline is a
PR-time reference for the dev population ONLY — predicate safety rests on the pure
table-tests + the documented rules above, not on this count (Codex ckpt-1).

## Tests

- Pure predicate table-tests per class (incl. NaN/None coercion edges, no-run-summary skip).
- One db-tier test driving the REAL query into the real reader (projection-gap class,
  prevention-log #2021 lesson).

## Out of scope

- Auto-regen / staleness-vs-now (belongs to #2010 re-thesis path).
- FE panel (no admin DQ panel exists for the audit family; API-only like siblings).
- Rebuilding context per block for "current" availability.
