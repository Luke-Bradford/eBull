# eBull operator wiki

Operator-facing reference for running eBull. Plain English, minimal
jargon, cross-linked to authoritative source docs where rules live.

**Audience.** Operator running eBull (the human deciding which
trades land, monitoring data freshness, responding to alerts).
Not the contributor writing code — for that, see `.claude/CLAUDE.md`
+ `docs/settled-decisions.md`.

**Status.** Living documentation. Update when the operator's
day-to-day workflow changes. Do not duplicate code-level rules —
link to the authoritative source.

## Contents

### Get started
- [`getting-started.md`](getting-started.md) — environment, .env,
  dev stack, first-run sequence.

### Concepts
- [`architecture.md`](architecture.md) — process topology
  (jobs vs API), data plane, broker boundary.
- [`data-sources.md`](data-sources.md) — eToro / SEC EDGAR / FINRA
  / Companies House. Coverage, cadence, rate limits.
- [`ownership-card.md`](ownership-card.md) — what the ownership
  rollup shows, where each slice comes from, what gates them.

### Day-to-day
- [`runbooks/runbook-after-parser-change.md`](runbooks/runbook-after-parser-change.md)
  — what to do after a PR lands that changes parser semantics.
- [`runbooks/runbook-cancel-and-resume.md`](runbooks/runbook-cancel-and-resume.md)
  — when to use Iterate / Full-wash / Cancel; cooperative-cancel state
  machine; resume from watermark.
- [`runbooks/runbook-data-freshness.md`](runbooks/runbook-data-freshness.md)
  — verifying ingest cadence + spotting stale data.
- [`runbooks/runbook-job-failures.md`](runbooks/runbook-job-failures.md)
  — diagnosing a failed scheduled job.
- [`runbooks/runbook-stuck-process-triage.md`](runbooks/runbook-stuck-process-triage.md)
  — four-case stale model; heartbeat + thresholds; jobs-process
  restart; missing-CIK diagnosis chain.

### Reference
- [`glossary.md`](glossary.md) — CIK, CUSIP, 13F-HR, NPORT-P,
  settlement_date, days_to_cover, and other domain terms in plain
  English.

## Source-of-truth pointers

| Question | Authoritative file |
|---|---|
| "What workflow rules apply when contributing?" | `.claude/CLAUDE.md` |
| "What design decisions are settled?" | `docs/settled-decisions.md` |
| "What recurring mistakes have we caught?" | `docs/review-prevention-log.md` |
| "What does this epic's design look like?" | `docs/superpowers/specs/README.md` |
| "What third-party libraries do we use?" | `THIRD_PARTY_NOTICES.md` |
| "What happened in this PR?" | `git log --oneline` + the PR body |
