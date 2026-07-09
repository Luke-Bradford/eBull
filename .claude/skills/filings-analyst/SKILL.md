---
name: filings-analyst
description: eBull SEC filings-analysis layer — the red-flag risk scorer, the per-form manifest parsers, and how filing signals reach scoring/portfolio; routes raw ingest + storage to the data-engineer and data-sources skills.
---

# filings-analyst

## When to use

Any change to the filings-analysis signal path: `app/services/filings_risk.py`
(red-flag scoring), the per-form parsers under `app/services/manifest_parsers/`,
`app/services/filings.py` (form allow-list + metadata ingest), the `/filings/*`
endpoints (`app/api/filings.py`), or how a filing signal reaches scoring/portfolio.
For raw SEC fetch/format/identifier mechanics read
`.claude/skills/data-sources/sec-edgar.md`; for the edgartools library
`.claude/skills/data-sources/edgartools.md`; for schema/storage/manifest invariants
`.claude/skills/data-engineer/SKILL.md`. This skill ROUTES — it does not duplicate
that ground.

## What it is

The layer that turns ingested filings into analysable signals.

**Ingest + allow-list** — `app/services/filings.py::refresh_filings` writes filing
metadata to `filing_events` (sql/001_init.sql). The #1011 three-tier form
allow-list (`SEC_PARSE_AND_RAW` / `SEC_METADATA_ONLY` / default SKIP, union
`SEC_INGEST_KEEP_FORMS`) bounds which forms land.

**Per-form parsers** — `app/services/manifest_parsers/`, each registered via
`register_parser`, drained by `app/jobs/sec_manifest_worker.py::run_manifest_worker`
(rate-limited): `eight_k`, `def14a` (exec-comp → `def14a_exec_compensation` sql/215;
beneficial holdings sql/097), `sec_10k`, `sec_10q` (synth no-op #1168), `sec_13dg`
(13D+13G), `insider_345` (form3/4/5), `sec_13f_hr`, `sec_n_csr`, `sec_n_port`,
`sec_nt` (NT late-filing #1015), `sec_424b` (prospectus offerings →
`prospectus_offerings` sql/216, B2 volume-gate sql/217 #1975), `sec_pre14a`.
Re-ingest after a parser change: `POST /jobs/sec_rebuild/run`
(`app/jobs/sec_rebuild.py`).

**Red-flag scorer** — `filings_risk.py::score_filing_red_flag` derives
`filing_events.red_flag_score` (NUMERIC(10,4), sql/001:53). Only genuine high red
flags are written: critical 8-K item (`CRITICAL_8K_SCORE = 1.0`, severity from
`sec_8k_item_codes` sql/053) and Form NT late filing (`NT_LATE_FILING_SCORE = 0.7`,
SEC Rule 12b-25). Everything else stays NULL. Spec:
`docs/specs/filings/2026-06-27-1748-filings-risk-scorer.md`.

**Downstream signal path:**
- Scoring turnaround family — `scoring.py::_turnaround_score` blends
  `1.0 - clip(avg_red_flag_score)` at weight 0.30; missing → 0.5 neutral. The
  `high_red_flag` penalty fires when avg > `_RED_FLAG_PENALTY_THRESHOLD` (0.60).
- Portfolio EXIT guard — `portfolio.py` exits when MAX recent (90d) `red_flag_score`
  >= `EXIT_RED_FLAG_THRESHOLD` (0.80).
- Eligibility — `coverage.py` Section 2 sets `coverage.filings_status = 'analysable'`
  (US domestic issuer, ≥2 10-Ks in 3y); scoring and `_load_ranked_scores` gate on it.

**Endpoints** (`app/api/filings.py`, prefix `/filings`): `GET /{instrument_id}`
(list), `/{instrument_id}/quarterly-counts` (#592 density), `/{instrument_id}/red-flag-trend`
(#1748). Parsed 424B offering rides `FilingItem.offering`. Exec-comp:
`GET /instruments/{symbol}/exec-compensation` (`app/api/instruments.py`).
Filing/coverage alerts: `app/api/alerts.py`, `app/api/coverage.py`.

**Planned (not shipped):** tender / going-private parser (SC TO-T/TO-I/14D9/13E3 →
`tender_offer_events`) — spec `docs/specs/filings/2026-07-05-tender-going-private-parser.md`;
no table/parser in the tree yet. Deep filing-text thesis analysis is build-priority
#3→#4 (CLAUDE.md); LLM memo work lives in the `thesis-writer` / `thesis-critic` skills.

## Invariants

- **Filing and fundamentals storage** (settled-decisions.md): `filing_events` stores
  metadata + extracted summary + risk score + provider payload + canonical link; full
  raw text is out of scope for v1 — separate table (`filing_raw_documents` sql/107) if
  ever needed. Filing dedupe is provider-scoped, stable, idempotent.
- **Filing lookup rule** (settled-decisions.md): never key filing lookup on `symbol` —
  SEC uses CIK, Companies House uses `company_number`.
- **Raw-payload retention (#1617)** (settled-decisions.md): every
  `raw_filings.DocumentKind` falls in exactly one of re-read / housekept-and-negligible
  / kept-and-negligible; CI-enforced.
- Every filing signal that reaches a trade path must be auditable and deterministic
  (repo non-negotiables) — no ML in the scoring consumption; red-flag inputs are fixed
  by SEC reg (8-K item severity, Rule 12b-25), not tuned.

## Failure conditions

Missing critical source data, stale filings, or contradictory evidence must surface as
explicit signals — never a papered-over neutral default:
- A benign filing is left `red_flag_score = NULL`, never a low score — a non-null value
  below 0.5 would *reward* the instrument and dilute a lone critical (avg stays 1.0).
  This is the core red-flag invariant (`filings_risk.py` docstring).
- Staleness surfaces as `coverage.filings_status` regressing off `analysable`, which
  drops the name from scoring eligibility — not as a silently-scored stale row.
- Data absence surfaces via the ranking completeness tier / missing-critical penalty
  (owned by `ranking-engine`), never a filled-in guess.
