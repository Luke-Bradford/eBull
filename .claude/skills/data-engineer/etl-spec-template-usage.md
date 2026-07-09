---
name: etl-spec-template-usage
description: Use when authoring a new per-source ETL spec (SEC EDGAR endpoint, FINRA CDN feed, OpenFIGI mapping path, eToro REST/WebSocket consumer, or any new data source), when editing an existing spec under docs/specs/etl/, or when the user references the "22-section spec template". Documents the canonical spec template, per-section authoring discipline, common foot-guns, Index Budget grandfathering, and the rationale log enforcement contract. Companion to (planned) docs/specs/etl/TEMPLATE.md.
---

# ETL spec template usage

## When to use

Read this skill before writing the first paragraph of any new ETL spec. Also read it before merging an edit to an existing spec — many template sections drift between authors and the consistency is load-bearing for reviewers (a per-source spec that omits §8 "Multi-writer sink" silently breaks the cross-writer conflict-key contract).

Skip this skill for:

- One-line fixes to an existing spec.
- ADR / settled-decision authoring (those use `docs/adr/` + `docs/settled-decisions.md`, not the ETL template).
- Implementation-only PRs that touch code but not the spec.

## What "spec" means here

Per the post-2f8894a reorg, plans + specs live under `docs/specs/<area>/<topic>.md` (live spec — describes what's in production) or `docs/proposals/<area>/<topic>.md` (unshipped plan — describes what's proposed). ETL specs land under `docs/specs/etl/` or `docs/proposals/etl/`. The 22-section template applies to BOTH.

A spec is the load-bearing contract a reviewer reads to answer: "if I let this PR merge, will the operator's data still be correct two weeks from now?" If the spec doesn't answer that, the bot review + Codex review can't catch the gap.

## Pre-write checklist — §0.0 grep proof MUST land in spec body

Before writing the first sentence of §1, complete `data-engineer/SKILL.md §0.0` (the before-spec gate). Then **embed the grep outputs in the spec itself** — under a new `## §0 Grep proof` heading at the top of the spec OR inline in §13 where the cited cap / table / writer first appears. The committee-review and Codex rounds depend on the spec being self-evidencing; "I ran the grep, trust me" is not auditable.

Minimum content for §0 (or inline equivalent):

```markdown
## §0 Grep proof

> Generated <YYYY-MM-DD> against branch <name> @ <SHA>. Outputs reproduced verbatim from the commands below; do NOT paraphrase.

### Cap vocabulary (cited in §13)
$ grep -n "Capability = Literal\[" app/services/bootstrap_orchestrator.py
261:Capability = Literal[
...
$ grep -n '"<cap_name>"' app/services/bootstrap_orchestrator.py
<paste lines>

### Sink writers (cited in §8)
$ grep -rn "INSERT INTO <sink_table>" --include="*.py" --include="*.sql"
<paste lines — one row per writer>

### Reader fan-out (cited in §2)
$ grep -rn "FROM <sink_table>\|JOIN <sink_table>" --include="*.py" -A3 | grep instrument_id
<paste lines>

### PK + FK shape (cited in §4)
$ grep -n "PRIMARY KEY\|REFERENCES" sql/<NNN>_<topic>.sql
<paste lines>
```

**Why inline, not external:**

- Skills + memory rot. A spec that cites "ran §0.0" with no embedded proof is unverifiable two weeks later when the cap vocabulary has drifted.
- Codex / committee lenses can grep-verify the embedded outputs against the current tree in one pass. External proof requires reviewers to re-run the gate themselves — they don't.
- Stream A v1 (2026-05-24) cited the skill but skipped this step. Four hallucinated cap names + wrong PK column + wrong stage number shipped to committee. The §0 inline-proof block is the cheapest enforcement that closes the loop.

**Acceptance:** a v2 spec that strengthens cap requirements, introduces a new sink, or relaxes a PK/UNIQUE MUST include the §0 block. Specs that only adjust runbook prose / open questions MAY omit it. When in doubt, include it.

## The 22-section template (target state)

Numbered. Required unless marked OPTIONAL. Empty sections must say "N/A — <reason>" rather than be silently dropped.

| # | Section | Required? | One-line purpose |
|---|---|---|---|
| 1 | Decisions | ✅ | The 3-7 sentences that survive into operator memory if this spec is the only thing read |
| 2 | Identifiers + identity-drift | ✅ | Which IDs flow (CIK, CUSIP, FIGI, etc.) + how identity drift (CIK reassignment, CUSIP retirement, symbol reuse) is detected and handled |
| 3 | Endpoint surface | ✅ | Per-URL `{url, method, body_schema_version, sample_response_fixture_path}` triple. Including pagination contract |
| 4 | Schema | ✅ | Table DDL excerpt + encoding/precision/NULL/timezone discipline per column. Index list + Index Budget grandfathering call-out |
| 5 | Fetch strategy + rate-limit composition | ✅ | One of `{bulk_archive, per_resource_http, batched_http, atom_feed, push, cache, derive}` per fetcher. If multiple, declare composition rules across the shared rate budget |
| 6 | Conditional-GET semantics | ✅ | `If-Modified-Since` / `If-None-Match` / 304 handling per resource. Distinguish 304 → skip vs update-freshness vs force-rewrite |
| 7 | Retry posture per error-class | ✅ | Per-HTTP-status mapping: 403/404/429/5xx → {transient retry / permanent tombstone / not-found}. Per-source (SEC vs FINRA) — 403 ≠ permanent for both |
| 8 | Multi-writer sink registry | ✅ | If this spec adds a writer to a multi-writer sink (`filing_events`, `sec_filing_manifest`, `unresolved_13f_cusips`, `external_identifiers`, any `ownership_*_observations`) — declare the conflict key + reference the sink registry doc |
| 9 | Watermark + retry-budget | ✅ | `last_known_*` storage shape. `next_retry_at` semantics. Idempotency contract under retry |
| 10 | Encoding / precision / NULL / timezone | ✅ | UTF-8 vs Latin-1, Decimal vs float, JSONB null vs SQL NULL, UTC vs ET vs filing-source TZ |
| 11 | Backfill horizon + retention | ✅ | Days/quarters of history. Storage budget calculation. Family-level retention if applicable (10-K + 10-K/A share the ANNUAL family) |
| 12 | Partition strategy + extension deadline | ✅ | Range/list partitioning. **Extension deadline alarm:** if static partitions end at year YYYY, declare the operator-visible alarm that fires before YYYY |
| 13 | Bootstrap vs steady-state mode | ✅ | Per-mode behaviour. Forbidden-HTTP-in-bootstrap declaration if `fetch_strategy ∈ {atom_feed, push}`. Coverage-floor pattern if applicable |
| 14 | Tombstones + soft-delete | ✅ | Which conditions tombstone. Which conditions soft-delete (`known_to`). NEVER hard-delete observations |
| 15 | `rows_skipped` closed-set + other | ✅ | Closed enum of skip reasons + an `other` catch-all with `partial_data_reason` free-form. Auditing depends on the enum being finite |
| 16 | Schema-evolution migration path | ✅ | Dual-parser window if breaking. Parser-version bump rules. Old-data backfill plan |
| 17 | Operator runbooks | ✅ | Executable: `app/runbooks/<source>_<endpoint>.py` — NOT SQL strings. Includes `--dry-run` for any DELETE. **Path is `app/runbooks/` NOT `app/cli/runbooks/`** — `app/cli.py` already exists as single-file break-glass credential CLI; sibling `app/cli/` package would shadow it (Stream A PR-D / #1311 chose flat layout). |
| 18 | Smoke matrix | ✅ | Per-source panel (default: AAPL, GME, MSFT, JPM, HD for issuer-keyed). For filer-keyed: 1 large fund-family + 1 small + 1 edge case |
| 19 | Cross-source verification | ✅ | One external authoritative source (gurufocus, marketbeat, EdgarTools golden file, SEC direct) + the figure compared |
| 20 | Test placement | ✅ | Which tests are unit / integration / contract / smoke / nightly. Flakiness budget |
| 21 | Rationale log | ✅ | For every NON-OBVIOUS decision: the alternative considered + why rejected. Operator can audit later |
| 22 | Open questions | OPTIONAL | Genuine open questions. Empty if all settled. NOT "todo — fix later" — those go in tracking issues, NOT specs |

The template is dense by design. Run #7 + the v3 committee surfaced ~50 distinct failure classes; sections 5/8/10/12/13 each correspond to a class the team has burned on. Mandating "N/A + reason" for empty sections is the cheapest enforcement — drift is much louder than absence.

## Section authoring guidance

### §1 Decisions

Lead with what changes operationally. Three to seven sentences. Not "we will implement Y" but "operator sees figure X via path Y; cron Z fires at cadence W; rollback by Q". The reviewer should be able to summarize the change to the user without reading §2+. If you can't, the spec isn't ready.

### §3 Endpoint surface

Each URL gets a `{url, method, body_schema_version, sample_response_fixture_path}` triple. The `sample_response_fixture_path` is REQUIRED — point at `tests/fixtures/<source>/<endpoint>.{json,xml,csv}`. If no fixture exists yet, the spec is missing a deliverable; either add the fixture in the same PR or flag as a §22 open question.

Pagination shape: explicit. `cursor` vs `offset` vs `page`. Edge cases on the last page. Empty-array vs missing-key shape.

### §4 Schema + Index Budget

DDL excerpt (the new columns / table only — not the whole file). Per-column: type, NOT NULL, DEFAULT, CHECK, GENERATED. Encoding for string columns if not UTF-8. Decimal precision for numeric columns.

Index Budget: every new table has a **hard cap of 4 indexes** (PK + at most 3 secondary). Grandfathered tables exceeding the cap MUST declare it in the spec ("`sec_filing_manifest` has 5 indexes; grandfathered per #863"). When ADDING an index to an existing table, the spec MUST count the current indexes and either justify going over 4 OR drop a less-load-bearing one.

### §8 Multi-writer sink registry

If the spec writes to any sink with ≥ 2 writers (`filing_events`, `sec_filing_manifest`, `unresolved_13f_cusips`, `external_identifiers WHERE provider='openfigi'`, any `ownership_*_observations`), it MUST:

1. List every existing writer to that sink (the sink registry doc — `docs/specs/etl/sinks/<table>.md` — is the authoritative source; if it doesn't exist yet, GREP for `INSERT INTO <table>` + `MERGE INTO <table>` across `app/`).
2. Declare the conflict key (PK or partial UNIQUE INDEX).
3. State how this writer's `parser_version` / `source_accession` / `filed_at` semantics align with existing writers.
4. State retention horizon ALIGNMENT with existing writers — if writer A keeps 10y and writer B keeps 2y, the sink's effective retention is the MAX, not the MIN. Specify whose retention sweep is authoritative.

Skipping §8 is how `filing_events` ends up with 4 writers and 4 different conflict resolutions.

### §12 Partition extension deadline alarm

Specs that introduce range-partitioned tables (`ownership_*_observations` quarterly through 2040 per sql/177, `finra_regsho_daily_observations` quarterly through 2035-Q1 per sql/174, etc.) MUST specify when the static window expires + which operator-visible signal fires before then. The skill `data-engineer/SKILL.md` §13.E mentions `GET /system/postgres-health` as the canonical surface — your spec wires into it OR justifies why not.

Concrete shape:

> Static partition window: 2024-Q1 → 2030-Q1 inclusive (25 quarters). Extension deadline: 2030-Q2 (≈ 2030-04-01). Alarm wires into `GET /system/postgres-health` as `partition_extension_due_soon_<table>=True` 90 days before deadline.

### §13 Bootstrap vs steady-state mode

If the source has bootstrap + steady-state forms:

- Steady-state declares fetch_strategy + cadence.
- Bootstrap declares either (a) same job with wider params, (b) same job with bounded params, or (c) a bootstrap-only stage with no steady-state analogue.
- Per [data-engineer/SKILL.md §6.5.15](SKILL.md): bootstrap-mode stages MUST be derivation-only OR claim a carve-out (sources with no bulk archive). Justify the carve-out in this section if claimed.
- **Forbidden-HTTP-in-bootstrap declaration**: state expected HTTP count = 0 (for `bulk_archive`/`cache`/`derive` stages) or bounded (for `per_resource_http` carve-outs with explicit cohort size).

### §17 Operator runbooks are executable

`docs/specs/etl/<source>.md` § 17 should reference `app/runbooks/<source>_<endpoint>.py` — a runnable Python file with click/typer CLI, NOT a Markdown SQL string the operator copy-pastes. SQL strings drift; runnable scripts type-check + can be unit-tested.

**Path is `app/runbooks/` NOT `app/cli/runbooks/`** — Stream A PR-D / #1311 chose the flat layout because `app/cli.py` already exists as the operator break-glass credential CLI (single-file module); adding a sibling `app/cli/` package would shadow it. The exemplars to copy: `app/runbooks/stream_a_run_8_verify.py` (destructive `--apply`), `app/runbooks/stream_a_t13_sidecar_repair.py` (per-CIK repair without re-fetch), `app/runbooks/stream_a_stream_c_gate.py` (read-only acceptance gate with pinned JSON envelope). Shared safety primitives in `app/runbooks/safety.py`: `assert_dev_env`, `assert_dev_db`, `assert_jobs_process_stopped`, `wait_for_jobs_process_started`.

Every runbook with a destructive action (`DELETE`, `UPDATE`, `TRUNCATE`, schema migration) has `--dry-run` as the default and `--apply` to commit. The prevention-log rule that destructive scripts default to `--dry-run` (with `--apply` to commit) applies universally.

### §18 Smoke matrix

The canonical issuer-keyed panel is `AAPL, GME, MSFT, JPM, HD` (per CLAUDE.md ETL clauses §8). For filer-keyed sources (13F filers, N-PORT trusts, blockholder filers), pick:

- 1 large (Vanguard / BlackRock / Berkshire — has thousands of holdings).
- 1 small (a 5-holding micro-fund — exercises the cardinality lower bound).
- 1 edge case (filer with known data quirks — tombstoned, multi-CIK, foreign-domiciled).

The PR description records WHICH panel rows were exercised + the operator-visible figure observed (clauses 8-12).

### §21 Rationale log

For every non-obvious decision in the spec, two lines:

```markdown
**Decision:** <what was chosen>
**Rejected:** <alternative> — <reason>
```

If you wrote "we chose X" without a paired "we rejected Y", §21 is incomplete. Codex catches the missing rationale in pre-spec review; the bot catches it in post-PR review; the operator catches it 6 months later when "why did we pick X?" has no answer in the repo.

## Cross-spec dependencies

Specs often reference each other (the OpenFIGI sweep spec cites the CUSIP-resolver spec; the FINRA RegSHO spec cites the bimonthly spec's shared throttle). When the dependency is load-bearing:

- Link with `[<topic>](../specs/etl/<topic>.md)` not just prose.
- State explicitly what the dependent spec must have shipped first.
- If the dependent spec hasn't shipped, the consuming spec is in `docs/proposals/etl/` not `docs/specs/etl/`.

## Spec lifecycle

| Stage | Location | Reviewer signals |
|---|---|---|
| Drafting | `docs/proposals/etl/<topic>.md` | User reads + iterates |
| Codex 1 pre-approval | (same path) | `codex.cmd exec` review pass |
| Approved | (same path; user signs off) | Ready to implement |
| Shipping | (same path during PR; updated alongside code) | Bot review compares spec to code |
| Live | `docs/specs/etl/<topic>.md` | Source-of-truth for future readers |
| Superseded | `docs/_archive/<YYYY-MM>/<old-topic>.md` | Renamed `superseded-<original>.md` |

When a spec is superseded, the new spec MUST link the archived one + state what changed. The reverse is also required — append a "Superseded by `<new-path>` <YYYY-MM-DD>" line to the archived spec's top.

## Common foot-guns

| Pattern | What goes wrong | Fix |
|---|---|---|
| Spec cites `<table>.cik` column that doesn't exist (v3 hit this on `financial_facts_raw`) | Codex passes (column name plausible); first PR fails at SQL level | §0.0 grep gate at [data-engineer/SKILL.md §0.0](SKILL.md) — every column name verified pre-Codex |
| Spec invokes API `<module>.<func>` that doesn't exist (v3 hit `master_key.is_bootstrapped()`, `coverage_audit()`) | First import in implementation fails | grep-verify every cited function name |
| Spec asserts numeric projection ("S25 101 min → 5 min") without code-grounded basis | Reviewer accepts at face value; reality differs by 2-3× | If projecting, cite the measured baseline (Run #N receipts) + the load-bearing change that achieves the new number |
| Spec cites file:line that's been re-numbered by a sibling PR | Spec rots between approval and merge | Use SYMBOL references (`grep -n` on function name) over numeric line refs where practical |
| Spec writes §22 "open questions" entry that's actually a punt | Reader sees 1 question, then 2 weeks later finds 5 | If you can't decide between A and B, force the decision in §21 — open-questions are for things genuinely outside scope |

## Cross-references

- [data-engineer/SKILL.md §0.0](SKILL.md) — before-spec gate (grep-verify table shapes).
- [data-engineer/SKILL.md §6.5.16](SKILL.md) — hallucinated-API class of defect.
- [data-engineer/etl-stage-declaration.md](etl-stage-declaration.md) — for the bootstrap-stage subset of any spec.
- [bootstrap-mode-discipline](../engineering/bootstrap-mode-discipline.md) — for §13.
- CLAUDE.md ETL clauses 8-12 — smoke + cross-source + backfill + operator-figure verification contract.
- (PLANNED) `docs/specs/etl/TEMPLATE.md` — the literal copy-paste template. Not yet shipped; bootstrap via this skill until it does.
