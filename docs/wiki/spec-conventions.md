# Doc conventions — where things live

**Status:** active 2026-05-23.

## Top-level layout

```
docs/
  wiki/           live truth. Architecture, conventions, runbooks, glossary, data-sources.
                  Never dated. Always current. The reader's first stop.

  specs/          LIVE specs for shipped + load-bearing systems. Topic-named, NEVER dated.
                  One spec per (source, endpoint, sink) triple or per derived sink.
                  Section structure follows docs/specs/README.md (when written).

  proposals/      Future work + design proposals that haven't landed yet.
                  Topic-named, NEVER dated. Move to specs/ once shipped + canonical.
                  Move to _archive/ if abandoned.

  adr/            Decision log. Numbered NNNN-title.md. Append-only. Never moved or edited
                  after acceptance. New decision → new ADR; superseded ADRs gain "Superseded
                  by ADR-NNNN" header.

  _archive/       Historical record. Dated work-receipts that are no longer load-bearing.
                  Organised by yyyy-mm/. Never read for current state; only for forensics.

  research/       Sample data + ad-hoc exploration outputs. Not promoted to specs.
```

## Naming rules

**LIVE files** (`wiki/`, `specs/`, `proposals/`, `adr/`):
- Topic-named, NEVER dated.
- Lowercase + hyphenated: `n-csr-metadata.md`, `bootstrap-orchestration.md`.
- ADRs use `NNNN-title.md` numbered sequence.
- One file per topic. v2 / v3 / "-design" / "-plan" suffixes are FORBIDDEN in live dirs.

**Archived files** (`_archive/`):
- Keep original dated names. They're historical receipts.
- `superseded-<original>.md` prefix when archiving an older version.
- `_archive/stale/` for designs that were never executed.

## Workflow

### New work
1. **Brainstorm / draft** → write proposal at `docs/proposals/<area>/<topic>.md`. No date in filename.
2. **Spec finalised** → if it stays under active maintenance, KEEP it at `proposals/` until shipped; or move to `specs/` if it describes a canonical contract going forward.
3. **Work ships** → move proposal to `specs/<area>/<topic>.md` (rename if needed to drop "-design" / "-plan" suffix). Update content to describe **current state**, not "the plan".
4. **Work abandoned** → move to `_archive/stale/`. Don't delete (history is useful).

### Doc evolution
- **Spec changes** → edit in place. NEVER create v2/v3 alongside.
- **Substantial overhaul** → archive original to `_archive/<yyyy-mm>/superseded-<name>.md`, then create the replacement at the same `specs/<area>/<topic>.md` path.
- **Decisions** → new ADR. Reference + supersede prior ADRs explicitly.

### Plans
- **Plans are session work-receipts, NOT long-lived docs.**
- Plans live in `~/.claude/projects/<slug>/memory/` (Claude memory dir), not in repo.
- When a plan ships, its substance becomes a spec or wiki page.
- When a plan is abandoned, its substance lives in memory or `_archive/stale/`.
- The repo does NOT carry dated plan files going forward.

### Handoff prompts
- Live in Claude memory dir: `~/.claude/projects/<slug>/memory/project_*_next_session_prompt.md`.
- NOT in repo. Future sessions read from memory.
- This is intentional — handoffs are session-local, not repo-local.

## Areas under `specs/` (current)

| Area | What it contains |
|------|------------------|
| `etl/` | Per-source / per-endpoint ETL contracts (SEC, FINRA, OpenFIGI, Frankfurter, etoro candle, etc.) |
| `bootstrap/` | First-install bootstrap orchestrator, capability layer, precondition gates, atomic enqueue |
| `orchestrator/` | Job orchestration: lane caps, family split, drain fairness, inner-lock removal |
| `fund-data/` | Fund-specific ingest (N-CSR, N-PORT) |
| `infra/` | Postgres tuning, max-locks guards, partition extension, etc. |
| `sinks/` | Sink-table contracts (multi-writer registry): `filing_events`, `financial_facts_raw`, `ownership_*_observations`, etc. (NOT YET POPULATED — see committee review findings.) |
| `derived/` | Derived sinks: `financial_periods`, `instrument_business_summary`, `ownership_*_current`, `report_snapshots`. (NOT YET POPULATED.) |
| `enrichment/` | Cross-source enrichment stages: OpenFIGI CUSIP resolver, ticker→CIK lookup. (NOT YET POPULATED.) |
| `ownership/`, `xbrl/`, `frontend/`, `admin/`, `operator/`, `testing/` | Reserved for future canonical specs as the system grows. |

Empty area dirs are **intentional** — they signal "spec coverage planned here." Removing the dir means we don't intend to spec the area.

## Spec template

Each spec under `specs/etl/<source>/<endpoint>.md` follows the 22-section template defined in (pending) `docs/specs/etl/TEMPLATE.md`. Until that lands, refer to `~/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_v3_consolidated_findings.md` for the template definition committee converged on.

## What NOT to do

- Don't add dated files anywhere except `_archive/`.
- Don't create v2 / v3 / "-design" / "-plan" suffix files in live dirs.
- Don't put plans in the repo. Plans are session ephemera; only their distilled substance (specs / wiki / ADR) lands in repo.
- Don't put handoff prompts in the repo. They live in Claude memory dir.
- Don't read `_archive/` for current state. It's forensic-only.

## Migration notes (2026-05-23)

This convention was adopted on 2026-05-23 after accumulating 52 dated plans + 89 dated specs in `docs/superpowers/{plans,specs}/`. The reorganisation:

- Moved 35 SHIPPED files (20 plans + 15 specs) + 5 SHIPPED spikes to `_archive/<yyyy-mm>/`.
- Moved 7 SUPERSEDED specs + 3 SUPERSEDED plans to `_archive/<yyyy-mm>/superseded-*`.
- Moved 23 STALE plans (abandoned / never executed) to `_archive/stale/`.
- Deleted 1 STALE spec (sec-incremental-fetch — superseded by ETL coverage model).
- Deleted 1 stale `next-task-prompt.md` (referenced a PR shipped 2026-05-18).
- Promoted 22 LIVE-SPEC files to `docs/specs/<area>/<topic>.md` (undated names).
- Promoted 44 LIVE-DESIGN files to `docs/proposals/<area>/<topic>.md` (undated names).
- Removed empty `docs/superpowers/` + `docs/internal/` + `docs/tickets/` dirs.

Result: 23 live specs, 49 active proposals, 4 ADRs, 15 wiki pages, 74 archived artifacts. Clean separation of "now" (specs/wiki/proposals) and "history" (_archive).
