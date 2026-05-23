# docs/specs/ — live specifications

Topic-named, undated specs for shipped + load-bearing systems. Read these when you need to know **what we currently do**.

For **what we plan to do**, see `docs/proposals/`.
For **decisions** (architectural choices, supersession history), see `docs/adr/`.
For **historical receipts** (shipped plans, abandoned designs), see `docs/_archive/`.
For **conventions** (naming, lifecycle, what-goes-where), see `docs/wiki/spec-conventions.md`.

## Areas

| Area | Purpose | Status |
| --- | --- | --- |
| `etl/` | Per-source ETL contracts (SEC, FINRA, OpenFIGI, Frankfurter, eToro, etc.) | 9 specs landed; sink registry + per-source matrix to expand |
| `bootstrap/` | First-install bootstrap orchestrator + capability gates | 6 specs landed |
| `orchestrator/` | Job orchestration: lane caps, family split, drain fairness, inner-lock removal | 3 specs landed |
| `fund-data/` | Fund-specific ingest (N-CSR metadata, N-CSR drain, N-PORT) | 3 specs landed |
| `infra/` | Postgres tuning, max-locks guards, partition extension | 1 spec landed |
| `sinks/` | Multi-writer sink registry (`filing_events`, `financial_facts_raw`, `ownership_*_observations`, etc.) | **PENDING** |
| `derived/` | Derived sinks (`financial_periods`, `instrument_business_summary`, `ownership_*_current`, `report_snapshots`) | **PENDING** |
| `enrichment/` | Cross-source enrichment (OpenFIGI CUSIP resolver, ticker→CIK lookup) | **PENDING** |
| `ownership/`, `xbrl/`, `frontend/`, `admin/`, `operator/`, `testing/` | Placeholders for future canonical specs | **PENDING** |

Empty area dirs are **intentional**: they signal "spec coverage planned here."

## Spec template (pending)

The canonical 22-section spec template will land at `docs/specs/etl/TEMPLATE.md` in a subsequent PR (per the v3 committee-review consolidated findings memo). Until then, refer to that memo in Claude memory for the agreed shape.

## What goes in specs vs proposals

- **`specs/`**: describes a currently-implemented contract. Reader expects code-grounded file:line citations. Updated in-place as code changes.
- **`proposals/`**: describes intended future work or unshipped designs. Reader expects "this is the plan" framing. Moved to specs/ when shipped + canonical.

## Lifecycle

1. Draft proposal at `docs/proposals/<area>/<topic>.md`.
2. Work ships → file moves to `docs/specs/<area>/<topic>.md`; content rewritten to current-state.
3. Spec changes in-place as code evolves (PRs that change pipeline code SHOULD update the spec in the same diff).
4. Spec abandoned/superseded → original moves to `_archive/<yyyy-mm>/superseded-<name>.md`; new spec at same path.

See [docs/wiki/spec-conventions.md](../wiki/spec-conventions.md) for full conventions.

## Historical note

Before 2026-05-23, specs lived at `docs/_archive/` with dated filenames. That layout accumulated 89 specs (mix of shipped, superseded, live, abandoned) and was reorganised in a single PR. Historical specs are in `docs/_archive/<yyyy-mm>/`. Newer specs follow the undated-topic-name convention.
