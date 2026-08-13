# docs/proposals/ — future work + unshipped designs

Topic-named, undated proposals. Read these when you want to know **what we plan to do** or what's been designed but not yet shipped.

For **current state**, see [`docs/specs/`](../specs/).
For **decisions** (architecture + supersession), see [`docs/adr/`](../adr/).
For **historical receipts**, see [`docs/_archive/`](../_archive/).
For **conventions**, see [`docs/wiki/spec-conventions.md`](../wiki/spec-conventions.md).

## Areas

| Area | Purpose |
| --- | --- |
| `etl/` | ETL designs not yet shipped (e.g. cap policies, ownership decomposition, retention designs, optimisation rollouts) |
| `ui/` | Frontend redesigns + admin console proposals + alert designs |
| `infra/` | Test infrastructure, job-runtime refactors, CI improvements |
| `operator/` | Operator-facing workflows (first-run setup, multi-operator, etc.) |
| `future/` | Long-horizon ideas not on near-term roadmap |

## Lifecycle

1. Author at `docs/proposals/<area>/<topic>.md`. No date in filename.
2. When work ships, MOVE (don't copy) to `docs/specs/<area>/<topic>.md` and rewrite content to current-state.
3. When work is abandoned, MOVE to `docs/_archive/stale/`.

## Naming

Topic-named, undated, kebab-case: `instrument-detail-density-grid.md`, `n-port-edgartools.md`. NEVER add v2/v3/-design/-plan suffixes.

## What goes here vs ADR

- **proposals/**: detailed design or planning doc. Multi-paragraph. Implementation-shaped.
- **adr/**: short, numbered, single-decision record. "We chose X over Y because Z."

Use both when needed: ADR records the decision; proposal records the design that flows from it.
