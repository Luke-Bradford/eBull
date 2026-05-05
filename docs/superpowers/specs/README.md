# Superpowers specs — index

This directory holds epic-level design documents. Specs are
authored before code, reviewed by Codex, and live as long as the
epic they describe is active. **Specs are not living documentation
once the epic ships** — they are point-in-time design records.

## How to read this directory

- Filenames are `YYYY-MM-DD-<topic>.md`. The date is the spec's
  authored date, not the epic's ship date.
- A merged epic does not delete its spec. The spec stays as a record
  of what was designed; subsequent changes are recorded in the code,
  in `docs/settled-decisions.md`, or in newer specs.
- For a spec that supersedes an earlier one, the newer spec
  cross-links back.

## Live specs (decisions still pending)

- `2026-05-04-etl-coverage-model.md` — sec_filing_manifest +
  data_freshness_index + 3-tier polling. #863-#873.
- `2026-05-04-ownership-full-decomposition-design.md` — full
  ownership decomposition redesign post-AAPL audit. Phase 0+1
  shipped (#836-#840); Phase 2+ pending.
- `2026-05-05-pytest-perf-redesign.md` — xdist + per-worker DB
  template. Shipped #893; record kept for future xdist tuning.

## Recently shipped (4-6 weeks)

These are recent enough that the spec may still be useful as
context. Older specs at the bottom of this list are pure history.

- `2026-05-03-ownership-tier0-and-cik-history-design.md` — #788
  Chain 2.x.
- `2026-04-27-instrument-charts-quant-redesign-design.md` — #585
  Phase 1.
- `2026-04-27-instrument-detail-*-design.md` — #585 Phase 1.5 +
  density grid.
- `2026-04-30-jobs-out-of-process-design.md` — #719 process
  topology (now in settled-decisions).

## Older specs (pure history)

Everything pre-2026-04-27 is in this directory by author date but no
longer load-bearing for live work. Reference for archaeology only.

## When to add a new spec

- Epic touches schema/migrations + service layer + frontend.
- Multiple sub-tickets that need a shared design.
- Trade-off space large enough to warrant Codex review of the
  design before implementation.

## When NOT to add a spec

- Single-PR features.
- Bug fixes.
- Refactors that preserve external surface.
- Anything where the design fits in a ticket body.

For settled cross-cutting decisions (provider strategy, identifier
strategy, filing dedupe semantics): use `docs/settled-decisions.md`
instead — that file is the live source of truth.
