# docs/_archive/ — historical record

Forensic-only. Read these to understand HOW WE GOT HERE. Do NOT read for current state.

For current state, see [`docs/specs/`](../specs/), [`docs/wiki/`](../wiki/), [`docs/proposals/`](../proposals/), [`docs/adr/`](../adr/).

## Layout

```
_archive/
  2026-04/     Shipped specs + plans authored in April 2026
  2026-05/     Shipped specs + plans authored in May 2026
  stale/       Designs that were never executed / abandoned direction
```

## Naming

- Files in `<yyyy-mm>/` keep their original dated names (work-receipts; date is meaningful).
- Files prefixed `superseded-` are older versions of multi-version specs (e.g. `superseded-etl-rollout-v1.md`).
- Files in `stale/` are abandoned designs; keep original names.

## Why archive instead of delete

History is useful when:
- Investigating "why did we do it this way" (e.g. reviewing an old shipped PR's design rationale).
- Understanding the evolution of a system (v1 → v2 → v3 of a spec; which assumptions changed).
- Audit / compliance / forensic post-mortems.

## Adding to archive

When promoting work out of `docs/proposals/` or `docs/specs/` because it shipped or was abandoned, move (not copy) the original file here. Preserves git history via `git mv`.
