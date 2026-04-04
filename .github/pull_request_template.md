## Issue reference

Closes #

## Summary

<!-- One or two sentences. What does this PR do and why? -->

## Changes

<!-- Specific list of what changed. Name files/areas touched. -->

-
-

## Security and audit model

<!-- Explicitly state the security story for this change.
     Does this touch any execution path? If so, confirm the execution guard is called
     and decision_audit is written before any order is staged.
     If no execution-relevant changes, write "No execution path touched." -->

## Testing

<!-- How was this verified? -->

- [ ] Tested locally against Docker PostgreSQL stack
- [ ] New service logic exercised with realistic inputs and edge cases (empty results, API errors)
- [ ] No raw user input reaches the DB without parameterisation

## Checklist

- [ ] Branch named `feature/NNN-short-description` or `fix/NNN-short-description`
- [ ] Raw API payloads persisted before normalisation (where applicable)
- [ ] Score model version / thesis version stamped on any new output rows
- [ ] No order placement path bypasses execution guard
- [ ] `uv run ruff check .` passes
- [ ] `uv run ruff format --check .` passes
- [ ] `uv run pyright` passes
- [ ] CI checks pass
