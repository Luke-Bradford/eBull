# Bash script hygiene

## When to use

Editing or adding any `scripts/*.sh` — especially the awk-/grep-heavy
chokepoint-lint guards (`scripts/check_*.sh`).

## The gate

`scripts/check_shellcheck.sh` runs `shellcheck -S warning` over every
`scripts/*.sh`. Wired into BOTH `.githooks/pre-push` and
`.github/workflows/ci.yml` (#1257); the parity guard
`check_ci_mirrors_prepush.sh` enforces the dual-location mirror.

Run before pushing any shell change:

```bash
bash scripts/check_shellcheck.sh        # whole tree
shellcheck -S warning scripts/my_new.sh # one file
```

## Severity floor = `-S warning`

Gate at errors + warnings, NOT `info`/`style`. The repo has a few
**intentional** note-level patterns that must not be "fixed":

- **SC2086** word-splitting in `check_ci_mirrors_prepush.sh`'s
  `printf '  - %s\n' $hook_only` — the splitting is the point (one
  line per drifted lint).
- **SC1003** single-quote-escape notes inside `printf` format strings.

## Bug classes the gate actually catches

- **SC2261** — two competing `2>/dev/null` on one command silently
  clobber each other. The `find … 2>/dev/null -exec grep … \; 2>/dev/null`
  idiom is the canonical offender; rewrite as `grep -r … --include='*.py'`
  with a single trailing redirect (#1257 fixed this in
  `check_13f_hr_retention.sh` + `check_nport_retention.sh`).
- **SC2034** — a variable assigned then immediately overwritten by the
  next line (dead assignment). PR #1255 shipped one of these; SC2034
  would have caught it pre-push (the bug that prompted #1257).
- **SC2046** — unquoted command-substitution into an argument list that
  word-splits unexpectedly.
- **SC2155** — `local x=$(cmd)` masks `cmd`'s exit status; declare then
  assign separately when the status matters under `set -e`.

## set -e correctness in `$( … )` assignments

`grep -c`/`grep -l` exit 1 when there are zero matches. In a pipeline
that ends in `awk` (exit 0) the pipeline status is fine; a bare
`var=$(grep -c …)` can trip `set -e`. End such pipelines in `awk`/`sort`
or append `|| true` — see `check_ci_mirrors_prepush.sh:64`.
