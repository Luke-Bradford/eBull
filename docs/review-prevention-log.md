# Review prevention log

## Purpose and usage

This file captures **recurring repo-specific mistakes** so they stop repeating across PRs.

- Use this file for bug classes that have appeared in eBull PRs and are likely to reappear.
- Use skill files (`.claude/skills/engineering/`) for reusable engineering rules that apply anywhere.
- Use `CLAUDE.md` for workflow and process rules.

Do not add one-off trivia. Add entries when a review catches a mistake that a future PR is plausible to repeat.

### When to read this file

Read it **before coding** for any issue. Identify which entries are relevant.
State which ones apply and how the plan avoids repeating them.
If none apply, say so explicitly.

### When to add entries

When a PREVENTION comment in a review reveals a bug class likely to recur in this repo,
add an entry here as part of resolving the comment (`EXTRACTED docs/review-prevention-log.md`).

---

## Entry template

### <bug class title>
- **Bug class:** short label
- **First seen in:** `#<pr-number>`
- **Example symptom:** what the reviewer observed
- **Root cause:** why it happened
- **Prevention rule:** the concrete check to apply before pushing
- **Enforced in:** skill file or checklist that now contains this rule
- **Promoted to skill?** yes / no — if yes, which file
- **Notes:** anything that helps judge edge cases

---

## Entries

### Latest-row queries must use a deterministic ORDER BY column

- **Bug class:** non-deterministic `fetchone()` / `LIMIT 1`
- **First seen in:** `#45`
- **Example symptom:** `_load_quote` used `ORDER BY instrument_id LIMIT 1`; since `instrument_id` is the `WHERE` predicate column, the sort was a no-op and row selection was arbitrary when multiple rows existed.
- **Root cause:** `ORDER BY` was added to satisfy the "every fetchone has ORDER BY" invariant, but the column chosen was the same one fixed by the `WHERE` clause — providing no actual ordering.
- **Prevention rule:** Before pushing any `fetchone()` with `ORDER BY <col> LIMIT 1`, verify the sort column is not the same column already pinned by the `WHERE` predicate. The correct column is the one that distinguishes rows — typically a timestamp (`quoted_at`, `created_at`) or an auto-increment PK.
- **Enforced in:** `.claude/skills/engineering/pre-flight-review.md` section B (SQL correctness)
- **Promoted to skill?** yes — `.claude/skills/engineering/pre-flight-review.md`
- **Notes:** For tables where singleton-per-key is guaranteed by a schema constraint (e.g. `coverage.instrument_id PRIMARY KEY`): drop the `ORDER BY` entirely and add a comment stating the PK guarantee — a no-op sort is more misleading than no sort. For tables where uniqueness is enforced only by application logic (e.g. `quotes` upserted but no schema constraint): use the freshness timestamp (`ORDER BY quoted_at DESC LIMIT 1`).

---

### Production invariants must not use bare `assert`

- **Bug class:** `assert` used as a runtime guard
- **First seen in:** `#45`
- **Example symptom:** `assert audit_row is not None` after an `INSERT … RETURNING` — if Python runs with `-O`, the assertion is stripped and the code silently continues with `audit_row = None`.
- **Root cause:** `assert` feels natural for "this should never happen" conditions, but it is optimised away in production builds.
- **Prevention rule:** Never use `assert` to guard a condition that must hold in production. Use `if … raise RuntimeError(...)`. Run `grep -n "^    assert\|^assert" app/services/*.py` before pushing and justify any remaining `assert` statements.
- **Enforced in:** `.claude/skills/engineering/python-hygiene.md` ("Production invariants" section)
- **Promoted to skill?** yes — `.claude/skills/engineering/python-hygiene.md`
- **Notes:** `assert` is fine for developer-assumption documentation in test code or unreachable-by-construction paths. It is not fine for guarding DB return values, API responses, or any path that could theoretically be reached.

---

### Boundary tests must prove business meaning, not just branch coverage

- **Bug class:** missing semantic boundary test
- **First seen in:** `#45`
- **Example symptom:** `_check_cash` returned `passed=True` for `cash=0.0`. A branch-coverage test would pass because the `if cash is None` branch was tested. But zero cash means no buying power — the rule's stated purpose — and that case was never tested.
- **Root cause:** tests were written to cover the None path and the positive path, omitting the zero boundary. Branch coverage was complete but the business meaning was not proven.
- **Prevention rule:** For any affordability, capacity, threshold, or limit rule, add explicit tests for: zero, exact cap, just below cap, just above cap. Assert `passed` and `rule` for each. Do not stop at branch execution.
- **Enforced in:** `.claude/skills/engineering/test-quality.md` ("Semantic boundary checks" section)
- **Promoted to skill?** yes — `.claude/skills/engineering/test-quality.md`
- **Notes:** The test name should state the business meaning: `test_zero_cash_fails` is correct; `test_cash_check_false` is not.

---

### Missing data on a hard-rule path must fail explicitly, not silently pass

- **Bug class:** `None` lookup returns silent pass on a hard rule
- **First seen in:** `#45`
- **Example symptom:** `_check_concentration` returned `passed=True` when `sector is None` (instrument not in instruments table, or sector column is NULL). A missing instrument silently bypassed the concentration cap.
- **Root cause:** early `if sector is None: return RuleResult(..., passed=True)` treated missing data as a safe default. For a hard-rule guard, missing data is not safe — it is a data-integrity problem.
- **Prevention rule:** After writing any check function that starts with a nullable lookup, ask: "Is `None` here a safe state or a data-integrity failure?" Add a test that passes `None` for the entity and asserts `passed=False` with a specific rule name. Do not default to `passed=True` when upstream data is absent on a hard-rule path.
- **Enforced in:** `.claude/skills/engineering/pre-flight-review.md` section A (first-row / empty-state correctness)
- **Promoted to skill?** no — already covered by pre-flight section A; this entry records the repo-specific pattern
- **Notes:** Applies specifically to hard-rule guards and audit-path functions. For best-effort/informational paths (e.g. portfolio manager recommendations), `None` may reasonably produce a pass with an explanation. The distinction is: hard rule = fail closed; best-effort = note and continue.

---

### JOIN fan-out can corrupt derived totals in aggregate queries

- **Bug class:** fan-out join inflating aggregate
- **First seen in:** `#45`
- **Example symptom:** `LEFT JOIN quotes q ON q.instrument_id = p.instrument_id` inside a `GROUP BY` aggregate. If `quotes` ever has more than one row per instrument, `SUM(market_value)` doubles or triples the position value for each extra quote row — silently inflating AUM and making concentration checks more permissive.
- **Root cause:** the join condition matched all rows for the instrument rather than the single latest row. The `GROUP BY` aggregated the fanned-out result without any visible warning.
- **Prevention rule:** Before writing any `JOIN <table>` inside a `GROUP BY` aggregate query, verify the join produces at most one row per driving-table row. If the joined table may have multiple rows per key, use a `LATERAL` subquery with `ORDER BY <timestamp> DESC LIMIT 1` to select a single row. Run `grep -n "JOIN quotes\|JOIN theses\|JOIN news" app/services/*.py` before pushing and confirm each join is safe.
- **Enforced in:** `.claude/skills/engineering/pre-flight-review.md` section B (SQL correctness)
- **Promoted to skill?** no — the general rule is in pre-flight section B; this entry records the repo-specific tables to watch
- **Notes:** Tables to watch in eBull: `quotes` (one row per instrument by upsert today, but the upsert is not a schema constraint); `theses` (multiple rows per instrument by design); `news_events` (multiple rows per instrument). Any of these joined naively inside an aggregate is a fan-out hazard.
