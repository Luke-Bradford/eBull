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
- **Prevention rule:** Before pushing any `fetchone()` with `ORDER BY <col> LIMIT 1`, verify the sort column is not the same column already pinned by the `WHERE` predicate. If it is: either drop the `ORDER BY` entirely and add a comment stating the schema constraint that guarantees at most one row; or switch to the freshness timestamp (e.g. `ORDER BY quoted_at DESC LIMIT 1`). A no-op sort is more misleading than no sort.
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

### Product name inconsistency introduced by documentation PRs

- **Bug class:** product name drift across docs
- **First seen in:** `#47`
- **Example symptom:** `CLAUDE.md` renamed the project to `eBull` but `docs/settled-decisions.md` never recorded the canonical name, and the previous name `trader-os` persisted in other contexts.
- **Root cause:** the rename was intentional but not propagated to settled-decisions.md; doc-only PRs that touch names can introduce inconsistency without a grep check.
- **Prevention rule:** Before pushing any documentation PR that touches product names, grep the entire diff for all name variants (`grep -i "trader-os\|ebull\|eBull"`). Confirm the name is consistent across every changed file and the PR description.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — too project-specific
- **Notes:** Canonical name is `eBull`. Retired name is `trader-os`. The settled-decisions.md Product name section is the authoritative record.

---

### JOIN fan-out can corrupt derived totals in aggregate queries

- **Bug class:** fan-out join inflating aggregate
- **First seen in:** `#45`
- **Example symptom:** `LEFT JOIN quotes q ON q.instrument_id = p.instrument_id` inside a `GROUP BY` aggregate. If `quotes` ever has more than one row per instrument, `SUM(market_value)` doubles or triples the position value for each extra quote row — silently inflating AUM and making concentration checks more permissive.
- **Root cause:** the join condition matched all rows for the instrument rather than the single latest row. The `GROUP BY` aggregated the fanned-out result without any visible warning.
- **Prevention rule:** Before writing any `JOIN <table>` inside a `GROUP BY` aggregate query, verify the join produces at most one row per driving-table row. If the joined table may have multiple rows per key, use a `LATERAL` subquery with `ORDER BY <timestamp> DESC LIMIT 1` to select a single row. Run `grep -n "JOIN quotes\|JOIN theses\|JOIN news" app/services/*.py` before pushing and confirm each join is safe.
- **Enforced in:** `.claude/skills/engineering/pre-flight-review.md` section B (SQL correctness)
- **Promoted to skill?** no — the general rule is in pre-flight section B; this entry records the repo-specific tables to watch
- **Notes:** Tables to watch in eBull (v1 state — update when schema constraints change): `quotes` (one row per instrument by upsert, but no unique constraint); `theses` (multiple rows per instrument by design); `news_events` (multiple rows per instrument). Any of these joined naively inside an aggregate is a fan-out hazard.

---

### Read-then-write cap/limit checks must be inside the same transaction

- **Bug class:** TOCTOU race on count-based limit enforcement
- **First seen in:** `#66`
- **Example symptom:** `override_tier` read `SELECT COUNT(*) FROM coverage WHERE coverage_tier = 1` in one cursor, then later opened a separate `conn.transaction()` for the write. A concurrent request could promote past the Tier 1 cap between the count read and the tier update.
- **Root cause:** the count query and the tier mutation were not in the same transaction, creating a window where the cap could be violated by concurrent callers.
- **Prevention rule:** Before pushing any read-then-write pattern involving a count/limit enforcement, verify the read and the write are inside the same `conn.transaction()` block. Grep for `SELECT COUNT` and confirm the next write is not separated by a cursor close, function boundary, or transaction boundary.
- **Enforced in:** `.claude/skills/engineering/pre-flight-review.md` section F (Concurrency / idempotency)
- **Promoted to skill?** no — the general concurrency check is already in pre-flight section F; this entry records the repo-specific pattern
- **Notes:** Applies to any cap or quota enforcement in eBull: Tier 1 cap, max active positions, sector exposure limits. The pattern is: read the current count, check against the limit, then write — all three must be atomic.

---

### Bucket-arithmetic formulas must account for all categories

- **Bug class:** off-by-N count from overlooked bucket
- **First seen in:** `#66`
- **Example symptom:** `unchanged = len(snapshots) - len(demotions) - len(all_promotions) - len(blocked)` subtracted `blocked` from the total, but blocked instruments did not change tier — they were double-counted as both "not unchanged" and "not tier-modified".
- **Root cause:** the formula assumed every bucket was mutually exclusive with "unchanged", but `blocked` instruments are tier-unmodified (their old_tier == new_tier).
- **Prevention rule:** After writing any formula of the form `total - bucket_a - bucket_b - ...`, verify each bucket is mutually exclusive. Add an `assert result >= 0` immediately after. Trace the formula with pen-and-paper values (e.g. 51 instruments, 1 blocked, 0 demoted) before pushing.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — too specific to summary-count patterns
- **Notes:** The correct formula is: only subtract categories that actually changed state. Categories that are "processed but unchanged" (like blocked promotions) should not reduce the unchanged count.

---

### Audit-record reads must be inside the write transaction

- **Bug class:** stale state recorded in audit table
- **First seen in:** `#66`
- **Example symptom:** `override_tier` read `coverage_tier` (for `old_tier`) in a cursor outside the transaction, then wrote the audit record inside a later `conn.transaction()`. A concurrent tier change between the read and write would record the wrong `old_tier` in `coverage_audit`.
- **Root cause:** the `SELECT` that populated the audit evidence was separated from the `INSERT INTO coverage_audit` by a transaction boundary.
- **Prevention rule:** Before pushing any `override_*` or `audit_*` function, grep for `SELECT` calls that read state used in audit records and confirm they are inside the same `conn.transaction()` block as the `INSERT INTO *_audit`. If the `SELECT` is outside the transaction, the audit row can record stale state.
- **Enforced in:** `.claude/skills/engineering/pre-flight-review.md` section E (Auditability) and section F (Concurrency)
- **Promoted to skill?** no — already covered by pre-flight sections E+F; this entry records the repo-specific pattern
- **Notes:** General rule: any data that appears in an audit record must be read within the same transaction that writes the audit row.
