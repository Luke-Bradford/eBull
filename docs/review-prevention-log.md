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

---

### Services receiving a decision_id must write back to decision_audit

- **Bug class:** missing audit close-out on execution path
- **First seen in:** `#68`
- **Example symptom:** `execute_order` accepted `decision_id`, linked it to the orders FK, but never wrote the execution outcome back to `decision_audit`. Success and failure paths were both unaudited.
- **Root cause:** the audit row from the execution guard was treated as the complete record; the subsequent execution stage did not write its own.
- **Prevention rule:** Before pushing any service that receives a `decision_id` parameter, grep the file for `decision_audit`. If the string does not appear, the audit close-out is missing. Every stage in the execution pipeline must write its own `decision_audit` row.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — too specific to the execution pipeline
- **Notes:** The execution guard writes stage='execution_guard'. The order client writes stage='order_execution'. Each stage is responsible for its own audit row.

---

### Zero-value fills must not be persisted as real fills

- **Bug class:** zero-unit fill written to fills/positions/cash_ledger
- **First seen in:** `#68`
- **Example symptom:** demo mode with no quote produced `filled_price=0, filled_units=0`. The condition `filled_price is not None and filled_units is not None` was True (Decimal("0") is not None), so garbage rows were written to `fills`, `positions`, and `cash_ledger`.
- **Root cause:** the fill guard only checked for None, not for zero values.
- **Prevention rule:** After writing any `if status == "filled"` persistence branch, verify the guard also checks `filled_units > 0`. A zero-unit fill from a demo or error path must not produce fill/position/cash rows.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to order execution
- **Notes:** Applies to any future fill processing (e.g. pending order polling). The guard pattern is: `status == "filled" and price is not None and units is not None and units > 0`.

---

### `or`-chaining on external API numeric fields silently discards zero values

- **Bug class:** falsy-zero fallthrough in `or` chains
- **First seen in:** `#68`
- **Example symptom:** `raw.get("Fees") or raw.get("fees")` — if the API returns `"Fees": 0`, `or` treats it as falsy and falls through to the next key.
- **Root cause:** Python `or` evaluates truthiness, not nullness. `0`, `0.0`, `Decimal("0")`, and `""` are all falsy.
- **Prevention rule:** After writing any `a or b` expression where `a` comes from an external API payload, grep the file for ` or raw.get(` — every hit should be reviewed for zero-value correctness. Use `a if a is not None else b` instead of `a or b` for numeric fields.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — already implicitly covered by python-hygiene; this entry records the repo-specific API pattern
- **Notes:** For string fields (order ref, status label), `or`-chaining is fine because empty strings are invalid. For numeric fields (price, units, fees), use explicit `is not None` checks.

---

### Multiplying two DB fields requires dimensional comment

- **Bug class:** dimensionally incorrect arithmetic
- **First seen in:** `#68`
- **Example symptom:** `target_entry * suggested_size_pct` — price × fraction = nonsensical number. The correct amount is `cash * suggested_size_pct`.
- **Root cause:** two fields from the same row were multiplied without checking units.
- **Prevention rule:** Before multiplying two fields from a DB row, add a one-line comment stating the units of each operand and the expected units of the result. If the comment cannot be written confidently, the expression is wrong.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — general enough but too edge-case for a skill; this entry is a repo-specific reminder
- **Notes:** Common correct patterns: `cash (USD) * size_pct (fraction) = dollar_amount (USD)`. Common wrong patterns: `price (USD/unit) * size_pct (fraction) = ??? (USD/unit × fraction = nonsense)`.

---

### Columns shared across stages must use a consistent vocabulary

- **Bug class:** vocabulary mismatch in shared column
- **First seen in:** `#68`
- **Example symptom:** execution guard writes `PASS`/`FAIL` to `decision_audit.pass_fail`; order client wrote `executed`/`execution_failed`/`execution_pending` to the same column. Downstream queries filtering on `pass_fail = 'PASS'` would miss all order execution rows.
- **Root cause:** the new stage used its own status enum instead of the established column vocabulary.
- **Prevention rule:** Before inserting into any column that another stage already writes to, grep the codebase for all `INSERT INTO <table>` statements targeting that column and verify the values match. If the column uses a fixed vocabulary (`PASS`/`FAIL`), new stages must map to that vocabulary. Detailed status goes in `explanation` or `evidence_json`.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to multi-stage audit patterns
- **Notes:** `decision_audit.pass_fail` is unconstrained `TEXT NOT NULL`, but the convention is `PASS`/`FAIL` across all stages.

---

### ON CONFLICT DO NOTHING counters must use rowcount, not unconditional increment

- **Bug class:** overcount on idempotent upsert
- **First seen in:** `#69`
- **Example symptom:** `_ingest_fills` incremented `written += 1` after every `INSERT ... ON CONFLICT DO NOTHING`, regardless of whether the row was actually inserted or silently skipped. `fills_ingested` was inflated and `already_present` went negative.
- **Root cause:** `ON CONFLICT DO NOTHING` succeeds silently — `conn.execute` does not raise — so a post-execute counter always increments.
- **Prevention rule:** Before pushing any `INSERT ... ON CONFLICT DO NOTHING` with a Python counter, verify the counter is gated on `result.rowcount > 0` (or count rows from a `RETURNING` clause). Grep for `+= 1` near any `ON CONFLICT` and confirm each is conditional.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to ingestion patterns
- **Notes:** `result.rowcount` is 0 when the conflict clause absorbs the duplicate. For `ON CONFLICT DO UPDATE`, rowcount is 1 even on conflict (because the row is updated), so this issue is specific to `DO NOTHING`.

---

### Decimal pool accumulators must avoid repeating-decimal drift

- **Bug class:** non-terminating decimal expansion corrupts pool cost
- **First seen in:** `#69`
- **Example symptom:** S104 pool with 3 units at 1000 GBP. `remaining * (amount_gbp / quantity)` = `3 * (1000/3)` = `3 * 333.333...` = `999.999...` instead of `1000`. After multiple partial disposals, pool cost drifted negative, tripping the invariant check.
- **Root cause:** `a * (b / c)` and `(a / c) * b` are mathematically equivalent but not equivalent in Decimal arithmetic when `b / c` is non-terminating. The former expands the repeating decimal first; the latter computes the ratio first (which is exact when `a == c`).
- **Prevention rule:** Before pushing any pool accumulator that computes `cost = units * (total_cost / total_units)`, rewrite as `cost = (units / total_units) * total_cost`. For pool withdrawals, quantize partial withdrawals to match the DB column precision (e.g. `NUMERIC(18,6)` → `.quantize(Decimal("0.000001"))`), and use exact remaining cost for full-depletion. Add a regression test with an indivisible lot (e.g. 3 units at 1000 GBP) asserting exact cost conservation.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to S104 pool and similar accumulator patterns
- **Notes:** The key insight is operation ordering: `(remaining / quantity) * amount` is exact when `remaining == quantity` because `(q/q) = 1` exactly. Combined with quantize + full-depletion, this guarantees `sum(all_withdrawals) == original_cost`.

---

### Single-row UPDATE must check rowcount before declaring success

- **Bug class:** silent no-op on UPDATE WHERE with missing row
- **First seen in:** `#70`
- **Example symptom:** `activate_kill_switch` ran `UPDATE kill_switch SET is_active = TRUE WHERE id = TRUE`, called `conn.commit()`, and logged "ACTIVATED" — but the kill_switch row was absent, so zero rows were updated and the system remained unprotected.
- **Root cause:** `UPDATE ... WHERE` silently affects zero rows when the predicate matches nothing. The function assumed the row existed and did not verify `rowcount`.
- **Prevention rule:** After writing any `UPDATE ... WHERE` that must affect exactly one row (singleton tables, primary-key lookups), check `result.rowcount` and raise `RuntimeError` if zero. Grep the file for `conn.execute("UPDATE` and confirm each has a rowcount guard or a comment explaining why zero rows is acceptable.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to singleton-table mutation patterns
- **Notes:** Applies to `kill_switch`, `coverage` (PK update), `positions` (PK upsert). For `UPDATE ... SET` with non-PK WHERE, zero rows may be acceptable — the key distinction is whether the caller assumes success.

---

### Health endpoints must not return HTTP 200 on infrastructure failure

- **Bug class:** error body with success status code
- **First seen in:** `#70`
- **Example symptom:** `GET /health/data` caught `Exception` and returned `{"error": "..."}` with HTTP 200. Monitoring tools polling the endpoint saw 200 OK and interpreted the system as healthy.
- **Root cause:** `except Exception: return {"error": ...}` produces a FastAPI JSONResponse with status 200. The intent was graceful degradation, but the effect was masking failures from automated health checks.
- **Prevention rule:** Before pushing any `except Exception` inside a route handler, verify it raises `HTTPException` with an appropriate status code (503 for infrastructure, 500 for unexpected errors) — never `return` a dict on exception. Grep for `except Exception` in route handlers and confirm each either raises or has a comment explaining why 200 is correct.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to FastAPI endpoint patterns
- **Notes:** `/health/db` follows the same pattern (returns 200 with `db_reachable: False`) — this is intentional there because it explicitly signals DB status in the response body and the endpoint itself is always reachable. The distinction is: health endpoints that are the primary liveness signal must use status codes; diagnostic endpoints that always succeed can use response body signaling.

---

### Spike/comparison queries must exclude the just-written row

- **Bug class:** self-referencing comparison query
- **First seen in:** `#70`
- **Example symptom:** `check_row_count_spike` queried the most recent successful job run to compare against the current count. Because `record_job_finish` committed the current run first, the query returned the just-written row — comparing the run against itself. Spikes were never detected.
- **Root cause:** `ORDER BY started_at DESC LIMIT 1` with no exclusion of the current `run_id` after an in-session commit.
- **Prevention rule:** After writing any query that records a row then immediately queries the same table for a "previous" value, verify the query explicitly excludes the just-written row by primary key. Grep for sequences of INSERT/UPDATE + SELECT on the same table and confirm each SELECT excludes the current PK.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to job-tracking and comparison patterns
- **Notes:** The pattern is: write row → commit → query for "previous" value. The exclusion can be `AND run_id != %(exclude_id)s` or a subquery. Either way, the caller must thread the PK through.

---

### Early return inside context-managed tracking must set row_count

- **Bug class:** tracked job with unset row_count on early exit
- **First seen in:** `#70`
- **Example symptom:** `hourly_market_refresh` returned early when no covered instruments were found. The `_tracked_job` context manager recorded the job as `success` with `row_count=None` — indistinguishable from a tracking failure and suppressing spike detection.
- **Root cause:** the `return` bypassed `tracker.row_count = ...` which was after the main work block.
- **Prevention rule:** Before pushing any scheduler job wrapped in `_tracked_job`, grep the function body for `return` statements and verify each one sets `tracker.row_count` first (usually `= 0` for "nothing to do" paths). A bare `return` inside `_tracked_job` without setting `row_count` is always a suspect pattern.
- **Enforced in:** this prevention log
- **Promoted to skill?** no — specific to scheduler job tracking
- **Notes:** The canonical early-exit pattern is: `tracker.row_count = 0; return`. This distinguishes "ran successfully with no work" from "tracking infrastructure failed".
