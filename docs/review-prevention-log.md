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

### Compact entry format

```md
### <bug class>
- First seen in: #<pr-or-issue>
- Symptom: <short>
- Prevention: <short>
- Enforced in: <file(s)>
```

---

## Entries

### No-op ORDER BY (sort column = WHERE predicate column)
- First seen in: #45
- Symptom: `ORDER BY instrument_id LIMIT 1` where `instrument_id` is already pinned by `WHERE` — provides no ordering.
- Prevention: Before pushing `fetchone()` with `ORDER BY <col> LIMIT 1`, verify the sort column is not the same column fixed by `WHERE`. Either drop `ORDER BY` and comment the PK guarantee, or sort by the freshness timestamp.
- Enforced in: `.claude/skills/engineering/pre-flight-review.md` section B

---

### Missing data on hard-rule path silently passes
- First seen in: #45
- Symptom: `_check_concentration` returned `passed=True` when sector was `None` — missing instrument silently bypassed the cap.
- Prevention: After writing any check that starts with a nullable lookup, ask: "Is `None` safe or a data-integrity failure?" Hard rule = fail closed; best-effort = note and continue.
- Enforced in: `.claude/skills/engineering/pre-flight-review.md` section A

---

### Product name drift across docs
- First seen in: #47
- Symptom: Rename to `eBull` was not propagated to all doc files; old name `trader-os` persisted.
- Prevention: Before pushing any doc PR that touches product names, grep for all name variants (`grep -i "trader-os\|ebull"`).
- Enforced in: this prevention log

---

### JOIN fan-out inflates aggregate totals
- First seen in: #45
- Symptom: `LEFT JOIN quotes` inside `GROUP BY` aggregate could double `SUM(market_value)` if `quotes` had multiple rows per instrument.
- Prevention: Before writing `JOIN <table>` inside `GROUP BY`, verify the join produces at most one row per key. Use `LATERAL ... ORDER BY <ts> DESC LIMIT 1` for multi-row tables. Watch tables: `quotes`, `theses`, `news_events`.
- Enforced in: `.claude/skills/engineering/pre-flight-review.md` section B

---

### Read-then-write cap enforcement outside transaction
- First seen in: #66
- Symptom: `override_tier` read `SELECT COUNT(*)` in one cursor, then wrote in a separate `conn.transaction()` — concurrent request could violate the Tier 1 cap.
- Prevention: Verify the count read and the write are inside the same `conn.transaction()`. Applies to: Tier 1 cap, max active positions, sector exposure limits.
- Enforced in: `.claude/skills/engineering/pre-flight-review.md` section F

---

### Bucket-arithmetic double-counting
- First seen in: #66
- Symptom: `unchanged = total - demotions - promotions - blocked` subtracted `blocked`, but blocked instruments were tier-unmodified — double-counted.
- Prevention: After writing `total - bucket_a - bucket_b - ...`, verify each bucket is mutually exclusive. Add `assert result >= 0` and trace with pen-and-paper values.
- Enforced in: this prevention log

---

### Audit reads outside the write transaction
- First seen in: #66
- Symptom: `old_tier` was read outside the transaction that wrote `coverage_audit` — a concurrent change could record the wrong value.
- Prevention: Any data appearing in an audit record must be read within the same transaction that writes the audit row.
- Enforced in: `.claude/skills/engineering/pre-flight-review.md` sections E + F

---

### decision_id received but not written back to decision_audit
- First seen in: #68
- Symptom: `execute_order` accepted `decision_id` but never wrote execution outcome to `decision_audit`. Both success and failure paths were unaudited.
- Prevention: Before pushing any service that receives `decision_id`, grep for `decision_audit`. If absent, the audit close-out is missing. Each execution pipeline stage writes its own audit row.
- Enforced in: this prevention log

---

### Zero-value fills persisted as real fills
- First seen in: #68
- Symptom: Demo mode with no quote produced `filled_price=0, filled_units=0`. The `is not None` guard passed because `Decimal("0") is not None`.
- Prevention: Any `if status == "filled"` persistence branch must also check `filled_units > 0`. Guard pattern: `status == "filled" and price is not None and units is not None and units > 0`.
- Enforced in: this prevention log

---

### Dimensional mismatch in field multiplication
- First seen in: #68
- Symptom: `target_entry * suggested_size_pct` — price x fraction = nonsense. Correct: `cash * size_pct`.
- Prevention: Before multiplying two DB fields, add a one-line comment stating the units of each operand and the expected result. If the comment can't be written confidently, the expression is wrong.
- Enforced in: this prevention log

---

### Shared column vocabulary mismatch across stages
- First seen in: #68
- Symptom: Execution guard writes `PASS`/`FAIL` to `decision_audit.pass_fail`; order client wrote `executed`/`execution_failed`. Downstream queries filtering on `PASS` missed all order rows.
- Prevention: Before inserting into any column another stage writes to, grep for all `INSERT INTO <table>` targeting that column and verify values match. Detailed status goes in `explanation` or `evidence_json`.
- Enforced in: this prevention log

---

### ON CONFLICT DO NOTHING counter overcount
- First seen in: #69
- Symptom: `written += 1` after `INSERT ... ON CONFLICT DO NOTHING` always incremented, even when the row was silently skipped. `fills_ingested` inflated, `already_present` went negative.
- Prevention: Gate counters on `result.rowcount > 0` (or count rows from `RETURNING`). Grep for `+= 1` near `ON CONFLICT` and confirm each is conditional.
- Enforced in: this prevention log

---

### Decimal repeating-decimal drift in pool accumulators
- First seen in: #69
- Symptom: `remaining * (amount_gbp / quantity)` with 3 units at 1000 GBP = `999.999...` instead of `1000`. After partial disposals, pool cost drifted negative.
- Prevention: Rewrite `units * (total_cost / total_units)` as `(units / total_units) * total_cost`. Quantize partial withdrawals to DB precision. Use exact remaining cost for full-depletion. Add regression test with indivisible lot.
- Enforced in: this prevention log

---

### Single-row UPDATE silent no-op on missing row
- First seen in: #70
- Symptom: `activate_kill_switch` ran UPDATE, committed, and logged "ACTIVATED" — but zero rows were affected because the singleton row was absent.
- Prevention: Check `result.rowcount == 0` after UPDATE and raise. Now promoted to a general rule.
- Enforced in: `.claude/skills/engineering/sql-correctness.md` ("Single-row UPDATE must verify rowcount")

---

### Health endpoint returns HTTP 200 on infrastructure failure
- First seen in: #70
- Symptom: `except Exception: return {"error": ...}` produced HTTP 200. Monitoring tools saw "healthy".
- Prevention: Inside route handlers, raise `HTTPException` with appropriate status (503 for infra, 500 for unexpected) — never `return` a dict on exception.
- Enforced in: this prevention log

---

### Spike query compares run against itself
- First seen in: #70
- Symptom: `check_row_count_spike` queried the most recent successful run after committing the current run — compared the run against itself. Spikes never detected.
- Prevention: After recording a row then querying the same table for a "previous" value, exclude the just-written row by PK (`AND run_id != %(exclude_id)s`).
- Enforced in: this prevention log

---

### Early return inside context-managed tracking without row_count
- First seen in: #70
- Symptom: `hourly_market_refresh` returned early inside `_tracked_job` without setting `tracker.row_count`. Job recorded as success with `row_count=None` — indistinguishable from tracking failure, suppressed spike detection.
- Prevention: Grep function body for `return` inside `_tracked_job` — each must set `tracker.row_count` first (usually `= 0` for "nothing to do" paths).
- Enforced in: this prevention log
