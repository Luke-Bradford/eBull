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

---

### Shared params dict passed to multiple queries with different placeholders
- First seen in: #73
- Symptom: `params` dict containing `limit`/`offset` was passed to a COUNT query that had no `%(limit)s`/`%(offset)s` placeholders. psycopg3 may raise on unused named params depending on version.
- Prevention: Before pushing any endpoint that builds a shared `params` dict across multiple queries, verify each key is consumed by every query that receives it — or use separate dicts per query.
- Enforced in: this prevention log

---

### Dead-code None-guard on aggregate fetchone()
- First seen in: #75
- Symptom: `ts_row = cur.fetchone(); val = ts_row["col"] if ts_row else None` — the `else None` branch is unreachable because `SELECT MAX/MIN/COUNT(...)` always returns exactly one row. Misleads the reader into thinking a no-rows case is possible.
- Prevention: After writing any `fetchone()` following an aggregate-only SELECT (MAX, MIN, COUNT, SUM without GROUP BY), confirm the None check is on the column value, not the row. The row is always non-None; the column value is None when the table is empty.
- Enforced in: this prevention log

---

### float(None) crash in parse helpers for nullable DB columns
- First seen in: #73
- Symptom: `_parse_quote` gated only on `quoted_at` being non-None, then called `float(row["bid"])`. A partially-written quote row with `quoted_at` set but `bid` NULL would crash with `TypeError`.
- Prevention: After writing any helper that casts nullable DB columns with `float(row["x"])`, guard all required fields before the cast — not just the sentinel field. Add a test where the sentinel is present but required fields are None.
- Enforced in: this prevention log

---

### Unbounded enum filters accept nonsense values silently
- First seen in: #77
- Symptom: `action: str | None = Query(...)` accepted `action="NUKE"` and silently returned empty results instead of a 422.
- Prevention: Before pushing any endpoint that filters on a column with a closed value set (action, status, direction, etc.), type the query parameter as `Literal[...]` — not bare `str`. Grep the route file for `Query(default=None)` and confirm each param with a bounded domain uses `Literal`.
- Enforced in: this prevention log

---

### Shared cursor across unrelated queries
- First seen in: #77
- Symptom: Two logically independent queries (positions, cash) shared a single cursor. After `fetchall()` on the first, reusing the cursor for the second relies on psycopg v3 internal state. Mock tests paper over this by resetting return values per `execute`.
- Prevention: Before pushing any handler that calls `cur.execute()` more than once on the same cursor, use separate `with conn.cursor(...) as cur:` blocks for logically independent queries.
- Enforced in: this prevention log

---

### Zero-unit position inflates AUM via cost_basis fallback
- First seen in: #77
- Symptom: A fully-liquidated position (`current_units=0`) with no quote fell back to `market_value = cost_basis`, inflating AUM despite holding no units.
- Prevention: Before pushing any MTM calculation that reads from `positions`, add `WHERE current_units > 0` to exclude fully-liquidated rows. Add a test with `current_units=0` and a non-zero `cost_basis` to verify it does not appear.
- Enforced in: this prevention log

---

### Weak operator credential silently accepted
- First seen in: #81
- Symptom: `api_key: str | None = None` accepted any non-empty value, including `"a"`. A misconfigured deploy with a single-character key would auth-pass without warning.
- Prevention: For any operator credential / token / key field on `Settings`, attach a `@field_validator` that rejects values shorter than a minimum-entropy threshold (32 chars baseline). Empty string is allowed only because it is treated as "unset" by the fail-closed branch.
- Enforced in: `app/config.py` (`_api_key_min_length`)

---

### Test teardown re-imports module-level fixture instead of capturing it
- First seen in: #81
- Symptom: `teardown_method` re-imported the conftest no-op override at teardown time. If the import path or symbol name ever changed, teardown would silently install `None` instead of raising — breaking auth bypass for the rest of the suite without a clear failure signal.
- Prevention: When a test temporarily mutates a module-level singleton (`app.dependency_overrides`, settings fields, env vars), capture the prior value in `setup_method` (`self._prior = container.get(key)`) and restore from `self._prior` in `teardown_method`. Never re-fetch from the source in teardown.
- Enforced in: `tests/test_api_auth.py` (`_AuthTestBase`)

---

### `None` last_status silently classified into a health bucket
- First seen in: #86
- Symptom: `_derive_overall_status` treated `job.last_status != "success"` as `"degraded"`, which folded `None` (no runs ever recorded) into the degraded signal. Every fresh deploy reported `overall_status="degraded"` purely on job state, with no way to distinguish "never run" from "currently running" from "failed".
- Prevention: When deriving an aggregate health status from per-job state, enumerate the bucket each `last_status` value belongs to explicitly (`"success"`, `"failure"`, `"running"`, `None`). `None` is a separate signal — it means "no evidence either way" and must not be folded into degraded/down without an explicit decision and a test that pins the choice. Add a fresh-deploy test (`last_status is None` for every job, layers ok → assert `overall_status == "ok"`).
- Enforced in: this prevention log

---

### Internal exception text leaked into HTTP response bodies
- First seen in: #86
- Symptom: Two distinct leak sites in the same PR:
  1. `/system/status` and `/system/jobs` raised `HTTPException(status_code=503, detail=f"...: {exc}")`, echoing driver / SQL error text to bearer-token holders.
  2. `check_all_layers` constructed `LayerHealth(detail=f"{layer}: query failed — {exc}")` in its per-layer error branch, leaking the same exception text into the `/system/status` 200 response payload (the `detail` field is surfaced verbatim in the JSON body).
- Prevention: Any string that ends up in an HTTP response body must be a fixed phrase when it carries an exception. This applies to **both** `HTTPException(detail=...)` at any status code **and** any service-layer `detail` / `message` / `error` field that the API serialises into the response. Full exception text goes to `logger.exception` server-side only. Before pushing, grep for `f"...{exc}"`, `str(exc)`, and `{exc}` inside any field that the API surfaces — including service-layer dataclasses that flow into response models. Tests should raise an exception with a unique marker and assert the marker is absent from `response.text`.
- Enforced in: this prevention log

---

### Frontend async render-surface isolation
- First seen in: #89
- Prevention: See `.claude/skills/frontend/async-data-loading.md`
- Enforced in: `.claude/skills/frontend/async-data-loading.md`

---

### Frontend safety-state persistence during refetch
- First seen in: #89
- Prevention: See `.claude/skills/frontend/safety-state-ui.md`
- Enforced in: `.claude/skills/frontend/safety-state-ui.md`

---

### Stale-closure data loss in monotonic state accumulators (frontend useEffect)
- First seen in: #93
- Symptom: A `useEffect` accumulating into a React state set (`knownSectors`) read the state variable from the closure (`new Set(knownSectors)`) and suppressed `react-hooks/exhaustive-deps` with a blanket disable. Two rapid data updates landing before a state flush could re-seed the `Set` from the pre-first-update snapshot, silently dropping items added in the first update.
- Prevention: When a `useEffect` derives new state from prior state, use the functional `setState(prev => ...)` form so React supplies the freshest snapshot. Do **not** suppress `react-hooks/exhaustive-deps` to paper over a closure-vs-state mismatch — the suppression hides exactly this bug class. Grep for `eslint-disable-next-line react-hooks/exhaustive-deps` in any new frontend PR; each occurrence must justify why a functional update is not viable.
- Enforced in: this prevention log

---

### Naive datetime in TIMESTAMPTZ query params
- First seen in: #80
- Symptom: A `datetime | None` query parameter without timezone info is sent to PostgreSQL as naive; comparing against a `TIMESTAMPTZ` column may cause mixed-offset rejection or silent misinterpretation.
- Prevention: Before pushing any endpoint that accepts a `datetime` query parameter compared against a `TIMESTAMPTZ` column, coerce naive datetimes to UTC: `dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)`.
- Enforced in: this prevention log

---

### Early return inside `with conn.transaction()` block
- First seen in: #109
- Symptom: A guard check inside `with conn.transaction(): ...` returns early on the failure path. psycopg v3 closes the context manager on the return, which commits the (possibly empty) transaction. Not strictly wrong, but obscures intent and was misread as "rollback" by the reviewer; in some shapes it can leave the caller's connection in an unexpected post-commit state.
- Prevention: Treat `with conn.transaction():` as an explicit unit. Hoist guard checks above the `with` line where possible, or capture the outcome in a local variable and `return` only after the `with` block has exited. Grep `with conn\.transaction\(\):` for `\breturn\b` in the same block during pre-flight review.
- Enforced in: this prevention log

---

### `SELECT COUNT(*)` race when gating a subsequent DELETE
- First seen in: #109
- Symptom: A "is this the last row?" check uses `SELECT COUNT(*)` under the default READ COMMITTED isolation, then deletes if the count exceeds 1. Two concurrent transactions can both observe count=2 and both delete, leaving zero rows.
- Prevention: When a DELETE depends on a count or existence check inside the same transaction, take a row-level lock that conflicts: `SELECT ... FROM target_table FOR UPDATE` (or a `pg_advisory_xact_lock` over the whole tx). A bare `COUNT(*)` does not serialise. Grep `SELECT COUNT\(\*\)` adjacent to a `DELETE FROM` in the same `with conn.transaction()`.
- Enforced in: this prevention log

---

### `request.client.host` is the TCP peer, not the HTTP `Host` header
- First seen in: #109
- Symptom: A FastAPI/Starlette handler used `request.client.host` under a variable named `request_host`, then compared it against a loopback set to gate a security check. `client.host` is the source IP of the TCP peer, not the `Host` header — and behind any reverse proxy it is the proxy's address, so the loopback check would always pass even for genuinely-remote clients.
- Prevention: When you read `request.client.host`, name the variable `request_client_ip` (or similar) and document explicitly that it is the TCP peer. If a check needs the real client behind a proxy, it must read `X-Forwarded-For` from a trusted proxy and the doc must say so. Do not gate security on `client.host` in any deployment that might sit behind a proxy.
- Enforced in: this prevention log

---

### Sentinel values on unused branches
- First seen in: #109
- Symptom: A function set ``operator_count = 2`` on the non-self-delete branch purely to satisfy a combined ``if is_self and operator_count <= 1`` condition on the self-delete branch. The variable was never read on the non-self path. A future maintainer adding ``elif not is_self and operator_count == 0`` (a legitimate defensive check) would silently get the hardcoded ``2`` and the new check would never fire.
- Prevention: Do not assign sentinel values on branches where a variable is unused. Use a clearly-named boolean (``last_operator_block = ...``) computed only on the path that needs it, or restructure the conditional so the unused branch never touches the variable. Grep for `= 0`, `= 1`, `= 2`, etc. assigned in service code on a branch with a comment like "not used" or "satisfies the branch below".
- Enforced in: this prevention log

---

### Advisory lock scope vs concurrent writers
- First seen in: #109
- Symptom: A `pg_advisory_xact_lock` was used to serialise a read-then-DELETE pattern (the "is this the only operator?" check before a self-delete). The lock correctly serialised concurrent self-deletes against each other, but a concurrent `INSERT` from a different code path (`create_operator`) did not take the same lock and could commit between the count read and the DELETE — leaving a row that the count check never saw.
- Prevention: When a pg advisory lock is used to gate a read-then-write pattern, every code path that mutates the same invariant must also take the same lock. Grep `pg_advisory_xact_lock` and verify the set of callers covers every writer to the counted/queried table, not just the one path that does the gated mutation. An advisory lock is cooperative — it only protects against callers that also acquire it.
- Enforced in: this prevention log

---

### `assert` as a runtime guard in service code
- First seen in: #109
- Symptom: A service function used `assert row is not None` after a `RETURNING` INSERT (or after a transaction block) to enforce a DB-contract invariant. `python -O` strips assertions, so the guard vanishes in any optimised build and the next line crashes with a confusing `TypeError: 'NoneType' object is not subscriptable` (or similar) instead of the intended structured error.
- Prevention: In service code, every guard that enforces a DB-contract or post-commit invariant must be `if x is None: raise RuntimeError(...)` (or a typed exception), not `assert`. `assert` is acceptable only for in-test fixtures and developer-only invariants that are clearly not on a production code path. Grep `^\s*assert ` in `app/services/` during pre-flight review.
- Enforced in: this prevention log
