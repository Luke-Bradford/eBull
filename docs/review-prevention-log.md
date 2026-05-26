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

---

### f-string SQL composition for column / table identifiers
- First seen in: #110
- Symptom: A service function built a query with `f"SELECT {_METADATA_COLS} FROM ..."` where `_METADATA_COLS` was a module-level constant string. Not exploitable today (the constant is not user-controlled), but the pattern bypasses psycopg's parameterisation for the column list and silently breaks if the constant ever contains a quote, is made configurable, or is reused on a path with caller-supplied input. Same risk class as raw `% formatting` in SQL.
- Prevention: Never f-string-interpolate identifiers into SQL templates. Use `psycopg.sql.SQL(...).format(cols=sql.SQL(", ").join(sql.Identifier(name) for name in COLS))` for column lists; `sql.Identifier()` for table/column names; `%s` placeholders for values. Grep `f"""\s*\n.*FROM\|f"\s*SELECT\|f"\s*INSERT\|f"\s*UPDATE` under `app/services/` during pre-flight review.
- Enforced in: this prevention log

---

### Mid-transaction `conn.commit()` in service functions that accept a caller's connection
- First seen in: #110
- Symptom: A service function (`load_credential_for_provider_use`) called `conn.commit()` internally while accepting an arbitrary `psycopg.Connection`. If the caller had any prior writes accumulated on the same connection, the function silently flushed them — a hard-to-reason-about side effect that broke transaction atomicity invisibly.
- Prevention: A service function that accepts a caller-supplied `Connection` MUST NOT call `conn.commit()` or `conn.rollback()`. If atomicity is needed, use `with conn.transaction()` (a savepoint when nested, a top-level txn otherwise) and let exceptions propagate. The caller owns the lifecycle. Grep `conn\.commit\|conn\.rollback` in `app/services/` and verify each call site is in a function that owns its connection (e.g. opens it via `psycopg.connect`), not one that takes `conn:` as a parameter.
- Enforced in: this prevention log

---

### `ON DELETE CASCADE` on `*_audit` / `*_log` tables destroys forensic history
- First seen in: #110
- Symptom: A migration declared `broker_credential_access_log.credential_id REFERENCES broker_credentials(id) ON DELETE CASCADE`. Soft-delete via `revoked_at` was the normal path, but a hard delete (DBA cleanup, accidental psql) would silently wipe the entire access log for that credential, violating audit-preservation guarantees.
- Prevention: Audit / log / journal tables must use `ON DELETE SET NULL` (with the column nullable) or `ON DELETE RESTRICT`, never `ON DELETE CASCADE` to a referenced "live data" table. Same pattern as `operator_audit` (017_operator_audit.sql), which drops the FK entirely so the audit row outlives the operator. Grep `ON DELETE CASCADE` against any new migration touching a table whose name ends in `_audit` or `_log`, and reject.
- Enforced in: this prevention log

---

### Startup gate must populate the runtime cache, not just validate
- First seen in: #110
- Symptom: A crypto module exposed `load_key()` (called once at startup to fail-fast on a missing key) and `_get_aesgcm()` (called on hot paths to get the cached primitive). `load_key()` validated and returned the key but did NOT populate the cache, so the first hot-path call re-read `settings.secrets_key` independently. A test (or future reload path) that mutated `settings` between startup and the first request could silently use a different key than the one validated at boot, with no error.
- Prevention: Any "validate at startup" function must populate the same cache the runtime path reads from, so the validated value is provably the value used at runtime. Add a regression test that calls the startup gate, mutates the underlying setting to garbage, and asserts a runtime call still works (proving it uses the cached value, not a re-read). Pattern: `load_X()` populates the global; `_get_X()` returns the global, falling through to `load_X()` only as a defensive last resort.
- Enforced in: this prevention log

---

### `except BaseException` in a destructive rollback handler destroys recovery state on signals
- First seen in: #118
- Symptom: A lazy-gen rollback handler used `except BaseException` so that `HTTPException` (a `BaseException` subclass in older designs is not the case, but the intent was "catch literally everything") would also trigger cleanup. This caught `KeyboardInterrupt` and `SystemExit`, meaning a `Ctrl-C` or `SIGTERM` mid-request would unlink the freshly-persisted root secret file before re-raising — an unrecoverable operator lockout caused by a clean shutdown.
- Prevention: A rollback / cleanup handler that performs destructive actions (file unlink, cache clear, state reset) MUST narrow its catch to `except Exception`. If you also want to handle some non-`Exception` cases, add explicit `except (KeyboardInterrupt, SystemExit): raise` ahead of the `Exception` handler so signals always propagate cleanly. A clean shutdown must never trigger destructive recovery paths. Grep `except BaseException` in any module that performs file or external-state cleanup and require a justification comment.
- Enforced in: this prevention log

---

### TOCTOU pre-check feeding a destructive rollback path
- First seen in: #118
- Symptom: A pre-flight `_active_credential_exists` SELECT ran outside a lock, then a long destructive sequence (generate root secret, persist file, install key, INSERT credential) ran inside the lock. Under `READ COMMITTED`, a concurrent commit between the pre-check and the INSERT would surface as `CredentialAlreadyExists` *after* the file was already on disk, triggering the rollback path and unlinking the freshly-written root secret.
- Prevention: When a pre-check gates a destructive sequence whose failure handler unwinds setup steps, the pre-check must be re-run inside the same lock as the destructive sequence — they must share one serialisation window. An outer pre-check is fine as a fast-fail optimisation, but the in-lock re-check is the authoritative one. Test pattern: assert that a concurrent commit between the outer and inner check is correctly turned into a 409 *before* any destructive setup runs.
- Enforced in: this prevention log

---

### Mutual-exclusion lock at the call site instead of inside the atomic operation
- First seen in: #118
- Symptom: `recover_from_phrase` ran a verify-then-write sequence (decrypt-check newest credential, then `write_root_secret`) with no lock held internally. The HTTP handler `auth_bootstrap.recover` acquired `lazy_gen_lock` around the call, so the production path was safe — but any future caller (a test, an internal recovery flow, an admin script) that did not happen to take the same lock would race itself.
- Prevention: When a function owns an atomic verify→write or read→modify→write sequence, the lock acquisition belongs *inside* the function, not at every call site. Caller-side locks rot the moment a new caller is added and there is no compiler check that catches it. Use `threading.RLock` if the function may be called by code that already holds the lock; otherwise document the locking discipline at the function level and remove duplicate outer acquisitions. Any state mutation that other lock holders read for their own gating decisions (e.g. setting `broker_key_loaded=True` after a recovery write) must also happen inside the same lock — releasing the lock before the gating mutation re-opens the race for any waiter that acquires it next.
- Enforced in: this prevention log

---

### In-lock re-check on a long-lived `READ COMMITTED` connection adds no isolation
- First seen in: #118
- Symptom: A duplicate-row pre-check was re-run inside a process-level lock to "close the TOCTOU window" against the same `INSERT` issued moments later. Both queries used the same `psycopg.Connection` from the FastAPI `get_conn` dependency, which had been in a `READ COMMITTED` transaction since the start of the request. The re-check provided no additional isolation guarantee against a concurrent writer beyond what the original outer pre-check already gave.
- Prevention: A process-level lock does not buy you DB-level isolation. If you need true serialisation against a concurrent writer between a read and a subsequent write, escalate to a DB-level boundary: `SELECT … FOR UPDATE` on a parent row, a Postgres advisory lock keyed to the natural identity, a `SERIALIZABLE` transaction, or rely on the unique constraint and catch the typed exception. Alternatively, prove via a structural invariant that no concurrent writer is possible (e.g. all writers are gated on a flag you hold + a lock you hold) and document the invariant in the call site instead of issuing a misleading "defensive" SELECT that suggests an isolation it does not provide.
- Enforced in: this prevention log

---

### Modulo bias on `crypto.getRandomValues`
- First seen in: #124
- Symptom: A "pick a uniform integer in `[0, n)`" helper drew a `Uint32` from `crypto.getRandomValues` and applied `% n` directly. Whenever `2^32` is not an exact multiple of `n`, the lower bins (`0 .. (2^32 mod n) - 1`) are returned marginally more often than the higher bins. With `n = 24` the bias per draw is ~1.5e-8 — not exploitable in this UI — but the same component had explicitly chosen `crypto.getRandomValues` over `Math.random` to avoid a "predictable RNG inside a security component" finding, and biased sampling defeats that argument by the same logic.
- Prevention: Any `getRandomValues` result used with `%` for bounded integer sampling MUST use rejection sampling: discard any draw at or above the largest multiple of `n` that fits in the buffer's range, and re-roll. Pattern: `const limit = 2^bits - (2^bits mod n); do { draw = next(); } while (draw >= limit); return draw % n;`. Grep `getRandomValues` for `% ` on the same line or the next line, and require the rejection-loop pattern.
- Enforced in: this prevention log

---

### Stale `useState` initializer when a parent later swaps the prop it reads
- First seen in: #124
- Symptom: A `useState(() => derive(prop))` initializer ran once at mount against an initial prop value (e.g. an empty/invalid phrase before the API response had arrived). When the parent then supplied a valid prop on a subsequent render, the component continued to display state derived from the original prop — the initializer did not re-run, and there was no fallback path to recompute. Specific case: a recovery-phrase confirmation component stuck in the unavailable state even after the parent supplied a valid 24-word phrase, silently stranding the operator.
- Prevention: When a `useState(() => expr(prop))` pattern depends on a prop the parent can legitimately change after mount, add an explicit reset path. Use the React docs' "storing information from previous renders" pattern: track a stable signature of the prop in a sibling `useState`, compare it during render, and call the resetter `setState` calls inline when the signature changes. Do not rely on `useEffect` for this — the reset must be visible in the same commit, not a frame later. As a review checkpoint, grep `useState\(\(\) =>` and for each match ask: "can the parent legitimately change the props this initializer reads, after mount?" If yes, the reset path is mandatory.
- Enforced in: this prevention log

---

### Stale exception-class docstring after collapsing the API mapping
- First seen in: #118
- Symptom: A new exception class (`RecoveryNotApplicableError`) was introduced in round 17 with a docstring asserting it existed so the API could return a *distinct 409*. Round 18 then unified the API mapping back to a generic 400 (per ADR-0003 §6, to prevent failure-mode fingerprinting), but the class-level docstring still claimed the 409 contract. A future maintainer reading the class would have plausible grounds to "restore" the 409 they thought was an oversight, silently breaking the privacy contract.
- Prevention: When an exception class docstring embeds a status-code claim, that claim is part of the contract and must be updated in the same commit that changes the handler `except` mapping. As a mechanical check, grep for `\b(4\d\d|5\d\d)\b` inside `class ...Error` docstrings and cross-reference each hit with the corresponding `except` clause in any HTTP handler that imports the class. If you decide the wire response must be uniform, say so explicitly in the docstring and reference the ADR section that requires it.
- Enforced in: this prevention log

---

### Refresh error swallowed by an overlay opened in the same handler
- First seen in: #125
- Symptom: A handler ran `await createX(...)` then unconditionally `await refresh()` — and on success also opened a fail-closed modal. If `refresh()` failed, its error was set on a list-level `loadError` slot that the freshly-opened modal completely covered. The operator saw their save complete and the modal appear, never knew the list re-fetch had failed, and would only discover stale data after closing the modal — at which point they would plausibly mistake the stale list for evidence that the save itself had failed.
- Prevention: When a click handler can both open an overlay AND run a follow-up data refresh, the refresh must either (a) be deferred until the overlay closes (run from the close handlers), or (b) surface its error on a slot the overlay does not cover. Default to (a) — it gives the operator one source of truth at a time. As a review checkpoint, grep for `await refresh\(` and for each call ask: "is this inside a code path that also opens a modal/dialog/overlay in the same tick?" If yes, the refresh must be moved to the close handlers.
- Enforced in: this prevention log

---

### Unmounted-component setState in async submit handlers
- First seen in: #127
- Symptom: An async form submit handler that awaited a network call and then awaited a follow-up state-refresh (e.g. `await postRecover(); await refreshBootstrapState();`) ran its `finally { setSubmitting(false); }` after the parent had already navigated away — because the refresh transitioned the global session status, which fired a bounce `useEffect` and unmounted the component mid-await. React then warned about a state update on an unmounted component, and any error path inside the same `try` was at risk of calling `setError` against a stale instance. A second hazard in the same shape: the bounce `useEffect` could fire on a *transient* unauthenticated status produced by an intermediate `getMe()` 401 mid-refresh, navigating the operator to `/login` instead of the post-recovery destination.
- Prevention: For any async submit handler that awaits something which can re-enter the global session/router state, add two refs at the top of the component: `mountedRef` (set false in a cleanup effect) and `submittingRef` (set true at the top of the handler, false in `finally`). Route every post-await `setState` through a `safeSetX` helper that early-returns on `!mountedRef.current`. Have any sibling bounce `useEffect` early-return when `submittingRef.current` is true, so transient status flips during the await are no-ops. As a review checkpoint, grep for `async function handleSubmit` (or any async event handler) followed by an `await` of a fetch + an `await` of a refresh; for each, confirm both refs are present.
- Enforced in: this prevention log

---

### API response shapes invented at the type boundary
- First seen in: #127 (PREVENTION raised in review)
- Symptom: A frontend `interface FooResponse { ... }` declared fields that did not exist on the backend response, or omitted fields the backend actually returned. Because `apiFetch<T>` casts the parsed JSON to `T` without runtime validation, the mismatch deserialised silently to `undefined` and only surfaced when a caller actually inspected the missing field — often far from the API boundary. The reverse case (declared field exists but with the wrong type) is even harder to spot because TypeScript happily accepts the unsafe cast.
- Prevention: When adding or changing an API client function, open the backend handler and copy the response model field-for-field into the TS interface in the same commit. Treat the Pydantic class as the source of truth and the TS interface as a mirror. Until we adopt runtime schema validation (Zod parse inside `apiFetch`), the only defence is the manual cross-check: a PR description that touches `frontend/src/api/*.ts` should name the backend file + class it mirrors, and reviewers should be able to grep that class to verify shape parity. Tracked as a tech-debt for runtime validation: see follow-up issue.
- Enforced in: this prevention log

---

### Positional or unscoped selectors in tests select wrong element on reorder
- First seen in: #135
- Symptom: `getAllByText("Clear filters")[0]` and `container.querySelector("button")` both relied on DOM render order to select the target element. If the two filter bars swapped position, the test would silently exercise the wrong component.
- Prevention: When a test needs a specific element among multiple matches, use `within(container).getByRole("button", { name: "…" })` to scope by both container and accessible name. Never rely on positional array indexing or unscoped `querySelector`.
- Enforced in: this prevention log

---

### Broker call with zero or missing order amount
- First seen in: #142
- Symptom: `place_order(amount=None, units=None)` fell through to the by-amount branch with `Amount: 0`, submitting a zero-amount live order to the broker instead of failing fast. The ABC contract allows both parameters to be `None` so the caller can provide exactly one, but without a pre-call guard the provider silently picked a default.
- Prevention: Any broker provider `place_order` implementation must validate that exactly one of `amount`/`units` is non-None before selecting the endpoint branch. Also validate that the chosen value is positive. Guard against unrecognised action strings with an allowlist before any HTTP call.
- Enforced in: this prevention log

---

### UnboundLocalError from variables assigned only inside conn.transaction()
- First seen in: #145
- Symptom: `seeded = result.rowcount` was assigned inside `with conn.transaction()` but the `return SeedResult(seeded=...)` was after the block. If `conn.transaction().__exit__` raises on commit failure, `seeded` is never assigned and the return raises `UnboundLocalError`.
- Prevention: Variables assigned inside a `with conn.transaction()` block must either be (a) initialised before the block, or (b) only referenced inside the block. Prefer returning from inside the block when the return value depends on the transaction succeeding.
- Enforced in: this prevention log

---

### Shared connection state between transaction-managing functions
- First seen in: #145
- Symptom: `sync_universe(conn)` and `seed_coverage(conn)` each opened their own `conn.transaction()` on a shared connection. Without an explicit outer transaction, the connection state between calls depended on psycopg3's implicit transaction behaviour, which is correct but non-obvious and fragile to refactoring.
- Prevention: When two functions that each manage their own `conn.transaction()` are called sequentially on the same connection, wrap them in an explicit outer `conn.transaction()` to make the commit boundary and connection state unambiguous. Document that inner `conn.transaction()` calls become savepoints.
- Enforced in: this prevention log

---

### Fire-and-forget job triggers missing first-time guard
- First seen in: #145
- Symptom: `runJob("nightly_universe_sync")` fired unconditionally after credential save in SetupPage, while SettingsPage correctly gated the same call on `wasCreate`. A returning operator re-entering the wizard would trigger a redundant sync.
- Prevention: Fire-and-forget job triggers in multi-step wizards or forms must be gated on a first-time-only condition (e.g. `mode === "create"` captured before the save). Both SetupPage and SettingsPage trigger paths must stay in sync.
- Enforced in: this prevention log

---

### psycopg v3 rowcount sentinel (-1) treated as valid count
- First seen in: #145
- Symptom: `result.rowcount` is `-1` in psycopg v3 when the server does not report a row count. Code using `if result.rowcount is not None else 0` passes `-1` through to `tracker.row_count`, producing invalid row counts in job history.
- Prevention: Guard `result.rowcount` with an explicit check: raise on `-1` rather than silently clamping with `max()`. A `-1` from a DML statement that should report a count (INSERT, UPDATE, DELETE) indicates a genuine server-side anomaly that must surface as an error, not be hidden.
- Enforced in: this prevention log

---

### Falsy-string suppression in frontend API fetcher params
- First seen in: #147
- Symptom: `if (query.search)` in an API fetcher suppressed the query param for any falsy-but-valid string (e.g. `"0"`). The declared type was `string | null`, but the guard treated `""` and `"0"` identically to `null`.
- Prevention: In frontend API fetcher files, use `!== null` (or `!== undefined`) to gate nullable string params — never bare truthiness. Grep `if \(query\.\w+\)` in `frontend/src/api/` and confirm each match uses explicit null/undefined checks.
- Enforced in: this prevention log

---

### Revoke-then-create sequences must surface partial failure
- First seen in: #144
- Symptom: A credential edit/replace flow revokes the old credential then creates a new one. If the create call fails after revoke succeeds, the credential is silently destroyed — the operator sees a generic error but doesn't know the old key is already gone.
- Prevention: Any client-side revoke-then-create sequence must track whether the revoke has already executed. If the subsequent create fails, surface a specific error message naming the destroyed credential and directing the operator to re-enter it. Use a mode-independent error display (e.g. `actionError` rendered outside conditional form sections) because mode may transition after `refresh()`.
- Enforced in: this prevention log

---

### Multiple ResilientClient instances sharing a rate limit must share throttle state
- First seen in: #168
- Symptom: eToro broker created separate `_http_read` and `_http_write` ResilientClient wrappers with independent `_last_request_at` timestamps. Same issue hit SEC EDGAR where two different `httpx.Client` instances (different hosts) share the same API rate limit. Interleaved calls had no coordination, so combined request rate could exceed API limits without either client detecting it.
- Prevention: When creating multiple `ResilientClient` instances that share a rate limit — whether wrapping the same or different `httpx.Client` objects — pass a shared `list[float]` via the `shared_last_request` parameter. Grep for `ResilientClient(` calls in any provider file — if two or more exist in the same class, verify they share a timestamp list.
- Enforced in: this prevention log

---

### Calendar-day freshness windows must account for weekends
- First seen in: #168
- Symptom: Candle freshness check used `<= 1 day` window, so on Monday, Friday's candle (2 days old) was considered stale and triggered unnecessary API requests — weekend candles never exist for equity markets.
- Prevention: Any freshness check using calendar-day gaps must account for the longest possible non-trading gap (3 days: Friday→Monday). Add explicit weekend boundary test cases (e.g. `today=Monday, latest=Friday`) when writing freshness logic.
- Enforced in: this prevention log

---

### Persist error response body before swallowing HTTP exceptions in providers
- First seen in: #171
- Symptom: `get_quotes()` caught `httpx.HTTPStatusError` on a 500 response but skipped the chunk without persisting the error response body. The 500 body (which may contain diagnostic info from the upstream API) was silently discarded, violating the raw-payload persistence rule.
- Prevention: When catching `httpx.HTTPStatusError` in provider code to skip/continue, call `_persist_raw(tag + "_error", exc.response.text)` before logging or continuing. Network errors (`httpx.RequestError`) have no response body — log with `exc_info` only.
- **Scope narrowed (#470, 2026-04-24):** the "raw payload persistence" imperative only applies to sources whose SQL normalisation is incomplete. Once every structured field lands in SQL (as for `sec`/`sec_fundamentals` post #449/#450/#451/#452/#463), raw disk persistence is redundant, not audit — operator explicitly directed drop-on-process.
- **Further narrowed (#471, 2026-04-24):** `etoro` + `etoro_broker` SQL coverage audit completed and provider-side raw writes dropped. Coverage map:
  - `etoro/instruments` → `instruments` table (provider_id, symbol, company_name, exchange, sector, is_tradable; `currency` enriched separately by FMP per the live-pricing spec; `instrument_type` added in #503 PR 4 for cross-validation against `exchanges.asset_class`).
  - `etoro/candles_*` → `price_daily` (price_date, open, high, low, close, volume).
  - `etoro/rates_batch*` → `quotes` (instrument_id, bid, ask, last_execution, date).
  - `etoro/exchanges` → `exchanges` table (#503 PR 4: provider_id, description; operator-curated `country` + `asset_class` not derived from the API).
  - `etoro/instrument-types` → `etoro_instrument_types` table (#515 PR 1: instrument_type_id, description).
  - `etoro/stocks-industries` → `etoro_stocks_industries` table (#515 PR 1: industry_id, name).
  - `etoro_broker/etoro_portfolio` → `broker_positions` + `cash_ledger` + `copy_mirror_positions` (full position + cash + mirror snapshot).
- **Rule remaining scope:** stands for `companies_house` and `fmp` whose SQL coverage is thinner; raw payloads still serve as parser substrate there until coverage audits land.
- Enforced in: this prevention log

---

### Raw payload persistence must precede raise_for_status()
- First seen in: #177
- Symptom: `_fetch_company_facts` called `resp.raise_for_status()` before `_persist_raw()`, so non-404 HTTP errors (429, 503) raised before the raw payload was written to disk, losing diagnostic data.
- Prevention: In provider HTTP methods, always call `_persist_raw()` before `resp.raise_for_status()`. The persist call captures the response body for auditability regardless of status code.
- Enforced in: this prevention log
- Carve-out (#600): ephemeral provider methods that explicitly do **not** feed any SQL audit trail and whose data does not drive scoring / thesis / recommendations / orders / dividends / tax do not require raw-payload persistence. Such methods must (a) be documented as a carve-out in the provider's class docstring naming the locked design ticket, (b) keep the data process-local (in-process cache OK, disk OK only as a debug aid), (c) be auth-gated at the API layer when they consume external quota. The first such carve-out is `EtoroMarketDataProvider.get_intraday_candles` (chart UI). Adding another carve-out requires reopening the design with an architecture-level review (Codex sign-off + epic update), not a unilateral provider-method addition.

---

### XBRL CapEx sign convention varies between filers
- First seen in: #177
- Symptom: FCF calculation `operating_cf - capex` was incorrect for filers reporting CapEx as a negative number (cash outflow sign convention), inflating FCF.
- Prevention: When subtracting CapEx from operating CF, use `abs(capex)` to normalise for sign convention differences. Apply to both latest-snapshot and historical-snapshot builders.
- Enforced in: this prevention log

---

### Guard-ordering in raise-before-write tests

- First seen in: #184
- Symptom: Test for a guard that is supposed to raise *before* any writes asserted only that a specific write kind (zero-out UPDATEs) was absent from `conn.execute.call_args_list` after the exception. A broken guard that raised *after* one or more real writes could still satisfy the assertion as long as the writes were of a different shape.
- Prevention: When asserting "guard prevents writes," check that the entire write-channel call list is empty at the point of raising — e.g. `assert conn.execute.call_args_list == []` if the production code routes all writes through `conn.execute` — rather than filtering for the specific write shape the guard is supposed to prevent. The strongest form of the assertion is "zero writes of any kind," not "zero writes of the kind I expected."
- Enforced in: this prevention log

---

### Fragile SQL string matching in tests breaks silently under whitespace changes

- First seen in: #184
- Symptom: Test asserted against the exact SQL fragment `"current_units  = 0"` (two spaces, matching column alignment in the production query). Any reformat that changes whitespace would make the substring match return no hits, and the `assert len(matches) == 1` would silently pass or a `not in` assertion would become vacuously true — the test would stop catching regressions without failing.
- Prevention: When matching raw SQL strings in tests, normalise whitespace first (`re.sub(r"\s+", " ", sql)`) and match the normalised form, or — better — extract a named helper (e.g. `_is_zero_out_update(sql)`) so the fragile concern is isolated. Never embed whitespace-alignment in a test literal.
- Enforced in: this prevention log

---

### SQL-shape tests on single-path calls can't exercise the ON CONFLICT branch

- First seen in: #185
- Symptom: Tests for the reset-on-reopen `CASE WHEN positions.current_units <= 0 THEN EXCLUDED.source ELSE positions.source END` asserted the CASE WHEN text appears in the captured SQL after calling `sync_portfolio` once with `local_positions=[]`. Because the mock guarantees the INSERT path (no pre-existing row), Postgres never reaches the ON CONFLICT branch — the assertion passes purely because the text is present in the INSERT string, not because the conflict branch was actually evaluated. A broken CASE WHEN (wrong predicate, swapped arms) would still pass.
- Prevention: SQL-shape assertions are only meaningful for clauses that the single code path being exercised will actually run. For ON CONFLICT / CASE WHEN / trigger-gated logic, either (a) drive the mock to produce an actual conflict and assert on effects, or (b) write a DB-level integration test against a real schema that inserts, reinserts, and reads back the resolved value. Never rely on substring-in-SQL as a proxy for "the conflict branch works."
- Enforced in: this prevention log

---

### Module-level `def` inside a class body silently orphans adjacent test methods

- First seen in: #191
- Symptom: A new module-level `def test_foo()` was inserted between two methods of an existing test class, with the new function placed at column 0 (outdent-to-module). Python's class-body parsing rule is that the first statement at an outer indent closes the class — so the **next** method after the orphaned function was silently reparsed as a nested function inside `def test_foo`, never collected by pytest. No `SyntaxError` fires, `py_compile` passes, targeted pytest for the class shows a test count one lower than before but doesn't fail. The bug hides until the reviewer notices the diff re-indents an existing method.
- Prevention: When adding a module-level test function to a file that already contains test classes, place it **after** every class in the file — never between class methods. Before pushing a test-file diff, run `uv run python -c "import ast; [print(f'{n.name}: {[m.name for m in n.body if isinstance(m, ast.FunctionDef)]}') for n in ast.parse(open('tests/<file>.py').read()).body if isinstance(n, ast.ClassDef)]"` to confirm every expected method is still inside its class. A drop in method count between pre- and post-edit is the canary. `py_compile` is **not** sufficient — the orphaned file is syntactically valid Python.
- Enforced in: this prevention log

---

### Raising inside a shared transaction helper after prior writes rolls back more than the helper's own work

- First seen in: #191
- Symptom: `_sync_mirrors()` raised `RuntimeError` on total mirror disappearance inside `sync_portfolio`'s transaction, *after* position and cash writes had already executed in the same transaction. The rollback discarded legitimate position/cash updates that had nothing to do with the mirror state — a broader blast radius than the spec's "operator investigates mirrors" framing implied.
- Prevention: When adding a `raise` inside a function that is called inside a caller's transaction, explicitly ask: *what other writes have already happened in this transaction when I raise?* If the answer is "writes I did not intend to roll back," hoist the raise to the caller **before** any of those writes run. Document the split scope in the helper's docstring so future callers know the helper no longer owns that guard.
- Enforced in: `.claude/skills/engineering/pre-flight-review.md` section F

---

### Plan documents drift from the prevention log

- First seen in: #193
- Symptom: An implementation plan under `docs/_archive/` contained a code-block showing `assert row is not None` for a DB-contract guard inside `app/services/`. The production code shipped correctly (using `RuntimeError` per prevention #109), but the plan's code block still showed the forbidden `assert` pattern. A future re-run of the plan by a fresh implementation agent would reintroduce the bug directly into production service code — exactly what the prevention-log rule is meant to stop.
- Prevention: When fixing a prevention-log hit during pre-flight review, grep the committed plan document for the same bad pattern and update it in the same PR. Plan files that appear under `docs/_archive/` are treated as historical record, but they also serve as a source of truth if the plan is re-run — so any code snippet that contradicts a prevention rule must be corrected in place. Pre-push check: `grep -rn "assert row is not None" docs/_archive/` should return zero lines touched by the current branch.
- Enforced in: this prevention log

---

### `INSERT INTO instruments` fixtures must supply `is_tradable`

- First seen in: #193
- Symptom: `tests/fixtures/copy_mirrors.py::mtm_delta_mirror_fixture` wrote `INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (...)` without `is_tradable`. The `instruments` table schema (`sql/001_init.sql:1-13`) has `is_tradable BOOLEAN DEFAULT TRUE`, so the insert succeeded on the local dev schema — but every other fixture in the same file explicitly supplies `is_tradable=TRUE`, so the omission read as a bug to a reviewer and also risked silently inserting rows with `NULL is_tradable` on any schema migration that removes the column default.
- Prevention: Every `INSERT INTO instruments` in a test fixture must supply `is_tradable` explicitly, even when the current schema has a default. `is_tradable` is load-bearing for the universe filter (`SELECT ... WHERE is_tradable`), so a `NULL` value silently excludes the row from every production query and corrupts the fixture's contract with whatever test uses it. Pre-push check: `grep -rn "INSERT INTO instruments" tests/fixtures/ | grep -v is_tradable` should return zero lines.
- Enforced in: this prevention log

---

### `_load_sector_exposure` docstring must name the queried-instrument exclusion

- First seen in: #193 (two rounds of the same misreading)
- Symptom: The docstring for `app/services/execution_guard.py::_load_sector_exposure` summarised `total_aum` as `SUM(position mark-to-market) + cash + active mirror equity` without naming the fact that the positions SELECT at line ~269 filters `p.instrument_id != %(iid)s`. The "excluding the instrument itself" note was only attached to the `current_sector_pct` parenthetical above, so two rounds of code reviewer (round 1 BLOCKING #4, round 2 single-finding rerun) read the docstring as "total_aum is the full AUM including the queried instrument". Both raised BLOCKING asks to revert `test_sector_numerator_unchanged_by_mirror`'s `aum_hc = 1650` assertion to `1900`. The test was correct — the filter at line 269 excludes the queried instrument from `total_positions` and therefore from `total_aum` — but the ambiguous docstring forced a REBUTTED-with-evidence round-trip each time.
- Prevention: When a docstring describes a helper whose SQL query filters certain rows, the filter's effect on **every** return value must be stated explicitly in the docstring, not just on the value the filter was originally added for. For `_load_sector_exposure`, the exclusion affects both `current_sector_pct` (intended) and `total_aum` (side effect). If two consecutive reviewers misread the same docstring with the same mistake, treat that as proof the docstring is the bug — not the test. Fix the docstring in place and add a prevention-log entry so the next reviewer reads the correct version. Pre-push check: `grep -rn "total_aum = SUM" app/services/` — every hit should name what is or is not included in the sum.
- Enforced in: this prevention log; `app/services/execution_guard.py:229-244`

---

### `conn.transaction()` without `conn.commit()` silently rolls back in psycopg v3

- First seen in: #216 (Frankfurter ECB rates never persisted)
- Symptom: `fx_rates_refresh` Phase 1 opened `psycopg.connect()`, wrote FX rates inside `conn.transaction()`, but never called `conn.commit()`. In psycopg v3, `conn.transaction()` creates a savepoint — it does NOT commit the outer implicit transaction. When the `with psycopg.connect()` context exits, the connection closes and PostgreSQL rolls back the uncommitted transaction. Every hourly run wrote rates successfully but silently discarded them.
- Prevention: Any `psycopg.connect()` block that writes data must include an explicit `conn.commit()` after the `conn.transaction()` savepoint exits. Pre-push check: `grep -Pzo 'with psycopg\.connect.*\n(?:.*\n)*?.*conn\.transaction' app/` — every match must have a corresponding `conn.commit()` before the connection's `with` block ends. This is the inverse of the "Mid-transaction `conn.commit()` in service functions" entry: service functions that *accept* a connection must NOT commit; code that *owns* the connection MUST commit.
- Enforced in: `docs/review-prevention-log.md`; `app/workers/scheduler.py` (fx_rates_refresh)

---

### External API timestamps must derive from the API response, not `datetime.now()`

- First seen in: #216 (Frankfurter `quoted_at` used `datetime.now(UTC)`)
- Symptom: `fx_rates_refresh` used `datetime.now(UTC)` as the `quoted_at` timestamp for ECB rates, but the Frankfurter API returns a `date` field reflecting the actual ECB publication date. On weekends/holidays this would record today's timestamp against a rate that was 1–3 days stale, misleading any freshness check on `live_fx_rates`.
- Prevention: When writing timestamps that represent "when was this data produced", always use the timestamp from the external source's response. `datetime.now()` is only appropriate for "when did we fetch this" — and even then, prefer the source's own timestamp. Pre-push check: `grep -n "datetime.now" app/workers/scheduler.py` — every hit must be justified in a comment or use a provider-supplied timestamp instead.
- Enforced in: `docs/review-prevention-log.md`; `app/providers/implementations/frankfurter.py` (returns `ecb_date`); `app/workers/scheduler.py` (fx_rates_refresh)

---

### Quote write ownership must be exclusive to one job

- First seen in: #211 (daily_candle_refresh shadowed hourly quotes)
- Symptom: `daily_candle_refresh` called `refresh_market_data` which upserted quotes, overwriting fresher hourly values from `fx_rates_refresh` with stale end-of-day data whenever the daily job ran after the hourly one.
- Prevention: When adding a new scheduled job that writes to a table, grep for all other jobs that also write to that table. If ownership is split, add `skip_*` flags so only one job is responsible. `daily_candle_refresh` must pass `skip_quotes=True`. Check: `grep -rn "_upsert_quote\|INSERT INTO quotes" app/` — only `fx_rates_refresh` and `refresh_market_data(skip_quotes=False)` should write quotes.
- Enforced in: `app/workers/scheduler.py` (`skip_quotes=True` in daily_candle_refresh); `app/services/market_data.py` (`skip_quotes` parameter)

---

### `conn.transaction()` savepoint release does not commit the outer transaction

- First seen in: #231
- Symptom: `with conn.transaction():` on a `psycopg.connect()`-opened connection (autocommit=False) creates a savepoint. The savepoint is released when the block exits, but the outer implicit transaction is not committed. If `conn.commit()` is omitted after the block, writes are silently rolled back when the connection closes.
- Prevention: After any `with conn.transaction():` block on a non-autocommit connection, verify that `conn.commit()` is called before the connection closes. Alternatively, avoid `conn.transaction()` and use a plain `conn.commit()` when savepoint semantics are not needed. Grep `with conn\.transaction\(\):` and verify each is followed by `conn.commit()` within the same `with psycopg.connect(...)` scope.
- Enforced in: this prevention log


---

### psycopg3 cursors are independent — mock cursor sequences are not

- First seen in: #232 (review BLOCKING 2)
- Symptom: Reviewer flagged a "cursor desync" when `BudgetConfigCorrupt` is raised mid-sequence in `compute_budget_state`, claiming it corrupts the cursor position for `_load_sector_exposure`. In production, each `conn.cursor()` creates an independent server cursor — there is no shared iterator to desync. The issue exists only in the test mock pattern where `conn.cursor` returns cursors via `side_effect` from a list.
- Prevention: When a test helper builds a cursor sequence (`_buy_cursors`, `_exit_cursors`, etc.) and includes an exception path that consumes fewer cursors than the happy path, the helper must emit the correct number of cursors for each path. See `_budget_cursors_list(budget_corrupt=True)` which returns 1 cursor instead of 6. Always count the cursors consumed by each branch.
- Enforced in: `tests/test_execution_guard.py` (`_budget_cursors_list` with `budget_corrupt` parameter)

---

### New TEXT columns in migrations need CHECK constraints or Literal types

- First seen in: #232 (review WARNING 1)
- Symptom: `capital_events.currency` was a free-text TEXT column with no CHECK constraint. While SQL injection is blocked by parameterized queries, arbitrary strings persisting in domain columns violate the enum-style semantics.
- Prevention: Before merge, `grep 'TEXT NOT NULL' sql/0*.sql` on new migrations and confirm each user-supplied column has either a CHECK constraint or an enum type. On the API model side, use `Literal["a", "b"]` instead of `str` for columns with constrained values.
- Enforced in: `sql/027_budget_capital.sql` (`chk_capital_events_currency`); `app/api/budget.py` (`Literal["USD", "GBP"]`)

---

### Dependency override save/restore must not gate on `value is not None`

- First seen in: #234 (review BLOCKING 2)
- Symptom: Smoke test saved a `dependency_overrides` entry with `pop(key, None)`, then only restored it when `saved is not None`. If the override was explicitly set to `None` (a valid FastAPI override value), the restore was skipped, permanently deleting the entry for subsequent tests.
- Prevention: When saving and restoring `app.dependency_overrides` entries, track presence separately (`had_key = key in overrides`) and restore unconditionally when the key was present. Do not use the value itself as a presence sentinel.
- Enforced in: `tests/smoke/test_app_boots.py` (`had_get_conn` / `saved_get_conn` pattern)

---

### Loose `string` on API response fields that mirror backend `Literal` types

- First seen in: #236 (review WARNING 3)
- Symptom: `types.ts` used `string` for `cgt_scenario`, `event_type`, `currency`, and `source` fields instead of literal unions. This erodes exhaustiveness checks — callers that destructure the response lose compile-time validation of discriminated values.
- Prevention: When adding a new interface to `types.ts`, if the backend Pydantic model uses `Literal["a", "b"]` for a field, use the matching union type in the TS interface. Grep `types.ts` for bare `string` on fields that correspond to constrained columns.
- Enforced in: `frontend/src/api/types.ts` (`BudgetStateResponse`, `CapitalEventResponse`, `BudgetConfigResponse`)

---

### Infinity/out-of-range numeric inputs bypass `Number.isNaN` guards

- First seen in: #236 (review WARNING 4)
- Symptom: `"1e308"` → `Number("1e308")` → `Infinity`; `Infinity` is not `NaN`, so `Number.isNaN(Infinity)` returns `false`, passing the guard and sending `amount: Infinity` to the backend.
- Prevention: Use `Number.isFinite(v)` instead of `!Number.isNaN(v)` for numeric input validation before API submission. `isFinite` rejects both `NaN` and `±Infinity`.
- Enforced in: `frontend/src/components/settings/BudgetConfigSection.tsx` (event amount guard)

---

### ON CONFLICT DO UPDATE on raw_payload tables must include raw_payload in SET clause

- First seen in: #237
- Symptom: `INSERT INTO broker_positions ... ON CONFLICT (position_id) DO UPDATE SET units, amount, updated_at` omitted `raw_payload` from the update list. If a broker sync race won the INSERT first, the subsequent eBull upsert preserved the sync's payload and silently dropped the eBull raw payload. Same pattern existed in `app/api/orders.py`.
- Prevention: Before pushing any `ON CONFLICT DO UPDATE` on a table with a `raw_payload` column, verify the SET clause includes `raw_payload = EXCLUDED.raw_payload`. Grep `ON CONFLICT.*DO UPDATE` in files that touch `broker_positions`, `copy_mirror_positions`, or any table with `raw_payload`, and confirm each includes the column. If payload should NOT be overwritten, document why in a SQL comment.
- Enforced in: `app/services/order_client.py`, `app/api/orders.py`

---

### ON CONFLICT DO UPDATE must cover all financial columns, not just units/amount

- First seen in: #237
- Symptom: `ON CONFLICT DO UPDATE SET units, amount, updated_at` omitted `total_fees`, `open_rate`, and `open_conversion_rate`. On a race, the losing writer's fee and rate values were silently discarded.
- Prevention: Before pushing any `ON CONFLICT DO UPDATE` on a financial table (`broker_positions`, `copy_mirror_positions`), verify the SET clause covers all columns where the eBull-originated values should be authoritative — especially fees and rates. If a column should intentionally NOT be updated on conflict, add a SQL comment explaining why.
- Enforced in: `app/services/order_client.py`, `app/api/orders.py`

---

### conn.rollback() needed after caught exception on a shared connection

- First seen in: #238
- Symptom: `evaluate_entry_conditions(conn, rec_id)` raises mid-cursor, leaving the connection in `InFailedSqlTransaction` state. Every subsequent `with conn.transaction()` on the same connection fails silently — the rest of the batch is dead.
- Prevention: When a service function calls I/O that may raise on a **shared connection** (one used for multiple sequential operations), wrap the call in `try/except` and call `conn.rollback()` in the except path before attempting any further DB work on that connection. This clears the error state.
- Enforced in: `app/services/deferred_retry.py` (retry_deferred_recommendations error path)

---

### Kill-switch + auto_trading gate at pipeline call sites

- First seen in: #238
- Symptom: `morning_candidate_review()` called `execute_approved_orders()` directly without checking the kill switch or `enable_auto_trading` flag. The guards *inside* `execute_approved_orders` would catch it, but the non-negotiable rule is that AI-generated trade actions must never reach the execution path without an explicit gate at the call site.
- Prevention: Any code path that invokes `execute_approved_orders()` (or any future order-execution function) must check both `get_kill_switch_status(conn)["is_active"]` and `get_runtime_config(conn).enable_auto_trading` before the call. The callee's internal guard is a second line of defence, not the primary one.
- Enforced in: `app/workers/scheduler.py` (morning_candidate_review pipeline trigger)

---

### Interval construction via string concatenation in SQL

- First seen in: #239
- Symptom: `(%(window_days)s || ' days')::INTERVAL` relies on driver-level string concatenation to build an interval value. Safe only when the parameter is always an integer, but fragile — a code change at the call site could introduce injection.
- Prevention: Use `make_interval(days => %(window_days)s)` instead of string concatenation for interval construction. Grep for `|| ' days'` or `|| ' hours'` in SQL strings before pushing.
- Enforced in: this prevention log

---

### Unbounded API limit parameters

- First seen in: #239
- Symptom: `limit: int = 50` with no upper bound allows callers to pass `limit=10000000`, holding a DB connection open for the full scan with no timeout.
- Prevention: Use `Query(default=N, le=1000)` (or appropriate upper bound) for all `limit` parameters in FastAPI routes. Grep for `limit: int =` in router files before pushing.
- Enforced in: this prevention log

---

### `UniqueViolation` scope too broad

- First seen in: #261
- Symptom: `_start_sync_run` wrapped `build_execution_plan` inside the same `try/except psycopg.errors.UniqueViolation` that guarded the `sync_runs` INSERT. A future freshness predicate that uses `ON CONFLICT DO NOTHING RETURNING` (or any other constraint path) would have its UniqueViolation misidentified as a concurrency conflict, silently surfaced as `SyncAlreadyRunning` with a nonsensical `active_sync_run_id` and the real bug hidden.
- Prevention: `except psycopg.errors.UniqueViolation` guards MUST wrap only the exact INSERT statement that can legitimately fire the unique violation. Grep for `except psycopg.errors.UniqueViolation` before push and verify the `try` block contains no additional queries beyond the guarded INSERT.
- Enforced in: `app/services/sync_orchestrator/executor.py` (`_start_sync_run`)

---

### `datetime.now(UTC)` vs DB `now()` in freshness windows

- First seen in: #261
- Symptom: Freshness predicates computed `age = datetime.now(UTC) - started_at` using the Python wall-clock. Under a long-lived planning transaction (or across process/DB time drift), Python time and Postgres time can diverge — for short-window layers (`portfolio_sync` / `fx_rates` at 5 minutes) the boundary comparison flipped spuriously.
- Prevention: For freshness windows shorter than ~10 minutes, do age comparison in SQL (`now() - started_at AS age`) on the same connection that wrote the `started_at` timestamp. Grep for `datetime.now(UTC)` inside freshness-predicate modules and verify it is not being subtracted from a DB timestamp.
- Enforced in: `app/services/sync_orchestrator/freshness.py` (`_fresh_by_audit`)

---

### PREREQ_SKIP inside `_tracked_job` double-writes `job_runs`

- First seen in: #261
- Symptom: `daily_news_refresh` entered `_tracked_job`, reached a no-provider guard deep inside the body, and called `record_job_skip`. `_tracked_job` then exited normally on `return` and wrote its own `status='success'` row — the single invocation produced two `job_runs` rows (one `skipped`, one `success`), breaking fresh_by_audit which reads only the latest row.
- Prevention: PREREQ_SKIP guards (missing creds, unwired providers, absent API keys) MUST live BEFORE the `with _tracked_job(...)` block. Grep for `record_job_skip` inside scheduler job bodies and verify every call-site is outside any `_tracked_job` context.
- Enforced in: `app/workers/scheduler.py` (`_record_prereq_skip` helper is always called outside `_tracked_job`)

---

### Unbound variable after context-manager exit

- First seen in: #261
- Symptom: `refresh_scoring_and_recommendations` assigned `result` and `outcome` INSIDE a `with JobLock + _tracked_job` block, then referenced them AFTER the block. A non-`JobAlreadyRunning` exception propagated out, leaving `result`/`outcome` unbound if a broader `except` were ever added — an `UnboundLocalError` waiting for a future code change to trigger.
- Prevention: When the success path of a `try` assigns variables used in the code after the `try/except`, verify every `except` branch either returns/raises or assigns the same variables. Grep for `result =` or similar assignments inside `with JobLock(...)` contexts and check the post-block readers are unreachable from any exception channel. The adapter pattern now uses explicit `JobLock.__enter__()` + `try/finally JobLock.__exit__()` so the success-path scoping is unambiguous.
- Enforced in: `app/services/sync_orchestrator/adapters.py` (`refresh_scoring_and_recommendations`)

---

### `setSubmitting(false)` missing on a modal submit's success path

- First seen in: #319
- Symptom: A modal's async submit handler reset `submitting=true` before awaiting the POST, then on success called `onFilled()` + `onRequestClose()` without first calling `setSubmitting(false)`. The parent normally unmounts the modal immediately via `onRequestClose`, so the stuck state is invisible in practice — but any future caller that delays unmount (a wrapping confirm dialog, a test reusing the modal instance, a test that does not trigger unmount through the same branch) leaves the submit button permanently locked in `"Placing…"` / `"Closing…"` with no recovery.
- Prevention: In any async submit handler, reset `setSubmitting(false)` on BOTH the success and error branches before handing control back to the parent. Pattern: `setSubmitting(false)` immediately after the `await` resolves, guarded by `mountedRef.current`, before the `onFilled`/`onRequestClose` calls. A bare `finally { setSubmitting(false) }` is an acceptable alternative when the success branch does not need early-return semantics. Grep for `setSubmitting(true)` in modal files and confirm each occurrence has a matching `setSubmitting(false)` on every exit path.
- Enforced in: this prevention log; `frontend/src/components/orders/OrderEntryModal.tsx`, `frontend/src/components/orders/ClosePositionModal.tsx`

---

### `canSubmit` expression omits `!async.loading` during refetch

- First seen in: #319
- Symptom: A modal's `canSubmit` boolean gated on `trade !== null && !submitting && !detail.error` but did not include `!detail.loading`. `useAsync` clears `data` to `null` at the start of a refetch (see `async-data-loading.md`), so this is usually safe — but any defensive caller that preserves prior `data` during refetch, or a future change to `useAsync`'s clear-on-refetch semantics, would leave submit enabled against stale context while the fresh fetch is mid-flight.
- Prevention: Any `canSubmit` / `canProceed` boolean whose inputs include an async data slot MUST also include `!async.loading`. Pattern: `const canSubmit = async.data !== null && !async.loading && !async.error && ...`. Grep `canSubmit` / `canSave` / `canProceed` in frontend modals and confirm each includes an explicit loading guard.
- Enforced in: this prevention log; `frontend/src/components/orders/OrderEntryModal.tsx`, `frontend/src/components/orders/ClosePositionModal.tsx`

---

### `handleSubmit` early-return guard diverges from `canSubmit`

- First seen in: #319
- Symptom: A submit handler's early-return guard checked only a subset of the conditions that `canSubmit` already enforced — e.g. partial-close handler checked `units > 0` but not `units <= trade.units` or `units >= MIN_UNITS`. Under the current call site the submit button is always disabled when any of those fail, so the gap is invisible in practice. A programmatic call (test, future caller that reuses the submit path without the button), or a future refactor that inlines the handler into a different trigger, would skip those additional constraints and POST an invalid payload the backend rejects with a confusing message.
- Prevention: In any handler whose validity is already computed as a `canSubmit` boolean for the button's `disabled` state, the handler's own early-return guard MUST be a strict superset of `canSubmit`'s conditions, OR use `if (!canSubmit) return` as a single gate. Defence-in-depth is cheap; divergence between the UI gate and the submit path is a class of bug that hides until a future caller lands. Grep `async function handleSubmit` in modal files and compare the return guards to the `canSubmit` expression at the top of the component.
- Enforced in: this prevention log; `frontend/src/components/orders/ClosePositionModal.tsx` (`handleSubmit` now re-checks `MIN_UNITS` and `<= trade.units`)

---

### Duplicate structurally-identical types declared in sibling files

- First seen in: #321
- Symptom: `CloseTarget` was declared in `PortfolioPage.tsx` and a sibling `CloseTargetInPanel` interface was declared in `DetailPanel.tsx` with the same fields. The code compiled because TypeScript resolves matching structural shapes, but the two types could drift — adding a field to one without the other, or changing a type, would silently break the prop wire while `tsc` stayed green (until the actual divergent field was read).
- Prevention: When two modules need to name the same shape — e.g. a page passes a target object to a child component and the child destructures it — export the interface from one file and import it in the other. Never declare the same shape twice. Before pushing any PR that adds a new cross-component type, `grep -rn "interface <Name>" frontend/src` should return exactly one match per unique name.
- Enforced in: this prevention log; `frontend/src/components/portfolio/DetailPanel.tsx` exports `CloseTarget`; `frontend/src/pages/PortfolioPage.tsx` imports it.

---

### Hint / warning state with no clear-on-next-transition

- First seen in: #321
- Symptom: A `hint` state was set when the operator pressed `c` on a multi-trade position, displaying a one-line warning. It was never cleared on any subsequent transition (selection change, successful `b`/`c`, `Esc`), so the warning persisted through unrelated actions and looked live even when it was stale.
- Prevention: Any transient operator-facing hint / toast / warning state must have a clear path tied to every transition that could invalidate it. For a hint tied to a keyboard action, clear it in: (a) the same action's success branch, (b) the opposite action's branch, (c) any selection change, (d) the `Esc` reset. Grep `setHint` / `setWarning` / `setNotice` in any new component and verify each state setter has corresponding clear calls — one set = at least one matching clear on every relevant transition.
- Enforced in: this prevention log; `frontend/src/pages/PortfolioPage.tsx` (`setHint(null)` now fires on row click, `Esc`, successful `b`, successful single-trade `c`, and selection changes via keyboard).

---

### Stale closure over derived state in window-level keyboard handlers

- First seen in: #321
- Symptom: A `useEffect` attached a `keydown` listener to `window` and read both `pageRows` and `focusedIdx` from the closure. The deps array included both, so React re-ran the effect on every change — but a rapid key sequence (`j` then `Enter` in the same microtask, a future batched update, or a test harness that flushes without a render between events) could invoke the listener with a stale `pageRows[focusedIdx]` combination from an earlier render. `setState` updater form protects the *setter*, but `setState(prev => ...)` does nothing for *reading* other state in the same handler.
- Prevention: In any `useEffect` that attaches a listener to a shared surface (`window`, `document`, a ref target), each piece of state the listener reads without using the updater form should be carried in a ref that is written on every render (`ref.current = value` at top level of the component body). The effect body then reads `ref.current`, which always has the freshest value. Grep `window.addEventListener` in components and verify every non-setter read of state goes through a ref.
- Enforced in: this prevention log; `frontend/src/pages/PortfolioPage.tsx` (`focusedIdxRef` + `pageRowsRef` synced on every render).

---

### Runtime import for type-annotation-only usage

- First seen in: #333
- Symptom: `app/services/sync_orchestrator/layer_types.py` imported `psycopg` at module level solely for a `psycopg.Connection[Any]` type annotation inside a `Protocol`. The module is declared the bottom of the orchestrator import graph and the only runtime requirement was stdlib + psycopg, but the annotation usage did not need a runtime import (the module already had `from __future__ import annotations`).
- Prevention: When a third-party module appears in this codebase purely inside a type annotation — `Protocol` methods, return types, parameter hints — guard the import under `if TYPE_CHECKING:`. With `from __future__ import annotations` active, the annotations are strings at runtime and do not need the module loaded. Before pushing any change that adds `import <third_party>` to an `app/services/**/*_types.py`, `*_protocol.py`, or any file that begins with a "bottom of the import graph" / "types module" docstring, grep for non-annotation references to the module (`<module>.<member>` outside a type annotation context) — if there are none, move the import under `TYPE_CHECKING`.
- Enforced in: this prevention log; `app/services/sync_orchestrator/layer_types.py` (`import psycopg` now under `TYPE_CHECKING`).

---

### Explicit-tuple isinstance check for exception hierarchies

- First seen in: #336
- Symptom: `classify_exception` used `isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout))` to route transport-layer failures to `SOURCE_DOWN`. The tuple missed siblings in the same `httpx.TransportError` hierarchy (`WriteTimeout`, `PoolTimeout`, `RemoteProtocolError`, `NetworkError`), which silently fell through to `INTERNAL_ERROR` and lost the self-heal signal for DB-or-network transient infrastructure.
- Prevention: When routing on an exception hierarchy, prefer the closest common base class (`httpx.TransportError`, `psycopg.errors.IntegrityError`) to a hand-enumerated tuple of leaf types. Hand-enumerated tuples go stale as the library adds sibling subclasses; base-class dispatch is closed under inheritance. Exception: when the classifier needs to branch *within* a hierarchy (e.g. different HTTPStatusError codes), keep the narrower check but make the branch explicit. When adding a new route, grep the library's `errors.py` or public API to list all leaves — if they share a parent, use the parent.
- Enforced in: this prevention log; `app/services/sync_orchestrator/exception_classifier.py` (now checks `httpx.TransportError` + `psycopg.errors.IntegrityError` base classes).

---

### Wrapper-lambda defeating useCallback memoisation

- First seen in: #405 (#327 frontend subset).
- Symptom: `SetupPage.tsx` passed `onComplete: () => completeWizard()` to `useSetupWizard`. Because `completeWizard`'s identity changes whenever `wizard.state.pendingOperator` changes, the inline arrow creates a new `onComplete` reference every render. The hook's `skipBroker`/`completeWizard` dispatchers list `onComplete` in useCallback deps and re-create on every state tick — defeating the memoisation entirely.
- Prevention: When passing a callback to a hook option, never wrap it in an inline arrow if the inner function's identity can change. Use the ref-and-stable-wrapper pattern:
  ```tsx
  const completeRef = useRef<() => void>(() => {});
  const onComplete = useCallback(() => completeRef.current(), []);
  const wizard = useHook({ onComplete });
  const completeWizard = useCallback(…);
  useEffect(() => { completeRef.current = completeWizard; }, [completeWizard]);
  ```
  This lets the option identity stay fixed while the real implementation rebinds freely. Before pushing any hook-wiring change, check whether the options passed to custom hooks are memoised — an inline `() => foo()` is a red flag whenever `foo` has non-trivial deps.
- Enforced in: this prevention log; `frontend/src/pages/SetupPage.tsx` (`completeRef` + stable `onComplete` pattern).

---

### Empty-parametrize silent pass

- First seen in: #436 (surfaced by Claude review bot on PR #445).
- Symptom: `tests/test_raw_persistence.py::TestProviderWriterDiscipline` used `@pytest.mark.parametrize("path", _iter_provider_files())`. When the generator resolves at collection time and returns `[]` (missing directory, wrong cwd, broken `rglob`, test run from an unexpected root), pytest skips every parametrised case silently with a green summary — the guard looks alive while checking nothing. Especially dangerous for regression-guard tests: the surface they're supposed to cover is exactly the kind of thing that rots without a loud failure.
- Prevention: When a parametrised test's input is a dynamic glob / query / reflection, add a non-parametrised sentinel assertion that the input source returns at least the expected minimum cardinality. Name it `test_<source>_sentinel` so it runs alongside the guard and fails the file if the source degrades. Applies to any `@pytest.mark.parametrize(arg, generator())` where the generator could return empty — glob-based file scans, DB fixture enumerations, manifest reads, `pkgutil.iter_modules` walks, etc.
- Enforced in: this prevention log; `tests/test_raw_persistence.py::test_provider_files_sentinel` pins `_iter_provider_files()` at `>= 10` entries.

---

### Every structured field from an upstream document lands in SQL — no silent drops, no raw-only persistence

- First seen in: #429 (surfaced by operator on PR #448).
- Symptom: Migration 056 and parser-services for Form 4 (#429), 10-K Item 1 business-summary (#428), and 8-K Item 8.01 dividend-calendar (#434) captured a narrow slice of each XML/HTML document and silently discarded the rest. The argument was "v1 scope"; the reality was that every downstream consumer (instrument page, thesis engine, ranking engine, audit trail) has to re-fetch or guess at fields that were already parsed and thrown away. In parallel, provider-level raw dumps accumulated under `data/raw/sec_fundamentals/` (11 GB), `data/raw/sec/` (1.1 GB), `data/raw/etoro/`, etc. — body text landing on disk instead of SQL, violating the operator's rule that every useful field must be queryable.
- Prevention: For any parser/ingester touching a structured upstream document (SEC XML/HTML, Companies House, broker payload), the rule is:
  1. Every element the schema defines lands in SQL — in the same table, a normalised child table, or a JSONB column — unless it is explicitly confidential (credentials, SEC `rptOwnerCcc`) or carries zero analytical value (schema version strings). Any exclusion is justified in the migration comment.
  2. Tombstoning goes on the filing-level row, not a synthetic sentinel in the fact table. `is_tombstone BOOLEAN` on the parent is the pattern.
  3. Provider-body fetches (`fetch_document_text` and equivalents) must flow through a service-layer ingester that normalises into SQL. Disk persistence under `data/raw/*` is for provider-level JSON payloads that are already richly structured and small; body text goes to SQL.
  4. Before merging any new or refactored ingester, the author names every top-level element the upstream document may carry and either (a) shows where it lands in SQL, or (b) justifies the exclusion.
- Enforced in: this prevention log; `sql/057_insider_transactions_richness.sql` (full Form 4 field capture); follow-up tickets for 10-K Item 1 (#449), 8-K Item 8.01 (#450), `data/raw/sec_fundamentals/` normalisation (#451), `data/raw/sec/` normalisation (#452), `fetch_document_text` retirement from disk-only path (#453).

---

### Wrong decimal cap on dollar-valued fields (`_MAX_SHARES` vs `_MAX_PRICE`)

- First seen in: #429 (surfaced by Claude review bot on PR #448 commit 7d6e8d9).
- Symptom: `insider_transactions.underlying_value` is a dollar amount (the reported market value of the underlying security when share count isn't meaningful) but was parsed with `_safe_decimal(..., max_value=_MAX_SHARES)` — the share-count cap (1e10) instead of the dollar cap (1e9). A malformed filing between 1e9 and 1e10 dollars would pass validation. Copy-paste from the adjacent `underlying_shares` line.
- Prevention: Whenever a new `_safe_decimal(...)` call is added, the `max_value` must match the unit of the field: share counts → `_MAX_SHARES`; dollar amounts (prices, values, fees) → `_MAX_PRICE`. Grep for `_safe_decimal(` at self-review time; any field name containing `price`, `value`, `cost`, `fee`, `amount` (dollar contexts) must use `_MAX_PRICE`. Any field name ending `_shares` / `_quantity` must use `_MAX_SHARES`.
- Enforced in: this prevention log; `app/services/insider_transactions.py::_parse_one_transaction`.

---

### Test-teardown list missing new FK-child tables

- First seen in: #429 (surfaced by Claude review bot on PR #448 commit 7d6e8d9).
- Symptom: A migration added new child tables (`insider_filings` / `insider_filers` / `insider_transaction_footnotes`) that FK into the existing tree. `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` was updated only to add the existing table name (`insider_transactions`). Because `TRUNCATE ... CASCADE` on the parent (`instruments`) cascades only through existing FK chains, any test that writes a row into a new child without its instrument row leaks the row into the next test. The bug is silent: the CASCADE looks like it covers the new tables because one of them *is* in the list, but the siblings aren't.
- Prevention: When a migration adds any table with a FK relationship, update `_PLANNER_TABLES` in `tests/fixtures/ebull_test_db.py` in the same commit. List every new table in child-to-parent order (even when the CASCADE would theoretically pick them up) so teardown is deterministic against FK rewrites. At self-review: grep the migration diff for `REFERENCES` and confirm every referenced / referencing table in the new shape appears in the teardown list.
- Enforced in: this prevention log; `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES`.

---

### DELETE-then-INSERT helper without a savepoint can commit an empty snapshot

- First seen in: #449 (surfaced by Claude review bot on PR #460).
- Symptom: `upsert_business_sections` issued a `DELETE FROM instrument_business_summary_sections WHERE instrument_id = %s AND source_accession = %s` followed by an INSERT loop. The caller ran the helper inside a wider `try` that logged and continued on failure; if any INSERT raised mid-loop (e.g. a UNIQUE violation on a malformed payload), the exception was caught and the caller still called `conn.commit()` on the outer unit — committing the DELETE alone. Result: a stored snapshot for the accession would silently become an empty list.
- Prevention: Any helper that clears-then-repopulates rows inside the same connection MUST wrap the clear + repopulate in a `with conn.transaction():` savepoint so a mid-loop failure rolls back the DELETE too. Alternatively, use ON CONFLICT DO UPDATE upserts (no DELETE) when the table has a natural conflict key. At self-review: grep for `DELETE FROM ... WHERE ...` followed by an `INSERT` in the same function, and confirm the pair is atomically scoped.
- Enforced in: this prevention log; `app/services/business_summary.py::upsert_business_sections` now uses `with conn.transaction():`. Regression pinned by `tests/test_business_summary_ingest.py::TestBusinessSectionsIngest::test_insert_failure_rolls_back_delete_atomically`.

---

### `str(row[N])` coerces SQL NULL to the literal string "None"

- First seen in: #450 (surfaced by Claude review bot on PR #461).
- Symptom: `_load_item_labels` used `str(r[2])` when building the `(label, severity)` lookup from `sec_8k_item_codes`. The schema today has `severity` NOT NULL, so the bug was latent — but the moment a future migration relaxes the constraint, every NULL severity would silently serialise to the literal four-character string `"None"` in the loaded dict and then propagate into `eight_k_items.severity` unnoticed.
- Prevention: In any DB reader helper, `str(row[N])` is only safe when the underlying column is NOT NULL. Before wrapping a column with `str()` / `int()` / `bool()` / `Decimal()`, confirm the schema declares it NOT NULL. For nullable columns, use the Optional-aware pattern: `val if val is None else str(val)` (and widen the return type). At self-review: grep for `str\(r\[|str\(row\[` in service modules and audit each occurrence against the source schema.
- Enforced in: this prevention log; `app/services/eight_k_events.py::_load_item_labels` now preserves NULL severity as Python `None`.

---

### Provider body-text fetches require a SQL-normalisation path (no disk-only persistence)

- First seen in: #448 (directive); #453 (guard shipped).
- Symptom: Any new service-layer caller of `SecFilingsProvider.fetch_document_text` that writes the returned body to `data/raw/*` without a matching normalised SQL table silently reintroduces the "body text on disk only" anti-pattern that the operator rejected at #448.
- Prevention: `tests/test_fetch_document_text_callers.py` pins the allow-listed caller set. Adding a new caller requires the test to be updated alongside a documented normalisation path into SQL (e.g. a dedicated table with every structured field captured as rows / columns / JSONB). For ad-hoc body inspection (debugging, one-off investigation), use a script outside `app/` — never add a service-layer caller without the normalisation pipeline.
- Enforced in: this prevention log; `tests/test_fetch_document_text_callers.py`; the docstring on `SecFilingsProvider.fetch_document_text` in `app/providers/implementations/sec_edgar.py` states the contract explicitly.

---

### Empty query-string params on third-party URLs

- First seen in: #562.
- Symptom: `secViewerUrlFor` built `cgi-bin/viewer?action=view&cik=&accession_number={naked}` with an empty `cik=` param. The SEC iXBRL viewer silently fails to load when CIK is missing, so every fallback link was broken without user visibility.
- Prevention: When a URL builder accepts an optional ID (e.g. CIK) and the third-party system requires it, return `null` and skip the link rather than embedding `id=` empty. Document the dependency and the follow-up work to plumb the missing data. At self-review: grep URLs for naked `=&` or `=\)` patterns that indicate missing query params.
- Enforced in: this prevention log; PR #562 fix uses EDGAR full-text search (works without CIK) as interim fallback, with a follow-up to plumb CIK into the response schema.

---

### Silent async error swallow in render branch

- First seen in: #562.
- Symptom: `Tenk10KDrilldownPage` had two parallel `useAsync` calls (`sectionsState`, `historyState`) but only one branch checked `.error`. When `historyState.error !== null`, the right rail silently rendered an empty filings list with no retry / notice — user can't tell whether the data is genuinely empty or the fetch failed.
- Prevention: When adding a `useAsync` call alongside others, every `xState.error` must appear somewhere in the render branch — either as a `SectionError` (with retry), an inline notice, or a deliberate skip. At self-review: grep for `useAsync` in the file and confirm each returned `error` field is referenced in the JSX.
- Enforced in: this prevention log; PR #562 fix shows an inline amber notice in the right rail when `historyState.error !== null`.

---

### Dead props on component interfaces

- First seen in: #566.
- Symptom: `DensityGrid` declared `thesis` and `thesisErrored` on its `DensityGridProps` interface; callers passed them; the function signature destructured neither and used neither. Silent dead path — TypeScript accepts it, callers think they're influencing render, no behaviour changes.
- Prevention: Every prop in an exported component interface must appear in the destructuring parameter list of the function. At self-review: visually scan each `interface XProps {...}` in a diff and confirm each field is destructured + referenced in the body. The TS strict check `noUnusedParameters` doesn't catch this case because the destructured object as a whole is used.
- Enforced in: this prevention log; PR #566 fix removes the dead props from the interface and call site.

---

### EmptyState copy depends on filter branch

- First seen in: #571.
- Symptom: `FilingsPane` rendered `"No 8-K / 10-K / 10-Q rows on file"` as its EmptyState description even for non-sec_edgar instruments (where no type filter was applied). The description was provider-specific but was not guarded on the same `isSecEdgar` condition as the filter.
- Prevention: When a filter or branch condition shapes the data that a component fetches, every user-visible string that describes that data must be gated on the same condition. At self-review: grep for all string literals in a component that reference filter-specific domain terms (form types, provider names, data source names) and confirm each is inside the same conditional branch as the corresponding filter.
- Enforced in: this prevention log; PR #571 fix gates the description on `isSecEdgar`.

---

### Conditional-branch CSS class silently untested

- First seen in: #572.
- Symptom: `DensityGrid` retained `overflow-auto` on the dividends+insider combined card (intentional scroll-bound), but the test asserting `.overflow-auto` count only rendered the default fixture where the combined card is hidden (`capabilities:{}`). The test passed with count=0, giving a future refactorer false confidence that *all* panes are free of overflow-auto.
- Prevention: When a CSS class is conditionally applied (present in one render path, absent in another), add a test variant for the branch where the class IS present and assert the exact count. This regression-guards future refactors that remove the class from that branch.
- Enforced in: this prevention log; PR #572 adds the active-branch variant asserting count=1.

---

### Navigation link outside data guard in async pane

- First seen in: #573.
- Symptom: `FundamentalsPane` rendered a "View statements →" `<Link>` as a sibling of the loading/error/empty conditional block inside `<Section>`, so the link was visible during skeleton and error states before data was confirmed present.
- Prevention: Navigation links (and any affordance that implies data is loaded) inside `useAsync`-driven panes must live inside the resolved-data branch, not as unconditional siblings of the loading/error/empty ternary. At self-review: grep for `<Link` in any component that also has `useAsync`, and confirm each link is inside the `state.data !== null` branch or the resolved conditional arm.
- Enforced in: this prevention log; PR #573 fix moves the "View statements" footer into the data-resolved `<>...</>` fragment.

---

### Teardown-step isolation in shared-DB schema fixtures

- First seen in: #631.
- Symptom: A pytest fixture restoring shared `ebull_test` schema in `finally` ran two SQL files sequentially without per-step `try`. If step N (re-running migration 076 to dedupe leftover seeds) ever raised, step N+1 (re-creating the migration 077 partial unique index) would be skipped — leaving the index dropped for every subsequent test in the run, because `apply_migrations_to_test_db` only applies files not yet recorded in `schema_migrations`.
- Prevention: Each step in a multi-step `finally` block that mutates shared DB schema (drop/recreate index, re-apply migration, restore singleton) must be wrapped in its own `try/except`. A failure in step N must not abandon steps N+1..end. Swallow + warn for non-fatal recovery steps; re-raise the final restore so the test framework reports the leak instead of silently corrupting later test runs.
- Enforced in: this prevention log; PR #631 fix wraps the migration 076 dedupe call in its own `try/except` so the migration 077 recreate runs unconditionally.

---

### Multi-query read handlers must use a single snapshot

- First seen in: #395.
- Symptom: GET handlers that issue 2+ sequential reads on the same `get_conn` connection see a fresh READ COMMITTED snapshot per statement. A concurrent writer between Q1 and Q2 produces brief drift — counts and lists disagree, totals and details lag by one. Cosmetic in steady state, hides real bugs in tests, becomes a correctness issue under multi-operator concurrency.
- Prevention: Any read handler that issues 2+ statements whose results must agree (counts, list of items, sub-aggregates of the same set) MUST wrap the reads in `with snapshot_read(conn): ...` from `app.db.snapshot`. The helper opens a REPEATABLE READ transaction so all statements run against one consistent snapshot. At self-review: grep for `cur.execute(` count >= 2 inside any GET handler and confirm `snapshot_read` wraps them, or that the handler's docstring justifies READ COMMITTED.
- Enforced in: this prevention log; PR for #395 introduces `app/db/snapshot.py::snapshot_read` and applies it to `GET /alerts/guard-rejections`. Apply to other multi-query GETs as the pattern is encountered.

---

### UPDATE-by-PK helpers must assert rowcount

- First seen in: #637 (durable order intent for #243).
- Symptom: `_update_order_with_broker_result` issued `conn.execute(UPDATE ... WHERE order_id = %s)` with no rowcount check. If the UPDATE matched zero rows (stale order_id, lost intent INSERT, schema drift) the function returned silently and `order_id` flowed forward as a foreign key into fills / cost records / positions, corrupting referential integrity invisibly.
- Prevention: Any helper that UPDATEs by primary key and threads the same id forward into FK-referencing writes MUST assert `cur.rowcount == 1` (or the equivalent `statusmessage == "UPDATE 1"`) and raise on mismatch. Use the cursor form `with conn.cursor() as cur: cur.execute(...); if cur.rowcount != 1: raise ...` rather than the connection-level `conn.execute(...)` shortcut, since the latter discards `rowcount`. At self-review: grep `conn.execute(\\s*"UPDATE ` in any service that takes an id from a prior INSERT and threads it into later writes; convert to the cursor + assertion form.
- Enforced in: this prevention log; PR #637 fix raises `RuntimeError` when the post-broker UPDATE on the #243 intent row matches anything other than 1 row.

---

### Positional `call_args_list` index in SQL-shape regression tests

- First seen in: #639 (#540 SEC-CIK cohort pins).
- Symptom: SQL-shape regression tests asserted on `mock.call_args_list[1].args[0]` (the second mock-conn execute call). A future refactor that inserts an extra `conn.execute()` earlier in the flow silently shifts the index, so the test asserts on the wrong call and the actual SQL filter regression goes undetected. The test stays green for the wrong reason.
- Prevention: Identify the target call by content filter, not position. Use `[c for c in mock.call_args_list if c.args and "<distinguishing token>" in c.args[0]]` and assert exactly one match before reading the SQL. The distinguishing token should be a noun the query is meant to use (e.g. `external_identifiers`, the table the predicate is enforcing) — not a generic SQL keyword. At self-review: grep `call_args_list\[\d\]\.args\[0\]` and convert each to a content-filter form.
- Enforced in: this prevention log; PR #639 fix replaces both positional indexes with content filters in `test_daily_research_refresh_dedupe.py` and `test_sync_orchestrator_freshness.py`.

---

### Committed git hooks must be 100755 in the index

- First seen in: #642 (#111 pre-push gate hook).
- Symptom: `.githooks/pre-push` was created on Windows and `chmod +x` made it executable on the local FS, but git's stored mode for the file was still `100644` because `core.fileMode` is false on Windows by default. After `git config core.hooksPath .githooks`, git did not execute the hook (non-executable) and the entire pre-push gate silently never ran on subsequent pushes — the very fix-and-repush cycle the hook was supposed to prevent could continue undetected.
- Prevention: When committing a git hook, fix the mode in git's index explicitly with `git update-index --chmod=+x <path>` (works regardless of OS). At self-review: `git ls-files -s <path>` must show `100755`. The repo CI lint job has a guard that fails when `.githooks/pre-push` is anything other than `100755`, so a re-added non-executable hook is caught automatically.
- Enforced in: this prevention log; PR #642 fix sets the mode in the index AND adds the CI guard at `.github/workflows/ci.yml`.

### `CREATE TABLE IF NOT EXISTS` does not add columns to pre-existing tables

- First seen in: #644 (dividend_events.last_parsed_at).
- Symptom: migration 054 declared `last_parsed_at` inside the `CREATE TABLE IF NOT EXISTS dividend_events (...)` block. On any database where `dividend_events` already existed when 054 ran (a partial earlier apply, a manual create, etc.), `IF NOT EXISTS` short-circuited the entire CREATE — the new column was silently never added. `schema_migrations` recorded 054 as applied. Daily `sec_dividend_calendar_ingest` then failed every run with `column de.last_parsed_at does not exist` because the ingester query referenced a column the migration had not actually added.
- Prevention: any column added in a "new table" migration must be paired with an idempotent `ALTER TABLE … ADD COLUMN IF NOT EXISTS` for the case where the table already exists. Either inline in the same migration after the CREATE, or as a follow-up backfill migration (the pattern in `sql/082_dividend_events_last_parsed_at_backfill.sql`). Self-review for any new-table migration: ask "what if this table already exists from an earlier shape?" — if the answer is "the column never lands", add the ALTER.
- Enforced in: this prevention log; PR #644 ships the backfill migration.

### Don't claim `except A, B:` is Python 2 syntax on a 3.14+ project

- First seen in: #644 review (PR #659).
- Symptom: a comment claimed `except KeyError, ValueError:` was Python-2 syntax that would `SyntaxError` on Python 3. PEP 758 (Python 3.14) makes the bare-tuple form legal again as a tuple-of-types catch — equivalent to `except (KeyError, ValueError):`. The project pins `requires-python>=3.14` and ruff format normalises away the parens. The misleading comment risked future contributors trying to "fix" non-broken code.
- Prevention: when the project pins a Python minimum, treat exception-clause forms as a syntax-by-version question. On <=3.13 projects, `except A, B:` parses as `except A as B:` (binds the second name) and IS a real bug; on 3.14+ projects it's the canonical form per PEP 758. Don't write comments claiming "Python 2 syntax" without checking the project's `requires-python` first.
- Enforced in: this prevention log.

### Map-iteration guards must be reachable given the initialiser

- First seen in: #588 review (PR #670).
- Symptom: in an aggregator that builds a `Map<key, {...; count: number}>` by either initialising an entry with `count: 1` on first sight or incrementing an existing entry, the read-out loop included `if (net === 0 && count === 0) continue;`. `count` can never be 0 — the entry exists because at least one row passed every upstream filter and bumped the counter. The guard is dead code, the comment beside it claimed an intent ("hide pure derivative-only entries") that the upstream filter already enforces, and the next reader gets a false signal that some additional filtering happens at this layer.
- Prevention: when writing or self-reviewing a `for (const [k, v] of map.entries())` body, ask "what initial values can `v.<field>` actually hold here?" against the `map.set(...)` initialisers earlier in the same function. If a guard predicate references a value that's unreachable given the initialiser, either (a) delete the guard, (b) move the guard upstream to where the value can actually hit that case, or (c) widen the initialiser to include the value if the guard's intent is real. Same rule for `map.get(...)`-and-default patterns: `(map.get(k) ?? 0) === 0` is unreachable when every `set(k, v)` writes a non-zero `v`.
- Enforced in: this prevention log; PR #670 deletes the dead guard and rewrites the comment to reflect the actual upstream filtering.

### Period-sensitive labels must derive from the period variable, not be hardcoded

- First seen in: #589 review (PR #672).
- Symptom: a Pane title `"Quarterly P&L"` was hardcoded on a page that already exposes a Quarterly ↔ Annual toggle via `?period=…`. Toggling to Annual still rendered the same title, telling the operator they were looking at quarterly numbers when the chart underneath had switched to annual rows. The `scope` line below the title (`"quarterly history"` / `"annual history"`) was period-aware, so the bug only manifested at the pane header — the kind of inconsistency that's invisible in screenshots until someone is mid-review and notices the period mismatch.
- Prevention: any time a page renders a static text label that names a time granularity (`"Quarterly"`, `"Annual"`, `"Daily"`, `"Last 24 months"`, `"TTM"`, etc.), grep the same file for the variable that drives the toggle and confirm every label either (a) reads through that variable, or (b) is period-agnostic at the language level (`"P&L breakdown"`, `"Margin trends"`). Self-review prompt: search the JSX for hardcoded period words and ask, for each hit, *"would this still be true if the user clicked the toggle to the other mode?"*. Bot review prompt: grep for `Quarterly|Annual|Daily|TTM|Trailing` in `pages/`/`components/` against any file that also defines or reads `searchParams.get("period")` / `useState<"quarterly" | "annual">`.
- Enforced in: this prevention log; PR #672 renames `"Quarterly P&L"` → `"P&L breakdown"` so the title is period-agnostic and the scope line carries the granularity.

### Empty-state guards must follow the data shape after a null-handling refactor

- First seen in: #590 review (PR #673).
- Symptom: `buildCumulativeDps` was changed to emit `cumulative_dps: null` for source rows with missing `dps_declared` so the chart shows a gap (instead of a misleading flat line). The empty-state guard above `<AreaChart>` still read `series[series.length - 1]!.cumulative_dps === 0` from the pre-refactor era, when the helper coerced nulls to zero. After the refactor, an issuer whose `dps_declared` column was empty everywhere produced a series of all-null cumulative values; `null === 0` is false, so the guard didn't fire and the chart rendered an empty AreaChart frame (axes, no series) instead of the inline "no data" hint. Tests passed because the test fixtures always had at least one non-null source row.
- Prevention: when a metric helper's null-handling contract changes (null-coerced → null-propagated, or vice versa), grep every call site for the empty-state guards that follow and re-verify each branch against the new contract. The guard must answer "does the user see *any* meaningful series?", not "is the latest scalar zero?". Self-review prompt: after editing a `build*` helper, grep for the helper name across the codebase and read each consumer's `if (...)` guard with the new return shape in mind. Test prompt: every chart component with an inline "no data" branch needs a vitest case where every row of its source data has a null in the relevant field.
- Enforced in: this prevention log; PR #673 widens the cumulative-DPS guard to recognise the all-null case and adds a regression test (`CumulativeDpsChart` "renders the inline no-data hint when every row has null dps_declared").

### Dark-mode token sweeps must produce neither duplicate nor missing partner utilities

- First seen in: #707 review + #709 review (Phase 2 dark-mode rollout, #700 epic).
- Symptom: two independent sweeps that each touch the same className end up with duplicate `dark:text-slate-100`s on one input (PR #707, RecoveryPhraseConfirm:365 — caught by `dark:text-slate-100.*dark:text-slate-100`); a follow-up PR adds `dark:hover:bg-slate-800/40` to a button but leaves its `border-slate-300` without the `dark:border-slate-700` partner the same PR's stated mapping requires (PR #709). Both shapes ship green CI because the existing typecheck + tests do not look at the visual tokens, only the runtime semantics — Tailwind dedupes duplicates at build, so there is no functional regression to catch.
- Prevention: enforced by `frontend/scripts/check-dark-classes.mjs`, wired into the pre-push hook and `frontend-ci.yml`. Three checks per line in every `frontend/src/**/*.tsx`: (a) duplicate `dark:|sm:|md:|lg:|xl:|2xl:`-prefixed utility token; (b) `border-slate-200|300` without a `dark:border-` partner; (c) `hover:bg-slate-50|100` without a `dark:hover:bg-` partner. Run locally with `pnpm --dir frontend dark:check`. New mapping pairs (e.g. extending the gate to text or background utilities later) get added to the script; do not introduce a separate ad-hoc grep step in the hook.
- Enforced in: `frontend/scripts/check-dark-classes.mjs`, `frontend/package.json` (`dark:check` script), `.githooks/pre-push`, `.github/workflows/frontend-ci.yml`.

### Don't add scheduling or job execution to the API process

- First seen in: #719 (the dev-stack wedge that triggered the jobs-out-of-process refactor).
- Symptom: pre-#719 the FastAPI process owned APScheduler, the manual-trigger ThreadPoolExecutor, the sync orchestrator's executor, the reaper, and a boot-time freshness sweep. A long-running job that hung an outbound HTTP call left a Postgres advisory lock + idle-in-transaction conn stranded; uvicorn `--reload` racing against an in-flight job left the worker mid-startup; sync HTTP `time.sleep` inside resilient_client backoff blocked the asyncio event loop. End result: the API would go unresponsive within hours of normal dev work, repeatedly. Multiple band-aid attempts (per-pool isolation, watchdog timers, `--reload-dir` narrowing) did not solve the underlying coupling.
- Prevention: APScheduler, the manual-trigger executor, the sync orchestrator's executor, the reaper, and the boot freshness sweep all live in `app.jobs` (`python -m app.jobs`). The FastAPI process serves HTTP only. Anything on the API side that triggers work goes through `pending_job_requests` + `pg_notify('ebull_job_request', ...)` via the publisher helpers in `app/services/sync_orchestrator/dispatcher.py`. A new `app.state.job_runtime` attribute on the FastAPI app would be a regression — `tests/smoke/test_app_boots.py` asserts the absence. Self-review prompt: when adding a feature that fires work in the background, ask "does this run on a thread the API loop owns?" — if yes, route it through the queue + dispatcher instead.
- Enforced in: this prevention log; `tests/smoke/test_app_boots.py` (asserts `app.state.job_runtime` is not set); `app/main.py` lifespan (no `start_runtime`, no `set_executor`, no reaper); `docs/settled-decisions.md` "Process topology".

### `psycopg_pool.ConnectionPool` needs explicit dead-conn defences

- First seen in: #717 (mid-session 2026-04-30 dev-stack wedge).
- Symptom: backend listened on `:8000` after ~6h uptime but `/health` blocked >30s; only 19s CPU consumed across the entire run, so the worker was wedged on a TCP read, not pinned. Postgres inside `ebull-postgres` container was healthy (10 conns total, 9 idle, 1 active). Default `ConnectionPool(conninfo, min_size=1, max_size=10)` had no TCP keepalives, no `check` validator, no `max_idle`, no `max_lifetime` — when the Docker port-forwarder reaped a half-open conn, the pool kept handing it out and every subsequent `pool.connection().__enter__` blocked the asyncio loop forever.
- Prevention: every `ConnectionPool(...)` call in the codebase must pass: (a) `kwargs={"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 3}` for libpq-level dead-peer detection; (b) `check=ConnectionPool.check_connection` for SELECT 1 validation on every checkout (~1ms overhead, catches conns the OS hasn't yet flagged); (c) `max_idle=600.0` and `max_lifetime=1800.0` to proactively recycle conns so a single bad conn cannot wedge the pool for the rest of uptime; (d) `timeout=15.0` so a saturated/wedged pool surfaces as a 503 instead of an indefinite event-loop block. Use the `_open_pool` helper in `app/main.py` rather than calling `ConnectionPool(...)` directly — adding a third pool with raw constructor args is the regression shape this entry exists to catch.
- Enforced in: this prevention log; `app/main.py::_open_pool` centralises the config; `tests/test_main_pool_hardening.py` pins it. Companion: `uvicorn --reload-dir app` (in `.vscode/tasks.json`, `Makefile`, `stack-restart.ps1`, `README.md`) so test/doc edits don't churn the worker through reload races, which is one path that turns flaky pooled conns into observed wedges.

### Correlated scalar subquery inside a UNION-of-sources CTE

- First seen in: PR #798 review (Batch 1 of #788, ownership rollup
  service).
- Symptom: `_CANONICAL_UNION_SQL` used
  `WHERE h.period_of_report = (SELECT MAX(period_of_report) FROM
  institutional_holdings WHERE instrument_id = %(iid)s)` inline on the
  13F branch of a five-source UNION ALL. The subquery is correlated on
  `instrument_id` and Postgres re-evaluates it for every candidate
  `institutional_holdings` row scanned — for high-13F-filer instruments
  (>300 13F-HR rows for the latest quarter) that's hundreds of MAX
  scans per request inside the rollup endpoint's hot path. The pattern
  is especially attractive in multi-source UNIONs because each branch
  feels self-contained and the CTE-promotion isn't visually obvious.
- Prevention: when a UNION ALL branch needs a scalar bound (latest
  period, max date, etc.) and the bound is keyed on the same instrument
  / entity that the outer query is already filtered by, hoist the
  bound into a leading CTE that runs once per request:
  `WITH latest_X AS (SELECT MAX(...) ... WHERE entity = %(eid)s)`,
  then reference `(SELECT col FROM latest_X)` from inside the UNION
  branch. Self-review prompt: grep new SQL changes for `SELECT MAX(`
  / `SELECT MIN(` inside a UNION branch's WHERE clause and flag for
  CTE promotion.
- Enforced in: this prevention log; `app/services/ownership_rollup.py`
  promotes `latest_13f_period` to a leading CTE in the v3 spec
  implementation.

### Column-side casts in WHERE clauses defeat indexes

- First seen in: #669 review (PR #679).
- Symptom: a bulk resolver landed with `WHERE instrument_id::text = ANY(%(ids)s)` and a parameter list of stringified ids. The cast on the *column side* runs once per row, so Postgres cannot use the primary-key index on `instrument_id` and falls back to a sequential scan over `external_identifiers`. With a 12k-row table and tens of refresh ticks per day, this is the difference between a 5ms indexed lookup and a multi-second scan. The PR was meant to eliminate per-row scan overhead by replacing 12k single-row `_resolve_identifier` queries with one bulk query — the column cast silently re-introduced the very pathology the PR was fixing.
- Prevention: when binding a list of ids (or any parameter) to a typed integer / date / timestamp column, coerce the parameter list to the column's type before binding instead of casting the column. `WHERE int_col = ANY(%(ids)s)` with `[int(i) for i in ids]` is fast; `WHERE int_col::text = ANY(%(ids)s)` with `[str(i) for i in ids]` is sequential. Self-review prompt: grep new SQL changes for `::text =`, `::int =`, `::date =` on the column side of any indexed WHERE predicate; flag for review whenever the cast is on the column rather than the parameter.
- Enforced in: this prevention log; PR #679 coerces the parameter list to int once at the top of `_bulk_resolve_identifiers` and binds against the unmodified `instrument_id` column.

### Interpolating conn.info.host into URLs without a None guard

- First seen in: #816 review (PR #816).
- Symptom: `_cache_database_url` interpolated `conn.info.host` and `conn.info.port` straight into a netloc f-string. Postgres connections opened over a Unix socket have `info.host = None` (and sometimes `info.port = 0/None`), so the resulting URL contained the literal string `"None:5432"` and `psycopg.connect()` raised at runtime. The bug is invisible on Windows / TCP setups but fires the moment a deployment uses Unix sockets — a deferred-detonation pattern that can ship through CI green.
- Prevention: when building a database URL (or any URL) from a `psycopg.Connection.info` object, treat `host` and `port` as optional and fall back: `host = info.host or settings_parsed.hostname or "localhost"`, `port = info.port or settings_parsed.port or 5432`. Same applies to any code path that interpolates `conn.info.*` fields into user-visible strings, error messages, or telemetry. Self-review prompt: grep new code for `info.host` / `info.port` interpolated into f-strings and flag any call site that doesn't have an explicit `or <fallback>` for both fields.
- Enforced in: this prevention log; `app/services/reconciliation.py:_cache_database_url` falls back to `settings_parsed.hostname` / `5432` when `info.host` / `info.port` is None.

### Module-level mutation of `app.dependency_overrides` races under xdist

- First seen in: #904 (`tests/test_api_instruments.py::TestListInstruments::test_negative_offset_rejected` flaked under `-n 4` after #893 enabled xdist).
- Symptom: 47 test files mutate `app.dependency_overrides[get_conn]` at module scope (each setting its own `_fallback_conn` via `setdefault`). pytest-xdist's default round-robin distributor can interleave tests from different files on the same worker, and each file's `_cleanup` restores its own fallback — so adjacent tests see the wrong override and intermittent assertions fail. Each flake costs a full pre-push retry (~5-7 min after #893).
- Prevention: when a test file mutates `app.dependency_overrides` at module scope, pin its tests to a per-file xdist group: `pytestmark = pytest.mark.xdist_group("test_<filename>")`. Combined with `--dist=loadgroup` (set in `pyproject.toml`), this co-locates the file's tests on a single worker without forcing every dependency-override test into the same worker (which would serialize half the suite). Long-term fix is to convert each file's override pattern into a pytest fixture that auto-restores via `app.dependency_overrides.pop(get_conn, None)` in teardown — but the per-file group is the immediate stabilisation. Self-review prompt: when adding a new test file with module-level `app.dependency_overrides[...]`, also add the `pytestmark` group pin.
- Enforced in: this prevention log; `tests/test_api_instruments.py` carries `pytestmark = pytest.mark.xdist_group("test_api_instruments")`.

### Test seed mirrors must replicate production write-through guards

- First seen in: #905 review round 2 (PR #911) — `_seed_form4` mirrored every legacy `insider_transactions` insert into `record_insider_observation` unconditionally. Production ingest in `app/services/insider_transactions.py` filters `is_derivative = FALSE` before calling the writer; the seed mirror dropped that filter, so a fixture that seeded a derivative Form 4 (RSU / option exposure) would inflate `ownership_insiders_current` post-#905 cutover with shares the production path excludes.
- Prevention: when extending a test seeder to mirror legacy → observations write-through, copy every **write-time** `WHERE` / `IF` guard that the production ingest applies before its `record_*_observation` / `refresh_*_current` calls. Distinguish carefully from **read-time** filters in the rollup query — those don't need a seeder mirror (the read path already excludes the row). Specifically: `is_derivative = FALSE` for insider Form 4 (write-side guard at `app/services/insider_transactions.py`), `aggregate_amount_owned IS NOT NULL` for blockholders, `shares IS NOT NULL` for DEF 14A. PUT/CALL 13F-HR option exposures DO get written to observations (production `_record_13f_observations_for_filing` writes EQUITY + PUT + CALL all three) — they're filtered at read in `_collect_canonical_holders_from_current` via `exposure_kind = 'EQUITY'`, NOT at write, so seeders should mirror that and the read path keeps them out of the rollup. Self-review prompt: open the production ingester next to the seeder, list every write-side filter, and confirm each is mirrored.
- Enforced in: this prevention log; `tests/test_ownership_rollup.py::_seed_form4` wraps the `record_insider_observation` mirror in `if not is_derivative:`.

### Raw API payload must be persisted before any parse / normalise step

- First seen in: #914 review round 1 (PR #927) — `cusip_universe_backfill` fetched the SEC quarterly Official List of Section 13(f) Securities and passed the body straight to `parse_13f_list` without writing the raw text to any table. Re-wash after a parser bug discovery would have forced a re-fetch from SEC at 10 req/s against a payload SEC can amend across quarters; the original snapshot would be unrecoverable.
- Prevention: any new job (or service helper) that fetches an external HTTP payload must `INSERT` the raw bytes / text into the appropriate raw-payload table BEFORE calling parser / normaliser code. Tables today: `filing_raw_documents` (per-accession), `cik_raw_documents` (per-CIK reference docs), `sec_reference_documents` (per-quarterly-period reference docs). If none fit, add a new sibling table — do NOT smuggle a non-fitting key into one of the existing PKs (PR 808 BLOCKING already caught the CIK-into-accession_number variant). Self-review prompt: grep new SQL changes for any `urllib.request.urlopen` / `httpx.get` / provider-fetch call site whose returned body flows into a `parse_*` / `_normalise_*` / `json.loads` call without an intervening `INSERT INTO *_raw_*`.
- Enforced in: this prevention log; `app/services/sec_13f_securities_list.py::backfill_cusip_coverage` calls `_store_raw_list` immediately after `fetch(...)` and before `parse_13f_list(...)`.

### `Decimal(<float-or-unknown>)` from third-party numeric returns
- First seen in: #925 review round 1 (PR #931) — wrapper around EdgarTools fed `Decimal(parsed.summary_page.total_value)` where the third-party return type was not contractually pinned. EdgarTools 5.30.2 happens to construct `total_value` from raw XML text via `Decimal(child_text(...))`, so the value is already a `Decimal` and the call is a no-op. A future EdgarTools release that switches to a `pandas.Series` / `float` return (the upstream parser is pandas-DataFrame backed) would silently introduce IEEE 754 rounding into our persisted summary-page total — a deferred-detonation bug invisible until a non-binary-representable filer total ships.
- Prevention: when constructing a `Decimal` from a third-party numeric value (any value the wrapper does not strictly own), coerce through `str()` first: `Decimal(str(x))`. The string form rounds at the XML / wire boundary — never at the float boundary. Same applies to any `Decimal(record["..."])` against a pandas DataFrame column, where a future schema change can flip int64 to float64 silently. Self-review prompt: grep new code for `Decimal\(` followed by anything other than a string literal, integer literal, or another `Decimal`; flag any call site sourcing the value from a library return without an explicit `str()` cast.
- Enforced in: this prevention log; `app/providers/implementations/sec_13f.py::parse_primary_doc` constructs `Decimal(str(table_value_raw))` and inlines a comment explaining the boundary.

### Don't claim snapshot isolation under READ COMMITTED
- First seen in: #995 review round 2 (PR #1001) — a docstring read "single-transaction snapshot ... so a stage transition landing between the two queries cannot produce an internally-inconsistent payload", but the connection ran at Postgres' default ``READ COMMITTED`` isolation, where every statement gets a fresh snapshot. The protection the docstring claimed (cross-statement consistency) was not in force.
- Prevention: when a docstring or comment cites ``conn.transaction()`` as the protection mechanism for cross-statement consistency, either the connection must be set to ``ISOLATION_LEVEL_REPEATABLE_READ`` (or stronger) for the duration, OR the wording must be downgraded to "transaction grouping" / "single transaction" without the snapshot-isolation claim. Self-review prompt: grep new docstrings for the substring "snapshot" near "transaction" — confirm either an isolation-level setter is present or the claim is qualified.
- Enforced in: this prevention log; ``app/api/bootstrap.py::_build_status_response`` docstring downgraded to "transaction grouping" with an explicit note about READ COMMITTED behaviour.

### TOCTOU on singleton state — read-then-mutate without `FOR UPDATE`
- First seen in: #995 review round 1 (PR #1001) — `retry_failed` and `mark_complete` API handlers read the bootstrap_state singleton, checked `status != 'running'`, then called a downstream mutator in a separate statement; a concurrent `/run` between the two could flip state to `running` while the downstream mutator was running, corrupting the in-flight bootstrap.
- Prevention: any read-then-mutate path on a singleton row whose state is the gate (e.g. `bootstrap_state`, `runtime_config`, `kill_switch`) must hold `SELECT ... FOR UPDATE` on that row across both operations. The cleanest implementation puts the `FOR UPDATE` *inside* the mutator and has it raise the same conflict exception the gate emits — that way callers cannot accidentally drop the lock by calling the read separately. Self-review prompt: grep new code for `read_state(...)` followed by a mutation call on the same singleton; confirm the mutator opens `with conn.transaction():` and runs `SELECT ... FOR UPDATE` first.
- Enforced in: this prevention log; `app/services/bootstrap_state.py::reset_failed_stages_for_retry` and `force_mark_complete` both lock the singleton before reading status; raise `BootstrapAlreadyRunning` if the lock-acquisition observed `running`.

### Docstring claims `conn.transaction()` isolation but body is unwrapped
- First seen in: #995 review round 1 (PR #1001) — `_build_status_response`'s docstring asserted "Reads happen inside one ``conn.transaction()``" but the function body had two reads under separate snapshots. Future maintainers reading the docstring would mistakenly trust an isolation guarantee that wasn't implemented.
- Prevention: when a docstring cites `conn.transaction()` as the isolation mechanism, the function body must contain a `with conn.transaction():` covering the cited reads. Inverse also applies: when adding `conn.transaction()` for snapshot isolation, document the contract in the docstring so a later refactor that pulls the wrapper out shows up as a docstring drift in review. Self-review prompt: grep the changed module's docstrings for `conn.transaction()` and confirm each cited callsite has a matching `with conn.transaction():` in the same function body.
- Enforced in: this prevention log; `app/api/bootstrap.py::_build_status_response` now wraps both reads in one `with conn.transaction():` block.

### Post-step DB re-read must fail closed on a missing snapshot
- First seen in: #994 review round 1 (PR #1000) — bootstrap orchestrator re-read the run snapshot after Phase A to decide whether Phase B should fire (`if snap_after_init is not None: ...`); if the read returned `None` (e.g. transient DB blip), `init_failed` stayed `False` and the orchestrator silently spawned Phase B threads against a run whose state was now unknown, racing the finalise step.
- Prevention: any control-flow gate that depends on a re-read of state already mutated earlier in the function must explicitly handle the "snapshot is None / empty" branch as a failure (treat the missing data as the worst-case state, not the optimistic one). Self-review prompt: grep new code for `if <snap> is not None:` pattern around a control-flow decision — every such site must have a paired `else:` that fails closed, not implicitly drop through. Same pattern applies to ``if rows:`` / ``if rows is not None:`` style gates around destructive or downstream-spawning steps.
- Enforced in: this prevention log; `app/services/bootstrap_orchestrator.py::run_bootstrap_orchestrator` treats a missing post-Phase-A snapshot as an init failure and skips Phase B.

### `mark_request_completed` after a fence-skipped run masks the audit trail
- First seen in: #1071 review round 1 (PR #1072) — `_run_manual` called `mark_request_completed(conn, request_id)` unconditionally after `run_with_prelude(...)` returned, even when the prelude wrote a `status='skipped'` `job_runs` row and never invoked the underlying job (full-wash fence held). The queue request was then marked `completed` despite the work not being done — the operator's `/jobs/requests` view showed a successful trigger when in reality nothing ran.
- Prevention: any wrapper that delegates "should this run actually fire" decisions to a callee (prelude / fence / advisory-lock holder) must propagate the callee's invoked-or-not signal back up so queue / audit transitions reflect reality. The wrapper has two terminal states — invoker-ran (mark `completed`) and invoker-skipped (mark `rejected` with the skip reason). Self-review prompt: grep `mark_request_completed` for paired calls with a callable-returning-None / Optional / `bool` skip signal; confirm the success path is gated on the signal, not on the wrapper having returned cleanly. Same pattern applies to any `with X(...)` whose `__exit__` may have suppressed the body.
- Enforced in: this prevention log; `app/jobs/runtime.py::run_with_prelude` returns `bool` and `_run_manual` calls `mark_request_rejected(error_msg='full-wash in progress for this process')` on the `False` path.

### Silent fallback in `.get(key, default)` for financial-semantic dictionary lookups
- First seen in: #925 review round 1 (PR #931) — `_TYPE_CODE_FROM_LABEL.get(type_label, "SH")` silently mapped any unknown EdgarTools `Type` label to the share-count code, mirroring the structure of `_normalise_put_call` but without the `logger.warning` companion the put/call helper carries. A future EdgarTools relabelling of the principal-amount code would silently misclassify bond holdings (PRN) as share counts (SH), corrupting any downstream slice that branches on `shares_or_principal_type`.
- Prevention: when a `.get(key, default)` is introduced in parser / normaliser code where the default carries financial-semantic meaning (a unit code, a currency code, a discretion label, an exposure kind), the fallback path must `logger.warning(...)` so library drift surfaces in the logs before it corrupts persisted data. The same pattern should be applied wherever a constrained `Literal` is collapsed from a freer source-of-truth string. Self-review prompt: grep new code for `.get(<key>, <non-None default>)` in `app/providers/` and `app/services/`; flag any call site whose default is a financial-semantic value and is not paired with a `logger.warning` on the unknown-label branch.
- Enforced in: this prevention log; `app/providers/implementations/sec_13f.py::parse_infotable` warns when an unrecognised `Type` label is observed before defaulting to `SH`.

### `useCallback` deps that include a full `useAsync` hook-return object

- First seen in: #1076 review round 1 (PR #1077) — `frontend/src/pages/ProcessDetailPage.tsx::refetchAll` listed `[detail, runs]` in its `useCallback` deps. `useAsync` returns a fresh `{data, error, loading, refetch, ...}` literal every render, so even though `refetch` itself is wrapped in `useCallback([], [])` and stable, the surrounding object's identity churns each render, ESLint cannot prove which member is depended on, and `refetchAll` was recreated every render. The instability propagated to every downstream `useCallback` (`handleIterate`, `handleFullWashConfirmed`, `handleCancelConfirmed`) that listed `refetchAll` as a dep, defeating their memoisation.
- Prevention: when listing a `useAsync` (or any object-returning hook) in `useCallback` / `useMemo` / `useEffect` deps, destructure the stable sub-fields into local `const` bindings first and list those instead. The pattern is established in `frontend/src/pages/AdminPage.tsx` lines 91-95 ("Extract the refetch refs as local const bindings so ESLint can see their identity..."). Self-review prompt: grep new pages for `useCallback\((.*)=>\s*\{[^}]*\},\s*\[<state>\]\)` where `<state>` is the name of a `useAsync` (or any object-returning hook); refactor to destructure first.
- Enforced in: `.claude/skills/frontend/async-data-loading.md` § "Destructure hook returns into stable refs before listing them in `useCallback` deps".

### `raise` inside `with conn.transaction():` rolls back stop-request bookkeeping

- First seen in: #1078 review round 1 (PR #1079) — `_check_cancel_signal` in `app/services/sync_orchestrator/executor.py` called `mark_observed` then `UPDATE sync_runs` then `raise RuntimeError(...)` on the rowcount-guard impossible path, ALL inside one `with conn.transaction():` block. The bare raise propagates → tx context exit triggers ROLLBACK → `mark_observed` write is discarded → `process_stop_requests.observed_at` stays NULL → `completed_at` stays NULL → the partial-unique active-stop slot is permanently held. Boot reaper does not reconcile sync_run-kind stop rows; the row would be stranded forever. Same shape recurred in `_finalize_sync_run` during PR6 codex pre-push round 2 follow-up — the in-tx late-cancel probe had the raise INSIDE the tx and discarded the cancel writes.
- Prevention: when a function MUST commit some writes (audit / signal observation) AND THEN raise to signal flow control to the caller, capture the data needed for the raise into a local variable, exit the `with conn.transaction():` block (which COMMITs on clean exit), THEN raise. Self-review prompt: grep service code for `with conn.transaction()` blocks containing both a `raise` and an audit / signal-observation write (`mark_observed`, `mark_completed`, INSERT into a `*_log` / `*_audit` / `*_requests` table); restructure so the raise lives outside the tx context.
- Enforced in: `_check_cancel_signal` + `_finalize_sync_run` shape — both capture flow-control state in a local var and raise after the tx context exits.

### f-string SQL interpolation in test helpers

- First seen in: #1083 review round 1 (PR #1084) — `_insert_freshness_with_expected_next_at` in `tests/test_ingest_sweep_adapter.py` interpolated `expected_next_at_offset_minutes` into the SQL via f-string (`f"... interval '{expected_next_at_offset_minutes} minutes' ..."`). The value was developer-controlled (every call site passed a literal `int`), so the runtime risk was nil; the lint risk was concrete: pyright in CI enforces `LiteralString` on `psycopg.execute(...)` query arguments and rejected the f-string string. Locally the strict-mode flag was effectively off, so the gate only fired on CI — wasted credits on the bot before the lint job caught it.
- Prevention: test helpers are not exempt from the parameterisation rule. SQL must be a literal string; dynamic values flow through psycopg parameters (`%s` + the value), or — for things you genuinely cannot bind, like an interval — use `make_interval(mins => %s)` / `(%s::int * INTERVAL '1 minute')`. Self-review prompt: grep `tests/` for `f"""`/`f'''` blocks containing `INSERT INTO` / `UPDATE` / `SELECT` and reject any f-string fragments inside the SQL. The lint already catches it on CI; the discipline catches it before pushing.
- Enforced in: this prevention log; `_insert_freshness_with_expected_next_at` rewritten to use `make_interval(mins => %s)` (commit `bac6723`).

### `as HTMLElement` / `as HTMLButtonElement` cast on `Array.prototype.find` result in tests

- First seen in: #1086 review round 1 (PR #1088) — `frontend/src/components/admin/a11y.test.tsx` opened the cancel-confirm dialog and located the disclosure toggle with `Array.from(dialog.querySelectorAll("button")).find((b) => b.textContent?.includes("More — terminate")) as HTMLButtonElement`. `find` returns `T | undefined`; the `as HTMLButtonElement` cast erases the `undefined` branch. If the disclosure copy ever changes ("More — terminate" → "Show terminate", spec drift, i18n), `moreToggle` becomes `undefined` at runtime and the next `fireEvent.click(moreToggle)` throws a low-signal `TypeError: Cannot read properties of undefined` instead of a clean test failure pointing at the missing accessible name.
- Prevention: in tests, prefer Testing Library's role-and-name queries (`screen.getByRole("button", { name: /More — terminate/ })`) over `querySelectorAll(...).find(...)` followed by a non-null cast. `getByRole` already throws a descriptive `Unable to find a button with the accessible name "..."` when the target is missing, which is exactly the failure mode you want a test to surface. The same shape exists in `frontend/src/components/admin/ProcessesTable.test.tsx` (pre-existing) — leave those for an opportunistic future cleanup, but new tests must use the role-and-name path. Self-review prompt: grep `frontend/src` test files for `(querySelectorAll|getElementsByTagName).*\.find\(.*\)\s*as\s+(HTML|HTMLButton|HTMLInput)`; replace with `screen.getByRole(...)` / `within(scope).getByRole(...)`.
- Enforced in: this prevention log; PR #1088 replaced the cast with `screen.getByRole("button", { name: /More — terminate/ })`.

### Cancel UX must be cooperative-with-checkpoints, never faked hard-kill

- First seen in: #1064 design discussion (pre-PR1, 2026-05-07/08). Operator quote §3.5: *"Restarting jobs but the jobs are still running."* The naïve fix is a hard-kill button that signals the worker to die mid-write — leaves partial rows on disk, no watermark advance, the next run reads a watermark that incorrectly suggests "we got that far" and skips re-fetching the partial scope.
- Prevention: any new lane in the admin control hub that supports cancel MUST emit a cooperative cancel signal that the worker observes at a checkpoint. The worker checkpoint completes the in-flight item (writes are idempotent); the caller transitions the run row to `cancelled` and then calls `mark_completed` to free the partial-unique active-stop slot. Watermark advances during normal in-flight commits; the next iterate reads a clean cursor and re-fetches anything not committed. **Never expose a hard-kill primary action.** When a worker is genuinely stuck (>2× per-process threshold past `last_progress_at`), the escape hatch is "Terminate (mark for cleanup)" via the cancel modal's More disclosure (chosen at cancel time, not as an upgrade path) plus an external jobs-process restart — not a fake-stop button on the row, and not a re-cancel which the partial-unique active-stop index will reject. Self-review prompt: when adding a new mechanism adapter (the `Mechanism` Literal in `app/services/processes/__init__.py`), confirm the cancel path lands at `mark_observed` + caller-side run-row transition + `mark_completed` on `process_stop_requests`, not at a `process.kill()` / signal write. Mechanisms with cancel coverage in PR1-PR6: bootstrap (checkpoint at `app/services/bootstrap_orchestrator.py:488`) and `orchestrator_full_sync` (checkpoints at `app/services/sync_orchestrator/executor.py:724` + `:1074`). Ingest sweeps are read-only (`can_cancel=False`); the API rejects direct sweep cancel with `cancel_not_supported` (`app/api/processes.py:1440`) — operators cancel the underlying scheduled job instead. Generic scheduled jobs write `cancel_requested_at` but only observe if the per-job loop polls `is_stop_requested`.
- Enforced in: `app/services/process_stop.py` state machine (request / observe / complete with partial-unique active-stop slot, cooperative + terminate `StopMode` Literal); `app/services/sync_orchestrator/executor.py::_check_cancel_signal` (in-tx late-cancel probe); FE `CancelConfirmDialog` (cooperative default; terminate is a controlled disclosure, not a primary affordance). Operator runbook: `docs/wiki/runbooks/runbook-cancel-and-resume.md` + `runbook-stuck-process-triage.md`. Settled decision: `docs/settled-decisions.md` `## Cancel UX (#1064, settled 2026-05-09)`.

### Service that accepts external connection must not commit

- First seen in: #819 review round 1 (PR #1121) — `app/services/canonical_instrument_redirects.py::populate_canonical_redirects` took a caller-supplied `psycopg.Connection` AND called `conn.commit()` mid-function. The job wrapper `populate_canonical_redirects_job` opened the connection via `with psycopg.connect(...) as conn:`, so the context manager committed AGAIN on clean exit. Two-commits-from-one-write is harmless in isolation, but the ownership contract is broken: any future caller that stages mutations on the same connection before calling `populate_canonical_redirects` would silently get those mutations flushed by the service's mid-function commit.
- Prevention: a service function that accepts a `conn` parameter does NOT own that connection's commit / rollback. The caller (a job wrapper, an API handler, a test) is responsible for transaction boundaries. The service may use `conn.transaction()` for inner savepoint scoping, but never `conn.commit()` / `conn.rollback()` at the function-level boundary. Self-review prompt: when a service signature is `def f(conn: psycopg.Connection, ...)`, grep its body for `conn.commit()` / `conn.rollback()` — neither should appear unless the docstring explicitly documents the function as "I own the connection's commit boundary" (rare, and almost always wrong for shared services).
- Enforced in: this prevention log; `populate_canonical_redirects` rewritten to leave the commit to its caller (the `_job` wrapper). Existing services that follow the contract correctly: `upsert_cik_mapping` (`app/services/filings.py`) — uses `conn.transaction()` for inner scoping, never calls commit. Counter-example to study: `app/services/sec_first_install_drain.py::run_first_install_drain` — opens its own connection inside, so commit is part of its ownership scope; this pattern is fine because the function does NOT accept an external `conn`.

### KeyError on enum-literal dict lookup when DB row carries unknown value

- First seen in: #935 §5 review round 1 (PR #1125) — ``manifest_parser_audit.py`` initialised the per-source counter dict from ``get_args(IngestStatus)`` (a fixed Literal), then wrote ``by_source[source][status] = int(count)`` for every row returned by ``SELECT source, ingest_status, COUNT(*) GROUP BY ...``. The DB has a ``CHECK`` constraint on ``ingest_status`` that today restricts it to the Literal's values — but a future CHECK relaxation or a direct DB edit would produce a row whose ``status`` is not in the inner dict, and the audit would KeyError mid-loop (HTTP 500 across the endpoint instead of a degraded but functional report).
- Prevention: when iterating SELECT results into a counter dict keyed off a typing.Literal, do NOT assume the DB row's enum-shaped column is a subset of the Literal. Either (a) explicitly skip unknown keys with a log line, (b) use ``dict.setdefault`` for the inner write, or (c) widen the dict's value space to absorb unknowns. The defensive log makes drift observable instead of crashing on it. Self-review prompt: grep for ``dict[A][B] = X`` patterns where A and B are both dynamic — at least one of the lookups needs a guard or a ``.setdefault`` fallback.
- Enforced in: this prevention log; ``manifest_parser_audit.py`` rewritten with the unknown-status guard + WARNING log.

### Bare call after committed savepoint can split raw/manifest status

- First seen in: PR #1126 review round 1 (Claude bot BLOCKING). The 8-K manifest parser at `app/services/manifest_parsers/eight_k.py` committed a `store_raw` savepoint (raw HTML landed in `filing_raw_documents`), then called `_load_item_labels(conn)` + `parse_8k_filing(...)` bare. If the labels read or the parse raised, the worker recorded `raw_status='absent'` on the manifest row — but the raw row already existed. Permanent split between the manifest's view (no raw stored) and the raw table's view (body present); re-runs would re-fetch from SEC + hit a store_raw UPSERT for an already-present body.
- Prevention: in any manifest parser, after a committed-state-changing savepoint (raw store, observation upsert, tombstone insert) every subsequent expression that can raise MUST be wrapped + return a ``_failed_outcome`` (or equivalent) with `raw_status` set to the actual stored state. Self-review prompt: grep parser bodies for `conn.transaction()` blocks and verify every line BETWEEN the savepoint close and the next return statement is inside a try/except. The bare-call pattern is the failure mode; the wrapper is the contract.
- Enforced in: this prevention log; PR #1126's 8-K parser wraps `_load_item_labels` + `parse_8k_filing` in try/except returning `_failed_outcome(..., raw_status="stored")`.

### Manifest parser parse-failure branch must write ingest-log on EVERY exception class

- First seen in: PR #1129 review round 1 (Claude bot WARNING). The SC 13D/G manifest parser at `app/services/manifest_parsers/sec_13dg.py` had two parse-failure branches: `(ValueError, ET.ParseError)` wrote `blockholder_filings_ingest_log` with status='failed'; a separate broad `except Exception` returned `_failed_outcome` without writing the log. Unexpected parser crashes (e.g. `AttributeError`, `RuntimeError`) therefore left no audit-log row while expected schema-error failures did, breaking the operator dashboard's gap-signal consistency for the same logical event.
- Prevention: in every per-source manifest parser, the parse-failure code path must call the source-specific `_record_ingest_attempt` (or equivalent log writer) on EVERY exception class it catches — there is no "expected vs unexpected" exception split that justifies a missing log row. Pattern: a single `except Exception as exc` block that tags the kind in the log error message and writes the log inside a savepoint. Self-review prompt: grep parser bodies for `except (ValueError, ...) as exc:` blocks followed by a sibling `except Exception` block. If the second block does not write the same log row the first block does, collapse them into one broad-except block.
- Enforced in: this prevention log; PR #1129's `sec_13dg.py` collapses the two parse-failure branches into one `except Exception` that always writes the ingest-log row, tagged with `kind="parse error (unexpected)"` vs `"parse error"` so operators can still distinguish the source of the failure.

### Manifest parser upsert exception must discriminate transient vs deterministic

- First seen in: PR #1130 review round 1 (Claude bot WARNING). The Form 4 manifest parser at `app/services/manifest_parsers/insider_345.py` returned `_failed_outcome` with the 1h backoff on every upsert exception, including deterministic constraint violations (`IntegrityError`, `CheckViolation`, `DataError`). The scheduler then re-picked the row on every retry tick and re-fetched the same dead XML from SEC forever — wasted fair-use budget, the manifest stuck in ``failed`` indefinitely. The same retry-loop existed in 8-K (`eight_k.py`), 13D/G (`sec_13dg.py`), DEF 14A (`def14a.py`), and the legacy Form 4 ingester (`insider_transactions.py::_process_candidates`) — issue #1131.
- Prevention: on the typed-table upsert phase of any manifest parser, branch by `psycopg.exceptions` class. Use the canonical helper at `app/services/manifest_parsers/_classify.py::is_transient_upsert_error`: `psycopg.errors.OperationalError` (parent of `SerializationFailure` + `DeadlockDetected` + connection-drop shapes) is transient → keep `_failed_outcome` with the 1h backoff. Everything else under `psycopg.Error` (`IntegrityError` / `DataError` / `ProgrammingError` and subclasses) plus any non-DB Python exception (`ValueError`, `KeyError`, `TypeError`) is deterministic → tombstone the row (via `_write_tombstone` for typed-table parsers; via `_record_ingest_attempt(status='failed')` for log-driven parsers) + return a `tombstoned` outcome so the worker stops re-fetching. Also embed the exception class name in the manifest `error` column via `format_upsert_error(exc)` so the one-shot `tombstone_stale_failed_upserts` backfill can skip transient-shape rows precisely. Self-review prompt: grep parser bodies for `except Exception as exc:` blocks adjacent to a `_failed_outcome(f"upsert error: ...")` return — every one of those needs the `is_transient_upsert_error(exc)` branch + tombstone fallback for the deterministic case.
- Enforced in: this prevention log; `.claude/skills/data-sources/sec-edgar.md` §11.3 (upsert exception discrimination); PR #1131 patches the five parsers + the legacy ingester + adds the backfill sweep + the classifier tests.

### `with conn.transaction()` inside an open implicit psycopg3 tx is SAVEPOINT not COMMIT

- First seen in: PR #1132 review round 1 (Claude bot BLOCKING). `tombstone_stale_failed_upserts` opened an implicit psycopg3 transaction on the candidate `SELECT` (`with conn.cursor() as cur: cur.execute(...)`) and then entered a per-row `with conn.transaction()` loop expecting each iteration to commit independently. psycopg3 actually issues `SAVEPOINT` when `conn.transaction()` is entered while a tx is already live, and `RELEASE SAVEPOINT` on exit — *not* `COMMIT`. Every per-row "commit" was a savepoint release; all tombstone writes were buffered inside the outer implicit tx and only persisted when the caller's `conn.commit()` fired at job end. A mid-loop connection drop would have silently lost every tombstone, contradicting the docstring's "per-row commit so a connection drop mid-loop preserves earlier progress" claim.
- Prevention: before entering a `with conn.transaction():` loop that promises per-row commit, **explicitly close any outer implicit tx with `conn.commit()`**. Without that flush, every iteration is a savepoint, not a top-level transaction. Self-review prompt: grep service-layer functions for `with conn.cursor() as cur: cur.execute(...)` followed by `with conn.transaction():` later in the body — if there's no `conn.commit()` between them and the function's docstring promises per-row durability, the promise is broken.
- Enforced in: this prevention log; PR #1132 adds the explicit `conn.commit()` after the candidate `SELECT` in `tombstone_stale_failed_upserts` + a comment pinning the reason.

### VS Code task `windows.command` with PowerShell syntax must declare `pwsh` shell

- First seen in: PR #1135 review round 1 (Claude bot BLOCKING). `.vscode/tasks.json` `stack: jobs` Windows command embedded inline PowerShell (`Get-CimInstance ... | Where-Object { ... } | ForEach-Object { Stop-Process ... }; uv run python -m app.jobs`) without setting `options.shell.executable = pwsh`. VS Code's default Windows shell honours `terminal.integrated.defaultProfile.windows` (cmd.exe or pwsh depending on user setting); on cmd.exe both the cmdlets and the `;` separator fail. Same shape: `pwsh -NoProfile -File ./script.ps1; cmd2` as a bare command — cmd.exe doesn't honour `;` as a command separator, so `cmd2` is silently dropped (passed as arg to the prior process). Bot caught it on the jobs task; the backend/frontend tasks had the same latent issue.
- Prevention: when a Windows task command contains PowerShell-specific syntax (`;` separator, cmdlets, `$_` pipeline var, `-match`, dotted-script invocation), set `options.shell` to `{ executable: "pwsh", args: ["-NoProfile", "-Command"] }` so the entire command runs inside one pwsh invocation. The repo precedent is the `stack: prepare` task at `.vscode/tasks.json:7-15`. Self-review prompt: grep `.vscode/tasks.json` for `"windows"` blocks; for each one, verify either (a) `options.shell.executable = "pwsh"` is set, or (b) the command is a bare program invocation (`./foo.ps1` or `./foo.exe`) with no shell metacharacters.
- Enforced in: this prevention log; PR #1135 sets `options.shell` to pwsh on all three `stack: backend|frontend|jobs` Windows commands, matching the `stack: prepare` precedent.

### Port-matching regex must anchor both sides of the port number

- First seen in: PR #1135 review round 1 (Claude bot WARNING). `scripts/clear-port.ps1` matched netstat output via `Select-String ":$Port\s.*LISTENING"`. The right side is anchored by `\s` (whitespace after the port digits), but the left side has no anchor before `:`. In practice, the only way `:8000\s` matches inside `:58000 ` would require digit alignment that doesn't actually occur (the chars between `:` and the next whitespace must be exactly `8000`, and they're `58000`), so the substring search is safe — but the regex is fragile under future netstat layout changes (alternative column orderings, IPv6 brackets, foreign-address ports that happen to align).
- Prevention: when matching a port number inside a multi-column protocol-table output (netstat, ss, lsof), parse the columns explicitly and compare the port field numerically — don't rely on substring regex matching. Split on whitespace, identify the local-address column by position, extract the port via `($local -split ':')[-1]`, compare to the target as a string. This survives layout shifts and is immune to coincidental digit alignment. Self-review prompt: grep for `:$port` / `:\$Port` / `:\d+\s.*LISTEN` patterns in shell scripts and rewrite as explicit column parse.
- Enforced in: this prevention log; PR #1135 rewrites `clear-port.ps1` to parse netstat columns and compare the local-address port field numerically.

### Test `failing_jobs` set must resolve stage_key → job_name through the catalogue, not hardcode strings

- First seen in: PR #1147 review round 1 (Claude bot WARNING). `tests/test_bootstrap_orchestrator.py::test_both_ownership_paths_fail_blocks_final_stage` built a `failing` set by hardcoding `"sec_13f_quarterly_sweep"` with only a comment naming the stage_key it underlies. The mapping was correct *at write time* — `JOB_SEC_13F_QUARTERLY_SWEEP` is the underlying job_name for stage_key `sec_13f_recent_sweep` (S21). But if the constant is ever renamed (or the bootstrap-bounded sweep moves to a dedicated job_name distinct from the standalone weekly sweep), the failing set silently no-ops on that stage and the test's `assert statuses["ownership_observations_backfill"] == "blocked"` masks the broken "both paths fail" regression scenario. Same pattern lurks in any test that pokes the dispatcher via `_patch_invokers_with_fakes(failing_jobs=...)`: job_name strings hardcoded in tests rot silently against the catalogue.
- Prevention: in any test that constructs a `failing_jobs` set for `_patch_invokers_with_fakes` (or similar invoker-patching helpers), enumerate stage_keys not job_names, then resolve via `{spec.stage_key: spec.job_name for spec in get_bootstrap_stage_specs()}` lookup. A `KeyError` raises loudly on a missing stage_key; a renamed constant flows through transparently. Self-review prompt: grep test bodies for `failing_jobs=` or `failing_jobs:` followed by a literal string set — every one of those needs the catalogue-resolved indirection.
- Enforced in: this prevention log; PR #1147 replaces the hardcoded `"sec_13f_quarterly_sweep"` literal with a `_job_by_stage[stage_key]` lookup in `test_both_ownership_paths_fail_blocks_final_stage`.

### Aborted tx after `pytest.raises(CheckViolation)` inside an open implicit psycopg3 tx

- Symptom: a test executes ≥ 1 statement on a shared psycopg3 connection (opening an implicit transaction), then triggers a deterministic constraint violation inside `pytest.raises(psycopg_errors.CheckViolation)` (or any other `psycopg_errors.*`). The raise leaves the transaction in `InFailedSqlTransaction` state. Any subsequent statement on that connection — including assertions later in the same test, or implicit work the fixture teardown performs before the explicit `conn.rollback()` — silently fails or no-ops.
- First seen in: PR #1150 review round 1 (Claude bot WARNING). `tests/test_db_lane_family_split.py::test_garbage_lane_rejected` did `INSERT INTO bootstrap_runs ... RETURNING id` (parent row), then `pytest.raises(CheckViolation)` around an `INSERT INTO bootstrap_stages` with a malformed `lane` value. The CheckViolation aborted the transaction the parent INSERT had implicitly opened; the fixture's per-test `conn.rollback()` teardown swept it, but a future contributor adding ANY post-raises statement (`SELECT`, assertion, fixture-internal `TRUNCATE`) on the same cursor would have hit `InFailedSqlTransaction` with no obvious symptom — the test still passes because the assertion in `pytest.raises` already fired.
- Prevention: wrap the failure-expected statement in `with conn.transaction():` so the failure rolls back to a SAVEPOINT and the outer implicit tx survives. Pattern:
  ```python
  with pytest.raises(psycopg_errors.CheckViolation):
      with conn.transaction():
          cur.execute(...)  # raises; SAVEPOINT rolled back
  # Outer tx still alive — subsequent statements OK.
  cur.execute("SELECT 1 FROM parent_row WHERE ...")  # confirms outer tx survived
  ```
  Add the post-raises sanity SELECT so a future regression that drops the savepoint wrapping fails the assertion instead of silently breaking. Self-review prompt: grep test bodies for `pytest.raises(psycopg` — every one needs a containing `conn.transaction()` and ideally a post-raises sanity statement that proves the outer tx survived.
- Enforced in: this prevention log; PR #1150 rewrites `test_garbage_lane_rejected` to wrap the failing INSERT in `ebull_test_conn.transaction()` and assert the parent `bootstrap_runs` row is still readable on the same connection after the raise.

---

### Test-side quota expectations must derive from the worker's sort, not a hand-ordered constant

- First seen in: PR #1180 review round 1 (Claude bot WARNING + PREVENTION). `tests/test_sec_manifest_worker.py` test cases for #1179 fairness computed expected quotas via `compute_quotas(_ALL_SOURCES, max_rows=100, tick_id=0)` against a module-level tuple in registration order, but the worker calls `compute_quotas(sorted(registered_parser_sources()), ...)`. The `_ALL_SOURCES` ordering and the lexicographic sort differ — same N, same max_rows, same tick_id, but different per-source `i` indices and therefore different `+1` slot recipients. The `>=` assertion only passed accidentally because total seeded rows (40) < `max_rows` (100), so Phase B filled the budget regardless of the quota floor. Cardinality changes in the seed would have surfaced the false-positive.
- Prevention: in any test that compares `WorkerStats.processed_by_source` (or any per-source quota allocation) against expected counts, derive the expected mapping by calling `compute_quotas(sorted(registered_parser_sources()), max_rows, tick_id)` AFTER registering the same parsers the worker will see. Hand-ordered constants for the `sources` argument silently drift from the worker's `sorted(...)` call. Self-review prompt: grep test bodies for `compute_quotas(` — every call site that derives an expected mapping for assertions must pass `sorted(registered_parser_sources())` (or another expression that matches the worker's exact sort), not a literal tuple in registration-order shape. Same applies to any future helper whose contract depends on a deterministic sort over a registry.
- Enforced in: this prevention log; PR #1180 round 2 patches `test_case1_every_source_progresses` + `test_case7_topup_no_double_dispatch` to derive quotas from `sorted(registered_parser_sources())` AFTER `_register_all_fakes`.

### Writer-vs-resolver `is_primary` mismatch on `external_identifiers`
- First seen in: PR #1172
- Symptom: `mf_directory.refresh_mf_directory` INSERTed into `external_identifiers` without specifying `is_primary`. The matching resolver `_fund_class_resolver.resolve_class_id_to_instrument` filters `is_primary = TRUE`. If the column DEFAULT were ever changed to `FALSE`, every `class_id` resolution would silently return `None` and every N-CSR observation would tombstone as `EXT_ID_NOT_YET_WRITTEN` forever.
- Prevention: When writing to a table whose readers filter on a flag column (`is_primary`, `is_active`, `is_default`, etc.), set the flag explicitly at the INSERT site — relying on the column DEFAULT couples the writer's correctness to a column-level decision that may be revisited. Grep `INSERT INTO external_identifiers` to verify every writer specifies `is_primary` explicitly. Add an integration-test assertion that round-trips a writer call + a resolver call to lock the contract.
- Enforced in: this prevention log; PR #1172 sets `is_primary = TRUE` explicitly in `mf_directory.refresh_mf_directory` and asserts the flag in `tests/test_mf_directory.py::test_refresh_mf_directory_first_run`.

### Design intent in `ScheduledJob.prerequisite=None` can be silently neutered by a later-added universal gate
- First seen in: PR #1157 (#1155 Layer 1/2/3 wiring) — caught on live-DB smoke 2026-05-16 (#1181).
- Symptom: `sec_daily_index_reconcile` (Layer 2) was declared `prerequisite=None` in #1155 to defend against the `catch_up_on_boot` boot-time-only evaluation trap (missed-yesterday daily-index lost forever otherwise). PR1b-2 of #1064 later added a universal `check_bootstrap_state_gate` that runs BEFORE any per-job prereq in every dispatch path (scheduled fire, catch-up, manual-queue). The Layer 2 carve-out was silently defeated for 9 days before live-DB smoke caught it (6 scheduled fires, all skipped with reason `bootstrap_not_complete`).
- Prevention: When introducing a cross-cutting gate that runs BEFORE per-job `ScheduledJob.prerequisite`, audit every existing job whose `prerequisite=None` is a deliberate carve-out (grep `prerequisite=None` in `app/workers/scheduler.py` and read the surrounding comment). Either preserve the per-job opt-out via an explicit field (the `exempt_from_universal_bootstrap_gate` pattern landed in #1181) or document the removal explicitly in the cross-cutting PR. Treat any `prerequisite=None` with a docstring rationale as a load-bearing signal, not a default. Verification beyond unit tests: run a live-DB smoke against `job_runs` after a representative cadence — `SELECT job_name, status, COUNT(*) FROM job_runs WHERE started_at > NOW() - INTERVAL '1 day' GROUP BY job_name, status` reveals silent-skip patterns immediately.
- Enforced in: `app/workers/scheduler.py::ScheduledJob.exempt_from_universal_bootstrap_gate` field + `tests/test_universal_gate_carve_out.py` registry-invariant allow-list + spec `docs/specs/etl/lane-b-discovery.md` §4.2 + this prevention log.

### Orchestrator adapter target without source-registry coverage
- First seen in: #1183 (boot-time fx_rates_refresh KeyError on 2026-05-16; root cause from #260 PR #262 + PR1a #1064 sequence).
- Symptom: `app/services/sync_orchestrator/adapters.py::_run_with_lock(job_name, ...)` was called with a `job_name` (`fx_rates_refresh`) absent from `SCHEDULED_JOBS`, `_BOOTSTRAP_STAGE_SPECS`, AND `MANUAL_TRIGGER_JOB_SOURCES`. `JobLock.__init__` calls `source_for(job_name)` which raised `KeyError`. The orchestrator layer was logged as failed; the legacy cron path had already been retired by #260, so fx_rates ceased to refresh. #1183 found 6 such adapter targets — all 6 had standalone ScheduledJob rows pre-#260; #260 removed the rows when moving the work into orchestrator cadences; PR1a #1064 later introduced the source-registry requirement, orphaning these names from coverage.
- Prevention: every `_run_with_lock(<job_name>, ...)` call site in `app/services/sync_orchestrator/adapters.py` must have `<job_name>` resolvable via `get_job_name_to_source()`. CI-enforced by `tests/test_job_registry.py::TestOrchestratorAdapterSourceCoverage::test_every_adapter_job_name_resolves`, which walks the adapter AST extracting every `job_name=<literal>` and asserts each is in the registry. When moving a job from standalone ScheduledJob into an orchestrator-driven adapter, EITHER keep the ScheduledJob row OR add to `MANUAL_TRIGGER_JOB_SOURCES` (`app/jobs/sources.py`) in the SAME PR. The grep for "removed ScheduledJob" in a PR diff is the audit trigger.
- Enforced in: `tests/test_job_registry.py::TestOrchestratorAdapterSourceCoverage` + `app/jobs/sources.py::MANUAL_TRIGGER_JOB_SOURCES` comment block.
- Resolution (2026-05-17, #1184): `JobLock` now detects same-source re-entrancy in the same call context via a process-local `ContextVar` (`_HELD_SOURCES` in `app/jobs/locks.py`). The outer orchestrator JobLock's source is recognised by inner adapter acquisitions on the scheduled-cron path; the redundant Postgres acquire is bypassed and the body runs to completion. The source-lock contract is unchanged for cross-process / cross-thread / cross-context acquisitions — manual triggers in the same source still serialise via the real Postgres advisory lock. Composite adapter `refresh_scoring_and_recommendations` also benefits (its previously dormant `morning_candidate_review` KeyError is eliminated by adding the entry to `MANUAL_TRIGGER_JOB_SOURCES`). Spec: `docs/specs/orchestrator/inner-lock-removal.md`. Tests: `tests/test_job_lock_reentrancy.py` (6 cases) + `tests/test_orchestrator_adapter_morning_candidate_review.py` + rewritten same-source cases in `tests/test_joblock_per_source.py` + `tests/test_db_lane_family_split.py` (cross-thread serialisation instead of same-context contention).

### Time-monotonicity assertions that reconstruct the prior value in Python are vacuously true

- First seen in: #1194 (G8 — `tests/test_exchange_directory.py::test_upsert_idempotency_advances_last_seen`). Codex review WARNING.
- Symptom: an upsert-idempotency test asserted `ts2 > ts1 - timedelta(minutes=1)` to prove a second-pass UPSERT advanced `last_seen` past a staled prior value. The check is vacuously true: even if the UPSERT's `DO UPDATE SET last_seen = NOW()` never fired, `ts2 == ts1 ≈ NOW()` which trivially exceeds `ts1 - 1 minute`. The assertion passes regardless of whether the write actually fired, so a regression that drops the `DO UPDATE` clause (or guards it on a condition that never holds) would slip through.
- Prevention: when asserting that a write advanced a monotonically-increasing column, read the actual post-stale DB value BEFORE the next write, then assert strict `>` against that. Never reconstruct the prior value via Python arithmetic on a captured timestamp (`ts1 - delta`) — that's the same value scaled, not the value the next write must beat. Self-review prompt: any `assert <new_ts> > <python_arithmetic_on_old_ts>` is suspect; replace with `stale = SELECT col FROM t WHERE ...` between the stale-update and the next write, then `assert <new_ts> > stale`.
- Enforced in: this prevention log; pattern documented inline at the test site (`tests/test_exchange_directory.py::test_upsert_idempotency_advances_last_seen` post-fix).

### Singleton-row migrations need a boot-time presence guard

- First seen in: 2026-05-18 — operator-reported `/config` returns 503 + slow login. `runtime_config` table on dev DB had 0 rows (manual `SELECT count(*) FROM runtime_config = 0`). Migration `sql/015_runtime_config.sql` had applied successfully (`schema_migrations` row present, applied 2026-05-06) and seeds the singleton via `INSERT ... ON CONFLICT (id) DO NOTHING`. `runtime_config_audit` was also empty (no operator-driven mutations). Forensic dead-end on exact deletion event — most likely a manual `DELETE FROM` during operator triage OR a database restore from a snapshot taken before the seed line landed.
- Symptom: every endpoint that depends on the singleton (`GET /config`, execution-guard for any BUY/ADD/EXIT, order client live-mode check) fails-closed with 503 + `runtime_config_corrupt`. FE polls `/config` on login + retries; user sees slow render.
- Prevention: any migration that creates a singleton (single-row guarded by `CHECK (id = TRUE)` or similar) and uses `INSERT ... ON CONFLICT DO NOTHING` to seed is a **load-bearing one-time write**. Once that singleton vanishes (by operator mistake, snapshot restore from pre-seed era, future bootstrap reset script), nothing re-creates it. Boot-time guard is mandatory:
  1. Add a `ensure_<singleton>_row_present(conn)` helper next to the service module that owns the table.
  2. Call it from `app/main.py` lifespan startup AFTER `run_migrations()` and BEFORE the pool opens. Idempotent: counts rows, re-INSERTs safe-defaults on zero with a WARNING log. Same fail-closed posture (safe-default values).
  3. Cover with a test: drop the row in `ebull_test_conn`; call the helper; assert row exists + values match the safe defaults + WARNING fired.
- Verification step (for any future singleton-table PR): `psql -c "DELETE FROM <table>" -d ebull` (against dev DB) → restart app → `SELECT count(*) FROM <table> = 1` AND `GET /<endpoint-that-uses-it>` returns 200. If both hold, the boot guard is in place; if either fails, the migration is the only writer and a future restore/reset will lose the row.
- Enforced in: this prevention log; `app/services/runtime_config.py::ensure_runtime_config_singleton`; wired into both `app/main.py` lifespan + `app/jobs/__main__.py` boot (`_ensure_runtime_config_singleton_with_cleanup`); tests `tests/test_runtime_config_boot_guard.py`. Spec `docs/_archive/2026-05-18-phase1-tuning-boot-guard.md`. Landed in #1208 Phase 1.

### Postgres on Docker Desktop macOS — defaults blow up partition-heavy workloads

- First seen in: 2026-05-18 — two PANIC events on dev container `ebull-postgres` at 09:05 + 12:54 UTC. `PANIC: could not create file "pg_wal/xlogtemp.NNN": No space left on device` despite host disk having free space. Recovery walked every relfile on fsync (relfile count on `ebull` = 4,723) and took 45+ min.
- Symptom: WAL writer SIGABRTs under autovacuum bursts on the 28 GB unpartitioned `financial_facts_raw` table. WAL fills `pg_wal/` faster than the default `max_wal_size=1024 MB` can rotate. Docker Desktop macOS amplifies the cost because every relfile is fsynced on recovery.
- Prevention: any Postgres container hosting eBull MUST set, at minimum:
  - `max_wal_size >= 4GB`
  - `min_wal_size >= 512MB`
  - `wal_compression = on`
  - `shared_buffers >= 1GB`
  - `maintenance_work_mem >= 256MB`
  - `work_mem >= 16MB`
  - container `mem_limit >= 4g` + `shm_size >= 1g`
- Enforced in: `sql/155_postgres_runtime_tuning.sql` (ALTER SYSTEM + `pg_reload_conf()`, applied via the `-- runner: autocommit` migration-runner directive added in this PR); `docker-compose.yml` (`mem_limit: 4g` + `shm_size: 1g`); spec `docs/_archive/2026-05-18-phase1-tuning-boot-guard.md`. Landed in #1208 Phase 1.
- Verification step: after merge + container restart, confirm `SHOW max_wal_size;` returns `4GB` and `SHOW shared_buffers;` returns `2GB`. Re-run an ingest burst (e.g. `POST /jobs/finra_regsho_daily_refresh/run` with `backfill_window_days=14`) and verify no PANIC entries in `docker logs ebull-postgres`. New mega-tables (>1 M rows / >1 GB) MUST be partitioned at design time; retrofit is #1208 Sub 3.

### Migrations containing ALTER SYSTEM (or other non-transactional commands) need an autocommit directive

- First seen in: 2026-05-18 — Codex 1a review on the Phase 1 spec for #1208. `ALTER SYSTEM cannot run inside a transaction block` (`ERROR: ALTER SYSTEM cannot run inside a transaction block`). The migration runner at `app/db/migrations.py` uses `psycopg.connect()` (autocommit=False) which implicitly opens a tx on the first `cur.execute()`, so an ALTER SYSTEM body fails.
- Prevention: any migration containing PG commands that cannot run inside a transaction block (`ALTER SYSTEM`, `CREATE DATABASE`, `VACUUM`, `REINDEX SYSTEM`, etc.) MUST start with the directive `-- runner: autocommit` on its first non-blank line. The runner detects the directive (strict line-1 check) and applies the file with `autocommit=True` so each statement runs in its own implicit transaction. The migration body MUST be idempotent because a partial failure (body succeeds, `schema_migrations` INSERT fails) re-runs the body on the next boot.
- Enforced in: `app/db/migrations.py::_wants_autocommit` + the autocommit branch in `run_migrations`; `tests/test_migration_runner_autocommit.py` covers the directive parser; reference migration `sql/155_postgres_runtime_tuning.sql`.

### Test-DB leaks accumulate on xdist-worker crash

- First seen in: 2026-05-19 — #1208 Phase 2 spike on the dev cluster found 45 leaked `ebull_test_<epoch>_<hex>_<gw>` databases (11 distinct run-id epochs + 2 legacy `*sanity*` leaks). `pg_database_size(...)` blocked indefinitely on the leaked set (single foreground probe hung >90s, required `pg_terminate_backend`).
- Symptom: `tests/fixtures/ebull_test_db.py::drop_worker_database()` runs in `pytest_sessionfinish` only on graceful exit. A worker SIGSEGV, operator `Ctrl+C`, OOM kill, or OS-level reboot all skip the hook and the per-worker DB is orphaned. The helper also catches every `Exception` in teardown and demotes to `warnings.warn`, so an admin-connect failure during shutdown silently leaks. Leaks accumulate at ~4 DBs per crash run; 45 leaks blocked autovacuum scheduling enough to hang `pg_database_size` on the whole cluster.
- Prevention: any per-process resource that is torn down only in `pytest_sessionfinish` MUST be paired with a controller-level sweep that runs at the START of the next pytest invocation. Specific contract for the Postgres worker-DB lifecycle:
  1. **Session-lifetime keepalive** — every worker holds one autocommit connection to its private DB for the whole pytest session (autouse `_worker_db_keepalive` fixture). Re-exported into `tests/conftest.py` so pytest's fixture discovery picks it up (fixtures defined under `tests/fixtures/` are NOT scanned).
  2. **Orphan sweep on controller start** — `_drop_orphan_workers_older_than` runs inside `build_template_if_stale()` under the existing `EBULL_TEMPLATE_LOCK` advisory lock. Three independent safety rails: name regex (`^ebull_test_(\d{10})_[0-9a-f]{6}_(?:gw\d+|main|sanity\d*)$`), `pg_stat_activity` filter (load-bearing — guaranteed by the keepalive), age backstop (`min_age=timedelta(hours=1)` default), plus a literal `_NEVER_DROP` set as a regex-regression backstop.
  3. **Plain DROP without FORCE** — `DROP DATABASE IF EXISTS {name}` (no `WITH (FORCE)`). PG raises `ObjectInUse` atomically if any backend reconnected in the activity-check gap; sweep catches + skips. Eviction-without-proof-of-ownership failure mode eliminated. The existing `_drop_database_force()` helper is retained for the per-run teardown path where the caller demonstrably owns the DB.
  4. **CI short-circuit** — `os.getenv("CI") == "true"` returns empty list; CI containers are ephemeral so leaks are zero-cost there.
- Enforced in: this prevention log; `tests/fixtures/ebull_test_db.py::_drop_orphan_workers_older_than` + `::_worker_db_keepalive`; `tests/conftest.py` re-export; `tests/test_orphan_sweep.py` (4 cases covering each rail); `.claude/skills/engineering/test-quality.md` §"Dev-DB isolation invariant" + §"Slim test-data posture". Spec `docs/_archive/2026-05-19-phase2-test-fixture-orphan-sweep.md`. Landed in #1208 Phase 2.
- Verification step: pre-sweep `SELECT count(*) FROM pg_database WHERE datname LIKE 'ebull_test_%' AND datname != 'ebull_test_template'` returned 45; after a single `uv run pytest tests/test_orphan_sweep.py` the count dropped to 0. Slim-data audit on the rebuilt `ebull_test_template` returned exactly one non-zero `pg_class.reltuples` row (`schema_migrations | 133` — migration tracking only). Migrations honour schema-only posture; no bulk-fixture defects found.

### Unpartitioned mega-tables autovacuum-burst the WAL — partition + sweep retention horizons

- First seen in: 2026-05-18 — the same dev-DB PANIC sequence that motivated the prior Postgres-on-Docker entry. Root cause split into two halves: half is container tuning (entry above); half is the table itself. `financial_facts_raw` had grown to **28 GB / 62 M rows** because the table was un-partitioned AND no retention sweep enforced the §13 horizons. Autovacuum on a single 28 GB heap generates a multi-GB WAL burst that the default `max_wal_size=1024 MB` cannot rotate fast enough → WAL writer PANICs.
- Symptom: autovacuum bursts on a single mega-heap fill `pg_wal/` faster than checkpoint can rotate. Recovery walks every relfile on fsync (relfile count = 4,723 on dev pre-partition); Docker Desktop macOS magnifies the cost. Tuning (Phase 1) raises the headroom but doesn't fix the underlying burst.
- Prevention: any table that crosses **1 M rows OR 1 GB on disk** MUST be designed as `PARTITION BY RANGE (<time-axis>)` from day one (or retrofit immediately on the first cross), AND the source's `.claude/skills/data-engineer/SKILL.md` §13 horizon MUST have a corresponding retention sweep enforced as a `ScheduledJob` daily. Documented horizons without an enforcer mean the table grows forever — that's the trap.
- The retention sweep MUST follow the service-no-commit + autocommit-conn rules:
  - service function takes a conn + does NOT enter `with conn.transaction()` (Phase 1 lesson §"psycopg3 service-no-commit invariant"),
  - orchestrator opens `psycopg.connect(url, autocommit=True)` then iterates units (instruments / shards) with `with conn.transaction()` per unit so each becomes a real top-level tx (not a SAVEPOINT — Codex 1b BLOCKING #2 on the Phase 3 spec).
- Partition retrofit specifics — every block below was caught by Codex on the Phase 3 spec; future partition retrofits must apply ALL of them:
  1. **Drop + re-create dependent views around the swap.** `DROP TABLE` cascades to dependent views even when they reference the table by name (PG binds them by OID at create time). Identify them via `pg_views` + `pg_depend`; drop in reverse-dependency order; re-create verbatim after the rename. Pin the view bodies inline in the migration so re-create is self-contained.
  2. **Detach BIGSERIAL-owned sequence before DROP TABLE.** `BIGSERIAL` creates a sequence `OWNED BY` the column. `DROP TABLE` cascades to the sequence — the new table's `DEFAULT nextval(...)` then breaks. Pre-step: `ALTER SEQUENCE ... OWNED BY NONE`. Post-rename: re-attach to the new column.
  3. **LOCK TABLE IN SHARE MODE before INSERT-SELECT.** Without this, late writes during the bulk copy commit on the old table but don't make it into the snapshot — they vanish at swap. SHARE MODE blocks INSERT/UPDATE/DELETE for the migration's tx duration. Permits concurrent reads.
  4. **Defer index builds until after the bulk copy.** Building indexes on an empty table then populating drags every INSERT through three btree-maintenance paths; build once on the populated table.
  5. **Use `_new` suffix for indexes during the swap.** Index names are schema-global. Rename to canonical names AFTER `DROP TABLE` of the old; explicitly rename the implicit `<table>_pkey` index (`ALTER TABLE ... RENAME` does NOT rename it).
  6. **Pre-1900 + far-future junk → DEFAULT, not the lowest quarterly catch.** Partition's lower bound MUST be a meaningful real date (e.g. `'1900-01-01'`), never `MINVALUE`. Out-of-window XBRL parser detritus belongs in the DEFAULT partition where the operator can triage it without mixing with valid historical data.
  7. **Retention by DISTINCT accession family, not by raw row count.** A 10-K filing has hundreds of XBRL facts under one accession; `ROW_NUMBER` over raw rows evicts facts 4..N of the latest filing while keeping facts 1..3, which destroys the accession's atomicity. Rank DISTINCT accessions (including amendments, e.g. `10-K/A` shares the `10-K` family budget), then DELETE all rows of out-of-horizon accessions.
  8. **Orchestrator sweep tests must NOT exact-assert `rows_deleted`.** Any `sweep_<table>_all_instruments(database_url=...)` integration test that uses the shared test DB (worker template clone) cannot reliably assert an exact `summary.rows_deleted` value — the orchestrator iterates EVERY instrument with swept-family facts, so any stray leftover from a future co-located test pushes the count above the seeded budget and flakes the assertion. The robust contract is: assert a floor on `rows_deleted` (`>= seeded_eviction_count`) AND assert per-instrument visibility from a SEPARATE conn (`SELECT count(*) FROM <table> WHERE instrument_id IN (...)` matches the post-sweep expectation). The per-instrument check is what proves the orchestrator's autocommit + per-instrument `with conn.transaction()` produced REAL top-level commits (a SAVEPOINT-only path would leave the changes invisible to a fresh conn). Caught on PR #1215 bot review for `test_orchestrator_commits_per_instrument_autocommit_contract`.
- Enforced in: `sql/156_financial_facts_raw_partition.sql` (swap-rename + 86 partitions + 5 indexes + view re-create); `app/services/financial_facts_retention.py` (service-no-commit retention sweep with autocommit orchestrator); `app/workers/scheduler.py::financial_facts_retention_sweep` + `JOB_FINANCIAL_FACTS_RETENTION_SWEEP` daily 02:45 UTC; `tests/test_financial_facts_raw_partition.py` + `tests/test_financial_facts_retention.py` + `tests/test_migration_156_partition_swap.py` + `tests/test_share_count_history_views_post_swap.py` (22 cases incl. regression coverage for every Codex finding above). Spec `docs/_archive/2026-05-19-phase3-financial-facts-raw-partition.md` (4 rounds: v1 → v4, 7 BLOCKING + 14 WARNING + 1 NIT resolved). Landed in #1208 Phase 3.
- Verification step: post-migration on dev DB, `SELECT count(*) FROM financial_facts_raw` matches the pre-migration count exactly (62 M ± concurrent-ingest drift); `\d+ financial_facts_raw` reports `Partitioned table` with 86 leaves; `share_count_history` returns the same row count as before; one manual `financial_facts_retention_sweep` fire drops the table to ~16 GB. New mega-tables (>1 M rows / >1 GB) failing this entry's checklist = NOT MERGEABLE.

### Per-query try/except inside a non-autocommit psycopg tx does not isolate failures

- First seen in: 2026-05-19 — Codex 1a review on Phase 4 spec (`/system/postgres-health` endpoint, #1208 Sub 4). The v1 service design wrapped each of seven metric probes in `try/except psycopg.Error` thinking failures would be isolated. They are not.
- Symptom: in a default (`autocommit=False`) psycopg connection, the first SQL error puts the entire transaction in `ABORTED` state. Every subsequent query — even ones with no relation to the failing query — fails with `current transaction is aborted, commands ignored until end of transaction block`. The `try/except` catches the second error, third error, etc., but the response is misleadingly "everything failed" when only one query actually had a problem. Worst case: a missing privilege on `pg_ls_waldir()` (`pg_monitor` role required) silently zeroes every metric on the dashboard.
- Prevention: any service that issues multiple independent SQL queries AND needs per-query failure isolation MUST open its own connection with `autocommit=True`. Each query then runs in its own implicit transaction; a failure on one query never leaks state into the next. The service-no-commit invariant from Phase 1's prevention-log §"psycopg3 service-no-commit invariant" still applies for SERVICES THAT TAKE A CALLER-OWNED CONN — but the "read-only metrics collection" pattern is a different shape: the service OWNS the conn lifecycle (open + autocommit + iterate + close), and the caller treats it as a higher-level operation. The `app/services/financial_facts_retention.py::sweep_retention_all_instruments` orchestrator (Phase 3) is the sister pattern: it opens its own autocommit conn and uses `with conn.transaction()` per unit so each becomes a real top-level tx rather than a savepoint.
- Decision tree for new services:
  - Service performs ONE query OR several queries with shared semantics (e.g. read-modify-write that must succeed atomically)? → take caller's conn (`def f(conn: psycopg.Connection)`) and do NOT enter `with conn.transaction()` — caller owns tx (service-no-commit).
  - Service performs N independent metric/probe queries where per-query failure should NOT poison the others? → open own conn with `autocommit=True` (`def f(*, database_url: str | None = None)`); per-query `try/except psycopg.Error` is real isolation here.
  - Service performs an orchestration of N independent ops (each a unit of work that should commit independently)? → open own conn with `autocommit=True`; iterate units with `with conn.transaction()` per unit.
- Enforced in: this prevention log; `app/services/postgres_health.py::collect_postgres_health` (opens own autocommit conn for 7 probes); `app/services/financial_facts_retention.py::sweep_retention_all_instruments` (orchestrator-of-N pattern); `tests/test_api_postgres_health.py::test_metric_isolation_returns_partial_payload` (regression). Spec `docs/_archive/2026-05-19-phase4-postgres-health.md` §4.2. Landed in #1208 Phase 4.
- Verification step: any new "collect_*_health" / "snapshot_*" / "probe_*" service that issues N queries gets a one-line code review check: "does the service open `autocommit=True`?" If not, the per-query try/except is a lie.

### Pre-push hooks must avoid backticks inside double-quoted echoes

- First seen in: 2026-05-19 — Codex 1a review on Phase 4 spec for the new dev-DB bloat warning. The proposed hook line was `echo "==> WARN: ... \`docker exec ebull-postgres psql ...\`"` — looks like a literal message, actually triggers command substitution at hook-run time, silently running a second `docker exec psql` for every push.
- Symptom: every push runs an unintended psql probe; with no error handling on the substituted command, a hung docker or stopped Postgres container would propagate the failure into the hook output without context.
- Prevention: hook files (`.githooks/pre-push` and any future hook) MUST use single-quoted echoes when the message contains characters that bash treats as syntax inside double quotes (backticks for command substitution, `$()` for command substitution, `$VAR` for variable expansion when literal interpolation isn't intended). Variable interpolation is achieved by concatenation: `echo '==> WARN: db size = '"${pretty}"' (over threshold).'`. This pattern combines the literal-safe outer single quotes with the controlled `"${var}"` interpolation in the middle. `tests/test_pre_push_hook_bloat_warn.py::test_pre_push_hook_avoids_backtick_in_echo` enforces the rule by scanning the hook for any double-quoted echo with a backtick character.
- Enforced in: this prevention log; `tests/test_pre_push_hook_bloat_warn.py` (3 tests: drift gate, `bash -n` syntax check, backtick scanner); `.githooks/pre-push` Phase 4 bloat-warn block. Landed in #1208 Phase 4.

### Isolation tests must assert on probes dispatched AFTER the patched failure

- First seen in: 2026-05-19 — bot review on PR #1216 (#1208 Phase 4). `tests/test_postgres_health_service.py` patched `_q_wal_dir` to raise + asserted `snapshot.db_size_bytes is not None`. The assertion ALWAYS passes regardless of autocommit because `_q_db_size` is dispatched BEFORE `_q_wal_dir` in `collect_postgres_health` — the conn isn't aborted yet when db_size runs. The test had a real autocommit guard further down (`last_checkpoint_at is not None`), but the misleading top assertion implied broader coverage than it actually had.
- Symptom: a regression that removes `autocommit=True` could silently pass the test under specific patch orderings; the test name says "isolation under real DB" but actually only verifies one specific position in the dispatch order.
- Prevention: when writing an isolation test that patches probe N to fail, assert ONLY on probes N+k (k >= 1) — probes dispatched AFTER the patched failure. Document the dispatch order in the test (a comment listing the seven probes in execution order is a cheap, self-validating anchor). Asserting on probes N-k is a false guard: those probes execute against an un-aborted conn and succeed regardless of autocommit.
- Generalises to any failure-isolation test (transaction state, file-handle exhaustion, rate-limit exhaustion) — assert downstream of the injected failure, never upstream.
- Enforced in: this prevention log; `tests/test_postgres_health_service.py::test_autocommit_isolates_failed_probe_under_real_db` (post-fix asserts on `wal_since_checkpoint_bytes`, `last_checkpoint_at`, `autovacuum_top10`, `financial_facts_raw_default_rows` — all probes dispatched after the patched `_q_wal_dir`). Landed in #1208 Phase 4 follow-up.

### Parser bugs that emit out-of-window dates silently fill the DEFAULT partition (or hide in normal partitions)

- First seen in: #1218 (XBRL parser emits `period_end='6016-06-30'` + pre-1900 dates → `financial_facts_raw_default` partition accumulates them until the 5000-row alarm fires hours later). Empirically caught one more row in the wild during cleanup: `period_start='205-01-01'` with `period_end='2025-03-31'` → routes to a normal `_2025q1` partition because `PARTITION BY RANGE (period_end)` keys on the end date, so bad `period_start` rows hide from the default-partition alarm entirely.
- Symptom: silent + cumulative. Every ingest cycle that hits the malformed input path adds rows; the operator-visible alarm only fires once the count crosses the warning threshold (5000 for `financial_facts_raw_default`). For data hidden in normal partitions, there is no alarm at all — the bug is unobservable until a downstream report goes wrong.
- Prevention: any parser ingesting third-party temporal data (XBRL period contexts, Form 4 transaction dates, 13F report periods, N-PORT period-of-report, news event timestamps, etc.) MUST validate at parse time against a SANITY window with explicit lower + upper bounds, reject + WARN-with-provenance on failure, never write the row. The cleanup tool that mirrors the parser predicate MUST exist and MUST be re-runnable so historical bleed can be evicted; the cleanup predicate MUST match the parser predicate 1-to-1 (extending one without the other re-opens the same gap).
- Generalises to: any pair of date/time fields where one is derived from external content and the other from system time — out-of-window + negative-duration are both real bug classes. The sanity window must be tight enough to catch digit-overflow / digit-truncation bugs (year 6016, year 205) without rejecting legitimate forward-projected schedule data (XBRL `OperatingLossCarryforwards` / `FiniteLivedIntangibleAssetsAmortizationExpenseAfterYearFive` legitimately schedule rows to 2041+). For SEC XBRL `[1900-01-01, 2100-01-01)` is the right balance — wide enough for legitimate forward-projected schedule items, tight enough to catch the year-6016 bug class. Bonus invariant: validate the LOGICAL ORDERING too (`period_start <= period_end`) — the same bug class that overflows a year typically also flips start/end.
- Single chokepoint discipline: when the same parser surface fans out to multiple entry points (per-API + bulk + companyconcept all calling one `_extract_*` helper), put the validation in the helper, not in the entry points. Cuts the surface area where a future caller can bypass the guard.
- Enforced in: this prevention log; `app/providers/implementations/sec_fundamentals.py::_classify_period_rejection` + `_extract_facts_from_section` (parser-side guard with per-(accession, reason) WARN dedup at the log site); `scripts/cleanup_1218_out_of_window_facts.py` (mirrored predicate, dry-run-default + `--apply`); `tests/test_sec_fundamentals_period_end_guard.py` (22 cases — classifier boundaries × period_end + period_start + ordering + dei/us-gaap parity + dedup behaviour). `.claude/skills/data-sources/sec-edgar.md` §7.16 documents the gotcha for future SEC ingest work. Spec `docs/_archive/2026-05-19-1218-parser-period-end.md`.
- Verification step: any new parser that ingests temporal data + writes to a partitioned table gets a one-line review check: "does the parser guard period validity at the boundary? Does the cleanup tool exist + mirror the guard?" If not, the partition's DEFAULT-bucket alarm is a lagging indicator, not a real safety net.

### Bootstrap recency constants must be namespaced per source, even when sharing a numerical value

- First seen in: 2026-05-20 — bot review on PR #1243 (#1233 PR7). N-PORT's stage 22 cohort cutoff (`_PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF` resolution) reached for `_BOOTSTRAP_13F_RECENCY_DAYS` directly. The two windows happen to be the same 380 days today, but a future 13F-only tuning (cohort widens to 500d, or narrows to 270d during a bandwidth squeeze) would silently drift the N-PORT cohort.
- Symptom: silent + cumulative. A 13F-only knob change ships, dev DB looks fine, post-merge the N-PORT bootstrap stage starts admitting a different cohort because the symbol it dereferenced was shared. Operator has no signal — the param shows in the run-log as a resolved date, not as "borrowed from 13F".
- Prevention: every source-scoped tunable that resolves a dispatch-time sentinel MUST have its own purpose-named alias at the module top (e.g. `_BOOTSTRAP_NPORT_RECENCY_DAYS = _BOOTSTRAP_13F_RECENCY_DAYS` — alias today, free to diverge tomorrow). Resolvers reference the source-specific name. Aliasing is cheap; the discipline boundary is "no cross-source constant references inside `_resolve_dynamic_params`."
- Generalises to: any retention window, cohort window, or rate budget that is logically per-source but happens to coincide numerically with another source's. Cost-modelling factors, schedule lookahead windows, batch-size knobs.
- Enforced in: this prevention log; `app/services/bootstrap_orchestrator.py::_BOOTSTRAP_NPORT_RECENCY_DAYS` (PR #1243 alias); future bootstrap stages copying the pattern.
- Verification step: any new `_PARAM_DYNAMIC_BOOTSTRAP_*_CUTOFF` sentinel resolution gets a one-line review check: "does the resolver reference a source-specific `*_RECENCY_DAYS` constant, or does it dereference a sibling source's symbol?" The second form is the bug.

### Isolation wrappers must catch broadly enough to cover every failure mode inside the wrapped function

- First seen in: 2026-05-19 — bot review on PR #1216 (#1208 Phase 4). `_safe(...)` in `app/services/postgres_health.py` caught only `psycopg.Error`, but the wrapped probe functions contained bare `assert row is not None` statements — an `AssertionError` would escape the wrapper, bypass the API's `psycopg.Error` handler, and produce an unhandled 500 instead of the documented "partial 200 with `metric_errors` populated" behaviour.
- Symptom: subtle. The "partial response on probe failure" contract is the entire point of the isolation wrapper. A wrapper that catches a strict subclass set has a hidden failure mode: any exception type outside the set silently breaks the contract. Bot called out the specific scenario (table absent → `psycopg.UndefinedTable` → caught; defensive `assert` fails → `AssertionError` → uncaught), but the pattern generalises: `KeyError` on an unexpected catalog column, `ValueError` on a malformed return, `RuntimeError` from a downstream library.
- Prevention: an isolation wrapper that promises a partial-response contract MUST catch `Exception`. The justification is intrinsic to the wrapper's purpose: it exists to make failures local, and "local" must mean "every failure type that could occur inside this function." Use `except Exception as exc: ... # noqa: BLE001 — isolation wrapper` with a docstring explanation rather than enumerating subclasses. Pair with a regression test that patches the probe to raise a non-DB exception (e.g. `AssertionError`) and asserts the wrapper still returns the partial-response shape.
- Generalises beyond psycopg probes to: retry-loop bodies, plugin-dispatcher wrappers, hook-firing wrappers, scheduled-job invokers — anywhere "isolate this unit's failure from the rest" is the contract.
- Enforced in: this prevention log; `app/services/postgres_health.py::_safe` (catches `Exception` with documented `noqa: BLE001`); `tests/test_postgres_health_service.py::test_safe_wrapper_catches_non_psycopg_exceptions` (patches `_q_default_partition_rows` to raise `AssertionError`; asserts partial response shape). Landed in #1208 Phase 4 follow-up.

### Empty-grep wc -l false-positive in bash idioms

- First seen in: 2026-05-20 — bot review on PR #1246 (#1233 PR10a). The pattern `count=$(echo "$grep_result" | wc -l | tr -d ' ')` looks like "count files matched by grep", but `echo ""` emits one newline, so `wc -l` of empty stdin returns 1 — meaning the counter silently reports 1 when grep matched nothing. A guard like `if (( count != 1 )); then fail; fi` would falsely PASS the "found exactly 1 file" check on zero matches. (The PR10a lint guard already had the `[[ -n "$grep_result" ]]` initialiser sentinel, so the BLOCKING was rebutted on that PR — the bot missed the guard. But the underlying gotcha is real for any bash lint script that copies the wc -l-on-echo idiom without the guard.)
- Symptom: silent + load-bearing. A "exactly-N-files" check that's used to catch missing predicates / refactored-away invariants would falsely pass when the invariant disappears entirely. The lint guard would print "OK" while the code is structurally broken.
- Prevention: any bash lint that converts a `grep -l` (or `find`, or any pipeline that may produce empty output) into a count via `wc -l` MUST guard against the empty-input case explicitly. Three acceptable shapes:
  - `[[ -z "$var" ]] && count=0 || count=$(echo "$var" | wc -l | tr -d ' ')` — explicit branch.
  - `count=$(printf '%s' "$var" | grep -c .)` — counts non-empty lines, handles empty input correctly (0).
  - Initialise `count=0` BEFORE an `if [[ -n "$var" ]]; then count=$(echo "$var" | wc -l | tr -d ' '); fi` block (the PR10a pattern).
- Pair every "exactly-N-files" lint check with an ADVERSARIAL TEST that removes the predicate from the canonical file and verifies the guard trips — without that test the false-positive is invisible until the invariant disappears in production.
- Generalises to: any pipeline-counter idiom in shell (rate-limit log analysis, autocompletion grep-then-count helpers, custom pre-commit/pre-push hooks that count "files matching pattern"). The trap is `echo` specifically — `printf '%s'` does not emit a trailing newline on empty input.
- Enforced in: this prevention log; `scripts/check_business_summary_latest_only.sh` lines 201-211 (the inline comment now explicitly documents the guard and links back to this entry; adversarial test in the lint-guard's own commit history). Landed in #1233 PR10a follow-up.

### Spec author must grep `KNOWN_FILING_AGENT_CIKS` before designing any manifest-parser archive-URL flow

- First seen in: 2026-05-21 — Codex 1c review on the PR11 (#1233) spec doc, after the v3 draft pivoted manifest's `cik` field to the accession-number prefix. Codex correctly caught that `sec_edgar.py:83-104` enumerates `KNOWN_FILING_AGENT_CIKS` (Donnelley/EdgarOnline/DFIN/Workiva/etc.) for whom the accession prefix is the AGENT's CIK but the archive directory lives under the issuer / filer CIK — so the pivot would 404 every agent-submitted accession.
- Symptom: spec ships, code lands, runtime tombstones every agent-filed accession with a generic "archive 404" error that masks the real root cause. The bug surfaces at the rollup layer (operator-visible chart missing institutional / fund holdings); operator opens the manifest backlog and sees thousands of identical tombstones.
- Prevention: when drafting any SEC-parser spec that involves `_archive_file_url` (or any per-accession SEC archive URL construction), the spec author MUST grep `KNOWN_FILING_AGENT_CIKS` in `app/providers/implementations/sec_edgar.py` AND read the surrounding comment block (`sec_edgar.py:83-104` + `:310-353` + `:397-417`) BEFORE drafting the CIK-derivation logic. The discipline boundary is: spec authoring loads the existing defense-in-depth surface as input data, not as something to re-discover via Codex review.
- Generalises to: any spec that touches a code surface with named-constant defensive guards (auth scopes, rate-limit overrides, retry-class allow-lists, schema CHECK constraints, etc.). Codex catches gaps that the spec author should have caught at draft time by reading the code, not waiting for a review pass.
- Enforced in: this prevention log; `.claude/skills/data-sources/sec-edgar.md` §3.7 (filing agents reference + KNOWN_FILING_AGENT_CIKS doc); `scripts/check_archive_url_agent_guard.sh` (cross-cutting pre-push lint that forbids `_archive_file_url(<cik>, ...)` without preceding `KNOWN_FILING_AGENT_CIKS` reference in the same file, with an ALLOW_LIST for parsers whose `subject_type='issuer'` contract excludes agent collisions); landed alongside the cleanup PR that closed #1249 + #1250.

### Spec author must read manifest CHECK constraints before designing `sec_filing_manifest` row shapes

- First seen in: 2026-05-21 — Codex 1a review on the PR11 (#1233) spec doc. The v1 draft proposed `subject_type='blockholder_filer'` rows carrying `instrument_id=issuer_instrument_id` to propagate the universe-resolved issuer hint through the manifest. Codex correctly caught that `sql/118_sec_filing_manifest.sql` has a CHECK constraint `(subject_type='issuer' AND instrument_id IS NOT NULL) OR (subject_type<>'issuer' AND instrument_id IS NULL)`, AND `record_manifest_entry` at `sec_manifest.py:223-231` raises `ValueError` enforcing the same invariant — so the proposed shape would crash at every INSERT. Pivot was to a side-table for the hint.
- Symptom: spec ships incompatible with schema. Either runtime crashes at first INSERT, or (worse) the schema gets a CHECK relaxation in the same PR to accommodate the spec, eroding an existing invariant for a new use case.
- Prevention: any spec that proposes new `sec_filing_manifest` row semantics — new `subject_type`, new `(subject_type, subject_id, instrument_id)` combinations, new `cik` field usage — MUST grep `CHECK` in `sql/118_sec_filing_manifest.sql` AND read `record_manifest_entry` at `sec_manifest.py:194-300` BEFORE drafting. Same discipline as the filing-agent-CIK entry above: the existing invariants are spec INPUT, not Codex DISCOVERY.
- Generalises to: any schema with NOT NULL + CHECK + UNIQUE + FK constraints that the spec touches. Schema invariants are load-bearing for spec correctness — they're cheaper to read at draft time than to discover at Codex round 1.
- Enforced in: this prevention log; cross-link from `.claude/skills/data-engineer/SKILL.md` §1 (schema layer by domain — `sec_filing_manifest` invariants listed); spec-authoring checklist convention: any spec touching `sec_filing_manifest` semantics opens with "schema invariants honoured: …" so reviewer can verify at a glance.

### Edgartools `parse_xml` returns a top-level dict; nested values are dataclasses with attribute access

- First seen in: 2026-05-21 — Codex 1d/1e/1f rounds on the PR11 (#1233) spec all consecutively caught wrong adapter patterns for `edgartools.beneficial_ownership.schedule13.Schedule13D.parse_xml`. (1d) v4 said `Schedule13D(**parse_xml(xml))` — fails because the constructor requires 7 positional args including `filing`. (1e) v5 pivoted to "read fields from the dict directly" but used nested-dict-key access — fails because nested values are dataclasses, not dicts. (1f) v6 finally pinned the correct shape: TOP-LEVEL dict access + nested dataclass attribute access. Field name `aggregate_amount` (NOT `aggregate_amount_owned`) further confused the API.
- Symptom: parser code looks reasonable at PR-review time + passes unit tests with mock fixtures keyed on dicts, but fails at runtime against real `parse_xml` output. Three Codex rounds were burnt narrowing the right shape; the cost in burned review time alone justifies pinning the contract.
- Prevention: any new ingest path adopting `edgartools.Schedule13*.parse_xml` (or any sibling library entry point returning composite Pydantic/dataclass structures) MUST exercise the actual library at SPEC TIME against a real fixture and capture the returned shape in the spec. The contract test (`tests/test_edgartools_schedule13_shape.py` for Schedule13D/G — pin BOTH top-level dict keys AND nested dataclass attribute names) is the long-term enforcement; the spec-time exercise prevents wasted Codex rounds.
- Generalises to: any third-party library API where the docstring promises a "dict" but the real return value is a hybrid of dicts + dataclasses + Pydantic models. `assert isinstance(...)` plus a one-line shape print in a spec spike beats three review rounds.
- Enforced in: this prevention log; `.claude/skills/data-sources/edgartools.md` G15 (full API contract + correct adapter pattern + recommended contract test shape); PR11 implementation will add `tests/test_edgartools_schedule13_shape.py` per the spec's §3.3 contract.

### MERGE `WHEN NOT MATCHED BY SOURCE` must carry the per-scope clamp on BOTH the ON clause AND the DELETE clause

- First seen in: 2026-05-21 — #1233 PR12 spec (Codex 1a HIGH-2 + HIGH-4; rev 4 lint invariant D split into D1 + D2 in response).
- Symptom: a future refactor relaxes the ON clause from `tgt.instrument_id = %(iid)s AND tgt.<pk_col> = src.<pk_col>` to `tgt.instrument_id = src.instrument_id AND tgt.<pk_col> = src.<pk_col>`. The USING source subquery still returns rows for one instrument, but the MERGE now evaluates target rows for ALL instruments under the relaxed ON; `WHEN NOT MATCHED BY SOURCE THEN DELETE` deletes every target row not in src — catastrophic global data loss across the whole table.
- Prevention: the scope clamp `AND tgt.<scope_col> = %(scope)s` MUST appear in BOTH (a) the ON clause AND (b) the `WHEN NOT MATCHED BY SOURCE` clause. Lint pins both literals via block-local awk extraction + exact-count assertions (one invariant per clause, not a single combined check — relaxing one but not the other must still fire).
- Exception: PG MERGE compiles `WHEN NOT MATCHED BY SOURCE` to an internal FULL OUTER JOIN which errors `FeatureNotSupported: FULL JOIN is only supported with merge-joinable or hash-joinable join conditions` when the ON clause is a const-only predicate. Single-PK-col tables (e.g. `ownership_treasury_current` with PK `(instrument_id)`) MUST use `tgt.instrument_id = src.instrument_id` in ON (equi-join condition) + put the const-clamp in the USING subquery's `WHERE instrument_id = %(iid)s` (1×1 cardinality holds). The NOT MATCHED BY SOURCE clamp is still required as defence-in-depth. Lint D1 expected count for single-PK-col helpers = 0; D2 expected count = 1.
- Generalises to: any PG17 diff-aware MERGE that uses `WHEN NOT MATCHED BY SOURCE` for the deletion arm. The same clamp split applies to any cohort-scoped MERGE writer (e.g. per-filer manifest reconciliation, per-cik companyfacts diffing).
- Enforced in: `scripts/check_ownership_refresh_writer_pattern.sh` invariants D1 + D2; `docs/_archive/2026-05-21-pr12-ownership-current-writer-merge.md` §4.3 + §5; this prevention log.

### Diff-aware writers (`MERGE … IS DISTINCT FROM`) must NOT include update-timestamp columns in the diff predicate — drift watermarks belong in a side-table

- First seen in: 2026-05-21 — #1233 PR12 spec (Codex 1a HIGH-1 + Codex 1b HIGH-2 race fix; rev 3 introduced the side-table + pre-MERGE watermark capture pattern).
- Symptom (variant A — bloat returns): `refresh_X_current` uses `MERGE … WHEN MATCHED AND (cols, refreshed_at) IS DISTINCT FROM (excluded.cols, now())`. `now()` always differs from the stored `refreshed_at` → every MATCHED row re-fires UPDATE → N dead tuples per call → bloat returns to the DELETE+INSERT baseline that prompted the MERGE rewrite in the first place. Symptom (variant B — repair-sweep forever-loop): the same predicate without `refreshed_at` works for bloat but leaves the watermark frozen on no-op refresh → repair-sweep `c.refreshed_at < MAX(obs.ingested_at)` is permanently true on any instrument whose observations were re-ingested (Form 4 reingest, etc.) → sweep re-selects the same instrument every tick.
- Prevention: `refreshed_at` (and any update-timestamp column the writer mutates on every call) MUST be EXCLUDED from the diff predicate on both LHS and RHS. The drift watermark for the repair-sweep MUST live in a separate side-table (e.g. `ownership_refresh_state.last_drained_observations_max_ingested_at`) updated on every refresh attempt regardless of MERGE outcome. The watermark value MUST be captured pre-MERGE in a Python variable (race-safe: prevents the post-MERGE UPSERT advancing past observations the MERGE did not see if new obs land between SELECT and MERGE).
- Generalises to: any diff-aware reconciler (`UPSERT … WHERE EXCLUDED.x IS DISTINCT FROM …`, materialised-view refresh diffs, cache-revalidation predicates). Whenever the predicate compares "what's stored" to "what we just computed", any column the writer mutates on every call must be excluded from the comparison — otherwise the comparison is vacuously true and the optimisation is defeated.
- Enforced in: `scripts/check_ownership_refresh_writer_pattern.sh` invariant E (refreshed_at not in IS DISTINCT FROM tuples) + invariant I (refreshed_at = now() lives only in UPDATE SET); `tests/test_ownership_refresh_writer_merge.py` case 2 (no-op churn — xmin stability + pgstattuple dead-tuple delta + refreshed_at unchanged) + case 8 (repair-sweep no-loop) + case 9 (known_to expiry watermark alignment); `docs/_archive/2026-05-21-pr12-ownership-current-writer-merge.md` §3.3 + §4.2; this prevention log.

### Integration tests must use DELTA-based counter assertions, never exact-equality on DB-global counters

- First seen in: 2026-05-24 — #1233 PR-B (PR #1308) bot review iter 1 BLOCKING. Tests in `tests/test_s14_uses_sidecar.py` called `walk_files_pages(conn=ebull_test_conn)` (which iterates EVERY in-universe CIK in the per-worker test DB) and asserted `result.parse_errors == 0`, `result.secondary_pages_fetched == 0`, etc. Any other tradable CIK in the test DB with an empty sidecar would inflate `parse_errors` + `ciks_with_empty_sidecar` and cause spurious failure.
- Symptom: per-worker test DB picks up `bf719c5`'s prior test residue (or any concurrent test's side effect) — exact-equality assertion fails on a counter that includes rows outside the test's control. Test is green locally on a freshly-wiped DB; fails in CI / on a polluted DB. Classic flake-vector class.
- Prevention: any integration test that calls a function iterating the full DB cohort MUST use DELTA-based assertions:
  ```python
  baseline = walk_files_pages(conn=ebull_test_conn)
  _seed_test_data(ebull_test_conn)
  try:
      after = walk_files_pages(conn=ebull_test_conn)
      assert after.counter_x - baseline.counter_x == EXPECTED_DELTA
  finally:
      _wipe_test_data(ebull_test_conn)
  ```
  OR use `>= N` / `<= N` bounds (less precise but acceptable for "at least this happened"); NEVER `== 0` / `== N` on accumulators that include rows outside the test's control.
- Generalises to: any integration test against a shared per-worker test DB where the function under test iterates rows beyond the fixture-seeded set (`walk_files_pages`, `sync_all_observations`, `compute_coverage_audit`, etc.). The delta pattern is the canonical fix.
- Enforced in: this prevention log; future bot review will catch on PR-B-style test additions; `.claude/skills/engineering/test-quality.md` should add a "DB-global counter exact-equality forbidden" rule (follow-up tightening).


### Spec sample code references columns that do not exist (phantom column class)

- First seen in: 2026-05-24 — #1233 Stream A spec v2.3 (PR-D pre-flight Codex 1 review). §1.8 C5 cited `data_freshness_index.last_seen_at` but the real column is `updated_at` (sql/120). Three committee rounds + the v2/v2.1/v2.2/v2.3 fold loop did not catch it; only a fresh Codex grep against the schema surfaced the phantom.
- Symptom: a spec lands with sample SQL or sample Python referencing columns / functions / paths that exist nowhere in the codebase. Reviewers reading the spec assume they're real; implementers code against them and lint/test catches the missing symbol only after the diff is opened.
- Prevention: any spec section that names a SQL column, function, file path, or method MUST be grep-validated against the actual repo at spec-finalisation time. The Codex 1 pre-spec checkpoint (`codex.cmd exec "Review this spec ... grep every cited symbol"`) is the authoritative gate; if Codex catches a phantom, the spec author owes a v-bump fold inline rather than a follow-up ticket.
- Generalises to: every spec sample-code block in `docs/proposals/` and `docs/specs/`. The `data-engineer/SKILL.md §0.0` "before-spec grep gate" rule documents the requirement; PR-D extended it with a concrete example.
- Enforced in: this prevention log; `data-engineer/SKILL.md §0.0`; `etl-spec-template-usage.md`; Codex 1 pre-spec checkpoint per CLAUDE.md "Codex second-opinion — mandatory checkpoints".

### Architectural-invariant claims about Postgres advisory locks must be empirically tested before they ship

- First seen in: 2026-05-24 — #1233 Stream A PR-D plan v3 + Codex re-pass on plan v3. The plan claimed (with Codex confirming) that "PG advisory locks are cluster-wide so `acquire_jobs_process_fence(postgres_url)` survives `DROP DATABASE ebull_dev`". Empirical test against PG 17 (`tests/test_jobs_process_probe_fence.py::test_per_database_isolation_regression_gate`) showed otherwise: locks are PER-DATABASE; two sessions on `postgres` and `ebull_dev` DBs in the same cluster acquired the same key independently. The B1 fold of the operator-lens finding was based on a wrong understanding of PG; only writing a test caught it before the runbook shipped against the bad assumption.
- Symptom: a spec / plan / committee review claims an architectural invariant about Postgres (or any database engine) behaviour. The claim sounds plausible, is repeated across reviews, and silently informs subsequent design decisions. Implementation lands depending on the claim. Production-equivalent runtime invalidates the claim only when it matters most (e.g. lock fails to block under concurrency).
- Prevention: any claim about PG advisory-lock scope, isolation-level effects, transaction-vs-session-vs-statement semantics, or cross-database/cross-cluster visibility MUST be backed by an empirical test (one-liner against the local PG before the design lands; persistent regression-gate test before the code lands). "PG docs say X" is not sufficient — docs are easy to misread. Direct cluster experiment is the only authoritative source.
- Generalises to: any architectural claim about a database, distributed system, or kernel/runtime primitive that informs a destructive runbook, lock design, isolation guarantee, or HA fail-over plan. Especially dangerous when the claim survives one review round without empirical challenge.
- Enforced in: this prevention log; `tests/test_jobs_process_probe_fence.py` regression gate; `app/jobs/locks.py::acquire_jobs_process_fence` docstring (LIMITATION block).

### Operator-CLI namespace collisions: `app/cli.py` would have been shadowed by `app/cli/`

- First seen in: 2026-05-24 — #1233 Stream A PR-D plan v2 round-2 committee review (RV1 BLOCKING). The spec v2.3 said runbooks live at `app/cli/runbooks/`. Existing `app/cli.py` (5532 bytes, May 6) is the break-glass operator credential CLI (`python -m app.cli set-password`, `create-operator`). Creating `app/cli/__init__.py` would have shadowed the module — `python -m app.cli` would have resolved to the new package, breaking the operator workflow. Caught pre-code by the second adversarial-reviewer lens; my initial `ls app/cli` (directory) check missed the sibling `app/cli.py` (file).
- Symptom: a spec/plan introduces a NEW package `app/<name>/` without checking whether `app/<name>.py` (file, not directory) already exists. `ls app/<name>` returns "No such file or directory" because directory does not exist; the sibling file is invisible to that probe. After the package lands, the file is shadowed by Python's package-over-module precedence; downstream `python -m app.<name>` invocations silently route to the new package's `__init__.py`.
- Prevention: before creating any new package directory `app/X/`, run BOTH `ls app/X` AND `ls -la app/X*` (or `git ls-files app/X*`) to catch a sibling `app/X.py` file. If a file exists, pick a different package name OR consolidate the file into the new package (convert `app/X.py` → `app/X/__init__.py` + `app/X/credentials.py` + ...). Spec authors must enumerate the proposed path against the existing tree at spec-finalisation time.
- Generalises to: any Python-package directory creation in a tree that already has same-named modules. Common when promoting a single-file utility to a multi-file package without ever auditing the imports.
- Enforced in: this prevention log; PR-D committee round 2 (RV1 finding); `app/runbooks/` (chosen over `app/cli/runbooks/` per round-2 fold).

### argparse `action="store_true" + default=True` is a no-op flag

- First seen in: 2026-05-24 — #1233 PR-D (PR #1311) bot review iter 1 BLOCKING. `app/runbooks/stream_a_stream_c_gate.py` defined `parser.add_argument("--strict", action="store_true", default=True)`. `action="store_true"` already implies `default=False`; explicitly setting `default=True` means the value is True whether or not the flag is passed. Argparse generates no `--no-strict` for this action, so the flag can never be disabled from the CLI. The runbook's "fail-closed default; opt-out via flag" intent was silently broken.
- Symptom: a CLI flag intended to provide a default-True opt-out is permanently on. No exception, no warning — the value is just always True. Bot/operator discovers it only when explicitly trying to pass `--no-flag` or testing with the flag unset.
- Prevention: when a boolean flag MUST default to True with an opt-out path, use `action=argparse.BooleanOptionalAction` (Python 3.9+) which generates both `--flag` and `--no-flag`. Use `action="store_true"` only when the flag is a true/false toggle with default-False ("opt-in"); use `action="store_false"` only with default-True ("opt-out"). Never mix `store_true` + `default=True` or `store_false` + `default=False`.
- Generalises to: every argparse-driven CLI in the repo. Lint rule candidate: `grep -nE 'action="store_true".*default=True|action="store_false".*default=False'` in `scripts/check_argparse_flag_sanity.sh`.
- Enforced in: this prevention log; PR-D fix uses `BooleanOptionalAction`; `app/runbooks/stream_a_stream_c_gate.py` flag definition.

### Shell `$(cmd)` capture swallows the wrapped process exit code — silent-fail audit pattern

- First seen in: 2026-05-25 — PR #1353 (issue #1256) bot review iter-1 BLOCKING. `scripts/check_ownership_refresh_writer_pattern.sh:519-524` captured the Python coverage-audit helper's stdout via `coverage_output=$(uv run python ...)` and then `grep -q "10 functions covered (expected 10)" <<<"$coverage_output"`. The Python `coverage_report()` function prints "10 functions covered (expected 10)" UNCONDITIONALLY (the count counts found helpers, not passed ones) then `sys.exit(2)` only when invariants fail. Bare `$(...)` discards the exit code, so a broken helper would print the count line, pass the grep, and the audit would silently succeed. Empirically demonstrated: synthetic source where 9 of 10 helpers fail invariant I still produces a green audit under the old pattern.
- Symptom: a shell script captures a subprocess's stdout with `output=$(cmd)`, then runs a downstream check (grep / sed / awk) on `$output`. The downstream check happens to pass even when the subprocess failed, because the relevant output already printed before the failure. The audit looks green; the underlying assertion is bypassed. Compounded when the subprocess prints summary lines (count / status banner) that grep matches independent of the actual pass/fail.
- Prevention: capture in the conditional form: `if ! output=$(cmd); then fail "..."; fi`. This propagates the subprocess exit to the shell test. THEN run any downstream string check as a secondary guard. Equivalently `cmd > /tmp/x; rc=$?; ... ; (( rc == 0 ))` works but is more code. **Never use bare `output=$(cmd)` followed by `grep` on `$output` without also checking `$?` or using the `if !` form.** The exit code check is load-bearing; the grep is a defence-in-depth check, not a substitute.
- Generalises to: every shell script that captures a subprocess's stdout AND treats a grep/sed/awk match as the audit gate. Especially dangerous when the subprocess is a Python helper that prints progress lines before `sys.exit(N)`. Lint rule candidate: shellcheck SC2181-equivalent — flag `$(cmd)` immediately followed by a string check on the captured variable without `$?` check or `if !` form.
- Enforced in: this prevention log; `scripts/check_ownership_refresh_writer_pattern.sh:521-535` (uses `if ! coverage_output=$(...)` form + grep as secondary); `tests/scripts/test_check_ownership_writer_columns.py::test_coverage_report_exits_nonzero_when_any_helper_fails` (regression-gate empirical test: synthetic source with 9/10 broken helpers MUST exit non-zero even though count line still prints).

### fork-pwn via `pull_request` + head-SHA checkout

- First seen in: 2026-05-26 — PR #1357 (issue #1356, Phase 0 NEW-A) bot review iter-2 BLOCKING. `.github/workflows/ci.yml` `perf-claim-lint` job ran on `pull_request` (NOT `pull_request_target`), checked out `${{ github.event.pull_request.head.sha }}`, then executed `uv run python -m scripts.perf_bench.lint_pr_artifacts`. A fork-authored PR could land arbitrary Python under that module path; CI would execute it with access to repository `vars`/`secrets` exposed to the job (here `PERF_CLAIM_LINT_BYPASS`).
- Symptom: a workflow job runs on the `pull_request` event AND checks out `head.sha` (fork-controlled tree) AND then executes any checked-out code (Python script, shell script, action local path, makefile target). The combination silently turns the CI runner into a fork-PR-controlled execution surface with access to whatever env / vars / secrets the job is granted.
- Prevention: any job that combines `ref: ${{ github.event.pull_request.head.sha }}` with `run:` (or `uses: ./...`) commands executing checked-out code MUST ALSO carry an `if:` guard restricting to operator-owned branches: `if: github.event.pull_request.head.repo.full_name == github.repository`. Fork PRs SKIP the job entirely (the gate is then enforced only on branches the operator owns). The alternative — switching to `pull_request_target` + a pinned trusted ref — is more correct in principle but pulls in head-sha label/body coupling complexity; for single-author repos the `if:` guard is the smallest-surface fix. Lint rule candidate: a pytest scan of `.github/workflows/*.yml` that asserts every job using `head.sha` checkout also carries the same-repo predicate.
- Enforced in: this prevention log; `.github/workflows/ci.yml` `perf-claim-lint` job (`if:` predicate with `head.repo.full_name == github.repository`); `tests/scripts/test_perf_bench_lint.py::test_20_workflow_perf_claim_lint_has_same_repo_guard` (asserts the predicate is present; a future job that needs head-sha checkout MUST extend this test or land its own equivalent).

### strip-then-split inconsistency in markdown-section detection

- First seen in: 2026-05-26 — PR #1357 (issue #1356) bot review iter-2 WARNING. `scripts/perf_bench/lint_pr_artifacts.py:_slice_bypass_section` used `_has_header_line(body, HEADER_BYPASS)` (operates on HTML-comment-stripped body) as the guard, then sliced via `body.split(HEADER_BYPASS, 1)` on the RAW body. A header line with a trailing/embedded HTML comment (`## Bypass justification<!-- x -->`) makes the guard fire (line strips to the header text) while the raw split can land at a different occurrence — or leak HTML-comment debris into the downstream regex search window.
- Symptom: a function uses one canonicalisation pass to GATE a downstream `str.split(needle, ...)` / `str.find(needle)` / regex `.search(needle)` operation that runs on the ORIGINAL string. Whenever the canonicalisation changes the string (HTML comments stripped, whitespace normalised, Unicode folded, …) the guard's view and the slice's view drift. The drift surface is silent: code looks correct because both expressions name the same `needle`, but they operate on different `haystack`s.
- Prevention: when a function uses `_has_header_line` (or any other guard that operates on a canonicalised view of the body) to gate a subsequent `str.split` / `str.find` / regex search, the slice MUST use the same canonicalised view, not the raw body. Pattern: `cleaned = _strip_html_comments(body); if not _has_header_line(cleaned, h): return None; after = cleaned.split(h, 1)[1]`. Mechanical check during pre-flight review: grep for `_has_header_line` callers and confirm each follow-up `split` / `find` / regex search names the same variable that was passed to the helper.
- Enforced in: this prevention log; `scripts/perf_bench/lint_pr_artifacts.py:_slice_bypass_section` (uses `cleaned` for both guard + split); `tests/scripts/test_perf_bench_lint.py::test_21_bypass_html_comment_trailing_header_does_not_misslice` (regression: bypass header with trailing HTML comment still slices the section + finds the Operator/Reason lines).

### Unquoted SQL identifier in shell-out harness

- First seen in: 2026-05-26 — PR #1357 (issue #1356) bot review iter-3 WARNING. `scripts/perf_bench/_run_explain.py:_row_count` shelled out via `psql -c "SELECT COUNT(*) FROM {table}"` where `table` came from an operator-authored per-ticket YAML. The `--check-floors-only` path validated `table` against `floors.yaml`, but the `main()` path called `_row_count` directly after `cfg["target_table"]` without re-checking. A YAML value like `x; DROP TABLE financial_facts_raw` would execute arbitrary SQL against the bench DB. The operator authors the YAML so escalation is bounded, but the exposed surface (`psql -c` shell-out + unquoted identifier) is the same bug class as any other unquoted-identifier SQL injection — fix the shape, not the threat model.
- Symptom: a script (Python helper, shell wrapper, etc.) builds a SQL string by f-string-interpolating a caller-supplied identifier (`table`, `column`, `schema`, `view`) into an `EXPLAIN` / `SELECT` / `COUNT(*)` and pipes it to `psql -c`. The interpolation is unquoted because `psycopg.sql.Identifier` doesn't apply to a `subprocess` invocation. Whoever can write the source of that identifier — config file, env var, CI variable, even an operator who has direct shell access — owns the SQL surface. The trust boundary is invisible because the value looks innocuous at the call site.
- Prevention: any helper that shells out to `psql` (or any other DB CLI) with an interpolated identifier MUST validate the value against a strict identifier regex (`^[a-z_][a-z0-9_]*$` for our codebase; tighten or widen per dialect) at the chokepoint, not at the loader. The chokepoint guard is non-negotiable so a future caller cannot bypass loader-side validation. Pattern: `if not _TABLE_IDENT_RE.fullmatch(table): _err(...); out = _psql(db_url, f"... FROM {table}")`. Secondary defence: validate at the loader too so the error surfaces early. If `psycopg` is in-process, prefer `psycopg.sql.SQL("...{}").format(sql.Identifier(table))` over any shell-out at all.
- Enforced in: this prevention log; `scripts/perf_bench/_run_explain.py:_row_count` + `_load_ticket_config` (both guards on `target_table`); `scripts/perf_bench/_run_explain.py:TABLE_IDENT_RE` (the shared regex constant); `tests/scripts/test_perf_bench_lint.py::test_22_harness_table_ident_regex_rejects_sql_fragments` (positive + negative cases incl. `x; DROP TABLE ...`, quote-escape, uppercase, leading digit, dash, newline, empty).
