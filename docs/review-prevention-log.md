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
- Enforced in: this prevention log

---

### Raw payload persistence must precede raise_for_status()
- First seen in: #177
- Symptom: `_fetch_company_facts` called `resp.raise_for_status()` before `_persist_raw()`, so non-404 HTTP errors (429, 503) raised before the raw payload was written to disk, losing diagnostic data.
- Prevention: In provider HTTP methods, always call `_persist_raw()` before `resp.raise_for_status()`. The persist call captures the response body for auditability regardless of status code.
- Enforced in: this prevention log

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
