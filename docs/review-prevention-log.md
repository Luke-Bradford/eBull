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
- Symptom: An implementation plan under `docs/superpowers/plans/` contained a code-block showing `assert row is not None` for a DB-contract guard inside `app/services/`. The production code shipped correctly (using `RuntimeError` per prevention #109), but the plan's code block still showed the forbidden `assert` pattern. A future re-run of the plan by a fresh implementation agent would reintroduce the bug directly into production service code — exactly what the prevention-log rule is meant to stop.
- Prevention: When fixing a prevention-log hit during pre-flight review, grep the committed plan document for the same bad pattern and update it in the same PR. Plan files that appear under `docs/superpowers/plans/` are treated as historical record, but they also serve as a source of truth if the plan is re-run — so any code snippet that contradicts a prevention rule must be corrected in place. Pre-push check: `grep -rn "assert row is not None" docs/superpowers/plans/` should return zero lines touched by the current branch.
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
