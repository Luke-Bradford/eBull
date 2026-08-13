# #1492 — release pooled conn across SEC fetch in lazy-fill 8-K / business-sections routes

Sub-PR of **#1472** (dev-PG connection discipline), PR2 audit V3/V4. Unblocks PR2b shrink.

## Problem
Two operator-facing lazy-fill GET routes hold a **pooled** conn across an external SEC EDGAR fetch:

- `GET /instruments/{symbol}/eight_k_filings/{accession}/body` → `eight_k_events.fetch_eight_k_body_now`
- `GET /instruments/{symbol}/business_sections` → `business_summary.fetch_business_summary_body_now`

Each service takes a `conn`, grabs a per-key **session** `pg_advisory_lock` on it, then runs `fetcher.fetch_document_text(...)` while holding the lock, then writes. The lock is session-scoped → the conn can't be released around the fetch without dropping it. Under the #1472 PR2b shrink (`db_pool` 10→4, `max_waiting=0`, `timeout=15`) a conn pinned across the SEC fetch becomes a queue-stall.

## Decision — Option A: service borrows pool, fetch-first, xact-lock
Chosen over (B) route-orchestrated prepare/finalize split and (C) dedicated raw conn holding the lock.

- **Why A over C:** C adds a raw `psycopg.connect()` site — directly against #1472's thesis (134 raw-connect sites are the disease) and settled-decision §"don't add a third pool / use the pool".
- **Why A over B:** A localizes the invariant ("no pooled conn across the SEC fetch") *inside the service* — the route physically can't reintroduce the bug because it no longer does the fetch. B duplicates the release-before-fetch dance across two routes and splits the #938 raw-before-parsed invariant.

### Lock: session `pg_advisory_lock` → `pg_advisory_xact_lock`
Once the conn is borrowed from a pool, a **session** lock leaks into the next borrower (psycopg_pool reset rolls back the tx but does NOT unlock session advisory locks). An **xact** lock dies with the transaction, guaranteed, even on exception. The write phase is one `with pool.connection()` transaction, so the xact lock spans exactly re-check→write and auto-releases at commit. Pool conns are READ COMMITTED → the re-check SELECT after acquiring the lock sees the prior writer's committed fill (prevention-log §409).

### Collapse trade-off (accepted by the issue)
Fetch-first means two concurrent viewers may both fetch (idempotent; the shared SEC rate-limiter absorbs it). The **write** stays collapsed: xact-lock + re-check-`body_deferred` → second caller returns `already` without writing.

## Service shape (both functions)
Signature `(conn, fetcher, *, key)` → `(pool: ConnectionPool, fetcher, *, key)`.

1. **Read deferred-state** (unlocked, read-only) — `with pool.connection() as conn:` read row (+ url + labels/known_items for 8-K). Not deferred → `not_deferred`. Capture `url` (decides *whether* to fetch) and the fetched accession identity. Conn released on block exit.
2. **Fetch** (only if `url` present) — `fetcher.fetch_document_text(url)` holding **no conn**. Transient → raise (503). `url` absent → skip; carry a "no_source" intent into phase 3.
3. **Lock + re-check + ALL writes** — `with pool.connection() as conn:` (one transaction; xact-lock held throughout):
   - `pg_advisory_xact_lock(hashtext(key)::int)` — first statement, opens the implicit tx.
   - re-check `body_deferred` (10-K: **and `source_accession` still equals the fetched accession** — drift ⇒ a newer filing landed, our fetched body is stale); cleared/drifted → `already`.
   - url-absent intent → tombstone / `record_parse_attempt(reason=fetch_other)` → `no_source`.
   - `html is None` / parse-miss → tombstone / `record_parse_attempt` → `failed`.
   - else **body upsert** (flips `body_deferred`) — atomic under the lock.
   - **best-effort manifest** `store_raw` → `transition_status` (#938 order) in a **nested `with conn.transaction()` SAVEPOINT** wrapped in `try/except` — a manifest failure rolls back the savepoint only, never the body upsert (preserves the current deliberate "body cached even if manifest lags" split; #938 raw-before-parsed order kept).
   - capture outcome in a local; `return` after the `with pool.connection()` block (prevention-log §303/§479).

All writes are serialized by the single xact-lock + re-check (Codex ckpt-1b: no-source must not write on stale pre-lock state). No bare `conn.commit()` in the service — the `with pool.connection()` CM commits on clean exit, rolls back on exception. The service now *owns* its conns, so the §359 "don't commit a caller's conn" concern is satisfied by construction.

**Implementation check:** confirm `upsert_business_summary` / `upsert_8k_filing` own staleness guards (filed_at / accession monotonicity, suppression) during coding — the phase-3 `source_accession` re-confirm is defense-in-depth on top of whatever the upsert already enforces, not a substitute for verifying it.

## Route shape (both)
- Drop `conn: ... = Depends(get_conn)`; add `request: Request`.
- Pre-reads (instrument lookup + cross-issuer scope check — security, stays in route) on a hand-driven short borrow (`gen = get_conn(request); conn = next(gen); … conn.commit(); gen.close()`), released **before** the SEC work.
- `with SecFilingsProvider(...) as provider: fetch_*_body_now(request.app.state.db_pool, provider, …)` — service holds no conn across the fetch.
- Post-reads (`get_8k_filing` / `get_business_sections` re-read + CIK) on a fresh short borrow.
- Hand-driven `get_conn(request)` is not the `Depends(get_conn)` default form → guard's `_holds_pooled_conn` is False → route not flagged even though it still references `SecFilingsProvider`.

## Guard
Remove both entries from `scripts/check_pooled_conn_across_http.py::ALLOWLIST`; the set becomes empty. Confirm `python scripts/check_pooled_conn_across_http.py` exits 0.

**Honesty (Codex ckpt-1b):** the guard is a *structural tripwire* — it catches `Depends(get_conn)` + an external marker in a route body, nothing more. It does NOT prove "no pooled conn across the fetch": it can't see a hand-driven `get_conn` left open across `SecFilingsProvider`, nor a service borrowing `pool.connection()` across `fetch_document_text`. The actual invariant is enforced by the **fetch-first service structure** and verified by **manual trace + Codex ckpt-2 + bot review**. Removing the allowlist re-arms the old tripwire (so a future `Depends(get_conn)`-shaped regression on these routes trips); a stronger service-level analyzer is deferred to a tech-debt follow-up. Likewise the "session-lock leak gone" property is prospective for *new* writes (xact-lock can't leak); no pre-existing leak has been observed and a pool-level `DISCARD` is out of scope.

## Tests (`tests/test_lazy_body_deferred.py`)
`conn` fixture (single autocommit raw conn) → `pool` fixture (`ConnectionPool(test_db_url, min_size=1, max_size=2, open=True)`). Update the 3 `fetch_*_body_now(conn, …)` call sites to pass the pool; assertions that read DB state open their own conn (pool or a side conn). Add a regression assertion that the service holds no conn across the fetch is implicit (fetcher stub raises on call for not_deferred path — already present).

## DoD 8-12 (filings ETL)
- **8 smoke** AAPL/GME/MSFT/JPM/HD: hit `/business_sections` + an 8-K body on dev DB; record figures.
- **9 cross-source** one fixture vs SEC EDGAR direct.
- **10 backfill** N/A for parse-version (no parser/schema change) — confirm no `sec_rebuild` needed; record reasoning.
- **11 operator-visible** `/instruments/{symbol}/business_sections` + 8-K body render post-change.
- **12** PR records each + commit SHA.

## Out of scope
PR2b shrink (`DB_POOL_MAX_SIZE` 10→4, `AUDIT_POOL_MAX_SIZE` 2→1) — separate PR after this closes.
