# Credential health as scheduling pre-condition

**Issue:** [#974](https://github.com/Luke-Bradford/eBull/issues/974)
**Phase:** Standalone — sits across orchestrator, WS subscriber, broker_credentials API, admin frontend
**Date:** 2026-05-06
**Author:** Luke + Claude (round 3 design, post-Codex r1 + r2)
**Codex r1:** `.claude/codex-974-r1-review.txt` — 10 high + 5 medium findings, all addressed.
**Codex r2:** `.claude/codex-974-r2-review.txt` — 3 high + 7 medium + 1 low residual findings, all addressed in this v3.

## Why

When broker credentials are not validated, the system today:

1. Keeps running every credential-using batch job on schedule. Each run 401s.
2. Lets dependent layers cascade-fail (`portfolio_sync` writing to `positions` FK-violates against an empty `instruments` table — surfaced as "Database constraint violated" in the admin Problems panel).
3. Caches creds at process start in [`app/services/etoro_websocket.py:419`](../../../app/services/etoro_websocket.py#L419) and reconnects every 5s with the **stale in-memory copy** even after the operator updates the keys via Settings. Fixed 5s backoff = constant log spam.
4. Forces the operator to manually click **Sync now** on the admin page after correcting keys to clear the failure state.

**Principle (operator quote, 2026-05-06):**
> we shouldnt be trying to poll anything if the keys aren't right, should be flagged out of the gate, holding any schedules off

The operator should never have to manually intervene after fixing the very thing the system told them to fix. Credential health must be a scheduling pre-condition.

## Schema reality check (Codex r1.1)

`broker_credentials` has **one row per `(operator_id, provider, label, environment)`**. eToro requires `label='api_key'` AND `label='user_key'` as **two separate rows**. There is no "credential pair" entity — the pair is implicit.

Every health concept in this spec must therefore distinguish:
- **Row-level health** — stored on the individual `broker_credentials` row (UNTESTED / VALID / REJECTED).
- **Operator-level health** — derived aggregate that consumers (orchestrator, WS subscriber, admin UI) actually care about.

Consumers operate on operator-level health only. Row-level health is implementation detail.

## Locked design decisions (revised)

| Decision | Rationale |
|---|---|
| Row-level health column on `broker_credentials`; operator-level computed | Schema reality: row per label. Avoids inventing a synthetic "pair" entity. |
| **REJECTED is sticky** at row level — only `validate-stored` probe success can promote REJECTED → VALID | Codex r1.3 + r3.1: avoids flap from incidental 2xx responses on partial-permission endpoints. The validation probe is the *canonical* signal. |
| Incidental 401/403 from any auth-using path → row REJECTED (sticky). Incidental 2xx → row VALID **only if** old state is `untested`; never overwrites `rejected`. | Codex r1.4 + r3.1: 2xx from a non-probe endpoint is suggestive (auth worked once) but not authoritative; allow it to settle the initial UNTESTED state, but never to clear an explicit REJECTED. |
| Health write-through uses a **dedicated side transaction** committed by the helper, not the caller's tx | Codex r1.5: a caller's rollback must never lose a health write. Helper opens its own connection from the pool, commits, releases. |
| **Operator-level aggregate precedence**: REJECTED > MISSING > UNTESTED > VALID. | Codex r1.2 + r3.2. REJECTED dominates MISSING because if the operator has saved at least one rejected key, they have a concrete "fix your key" to do; reporting MISSING for the other label would mask that. The message "Update the API key in Settings → Providers" applies and naturally surfaces both issues. |
| **NOTIFY payload carries `operator_id`**, not credential_id alone | Codex r1.13: consumers care about operator-level state; row-level credential_id is implementation noise. |
| **Subscribers MUST do a full DB scan on startup** to populate state, before subscribing to NOTIFY. NOTIFY is a wake-up, not the source of truth. | Codex r1.10/11: there is no durable event table for health (unlike `pending_job_requests`). Subscribers cannot recover dropped notifies from a queue. |
| Atomic credential replacement via new `PUT /broker-credentials/replace` endpoint | Codex r1.6: revoke-then-create is non-atomic; pre-flight sees transient MISSING. The replace endpoint does both in one tx. |
| `requires_broker_credential: bool` on `DataLayer` | Codex r1.14: the existing `secret_refs` field is env-secret only, not DB credentials. New flag, no overlap. |
| `requires_layer_initialized: tuple[str, ...]` on `DataLayer` | Codex r1.7 + r2.5 + r3.3 + portfolio_sync FK fix: stricter than `dependencies` (which is per-tick). Means "the named dep's data table is content-initialized per its `INIT_CHECKS` predicate". Content-driven, not job_runs-driven. `portfolio_sync` gets `("universe",)` — fixes the FK cascade in scope, not as sibling. |
| Postgres LISTEN/NOTIFY (not Redis pub-sub, not in-process) | Settled-decisions: Postgres-first; no Redis pub-sub for control plane. Cross-process: API + jobs process + WS subscriber may run separately. The `ebull_job_request` channel at [`app/api/sync.py:202`](../../../app/api/sync.py#L202) is the precedent **for wake-ups only**, not durable delivery. |

## Schema

`sql/128_broker_credentials_health_state.sql`:

```sql
ALTER TABLE broker_credentials
  ADD COLUMN health_state TEXT NOT NULL DEFAULT 'untested'
    CHECK (health_state IN ('untested', 'valid', 'rejected')),
  ADD COLUMN health_state_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ADD COLUMN last_health_check_at TIMESTAMPTZ,
  ADD COLUMN last_health_error TEXT;

-- Index for the operator-level aggregate computation. Filters out
-- revoked rows since they don't participate in health computation.
CREATE INDEX idx_broker_credentials_operator_health
  ON broker_credentials (operator_id, label, health_state)
  WHERE revoked_at IS NULL;

-- Tracks the most recent REJECTED -> VALID transition timestamp per
-- operator. Used by query-time filter for AUTH_EXPIRED failure
-- suppression (Codex r1.7 + r1.8). NULL means no transition yet.
CREATE TABLE operator_credential_health_transitions (
    operator_id              UUID NOT NULL,
    last_recovered_at        TIMESTAMPTZ,
    PRIMARY KEY (operator_id)
);
```

The fourth conceptual state — `MISSING` — is **derived** from absence of either label row for the operator. Not stored at row level.

`operator_credential_health_transitions` exists because we need a single timestamp per operator to query against when filtering AUTH_EXPIRED failure-history rows from operator-visible displays. Updated atomically inside the same side-tx that flips any of the operator's rows from REJECTED → VALID.

## Service layer

New file `app/services/credential_health.py`:

```python
class CredentialHealth(StrEnum):
    UNTESTED = "untested"
    VALID = "valid"
    REJECTED = "rejected"
    MISSING = "missing"  # derived, never stored at row level


REQUIRED_LABELS_BY_PROVIDER: dict[str, tuple[str, ...]] = {
    "etoro": ("api_key", "user_key"),
}


def get_operator_credential_health(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    provider: str = "etoro",
) -> CredentialHealth:
    """Compute the operator's aggregate credential health.

    Aggregation rule (worst-of):
      MISSING   any required label has no non-revoked row
      REJECTED  any non-revoked row for required labels has health_state='rejected'
      UNTESTED  all required labels present, but >=1 row is 'untested'
      VALID     all required labels present and all rows are 'valid'

    Required labels per provider come from REQUIRED_LABELS_BY_PROVIDER.

    Implementation (Codex r2.2 — exact SQL contract, dict_row factory):

      WITH required(label) AS (
        VALUES ('api_key'), ('user_key')
      ),
      observed AS (
        SELECT label, health_state
          FROM broker_credentials
         WHERE operator_id = %(op)s
           AND provider    = %(prov)s
           AND revoked_at IS NULL
      ),
      label_join AS (
        SELECT r.label,
               obs.health_state
          FROM required r
          LEFT JOIN observed obs USING (label)
      )
      SELECT
        bool_or(health_state IS NULL)            AS any_missing,
        bool_or(health_state = 'rejected')       AS any_rejected,
        bool_or(health_state = 'untested')       AS any_untested,
        bool_and(health_state = 'valid')         AS all_valid
      FROM label_join;

    Result is a single dict_row. Decision tree (in order, REJECTED-first
    per locked precedence in the table above):
      1. any_rejected -> REJECTED
      2. any_missing  -> MISSING
      3. any_untested -> UNTESTED
      4. all_valid    -> VALID
      5. otherwise (logical impossibility, e.g. all rows NULL after the
         required CROSS JOIN) raise RuntimeError; do not silently default.

    Tests pin tuple/dict/scalar row-factory shape so a future
    row_factory change cannot accidentally treat partial coverage as
    VALID (Codex r2.2).
    """


def record_row_health_transition(
    *,
    credential_id: UUID,
    new_state: Literal["valid", "rejected"],
    source: Literal["probe", "incidental"],
    error_detail: str | None,
    pool: psycopg.ConnectionPool,
) -> None:
    """Update a single row's health and pg_notify the operator-scoped channel.

    Side-transaction contract (Codex r1.5 + r2.1):
      - Acquires its OWN connection from the pool. Does NOT take a conn arg.
      - UPDATE the row in this side tx.
      - REJECTED is sticky: if old_state == 'rejected' and new_state ==
        'valid', the UPDATE only proceeds when called from the explicit
        validation-probe path (parameter source='probe' below).
        Incidental 2xx (source='incidental') only writes valid when
        old_state in ('untested',) — never overwrites rejected.
      - On VALID transition: also UPSERT operator_credential_health_transitions
        with last_recovered_at=NOW() so AUTH_EXPIRED suppression has a
        timestamp to filter against.
      - COMMIT the side tx.
      - pg_notify('ebull_credential_health', payload) AFTER commit
        (Postgres delivers notifies on commit anyway, but committing
        first means the notify carries the durably-stored state).
      - Idempotent: same-state transitions skip the NOTIFY.

    Pool-exhaustion semantics (Codex r2.1):
      - If the pool acquire times out (PoolTimeout): RAISE — do NOT
        swallow. Caller is responsible for catching and deciding whether
        the auth-using operation should fail or proceed without a
        health update. We log at ERROR with credential_id + intended
        new_state so the dropped write is greppable.
      - We expose a metric counter `credential_health_write_failed`
        (label: reason=pool_timeout|db_error|integrity_violation) so
        a sustained pool-pressure regression is visible.
      - Callers in the auth-using request path catch PoolTimeout and
        log a single warning per request, but do NOT fail the user-
        facing call on health-write failure (auth itself succeeded or
        failed independently). Auth outcome bookkeeping is best-effort
        beyond the side-tx contract — but the surrounding NOTIFY/health
        pipeline IS guaranteed once the row write commits.

    Payload shape:
      {
        "operator_id": "<uuid>",
        "provider": "etoro",
        "old_aggregate": "rejected",
        "new_aggregate": "valid",
        "at": "2026-05-06T20:25:00Z"
      }

    Note: payload carries OPERATOR-LEVEL aggregate, not the row state
    that triggered the transition. The helper recomputes the operator
    aggregate after the row update and emits the notify only if the
    aggregate actually moved (idempotent).
    """


def record_health_outcome(
    *,
    credential_id: UUID,
    success: bool,
    source: Literal["probe", "incidental"],
    error_detail: str | None,
    pool: psycopg.ConnectionPool,
) -> None:
    """Public write-through helper for auth-using paths.

    success=True, source='probe'      -> row VALID (clears REJECTED)
    success=True, source='incidental' -> row VALID iff old in ('untested',); ignored if 'rejected'
    success=False (any source)        -> row REJECTED (sticky)
    """
```

**REJECTED-stickiness invariant** (Codex r1.3):

```
For any row with health_state='rejected':
  -> The ONLY paths that may flip it to 'valid' are:
     1. POST /broker-credentials/validate-stored returning 200 (source='probe')
     2. PUT /broker-credentials/replace creating a fresh row (replaces the
        rejected row entirely; the new row starts at 'untested')

  -> Incidental 2xx responses from any other auth-using path do NOT
     flip rejected to valid.
```

This is enforced inside `record_row_health_transition` by the `source` parameter check.

## NOTIFY contract

Channel: `ebull_credential_health`. Wake-up only. Payload carries operator-level aggregate.

**Subscribers MUST**:
1. On startup: full-table scan of `broker_credentials` (filtered to non-revoked) joined to compute operator aggregate per operator. Populate in-memory cache.
2. THEN subscribe to NOTIFY.
3. On NOTIFY arrival: re-read DB truth for that operator (don't trust payload alone). Update cache.
4. On 5s poll fallback: re-read DB truth for any operator whose cache is older than the threshold OR for the full table if the listener thread has been disconnected.

This is more conservative than the `ebull_job_request` pattern because there is no durable event table to recover from (Codex r1.10).

**Startup retry contract (Codex r2.8):** the listener thread runs the initial full-scan with retry-with-backoff (1s, 2s, 5s, 10s, 30s cap) until success. While the initial scan has not completed, the cache is in `INITIALIZING` state and consumers (orchestrator pre-flight, WS subscriber) treat it as `MISSING` for safety — no credential-using layers run, WS stays disconnected. This avoids any false-VALID window during a slow DB warm-up.

The `INITIALIZING` state is exposed on `GET /system/status` so the admin UI can show a transient "Initializing credential health…" banner instead of a confusing "MISSING" during a clean restart.

## Orchestrator pre-flight gate

`DataLayer` registry at [`app/services/sync_orchestrator/registry.py:84`](../../../app/services/sync_orchestrator/registry.py#L84) gets two new fields:

```python
@dataclass(frozen=True)
class DataLayer:
    ...
    requires_broker_credential: bool = False
    # Layers whose data table must be content-initialized (i.e. has at
    # least one usable row per the layer's INIT_CHECKS predicate)
    # before this layer is eligible. Stricter than `dependencies`
    # which only requires the dep to have completed in the current
    # run. Used to break the FK-violation cascade where portfolio_sync
    # writes to `positions` referencing `instruments` that universe
    # has not yet populated. (Codex r1.7 / r2.5 / r3.3)
    requires_layer_initialized: tuple[str, ...] = ()
```

Layers tagged `requires_broker_credential=True`: `universe`, `portfolio_sync`, `candles`, `fundamentals`, `fx_rates`, `cost_models`. (Verified by reading each refresh function during implementation; PR description records the audit.)

`portfolio_sync` additionally gets `requires_layer_initialized=("universe",)` — fixes the FK cascade explicitly. (Codex r2.5/r2.6.)

**The init-check is content-driven, not job_runs-driven.** Each layer name in `requires_layer_initialized` is mapped to a content predicate that asserts the layer's data table is non-empty. This is more reliable than chasing `job_runs.status` semantics across cleanup/archival:

```python
INIT_CHECKS: dict[str, str] = {
    "universe":     "SELECT EXISTS (SELECT 1 FROM instruments WHERE is_tradable = true)",
    "candles":      "SELECT EXISTS (SELECT 1 FROM quotes)",
    "fundamentals": "SELECT EXISTS (SELECT 1 FROM financial_facts_raw)",
    # ...one per layer that any other layer depends on at init time.
}
```

The init check answers "has this dep produced ANY rows the dependent layer needs". For `portfolio_sync` waiting on `universe`, that means `instruments` table is non-empty AND has at least one tradable row. This is stricter than "any row" because a partial-failure universe sync could leave only delisted rows — those don't unblock portfolio_sync writes.

The existing index `idx_instruments_is_tradable` (verify during implementation; if absent, add one) supports the EXISTS short-circuit.

Executor change at [`app/services/sync_orchestrator/executor.py`](../../../app/services/sync_orchestrator/executor.py): the existing `_blocking_dependency_failed` check at line 286 gets two sibling checks running BEFORE it:
1. `_credential_health_blocks(layer)` — returns `PREREQ_SKIP` if `requires_broker_credential` and operator health ≠ VALID.
2. `_layer_initialization_blocks(layer)` — returns `PREREQ_SKIP` if any name in `requires_layer_initialized` fails its `INIT_CHECKS` predicate.

Cascade-skip via the existing `_blocking_dependency_failed` mechanism handles dependent layers.

## AUTH_EXPIRED failure-row suppression (revised)

Codex r1.7/r1.8 caught that mutating `consecutive_failures` rows is wrong. Replaced with a **query-time filter**:

When the orchestrator computes operator-visible problem rows, AUTH_EXPIRED failures with `failed_at < operator_credential_health_transitions.last_recovered_at` are excluded from the streak count.

**Implementation surface (Codex r2.9):** the suppression must apply to BOTH the single-layer helper at [`layer_failure_history.py:43`](../../../app/services/sync_orchestrator/layer_failure_history.py#L43) AND the batched helper `all_layer_histories` invoked at [`app/api/sync.py:283,362`](../../../app/api/sync.py#L283) — that's the path operator-visible v2 takes. Both signatures gain `suppress_auth_expired_before: datetime | None = None`. The `/system/status` and `/sync/layers` API handlers populate it from `operator_credential_health_transitions.last_recovered_at` for the calling operator.

**Initial-row + NULL semantics (Codex r2.4):**
- `operator_credential_health_transitions` has NO row written until the first REJECTED → VALID transition occurs for that operator. Missing-row case = no recovery has ever happened.
- When the suppression API resolves `suppress_auth_expired_before`:
  - Missing row OR `last_recovered_at IS NULL` → pass `None` to the helper → no filter applied (all AUTH_EXPIRED rows visible).
  - Row exists with non-null timestamp → pass that timestamp → filter applied.
- Tests pin both branches: missing-row, NULL-row, and recent-row.

This means:
- AUTH_EXPIRED failures from the rejected window stay in `job_runs` (audit history is immutable).
- They no longer count toward operator-visible "consecutive failures" once the operator has saved valid creds.
- New failures (RATE_LIMITED, SOURCE_DOWN, etc.) after the recovery timestamp are visible normally.

## WS subscriber reload (revised, Codex r1.11)

[`app/services/etoro_websocket.py:411`](../../../app/services/etoro_websocket.py#L411) currently takes `api_key`/`user_key` as construction args. Refactor to take `operator_id` and pull credentials from DB.

**Callsite enumeration (Codex r2.10):** the only production constructor is at [`app/main.py:298`](../../../app/main.py#L298) (FastAPI lifespan). Tests construct via fixtures in `tests/test_etoro_websocket.py`. The refactor:
1. Removes the `api_key` / `user_key` parameters entirely from `__init__`.
2. Adds `operator_id: UUID` and `pool: psycopg.ConnectionPool`.
3. Updates the lifespan call site to pass operator_id (loaded from session bootstrap during startup) + the existing pool.
4. Updates all test constructors to pass operator_id + a test-pool fixture.

PR #974/D acceptance includes: grep `EtoroWebSocketSubscriber\(` returns zero matches with `api_key=` or `user_key=` after the change. Enforced as part of `uv run ruff check .`-grade discipline; not a separate lint rule.

```python
class EtoroWebSocketSubscriber:
    def __init__(
        self,
        *,
        operator_id: UUID,
        pool: psycopg.ConnectionPool,
        ...
    ) -> None:
        self._operator_id = operator_id
        self._pool = pool
        self._api_key: str | None = None
        self._user_key: str | None = None
        self._consecutive_auth_failures: int = 0

    def _reload_credentials(self) -> bool:
        """Re-read credentials from DB. Returns True iff both labels
        present and operator health = VALID."""

    async def _run(self) -> None:
        """Background loop:
          1. On startup + every 5s poll fallback: re-read DB.
          2. Subscribe to ebull_credential_health NOTIFY.
          3. On VALID notify or DB-confirmed VALID: connect/reconnect.
          4. On REJECTED: drop connection; stay disconnected until VALID.
          5. Auth-failure backoff: 5s, 30s, 2min, 10min, 10min cap.
             Reset to 5s on first successful auth.
          6. On every auth reply: write through via record_health_outcome
             (source='incidental' for 2xx, always for 4xx).
        """
```

Backoff sequence: **(5, 30, 120, 600, 600)** — capped at 600s after the 4th consecutive auth failure. Locked here, asserted by tests in #974/D. Reset to 5s on the first successful auth.

**Prolonged REJECTED behavior (Codex r2.11):** at the 600s cap, an operator who never fixes their keys would still see ~144 reconnect attempts per day. That's wasteful and unfriendly to eToro. New rule:

- After health = REJECTED is observed (via NOTIFY or the WS's own write-through of a 401), the WS subscriber **stops auto-reconnecting entirely**. Backoff is irrelevant in this state.
- The only path back to reconnecting is a NOTIFY of `new_aggregate=valid` from the credential-health channel.
- The 5s health-cache poll fallback continues — that's how the WS detects a missed VALID notify.
- Acceptance test: simulate REJECTED for 1 hour (mocked clock); assert zero reconnect attempts; then simulate a VALID notify; assert reconnect within one cache poll cycle (5s).

This means the backoff sequence (5,30,120,600,600) only applies during transient auth failures (server-side hiccup, eToro rotation lag) where health hasn't yet flipped to REJECTED. The instant health flips REJECTED, the WS goes quiet.

## Atomic credential replacement (Codex r1.6)

New endpoint `PUT /broker-credentials/replace` in [`app/api/broker_credentials.py`](../../../app/api/broker_credentials.py):

```
PUT /broker-credentials/replace
Body:
  { "provider": "etoro",
    "label": "api_key",
    "environment": "demo",
    "secret": "<new-plaintext>" }

Behavior:
  In ONE transaction:
    1. SET revoked_at = NOW() WHERE operator_id, provider, label, environment
       AND revoked_at IS NULL.
    2. INSERT a new row with the new ciphertext, health_state='untested'.
    3. Recompute operator aggregate.
    4. NOTIFY (in side tx after commit, per the contract above).
```

The wizard + Settings page switch from "delete + create" to a single PUT call. Pre-flight gate never sees a transient MISSING.

The existing `POST /broker-credentials` stays for the genuine first-create case (no existing row). The frontend chooses which based on whether a row of that label already exists.

**Identical-secret semantics (Codex r2.3):** when `PUT /replace` is called with a new secret whose ciphertext (after AAD-bound AESGCM) decrypts to the same plaintext as the active row's, **no row update happens** and **no notify fires**. Implementation: decrypt the active row inside the transaction, compare plaintext, short-circuit on match. Returns 200 with body `{"changed": false}`. Avoids spurious VALID → UNTESTED → VALID flap from a re-save. Tests pin both branches (changed=true and changed=false).

**DELETE+POST deprecation (Codex r2.7):** the existing `DELETE /broker-credentials/{id}` followed by `POST /broker-credentials` flow remains in the codebase but is **deprecated for active eToro labels**. The new policy:

1. `DELETE /broker-credentials/{id}` returns `409 Conflict` with body `{"error": "use_replace_endpoint", "message": "Active broker credentials must be updated via PUT /broker-credentials/replace; DELETE is reserved for permanent revocation"}` when:
   - The credential being deleted has `provider='etoro'` AND `revoked_at IS NULL`.
   - There is no concurrent `PUT /broker-credentials/replace` with the same `(operator, provider, label)` already in flight (best-effort detection: a new row with `created_at` within the last 5 seconds and matching label).
2. Operators can still permanently revoke via the Settings UI, which calls a separate `DELETE` with explicit `revoke=true` confirmation; that path proceeds and emits a MISSING transition.
3. Old clients still calling DELETE+POST will see the 409, fail loudly, and prompt the operator to refresh. This is preferable to silent transient MISSING. The frontend bumps a `client_version` header so the server can log when an outdated client tries the old flow.

## Frontend integration

`/system/status` API extends with operator credential health:

```typescript
interface SystemStatusResponse {
  ...
  credential_health: {
    state: "missing" | "untested" | "valid" | "rejected";
    last_validated_at: string | null;
    last_recovered_at: string | null;
    last_error: string | null;
  };
}
```

Admin Problems panel at [`frontend/src/components/admin/ProblemsPanel.tsx`](../../../frontend/src/components/admin/ProblemsPanel.tsx):
- If `credential_health.state === 'rejected'`: render single high-severity row "Credentials rejected by provider — update the API key in [Settings → Providers]". Other items tagged `error_category === 'AUTH_EXPIRED'` are folded under that row (collapsed, expandable).
- If `valid`/`untested`/`missing`: render the panel as today, but `AUTH_EXPIRED` rows from before `last_recovered_at` are filtered out server-side (handled by the suppression query above).

Setup wizard's broker step (and Settings cred form, both pre-#971) call the new `PUT /broker-credentials/replace` instead of DELETE + POST when an existing row is present. Validation always uses `POST /broker-credentials/validate-stored` after save (the canonical probe path that promotes REJECTED → VALID).

## Test plan

### Unit tests

`tests/test_credential_health.py`:
- `get_operator_credential_health` returns each of MISSING/UNTESTED/VALID/REJECTED for the right row mixes — including missing-label cases (api_key valid, user_key absent → MISSING).
- `record_row_health_transition` REJECTED sticky: source='incidental' success on a rejected row leaves it rejected.
- `record_row_health_transition` REJECTED clears: source='probe' success on a rejected row promotes to valid.
- Side-tx commit: helper commits even if a separate caller-supplied tx rolls back (simulated via concurrent calls).
- Idempotent: same-state transition does not NOTIFY.

`tests/test_credential_health_listener.py`:
- Notify arrives at subscriber within 1s.
- 5s poll fallback recovers from dropped notify.
- Startup full-scan populates cache for operators not in any in-flight notify.

`tests/test_sync_orchestrator_credential_gate.py`:
- `requires_broker_credential=True` layer PREREQ_SKIPs when health=REJECTED.
- `requires_broker_credential=False` layer runs normally.
- Cascade: dependent layer skips via existing mechanism.
- `requires_layer_initialized=("universe",)`: portfolio_sync skips until universe has historical success.
- AUTH_EXPIRED suppression: failures from the rejected window are excluded from operator-visible streak counts after a VALID transition; new RATE_LIMITED failures still surface.

`tests/test_etoro_websocket.py` extension:
- WS reload picks up new keys without process restart (notify-driven).
- Backoff sequence asserted: (5, 30, 120, 600, 600).
- Counter resets to 0 on successful auth.
- Auth-failure write-through emits the right `record_health_outcome` calls.

### Integration tests

`tests/test_credential_health_e2e.py` (real Postgres):
- Save creds → both labels UNTESTED → operator UNTESTED.
- Validate-stored success → both labels VALID → operator VALID; subscribers see notify.
- 401 from any auth-using path → row REJECTED (sticky); operator REJECTED; orchestrator pauses dependent layers; WS subscriber drops connection.
- Replace flow: PUT with corrected secret → atomic revoke+insert; pre-flight never sees MISSING.
- Validate-stored success after replace → operator VALID; orchestrator resumes; WS reconnects.
- No manual Sync-now click required.

### Frontend tests

`ProblemsPanel.test.tsx`:
- REJECTED state shows single banner; AUTH_EXPIRED rows folded.
- VALID state shows all rows independently (none from before `last_recovered_at`).
- MISSING state shows "Save credentials in Settings → Providers" with a deep link.

### Smoke test (manual operator)

1. Fresh install, save deliberately-swapped keys via wizard.
2. Wait for next sync tick. Confirm: single banner, no cascade rows, no FK violation in logs (`portfolio_sync` skipped via `requires_layer_initialized`).
3. Update keys to correct values via Settings (PUT replace).
4. **Without clicking anything else**, wait one sync cycle. Confirm: banner gone, layers running, admin Problems panel clean, WS subscriber connected.

## Decomposition into child PRs

Each child = its own branch + PR. **A and B ship first** (foundational); **C/D/E/F in parallel** consume them. Codex r2 review covers the decomposition shape (CLAUDE.md checkpoint 1b) — pending.

### #974/A — Schema + state machine + write-through helper + replace endpoint

**Branch:** `feature/974A-credential-health-state-machine`
**Files:**
- New: `sql/128_broker_credentials_health_state.sql` (col + index + transitions table).
- New: `app/services/credential_health.py` (state machine + side-tx helpers).
- New: `app/api/broker_credentials.py` `PUT /replace` endpoint.
- Edit: existing `POST /broker-credentials` (`create`), `DELETE /{id}`, `POST /validate`, `POST /validate-stored` — call `record_health_outcome` with appropriate source.
- New: `tests/test_credential_health.py`.

**Acceptance:**
- Migration applies cleanly; existing rows backfill `untested`.
- All POST/DELETE/validate/replace paths write health correctly.
- REJECTED-stickiness covered by tests; race scenario (concurrent probe + incidental) covered.
- pg_notify fires once per real operator-aggregate transition, never on idempotent same-state.

### #974/B — LISTEN/NOTIFY listener + operator-health cache

**Branch:** `feature/974B-credential-health-pubsub`
**Files:**
- New: `app/jobs/credential_health_listener.py` (mirrors [`app/jobs/listener.py`](../../../app/jobs/listener.py) shape).
- Edit: [`app/jobs/__main__.py`](../../../app/jobs/__main__.py) — start the listener thread alongside the existing job listener.
- New: `app/services/credential_health_cache.py` — in-memory cache populated from DB scan + notifies + 5s poll fallback.
- New: `tests/test_credential_health_listener.py`.

**Acceptance:**
- Notify on `ebull_credential_health` arrives at subscriber within 1s.
- 5s poll fallback recovers from dropped notify.
- Startup full-scan populates cache for operators not in any pending notify.
- Cache returns MISSING for an operator with no rows.

### #974/C — Orchestrator pre-flight gate + AUTH_EXPIRED suppression

**Branch:** `feature/974C-orchestrator-credential-gate`
**Files:**
- Edit: [`app/services/sync_orchestrator/registry.py`](../../../app/services/sync_orchestrator/registry.py) — add fields; tag relevant layers; `portfolio_sync` gets `requires_layer_initialized=("universe",)`.
- Edit: [`app/services/sync_orchestrator/executor.py:286`](../../../app/services/sync_orchestrator/executor.py#L286) — add the two sibling checks before `_blocking_dependency_failed`.
- Edit: [`app/services/sync_orchestrator/layer_failure_history.py`](../../../app/services/sync_orchestrator/layer_failure_history.py) `consecutive_failures` — add `suppress_auth_expired_before` parameter; update the v2 API endpoint to pass it.
- New: `tests/test_sync_orchestrator_credential_gate.py`.

**Acceptance:**
- Layer tagged `requires_broker_credential=True` PREREQ_SKIPs when health=REJECTED.
- Dependent layers cascade-skip via existing mechanism.
- `portfolio_sync` PREREQ_SKIPs until `INIT_CHECKS["universe"]` returns true (i.e. `instruments` table has ≥1 tradable row).
- AUTH_EXPIRED rows from before `last_recovered_at` are excluded from streak counts.

### #974/D — WS subscriber operator-scoped reload + backoff

**Branch:** `feature/974D-ws-subscriber-reload-backoff`
**Files:**
- Edit: [`app/services/etoro_websocket.py:411`](../../../app/services/etoro_websocket.py#L411) — refactor `__init__` to `operator_id` + pool; add `_reload_credentials`.
- Edit: [`app/services/etoro_websocket.py:671,692`](../../../app/services/etoro_websocket.py#L671) — backoff sequence + write-through.
- Edit: subscribe to `ebull_credential_health` notifies; reload + reconnect on VALID; pause on REJECTED.
- Edit: callers that construct the subscriber to pass `operator_id`+pool instead of raw keys.
- Edit: `tests/test_etoro_websocket.py` — backoff + reload coverage.

**Acceptance:**
- After REJECTED notify: WS drops connection and stops reconnecting.
- After VALID notify: WS reads fresh creds + reconnects.
- Backoff sequence exact: 5, 30, 120, 600, 600.
- No 5s/loop spam during prolonged auth failure.
- Auth-failure write-through correctly emits `record_health_outcome(source='incidental')`.

### #974/E — Admin UI cred-health banner + cascade-row hide

**Branch:** `feature/974E-admin-cred-health-banner`
**Files:**
- Edit: `/system/status` handler — include `credential_health` in response.
- Edit: [`frontend/src/api/types.ts`](../../../frontend/src/api/types.ts) — add `CredentialHealth`.
- Edit: [`frontend/src/components/admin/ProblemsPanel.tsx`](../../../frontend/src/components/admin/ProblemsPanel.tsx) — single banner when REJECTED, fold AUTH_EXPIRED rows.
- Edit: [`frontend/src/pages/AdminPage.tsx`](../../../frontend/src/pages/AdminPage.tsx) — verify polling cadence is acceptable; no SSE.
- Edit: `ProblemsPanel.test.tsx`.

**Acceptance:**
- Single banner when `credential_health.state==='rejected'`; cascade rows folded.
- After operator saves valid keys via PUT replace, admin page reflects state within one poll cycle.
- All existing ProblemsPanel cases unchanged when state==='valid'.

### #974/F — Settings + wizard switch to PUT /replace

**Branch:** `feature/974F-frontend-credential-replace`
**Files:**
- Edit: [`frontend/src/api/brokerCredentials.ts`](../../../frontend/src/api/brokerCredentials.ts) — add `replaceBrokerCredential`.
- Edit: [`frontend/src/pages/SettingsPage.tsx`](../../../frontend/src/pages/SettingsPage.tsx) — use replace when row exists for label.
- Edit: [`frontend/src/pages/SetupPage.tsx`](../../../frontend/src/pages/SetupPage.tsx) — same.
- Edit: respective tests.

**Acceptance:**
- Editing an existing credential calls PUT replace, not DELETE + POST.
- New credentials still call POST.
- Atomic from frontend perspective — no transient MISSING state.

## Out of scope

- Multi-cred per operator (#971 wizard simplification — separate ticket).
- Multi-broker (eToro is the only v1 broker).
- The recovery-phrase ceremony (#972 — separate ADR amendment).
- The eToro key terminology mismatch (#973 — separate label fix).
- Generic "any credential rejected anywhere" — scope is broker_credentials only. EBULL_SECRETS_KEY and other env-secret paths are unchanged.

## ETL DoD clauses 8-12

**N/A.** This change touches orchestrator + auth lifecycle, not filings ETL / parsers / ingest pipelines / schema migrations affecting ownership data. Standard PR DoD applies (clauses 1-7).

## Codex sign-off

- Round 1: see `.claude/codex-974-r1-review.txt`. 10 high + 5 medium findings; all addressed in this v2.
- Round 2 (this revision): mandatory per CLAUDE.md checkpoint 1b — re-review v2 spec for residual gaps before child-ticket dispatch.
- Round 3 (per-PR plan + diff review): mandatory per CLAUDE.md checkpoint 2 for each child PR.
