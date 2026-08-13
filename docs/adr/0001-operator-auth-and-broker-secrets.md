# ADR 0001 — Operator authentication and encrypted broker-secret storage

**Status:** Accepted
**Date:** 2026-04-07

---

## Context

eBull is a single-operator, long-horizon investment engine that controls real
money via the eToro broker API. Until now:

- The backend has been protected by a single static bearer token
  (`settings.api_key`), supplied via `Authorization: Bearer <token>`. This was
  shipped as a deliberate fail-closed minimum (see [app/api/auth.py](../../app/api/auth.py)).
- The frontend has no login flow. The token wiring slot in
  [frontend/src/api/client.ts](../../frontend/src/api/client.ts) is unused, so
  every browser request returns 401 and the dashboard renders entirely as
  error states.
- The eToro API key is read from an environment variable
  (`settings.etoro_api_key`) and lives in `.env` for the lifetime of the
  process.

Two problems follow from this:

1. **The frontend is unusable.** None of the operator pages (dashboard,
   rankings, recommendations, admin, settings) can render real data without a
   real session model in the browser.
2. **Broker secrets live in plaintext on disk.** A `.env` file holding the
   live eToro API key is acceptable for early scaffolding but is not an
   acceptable steady state for a tool that places real orders. It also has no
   audit trail — there is no record of when the key was read, by what code
   path, or whether reads succeeded.

This ADR records the decisions that govern how we close both gaps, and the
boundaries we are deliberately *not* crossing in v1.

---

## Decision

We will introduce **operator browser authentication** and **encrypted
broker-credential storage** as three sequenced tickets, governed by the
decisions below.

### 1. Auth mechanism: username + password, no OAuth

Operator login uses a username and an Argon2id password hash, stored in a new
`operators` table. There is no OAuth, no SSO, no magic link, and no email
column in v1.

Reasons:

- eBull is single-operator. The main value of OAuth — outsourcing user
  management for many users — does not apply.
- OAuth would introduce an external dependency on a third-party identity
  provider for the one tool that controls real positions. A locked Google
  account or a drifted OAuth app config must not be able to lock the operator
  out of their own kill switch.
- Email-based flows (reset, magic link, verification) require SMTP
  infrastructure and introduce email as an attack surface. Neither is
  justified for one operator.
- Password-only is the boring, well-understood baseline. It does not preclude
  adding WebAuthn as a second factor later (see Operational preconditions).

Password change is performed via a CLI command. There is no in-app password
reset flow in v1; if the operator forgets the password, they re-bootstrap
from the CLI.

### 2. Sessions: server-side rows, opaque cookie, no JWT

Browser sessions are stored as rows in a new `sessions` table. The cookie
holds a random opaque session ID, not a JWT. Cookie attributes:
`HttpOnly`, `Secure`, `SameSite=Lax`, `Path=/`.

Reasons:

- Server-side sessions support immediate logout (delete the row) and
  forced logout-everywhere (delete all rows for an operator). Stateless JWTs
  cannot do either without bolting on a revocation list, at which point they
  are no longer stateless.
- JWTs in `localStorage` are the pattern that recurs in browser-app breach
  post-mortems. We are not doing it.
- Idle and absolute session timeouts are enforced server-side against the
  session row, not from claims that a client could replay.

### 3. `operators` table exists even with one row

A real `operators` table is introduced from day one, even though it will hold
exactly one row in v1.

Reasons:

- Sessions need a real subject. A session that points at "the system" is not
  auditable; one that points at `operator_id` is.
- Encrypted broker credentials need a real owner. The alternative — a magic
  constant or a nullable owner column — is harder to walk back than carrying
  a one-row table.
- Migrating an existing secrets table and an existing sessions table to
  acquire an owner *after the fact* is uglier than starting with the anchor
  in place.

This is **not** a step toward multi-user eBull. Existing tables (`portfolio`,
`recommendations`, `decision_audit`) do **not** gain an `operator_id` column
in this work. The `operators` table exists solely as the identity anchor for
sessions and broker credentials.

### 4. Static token survives, renamed to `service_token`

The existing `settings.api_key` is renamed to `settings.service_token` and
preserved for non-browser callers: tests, scripts, ops/admin tooling, cron.

Route protection becomes explicit per route:

- **Browser-session-only** routes (`require_session`): broker-credential
  management, anything that mutates operator-owned secrets.
- **Service-token-only** routes (`require_service_token`): automation
  endpoints called by jobs or scripts.
- **Either** routes (`require_session_or_service_token`): read-only
  operator-facing endpoints that are also useful from scripts.

The intent is recorded at the route, not implied by a global default.

### 5. Broker secrets: app-layer AESGCM, env-keyed, audited

Broker credentials are stored in a new `broker_credentials` table as
ciphertext. The encryption scheme:

- **Algorithm:** AES-256-GCM via
  `cryptography.hazmat.primitives.ciphers.aead.AESGCM`. No home-grown crypto.
- **Key source:** `EBULL_SECRETS_KEY` environment variable. The server fails
  to start if it is missing or the wrong length. There is no fallback,
  no default, and no "generate one if missing" mode.
- **Key versioning:** the table carries a `key_version` column from day one.
  v1 has a single key version; the dispatch is in place so that future
  rotation is a re-encrypt-and-bump rather than a schema change.
- **Associated data:** every encryption binds
  `f"{operator_id}|{provider}|{label}|{key_version}"` as AEAD associated
  data. Decryption verifies it. This means a ciphertext copied between rows,
  between operators, or replayed across a key rotation will fail to decrypt,
  not silently produce another row's secret.
- **Decryption is server-side only.** Plaintext never leaves the backend.
- **Decryption is audited.** Every call to the decrypt path writes a row to
  `broker_credential_access_log`, including failures. The audit log is the
  primary forensic artifact if a key is suspected of misuse.
- **Write-only from the UI.** The frontend can create credentials and revoke
  them. It cannot read them back, cannot "show once", and does not get a
  decrypt-on-demand affordance for any reason. The UI sees metadata only:
  provider, label, last four characters, timestamps, revoked state.

### 6. ADR-first, then three sequenced tickets

The work is split into:

- **Ticket A** — operator browser auth & session.
- **Ticket B** — encrypted broker credential storage tied to operator.
- **Ticket C** — migrate eToro credential consumption from env to encrypted
  store, update the deployment guide.

Tickets D (WebAuthn / 2FA), E (login attempt audit + lockout), and F
(password change UI) are opened as follow-ups but not implemented in this
sequence. Ticket D is flagged as a precondition for live trading — see
Operational preconditions.

This ADR is merged before any of A/B/C so that implementation review focuses
on implementation, not on re-litigating these decisions.

---

## Consequences

### Positive

- The frontend becomes usable end-to-end as soon as Ticket A merges.
- Broker secrets stop living in `.env` once Ticket C merges.
- Every read of a broker secret leaves an audit row.
- The static token path remains available for tests, scripts, and ops without
  weakening browser auth.
- The `operators` + `sessions` + `broker_credentials` shape is forward
  compatible with WebAuthn (Ticket D) and a real lockout policy (Ticket E)
  without further schema churn.
- Key versioning is in place from day one, so the first rotation is a
  routine operation rather than a migration.

### Negative / accepted costs

- The ADR + three tickets is more upfront work than a single quick fix.
- Adding an `operators` table for one operator looks like over-engineering at
  a glance. The justification is the identity anchor; the cost is one
  migration and one bootstrap CLI command.
- Password-only login is weaker than password + WebAuthn. We accept this
  **only for non-live-trading use** in the window between Ticket A merging
  and Ticket D merging. Live trading against a funded account must not begin
  in that window — Ticket D is a hard gate, recorded again under
  Operational preconditions.
- A forgotten password requires CLI access to the host. That is acceptable
  for a self-hosted single-operator tool and unacceptable for a hosted
  product; eBull is the former.

### Out of scope (explicitly)

The following are **not** part of this work and must not be smuggled into
Tickets A/B/C:

- OAuth, SSO, federated identity, social login.
- SMTP, email verification, password reset emails, magic links, "forgot
  password" flow.
- Email column on `operators`.
- RBAC, roles, permissions, scopes.
- `operator_id` columns on `portfolio`, `recommendations`, `decision_audit`,
  or any other existing domain table.
- Multi-operator UI, operator management UI, operator invitation flow.
- Hardware-backed key storage on the operator's machine.
- HSM or cloud KMS integration for `EBULL_SECRETS_KEY`.
- Automated key rotation. Manual rotation is documented; automation is a
  later ticket if it ever matters.
- Per-credential trading scopes (eToro's API does not expose this
  granularity).
- Returning broker plaintext to the frontend under any circumstance —
  including "just this once for the user to verify".

---

## Security assumptions

These assumptions are load-bearing. If any of them stops being true, this
ADR must be revisited before continuing.

1. **Same-origin frontend and backend in production.** The Vite dev proxy is
   the dev-mode equivalent. Cross-origin browser auth is not in scope.
2. **CSRF tokens are deferred** on the strength of (a) same-origin, (b)
   `HttpOnly` + `Secure` + `SameSite=Lax` cookies, (c) a tight CORS
   allowlist. If the frontend is ever served from a different origin, CSRF
   tokens become mandatory and this ADR is revisited.
3. **Threat model for broker secrets is "stolen DB dump", not "rooted host".**
   Env-keyed AESGCM defeats the former. It does not defeat the latter, and
   no software-only scheme short of an HSM does. We accept this.
4. **The operator's host is trusted.** A compromised operator workstation can
   read the session cookie and the eToro API key in flight. This is true of
   every browser-based operator tool and is not in scope to fix.
5. **`EBULL_SECRETS_KEY` is provisioned out of band**, stored in a password
   manager, and never committed. The deployment guide (#94) will document
   the bootstrap procedure.
6. **Generic 401 discipline is preserved.** Login failures, missing
   credentials, expired sessions, and invalid service tokens all return the
   same opaque 401. Callers cannot distinguish failure modes.
7. **Argon2id parameters are chosen at implementation time** to target
   ~250–500ms on the deployment host, and are recorded in the password hash
   itself so that future tuning does not invalidate existing hashes.

---

## Operational preconditions

These conditions must hold before eBull is used to place real orders against
a funded account:

1. **Ticket A merged** — operator login, sessions, RequireAuth, /auth/me.
2. **Ticket B merged** — broker credentials stored encrypted, never in env.
3. **Ticket C merged** — eToro provider reads from the encrypted store,
   `settings.etoro_api_key` removed from the codebase.
4. **Ticket D merged** — WebAuthn / 2FA on operator login. Password-only is
   the v1 baseline, not the live-trading baseline. **Hard gate.**
5. **`EBULL_SECRETS_KEY` rotated** away from any value used during
   development or testing.
6. **Deployment guide (#94) reflects the post-Ticket-C reality** — no
   `ETORO_API_KEY` in env, bootstrap via CLI, key entered through the
   Settings UI.

Strongly recommended, but not a hard gate:

- **Ticket E merged** — durable login attempt audit and lockout policy.
  In-process rate limiting from Ticket A is a stopgap, not a steady state.
  Live trading should not begin without it for long, but it is not a
  blocking precondition the way Ticket D is.

Tickets A/B/C are sufficient to make the application *functional*. They are
not sufficient to make it *safe to trade real money with*. That bar is the
full list above.

---

## Follow-up tickets

Opened as part of this work, scoped from this ADR, not implemented in the
A/B/C sequence:

- **Ticket D — WebAuthn / 2FA for operator login.** High priority.
  Precondition for live trading. WebAuthn (hardware key or platform
  authenticator) is preferred over TOTP and SMS.
- **Ticket E — Login attempt audit + lockout policy.** Durable
  `login_attempts` table, configurable lockout window, replaces the
  in-process rate limiting from Ticket A. Strongly recommended before live
  trading, but not a hard gate (see Operational preconditions).
- **Ticket F — Password change UI.** Ticket A ships only a CLI command for
  password change. A small Settings form is a separate, optional improvement.

Deliberately **not** opened:

- "Operator profile / account bootstrap UI" — the CLI bootstrap is
  sufficient for one operator.
- "Frontend global auth redirect handling" — this is part of Ticket A scope.
- "Broker secret rotation UX" — create-new + revoke-old in Ticket B *is*
  the v1 rotation flow.
