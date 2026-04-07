# ADR 0002 — Local-only deployment, browser bootstrap, multi-operator-capable

**Status:** Accepted
**Date:** 2026-04-07
**Supersedes:** [ADR 0001](0001-operator-auth-and-broker-secrets.md) (in part — see "Relationship to ADR 0001")

---

## Context

ADR 0001 defined the operator auth model under the assumption that eBull is a
**single-operator** tool whose first operator is bootstrapped from the host
shell via `python -m app.cli create-operator`. Tickets A (#98) and B/C (#99,
#100) were sequenced on that basis.

Since A merged, the product direction has been refined:

1. **eBull is a personal, locally-run tool, not a hosted service.** A user
   clones the repo, runs `docker compose up`, and uses it from their own
   browser on their own machine. There is no public deployment, no SaaS, no
   tenant model.
2. **A user may want more than one local operator account.** Examples: a
   "paper" persona vs a "live" persona; a household where two people both
   trade their own portfolios on the same host. This is *multiple logins on
   one local instance*, not multi-tenancy. The data model stays
   single-instance.
3. **Requiring a CLI invocation to create the first account is bad onboarding
   for a personal tool that is meant to be cloned and used.** A user picking
   up the repo should not have to learn `uv run python -m app.cli`. The
   browser is the only UI we want to expose for normal lifecycle operations.
4. **Open LAN signup is not acceptable.** "First request to /setup wins"
   creates a footgun: anyone reachable on the local network during the
   bootstrap window can claim the operator slot. The right answer is browser
   onboarding *plus* a bootstrap authorization step, not removing the
   authorization altogether.

This ADR records the revised decisions and the boundaries that remain
unchanged from ADR 0001.

---

## Decision

### 1. eBull is a local, single-instance tool with multiple possible logins

The deployment model is fixed: eBull runs on the operator's own host, bound
locally, and is not exposed as a hosted service. The data model remains
single-instance — one `portfolio`, one `recommendations` stream, one
`decision_audit` log. What changes is that **multiple `operators` rows are a
supported, first-class case**, sharing that single instance.

Existing domain tables (`portfolio`, `recommendations`, `decision_audit`,
etc.) **do not** gain an `operator_id` column. There is no per-operator
portfolio, no per-operator recommendations stream, no per-operator data
isolation. Operators are *login identities on the same instance*, not
tenants. If two operators sign in, they see the same portfolio and the same
recommendations.

This is a deliberate scope cut. The use case for multiple operators is
"shared local instance" — e.g. household trust — not "logically separated
accounts." A future move to per-operator data is a much larger ADR and is
not in scope here.

### 2. First-run bootstrap is browser-driven, not CLI-driven

When the `operators` table is empty, the application enters **setup mode**:

- `GET /auth/setup-status` returns `{ needs_setup: true }`.
- The frontend, on its bootstrap probe, redirects all routes to `/setup`
  instead of `/login`.
- `POST /auth/setup` is the only mutating endpoint that accepts an
  unauthenticated request in this mode. It creates the first operator row
  and immediately establishes a browser session for that operator (same
  cookie machinery as `/auth/login`).
- After the first successful `POST /auth/setup`, the endpoint is permanently
  closed: subsequent requests return `404`, regardless of whether the table
  is later emptied. (See "Re-entry to setup mode" below.)

The CLI `create-operator` command is **kept**, but is reframed as a
break-glass tool — see Decision 5.

### 3. Bootstrap authorization: required when not localhost-only

The setup endpoint must not be a "first-on-the-LAN-wins" race. Authorization
is enforced at the request layer:

- **Mode A — localhost-only bootstrap (no token required).** If, and only
  if, *all* of the following are true, `POST /auth/setup` is accepted
  without a bootstrap token:
  - the request `client.host` is `127.0.0.1` or `::1`
  - the server is bound to a loopback address (`127.0.0.1` / `::1`), not
    `0.0.0.0`
  - no `EBULL_SETUP_TOKEN` is configured

  This is the zero-config path for "I'm the only person on this box."

- **Mode B — bootstrap token required (default for any LAN-reachable
  deployment).** If the server is bound to a non-loopback address *or* the
  user has set `EBULL_SETUP_TOKEN`, then `POST /auth/setup` requires the
  caller to present a matching one-time bootstrap token in the request body.
  Token validation:
  - constant-time compare against the configured value
  - rejected (404, identical body to "already set up") if no operators row
    exists *and* the token is wrong, missing, or unconfigured-but-required
  - the token is single-use: on successful setup it is invalidated in
    process memory so a leaked token cannot be reused even if the operators
    row is later deleted

  How the user obtains the token:
  - `EBULL_SETUP_TOKEN` env var, if they set one (most explicit)
  - otherwise, on startup, if the server is non-loopback-bound and no
    operators row exists, the server **generates** a fresh token and prints
    it to the application log/console exactly once, with a clear "BOOTSTRAP
    TOKEN" banner. The user copies it from the terminal that started the
    app. It is not written to disk.

The mode is decided at request time, not at startup, so a user who toggles
between local and LAN bind addresses gets the right behaviour without
restarting setup logic.

**Why not "trust localhost always":** because plenty of users will start
with `--host 0.0.0.0` to access the dashboard from their phone on the same
WiFi, and the setup window must not silently become open-to-LAN.

**Why not "always require a token":** because the most common case is "one
person on one laptop," and demanding a copy-paste from the terminal for that
case is friction without benefit.

### 4. Additional operators: only by an authenticated operator

After the first operator is created, the only path to additional operators
is **through the authenticated operator management UI**, run by an existing
logged-in operator. There is no public signup, no invitation email, no
"anyone on localhost can create accounts" backdoor.

The endpoints (all `require_session`):

- `GET /operators` — list (id, username, created_at, last_login_at, is_self)
- `POST /operators` — create (username, password) → new row, no auto-login
- `DELETE /operators/{id}` — delete. Self-deletion is allowed only when
  at least one *other* operator row exists; the last remaining operator
  can never delete itself. On a successful self-delete, the caller's
  current session row is destroyed in the same transaction as the
  operator row, the session cookie is cleared on the response, and the
  frontend navigates to `/login`. Deleting *another* operator does not
  affect the caller's session.

Password rotation for the *current* operator stays in scope of the existing
follow-up Ticket F (#103). Cross-operator password reset (one operator
resetting another's password) is **out of scope** — the recovery path for a
forgotten password is the CLI, by design.

### 5. CLI is repurposed as break-glass recovery

The `app.cli` module is not removed. Its role is reframed:

- **Was:** the documented way to create the first operator and rotate
  passwords.
- **Now:** a break-glass tool for when the browser path is unavailable —
  forgotten password, bricked lockout (Ticket E), corrupted last-operator
  row. It is documented in the README under a "Recovery" heading, not under
  "Getting started."

Commands kept: `set-password`, `create-operator` (still useful for scripted
test setup and as a "wipe-and-restart" path). Commands considered for
addition under follow-up tickets, not this one: `clear-lockout`,
`list-operators`.

The README's "Getting started" section will say only:
> Run `docker compose up`. Open the app in your browser. You will be guided
> through creating your first operator account. If you ever lock yourself
> out, see [Recovery](#recovery).

### 6. Re-entry to setup mode is explicit, not accidental

If an administrator manually deletes every row from `operators` (e.g. via
`psql`), the application **does not silently re-open setup mode** for the
first browser to connect. Instead:

- `/auth/setup-status` returns `needs_setup: true` again (the underlying
  state is genuinely "no operators exist").
- `POST /auth/setup` requires a fresh bootstrap token under Mode B even if
  the previous-instance token was already used in memory, because process
  restart clears the in-memory single-use marker.
- Under Mode A (loopback-bound, no token configured), accidental re-entry
  *is* possible but the trust model already says "loopback caller is
  trusted" in that mode. We accept this.

We document explicitly that "delete all operators" is a destructive recovery
action whose consequence is that setup mode re-opens. This is a tradeoff:
the alternative ("once setup has ever been done, the only way to re-open
setup is a separate CLI command") makes recovery harder for the
single-operator zero-config case, which is the primary use case.

### 7. Sequencing: this work lands before broker credentials

ADR 0001 sequenced Ticket B (broker credentials, #99) immediately after
Ticket A (auth, #98). This ADR moves a new ticket between them:

- ✅ **Ticket A (#98)** — operator browser auth & session — *merged*.
- 🆕 **Ticket G — first-run setup + operator management UI** — **next**,
  per this ADR.
- **Ticket B (#99)** — encrypted broker credentials — after Ticket G.
- **Ticket C (#100)** — eToro consumer migration — after Ticket B.
- Tickets D / E / F — unchanged.

The reason is concrete: broker credentials are keyed by `operator_id`. If we
ship #99 against the current "single hard-coded CLI-bootstrapped operator"
model and *then* introduce the multi-operator UI, we ship a UI for managing
broker secrets that has no concept of *which* operator's secrets are being
managed and silently has to be retrofitted. Doing the operator model first
means #99 lands into a frontend that already knows how to talk about
"current operator" correctly.

---

## Relationship to ADR 0001

ADR 0001 remains the source of truth for everything not contradicted here.
Specifically, **the following ADR 0001 decisions are unchanged and still
load-bearing**:

- Argon2id password hashing.
- Server-side opaque sessions in a `sessions` table (no JWT).
- Cookie attributes: `HttpOnly`, `Secure` (production), `SameSite=Lax`.
- Generic 401 discipline on all auth failures.
- `service_token` for non-browser callers, distinct from operator sessions.
- Broker secrets at-rest design: AES-256-GCM, env-keyed, AAD-bound,
  audited, write-only from the UI, key-versioned from day one.
- The `operators` table as the identity anchor for sessions and broker
  credentials.

**The following ADR 0001 decisions are explicitly superseded by this ADR:**

- *"Password change is performed via a CLI command."* — superseded.
  Password change is part of Ticket F (#103), through the browser. CLI
  remains as recovery.
- *"`operators` table exists even with one row ... This is **not** a step
  toward multi-user eBull."* — partially superseded. The `operators` table
  is now expected to hold *N ≥ 1* rows in normal use. Domain tables still
  do not gain `operator_id`.
- *"Operator profile / account bootstrap UI — deliberately **not** opened.
  The CLI bootstrap is sufficient for one operator."* — superseded. This
  ADR opens that ticket as Ticket G and makes browser bootstrap the
  primary path.
- The ADR 0001 *Operational preconditions* list does not change, except
  that "Deployment guide reflects post-Ticket-C reality" must additionally
  reflect the browser-bootstrap flow described here.

---

## Consequences

### Positive

- Zero-CLI onboarding: a user clones the repo, runs `docker compose up`,
  opens the browser, and is guided through account creation.
- The first-on-the-LAN race is closed by the bootstrap-token requirement,
  without sacrificing the loopback-only zero-config path.
- Multi-operator is a supported case for shared-local-instance use without
  pulling the project into multi-tenancy.
- Broker credentials (#99) land into a frontend that already knows how to
  reason about "current operator," avoiding a retrofit.
- The CLI remains available as a recovery tool, so a forgotten password
  does not require a database wipe.

### Negative / accepted costs

- One more ticket lands before broker credentials. We accept this because
  the alternative is a worse identity model under the broker-credential UI.
- The bootstrap-token mode adds complexity (a second branch in
  `/auth/setup`, a startup-time printer for the generated token). We accept
  this because open LAN signup is not acceptable.
- Re-entering setup mode by deleting every operator row is possible. We
  document it as a destructive recovery action; we do not technically
  forbid it, because doing so would make recovery harder than it needs to
  be for the primary use case.
- Multi-operator with a single shared portfolio is unusual and will surprise
  some users who assume multi-operator means multi-tenant. The Operator
  Management UI screen will carry a one-line note: *"All operators on this
  instance share the same portfolio and recommendations. Add operators
  here only for shared-host use."*

### Out of scope (explicitly)

- Per-operator portfolio rows.
- Per-operator recommendations.
- Per-operator decision audit.
- Operator roles / permissions / RBAC.
- Cross-operator password reset by another operator.
- Email-based password reset, magic links, SMTP integration.
- OAuth, SSO, federated identity.
- Invitation links / pre-shared invite tokens for new operators.
- Operator self-service deletion of the *last* operator.
- A web UI for the bootstrap token (it is read from env or printed to the
  startup log; there is no in-app generator screen for it).
- An "are you sure setup mode is closed?" admin probe — setup mode being
  closed is a function of `operators` row existence, not a separate flag.

---

## Security assumptions

These extend the ADR 0001 assumptions; the ADR 0001 list (same-origin
frontend/backend, CSRF deferred, "stolen DB dump" threat model for broker
secrets, etc.) still applies.

1. **Loopback bind is honest.** If the server reports it is listening on
   `127.0.0.1`, no LAN client can reach it. We rely on the OS for this.
2. **`request.client.host` is honest under loopback.** We are not behind a
   reverse proxy in the local-tool deployment model, so there is no
   `X-Forwarded-For` to spoof. If a user puts eBull behind a reverse proxy
   (out of scope), Mode A's loopback check becomes incorrect and Mode B
   (token-required) must be used; this is documented.
3. **`EBULL_SETUP_TOKEN`, when set, has the same handling discipline as
   `EBULL_SECRETS_KEY`:** never committed, never logged after the bootstrap
   banner, never returned from any API.
4. **The bootstrap token is single-use within a process lifetime.** Process
   restart clears the marker, which is acceptable because process restart
   is itself a host-trusted operation.
5. **No structural oracle on `/auth/setup-status`.** It returns the same
   shape regardless of bootstrap mode and does not leak whether a token
   would be required, only whether a setup is needed. We acknowledge a
   residual *timing* side-channel: an empty-table query plan is not
   identical to a populated-table plan, and on a sufficiently quiet
   loopback instance the latency difference could in principle reveal
   setup state. We accept this — the local trust model means a caller
   close enough to measure that timing is already inside the trust
   boundary, and adding caching would be more complexity than the leak
   warrants.
6. **Explicit UI tradeoff: setup token field is always visible.** The
   frontend always renders the bootstrap token input on the setup page
   (labelled "leave blank if running locally") rather than asking the
   backend whether the token is required. This trades a small implicit
   signal ("a token field exists, therefore the setup endpoint accepts
   one") for the stronger property that `/auth/setup-status` cannot be
   used to enumerate the bootstrap mode. We accept the tradeoff because
   the field's existence reveals nothing useful — it does not reveal
   whether the field is required, what a valid token looks like, or
   whether one is currently configured.

---

## Operational preconditions

Unchanged from ADR 0001 except:

- The "deployment guide reflects post-Ticket-C reality" item additionally
  requires documenting:
  - the loopback-vs-LAN bind decision and its effect on bootstrap mode
  - how to set `EBULL_SETUP_TOKEN`
  - how to read the auto-generated token from the startup log
  - how to use the CLI as recovery if locked out

Ticket G is **not** added to the live-trading hard-gate list. The hard
gates remain Tickets A, B, C, D plus the operational items from ADR 0001.

---

## Follow-up tickets

Opened or re-scoped as part of this ADR:

- **Ticket G — First-run setup + operator management UI.** New. Next in
  sequence after this ADR merges. Replaces the implicit "CLI is the only
  bootstrap" decision from ADR 0001.

Existing tickets, unchanged in scope but re-sequenced:

- **Ticket B (#99)** — moves to *after* Ticket G.
- **Ticket C (#100)** — moves to *after* Ticket B (no change to its
  relative position).
- **Ticket F (#103)** — unchanged. In-app password change for the current
  operator.

Existing tickets, unchanged in scope and unchanged in sequence:

- **Ticket D (#101)** — WebAuthn / 2FA.
- **Ticket E (#102)** — durable login attempt audit + lockout policy.

Deliberately **not** opened by this ADR:

- "Per-operator portfolio / recommendations isolation."
- "Operator role / permission system."
- "Email-based password recovery."
- "Bootstrap token web UI."
- "Operator invitation flow."
