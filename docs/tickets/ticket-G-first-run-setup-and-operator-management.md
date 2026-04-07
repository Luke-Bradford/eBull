# Ticket G — First-run setup + operator management UI

**Depends on:** Ticket A (#98) — operators table, sessions, require_session
**Blocks:** Ticket B (#99) — broker credentials must land into the multi-operator-aware frontend
**ADR:** [ADR 0002](../adr/0002-local-browser-bootstrap-and-multi-operator.md)

---

## Goal

Replace the CLI-only bootstrap path with a browser-driven first-run setup
flow, and add an operator management UI so additional operators can be
created without ever touching the CLI. Decisions are locked in ADR 0002 —
this ticket is execution only.

---

## In scope

### Backend — setup mode

- New endpoint `GET /auth/setup-status`
  - returns `{ "needs_setup": bool }`
  - `needs_setup` is `true` iff the `operators` table is empty
  - **must not** leak whether a bootstrap token is configured or required
  - public, unauthenticated, no rate-limiting (it is a single boolean)
- New endpoint `POST /auth/setup`
  - body: `{ username, password, setup_token? }`
  - **only succeeds if** `operators` is empty *and* bootstrap authorization
    passes (see Mode A / Mode B below)
  - on success: creates the first operator row, immediately establishes a
    browser session for that operator (same cookie machinery as
    `/auth/login`), returns `LoginResponse`
  - on any failure: returns `404 Not Found` with body `{"detail":"Not Found"}`
    — identical to "endpoint does not exist" so the same response covers
    "already set up", "wrong token", "no token but token required", and
    "missing token under Mode B". This is the generic-401 discipline from
    ADR 0001 applied to setup.
- Race-safety: the empty-table check and the insert must be in a single
  transaction. Two simultaneous `POST /auth/setup` requests must result in
  exactly one operator row, and the loser must see the same `404` body.
  Implementation: `INSERT … SELECT WHERE NOT EXISTS (SELECT 1 FROM operators)`
  or equivalent, then check `cur.rowcount == 1` inside the same transaction.

### Backend — bootstrap authorization

- **Mode A (loopback-only zero-config):** accept setup with no token iff
  *all* of:
  - `request.client.host in {"127.0.0.1", "::1"}`
  - the server is bound to a loopback address (read from
    `settings.host` — see Config changes)
  - `settings.bootstrap_token` is unset
- **Mode B (token required):** if Mode A does not apply, the request body
  must contain `setup_token` and it must constant-time-compare equal to the
  active bootstrap token.
- **Active bootstrap token resolution:**
  - if `EBULL_SETUP_TOKEN` is set in env, that is the active token
  - else, if Mode B applies (i.e. server is non-loopback-bound and
    operators is empty at startup), the application **generates** a fresh
    token via `secrets.token_urlsafe(32)` once at startup and:
    - stores it in a process-global single-use slot
    - prints a banner to the application log:
      ```
      ============================================================
      EBULL BOOTSTRAP TOKEN (use once during /setup):
      <token>
      ============================================================
      ```
    - the token is **never** written to disk
  - the active token is consumed (zeroed in memory) on the first successful
    `POST /auth/setup`. After that, any subsequent setup attempt under
    Mode B fails with `404` until the process is restarted.

### Backend — operator management

All routes are `require_session` only. Never `service_token`.

- `GET /operators` — list. Each row: `{ id, username, created_at,
  last_login_at, is_self }`. `is_self` is true for the row matching the
  caller's session.
- `POST /operators` — create. Body `{ username, password }`. Username
  uniqueness enforced at DB level (existing constraint). Password validated
  against the same min-length rule as the CLI (12 chars). Does **not**
  log the new operator in — the calling operator stays signed in, the new
  operator must log in separately.
- `DELETE /operators/{id}` — delete. Rules:
  - target row must exist; otherwise 404
  - if the target is **not** the caller: succeed (subject to row exists)
  - if the target **is** the caller (self-delete):
    - if at least one other operator row exists: the operator row
      delete and the caller's session row delete happen in the **same
      transaction**; on commit, the response clears the session cookie.
      The caller is now logged out.
    - if the caller is the only operator: 409. The last operator can
      never delete itself.
  - 409 (not 404) is the response for the last-operator self-delete
    attempt — distinguishing "you can't do this" from "no such row" is
    fine here because both states are visible to the authenticated
    caller via `GET /operators` anyway, so there is no information leak.
  - The operator-row DELETE and the session-row DELETE are wrapped in a
    single `conn.transaction()` so a partial failure cannot leave a
    logged-in session pointing at a non-existent operator. The cookie
    clear happens on the response *after* the transaction commits.
- All write operations are wrapped in `conn.transaction()`.
- Username normalisation: same as `/auth/login` — strip + lowercase before
  insert and before lookup.

### Frontend — setup flow

- New `/setup` route, rendered by a new `SetupPage` component.
- On app boot, the existing `SessionProvider` bootstrap probe (`getMe`) is
  augmented:
  - if `getMe` returns 401, additionally call `getSetupStatus`
  - if `needs_setup`, set status to `"needs-setup"` and render the
    setup page regardless of the requested route
  - else, behaviour is unchanged (redirect to `/login`)
- `SetupPage` form fields: username, password, password confirm, setup
  token (only shown if `needs_setup` says nothing about token requirement
  — so the field is **always shown** but labelled as "Setup token (leave
  blank if running locally)"). On submit, post to `/auth/setup`. On
  success, the session cookie is set by the backend and the user is
  navigated to `/`.
- The setup page must not be reachable when `needs_setup` is false. Direct
  navigation to `/setup` redirects to `/login`.
- **Setup failure UX:** the backend returns a generic `404` for every
  failure mode (already set up, wrong token, missing token, token
  required but absent — see backend section). The frontend maps any
  non-2xx response from `POST /auth/setup` to a single fixed
  user-facing message:
  > **Setup unavailable or invalid token.**

  No reason-specific text, no parsing of the response body. The fixed
  string preserves the no-leak property of the backend contract while
  still giving the user a non-blank failure indication.

### Frontend — operator management UI

- New section in the existing settings page (or new `/settings/operators`
  route — pick whichever fits the existing settings layout): list of
  operators with username, created_at, last_login_at, and an `(you)`
  marker on the current row.
- "Add operator" form: username, password, password confirm. Submit posts
  to `POST /operators`, then re-fetches the list.
- "Delete" button per row, with a confirmation dialog. Hits
  `DELETE /operators/{id}`. Behaviour:
  - **Other operator:** on success, re-fetch the list. Caller stays
    signed in.
  - **Self when another operator exists:** confirmation dialog must
    explicitly say "This will sign you out." On success, the backend
    has already cleared the session cookie; the frontend drives the
    SessionProvider to `unauthenticated` and navigates to `/login`.
  - **Self when only operator:** the delete button is disabled with a
    tooltip ("You are the only operator — create another operator
    before deleting this one"). The 409 path is still tested as a
    backend guarantee, but the UI prevents reaching it.
- One-line note at the top of the section, exact text per ADR 0002:
  *"All operators on this instance share the same portfolio and
  recommendations. Add operators here only for shared-host use."*

### Config changes

- New `settings.bootstrap_token: str | None` — sourced from
  `EBULL_SETUP_TOKEN`. None means "not configured" (Mode A may apply, or
  the server will auto-generate one at startup if Mode B is needed).
- New `settings.host: str` — sourced from `EBULL_HOST`, default
  `"127.0.0.1"`. Used by the loopback check in Mode A and (separately) by
  the uvicorn launch command in `app/main.py` / the dev script. The
  uvicorn launcher must read this same setting; otherwise the bind address
  and the bootstrap-mode check disagree.
- `.env.example` documents both with a note about the loopback-vs-LAN
  decision.

### CLI re-framing

- `app/cli.py` is **not** modified in this ticket beyond a docstring
  update: the module docstring should say "Break-glass recovery CLI" and
  reference the README's Recovery section. The commands themselves
  (`create-operator`, `set-password`) are unchanged. They keep working,
  including `create-operator` against an empty table (which is now also
  the test-fixture path for setting up known operators in tests).

### README

- "Getting started" section becomes:
  > 1. `cp .env.example .env` and edit as needed
  > 2. `docker compose up`
  > 3. Open `http://localhost:8000` in your browser
  > 4. Follow the on-screen prompts to create your first operator account
- New "Recovery" section: `uv run python -m app.cli set-password <name>`
  for a forgotten password; `uv run python -m app.cli create-operator
  <name>` to recreate a wiped operator.
- New "Running on a non-loopback address" subsection: explains
  `EBULL_HOST=0.0.0.0` requires `EBULL_SETUP_TOKEN` (or the auto-generated
  banner token from the startup log).

### Tests

**Backend — setup status:**
- empty operators → `needs_setup: true`
- one operator → `needs_setup: false`
- many operators → `needs_setup: false`
- response shape is identical regardless of token configuration

**Backend — setup endpoint, Mode A (loopback, no token):**
- localhost client + loopback bind + empty table → success, returns
  `LoginResponse`, sets cookie, creates row
- localhost client + loopback bind + already has an operator → 404
- non-localhost client + loopback bind → 404 (request shouldn't reach
  this case in practice but defence in depth)
- localhost client + non-loopback bind → 404 (Mode A does not apply)

**Backend — setup endpoint, Mode B (token required):**
- non-loopback bind + empty table + correct token → success
- non-loopback bind + empty table + wrong token → 404
- non-loopback bind + empty table + missing token field → 404
- non-loopback bind + empty table + correct token after a previous
  successful setup (in same process) → 404 (token consumed)
- non-loopback bind + already has an operator + correct token → 404
- the auto-generated token, when no `EBULL_SETUP_TOKEN` is set, must be
  printed exactly once on first relevant startup, and must be 32+ bytes of
  url-safe random

**Backend — race safety:**
- two threads / two requests both calling `POST /auth/setup` simultaneously
  against an empty table → exactly one row created, exactly one success,
  one 404. Use real DB transactions in this test, not mocks.

**Backend — operator management:**
- list as authenticated operator returns own row with `is_self: true`
- create operator: success path inserts row, response excludes
  password_hash
- create operator: short password (< 12 chars) → 400
- create operator: duplicate username → 409
- create operator: not authenticated → 401
- delete operator: another operator → success, row gone, caller's
  session **unaffected** (subsequent `/auth/me` still works)
- delete operator: self when another operator exists → success, row
  gone, caller's session row also gone in the same transaction, cookie
  cleared on response, subsequent `/auth/me` returns 401
- delete operator: self when last → 409, row still present, session
  intact
- delete operator: nonexistent id → 404
- delete operator: not authenticated → 401
- after delete-all (forced via direct DB), `/auth/setup-status` returns
  `needs_setup: true` again

**Frontend — setup flow:**
- bootstrap probe with `needs_setup: true` lands on `SetupPage`
  regardless of requested URL
- successful setup transitions to authenticated state and renders the app
- direct navigation to `/setup` after setup is complete redirects to
  `/login`
- form-level validation: password mismatch, password too short
- any non-2xx response from `POST /auth/setup` renders the fixed string
  "Setup unavailable or invalid token." regardless of body / status

**Frontend — operator management:**
- list renders rows with `(you)` marker
- "delete" disabled with tooltip on the only operator
- after add, list re-fetches and includes the new operator
- after delete (of another operator), list re-fetches without the deleted row
- self-delete (when another operator exists) drives the session to
  `unauthenticated` and navigates to `/login`

---

## Out of scope (per ADR 0002)

- Per-operator portfolio / recommendations / decision_audit isolation.
- Operator roles or permissions.
- Cross-operator password reset (one operator resets another's password).
- Email-based password recovery, magic links, SMTP.
- Invitation links / pre-shared invite tokens.
- Operator self-deletion of the last operator.
- A web UI for the bootstrap token (env var or startup banner only).
- Behavioural changes to `app/cli.py` beyond docstring update.
- Any change to broker credentials (#99) — that ticket lands next, against
  the model produced by this ticket.
- Any change to existing domain tables (no `operator_id` columns added).

---

## Definition of done

- ADR 0002 merged as `Accepted` (separate PR or same PR — operator
  preference).
- All checks green (ruff, ruff format, pyright, pytest, frontend
  typecheck, frontend lint).
- README Getting Started flow walks a new user from `git clone` to a
  working browser session **without** any CLI invocation, on a
  loopback-bound default install.
- README Recovery section documents the CLI as the break-glass path.
- The setup endpoint is verifiably single-use within a process and
  race-safe across concurrent requests (test required).
- Manual smoke test: wipe `operators` table, restart backend, hit `/`
  in a clean browser → setup form appears → submit → land on dashboard
  signed in.
- Manual smoke test: with one operator existing, navigate to operator
  management, add a second operator, log out, log in as the second
  operator, delete the first, observe the "you can't delete the last
  operator" guard if you try to delete yourself.

---

## Follow-up tickets to file alongside this one

These are explicitly *not* in scope of Ticket G but should be opened as
issues so they don't get lost:

- **Ticket G-1 — `clear-lockout` CLI command.** Becomes meaningful once
  Ticket E (#102) lands. Pure recovery tool.
- **Ticket G-2 — Bootstrap token persistence across restart.** Today the
  auto-generated token is process-memory only; if the user restarts the
  backend mid-bootstrap, they get a new token. This is fine for v1 but
  worth a follow-up if it bites.
- **Ticket G-3 — Operator management audit log.** Add/delete operator
  events should land in `decision_audit` (or a new `operator_audit`
  table) eventually. Out of scope here to keep the diff small.
