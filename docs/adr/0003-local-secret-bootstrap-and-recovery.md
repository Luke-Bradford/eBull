# ADR 0003 — Local secret bootstrap and recovery

**Status:** Proposed
**Date:** 2026-04-08
**Relates to:** [ADR 0001](0001-operator-auth-and-broker-secrets.md), [ADR 0002](0002-local-browser-bootstrap-and-multi-operator.md)

---

## Context

ADR 0001 defined the encryption-at-rest model for broker credentials: AES-256-GCM
with per-row AAD binding, the working key loaded from `EBULL_SECRETS_KEY` at
startup. ADR 0002 committed eBull to a local-only, browser-first onboarding
posture: an operator clones the repo, runs `docker compose up`, and never
touches a CLI or env file for normal lifecycle operations.

Neither ADR addressed **how the master encryption key gets onto the machine in
the first place**. PR #110 (Ticket B / #99) shipped the encryption layer with a
hard fail-fast: if `EBULL_SECRETS_KEY` is unset or malformed, the backend
refuses to boot. That is the right behaviour for a misconfigured production
service, but it is incompatible with ADR 0002's onboarding posture: a
non-technical operator who clones the repo and starts the stack hits an opaque
startup crash and has no path forward without editing `.env`, generating a
base64 secret, and understanding what the key is for.

This is a real product gap, surfaced by direct user testing. We need a model
where:

1. The operator never edits `.env`, never generates a base64 secret, never sees
   the master key.
2. The operator's eToro API key (the user-facing secret) is still encrypted at
   rest under a key the database alone cannot reveal.
3. The operator can move to a new machine, restore from a database backup, or
   recover from a lost data directory without losing access to their stored
   broker credentials — and without depending on a cloud KMS, an OS keychain,
   or any service we don't control.
4. The backend never silently generates a new master key while encrypted
   credentials still exist in the database, because doing so would orphan every
   ciphertext row irrecoverably.
5. Advanced operators and CI environments can still inject a key explicitly via
   environment variable, without that being the normal path.

The previous CLI fallback path (`uv run python -m app.cli ...`) is not
available here: the master key is infrastructure, not user data, and there is
no operator account to authenticate against at the moment the key is needed.

---

## Decision

We introduce a **local secret bootstrap and recovery model** that owns the
lifecycle of the master encryption key on behalf of the operator.

### 1. Two layers, not one: root secret and broker-encryption key

The persisted artifact is a **32-byte root secret**, not the AES key directly.
The working **broker-encryption key** used by `app.security.secrets_crypto` is
derived from the root secret in memory at runtime via HKDF-SHA256:

```
broker_encryption_key = HKDF-SHA256(
    ikm  = root_secret,
    salt = b"",
    info = b"ebull-broker-encryption-key-v1",
    L    = 32,
)
```

The `info` string is the version pin and the domain separator. A future
feature that needs a *different* key derived from the same root secret uses a
different `info` string (e.g. `"ebull-backup-encryption-key-v1"`); the seam
exists today even though we have no second consumer.

The root secret is also encoded as a **24-word recovery phrase** using a
vendored 2048-word wordlist with an 8-bit SHA-256 checksum (BIP39-compatible
encoding, but not exposed to the operator under that name — see §5). The
phrase and the file are two encodings of the same 32 bytes; either can
reconstruct the broker-encryption key on any machine, on any OS, deterministically.

**Why this layering matters:** persisting the derived key directly would make
the recovery phrase a write-once token the backend could no longer reconstruct
or verify against. By persisting the root secret and deriving the working key
on demand, the phrase and the file remain interchangeable forever. HKDF runs
forward only, in memory, on every boot.

**Why HKDF and not Argon2id:** Argon2id is for stretching low-entropy human
passwords. The recovery phrase is a faithful encoding of 256 random bits — it
already has full key strength. HKDF is the correct primitive for "I have
keying material, give me a usable key with domain separation." Argon2id stays
where it belongs: operator password hashing in `app.security.passwords`.

### 2. Three formal boot states

The backend computes its boot state during FastAPI lifespan from three
independent probes — operator count, master key file presence, and "any rows
exist in `broker_credentials`" — and exposes the result on `app.state`.

| State | Lifespan action | Effect on broker-secret routes |
|---|---|---|
| `clean_install` | No root secret generated yet. In-memory broker-encryption key is unset. | Blocked by `require_master_key` until lazy generation runs (see §3). |
| `normal` | Root secret read from disk (or env override consumed); broker-encryption key derived into in-memory cache. | Available. |
| `recovery_required` | No generation. No raise. In-memory key stays unset. | Blocked by `require_master_key` until `POST /auth/recover` succeeds. |

The bootstrap module is the only code path that writes the master key file,
and it refuses to write under any condition where existing encrypted
credentials would be orphaned. This is the structural enforcement of the "do
not silently regenerate" invariant.

`recovery_required` is a first-class boot state, not an exception. The backend
finishes startup, mounts all routers, and serves traffic — operator login
works because Argon2id password hashing has no dependency on the
broker-encryption key. The only routes that fail are those that need the key,
and they fail with a structured `503 {"detail": "recovery_required"}` via the
`require_master_key` FastAPI dependency.

### 3. Lazy generation, tied to the moment of relevance

The root secret is generated at the moment it first becomes meaningful — never
silently at lifespan startup. There are exactly two generation moments:

1. **First-run setup (clean install path):** when the operator reaches the
   broker-credentials step of the first-run setup wizard, the backend
   generates the root secret, persists it, derives the broker-encryption key,
   and the wizard displays the recovery phrase. The operator must re-type 3
   randomly chosen word positions to confirm before setup can complete.

2. **Edge case C (existing operator, fresh data directory, no credentials
   yet):** when an operator who already has an account navigates to the
   broker-credentials page on a system with no master key file and no
   encrypted credentials, the backend generates the root secret on demand, the
   page displays the recovery phrase via a one-time interstitial, and the
   operator must confirm 3 word positions before any credential can be stored.

In both cases, the phrase is **bound to the moment the operator can write it
down**. There is no silent background generation, no post-login banner, no
"show me my phrase" page anywhere else in the app.

### 4. Recovery flow with backend invariant

When `POST /auth/recover` receives a phrase:

1. Validate wordlist checksum. On failure → `400 invalid_phrase`. Nothing
   persisted, nothing cached.
2. Decode to the 32-byte root secret. Derive the broker-encryption key in
   memory via HKDF.
3. **Wrong-phrase verification (conditional):**
   - If at least one row exists in `broker_credentials WHERE revoked_at IS
     NULL`: select the **most recent active non-revoked credential**, defined
     deterministically as `ORDER BY created_at DESC, id DESC LIMIT 1` (the
     `id` tiebreaker keeps the contract deterministic when two rows share a
     `created_at` to microsecond precision). Attempt
     `secrets_crypto.decrypt(...)` against that row with the derived key and
     the row's AAD inputs. On `CredentialDecryptError` → `400
     phrase_does_not_match_database`. Nothing persisted, nothing cached, boot
     state unchanged.
   - If no active non-revoked rows exist: skip verification entirely. The
     phrase is accepted on checksum validity alone. (See "trade-offs" below.)
4. Persist the root secret to `<data-dir>/secrets/master.key` atomically.
   The temporary file MUST be created in the **same destination directory**
   (`<data-dir>/secrets/`) as the final path, so that the subsequent
   `os.replace` is a same-filesystem rename and therefore atomic. Creating
   the temp file in the system temp directory is not acceptable: on
   Docker/volume deployments the system temp dir is frequently on a
   different filesystem from the mounted data volume, which would degrade
   the rename to a non-atomic copy. The file is written with mode `0600`.
5. Populate the in-memory broker-encryption key on `app.state`.
6. Flip `app.state.boot_state = normal`, `app.state.recovery_required = false`.
7. Return 204.

The "verify before persist" sequence is enforced by the call graph itself —
each step is a function call that either raises (don't proceed) or returns
cleanly (proceed). There is no flag-passing path that could leave a wrong key
persisted.

**Why active non-revoked rows specifically:** revoked rows are kept forever
for audit history. Today they are encrypted under the same key as active rows,
so verifying against either would work. Restricting verification to active
rows is the future-proof choice: a future re-keying flow that re-encrypts only
active rows under a new derivation would otherwise cause the verification rule
to falsely reject a legitimate recovery on a re-keyed installation.

**Why we skip verification when no active rows exist:** an operator who
revoked all their credentials and then lost their data directory would
otherwise be unable to recover. The trade-off is real: in this case the
backend cannot distinguish "the right phrase for this installation" from "any
phrase with a valid checksum." The blast radius is bounded — the operator
will not be able to decrypt their revoked rows (which they could not access
anyway), and any new credentials they add will be encrypted under whatever key
they accepted. They will notice immediately if they were trying to recover
specifically to access a backup of revoked-row history. We document this
limitation rather than refuse recovery in this case.

### 5. Edge case map

| Operators | Key file | Encrypted creds | `needs_setup` | `recovery_required` | Routing | Notes |
|---|---|---|---|---|---|---|
| 0 | absent | no | true | false | setup | Canonical first-run. Lazy generation in wizard. |
| 0 | absent | yes | true | **true** | **recovery → setup** | **Hardened.** Encrypted ciphertext exists with no key file → recovery required regardless of operator count. After recovery, setup runs to create the operator account. |
| 0 | present | no | true | false | setup | Setup runs against existing key file. **No phrase displayed** — the existing key is presumed already managed by whoever placed it. |
| 0 | present | yes | true | false | setup | Setup runs. Pre-existing creds belong to the previous (wiped) operator scope and remain orphaned-but-harmless under PR #110's per-operator scoping. Documented as a known acceptable state. |
| >0 | absent | no | false | false | normal | **Edge case C.** App runs normally. Root secret generated lazily on first visit to broker-credentials page; phrase shown there. |
| >0 | absent | yes | false | true | recovery | The headline recovery case. New machine, restored DB, no `secrets/`. |
| >0 | present | no | false | false | normal | Normal boot, no credentials yet. |
| >0 | present | yes | false | false | normal | Normal boot, fully populated. The 99% steady state. |

The two flags are computed independently from independent signals; they are
never collapsed into one mixed expression:

```
needs_setup       = (operators_count == 0)
recovery_required = (root secret file does not exist)
                AND (any row exists in broker_credentials)
```

### 6. Frontend routing precedence

The frontend reads `GET /auth/bootstrap-state` at app load (and after
successful recovery, and after successful first-run setup completion — not
after every login) and applies a fixed precedence:

```
1. if recovery_required → /recover
2. else if needs_setup  → /setup
3. else                  → normal login / app
```

This handles the mixed-flag case (Edge case A) cleanly: recovery runs first
to restore the root secret, then the frontend re-fetches bootstrap-state and
sees `needs_setup=true, recovery_required=false`, then the setup wizard runs.
The operator experiences "recover, then create your account" as one
continuous flow. The reverse precedence would let setup run against an
unrecoverable database.

### 7. Public bootstrap surface (under `/auth/`, not `/system/`)

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /auth/bootstrap-state` | public | Returns `{ needs_setup: bool, recovery_required: bool }`. Frontend boot routing only. Two booleans, no admin/system surface leak. |
| `POST /auth/recover` | public | Accepts the 24-word recovery phrase. Runs the verification rule from §4. Returns 204 on success, 400 on checksum failure or wrong-phrase-right-format, 409 if called outside `recovery_required` state. |

The recovery endpoint deliberately lives under `/auth/` rather than
`/system/`. Both endpoints are part of the public boot/auth surface — the
frontend hits them before any login attempt. Introducing a `/system/*`
namespace just for one endpoint would create a magnet for unrelated
operational state to accumulate over time, which we explicitly want to avoid.

The lazy generation paths (first-run wizard step, Edge case C interstitial)
expose the recovery phrase through additional gated endpoints whose
implementation shape is left to Ticket 1; the architecturally significant
property is that the phrase is **only ever displayed at one of the two
generation moments**, and the gate prevents replay after confirmation.

### 8. App-data directory and configuration

The master key file lives at `<data-dir>/secrets/master.key`, with the data
directory resolved in this order (highest priority wins):

1. `EBULL_DATA_DIR` environment variable, if set. Used by Docker.
2. `settings.data_dir` from `app/config.py`, if set in a config file.
3. Per-OS default via `platformdirs` (host installs only):
   `%APPDATA%/eBull/` on Windows, `~/Library/Application Support/eBull/` on
   macOS, `~/.local/share/ebull/` on Linux.

On Unix the file is created mode `0600` and the `secrets/` directory `0700`.
On Windows we rely on profile-directory ACL inheritance, which is correct in
practice; we do not attempt to enforce ACLs programmatically.

`platformdirs` is a small, pure-Python dependency with no native build step.
It is only consulted as the host fallback; Docker deployments always set
`EBULL_DATA_DIR` explicitly and never reach the platformdirs path.

### 9. Env override semantics: `EBULL_SECRETS_KEY`

`EBULL_SECRETS_KEY`, when set, is consumed **as-is** as the working
broker-encryption key. It bypasses both the file-based bootstrap and the HKDF
derivation entirely. In this mode:

- The boot state is forced to `normal`.
- The system never enters `recovery_required`.
- No recovery phrase exists.
- The master key file is neither read nor written.

This preserves backwards compatibility with PR #110, with CI environments,
and with developers who explicitly want to manage their own key material
(e.g. piping it from a password manager). It is documented in `.env.example`
as: *"Advanced override. You don't normally need this. The app generates and
manages its own master key automatically."*

If both `EBULL_SECRETS_KEY` and a non-empty `secrets/master.key` exist and
disagree, the env var wins (it is the explicit override) and a loud warning
is logged.

**Env override mismatch behaviour.** If `EBULL_SECRETS_KEY` is set and
encrypted credentials exist in the database, the bootstrap module MUST
verify the override key by attempting to decrypt the most recent active
non-revoked credential (same `ORDER BY created_at DESC, id DESC LIMIT 1`
contract as §4) before completing startup. On verification failure, the
backend MUST fail loud at startup with a clear configuration error
identifying the mismatch — it does **not** silently continue, and it does
**not** fall through into `recovery_required`. The recovery flow is only
for the file-based bootstrap path; an operator who has explicitly opted
into env-override mode is responsible for supplying a key that matches
their database, and a mismatch is a configuration bug, not a recovery
scenario. If no active non-revoked rows exist, verification is skipped and
the override is accepted on its own (same trade-off as §4).

### 10. Required refactor of `secrets_crypto.py`

PR #110 introduced a module-level `_aesgcm` cache populated by `load_key()`
at startup, with a regression test that protected the invariant "the key
validated at startup is the key used at runtime." Under the new model, that
invariant is satisfied more directly by storing the broker-encryption key on
`app.state` once at lifespan and reading it per-request — there is no
module-level state to drift.

Ticket 1 will refactor `secrets_crypto.py` to read the broker-encryption key
from `app.state` (passed in by callers) and remove the module-level cache.
The PR #110 regression test is retired; the new architecture makes its
invariant structurally stronger by eliminating the global mutable state
entirely. This also closes the structural side of #112 (commit lifecycle
ownership for service functions): per-request key access removes one of the
reasons services held connection state.

---

## Rejected alternatives

- **Argon2id-derived master key from the recovery phrase.** Rejected: the
  phrase is high-entropy keying material, not a user password. Argon2id is
  the wrong primitive — it would waste CPU and tie us to KDF parameters that
  complicate future verification on new machines.
- **OS keychain (Windows Credential Manager / macOS Keychain / libsecret).**
  Rejected: per-OS-user, doesn't survive reinstall, can't move between
  Windows ↔ macOS ↔ Linux, and inaccessible to a Docker container without
  bind-mounting host secrets in. The portability and Docker stories are
  dealbreakers for a self-hosted app.
- **Master key derived from operator password.** Rejected: forgotten
  password = permanently lost broker keys, no recovery possible. For a
  long-horizon investment tool the irreversible-loss failure mode is
  unacceptable.
- **Hard-fail at startup when the key file is missing.** Rejected: if the
  backend crashes, the operator cannot reach the recovery flow. Recovery has
  to be reachable from a running backend, which means a formal degraded
  boot state.
- **Persisting the HKDF-derived key directly.** Rejected: the recovery
  phrase would become a write-once token that the backend could no longer
  reconstruct or verify against the persisted material. By persisting the
  root secret instead, the phrase and the file remain interchangeable.
- **`/system/status` for boot routing.** Rejected: creates an admin/system
  namespace that would accumulate unrelated operational state. The frontend
  needs exactly two booleans at app load; `/auth/bootstrap-state` is the
  minimal surface that answers that question.
- **Eager root-secret generation at lifespan.** Rejected: silently creating
  a key the operator does not yet know about decouples the phrase from the
  moment the operator can write it down. Lazy generation, tied to the
  moment of relevance, keeps the phrase bound to a clear UX checkpoint.
- **Collapsing `needs_setup` and `recovery_required` into one expression.**
  Rejected: they answer different questions and need to be reasoned about
  independently in tests, in the boot state computation, and in the
  frontend. Mixed states (Edge case A) are handled by routing precedence,
  not by signal collapsing.
- **Cloud KMS / HSM.** Rejected for v1: introduces a runtime dependency on
  a service we don't control, contradicts the local-only posture of ADR
  0002, and adds operational complexity that no current threat model
  requires.
- **Key rotation as part of this milestone.** Deferred: not in scope. The
  HKDF `info` string `"ebull-broker-encryption-key-v1"` is the version seam
  for a future rotation ticket. This ADR does not commit to a rotation
  mechanism.
- **A "show me my phrase" page.** Rejected: would defeat the bound-to-the-
  moment-of-generation property. The phrase is shown exactly twice in the
  product's lifetime — once at first-run setup, or once at Edge case C
  interstitial — and never again. Recovery is the only path back if it is
  lost.

---

## Trade-offs and known limitations

- **Total loss is unrecoverable.** If the operator loses both the data
  directory *and* the recovery phrase, their encrypted broker credentials
  are gone. This is the price of "no cloud KMS dependency." Documented
  prominently in Ticket 4's backup README.
- **Wrong-phrase-with-no-active-rows accepts any valid phrase.** See §4
  trade-off discussion. Bounded blast radius; documented limitation.
- **Edge case B orphans pre-wipe credentials.** When operators are wiped
  but the data-dir survives, existing encrypted rows belong to a now-
  nonexistent operator UUID and remain in the database, decryptable in
  principle but invisible under PR #110's per-operator scoping. A future
  cleanup ticket can address this; for v1 the orphan rows are harmless.
- **Edge case C requires a one-time interstitial confirmation step** the
  first time an existing operator visits broker-credentials on a fresh
  data-dir. The flow is deliberately blocking: credentials cannot be stored
  until the phrase is confirmed.
- **Windows file permissions are best-effort.** Profile-directory ACL
  inheritance is correct in practice but we do not enforce ACLs
  programmatically. A multi-user Windows machine where another local user
  has admin rights is outside the threat model.

---

## Relationship to ADR 0001 and ADR 0002

This ADR **does not supersede** either prior ADR. It fills a gap both of
them assumed someone else would fill:

- **ADR 0001** defines the encryption-at-rest contract (AES-256-GCM,
  per-row AAD, key versioning). That contract is preserved exactly. The
  broker-encryption key produced by HKDF in this ADR is fed into the same
  AEAD primitive ADR 0001 specifies. No changes to the AAD format, no
  changes to the on-disk ciphertext layout.
- **ADR 0002** commits to local-only browser bootstrap and "operator never
  touches a CLI for normal lifecycle operations." This ADR is the missing
  piece that makes that commitment hold for the master key as well as the
  operator account.

The three ADRs together define the full local-secret model: ADR 0001 says
how secrets are encrypted, ADR 0002 says how operators get into the
system, ADR 0003 says how the encryption key gets onto the machine and how
the operator recovers it.

---

## Consequences

- A new milestone (**M-secrets-bootstrap**) gates Ticket C (#100, the first
  real consumer of the encrypted broker credentials). #100 cannot merge
  until all four tickets in this milestone are merged. This is non-
  negotiable: shipping a trade-execution path on top of an incomplete
  recovery story is the kind of load-bearing tech debt we explicitly want
  to avoid.
- PR #110's `secrets_crypto.py` module-level cache is removed and its
  regression test retired. The replacement architecture makes the same
  invariant stronger.
- A new dependency (`platformdirs`) is added. Pure Python, small, well
  maintained, no native build step.
- A 2048-word wordlist is vendored into the repo (~13 KB) along with a
  ~50-line encode/decode/checksum module. No external "BIP39" or
  "mnemonic" dependency, and no such naming exposed in user-facing copy.
- The product gains a new operator-visible concept: **the recovery phrase**.
  All user-facing copy uses that exact term. "Seed phrase," "mnemonic,"
  and "wallet" are never used in UI, docs, or operator-facing logs.
- A new public API surface (`GET /auth/bootstrap-state`, `POST
  /auth/recover`) becomes part of the supported contract. Two booleans
  and one phrase-submission endpoint — deliberately minimal so the surface
  doesn't accumulate unrelated boot/admin state over time.
- Backups gain a clearer story: either back up the data directory and the
  database together (no phrase needed for restore), or back up the
  database alone and rely on the phrase. Documented in Ticket 4.

---

## Status

Proposed. Awaiting review and acceptance before Tickets 1–4 are filed.
