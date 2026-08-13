# Spec — drop recovery phrase ceremony + wizard broker step (#971 + #972)

**Date:** 2026-05-07
**Status:** v4, post-Codex round 3
**Closes:** #971, #972, #969 (superseded)
**Amends:** [ADR-0003](../../adr/0003-local-secret-bootstrap-and-recovery.md)

---

## Operator-given direction (2026-05-06 audit)

- eToro is the source of truth for credentials. Operators can always re-issue keys from the eToro dashboard.
- We protect keys at rest (AES-GCM, on-disk root secret) but do **not** offer phrase-based recovery, OS-keychain integration, email recovery, or any third-party ceremony.
- "I lost my keys / wiped my data dir / forgot my password" → re-enter eToro keys via Settings; create a fresh operator if the local password is also lost. Expected, fine.
- Encryption stays. Phrase ceremony goes.
- First-run wizard becomes a single-step operator-create form. Broker credentials live in Settings, where multi-cred / repair / test flows already live.

## State-machine semantics

Three independent dimensions, never collapsed:

| Dimension | Values | Source |
|---|---|---|
| `boot_state` (key state) | `clean_install`, `normal` | `master_key.compute_boot_state` |
| `needs_setup` (operator state) | `true`, `false` | `operators_empty(conn)` — derived from operators table only, NOT from key state |
| `broker_key_loaded` (in-memory key cache) | `true`, `false` | `app.state.broker_key_loaded` |

`recovery_required` is **deleted** entirely. `BootstrapStateResponse` drops the field. Frontend `/recover` route is removed.

**Decoupling rule (vs prior `key_needs_setup OR operators_empty`):** `needs_setup` is operators-only. The wizard exists to create the first operator account. "No active credentials" no longer routes to `/setup` — it surfaces a "add eToro creds" banner inside the normal app shell. This eliminates the awkward state where an existing operator gets bounced back to the setup wizard after a stale-cipher revoke.

## Scope

### Delete entirely

- `app/security/recovery_phrase.py` + `app/security/wordlist_english.txt`
- `tests/test_security_recovery_phrase.py`
- `frontend/src/components/security/RecoveryPhraseConfirm.{tsx,test.tsx}`
- `frontend/src/components/security/RecoveryPhraseModal.tsx`
- `frontend/src/lib/recoveryPhrase.{ts,test.ts}` + `frontend/src/lib/bip39-wordlist.txt`
- `frontend/src/pages/RecoverPage.{tsx,test.tsx}`
- `POST /auth/recover` endpoint (incl. `RecoverRequest` / `RecoverResponse` models, `recover_from_phrase`, `RecoveryVerificationError`, `RecoveryNotApplicableError`)
- All references to `recovery_required` boot state on backend + frontend
- Wizard broker-credential step (step 2 of `SetupPage.tsx`) and any logic gating it (`useSetupWizard.ts` broker step state, skip-broker path)
- `recovery_phrase` field on `CreateCredentialResponse` (backend) + `CreateBrokerCredentialResponse` (frontend type)

### Modify

**Backend:**
- `app/security/master_key.py`:
  - Remove `RecoveryVerificationError`, `RecoveryNotApplicableError`, `recover_from_phrase`
  - Drop `encode_phrase`/`decode_phrase` imports + `phrase` element from `generate_root_secret_in_memory()` return tuple
  - `BootState` literal collapses to `"clean_install" | "normal"` (drop `"recovery_required"`)
  - `compute_boot_state()` simplifies (no more recovery_required branch)
  - Add `_revoke_stale_ciphertext(conn, derived_key: bytes | None)` — soft-revoke via `UPDATE broker_credentials SET revoked_at = NOW() WHERE id = ANY(stale_ids)`. Branches:
    - `derived_key is None` (root secret file missing AND no env override): every `revoked_at IS NULL` row whose operator still exists is stale (no key to test against). Soft-revoke all.
    - `derived_key is bytes` (file present OR `EBULL_SECRETS_KEY` env override): row is stale if `_key_decrypts_row(row, derived_key)` returns False. Soft-revoke per-row. The env-override path passes the env-derived bytes here and never enters the no-key branch.
    - Plus: any `revoked_at IS NULL` row whose `operator_id` is no longer in `operators` (orphan ciphertext from a wiped operator) is stale regardless of key match. Soft-revoke.
    - Logs `WARNING` with row count + reason class. Audit trail preserved (revoked rows stay; access log FK preserved).
    - Idempotent under concurrent boots (jobs + API): `WHERE revoked_at IS NULL` filter means a second pass finds nothing to update.
    - After revoking ≥1 row, issues **one `NOTIFY ebull_credential_health` per affected `operator_id`** (the existing listener channel expects `{"operator_id": <uuid>, ...}` payloads — a single bulk NOTIFY would be ignored or mis-handled). Implementation: `SELECT DISTINCT operator_id FROM broker_credentials WHERE id = ANY(:stale_ids)` post-UPDATE, then loop `pg_notify('ebull_credential_health', json_build_object('operator_id', op_id, 'reason', 'stale_cipher_revoke')::text)`.
  - `bootstrap()` flow: stale-revoke runs **unconditionally** (cheap UPDATE, no-op when nothing matches) before the existence check. The `_credentials_exist()` query keeps its `JOIN operators` shape; orphan rows are out of scope for that gate (they cannot belong to anyone, so they cannot affect the boot state machine).
  - `BootResult` drops `recovery_required` field. `needs_setup` is removed from `BootResult` entirely — the field is now derived strictly from `operators_empty(conn)` at request time, not at lifespan time. (Lifespan does not have a stable view of operator-state across the lifetime of the app, so caching it on `BootResult` is misleading.)
- `app/api/auth_bootstrap.py`:
  - Remove `recover` route, `RecoverRequest`, `RecoverResponse`, `recovery_required` field on `BootstrapStateResponse`
  - `require_master_key` keeps the `broker_key_loaded` check, drops the `recovery_required` branch
  - `BootstrapStateResponse.needs_setup` becomes `operators_empty(conn)` only — drops the `key_needs_setup` term entirely. Operator-state is the sole driver.
- `app/api/broker_credentials.py`:
  - Drop `recovery_phrase` field from `CreateCredentialResponse`
  - Drop both `request.app.state.recovery_required = False` writes (no such state exists)
  - Drop both `getattr(request.app.state, "recovery_required", False)` checks
  - First-save lazy-gen no longer surfaces phrase to client; removes the `phrase` local in `_to_out` callers
- `app/main.py` — drop `app.state.recovery_required = boot.recovery_required` line; drop `app.state.needs_setup = boot.needs_setup` line (operator-state is now derived from `operators_empty(conn)` per request, not cached at lifespan); drop both fields from the bootstrap log line
- `app/jobs/__main__.py` — same: drop `recovery_required` + `needs_setup` from log line + any state writes
- `app/api/broker_credentials.py` — drop `request.app.state.needs_setup = False` writes (currently fired after first cred save). Operator-state is independent of credential-state under v4.
- `app/services/sync_orchestrator/layer_types.py` — operator-facing copy that points to `/recover` updated to "open Settings to re-enter eToro credentials" (no in-app `/recover` route exists)
- `app/security/secrets_crypto.py` — drop any phrase imports (none found in current grep but confirm)

**Frontend:**
- `frontend/src/pages/SetupPage.tsx` — collapse multi-step wizard to single operator-create form; drop broker step + skip-broker path
- `frontend/src/pages/SetupPage.test.tsx` — drop broker-step assertions; assert single-step rendering + post-create routing
- `frontend/src/pages/useSetupWizard.{ts,test.ts}` — drop broker step state machine, skip-broker dispatcher
- `frontend/src/pages/SettingsPage.tsx` — drop `useRecoveryPhraseModal` import + usage (`phraseModal`, `phrase = response.recovery_phrase` post-save flow)
- `frontend/src/pages/SettingsPage.test.tsx` — drop tests asserting on phrase modal
- `frontend/src/pages/LoginPage.tsx` — drop `recovery_required` routing branch
- `frontend/src/components/RequireAuth.tsx` — drop `recovery_required` redirect
- `frontend/src/lib/session.tsx` — drop `recovery_required` from session shape; drop `postRecover` consumer
- `frontend/src/api/auth.ts` — drop `postRecover`; drop `recovery_required` from `BootstrapStateResponse` type
- `frontend/src/api/brokerCredentials.ts` — drop `recovery_phrase` field from `CreateBrokerCredentialResponse`
- `frontend/src/App.tsx` — drop `<Route path="/recover">`
- `frontend/scripts/check-dark-classes.mjs` — drop `RecoverPage.tsx` + `RecoveryPhraseConfirm.tsx` from `CHECK_F_SKIP_FILES` (files no longer exist)

### Add

- `_revoke_stale_ciphertext(conn, derived_key)` helper in `master_key.py` (predicate above)
- ADR-0003 **amendment block** appended at top: status flips to `Amended`. New `## Amendment 2026-05-07` section appended at the bottom explaining: phrase-based recovery removed in favour of operator-driven re-entry; rationale (eToro is source of truth, demo-first risk posture); behavioural changes (boot state machine simplification, stale-cipher soft-revoke at boot). Prior body preserved verbatim for history.
- `docs/settled-decisions.md` § "Operator auth and broker-secret storage": add ADR-0003 reference + one-line summary of new posture.

## Edge case map post-amendment

| Operators | Key file | Creds | After bootstrap | `boot_state` | `needs_setup` | Frontend route |
|---|---|---|---|---|---|---|
| 0 | absent | no | unconditional stale-revoke pass: no rows to touch | `clean_install` | true | `/setup` (single-step) |
| 0 | absent | yes | stale-revoke (no key + orphan operator) → all rows soft-revoked | `clean_install` | true | `/setup` |
| 0 | present | no | unchanged | `clean_install` | true | `/setup` |
| 0 | present | yes | stale-revoke (orphan operator branch) → all rows soft-revoked even if key would decrypt | `clean_install` | true | `/setup` |
| >0 | absent | no | unconditional stale-revoke pass: no rows | `clean_install` | false | normal app; "add eToro creds" banner |
| >0 | absent | yes | stale-revoke (no key) → all active rows for this operator soft-revoked; lazy-gen on next save | `clean_install` | false | normal app; "encryption key was lost — re-add eToro creds in Settings" banner |
| >0 | present | no | unchanged | `clean_install` | false | normal app |
| >0 | present | yes (all match) | stale-revoke pass: zero rows match the predicate | `normal` | false | steady state |
| >0 | present | yes (some mismatch) | stale-revoke (key-mismatch per-row) → only mismatched rows revoked; matching rows survive | `normal` | false | normal app + per-operator health drops to MISSING/REJECTED if their pair is now incomplete; banner: "one or more eToro keys could not be decrypted — re-add the missing key in Settings" |

Predicate for `_revoke_stale_ciphertext` (one UNION-ed UPDATE per boot):

1. Rows whose `operator_id NOT IN (SELECT operator_id FROM operators)` (orphans).
2. Rows where the derived key is `None` (file missing).
3. Rows where `_key_decrypts_row(row, derived_key)` returns False (mismatch).

Action: `UPDATE broker_credentials SET revoked_at = NOW() WHERE id = ANY(:stale_ids)`. Logs `WARNING` once per boot with class breakdown (orphan count, no-key count, mismatch count) so an operator reading the journal sees what was discarded and why. Issues a single `NOTIFY ebull_credential_health` after the UPDATE so any cached health state across processes refreshes.

## Migration story

- **Existing dev installs with `secrets/master.key` + creds + matching** → no-op. Boot state = `normal`. Phrase code paths gone but the file is still the source of truth.
- **Existing dev installs that already saw the phrase ceremony** → no migration needed. Phrase was never persisted server-side beyond the file (which stays).
- **Existing dev installs with `secrets/master.key` mismatched / missing + creds** → stale-cipher soft-revoke fires on first boot. Loud `WARNING` log. Frontend shows "re-add creds" banner.
- **No DB migration / no schema change.** `revoked_at` already exists; we only add UPDATE callsites.

## Sequence (one PR, ordered commits for review)

Ordering rule per Codex finding: **callers must be updated before the deleted module disappears, in the same commit.** Otherwise intermediate `git bisect` / partial cherry-pick / per-file CI runs fail on dangling imports.

1. **Commit 1 — docs.** Append ADR-0003 amendment + update `settled-decisions.md`. No code change.
2. **Commit 2 — backend caller cleanup + new behavior + tests.** In one diff: update `app/main.py`, `app/jobs/__main__.py`, `app/services/sync_orchestrator/layer_types.py`, `app/api/broker_credentials.py`, `app/api/auth_bootstrap.py` (drop recover route + recovery_required field), `app/security/master_key.py` (drop phrase imports + recover_from_phrase function + RecoveryVerificationError/RecoveryNotApplicableError + recovery_required from BootResult + needs_setup from BootResult; add `_revoke_stale_ciphertext`; simplify `compute_boot_state`/`bootstrap`; emit NOTIFY after revoke). Add tests for stale-revoke (orphan / no-key / mismatch branches) + bootstrap end-to-end + smoke-boot under each edge case row. Then delete `app/security/recovery_phrase.py` + `wordlist_english.txt` + `tests/test_security_recovery_phrase.py`. All in one commit so backend tests pass at every intermediate step.
3. **Commit 3 — frontend caller cleanup.** In one diff: update `App.tsx`, `RequireAuth.tsx`, `session.tsx`, `LoginPage.tsx`, `api/auth.ts`, `api/brokerCredentials.ts`, `SetupPage.{tsx,test.tsx}`, `useSetupWizard.{ts,test.ts}`, `SettingsPage.{tsx,test.tsx}`, `scripts/check-dark-classes.mjs` (drop two skip entries). Then delete `pages/RecoverPage.{tsx,test.tsx}`, `components/security/RecoveryPhrase{Confirm,Modal}.{tsx,test.tsx}`, `lib/recoveryPhrase.{ts,test.ts}`, `lib/bip39-wordlist.txt`. All in one commit so typecheck passes at every step.

## Tests (covered inside their respective commits per Sequence above)

- `tests/test_security_master_key.py` (actual filename, not `test_master_key.py`) — extend in Commit 2:
  - `_revoke_stale_ciphertext` direct test, three branches: orphan, no-key, mismatch — each soft-revokes the right row set and leaves matching rows alone.
  - `bootstrap()` end-to-end with mismatched key + creds: returns `clean_install`, logs WARNING, no active creds remain, NOTIFY fires.
  - `bootstrap()` with missing key + orphan creds: same outcome.
  - `bootstrap()` with matching key + creds: still returns `normal`, no revokes triggered, no NOTIFY.
  - `bootstrap()` with empty DB: no-op, no NOTIFY.
  - Concurrent boot idempotency: second invocation finds zero stale rows.
- `tests/smoke/test_app_boots.py` — extend in Commit 2: assert lifespan boots cleanly when `secrets/master.key` is absent and `broker_credentials` rows exist; `app.state.broker_key_loaded` is `False` post-revoke; `/health` does not 503.
- Frontend (Commit 3): `SetupPage.test.tsx` rewritten for single-step; `useSetupWizard.test.ts` drops broker state assertions; `SettingsPage.test.tsx` drops phrase-modal assertions.
- Delete: `tests/test_security_recovery_phrase.py`, `RecoveryPhraseConfirm.test.tsx`, `RecoverPage.test.tsx`, `recoveryPhrase.test.ts` (each inside their respective commit).

## Non-goals

- No password-recovery flow. "Forgot password" = wipe operators table + redo setup (CLI / DB-level operation, not in-app).
- No multi-machine / portable-secret story. Operator backs up their data dir or re-enters keys.
- No third-party recovery (KMS, email, OS keychain). Per CLAUDE.md "do not add libraries casually" — net dependency burn is **negative** in this PR.

## Settled-decisions impact

`docs/settled-decisions.md` § "Operator auth and broker-secret storage" updated to read:

> Governed by [`ADR-0001`](adr/0001-operator-auth-and-broker-secrets.md) and [`ADR-0003`](adr/0003-local-secret-bootstrap-and-recovery.md) (amended 2026-05-07: phrase-based recovery removed in favour of operator-driven re-entry; stale-cipher soft-revoke runs at boot when key material is missing or mismatches existing ciphertext).

Risk-posture (Demo-first / solo-operator local install, per CLAUDE.md) makes the trade-off acceptable: total loss of data dir = re-enter creds at eToro. Stale ciphertext is soft-revoked at boot (audit trail preserved); no silent data loss.

## Open questions

None at spec time. v4 captures all Codex round 1 + 2 + 3 findings:
- round 1: state-machine decoupling, soft-revoke (not hard delete), missing callers (main.py, jobs/__main__.py, layer_types.py, SettingsPage, brokerCredentials.ts), test file naming, caller-update-before-delete sequencing
- round 2: needs_setup operator-only (drop key_needs_setup OR), nullable derived_key, orphan branch independent of operators_join, NOTIFY on revoke, test sequencing inside commits
- round 3: drop `app.state.needs_setup` writes (BootResult.needs_setup removal cascade), per-operator NOTIFY (matches listener payload contract), env-override passes derived_key (does not enter no-key branch), edge map repair-mode banner
