# Frontend test speedup — design spec

**Ticket:** #327 (remaining frontend subset — vitest cap, SetupPage refactor, test-script split. Backend pytest-xdist deferred to separate PR with DB-race audit.)

## Problem

From #327 ticket body:

> Frontend is the CPU hog (parallel jsdom workers pin every core). SetupPage wizard tests alone are 9+ tests at 1000–1500ms each. Operator reports fan spin during runs.

Plus an observed reliability issue: default vitest threads pool crashed with `Maximum call stack size exceeded` during #399 execution. Forks pool avoids it.

## Decisions (v3 — after Codex ckpt 1 round 2)

Codex round 1 surfaced 5 issues; v2 addressed them but round 2 found 3 more SetupPage-code mismatches. This v3 matches the live state machine exactly.

| # | Decision | Reason |
| --- | --- | --- |
| 1 | **Pin `pool: "forks"` + `maxForks: 2` in `vitest.config.ts`.** | Vitest v2 already defaults to forks per v2.1.8 migration guide; the real change is the worker cap + making the pool choice explicit so a future `vitest.config.ts` edit doesn't silently switch back to threads (where #399 hit the crash). |
| 2 | **Extract `useSetupWizard` hook + pure `wizardReducer` from `SetupPage.tsx`.** Reducer covers wizard state machine + `credRows`; form-field inputs stay as component-level `useState`. Derived broker-mode (`create` / `repair` / `complete`) comes from `deriveCredentialSetMode(credRows)` — NOT a reducer field. | State-machine transitions are what reducer tests can cover cheaply. Form-field churn doesn't belong in the state machine. `deriveCredentialSetMode` is already a pure function in `frontend/src/lib/credentialSetMode.ts`; keeping it as a selector over `credRows` preserves the single-source-of-truth pattern. |
| 3 | **Hook method dispatchers preserve existing UX/security contracts exactly.** Setup-operator error path dispatches `GENERIC_ERROR` (constant) — never `err.message`. Preserves #98 non-leaky error contract. | Codex finding 3: my v1 sample hook drifted from `SetupPage.tsx:59,141` — setup fetch errors must ALL map to `"Setup unavailable or invalid token."` so auth failure modes are not distinguishable to an unauthenticated attacker. |
| 4 | **Keep 6 integration tests** (down from ~12): happy-path save, happy-path skip, confirm-cancel, phrase-modal, repair-mode, already-complete. | Codex finding 1 + 2: repair-mode + already-complete are distinct behaviours; happy-path save + skip are distinct paths (skip has no broker call). Integration tests cover fetch wiring + multi-step UX that reducer tests cannot. |
| 5 | **Hook-level integration test covers the GENERIC_ERROR contract** via `renderHook(useSetupWizard).submitOperator()` against a mocked rejecting fetch. | Codex finding 3: this is the #98 contract; cannot drop it. `renderHook` is cheap — no full SetupPage render, just the hook. |
| 6 | **Script split: `test` (both) / `test:unit` (exclude SetupPage.test.tsx) / `test:integration` (SetupPage only).** | Ticket §3. Pre-push local runs `test:unit`; CI runs `test` (full). |
| 7 | **Update `.claude/CLAUDE.md` pre-push block.** `pnpm --dir frontend test` → `pnpm --dir frontend test:unit`. | Fast local loop without losing CI coverage. |
| 8 | **Backend pytest-xdist deferred.** | Codex ckpt 1 on #404 flagged that many tests touch `ebull_test` without the `ebull_test_conn` fixture + collection-bootstrap races under multi-worker xdist. Needs its own audit PR. |

## Architecture

Frontend-only PR. Seven surfaces touched:

| Path | Responsibility | Action |
| --- | --- | --- |
| `frontend/vitest.config.ts` | Vitest config | Modify (pin pool + `maxForks: 2`) |
| `frontend/package.json` | Test scripts | Modify (add `test:unit`, `test:integration`) |
| `frontend/src/pages/useSetupWizard.ts` | New — `wizardReducer` + `useSetupWizard` hook | Create |
| `frontend/src/pages/useSetupWizard.test.ts` | New — reducer unit tests + hook-integration tests | Create |
| `frontend/src/pages/SetupPage.tsx` | Consume hook; drop the 13-`useState` constellation | Modify |
| `frontend/src/pages/SetupPage.test.tsx` | Trim to 6 integration tests | Modify |
| `.claude/CLAUDE.md` | Pre-push block points to `test:unit` | Modify |

## `useSetupWizard` — reducer + hook (v2)

### State

```ts
// frontend/src/pages/useSetupWizard.ts
import { useReducer, useCallback } from "react";
// imports: Operator, BrokerCredentialView, ValidateCredentialResponse types + api fns

// Matches SetupPage.tsx: no "done" step. Completion is a side effect
// (markAuthenticated + navigate("/") ) driven by the component via the
// hook's onComplete callback, not a reducer state.
export type WizardStep = "operator" | "broker";

export type WizardState = {
  step: WizardStep;
  pendingOperator: Operator | null;
  operatorSubmitting: boolean;
  operatorError: string | null;
  // Step-2 fetch + submit state
  credRows: BrokerCredentialView[] | null;
  credRowsLoading: boolean;
  credRowsError: string | null;
  brokerSubmitting: boolean;
  brokerError: string | null;
  // Validation state
  validating: boolean;
  validation: ValidateCredentialResponse | null;
  validationError: string | null;
};

export const initialWizardState: WizardState = {
  step: "operator",
  pendingOperator: null,
  operatorSubmitting: false,
  operatorError: null,
  credRows: null,
  credRowsLoading: false,
  credRowsError: null,
  brokerSubmitting: false,
  brokerError: null,
  validating: false,
  validation: null,
  validationError: null,
};
```

Note: **derived broker-mode (`create` / `repair` / `complete`) is NOT a reducer field.** Components + tests call `deriveCredentialSetMode(state.credRows)` — the existing pure function — to get the mode. Single source of truth stays `credRows`.

### Actions

```ts
export type WizardAction =
  | { type: "OPERATOR_SUBMIT_START" }
  | { type: "OPERATOR_SUBMIT_SUCCESS"; operator: Operator }
  | { type: "OPERATOR_SUBMIT_ERROR" }               // no payload — always GENERIC_ERROR
  | { type: "BROKER_CREDS_LOAD_START" }
  | { type: "BROKER_CREDS_LOAD_SUCCESS"; rows: BrokerCredentialView[] }
  | { type: "BROKER_CREDS_LOAD_ERROR"; error: string }   // also sets credRows=null so deriveCredentialSetMode() falls back to 'create'
  | { type: "BROKER_SUBMIT_START" }
  | { type: "BROKER_SUBMIT_SUCCESS"; rows: BrokerCredentialView[] }  // carries refreshed rows
  | { type: "BROKER_SUBMIT_ERROR"; error: string; rows: BrokerCredentialView[] | null }  // error already classified to fixed string by classifyBrokerSaveError()
  | { type: "VALIDATION_START" }
  | { type: "VALIDATION_SUCCESS"; result: ValidateCredentialResponse }
  | { type: "VALIDATION_ERROR"; error: string };

// NOTE: no BROKER_SKIP or WIZARD_COMPLETE actions. Both are side effects
// (component invokes onComplete callback → markAuthenticated + navigate).
// Matches SetupPage.tsx completeWizard() at lines 122-127.
```

Key design points:

- `OPERATOR_SUBMIT_ERROR` carries no payload. Reducer unconditionally sets `operatorError = GENERIC_ERROR`. Preserves the #98 non-leaky contract — no error message can leak from an action into state.
- `BROKER_SUBMIT_ERROR.error` is already a fixed classified string ("A credential with that label already exists..." / "Invalid API key or user key value." / "Could not save credential."). Classification happens in the pure helper `classifyBrokerSaveError(err)` inside `useSetupWizard.ts`. The helper is unit-tested independently; the reducer just stores whatever string the hook passes.
- `BROKER_SUBMIT_ERROR` carries `rows` so the component's next render has the re-fetched credential state to derive `repair` mode. Pairs with `listBrokerCredentials()` inside the hook's `submitBroker` catch block.
- `BROKER_SUBMIT_SUCCESS` also carries `rows` — refreshed post-save so the UI can flip to `complete` mode immediately.
- `BROKER_CREDS_LOAD_ERROR` also sets `credRows: null` so `deriveCredentialSetMode(null)` returns `create` mode — matches the fallback at `SetupPage.tsx:97`.
- `VALIDATION_START` clears both `validation` AND `validationError` — matches `SetupPage.tsx:160-162` which clears result before the request.

### Reducer

```ts
import { GENERIC_ERROR } from "@/pages/setupErrorMessages"; // move constant to shared module

export function wizardReducer(state: WizardState, action: WizardAction): WizardState {
  switch (action.type) {
    case "OPERATOR_SUBMIT_START":
      return { ...state, operatorSubmitting: true, operatorError: null };
    case "OPERATOR_SUBMIT_SUCCESS":
      return {
        ...state,
        step: "broker",
        pendingOperator: action.operator,
        operatorSubmitting: false,
        operatorError: null,
      };
    case "OPERATOR_SUBMIT_ERROR":
      return { ...state, operatorSubmitting: false, operatorError: GENERIC_ERROR };
    case "BROKER_CREDS_LOAD_START":
      return { ...state, credRowsLoading: true, credRowsError: null };
    case "BROKER_CREDS_LOAD_SUCCESS":
      return { ...state, credRowsLoading: false, credRows: action.rows };
    case "BROKER_CREDS_LOAD_ERROR":
      // credRows=null → deriveCredentialSetMode returns 'create'. Matches
      // the current SetupPage fallback when list-creds fetch fails.
      return { ...state, credRowsLoading: false, credRowsError: action.error, credRows: null };
    case "BROKER_SUBMIT_START":
      return { ...state, brokerSubmitting: true, brokerError: null };
    case "BROKER_SUBMIT_SUCCESS":
      return {
        ...state,
        brokerSubmitting: false,
        brokerError: null,
        credRows: action.rows,
      };
    case "BROKER_SUBMIT_ERROR":
      return {
        ...state,
        brokerSubmitting: false,
        brokerError: action.error,
        credRows: action.rows,
      };
    case "VALIDATION_START":
      // Clear prior result too — matches SetupPage.tsx:160-162 where
      // both setValidationResult(null) and setValidationError(null) fire
      // before the request.
      return { ...state, validating: true, validation: null, validationError: null };
    case "VALIDATION_SUCCESS":
      return { ...state, validating: false, validation: action.result, validationError: null };
    case "VALIDATION_ERROR":
      return { ...state, validating: false, validationError: action.error };
  }
}
```

### Broker save error classifier (pure helper)

```ts
// frontend/src/pages/useSetupWizard.ts
import { ApiError } from "@/api/client";

export function classifyBrokerSaveError(err: unknown): string {
  if (err instanceof ApiError && err.status === 409) {
    return "A credential with that label already exists. Revoke it from Settings to replace.";
  }
  if (err instanceof ApiError && err.status === 400) {
    return "Invalid API key or user key value.";
  }
  return "Could not save credential.";
}
```

Unit-tested independently (fast, no mocks). Matches the mapping in `SetupPage.tsx:229-236`.

### Hook

```ts
export interface UseSetupWizardOptions {
  onComplete: () => void;   // markAuthenticated + navigate side effect
}

export function useSetupWizard({ onComplete }: UseSetupWizardOptions) {
  const [state, dispatch] = useReducer(wizardReducer, initialWizardState);

  const loadCredentials = useCallback(async (): Promise<void> => {
    dispatch({ type: "BROKER_CREDS_LOAD_START" });
    try {
      const rows = await listBrokerCredentials();
      dispatch({ type: "BROKER_CREDS_LOAD_SUCCESS", rows });
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to load credentials";
      dispatch({ type: "BROKER_CREDS_LOAD_ERROR", error: msg });
    }
  }, []);

  const submitOperator = useCallback(async (form: OperatorForm): Promise<boolean> => {
    dispatch({ type: "OPERATOR_SUBMIT_START" });
    try {
      const { operator } = await postSetup(form.username, form.password, form.setupToken);
      dispatch({ type: "OPERATOR_SUBMIT_SUCCESS", operator });
      return true;
    } catch {
      // #98: never leak err.message. Reducer sets GENERIC_ERROR unconditionally.
      dispatch({ type: "OPERATOR_SUBMIT_ERROR" });
      return false;
    }
  }, []);

  // Submit one broker credential. Returns an object describing what to do
  // next. Mirrors SetupPage.tsx's inner handleSaveOne / handleBrokerSubmit
  // split. Only called by the component's top-level submit handler.
  const submitBroker = useCallback(
    async (form: BrokerForm): Promise<{ ok: true; recoveryPhrase: readonly string[] | null } | { ok: false }> => {
      // Snapshot the pre-save mode for the "only fire universe sync on
      // first-time create" branch below.
      const wasCreate = deriveCredentialSetMode(state.credRows).mode === "create";

      dispatch({ type: "BROKER_SUBMIT_START" });
      try {
        const result = await saveBrokerCredentials(form);  // may return recovery_phrase on first save
        const rows = await listBrokerCredentials();
        dispatch({ type: "BROKER_SUBMIT_SUCCESS", rows });

        // First-run bootstrap (matches SetupPage.tsx:213-219). Fire-and-forget.
        if (wasCreate) {
          void runJob("nightly_universe_sync").catch(() => {});
        }
        return { ok: true, recoveryPhrase: result.recovery_phrase ?? null };
      } catch (err) {
        const msg = classifyBrokerSaveError(err);
        let rows: BrokerCredentialView[] | null = null;
        try {
          rows = await listBrokerCredentials();
        } catch {
          // swallow — deriveCredentialSetMode(null) falls back to 'create'
        }
        dispatch({ type: "BROKER_SUBMIT_ERROR", error: msg, rows });
        return { ok: false };
      }
    },
    [state.credRows],
  );

  // Side-effect callbacks — no state mutation; caller invokes onComplete.
  const skipBroker = useCallback(() => onComplete(), [onComplete]);
  const completeWizard = useCallback(() => onComplete(), [onComplete]);

  const validateCredentials = useCallback(async (form: BrokerForm): Promise<void> => {
    dispatch({ type: "VALIDATION_START" });
    try {
      const result = await validateBrokerCredential({
        api_key: form.apiKey,
        user_key: form.userKey,
        environment: ENVIRONMENT,
      });
      dispatch({ type: "VALIDATION_SUCCESS", result });
    } catch {
      dispatch({ type: "VALIDATION_ERROR", error: "Could not reach the validation endpoint." });
    }
  }, []);

  return {
    state,
    loadCredentials,
    submitOperator,
    submitBroker,
    skipBroker,
    completeWizard,
    validateCredentials,
  };
}
```

`onComplete` is the component-side side-effect bundle (`markAuthenticated + navigate`). The hook never mutates navigation state; it just invokes the callback. That keeps the hook DOM-free + testable via `renderHook` with a mock `onComplete`.

## GENERIC_ERROR extraction

Current `SetupPage.tsx:59` has `const GENERIC_ERROR = "Setup unavailable or invalid token."` as a module-local constant. Move to a new `frontend/src/pages/setupErrorMessages.ts` so both the component AND the hook import it from the same place:

```ts
// frontend/src/pages/setupErrorMessages.ts
export const GENERIC_ERROR = "Setup unavailable or invalid token.";
```

## Unit tests — `useSetupWizard.test.ts`

### Reducer pure tests (~17)

```ts
import { describe, expect, it } from "vitest";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import { initialWizardState, wizardReducer } from "@/pages/useSetupWizard";

describe("wizardReducer", () => {
  // --- OPERATOR transitions ---
  it("OPERATOR_SUBMIT_START: sets submitting, clears error", () => { /* ... */ });
  it("OPERATOR_SUBMIT_SUCCESS: advances step to broker, stores operator, clears submitting", () => { /* ... */ });
  it("OPERATOR_SUBMIT_ERROR: sets error to GENERIC_ERROR exactly", () => {
    const s = wizardReducer(initialWizardState, { type: "OPERATOR_SUBMIT_START" });
    const after = wizardReducer(s, { type: "OPERATOR_SUBMIT_ERROR" });
    expect(after.operatorError).toBe(GENERIC_ERROR);
    expect(after.operatorSubmitting).toBe(false);
    expect(after.step).toBe("operator");
  });

  // --- BROKER CREDS LOAD transitions ---
  it("BROKER_CREDS_LOAD_START: sets loading, clears error", () => { /* ... */ });
  it("BROKER_CREDS_LOAD_SUCCESS: stores rows, clears loading", () => { /* ... */ });
  it("BROKER_CREDS_LOAD_ERROR: stores error, clears loading", () => { /* ... */ });

  // --- BROKER SUBMIT transitions ---
  it("BROKER_SUBMIT_START: sets submitting, clears error", () => { /* ... */ });
  it("BROKER_SUBMIT_SUCCESS: stores refreshed rows, clears submitting", () => { /* ... */ });
  it("BROKER_SUBMIT_ERROR: stores rows (for repair-mode derivation) + error", () => { /* ... */ });
  it("BROKER_SUBMIT_ERROR with rows=null: repair-mode derivation falls back to create", () => {
    // Integration with deriveCredentialSetMode: rows=null → mode='create'
    // Documenting the invariant here so a future reducer change can't break it silently.
    const s = wizardReducer(initialWizardState, {
      type: "BROKER_SUBMIT_ERROR",
      error: "boom",
      rows: null,
    });
    expect(s.credRows).toBeNull();
    expect(s.brokerError).toBe("boom");
  });

  // --- VALIDATION ---
  it("VALIDATION_START: sets validating, clears BOTH prior validation AND validationError", () => {
    const seeded: WizardState = {
      ...initialWizardState,
      validation: { ok: true, detail: "old" } as ValidateCredentialResponse,
      validationError: "old err",
    };
    const s = wizardReducer(seeded, { type: "VALIDATION_START" });
    expect(s.validating).toBe(true);
    expect(s.validation).toBeNull();
    expect(s.validationError).toBeNull();
  });
  it("VALIDATION_SUCCESS: stores result", () => { /* ... */ });
  it("VALIDATION_ERROR: stores error", () => { /* ... */ });

  // --- Invariants ---
  it("OPERATOR_SUBMIT_ERROR does not advance step", () => { /* ... */ });
  it("BROKER_SUBMIT_ERROR does not advance step", () => { /* ... */ });
  it("BROKER_CREDS_LOAD_ERROR forces credRows=null for create-mode fallback", () => {
    const seeded: WizardState = {
      ...initialWizardState,
      credRows: [{ provider: "etoro", label: "api_key" }] as BrokerCredentialView[],
    };
    const s = wizardReducer(seeded, { type: "BROKER_CREDS_LOAD_ERROR", error: "network" });
    expect(s.credRows).toBeNull();
  });
});

describe("classifyBrokerSaveError", () => {
  it("409 ApiError: maps to 'A credential with that label already exists...'", () => {
    expect(classifyBrokerSaveError(new ApiError(409, "conflict"))).toBe(
      "A credential with that label already exists. Revoke it from Settings to replace.",
    );
  });
  it("400 ApiError: maps to 'Invalid API key or user key value.'", () => {
    expect(classifyBrokerSaveError(new ApiError(400, "bad"))).toBe("Invalid API key or user key value.");
  });
  it("other ApiError: maps to 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError(new ApiError(500, "boom"))).toBe("Could not save credential.");
  });
  it("non-ApiError: maps to 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError(new Error("random"))).toBe("Could not save credential.");
  });
});
```

### Hook-level integration tests (~4) — `renderHook`, no DOM

```ts
import { renderHook, act } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import { useSetupWizard } from "@/pages/useSetupWizard";

vi.mock("@/api/auth");
vi.mock("@/api/brokerCredentials");
vi.mock("@/api/jobs");

describe("useSetupWizard (hook)", () => {
  const onComplete = vi.fn();

  it("submitOperator maps any fetch failure to GENERIC_ERROR (not err.message)", async () => {
    const { postSetup } = await import("@/api/auth");
    vi.mocked(postSetup).mockRejectedValue(new Error("Leaky detail"));

    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    await act(async () => {
      await result.current.submitOperator({ username: "x", password: "y", setupToken: "z" });
    });
    expect(result.current.state.operatorError).toBe(GENERIC_ERROR);
    expect(result.current.state.operatorError).not.toContain("Leaky detail");
  });

  it("submitBroker success on create mode fires runJob(nightly_universe_sync) fire-and-forget", async () => {
    // Seed state with no credRows → mode='create' → wasCreate=true
    // Assert runJob called; assert dispatcher still returns ok:true even if
    // runJob rejects asynchronously.
    /* ... */
  });

  it("submitBroker success on repair mode does NOT fire runJob", async () => {
    // Seed credRows with just api_key → mode='repair' → wasCreate=false
    /* ... */
  });

  it("submitBroker failure re-fetches credentials; list fetch also failing leaves credRows=null", async () => { /* ... */ });
});
```

## Integration tests — `SetupPage.test.tsx` trim (v2: 6 kept)

Codex finding 1 + 2: the original "3 tests" scope lost repair-mode, already-complete, and save-vs-skip distinction coverage. Keep 6:

1. **Happy path save** — submit operator → step 2 render with loaded credRows → submit both credentials → `markAuthenticated` + `navigate` side effects + `nightly_universe_sync` runJob fires.
2. **Happy path skip** — submit operator → step 2 render → click Skip → `markAuthenticated` fires, NO broker POST, NO universe-sync call.
3. **Confirm-cancel gate** — Cancel during broker step → confirm dialog → "Close anyway" fires `markAuthenticated`.
4. **Phrase-modal branch** — first save returns `recovery_phrase` → modal opens → operator passes challenge → `markAuthenticated` fires.
5. **Repair mode** — first save partial-fails → `credRows` refreshed → UI shows `repair` label + "finish" CTA → second save succeeds → `markAuthenticated` fires.
6. **Already-complete branch** — step 2 opens with existing active credRows (both api_key + user_key) → UI shows `complete` label + Continue button → click Continue → `markAuthenticated` fires.

**Dropped** (covered by reducer or folded into happy-path):
- `surfaces the generic error and stays on step 1 when /auth/setup fails` → hook-integration `submitOperator maps any fetch failure to GENERIC_ERROR` test covers the security contract more precisely.
- `advances to step 2 on success WITHOUT calling markAuthenticated yet` → asserted in happy-path save/skip via mock-call-order.
- `creates both api_key and user_key rows on save` → covered by happy-path save (both POSTs asserted).
- `completes wizard with NO phrase modal when response has no recovery_phrase` → covered by happy-path save (default mock has no recovery_phrase).

## Vitest config change

```ts
// frontend/vitest.config.ts
export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: "jsdom",
      globals: false,
      setupFiles: ["./src/test/setup.ts"],
      css: false,
      include: ["src/**/*.{test,spec}.{ts,tsx}"],
      // Pin pool + cap concurrency. Vitest v2 defaults to forks already;
      // pinning makes the choice explicit so a future edit doesn't silently
      // switch back to threads (where #399 hit a tinypool crash).
      pool: "forks",
      poolOptions: { forks: { maxForks: 2 } },
    },
  }),
);
```

## Script split

```json
"scripts": {
  "dev": "vite",
  "build": "tsc -b && vite build",
  "typecheck": "tsc -b --noEmit",
  "preview": "vite preview",
  "test": "vitest run",
  "test:unit": "vitest run --exclude src/pages/SetupPage.test.tsx",
  "test:integration": "vitest run src/pages/SetupPage.test.tsx",
  "test:watch": "vitest"
}
```

## CLAUDE.md pre-push block

Change:

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

To:

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test:unit
```

Add callout below the block:

> **Note:** Pre-push runs `test:unit` (excludes `src/pages/SetupPage.test.tsx`). CI runs the full `test` script on push — integration tests still gate merge.

## Edge cases

| Case | Behaviour |
| --- | --- |
| Developer runs `pnpm --dir frontend test` | Full suite including SetupPage integration. Matches CI. |
| Developer runs `pnpm --dir frontend test:unit` | Excludes SetupPage.test.tsx. Fast local iteration. |
| CI | Runs `pnpm --dir frontend test` unchanged. |
| Hook-integration tests | Use `renderHook` — no DOM render cost. Sub-50ms each. |
| `BROKER_SUBMIT_ERROR` with `listBrokerCredentials` also failing | Hook catches the list error + dispatches `rows: null`. Reducer stores `credRows: null`. `deriveCredentialSetMode(null)` returns `create` mode — safe fallback; user can retry. Documented in reducer test. |
| Pre-push local run | ~2-4s (only unit + hook-integration) vs. the current full-suite cost. Full integration still runs in CI + on merge. |

## Verification

- `pnpm --dir frontend test src/pages/useSetupWizard.test.ts` → ~20 tests PASS, <500ms.
- `pnpm --dir frontend test:integration` → 6 tests PASS.
- `pnpm --dir frontend test` → full suite PASS.
- `pnpm --dir frontend test:unit` → excludes SetupPage integration file.
- `pnpm --dir frontend typecheck` → green.
- Backend gates (unchanged by this PR) still green.

## Rollback

Revert the commit — reducer + hook + error-messages module deleted, SetupPage restored. Vitest config + package.json + CLAUDE.md revert cleanly.

## Follow-up (not in this PR)

- Backend pytest-xdist (needs `ebull_test` audit + collection-bootstrap idempotency — Codex ckpt 1 on #404 predecessor).
- Additional integration tests under `*.integration.test.tsx` convention if the integration set grows.

## PR description skeleton

Title: `fix(#327): frontend test speedup — vitest cap + SetupPage reducer + script split`

Body:

> **What**
>
> - `vitest.config.ts` — pin `pool: "forks"` + `maxForks: 2`. Vitest v2 defaults to forks already; pinning prevents silent regression + the cap halves sustained CPU.
> - New `frontend/src/pages/setupErrorMessages.ts` — shared `GENERIC_ERROR` constant.
> - New `frontend/src/pages/useSetupWizard.ts` — pure `wizardReducer` + thin `useSetupWizard` hook. Extracts wizard state machine from `SetupPage.tsx`.
> - New `frontend/src/pages/useSetupWizard.test.ts` — ~17 reducer unit tests + ~3 `renderHook` integration tests.
> - `SetupPage.test.tsx` trimmed from ~12 tests to 6 (happy-path save, happy-path skip, confirm-cancel, phrase-modal, repair-mode, already-complete).
> - `package.json` — `test:unit` / `test:integration` / `test` script split.
> - `.claude/CLAUDE.md` — pre-push block points to `test:unit`; CI still runs full `test`.
>
> **Why**
>
> Closes frontend items from #327. SetupPage previously had ~12 tests at 1000-1500ms each because every transition did a full render + fetch mock dance. Pure reducer covers transition logic at <50ms total; hook-level `renderHook` covers fetch wiring; 6 kept integration tests cover irreducible multi-step UX (repair-mode, already-complete, phrase-modal, confirm-cancel gate, save/skip divergence).
>
> Vitest v2 already defaults to forks, so the pool change is pinning + cap. Backend pytest-xdist from #327 deferred to follow-up (needs `ebull_test` audit).
>
> **Test plan**
>
> - Reducer unit tests: ~17 PASS <500ms.
> - Hook-integration tests: 3 PASS.
> - Integration tests: 6 PASS.
> - Full suite green (frontend + backend).
>
> **Called out**
>
> - #98 non-leaky-error contract preserved: `OPERATOR_SUBMIT_ERROR` action carries no payload; reducer sets `operatorError = GENERIC_ERROR` unconditionally. Hook-integration test asserts the contract explicitly.
> - Derived broker-mode (`create` / `repair` / `complete`) stays a pure selector over `credRows` — not a reducer field. Single source of truth.
> - CI change: none. CI runs full `test`; pre-push local runs `test:unit`.
> - pytest-xdist deferred per prior Codex ckpt 1 finding on #404 predecessor.
