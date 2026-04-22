# Frontend Test Speedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the frontend items from #327 — extract `useSetupWizard` hook + pure `wizardReducer` from `SetupPage.tsx`, trim `SetupPage.test.tsx` from ~12 tests to 6 integration tests, pin vitest `forks` pool with `maxForks: 2`, add `test:unit`/`test:integration` script split, update CLAUDE.md pre-push.

**Architecture:** Reducer owns wizard state machine (step + submit/error/validation flags + credRows). Derived mode (`create`/`repair`/`complete`) stays a pure selector over `credRows`. Hook wraps reducer with fetch dispatchers, takes `onComplete` callback (no `"done"` state — completion is side effect). Pure `classifyBrokerSaveError` maps `ApiError` status → fixed string. Component consumes hook + owns form-field `useState`.

**Tech Stack:** React + TypeScript + Vite + Vitest ^2.1.8 + @testing-library/react.

**Spec:** `docs/superpowers/specs/2026-04-22-frontend-test-speedup.md`

**Ticket:** #327 (frontend subset). Branch `fix/327b-frontend-test-speedup` exists; spec committed.

---

## File Structure

| Path | Responsibility | Action |
| --- | --- | --- |
| `frontend/src/pages/setupErrorMessages.ts` | Shared `GENERIC_ERROR` constant | Create |
| `frontend/src/pages/useSetupWizard.ts` | `wizardReducer` + `useSetupWizard` hook + `classifyBrokerSaveError` | Create |
| `frontend/src/pages/useSetupWizard.test.ts` | Reducer unit tests + hook integration tests + classifier tests | Create |
| `frontend/src/pages/SetupPage.tsx` | Consume hook; keep form-field `useState`; drop 13-state constellation | Modify |
| `frontend/src/pages/SetupPage.test.tsx` | Trim to 6 integration tests | Modify |
| `frontend/vitest.config.ts` | Pin forks pool + `maxForks: 2` | Modify |
| `frontend/package.json` | Add `test:unit`, `test:integration` scripts | Modify |
| `.claude/CLAUDE.md` | Pre-push block points to `test:unit` | Modify |

---

## Task 1: vitest config pin + script split

Smallest change; ships independently. Unlocks speed for subsequent tasks.

- [ ] **Step 1: Edit `frontend/vitest.config.ts`**

Replace the existing `test` block with:

```ts
import { defineConfig, mergeConfig } from "vitest/config";

import viteConfig from "./vite.config";

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

- [ ] **Step 2: Edit `frontend/package.json` scripts block**

Replace:

```json
"test": "vitest run",
"test:watch": "vitest"
```

With:

```json
"test": "vitest run",
"test:unit": "vitest run --exclude src/pages/SetupPage.test.tsx",
"test:integration": "vitest run src/pages/SetupPage.test.tsx",
"test:watch": "vitest"
```

- [ ] **Step 3: Verify existing tests still green**

```bash
pnpm --dir frontend test
```

Expected: all current tests PASS (behaviour unchanged; pool pinning is a no-op on v2.1.8).

- [ ] **Step 4: Verify `test:unit` excludes correctly**

```bash
pnpm --dir frontend test:unit
```

Expected: no tests from `src/pages/SetupPage.test.tsx` in the output. Faster total run.

- [ ] **Step 5: Verify `test:integration` runs just SetupPage**

```bash
pnpm --dir frontend test:integration
```

Expected: only SetupPage tests. Handful of tests, focused run.

- [ ] **Step 6: Commit**

```bash
git add frontend/vitest.config.ts frontend/package.json
git commit -m "build(#327): pin vitest forks pool + maxForks:2 + unit/integration script split"
```

---

## Task 2: `setupErrorMessages.ts` (shared constant)

Trivial extraction. Unblocks both hook + component + tests importing from a stable location.

- [ ] **Step 1: Create the file**

Create `frontend/src/pages/setupErrorMessages.ts`:

```ts
/**
 * Shared error-message constants for the operator setup wizard.
 *
 * GENERIC_ERROR is the single string every operator-setup fetch failure
 * maps to. Used by SetupPage.tsx + useSetupWizard.ts to preserve the
 * #98 non-leaky-error contract: an unauthenticated attacker must not
 * be able to distinguish failure modes of the /auth/setup endpoint.
 */
export const GENERIC_ERROR = "Setup unavailable or invalid token.";
```

- [ ] **Step 2: Typecheck**

```bash
pnpm --dir frontend typecheck
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/setupErrorMessages.ts
git commit -m "refactor(#327): extract GENERIC_ERROR constant to setupErrorMessages module"
```

---

## Task 3: `useSetupWizard.ts` — reducer + hook + classifier (TDD)

TDD: reducer unit tests first (red), then reducer body, then classifier, then hook. Hook tests use `renderHook`.

**Files:**

- Create: `frontend/src/pages/useSetupWizard.ts`
- Create: `frontend/src/pages/useSetupWizard.test.ts`

- [ ] **Step 1: Write the reducer + classifier test file**

Create `frontend/src/pages/useSetupWizard.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { ApiError } from "@/api/client";
import type { BrokerCredentialView } from "@/api/brokerCredentials";
import type { Operator } from "@/api/auth";
import type { ValidateCredentialResponse } from "@/api/brokerCredentials";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";

import {
  classifyBrokerSaveError,
  initialWizardState,
  wizardReducer,
  type WizardState,
} from "@/pages/useSetupWizard";

const OP: Operator = { operator_id: "op-1", username: "test", display_name: "Test" };
const ROW_API: BrokerCredentialView = {
  credential_id: "c-1",
  operator_id: "op-1",
  provider: "etoro",
  label: "api_key",
  environment: "demo",
  created_at: "2026-04-22T00:00:00Z",
  revoked_at: null,
};
const ROW_USER: BrokerCredentialView = { ...ROW_API, credential_id: "c-2", label: "user_key" };
const VAL_OK: ValidateCredentialResponse = { ok: true, detail: "fine" };

describe("wizardReducer — OPERATOR", () => {
  it("OPERATOR_SUBMIT_START: sets submitting, clears error", () => {
    const s = wizardReducer(
      { ...initialWizardState, operatorError: "prev" },
      { type: "OPERATOR_SUBMIT_START" },
    );
    expect(s.operatorSubmitting).toBe(true);
    expect(s.operatorError).toBeNull();
  });

  it("OPERATOR_SUBMIT_SUCCESS: advances step to broker, stores operator", () => {
    const s = wizardReducer(
      { ...initialWizardState, operatorSubmitting: true },
      { type: "OPERATOR_SUBMIT_SUCCESS", operator: OP },
    );
    expect(s.step).toBe("broker");
    expect(s.pendingOperator).toEqual(OP);
    expect(s.operatorSubmitting).toBe(false);
    expect(s.operatorError).toBeNull();
  });

  it("OPERATOR_SUBMIT_ERROR: sets error to GENERIC_ERROR exactly (no payload)", () => {
    const s = wizardReducer(
      { ...initialWizardState, operatorSubmitting: true },
      { type: "OPERATOR_SUBMIT_ERROR" },
    );
    expect(s.operatorError).toBe(GENERIC_ERROR);
    expect(s.operatorSubmitting).toBe(false);
    expect(s.step).toBe("operator");
  });
});

describe("wizardReducer — BROKER creds load", () => {
  it("BROKER_CREDS_LOAD_START: sets loading, clears prior error", () => {
    const s = wizardReducer(
      { ...initialWizardState, credRowsError: "old" },
      { type: "BROKER_CREDS_LOAD_START" },
    );
    expect(s.credRowsLoading).toBe(true);
    expect(s.credRowsError).toBeNull();
  });

  it("BROKER_CREDS_LOAD_SUCCESS: stores rows, clears loading", () => {
    const s = wizardReducer(
      { ...initialWizardState, credRowsLoading: true },
      { type: "BROKER_CREDS_LOAD_SUCCESS", rows: [ROW_API] },
    );
    expect(s.credRows).toEqual([ROW_API]);
    expect(s.credRowsLoading).toBe(false);
  });

  it("BROKER_CREDS_LOAD_ERROR: forces credRows=null for create-mode fallback", () => {
    const s = wizardReducer(
      { ...initialWizardState, credRows: [ROW_API] },
      { type: "BROKER_CREDS_LOAD_ERROR", error: "network" },
    );
    expect(s.credRows).toBeNull();
    expect(s.credRowsError).toBe("network");
    expect(s.credRowsLoading).toBe(false);
  });
});

describe("wizardReducer — BROKER submit", () => {
  it("BROKER_SUBMIT_START: sets submitting, clears prior error", () => {
    const s = wizardReducer(
      { ...initialWizardState, brokerError: "old" },
      { type: "BROKER_SUBMIT_START" },
    );
    expect(s.brokerSubmitting).toBe(true);
    expect(s.brokerError).toBeNull();
  });

  it("BROKER_SUBMIT_SUCCESS: stores refreshed rows, clears submitting", () => {
    const s = wizardReducer(
      { ...initialWizardState, brokerSubmitting: true },
      { type: "BROKER_SUBMIT_SUCCESS", rows: [ROW_API, ROW_USER] },
    );
    expect(s.credRows).toEqual([ROW_API, ROW_USER]);
    expect(s.brokerSubmitting).toBe(false);
    expect(s.brokerError).toBeNull();
  });

  it("BROKER_SUBMIT_ERROR: stores error + rows for repair-mode derivation", () => {
    const s = wizardReducer(
      { ...initialWizardState, brokerSubmitting: true },
      { type: "BROKER_SUBMIT_ERROR", error: "Invalid API key or user key value.", rows: [ROW_API] },
    );
    expect(s.brokerError).toBe("Invalid API key or user key value.");
    expect(s.credRows).toEqual([ROW_API]);
    expect(s.brokerSubmitting).toBe(false);
    expect(s.step).toBe("operator"); // no step advance
  });

  it("BROKER_SUBMIT_ERROR with rows=null leaves credRows=null (create-mode fallback)", () => {
    const s = wizardReducer(
      { ...initialWizardState, brokerSubmitting: true },
      { type: "BROKER_SUBMIT_ERROR", error: "Could not save credential.", rows: null },
    );
    expect(s.credRows).toBeNull();
  });
});

describe("wizardReducer — VALIDATION", () => {
  it("VALIDATION_START: clears BOTH prior validation result AND validationError", () => {
    const seeded: WizardState = {
      ...initialWizardState,
      validation: VAL_OK,
      validationError: "old",
    };
    const s = wizardReducer(seeded, { type: "VALIDATION_START" });
    expect(s.validating).toBe(true);
    expect(s.validation).toBeNull();
    expect(s.validationError).toBeNull();
  });

  it("VALIDATION_SUCCESS: stores result", () => {
    const s = wizardReducer(
      { ...initialWizardState, validating: true },
      { type: "VALIDATION_SUCCESS", result: VAL_OK },
    );
    expect(s.validation).toEqual(VAL_OK);
    expect(s.validating).toBe(false);
  });

  it("VALIDATION_ERROR: stores error", () => {
    const s = wizardReducer(
      { ...initialWizardState, validating: true },
      { type: "VALIDATION_ERROR", error: "Could not reach the validation endpoint." },
    );
    expect(s.validationError).toBe("Could not reach the validation endpoint.");
    expect(s.validating).toBe(false);
  });
});

describe("classifyBrokerSaveError", () => {
  it("409 ApiError → fixed 'A credential with that label already exists...'", () => {
    expect(classifyBrokerSaveError(new ApiError(409, "conflict"))).toBe(
      "A credential with that label already exists. Revoke it from Settings to replace.",
    );
  });
  it("400 ApiError → 'Invalid API key or user key value.'", () => {
    expect(classifyBrokerSaveError(new ApiError(400, "bad"))).toBe(
      "Invalid API key or user key value.",
    );
  });
  it("other ApiError → 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError(new ApiError(500, "boom"))).toBe("Could not save credential.");
  });
  it("non-ApiError → 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError(new Error("random"))).toBe("Could not save credential.");
  });
  it("non-Error → 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError("plain string")).toBe("Could not save credential.");
  });
});
```

- [ ] **Step 2: Run — expect all FAIL (module missing)**

```bash
pnpm --dir frontend exec vitest run src/pages/useSetupWizard.test.ts
```

Expected: all tests FAIL with import errors (`useSetupWizard` module not found).

- [ ] **Step 3: Create `useSetupWizard.ts` with reducer + classifier**

Create `frontend/src/pages/useSetupWizard.ts`:

```ts
/**
 * useSetupWizard — state machine + fetch dispatchers for SetupPage.
 *
 * Reducer covers wizard-step transitions and fetch-status flags. Form-field
 * inputs (username, password, apiKey, userKey, setupToken) stay as
 * component-level `useState` in SetupPage.tsx — field churn does not belong
 * in the state machine.
 *
 * Wizard step is "operator" | "broker" only. Completion is a side effect
 * (markAuthenticated + navigate) driven by the component via the
 * `onComplete` callback option — there is no "done" state.
 *
 * Derived broker-mode (create/repair/complete) is a pure selector over
 * state.credRows: callers invoke deriveCredentialSetMode(state.credRows)
 * from @/lib/credentialSetMode. Never a reducer field.
 */
import { useCallback, useReducer } from "react";

import type { Operator } from "@/api/auth";
import { postSetup } from "@/api/auth";
import type { BrokerCredentialView, ValidateCredentialResponse } from "@/api/brokerCredentials";
import {
  createBrokerCredential,
  listBrokerCredentials,
  validateBrokerCredential,
} from "@/api/brokerCredentials";
import { ApiError } from "@/api/client";
import { runJob } from "@/api/jobs";
import { deriveCredentialSetMode, ENVIRONMENT } from "@/lib/credentialSetMode";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";

// ---------------------------------------------------------------------------
// State + actions
// ---------------------------------------------------------------------------

export type WizardStep = "operator" | "broker";

export type WizardState = {
  step: WizardStep;
  pendingOperator: Operator | null;
  operatorSubmitting: boolean;
  operatorError: string | null;
  credRows: BrokerCredentialView[] | null;
  credRowsLoading: boolean;
  credRowsError: string | null;
  brokerSubmitting: boolean;
  brokerError: string | null;
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

export type WizardAction =
  | { type: "OPERATOR_SUBMIT_START" }
  | { type: "OPERATOR_SUBMIT_SUCCESS"; operator: Operator }
  | { type: "OPERATOR_SUBMIT_ERROR" }
  | { type: "BROKER_CREDS_LOAD_START" }
  | { type: "BROKER_CREDS_LOAD_SUCCESS"; rows: BrokerCredentialView[] }
  | { type: "BROKER_CREDS_LOAD_ERROR"; error: string }
  | { type: "BROKER_SUBMIT_START" }
  | { type: "BROKER_SUBMIT_SUCCESS"; rows: BrokerCredentialView[] }
  | { type: "BROKER_SUBMIT_ERROR"; error: string; rows: BrokerCredentialView[] | null }
  | { type: "VALIDATION_START" }
  | { type: "VALIDATION_SUCCESS"; result: ValidateCredentialResponse }
  | { type: "VALIDATION_ERROR"; error: string };

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
      // #98 non-leaky contract: never propagate err.message into state.
      return { ...state, operatorSubmitting: false, operatorError: GENERIC_ERROR };
    case "BROKER_CREDS_LOAD_START":
      return { ...state, credRowsLoading: true, credRowsError: null };
    case "BROKER_CREDS_LOAD_SUCCESS":
      return { ...state, credRowsLoading: false, credRows: action.rows };
    case "BROKER_CREDS_LOAD_ERROR":
      // credRows=null → deriveCredentialSetMode returns 'create'.
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
      return { ...state, validating: true, validation: null, validationError: null };
    case "VALIDATION_SUCCESS":
      return { ...state, validating: false, validation: action.result, validationError: null };
    case "VALIDATION_ERROR":
      return { ...state, validating: false, validationError: action.error };
  }
}

// ---------------------------------------------------------------------------
// Error classifier (pure; unit-tested)
// ---------------------------------------------------------------------------

export function classifyBrokerSaveError(err: unknown): string {
  if (err instanceof ApiError && err.status === 409) {
    return "A credential with that label already exists. Revoke it from Settings to replace.";
  }
  if (err instanceof ApiError && err.status === 400) {
    return "Invalid API key or user key value.";
  }
  return "Could not save credential.";
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export interface UseSetupWizardOptions {
  onComplete: () => void;
}

export interface OperatorSubmitForm {
  username: string;
  password: string;
  setupToken: string;  // already trimmed by caller; empty string = omit
}

export interface BrokerSubmitForm {
  apiKey: string;
  userKey: string;
}

export type BrokerSubmitResult =
  | { ok: true; recoveryPhrase: readonly string[] | null }
  | { ok: false };

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

  const submitOperator = useCallback(async (form: OperatorSubmitForm): Promise<boolean> => {
    dispatch({ type: "OPERATOR_SUBMIT_START" });
    try {
      const { operator } = await postSetup(
        form.username,
        form.password,
        form.setupToken.trim() === "" ? null : form.setupToken.trim(),
      );
      dispatch({ type: "OPERATOR_SUBMIT_SUCCESS", operator });
      return true;
    } catch {
      // #98: never leak err.message. Reducer sets GENERIC_ERROR unconditionally.
      dispatch({ type: "OPERATOR_SUBMIT_ERROR" });
      return false;
    }
  }, []);

  const submitBroker = useCallback(
    async (form: BrokerSubmitForm): Promise<BrokerSubmitResult> => {
      // Snapshot mode before save so we can decide whether to fire the
      // first-run universe-sync bootstrap (only on first-time create).
      const derived = deriveCredentialSetMode(state.credRows);
      const mode = derived.mode;
      const missingLabel = derived.missingLabel;
      const wasCreate = mode === "create";

      dispatch({ type: "BROKER_SUBMIT_START" });
      try {
        let phrase: readonly string[] | null = null;

        // Save api_key if needed (Create OR Repair with api_key missing).
        if (mode === "create" || (mode === "repair" && missingLabel === "api_key")) {
          const response = await createBrokerCredential({
            provider: "etoro",
            label: "api_key",
            environment: ENVIRONMENT,
            secret: form.apiKey,
          });
          if (response.recovery_phrase != null && response.recovery_phrase.length > 0) {
            phrase = response.recovery_phrase;
          }
        }

        // Save user_key if needed (Create OR Repair with user_key missing).
        if (mode === "create" || (mode === "repair" && missingLabel === "user_key")) {
          await createBrokerCredential({
            provider: "etoro",
            label: "user_key",
            environment: ENVIRONMENT,
            secret: form.userKey,
          });
        }

        const rows = await listBrokerCredentials();
        dispatch({ type: "BROKER_SUBMIT_SUCCESS", rows });

        // First-run bootstrap: fire-and-forget universe sync, only on
        // first-time create (not Repair). Matches SetupPage.tsx:213-219.
        if (wasCreate) {
          void runJob("nightly_universe_sync").catch(() => {});
        }

        return { ok: true, recoveryPhrase: phrase };
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

  const skipBroker = useCallback((): void => {
    onComplete();
  }, [onComplete]);

  const completeWizard = useCallback((): void => {
    onComplete();
  }, [onComplete]);

  const validateCredentials = useCallback(async (form: BrokerSubmitForm): Promise<void> => {
    dispatch({ type: "VALIDATION_START" });
    try {
      const result = await validateBrokerCredential({
        api_key: form.apiKey,
        user_key: form.userKey,
        environment: ENVIRONMENT,
      });
      dispatch({ type: "VALIDATION_SUCCESS", result });
    } catch {
      dispatch({
        type: "VALIDATION_ERROR",
        error: "Could not reach the validation endpoint.",
      });
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

- [ ] **Step 4: Run — expect reducer + classifier tests PASS**

```bash
pnpm --dir frontend exec vitest run src/pages/useSetupWizard.test.ts
```

Expected: all reducer + classifier tests PASS.

- [ ] **Step 5: Typecheck**

```bash
pnpm --dir frontend typecheck
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/pages/useSetupWizard.ts frontend/src/pages/useSetupWizard.test.ts
git commit -m "feat(#327): extract useSetupWizard hook + wizardReducer + classifyBrokerSaveError"
```

---

## Task 4: Hook-level tests (`renderHook`)

Append to `useSetupWizard.test.ts`. Cover the #98 GENERIC_ERROR contract + first-time-create runJob firing + partial-failure credRows fallback.

- [ ] **Step 1: Append hook tests**

Add to end of `frontend/src/pages/useSetupWizard.test.ts`:

```ts
import { act, renderHook, waitFor } from "@testing-library/react";
import { vi } from "vitest";

vi.mock("@/api/auth");
vi.mock("@/api/brokerCredentials");
vi.mock("@/api/jobs");

describe("useSetupWizard (hook)", () => {
  const onComplete = vi.fn();

  it("submitOperator maps any fetch failure to GENERIC_ERROR (not err.message)", async () => {
    const { postSetup } = await import("@/api/auth");
    vi.mocked(postSetup).mockRejectedValue(new Error("Leaky backend detail"));

    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    await act(async () => {
      await result.current.submitOperator({ username: "u", password: "p", setupToken: "" });
    });
    expect(result.current.state.operatorError).toBe(GENERIC_ERROR);
    expect(result.current.state.operatorError).not.toContain("Leaky backend detail");
    expect(result.current.state.step).toBe("operator");
  });

  it("submitBroker in create-mode fires runJob(nightly_universe_sync) fire-and-forget", async () => {
    const { createBrokerCredential, listBrokerCredentials } = await import("@/api/brokerCredentials");
    const { runJob } = await import("@/api/jobs");
    vi.mocked(createBrokerCredential).mockResolvedValue({
      credential: ROW_API,
      recovery_phrase: null,
    });
    vi.mocked(listBrokerCredentials).mockResolvedValue([ROW_API, ROW_USER]);
    vi.mocked(runJob).mockResolvedValue(undefined);

    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    // state.credRows === null → mode === 'create' → wasCreate=true
    await act(async () => {
      await result.current.submitBroker({ apiKey: "a", userKey: "u" });
    });
    expect(runJob).toHaveBeenCalledWith("nightly_universe_sync");
  });

  it("submitBroker in repair-mode does NOT fire runJob", async () => {
    const { createBrokerCredential, listBrokerCredentials } = await import("@/api/brokerCredentials");
    const { runJob } = await import("@/api/jobs");
    vi.mocked(createBrokerCredential).mockResolvedValue({
      credential: ROW_USER,
      recovery_phrase: null,
    });
    vi.mocked(listBrokerCredentials).mockResolvedValue([ROW_API, ROW_USER]);
    vi.mocked(runJob).mockResolvedValue(undefined);

    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    // Seed credRows with api_key present → mode='repair', missingLabel='user_key'
    await act(async () => {
      await result.current.loadCredentials();
    });
    // Mock now returns the post-save list with BOTH keys for the next call.
    vi.mocked(listBrokerCredentials).mockResolvedValue([ROW_API, ROW_USER]);
    // Actually: we need credRows = [ROW_API] only. Override:
    vi.mocked(listBrokerCredentials).mockResolvedValueOnce([ROW_API]);
    await act(async () => {
      await result.current.loadCredentials();
    });
    expect(result.current.state.credRows).toEqual([ROW_API]);

    vi.mocked(listBrokerCredentials).mockResolvedValue([ROW_API, ROW_USER]);
    await act(async () => {
      await result.current.submitBroker({ apiKey: "a", userKey: "u" });
    });
    expect(runJob).not.toHaveBeenCalled();
  });

  it("submitBroker failure + listBrokerCredentials also failing leaves credRows=null", async () => {
    const { createBrokerCredential, listBrokerCredentials } = await import("@/api/brokerCredentials");
    vi.mocked(createBrokerCredential).mockRejectedValue(new ApiError(500, "boom"));
    vi.mocked(listBrokerCredentials).mockRejectedValue(new Error("list boom"));

    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    await act(async () => {
      await result.current.submitBroker({ apiKey: "a", userKey: "u" });
    });
    expect(result.current.state.brokerError).toBe("Could not save credential.");
    expect(result.current.state.credRows).toBeNull();
  });
});
```

- [ ] **Step 2: Run**

```bash
pnpm --dir frontend exec vitest run src/pages/useSetupWizard.test.ts
```

Expected: all reducer + classifier + hook tests PASS (total ~20-25 tests).

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/useSetupWizard.test.ts
git commit -m "test(#327): useSetupWizard hook integration tests — GENERIC_ERROR + runJob + partial-failure"
```

---

## Task 5: Refactor `SetupPage.tsx` to consume the hook

Preserve every user-visible behaviour. Component keeps form-field `useState`; hook owns wizard state.

- [ ] **Step 1: Read the current `SetupPage.tsx` + identify preserved behaviours**

```bash
cat frontend/src/pages/SetupPage.tsx | head -250
```

Behaviours to preserve:
- Phrase modal shown AFTER both credential saves durable (not mid-save).
- `completeWizard()` runs `markAuthenticated(pendingOperator)` + `navigate("/", { replace: true })`.
- Clear stale validation result when brokerApiKey / brokerUserKey / mode changes (existing `useEffect` at lines 117-120).
- useEffect on mount that loads credentials when step transitions to "broker".

- [ ] **Step 2: Replace the `SetupPage` body**

Modify `frontend/src/pages/SetupPage.tsx`. Delete the 13 `useState` declarations + `refreshCredentials` callback + `completeWizard` function + `handleOperatorSubmit` + `handleTestConnection` + `handleBrokerSubmit` + `handleSkipBroker` functions. Replace with hook consumption.

Keep:
- `const GENERIC_ERROR` — change to `import { GENERIC_ERROR } from "@/pages/setupErrorMessages"`.
- Form-field `useState` (username, password, setupToken, brokerApiKey, brokerUserKey) — stay local.
- Phrase modal hook (`useRecoveryPhraseModal`).
- Session + navigation `useEffect`.
- Stale-validation clear `useEffect` (rewrite to watch `[brokerApiKey, brokerUserKey, mode]` — mode derived from `state.credRows`).

New component body:

```tsx
import { useCallback, useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { useRecoveryPhraseModal } from "@/components/security/RecoveryPhraseModal";
import { deriveCredentialSetMode } from "@/lib/credentialSetMode";
import { useSession } from "@/lib/session";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import { useSetupWizard } from "@/pages/useSetupWizard";

// ... existing imports for SetupLayout, ValidationSummary, etc.

const MIN_PASSWORD_LEN = 12;
const MIN_SECRET_LEN = 4;

export function SetupPage(): JSX.Element {
  const { status, markAuthenticated } = useSession();
  const navigate = useNavigate();

  const completeWizard = useCallback(() => {
    if (wizard.state.pendingOperator !== null) {
      markAuthenticated(wizard.state.pendingOperator);
    }
    navigate("/", { replace: true });
    // NB: wizard is declared below; this callback is defined after useSetupWizard
    //     in the actual file order (see Step 3 for complete block).
  }, [markAuthenticated, navigate]);  // wizard.state read via closure

  const wizard = useSetupWizard({ onComplete: completeWizard });

  // Step-1 form state.
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [setupToken, setSetupToken] = useState("");

  // Step-2 form state.
  const [brokerApiKey, setBrokerApiKey] = useState("");
  const [brokerUserKey, setBrokerUserKey] = useState("");

  const derived = deriveCredentialSetMode(wizard.state.credRows);
  const mode = derived.mode;
  const missingLabel = derived.missingLabel;

  // ... rest of SetupPage — use wizard.state.* for error/submitting/etc.,
  //     call wizard.submitOperator / wizard.submitBroker / wizard.skipBroker /
  //     wizard.validateCredentials / wizard.loadCredentials.
}
```

Note the forward-reference issue: `completeWizard` needs `wizard.state.pendingOperator`, but `wizard` is initialized with `completeWizard` as a dep. Resolve by using a ref or restructuring:

```tsx
// Use ref so completeWizard can read the latest pendingOperator
// without re-binding the callback (which would re-init the hook).
const pendingOperatorRef = useRef<Operator | null>(null);

const completeWizard = useCallback(() => {
  if (pendingOperatorRef.current !== null) {
    markAuthenticated(pendingOperatorRef.current);
  }
  navigate("/", { replace: true });
}, [markAuthenticated, navigate]);

const wizard = useSetupWizard({ onComplete: completeWizard });

// Sync ref whenever the hook's state advances.
useEffect(() => {
  pendingOperatorRef.current = wizard.state.pendingOperator;
}, [wizard.state.pendingOperator]);
```

Add `import { useRef } from "react"` at the top.

- [ ] **Step 3: Write the full refactored `SetupPage.tsx`**

The new file is 250-300 lines (down from 450). Structure:

```tsx
// imports
// constants (MIN_PASSWORD_LEN, MIN_SECRET_LEN)
// SetupPage() {
//   session + navigate
//   pendingOperatorRef + completeWizard callback (via ref to avoid dep cycle)
//   wizard = useSetupWizard({ onComplete: completeWizard })
//   form-field state (username, password, setupToken, brokerApiKey, brokerUserKey)
//   derived = deriveCredentialSetMode(wizard.state.credRows)
//   phraseModal = useRecoveryPhraseModal({ onClose: completeWizard })
//   session-redirect useEffect
//   load-creds-on-step-broker useEffect
//   stale-validation-clear useEffect (watch brokerApiKey, brokerUserKey, mode)
//   handleOperatorSubmit: wraps wizard.submitOperator
//   handleTestConnection: wraps wizard.validateCredentials
//   handleBrokerSubmit: wraps wizard.submitBroker, opens modal or completes
//   handleSkipBroker: confirm-dialog gate, then wizard.skipBroker()
//   render — same JSX, read from wizard.state.* and form-field state
// }
```

Complete refactored body (follows the structure above, preserves exact JSX + behaviours; reads state from `wizard.state.*` instead of local `useState`s). Exact line-by-line diff is long but mechanical.

Key behavioural mappings:

| Before (local state) | After (hook) |
| --- | --- |
| `step` | `wizard.state.step` |
| `pendingOperator` | `wizard.state.pendingOperator` |
| `submitting` | `wizard.state.operatorSubmitting` |
| `error` | `wizard.state.operatorError` |
| `brokerSubmitting` | `wizard.state.brokerSubmitting` |
| `brokerError` | `wizard.state.brokerError` |
| `credRows` | `wizard.state.credRows` |
| `validating` | `wizard.state.validating` |
| `validationResult` | `wizard.state.validation` |
| `validationError` | `wizard.state.validationError` |
| `setError(GENERIC_ERROR)` (in catch) | `wizard.submitOperator(...)` — reducer does it |
| `refreshCredentials()` | `wizard.loadCredentials()` |
| `completeWizard()` in submit success | `wizard.submitBroker(...)` success path returns `recoveryPhrase`; if null, call `completeWizard()` ourselves; if non-null, open modal (modal's onClose fires completeWizard). |

`handleBrokerSubmit`:

```tsx
async function handleBrokerSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
  e.preventDefault();
  const result = await wizard.submitBroker({ apiKey: brokerApiKey, userKey: brokerUserKey });
  if (!result.ok) return;
  setBrokerApiKey("");
  setBrokerUserKey("");
  if (result.recoveryPhrase !== null) {
    phraseModal.open(result.recoveryPhrase);
    return;
  }
  completeWizard();
}
```

`handleSkipBroker` keeps its confirm-dialog gate if the current file has one; if it's just `completeWizard()`, replace with `wizard.skipBroker()`.

- [ ] **Step 4: Typecheck**

```bash
pnpm --dir frontend typecheck
```

Expected: PASS. Fix any type errors from prop drift.

- [ ] **Step 5: Run existing SetupPage tests — expect most to PASS (some will fail until Task 6 trim)**

```bash
pnpm --dir frontend exec vitest run src/pages/SetupPage.test.tsx
```

Expected: the 6 tests we're keeping should PASS (the refactor preserves every user-visible behaviour). The ~9 tests we're dropping may still be in the file — they may pass or fail depending on what they asserted. That's fine; Task 6 trims them.

Note: if any kept test fails, read the failure + fix the refactor. The refactor must preserve behaviour exactly. Common pitfalls:
- `markAuthenticated` called twice (once from completeWizard, once from phraseModal.onClose) — must fire exactly once. The ref pattern keeps a single call.
- `wizard.state.credRows` being null on first render → mode='create' → render branch consistent.

- [ ] **Step 6: Full frontend suite**

```bash
pnpm --dir frontend test
```

Expected: full suite green. If any test outside SetupPage.test.tsx fails, the refactor drifted — fix before proceeding.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/pages/SetupPage.tsx
git commit -m "refactor(#327): SetupPage consumes useSetupWizard hook"
```

---

## Task 6: Trim `SetupPage.test.tsx` to 6 integration tests

Keep: happy-path save, happy-path skip, confirm-cancel, phrase-modal, repair-mode, already-complete. Drop the other ~6.

- [ ] **Step 1: Read current test file**

```bash
cat frontend/src/pages/SetupPage.test.tsx
```

Identify the ~12 tests. Decide which are kept.

Keep (6):
1. `"Skip for now" completes the wizard with no broker call` (happy-path skip).
2. `creates both api_key and user_key rows on save` (happy-path save — include the universe-sync-fires assertion from the bootstrap-trigger cluster).
3. `routes Cancel through confirm-cancel gate, then 'Close anyway' completes wizard` (confirm-cancel).
4. `opens the phrase modal when the first create response carries a recovery_phrase` + `completes the wizard after the operator passes the challenge` — merge into one test that covers the full phrase-modal branch.
5. `enters Repair mode and keeps Skip available when second save fails` (repair-mode; adjust assertions to the new hook shape).
6. A new test: `already-complete branch renders Continue + clicking it completes wizard` (add if not present; uses seeded `listBrokerCredentials` response with both api_key + user_key).

Drop (6-7):
- `surfaces the generic error and stays on step 1 when /auth/setup fails` (covered by hook test).
- `advances to step 2 on success WITHOUT calling markAuthenticated yet` (covered by happy-path save).
- `fires nightly_universe_sync after both credentials are saved` → merged into happy-path save.
- `does not fire universe sync when operator skips credentials` → merged into happy-path skip.
- `swallows universe sync errors silently` → covered by hook test (fire-and-forget pattern).
- `completes wizard with NO phrase modal when response has no recovery_phrase` → covered by happy-path save (default mock has no phrase).

- [ ] **Step 2: Rewrite `SetupPage.test.tsx`**

New file has 6 top-level `it(...)` blocks grouped by `describe("SetupPage — integration")`. Shared mock setup at the top. Each test uses `render(<SetupPage />)` + `fireEvent` / `userEvent`. Mocks are the same fetchers the hook uses (`postSetup`, `createBrokerCredential`, `listBrokerCredentials`, `validateBrokerCredential`, `runJob`). The component's `completeWizard` still fires `markAuthenticated` + `navigate`, so tests assert against those mocks.

Exact rewrite is mechanical — preserve fixture helpers, update mock-import list to include `runJob` + `listBrokerCredentials`. Hard-code the 6 tests per the list above; delete the rest. File size drops from ~412 lines to ~200.

- [ ] **Step 3: Run**

```bash
pnpm --dir frontend test:integration
```

Expected: 6 tests PASS.

- [ ] **Step 4: Full frontend suite**

```bash
pnpm --dir frontend test
```

Expected: full suite green.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/SetupPage.test.tsx
git commit -m "test(#327): trim SetupPage.test.tsx to 6 integration tests"
```

---

## Task 7: CLAUDE.md pre-push block update

- [ ] **Step 1: Read current pre-push block**

```bash
grep -n "pnpm --dir frontend" .claude/CLAUDE.md
```

Find the block with `pnpm --dir frontend typecheck` + `pnpm --dir frontend test`.

- [ ] **Step 2: Edit**

Replace `pnpm --dir frontend test` with `pnpm --dir frontend test:unit` inside the `If the PR touches frontend/, also run:` block. Add a note below:

```markdown
If the PR touches `frontend/`, also run:

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test:unit
```

Both must pass.

**Note:** `test:unit` excludes heavy integration tests under `src/pages/SetupPage.test.tsx`. CI runs the full `test` script on push — integration tests still gate merge. Run `pnpm --dir frontend test` locally when explicitly debugging integration coverage.
```

- [ ] **Step 3: Verify**

Render the file in a markdown preview (or just eyeball the diff). Confirm the code-fence + note structure match surrounding sections.

- [ ] **Step 4: Commit**

```bash
git add .claude/CLAUDE.md
git commit -m "docs(#327): CLAUDE.md pre-push points to test:unit (CI still runs full test)"
```

---

## Task 8: Pre-push gates + Codex checkpoint 2 + push + PR

- [ ] **Step 1: Backend gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. (Backend unchanged by this PR; must still be green.)

- [ ] **Step 2: Frontend gates**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Both must pass.

- [ ] **Step 3: Pre-push fast path sanity**

```bash
pnpm --dir frontend test:unit
```

Expected: fewer tests than `test`; green; noticeably faster.

- [ ] **Step 4: Codex checkpoint 2 — diff review**

```bash
git diff main...HEAD > /tmp/pr327b_diff.txt
codex.cmd exec "Checkpoint 2 diff review for PR (#327 frontend subset). Diff at /tmp/pr327b_diff.txt. Spec at d:/Repos/eBull/docs/superpowers/specs/2026-04-22-frontend-test-speedup.md.

Focus: useSetupWizard hook preserves SetupPage.tsx behavioural contracts (GENERIC_ERROR mapping, fixed broker-save error strings, runJob fire-and-forget on first-time create only, credRows=null fallback on list error, phrase-modal-before-complete ordering). SetupPage.tsx refactor preserves JSX + ref-based completeWizard pattern (no duplicate markAuthenticated). Integration test trim preserves all user-visible coverage. Reply terse."
```

Fix any blocking findings before pushing.

- [ ] **Step 5: Push + open PR**

```bash
git push -u origin fix/327b-frontend-test-speedup
gh pr create --title "fix(#327): frontend test speedup — vitest cap + SetupPage reducer + script split" --body "$(cat <<'EOF'
## What

- `vitest.config.ts` — pin `pool: "forks"` + `maxForks: 2`. v2 defaults to forks already; pinning prevents silent regression + the cap halves sustained CPU.
- New `frontend/src/pages/setupErrorMessages.ts` — shared `GENERIC_ERROR` constant.
- New `frontend/src/pages/useSetupWizard.ts` — pure `wizardReducer`, `classifyBrokerSaveError` helper, `useSetupWizard` hook. Extracts wizard state machine from `SetupPage.tsx`.
- New `frontend/src/pages/useSetupWizard.test.ts` — reducer unit tests + classifier unit tests + `renderHook` integration tests.
- `SetupPage.tsx` consumes the hook; keeps form-field `useState` local; `completeWizard` uses a ref to avoid dep-cycle on the hook's `onComplete` option.
- `SetupPage.test.tsx` trimmed to 6 integration tests (happy-path save, happy-path skip, confirm-cancel, phrase-modal, repair-mode, already-complete).
- `package.json` — `test:unit` / `test:integration` / `test` script split.
- `.claude/CLAUDE.md` pre-push block points to `test:unit`; CI still runs full `test`.

## Why

Closes frontend items from #327. SetupPage had ~12 tests at 1000-1500ms each because every transition did full render + fetch mock. Pure reducer covers state transitions at <50ms total; hook-level `renderHook` covers fetch wiring; 6 kept integration tests cover irreducible multi-step UX.

Backend pytest-xdist from #327 deferred to follow-up (needs `ebull_test` audit).

## Test plan

- Reducer + classifier unit tests: ~20 PASS <500ms.
- Hook integration tests: 4 PASS.
- SetupPage integration tests: 6 PASS.
- Full suite green (frontend + backend).
- Local `test:unit` noticeably faster than `test` (excludes heavy file).

## Called out

- #98 non-leaky-error contract preserved: `OPERATOR_SUBMIT_ERROR` carries no payload; reducer hard-codes `GENERIC_ERROR`.
- Derived broker-mode stays a pure selector over `credRows` — not a reducer field.
- `completeWizard` uses `pendingOperatorRef` to avoid re-binding the hook's `onComplete` on every state change.
- CI runs full `test`; pre-push local runs `test:unit`.
- pytest-xdist deferred per prior Codex ckpt 1 finding on #404 predecessor (`ebull_test` audit + collection-bootstrap idempotency).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Poll review + CI**

```bash
gh pr checks --watch
gh pr view --comments
```

Resolve every comment as FIXED / DEFERRED / REBUTTED. Merge after APPROVE on latest commit + CI green.

---

## Self-review

**1. Spec coverage:**

- Vitest pin + cap → Task 1.
- Script split → Task 1.
- `setupErrorMessages.ts` → Task 2.
- Reducer + hook + classifier + unit tests → Task 3.
- Hook integration tests (`renderHook`) → Task 4.
- SetupPage refactor to consume hook → Task 5.
- Integration test trim to 6 → Task 6.
- CLAUDE.md pre-push update → Task 7.
- Gates + Codex + PR → Task 8.

All spec sections covered.

**2. Placeholder scan:** no "TBD", "TODO", "implement later". Code steps show concrete diffs; test steps show concrete test bodies.

**3. Type consistency:**

- `WizardStep = "operator" | "broker"` — no `"done"` — consistent everywhere.
- `OperatorSubmitForm`, `BrokerSubmitForm`, `BrokerSubmitResult` shapes stable across hook sig + SetupPage consumer + tests.
- `classifyBrokerSaveError` signature: `(unknown) → string`. Matches test + hook.
- `onComplete: () => void` callback shape consistent.
- `deriveCredentialSetMode(state.credRows)` — pure selector, matches `@/lib/credentialSetMode`.
- `pendingOperatorRef.current` pattern avoids re-binding `onComplete` on every state change.

**4. Known risks:**

- Task 5 refactor is the largest change (~300 lines modified). The ref-based `completeWizard` pattern needs care to avoid double-firing `markAuthenticated` — covered by the existing test `'Close anyway' completes wizard` which asserts call count.
- Vitest v2.1.8 supports `maxForks` per docs but any prior config override in inherited `vite.config.ts` could conflict. Task 1 step 3 runs full suite to catch any issue.
