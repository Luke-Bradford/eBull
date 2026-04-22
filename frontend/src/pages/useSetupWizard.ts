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
import type {
  BrokerCredentialView,
  ValidateCredentialResponse,
} from "@/api/brokerCredentials";
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
      return {
        ...state,
        credRowsLoading: false,
        credRowsError: action.error,
        credRows: null,
      };
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
      return {
        ...state,
        validating: false,
        validation: action.result,
        validationError: null,
      };
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
  setupToken: string; // raw input; hook trims + coerces empty → null
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
      const trimmed = form.setupToken.trim();
      const { operator } = await postSetup(
        form.username,
        form.password,
        trimmed === "" ? null : trimmed,
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
