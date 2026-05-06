/**
 * useSetupWizard — state machine + fetch dispatcher for SetupPage.
 *
 * Post-amendment 2026-05-07 (#971): the wizard is single-step. The
 * broker-credential step has been removed; operators add eToro keys in
 * Settings after first-run setup.
 *
 * Reducer covers operator-submit fetch-status flags. Form-field inputs
 * (username, password, setupToken) stay as component-level useState in
 * SetupPage.tsx — field churn does not belong in the state machine.
 *
 * Completion is a side effect (markAuthenticated + navigate) driven by
 * the component via the `onComplete` callback option.
 */
import { useCallback, useReducer } from "react";

import type { Operator } from "@/api/auth";
import { postSetup } from "@/api/auth";
import { ApiError } from "@/api/client";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";

// ---------------------------------------------------------------------------
// State + actions
// ---------------------------------------------------------------------------

export type WizardState = {
  pendingOperator: Operator | null;
  operatorSubmitting: boolean;
  operatorError: string | null;
};

export const initialWizardState: WizardState = {
  pendingOperator: null,
  operatorSubmitting: false,
  operatorError: null,
};

export type WizardAction =
  | { type: "OPERATOR_SUBMIT_START" }
  | { type: "OPERATOR_SUBMIT_SUCCESS"; operator: Operator }
  | { type: "OPERATOR_SUBMIT_ERROR" };

export function wizardReducer(state: WizardState, action: WizardAction): WizardState {
  switch (action.type) {
    case "OPERATOR_SUBMIT_START":
      return { ...state, operatorSubmitting: true, operatorError: null };
    case "OPERATOR_SUBMIT_SUCCESS":
      return {
        ...state,
        pendingOperator: action.operator,
        operatorSubmitting: false,
        operatorError: null,
      };
    case "OPERATOR_SUBMIT_ERROR":
      // #98 non-leaky contract: never propagate err.message into state.
      return { ...state, operatorSubmitting: false, operatorError: GENERIC_ERROR };
    default: {
      const _exhaustive: never = action;
      return _exhaustive;
    }
  }
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export interface UseSetupWizardOptions {
  /**
   * Called after a successful operator-create. The component reads
   * `state.pendingOperator` from the latest state snapshot to drive
   * `markAuthenticated` + navigation.
   */
  onComplete: () => void;
}

export interface UseSetupWizardResult {
  state: WizardState;
  submitOperator: (input: {
    username: string;
    password: string;
    setupToken: string;
  }) => Promise<void>;
}

export function useSetupWizard(opts: UseSetupWizardOptions): UseSetupWizardResult {
  const [state, dispatch] = useReducer(wizardReducer, initialWizardState);

  const submitOperator = useCallback(
    async ({
      username,
      password,
      setupToken,
    }: {
      username: string;
      password: string;
      setupToken: string;
    }): Promise<void> => {
      dispatch({ type: "OPERATOR_SUBMIT_START" });
      try {
        const { operator } = await postSetup(
          username,
          password,
          setupToken === "" ? null : setupToken,
        );
        dispatch({ type: "OPERATOR_SUBMIT_SUCCESS", operator });
        // Defer onComplete to next tick so the dispatched state lands
        // before the component reads pendingOperator.
        queueMicrotask(() => opts.onComplete());
      } catch (err) {
        if (err instanceof ApiError) {
          dispatch({ type: "OPERATOR_SUBMIT_ERROR" });
          return;
        }
        dispatch({ type: "OPERATOR_SUBMIT_ERROR" });
      }
    },
    [opts],
  );

  return { state, submitOperator };
}
