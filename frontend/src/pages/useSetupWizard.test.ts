/**
 * Tests for useSetupWizard (post-amendment 2026-05-07: single-step,
 * operator-only).
 */

import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as authApi from "@/api/auth";
import { ApiError } from "@/api/client";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import {
  initialWizardState,
  useSetupWizard,
  wizardReducer,
} from "@/pages/useSetupWizard";

vi.mock("@/api/auth");

const mockedAuth = vi.mocked(authApi);

beforeEach(() => {
  vi.clearAllMocks();
});

describe("wizardReducer", () => {
  it("OPERATOR_SUBMIT_START flips submitting + clears prior error", () => {
    const next = wizardReducer(
      { ...initialWizardState, operatorError: "stale" },
      { type: "OPERATOR_SUBMIT_START" },
    );
    expect(next.operatorSubmitting).toBe(true);
    expect(next.operatorError).toBeNull();
  });

  it("OPERATOR_SUBMIT_SUCCESS records pendingOperator + clears submit flag", () => {
    const op = { id: "abc", username: "alice" };
    const next = wizardReducer(initialWizardState, {
      type: "OPERATOR_SUBMIT_SUCCESS",
      operator: op,
    });
    expect(next.pendingOperator).toEqual(op);
    expect(next.operatorSubmitting).toBe(false);
    expect(next.operatorError).toBeNull();
  });

  it("OPERATOR_SUBMIT_ERROR sets generic non-leaky error", () => {
    const next = wizardReducer(
      { ...initialWizardState, operatorSubmitting: true },
      { type: "OPERATOR_SUBMIT_ERROR" },
    );
    expect(next.operatorSubmitting).toBe(false);
    expect(next.operatorError).toBe(GENERIC_ERROR);
  });
});

describe("useSetupWizard.submitOperator", () => {
  it("dispatches success on 2xx and triggers onComplete", async () => {
    const op = { id: "id-1", username: "alice" };
    mockedAuth.postSetup.mockResolvedValueOnce({ operator: op });
    const onComplete = vi.fn();
    const { result } = renderHook(() => useSetupWizard({ onComplete }));

    await act(async () => {
      await result.current.submitOperator({
        username: "alice",
        password: "12345678abcd",
        setupToken: "",
      });
    });

    expect(result.current.state.pendingOperator).toEqual(op);
    expect(result.current.state.operatorSubmitting).toBe(false);
    expect(result.current.state.operatorError).toBeNull();
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(onComplete).toHaveBeenCalledWith(op);
  });

  it("dispatches generic error on 4xx without leaking server message", async () => {
    mockedAuth.postSetup.mockRejectedValueOnce(
      new ApiError(401, "invalid setup token"),
    );
    const onComplete = vi.fn();
    const { result } = renderHook(() => useSetupWizard({ onComplete }));

    await act(async () => {
      await result.current.submitOperator({
        username: "alice",
        password: "12345678abcd",
        setupToken: "wrong",
      });
    });

    expect(result.current.state.operatorError).toBe(GENERIC_ERROR);
    expect(onComplete).not.toHaveBeenCalled();
  });

  it("trims empty setup token to null on the wire", async () => {
    mockedAuth.postSetup.mockResolvedValueOnce({
      operator: { id: "x", username: "y" },
    });
    const { result } = renderHook(() =>
      useSetupWizard({ onComplete: vi.fn() }),
    );
    await act(async () => {
      await result.current.submitOperator({
        username: "alice",
        password: "12345678abcd",
        setupToken: "",
      });
    });
    expect(mockedAuth.postSetup).toHaveBeenCalledWith("alice", "12345678abcd", null);
  });
});
