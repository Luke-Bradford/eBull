import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Operator } from "@/api/auth";
import type {
  BrokerCredentialView,
  ValidateCredentialResponse,
} from "@/api/brokerCredentials";
import { ApiError } from "@/api/client";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import {
  classifyBrokerSaveError,
  initialWizardState,
  useSetupWizard,
  wizardReducer,
  type WizardState,
} from "@/pages/useSetupWizard";

vi.mock("@/api/auth");
vi.mock("@/api/brokerCredentials");
vi.mock("@/api/jobs");

const OP: Operator = { id: "op-1", username: "test" };
const ROW_API: BrokerCredentialView = {
  id: "c-1",
  provider: "etoro",
  label: "api_key",
  environment: "demo",
  last_four: "abcd",
  created_at: "2026-04-22T00:00:00Z",
  last_used_at: null,
  revoked_at: null,
};
const ROW_USER: BrokerCredentialView = { ...ROW_API, id: "c-2", label: "user_key" };
const VAL_OK: ValidateCredentialResponse = {
  auth_valid: true,
  identity: null,
  environment: "demo",
  env_valid: true,
  env_check: "ok",
  note: "fine",
};

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
      {
        type: "BROKER_SUBMIT_ERROR",
        error: "Invalid API key or user key value.",
        rows: [ROW_API],
      },
    );
    expect(s.brokerError).toBe("Invalid API key or user key value.");
    expect(s.credRows).toEqual([ROW_API]);
    expect(s.brokerSubmitting).toBe(false);
    expect(s.step).toBe("operator"); // no step advance
  });

  it("BROKER_SUBMIT_ERROR with rows=null leaves credRows=null (create-mode fallback)", () => {
    const s = wizardReducer(
      { ...initialWizardState, brokerSubmitting: true },
      {
        type: "BROKER_SUBMIT_ERROR",
        error: "Could not save credential.",
        rows: null,
      },
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
    expect(classifyBrokerSaveError(new ApiError(500, "boom"))).toBe(
      "Could not save credential.",
    );
  });
  it("non-ApiError Error → 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError(new Error("random"))).toBe("Could not save credential.");
  });
  it("non-Error value → 'Could not save credential.'", () => {
    expect(classifyBrokerSaveError("plain string")).toBe("Could not save credential.");
  });
});

describe("useSetupWizard (hook)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("submitOperator maps any fetch failure to GENERIC_ERROR (not err.message)", async () => {
    const { postSetup } = await import("@/api/auth");
    vi.mocked(postSetup).mockRejectedValue(new Error("Leaky backend detail"));

    const onComplete = vi.fn();
    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    await act(async () => {
      await result.current.submitOperator({ username: "u", password: "p", setupToken: "" });
    });
    expect(result.current.state.operatorError).toBe(GENERIC_ERROR);
    expect(result.current.state.operatorError).not.toContain("Leaky backend detail");
    expect(result.current.state.step).toBe("operator");
    expect(onComplete).not.toHaveBeenCalled();
  });

  it("submitOperator success advances step to broker + stores operator", async () => {
    const { postSetup } = await import("@/api/auth");
    vi.mocked(postSetup).mockResolvedValue({ operator: OP });

    const onComplete = vi.fn();
    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    await act(async () => {
      await result.current.submitOperator({ username: "u", password: "p", setupToken: "" });
    });
    expect(result.current.state.step).toBe("broker");
    expect(result.current.state.pendingOperator).toEqual(OP);
    expect(onComplete).not.toHaveBeenCalled();
  });

  it("submitBroker in create-mode fires runJob(nightly_universe_sync) fire-and-forget", async () => {
    const { createBrokerCredential, listBrokerCredentials } = await import(
      "@/api/brokerCredentials"
    );
    const { runJob } = await import("@/api/jobs");
    vi.mocked(createBrokerCredential).mockResolvedValue({
      credential: ROW_API,
      recovery_phrase: null,
    });
    vi.mocked(listBrokerCredentials).mockResolvedValue([ROW_API, ROW_USER]);
    vi.mocked(runJob).mockResolvedValue(undefined);

    const { result } = renderHook(() => useSetupWizard({ onComplete: vi.fn() }));
    // state.credRows === null → mode === 'create' → wasCreate=true
    await act(async () => {
      await result.current.submitBroker({ apiKey: "a", userKey: "u" });
    });
    expect(runJob).toHaveBeenCalledWith("nightly_universe_sync");
    expect(result.current.state.brokerError).toBeNull();
  });

  it("submitBroker in repair-mode does NOT fire runJob", async () => {
    const { createBrokerCredential, listBrokerCredentials } = await import(
      "@/api/brokerCredentials"
    );
    const { runJob } = await import("@/api/jobs");
    vi.mocked(createBrokerCredential).mockResolvedValue({
      credential: ROW_USER,
      recovery_phrase: null,
    });
    // Seed hook with credRows = [ROW_API] → mode='repair', missingLabel='user_key'
    vi.mocked(listBrokerCredentials).mockResolvedValueOnce([ROW_API]);

    const { result } = renderHook(() => useSetupWizard({ onComplete: vi.fn() }));
    await act(async () => {
      await result.current.loadCredentials();
    });
    expect(result.current.state.credRows).toEqual([ROW_API]);

    // Subsequent list call (post-save) returns both rows.
    vi.mocked(listBrokerCredentials).mockResolvedValue([ROW_API, ROW_USER]);
    await act(async () => {
      await result.current.submitBroker({ apiKey: "a", userKey: "u" });
    });
    expect(runJob).not.toHaveBeenCalled();
  });

  it("submitBroker failure with listBrokerCredentials also failing leaves credRows=null", async () => {
    const { createBrokerCredential, listBrokerCredentials } = await import(
      "@/api/brokerCredentials"
    );
    vi.mocked(createBrokerCredential).mockRejectedValue(new ApiError(500, "boom"));
    vi.mocked(listBrokerCredentials).mockRejectedValue(new Error("list boom"));

    const { result } = renderHook(() => useSetupWizard({ onComplete: vi.fn() }));
    await act(async () => {
      await result.current.submitBroker({ apiKey: "a", userKey: "u" });
    });
    expect(result.current.state.brokerError).toBe("Could not save credential.");
    expect(result.current.state.credRows).toBeNull();
  });

  it("skipBroker invokes onComplete without dispatching state transitions", async () => {
    const onComplete = vi.fn();
    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    act(() => {
      result.current.skipBroker();
    });
    expect(onComplete).toHaveBeenCalledOnce();
    expect(result.current.state.step).toBe("operator");
  });

  it("completeWizard invokes onComplete without dispatching state transitions", async () => {
    const onComplete = vi.fn();
    const { result } = renderHook(() => useSetupWizard({ onComplete }));
    act(() => {
      result.current.completeWizard();
    });
    expect(onComplete).toHaveBeenCalledOnce();
  });
});
