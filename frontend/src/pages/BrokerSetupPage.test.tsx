import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  createBrokerCredential,
  validateBrokerCredential,
  validateStoredCredentials,
} from "@/api/brokerCredentials";
import { BrokerSetupPage } from "@/pages/BrokerSetupPage";

vi.mock("@/api/brokerCredentials", async () => {
  const actual = await vi.importActual<
    typeof import("@/api/brokerCredentials")
  >("@/api/brokerCredentials");
  return {
    ...actual,
    validateBrokerCredential: vi.fn(),
    createBrokerCredential: vi.fn(),
    validateStoredCredentials: vi.fn(),
    revokeBrokerCredential: vi.fn(),
  };
});

const refreshBootstrapState = vi.fn();
vi.mock("@/lib/session", () => ({
  useSession: () => ({
    status: "authenticated",
    operator: null,
    bootstrapState: { needs_broker_credentials: true },
    refreshBootstrapState,
    logout: vi.fn(),
  }),
}));

const mockValidate = vi.mocked(validateBrokerCredential);
const mockCreate = vi.mocked(createBrokerCredential);
const mockValidateStored = vi.mocked(validateStoredCredentials);

beforeEach(() => {
  vi.clearAllMocks();
});

describe("BrokerSetupPage", () => {
  it("validates the STORED credentials after saving so they land VALID, not UNTESTED (#1453)", async () => {
    const user = userEvent.setup();
    mockValidate.mockResolvedValue({
      auth_valid: true,
      identity: null,
      env_valid: true,
      note: "ok",
    } as never);
    mockCreate
      .mockResolvedValueOnce({ credential: { id: "api-1" } } as never)
      .mockResolvedValueOnce({ credential: { id: "user-1" } } as never);
    mockValidateStored.mockResolvedValue({
      auth_valid: true,
      identity: null,
      env_valid: true,
      note: "ok",
    } as never);

    render(
      <MemoryRouter initialEntries={["/setup/broker"]}>
        <BrokerSetupPage />
      </MemoryRouter>,
    );

    await user.type(screen.getByLabelText(/Public key/i), "api-key-value");
    await user.type(screen.getByLabelText(/Private key/i), "user-key-value");
    await user.click(screen.getByRole("button", { name: /Save & continue/i }));

    // Both rows stored...
    await waitFor(() => expect(mockCreate).toHaveBeenCalledTimes(2));
    // ...then the canonical probe runs so health flips UNTESTED→VALID and
    // portfolio_sync is no longer gated (#1453).
    await waitFor(() => expect(mockValidateStored).toHaveBeenCalledTimes(1));
    // Ordering matters: validate-stored MUST run after both creates (the
    // rows must exist before the probe can record health on them).
    const lastCreateOrder = Math.max(
      ...mockCreate.mock.invocationCallOrder,
    );
    expect(mockValidateStored.mock.invocationCallOrder[0]).toBeGreaterThan(
      lastCreateOrder,
    );
  });

  it("does not strand the operator when validate-stored fails (best-effort)", async () => {
    const user = userEvent.setup();
    mockValidate.mockResolvedValue({
      auth_valid: true,
      identity: null,
      env_valid: true,
      note: "ok",
    } as never);
    mockCreate
      .mockResolvedValueOnce({ credential: { id: "api-1" } } as never)
      .mockResolvedValueOnce({ credential: { id: "user-1" } } as never);
    mockValidateStored.mockRejectedValue(new Error("network blip"));

    render(
      <MemoryRouter initialEntries={["/setup/broker"]}>
        <BrokerSetupPage />
      </MemoryRouter>,
    );

    await user.type(screen.getByLabelText(/Public key/i), "api-key-value");
    await user.type(screen.getByLabelText(/Private key/i), "user-key-value");
    await user.click(screen.getByRole("button", { name: /Save & continue/i }));

    // The rejecting probe must actually have been invoked — otherwise the
    // "swallow" assertion below would be vacuously true (it would pass even
    // if the validateStoredCredentials call were deleted entirely).
    await waitFor(() => expect(mockValidateStored).toHaveBeenCalledTimes(1));
    // Setup still completes: the gate is flipped and we navigate home even
    // though validate-stored threw.
    await waitFor(() => expect(refreshBootstrapState).toHaveBeenCalledTimes(1));
  });
});
