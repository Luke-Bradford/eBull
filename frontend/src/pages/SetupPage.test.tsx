/**
 * Tests for SetupPage (post-amendment 2026-05-07: single-step
 * operator-create form, broker-cred wizard step removed).
 *
 * Reducer + hook-level behaviour (GENERIC_ERROR mapping, postSetup
 * payload shape) is covered by useSetupWizard.test.ts.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { SetupPage } from "@/pages/SetupPage";
import { postSetup } from "@/api/auth";
import type { Operator } from "@/api/auth";
import { useSession } from "@/lib/session";

vi.mock("@/api/auth", async () => {
  const actual = await vi.importActual<typeof import("@/api/auth")>("@/api/auth");
  return {
    ...actual,
    postSetup: vi.fn(),
  };
});

const navigateMock = vi.fn();
vi.mock("react-router-dom", () => ({
  useNavigate: () => navigateMock,
}));

const markAuthenticatedMock = vi.fn();
const useSessionMock = vi.fn();
vi.mock("@/lib/session", () => ({
  useSession: () => useSessionMock(),
}));

const mockedPostSetup = vi.mocked(postSetup);

const OPERATOR: Operator = {
  id: "00000000-0000-0000-0000-000000000001",
  username: "alice",
};

beforeEach(() => {
  mockedPostSetup.mockReset();
  navigateMock.mockReset();
  markAuthenticatedMock.mockReset();
  useSessionMock.mockReset();
  useSessionMock.mockReturnValue({
    status: "needs_setup",
    operator: null,
    bootstrapState: { needs_setup: true, boot_state: "clean_install" },
    login: vi.fn(),
    logout: vi.fn(),
    markAuthenticated: markAuthenticatedMock,
    refreshBootstrapState: vi.fn(),
  } as unknown as ReturnType<typeof useSession>);
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("SetupPage — single step", () => {
  it("renders only the operator-create form (no broker step UI)", () => {
    render(<SetupPage />);
    expect(screen.getByLabelText(/Username/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Password/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Setup token/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Create operator/i }),
    ).toBeInTheDocument();
    // Broker step is gone: no API/User key fields, no Test connection,
    // no Skip-for-now.
    expect(screen.queryByLabelText(/API key/i)).toBeNull();
    expect(screen.queryByLabelText(/User key/i)).toBeNull();
    expect(screen.queryByRole("button", { name: /Test connection/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /Skip for now/i })).toBeNull();
  });

  it("happy path: creates operator, fires markAuthenticated, navigates home", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });

    render(<SetupPage />);
    await user.type(screen.getByLabelText(/Username/i), "alice");
    await user.type(screen.getByLabelText(/Password/i), "correct-horse-battery");
    await user.click(screen.getByRole("button", { name: /Create operator/i }));

    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });
});
