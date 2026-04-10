/**
 * Tests for SetupPage 2-step wizard (#122 / ADR-0003 Ticket 2c,
 * updated #139 PR D for two-key credential model).
 *
 * Scope:
 *   - Step 1 creates the operator (unchanged from #122).
 *   - Step 2 shows two-key form (API key + User key), no label field.
 *   - Two sequential createBrokerCredential calls on save.
 *   - "Skip for now" completes the wizard with no broker call.
 *   - Recovery phrase modal flow (first-save lazy-gen).
 *   - Partial-save recovery (first key saved, second failed → Repair).
 *   - markAuthenticated called exactly once, at wizard completion.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { SetupPage } from "@/pages/SetupPage";
import {
  type BrokerCredentialView,
  type CreateBrokerCredentialResponse,
  createBrokerCredential,
  listBrokerCredentials,
  validateBrokerCredential,
} from "@/api/brokerCredentials";
import { postSetup } from "@/api/auth";
import type { Operator } from "@/api/auth";
import { ApiError } from "@/api/client";
import { useSession } from "@/lib/session";

vi.mock("@/api/auth", async () => {
  const actual = await vi.importActual<typeof import("@/api/auth")>("@/api/auth");
  return {
    ...actual,
    postSetup: vi.fn(),
  };
});
vi.mock("@/api/brokerCredentials", () => ({
  createBrokerCredential: vi.fn(),
  listBrokerCredentials: vi.fn(),
  validateBrokerCredential: vi.fn(),
}));

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
const mockedCreate = vi.mocked(createBrokerCredential);
const mockedList = vi.mocked(listBrokerCredentials);
const mockedValidate = vi.mocked(validateBrokerCredential);

const OPERATOR: Operator = {
  id: "00000000-0000-0000-0000-000000000001",
  username: "alice",
};

const PHRASE: readonly string[] = [
  "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
  "golf", "hotel", "india", "juliet", "kilo", "lima",
  "mike", "november", "oscar", "papa", "quebec", "romeo",
  "sierra", "tango", "uniform", "victor", "whiskey", "xray",
];

function makeRow(overrides: Partial<BrokerCredentialView> = {}): BrokerCredentialView {
  return {
    id: "11111111-1111-1111-1111-111111111111",
    provider: "etoro",
    label: "api_key",
    environment: "demo",
    last_four: "1234",
    created_at: "2026-04-08T00:00:00Z",
    last_used_at: null,
    revoked_at: null,
    ...overrides,
  };
}

function apiKeyRow(): BrokerCredentialView {
  return makeRow({ id: "aaaa-1111", label: "api_key", last_four: "aaaa" });
}

function withPhrase(): CreateBrokerCredentialResponse {
  return { credential: makeRow(), recovery_phrase: PHRASE };
}

function withoutPhrase(): CreateBrokerCredentialResponse {
  return { credential: makeRow(), recovery_phrase: null };
}

async function completeStep1(): Promise<void> {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText(/Username/i), "alice");
  await user.type(screen.getByLabelText(/Password/i), "correct-horse-battery");
  await user.click(screen.getByRole("button", { name: /Create operator/i }));
}

async function fillStep2(apiKey: string, userKey: string): Promise<void> {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText("API key"), apiKey);
  await user.type(screen.getByLabelText("User key"), userKey);
  await user.click(screen.getByRole("button", { name: /Save credentials/i }));
}

async function answerChallengeCorrectly(): Promise<void> {
  const user = userEvent.setup();
  const labels = await screen.findAllByText(/^Word #\d+$/);
  for (const labelText of labels) {
    const labelEl = labelText.closest("label")!;
    const input = within(labelEl).getByRole("textbox");
    const position = Number.parseInt(/Word #(\d+)/.exec(labelText.textContent ?? "")![1]!, 10);
    await user.type(input, PHRASE[position - 1]!);
  }
  await user.click(screen.getByRole("button", { name: "Confirm" }));
}

async function advancePastWrittenDownGate(): Promise<void> {
  const user = userEvent.setup();
  await user.click(await screen.findByLabelText(/I have written down/i));
  await user.click(screen.getByRole("button", { name: "Continue" }));
}

beforeEach(() => {
  mockedPostSetup.mockReset();
  mockedCreate.mockReset();
  mockedList.mockReset();
  mockedValidate.mockReset();
  navigateMock.mockReset();
  markAuthenticatedMock.mockReset();
  useSessionMock.mockReset();
  useSessionMock.mockReturnValue({
    status: "needs_setup",
    operator: null,
    bootstrapState: { needs_setup: true, recovery_required: false },
    login: vi.fn(),
    logout: vi.fn(),
    markAuthenticated: markAuthenticatedMock,
    refreshBootstrapState: vi.fn(),
  } as unknown as ReturnType<typeof useSession>);
  // Default: no credentials exist (fresh setup).
  mockedList.mockResolvedValue([]);
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Step 1 (operator) — unchanged from #122
// ---------------------------------------------------------------------------

describe("SetupPage — step 1 (operator)", () => {
  it("surfaces the generic error and stays on step 1 when /auth/setup fails", async () => {
    mockedPostSetup.mockRejectedValueOnce(new ApiError(404, "nope"));
    render(<SetupPage />);
    await completeStep1();
    expect(
      await screen.findByText(/Setup unavailable or invalid token/i),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Create operator/i })).toBeInTheDocument();
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
    expect(navigateMock).not.toHaveBeenCalled();
  });

  it("advances to step 2 on success WITHOUT calling markAuthenticated yet", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    render(<SetupPage />);
    await completeStep1();
    expect(
      await screen.findByRole("button", { name: /Save credentials/i }),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Skip for now/i })).toBeInTheDocument();
    // Two-key fields visible.
    expect(screen.getByLabelText("API key")).toBeInTheDocument();
    expect(screen.getByLabelText("User key")).toBeInTheDocument();
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
    expect(navigateMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Step 2 (broker, optional) — updated for two-key model
// ---------------------------------------------------------------------------

describe("SetupPage — step 2 (broker, optional)", () => {
  it("'Skip for now' completes the wizard with no broker call", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    render(<SetupPage />);
    await completeStep1();
    await screen.findByRole("button", { name: /Skip for now/i });

    await user.click(screen.getByRole("button", { name: /Skip for now/i }));

    expect(mockedCreate).not.toHaveBeenCalled();
    expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("creates both api_key and user_key rows on save", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())  // api_key
      .mockResolvedValueOnce(withoutPhrase()); // user_key

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");

    await waitFor(() => {
      expect(mockedCreate).toHaveBeenCalledTimes(2);
    });
    expect(mockedCreate).toHaveBeenNthCalledWith(1, {
      provider: "etoro",
      label: "api_key",
      environment: "demo",
      secret: "test-api-key",
    });
    expect(mockedCreate).toHaveBeenNthCalledWith(2, {
      provider: "etoro",
      label: "user_key",
      environment: "demo",
      secret: "test-user-key",
    });
    // Wizard completes after both saves.
    expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("opens the phrase modal when the first create response carries a recovery_phrase", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withPhrase())     // api_key (triggers phrase)
      .mockResolvedValueOnce(withoutPhrase()); // user_key

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");

    const dialog = await screen.findByRole("dialog", {
      name: "Recovery phrase confirmation",
    });
    expect(dialog).toBeInTheDocument();
    // Both credentials saved before modal opens.
    expect(mockedCreate).toHaveBeenCalledTimes(2);
    // Wizard NOT yet complete.
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
    expect(navigateMock).not.toHaveBeenCalled();
  });

  it("completes the wizard after the operator passes the challenge", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withPhrase())
      .mockResolvedValueOnce(withoutPhrase());

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");
    await screen.findByRole("dialog");

    await advancePastWrittenDownGate();
    await answerChallengeCorrectly();

    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("routes Cancel through confirm-cancel gate, then 'Close anyway' completes wizard", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withPhrase())
      .mockResolvedValueOnce(withoutPhrase());

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(
      screen.getByText(/you may lose recovery ability/i),
    ).toBeInTheDocument();
    expect(markAuthenticatedMock).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Close anyway" }));
    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("enters Repair mode and keeps Skip available when second save fails", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())  // api_key succeeds
      .mockRejectedValueOnce(new Error("boom")); // user_key fails
    // Mount fetch on step-2 entry returns empty (fresh setup), then
    // post-error refresh returns the saved api_key.
    mockedList
      .mockResolvedValueOnce([])           // mount fetch
      .mockResolvedValueOnce([apiKeyRow()]); // post-error refresh

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");

    // Error surfaced.
    expect(await screen.findByText(/Could not save credential/i)).toBeInTheDocument();
    // Re-derived to Repair mode.
    await waitFor(() => {
      expect(screen.queryByLabelText("API key")).toBeNull();
    });
    expect(screen.getByLabelText("User key")).toBeInTheDocument();
    // Skip still available.
    expect(screen.getByRole("button", { name: /Skip for now/i })).toBeEnabled();
    // Wizard NOT yet complete.
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Edge case 2 (ADR-0003 §5 row 3)
// ---------------------------------------------------------------------------

describe("SetupPage — edge case 2", () => {
  it("completes wizard with NO phrase modal when response has no recovery_phrase", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())
      .mockResolvedValueOnce(withoutPhrase());

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");

    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });
});
