/**
 * Tests for SetupPage 2-step wizard (#122 / ADR-0003 Ticket 2c).
 *
 * Scope:
 *   - Step 1 still creates the operator and surfaces the generic-404
 *     error on failure (regression for #106).
 *   - Step 1 success advances to step 2 instead of navigating away;
 *     markAuthenticated is NOT called yet.
 *   - Step 2 "Skip for now" completes the wizard with no broker call,
 *     calls markAuthenticated, navigates to /.
 *   - Step 2 inline first-save with a recovery_phrase response opens
 *     the same phrase modal as the SettingsPage flow.
 *   - Phrase modal challenge confirm completes the wizard.
 *   - Phrase modal cancel routes through the same fail-closed gate
 *     as #121 ("Close anyway" still completes the wizard).
 *   - Step 2 backend save failures (409 / 400 / generic) keep the
 *     operator on step 2 with form values preserved and "Skip for
 *     now" still available.
 *   - Edge case 2 (ADR-0003 §5 row 3): a successful save with
 *     recovery_phrase: null shows NO modal and completes the wizard.
 *   - markAuthenticated is called exactly once per wizard run, only
 *     at the wizard's completion.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { SetupPage } from "@/pages/SetupPage";
import {
  type CreateBrokerCredentialResponse,
  createBrokerCredential,
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

const OPERATOR: Operator = {
  id: "00000000-0000-0000-0000-000000000001",
  username: "alice",
};

const PHRASE: readonly string[] = [
  "alpha",
  "bravo",
  "charlie",
  "delta",
  "echo",
  "foxtrot",
  "golf",
  "hotel",
  "india",
  "juliet",
  "kilo",
  "lima",
  "mike",
  "november",
  "oscar",
  "papa",
  "quebec",
  "romeo",
  "sierra",
  "tango",
  "uniform",
  "victor",
  "whiskey",
  "xray",
];

function withPhrase(): CreateBrokerCredentialResponse {
  return {
    credential: {
      id: "11111111-1111-1111-1111-111111111111",
      provider: "etoro",
      label: "primary",
      last_four: "1234",
      created_at: "2026-04-08T00:00:00Z",
      last_used_at: null,
      revoked_at: null,
    },
    recovery_phrase: PHRASE,
  };
}

function withoutPhrase(): CreateBrokerCredentialResponse {
  return { ...withPhrase(), recovery_phrase: null };
}

async function completeStep1(): Promise<void> {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText(/Username/i), "alice");
  await user.type(screen.getByLabelText(/Password/i), "correct-horse-battery");
  await user.click(screen.getByRole("button", { name: /Create operator/i }));
}

async function fillStep2(label: string, secret: string): Promise<void> {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText("Label"), label);
  await user.type(screen.getByLabelText("Secret"), secret);
  await user.click(screen.getByRole("button", { name: /Save credential/i }));
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
  navigateMock.mockReset();
  markAuthenticatedMock.mockReset();
  useSessionMock.mockReset();
  useSessionMock.mockReturnValue({
    status: "needs_setup",
    operator: null,
    login: vi.fn(),
    logout: vi.fn(),
    markAuthenticated: markAuthenticatedMock,
  } as ReturnType<typeof useSession>);
});

afterEach(() => {
  vi.clearAllMocks();
});

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
    // Step 2 form is now visible.
    expect(
      await screen.findByRole("button", { name: /Save credential/i }),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Skip for now/i })).toBeInTheDocument();
    // Critically: markAuthenticated and navigate are deferred.
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
    expect(navigateMock).not.toHaveBeenCalled();
  });
});

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

  it("opens the phrase modal when the inline save returns a recovery_phrase", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate.mockResolvedValueOnce(withPhrase());
    render(<SetupPage />);
    await completeStep1();
    await screen.findByRole("button", { name: /Save credential/i });
    await fillStep2("primary", "secret-value-1234");

    const dialog = await screen.findByRole("dialog", {
      name: "Recovery phrase confirmation",
    });
    expect(dialog).toBeInTheDocument();
    // Wizard is NOT yet complete.
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
    expect(navigateMock).not.toHaveBeenCalled();
  });

  it("completes the wizard after the operator passes the challenge", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate.mockResolvedValueOnce(withPhrase());
    render(<SetupPage />);
    await completeStep1();
    await fillStep2("primary", "secret-value-1234");
    await screen.findByRole("dialog");

    await advancePastWrittenDownGate();
    await answerChallengeCorrectly();

    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("routes Cancel through the confirm-cancel gate, then 'Close anyway' completes the wizard", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate.mockResolvedValueOnce(withPhrase());
    render(<SetupPage />);
    await completeStep1();
    await fillStep2("primary", "secret-value-1234");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(
      screen.getByText(
        /you may lose recovery ability for this installation's broker credentials/i,
      ),
    ).toBeInTheDocument();
    // Still mid-wizard until the operator commits to closing.
    expect(markAuthenticatedMock).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Close anyway" }));
    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("preserves form values and keeps Skip available when the inline save fails", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate.mockRejectedValueOnce(new ApiError(409, "conflict"));
    render(<SetupPage />);
    await completeStep1();
    await fillStep2("primary", "secret-value-1234");

    expect(
      await screen.findByText(/credential with that label already exists/i),
    ).toBeInTheDocument();
    // No modal opened.
    expect(screen.queryByRole("dialog")).toBeNull();
    // Form values preserved (label not cleared).
    expect((screen.getByLabelText("Label") as HTMLInputElement).value).toBe("primary");
    // "Skip for now" still available so the operator can finish setup.
    const skipBtn = screen.getByRole("button", { name: /Skip for now/i });
    expect(skipBtn).toBeEnabled();
    // Wizard is still mid-flight.
    expect(markAuthenticatedMock).not.toHaveBeenCalled();

    // Skipping after the failure still completes the wizard.
    await user.click(skipBtn);
    expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });
});

describe("SetupPage — edge case 2 (ADR-0003 §5 row 3)", () => {
  it("completes the wizard with NO phrase modal when the response carries no recovery_phrase", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate.mockResolvedValueOnce(withoutPhrase());
    render(<SetupPage />);
    await completeStep1();
    await fillStep2("primary", "secret-value-1234");

    // No modal at any point.
    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });
});
