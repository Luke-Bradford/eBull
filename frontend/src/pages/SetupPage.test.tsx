/**
 * Tests for SetupPage 2-step wizard (#122 / ADR-0003 Ticket 2c,
 * updated #139 PR D for two-key credential model, trimmed #327 for
 * state-machine extraction into useSetupWizard).
 *
 * Scope (integration-level — multi-step UX that reducer unit tests
 * cannot cover):
 *   1. Happy-path save — both credential POSTs + markAuthenticated +
 *      nightly_universe_sync fire.
 *   2. Happy-path skip — Skip completes wizard, no broker POST, no
 *      universe-sync call.
 *   3. Phrase modal — recovery_phrase response opens modal, operator
 *      passes challenge, markAuthenticated fires.
 *   4. Confirm-cancel gate — Cancel during phrase modal routes through
 *      confirm dialog, "Close anyway" completes wizard.
 *   5. Repair mode — first save partial-fails, UI re-derives to repair
 *      mode with only the missing-key field, Skip still available.
 *   6. Already-complete branch — step 2 opens with both credRows
 *      present, UI shows Continue button, click completes wizard.
 *
 * Reducer + hook-level behaviour (GENERIC_ERROR mapping, classifier
 * fixed strings, runJob fire-and-forget swallowing, credRows fallback
 * on list error) is covered by useSetupWizard.test.ts.
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
import { runJob } from "@/api/jobs";
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
vi.mock("@/api/jobs", () => ({
  runJob: vi.fn(),
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
const mockedRunJob = vi.mocked(runJob);

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

function userKeyRow(): BrokerCredentialView {
  return makeRow({ id: "bbbb-2222", label: "user_key", last_four: "bbbb" });
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
  mockedRunJob.mockReset();
  mockedRunJob.mockResolvedValue(undefined);
  // Default: no credentials exist (fresh setup).
  mockedList.mockResolvedValue([]);
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("SetupPage — integration", () => {
  it("happy-path save: creates both credentials, fires universe-sync, completes wizard", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase()) // api_key
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
    await waitFor(() => {
      expect(mockedRunJob).toHaveBeenCalledWith("nightly_universe_sync");
    });
    expect(screen.queryByRole("dialog")).toBeNull();
    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("happy-path skip: no broker POST, no universe-sync, wizard completes", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    render(<SetupPage />);
    await completeStep1();
    await screen.findByRole("button", { name: /Skip for now/i });

    await user.click(screen.getByRole("button", { name: /Skip for now/i }));

    expect(mockedCreate).not.toHaveBeenCalled();
    expect(mockedRunJob).not.toHaveBeenCalled();
    expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("phrase-modal branch: recovery_phrase opens modal, passing challenge completes wizard", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withPhrase())
      .mockResolvedValueOnce(withoutPhrase());

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");

    const dialog = await screen.findByRole("dialog", {
      name: "Recovery phrase confirmation",
    });
    expect(dialog).toBeInTheDocument();
    expect(mockedCreate).toHaveBeenCalledTimes(2);
    expect(markAuthenticatedMock).not.toHaveBeenCalled();

    await advancePastWrittenDownGate();
    await answerChallengeCorrectly();

    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
  });

  it("confirm-cancel gate: Cancel during phrase modal → 'Close anyway' completes wizard", async () => {
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

  it("repair-mode: partial save failure re-derives UI to show only missing key, Skip stays available", async () => {
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())
      .mockRejectedValueOnce(new Error("boom"));
    mockedList
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([apiKeyRow()]);

    render(<SetupPage />);
    await completeStep1();
    await fillStep2("test-api-key", "test-user-key");

    expect(await screen.findByText(/Could not save credential/i)).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.queryByLabelText("API key")).toBeNull();
    });
    expect(screen.getByLabelText("User key")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Skip for now/i })).toBeEnabled();
    expect(markAuthenticatedMock).not.toHaveBeenCalled();
  });

  it("already-complete branch: step 2 opens with both credentials present, Continue completes wizard", async () => {
    const user = userEvent.setup();
    mockedPostSetup.mockResolvedValueOnce({ operator: OPERATOR });
    mockedList.mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);

    render(<SetupPage />);
    await completeStep1();

    expect(await screen.findByText(/Credentials configured/i)).toBeInTheDocument();
    const continueBtn = screen.getByRole("button", { name: /Continue/i });
    expect(continueBtn).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Save credentials/i })).toBeNull();
    expect(screen.queryByLabelText("API key")).toBeNull();
    expect(markAuthenticatedMock).not.toHaveBeenCalled();

    await user.click(continueBtn);
    await waitFor(() => {
      expect(markAuthenticatedMock).toHaveBeenCalledTimes(1);
    });
    expect(markAuthenticatedMock).toHaveBeenCalledWith(OPERATOR);
    expect(navigateMock).toHaveBeenCalledWith("/", { replace: true });
    expect(mockedCreate).not.toHaveBeenCalled();
    expect(mockedRunJob).not.toHaveBeenCalled();
  });
});
