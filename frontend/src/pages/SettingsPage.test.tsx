/**
 * Tests for SettingsPage broker-credentials section (#121, updated
 * #139 PR D for two-key credential model).
 *
 * Scope:
 *   - Credential-set mode detection (Create / Repair / Complete)
 *   - Two-key form: API key + user key fields, no label/secret fields
 *   - Two sequential createBrokerCredential calls on save
 *   - Test connection button behaviour and validation display
 *   - Recovery phrase modal flow (unchanged from #121)
 *   - Partial-save recovery (first key saved, second failed → Repair)
 *   - Revoke updates the credential list and can re-enable the form
 *
 * The API client is mocked at the module boundary so tests do not hit
 * the network. The phrase fixture is the same alphabet used in the
 * RecoveryPhraseConfirm tests.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { SettingsPage } from "@/pages/SettingsPage";
import {
  type BrokerCredentialView,
  type CreateBrokerCredentialResponse,
  createBrokerCredential,
  listBrokerCredentials,
  revokeBrokerCredential,
  validateBrokerCredential,
  validateStoredCredentials,
} from "@/api/brokerCredentials";
import { ApiError } from "@/api/client";

vi.mock("@/api/brokerCredentials", () => ({
  listBrokerCredentials: vi.fn(),
  createBrokerCredential: vi.fn(),
  revokeBrokerCredential: vi.fn(),
  validateBrokerCredential: vi.fn(),
  validateStoredCredentials: vi.fn(),
}));

// Budget API mock — prevents BudgetConfigSection from making real fetch
// calls, which would produce extra role="alert" elements and break the
// broker-credential test assertions that use getByRole("alert").
vi.mock("@/api/budget", () => ({
  fetchBudgetConfig: vi.fn().mockResolvedValue({
    cash_buffer_pct: 0.05,
    cgt_scenario: "higher",
    updated_at: "2026-04-15T00:00:00Z",
    updated_by: "system",
    reason: "initial seed",
  }),
  fetchCapitalEvents: vi.fn().mockResolvedValue([]),
  updateBudgetConfig: vi.fn(),
  createCapitalEvent: vi.fn(),
}));

const mockedList = vi.mocked(listBrokerCredentials);
const mockedCreate = vi.mocked(createBrokerCredential);
const mockedRevoke = vi.mocked(revokeBrokerCredential);
const mockedValidate = vi.mocked(validateBrokerCredential);
const mockedValidateStored = vi.mocked(validateStoredCredentials);

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

async function fillAndSubmit(apiKey: string, userKey: string): Promise<void> {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText("API key"), apiKey);
  await user.type(screen.getByLabelText("User key"), userKey);
  await user.click(screen.getByRole("button", { name: /save credential/i }));
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
  mockedList.mockReset();
  mockedCreate.mockReset();
  mockedRevoke.mockReset();
  mockedValidate.mockReset();
  mockedValidateStored.mockReset();
  mockedList.mockResolvedValue([]);
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Credential-set mode detection
// ---------------------------------------------------------------------------

describe("SettingsPage — credential-set modes", () => {
  it("shows both key fields when no credentials exist (Create mode)", async () => {
    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    expect(screen.getByLabelText("API key")).toBeInTheDocument();
    expect(screen.getByLabelText("User key")).toBeInTheDocument();
  });

  it("shows only the missing field when one key exists (Repair mode)", async () => {
    mockedList.mockResolvedValueOnce([apiKeyRow()]);
    render(<SettingsPage />);
    await screen.findByText("api_key");
    // api_key exists, so only user_key field should be shown.
    expect(screen.queryByLabelText("API key")).toBeNull();
    expect(screen.getByLabelText("User key")).toBeInTheDocument();
    expect(screen.getByText(/One key was already saved/i)).toBeInTheDocument();
  });

  it("hides the create form when both keys exist (Complete mode)", async () => {
    mockedList.mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);
    render(<SettingsPage />);
    await screen.findByText("api_key");
    expect(screen.getByText(/Credentials configured/i)).toBeInTheDocument();
    expect(screen.queryByLabelText("API key")).toBeNull();
    expect(screen.queryByLabelText("User key")).toBeNull();
  });

  it("shows environment in the credential list", async () => {
    mockedList.mockResolvedValueOnce([apiKeyRow()]);
    render(<SettingsPage />);
    expect(await screen.findByText(/demo/)).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Test connection
// ---------------------------------------------------------------------------

describe("SettingsPage — test connection", () => {
  it("calls validateBrokerCredential and shows success result", async () => {
    const user = userEvent.setup();
    mockedValidate.mockResolvedValueOnce({
      auth_valid: true,
      identity: { gcid: 12345, demo_cid: 67890, real_cid: null },
      environment: "demo",
      env_valid: true,
      env_check: "ok",
      note: "Does not verify write permission.",
    });

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await userEvent.setup().type(screen.getByLabelText("API key"), "test-api-key");
    await user.type(screen.getByLabelText("User key"), "test-user-key");
    await user.click(screen.getByRole("button", { name: /test connection/i }));

    expect(await screen.findByText(/Connection verified/i)).toBeInTheDocument();
    expect(screen.getByText(/account 12345/i)).toBeInTheDocument();
    // note shown as supplementary text, not as the primary message.
    expect(screen.getByText(/Does not verify write permission/i)).toBeInTheDocument();
  });

  it("shows auth failure without using note as error message", async () => {
    const user = userEvent.setup();
    mockedValidate.mockResolvedValueOnce({
      auth_valid: false,
      identity: null,
      environment: "demo",
      env_valid: false,
      env_check: "skipped",
      note: "Connection to eToro failed",
    });

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await user.type(screen.getByLabelText("API key"), "bad-api-key");
    await user.type(screen.getByLabelText("User key"), "bad-user-key");
    await user.click(screen.getByRole("button", { name: /test connection/i }));

    expect(
      await screen.findByText(/Authentication failed/i),
    ).toBeInTheDocument();
    // note should NOT be shown as the primary error.
    expect(screen.queryByText(/Connection to eToro failed/i)).toBeNull();
  });

  it("shows environment check failure as amber warning", async () => {
    const user = userEvent.setup();
    mockedValidate.mockResolvedValueOnce({
      auth_valid: true,
      identity: { gcid: 12345, demo_cid: null, real_cid: null },
      environment: "demo",
      env_valid: false,
      env_check: "403 Forbidden",
      note: "Does not verify write permission.",
    });

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await user.type(screen.getByLabelText("API key"), "test-api-key");
    await user.type(screen.getByLabelText("User key"), "test-user-key");
    await user.click(screen.getByRole("button", { name: /test connection/i }));

    expect(
      await screen.findByText(/environment check failed/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/403 Forbidden/)).toBeInTheDocument();
  });

  it("disables Test connection in Repair mode", async () => {
    mockedList.mockResolvedValueOnce([apiKeyRow()]);
    render(<SettingsPage />);
    await screen.findByText("api_key");

    const btn = screen.getByRole("button", { name: /test connection/i });
    expect(btn).toBeDisabled();
    expect(
      screen.getByText(/Connection testing requires both keys/i),
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Two-key save flow
// ---------------------------------------------------------------------------

describe("SettingsPage — two-key save", () => {
  it("creates both api_key and user_key rows on save in Create mode", async () => {
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())
      .mockResolvedValueOnce(withoutPhrase());
    mockedList
      .mockResolvedValueOnce([])                           // initial load
      .mockResolvedValueOnce([apiKeyRow(), userKeyRow()]); // refresh after save

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");

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
  });

  it("creates only the missing key in Repair mode", async () => {
    mockedList.mockResolvedValueOnce([apiKeyRow()]);
    mockedCreate.mockResolvedValueOnce(withoutPhrase());

    render(<SettingsPage />);
    await screen.findByText("api_key");

    const user = userEvent.setup();
    await user.type(screen.getByLabelText("User key"), "test-user-key");
    await user.click(screen.getByRole("button", { name: /save credential/i }));

    await waitFor(() => {
      expect(mockedCreate).toHaveBeenCalledTimes(1);
    });
    expect(mockedCreate).toHaveBeenCalledWith({
      provider: "etoro",
      label: "user_key",
      environment: "demo",
      secret: "test-user-key",
    });
  });

  it("enters Repair mode when first save succeeds but second fails", async () => {
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())
      .mockRejectedValueOnce(new Error("boom"));
    // Initial load: empty. After error refresh: api_key exists.
    mockedList
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([apiKeyRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");

    // Error surfaced.
    expect(await screen.findByText(/Could not save credential/i)).toBeInTheDocument();
    // Re-derived to Repair mode: only user_key field visible.
    await waitFor(() => {
      expect(screen.queryByLabelText("API key")).toBeNull();
    });
    expect(screen.getByLabelText("User key")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Recovery phrase modal (inherited from #121, updated for two-key)
// ---------------------------------------------------------------------------

describe("SettingsPage — recovery phrase modal", () => {
  it("opens the modal when the first create response carries a recovery_phrase", async () => {
    mockedCreate
      .mockResolvedValueOnce(withPhrase())   // api_key
      .mockResolvedValueOnce(withoutPhrase()); // user_key
    mockedList
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");

    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveAttribute("aria-modal", "true");
    // Both credentials should be saved before the modal opens.
    expect(mockedCreate).toHaveBeenCalledTimes(2);
  });

  it("does NOT open the modal when create returns no recovery_phrase", async () => {
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())
      .mockResolvedValueOnce(withoutPhrase());
    mockedList
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");

    await waitFor(() => {
      expect(mockedList).toHaveBeenCalledTimes(2);
    });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("closes the modal and refreshes the list after challenge passes", async () => {
    mockedCreate
      .mockResolvedValueOnce(withPhrase())
      .mockResolvedValueOnce(withoutPhrase());
    mockedList
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");
    await screen.findByRole("dialog");

    await advancePastWrittenDownGate();
    await answerChallengeCorrectly();

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
    await waitFor(() => {
      expect(mockedList).toHaveBeenCalledTimes(2);
    });
  });
});

// ---------------------------------------------------------------------------
// Phrase modal cancel gate
// ---------------------------------------------------------------------------

describe("SettingsPage — phrase modal cancel gate", () => {
  it("routes Cancel through the confirm-cancel warning", async () => {
    const user = userEvent.setup();
    mockedCreate
      .mockResolvedValueOnce(withPhrase())
      .mockResolvedValueOnce(withoutPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(
      screen.getByText(/you may lose recovery ability/i),
    ).toBeInTheDocument();
  });

  it("closes and clears the phrase when the operator confirms 'Close anyway'", async () => {
    const user = userEvent.setup();
    mockedCreate
      .mockResolvedValueOnce(withPhrase())
      .mockResolvedValueOnce(withoutPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await user.click(screen.getByRole("button", { name: "Close anyway" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
    expect(mockedRevoke).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Backend failure
// ---------------------------------------------------------------------------

describe("SettingsPage — backend failure", () => {
  it("surfaces a 409 conflict and does NOT open the modal", async () => {
    mockedCreate.mockRejectedValueOnce(new ApiError(409, "conflict"));

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");

    expect(
      await screen.findByText(/credential with that label already exists/i),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("surfaces a generic failure and does NOT open the modal", async () => {
    mockedCreate.mockRejectedValueOnce(new Error("boom"));

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("test-api-key", "test-user-key");

    expect(
      await screen.findByText(/Could not save credential/i),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Complete mode — management panel (#144)
// ---------------------------------------------------------------------------

describe("SettingsPage — complete mode management", () => {
  beforeEach(() => {
    mockedList.mockResolvedValue([apiKeyRow(), userKeyRow()]);
  });

  it("shows Test connection and Replace both buttons", async () => {
    render(<SettingsPage />);
    await screen.findByText(/Credentials configured/i);
    expect(screen.getByRole("button", { name: /test connection/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /replace both/i })).toBeInTheDocument();
  });

  it("shows Edit button on each active credential row", async () => {
    render(<SettingsPage />);
    await screen.findByText("api_key");
    const editButtons = screen.getAllByRole("button", { name: "Edit" });
    expect(editButtons).toHaveLength(2);
  });

  it("test connection calls validateStoredCredentials and shows result", async () => {
    mockedValidateStored.mockResolvedValueOnce({
      auth_valid: true,
      identity: { gcid: 999, demo_cid: null, real_cid: null },
      environment: "demo",
      env_valid: true,
      env_check: "ok",
      note: "Does not verify write permission",
    });
    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText(/Credentials configured/i);
    await user.click(screen.getByRole("button", { name: /test connection/i }));

    await waitFor(() => {
      expect(screen.getByText(/Connection verified/i)).toBeInTheDocument();
    });
    expect(mockedValidateStored).toHaveBeenCalledOnce();
    // Transient validate should NOT have been called
    expect(mockedValidate).not.toHaveBeenCalled();
  });

  it("shows error when stored validation returns 404", async () => {
    mockedValidateStored.mockRejectedValueOnce(new ApiError(404, "not found"));
    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText(/Credentials configured/i);
    await user.click(screen.getByRole("button", { name: /test connection/i }));

    await waitFor(() => {
      expect(screen.getByText(/must be stored/i)).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// Edit single key (#144)
// ---------------------------------------------------------------------------

describe("SettingsPage — edit single key", () => {
  beforeEach(() => {
    mockedList.mockResolvedValue([apiKeyRow(), userKeyRow()]);
    mockedRevoke.mockResolvedValue(undefined);
    mockedCreate.mockResolvedValue(withoutPhrase());
  });

  it("opens edit form for API key and saves (revoke + create)", async () => {
    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText("api_key");

    // Click Edit on the first credential row
    const editButtons = screen.getAllByRole("button", { name: "Edit" });
    await user.click(editButtons[0]!);

    // Edit form should appear
    expect(screen.getByText("Edit API key")).toBeInTheDocument();
    const input = screen.getByLabelText("New API key");
    await user.type(input, "new-secret-value");

    // After save, list refreshes — set up the mock for refresh
    mockedList.mockResolvedValueOnce([
      makeRow({ id: "new-id", label: "api_key", last_four: "alue" }),
      userKeyRow(),
    ]);

    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      // Revoke was called for the old api_key
      expect(mockedRevoke).toHaveBeenCalledWith("aaaa-1111");
    });
    // Create was called with the new secret
    expect(mockedCreate).toHaveBeenCalledWith({
      provider: "etoro",
      label: "api_key",
      environment: "demo",
      secret: "new-secret-value",
    });
  });

  it("cancel returns to idle management panel", async () => {
    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText("api_key");

    const editButtons = screen.getAllByRole("button", { name: "Edit" });
    await user.click(editButtons[0]!);

    expect(screen.getByText("Edit API key")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    // Should be back to idle — management buttons visible
    expect(screen.getByRole("button", { name: /test connection/i })).toBeInTheDocument();
  });

  it("surfaces partial failure when revoke succeeds but create fails", async () => {
    mockedRevoke.mockResolvedValue(undefined);
    mockedCreate.mockRejectedValueOnce(new Error("network"));
    // After refresh, the revoked key is gone → repair mode
    mockedList.mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);
    mockedList.mockResolvedValueOnce([userKeyRow()]);

    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText("api_key");

    const editButtons = screen.getAllByRole("button", { name: "Edit" });
    await user.click(editButtons[0]!);

    await user.type(screen.getByLabelText("New API key"), "new-val");
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(/old api_key was revoked/i);
    });
  });
});

// ---------------------------------------------------------------------------
// Replace both keys (#144)
// ---------------------------------------------------------------------------

describe("SettingsPage — replace both keys", () => {
  beforeEach(() => {
    mockedList.mockResolvedValue([apiKeyRow(), userKeyRow()]);
    mockedRevoke.mockResolvedValue(undefined);
    mockedCreate.mockResolvedValue(withoutPhrase());
  });

  it("opens replace form and saves (revoke both + create both)", async () => {
    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText(/Credentials configured/i);

    await user.click(screen.getByRole("button", { name: /replace both/i }));

    expect(screen.getByText("Replace both keys")).toBeInTheDocument();
    await user.type(screen.getByLabelText("New API key"), "new-api");
    await user.type(screen.getByLabelText("New user key"), "new-user");

    mockedList.mockResolvedValueOnce([
      makeRow({ id: "new-api-id", label: "api_key" }),
      makeRow({ id: "new-user-id", label: "user_key" }),
    ]);

    await user.click(screen.getByRole("button", { name: /^replace both$/i }));

    await waitFor(() => {
      // Both old credentials revoked
      expect(mockedRevoke).toHaveBeenCalledTimes(2);
    });
    // Both new credentials created
    expect(mockedCreate).toHaveBeenCalledTimes(2);
  });

  it("surfaces partial failure when api_key created but user_key fails", async () => {
    mockedRevoke.mockResolvedValue(undefined);
    // api_key create succeeds, user_key create fails
    mockedCreate
      .mockResolvedValueOnce(withoutPhrase())
      .mockRejectedValueOnce(new Error("network"));
    // After refresh, only api_key exists → repair mode
    mockedList.mockResolvedValueOnce([apiKeyRow(), userKeyRow()]);
    mockedList.mockResolvedValueOnce([makeRow({ label: "api_key" })]);

    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText(/Credentials configured/i);

    await user.click(screen.getByRole("button", { name: /replace both/i }));
    await user.type(screen.getByLabelText("New API key"), "new-api");
    await user.type(screen.getByLabelText("New user key"), "new-user");
    await user.click(screen.getByRole("button", { name: /^replace both$/i }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(/api_key was saved but user_key failed/i);
    });
  });

  it("replace form has working test connection with candidate keys", async () => {
    mockedValidate.mockResolvedValueOnce({
      auth_valid: true,
      identity: null,
      environment: "demo",
      env_valid: true,
      env_check: "ok",
      note: "test",
    });
    const user = userEvent.setup();
    render(<SettingsPage />);
    await screen.findByText(/Credentials configured/i);

    await user.click(screen.getByRole("button", { name: /replace both/i }));
    await user.type(screen.getByLabelText("New API key"), "test-api");
    await user.type(screen.getByLabelText("New user key"), "test-user");

    await user.click(screen.getByRole("button", { name: /test connection/i }));

    await waitFor(() => {
      expect(screen.getByText(/Connection verified/i)).toBeInTheDocument();
    });
    // Should use transient validate, not stored
    expect(mockedValidate).toHaveBeenCalledOnce();
    expect(mockedValidateStored).not.toHaveBeenCalled();
  });
});
