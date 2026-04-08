/**
 * Tests for SettingsPage broker-credentials first-save flow (#121).
 *
 * Scope:
 *   - empty state visible when no credentials exist
 *   - first save returning a recovery_phrase opens the confirmation modal
 *   - subsequent save returning no recovery_phrase does NOT open the modal
 *   - challenge confirm closes the modal and refreshes the list
 *   - operator-initiated cancel routes through the confirm-cancel gate
 *   - Escape inside the modal also routes through the confirm-cancel gate
 *   - "Go back" from the gate returns to the phrase view
 *   - "Close anyway" closes the modal and clears the phrase
 *   - backend save failure surfaces an error and does NOT open the modal
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
} from "@/api/brokerCredentials";
import { ApiError } from "@/api/client";

vi.mock("@/api/brokerCredentials", () => ({
  listBrokerCredentials: vi.fn(),
  createBrokerCredential: vi.fn(),
  revokeBrokerCredential: vi.fn(),
}));

const mockedList = vi.mocked(listBrokerCredentials);
const mockedCreate = vi.mocked(createBrokerCredential);
const mockedRevoke = vi.mocked(revokeBrokerCredential);

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

function makeRow(overrides: Partial<BrokerCredentialView> = {}): BrokerCredentialView {
  return {
    id: "11111111-1111-1111-1111-111111111111",
    provider: "etoro",
    label: "primary",
    last_four: "1234",
    created_at: "2026-04-08T00:00:00Z",
    last_used_at: null,
    revoked_at: null,
    ...overrides,
  };
}

function withPhrase(): CreateBrokerCredentialResponse {
  return { credential: makeRow(), recovery_phrase: PHRASE };
}

function withoutPhrase(): CreateBrokerCredentialResponse {
  return { credential: makeRow(), recovery_phrase: null };
}

async function fillAndSubmit(label: string, secret: string): Promise<void> {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText("Label"), label);
  await user.type(screen.getByLabelText("Secret"), secret);
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
  mockedList.mockResolvedValue([]);
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("SettingsPage — broker credentials empty state", () => {
  it("shows the empty-state message when no credentials are saved", async () => {
    render(<SettingsPage />);
    expect(
      await screen.findByText(/No broker credentials saved yet/i),
    ).toBeInTheDocument();
  });
});

describe("SettingsPage — first-save recovery phrase modal", () => {
  it("opens the modal when the create response carries a recovery_phrase", async () => {
    mockedCreate.mockResolvedValueOnce(withPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");

    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(
      within(dialog).getByText(/Write down your recovery phrase/i),
    ).toBeInTheDocument();
    // Saved row reflected in the list behind the modal.
    await waitFor(() => {
      expect(mockedList).toHaveBeenCalledTimes(2);
    });
  });

  it("does NOT open the modal when create returns no recovery_phrase", async () => {
    mockedCreate.mockResolvedValueOnce(withoutPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");

    await waitFor(() => {
      expect(mockedList).toHaveBeenCalledTimes(2);
    });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("closes the modal after the operator passes the challenge", async () => {
    mockedCreate.mockResolvedValueOnce(withPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");
    await screen.findByRole("dialog");

    await advancePastWrittenDownGate();
    await answerChallengeCorrectly();

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
  });
});

describe("SettingsPage — phrase modal cancel gate", () => {
  it("routes Cancel through the confirm-cancel warning, not silent dismissal", async () => {
    const user = userEvent.setup();
    mockedCreate.mockResolvedValueOnce(withPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));

    // The dialog must still be present, now showing the warning copy.
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(
      screen.getByText(
        /you may lose recovery ability for this installation's broker credentials/i,
      ),
    ).toBeInTheDocument();
  });

  it("routes Escape through the confirm-cancel warning, not silent dismissal", async () => {
    const user = userEvent.setup();
    mockedCreate.mockResolvedValueOnce(withPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");
    await screen.findByRole("dialog");

    await user.keyboard("{Escape}");

    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(
      screen.getByText(/you may lose recovery ability/i),
    ).toBeInTheDocument();
  });

  it("returns to the phrase view when the operator chooses 'Go back'", async () => {
    const user = userEvent.setup();
    mockedCreate.mockResolvedValueOnce(withPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await user.click(screen.getByRole("button", { name: "Go back" }));

    expect(
      screen.getByText(/Write down your recovery phrase/i),
    ).toBeInTheDocument();
  });

  it("closes and clears the phrase when the operator confirms 'Close anyway'", async () => {
    const user = userEvent.setup();
    mockedCreate.mockResolvedValueOnce(withPhrase());
    mockedList.mockResolvedValueOnce([]).mockResolvedValueOnce([makeRow()]);

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");
    const dialog = await screen.findByRole("dialog");

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await user.click(screen.getByRole("button", { name: "Close anyway" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
    // No revoke call -- the credential row stays committed (the phrase
    // is root-secret scoped, not row scoped).
    expect(mockedRevoke).not.toHaveBeenCalled();
  });
});

describe("SettingsPage — backend failure on first save", () => {
  it("surfaces a 409 conflict and does NOT open the modal", async () => {
    mockedCreate.mockRejectedValueOnce(new ApiError(409, "conflict"));

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");

    expect(
      await screen.findByText(/credential with that label already exists/i),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("surfaces a generic failure and does NOT open the modal", async () => {
    mockedCreate.mockRejectedValueOnce(new Error("boom"));

    render(<SettingsPage />);
    await screen.findByText(/No broker credentials saved yet/i);
    await fillAndSubmit("primary", "secret-value-1234");

    expect(
      await screen.findByText(/Could not save credential/i),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});
