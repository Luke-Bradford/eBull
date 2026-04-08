/**
 * Tests for RecoverPage (#116 / ADR-0003 Ticket 3).
 *
 * Coverage:
 *   - paste-24-words fans out across the inputs from any field
 *   - unknown word produces "Word N is not recognised", no fetch
 *   - bad checksum is rejected client-side, no fetch
 *   - happy path posts the joined phrase and re-fetches bootstrap-state
 *   - 400 from /auth/recover renders the generic
 *     "phrase doesn't match this installation" message
 *   - phrase is never written to localStorage / sessionStorage
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { RecoverPage } from "@/pages/RecoverPage";
import { postRecover } from "@/api/auth";
import { ApiError } from "@/api/client";
import { useSession } from "@/lib/session";

vi.mock("@/api/auth", async () => {
  const actual = await vi.importActual<typeof import("@/api/auth")>("@/api/auth");
  return { ...actual, postRecover: vi.fn() };
});

const navigateMock = vi.fn();
vi.mock("react-router-dom", () => ({
  useNavigate: () => navigateMock,
}));

const refreshBootstrapStateMock = vi.fn();
const useSessionMock = vi.fn();
vi.mock("@/lib/session", () => ({
  useSession: () => useSessionMock(),
}));

const mockedPostRecover = vi.mocked(postRecover);

// Canonical BIP39 24-word vector for the all-zeros seed.
const VALID_PHRASE: readonly string[] = [
  ...Array(23).fill("abandon"),
  "art",
];

function defaultSession(): ReturnType<typeof useSession> {
  return {
    status: "needs_recovery",
    operator: null,
    bootstrapState: { needs_setup: false, recovery_required: true },
    login: vi.fn(),
    logout: vi.fn(),
    markAuthenticated: vi.fn(),
    refreshBootstrapState: refreshBootstrapStateMock,
  } as unknown as ReturnType<typeof useSession>;
}

beforeEach(() => {
  mockedPostRecover.mockReset();
  navigateMock.mockReset();
  refreshBootstrapStateMock.mockReset();
  useSessionMock.mockReset();
  useSessionMock.mockReturnValue(defaultSession());
  // Snapshot storage so we can assert nothing landed in it.
  window.localStorage.clear();
  window.sessionStorage.clear();
});

afterEach(() => {
  vi.clearAllMocks();
});

async function fillAllInputs(words: readonly string[]): Promise<void> {
  const user = userEvent.setup();
  for (let i = 0; i < words.length; i++) {
    const input = screen.getByLabelText(`Word ${i + 1}`);
    await user.clear(input);
    await user.type(input, words[i]!);
  }
}

describe("RecoverPage", () => {
  it("fans pasted 24 words across the inputs starting at the focused field", async () => {
    const user = userEvent.setup();
    render(<RecoverPage />);
    const first = screen.getByLabelText("Word 1");
    await user.click(first);

    // userEvent.paste pastes into the focused element.
    await user.paste(VALID_PHRASE.join(" "));

    for (let i = 0; i < VALID_PHRASE.length; i++) {
      expect(
        (screen.getByLabelText(`Word ${i + 1}`) as HTMLInputElement).value,
      ).toBe(VALID_PHRASE[i]);
    }
  });

  it("rejects an unknown word client-side with a precise position-based message", async () => {
    const phrase = [...VALID_PHRASE];
    phrase[4] = "notaword";
    render(<RecoverPage />);
    await fillAllInputs(phrase);

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Recover/i }));

    expect(
      await screen.findByText(/Word 5 is not recognised/i),
    ).toBeInTheDocument();
    expect(mockedPostRecover).not.toHaveBeenCalled();
  });

  it("rejects a phrase with a bad checksum client-side", async () => {
    const phrase = [...VALID_PHRASE];
    phrase[23] = "ability"; // valid wordlist entry, wrong checksum
    render(<RecoverPage />);
    await fillAllInputs(phrase);

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Recover/i }));

    expect(
      await screen.findByText(/checksum is invalid/i),
    ).toBeInTheDocument();
    expect(mockedPostRecover).not.toHaveBeenCalled();
  });

  it("posts the joined phrase and refreshes bootstrap-state on success", async () => {
    mockedPostRecover.mockResolvedValueOnce({
      boot_state: "normal",
      recovery_required: false,
    });
    render(<RecoverPage />);
    await fillAllInputs(VALID_PHRASE);

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Recover/i }));

    await waitFor(() => {
      expect(mockedPostRecover).toHaveBeenCalledTimes(1);
    });
    expect(mockedPostRecover).toHaveBeenCalledWith(VALID_PHRASE.join(" "));
    expect(refreshBootstrapStateMock).toHaveBeenCalledTimes(1);

    // The phrase must NOT have been persisted to web storage.
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
  });

  it("renders the generic 'wrong phrase for this installation' message on a 400", async () => {
    mockedPostRecover.mockRejectedValueOnce(
      new ApiError(400, "recovery phrase invalid"),
    );
    render(<RecoverPage />);
    await fillAllInputs(VALID_PHRASE);

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Recover/i }));

    expect(
      await screen.findByText(/doesn't match this installation/i),
    ).toBeInTheDocument();
  });
});
