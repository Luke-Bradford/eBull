/**
 * DisplayCurrencySection — audit attribution (#1992).
 *
 * Pins the PATCH /config body's updated_by to the authenticated
 * operator's username (KillSwitchSection precedent) and the blocked-save
 * behaviour when no operator is authenticated.
 */
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { DisplayCurrencySection } from "@/components/settings/DisplayCurrencySection";

vi.mock("@/api/client", () => ({ apiFetch: vi.fn() }));

// #1992: sections read the authenticated operator for audit attribution.
const useSessionMock = vi.fn((): { operator: { id: string; username: string } | null } => ({
  operator: { id: "1", username: "luke" },
}));
vi.mock("@/lib/session", () => ({ useSession: () => useSessionMock() }));

import { apiFetch } from "@/api/client";

const mockedApiFetch = vi.mocked(apiFetch);

describe("DisplayCurrencySection", () => {
  beforeEach(() => {
    mockedApiFetch.mockReset();
    useSessionMock.mockClear();
  });

  it("sends the authenticated operator as updated_by (#1992)", async () => {
    mockedApiFetch.mockResolvedValue({});
    const user = userEvent.setup();
    render(<DisplayCurrencySection currentCurrency="GBP" onChanged={() => {}} />);

    await user.selectOptions(screen.getByRole("combobox"), "USD");
    await user.click(screen.getByRole("button", { name: /save currency/i }));

    await waitFor(() => expect(mockedApiFetch).toHaveBeenCalledOnce());
    const call = mockedApiFetch.mock.calls[0]!;
    const init = call[1];
    expect(JSON.parse((init as RequestInit).body as string)).toEqual({
      updated_by: "luke",
      reason: "Changed display currency to USD",
      display_currency: "USD",
    });
  });

  it("blocks save when no operator is authenticated (#1992)", async () => {
    useSessionMock.mockReturnValue({ operator: null });
    const user = userEvent.setup();
    render(<DisplayCurrencySection currentCurrency="GBP" onChanged={() => {}} />);

    await user.selectOptions(screen.getByRole("combobox"), "USD");
    // Real identity required for runtime_config_audit attribution — the
    // save button is disabled rather than sending a fabricated fallback.
    expect(screen.getByRole("button", { name: /save currency/i })).toBeDisabled();
    expect(mockedApiFetch).not.toHaveBeenCalled();
  });
});
