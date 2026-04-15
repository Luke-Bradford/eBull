/**
 * Tests for BudgetConfigSection (#233).
 *
 * Scope:
 *   - Config controls display correct values after fraction → percentage
 *     conversion (backend stores 0.05, input shows 5)
 *   - Save button disabled when no field changed or reason is empty
 *   - Save sends only changed fields, converting percentage back to fraction
 *   - Save error surfaces the alert banner
 *   - Capital event form validation and submission
 *   - Capital events history table renders rows
 *   - Empty-state for no capital events
 *
 * The API client is mocked at the module boundary.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { BudgetConfigSection } from "@/components/settings/BudgetConfigSection";
import {
  createCapitalEvent,
  fetchBudgetConfig,
  fetchCapitalEvents,
  updateBudgetConfig,
} from "@/api/budget";
import type {
  BudgetConfigResponse,
  CapitalEventResponse,
} from "@/api/types";

vi.mock("@/api/budget", () => ({
  fetchBudgetConfig: vi.fn(),
  fetchCapitalEvents: vi.fn(),
  updateBudgetConfig: vi.fn(),
  createCapitalEvent: vi.fn(),
}));

const mockedFetchConfig = vi.mocked(fetchBudgetConfig);
const mockedFetchEvents = vi.mocked(fetchCapitalEvents);
const mockedUpdateConfig = vi.mocked(updateBudgetConfig);
const mockedCreateEvent = vi.mocked(createCapitalEvent);

function configResponse(
  overrides: Partial<BudgetConfigResponse> = {},
): BudgetConfigResponse {
  return {
    cash_buffer_pct: 0.05,
    cgt_scenario: "higher",
    updated_at: "2026-04-15T10:00:00Z",
    updated_by: "system",
    reason: "initial seed",
    ...overrides,
  };
}

function eventResponse(
  overrides: Partial<CapitalEventResponse> = {},
): CapitalEventResponse {
  return {
    event_id: 1,
    event_time: "2026-04-15T10:00:00Z",
    event_type: "injection",
    amount: 5000,
    currency: "USD",
    source: "operator",
    note: null,
    created_by: "operator",
    ...overrides,
  };
}

beforeEach(() => {
  mockedFetchConfig.mockReset();
  mockedFetchEvents.mockReset();
  mockedUpdateConfig.mockReset();
  mockedCreateEvent.mockReset();
  mockedFetchConfig.mockResolvedValue(configResponse());
  mockedFetchEvents.mockResolvedValue([]);
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Config controls — display
// ---------------------------------------------------------------------------

describe("BudgetConfigSection — config display", () => {
  it("converts fraction to percentage for the buffer input (0.05 → 5)", async () => {
    render(<BudgetConfigSection />);
    const input = await screen.findByLabelText("Cash buffer %");
    expect(input).toHaveValue(5);
  });

  it("displays the CGT scenario from the server", async () => {
    render(<BudgetConfigSection />);
    const select = await screen.findByLabelText("CGT scenario");
    expect(select).toHaveValue("higher");
  });

  it("shows section heading", async () => {
    render(<BudgetConfigSection />);
    expect(
      await screen.findByText("Budget Configuration"),
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Config controls — save button state
// ---------------------------------------------------------------------------

describe("BudgetConfigSection — save button", () => {
  it("is disabled when no field has been changed", async () => {
    render(<BudgetConfigSection />);
    // Wait for config to load and form to appear
    await screen.findByLabelText("Cash buffer %");
    const btn = screen.getByRole("button", { name: "Save config" });
    expect(btn).toBeDisabled();
  });

  it("is disabled when reason is empty even if a field changed", async () => {
    const user = userEvent.setup();
    render(<BudgetConfigSection />);
    const input = await screen.findByLabelText("Cash buffer %");
    await user.clear(input);
    await user.type(input, "10");
    const btn = screen.getByRole("button", { name: "Save config" });
    expect(btn).toBeDisabled();
  });

  it("is enabled when a field changed AND reason is provided", async () => {
    const user = userEvent.setup();
    render(<BudgetConfigSection />);
    const input = await screen.findByLabelText("Cash buffer %");
    await user.clear(input);
    await user.type(input, "10");
    await user.type(
      screen.getByPlaceholderText("Why are you changing this?"),
      "test reason",
    );
    const btn = screen.getByRole("button", { name: "Save config" });
    expect(btn).toBeEnabled();
  });
});

// ---------------------------------------------------------------------------
// Config controls — save behaviour
// ---------------------------------------------------------------------------

describe("BudgetConfigSection — save", () => {
  it("converts percentage back to fraction when saving (10 → 0.10)", async () => {
    mockedUpdateConfig.mockResolvedValueOnce(
      configResponse({ cash_buffer_pct: 0.1 }),
    );
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    const input = await screen.findByLabelText("Cash buffer %");
    await user.clear(input);
    await user.type(input, "10");
    await user.type(
      screen.getByPlaceholderText("Why are you changing this?"),
      "increasing buffer",
    );
    await user.click(screen.getByRole("button", { name: "Save config" }));

    await waitFor(() => {
      expect(mockedUpdateConfig).toHaveBeenCalledOnce();
    });
    expect(mockedUpdateConfig).toHaveBeenCalledWith({
      cash_buffer_pct: 0.1,
      cgt_scenario: undefined,
      updated_by: "operator",
      reason: "increasing buffer",
    });
  });

  it("sends only changed fields (scenario changed, buffer unchanged)", async () => {
    mockedUpdateConfig.mockResolvedValueOnce(
      configResponse({ cgt_scenario: "basic" }),
    );
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    await screen.findByLabelText("Cash buffer %");
    await user.selectOptions(screen.getByLabelText("CGT scenario"), "basic");
    await user.type(
      screen.getByPlaceholderText("Why are you changing this?"),
      "switching scenario",
    );
    await user.click(screen.getByRole("button", { name: "Save config" }));

    await waitFor(() => {
      expect(mockedUpdateConfig).toHaveBeenCalledOnce();
    });
    expect(mockedUpdateConfig).toHaveBeenCalledWith({
      cash_buffer_pct: undefined,
      cgt_scenario: "basic",
      updated_by: "operator",
      reason: "switching scenario",
    });
  });

  it("shows success message after save", async () => {
    mockedUpdateConfig.mockResolvedValueOnce(
      configResponse({ cash_buffer_pct: 0.1 }),
    );
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    const input = await screen.findByLabelText("Cash buffer %");
    await user.clear(input);
    await user.type(input, "10");
    await user.type(
      screen.getByPlaceholderText("Why are you changing this?"),
      "test",
    );
    await user.click(screen.getByRole("button", { name: "Save config" }));

    expect(
      await screen.findByText("Budget config updated."),
    ).toBeInTheDocument();
  });

  it("shows error alert when save fails", async () => {
    mockedUpdateConfig.mockRejectedValueOnce(new Error("network error"));
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    const input = await screen.findByLabelText("Cash buffer %");
    await user.clear(input);
    await user.type(input, "10");
    await user.type(
      screen.getByPlaceholderText("Why are you changing this?"),
      "test",
    );
    await user.click(screen.getByRole("button", { name: "Save config" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      /Failed to save budget config/,
    );
  });
});

// ---------------------------------------------------------------------------
// Capital event form
// ---------------------------------------------------------------------------

describe("BudgetConfigSection — capital event form", () => {
  it("Record event button is disabled when amount is empty", async () => {
    render(<BudgetConfigSection />);
    await screen.findByLabelText("Cash buffer %");
    const btn = screen.getByRole("button", { name: "Record event" });
    expect(btn).toBeDisabled();
  });

  it("submits a capital event with the correct payload", async () => {
    mockedCreateEvent.mockResolvedValueOnce(
      eventResponse({ amount: 1000, currency: "GBP", note: "test deposit" }),
    );
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    await screen.findByLabelText("Cash buffer %");
    await user.type(screen.getByLabelText("Amount"), "1000");
    await user.selectOptions(screen.getByLabelText("Currency"), "GBP");
    await user.type(screen.getByLabelText("Note (optional)"), "test deposit");
    await user.click(screen.getByRole("button", { name: "Record event" }));

    await waitFor(() => {
      expect(mockedCreateEvent).toHaveBeenCalledOnce();
    });
    expect(mockedCreateEvent).toHaveBeenCalledWith({
      event_type: "injection",
      amount: 1000,
      currency: "GBP",
      note: "test deposit",
    });
  });

  it("shows success message after recording an event", async () => {
    mockedCreateEvent.mockResolvedValueOnce(eventResponse());
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    await screen.findByLabelText("Cash buffer %");
    await user.type(screen.getByLabelText("Amount"), "500");
    await user.click(screen.getByRole("button", { name: "Record event" }));

    expect(
      await screen.findByText("Capital event recorded."),
    ).toBeInTheDocument();
  });

  it("shows error alert when event creation fails", async () => {
    mockedCreateEvent.mockRejectedValueOnce(new Error("server error"));
    const user = userEvent.setup();
    render(<BudgetConfigSection />);

    await screen.findByLabelText("Cash buffer %");
    await user.type(screen.getByLabelText("Amount"), "500");
    await user.click(screen.getByRole("button", { name: "Record event" }));

    const alerts = await screen.findAllByRole("alert");
    const eventAlert = alerts.find((a) =>
      a.textContent?.includes("Failed to record capital event"),
    );
    expect(eventAlert).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Capital events history
// ---------------------------------------------------------------------------

describe("BudgetConfigSection — events history", () => {
  it("shows empty state when no events exist", async () => {
    render(<BudgetConfigSection />);
    expect(
      await screen.findByText("No capital events recorded yet."),
    ).toBeInTheDocument();
  });

  it("renders event rows in the history table", async () => {
    mockedFetchEvents.mockResolvedValueOnce([
      eventResponse({
        event_id: 1,
        event_type: "injection",
        amount: 5000,
        currency: "USD",
        note: "initial deposit",
      }),
      eventResponse({
        event_id: 2,
        event_type: "withdrawal",
        amount: 1000,
        currency: "GBP",
        note: null,
      }),
    ]);
    render(<BudgetConfigSection />);

    // Wait for the table to render — "injection" and "withdrawal" also
    // appear in the form's Type dropdown, so use getAllByText and check
    // that the table added at least one extra occurrence.
    expect(await screen.findByText("initial deposit")).toBeInTheDocument();
    // "injection" appears in the dropdown option + table row = 2+
    expect(screen.getAllByText("injection").length).toBeGreaterThanOrEqual(2);
    // "withdrawal" appears in the dropdown option + table row = 2+
    expect(screen.getAllByText("withdrawal").length).toBeGreaterThanOrEqual(2);
    // Null note renders as dash
    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(1);
  });
});
