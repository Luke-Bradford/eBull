/**
 * Tests for KillSwitchSection (#1231).
 *
 * Pins the contract:
 *   - Pill renders Active (red) / Inactive (emerald) from shared config.
 *   - Confirm flow posts { active: !current, reason, activated_by } and
 *     refetches the shared config on success.
 *   - 503 (singleton missing) surfaces a distinct fixed phrase.
 *   - Loading / error states render skeleton / SectionError.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ApiError } from "@/api/client";
import { KillSwitchSection } from "@/components/admin/KillSwitchSection";
import { TestConfigProvider } from "@/lib/ConfigContext";
import type { ConfigResponse } from "@/api/types";
import type { AsyncState } from "@/lib/useAsync";

vi.mock("@/api/config", () => ({ postKillSwitch: vi.fn() }));
vi.mock("@/lib/session", () => ({
  useSession: () => ({ operator: { id: "1", username: "luke" } }),
}));

import { postKillSwitch } from "@/api/config";

const mockedPost = vi.mocked(postKillSwitch);

function config(active: boolean): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "GBP",
      updated_at: "2026-04-18T00:00:00Z",
      updated_by: "system",
      reason: "",
    },
    kill_switch: {
      active,
      activated_at: active ? "2026-06-24T00:00:00Z" : null,
      activated_by: active ? "luke" : null,
      reason: active ? "manual ops" : null,
    },
  };
}

function renderSection(value: Partial<AsyncState<ConfigResponse>>) {
  const refetch = vi.fn();
  render(
    <TestConfigProvider value={{ refetch, ...value }}>
      <KillSwitchSection />
    </TestConfigProvider>,
  );
  return { refetch };
}

beforeEach(() => {
  mockedPost.mockReset();
});
afterEach(() => {
  vi.clearAllMocks();
});

describe("KillSwitchSection", () => {
  it("renders Inactive pill + Activate button when kill switch is off", () => {
    renderSection({ data: config(false) });
    expect(screen.getByText("Inactive")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Activate kill switch" }),
    ).toBeInTheDocument();
  });

  it("renders Active pill + Deactivate button when kill switch is on", () => {
    renderSection({ data: config(true) });
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Deactivate kill switch" }),
    ).toBeInTheDocument();
  });

  it("shows a skeleton while config is loading with no data", () => {
    renderSection({ loading: true, data: null });
    expect(
      screen.queryByRole("button", { name: /kill switch/i }),
    ).not.toBeInTheDocument();
  });

  it("shows SectionError with retry when config failed with no data", async () => {
    const { refetch } = renderSection({ error: new Error("boom"), data: null });
    const retry = screen.getByRole("button", { name: /retry/i });
    await userEvent.click(retry);
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it("activates: posts { active: true, ... } and refetches on success", async () => {
    mockedPost.mockResolvedValue({
      active: true,
      activated_at: "2026-06-24T00:00:00Z",
      activated_by: "luke",
      reason: "manual ops",
    });
    const { refetch } = renderSection({ data: config(false) });

    await userEvent.click(
      screen.getByRole("button", { name: "Activate kill switch" }),
    );
    // Reason is pre-filled from the first common reason.
    await userEvent.click(screen.getByRole("button", { name: "Activate" }));

    await waitFor(() => expect(mockedPost).toHaveBeenCalledTimes(1));
    expect(mockedPost).toHaveBeenCalledWith({
      active: true,
      reason: "manual ops",
      activated_by: "luke",
    });
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it("deactivates: posts { active: false, ... } with edited reason", async () => {
    mockedPost.mockResolvedValue({
      active: false,
      activated_at: null,
      activated_by: "luke",
      reason: "after maintenance",
    });
    const { refetch } = renderSection({ data: config(true) });

    await userEvent.click(
      screen.getByRole("button", { name: "Deactivate kill switch" }),
    );
    const reasonInput = screen.getByLabelText("Reason");
    await userEvent.clear(reasonInput);
    await userEvent.type(reasonInput, "after maintenance");
    await userEvent.click(screen.getByRole("button", { name: "Deactivate" }));

    await waitFor(() => expect(mockedPost).toHaveBeenCalledTimes(1));
    expect(mockedPost).toHaveBeenCalledWith({
      active: false,
      reason: "after maintenance",
      activated_by: "luke",
    });
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it("blocks confirm when reason is blank", async () => {
    const { refetch } = renderSection({ data: config(false) });
    await userEvent.click(
      screen.getByRole("button", { name: "Activate kill switch" }),
    );
    const reasonInput = screen.getByLabelText("Reason");
    await userEvent.clear(reasonInput);
    expect(screen.getByRole("button", { name: "Activate" })).toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "Activate" }));
    expect(mockedPost).not.toHaveBeenCalled();
    expect(refetch).not.toHaveBeenCalled();
  });

  it("surfaces a distinct phrase on 503 (singleton missing)", async () => {
    mockedPost.mockRejectedValue(new ApiError(503, "kill switch unavailable"));
    const { refetch } = renderSection({ data: config(false) });

    await userEvent.click(
      screen.getByRole("button", { name: "Activate kill switch" }),
    );
    await userEvent.click(screen.getByRole("button", { name: "Activate" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      /config singleton row is missing/i,
    );
    // Modal stays open on failure; no refetch on the error path.
    expect(refetch).not.toHaveBeenCalled();
  });

  it("surfaces a generic phrase on a non-503 failure", async () => {
    mockedPost.mockRejectedValue(new ApiError(500, "boom"));
    renderSection({ data: config(false) });

    await userEvent.click(
      screen.getByRole("button", { name: "Activate kill switch" }),
    );
    await userEvent.click(screen.getByRole("button", { name: "Activate" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      /Failed to update the kill switch/i,
    );
  });
});
